#include <catch.hpp>

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"

using namespace std;
using namespace leveldb;
using namespace leveldb::log;

// Construct a string of the specified length made out of the supplied
// partial string.
static std::string BigString(const std::string& partial_string, size_t n) {
  std::string result;
  while (result.size() < n) {
    result.append(partial_string);
  }
  result.resize(n);
  return result;
}

// Construct a string from a number
static std::string NumberString(int n) {
  char buf[50];
  std::snprintf(buf, sizeof(buf), "%d.", n);
  return std::string(buf);
}

// Return a skewed potentially long string
static std::string RandomSkewedString(int i, Random* rnd) {
  return BigString(NumberString(i), rnd->Skewed(17));
}

class LogTest {
 public:
  LogTest()
      : reading_(false),
        writer_(new log::Writer(&dest_)),
        reader_(new log::Reader(&source_, &report_, true /*checksum*/,
                                0 /*initial_offset*/)) {}

  ~LogTest() {
    delete writer_;
    delete reader_;
  }

  void ReopenForAppend() {
    delete writer_;
    writer_ = new log::Writer(&dest_, dest_.contents_.size());
  }

  void Write(const std::string& msg) {
    REQUIRE(!reading_);
    writer_->AddRecord(Slice(msg));
  }

  size_t WrittenBytes() const { return dest_.contents_.size(); }

  std::string Read() {
    if (!reading_) {
      reading_ = true;
      source_.contents_ = Slice(dest_.contents_);
    }
    std::string scratch;
    Slice record;
    if (reader_->ReadRecord(&record, &scratch)) {
      return record.ToString();
    } else {
      return "EOF";
    }
  }

  void IncrementByte(int offset, int delta) {
    dest_.contents_[offset] += delta;
  }

  void SetByte(int offset, char new_byte) {
    dest_.contents_[offset] = new_byte;
  }

  void ShrinkSize(int bytes) {
    dest_.contents_.resize(dest_.contents_.size() - bytes);
  }

  void FixChecksum(int header_offset, int len) {
    // Compute crc of type/len/data
    uint32_t crc = crc32c::Value(&dest_.contents_[header_offset + 6], 1 + len);
    crc = crc32c::Mask(crc);
    EncodeFixed32(&dest_.contents_[header_offset], crc);
  }

  void ForceError() { source_.force_error_ = true; }

  size_t DroppedBytes() const { return report_.dropped_bytes_; }

  std::string ReportMessage() const { return report_.message_; }

  // Returns OK iff recorded error message contains "msg"
  std::string MatchError(const std::string& msg) const {
    if (report_.message_.find(msg) == std::string::npos) {
      return report_.message_;
    } else {
      return "OK";
    }
  }

  void WriteInitialOffsetLog() {
    for (int i = 0; i < num_initial_offset_records_; i++) {
      std::string record(initial_offset_record_sizes_[i],
                         static_cast<char>('a' + i));
      Write(record);
    }
  }

  void StartReadingAt(uint64_t initial_offset) {
    delete reader_;
    reader_ = new Reader(&source_, &report_, true /*checksum*/, initial_offset);
  }

  void CheckOffsetPastEndReturnsNoRecords(uint64_t offset_past_end) {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    Reader* offset_reader = new Reader(&source_, &report_, true /*checksum*/,
                                       WrittenBytes() + offset_past_end);
    Slice record;
    std::string scratch;
    REQUIRE(!offset_reader->ReadRecord(&record, &scratch));
    delete offset_reader;
  }

  void CheckInitialOffsetRecord(uint64_t initial_offset,
                                int expected_record_offset) {
    WriteInitialOffsetLog();
    reading_ = true;
    source_.contents_ = Slice(dest_.contents_);
    Reader* offset_reader =
        new Reader(&source_, &report_, true /*checksum*/, initial_offset);

    // Read all records from expected_record_offset through the last one.
    REQUIRE(expected_record_offset < num_initial_offset_records_);
    for (; expected_record_offset < num_initial_offset_records_;
         ++expected_record_offset) {
      Slice record;
      std::string scratch;
      REQUIRE(offset_reader->ReadRecord(&record, &scratch));
      REQUIRE(initial_offset_record_sizes_[expected_record_offset] ==
              record.size());
      REQUIRE(initial_offset_last_record_offsets_[expected_record_offset] ==
              offset_reader->LastRecordOffset());
      REQUIRE((char)('a' + expected_record_offset) == record.data()[0]);
    }
    delete offset_reader;
  }

 private:
  class StringDest : public WritableFile {
   public:
    Status Close() override { return Status::OK(); }
    Status Flush() override { return Status::OK(); }
    Status Sync() override { return Status::OK(); }
    Status Append(const Slice& slice) override {
      contents_.append(slice.data(), slice.size());
      return Status::OK();
    }

    std::string contents_;
  };

  class StringSource : public SequentialFile {
   public:
    StringSource() : force_error_(false), returned_partial_(false) {}

    Status Read(size_t n, Slice* result, char* scratch) override {
      REQUIRE(!returned_partial_);

      if (force_error_) {
        force_error_ = false;
        returned_partial_ = true;
        return Status::Corruption("read error");
      }

      if (contents_.size() < n) {
        n = contents_.size();
        returned_partial_ = true;
      }
      *result = Slice(contents_.data(), n);
      contents_.remove_prefix(n);
      return Status::OK();
    }

    Status Skip(uint64_t n) override {
      if (n > contents_.size()) {
        contents_.clear();
        return Status::NotFound("in-memory file skipped past end");
      }

      contents_.remove_prefix(n);

      return Status::OK();
    }

    Slice contents_;
    bool force_error_;
    bool returned_partial_;
  };

  class ReportCollector : public log::Reader::Reporter {
   public:
    ReportCollector() : dropped_bytes_(0) {}
    void Corruption(size_t bytes, const Status& status) override {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }

    size_t dropped_bytes_;
    std::string message_;
  };

  // Record metadata for testing initial offset functionality
  static size_t initial_offset_record_sizes_[];
  static uint64_t initial_offset_last_record_offsets_[];
  static int num_initial_offset_records_;

  StringDest dest_;
  StringSource source_;
  ReportCollector report_;
  bool reading_;
  log::Writer* writer_;
  log::Reader* reader_;
};

size_t LogTest::initial_offset_record_sizes_[] = {
    10000,  // Two sizable records in first block
    10000,
    2 * log::kBlockSize - 1000,  // Span three blocks
    1,
    13716,                          // Consume all but two bytes of block 3.
    log::kBlockSize - kHeaderSize,  // Consume the entirety of block 4.
};

uint64_t LogTest::initial_offset_last_record_offsets_[] = {
    0,
    kHeaderSize + 10000,
    2 * (kHeaderSize + 10000),
    2 * (kHeaderSize + 10000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize,
    2 * (kHeaderSize + 10000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize +
        kHeaderSize + 1,
    3 * log::kBlockSize,
};

// LogTest::initial_offset_last_record_offsets_ must be defined before this.
int LogTest::num_initial_offset_records_ =
    sizeof(LogTest::initial_offset_last_record_offsets_) / sizeof(uint64_t);

TEST_CASE("LogTest Empty", "[LogTest]") {
  LogTest log;
  REQUIRE(log.Read() == "EOF");
}

TEST_CASE("LogTest", "[LogTest]") {
  LogTest log;
  log.Write("foo");
  log.Write("bar");
  log.Write("");
  log.Write("xxxx");
  REQUIRE("foo" == log.Read());
  REQUIRE("bar" == log.Read());
  REQUIRE("" == log.Read());
  REQUIRE("xxxx" == log.Read());
  REQUIRE("EOF" == log.Read());
  REQUIRE("EOF" == log.Read());
}

TEST_CASE("LogTest ManyBlocks", "[LogTest]") {
  LogTest log;
  for (int i = 0; i < 100000; i++) {
    log.Write(NumberString(i));
  }
  for (int i = 0; i < 100000; i++) {
    REQUIRE(NumberString(i) == log.Read());
  }
}

TEST_CASE("LogTest Fragmentation", "[LogTest]") {
  LogTest log;
  log.Write("small");
  log.Write(BigString("medium", 50000));
  log.Write(BigString("large", 100000));
  REQUIRE("small" == log.Read());
  REQUIRE(BigString("medium", 50000) == log.Read());
  REQUIRE(BigString("large", 100000) == log.Read());
  REQUIRE("EOF" == log.Read());
}

TEST_CASE("LogTest MarginalTrailer", "[LogTest]") {
  LogTest log;
  const int n = kBlockSize - 2 * kHeaderSize;
  log.Write(BigString("foo", n));
  REQUIRE(kBlockSize - kHeaderSize == log.WrittenBytes());
  log.Write("");
  log.Write("bar");
  REQUIRE(BigString("foo", n) == log.Read());
  REQUIRE("" == log.Read());
  REQUIRE("bar" == log.Read());
  REQUIRE("EOF" == log.Read());
}

TEST_CASE("LogTest MarginalTrailer2", "[LogTest]") {
  LogTest log;
  const int n = kBlockSize - 2 * kHeaderSize;
  log.Write(BigString("foo", n));
  REQUIRE(kBlockSize - kHeaderSize == log.WrittenBytes());
  log.Write("bar");
  REQUIRE(BigString("foo", n) == log.Read());
  REQUIRE("bar" == log.Read());
  REQUIRE("EOF" == log.Read());
  REQUIRE(0 == log.DroppedBytes());
  REQUIRE("" == log.ReportMessage());
}

TEST_CASE("LogTest ShortTrailer", "[LogTest]") {
  LogTest log;
  const int n = kBlockSize - 2 * kHeaderSize + 4;
  log.Write(BigString("foo", n));
  REQUIRE(kBlockSize - kHeaderSize + 4 == log.WrittenBytes());
  log.Write("");
  log.Write("bar");
  REQUIRE(BigString("foo", n) == log.Read());
  REQUIRE("" == log.Read());
  REQUIRE("bar" == log.Read());
  REQUIRE("EOF" == log.Read());
}

TEST_CASE("LogTest AlignedEof", "[LogTest]") {
  LogTest log;
  const int n = kBlockSize - 2 * kHeaderSize + 4;
  log.Write(BigString("foo", n));
  REQUIRE(kBlockSize - kHeaderSize + 4 == log.WrittenBytes());
  REQUIRE(BigString("foo", n) == log.Read());
  REQUIRE("EOF" == log.Read());
}

TEST_CASE("LogTest OpenForAppend", "[LogTest]") {
  LogTest log;
  log.Write("hello");
  log.ReopenForAppend();
  log.Write("world");
  REQUIRE("hello" == log.Read());
  REQUIRE("world" == log.Read());
  REQUIRE("EOF" == log.Read());
}

TEST_CASE("LogTest RandomRead", "[LogTest]") {
  LogTest log;
  const int N = 500;
  Random write_rnd(301);
  for (int i = 0; i < N; i++) {
    log.Write(RandomSkewedString(i, &write_rnd));
  }
  Random read_rnd(301);
  for (int i = 0; i < N; i++) {
    REQUIRE(RandomSkewedString(i, &read_rnd) == log.Read());
  }
  REQUIRE("EOF" == log.Read());
}

TEST_CASE("LogTest ReadError", "[LogTest]") {
  LogTest log;
  log.Write("foo");
  log.ForceError();
  REQUIRE("EOF" == log.Read());
  REQUIRE(kBlockSize == log.DroppedBytes());
  REQUIRE("OK" == log.MatchError("read error"));
}

TEST_CASE("LogTest BadRecordType", "[LogTest]") {
  LogTest log;
  log.Write("foo");
  // Type is stored in header[6]
  log.IncrementByte(6, 100);
  log.FixChecksum(0, 3);
  REQUIRE("EOF" == log.Read());
  REQUIRE(3 == log.DroppedBytes());
  REQUIRE("OK" == log.MatchError("unknown record type"));
}

TEST_CASE("LogTest TruncatedTrailingRecordIsIgnored", "[LogTest]") {
  LogTest log;
  log.Write("foo");
  log.ShrinkSize(4);  // Drop all payload as well as a header byte
  REQUIRE("EOF" == log.Read());
  // Truncated last record is ignored, not treated as an error.
  REQUIRE(0 == log.DroppedBytes());
  REQUIRE("" == log.ReportMessage());
}

TEST_CASE("LogTest BadLength", "[LogTest]") {
  LogTest log;
  const int kPayloadSize = kBlockSize - kHeaderSize;
  log.Write(BigString("bar", kPayloadSize));
  log.Write("foo");
  // Least significant size byte is stored in header[4].
  log.IncrementByte(4, 1);
  REQUIRE("foo" == log.Read());
  REQUIRE(kBlockSize == log.DroppedBytes());
  REQUIRE("OK" == log.MatchError("bad record length"));
}

TEST_CASE("LogTest BadLengthAtEndIsIgnored", "[LogTest]") {
  LogTest log;
  log.Write("foo");
  log.ShrinkSize(1);
  REQUIRE("EOF" == log.Read());
  REQUIRE(0 == log.DroppedBytes());
  REQUIRE("" == log.ReportMessage());
}

TEST_CASE("LogTest ChecksumMismatch", "[LogTest]") {
  LogTest log;
  log.Write("foo");
  log.IncrementByte(0, 10);
  REQUIRE("EOF" == log.Read());
  REQUIRE(10 == log.DroppedBytes());
  REQUIRE("OK" == log.MatchError("checksum mismatch"));
}

TEST_CASE("LogTest UnexpectedMiddleType", "[LogTest]") {
  LogTest log;
  log.Write("foo");
  log.SetByte(6, kMiddleType);
  log.FixChecksum(0, 3);
  REQUIRE("EOF" == log.Read());
  REQUIRE(3 == log.DroppedBytes());
  REQUIRE("OK" == log.MatchError("missing start"));
}

TEST_CASE("LogTest UnexpectedLastType", "[LogTest]") {
  LogTest log;
  log.Write("foo");
  log.SetByte(6, kLastType);
  log.FixChecksum(0, 3);
  REQUIRE("EOF" == log.Read());
  REQUIRE(3 == log.DroppedBytes());
  REQUIRE("OK" == log.MatchError("missing start"));
}

TEST_CASE("LogTest UnexpectedFullType", "[LogTest]") {
  LogTest log;
  log.Write("foo");
  log.Write("bar");
  log.SetByte(6, kFirstType);
  log.FixChecksum(0, 3);
  REQUIRE("bar" == log.Read());
  REQUIRE("EOF" == log.Read());
  REQUIRE(3 == log.DroppedBytes());
  REQUIRE("OK" == log.MatchError("partial record without end"));
}

TEST_CASE("LogTest UnexpectedFirstType", "[LogTest]") {
  LogTest log;
  log.Write("foo");
  log.Write(BigString("bar", 100000));
  log.SetByte(6, kFirstType);
  log.FixChecksum(0, 3);
  REQUIRE(BigString("bar", 100000) == log.Read());
  REQUIRE("EOF" == log.Read());
  REQUIRE(3 == log.DroppedBytes());
  REQUIRE("OK" == log.MatchError("partial record without end"));
}

TEST_CASE("LogTest MissingLastIsIgnored", "[LogTest]") {
  LogTest log;
  log.Write(BigString("bar", kBlockSize));
  // Remove the LAST block, including header.
  log.ShrinkSize(14);
  REQUIRE("EOF" == log.Read());
  REQUIRE("" == log.ReportMessage());
  REQUIRE(0 == log.DroppedBytes());
}

TEST_CASE("LogTest PartialLastIsIgnored", "[LogTest]") {
  LogTest log;
  log.Write(BigString("bar", kBlockSize));
  // Cause a bad record length in the LAST block.
  log.ShrinkSize(1);
  REQUIRE("EOF" == log.Read());
  REQUIRE("" == log.ReportMessage());
  REQUIRE(0 == log.DroppedBytes());
}

TEST_CASE("LogTest SkipIntoMultiRecord", "[LogTest]") {
  LogTest log;
  // Consider a fragmented record:
  //    first(R1), middle(R1), last(R1), first(R2)
  // If initial_offset points to a record after first(R1) but before first(R2)
  // incomplete fragment errors are not actual errors, and must be suppressed
  // until a new first or full record is encountered.
  log.Write(BigString("foo", 3 * kBlockSize));
  log.Write("correct");
  log.StartReadingAt(kBlockSize);

  REQUIRE("correct" == log.Read());
  REQUIRE("" == log.ReportMessage());
  REQUIRE(0 == log.DroppedBytes());
  REQUIRE("EOF" == log.Read());
}

TEST_CASE("LogTest ErrorJoinsRecords", "[LogTest]") {
  LogTest log;
  // Consider two fragmented records:
  //    first(R1) last(R1) first(R2) last(R2)
  // where the middle two fragments disappear.  We do not want
  // first(R1),last(R2) to get joined and returned as a valid record.

  // Write records that span two blocks
  log.Write(BigString("foo", kBlockSize));
  log.Write(BigString("bar", kBlockSize));
  log.Write("correct");

  // Wipe the middle block
  for (int offset = kBlockSize; offset < 2 * kBlockSize; offset++) {
    log.SetByte(offset, 'x');
  }

  REQUIRE("correct" == log.Read());
  REQUIRE("EOF" == log.Read());
  const size_t dropped = log.DroppedBytes();
  REQUIRE(dropped <= 2 * kBlockSize + 100);
  REQUIRE(dropped >= 2 * kBlockSize);
}

TEST_CASE("LogTest ReadStart", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(0, 0);
}

TEST_CASE("LogTest ReadSecondOneOff", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(1, 1);
}

TEST_CASE("LogTest ReadSecondTenThousand", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(10000, 1);
}

TEST_CASE("LogTest ReadSecondStart", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(10007, 1);
}

TEST_CASE("LogTest ReadThirdOneOff", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(10008, 2);
}

TEST_CASE("LogTest ReadThirdStart", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(20014, 2);
}

TEST_CASE("LogTest ReadFourthOneOff", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(20015, 3);
}

TEST_CASE("LogTest ReadFourthFirstBlockTrailer", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(log::kBlockSize - 4, 3);
}

TEST_CASE("LogTest ReadFourthMiddleBlock", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(log::kBlockSize + 1, 3);
}

TEST_CASE("LogTest ReadFourthLastBlock", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(2 * log::kBlockSize + 1, 3);
}

TEST_CASE("LogTest ReadFourthStart", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(
      2 * (kHeaderSize + 1000) + (2 * log::kBlockSize - 1000) + 3 * kHeaderSize,
      3);
}

TEST_CASE("LogTest ReadInitialOffsetIntoBlockPadding", "[LogTest]") {
  LogTest log;
  log.CheckInitialOffsetRecord(3 * log::kBlockSize - 3, 5);
}

TEST_CASE("LogTest ReadEnd", "[LogTest]") {
  LogTest log;
  log.CheckOffsetPastEndReturnsNoRecords(0);
}

TEST_CASE("LogTest ReadPastEnd", "[LogTest]") {
  LogTest log;
  log.CheckOffsetPastEndReturnsNoRecords(5);
}
