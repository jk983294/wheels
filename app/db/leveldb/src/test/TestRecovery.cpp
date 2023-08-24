#include <catch.hpp>
#include <iostream>

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "testutil.h"
#include "util/logging.h"

using namespace std;
using namespace leveldb;

class RecoveryTest {
 public:
  RecoveryTest() : env_(Env::Default()), db_(nullptr) {
    dbname_ = "/tmp/recovery_test";
    DestroyDB(dbname_, Options());
    Open();
  }

  ~RecoveryTest() {
    Close();
    DestroyDB(dbname_, Options());
  }

  DBImpl* dbfull() const { return reinterpret_cast<DBImpl*>(db_); }
  Env* env() const { return env_; }

  bool CanAppend() {
    WritableFile* tmp;
    Status s = env_->NewAppendableFile(CurrentFileName(dbname_), &tmp);
    delete tmp;
    if (s.IsNotSupportedError()) {
      return false;
    } else {
      return true;
    }
  }

  void Close() {
    delete db_;
    db_ = nullptr;
  }

  Status OpenWithStatus(Options* options = nullptr) {
    Close();
    Options opts;
    if (options != nullptr) {
      opts = *options;
    } else {
      opts.reuse_logs = true;  // TODO(sanjay): test both ways
      opts.create_if_missing = true;
    }
    if (opts.env == nullptr) {
      opts.env = env_;
    }
    return DB::Open(opts, dbname_, &db_);
  }

  void Open(Options* options = nullptr) {
    ASSERT_LEVELDB_OK(OpenWithStatus(options));
    REQUIRE(1 == NumLogs());
  }

  Status Put(const std::string& k, const std::string& v) {
    return db_->Put(WriteOptions(), k, v);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    std::string result;
    Status s = db_->Get(ReadOptions(), k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  std::string ManifestFileName() {
    std::string current;
    EXPECT_LEVELDB_OK(
        ReadFileToString(env_, CurrentFileName(dbname_), &current));
    size_t len = current.size();
    if (len > 0 && current[len - 1] == '\n') {
      current.resize(len - 1);
    }
    return dbname_ + "/" + current;
  }

  std::string LogName(uint64_t number) { return LogFileName(dbname_, number); }

  size_t RemoveLogFiles() {
    // Linux allows unlinking open files, but Windows does not.
    // Closing the db allows for file deletion.
    Close();
    std::vector<uint64_t> logs = GetFiles(kLogFile);
    for (size_t i = 0; i < logs.size(); i++) {
      EXPECT_LEVELDB_OK(env_->RemoveFile(LogName(logs[i])));
    }
    return logs.size();
  }

  void RemoveManifestFile() {
    ASSERT_LEVELDB_OK(env_->RemoveFile(ManifestFileName()));
  }

  uint64_t FirstLogFile() { return GetFiles(kLogFile)[0]; }

  std::vector<uint64_t> GetFiles(FileType t) {
    std::vector<std::string> filenames;
    EXPECT_LEVELDB_OK(env_->GetChildren(dbname_, &filenames));
    std::vector<uint64_t> result;
    for (size_t i = 0; i < filenames.size(); i++) {
      uint64_t number;
      FileType type;
      if (ParseFileName(filenames[i], &number, &type) && type == t) {
        result.push_back(number);
      }
    }
    return result;
  }

  int NumLogs() { return GetFiles(kLogFile).size(); }

  int NumTables() { return GetFiles(kTableFile).size(); }

  uint64_t FileSize(const std::string& fname) {
    uint64_t result;
    EXPECT_LEVELDB_OK(env_->GetFileSize(fname, &result));
    return result;
  }

  void CompactMemTable() { dbfull()->TEST_CompactMemTable(); }

  // Directly construct a log file that sets key to val.
  void MakeLogFile(uint64_t lognum, SequenceNumber seq, Slice key, Slice val) {
    std::string fname = LogFileName(dbname_, lognum);
    WritableFile* file;
    ASSERT_LEVELDB_OK(env_->NewWritableFile(fname, &file));
    log::Writer writer(file);
    WriteBatch batch;
    batch.Put(key, val);
    WriteBatchInternal::SetSequence(&batch, seq);
    ASSERT_LEVELDB_OK(writer.AddRecord(WriteBatchInternal::Contents(&batch)));
    ASSERT_LEVELDB_OK(file->Flush());
    delete file;
  }

 private:
  std::string dbname_;
  Env* env_;
  DB* db_;
};

TEST_CASE("Recovery ManifestReused", "[Recovery]") {
  RecoveryTest test;
  if (!test.CanAppend()) {
    std::fprintf(stderr,
                 "skipping test because env does not support appending\n");
    return;
  }
  ASSERT_LEVELDB_OK(test.Put("foo", "bar"));
  test.Close();
  std::string old_manifest = test.ManifestFileName();
  test.Open();
  REQUIRE(old_manifest == test.ManifestFileName());
  REQUIRE("bar" == test.Get("foo"));
  test.Open();
  REQUIRE(old_manifest == test.ManifestFileName());
  REQUIRE("bar" == test.Get("foo"));
}
TEST_CASE("Recovery LargeManifestCompacted", "[Recovery]") {
  RecoveryTest test;
  if (!test.CanAppend()) {
    std::fprintf(stderr,
                 "skipping test because env does not support appending\n");
    return;
  }
  ASSERT_LEVELDB_OK(test.Put("foo", "bar"));
  test.Close();
  std::string old_manifest = test.ManifestFileName();

  // Pad with zeroes to make manifest file very big.
  {
    uint64_t len = test.FileSize(old_manifest);
    WritableFile* file;
    ASSERT_LEVELDB_OK(test.env()->NewAppendableFile(old_manifest, &file));
    std::string zeroes(3 * 1048576 - static_cast<size_t>(len), 0);
    ASSERT_LEVELDB_OK(file->Append(zeroes));
    ASSERT_LEVELDB_OK(file->Flush());
    delete file;
  }

  test.Open();
  std::string new_manifest = test.ManifestFileName();
  REQUIRE(old_manifest != new_manifest);
  REQUIRE(10000 > test.FileSize(new_manifest));
  REQUIRE("bar" == test.Get("foo"));

  test.Open();
  REQUIRE(new_manifest == test.ManifestFileName());
  REQUIRE("bar" == test.Get("foo"));
}
TEST_CASE("Recovery NoLogFiles", "[Recovery]") {
  RecoveryTest test;
  ASSERT_LEVELDB_OK(test.Put("foo", "bar"));
  REQUIRE(1 == test.RemoveLogFiles());
  test.Open();
  REQUIRE("NOT_FOUND" == test.Get("foo"));
  test.Open();
  REQUIRE("NOT_FOUND" == test.Get("foo"));
}
TEST_CASE("Recovery LogFileReuse", "[Recovery]") {
  RecoveryTest test;
  if (!test.CanAppend()) {
    std::fprintf(stderr,
                 "skipping test because env does not support appending\n");
    return;
  }
  for (int i = 0; i < 2; i++) {
    ASSERT_LEVELDB_OK(test.Put("foo", "bar"));
    if (i == 0) {
      // Compact to ensure current log is empty
      test.CompactMemTable();
    }
    test.Close();
    REQUIRE(1 == test.NumLogs());
    uint64_t number = test.FirstLogFile();
    if (i == 0) {
      REQUIRE(0 == test.FileSize(test.LogName(number)));
    } else {
      REQUIRE(0 < test.FileSize(test.LogName(number)));
    }
    test.Open();
    REQUIRE(1 == test.NumLogs());
    REQUIRE(number == test.FirstLogFile());
    REQUIRE("bar" == test.Get("foo"));
    test.Open();
    REQUIRE(1 == test.NumLogs());
    REQUIRE(number == test.FirstLogFile());
    REQUIRE("bar" == test.Get("foo"));
  }
}
TEST_CASE("Recovery MultipleMemTables", "[Recovery]") {
  RecoveryTest test;
  // Make a large log.
  const int kNum = 1000;
  for (int i = 0; i < kNum; i++) {
    char buf[100];
    std::snprintf(buf, sizeof(buf), "%050d", i);
    ASSERT_LEVELDB_OK(test.Put(buf, buf));
  }
  REQUIRE(0 == test.NumTables());
  test.Close();
  REQUIRE(0 == test.NumTables());
  REQUIRE(1 == test.NumLogs());
  uint64_t old_log_file = test.FirstLogFile();

  // Force creation of multiple memtables by reducing the write buffer size.
  Options opt;
  opt.reuse_logs = true;
  opt.write_buffer_size = (kNum * 100) / 2;
  test.Open(&opt);
  REQUIRE(2 <= test.NumTables());
  REQUIRE(1 == test.NumLogs());
  REQUIRE(old_log_file != test.FirstLogFile());
  for (int i = 0; i < kNum; i++) {
    char buf[100];
    std::snprintf(buf, sizeof(buf), "%050d", i);
    REQUIRE(buf == test.Get(buf));
  }
}
TEST_CASE("Recovery MultipleLogFiles", "[Recovery]") {
  RecoveryTest test;
  ASSERT_LEVELDB_OK(test.Put("foo", "bar"));
  test.Close();
  REQUIRE(1 == test.NumLogs());

  // Make a bunch of uncompacted log files.
  uint64_t old_log = test.FirstLogFile();
  test.MakeLogFile(old_log + 1, 1000, "hello", "world");
  test.MakeLogFile(old_log + 2, 1001, "hi", "there");
  test.MakeLogFile(old_log + 3, 1002, "foo", "bar2");

  // Recover and check that all log files were processed.
  test.Open();
  REQUIRE(1 <= test.NumTables());
  REQUIRE(1 == test.NumLogs());
  uint64_t new_log = test.FirstLogFile();
  REQUIRE(old_log + 3 <= new_log);
  REQUIRE("bar2" == test.Get("foo"));
  REQUIRE("world" == test.Get("hello"));
  REQUIRE("there" == test.Get("hi"));

  // Test that previous recovery produced recoverable state.
  test.Open();
  REQUIRE(1 <= test.NumTables());
  REQUIRE(1 == test.NumLogs());
  if (test.CanAppend()) {
    REQUIRE(new_log == test.FirstLogFile());
  }
  REQUIRE("bar2" == test.Get("foo"));
  REQUIRE("world" == test.Get("hello"));
  REQUIRE("there" == test.Get("hi"));

  // Check that introducing an older log file does not cause it to be re-read.
  test.Close();
  test.MakeLogFile(old_log + 1, 2000, "hello", "stale write");
  test.Open();
  REQUIRE(1 <= test.NumTables());
  REQUIRE(1 == test.NumLogs());
  if (test.CanAppend()) {
    REQUIRE(new_log == test.FirstLogFile());
  }
  REQUIRE("bar2" == test.Get("foo"));
  REQUIRE("world" == test.Get("hello"));
  REQUIRE("there" == test.Get("hi"));
}
TEST_CASE("Recovery ManifestMissing", "[Recovery]") {
  RecoveryTest test;
  ASSERT_LEVELDB_OK(test.Put("foo", "bar"));
  test.Close();
  test.RemoveManifestFile();

  Status status = test.OpenWithStatus();

  REQUIRE(status.IsCorruption());
}
