#include <sys/types.h>

#include <catch.hpp>
#include <iostream>

#include "db/db_impl.h"
#include "db/filename.h"
#include "db/log_format.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/table.h"
#include "leveldb/write_batch.h"
#include "testutil.h"
#include "util/logging.h"

using namespace std;
using namespace leveldb;

static const int kValueSize = 1000;

class CorruptionTest {
 public:
  CorruptionTest()
      : db_(nullptr),
        dbname_("/memenv/corruption_test"),
        tiny_cache_(NewLRUCache(100)) {
    options_.env = &env_;
    options_.block_cache = tiny_cache_;
    DestroyDB(dbname_, options_);

    options_.create_if_missing = true;
    Reopen();
    options_.create_if_missing = false;
  }

  ~CorruptionTest() {
    delete db_;
    delete tiny_cache_;
  }

  Status TryReopen() {
    delete db_;
    db_ = nullptr;
    return DB::Open(options_, dbname_, &db_);
  }

  void Reopen() { ASSERT_LEVELDB_OK(TryReopen()); }

  void RepairDB() {
    delete db_;
    db_ = nullptr;
    ASSERT_LEVELDB_OK(::leveldb::RepairDB(dbname_, options_));
  }

  void Build(int n) {
    std::string key_space, value_space;
    WriteBatch batch;
    for (int i = 0; i < n; i++) {
      // if ((i % 100) == 0) std::fprintf(stderr, "@ %d of %d\n", i, n);
      Slice key = Key(i, &key_space);
      batch.Clear();
      batch.Put(key, Value(i, &value_space));
      WriteOptions options;
      // Corrupt() doesn't work without this sync on windows; stat reports 0 for
      // the file size.
      if (i == n - 1) {
        options.sync = true;
      }
      ASSERT_LEVELDB_OK(db_->Write(options, &batch));
    }
  }

  void Check(int min_expected, int max_expected) {
    int next_expected = 0;
    int missed = 0;
    int bad_keys = 0;
    int bad_values = 0;
    int correct = 0;
    std::string value_space;
    Iterator* iter = db_->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      uint64_t key;
      Slice in(iter->key());
      if (in == "" || in == "~") {
        // Ignore boundary keys.
        continue;
      }
      if (!ConsumeDecimalNumber(&in, &key) || !in.empty() ||
          key < next_expected) {
        bad_keys++;
        continue;
      }
      missed += (key - next_expected);
      next_expected = key + 1;
      if (iter->value() != Value(key, &value_space)) {
        bad_values++;
      } else {
        correct++;
      }
    }
    delete iter;

    std::fprintf(
        stderr,
        "expected=%d..%d; got=%d; bad_keys=%d; bad_values=%d; missed=%d\n",
        min_expected, max_expected, correct, bad_keys, bad_values, missed);
    REQUIRE(min_expected <= correct);
    REQUIRE(max_expected >= correct);
  }

  void Corrupt(FileType filetype, int offset, int bytes_to_corrupt) {
    // Pick file to corrupt
    std::vector<std::string> filenames;
    ASSERT_LEVELDB_OK(env_.target()->GetChildren(dbname_, &filenames));
    uint64_t number;
    FileType type;
    std::string fname;
    int picked_number = -1;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) && type == filetype &&
          int(number) > picked_number) {  // Pick latest file
        fname = dbname_ + "/" + filenames[i];
        picked_number = number;
      }
    }
    REQUIRE(!fname.empty());

    uint64_t file_size;
    ASSERT_LEVELDB_OK(env_.target()->GetFileSize(fname, &file_size));

    if (offset < 0) {
      // Relative to end of file; make it absolute
      if (-offset > file_size) {
        offset = 0;
      } else {
        offset = file_size + offset;
      }
    }
    if (offset > file_size) {
      offset = file_size;
    }
    if (offset + bytes_to_corrupt > file_size) {
      bytes_to_corrupt = file_size - offset;
    }

    // Do it
    std::string contents;
    Status s = ReadFileToString(env_.target(), fname, &contents);
    REQUIRE(s.ok());
    for (int i = 0; i < bytes_to_corrupt; i++) {
      contents[i + offset] ^= 0x80;
    }
    s = WriteStringToFile(env_.target(), contents, fname);
    REQUIRE(s.ok());
  }

  int Property(const std::string& name) {
    std::string property;
    int result;
    if (db_->GetProperty(name, &property) &&
        sscanf(property.c_str(), "%d", &result) == 1) {
      return result;
    } else {
      return -1;
    }
  }

  // Return the ith key
  Slice Key(int i, std::string* storage) {
    char buf[100];
    std::snprintf(buf, sizeof(buf), "%016d", i);
    storage->assign(buf, strlen(buf));
    return Slice(*storage);
  }

  // Return the value to associate with the specified key
  Slice Value(int k, std::string* storage) {
    Random r(k);
    return test::RandomString(&r, kValueSize, storage);
  }

  test::ErrorEnv env_;
  Options options_;
  DB* db_;

 private:
  std::string dbname_;
  Cache* tiny_cache_;
};

TEST_CASE("Corruption Recovery", "[Corruption]") {
  CorruptionTest test;
  test.Build(100);
  test.Check(100, 100);
  test.Corrupt(kLogFile, 19, 1);  // WriteBatch tag for first record
  test.Corrupt(kLogFile, log::kBlockSize + 1000,
               1);  // Somewhere in second block
  test.Reopen();

  // The 64 records in the first two log blocks are completely lost.
  test.Check(36, 36);
}

TEST_CASE("Corruption RecoverWriteError", "[Corruption]") {
  CorruptionTest test;
  test.env_.writable_file_error_ = true;
  Status s = test.TryReopen();
  REQUIRE(!s.ok());
}
TEST_CASE("Corruption NewFileErrorDuringWrite", "[Corruption]") {
  CorruptionTest test;
  // Do enough writing to force minor compaction
  test.env_.writable_file_error_ = true;
  const int num = 3 + (Options().write_buffer_size / kValueSize);
  std::string value_storage;
  Status s;
  for (int i = 0; s.ok() && i < num; i++) {
    WriteBatch batch;
    batch.Put("a", test.Value(100, &value_storage));
    s = test.db_->Write(WriteOptions(), &batch);
  }
  REQUIRE(!s.ok());
  REQUIRE(test.env_.num_writable_file_errors_ >= 1);
  test.env_.writable_file_error_ = false;
  test.Reopen();
}
TEST_CASE("Corruption TableFile", "[Corruption]") {
  CorruptionTest test;
  test.Build(100);
  DBImpl* dbi = reinterpret_cast<DBImpl*>(test.db_);
  dbi->TEST_CompactMemTable();
  dbi->TEST_CompactRange(0, nullptr, nullptr);
  dbi->TEST_CompactRange(1, nullptr, nullptr);

  test.Corrupt(kTableFile, 100, 1);
  test.Check(90, 99);
}
TEST_CASE("Corruption TableFileRepair", "[Corruption]") {
  CorruptionTest test;
  test.options_.block_size = 2 * kValueSize;  // Limit scope of corruption
  test.options_.paranoid_checks = true;
  test.Reopen();
  test.Build(100);
  DBImpl* dbi = reinterpret_cast<DBImpl*>(test.db_);
  dbi->TEST_CompactMemTable();
  dbi->TEST_CompactRange(0, nullptr, nullptr);
  dbi->TEST_CompactRange(1, nullptr, nullptr);

  test.Corrupt(kTableFile, 100, 1);
  test.RepairDB();
  test.Reopen();
  test.Check(95, 99);
}
TEST_CASE("Corruption TableFileIndexData", "[Corruption]") {
  CorruptionTest test;
  test.Build(10000);  // Enough to build multiple Tables
  DBImpl* dbi = reinterpret_cast<DBImpl*>(test.db_);
  dbi->TEST_CompactMemTable();

  test.Corrupt(kTableFile, -2000, 500);
  test.Reopen();
  test.Check(5000, 9999);
}
TEST_CASE("Corruption MissingDescriptor", "[Corruption]") {
  CorruptionTest test;
  test.Build(1000);
  test.RepairDB();
  test.Reopen();
  test.Check(1000, 1000);
}
TEST_CASE("Corruption SequenceNumberRecovery", "[Corruption]") {
  CorruptionTest test;
  ASSERT_LEVELDB_OK(test.db_->Put(WriteOptions(), "foo", "v1"));
  ASSERT_LEVELDB_OK(test.db_->Put(WriteOptions(), "foo", "v2"));
  ASSERT_LEVELDB_OK(test.db_->Put(WriteOptions(), "foo", "v3"));
  ASSERT_LEVELDB_OK(test.db_->Put(WriteOptions(), "foo", "v4"));
  ASSERT_LEVELDB_OK(test.db_->Put(WriteOptions(), "foo", "v5"));
  test.RepairDB();
  test.Reopen();
  std::string v;
  ASSERT_LEVELDB_OK(test.db_->Get(ReadOptions(), "foo", &v));
  REQUIRE("v5" == v);
  // Write something.  If sequence number was not recovered properly,
  // it will be hidden by an earlier write.
  ASSERT_LEVELDB_OK(test.db_->Put(WriteOptions(), "foo", "v6"));
  ASSERT_LEVELDB_OK(test.db_->Get(ReadOptions(), "foo", &v));
  REQUIRE("v6" == v);
  test.Reopen();
  ASSERT_LEVELDB_OK(test.db_->Get(ReadOptions(), "foo", &v));
  REQUIRE("v6" == v);
}
TEST_CASE("Corruption CorruptedDescriptor", "[Corruption]") {
  CorruptionTest test;
  ASSERT_LEVELDB_OK(test.db_->Put(WriteOptions(), "foo", "hello"));
  DBImpl* dbi = reinterpret_cast<DBImpl*>(test.db_);
  dbi->TEST_CompactMemTable();
  dbi->TEST_CompactRange(0, nullptr, nullptr);

  test.Corrupt(kDescriptorFile, 0, 1000);
  Status s = test.TryReopen();
  REQUIRE(!s.ok());

  test.RepairDB();
  test.Reopen();
  std::string v;
  ASSERT_LEVELDB_OK(test.db_->Get(ReadOptions(), "foo", &v));
  REQUIRE("hello" == v);
}
TEST_CASE("Corruption CompactionInputError", "[Corruption]") {
  CorruptionTest test;
  test.Build(10);
  DBImpl* dbi = reinterpret_cast<DBImpl*>(test.db_);
  dbi->TEST_CompactMemTable();
  const int last = config::kMaxMemCompactLevel;
  REQUIRE(1 ==
          test.Property("leveldb.num-files-at-level" + NumberToString(last)));

  test.Corrupt(kTableFile, 100, 1);
  test.Check(5, 9);

  // Force compactions by writing lots of values
  test.Build(10000);
  test.Check(10000, 10000);
}
TEST_CASE("Corruption CompactionInputErrorParanoid", "[Corruption]") {
  CorruptionTest test;
  test.options_.paranoid_checks = true;
  test.options_.write_buffer_size = 512 << 10;
  test.Reopen();
  DBImpl* dbi = reinterpret_cast<DBImpl*>(test.db_);

  // Make multiple inputs so we need to compact.
  for (int i = 0; i < 2; i++) {
    test.Build(10);
    dbi->TEST_CompactMemTable();
    test.Corrupt(kTableFile, 100, 1);
    test.env_.SleepForMicroseconds(100000);
  }
  dbi->CompactRange(nullptr, nullptr);

  // Write must fail because of corrupted table
  std::string tmp1, tmp2;
  Status s =
      test.db_->Put(WriteOptions(), test.Key(5, &tmp1), test.Value(5, &tmp2));
  REQUIRE(!s.ok());
}
TEST_CASE("Corruption UnrelatedKeys", "[Corruption]") {
  CorruptionTest test;
  test.Build(10);
  DBImpl* dbi = reinterpret_cast<DBImpl*>(test.db_);
  dbi->TEST_CompactMemTable();
  test.Corrupt(kTableFile, 100, 1);

  std::string tmp1, tmp2;
  ASSERT_LEVELDB_OK(test.db_->Put(WriteOptions(), test.Key(1000, &tmp1),
                                  test.Value(1000, &tmp2)));
  std::string v;
  ASSERT_LEVELDB_OK(test.db_->Get(ReadOptions(), test.Key(1000, &tmp1), &v));
  REQUIRE(test.Value(1000, &tmp2).ToString() == v);
  dbi->TEST_CompactMemTable();
  ASSERT_LEVELDB_OK(test.db_->Get(ReadOptions(), test.Key(1000, &tmp1), &v));
  REQUIRE(test.Value(1000, &tmp2).ToString() == v);
}
