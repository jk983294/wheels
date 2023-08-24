#include <util/random.h>

#include <algorithm>
#include <catch.hpp>

#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "testutil.h"
#include "util/mutexlock.h"

using namespace std;
using namespace leveldb;

class EnvTest {
 public:
  EnvTest() : env_(Env::Default()) {}

  Env* env_;
};

TEST_CASE("EnvTest ReadWrite", "[EnvTest]") {
  EnvTest test;
  Random rnd(test::RandomSeed());

  // Get file to use for testing.
  std::string test_dir;
  ASSERT_LEVELDB_OK(test.env_->GetTestDirectory(&test_dir));
  std::string test_file_name = test_dir + "/open_on_read.txt";
  WritableFile* writable_file;
  ASSERT_LEVELDB_OK(test.env_->NewWritableFile(test_file_name, &writable_file));

  // Fill a file with data generated via a sequence of randomly sized writes.
  static const size_t kDataSize = 10 * 1048576;
  std::string data;
  while (data.size() < kDataSize) {
    int len = rnd.Skewed(18);  // Up to 2^18 - 1, but typically much smaller
    std::string r;
    test::RandomString(&rnd, len, &r);
    ASSERT_LEVELDB_OK(writable_file->Append(r));
    data += r;
    if (rnd.OneIn(10)) {
      ASSERT_LEVELDB_OK(writable_file->Flush());
    }
  }
  ASSERT_LEVELDB_OK(writable_file->Sync());
  ASSERT_LEVELDB_OK(writable_file->Close());
  delete writable_file;

  // Read all data using a sequence of randomly sized reads.
  SequentialFile* sequential_file;
  ASSERT_LEVELDB_OK(
      test.env_->NewSequentialFile(test_file_name, &sequential_file));
  std::string read_result;
  std::string scratch;
  while (read_result.size() < data.size()) {
    int len = std::min<int>(rnd.Skewed(18), data.size() - read_result.size());
    scratch.resize(std::max(len, 1));  // at least 1 so &scratch[0] is legal
    Slice read;
    ASSERT_LEVELDB_OK(sequential_file->Read(len, &read, &scratch[0]));
    if (len > 0) {
      REQUIRE(read.size() > 0);
    }
    REQUIRE(read.size() <= len);
    read_result.append(read.data(), read.size());
  }
  REQUIRE(read_result == data);
  delete sequential_file;
}
TEST_CASE("EnvTest RunImmediately", "[EnvTest]") {
  EnvTest test;
  struct RunState {
    port::Mutex mu;
    port::CondVar cvar{&mu};
    bool called = false;

    static void Run(void* arg) {
      RunState* state = reinterpret_cast<RunState*>(arg);
      MutexLock l(&state->mu);
      REQUIRE(state->called == false);
      state->called = true;
      state->cvar.Signal();
    }
  };

  RunState state;
  test.env_->Schedule(&RunState::Run, &state);

  MutexLock l(&state.mu);
  while (!state.called) {
    state.cvar.Wait();
  }
}
TEST_CASE("EnvTest RunMany", "[EnvTest]") {
  EnvTest test;
  struct RunState {
    port::Mutex mu;
    port::CondVar cvar{&mu};
    int run_count = 0;
  };

  struct Callback {
    RunState* const state_;  // Pointer to shared state.
    bool run = false;

    Callback(RunState* s) : state_(s) {}

    static void Run(void* arg) {
      Callback* callback = reinterpret_cast<Callback*>(arg);
      RunState* state = callback->state_;

      MutexLock l(&state->mu);
      state->run_count++;
      callback->run = true;
      state->cvar.Signal();
    }
  };

  RunState state;
  Callback callback1(&state);
  Callback callback2(&state);
  Callback callback3(&state);
  Callback callback4(&state);
  test.env_->Schedule(&Callback::Run, &callback1);
  test.env_->Schedule(&Callback::Run, &callback2);
  test.env_->Schedule(&Callback::Run, &callback3);
  test.env_->Schedule(&Callback::Run, &callback4);

  MutexLock l(&state.mu);
  while (state.run_count != 4) {
    state.cvar.Wait();
  }

  REQUIRE(callback1.run);
  REQUIRE(callback2.run);
  REQUIRE(callback3.run);
  REQUIRE(callback4.run);
}

struct State {
  port::Mutex mu;
  port::CondVar cvar{&mu};

  int val GUARDED_BY(mu);
  int num_running GUARDED_BY(mu);

  State(int val, int num_running) : val(val), num_running(num_running) {}
};

static void ThreadBody(void* arg) {
  State* s = reinterpret_cast<State*>(arg);
  s->mu.Lock();
  s->val += 1;
  s->num_running -= 1;
  s->cvar.Signal();
  s->mu.Unlock();
}

TEST_CASE("EnvTest StartThread", "[EnvTest]") {
  EnvTest test;
  State state(0, 3);
  for (int i = 0; i < 3; i++) {
    test.env_->StartThread(&ThreadBody, &state);
  }

  MutexLock l(&state.mu);
  while (state.num_running != 0) {
    state.cvar.Wait();
  }
  REQUIRE(state.val == 3);
}
TEST_CASE("EnvTest TestOpenNonExistentFile", "[EnvTest]") {
  EnvTest test;
  // Write some test data to a single file that will be opened |n| times.
  std::string test_dir;
  ASSERT_LEVELDB_OK(test.env_->GetTestDirectory(&test_dir));

  std::string non_existent_file = test_dir + "/non_existent_file";
  REQUIRE(!test.env_->FileExists(non_existent_file));

  RandomAccessFile* random_access_file;
  Status status =
      test.env_->NewRandomAccessFile(non_existent_file, &random_access_file);

  REQUIRE(status.IsNotFound());

  SequentialFile* sequential_file;
  status = test.env_->NewSequentialFile(non_existent_file, &sequential_file);

  REQUIRE(status.IsNotFound());
}
TEST_CASE("EnvTest ReopenWritableFile", "[EnvTest]") {
  EnvTest test;
  std::string test_dir;
  ASSERT_LEVELDB_OK(test.env_->GetTestDirectory(&test_dir));
  std::string test_file_name = test_dir + "/reopen_writable_file.txt";
  test.env_->RemoveFile(test_file_name);

  WritableFile* writable_file;
  ASSERT_LEVELDB_OK(test.env_->NewWritableFile(test_file_name, &writable_file));
  std::string data("hello world!");
  ASSERT_LEVELDB_OK(writable_file->Append(data));
  ASSERT_LEVELDB_OK(writable_file->Close());
  delete writable_file;

  ASSERT_LEVELDB_OK(test.env_->NewWritableFile(test_file_name, &writable_file));
  data = "42";
  ASSERT_LEVELDB_OK(writable_file->Append(data));
  ASSERT_LEVELDB_OK(writable_file->Close());
  delete writable_file;

  ASSERT_LEVELDB_OK(ReadFileToString(test.env_, test_file_name, &data));
  REQUIRE(std::string("42") == data);
  test.env_->RemoveFile(test_file_name);
}
TEST_CASE("EnvTest ReopenAppendableFile", "[EnvTest]") {
  EnvTest test;
  std::string test_dir;
  ASSERT_LEVELDB_OK(test.env_->GetTestDirectory(&test_dir));
  std::string test_file_name = test_dir + "/reopen_appendable_file.txt";
  test.env_->RemoveFile(test_file_name);

  WritableFile* appendable_file;
  ASSERT_LEVELDB_OK(
      test.env_->NewAppendableFile(test_file_name, &appendable_file));
  std::string data("hello world!");
  ASSERT_LEVELDB_OK(appendable_file->Append(data));
  ASSERT_LEVELDB_OK(appendable_file->Close());
  delete appendable_file;

  ASSERT_LEVELDB_OK(
      test.env_->NewAppendableFile(test_file_name, &appendable_file));
  data = "42";
  ASSERT_LEVELDB_OK(appendable_file->Append(data));
  ASSERT_LEVELDB_OK(appendable_file->Close());
  delete appendable_file;

  ASSERT_LEVELDB_OK(ReadFileToString(test.env_, test_file_name, &data));
  REQUIRE(std::string("hello world!42") == data);
  test.env_->RemoveFile(test_file_name);
}
