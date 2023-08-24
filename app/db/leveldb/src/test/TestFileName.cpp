#include <catch.hpp>

#include "db/dbformat.h"
#include "db/filename.h"
#include "port/port.h"
#include "util/logging.h"

using namespace std;
using namespace leveldb;

TEST_CASE("FileName Parse", "[FileName]") {
  Slice db;
  FileType type;
  uint64_t number;

  // Successful parses
  static struct {
    const char* fname;
    uint64_t number;
    FileType type;
  } cases[] = {
      {"100.log", 100, kLogFile},
      {"0.log", 0, kLogFile},
      {"0.sst", 0, kTableFile},
      {"0.ldb", 0, kTableFile},
      {"CURRENT", 0, kCurrentFile},
      {"LOCK", 0, kDBLockFile},
      {"MANIFEST-2", 2, kDescriptorFile},
      {"MANIFEST-7", 7, kDescriptorFile},
      {"LOG", 0, kInfoLogFile},
      {"LOG.old", 0, kInfoLogFile},
      {"18446744073709551615.log", 18446744073709551615ull, kLogFile},
  };
  for (int i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
    std::string f = cases[i].fname;
    REQUIRE(ParseFileName(f, &number, &type));
    REQUIRE(cases[i].type == type);
    REQUIRE(cases[i].number == number);
  }

  // Errors
  static const char* errors[] = {"",
                                 "foo",
                                 "foo-dx-100.log",
                                 ".log",
                                 "",
                                 "manifest",
                                 "CURREN",
                                 "CURRENTX",
                                 "MANIFES",
                                 "MANIFEST",
                                 "MANIFEST-",
                                 "XMANIFEST-3",
                                 "MANIFEST-3x",
                                 "LOC",
                                 "LOCKx",
                                 "LO",
                                 "LOGx",
                                 "18446744073709551616.log",
                                 "184467440737095516150.log",
                                 "100",
                                 "100.",
                                 "100.lop"};
  for (int i = 0; i < sizeof(errors) / sizeof(errors[0]); i++) {
    std::string f = errors[i];
    REQUIRE(!ParseFileName(f, &number, &type));
  }
}

TEST_CASE("FileName Construction", "[FileName]") {
  uint64_t number;
  FileType type;
  std::string fname;

  fname = CurrentFileName("foo");
  REQUIRE("foo/" == std::string(fname.data(), 4));
  REQUIRE(ParseFileName(fname.c_str() + 4, &number, &type));
  REQUIRE(0 == number);
  REQUIRE(kCurrentFile == type);

  fname = LockFileName("foo");
  REQUIRE("foo/" == std::string(fname.data(), 4));
  REQUIRE(ParseFileName(fname.c_str() + 4, &number, &type));
  REQUIRE(0 == number);
  REQUIRE(kDBLockFile == type);

  fname = LogFileName("foo", 192);
  REQUIRE("foo/" == std::string(fname.data(), 4));
  REQUIRE(ParseFileName(fname.c_str() + 4, &number, &type));
  REQUIRE(192 == number);
  REQUIRE(kLogFile == type);

  fname = TableFileName("bar", 200);
  REQUIRE("bar/" == std::string(fname.data(), 4));
  REQUIRE(ParseFileName(fname.c_str() + 4, &number, &type));
  REQUIRE(200 == number);
  REQUIRE(kTableFile == type);

  fname = DescriptorFileName("bar", 100);
  REQUIRE("bar/" == std::string(fname.data(), 4));
  REQUIRE(ParseFileName(fname.c_str() + 4, &number, &type));
  REQUIRE(100 == number);
  REQUIRE(kDescriptorFile == type);

  fname = TempFileName("tmp", 999);
  REQUIRE("tmp/" == std::string(fname.data(), 4));
  REQUIRE(ParseFileName(fname.c_str() + 4, &number, &type));
  REQUIRE(999 == number);
  REQUIRE(kTempFile == type);

  fname = InfoLogFileName("foo");
  REQUIRE("foo/" == std::string(fname.data(), 4));
  REQUIRE(ParseFileName(fname.c_str() + 4, &number, &type));
  REQUIRE(0 == number);
  REQUIRE(kInfoLogFile == type);

  fname = OldInfoLogFileName("foo");
  REQUIRE("foo/" == std::string(fname.data(), 4));
  REQUIRE(ParseFileName(fname.c_str() + 4, &number, &type));
  REQUIRE(0 == number);
  REQUIRE(kInfoLogFile == type);
}
