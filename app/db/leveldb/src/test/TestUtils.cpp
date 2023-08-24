#include <catch.hpp>
#include <iostream>

#include "testutil.h"
#include "util/hash.h"

using namespace std;
using namespace leveldb;

TEST_CASE("Random", "[Random]") {
  leveldb::Random rnd(42);
  string str = test::RandomKey(&rnd, 10);
  REQUIRE(str.size() == 10);
}

TEST_CASE("Slice", "[Slice]") {
  leveldb::Slice s1 = "hello";
  std::string str("world");
  leveldb::Slice s2 = str;
  leveldb::Slice s3 = s1;

  REQUIRE((s1 != s2));
  REQUIRE((s1 == s3));
  REQUIRE(s2.ToString() == str);
}

TEST_CASE("SignedUnsignedIssue", "[HASH]") {
  const uint8_t data1[1] = {0x62};
  const uint8_t data2[2] = {0xc3, 0x97};
  const uint8_t data3[3] = {0xe2, 0x99, 0xa5};
  const uint8_t data4[4] = {0xe1, 0x80, 0xb9, 0x32};
  const uint8_t data5[48] = {
      0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
      0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  };

  REQUIRE(Hash(0, 0, 0xbc9f1d34) == 0xbc9f1d34);
  REQUIRE(Hash(reinterpret_cast<const char*>(data1), sizeof(data1),
               0xbc9f1d34) == 0xef1345c4);
  REQUIRE(Hash(reinterpret_cast<const char*>(data2), sizeof(data2),
               0xbc9f1d34) == 0x5b663814);
  REQUIRE(Hash(reinterpret_cast<const char*>(data3), sizeof(data3),
               0xbc9f1d34) == 0x323c078f);
  REQUIRE(Hash(reinterpret_cast<const char*>(data4), sizeof(data4),
               0xbc9f1d34) == 0xed21633a);
  REQUIRE(Hash(reinterpret_cast<const char*>(data5), sizeof(data5),
               0x12345678) == 0xf333dabb);
}

TEST_CASE("MoveConstructor", "[Status]") {
  {
    Status ok = Status::OK();
    Status ok2 = std::move(ok);

    REQUIRE(ok2.ok());
  }

  {
    Status status = Status::NotFound("custom NotFound status message");
    Status status2 = std::move(status);

    REQUIRE(status2.IsNotFound());
    REQUIRE("NotFound: custom NotFound status message" == status2.ToString());
  }

  {
    Status self_moved = Status::IOError("custom IOError status message");

    // Needed to bypass compiler warning about explicit move-assignment.
    Status& self_moved_reference = self_moved;
    self_moved_reference = std::move(self_moved);
  }
}
