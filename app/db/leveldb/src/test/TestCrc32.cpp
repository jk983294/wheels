#include <catch.hpp>
#include <iostream>

#include "testutil.h"
#include "util/crc32c.h"

using namespace std;
using namespace leveldb::crc32c;

TEST_CASE("CRC StandardResults", "[CRC]") {
  // From rfc3720 section B.4.
  char buf[32];

  memset(buf, 0, sizeof(buf));
  REQUIRE(0x8a9136aa == Value(buf, sizeof(buf)));

  memset(buf, 0xff, sizeof(buf));
  REQUIRE(0x62a8ab43 == Value(buf, sizeof(buf)));

  for (int i = 0; i < 32; i++) {
    buf[i] = i;
  }
  REQUIRE(0x46dd794e == Value(buf, sizeof(buf)));

  for (int i = 0; i < 32; i++) {
    buf[i] = 31 - i;
  }
  REQUIRE(0x113fdb5c == Value(buf, sizeof(buf)));

  uint8_t data[48] = {
      0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
      0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  };
  REQUIRE(0xd9963a56 == Value(reinterpret_cast<char*>(data), sizeof(data)));
}

TEST_CASE("CRC Values", "[CRC]") { REQUIRE(Value("a", 1) != Value("foo", 3)); }

TEST_CASE("CRC Extend", "[CRC]") {
  REQUIRE(Value("hello world", 11) == Extend(Value("hello ", 6), "world", 5));
}

TEST_CASE("CRC Mask", "[CRC]") {
  uint32_t crc = Value("foo", 3);
  REQUIRE(crc != Mask(crc));
  REQUIRE(crc != Mask(Mask(crc)));
  REQUIRE(crc == Unmask(Mask(crc)));
  REQUIRE(crc == Unmask(Unmask(Mask(Mask(crc)))));
}
