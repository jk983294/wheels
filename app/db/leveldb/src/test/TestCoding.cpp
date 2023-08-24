#include <catch.hpp>
#include <iostream>

#include "util/coding.h"

using namespace std;
using namespace leveldb;

TEST_CASE("Coding Fixed32", "[Coding]") {
  std::string s;
  for (uint32_t v = 0; v < 100000; v++) {
    PutFixed32(&s, v);
  }

  const char* p = s.data();
  for (uint32_t v = 0; v < 100000; v++) {
    uint32_t actual = DecodeFixed32(p);
    REQUIRE(v == actual);
    p += sizeof(uint32_t);
  }
}

TEST_CASE("Coding Fixed64", "[Coding]") {
  std::string s;
  for (int power = 0; power <= 63; power++) {
    uint64_t v = static_cast<uint64_t>(1) << power;
    PutFixed64(&s, v - 1);
    PutFixed64(&s, v + 0);
    PutFixed64(&s, v + 1);
  }

  const char* p = s.data();
  for (int power = 0; power <= 63; power++) {
    uint64_t v = static_cast<uint64_t>(1) << power;
    uint64_t actual;
    actual = DecodeFixed64(p);
    REQUIRE(v - 1 == actual);
    p += sizeof(uint64_t);

    actual = DecodeFixed64(p);
    REQUIRE(v + 0 == actual);
    p += sizeof(uint64_t);

    actual = DecodeFixed64(p);
    REQUIRE(v + 1 == actual);
    p += sizeof(uint64_t);
  }
}

TEST_CASE("Coding EncodingOutput", "[Coding]") {
  std::string dst;
  PutFixed32(&dst, 0x04030201);
  REQUIRE(4 == dst.size());
  REQUIRE(0x01 == static_cast<int>(dst[0]));
  REQUIRE(0x02 == static_cast<int>(dst[1]));
  REQUIRE(0x03 == static_cast<int>(dst[2]));
  REQUIRE(0x04 == static_cast<int>(dst[3]));

  dst.clear();
  PutFixed64(&dst, 0x0807060504030201ull);
  REQUIRE(8 == dst.size());
  REQUIRE(0x01 == static_cast<int>(dst[0]));
  REQUIRE(0x02 == static_cast<int>(dst[1]));
  REQUIRE(0x03 == static_cast<int>(dst[2]));
  REQUIRE(0x04 == static_cast<int>(dst[3]));
  REQUIRE(0x05 == static_cast<int>(dst[4]));
  REQUIRE(0x06 == static_cast<int>(dst[5]));
  REQUIRE(0x07 == static_cast<int>(dst[6]));
  REQUIRE(0x08 == static_cast<int>(dst[7]));
}

TEST_CASE("Coding Varint32", "[Coding]") {
  std::string s;
  for (uint32_t i = 0; i < (32 * 32); i++) {
    uint32_t v = (i / 32) << (i % 32);
    PutVarint32(&s, v);
  }

  const char* p = s.data();
  const char* limit = p + s.size();
  for (uint32_t i = 0; i < (32 * 32); i++) {
    uint32_t expected = (i / 32) << (i % 32);
    uint32_t actual;
    const char* start = p;
    p = GetVarint32Ptr(p, limit, &actual);
    REQUIRE(p != nullptr);
    REQUIRE(expected == actual);
    REQUIRE(VarintLength(actual) == p - start);
  }
  REQUIRE(p == s.data() + s.size());
}

TEST_CASE("Coding Varint64", "[Coding]") {
  // Construct the list of values to check
  std::vector<uint64_t> values;
  // Some special values
  values.push_back(0);
  values.push_back(100);
  values.push_back(~static_cast<uint64_t>(0));
  values.push_back(~static_cast<uint64_t>(0) - 1);
  for (uint32_t k = 0; k < 64; k++) {
    // Test values near powers of two
    const uint64_t power = 1ull << k;
    values.push_back(power);
    values.push_back(power - 1);
    values.push_back(power + 1);
  }

  std::string s;
  for (size_t i = 0; i < values.size(); i++) {
    PutVarint64(&s, values[i]);
  }

  const char* p = s.data();
  const char* limit = p + s.size();
  for (size_t i = 0; i < values.size(); i++) {
    REQUIRE(p < limit);
    uint64_t actual;
    const char* start = p;
    p = GetVarint64Ptr(p, limit, &actual);
    REQUIRE(p != nullptr);
    REQUIRE(values[i] == actual);
    REQUIRE(VarintLength(actual) == p - start);
  }
  REQUIRE(p == limit);
}

TEST_CASE("Coding Varint32Overflow", "[Coding]") {
  uint32_t result;
  std::string input("\x81\x82\x83\x84\x85\x11");
  REQUIRE(GetVarint32Ptr(input.data(), input.data() + input.size(), &result) ==
          nullptr);
}

TEST_CASE("Coding Varint32Truncation", "[Coding]") {
  uint32_t large_value = (1u << 31) + 100;
  std::string s;
  PutVarint32(&s, large_value);
  uint32_t result;
  for (size_t len = 0; len < s.size() - 1; len++) {
    REQUIRE(GetVarint32Ptr(s.data(), s.data() + len, &result) == nullptr);
  }
  REQUIRE(GetVarint32Ptr(s.data(), s.data() + s.size(), &result) != nullptr);
  REQUIRE(large_value == result);
}

TEST_CASE("Coding Varint64Overflow", "[Coding]") {
  uint64_t result;
  std::string input("\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11");
  REQUIRE(GetVarint64Ptr(input.data(), input.data() + input.size(), &result) ==
          nullptr);
}

TEST_CASE("Coding Varint64Truncation", "[Coding]") {
  uint64_t large_value = (1ull << 63) + 100ull;
  std::string s;
  PutVarint64(&s, large_value);
  uint64_t result;
  for (size_t len = 0; len < s.size() - 1; len++) {
    REQUIRE(GetVarint64Ptr(s.data(), s.data() + len, &result) == nullptr);
  }
  REQUIRE(GetVarint64Ptr(s.data(), s.data() + s.size(), &result) != nullptr);
  REQUIRE(large_value == result);
}

TEST_CASE("Coding Strings", "[Coding]") {
  std::string s;
  PutLengthPrefixedSlice(&s, Slice(""));
  PutLengthPrefixedSlice(&s, Slice("foo"));
  PutLengthPrefixedSlice(&s, Slice("bar"));
  PutLengthPrefixedSlice(&s, Slice(std::string(200, 'x')));

  Slice input(s);
  Slice v;
  REQUIRE(GetLengthPrefixedSlice(&input, &v));
  REQUIRE("" == v.ToString());
  REQUIRE(GetLengthPrefixedSlice(&input, &v));
  REQUIRE("foo" == v.ToString());
  REQUIRE(GetLengthPrefixedSlice(&input, &v));
  REQUIRE("bar" == v.ToString());
  REQUIRE(GetLengthPrefixedSlice(&input, &v));
  REQUIRE(std::string(200, 'x') == v.ToString());
  REQUIRE("" == input.ToString());
}
