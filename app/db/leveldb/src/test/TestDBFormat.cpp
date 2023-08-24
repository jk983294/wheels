#include <catch.hpp>

#include "db/dbformat.h"

using namespace std;
using namespace leveldb;

static std::string IKey(const std::string& user_key, uint64_t seq,
                        ValueType vt) {
  std::string encoded;
  AppendInternalKey(&encoded, ParsedInternalKey(user_key, seq, vt));
  return encoded;
}

static std::string Shorten(const std::string& s, const std::string& l) {
  std::string result = s;
  InternalKeyComparator(BytewiseComparator()).FindShortestSeparator(&result, l);
  return result;
}

static std::string ShortSuccessor(const std::string& s) {
  std::string result = s;
  InternalKeyComparator(BytewiseComparator()).FindShortSuccessor(&result);
  return result;
}

static void TestKey(const std::string& key, uint64_t seq, ValueType vt) {
  std::string encoded = IKey(key, seq, vt);

  Slice in(encoded);
  ParsedInternalKey decoded("", 0, kTypeValue);

  REQUIRE(ParseInternalKey(in, &decoded));
  REQUIRE(key == decoded.user_key.ToString());
  REQUIRE(seq == decoded.sequence);
  REQUIRE(vt == decoded.type);

  REQUIRE(!ParseInternalKey(Slice("bar"), &decoded));
}

TEST_CASE("DBFormat InternalKey_EncodeDecode", "[DBFormat]") {
  const char* keys[] = {"", "k", "hello", "longggggggggggggggggggggg"};
  const uint64_t seq[] = {1,
                          2,
                          3,
                          (1ull << 8) - 1,
                          1ull << 8,
                          (1ull << 8) + 1,
                          (1ull << 16) - 1,
                          1ull << 16,
                          (1ull << 16) + 1,
                          (1ull << 32) - 1,
                          1ull << 32,
                          (1ull << 32) + 1};
  for (int k = 0; k < sizeof(keys) / sizeof(keys[0]); k++) {
    for (int s = 0; s < sizeof(seq) / sizeof(seq[0]); s++) {
      TestKey(keys[k], seq[s], kTypeValue);
      TestKey("hello", 1, kTypeDeletion);
    }
  }
}

TEST_CASE("DBFormat InternalKey_DecodeFromEmpty", "[DBFormat]") {
  InternalKey internal_key;

  REQUIRE(!internal_key.DecodeFrom(""));
}

TEST_CASE("DBFormat InternalKeyShortSeparator", "[DBFormat]") {
  // When user keys are same
  REQUIRE(IKey("foo", 100, kTypeValue) ==
          Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 99, kTypeValue)));
  REQUIRE(IKey("foo", 100, kTypeValue) ==
          Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 101, kTypeValue)));
  REQUIRE(IKey("foo", 100, kTypeValue) ==
          Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 100, kTypeValue)));
  REQUIRE(
      IKey("foo", 100, kTypeValue) ==
      Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 100, kTypeDeletion)));

  // When user keys are misordered
  REQUIRE(IKey("foo", 100, kTypeValue) ==
          Shorten(IKey("foo", 100, kTypeValue), IKey("bar", 99, kTypeValue)));

  // When user keys are different, but correctly ordered
  REQUIRE(
      IKey("g", kMaxSequenceNumber, kValueTypeForSeek) ==
      Shorten(IKey("foo", 100, kTypeValue), IKey("hello", 200, kTypeValue)));

  // When start user key is prefix of limit user key
  REQUIRE(
      IKey("foo", 100, kTypeValue) ==
      Shorten(IKey("foo", 100, kTypeValue), IKey("foobar", 200, kTypeValue)));

  // When limit user key is prefix of start user key
  REQUIRE(
      IKey("foobar", 100, kTypeValue) ==
      Shorten(IKey("foobar", 100, kTypeValue), IKey("foo", 200, kTypeValue)));
}
TEST_CASE("DBFormat InternalKeyShortestSuccessor", "[DBFormat]") {
  REQUIRE(IKey("g", kMaxSequenceNumber, kValueTypeForSeek) ==
          ShortSuccessor(IKey("foo", 100, kTypeValue)));
  REQUIRE(IKey("\xff\xff", 100, kTypeValue) ==
          ShortSuccessor(IKey("\xff\xff", 100, kTypeValue)));
}
TEST_CASE("DBFormat ParsedInternalKeyDebugString", "[DBFormat]") {
  ParsedInternalKey key("The \"key\" in 'single quotes'", 42, kTypeValue);

  REQUIRE("'The \"key\" in 'single quotes'' @ 42 : 1" == key.DebugString());
}
TEST_CASE("DBFormat InternalKeyDebugString", "[DBFormat]") {
  InternalKey key("The \"key\" in 'single quotes'", 42, kTypeValue);
  REQUIRE("'The \"key\" in 'single quotes'' @ 42 : 1" == key.DebugString());

  InternalKey invalid_key;
  REQUIRE("(bad)" == invalid_key.DebugString());
}