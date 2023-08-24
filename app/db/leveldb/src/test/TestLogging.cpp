#include <catch.hpp>
#include <iostream>
#include <limits>
#include <string>

#include "leveldb/slice.h"
#include "testutil.h"
#include "util/logging.h"

using namespace std;
using namespace leveldb;

TEST_CASE("Logging NumberToString", "[Logging]") {
  REQUIRE("0" == NumberToString(0));
  REQUIRE("1" == NumberToString(1));
  REQUIRE("9" == NumberToString(9));

  REQUIRE("10" == NumberToString(10));
  REQUIRE("11" == NumberToString(11));
  REQUIRE("19" == NumberToString(19));
  REQUIRE("99" == NumberToString(99));

  REQUIRE("100" == NumberToString(100));
  REQUIRE("109" == NumberToString(109));
  REQUIRE("190" == NumberToString(190));
  REQUIRE("123" == NumberToString(123));
  REQUIRE("12345678" == NumberToString(12345678));

  static_assert(std::numeric_limits<uint64_t>::max() == 18446744073709551615U,
                "Test consistency check");
  REQUIRE("18446744073709551000" == NumberToString(18446744073709551000U));
  REQUIRE("18446744073709551600" == NumberToString(18446744073709551600U));
  REQUIRE("18446744073709551610" == NumberToString(18446744073709551610U));
  REQUIRE("18446744073709551614" == NumberToString(18446744073709551614U));
  REQUIRE("18446744073709551615" == NumberToString(18446744073709551615U));
}
static void ConsumeDecimalNumberRoundtripTest(uint64_t number,
                                              const std::string& padding = "") {
  std::string decimal_number = NumberToString(number);
  std::string input_string = decimal_number + padding;
  Slice input(input_string);
  Slice output = input;
  uint64_t result;
  REQUIRE(ConsumeDecimalNumber(&output, &result));
  REQUIRE(number == result);
  REQUIRE(decimal_number.size() == output.data() - input.data());
  REQUIRE(padding.size() == output.size());
}

TEST_CASE("Logging ConsumeDecimalNumberRoundtrip", "[Logging]") {
  ConsumeDecimalNumberRoundtripTest(0);
  ConsumeDecimalNumberRoundtripTest(1);
  ConsumeDecimalNumberRoundtripTest(9);

  ConsumeDecimalNumberRoundtripTest(10);
  ConsumeDecimalNumberRoundtripTest(11);
  ConsumeDecimalNumberRoundtripTest(19);
  ConsumeDecimalNumberRoundtripTest(99);

  ConsumeDecimalNumberRoundtripTest(100);
  ConsumeDecimalNumberRoundtripTest(109);
  ConsumeDecimalNumberRoundtripTest(190);
  ConsumeDecimalNumberRoundtripTest(123);
  REQUIRE("12345678" == NumberToString(12345678));

  for (uint64_t i = 0; i < 100; ++i) {
    uint64_t large_number = std::numeric_limits<uint64_t>::max() - i;
    ConsumeDecimalNumberRoundtripTest(large_number);
  }
}
TEST_CASE("Logging ConsumeDecimalNumberRoundtripWithPadding", "[Logging]") {
  ConsumeDecimalNumberRoundtripTest(0, " ");
  ConsumeDecimalNumberRoundtripTest(1, "abc");
  ConsumeDecimalNumberRoundtripTest(9, "x");

  ConsumeDecimalNumberRoundtripTest(10, "_");
  ConsumeDecimalNumberRoundtripTest(11, std::string("\0\0\0", 3));
  ConsumeDecimalNumberRoundtripTest(19, "abc");
  ConsumeDecimalNumberRoundtripTest(99, "padding");

  ConsumeDecimalNumberRoundtripTest(100, " ");

  for (uint64_t i = 0; i < 100; ++i) {
    uint64_t large_number = std::numeric_limits<uint64_t>::max() - i;
    ConsumeDecimalNumberRoundtripTest(large_number, "pad");
  }
}
static void ConsumeDecimalNumberOverflowTest(const std::string& input_string) {
  Slice input(input_string);
  Slice output = input;
  uint64_t result;
  REQUIRE(!ConsumeDecimalNumber(&output, &result));
}

TEST_CASE("Logging ConsumeDecimalNumberOverflow", "[Logging]") {
  static_assert(std::numeric_limits<uint64_t>::max() == 18446744073709551615U,
                "Test consistency check");
  ConsumeDecimalNumberOverflowTest("18446744073709551616");
  ConsumeDecimalNumberOverflowTest("18446744073709551617");
  ConsumeDecimalNumberOverflowTest("18446744073709551618");
  ConsumeDecimalNumberOverflowTest("18446744073709551619");
  ConsumeDecimalNumberOverflowTest("18446744073709551620");
  ConsumeDecimalNumberOverflowTest("18446744073709551621");
  ConsumeDecimalNumberOverflowTest("18446744073709551622");
  ConsumeDecimalNumberOverflowTest("18446744073709551623");
  ConsumeDecimalNumberOverflowTest("18446744073709551624");
  ConsumeDecimalNumberOverflowTest("18446744073709551625");
  ConsumeDecimalNumberOverflowTest("18446744073709551626");

  ConsumeDecimalNumberOverflowTest("18446744073709551700");

  ConsumeDecimalNumberOverflowTest("99999999999999999999");
}

static void ConsumeDecimalNumberNoDigitsTest(const std::string& input_string) {
  Slice input(input_string);
  Slice output = input;
  uint64_t result;
  REQUIRE(not ConsumeDecimalNumber(&output, &result));
  REQUIRE(input.data() == output.data());
  REQUIRE(input.size() == output.size());
}

TEST_CASE("Logging ConsumeDecimalNumberNoDigits", "[Logging]") {
  ConsumeDecimalNumberNoDigitsTest("");
  ConsumeDecimalNumberNoDigitsTest(" ");
  ConsumeDecimalNumberNoDigitsTest("a");
  ConsumeDecimalNumberNoDigitsTest(" 123");
  ConsumeDecimalNumberNoDigitsTest("a123");
  ConsumeDecimalNumberNoDigitsTest(std::string("\000123", 4));
  ConsumeDecimalNumberNoDigitsTest(std::string("\177123", 4));
  ConsumeDecimalNumberNoDigitsTest(std::string("\377123", 4));
}
