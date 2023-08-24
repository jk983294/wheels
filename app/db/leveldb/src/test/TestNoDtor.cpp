#include <catch.hpp>
#include <iostream>

#include "testutil.h"
#include "util/no_destructor.h"
using namespace std;
using namespace leveldb;

struct DoNotDestruct {
 public:
  DoNotDestruct(uint32_t a, uint64_t b) : a(a), b(b) {}
  ~DoNotDestruct() { std::abort(); }

  // Used to check constructor argument forwarding.
  uint32_t a;
  uint64_t b;
};

constexpr const uint32_t kGoldenA = 0xdeadbeef;
constexpr const uint64_t kGoldenB = 0xaabbccddeeffaabb;

TEST_CASE("StackInstance", "[DoNotDestruct]") {
  NoDestructor<DoNotDestruct> instance(kGoldenA, kGoldenB);
  REQUIRE(kGoldenA == instance.get()->a);
  REQUIRE(kGoldenB == instance.get()->b);
}
TEST_CASE("StaticInstance", "[DoNotDestruct]") {
  static NoDestructor<DoNotDestruct> instance(kGoldenA, kGoldenB);
  REQUIRE(kGoldenA == instance.get()->a);
  REQUIRE(kGoldenB == instance.get()->b);
}
