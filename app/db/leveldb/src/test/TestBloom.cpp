#include <catch.hpp>
#include <iostream>

#include "leveldb/filter_policy.h"
#include "util/coding.h"
#include "util/logging.h"

using namespace std;
using namespace leveldb;

static const int kVerbose = 1;

static Slice Key(int i, char* buffer) {
  EncodeFixed32(buffer, i);
  return Slice(buffer, sizeof(uint32_t));
}

class BloomTest {
 public:
  BloomTest() : policy_(NewBloomFilterPolicy(10)) {}

  ~BloomTest() { delete policy_; }

  void Reset() {
    keys_.clear();
    filter_.clear();
  }

  void Add(const Slice& s) { keys_.push_back(s.ToString()); }

  void Build() {
    std::vector<Slice> key_slices;
    for (size_t i = 0; i < keys_.size(); i++) {
      key_slices.push_back(Slice(keys_[i]));
    }
    filter_.clear();
    policy_->CreateFilter(&key_slices[0], static_cast<int>(key_slices.size()),
                          &filter_);
    keys_.clear();
    if (kVerbose >= 2) DumpFilter();
  }

  size_t FilterSize() const { return filter_.size(); }

  void DumpFilter() {
    std::fprintf(stderr, "F(");
    for (size_t i = 0; i + 1 < filter_.size(); i++) {
      const unsigned int c = static_cast<unsigned int>(filter_[i]);
      for (int j = 0; j < 8; j++) {
        std::fprintf(stderr, "%c", (c & (1 << j)) ? '1' : '.');
      }
    }
    std::fprintf(stderr, ")\n");
  }

  bool Matches(const Slice& s) {
    if (!keys_.empty()) {
      Build();
    }
    return policy_->KeyMayMatch(s, filter_);
  }

  double FalsePositiveRate() {
    char buffer[sizeof(int)];
    int result = 0;
    for (int i = 0; i < 10000; i++) {
      if (Matches(Key(i + 1000000000, buffer))) {
        result++;
      }
    }
    return result / 10000.0;
  }

 private:
  const FilterPolicy* policy_;
  std::string filter_;
  std::vector<std::string> keys_;
};

TEST_CASE("BloomTest EmptyFilter", "[BloomTest]") {
  BloomTest test;
  REQUIRE(!test.Matches("hello"));
  REQUIRE(!test.Matches("world"));
}

TEST_CASE("BloomTest Small", "[BloomTest]") {
  BloomTest test;
  test.Add("hello");
  test.Add("world");
  REQUIRE(test.Matches("hello"));
  REQUIRE(test.Matches("world"));
  REQUIRE(!test.Matches("x"));
  REQUIRE(!test.Matches("foo"));
}

static int NextLength(int length) {
  if (length < 10) {
    length += 1;
  } else if (length < 100) {
    length += 10;
  } else if (length < 1000) {
    length += 100;
  } else {
    length += 1000;
  }
  return length;
}

TEST_CASE("BloomTest VaryingLengths", "[BloomTest]") {
  BloomTest test;
  char buffer[sizeof(int)];

  // Count number of filters that significantly exceed the false positive rate
  int mediocre_filters = 0;
  int good_filters = 0;

  for (int length = 1; length <= 10000; length = NextLength(length)) {
    test.Reset();
    for (int i = 0; i < length; i++) {
      test.Add(Key(i, buffer));
    }
    test.Build();

    REQUIRE(test.FilterSize() <= static_cast<size_t>((length * 10 / 8) + 40));

    // All added keys must match
    for (int i = 0; i < length; i++) {
      REQUIRE(test.Matches(Key(i, buffer)));
    }

    // Check false positive rate
    double rate = test.FalsePositiveRate();
    if (kVerbose >= 1) {
      std::fprintf(stderr,
                   "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                   rate * 100.0, length, static_cast<int>(test.FilterSize()));
    }
    REQUIRE(rate <= 0.02);  // Must not be over 2%
    if (rate > 0.0125)
      mediocre_filters++;  // Allowed, but not too often
    else
      good_filters++;
  }
  if (kVerbose >= 1) {
    std::fprintf(stderr, "Filters: %d good, %d mediocre\n", good_filters,
                 mediocre_filters);
  }
  REQUIRE(mediocre_filters <= good_filters / 5);
}
