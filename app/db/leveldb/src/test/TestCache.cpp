#include <catch.hpp>
#include <vector>

#include "leveldb/cache.h"
#include "util/coding.h"

using namespace std;
using namespace leveldb;

// Conversions between numeric keys/values and the types expected by Cache.
static std::string EncodeKey(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}
static int DecodeKey(const Slice& k) {
  assert(k.size() == 4);
  return DecodeFixed32(k.data());
}
static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }
static int DecodeValue(void* v) { return reinterpret_cast<uintptr_t>(v); }

class CacheTest {
 public:
  static void Deleter(const Slice& key, void* v) {
    current_->deleted_keys_.push_back(DecodeKey(key));
    current_->deleted_values_.push_back(DecodeValue(v));
  }

  static constexpr int kCacheSize = 1000;
  std::vector<int> deleted_keys_;
  std::vector<int> deleted_values_;
  Cache* cache_;

  CacheTest() : cache_(NewLRUCache(kCacheSize)) { current_ = this; }

  ~CacheTest() { delete cache_; }

  int Lookup(int key) {
    Cache::Handle* handle = cache_->Lookup(EncodeKey(key));
    const int r = (handle == nullptr) ? -1 : DecodeValue(cache_->Value(handle));
    if (handle != nullptr) {
      cache_->Release(handle);
    }
    return r;
  }

  void Insert(int key, int value, int charge = 1) {
    cache_->Release(cache_->Insert(EncodeKey(key), EncodeValue(value), charge,
                                   &CacheTest::Deleter));
  }

  Cache::Handle* InsertAndReturnHandle(int key, int value, int charge = 1) {
    return cache_->Insert(EncodeKey(key), EncodeValue(value), charge,
                          &CacheTest::Deleter);
  }

  void Erase(int key) { cache_->Erase(EncodeKey(key)); }
  static CacheTest* current_;
};
CacheTest* CacheTest::current_;

TEST_CASE("CacheTest HitAndMiss", "[CacheTest]") {
  CacheTest test;
  REQUIRE(-1 == test.Lookup(100));

  test.Insert(100, 101);
  REQUIRE(101 == test.Lookup(100));
  REQUIRE(-1 == test.Lookup(200));
  REQUIRE(-1 == test.Lookup(300));

  test.Insert(200, 201);
  REQUIRE(101 == test.Lookup(100));
  REQUIRE(201 == test.Lookup(200));
  REQUIRE(-1 == test.Lookup(300));

  test.Insert(100, 102);
  REQUIRE(102 == test.Lookup(100));
  REQUIRE(201 == test.Lookup(200));
  REQUIRE(-1 == test.Lookup(300));

  REQUIRE(1 == test.deleted_keys_.size());
  REQUIRE(100 == test.deleted_keys_[0]);
  REQUIRE(101 == test.deleted_values_[0]);
}

TEST_CASE("CacheTest Erase", "[CacheTest]") {
  CacheTest test;
  test.Erase(200);
  REQUIRE(0 == test.deleted_keys_.size());

  test.Insert(100, 101);
  test.Insert(200, 201);
  test.Erase(100);
  REQUIRE(-1 == test.Lookup(100));
  REQUIRE(201 == test.Lookup(200));
  REQUIRE(1 == test.deleted_keys_.size());
  REQUIRE(100 == test.deleted_keys_[0]);
  REQUIRE(101 == test.deleted_values_[0]);

  test.Erase(100);
  REQUIRE(-1 == test.Lookup(100));
  REQUIRE(201 == test.Lookup(200));
  REQUIRE(1 == test.deleted_keys_.size());
}

TEST_CASE("CacheTest EntriesArePinned", "[CacheTest]") {
  CacheTest test;
  test.Insert(100, 101);
  Cache::Handle* h1 = test.cache_->Lookup(EncodeKey(100));
  REQUIRE(101 == DecodeValue(test.cache_->Value(h1)));

  test.Insert(100, 102);
  Cache::Handle* h2 = test.cache_->Lookup(EncodeKey(100));
  REQUIRE(102 == DecodeValue(test.cache_->Value(h2)));
  REQUIRE(0 == test.deleted_keys_.size());

  test.cache_->Release(h1);
  REQUIRE(1 == test.deleted_keys_.size());
  REQUIRE(100 == test.deleted_keys_[0]);
  REQUIRE(101 == test.deleted_values_[0]);

  test.Erase(100);
  REQUIRE(-1 == test.Lookup(100));
  REQUIRE(1 == test.deleted_keys_.size());

  test.cache_->Release(h2);
  REQUIRE(2 == test.deleted_keys_.size());
  REQUIRE(100 == test.deleted_keys_[1]);
  REQUIRE(102 == test.deleted_values_[1]);
}

TEST_CASE("CacheTest EvictionPolicy", "[CacheTest]") {
  CacheTest test;
  test.Insert(100, 101);
  test.Insert(200, 201);
  test.Insert(300, 301);
  Cache::Handle* h = test.cache_->Lookup(EncodeKey(300));

  // Frequently used entry must be kept around,
  // as must things that are still in use.
  for (int i = 0; i < test.kCacheSize + 100; i++) {
    test.Insert(1000 + i, 2000 + i);
    REQUIRE(2000 + i == test.Lookup(1000 + i));
    REQUIRE(101 == test.Lookup(100));
  }
  REQUIRE(101 == test.Lookup(100));
  REQUIRE(-1 == test.Lookup(200));
  REQUIRE(301 == test.Lookup(300));
  test.cache_->Release(h);
}

TEST_CASE("CacheTest UseExceedsCacheSize", "[CacheTest]") {
  CacheTest test;
  std::vector<Cache::Handle*> h;
  for (int i = 0; i < test.kCacheSize + 100; i++) {
    h.push_back(test.InsertAndReturnHandle(1000 + i, 2000 + i));
  }

  // Check that all the entries can be found in the cache.
  for (int i = 0; i < h.size(); i++) {
    REQUIRE(2000 + i == test.Lookup(1000 + i));
  }

  for (int i = 0; i < h.size(); i++) {
    test.cache_->Release(h[i]);
  }
}

TEST_CASE("CacheTest HeavyEntries", "[CacheTest]") {
  CacheTest test;
  // Add a bunch of light and heavy entries and then count the combined
  // size of items still in the cache, which must be approximately the
  // same as the total capacity.
  const int kLight = 1;
  const int kHeavy = 10;
  int added = 0;
  int index = 0;
  while (added < 2 * test.kCacheSize) {
    const int weight = (index & 1) ? kLight : kHeavy;
    test.Insert(index, 1000 + index, weight);
    added += weight;
    index++;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; i++) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = test.Lookup(i);
    if (r >= 0) {
      cached_weight += weight;
      REQUIRE(1000 + i == r);
    }
  }
  REQUIRE(cached_weight <= test.kCacheSize + test.kCacheSize / 10);
}

TEST_CASE("CacheTest NewId", "[CacheTest]") {
  CacheTest test;
  uint64_t a = test.cache_->NewId();
  uint64_t b = test.cache_->NewId();
  REQUIRE(a != b);
}

TEST_CASE("CacheTest Prune", "[CacheTest]") {
  CacheTest test;
  test.Insert(1, 100);
  test.Insert(2, 200);

  Cache::Handle* handle = test.cache_->Lookup(EncodeKey(1));
  REQUIRE(handle);
  test.cache_->Prune();
  test.cache_->Release(handle);

  REQUIRE(100 == test.Lookup(1));
  REQUIRE(-1 == test.Lookup(2));
}

TEST_CASE("CacheTest ZeroSizeCache", "[CacheTest]") {
  CacheTest test;
  delete test.cache_;
  test.cache_ = NewLRUCache(0);

  test.Insert(1, 100);
  REQUIRE(-1 == test.Lookup(1));
}