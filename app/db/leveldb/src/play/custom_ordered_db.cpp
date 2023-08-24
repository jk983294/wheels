#include <getopt.h>
#include <leveldb/cache.h>
#include <leveldb/filter_policy.h>

#include <iostream>

#include "leveldb/comparator.h"
#include "leveldb/db.h"

using namespace std;

class TwoPartComparator : public leveldb::Comparator {
 public:
  // Three-way comparison function:
  //   if a < b: negative result
  //   if a > b: positive result
  //   else: zero result
  int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
    int a1, a2, b1, b2;
    ParseKey(a, a1, a2);
    ParseKey(b, b1, b2);
    if (a1 < b1) return -1;
    if (a1 > b1) return +1;
    if (a2 < b2) return -1;
    if (a2 > b2) return +1;
    return 0;
  }

  void ParseKey(const leveldb::Slice& a, int& a1, int& a2) const {
    a1 = *reinterpret_cast<const int*>(a.data());
    a2 = *(reinterpret_cast<const int*>(a.data()) + 1);
  }

  // Ignore the following methods for now:
  const char* Name() const { return "TwoPartComparator"; }
  void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
  void FindShortSuccessor(std::string*) const {}
};

leveldb::Slice create_key(char* arr, int len, int a, int b) {
  memset(arr, len, 0);
  *(reinterpret_cast<int*>(arr)) = a;
  *(reinterpret_cast<int*>(arr) + 1) = b;
  return leveldb::Slice(arr);
}

int main(int argc, char** argv) {
  TwoPartComparator cmp;
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  options.comparator = &cmp;
  options.block_cache = leveldb::NewLRUCache(10 * 1024 * 1024);  // 10MB
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  leveldb::Status s = leveldb::DB::Open(options, "/tmp/test_ordered_db", &db);

  constexpr int len_ = 16;
  char tmp[16] = {};
  leveldb::Slice key1 = create_key(tmp, len_, 4, 2);
  std::string value = "haha";
  s = db->Put(leveldb::WriteOptions(), key1, value);

  std::string value1;
  s = db->Get(leveldb::ReadOptions(), key1, &value1);
  if (!s.ok()) cerr << s.ToString() << endl;
  else cout << "saved value is " << value1 << endl;

  s = db->Delete(leveldb::WriteOptions(), key1);
  if (!s.ok()) cerr << s.ToString() << endl;
  else cout << "delete key is " << tmp << endl;

  delete db;
  delete options.filter_policy;
}