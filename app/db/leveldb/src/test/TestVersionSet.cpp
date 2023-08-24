#include <catch.hpp>
#include <iostream>

#include "db/version_set.h"
#include "testutil.h"
#include "util/logging.h"

using namespace std;
using namespace leveldb;

class FindFileTest {
 public:
  FindFileTest() : disjoint_sorted_files_(true) {}

  ~FindFileTest() {
    for (int i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    FileMetaData* f = new FileMetaData;
    f->number = files_.size() + 1;
    f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
    f->largest = InternalKey(largest, largest_seq, kTypeValue);
    files_.push_back(f);
  }

  int Find(const char* key) {
    InternalKey target(key, 100, kTypeValue);
    InternalKeyComparator cmp(BytewiseComparator());
    return FindFile(cmp, files_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    InternalKeyComparator cmp(BytewiseComparator());
    Slice s(smallest != nullptr ? smallest : "");
    Slice l(largest != nullptr ? largest : "");
    return SomeFileOverlapsRange(cmp, disjoint_sorted_files_, files_,
                                 (smallest != nullptr ? &s : nullptr),
                                 (largest != nullptr ? &l : nullptr));
  }

  bool disjoint_sorted_files_;

 private:
  std::vector<FileMetaData*> files_;
};

TEST_CASE("VersionSet Empty", "[VersionSet]") {
  FindFileTest test;
  REQUIRE(0 == test.Find("foo"));
  REQUIRE(!test.Overlaps("a", "z"));
  REQUIRE(!test.Overlaps(nullptr, "z"));
  REQUIRE(!test.Overlaps("a", nullptr));
  REQUIRE(!test.Overlaps(nullptr, nullptr));
}
TEST_CASE("VersionSet Single", "[VersionSet]") {
  FindFileTest test;
  test.Add("p", "q");
  REQUIRE(0 == test.Find("a"));
  REQUIRE(0 == test.Find("p"));
  REQUIRE(0 == test.Find("p1"));
  REQUIRE(0 == test.Find("q"));
  REQUIRE(1 == test.Find("q1"));
  REQUIRE(1 == test.Find("z"));

  REQUIRE(!test.Overlaps("a", "b"));
  REQUIRE(!test.Overlaps("z1", "z2"));
  REQUIRE(test.Overlaps("a", "p"));
  REQUIRE(test.Overlaps("a", "q"));
  REQUIRE(test.Overlaps("a", "z"));
  REQUIRE(test.Overlaps("p", "p1"));
  REQUIRE(test.Overlaps("p", "q"));
  REQUIRE(test.Overlaps("p", "z"));
  REQUIRE(test.Overlaps("p1", "p2"));
  REQUIRE(test.Overlaps("p1", "z"));
  REQUIRE(test.Overlaps("q", "q"));
  REQUIRE(test.Overlaps("q", "q1"));

  REQUIRE(!test.Overlaps(nullptr, "j"));
  REQUIRE(!test.Overlaps("r", nullptr));
  REQUIRE(test.Overlaps(nullptr, "p"));
  REQUIRE(test.Overlaps(nullptr, "p1"));
  REQUIRE(test.Overlaps("q", nullptr));
  REQUIRE(test.Overlaps(nullptr, nullptr));
}
TEST_CASE("VersionSet Multiple", "[VersionSet]") {
  FindFileTest test;
  test.Add("150", "200");
  test.Add("200", "250");
  test.Add("300", "350");
  test.Add("400", "450");
  REQUIRE(0 == test.Find("100"));
  REQUIRE(0 == test.Find("150"));
  REQUIRE(0 == test.Find("151"));
  REQUIRE(0 == test.Find("199"));
  REQUIRE(0 == test.Find("200"));
  REQUIRE(1 == test.Find("201"));
  REQUIRE(1 == test.Find("249"));
  REQUIRE(1 == test.Find("250"));
  REQUIRE(2 == test.Find("251"));
  REQUIRE(2 == test.Find("299"));
  REQUIRE(2 == test.Find("300"));
  REQUIRE(2 == test.Find("349"));
  REQUIRE(2 == test.Find("350"));
  REQUIRE(3 == test.Find("351"));
  REQUIRE(3 == test.Find("400"));
  REQUIRE(3 == test.Find("450"));
  REQUIRE(4 == test.Find("451"));

  REQUIRE(!test.Overlaps("100", "149"));
  REQUIRE(!test.Overlaps("251", "299"));
  REQUIRE(!test.Overlaps("451", "500"));
  REQUIRE(!test.Overlaps("351", "399"));

  REQUIRE(test.Overlaps("100", "150"));
  REQUIRE(test.Overlaps("100", "200"));
  REQUIRE(test.Overlaps("100", "300"));
  REQUIRE(test.Overlaps("100", "400"));
  REQUIRE(test.Overlaps("100", "500"));
  REQUIRE(test.Overlaps("375", "400"));
  REQUIRE(test.Overlaps("450", "450"));
  REQUIRE(test.Overlaps("450", "500"));
}
TEST_CASE("VersionSet MultipleNullBoundaries", "[VersionSet]") {
  FindFileTest test;
  test.Add("150", "200");
  test.Add("200", "250");
  test.Add("300", "350");
  test.Add("400", "450");
  REQUIRE(!test.Overlaps(nullptr, "149"));
  REQUIRE(!test.Overlaps("451", nullptr));
  REQUIRE(test.Overlaps(nullptr, nullptr));
  REQUIRE(test.Overlaps(nullptr, "150"));
  REQUIRE(test.Overlaps(nullptr, "199"));
  REQUIRE(test.Overlaps(nullptr, "200"));
  REQUIRE(test.Overlaps(nullptr, "201"));
  REQUIRE(test.Overlaps(nullptr, "400"));
  REQUIRE(test.Overlaps(nullptr, "800"));
  REQUIRE(test.Overlaps("100", nullptr));
  REQUIRE(test.Overlaps("200", nullptr));
  REQUIRE(test.Overlaps("449", nullptr));
  REQUIRE(test.Overlaps("450", nullptr));
}
TEST_CASE("VersionSet OverlapSequenceChecks", "[VersionSet]") {
  FindFileTest test;
  test.Add("200", "200", 5000, 3000);
  REQUIRE(!test.Overlaps("199", "199"));
  REQUIRE(!test.Overlaps("201", "300"));
  REQUIRE(test.Overlaps("200", "200"));
  REQUIRE(test.Overlaps("190", "200"));
  REQUIRE(test.Overlaps("200", "210"));
}
TEST_CASE("VersionSet OverlappingFiles", "[VersionSet]") {
  FindFileTest test;
  test.Add("150", "600");
  test.Add("400", "500");
  test.disjoint_sorted_files_ = false;
  REQUIRE(!test.Overlaps("100", "149"));
  REQUIRE(!test.Overlaps("601", "700"));
  REQUIRE(test.Overlaps("100", "150"));
  REQUIRE(test.Overlaps("100", "200"));
  REQUIRE(test.Overlaps("100", "300"));
  REQUIRE(test.Overlaps("100", "400"));
  REQUIRE(test.Overlaps("100", "500"));
  REQUIRE(test.Overlaps("375", "400"));
  REQUIRE(test.Overlaps("450", "450"));
  REQUIRE(test.Overlaps("450", "500"));
  REQUIRE(test.Overlaps("450", "700"));
  REQUIRE(test.Overlaps("600", "700"));
}

class AddBoundaryInputsTest {
 public:
  std::vector<FileMetaData*> level_files_;
  std::vector<FileMetaData*> compaction_files_;
  std::vector<FileMetaData*> all_files_;
  InternalKeyComparator icmp_;

  AddBoundaryInputsTest() : icmp_(BytewiseComparator()) {}

  ~AddBoundaryInputsTest() {
    for (size_t i = 0; i < all_files_.size(); ++i) {
      delete all_files_[i];
    }
    all_files_.clear();
  }

  FileMetaData* CreateFileMetaData(uint64_t number, InternalKey smallest,
                                   InternalKey largest) {
    FileMetaData* f = new FileMetaData();
    f->number = number;
    f->smallest = smallest;
    f->largest = largest;
    all_files_.push_back(f);
    return f;
  }
};

namespace leveldb {
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files);
}
TEST_CASE("VersionSet TestEmptyFileSets", "[VersionSet]") {
  AddBoundaryInputsTest test;
  AddBoundaryInputs(test.icmp_, test.level_files_, &test.compaction_files_);
  REQUIRE(test.compaction_files_.empty());
  REQUIRE(test.level_files_.empty());
}
TEST_CASE("VersionSet TestEmptyLevelFiles", "[VersionSet]") {
  AddBoundaryInputsTest test;
  FileMetaData* f1 =
      test.CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
                              InternalKey(InternalKey("100", 1, kTypeValue)));
  test.compaction_files_.push_back(f1);

  AddBoundaryInputs(test.icmp_, test.level_files_, &test.compaction_files_);
  REQUIRE(1 == test.compaction_files_.size());
  REQUIRE(f1 == test.compaction_files_[0]);
  REQUIRE(test.level_files_.empty());
}
TEST_CASE("VersionSet TestEmptyCompactionFiles", "[VersionSet]") {
  AddBoundaryInputsTest test;
  FileMetaData* f1 =
      test.CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
                              InternalKey(InternalKey("100", 1, kTypeValue)));
  test.level_files_.push_back(f1);

  AddBoundaryInputs(test.icmp_, test.level_files_, &test.compaction_files_);
  REQUIRE(test.compaction_files_.empty());
  REQUIRE(1 == test.level_files_.size());
  REQUIRE(f1 == test.level_files_[0]);
}
TEST_CASE("VersionSet TestNoBoundaryFiles", "[VersionSet]") {
  AddBoundaryInputsTest test;
  FileMetaData* f1 =
      test.CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
                              InternalKey(InternalKey("100", 1, kTypeValue)));
  FileMetaData* f2 =
      test.CreateFileMetaData(1, InternalKey("200", 2, kTypeValue),
                              InternalKey(InternalKey("200", 1, kTypeValue)));
  FileMetaData* f3 =
      test.CreateFileMetaData(1, InternalKey("300", 2, kTypeValue),
                              InternalKey(InternalKey("300", 1, kTypeValue)));

  test.level_files_.push_back(f3);
  test.level_files_.push_back(f2);
  test.level_files_.push_back(f1);
  test.compaction_files_.push_back(f2);
  test.compaction_files_.push_back(f3);

  AddBoundaryInputs(test.icmp_, test.level_files_, &test.compaction_files_);
  REQUIRE(2 == test.compaction_files_.size());
}
TEST_CASE("VersionSet TestOneBoundaryFiles", "[VersionSet]") {
  AddBoundaryInputsTest test;
  FileMetaData* f1 =
      test.CreateFileMetaData(1, InternalKey("100", 3, kTypeValue),
                              InternalKey(InternalKey("100", 2, kTypeValue)));
  FileMetaData* f2 =
      test.CreateFileMetaData(1, InternalKey("100", 1, kTypeValue),
                              InternalKey(InternalKey("200", 3, kTypeValue)));
  FileMetaData* f3 =
      test.CreateFileMetaData(1, InternalKey("300", 2, kTypeValue),
                              InternalKey(InternalKey("300", 1, kTypeValue)));

  test.level_files_.push_back(f3);
  test.level_files_.push_back(f2);
  test.level_files_.push_back(f1);
  test.compaction_files_.push_back(f1);

  AddBoundaryInputs(test.icmp_, test.level_files_, &test.compaction_files_);
  REQUIRE(2 == test.compaction_files_.size());
  REQUIRE(f1 == test.compaction_files_[0]);
  REQUIRE(f2 == test.compaction_files_[1]);
}
TEST_CASE("VersionSet TestTwoBoundaryFiles", "[VersionSet]") {
  AddBoundaryInputsTest test;
  FileMetaData* f1 =
      test.CreateFileMetaData(1, InternalKey("100", 6, kTypeValue),
                              InternalKey(InternalKey("100", 5, kTypeValue)));
  FileMetaData* f2 =
      test.CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
                              InternalKey(InternalKey("300", 1, kTypeValue)));
  FileMetaData* f3 =
      test.CreateFileMetaData(1, InternalKey("100", 4, kTypeValue),
                              InternalKey(InternalKey("100", 3, kTypeValue)));

  test.level_files_.push_back(f2);
  test.level_files_.push_back(f3);
  test.level_files_.push_back(f1);
  test.compaction_files_.push_back(f1);

  AddBoundaryInputs(test.icmp_, test.level_files_, &test.compaction_files_);
  REQUIRE(3 == test.compaction_files_.size());
  REQUIRE(f1 == test.compaction_files_[0]);
  REQUIRE(f3 == test.compaction_files_[1]);
  REQUIRE(f2 == test.compaction_files_[2]);
}
TEST_CASE("VersionSet TestDisjoinFilePointers", "[VersionSet]") {
  AddBoundaryInputsTest test;
  FileMetaData* f1 =
      test.CreateFileMetaData(1, InternalKey("100", 6, kTypeValue),
                              InternalKey(InternalKey("100", 5, kTypeValue)));
  FileMetaData* f2 =
      test.CreateFileMetaData(1, InternalKey("100", 6, kTypeValue),
                              InternalKey(InternalKey("100", 5, kTypeValue)));
  FileMetaData* f3 =
      test.CreateFileMetaData(1, InternalKey("100", 2, kTypeValue),
                              InternalKey(InternalKey("300", 1, kTypeValue)));
  FileMetaData* f4 =
      test.CreateFileMetaData(1, InternalKey("100", 4, kTypeValue),
                              InternalKey(InternalKey("100", 3, kTypeValue)));

  test.level_files_.push_back(f2);
  test.level_files_.push_back(f3);
  test.level_files_.push_back(f4);

  test.compaction_files_.push_back(f1);

  AddBoundaryInputs(test.icmp_, test.level_files_, &test.compaction_files_);
  REQUIRE(3 == test.compaction_files_.size());
  REQUIRE(f1 == test.compaction_files_[0]);
  REQUIRE(f4 == test.compaction_files_[1]);
  REQUIRE(f3 == test.compaction_files_[2]);
}
