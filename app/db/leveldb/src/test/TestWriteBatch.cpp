#include <catch.hpp>
#include <iostream>

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "testutil.h"
#include "util/logging.h"

using namespace std;
using namespace leveldb;

static std::string PrintContents(WriteBatch* b) {
  InternalKeyComparator cmp(BytewiseComparator());
  MemTable* mem = new MemTable(cmp);
  mem->Ref();
  std::string state;
  Status s = WriteBatchInternal::InsertInto(b, mem);
  int count = 0;
  Iterator* iter = mem->NewIterator();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey ikey;
    REQUIRE(ParseInternalKey(iter->key(), &ikey));
    switch (ikey.type) {
      case kTypeValue:
        state.append("Put(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case kTypeDeletion:
        state.append("Delete(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
    }
    state.append("@");
    state.append(NumberToString(ikey.sequence));
  }
  delete iter;
  if (!s.ok()) {
    state.append("ParseError()");
  } else if (count != WriteBatchInternal::Count(b)) {
    state.append("CountMismatch()");
  }
  mem->Unref();
  return state;
}

TEST_CASE("WriteBatch Empty", "[WriteBatch]") {
  WriteBatch batch;
  REQUIRE("" == PrintContents(&batch));
  REQUIRE(0 == WriteBatchInternal::Count(&batch));
}
TEST_CASE("WriteBatch Multiple", "[WriteBatch]") {
  WriteBatch batch;
  batch.Put(Slice("foo"), Slice("bar"));
  batch.Delete(Slice("box"));
  batch.Put(Slice("baz"), Slice("boo"));
  WriteBatchInternal::SetSequence(&batch, 100);
  REQUIRE(100 == WriteBatchInternal::Sequence(&batch));
  REQUIRE(3 == WriteBatchInternal::Count(&batch));
  REQUIRE(
      "Put(baz, boo)@102"
      "Delete(box)@101"
      "Put(foo, bar)@100" == PrintContents(&batch));
}
TEST_CASE("WriteBatch Corruption", "[WriteBatch]") {
  WriteBatch batch;
  batch.Put(Slice("foo"), Slice("bar"));
  batch.Delete(Slice("box"));
  WriteBatchInternal::SetSequence(&batch, 200);
  Slice contents = WriteBatchInternal::Contents(&batch);
  WriteBatchInternal::SetContents(&batch,
                                  Slice(contents.data(), contents.size() - 1));
  REQUIRE(
      "Put(foo, bar)@200"
      "ParseError()" == PrintContents(&batch));
}
TEST_CASE("WriteBatch Append", "[WriteBatch]") {
  WriteBatch b1, b2;
  WriteBatchInternal::SetSequence(&b1, 200);
  WriteBatchInternal::SetSequence(&b2, 300);
  b1.Append(b2);
  REQUIRE("" == PrintContents(&b1));
  b2.Put("a", "va");
  b1.Append(b2);
  REQUIRE("Put(a, va)@200" == PrintContents(&b1));
  b2.Clear();
  b2.Put("b", "vb");
  b1.Append(b2);
  REQUIRE(
      "Put(a, va)@200"
      "Put(b, vb)@201" == PrintContents(&b1));
  b2.Delete("foo");
  b1.Append(b2);
  REQUIRE(
      "Put(a, va)@200"
      "Put(b, vb)@202"
      "Put(b, vb)@201"
      "Delete(foo)@203" == PrintContents(&b1));
}
TEST_CASE("WriteBatch ApproximateSize", "[WriteBatch]") {
  WriteBatch batch;
  size_t empty_size = batch.ApproximateSize();

  batch.Put(Slice("foo"), Slice("bar"));
  size_t one_key_size = batch.ApproximateSize();
  REQUIRE(empty_size < one_key_size);

  batch.Put(Slice("baz"), Slice("boo"));
  size_t two_keys_size = batch.ApproximateSize();
  REQUIRE(one_key_size < two_keys_size);

  batch.Delete(Slice("box"));
  size_t post_delete_size = batch.ApproximateSize();
  REQUIRE(two_keys_size < post_delete_size);
}
