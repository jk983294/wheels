#include <getopt.h>
#include <iostream>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

using namespace std;

void basic(leveldb::DB* db);
void batch_update(leveldb::DB* db);
void sync_update(leveldb::DB* db);
void iteration(leveldb::DB* db);
void cache_disable_iteration(leveldb::DB* db);
void snapshot_demo(leveldb::DB* db);

int main(int argc, char** argv) {
  int type = 1;
  int opt;
  while ((opt = getopt(argc, argv, "t:")) != -1) {
    switch (opt) {
      case 't':
        type = std::stoi(optarg);
        break;
      default:
        return 0;
    }
  }

  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status s = leveldb::DB::Open(options, "/tmp/testdb", &db);

  if (type == 1) {
    basic(db);
  } else if (type == 2) {
    batch_update(db);
  } else if (type == 3) {
    sync_update(db);
  } else if (type == 4) {
    iteration(db);
  } else if (type == 5) {
    snapshot_demo(db);
  } else if (type == 6) {
    cache_disable_iteration(db);
  }
  delete db;
}

void snapshot_demo(leveldb::DB* db) {
  leveldb::ReadOptions options;
  options.snapshot = db->GetSnapshot();
  // ... apply some updates to db ...
  leveldb::Iterator* iter = db->NewIterator(options);
  // ... read using iter to view the state when the snapshot was created ...
  delete iter;
  db->ReleaseSnapshot(options.snapshot);
}

void iteration(leveldb::DB* db) {
  leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
  }
  delete it;
}

void cache_disable_iteration(leveldb::DB* db) {
  leveldb::ReadOptions options;
  options.fill_cache = false;
  leveldb::Iterator* it = db->NewIterator(options);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
  }
  delete it;
}

void sync_update(leveldb::DB* db) {
  leveldb::Slice key1 = "key1";
  std::string value = "haha";
  leveldb::WriteOptions write_options;
  write_options.sync = true;
  db->Put(write_options, key1, value);
}

void batch_update(leveldb::DB* db) {
  leveldb::Slice key1 = "key1";
  leveldb::Slice key2 = "key2";
  std::string value;
  leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
  if (s.ok()) {
    leveldb::WriteBatch batch;
    batch.Delete(key1);
    batch.Put(key2, value);
    s = db->Write(leveldb::WriteOptions(), &batch);
  }
}

void basic(leveldb::DB* db) {
  leveldb::Slice key1 = "key1";
  std::string value = "haha";
  leveldb::Status s = db->Put(leveldb::WriteOptions(), key1, value);

  if (s.ok()) {
    std::string value1;
    s = db->Get(leveldb::ReadOptions(), key1, &value1);
    if (!s.ok()) cerr << s.ToString() << endl;
    else cout << "get " << value1 << endl;
    s = db->Delete(leveldb::WriteOptions(), key1);
    if (!s.ok()) cerr << s.ToString() << endl;
  }
}