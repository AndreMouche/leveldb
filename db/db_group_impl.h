//Copyright(c) 2013 The jd.com developers. All rights reserved.

#ifndef STORAGE_LEVELDB_DB_DB_GROUP_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_GROUP_IMPL_H_
#include "leveldb/db_group.h"
#include "leveldb/env.h"
#include "db/log_writer.h"
#include "db/dbformat.h"
#include "port/port.h"
#include <map>
#include <vector>
#include <deque>


namespace leveldb {

class DB;	
class DBImpl;
struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch;

static const uint64_t kCommitLogSize = 4 << 20;

class DB_GROUP_Impl:public DB_GROUP {
 public:
  //Open the database group with the specified "name"
  //Stores a pointer to a heap-allocated database group and returns
  //OK on success
  DB_GROUP_Impl(const Options &options,
		      const std::string& name);

  virtual ~DB_GROUP_Impl();

  //Implementation of the DB_GROUP interface
  //Open the database & name,and insert itinto opened_dbs_ on success
  //return OK on success
  //Caller should use CloseDB when the db is no longer needed.
  virtual Status OpenDB(const Options &options,
		        const std::string& name
			);

  //Close the database &name and remove if from opened_dbs_ on success
  //return OK on success
  virtual Status CloseDB(const std::string& name);

  //list opened dbs, return opened db's name in list and
  //OK on success
  virtual Status ListOpenedDBs(std::vector<std::string> &list);

  //Append the specified updates to the log
  //Returns SequenceNumber on success,0 on failure
  //Note:the sequence number here will always be possitive
  //Note:consider setting options.syc = true
  virtual Status WriteToDBJustInLog(const WriteOptions& options,
		                    const std::string &dbname,
				    WriteBatch* updates,
				    uint64_t &sequenceNumber
				    );
  //if SLog contains entries whose sequence Number is smaller or equal to the 
  //given one,store all those corresponding data to the database
  //Return Ok on success,non-OK on failure
  virtual Status SetLogVisible(const uint64_t sequenceNumber);

  //It seems the most smart way,we will implement it later 
  virtual Status Write(const WriteOptions &options,
			    const std::string &dbname,
			    WriteBatch* updates); 

  //If the database contains an entry for "key" store the 
  //corresponding value in *value and return OK
  //If there is no entry for "key" leave *value unchanged and return
  //a status for which Status::IsNotFound() returns true
  //May return some other Status on an error 
  virtual Status Get(const ReadOptions& options,
		     const std::string &dbname,
		     const Slice& key,
		     std::string* value);
  Status CheckAndGetDB(const std::string &dbname,DB **db);
  Status MakeRoomForWrite(bool force);
  Status GetLogBatchBySequenceNumber(const uint64_t sequenceNumber,
		WriteBatch &batch);
 private:
  friend class DB_GROUP;
  struct Writer;
  //NO copyting allowed
  DB_GROUP_Impl(const DB_GROUP_Impl&);
  void operator=(const DB_GROUP_Impl&);
  std::map<std::string,DB*> opened_dbs_;
  int64_t max_sequence_number_;

  LogItemGroup* log_item_group_;

  port::Mutex mutex_;
  log::Writer* log_;
  uint32_t seed_;
  WritableFile* logfile_; 
  uint64_t logfile_number_;

//queue for writers
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;
  
  //constant after construction
  Env* const env_; 
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;
  bool owns_info_log_;
  bool owns_cache_;
  const std::string group_name_;

 };//DB_GROUP
}
//namespace leveldb
#endif//STORAGE_LEVELDB_EB_DB_GROUP_IMPL_H_
