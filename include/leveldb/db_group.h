//Copyright(c) 2013 The jd.com developers. All rights reserved.

#ifndef STORAGE_LEVELDB_EB_DB_GROUP_H_
#define STORAGE_LEVELDB_EB_DB_GROUP_H_
#include "leveldb/db.h"
#include <map>
#include <vector>
namespace leveldb {

class DB;	
class DBImpl;
struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch;

class DB_GROUP {
 public:
   DB_GROUP(){} 
   ~DB_GROUP(){}
  //Open the database group with the specified "name"
  //Stores a pointer to a heap-allocated database group and returns
  //OK on success
  //Stores NULL in *dbgroup_ptr and returns an error
  //Caller should delete *dbgroup_ptr when it is no longer needed.
  static Status Open(const Options &options,
		      const std::string& name,
		      DB_GROUP** dbgroup_ptr);

  //Open the database & name,and insert itinto opened_dbs_ on success
  //return OK on success
  //Caller should use CloseDB when the db is no longer needed.
  virtual Status OpenDB(const Options &options,
		        const std::string& name
			) = 0;

  //Close the database &name and remove if from opened_dbs_ on success
  //return OK on success
  virtual Status CloseDB(const std::string& name) = 0;

  //list opened dbs, return opened db's name in list and
  //OK on success
  virtual Status ListOpenedDBs(std::vector<std::string> &list) = 0;

  //Append the specified updates to the log
  //Returns SequenceNumber on success,0 on failure
  //Note:the sequence number here will always be possitive
  //Note:consider setting options.syc = true
  virtual Status WriteToDBJustInLog(const WriteOptions& options,
		                    const std::string &dbname,
				    WriteBatch* updates,
				    uint64_t &sequenceNumber
				    ) = 0;
  //if SLog contains entries whose sequence Number is smaller or equal to the 
  //given one,store all those corresponding data to the database
  //Return Ok on success,non-OK on failure
  virtual Status SetLogVisible(const uint64_t sequenceNumber) = 0 ;

  //It seems the most smart way,we will implement it later 
  virtual Status Write(const WriteOptions &options,
			    const std::string &dbname,
			    WriteBatch* updates) = 0;

  //If the database contains an entry for "key" store the 
  //corresponding value in *value and return OK
  //If there is no entry for "key" leave *value unchanged and return
  //a status for which Status::IsNotFound() returns true
  //May return some other Status on an error 
  virtual Status Get(const ReadOptions& options,
		     const std::string &dbname,
		     const Slice& key,
		     std::string* value) = 0;
 private:
  //NO copyting allowed
  DB_GROUP(const DB_GROUP&);
  void operator=(const DB_GROUP&);
 // std::map<std::string,DB**> opened_dbs_;
 // int64_t max_sequence_number_;    
};//DB_GROUP
}
//namespace leveldb
#endif//STORAGE_LEVELDB_EB_DB_GROUP_IMPL_H_
