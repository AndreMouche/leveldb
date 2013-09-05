//Author:jingdong

#include "db/db_group_impl.h"
#include "leveldb/db.h"
#include <algorithm>
#include <string>
#include <stdio.h>
#include <iostream>
#include <vector>
#include "port/port.h"
#include "leveldb/env.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/filename.h"
#include "db/dbformat.h"
#include "db/write_batch_internal.h"
#include "util/mutexlock.h"
#include <sys/stat.h>
namespace leveldb {

class JDebug;	
class DB;
const int kNumNonTableCacheFiles = 10;

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange2(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

Options SanitizeOptions2(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange2(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange2(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange2(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  return result;
}


struct DB_GROUP_Impl::Writer {
  Status status;
  WriteBatch * batch;
  bool sync;
  bool done;
  port::CondVar cv;
  explicit Writer(port::Mutex* mu) : cv(mu){}
};

DB_GROUP_Impl::DB_GROUP_Impl(const Options& options,
		             const std::string& name) 
        :env_(options.env),
	internal_comparator_(options.comparator),
	internal_filter_policy_(options.filter_policy),
	options_(SanitizeOptions2(name,&internal_comparator_,
				&internal_filter_policy_,options)),
	group_name_(name),
        logfile_(NULL),
        logfile_number_(0),
        log_(NULL),
        seed_(0) {
   log_item_group_ = new LogItemGroup();
   //TODO
}
Status DB_GROUP_Impl::OpenDB(const Options &options,
		      const std::string &name){ 
   Status status = Status::OK();
   std::map<std::string,DB*>::iterator it;
   it  = opened_dbs_.find(name);
   if(it != opened_dbs_.end()) {
     status = Status::NotSupported(
		  "The special db is already in the oppened set");
     return status;
   }
   DB* db;
   std::string db_path = group_name_ + "/" + name; 
   status = DB::Open(options,db_path,&db);
   if(status.ok()) {
      opened_dbs_.insert(std::pair<std::string,DB*>(name,db));
   } 
   return status;
}

Status DB_GROUP_Impl::CloseDB(const std::string& name) {
   Status status = Status::OK();
   std::map<std::string,DB*>::iterator it;
   it = opened_dbs_.find(name);
   if(it == opened_dbs_.end()) {
      status = Status::NotSupported(
		      "The special db is not in the opened set");
      return status;
   }

   delete it->second;
   opened_dbs_.erase(it);

   return status;
}

Status DB_GROUP_Impl::ListOpenedDBs(std::vector<std::string> &list) {
   Status status = Status::OK();
   std::map<std::string,DB*>::iterator it;
   for(it = opened_dbs_.begin();it != opened_dbs_.end(); ++it) {
       list.push_back(it->first);
   }
   return status;
}

Status DB_GROUP_Impl::WriteToDBJustInLog(const WriteOptions &options,
		                         const std::string &dbname,
					 WriteBatch* updates,
					 uint64_t &sequenceNumber) {
   if(updates == NULL) {
      return Status::NotSupported("updates shoudn't be NULL!");
   } 
   DB *db;
   Status status = CheckAndGetDB(dbname,&db);
   if(!status.ok()) {
       return status;
   }

   Writer w(&mutex_);
   w.batch = updates;
   w.sync = options.sync;
   w.done = false;
   MutexLock l(&mutex_);
   writers_.push_back(&w);
   while(!w.done && &w != writers_.front()) { //we wait the pre one finished
        w.cv.Wait();
   }

   if(w.done) {
      return w.status;
   }

   status = MakeRoomForWrite(updates == NULL);
   uint64_t last_sequence =  max_sequence_number_ 
	    > log_item_group_->GetLastSequenceNumber()?
	      max_sequence_number_:
	      log_item_group_->GetLastSequenceNumber();
   Writer* last_writer = &w;
   if(status.ok()) {
       sequenceNumber = last_sequence + 1;
       WriteBatchInternal::SetSequence(updates,sequenceNumber);
       WriteBatchInternal::SetDBname(updates,dbname);
       JDebug::JDebugInfo("Insert to log with sequenceNumber",sequenceNumber);
       //add to log
       {
         mutex_.Unlock();
	 uint64_t cur_offset;
	 status = log_->AddRecord2(WriteBatchInternal::Contents(updates),
			 cur_offset);
	 if(status.ok() && options.sync) {
	    status = logfile_->Sync();
	 }

	 mutex_.Lock();
	 if(status.ok()) {
		 std::string fname;
		 status = log_->GetFileName(fname);
		 if(status.ok()) {
		    LogSequenceItem log_seq_item(sequenceNumber,
				    fname,cur_offset,dbname);
		    status = log_item_group_->PutSequenceItem(log_seq_item);
		 }
	 }
       }
   }

   while(true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if(ready != &w) {
      ready -> status = status;
      ready->done  = true;
      ready->cv.Signal();
    }

    if(ready == last_writer) break;
   }  

   //Notify new head write queue.
   if(!writers_.empty()) {
       writers_.front()->cv.Signal();
   }
   return status;
}

Status DB_GROUP_Impl::CheckAndGetDB(const std::string &dbname,DB **db) {
   Status status = Status::OK();
   std::map<std::string,DB*>::iterator it;
   it = opened_dbs_.find(dbname);
   if(it == opened_dbs_.end()){
       status = Status::NotSupported(
		       "The special db is not opened.Please open it first.");
       return status;
   }
   *db = it->second;
   return status;
}

Status DB_GROUP_Impl::SetLogVisible(const uint64_t seqNum) {
   LogSequenceItem front_item;
   Status status = Status::OK();
   while(log_item_group_->GetFrontItem(&front_item).ok()&&
		   front_item.sequence_number() <= seqNum) {
      SequenceNumber sequenceNumber = front_item.sequence_number();
      WriteBatch my_batch;
      status = GetLogBatchBySequenceNumber(sequenceNumber,my_batch);
      if(!status.ok()) {
           //TODO
	   break;
      } 

      DB *db;
      std::string dbname = WriteBatchInternal::GetDBname(&my_batch);
      JDebug::JDebugInfo("Get DB name from log:" + dbname , sequenceNumber);
      //std::string dbname = front_item.dbname();
      status = CheckAndGetDB(dbname,&db);
      if(!status.ok()) {
         break;
      }
      JDebug::JDebugInfo("Set Log visible",sequenceNumber);
      status = db -> WriteToMemtable(WriteOptions(),&my_batch);
      if(!status.ok()) {
         break;
      }
      max_sequence_number_ = sequenceNumber - 1
	      + WriteBatchInternal::Count(&my_batch);
      JDebug::JDebugInfo("Max sequenceNumber now ",max_sequence_number_);
      log_item_group_->PopEarlierItemBeforeOrEqual(sequenceNumber);
   }
   return status;
}

Status DB_GROUP_Impl::Get(const ReadOptions& options,
		          const std::string &dbname,
			  const Slice& key,
			  std::string* value){
   DB *db;
   Status status = CheckAndGetDB(dbname,&db);
   if(status.ok()) {
     status = db->Get(options,key,value);
   }
   return status;
}

//TODO..
Status DB_GROUP_Impl::Write(const WriteOptions &options,
			    const std::string &dbname,
			    WriteBatch* updates) {
 /*  DB *db;
   Status status = CheckAndGetDB(dbname,&db);
   if(status.ok()) {
       uint64_t sequenceNumber;	   
       status = db->WriteToLog(options,updates,sequenceNumber);
       if(status.ok()) {
          status = db->SetLogVisible(sequenceNumber);
       }
   } 
   return status;
 */
}


//Check whether log file is full
Status DB_GROUP_Impl::MakeRoomForWrite(bool force){
  Status status = Status::OK();
  //TODO
  return status;
}

Status DB_GROUP_Impl::GetLogBatchBySequenceNumber(const uint64_t sequenceNumber,
		WriteBatch &batch) {
    
    LogSequenceItem log_sequence_item;
    Status status = log_item_group_->GetSequenceItem(sequenceNumber,
		    &log_sequence_item);
    if(!status.ok()) {
       return status;
    }

  // Open the log file
   std::string fname = log_sequence_item.filename();
  
   struct LogReporter : public log::Reader::Reporter {
      Env* env;
      Logger* info_log;
      const char* fname;
      Status* status;  // NULL if options_.paranoid_checks==false
      virtual void Corruption(size_t bytes, const Status& s) {
        Log(info_log, "%s%s: dropping %d bytes; %s",
            (this->status == NULL ? "(ignoring error) " : ""),
             fname, static_cast<int>(bytes), s.ToString().c_str());
           if (this->status != NULL && this->status->ok()) *this->status = s;
        }
   };

   mutex_.AssertHeld();

   
   SequentialFile* file;
   status = env_->NewSequentialFile(fname, &file);
   if (!status.ok()) {
     return status;
   } 

   // Create the log reader.
   LogReporter reporter;
   reporter.env = env_;
   reporter.info_log = options_.info_log;
   reporter.fname = fname.c_str();
   reporter.status = (options_.paranoid_checks ? &status : NULL);
   // We intentially make log::Reader do checksumming even if
   // paranoid_checks==false so that corruptions cause entire commits
   // to be skipped instead of propagating bad information (like overly
   // large sequence numbers).
   log::Reader reader(file, &reporter, true/*checksum*/,
                     log_sequence_item.offset()/*initial_offset*/);

   // Read all the records and add to a memtable
   std::string scratch;
   Slice record;
   while(reader.ReadRecord(&record, &scratch)) { //acturally do only once here
      if (record.size() < 12) { 
        reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
        status =  Status::Corruption("log record too small");
        break;
    }
    WriteBatchInternal::SetContents(&batch, record);
    //check sequenceNumber
    SequenceNumber cur_sq = WriteBatchInternal::Sequence(&batch);
    if(cur_sq != sequenceNumber) {
       status =  Status::Corruption("Inner Error:sequnceNumber not match");
    } else {
       const SequenceNumber last_seq = cur_sq +
                    WriteBatchInternal::Count(&batch) - 1;
    
       if (last_seq <= max_sequence_number_) {
          status = Status::Corruption(
		      "Illegal SequenceNumber:SeqNumber Expired.");
       }
    }
    break; // do only once
  }
  delete file;
  return status;
}  
DB_GROUP_Impl::~DB_GROUP_Impl(){
   Status status = Status::OK();
   std::map<std::string,DB*>::iterator it;
   
   for(it = opened_dbs_.begin();it != opened_dbs_.end(); ++it) {
      delete it->second;
   }

   delete log_;
   delete logfile_;
   opened_dbs_.clear();
   
}

Status DB_GROUP::Open(const Options &options,
		      const std::string& name,
		      DB_GROUP** dbgoup_ptr){
   *dbgoup_ptr = NULL;
   Status status = Status::OK();
   DB_GROUP_Impl* impl = new DB_GROUP_Impl(options,name);
   if(access(name.c_str(), F_OK) != 0) { //db donot exists 
      if(options.create_if_missing) {
          if (mkdir(name.c_str(), 0755) != 0) {                                      
		status = Status::IOError(name);                                           
	  }   
      } else {
         status = Status::Corruption(
	      "DB group do not exist while create_if missing is false");
      }
   } else if (options.error_if_exists == true) {
      status  = Status::Corruption(
		      "DB group do exist while error_if_exists is true");
   }
   if(!status.ok()) {
      delete impl;
      return status;
   }
   
   impl->max_sequence_number_ = 1;//TODO:we'd like to read it from some file.
   impl->logfile_number_ = 0;//TODO:we should preprocess the max log file..
    
   uint64_t new_log_number = impl->logfile_number_ + 1;
   WritableFile* lfile;
   status = options.env->NewWritableFile(
		   LogFileName(name,new_log_number),&lfile);
   if(status.ok()) {
      impl->logfile_number_ += 1;
      impl->logfile_ = lfile;
      impl->log_ = new log::Writer(lfile);
      *dbgoup_ptr = impl; 
   } else {
      delete impl;
   }
   
   return status;
}

} //namespace leveldb
