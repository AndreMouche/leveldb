// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "db/dbformat.h"
#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

std::string ParsedInternalKey::DebugString() const {
  char buf[50];
  snprintf(buf, sizeof(buf), "' @ %llu : %d",
           (unsigned long long) sequence,
           int(type));
  std::string result = "'";
  result += EscapeString(user_key.ToString());
  result += buf;
  return result;
}

std::string InternalKey::DebugString() const {
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    result = parsed.DebugString();
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

const char* InternalFilterPolicy::Name() const {
  return user_policy_->Name();
}

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);
  kstart_ = dst;
  memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

//Author:jingdong
LogSequenceItem::LogSequenceItem(const SequenceNumber sequence,
		                 const std::string filename,
				 const uint64_t offset,
				 const std::string dbname) {
 
  sequence_ = sequence;
  filename_ = filename;
  offset_ = offset;
  dbname_ = dbname;
}

//Author:jingdong
LogItemGroup::LogItemGroup(){
  while(!items_.empty()){
     items_.pop();
  }
}

//Author:jingdong
Status LogItemGroup::PutSequenceItem(const LogSequenceItem &item) {

  mutex_.Lock();	
  if(!items_.empty() && last_sequence_ > item.sequence_number()) {
	mutex_.Unlock();
	return Status::Corruption("LogItemGroup:Invalid sequenceNumber");
  }

  items_.push(item);
  last_sequence_ = item.sequence_number();
  //Check length..
  while(items_.size() > config::kMaxLogItemNumberInGroup) {
	  items_.pop();
  }

  mutex_.Unlock();
  return Status::OK();
}
 
//Author:jingdong    
bool LogItemGroup::Check(const SequenceNumber seq) {
  if(items_.empty()) {
	   return false;
  }
        
  if(seq <= last_sequence_ && seq >= items_.front().sequence_number()) {	    
	  // Should we check in the queue?
          // here we assume that the sequence_number will be 
	  // continuous in the queue...
	  return true;
  } 
  
  return false;
}


//Author:jingdong    
Status LogItemGroup::GetSequenceItem(const SequenceNumber seq,
				     LogSequenceItem *item) {
  mutex_.Lock();
  if(false == Check(seq)) {
	  mutex_.Unlock();
	  return Status::Corruption("Invalid SequenceNumber");
  }

  *item =items_.front();
  
  //Here we will remove all items in group which satisfy following
  //conditions:
  //sequence_number < seq
  //as if we donot drop the earliser items, it will make no sense when
  //there are two items which share the same key.
  while(!items_.empty() && (*item).sequence_number() < seq) {
	  items_.pop();
	  *item = items_.front();
  }

  //this may never happend as we 
  //assume the sequencenumber in the queue
  //will be continuous
  if(item->sequence_number() != seq) {
	mutex_.Unlock();
	return Status::Corruption("The Special SequenceNumber do not exits.");
  }
  mutex_.Unlock();
  return Status::OK();
}

//Author:jingdong    
SequenceNumber LogItemGroup::GetLastSequenceNumber() {
  if(true == items_.empty()){
	  return 0;
  }      
  return last_sequence_;
}

//Author:jingdong    
Status LogItemGroup::GetFrontItem(LogSequenceItem *item) {
  mutex_.Lock();
  if(items_.empty()) {
      mutex_.Unlock();	  
      return Status::Corruption("Empty Queue!");
  }
  *item = items_.front();
  mutex_.Unlock();
  return Status::OK();
}

//Author:jingdong    
Status LogItemGroup::PopEarlierItemBeforeOrEqual(const SequenceNumber seq){
   mutex_.Lock();
   while(!(items_.empty()) && items_.front().sequence_number() <= seq) {
	   items_.pop();
   } 
   mutex_.Unlock();
   return Status::OK();

}

}// namespace leveldb
