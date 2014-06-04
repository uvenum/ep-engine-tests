#include "gtest/gtest.h"
#include "internal.h"
#include <unistd.h>
#include <netinet/in.h>
#include "libcouchstore/couch_db.h"
#include "libcouchstore/couch_common.h"
#include "libcouchstore/couch_index.h"
#include "libcouchstore/error.h"
#include <cstring>
#include <iostream>
#include <libcouchbase/couchbase.h>
#include "dataclient.c"

extern char *getvaluebuf;
extern long *getvaluesize;
char **testargv;
char testargc;

class CompactionTest : public testing::Test {

protected:

   virtual void SetUp(){
   memset(&create_options, 0, sizeof(create_options));
   if (testargc > 0) {
        create_options.v.v0.host = testargv[1];
    }
    if (testargc > 1) {
        create_options.v.v0.user = testargv[2];
        create_options.v.v0.bucket = testargv[2];
    }
    if (testargc > 2) {
        create_options.v.v0.passwd = testargv[3];
    }
   err = lcb_create(&instance, &create_options);
   assert(err==LCB_SUCCESS);
   (void)lcb_set_error_callback(instance, error_callback);
   err = lcb_connect(instance);
   assert(err==LCB_SUCCESS);
   (void)lcb_set_get_callback(instance, get_callback);
   (void)lcb_set_store_callback(instance, store_callback);
      
       
   }


   virtual void TearDown(){
   lcb_destroy(instance);
   }
   void sendHello(){
   
   lcb_wait(instance);
   {
   err = lcb_hello(instance, NULL);
   assert(err==LCB_SUCCESS);
   }
   
   }
    
   void sendcompact(uint16_t vbid, uint64_t purge_before_ts, uint64_t purge_before_seq, uint8_t drop_deletes){
   
   lcb_wait(instance);
   {
   err = lcb_compact(instance, NULL,vbid,purge_before_ts,purge_before_seq,drop_deletes);
   assert(err==LCB_SUCCESS);
   }
   }
   
   std::string exec(char* cmd) {
    FILE* pipe = popen(cmd, "r");
    if (!pipe) return "ERROR";
    char buffer[128];
    std::string result = "";
    while(!feof(pipe)) {
    	if(fgets(buffer, 128, pipe) != NULL)
    		result += buffer;
    }
    pclose(pipe);
    return result;
   }
 
   void compareDocs(const lcb_store_cmd_t *commands){
   EXPECT_EQ(*getvaluedtype,commands->v.v0.datatype);
   EXPECT_EQ(*getvaluesize,commands->v.v0.nbytes);
   for (long i=0; i<*getvaluesize;i++){
       EXPECT_EQ(*(getvaluebuf+i),*((char*)commands->v.v0.bytes+i)); 
       } 
   }
   
  void DatatypeTester(const lcb_store_cmd_t *commands) {
  lcb_wait(instance);
   {
   err = lcb_store(instance, NULL, 1, &commands);
   assert(err==LCB_SUCCESS);
   }
  lcb_wait(instance);
  callget(&instance, commands->v.v0.key, commands->v.v0.nkey);   
  lcb_wait(instance);
  fprintf(stderr, "\nInside DatatypeTester\n"); 
  compareDocs(commands);
  }
  
 void storekey(std::string& myString) {
  lcb_store_cmd_t cmd;
  const lcb_store_cmd_t *commands[1];
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.key = myString.c_str();
  cmd.v.v0.nkey = myString.size();
  const char inflated[] = "abc123";
  size_t inflated_len = strlen(inflated);
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES;
  cmd.v.v0.bytes = inflated;
  cmd.v.v0.nbytes = inflated_len;
  cmd.v.v0.exptime = 0x5;
  lcb_store(instance, NULL, 1, &commands[0]);
  lcb_wait(instance);
 }

  lcb_uint32_t tmo;
  const lcb_store_cmd_t *commands[1];
  lcb_error_t err;
  lcb_t instance;
  struct lcb_create_st create_options;
  std::vector<std::string> vb14keylist;
};


TEST_F(CompactionTest, InitTest) {
  sendHello();
  std::stringstream ss;
  ss << "fooaaa";
  std::string myString = ss.str();
  for(int  i = 0;i<100000;i++) {
   storekey(myString);
  }
  sleep(30);
  fprintf(stderr, "\nperformed 100000 mutations on vbucket 14\n"); 
    
}

TEST_F(CompactionTest, SizeReductionTest) {
  couchstore_error_t error; 
  //structs to hold vB file information
  Db* db; 
  DbInfo* dbinfo;
  db = (Db*)malloc(sizeof(Db));
  dbinfo = (DbInfo*)malloc(sizeof(DbInfo));
  
  //parameters for LCB_COMPACT command
  uint16_t vbid = 14;
  uint64_t purge_before_ts = 0;  
  uint64_t purge_before_seq =0;
  uint8_t drop_deletes = 0;

  //file sizes before and after compaction
  uint64_t fsize_b4compact =0;
  uint64_t fsize_a4trcompact =0;

  const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.1";

  //open vB file
  error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
  fprintf(stderr, "\nopendb error %d",error);
  //get vB info 
  error = couchstore_db_info(db, dbinfo);
  fprintf(stderr, "\ndbinfo error %d",error); 
  fprintf(stderr, "\nDocinfo file size used  %lu bytes", dbinfo->file_size); 
  fsize_b4compact = dbinfo->file_size;
  //close db handles
  couchstore_close_db(db);
  //send lcb_compact command
  sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
  lcb_wait(instance);

  //open vB file with new rev number 
  filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 
  error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
  fprintf(stderr, "\nopendb error %d",error); 
  error = couchstore_db_info(db, dbinfo);
  fprintf(stderr, "\ndbinfo error %d",error); 
  fprintf(stderr, "\nDocinfo filename %s", dbinfo->filename); 
  fprintf(stderr, "\nDocinfo doc count  %lu \n", dbinfo->doc_count); 
  fprintf(stderr, "\nDocinfo file size used  %lu bytes", dbinfo->file_size); 
  fprintf(stderr, "\nDocinfo deleted count  %lu", dbinfo->deleted_count); 
  fsize_a4trcompact = dbinfo->file_size;
  //assert the expected file sizes
  EXPECT_LT(fsize_a4trcompact,0.01*fsize_b4compact);
}

TEST_F(CompactionTest, ExpiredItemPurgeTest){
  sendHello();
  couchstore_error_t error; 
  int numitems = 0;
  int key = 1;

  //Store N items with short TTL in the same vBucket
  while(numitems<10) {
  
   std::stringstream ss;
   ss << key;
   std::string myString = ss.str();
   int vb =  getvbucketbykey(instance, (const void*) myString.c_str(), myString.size()); 
   if(vb==15) {
     //fprintf(stderr, "\nvb:  %d  key: %s\n",vb,myString.c_str());
     //vb14keylist.push_back(myString);
     storekey(myString);
     numitems++; 
   }
   key++;
   } 
    
   fprintf(stderr, "\nsleeping for 30 seconds");
   sleep(30);
   //structs to hold vB file information
   Db* db; 
   DbInfo* dbinfo;
   db = (Db*)malloc(sizeof(Db));
   dbinfo = (DbInfo*)malloc(sizeof(DbInfo));
  
   //parameters for LCB_COMPACT command
   uint16_t vbid = 15;
   uint64_t purge_before_ts = 0;  
   uint64_t purge_before_seq =0;
   uint8_t drop_deletes = 0;

   //file sizes before and after compaction
   uint64_t fsize_b4compact =0;
   uint64_t fsize_a4trcompact =0;

   const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/15.couch.1";

   //open vB file
   error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
   fprintf(stderr, "\nopendb error %d",error);
   //get vB info 
   error = couchstore_db_info(db, dbinfo);
   fprintf(stderr, "\ndbinfo error %d",error); 
   fprintf(stderr, "\nDocinfo file size used  %lu bytes", dbinfo->file_size); 
   fsize_b4compact = dbinfo->file_size;
   
   //no documents should be deleted prior to compaction
   EXPECT_EQ(0,dbinfo->deleted_count);
   
   //close db handles
   couchstore_close_db(db);
   //send lcb_compact command
   sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
   lcb_wait(instance);

   fprintf(stderr, "\nsleeping for 60 seconds");
   sleep(120);
   //open vB file with new rev number 
   filename = "/opt/couchbase/var/lib/couchbase/data/default/15.couch.2"; 
   error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
   fprintf(stderr, "\nopendb error %d",error); 
   error = couchstore_db_info(db, dbinfo);
   fprintf(stderr, "\ndbinfo error %d",error); 
   fprintf(stderr, "\nDocinfo filename %s", dbinfo->filename); 
   fprintf(stderr, "\nDocinfo doc count  %lu \n", dbinfo->doc_count); 
   fprintf(stderr, "\nDocinfo file size used  %lu bytes", dbinfo->file_size); 
   fprintf(stderr, "\nDocinfo deleted count  %lu", dbinfo->deleted_count); 
   fsize_a4trcompact = dbinfo->file_size;
   //11 documents should be deleted post compaction
   EXPECT_EQ(10,dbinfo->deleted_count);
   
}


TEST_F(CompactionTest, DropDeletesFalseTest) {
  
  couchstore_error_t error; 
  Db* db1; 
  Db* db2; 
  DbInfo* dbinfo1;
  DbInfo* dbinfo2;
  DocInfo* docinfo1;
  DocInfo* docinfo2;
  
  db1 = (Db*)malloc(sizeof(Db));
  db2 = (Db*)malloc(sizeof(Db));
  dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
  dbinfo2 = (DbInfo*)malloc(sizeof(DbInfo));
  docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
  docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));
  //get the expiry time of a doc
  uint32_t exptime;
  const char* key = "fooaaa"; 
  uint16_t vbid = 14;
  uint64_t fsize_b4compact =0;
  uint64_t fsize_a4trcompact =0;
  const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.2"; 

  error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
  fprintf(stderr, "\nopendb error %d ",error); 
  error = couchstore_docinfo_by_id(db1,(void*)key,6,&docinfo1);
  EXPECT_EQ(0,error);
  fprintf(stderr, "\ndocinfo by id error %d ",error); 
  fprintf(stderr, "\ndocinfo deleted %d ",docinfo1->deleted); 
  fprintf(stderr, "\ndocinfo doc size %lu ",docinfo1->size);

  //read the document expiry time and decrement it 
  memcpy(&exptime, docinfo1->rev_meta.buf + 8, 4);
  exptime = ntohl(exptime);
  exptime = exptime -100;
 
  error= couchstore_db_info(db1, dbinfo1);
  fprintf(stderr, "\ndbinfo error %d ",error); 
  fprintf(stderr, "\nDocinfo file size used  %lu ", dbinfo1->file_size); 
  fsize_b4compact = dbinfo1->file_size;
  couchstore_close_db(db1);
  //set compact_cmd parameters 
  uint64_t purge_before_ts = exptime;  
  uint64_t purge_before_seq =0;
  uint8_t drop_deletes = 1;
  
  sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
  lcb_wait(instance);

  filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 
  error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
  fprintf(stderr, "\nopendb error %d ",error); 
  error = couchstore_docinfo_by_id(db2, (void*)key,6,&docinfo2);
  fprintf(stderr, "\ndocinfo by id error %d ",error); 
  fprintf(stderr, "\ndocinfo deleted %d ",docinfo2->deleted); 
  fprintf(stderr, "\ndocinfo doc size %lu ",docinfo2->size); 
  EXPECT_EQ(0,error);
  error = couchstore_db_info(db2, dbinfo2);
  fprintf(stderr, "\ndbinfo error %d ",error); 
  fprintf(stderr, "\nDocinfo filename %s ", dbinfo2->filename); 
  fprintf(stderr, "\nDocinfo doc count  %lu ", dbinfo2->doc_count); 
  fprintf(stderr, "\nDocinfo file size used  %lu ", dbinfo2->file_size); 
  fprintf(stderr, "\nDocinfo deleted count  %lu ", dbinfo2->deleted_count); 
  fprintf(stderr, "\nDocinfo last seq no  %lu ", dbinfo2->last_sequence); 
  fprintf(stderr, "\nDocinfo purge seq number  %lu ", dbinfo2->purge_seq); 
  fsize_a4trcompact = dbinfo2->file_size;
  EXPECT_EQ(1,dbinfo2->deleted_count);
}


TEST_F(CompactionTest, DropDeletesTrueTest) {
  
  couchstore_error_t error; 
  Db* db1; 
  Db* db2; 
  DbInfo* dbinfo1;
  DbInfo* dbinfo2;
  DocInfo* docinfo1;
  DocInfo* docinfo2;
  
  db1 = (Db*)malloc(sizeof(Db));
  db2 = (Db*)malloc(sizeof(Db));
  dbinfo1 = (DbInfo*)malloc(sizeof(DbInfo));
  dbinfo2 = (DbInfo*)malloc(sizeof(DbInfo));
  docinfo1 = (DocInfo*)malloc(sizeof(DocInfo));
  docinfo2 = (DocInfo*)malloc(sizeof(DocInfo));
  //get the expiry time of a doc
  uint32_t exptime;
  const char* key = "fooaaa"; 
  uint16_t vbid = 14;
  uint64_t fsize_b4compact =0;
  uint64_t fsize_a4trcompact =0;
  const char *filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.3"; 

  error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db1);
  fprintf(stderr, "\nopendb error %d ",error); 
  error = couchstore_docinfo_by_id(db1,(void*)key,6,&docinfo1);
  EXPECT_EQ(0,error);
  fprintf(stderr, "\ndocinfo by id error %d ",error); 
  fprintf(stderr, "\ndocinfo deleted %d ",docinfo1->deleted); 
  fprintf(stderr, "\ndocinfo doc size %lu ",docinfo1->size);

  //read the document expiry time and increment it 
  memcpy(&exptime, docinfo1->rev_meta.buf + 8, 4);
  exptime = ntohl(exptime);
  exptime = exptime+100;
 
  error= couchstore_db_info(db1, dbinfo1);
  fprintf(stderr, "\ndbinfo error %d \n",error); 
  fprintf(stderr, "\nDocinfo file size used  %lu \n", dbinfo1->file_size); 
  fsize_b4compact = dbinfo1->file_size;
  couchstore_close_db(db1);
  //set compact_cmd parameters 
  uint64_t purge_before_ts = exptime;  
  uint64_t purge_before_seq =0;
  uint8_t drop_deletes = 1;
  
  sendcompact(vbid, purge_before_ts, purge_before_seq, drop_deletes);  
  lcb_wait(instance);

  filename = "/opt/couchbase/var/lib/couchbase/data/default/14.couch.4"; 
  error = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &db2);
  fprintf(stderr, "\nopendb error %d ",error); 
  error = couchstore_docinfo_by_id(db2, (void*)key,6,&docinfo2);
  fprintf(stderr, "\ndocinfo by id error %d ",error); 
  fprintf(stderr, "\ndocinfo deleted %d ",docinfo2->deleted); 
  fprintf(stderr, "\ndocinfo doc size %lu ",docinfo2->size); 
  EXPECT_EQ(-5,error);
  error = couchstore_db_info(db2, dbinfo2);
  fprintf(stderr, "\ndbinfo error %d ",error); 
  fprintf(stderr, "\ndb filename %s ", dbinfo2->filename); 
  fprintf(stderr, "\ndbinfo doc count  %lu ", dbinfo2->doc_count); 
  fprintf(stderr, "\ndbinfo file size used  %lu ", dbinfo2->file_size); 
  fprintf(stderr, "\ndbinfo deleted count  %lu ", dbinfo2->deleted_count); 
  fprintf(stderr, "\ndbinfo last seq no  %lu ", dbinfo2->last_sequence); 
  fprintf(stderr, "\ndbinfo purge seq number  %lu", dbinfo2->purge_seq); 
  fsize_a4trcompact = dbinfo2->file_size;
  EXPECT_EQ(0,dbinfo2->deleted_count);

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  testargv = argv;
  testargc = argc;
  return RUN_ALL_TESTS();

}
