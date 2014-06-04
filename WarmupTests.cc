#include "gtest/gtest.h"
#include "stdlib.h"
#include <limits>
#include <cstring>
#include <iostream>
#include <vector>
#include <sstream>
#include <libcouchbase/couchbase.h>
#include "dataclient.c"

extern char *getvaluebuf;
extern long *getvaluesize;
extern bool warmupdone;
extern std::vector<std::string> threadstate;
char **testargv;
char testargc;

class WarmupTest : public testing::Test {

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
   (void)lcb_set_stat_callback(instance, stats_callback);
      
       
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
  lcb_uint32_t tmo;
  const lcb_store_cmd_t *commands[1];
  lcb_error_t err;
  lcb_t instance;
  struct lcb_create_st create_options;
};

TEST_F(WarmupTest, InsertItemsTest) {

/*tmo = 350000000;
lcb_cntl(instance, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &tmo);
FILE *file20MB;
char *membuffer;
long numbytes;
file20MB = fopen("out20M.dat","r");
fseek(file20MB,0L,SEEK_END);
numbytes=ftell(file20MB);
fprintf(stderr, "\nnumbytes %ld\n",numbytes); 
fseek(file20MB,0L,SEEK_SET);
membuffer = (char*)calloc(numbytes,sizeof(char));
if(membuffer==NULL) exit(EXIT_FAILURE);
fread(membuffer, sizeof(char),numbytes,file20MB);
fclose(file20MB);*/
const char inflated[] = "abc123";
size_t inflated_len = strlen(inflated);

for(uint64_t  i = 2000;i<std::numeric_limits<uint64_t>::max() ;i++) {
  lcb_store_cmd_t cmd;
  char buf[20];
  const lcb_store_cmd_t *commands[1];
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES;
  std::stringstream ss;
  ss << i;
  ss << "dexpire";
  std::string myString = ss.str();
  cmd.v.v0.key = myString.c_str();
  cmd.v.v0.nkey = myString.size();
  cmd.v.v0.bytes = inflated;
  cmd.v.v0.nbytes = inflated_len;
  cmd.v.v0.exptime = 0x9900;
  lcb_store(instance, NULL, 1, &commands[0]);  
  //callget(&instance, commands[0]->v.v0.key, commands[0]->v.v0.nkey);   
  lcb_wait(instance);
  }
}

TEST_F(WarmupTest, WarmupStatsTest) {
const char dummy = 'a';
lcb_store_cmd_t cmd;
const lcb_store_cmd_t *commands[1];
commands[0] = &cmd;
memset(&cmd, 0, sizeof(cmd));
cmd.v.v0.operation = LCB_SET;
cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES;
cmd.v.v0.key = "1expire";
cmd.v.v0.nkey = 7;
cmd.v.v0.bytes = &dummy;
cmd.v.v0.nbytes = 1;
cmd.v.v0.exptime = 0x99;
lcb_store(instance, NULL, 1, &commands[0]);  
lcb_wait(instance);
callget(&instance, commands[0]->v.v0.key, commands[0]->v.v0.nkey);   

/*
lcb_server_stats_cmd_t stats;
stats.version = 0;
stats.v.v0.name = "warmup";
stats.v.v0.nname = 6;
const lcb_server_stats_cmd_t *commandstat[1];
commandstat[0] = &stats;
lcb_server_stats(instance,NULL,1, &commandstat[0]);
lcb_wait(instance);
*/
}

TEST_F(WarmupTest, ep_warmup_stateTest) {

const char dummy = 'a';
lcb_store_cmd_t cmd;
const lcb_store_cmd_t *commands[1];
commands[0] = &cmd;
memset(&cmd, 0, sizeof(cmd));
cmd.v.v0.operation = LCB_SET;
cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES;
cmd.v.v0.key = "1";
cmd.v.v0.nkey = 1;
cmd.v.v0.bytes = &dummy;
cmd.v.v0.nbytes = 1;
lcb_store(instance, NULL, 1, &commands[0]);  
lcb_wait(instance);
char restart[] = "sudo /etc/init.d/couchbase-server restart";
exec(restart);
while(!warmupdone){
 lcb_server_stats_cmd_t stats;
 stats.version = 0;
 stats.v.v0.name = "warmup";
 stats.v.v0.nname = 6;
 const lcb_server_stats_cmd_t *commandstat[1];
 commandstat[0] = &stats;
 lcb_server_stats(instance,NULL,1, &commandstat[0]);
 lcb_wait(instance);
 }
std::vector<std::string> uuphases;
std::string prevstr = "";
for(std::vector<std::string>::iterator i = threadstate.begin();i!=threadstate.end();i++){
   if(prevstr.compare(*i)!=0) {
      uuphases.push_back(*i);  
      std::cout << *i << '\n';}
   prevstr = *i;
}
}

TEST_F(WarmupTest, AccessLogCheckTest) {
const char inflated[] = "abc123";
size_t inflated_len = strlen(inflated);
lcb_store_cmd_t cmd;
const lcb_store_cmd_t *commands[1];
commands[0] = &cmd;
memset(&cmd, 0, sizeof(cmd));
cmd.v.v0.operation = LCB_SET;
cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES;
cmd.v.v0.key = "fooaaa";
cmd.v.v0.nkey = 6;
cmd.v.v0.bytes = inflated;
cmd.v.v0.nbytes = inflated_len;
lcb_wait(instance);

lcb_server_stats_cmd_t stats;
stats.version = 0;
stats.v.v0.name = "warmup";
stats.v.v0.nname = 6;
const lcb_server_stats_cmd_t *commandstat[1];
commandstat[0] = &stats;
lcb_server_stats(instance,NULL,1, &commandstat[0]);
lcb_wait(instance);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  testargv = argv;
  testargc = argc;
  return RUN_ALL_TESTS();
}
