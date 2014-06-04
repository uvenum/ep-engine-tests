#include "gtest/gtest.h"
#include <sys/time.h>
#include <cstring>
#include <iostream>
#include <libcouchbase/couchbase.h>
#include <libcouchbase/durability.h>
#include <unistd.h>
#include <iterator>
#include "dataclient.c"

extern char *getvaluebuf;
extern long *getvaluesize;
extern lcb_datatype_t* getvaluedtype;
char **testargv;
char testargc;
struct timeval tim;
double t1;
extern std::vector<std::pair<std::string, double> > replatencies;
std::vector<double> storetime;
std::vector<double> netLatency;

class ReplatencyTest : public testing::Test {

protected:
   
   lcb_uint32_t tmo;
   const lcb_store_cmd_t *commands[1];
   lcb_error_t err;
   lcb_t instance;
   struct lcb_create_st create_opts;
  
   virtual void SetUp(){
   memset(&create_opts, 0, sizeof(create_opts));
   if (testargc > 0) {
        create_opts.v.v0.host = testargv[1];
    }
    if (testargc > 1) {
        create_opts.v.v0.user = testargv[2];
        create_opts.v.v0.bucket = testargv[2];
    }
    if (testargc > 2) {
        create_opts.v.v0.passwd = testargv[3];
    }
   err = lcb_create(&instance, &create_opts);
   EXPECT_EQ(err,LCB_SUCCESS);
   (void)lcb_set_error_callback(instance, error_callback);
   err = lcb_connect(instance);
   EXPECT_EQ(err,LCB_SUCCESS);
   (void)lcb_set_get_callback(instance, get_callback);
   (void)lcb_set_store_callback(instance, store_callback);
   (void)lcb_set_durability_callback(instance, durability_callback);   
       
   }


   virtual void TearDown(){
   lcb_destroy(instance);
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

   void sendHello(){
   
   fprintf(stderr, "\nInside HELLO\n"); 
   lcb_wait(instance);
   {
   err = lcb_hello(instance, NULL);
   EXPECT_EQ(err,LCB_SUCCESS);
   fprintf(stderr, "\nHELLO is sent\n"); 
   }
   
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
   EXPECT_EQ(err,LCB_SUCCESS);
   }
  lcb_wait(instance);
  callget(&instance, commands->v.v0.key, commands->v.v0.nkey);   
  lcb_wait(instance);
  fprintf(stderr, "\nInside DatatyepTester\n"); 
  compareDocs(commands);
  }
  
  //void statsLatency()

};

TEST_F(ReplatencyTest, repSingleTest) {
  const char inflated[] = "abc123";
  size_t inflated_len = strlen(inflated);

  for(uint64_t  i = 2000;i<2002 ;i++) {
  usleep(5000);
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
  lcb_store(instance, NULL, 1, &commands[0]);
  gettimeofday(&tim, NULL);   
  storetime.push_back((double) (tim.tv_sec*1000000+tim.tv_usec));
  lcb_durability_opts_t opts = { 0 };
  opts.v.v0.persist_to = 1;
  opts.v.v0.replicate_to = 1;
  lcb_durability_cmd_t endure = { 0 };
  const lcb_durability_cmd_t *cmdlist = &endure;
  endure.v.v0.key = myString.c_str();
  endure.v.v0.nkey = myString.size();
  lcb_durability_poll(instance, NULL, &opts, 1, &cmdlist);
  //callget(&instance, commands[0]->v.v0.key, commands[0]->v.v0.nkey);   
  lcb_wait(instance);
  }
  //lcb_wait(instance);
  //copy(replatencies.begin(), replatencies.end(), std::ostream_iterator<std::string, double>(std::cout, "\n"));
  usleep(50000);
  for(int i =0; i<replatencies.size();i++){
     std::cout << replatencies[i].first << ",callbacktime " << replatencies[i].second << ",storetime " << storetime[i] << ",latency " << replatencies[i].second-storetime[i]  << std::endl;
  }  
}


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  testargv = argv;
  testargc = argc;
  return RUN_ALL_TESTS();
}
