#include "gtest/gtest.h"
#include <cstring>
#include <iostream>
#include <libcouchbase/couchbase.h>
#include "dataclient.c"

extern char *getvaluebuf;
extern long *getvaluesize;
extern lcb_datatype_t* getvaluedtype;
char **testargv;
char testargc;

class DatatypeTest : public testing::Test {

protected:
   
   lcb_uint32_t tmo;
   const lcb_store_cmd_t *commands[1];
   lcb_error_t err;
   lcb_t instance;
   struct lcb_create_st create_options;
  
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
   EXPECT_EQ(err,LCB_SUCCESS);
   (void)lcb_set_error_callback(instance, error_callback);
   err = lcb_connect(instance);
   EXPECT_EQ(err,LCB_SUCCESS);
   (void)lcb_set_get_callback(instance, get_callback);
   (void)lcb_set_store_callback(instance, store_callback);
      
       
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

};

TEST_F(DatatypeTest, rawData) {
  sendHello();
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
 // cmd.v.v0.cas = 527601;
  cmd.v.v0.bytes = inflated;
  cmd.v.v0.nbytes = inflated_len;
  cmd.v.v0.exptime = 0x2000; 
  DatatypeTester(commands[0]);
}

TEST_F(DatatypeTest, callGet) {
  sendHello();
  lcb_wait(instance);
  callget(&instance, "fooJSON",7);
  lcb_wait(instance);
  fprintf(stderr, "\ndatatype for key fooJSON is %d\n",*getvaluedtype); 
}

TEST_F(DatatypeTest, raw20M) {
  sendHello();
  tmo = 350000000; 
  lcb_cntl(instance, LCB_CNTL_SET, LCB_CNTL_OP_TIMEOUT, &tmo);
  FILE *file20MB;
  char *membuffer;
  long numbytes;
  file20MB = fopen("/root/Datatypetests/out20M.dat","r");
  fseek(file20MB,0L,SEEK_END);
  numbytes=ftell(file20MB);
  fseek(file20MB,0L,SEEK_SET);
  membuffer = (char*)calloc(numbytes,sizeof(char));
  if(membuffer==NULL) exit(EXIT_FAILURE);
  fread(membuffer, sizeof(char),numbytes,file20MB);
  fclose(file20MB); 
  lcb_store_cmd_t cmd;
  const lcb_store_cmd_t *commands[1];
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_RAW_BYTES; //DATATYPE_COMPRESSED;
  cmd.v.v0.key = "foo20M";
  cmd.v.v0.nkey = 6;
  cmd.v.v0.bytes = membuffer;
  cmd.v.v0.nbytes = numbytes; 
  DatatypeTester(commands[0]);
}

TEST_F(DatatypeTest, rawCompressedData) {
  sendHello();
  const char inflated[] = "gggggggggghhhhhhhhhhhh";
  size_t inflated_len = strlen(inflated);
  char deflated[256];
  size_t deflated_len = 256;
  snappy_status status;
  status = snappy_compress(inflated, inflated_len,
                            deflated, &deflated_len);
  EXPECT_EQ(0,status);
  lcb_store_cmd_t cmd;
  const lcb_store_cmd_t *commands[1];
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_DATATYPE_COMPRESSED;
  cmd.v.v0.key = "fooccc";
  cmd.v.v0.nkey = 6;
  cmd.v.v0.bytes = deflated;
  cmd.v.v0.nbytes = deflated_len; 
  DatatypeTester(commands[0]);
}

TEST_F(DatatypeTest, jsonData) {
  sendHello();
  FILE *fileJSON;
  char *membuffer;
  long numbytes;
  fileJSON = fopen("/root/Datatypetests/buzz.json","r");
  fseek(fileJSON,0L,SEEK_END);
  numbytes=ftell(fileJSON);
  fseek(fileJSON,0L,SEEK_SET);
  membuffer = (char*)calloc(numbytes,sizeof(char));
  if(membuffer==NULL) exit(EXIT_FAILURE);
  fread(membuffer, sizeof(char),numbytes,fileJSON);
  fclose(fileJSON); 
  //command to store
  lcb_store_cmd_t cmd;
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_DATATYPE_JSON;
  cmd.v.v0.key = "fooJSON";
  cmd.v.v0.nkey = 7;
  cmd.v.v0.bytes = membuffer;
  cmd.v.v0.nbytes = numbytes;
  DatatypeTester(commands[0]);
}

TEST_F(DatatypeTest, jsonCompressedData) {
  sendHello();
  FILE *fileJSON;
  char *membuffer;
  long numbytes;
  char* deflated;
  size_t deflated_len = 2560;
  snappy_status status;
  fileJSON = fopen("/root/Datatypetests/buzz.json","r");
  fseek(fileJSON,0L,SEEK_END);
  numbytes=ftell(fileJSON);
  fseek(fileJSON,0L,SEEK_SET);
  membuffer = (char*)calloc(numbytes,sizeof(char));
  if(membuffer==NULL) exit(EXIT_FAILURE);
  fread(membuffer, sizeof(char),numbytes,fileJSON);
  fclose(fileJSON);
  deflated = (char*)calloc(numbytes,sizeof(char));
  status = snappy_compress(membuffer, numbytes,
                            deflated, &deflated_len);
  //command to store
  lcb_store_cmd_t cmd;
  commands[0] = &cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.v.v0.operation = LCB_SET;
  cmd.v.v0.datatype = LCB_BINARY_DATATYPE_COMPRESSED_JSON;
  cmd.v.v0.key = "fooJSONC";
  cmd.v.v0.nkey = 8;
  cmd.v.v0.bytes = deflated;
  cmd.v.v0.nbytes = deflated_len;
  DatatypeTester(commands[0]);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  testargv = argv;
  testargc = argc;
  return RUN_ALL_TESTS();
}
