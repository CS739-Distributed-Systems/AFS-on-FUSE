#include <iostream>
#include <memory>
#include <string>
#include <ctime>
#include <time.h>
#include <limits>
#include <chrono>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <grpcpp/grpcpp.h>
#include "afs.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace afs;
using namespace std;

class AFSClient {
 public:
  AFSClient(std::shared_ptr<Channel> channel)
      : stub_(AFS::NewStub(channel)) {}

  void DeleteFile(string path) {
    ClientContext context;
    DeleteFileRequest request;
    DeleteFileReply reply;

    request.set_path(path);
    
    Status status = stub_->DeleteFile(&context, request, &reply);

    if (status.ok()) {
      std::cout << "answer from server:" << reply.error() << std::endl;
    } else {
      std::cout << "SendInt rpc failed." << std::endl;
    }
    
    return;
  }

  int GetAttr(string path, struct stat* output){
    printf("Reached afs_client GetAttr\n");
    GetAttrReply reply;
    ClientContext context;
    GetAttrRequest request;
    request.set_path(path);

    memset(output, 0, sizeof(struct stat));

    Status status = stub_->GetAttr(&context, request , &reply);

    if(reply.error() != 0){
      return -reply.error();
    }

    output->st_ino = reply.inode();
    output->st_mode = reply.mode();
    // output->st_nlink = result.nlink();
    // output->st_uid = reply.uid();
    // output->st_gid = result.gid();
    // output->st_size = result.size();
    output->st_blksize = reply.block_size();
    output->st_blocks = reply.blocks();
    // output->st_atime = reply.
    output->st_mtime = reply.last_mod_time();
    // output->st_ctime = reply.st_ctim();
    cout<<output->st_mtime<<endl;
    cout<<reply.last_mod_time()<<endl;
    cout<<reply.last_acess_time()<<endl; // understand
    cout<<reply.last_stat_change_time()<<endl;
    printf("done\n");
    return 0;
  }


  int MakeDir(string path, mode_t mode){
    ClientContext context;
    MakeDirRequest request;
    MakeDirReply reply;

    request.set_path(path);
    request.set_mode(mode);
    Status status = stub_->MakeDir(&context, request, &reply);
  }

 private:
  std::unique_ptr<AFS::Stub> stub_;
};

int main1(int argc, char** argv) {
  std::string target_str;
  std::string arg_str("--target");
  if (argc > 1) {
    std::string arg_val = argv[1];
    size_t start_pos = arg_val.find(arg_str);
    if (start_pos != std::string::npos) {
      start_pos += arg_str.size();
      if (arg_val[start_pos] == '=') {
        target_str = arg_val.substr(start_pos + 1);
      } else {
        std::cout << "The only correct argument syntax is --target="
                  << std::endl;
        return 0;
      }
    } else {
      std::cout << "The only acceptable argument is --target=" << std::endl;
      return 0;
    }
  } else {
    target_str = "localhost:50051";
  }

  AFSClient afsClient(
      grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

  std::string fileName("junk file");
  afsClient.DeleteFile(fileName);

  return 0;
}

int test() {
  printf("hello bullshit!\n");
  return 40;
}
