#include <iostream>
#include <memory>
#include <string>
#include <ctime>
#include <time.h>
#include <limits>
#include <chrono>

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

  void GetAttr(string path){
    ClientContext context;
    DeleteFileRequest request;
    DeleteFileReply reply;

    request.set_path(path);
    
    Status status = stub_->DeleteFile(&context, request, &reply);

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