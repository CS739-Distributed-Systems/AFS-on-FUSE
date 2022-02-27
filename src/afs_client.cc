#define FUSE_USE_VERSION 31

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
#include <dirent.h>
#include <stddef.h>
#include <stdio.h>
#include <fuse.h>
#include <grpcpp/grpcpp.h>
#include "afs.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace afs;
using namespace std;

static string cache_path = "/home/hemalkumar/hemal/client_cache_dir"; 

string getCachePath() {
  return cache_path;
}

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
    printf("Reached afs_client GetAttr: %s\n", path.c_str());
    GetAttrReply reply;
    ClientContext context;
    GetAttrRequest request;
    request.set_path(path);

    memset(output, 0, sizeof(struct stat));

    Status status = stub_->GetAttr(&context, request , &reply);

    if(reply.error() != 0){
      return -reply.error();
    }
    printf("Successful GetAttr: %s\n", path.c_str());
    output->st_ino = reply.inode();
    output->st_mode = reply.mode();
    output->st_nlink = reply.num_hlinks();
    output->st_uid = reply.user_id();
    output->st_gid = reply.groud_id();
    output->st_size = reply.size();
    output->st_blksize = reply.block_size();
    output->st_blocks = reply.blocks();
    output->st_atime = reply.last_acess_time();
    output->st_mtime = reply.last_mod_time();
    output->st_ctime = reply.last_stat_change_time();
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

    return -reply.error();
  }

  int DeleteDir(string path){
    ClientContext context;
    DeleteDirRequest request;
    DeleteDirReply reply;

    request.set_path(path);
    Status status = stub_->DeleteDir(&context, request, &reply);

    return -reply.error();
  }

  bool fileExisitsInsideCache(string path){
      return false; //TODO
  }

  int cacheFileLocally(string buffer, unsigned int size, const string path){
    int fd = open((cache_path + path).c_str(), O_WRONLY);
    if(fd == -1){
      perror(strerror(errno));
      return errno;
    }
    off_t offset = 0;
    int res = pwrite(fd, buffer.c_str(), size, offset);
    fsync(fd);
    if(res == -1){
      perror(strerror(errno));
      return errno;
    }
    if(fd>0)
      close(fd);
    return 0;    // TODO: check the error code
   }

  int Open(string path, struct fuse_file_info *fi) {
    // check if file exists inside cache and set fh to fi

    if(!fileExisitsInsideCache(path)) { 
    // fetch the file from the server and save it
      ClientContext context;
      OpenRequest request;
      OpenReply reply;

      request.set_path(path);
      request.set_flags(fi->flags);

      Status status = stub_->Open(&context, request, &reply);
      // check if status is false
      //if(status != Status::OK){
      //  return -1;
        //return reply->error(); //TODO: return proper error code
      //}
      cacheFileLocally(reply.buffer(), reply.size(), request.path());       // TODO: check name of the file
    }
    int fd = open((cache_path + string(path)).c_str(), fi->flags);
    if(fd == -1){
      perror(strerror(errno));
      return errno;
    }
    fi->fh = fd; 
    // read the file and set its fh to fi->fh and return
    return 0;
  }

  int Create(string path, struct fuse_file_info *fi, mode_t mode) {
      ClientContext context;
      CreateRequest request;
      CreateReply reply;

      request.set_path(path);
      request.set_mode(mode);
      request.set_flags(fi->flags);

      Status status = stub_->Create(&context, request, &reply);

      return reply.error();
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
