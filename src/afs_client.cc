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
    // printf("Successful GetAttr: %s\n", path.c_str());
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
    // cout<<output->st_mtime<<endl;
    // cout<<reply.last_mod_time()<<endl;
    // cout<<reply.last_acess_time()<<endl; // understand
    // cout<<reply.last_stat_change_time()<<endl;
    //printf("done\n");
 
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
    struct stat stbuf;
      memset(&stbuf, 0, sizeof(struct stat));
      std::string pathname = cache_path + path;
      cout<<"checking if file "<<pathname<<" exists in cahce"<<endl;
	    int res = lstat(pathname.c_str(), &stbuf);
      return res == 0;
  }

  int cacheFileLocally(string buffer, unsigned int size, const string path){
    cout<<__func__<<__LINE__<<endl;
    int fd = open((cache_path + path).c_str(), O_WRONLY);
    if(fd == -1){
      cout<<"open local failed"<<__func__<<endl;
      perror(strerror(errno));
      return errno;
    }
    cout<<__func__<<__LINE__<<endl;
    off_t offset = 0;
    int res = pwrite(fd, buffer.c_str(), size, offset);
    fsync(fd);
    cout<<__func__<<__LINE__<<endl;
    if(res == -1){
      cout<<"pwrte local failed"<<__func__<<endl;
      perror(strerror(errno));
      return errno;
    }
    cout<<__func__<<__LINE__<<endl;
    close(fd);
    return 0;    // TODO: check the error code
   }

  int Open(string path, struct fuse_file_info *fi) {
    // check if file exists inside cache and set fh to fi

    if(!fileExisitsInsideCache(path)) { 
      cout<<"file was not there in cache"<<endl;
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
      cout<<"client got bytes "<<reply.size()<<endl;
      cacheFileLocally(reply.buffer(), reply.size(), request.path());       // TODO: check name of the file
    }
    int fd = open((cache_path + string(path)).c_str(), fi->flags);
    if(fd == -1){
      cout<<"file open failed in client "<<__func__<<endl;
      perror(strerror(errno));
      return errno;
    }
    fi->fh = fd; 
    // read the file and set its fh to fi->fh and return
    return 0;
  }

  int Close(string path, struct fuse_file_info *fi){
      cout<<"close "<<__func__<<endl;
      ClientContext context;
      CloseRequest request;
      CloseReply reply;

      request.set_path(path);
      cout<<"close "<<__LINE__<<endl;

      ReadFileLocally(fi->fh, request);
      cout<<"close "<<__LINE__<<endl;
      cout<<"done buf as "<<request.buffer()<<endl;

      request.set_offset(0);
      Status status = stub_->Close(&context, request, &reply);
      
      return -reply.error();

  }

  void ReadFileLocally(uint64_t fd, CloseRequest &request){
    cout<<"reading file locally in client"<<endl;;
    struct stat s;
    if (fstat(fd, &s) == -1) {
      printf("fstat failed in cache\n");
      perror(strerror(errno));
      return;
    }
    off_t fsize = s.st_size;
    char *buf = new char[fsize];
    cout << "got cache size as :" << fsize << endl;
    off_t offset = 0;
    int res = pread(fd, buf, fsize, offset);
    if (res == -1){
      cout << "pread failed" << endl;
      perror(strerror(errno));
      return;
    }
    request.set_buffer(buf);
      request.set_size(fsize);
      cout<<"saved buf as "<<request.buffer()<<endl;
      
    free(buf);
}
  void ReadFileLocally1(uint64_t fd){
   
    cout<<__LINE__<<__func__<<endl;
      lseek(fd, 0, SEEK_SET);
      cout<<__LINE__<<__func__<<endl;
      int size = lseek(fd, (size_t)0, SEEK_END);
      cout<<"ERRR:::: "<<size<<endl;
      char *buf = new char[size];
      //TODO ERROR HONGE ISSE BOHOT BADE
      if(size ==0)
        return;
      cout<<__LINE__<<__func__<<endl;
      lseek(fd, 0, SEEK_SET);
      cout<<__LINE__<<__func__<<endl;
      cout << "FD: " << fd << endl;
      int res = pread(fd, buf, size, 0);
      if (res == -1){
        cout << "pread failed in " <<__func__<< endl;
        perror(strerror(errno));
      }
      free(buf);

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

  int DeleteFile(string path) {
      ClientContext context;
      DeleteFileRequest request;
      DeleteFileReply reply;

      request.set_path(path);

      Status status = stub_->DeleteFile(&context, request, &reply);

      return reply.error();
  }



 private:
  std::unique_ptr<AFS::Stub> stub_;
};
