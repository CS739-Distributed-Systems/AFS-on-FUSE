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
#include <fcntl.h>
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientReader;
using grpc::ClientWriter;
using namespace afs;
using namespace std;

#define BUF_SIZE 10

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
    cout<<"checking if file "<<pathname<<" exists in cache"<<endl;
	  int res = lstat(pathname.c_str(), &stbuf);
    return res == 0;
  }

  int cacheFileLocally(string buffer, unsigned int size, const string path)
  {
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
      fetchFileAndUpdateCache(path, fi);
    } else {
      struct stat result;
      std::string path_in_cache = getCachePath() + path;
      int statRes = stat(path_in_cache.c_str(), &result); 
      if(statRes!=0)
        cout<<"stat failed"<<statRes<<"path "<<path<<endl;
      else {
        auto cache_mod_time = result.st_mtime;
        auto server_mod_time = getLastModTimeFromServer(path);
        if (server_mod_time!=-1 && server_mod_time > cache_mod_time){
          cout<<"stale cache, fetching file from server"<<endl;
          fetchFileAndUpdateCache(path, fi);
        }
      }
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

  int OpenStream(string path, struct fuse_file_info *fi) {
    // check if file exists inside cache and set fh to fi
    if(!fileExisitsInsideCache(path)) { 
      cout<<"file was not there in cache"<<endl;
      fetchFileAndUpdateCache_stream(path, fi);
    } else {
      struct stat result;
      std::string path_in_cache = getCachePath() + path;
      int statRes = stat(path_in_cache.c_str(), &result); 
      if(statRes!=0)
        cout<<"stat failed"<<statRes<<"path "<<path<<endl;
      else {
        auto cache_mod_time = result.st_mtime;
        auto server_mod_time = getLastModTimeFromServer(path);
        if (server_mod_time!=-1 && server_mod_time > cache_mod_time){
          cout<<"stale cache, fetching file from server"<<endl;
          fetchFileAndUpdateCache_stream(path, fi);
        }
      }
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
      int _fd = open((getCachePath() + string(path)).c_str(), O_RDONLY);
      ReadFileLocally(_fd, request);
      cout<<"close "<<__LINE__<<endl;
      cout<<"done buf as "<<request.buffer()<<endl;

      request.set_offset(0);
      Status status = stub_->Close(&context, request, &reply);
      
      return -reply.error();

  }

  int CloseStream(string path, struct fuse_file_info *fi){
    int fd = open((getCachePath() + path).c_str(), O_RDONLY);
    if (fd == -1){
      cerr << "Close stream-client: could not open file:" << strerror(errno) << endl;
      return errno;
    }

    ClientContext context;
    CloseRequest request;
    CloseReply reply;

    request.set_path(path);
    std::unique_ptr<ClientWriter<CloseRequest> > writer(
        stub_->CloseStream(&context, &reply));

    char *buf = new char[BUF_SIZE];

    while(1) {
      int bytesRead = read(fd, buf, BUF_SIZE);

      // done with the reading
      if (bytesRead == 0) {
        break;
      }

      // error with reading, return -1
      if (bytesRead == -1) {
        cerr << "client read error while reading op - err:" << errno << endl;
        return -1;
      }

      request.set_buffer(buf);
      request.set_size(bytesRead);

      if (!writer->Write(request)) {
        cerr << "stream broke:" << errno << endl;
        return -1;
      }

      cout << "client sent bytes:" << bytesRead << endl;
    }
    
    writer->WritesDone();
    Status status = writer->Finish();

    cout<<"client had fd = "<<fd<<endl;
    close(fd);
    free(buf);

    // TODO: check status and retry operation if required
      
    return -1;
  }

  void ReadFileLocally(int fd, CloseRequest &request){
    cout<<"reading file locally in client"<<endl;;
    struct stat s;
    if (fstat(fd, &s) != 0) {
      printf("fstat failed in cache\n");
      perror(strerror(errno));
      return;
    }
    off_t fsize = s.st_size;
   // fsize=1;
    char *buf = new char[fsize+1];
    buf[fsize] = '\0';
    cout << "got cache size as :" << fsize << endl;
    cout<<"fd is "<<fd<<endl;
    off_t offset = 0;
    // read returns -1 for error. Number for bytes for successful read op
    int res = read(fd, buf, fsize);
    if (res == -1){
      cout << "read failed" << endl;
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

  int getLastModTimeFromServer(string path){
    GetAttrReply reply;
    ClientContext context;
    GetAttrRequest request;
    request.set_path(path);
    struct stat output;
    memset(&output, 0, sizeof(struct stat));

    Status status = stub_->GetAttr(&context, request , &reply);

    if(reply.error() != 0){
      return -1;
    }
    return reply.last_mod_time();
  }

  int fetchFileAndUpdateCache_stream(string path, struct fuse_file_info *fi) {
      // open file locally in write mode. If this fails then don't contact the server
      int fd = open((getCachePath() + path).c_str(), O_WRONLY);
      if(fd == -1){
        cout<<"open local failed"<<__func__<<endl;
        perror(strerror(errno));
        // TODO: return errorno and honor it from the caller
        return errno;
      }

      ClientContext context;
      OpenRequest request;
      OpenReply reply;
      
      request.set_path(path);
      request.set_flags(fi->flags);

      std::unique_ptr<ClientReader<OpenReply> > reader(
        stub_->OpenStream(&context, request));

      while (reader->Read(&reply)) {
        // write to file
        if (reply.error() != 0) {
          // TODO: retry operation
          // clean up and retry operation
          return -1;
        }

        int res = write(fd, reply.buffer().c_str(), reply.size());
        if (res == -1) {
          cerr << "write failed with err: " << errno << endl;
          // TODO: retry operation
          // clean up and retry operation
          return -1;
        }
      }

      Status status = reader->Finish();
      // TODO: check if the read from server finished or not. 
      // TODO: If not, then retry the whole operation

      fsync(fd);
      close(fd);

      return 0;
  }

  void fetchFileAndUpdateCache(string path, struct fuse_file_info *fi){
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
    cacheFileLocally(reply.buffer(), reply.size(), request.path());
    cout<<"client got bytes "<<reply.size()<<endl;      // TODO: check name of the file
  }

 private:
  std::unique_ptr<AFS::Stub> stub_;
};
