#define FUSE_USE_VERSION 31

#include <iostream>
#include <memory>
#include <string>
#include <sys/sendfile.h>  // sendfile
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
#include <thread>
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientReader;
using grpc::ClientWriter;
using namespace afs;
using namespace std;

#define BUF_SIZE 10
#define RETRY_INTERVAL 100
#define MAX_RETRIES 5

static string cache_path = "/home/hemalkumar/reetu/client_cache_dir";

string getCachePath() {
  return cache_path;
}

string getTempPath(string path) {
  return cache_path + path + ".tmp";
}

string generateTempPath(string path) {
  return cache_path + path + ".tmp" + to_string(rand() % 101743);
}

class AFSClient {
 public:
  AFSClient(std::shared_ptr<Channel> channel)
      : stub_(AFS::NewStub(channel)) {
        fd_tmp_file_map.clear();      
      }

  int GetAttr(string path, struct stat* output){
    //printf("Reached afs_client GetAttr: %s\n", path.c_str());
    GetAttrRequest request;
    request.set_path(path);
    GetAttrReply reply;
    reply.set_error(-1);

    memset(output, 0, sizeof(struct stat));
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;
    Status status;
    do{
      ClientContext context;
      status = stub_->GetAttr(&context, request , &reply);
    } while (reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

    if(reply.error() != 0){
      cout<<"ERR: GetAttr failed from server"<<endl;
      return -reply.error();
    }

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
    return 0;
  }

  int MakeDir(string path, mode_t mode){
    MakeDirReply reply;
    reply.set_error(-1);
    MakeDirRequest request;
    request.set_path(path);
    request.set_mode(mode);
    Status status;
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;
    do{
      ClientContext context;
      status = stub_->MakeDir(&context, request, &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));
    
    return -reply.error();
  }

  int DeleteDir(string path){
    DeleteDirRequest request;
    request.set_path(path);
    DeleteDirReply reply;
    reply.set_error(-1);
    Status status;
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;
    do{
      ClientContext context;
      status = stub_->DeleteDir(&context, request, &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

    return -reply.error();
  }

  bool fileExisitsInsideCache(string path){
    struct stat stbuf;
    memset(&stbuf, 0, sizeof(struct stat));
    std::string pathname = cache_path + path;
    int res = lstat(pathname.c_str(), &stbuf);
    cout<<"fileExistsInCache for "<<pathname<<" returns "<<(res == 0)<<endl;
    return res == 0;
  }

  int cacheFileLocally(string buffer, unsigned int size, const string path, struct fuse_file_info *fi)
  {
    //int fd = open((cache_path + path).c_str(), O_WRONLY | O_CREAT | O_TRUNC);
    cout<<"Inside function: "<<__func__<<endl;
    int fd = open((cache_path + path).c_str(), fi->flags | O_CREAT | O_WRONLY);
    if(fd == -1){
      cout<<"ERR: open failed on cache file "<<(cache_path + path)<<endl;
      perror(strerror(errno));
      return errno;
    }
    off_t offset = 0;
    int res = pwrite(fd, buffer.c_str(), size, offset);
    fsync(fd);
    if(res == -1){
      cout<<"ERR: pwrite failed on cache file "<<(cache_path + path)<<endl;
      perror(strerror(errno));
      return errno;
    }
    close(fd);
    return 0;    // TODO: check the error code
  }

  int Open(string path, struct fuse_file_info *fi) {
    // check if file exists inside cache and set fh to fi
    if(!fileExisitsInsideCache(path)) { 
      cout<<"file was not present in cache"<<endl;
      fetchFileAndUpdateCache(path, fi);
    } else {
      cout<<"file was present in cache"<<endl;
      struct stat result;
      std::string path_in_cache = getCachePath() + path;
      int statRes = stat(path_in_cache.c_str(), &result); 
      if(statRes!=0)
        cout<<"ERR: stat failed"<<statRes<<"path "<<path<<endl;
      else {
        auto cache_mod_time = result.st_mtime;
        auto server_mod_time = getLastModTimeFromServer(path);
        if (server_mod_time!=-1 && server_mod_time > cache_mod_time){
          cout<<"stale cache, fetching file from server"<<endl;
          fetchFileAndUpdateCache(path, fi);
        }
      }
    }
    // assuming that until this point the file exists in the client FS cache
    // each client creates a temporary copy of this file to work upon 
    
    fi->fh = createTemporaryFile(path, fi);
    cout<<"Open set fh of temp file as "<<fi->fh<<endl;
    return 0;
  }

  int createTemporaryFile(string path, struct fuse_file_info *fi, mode_t mode = -1){
    // creates a temporary file for the client to work upon instead of main file

    int source_file_fd = open((cache_path + string(path)).c_str(), O_RDONLY, 0644);
    string tmp_file_name = generateTempPath(string(path)); 
    cout<<"Creating temp file with name "<<tmp_file_name<<endl;
    int dest_file_fd;
    if(mode == -1)
	  dest_file_fd  = open(tmp_file_name.c_str(), O_RDWR | O_CREAT, 0644);
    else
	  dest_file_fd = open(tmp_file_name.c_str(), fi -> flags, mode);
    
    if(source_file_fd == -1 || dest_file_fd == -1){
      cout<<"ERR: file open failed for cacheFile/temporaryFile "<<__func__<<endl;
      cout<<source_file_fd<<", "<<dest_file_fd<<endl;
      perror(strerror(errno));
      return errno;
    }
    struct stat stat_source;
    fstat(source_file_fd, &stat_source);
    
    off_t fsize = stat_source.st_size;
    cout << "source file size:" << fsize << endl;
    char *buf = new char[fsize];
    off_t offset = 0;
    int res = pread(source_file_fd, buf, fsize, offset);
    if (res == -1){
      cout << "ERR: pread of file failed" <<(cache_path + string(path))<< endl;
      perror(strerror(errno));
    }
    int num_bytes = pwrite(dest_file_fd, buf, fsize, offset);
    cout<<"Temp file creation passed, size = " <<num_bytes<<endl; 
    if (res == -1){
      cout << "ERR: pwrite of file failed" << (cache_path + string(path))<<endl;
      perror(strerror(errno));
    }
    close(source_file_fd);
    close(dest_file_fd);
    dest_file_fd  = open(tmp_file_name.c_str(), O_RDWR, 0644);
    if (dest_file_fd == -1){
      cout << "ERR: open of file failed" <<tmp_file_name << endl;
      perror(strerror(errno));
    }
    cout<<"dest file fd was "<<dest_file_fd<<endl;
    if(fd_tmp_file_map.find(dest_file_fd) == fd_tmp_file_map.end()){
	  cout<<"::::::: inserting inside map "<<dest_file_fd<<" " << tmp_file_name<<endl;
    	fd_tmp_file_map.insert({dest_file_fd, tmp_file_name});
    } else{
        fd_tmp_file_map[dest_file_fd] = tmp_file_name;
    }
    free(buf);
    return dest_file_fd;
  }

  int OpenStream(string path, struct fuse_file_info *fi) {
    // check if file exists inside cache and set fh to fi
    int numberOfRetries=0;
    int res;
    if(!fileExisitsInsideCache(path)) { 
      cout<<"file was not there in cache"<<endl;
      do {
        res = fetchFileAndUpdateCache_stream(path, fi);
        numberOfRetries++;
      } while(res!=0 && numberOfRetries<MAX_RETRIES);
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
          do {
            res = fetchFileAndUpdateCache_stream(path, fi);
            numberOfRetries++;
          } while(res!=0 && numberOfRetries<MAX_RETRIES);
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
  
  int ReadDir(string p, void *buf, fuse_fill_dir_t filler){
      ClientContext context;
      ReadDirRequest request;
      ReadDirReply reply;
      reply.set_error(-1);
      dirent de;
      request.set_path(p);

      std::unique_ptr<ClientReader<ReadDirReply> >reader(
            stub_->ReadDir(&context, request));
        while(reader->Read(&reply)){
            struct stat st;
            memset(&st, 0, sizeof(st));

            de.d_ino = reply.dino();
            strcpy(de.d_name, reply.dname().c_str());
            de.d_type = reply.dtype();

            st.st_ino = de.d_ino;
            st.st_mode = de.d_type << 12;

            if (filler(buf, de.d_name, &st, 0, static_cast<fuse_fill_dir_flags>(0)))
                break;
            }

        Status status = reader->Finish();

        return -reply.error();
  }

  int Close(string path, struct fuse_file_info *fi){
    cout<<"close "<<__func__<<endl;
    CloseRequest request;
    request.set_path(path);
    CloseReply reply;
    reply.set_error(-1);
    Status status;
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;
    SaveTempFileToCache(fi->fh, path);
    FlushFileToServer(path, request, fi);
    cout<<"Client sending buffer as :"<<request.buffer()<<endl;
    do{
      ClientContext context;
      status = stub_->Close(&context, request, &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));
     
    cout<<"Close done "<<endl; 
    return -reply.error();
  }

  int CloseStream(string path, struct fuse_file_info *fi){
    int fd = open((getCachePath() + path).c_str(), O_RDONLY);
    if (fd == -1){
      cerr << "Close stream-client: could not open file:" << strerror(errno) << endl;
      return -1;
    }

    ClientContext context;
    CloseRequest request;
    CloseReply reply;
    reply.set_error(-1);
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
      
    return 0;
  }

  void SaveTempFileToCache(int fd, string path){
      // moves the temp file to the cache file directory. Overwrites it
      if(fd_tmp_file_map.find(fd) == fd_tmp_file_map.end()){
        cout<<"ERR: cant find fd= "<<fd<<" inside map"<<endl;
      }
      string temp_file = fd_tmp_file_map[fd];
      string cache_file = getCachePath() + path;
      cout<<"Renaming "<<temp_file<<" to "<<cache_file<<endl;
      rename(temp_file.c_str(), cache_file.c_str());
  }

  void FlushFileToServer(string path, CloseRequest &request, struct fuse_file_info *fi){
    cout<<"reading cache file locally from client "<<(getCachePath() + path)<<endl;;
    
    int file_fd = open((getCachePath() + path).c_str(), O_RDONLY, 0644);
    struct stat s;
    if (fstat(file_fd, &s) != 0) {
      printf("ERR: fstat failed in cache\n");
      perror(strerror(errno));
      return;
    }
    
    off_t fsize = s.st_size;
    char *buf = new char[fsize+1];
    buf[fsize] = '\0';
    cout << "Read size: " << fsize <<" fd:" << file_fd << endl;
    off_t offset = 0;
    // read returns -1 for error. Number for bytes for successful read op
    int res = read(file_fd, buf, fsize);
    if (res == -1){
      cout << "ERR: read failed for " <<(getCachePath() + path) << endl;
      perror(strerror(errno));
      return;
    }
    
    
    request.set_offset(0);
    request.set_buffer(buf);
    request.set_size(fsize);
    cout<<"saved buf as "<<request.buffer()<<endl;
    close(file_fd);
    free(buf);
}

  int Create(string path, struct fuse_file_info *fi, mode_t mode) {
    if(!fileExisitsInsideCache(path)) {
      //create a new file inside cache directory
	    //int fd = open((getCachePath() + string(path)).c_str(), 34881 | fi->flags, mode);
	    int fd = open((getCachePath() + string(path)).c_str(), 34881 | fi->flags, 0644);
      if (fd == -1) {
        cout << "ERR: local cache create failed with:" << errno << endl;
        perror(strerror(errno));
	      return -1;
	    }
	    close(fd);
    }
      // create a temp file to work upon by the client
    fi->fh = createTemporaryFile(path, fi, mode);
    CreateRequest request;
    request.set_path(path);
    request.set_mode(mode);
    request.set_flags(fi->flags);
    CreateReply reply;
    reply.set_error(-1);
    Status status;
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;
    do{
      ClientContext context;
      status = stub_->Create(&context, request, &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

    return reply.error();
  }

  //TODO check with temp perspective
  int DeleteFile(string path) {
    DeleteFileReply reply;
    reply.set_error(-1);
    DeleteFileRequest request;
    Status status; 
    request.set_path(path);
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;
    do {
      ClientContext context;
      status = stub_->DeleteFile(&context, request, &reply);
    } while(reply.error()!=0 || retryRequired(status, retry_interval, ++numberOfRetries));
    return reply.error();
  }

  int getLastModTimeFromServer(string path){
    GetAttrRequest request;
    request.set_path(path);
    GetAttrReply reply;
    reply.set_error(-1);
    Status status;
    struct stat output;
    memset(&output, 0, sizeof(struct stat));
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;
    do {
      ClientContext context;
      status = stub_->GetAttr(&context, request , &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

    if(reply.error() != 0){
      return -1;
    }
    return reply.last_mod_time();
  }

  int fetchFileAndUpdateCache_stream(string path, struct fuse_file_info *fi) {
      // open file locally in write mode. If this fails then don't contact the server
      // create a new file if it does not exist
      // We are fetching a new file from server anyway, should not be an issue
      int fd = open((getCachePath() + path).c_str(), O_WRONLY | O_CREAT | O_TRUNC);
      if(fd == -1){
        cout<<"open local failed"<<__func__<<endl;
        perror(strerror(errno));
        // TODO: return errorno and honor it from the caller
        return errno;
      }
    
      
      OpenRequest request;
      OpenReply reply;
      reply.set_error(-1);
      request.set_path(path);
      request.set_flags(fi->flags);
      ClientContext context;
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
    OpenRequest request;
    OpenReply reply;
    reply.set_error(-1);
    Status status;
    request.set_path(path);
    request.set_flags(fi->flags);
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;
    do{
      ClientContext context;
      status = stub_->Open(&context, request, &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

      // check if status is false
      //if(status != Status::OK){
      //  return -1;
        //return reply->error(); //TODO: return proper error code
      //}
    if(reply.error()!=0){
      cacheFileLocally(reply.buffer(), reply.size(), request.path(), fi);
      cout<<"client got bytes "<<reply.size()<<endl;      // TODO: check name of the file
    }
    else {
      cout<<"reply error "<<reply.error()<<endl;
    }
  }

  bool retryRequired(const Status &status, int &retry_interval, int numberOfRetries) {
    if (status.ok() || numberOfRetries >= MAX_RETRIES) {
      cout<<"Retry not needed"<<endl;
      return false;
    }
    else {
      cout<<"Retrying"<<endl;
      std::this_thread::sleep_for (std::chrono::milliseconds(retry_interval));
      retry_interval *= 2; 
      cout<<"Slept, waking up"<<endl;
      return true;
    }
  }

 private:
  std::unique_ptr<AFS::Stub> stub_;
  unordered_map<int, string> fd_tmp_file_map;
};
