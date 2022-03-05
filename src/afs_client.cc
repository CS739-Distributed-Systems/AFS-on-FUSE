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
#include <unordered_set>
#include <chrono>
#include <fstream>
#include <iostream>
#include <experimental/filesystem>
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientReader;
using grpc::ClientWriter;

using namespace afs;
using namespace std;

#define BUF_SIZE 1
#define RETRY_INTERVAL 100
#define MAX_RETRIES 5
#define TIME_LIMIT 3
// #define IS_DEBUG_ON

// static string cache_path = "/users/akshay95/cache_dir";
static string cache_path = "/home/hemalkumar/hemal/client_cache_dir";
static std::string consistent_ext(".consistent");
static std::string dirty_file_ext(".tmp");

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
        dirty_temp_fd.clear();      
      }

  void init(){
    std::cout<<"Running Garbage Collector and Crash Recovery System"<<endl;

    for (auto &p : std::experimental::filesystem::recursive_directory_iterator(cache_path))
    {
         if(p.path().extension().string().rfind(dirty_file_ext) == 0){
           cout<<"Deleting dirty tmp file: "<<p.path().string()<<endl;
           int ret = unlink(p.path().string().c_str());
           if(ret == -1){
             cout<<"ERR: init deletion of dirty tmp file failed"<<__func__<<endl;
             perror(strerror(errno));
	         }
         } else if (p.path().extension() == consistent_ext){

            // fetch the original path after stripping .tmpXXX.consistent
	          string absolute_path = p.path().string();
	          string orig_path = absolute_path.substr(0, absolute_path.find_last_of("."));
	          orig_path = orig_path.substr(0, orig_path.find_last_of("."));
	          string cache_relative_path = orig_path.substr(orig_path.find(cache_path)+cache_path.size());
	  
	          // send the file to server
            int res = flushFileToServer(absolute_path.c_str(), cache_relative_path);
            if(res == -1){
             cout<<"ERR: flushing to server failed"<<endl;
            }

            // rename the .consistent file to original name
            SaveConsistentTempFileToCache(absolute_path, orig_path);
        }
    } 

    std::cout<<"Done: Garbage Collector and Crash Recovery System"<<endl;

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
      setContextDeadline(context);
      status = stub_->GetAttr(&context, request , &reply);
    } while (reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

    if(reply.error() != 0){
      cout<<"ERR: GetAttr failed from server"<<reply.error()<<endl;
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
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif
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
      setContextDeadline(context);
      status = stub_->MakeDir(&context, request, &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

    if(reply.error() == 0){
      int local_res = mkdir((getCachePath() + string(path)).c_str(), mode);
      if(local_res !=0){
        //TODO what to do if server pass but local dir fails
        cout<<"ERR: client local dir creation failed"<<endl;
      }
    }
    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif
    return -reply.error();
  }

  int DeleteDir(string path){
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    DeleteDirRequest request;
    request.set_path(path);
    DeleteDirReply reply;
    reply.set_error(-1);
    Status status;
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;
    do{
      ClientContext context;
      setContextDeadline(context);
      status = stub_->DeleteDir(&context, request, &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

    if(reply.error() == 0){
      #ifdef IS_DEBUG_ON
        cout << "rmdir success on server" << endl;
      #endif

      int local_res = rmdir((getCachePath() + string(path)).c_str());
      if(local_res !=0){
        //TODO what to do if server pass but local dir fails
        cout<<"ERR: client local dir creation failed"<<endl;
      }
    } 

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif

    return -reply.error();
  }

  int Write(int fd) {
    dirty_temp_fd.insert(fd);
    return 0;
  }

  bool checkIfFileExistsViaLocalLstat(string path){
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    struct stat stbuf;
    memset(&stbuf, 0, sizeof(struct stat));
    int res = lstat(path.c_str(), &stbuf);

    #ifdef IS_DEBUG_ON
      cout << "Lstat for "<<path<<" returns "<<(res == 0)<<endl;
      cout << "END:" << __func__<< endl;
    #endif
    return res == 0;
  }

  int cacheFileLocally(string buffer, unsigned int size, const string path, struct fuse_file_info *fi)
  {
    //int fd = open((cache_path + path).c_str(), O_WRONLY | O_CREAT | O_TRUNC);
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

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

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif

    return 0;    // TODO: check the error code
  }

  int Open(string path, struct fuse_file_info *fi) {
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    // check if file exists inside cache and set fh to fi
    if(!checkIfFileExistsViaLocalLstat(getCachePath() + path)) { 
      cout<<"File not in cache"<<endl;
      fetchFileAndUpdateCache(path, fi);   //TODO ERR: shouldnt it be path ?????
    } else {
      cout<<"file present in cache"<<endl;
      struct stat result;
      std::string path_in_cache = getCachePath() + path;
      int statRes = stat(path_in_cache.c_str(), &result); 
      if(statRes!=0)
        cout<<"ERR: stat failed"<<statRes<<"path "<<path<<endl;
      else {
        auto cache_mod_time = result.st_mtime;
        auto server_mod_time = getLastModTimeFromServer(path);
        if (server_mod_time!=-1 && server_mod_time > cache_mod_time){
          #ifdef IS_DEBUG_ON
            cout<<"Cache Invalidated/Stale, Fetching from Server"<<endl;
          #endif
          fetchFileAndUpdateCache(path, fi);
        }
      }
    }
    // assuming that until this point the file exists in the client FS cache
    // each client creates a temporary copy of this file to work upon 
    
    fi->fh = createTemporaryFile(path, fi);

    #ifdef IS_DEBUG_ON
      cout<<"Open set fh of temp file as "<<fi->fh<<endl;
      cout << "END:" << __func__<< endl;
    #endif
    return 0;
  }

  int createTemporaryFile(string path, struct fuse_file_info *fi, mode_t mode = -1){
    // creates a temporary file for the client to work upon instead of main file

    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    int source_file_fd = open((cache_path + string(path)).c_str(), O_RDONLY, 0644);
    string tmp_file_name = generateTempPath(string(path)); 

    #ifdef IS_DEBUG_ON
      cout<<"Creating temp file with name "<<tmp_file_name<<endl;
    #endif
    
    int dest_file_fd;
    if(mode == -1)
	    dest_file_fd  = open(tmp_file_name.c_str(), O_RDWR | O_CREAT, 0644);
    else
	    dest_file_fd = open(tmp_file_name.c_str(), fi -> flags, mode);
    
    if(source_file_fd == -1 || dest_file_fd == -1){
      cout<<"ERR: file open failed for cacheFile/temporaryFile "<<__func__<<endl;
      cout<<source_file_fd<<", "<<dest_file_fd<<endl;
      perror(strerror(errno));
      return -errno;
    }
    struct stat stat_source;
    fstat(source_file_fd, &stat_source);
    char *buf = new char[BUF_SIZE];

    while(1) {
      int readBytes = read(source_file_fd, buf, BUF_SIZE);

      if (readBytes == 0) {
        break;
      }

      if (readBytes == -1) {
        cerr<<"ERR: failed to read from source file "<<cache_path + string(path)<<endl;
        perror(strerror(errno));
        return -errno;
      }

      int bytesWritten = write(dest_file_fd, buf, readBytes);

      if (bytesWritten == -1) {
        cerr<<"ERR: failed to write to dest file "<<tmp_file_name<<endl;
        perror(strerror(errno));
        return -errno;
      }
    }

    close(source_file_fd);
    close(dest_file_fd);

    dest_file_fd  = open(tmp_file_name.c_str(), O_RDWR, 0644);
    if (dest_file_fd == -1){
      cout << "ERR: open of file failed" <<tmp_file_name << endl;
      perror(strerror(errno));
    }

    #ifdef IS_DEBUG_ON
      cout<<"dest file fd was "<<dest_file_fd<<endl;
    #endif
    
    
    if(fd_tmp_file_map.find(dest_file_fd) == fd_tmp_file_map.end()){

      #ifdef IS_DEBUG_ON
          cout<<"Inserting inside map "<<dest_file_fd<<" " << tmp_file_name<<endl;
      #endif
	 
    	fd_tmp_file_map.insert({dest_file_fd, tmp_file_name});
    } else{
        fd_tmp_file_map[dest_file_fd] = tmp_file_name;
    }

    free(buf);

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif

    return dest_file_fd;
  }

  int OpenStream(string path, struct fuse_file_info *fi) {
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif
    // check if file exists inside cache and set fh to fi
    int numberOfRetries=0;
    int res;
    if(!checkIfFileExistsViaLocalLstat(getCachePath() + path)) { 
      #ifdef IS_DEBUG_ON
          cout<<"file not present in Cache"<<endl;
      #endif
      
      do {
        res = fetchFileAndUpdateCache_stream(path, fi);
        numberOfRetries++;
      } while(res!=0 && numberOfRetries<MAX_RETRIES);
    } else {
      #ifdef IS_DEBUG_ON
          cout<<"File present in cache"<<endl;
      #endif

      struct stat result;
      std::string path_in_cache = getCachePath() + path;
      int statRes = stat(path_in_cache.c_str(), &result); 
      if(statRes!=0)
        cout<<"ERR: stat failed"<<statRes<<"path "<<path<<endl;
      else {
        auto cache_mod_time = result.st_mtime;
        auto server_mod_time = getLastModTimeFromServer(path);
        if (server_mod_time!=-1 && server_mod_time > cache_mod_time){
          
          #ifdef IS_DEBUG_ON
            cout<<"Cache Invalidate/Stale, fetching from server"<<endl;
          #endif
          
          do {
            res = fetchFileAndUpdateCache_stream(path, fi);
            numberOfRetries++;
          } while(res!=0 && numberOfRetries<MAX_RETRIES);
        }
      }
    }

    fi->fh = createTemporaryFile(path, fi);

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
      cout<<"Open set fh of temp file as "<<fi->fh<<endl;
    #endif
    
    return 0;
  }
  
  int ReadDir(string p, void *buf, fuse_fill_dir_t filler){

      #ifdef IS_DEBUG_ON
        cout << "START:" << __func__<< endl;
      #endif

      ClientContext context;
      setContextDeadline(context);
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

    #ifdef IS_DEBUG_ON
        cout << "END:" << __func__<< endl;
    #endif

    return -reply.error();
  }

  int Close(string path, struct fuse_file_info *fi){
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif
    // step - 1: save temp file to consistent tmp file
    string consistent_tmp_path = SaveTempFileToConsistentTemp(fi->fh);
    
    // Step - 2 If there are no changes to the file, skip sending it to server
    if (dirty_temp_fd.find(fi->fh) == dirty_temp_fd.end()) {
      #ifdef IS_DEBUG_ON
        cout << "not dirty, skipping flush to server" << endl;
      #endif
      
      //TODO: delete the tmp file as it is not modified (so not renamed to cache file)
      deleteLocalFile(consistent_tmp_path);
      return 0;
    }
    
    // step - 3: flush consistent tmp file to server
    CloseRequest request;
    request.set_path(path);
    CloseReply reply;
    reply.set_error(-1);
    ReadTempFileIntoMemory(consistent_tmp_path, request, fi);
    Status status;
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;

    #ifdef IS_DEBUG_ON
        cout<<"Client sending buffer size as :"<<request.size()<<endl;
    #endif
    
    do{
      ClientContext context;
      setContextDeadline(context);
      status = stub_->Close(&context, request, &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));
    
    // step 4: Save the written file to cache file as well
    SaveConsistentTempFileToCache(consistent_tmp_path, getCachePath() + path);

    #ifdef IS_DEBUG_ON
        cout << "END:" << __func__<< endl;
    #endif

    return -reply.error();
  }

  void deleteLocalFile(string file_path){
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    int res = unlink(file_path.c_str());
    if (res == -1) {
      std::cout<<"ERR: Error deleting file locally "<<file_path<<endl;
      perror(strerror(errno));
    }

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif
  }

  int CloseStream(string path, struct fuse_file_info *fi){
 
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    // step - 1: save temp file to consistent tmp file to indicate non-dirty local cache
    string consistent_tmp_path = SaveTempFileToConsistentTemp(fi->fh);
    
    // Step - 2 If there are no changes to the file, skip sending it to server
    if (dirty_temp_fd.find(fi->fh) == dirty_temp_fd.end()) {
      #ifdef IS_DEBUG_ON
        cout << "not dirty, skipping flush to server" << endl;
        cout << "END:" << __func__<< endl;
      #endif
      
      //TODO: delete the consistent tmp file as it was not changed, hence not needed
      deleteLocalFile(consistent_tmp_path);
      return 0;
    }


    #ifdef IS_DEBUG_ON
      cout << "Local cache file was modified, So needs server flush" << endl;
    #endif
    
    dirty_temp_fd.erase(fi->fh);
    
    // step - 3: flush consistent tmp file to server
    int res = flushFileToServer(consistent_tmp_path.c_str(), path);
    if(res == -1){
      cout<<"ERR: flushing to server failed"<<endl;
    }
    
    // step 4: Save the written file to cache file as well
    SaveConsistentTempFileToCache(consistent_tmp_path, getCachePath() + path);


    // TODO: check status and retry operation if required
    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif

    return 0;
  }

  int flushFileToServer(string consistent_tmp_path, string path){

    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    int fd = open(consistent_tmp_path.c_str(), O_RDONLY);
    if (fd == -1){
      cerr << "Close stream-client: could not open file:" << strerror(errno) << endl;
      return -1;
    }

    ClientContext context;
    setContextDeadline(context);
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

      #ifdef IS_DEBUG_ON
        cout << "client sent bytes:" << bytesRead << endl;
      #endif
    }
    
    writer->WritesDone();
    Status status = writer->Finish();

    #ifdef IS_DEBUG_ON
      cout<<"client had fd = "<<fd<<endl;
    #endif
    
    close(fd);
    free(buf);

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif

    return 0;
  }

  string SaveTempFileToConsistentTemp(int fd){
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    // moves the temp file to the cache file directory. Overwrites it
    if(fd_tmp_file_map.find(fd) == fd_tmp_file_map.end()){
      cout<<"ERR: cant find fd= "<<fd<<" inside map"<<endl;
      // TODO: return valid error and handling
      return "";
    }

    string temp_file = fd_tmp_file_map[fd];
    //TODO: remove the df from fd_tmp_file_map
    string consistent_tmp_file = temp_file + ".consistent";

    #ifdef IS_DEBUG_ON
      cout<<"Renaming "<<temp_file<<" to "<<consistent_tmp_file<<endl;
    #endif

    rename(temp_file.c_str(), consistent_tmp_file.c_str());

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif

    return consistent_tmp_file;
  }
  
  void SaveConsistentTempFileToCache(string consistent_tmp_path, string path){
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    string cache_file_path = path;

    #ifdef IS_DEBUG_ON
      cout<<"Renaming "<<consistent_tmp_path<<" to "<<cache_file_path<<endl;
    #endif

    rename(consistent_tmp_path.c_str(), cache_file_path.c_str());

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif
  
  }

  void ReadTempFileIntoMemory(string path, CloseRequest &request, struct fuse_file_info *fi){
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
      cout<<"reading cache file locally from client "<<(getCachePath() + path)<<endl;;
    #endif
    
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

    #ifdef IS_DEBUG_ON
      cout << "Read size: " << fsize <<" fd:" << file_fd << endl;
    #endif
    
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

    #ifdef IS_DEBUG_ON
      cout<<"saved buf size "<<request.size()<<endl;
    #endif
    
    close(file_fd);
    free(buf);

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif
}
  void mirrorDirectoryStructureInsideCache(string cache_dir_path, string directory_path,  mode_t mode=0777){

    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
      cout<<"full_path = "<<directory_path<<endl;
      cout<<"stripping string "<<directory_path.substr(0, directory_path.find_last_of("\\/"))<<endl;
    #endif

    string directories = (directory_path.substr(0, directory_path.find_last_of("\\/")));
    directories = directories.substr(directories.find_first_of("\\/")+1);
    if(checkIfFileExistsViaLocalLstat(getCachePath() + directories)){
      #ifdef IS_DEBUG_ON
         cout<<"this directory already exists "<<getCachePath() + directories<<endl;
      #endif
      
      return;
    }
   		    
    string dir;
    string prefix = cache_dir_path;
    while(directories.find("/") != std::string::npos){
       dir = prefix + "/" + directories.substr(0, directories.find_first_of("\\/"));
       #ifdef IS_DEBUG_ON
         cout<<"MakeDirectory on "<<dir<<endl;
       #endif
       
       int res = mkdir(dir.c_str(),mode);
       if(res != 0){
         cout << "ERR: MirrorDirectory: makedir failed "<<dir<< endl;
         perror(strerror(errno));
         return;
       }
       prefix = dir;
       directories = directories.substr(directories.find_first_of("\\/")+1);
    }
    dir = prefix + "/" +directories;

    #ifdef IS_DEBUG_ON
        cout<<"MakeDirectory on "<<dir<<endl;
    #endif
    
    int res = mkdir(dir.c_str(),mode);
    if(res != 0){
      cout << "ERR: MirrorDirectory: makedir failed " << dir << endl;
      perror(strerror(errno));
      return;
    }

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif
    
  }

  int Create(string path, struct fuse_file_info *fi, mode_t mode) {
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    if(!checkIfFileExistsViaLocalLstat(getCachePath() + path)) { 
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
      setContextDeadline(context);
      status = stub_->Create(&context, request, &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

    if(reply.error() == 0){
	    mirrorDirectoryStructureInsideCache(getCachePath(), path, mode);
      if(!checkIfFileExistsViaLocalLstat(getCachePath() + path)) {
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
      }

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif
    
    return reply.error();
  }

  //TODO check with temp perspective
  int DeleteFile(string path) {
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    DeleteFileReply reply;
    reply.set_error(-1);
    DeleteFileRequest request;
    Status status; 
    request.set_path(path);
    int numberOfRetries = 0;
    int retry_interval = RETRY_INTERVAL;
    do {
      ClientContext context;
      setContextDeadline(context);
      status = stub_->DeleteFile(&context, request, &reply);
    } while(reply.error()!=0 || retryRequired(status, retry_interval, ++numberOfRetries));

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif

    return reply.error();
  }

  int getLastModTimeFromServer(string path){
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

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
      setContextDeadline(context);
      status = stub_->GetAttr(&context, request , &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

    if(reply.error() != 0){
      return -1;
    }

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif

    return reply.last_mod_time();
  }

  int fetchFileAndUpdateCache_stream(string path, struct fuse_file_info *fi) {
      #ifdef IS_DEBUG_ON
        cout << "START:" << __func__<< endl;
      #endif
      // open file locally in write mode. If this fails then don't contact the server
      // create a new file if it does not exist
      // We are fetching a new file from server anyway, should not be an issue
      int fd = open((getCachePath() + path).c_str(), O_WRONLY | O_CREAT | O_TRUNC);
      if(fd == -1){
        cout<<"Stream: open local failed"<<__func__<<endl;
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
      setContextDeadline(context);
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

      #ifdef IS_DEBUG_ON
        cout << "END:" << __func__<< endl;
      #endif
      return 0;
  }

  void fetchFileAndUpdateCache(string path, struct fuse_file_info *fi){
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

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
      setContextDeadline(context);
      status = stub_->Open(&context, request, &reply);
    } while(reply.error()!=0 && retryRequired(status, retry_interval, ++numberOfRetries));

      // check if status is false
      //if(status != Status::OK){
      //  return -1;
        //return reply->error(); //TODO: return proper error code
      //}
    
    if(reply.error() == 0){
	    mirrorDirectoryStructureInsideCache(getCachePath(), path);
      cacheFileLocally(reply.buffer(), reply.size(), request.path(), fi);

      #ifdef IS_DEBUG_ON
        cout<<"client got bytes "<<reply.size()<<endl;      // TODO: check name of the file
      #endif
      
    } else {
      cacheFileLocally(reply.buffer(), reply.size(), request.path(), fi);
      cout<<"error  "<<reply.error()<<endl;      // TODO: check name of the file
    }

    #ifdef IS_DEBUG_ON
      cout << "END:" << __func__<< endl;
    #endif
  }

  bool retryRequired(const Status &status, int &retry_interval, int numberOfRetries) {
    #ifdef IS_DEBUG_ON
      cout << "START:" << __func__<< endl;
    #endif

    if (status.ok() || numberOfRetries >= MAX_RETRIES) {
      #ifdef IS_DEBUG_ON
        cout<<"Retry not needed"<<endl;
        cout << "END:" << __func__<< endl;
      #endif
      return false;
    }else {
      #ifdef IS_DEBUG_ON
          cout<<"Retrying"<<endl;
      #endif
      
      std::this_thread::sleep_for (std::chrono::milliseconds(retry_interval));
      retry_interval *= 2; 
      
      #ifdef IS_DEBUG_ON
        cout<<"Slept, waking up"<<endl;
        cout << "END:" << __func__<< endl;
      #endif
      
      return true;
    }


  }

  void setContextDeadline(ClientContext &context, int time_limit=TIME_LIMIT){
    std::chrono::system_clock::time_point deadline =
    std::chrono::system_clock::now() + std::chrono::seconds(time_limit);
    context.set_deadline(deadline);
  }

 private:
  std::unique_ptr<AFS::Stub> stub_;
  unordered_map<int, string> fd_tmp_file_map;
  unordered_set<int> dirty_temp_fd;
};
