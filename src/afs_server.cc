#include <iostream>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include "afs.grpc.pb.h"
#include <dirent.h>

#define BUF_SIZE 1

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReader;
using grpc::ServerWriter;

using namespace afs;
using namespace std;

// Logic and data behind the server's behavior.
class AFSServiceImpl final : public AFS::Service {

  const char *serverPath = "/home/hemalkumar/hemal/server";
  

  string generateTempPath(string path){
    return path + ".tmp";
  }

  Status MakeDir(ServerContext* context, const MakeDirRequest* request,
                  MakeDirReply* reply) override {
    cout<<"Reached "<<__func__<<" on server"<<endl;
    int res = mkdir((serverPath + request->path()).c_str(), request->mode());
    if (res == -1) {
      printf("Error in mkdir ErrorNo: %d\n",errno);
      perror(strerror(errno));
      reply->set_error(errno);
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "path not found on server");
    } else {
      printf("MakeDirectory success\n");
      reply->set_error(0);
     }
    return Status::OK;
  }

  Status ReadDir(ServerContext* context, const ReadDirRequest* request,
		  ServerWriter<ReadDirReply>* writer) override {

		DIR *dp;
		struct dirent *de;
		ReadDirReply directory;

		dp = opendir((serverPath + request->path()).c_str());
		if (dp == NULL){
			perror(strerror(errno));
			directory.set_error(errno);
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "dp not found on server");
		}

		while((de = readdir(dp)) != NULL){
		    directory.set_dino(de->d_ino);
		    directory.set_dname(de->d_name);
		    directory.set_dtype(de->d_type);
		    writer->Write(directory);
		}
		directory.set_error(0);
    closedir(dp);

		return Status::OK;
  }

  Status DeleteDir(ServerContext* context, const DeleteDirRequest* request,
                  DeleteDirReply* reply) override {
    cout<<"Reached "<<__func__<<" on server"<<endl;
    int res = rmdir((serverPath + request->path()).c_str());
    if (res == -1) {
      cout << "Error in DeleteDir ErrorNo: " << errno << endl;
      perror(strerror(errno));
      reply->set_error(errno);
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "path not found on server");
      
    } else {
      cout << "DeleteDir success" << endl;
      reply->set_error(0);
     }
    return Status::OK;
  }

  Status GetAttr(ServerContext *context, const GetAttrRequest *request,
                 GetAttrReply *reply) override
  {
    int res;
    struct stat st;
    std::string path = serverPath + request->path(); // TODO: check path or add prefix if needed
    printf("GetAttr: %s \n", path.c_str());

    res = lstat(path.c_str(), &st);
    if (res < 0)
    {
      perror(strerror(errno));
      reply->set_error(errno);
      return Status::OK;
    }
    reply->set_inode(st.st_ino);
    reply->set_mode(st.st_mode);
    reply->set_num_hlinks(st.st_nlink);
    reply->set_user_id(st.st_uid);
    reply->set_groud_id(st.st_gid);
    reply->set_size(st.st_size);
    reply->set_block_size(st.st_blksize);
    reply->set_blocks(st.st_blocks);
    reply->set_last_acess_time(st.st_atime);
    reply->set_last_mod_time(st.st_mtime);
    reply->set_last_stat_change_time(st.st_ctime);
    reply->set_error(0);
    return Status::OK;
  }

  Status Open(ServerContext *context, const OpenRequest *request, OpenReply *reply){
    printf("Server Open stub\n");
    int fd = open((serverPath + request->path()).c_str(), O_RDONLY);
    if (fd == -1){
      printf("could not open file\n");
      reply->set_error(errno);
      perror(strerror(errno));
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "custom error msg");
    }

    //calculate the size of the file
    struct stat s;
    if (fstat(fd, &s) == -1) {
      printf("fstat failed\n");
      reply->set_error(errno);
      perror(strerror(errno));
      close(fd);
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "custom error msg");
    }
    
    off_t fsize = s.st_size;
    cout << "size:" << fsize << endl;
    char *buf = new char[fsize];
    off_t offset = 0;
    int res = pread(fd, buf, fsize, offset);
    if (res == -1){
      cout << "pread failed" << endl;
      reply->set_error(errno);
      perror(strerror(errno));
      close(fd);
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "custom error msg");
    }
    printf("read bytes %d",res);
    reply->set_error(0);
    reply->set_buffer(buf);
    reply->set_size(res);
    cout<<"server had fd = "<<fd<<endl;
    close(fd);
    free(buf);
    return Status::OK;
  }

  Status OpenStream(ServerContext* context, const OpenRequest* request, ServerWriter<OpenReply>* writer){
    OpenReply reply;

    printf("Server Open stub\n");
    int fd = open((serverPath + request->path()).c_str(), O_RDONLY);
    if (fd == -1){
      printf("could not open file\n");
      reply.set_error(errno);
      perror(strerror(errno));
      writer->Write(reply);
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "custom error msg");
    }

    char *buf = new char[BUF_SIZE];

    while(1) {
      OpenReply reply;

      int bytesRead = read(fd, buf, BUF_SIZE);

      // done with the reading
      if (bytesRead == 0) {
        break;
      }

      // error with reading, inform client
      if (bytesRead == -1) {
        cerr << "server read error while reading op - err:" << errno << endl;
        reply.set_error(errno);
        writer->Write(reply);
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "custom error msg");
      }

      reply.set_error(0);
      reply.set_buffer(buf);
      reply.set_size(bytesRead);

      writer->Write(reply);
      cout << "server sent bytes:" << bytesRead << endl;
    }
    
    cout<<"server had fd = "<<fd<<endl;
    close(fd);
    free(buf);
    return Status::OK;
  }

  Status Create(ServerContext *context, const CreateRequest *request, CreateReply *reply){
    string path = serverPath + string(request->path());
    int fd = open(path.c_str(), request->flags(), request->mode());

    cout << "Server Attempted to Create File at:" << path << " with fd:" << fd << endl;
    
    if (fd == -1) {
      perror(strerror(errno));
  	  reply->set_error(errno);
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "custom error msg");
    } else {
      close(fd);
      reply->set_error(0);
    }

    return Status::OK;
  }

  Status DeleteFile(ServerContext *context, const DeleteFileRequest *request, DeleteFileReply *reply) {
    string path = serverPath + string(request->path());
    int res = unlink(path.c_str());

    cout << "Server Attempted to Delete File at:" << path << " with res:" << res << endl;

    if (res == -1) {
  	  reply->set_error(res);
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "custom error msg");
    } else {
      reply->set_error(0);
    }
    return Status::OK;
  }

  Status Close(ServerContext *context, const CloseRequest *request, CloseReply *reply) {
    
    string path = serverPath + string(request->path());
    cout << "Server close: " << path << endl;
    string tempPath = generateTempPath(path);

    int res = WriteToTempFile(request->buffer(), request->size(), tempPath.c_str());
    SaveTempFileToCache(tempPath, path);
    if (res == -1) {
  	  reply->set_error(res);
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "custom error msg");
    } else {
      reply->set_error(0);
    }
    return Status::OK;
  }

  Status CloseStream(ServerContext* context, ServerReader<CloseRequest>* reader,
                     CloseReply* reply) {
    
    CloseRequest request;

    int fd, res;
    bool firstReq = true;

    while (reader->Read(&request)) {
      if (firstReq) {
        string path = serverPath + string(request.path());
        fd = open(path.c_str(), O_WRONLY);

        if(fd == -1){
          cerr << "server tried to open file:" << path << endl;
          cerr <<"server close - local failed: with err - " << strerror(errno) << endl;
          reply->set_error(-1);
          return grpc::Status(grpc::StatusCode::NOT_FOUND, "custom error msg");
        }

        firstReq = false;
      }

      res = write(fd, request.buffer().c_str(), request.size());
      if (res == -1) {
        cerr <<"server close - local write failed: with err - " << strerror(errno) << endl;
        reply->set_error(-1);
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "custom error msg");
      }
    }

    fsync(fd);
    close(fd);
    reply->set_error(0);

    return Status::OK; 
  }
  

  void SaveTempFileToCache(string temp_file_path, string cache_file_path){
      // moves the temp file to the cache file directory. Overwrites it

      cout<<"Renaming "<<temp_file_path<<" to "<<cache_file_path<<endl;
      rename(temp_file_path.c_str(), cache_file_path.c_str());
  }

   int WriteToTempFile(string buffer, unsigned int size, const string temp_path){
    cout<<"server to write data "<<buffer<<" to file:"<<temp_path<<endl;

    int fd = open(temp_path.c_str(), O_RDWR | O_CREAT, 0644);
    if(fd == -1){
      cout<<"ERR: server open local failed"<<__func__<<endl;
      perror(strerror(errno));
      return errno;
    }
    off_t offset = 0;
    int res = pwrite(fd, buffer.c_str(), size, offset);
    if(res == -1){
      cout<<"ERR: pwrite local failed in "<<__func__<<endl;
      perror(strerror(errno));
      return errno;
    }
    fsync(fd);
    close(fd);
    return 0;    // TODO: check the error code
   }

};

void RunServer() {
  std::string server_address("0.0.0.0:50054");
  AFSServiceImpl service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}
