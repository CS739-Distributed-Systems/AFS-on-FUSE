#include <iostream>
#include <memory>
#include <string>
#include <sys/stat.h>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include "afs.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace afs;
using namespace std;


// Logic and data behind the server's behavior.
class AFSServiceImpl final : public AFS::Service {

  const char *serverPath = "/users/akshay95/server_space";

  Status DeleteFile(ServerContext* context, const DeleteFileRequest* request,
                  DeleteFileReply* reply) override {
    std:string path = request->path();

    reply->set_error(0);

    return Status::OK;
  }

  Status MakeDir(ServerContext* context, const MakeDirRequest* request,
                  MakeDirReply* reply) override {

    int res = mkdir((serverPath + request->path()).c_str(), request->mode());
    if (res == -1) {
      printf("Error in mkdir ErrorNo: %d\n",errno);
      perror(strerror(errno));
      reply->set_error(errno);
    } else {
      printf("MakeDirectory success\n");
      reply->set_error(0);
     }
    return Status::OK;
  }

  Status GetAttr(ServerContext *context, const GetAttrRequest *request,
                 GetAttrReply *reply) override
  {
    printf("Reached server getAttr\n");
    int res;
    struct stat st;
    std::string path = "/home/hemalkumar/server" + request->path(); // TODO: check path or add prefix if needed
    printf("GetAttr: %s \n", path.c_str());

    res = lstat(path.c_str(), &st);
    if (res < 0)
    {
      perror(strerror(errno));
      reply->set_error(errno);
      return Status::CANCELLED; // TODO: find apt error 
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
};

void RunServer() {
  std::string server_address("0.0.0.0:50051");
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
