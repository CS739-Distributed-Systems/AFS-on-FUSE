# gRPC C++ Hello World Example

You can find a complete set of instructions for building gRPC and running the
Hello World app in the [C++ Quick Start][].

[C++ Quick Start]: https://grpc.io/docs/languages/cpp/quickstart

### Installation:
Follow these steps to install gRPC lib using cmake: https://grpc.io/docs/languages/cpp/quickstart/#setup. 
:warning: make sure to limit the processes by passing number(e.g. 4) during `make -j` command.

for example, instead of `make -j` use `make -j 4`

### Build
1. mkdir -p cmake/build
2. pushd cmake/build
3. cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ../..
4. make -j 4
  

### Execute
1. run ./afs_server in one terminal
2. run ./afs_client in another terminal

### Clean
execute `make clean` from `cmake/build` directory.