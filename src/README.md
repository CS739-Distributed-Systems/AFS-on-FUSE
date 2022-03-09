# AFS using FUSE and gRPC

### FUSE Installation
Please go through [FUSE](https://github.com/libfuse/libfuse) for installation guide and understanding how it works.

### gRPC Installation:
Follow these steps to install gRPC lib using cmake: https://grpc.io/docs/languages/cpp/quickstart/#setup. 
:warning: make sure to limit the processes by passing number(e.g. 4) during `make -j` command.

for example, instead of `make -j` use `make -j 4`

### Build
#### Config Setup:
1. Create `cache_path`in root mode. 
2. Update [`cache_path`](https://github.com/hemal7735/CS739-p2/blob/main/src/afs_client.cc#L70) on client side.
3. Create `server_storage_path`in root mode. 
4. Update [`server storage path`](https://github.com/hemal7735/CS739-p2/blob/main/src/afs_server.cc#L39) on server side.
5. mkdir -p cmake/build
6. pushd cmake/build
7. cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ../..
8. make -j 4
  

### Execute
1. In root mode, run `afs_server` in one terminal
2. In root mode, run `fuse-client` by passing appropriate fuse arguments as shown below.

```console
./fuse-client -o allow_other -o auto_unmount -f /mount_dir
```

### Clean
execute `make clean` from `cmake/build` directory.

### Misc
There are some flags that we have inserted in our code to test various crash scenarios and enable logs. There is also configurable buffer size for read/write/stream operations.

- Client side config: https://github.com/hemal7735/CS739-p2/blob/main/src/afs_client.cc#L43-L64
- Server side config: https://github.com/hemal7735/CS739-p2/blob/main/src/afs_client.cc#L43-L64
