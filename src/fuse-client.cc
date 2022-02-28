#define FUSE_USE_VERSION 31

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#ifdef __FreeBSD__
#include <sys/socket.h>
#include <sys/un.h>
#endif
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include "afs_client.cc"

static int fill_dir_plus = 0;
static int count_readdir = 0;
static int count_getattr = 0;

static AFSClient *afsClient;

static void *xmp_init(struct fuse_conn_info *conn,
		      struct fuse_config *cfg)
{
	(void) conn;
	cfg->use_ino = 1;

	/* Pick up changes from lower filesystem right away. This is
	   also necessary for better hardlink support. When the kernel
	   calls the unlink() handler, it does not know the inode of
	   the to-be-removed entry and can therefore not invalidate
	   the cache of the associated inode - resulting in an
	   incorrect st_nlink value being reported for any remaining
	   hardlinks to this inode. */
	cfg->entry_timeout = 0;
	cfg->attr_timeout = 0;
	cfg->negative_timeout = 0;

	return NULL;
}


static int xmp_getattr(const char *path, struct stat *stbuf,
		       struct fuse_file_info *fi)
{
    count_getattr++;
    printf("entered:xmp_getattr: %d\n", count_getattr);

	std::string pathname = cache_path + path;
	printf("GetAttr: %s \n", pathname.c_str());
	int res = lstat(pathname.c_str(), stbuf);

	if(res==0) {
		cout<<"in cache"<<endl;
		return res;
	}
	cout<<"Not in cache, initiating RPC"<<endl;
	memset(stbuf, 0, sizeof(struct stat));
	return afsClient->GetAttr(path, stbuf);
}

static int xmp_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi,
		       enum fuse_readdir_flags flags)
{

    count_readdir++;
    printf("entered:xmp_readdir: %d\n", count_readdir);
	DIR *dp;
	struct dirent *de;

	(void) offset;
	(void) fi;
	(void) flags;

	dp = opendir(path);
	if (dp == NULL)
		return -errno;

	while ((de = readdir(dp)) != NULL) {
		struct stat st;
		memset(&st, 0, sizeof(st));
		st.st_ino = de->d_ino;
		st.st_mode = de->d_type << 12;
		if (filler(buf, de->d_name, &st, 0, (fuse_fill_dir_flags)(fill_dir_plus)))
			break;
	}

	closedir(dp);
	return 0;
}

static int xmp_mkdir(const char *path, mode_t mode) {
    printf("akshay mkdir %s\n",path);
    
    int res =  afsClient->MakeDir(path, mode);
    if(res == 0){
      cout<<"mkdir success on server"<<endl;
      int local_res = mkdir((getCachePath() + string(path)).c_str(), mode);
      if(local_res !=0){
        //TODO what to do if server pass but local dir fails
        cout<<"client local dir creation failed"<<endl;
      }
    } else {
      cout<<"server dir failed "<<endl;
    }
    return res;
}

static int xmp_rmdir(const char *path)
{
    int res = afsClient->DeleteDir(path);
    if(res == 0){
      cout<<"mkdir success on server"<<endl;
      int local_res = rmdir((getCachePath() + string(path)).c_str());
      if(local_res !=0){
        //TODO what to do if server pass but local dir fails
        cout<<"client local dir creation failed"<<endl;
      }
    } else {
      cout<<"server dir failed "<<endl;
    }
    return res;
}

static int xmp_open(const char *path, struct fuse_file_info *fi)
{
    return afsClient->OpenStream(path, fi);
}

static int xmp_release(const char *path, struct fuse_file_info *fi)
{
	fsync(fi->fh);
	close(fi->fh);
	cout<<"release called"<<endl;
	cout<<"release with fd: "<<fi->fh<<endl;
    int res = afsClient->CloseStream(path, fi);    
	if (res == -1){
		// TODO: what to do if server close failed?
		cout << "server close failed" << endl;
    }
	cout<<"close done &&&&&&& "<<fi->fh<<endl;
    return res;
}

static int xmp_flush(const char *path, struct fuse_file_info *fi)
{
	cout<<"flush called " << path << fi->fh<< endl;
	
	//fsync(fi->fh);
	int ret = close(dup(fi->fh));
	cout<<"flush success with fd: "<<fi->fh<<endl;
	
	// int ret = fsync(fi->fh);
	// if (ret == -1){
    //   cout << "xmp_flush failed" << endl;
    //   perror(strerror(errno));
    // } else {
	// 	close(dup(fi->fh));
	// 	cout<<"flush success"<<endl;
	// }
    return ret;
}

static int xmp_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	int res = afsClient->Create(path, fi, mode);
	if (res == -1) {
		cout << "server create failed with:" << res << endl;
		return -1;
	}

	int fd = open((getCachePath() + string(path)).c_str(), fi->flags, mode);
	if (fd == -1) {
		cout << "local open failed with:" << errno << endl;
		return -1;
	}
    cout<<"***********create with fd: "<<fd<<endl;
	fi->fh = fd;
	return 0;
}

static int xmp_unlink(const char *path)
{
	int res = afsClient->DeleteFile(path);
	if (res == -1) {
		cout << "server unlink failed with:" << res << endl;
		return -1;
	}

	res = unlink((getCachePath() + string(path)).c_str());
	if (res == -1) {
		cout << "local unlink failed with:" << -errno << endl;
	}

	return res;
}

static int xmp_read(const char *path, char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi) {
	
	int res = pread(fi->fh, buf, size, offset);
    if (res == -1){
      cout << "pread failed" << endl;
      perror(strerror(errno));
    }
	cout<<"buffer from pread : "<<buf<<endl;
	return res;
}


static int xmp_write(const char *path, const char *buf, size_t size,
             off_t offset, struct fuse_file_info *fi)
{
    
	int res = pwrite(fi->fh, buf, size, offset);
    if (res == -1){
      cout << "pwrite failed" << endl;
      perror(strerror(errno));
    }
	cout<<"buffer from pwrite : "<<buf<<endl;
	return res;
}

static struct client_ops: fuse_operations {
	client_ops() {
		init = xmp_init;
		getattr	= xmp_getattr;
		readdir = xmp_readdir;
		mkdir	= xmp_mkdir;
		rmdir 	= xmp_rmdir;
		open 	= xmp_open;
		release = xmp_release;
		flush   = xmp_flush; 
		create 	= xmp_create;
		unlink  = xmp_unlink;
		read	= xmp_read;
		write 	= xmp_write;
	}
} xmp_oper;


int main(int argc, char *argv[])
{
	enum { MAX_ARGS = 10 };
	int i,new_argc;
	char *new_argv[MAX_ARGS];

	string target_str = "localhost:50054";

    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    afsClient = new AFSClient(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
	umask(0);
			/* Process the "--plus" option apart */
	for (i=0, new_argc=0; (i<argc) && (new_argc<MAX_ARGS); i++) {
		if (!strcmp(argv[i], "--plus")) {
			fill_dir_plus = FUSE_FILL_DIR_PLUS;
		} else {
			new_argv[new_argc++] = argv[i];
		}
	}
	return fuse_main(new_argc, new_argv, &xmp_oper, NULL);
}

