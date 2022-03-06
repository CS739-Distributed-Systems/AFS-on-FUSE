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
        afsClient->init();
	return NULL;
}


static int xmp_getattr(const char *path, struct stat *stbuf,
		       struct fuse_file_info *fi)
{
	std::string pathname = cache_path + path;

	#ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << pathname << endl;
	#endif
	
	int res = lstat(pathname.c_str(), stbuf);

	if(res==0) {
		#ifdef IS_DEBUG_ON
			cout<<"in cache"<<endl;
			cout << "END:" << __func__ << endl;
		#endif
		return res;
	}

	#ifdef IS_DEBUG_ON
		cout<<"Not in cache, initiating RPC"<<endl;
	#endif

	memset(stbuf, 0, sizeof(struct stat));

	res = afsClient->GetAttr(path, stbuf);

	#ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << endl;
	#endif

	return res;
}

static int xmp_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi,
		       enum fuse_readdir_flags flags)
{

	#ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << endl;
	#endif

    int res = afsClient->ReadDir(path, buf, filler);

	#ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << endl;
	#endif

	return res;
}

static int xmp_mkdir(const char *path, mode_t mode) {
    #ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << " " << path << endl;
	#endif

    int res =  afsClient->MakeDir(path, mode);
    
	if(res != 0){
	 	cout<<"ERR: server mkddir failed "<<endl;
    }
	
    #ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << " " << path << endl;
	#endif
    return res;
}

static int xmp_rmdir(const char *path)
{
    #ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << " " << path << endl;
	#endif

    int res = afsClient->DeleteDir(path);
    
	if(res != 0){
      	cout<<"ERR: server dir failed "<<endl;
    }

    #ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << " " << path << endl;
	#endif

    return res;
}

static int xmp_open(const char *path, struct fuse_file_info *fi)
{
    #ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << " " << path << endl;
	#endif
    int res = afsClient->OpenStream(path, fi);
    
	#ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << " " << path << endl;
	#endif

    return res;
}

static int xmp_release(const char *path, struct fuse_file_info *fi)
{
    #ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << " " << path << endl;
	#endif

	fsync(fi->fh);
	close(fi->fh);

	#ifdef IS_DEBUG_ON
		cout<<"fsync and close done with fd: "<<fi->fh<<endl;
	#endif
    
	int res = afsClient->CloseStream(path, fi);    
	if (res != 0){
        cout << "ERR: server close failed" << endl;
    }
    
	#ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << " " << path << endl;
	#endif
    
	return res;
}

static int xmp_flush(const char *path, struct fuse_file_info *fi)
{
    #ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << " " << path << endl;
	#endif
	
	int ret = close(dup(fi->fh));

	#ifdef IS_DEBUG_ON
		cout<<"flush success with fd: "<<fi->fh<<endl;
		cout << "END:" << __func__ << " " << path << endl;
	#endif
	
    return ret;
}

static int xmp_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    #ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << " " << path << endl;
	#endif

	int res = afsClient->Create(path, fi, mode);

	if (res != 0) {
		cout << "ERR: server create failed with:" << res << endl;
		return -1;
	}
    
	#ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << " " << path << endl;
	#endif

	return 0;
}

static int xmp_unlink(const char *path)
{
    #ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << " " << path << endl;
	#endif

	int res = afsClient->DeleteFile(path);
	if (res != 0) {
		cout << "ERR: server unlink failed with:" << res << endl;
		return -1;
	}

	res = unlink((getCachePath() + string(path)).c_str());
	if (res == -1) {
		cout << "ERR: local unlink failed with:" << -errno << endl;
	}

    #ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << " " << path << endl;
	#endif

	return res;
}

static int xmp_read(const char *path, char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi) {
	
    #ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << " " << path << endl;
		cout<<"read with fd "<<fi->fh<<endl;
	#endif

	int res = pread(fi->fh, buf, size, offset);

	#ifdef CRASH_READ
		afsClient->killMe("crashing after read");
	#endif
    
	if (res == -1){
      cout << "ERR: pread failed" << endl;
      perror(strerror(errno));
    }
    
	#ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << " " << path << endl;
	#endif

	return res;
}


static int xmp_write(const char *path, const char *buf, size_t size,
             off_t offset, struct fuse_file_info *fi)
{
    
    #ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << " " << path << endl;
	#endif
	
	afsClient->Write(fi->fh);

	int res = pwrite(fi->fh, buf, size, offset);

	#ifdef CRASH_WRITE
		afsClient->killMe("crashing after write");
	#endif
    
	if (res == -1){
      cout << "ERR: pwrite failed" << endl;
      perror(strerror(errno));
    }
	

	#ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << " " << path << endl;
	#endif
    
	return res;
}

static int xmp_utimens(const char *path, const struct timespec ts[2],
		       struct fuse_file_info *fi)
{

	#ifdef IS_DEBUG_ON
		cout << "START:" << __func__ << " " << path << endl;
	#endif

    int res = utimensat(AT_FDCWD, (getCachePath() + string(path)).c_str(), ts, AT_SYMLINK_NOFOLLOW);
    
	if (res == -1) {
        cout<<"ERR: utimens error"<<endl;
	    perror(strerror(errno));
    }

	#ifdef IS_DEBUG_ON
		cout << "END:" << __func__ << " " << path << endl;
	#endif

    return res;
    //TODO check if server call is needed 
    //return afsClient->Utimes(path, fi, mode);
}

static int xmp_fsync(const char *path, int isdatasync, struct fuse_file_info *fi){
	if(isdatasync == 0){ 
		return fsync(fi->fh);
	} else {
		return fdatasync(fi->fh);
	}
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
		utimens = xmp_utimens;
		fsync   = xmp_fsync;
	}
} xmp_oper;


int main(int argc, char *argv[])
{
	enum { MAX_ARGS = 10 };
	int i,new_argc;
	char *new_argv[MAX_ARGS];

	string target_str = "localhost:51054";

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

