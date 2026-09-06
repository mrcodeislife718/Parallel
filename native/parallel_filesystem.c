#define _POSIX_C_SOURCE 200809L
#include "parallel_runtime.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

struct parallel_fs_scope { int read_root_fd; int write_root_fd; };

static int invalid_relative_path(const char *path) {
  if (path == NULL || path[0] == '\0' || path[0] == '/') return 1;
  const char *cursor = path;
  while (*cursor != '\0') {
    while (*cursor == '/') cursor++;
    if (*cursor == '\0') break;
    const char *start = cursor;
    while (*cursor != '\0' && *cursor != '/') cursor++;
    size_t length = (size_t)(cursor - start);
    if ((length == 1u && start[0] == '.') || (length == 2u && start[0] == '.' && start[1] == '.')) return 1;
  }
  return 0;
}

static int open_parent(int root_fd, const char *path, int *parent_fd, const char **leaf) {
  if (root_fd < 0 || invalid_relative_path(path) || parent_fd == NULL || leaf == NULL) return -1;
  int current = fcntl(root_fd, F_DUPFD_CLOEXEC, 3);
  if (current < 0) return -errno;
  const char *cursor = path;
  const char *segment = cursor;
  for (;;) {
    while (*cursor != '\0' && *cursor != '/') cursor++;
    if (*cursor == '\0') { *parent_fd = current; *leaf = segment; return 0; }
    size_t length = (size_t)(cursor - segment);
    char name[256];
    if (length == 0u || length >= sizeof(name)) { close(current); return -36; }
    memcpy(name, segment, length); name[length] = '\0';
    int next = openat(current, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (next < 0) { int saved = errno; close(current); return -saved; }
    close(current); current = next; cursor++; segment = cursor;
  }
}

int parallel_fs_scope_create(parallel_runtime *runtime, const char *read_root, const char *write_root, parallel_fs_scope **scope_out) {
  if (runtime == NULL || scope_out == NULL) return -1;
  if (runtime->closed) return -2;
  if (!parallel_runtime_has_capability(runtime, PARALLEL_CAP_FILESYSTEM)) return -3;
  if (read_root == NULL && write_root == NULL) return -1;
  parallel_fs_scope *scope = (parallel_fs_scope *) calloc(1u, sizeof(*scope));
  if (scope == NULL) return -5;
  scope->read_root_fd = -1; scope->write_root_fd = -1;
  if (read_root != NULL) { scope->read_root_fd = open(read_root, O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW); if (scope->read_root_fd < 0) { int saved=errno; free(scope); return -saved; } }
  if (write_root != NULL) { scope->write_root_fd = open(write_root, O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW); if (scope->write_root_fd < 0) { int saved=errno; if(scope->read_root_fd>=0) close(scope->read_root_fd); free(scope); return -saved; } }
  *scope_out = scope; return 0;
}

int parallel_fs_scope_close(parallel_fs_scope *scope) {
  if (scope == NULL) return -1;
  if (scope->read_root_fd >= 0) close(scope->read_root_fd);
  if (scope->write_root_fd >= 0) close(scope->write_root_fd);
  free(scope); return 0;
}

int parallel_fs_open_read(parallel_fs_scope *scope, const char *path, int *descriptor) {
  if (scope == NULL || descriptor == NULL || scope->read_root_fd < 0) return -3;
  int parent=-1; const char *leaf=NULL; int result=open_parent(scope->read_root_fd,path,&parent,&leaf); if(result!=0)return result;
  int fd=openat(parent,leaf,O_RDONLY|O_CLOEXEC|O_NOFOLLOW); int saved=errno; close(parent); if(fd<0)return -saved; *descriptor=fd; return 0;
}

int parallel_fs_open_write(parallel_fs_scope *scope, const char *path, int create, int truncate, int append, uint32_t mode, int *descriptor) {
  if (scope == NULL || descriptor == NULL || scope->write_root_fd < 0) return -3;
  int parent=-1; const char *leaf=NULL; int result=open_parent(scope->write_root_fd,path,&parent,&leaf); if(result!=0)return result;
  int flags=O_WRONLY|O_CLOEXEC|O_NOFOLLOW; if(create)flags|=O_CREAT; if(truncate)flags|=O_TRUNC; if(append)flags|=O_APPEND;
  int fd=openat(parent,leaf,flags,(mode_t)(mode==0?0600u:mode)); int saved=errno; close(parent); if(fd<0)return -saved; *descriptor=fd; return 0;
}

int64_t parallel_fs_read(int descriptor, void *buffer, size_t capacity) { if(descriptor<0||buffer==NULL)return -1; ssize_t r; do{r=read(descriptor,buffer,capacity);}while(r<0&&errno==EINTR); return r<0?-(int64_t)errno:(int64_t)r; }
int64_t parallel_fs_write(int descriptor, const void *buffer, size_t length) { if(descriptor<0||(buffer==NULL&&length!=0))return -1; ssize_t r; do{r=write(descriptor,buffer,length);}while(r<0&&errno==EINTR); return r<0?-(int64_t)errno:(int64_t)r; }
int parallel_fs_sync(int descriptor) { if(descriptor<0)return -1; return fsync(descriptor)==0?0:-errno; }
int parallel_fs_close(int descriptor) { if(descriptor<0)return -1; return close(descriptor)==0?0:-errno; }

int parallel_fs_stat(parallel_fs_scope *scope, const char *path, parallel_fs_stat_info *info) {
  if(scope==NULL||info==NULL||scope->read_root_fd<0)return -3;
  int parent=-1; const char *leaf=NULL; int result=open_parent(scope->read_root_fd,path,&parent,&leaf); if(result!=0)return result;
  struct stat value; if(fstatat(parent,leaf,&value,AT_SYMLINK_NOFOLLOW)!=0){int saved=errno;close(parent);return -saved;} close(parent);
  info->size=(uint64_t)value.st_size; info->mode=(uint32_t)value.st_mode; info->is_file=S_ISREG(value.st_mode)?1:0; info->is_directory=S_ISDIR(value.st_mode)?1:0; info->is_symlink=S_ISLNK(value.st_mode)?1:0; info->modified_ns=((int64_t)value.st_mtim.tv_sec*1000000000ll)+value.st_mtim.tv_nsec; return 0;
}

int parallel_fs_read_dir(parallel_fs_scope *scope, const char *path, parallel_fs_dir_entry *entries, size_t capacity, size_t *count) {
  if(scope==NULL||entries==NULL||count==NULL||scope->read_root_fd<0)return -3;
  int parent=-1; const char *leaf=NULL; int result=open_parent(scope->read_root_fd,path,&parent,&leaf); if(result!=0)return result;
  int dfd=openat(parent,leaf,O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW); int saved=errno; close(parent); if(dfd<0)return -saved;
  DIR *dir=fdopendir(dfd); if(dir==NULL){saved=errno;close(dfd);return -saved;} size_t used=0; errno=0;
  for(;;){struct dirent *entry=readdir(dir); if(entry==NULL)break; if(strcmp(entry->d_name,".")==0||strcmp(entry->d_name,"..")==0)continue; if(used>=capacity){closedir(dir);return -12;} size_t length=strlen(entry->d_name); if(length>=sizeof(entries[used].name)){closedir(dir);return -36;} memcpy(entries[used].name,entry->d_name,length+1u); entries[used].type=(uint8_t)entry->d_type; used++;}
  saved=errno; closedir(dir); if(saved!=0)return -saved; *count=used; return 0;
}
