#include <stdio.h>
#define __USE_FILE_OFFSET64
#include <cephfs/libcephfs.h>
#include <errno.h>
#include <fcntl.h>
#include <string>
#include <cstring>
#include <iostream>
#include <time.h>
#include <pthread.h>
#include <vector>
#include <dirent.h>
#include <thread>
#include "thread_safe_queue.h"

int debug = 0;

ThreadSafeQueue<std::string> queue(4096);
std::vector<std::string> dirs;

void list_dir(std::string path, struct ceph_mount_info *cmount) {
	ceph_dir_result *dr = NULL;
	int r = ceph_opendir(cmount, path.c_str(), &dr);
	if (r != 0) {
		std::cout << "Failed to open path: " << std::strerror(-r) << std::endl;
		return;
	}
	struct dirent de;
	struct ceph_statx stx;
	while ((r = ceph_readdirplus_r(cmount, dr, &de, &stx, CEPH_STATX_INO, AT_NO_ATTR_SYNC, NULL)) > 0) {
		if (r < 0) {
			std::cout << "Error reading path " << path << ": " << std::strerror(r)
				<< std::endl;
			ceph_closedir(cmount, dr); // best effort, ignore r
			return;
		}
		if (std::string(de.d_name) == "." || std::string(de.d_name) == "..") continue;

		std::string de_path = (path + std::string("/") + de.d_name);
		if (S_ISREG(stx.stx_mode)) {
			queue.push(de_path);
		} else if (S_ISDIR(stx.stx_mode)) {
			list_dir(de_path, cmount);
		} else {
			std::cout << "Skipping non reg/dir file: " << de_path << std::endl;
		}
	}
	if(ceph_closedir(cmount, dr)<0) return; 

	//push directory name to vector
	dirs.push_back(path);
}

int main(int argc , char *argv[]) {
	if (argc < 2) {
		std::cout << "usage: ./list_dir <dirname>" << std::endl;
		return -1;
	}
	int ret;
	struct ceph_mount_info *cmount;	
	if ((ret = ceph_create(&cmount, NULL)) != 0) {
		perror("ceph_create failed");
		return 0;
	}
	if ((ret = ceph_conf_read_file(cmount, NULL)) != 0 ) {
		perror("ceph_conf_read failed");
		return 0;
	}
	if ((ret = ceph_mount(cmount, NULL)) != 0) {
		perror("ceph_mount failed");
		return 0;
	}

	//pthread_create...
	std::vector<std::thread> workers;
	for(int t = 0 ; t < 128; t ++) {
		workers.push_back(std::thread([&](){
                ThreadSafeQueue<std::string>::QueueResult result;
		std::string path;
		int r;
                while ((result = queue.pop(path)) != ThreadSafeQueue<std::string>::CLOSED) {
                
			std::cout << "regular file:" <<  path << std::endl;
                }
		}));
	}

	list_dir(argv[1], cmount);

	queue.close();
    	for (auto & w : workers)
        	w.join();
}

