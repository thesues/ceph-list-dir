CFLAGS=-std=c++11 -g
del_dir:list_dir.o
	g++ ${CFLAGS} -o $@ $^ -lcephfs
list_dir.o:list_dir.cc
	g++ ${CFLAGS}  -c $<
