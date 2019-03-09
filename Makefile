CC=gcc
SRC := src
OBJ := obj
GIT_SHA := $(shell git rev-parse HEAD)

$(shell mkdir -p $(OBJ))
$(shell mkdir -p $(OBJ)/utils)

SOURCES=src/utils/adlist.c src/utils/buffer.c src/utils/dict.c src/module.c src/execution_plan.c \
       	src/mgmt.c src/keys_reader.c src/keys_writer.c src/example.c src/filters.c src/mappers.c \
        src/extractors.c src/reducers.c src/record.c src/cluster.c src/commands.c src/streams_reader.c \
        src/globals.c src/config.c src/lock_handler.c
CFLAGS=-fPIC -I./src/ -I./include/ -DREDISMODULE_EXPERIMENTAL_API -DREDISGEARS_GIT_SHA=\"$(GIT_SHA)\" -std=gnu99
LFLAGS=-L./libs/ -Wl,-Bstatic -levent -Wl,-Bdynamic
ifeq ($(DEBUG), 1)
    CFLAGS+=-g -O0 -DVALGRIND
else
	CFLAGS+=-O2 -Wno-unused-result
endif
ifeq ($(WITHPYTHON), 1)
	SOURCES+=src/redisgears_python.c
	PYTHON_CFLAGS=-I./src/deps/cpython/Include/ -I./src/deps/cpython/
	PYTHON_LFLAGS=-L./src/deps/cpython/ -Wl,--whole-archive -Wl,-Bstatic -lpython2.7 -Wl,-Bdynamic -Wl,--no-whole-archive -lutil
	CFLAGS+=-DWITHPYTHON
    CFLAGS+=$(PYTHON_CFLAGS)
    LFLAGS+=$(PYTHON_LFLAGS)
endif

OBJECTS=$(patsubst $(SRC)/%.c, $(OBJ)/%.o, $(SOURCES))

$(OBJ)/%.o: $(SRC)/%.c
	$(CC) -I$(SRC) $(CFLAGS) -c $< -o $@

all: redisgears.so

python:
	cd src/deps/cpython;CFLAGS="-fPIC -DREDIS_ALLOC -DPy_UNICODE_WIDE" ./configure --without-pymalloc;make
	ln -fs /usr/lib/python2.7/plat-x86_64-linux-gnu/_sysconfigdata.py /usr/lib/python2.7/
	ln -fs /usr/lib/python2.7/plat-x86_64-linux-gnu/_sysconfigdata_nd.py /usr/lib/python2.7/

python_clean:
	cd src/deps/cpython;make clean

redisgears.so: $(OBJECTS) $(OBJ)/module_init.o
	$(CC) -shared -o redisgears.so $(OBJECTS) $(OBJ)/module_init.o $(LFLAGS)
	
static: $(OBJECTS)
	ar rcs redisgears.a $(OBJECTS) ./libs/libevent.a

clean:
	rm -f redisgears.so redisgears.a obj/*.o obj/utils/*.o redisgears.zip
	
get_deps: python
	rm -rf deps
	rm -rf libs
	mkdir deps
	mkdir libs
	cd deps;git clone --single-branch --branch release-2.1.8-stable https://github.com/libevent/libevent.git;cd ./libevent/;libtoolize;aclocal;autoheader;autoconf;automake --add-missing;CFLAGS=-fPIC ./configure;make;
	cp ./deps/libevent/.libs/libevent.a ./libs/
	rm -rf deps
	
ramp_pack: all
	ramp pack $(realpath ./redisgears.so) -m ramp.yml -o redisgears.zip
