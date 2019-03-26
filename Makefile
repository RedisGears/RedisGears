WITHPYTHON ?= 1

CC=gcc
SRC := src
OBJ := obj
GIT_SHA := $(shell git rev-parse HEAD)
OS := $(shell sh -c 'uname -s 2>/dev/null || echo not')

$(shell mkdir -p $(OBJ))
$(shell mkdir -p $(OBJ)/utils)

ifndef PACKAGE_NAME
	PACKAGE_NAME := redisgears
endif

ifndef CIRCLE_BRANCH
	CIRCLE_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
endif

ifndef PYTHON_ENCODING_FLAG
	PYTHON_ENCODING_FLAG := --enable-unicode=ucs2
endif

CPYTHON_PREFIX=/opt/redislabs/lib/modules/python27

CPYTHON_FLAGS=$(PYTHON_ENCODING_FLAG) \
	--prefix=$(CPYTHON_PREFIX) --with-zlib --with-ssl --with-readline

ifndef OS_VERSION
	OS_VERSION := linux
endif

CPYTHON_PATH := $(realpath ./src/deps/cpython/)

SOURCES=src/utils/adlist.c src/utils/buffer.c src/utils/dict.c src/module.c src/execution_plan.c \
       	src/mgmt.c src/keys_reader.c src/keys_writer.c src/example.c src/filters.c src/mappers.c \
        src/extractors.c src/reducers.c src/record.c src/cluster.c src/commands.c src/streams_reader.c \
        src/globals.c src/config.c src/lock_handler.c

CFLAGS=-fPIC -I./src/ -I./include/ -DREDISMODULE_EXPERIMENTAL_API -DREDISGEARS_GIT_SHA=\"$(GIT_SHA)\" -DCPYTHON_PATH=\"$(CPYTHON_PATH)/\" -std=gnu99

ifeq ($(OS),Linux)
	LFLAGS=-L./libs/ -Wl,-Bstatic -levent -Wl,-Bdynamic
	LIBTOOLIZE=libtoolize
else
	LFLAGS=-L./libs/ -static -levent -dynamic -undefined dynamic_lookup -lc
	LIBTOOLIZE=glibtoolize
endif

ifeq ($(DEBUG), 1)
    CFLAGS+=-g -O0 -DVALGRIND
else
	CFLAGS+=-O2 -Wno-unused-result
endif

ifeq ($(WITHPYTHON), 1)
	SOURCES+=src/redisgears_python.c
	PYTHON_CFLAGS=-I./src/deps/cpython/Include/ -I./src/deps/cpython/
	ifeq ($(OS),Linux)
		PYTHON_LFLAGS=-L./src/deps/cpython/ -Wl,--whole-archive -Wl,-Bstatic -lpython2.7 -Wl,-Bdynamic -Wl,--no-whole-archive -lutil
	else
		PYTHON_LFLAGS=-L./src/deps/cpython/ -all_load -static -lpython2.7 -dynamic -lutil
	endif
	CFLAGS+=-DWITHPYTHON
    CFLAGS+=$(PYTHON_CFLAGS)
    LFLAGS+=$(PYTHON_LFLAGS)
endif

OBJECTS=$(patsubst $(SRC)/%.c, $(OBJ)/%.o, $(SOURCES))

$(OBJ)/%.o: $(SRC)/%.c
	$(CC) -I$(SRC) $(CFLAGS) -c $< -o $@

.PHONY: all python python_clean pyenv static clean get_deps ramp_pack

all: GearsBuilder.py redisgears.so

python:
	cd src/deps/cpython; \
	CFLAGS="-fPIC -DREDIS_ALLOC" ./configure --without-pymalloc $(CPYTHON_FLAGS); \
	make

python_clean:
	cd src/deps/cpython;make clean

pyenv:
	make -C src/deps/cpython install > $(PWD)/python-install.log
	cp pyenv/Pipfile* $(CPYTHON_PREFIX)
	cd $(CPYTHON_PREFIX); \
	export PIPENV_VENV_IN_PROJECT=1; \
	pipenv install
	cp $(CPYTHON_PREFIX)/Pipfile.lock pyenv/

redisgears.so: $(OBJECTS) $(OBJ)/module_init.o
	$(CC) -shared -o redisgears.so $(OBJECTS) $(OBJ)/module_init.o $(LFLAGS)
	
GearsBuilder.py:
	xxd -i src/GearsBuilder.py > src/GearsBuilder.auto.h
	xxd -i src/cloudpickle.py > src/cloudpickle.auto.h

static: $(OBJECTS)
	ar rcs redisgears.a $(OBJECTS) ./libs/libevent.a

clean:
	rm -f redisgears.so redisgears.a obj/*.o obj/utils/*.o artifacts/release/*.zip artifacts/snapshot/*.zip
	
get_deps: python
	rm -rf deps
	rm -rf libs
	mkdir deps
	mkdir libs
	cd deps;git clone --single-branch --branch release-2.1.8-stable https://github.com/libevent/libevent.git;cd ./libevent/;$(LIBTOOLIZE);aclocal;autoheader;autoconf;automake --add-missing;CFLAGS=-fPIC ./configure;make;
	cp ./deps/libevent/.libs/libevent.a ./libs/
	rm -rf deps

ramp_pack: all
	mkdir -p artifacts
	mkdir -p artifacts/snapshot
	mkdir -p artifacts/release
	$(eval SNAPSHOT=$(shell PYTHONWARNINGS=ignore PYTHON_HOME_DIR=$(CPYTHON_PATH)/ ramp pack $(realpath ./redisgears.so) -m ramp.yml -o {os}-$(OS_VERSION)-{architecture}.$(CIRCLE_BRANCH).zip | tail -1))
	$(eval DEPLOY=$(shell PYTHONWARNINGS=ignore PYTHON_HOME_DIR=$(CPYTHON_PATH)/ ramp pack $(realpath ./redisgears.so) -m ramp.yml -o {os}-$(OS_VERSION)-{architecture}.{semantic_version}.zip | tail -1))
	mv ./$(SNAPSHOT) artifacts/snapshot/$(PACKAGE_NAME).$(SNAPSHOT)
	mv ./$(DEPLOY) artifacts/release/$(PACKAGE_NAME).$(DEPLOY)
	zip -rq -y artifacts/release/$(PACKAGE_NAME)-dependencies.$(DEPLOY) $(CPYTHON_PREFIX)/
