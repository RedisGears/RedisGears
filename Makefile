CC=gcc
SRC := src
OBJ := obj

$(shell mkdir -p $(OBJ))
$(shell mkdir -p $(OBJ)/utils)

SOURCES=src/utils/adlist.c src/utils/buffer.c src/utils/dict.c src/module.c src/execution_plan.c \
       	src/mgmt.c src/keys_reader.c src/keys_writer.c src/example.c src/filters.c src/mappers.c \
        src/extractors.c src/reducers.c src/record.c src/cluster.c src/commands.c src/streams_reader.c \
        src/globals.c
CFLAGS=-fPIC -I./src/ -I./include/ -DREDISMODULE_EXPERIMENTAL_API -std=gnu99
LFLAGS=-L./libs/ -Wl,-Bstatic -levent -Wl,-Bdynamic
ifeq ($(DEBUG), 1)
    CFLAGS+=-O0 -g -DVALGRIND
else
	CFLAGS+=-O2 -g -DVALGRIND
endif
ifeq ($(WITHPYTHON), 1)
	SOURCES+=src/redisgears_python.c
	PYTHON_CFLAGS=$(shell python-config --includes)
	PYTHON_LFLAGS=-L$(shell python-config --configdir) -L/usr/lib $(shell python-config --libs)
	CFLAGS+=-DWITHPYTHON
    CFLAGS+=$(PYTHON_CFLAGS)
    LFLAGS+=$(PYTHON_LFLAGS)
endif

OBJECTS=$(patsubst $(SRC)/%.c, $(OBJ)/%.o, $(SOURCES))

$(OBJ)/%.o: $(SRC)/%.c
	$(CC) -I$(SRC) $(CFLAGS) -c $< -o $@

all: redisgears.so
	
redisgears.so: $(OBJECTS) $(OBJ)/module_init.o
	$(CC) -shared -o redisgears.so $(OBJECTS) $(OBJ)/module_init.o $(LFLAGS)
	
static: $(OBJECTS)
	ar rcs redisgears.a $(OBJECTS) ./libs/libevent.a

clean:
	rm -f redisgears.so redisgears.a obj/*.o obj/utils/*.o redisgears.zip
	
get_deps:
	rm -rf deps
	rm -rf libs
	mkdir deps
	mkdir libs
	cd deps;git clone https://github.com/libevent/libevent.git;cd ./libevent/;git checkout release-2.1.8-stable;libtoolize;aclocal;autoheader;autoconf;automake --add-missing;CFLAGS=-fPIC ./configure;make;
	cp ./deps/libevent/.libs/libevent.a ./libs/
	rm -rf deps
	
ramp_pack: all
	ramp pack $(realpath ./redisgears.so) -m ramp.yml -o redisgears.zip
