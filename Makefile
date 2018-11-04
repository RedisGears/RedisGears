CC=gcc
SOURCES=src/utils/adlist.c src/utils/buffer.c src/utils/dict.c src/module.c src/execution_plan.c \
        src/mgmt.c src/keys_reader.c src/keys_writer.c src/example.c src/filters.c src/mappers.c \
        src/extractors.c src/reducers.c src/record.c src/cluster.c
CFLAGS=-fPIC -std=gnu99 -I./src/ -DREDISMODULE_EXPERIMENTAL_API -DVALGRIND -std=gnu99
LFLAGS= 
ifeq ($(DEBUG), 1)
    CFLAGS+=-g
endif
ifeq ($(WITHPYTHON), 1)
	SOURCES+=src/redistar_python.c
	PYTHON_CFLAGS=-I/usr/include/python2.7 -I/usr/include/x86_64-linux-gnu/python2.7
	PYTHON_LFLAGS=-Bsymbolic -lc -L/usr/lib/python2.7/config-x86_64-linux-gnu -L/usr/lib -lpython2.7 -lpthread -ldl -lutil -lm
	CFLAGS+=-DWITHPYTHON
    CFLAGS+=$(PYTHON_CFLAGS)
    LFLAGS+=PYTHON_LFLAGS
endif

all: $(SOURCES)
	$(CC) -shared -o redistar.so $(CFLAGS) $(SOURCES) $(PYTHON_LFLAGS)

clean:
	rm *.so *.o src/*.o
