# .PHONY: clean

# CFLAGS  := -Wall -g
# LD      := gcc
# LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread -pmem -lpmemobj

# APPS    := server client

# all: ${APPS}

# client: rdma-common.o client.o
# 	${LD} -o $@ $^ ${LDLIBS}

# server: rdma-common.o server.o
# 	${LD} -o $@ $^ ${LDLIBS}


# clean:
# 	rm -f ${APPS}


.PHONY: clean

CFLAGS  := -Wall  -g
LD      := gcc
LDFLAGS := ${LDFLAGS} -lrdmacm -lpthread -libverbs -lpmem -lpmemobj

APPS    := nvm_engine.o rdma-common.o client server


all: ${APPS}
nvm_engine.o: nvm_engine.c nvm_engine.h
	#${LD} -o $@ $^ ${LDFLAGS}
	${LD} -o nvm_engine.o  -c nvm_engine.c  ${LDFLAGS} 

rdma-common.o: rdma_common.c rdma_common.h
	#${LD} -o $@ $^ ${LDFLAGS}
	${LD} -o rdma-common.o  -c rdma_common.c  ${LDFLAGS}
client: rdma-common.o client.o
	#${LD} -o $@  -c rdma_common.c  ${LDFLAGS}
	#gcc -o client memctl.c nvm_engine.c rdma_common.c client.c  ${LDFLAGS}
	${LD} -o $@ $^ ${LDFLAGS}

server: rdma-common.o  nvm_engine.o server.o
	${LD} -o $@ $^ ${LDFLAGS}

clean:
	rm -f *.o ${APPS}


