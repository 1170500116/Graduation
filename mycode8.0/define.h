//
// Created by andyshen on 1/15/21.
//



#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <rdma/rdma_cma.h>

#include <sys/types.h>  
#include<time.h>



typedef uint32_t HASH_VALUE;
typedef uint32_t VALUE_LEN_TYPE;
typedef uint32_t KEY_INDEX_TYPE;
typedef uint32_t BLOCK_INDEX_TYPE;
typedef int32_t HASH_INDEX_TYPE;
// size of record
#define KEY_LEN 16
static const uint8_t VAL_SIZE_LEN = 8;
static const uint8_t RECORD_FIX_LEN = 24;
static const uint32_t VALUE_MAX_LEN_ARR[7] = {16,32,64,128,256,512,1024};
static const uint64_t VALUE_MAX_LEN = 1000000;

// offset of record
static const uint8_t VAL_SIZE_OFFSET = 0;
static const uint8_t KEY_OFFSET = 4;//VAL_SIZE_LEN;
static const uint8_t VALUE_OFFSET = 20;//VERSION_OFFSET + VERSION_LEN;

// aep setting

static uint32_t FILE_SIZE_log = 6400000U;//6.40M
static uint32_t FILE_SIZE_hou = 640000000U;//640M
static uint32_t FILE_SIZE_qian = 0;
static uint32_t FILE_SIZE_zong = 0;
static const uint8_t BLOCK_SIZE = 64;

static const BLOCK_INDEX_TYPE max_block_index_ = 10000000;//(BLOCK_INDEX_TYPE)(FILE_SIZE /  BLOCK_SIZE);
//static const BLOCK_INDEX_TYPE max_block_index_ = 10;



#define ZONG_HASH_MAP_SIZE 1000
#define LOCK_LEN 14
#define LOCK_TIME 1


/* Default BUFFER_SIZE */
#define SEND_RECV_BUFFER_SIZE (100)
#define SRC_DST_BUFFER_SIZE (100)
extern int file_exist;
//#define BUFFER_SIZE (10000)
//#define BUFFER_SIZE (100024)

struct __attribute((packed)) rdma_buffer_attr {
  uint64_t address;
  uint32_t length;
  union stag {
	  /* if we send, we call it local stags */
	  uint32_t local_stag;
	  /* if we receive, we call it remote stag */
	  uint32_t remote_stag;
  }stag;
};

static int server_num = 2;
static char * server_ip[100];
static int server_port[100];
typedef struct resourse_client
{
	/* These are basic RDMA resources */
	/* These are RDMA connection related resources */
	char * server_ip;
	struct sockaddr_in server_sockaddr;	
	struct rdma_event_channel *cm_event_channel ;
	struct rdma_cm_id *cm_client_id ;
	struct ibv_pd *pd ;
	struct ibv_comp_channel *io_completion_channel ;
	struct ibv_cq *client_cq ;
	struct ibv_qp_init_attr qp_init_attr;
	struct ibv_qp *client_qp;
	/* These are memory buffers related resources */
	struct ibv_mr *recv_mr ,  *send_mr;

	struct ibv_send_wr send_wr, *bad_send_wr;
	struct ibv_recv_wr recv_wr, *bad_recv_wr ;
	struct ibv_sge send_sge, recv_sge;

	struct rdma_buffer_attr server_metadata_attr;  
    struct ibv_mr *server_metadata_mr;   
 

	char *send_region;
	char *recv_region;


	/*****************/
	struct ibv_mr * client_dst_mr ,*client_src_mr ;
	struct ibv_send_wr write_wr, *bad_write_wr ;
	struct ibv_send_wr read_wr, *bad_read_wr ;
	struct ibv_sge read_sge,write_sge;    
	char * dst;
	char * src;



    struct rdma_buffer_attr server_metadata_attr_log;
    struct ibv_mr *server_metadata_mr_log; 

	struct ibv_mr * client_src_mr_log,*client_dst_mr_log ;
    struct ibv_send_wr write_wr_log, *bad_write_wr_log ;
	struct ibv_send_wr read_wr_log, *bad_read_wr_log ;
	struct ibv_sge read_sge_log,write_sge_log;

    char * dst_log;
	char * src_log;

	
   
  
}resourse_client;


typedef struct resourse_server
{
     struct sockaddr_in remote_sockaddr;   
     struct rdma_cm_id *cm_client_id;
     struct ibv_pd *pd;
     struct ibv_comp_channel *io_completion_channel;
     struct ibv_cq *cq;
     struct ibv_qp_init_attr qp_init_attr;
     struct ibv_qp *client_qp;
    /* RDMA memory resources */
    char * send_region;
    char * recv_region;
    char * recv_buffer;

    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr; 
    struct ibv_mr *server_buffer_mr; 
    struct ibv_mr *server_metadata_mr ;
    struct ibv_recv_wr recv_wr, *bad_recv_wr;
    struct ibv_send_wr send_wr, *bad_send_wr;
    struct ibv_sge recv_sge, send_sge;
    struct rdma_buffer_attr server_metadata_attr;
   
    
}resourse_server;


