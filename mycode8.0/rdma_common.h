/*
 * Header file for the common RDMA routines used in the server/client example 
 * program. 
 *
 * Author: Animesh Trivedi 
 *          atrivedi@apache.org 
 *
 */

#ifndef RDMA_COMMON_H
#define RDMA_COMMON_H


#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include "define.h"
#include <netdb.h>
#include <netinet/in.h>	
#include <arpa/inet.h>
#include <sys/socket.h>



#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

/* Error Macro*/
#define rdma_error(msg, args...) do {\
	fprintf(stderr, "%s : %d : ERROR : "msg, __FILE__, __LINE__, ## args);\
}while(0);

// #ifndef ACN_RDMA_DEBUG
// #define ACN_RDMA_DEBUG
// #endif /* ACN_RDMA_DEBUG */

#ifdef ACN_RDMA_DEBUG 
/* Debug Macro */
#define debug(msg, args...) do {\
    printf("DEBUG: "msg, ## args);\
}while(0);

#else 

#define debug(msg, args...) 

#endif /* ACN_RDMA_DEBUG */

/* Capacity of the completion queue (CQ) */
#define CQ_CAPACITY (64)//32
/* MAX SGE capacity */
#define MAX_SGE (16)//4
/* MAX work requests */
#define MAX_WR (32)//16
/* Default port where the RDMA server is listening */

/* 
 * We use attribute so that compiler does not step in and try to pad the structure.
 * We use this structure to exchange information between the server and the client. 
 *
 * For details see: http://gcc.gnu.org/onlinedocs/gcc/Type-Attributes.html
 */


/* resolves a given destination name to sin_addr */
int get_addr(char *dst, struct sockaddr *addr);


/* prints RDMA buffer info structure */
void show_rdma_buffer_attr(struct rdma_buffer_attr *attr);

/* 
 * Processes an RDMA connection management (CM) event. 
 * @echannel: CM event channel where the event is expected. 
 * @expected_event: Expected event type 
 * @cm_event: where the event will be stored 
 */
int process_rdma_cm_event(struct rdma_event_channel *echannel, 
		enum rdma_cm_event_type expected_event,
		struct rdma_cm_event **cm_event);




/* This function registers a previously allocated memory. Returns a memory region 
 * (MR) identifier or NULL on error.
 * @pd: protection domain where to register memory 
 * @addr: Buffer address 
 * @length: Length of the buffer 
 * @permission: OR of IBV_ACCESS_* permissions as defined for the enum ibv_access_flags
 */
struct ibv_mr *rdma_buffer_register(struct ibv_pd *pd, 
		void *addr, 
		uint32_t length, 
		enum ibv_access_flags permission);


/* Processes a work completion (WC) notification. 
 * @comp_channel: Completion channel where the notifications are expected to arrive 
 * @wc: Array where to hold the work completion elements 
 * @max_wc: Maximum number of expected work completion (WC) elements. wc must be 
 *          atleast this size.
 */
int process_work_completion_events(struct ibv_comp_channel *comp_channel, 
		struct ibv_wc *wc, 
		int max_wc);

int process_rdma_cm_event1(struct rdma_event_channel *echannel,
		struct rdma_cm_event **cm_event);
/***************************************

client functions

***************************************/
// int client_prepare_connection(struct resourse_client ** res,struct sockaddr_in *s_addr,int i);
// int client_connect_to_server(resourse_client** res,int i) ;
// int recv_server_metadata(resourse_client** res,int i);
// int register_all(resourse_client** res,int SEND_REGION_MAX_LEN,int i);
// int client_disconnect_and_clean(resourse_client** res,int i);

// int post_send_client(resourse_client** res,int i);
// int post_recv(resourse_client** res,int i);
// int post_read(resourse_client** res,uint64_t offset,uint64_t record_len,int i);
// int post_write(resourse_client** res,uint64_t offset,uint64_t record_len,int i);

/***************************************

server functions

***************************************/

// int start_rdma_server(struct rdma_event_channel* cm_event_channel,struct rdma_cm_id *cm_server_id,int port,int MAX_client_num);
// int wait_for_client(struct rdma_event_channel* cm_event_channel,struct resourse_server** res,int i) ;
// int setup_client_resources(struct resourse_server** res,int i);
// int accept_client_connection(struct rdma_event_channel *cm_event_channel,struct resourse_server** res,int i);
// int send_server_metadata(struct resourse_server** res,char * filebuffer,uint32_t filesize,int i);
// int register_send_recv(struct resourse_server** res,int SEND_REGION_MAX_LEN,int i);
// int recv_data_and_post_next_recv(struct resourse_server** res,int i,int verbose) ;
// int disconnect_and_cleanup_client(struct rdma_event_channel *cm_event_channel,struct resourse_server** res,int i);
// int cleanup_server(struct rdma_event_channel *cm_event_channel,struct rdma_cm_id *cm_server_id);

// int post_send_server(resourse_server** res,int i,int verbose);
#endif /* RDMA_COMMON_H */



