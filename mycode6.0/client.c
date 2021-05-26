#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include<time.h>
#include <rdma/rdma_cma.h>
#include "rdma_common.h"
#include "define.h"
#include <sys/select.h>


typedef struct resourse
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
}resourse;


resourse* res[server_num];



/*****************/
static int get(char* key,size_t* value_len,char ** value,int i);//0
static int put(char* key,char * value,int i);//1
static int del(char* key,int i);//2
#define SEND_REGION_MAX_LEN 100

int verbose=0;
int itimes =0;
int ioshu = 0;
char *IPbuffer;
int ip_offset=0;
pid_t pid =0;
int period =3000;//ms
/* This function prepares client side connection resources for an RDMA connection */
HASH_VALUE DJBHash(char* _str) {
  unsigned int hash = 5381;
  int len = KEY_LEN;
  int i;
  for( i = KEY_LEN-1;i>=0;i--){
      if(_str[i]!=' '){
        len = i+1;
        break;
      }
  }
//   printf("%s,%ld",_str,strlen(_str));
//   printf("len:%d\n",len);
  for ( i = 0; i < len; ++_str, ++i) {
    hash = ((hash << 5) + hash) + (*_str);
  }
  return hash%ZONG_HASH_MAP_SIZE;
}

int select_server(char key[KEY_LEN]){
	  HASH_VALUE hash_val =  DJBHash(key);  
	  
     // printf("hash_val:%d\n",hash_val);
	  int jiange = ZONG_HASH_MAP_SIZE/server_num;
	  int i=0;
	  int tmp = jiange;
	  for(i=0;i<server_num;i++){
		  if(hash_val<tmp){
			  return i;
		  }
		  tmp+=jiange;
	  } 
    //   printf("select %d",i);
      return i;
	
}
static int client_prepare_connection(struct sockaddr_in *s_addr,int i)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/*  Open a channel used to report asynchronous communication event */
	res[i]->cm_event_channel = rdma_create_event_channel();
	if (!res[i]->cm_event_channel) {
		rdma_error("Creating cm event channel failed, errno: %d \n", -errno);
		return -errno;
	}
	//debug("RDMA CM event channel is created at : %p \n", cm_event_channel);
	/* rdma_cm_id is the connection identifier (like socket) which is used 
	 * to define an RDMA connection. 
	 */
	ret = rdma_create_id(res[i]->cm_event_channel, &res[i]->cm_client_id, 
			NULL,
			RDMA_PS_TCP);
	if (ret) {
		rdma_error("Creating cm id failed with errno: %d \n", -errno); 
		return -errno;
	}
	/* Resolve destination and optional source addresses from IP addresses  to
	 * an RDMA address.  If successful, the specified rdma_cm_id will be bound
	 * to a local device. */
	ret = rdma_resolve_addr(res[i]->cm_client_id, NULL, (struct sockaddr*) s_addr, 2000);
	if (ret) {
		rdma_error("Failed to resolve address, errno: %d \n", -errno);
		return -errno;
	}
	//debug("waiting for cm event: RDMA_CM_EVENT_ADDR_RESOLVED\n");
	ret  = process_rdma_cm_event(res[i]->cm_event_channel, 
			RDMA_CM_EVENT_ADDR_RESOLVED,
			&cm_event);
	if (ret) {
		rdma_error("Failed to receive a valid event, ret = %d \n", ret);
		return ret;
	}
	/* we ack the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the CM event, errno: %d\n", -errno);
		return -errno;
	}
	//debug("RDMA address is resolved \n");

	 /* Resolves an RDMA route to the destination address in order to 
	  * establish a connection */
	ret = rdma_resolve_route(res[i]->cm_client_id, 2000);
	if (ret) {
		rdma_error("Failed to resolve route, erno: %d \n", -errno);
	       return -errno;
	}
	//debug("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED\n");
	ret = process_rdma_cm_event(res[i]->cm_event_channel, 
			RDMA_CM_EVENT_ROUTE_RESOLVED,
			&cm_event);
	if (ret) {
		rdma_error("Failed to receive a valid event, ret = %d \n", ret);
		return ret;
	}
	/* we ack the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the CM event, errno: %d \n", -errno);
		return -errno;
	}
	printf("Trying to connect to server at : %s port: %d \n", 
			inet_ntoa(s_addr->sin_addr),
			ntohs(s_addr->sin_port));
	/* Protection Domain (PD) is similar to a "process abstraction" 
	 * in the operating system. All resources are tied to a particular PD. 
	 * And accessing recourses across PD will result in a protection fault.
	 */
	res[i]->pd = ibv_alloc_pd(res[i]->cm_client_id->verbs);
	if (!res[i]->pd) {
		rdma_error("Failed to alloc pd, errno: %d \n", -errno);
		return -errno;
	}
	//debug("pd allocated at %p \n", pd);
	/* Now we need a completion channel, were the I/O completion 
	 * notifications are sent. Remember, this is different from connection 
	 * management (CM) event notifications. 
	 * A completion channel is also tied to an RDMA device, hence we will 
	 * use cm_client_id->verbs. 
	 */
	res[i]->io_completion_channel = ibv_create_comp_channel(res[i]->cm_client_id->verbs);
	if (!res[i]->io_completion_channel) {
		rdma_error("Failed to create IO completion event channel, errno: %d\n",
			       -errno);
	return -errno;
	}
	//debug("completion event channel created at : %p \n", io_completion_channel);
	/* Now we create a completion queue (CQ) where actual I/O 
	 * completion metadata is placed. The metadata is packed into a structure 
	 * called struct ibv_wc (wc = work completion). ibv_wc has detailed 
	 * information about the work completion. An I/O request in RDMA world 
	 * is called "work" ;) 
	 */
	res[i]->client_cq = ibv_create_cq(res[i]->cm_client_id->verbs /* which device*/, 
			CQ_CAPACITY /* maximum capacity*/, 
			NULL /* user context, not used here */,
			res[i]->io_completion_channel /* which IO completion channel */, 
			0 /* signaling vector, not used here*/);
	if (!res[i]->client_cq) {
		rdma_error("Failed to create CQ, errno: %d \n", -errno);
		return -errno;
	}
	//debug("CQ created at %p with %d elements \n", client_cq, client_cq->cqe);
	ret = ibv_req_notify_cq(res[i]->client_cq, 0);
	if (ret) {
		rdma_error("Failed to request notifications, errno: %d\n", -errno);
		return -errno;
	}
       /* Now the last step, set up the queue pair (send, recv) queues and their capacity.
         * The capacity here is define statically but this can be probed from the 
	 * device. We just use a small number as defined in rdma_common.h */
	bzero(&res[i]->qp_init_attr, sizeof (res[i]->qp_init_attr));
	res[i]->qp_init_attr.cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
	res[i]->qp_init_attr.cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
	res[i]->qp_init_attr.cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
	res[i]->qp_init_attr.cap.max_send_wr = MAX_WR; /* Maximum send posting capacity */
	res[i]->qp_init_attr.qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
	/* We use same completion queue, but one can use different queues */
	res[i]->qp_init_attr.recv_cq = res[i]->client_cq; /* Where should I notify for receive completion operations */
	res[i]->qp_init_attr.send_cq = res[i]->client_cq; /* Where should I notify for send completion operations */
	/*Lets create a QP */
	ret = rdma_create_qp(res[i]->cm_client_id /* which connection id */,
			res[i]->pd /* which protection domain*/,
			&res[i]->qp_init_attr /* Initial attributes */);
	if (ret) {
		rdma_error("Failed to create QP, errno: %d \n", -errno);
	       return -errno;
	}
	res[i]->client_qp = res[i]->cm_client_id->qp;
	//debug("QP created at %p \n", client_qp);
	return 0;
}

/* Pre-posts a receive buffer before calling rdma_connect () */
static int post_recv(int i)
{
	int ret = -1;
	memset(res[i]->recv_region,0,BUFFER_SIZE);
	ret = ibv_post_recv(res[i]->client_qp /* which QP */,
		      &res[i]->recv_wr /* receive work request*/,
		      &res[i]->bad_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
//	debug("Receive buffer pre-posting is successful \n");
	return 0;
}

/* Connects to the RDMA server */
static int client_connect_to_server(int i) 
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	bzero(&conn_param, sizeof(conn_param));
	conn_param.initiator_depth = 3;
	conn_param.responder_resources = 3;
	conn_param.retry_count = 7; // if fail, then how many times to retry
	ret = rdma_connect(res[i]->cm_client_id, &conn_param);
	if (ret) {
		rdma_error("Failed to connect to remote host , errno: %d\n", -errno);
		return -errno;
	}
	//debug("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED\n");
	ret = process_rdma_cm_event(res[i]->cm_event_channel, 
			RDMA_CM_EVENT_ESTABLISHED,
			&cm_event);
	if (ret) {
		rdma_error("Failed to get cm event, ret = %d \n", ret);
	       return ret;
	}
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge cm event, errno: %d\n", 
			       -errno);
		return -errno;
	}
	printf("The client is connected successfully \n");
	return 0;
}

static int recv_server_metadata(int i){
	struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr = NULL;
	struct ibv_sge  server_recv_sge;
	int ret = -1;

	res[i]->server_metadata_mr = rdma_buffer_register(res[i]->pd,
			&res[i]->server_metadata_attr,
			sizeof(res[i]->server_metadata_attr),
			(IBV_ACCESS_LOCAL_WRITE));
	if(!res[i]->server_metadata_mr){
		rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
		return -ENOMEM;
	}
	server_recv_sge.addr = (uint64_t) res[i]->server_metadata_mr->addr;
	server_recv_sge.length = (uint32_t) res[i]->server_metadata_mr->length;
	server_recv_sge.lkey = (uint32_t) res[i]->server_metadata_mr->lkey;
	/* now we link it to the request */
	bzero(&server_recv_wr, sizeof(server_recv_wr));
	server_recv_wr.sg_list = &server_recv_sge;
	server_recv_wr.num_sge = 1;
	ret = ibv_post_recv(res[i]->client_qp /* which QP */,
		      &server_recv_wr /* receive work request*/,
		      &bad_server_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
	//debug("Receive buffer pre-posting is successful \n");
	struct ibv_wc wc;
	ret = process_work_completion_events(res[i]->io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}	
	show_rdma_buffer_attr(&res[i]->server_metadata_attr);
	return 0;
}
static int register_all(int i){
	res[i]->send_mr = rdma_buffer_register(res[i]->pd,
			res[i]->send_region,
			SEND_REGION_MAX_LEN,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if(!res[i]->send_mr){
		rdma_error("Failed to register the send region\n");
		return -1;
	}
	/* now we fill up SGE */
	res[i]->send_sge.addr = (uint64_t) res[i]->send_mr->addr;
	res[i]->send_sge.length = (uint32_t) res[i]->send_mr->length;
	res[i]->send_sge.lkey = res[i]->send_mr->lkey;
	/* now we link to the send work request */
	bzero(&res[i]->send_wr, sizeof(res[i]->send_wr));
	res[i]->send_wr.sg_list = &res[i]->send_sge;
	res[i]->send_wr.num_sge = 1;
	res[i]->send_wr.opcode = IBV_WR_SEND;
	res[i]->send_wr.send_flags = IBV_SEND_SIGNALED;


	res[i]->recv_mr = rdma_buffer_register(res[i]->pd,
			res[i]->recv_region,
			BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE));
	if(!res[i]->recv_mr){
		rdma_error("Failed to register the recv region\n");
		return -1;
	}
	res[i]->recv_sge.addr = (uint64_t)res[i]->recv_mr->addr;
	res[i]->recv_sge.length = (uint32_t) res[i]->recv_mr->length;
	res[i]->recv_sge.lkey = (uint32_t) res[i]->recv_mr->lkey;
	/* now we link it to the request */
	bzero(&res[i]->recv_wr, sizeof(res[i]->recv_wr));
	res[i]->recv_wr.sg_list = &res[i]->recv_sge;
	res[i]->recv_wr.num_sge = 1;

	res[i]->client_src_mr = rdma_buffer_register(res[i]->pd,
			res[i]->src,
			BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if (!res[i]->client_src_mr) {
		rdma_error("We failed to create the source buffer, -ENOMEM\n");
		return -ENOMEM;
	}
	res[i]->client_dst_mr = rdma_buffer_register(res[i]->pd,
			res[i]->dst,
			BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE | 
			 IBV_ACCESS_REMOTE_WRITE | 
			 IBV_ACCESS_REMOTE_READ));
	
	if (!res[i]->client_dst_mr) {
		rdma_error("We failed to create the destination buffer, -ENOMEM\n");
		return -ENOMEM;
	}
	/* Step 1: is to copy the local buffer into the remote buffer. We will 
	 * reuse the previous variables. */
	/* now we fill up SGE */
	res[i]->write_sge.addr = (uint64_t) res[i]->client_src_mr->addr;
	res[i]->write_sge.length = (uint32_t) res[i]->client_src_mr->length;
	res[i]->write_sge.lkey = res[i]->client_src_mr->lkey;
	/* now we link to the send work request */
	bzero(&res[i]->write_wr, sizeof(res[i]->write_wr));
	res[i]->write_wr.sg_list = &res[i]->write_sge;
	res[i]->write_wr.num_sge = 1;
	res[i]->write_wr.opcode = IBV_WR_RDMA_WRITE;
	res[i]->write_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	res[i]->write_wr.wr.rdma.rkey = res[i]->server_metadata_attr.stag.remote_stag;
	res[i]->write_wr.wr.rdma.remote_addr = res[i]->server_metadata_attr.address;
	//printf("remote_addr:%p\n",write_wr.wr.rdma.remote_addr);




	res[i]->read_sge.addr = (uint64_t) res[i]->client_dst_mr->addr;
	res[i]->read_sge.length = (uint32_t) res[i]->client_dst_mr->length;
	res[i]->read_sge.lkey = res[i]->client_dst_mr->lkey;
	/* now we link to the send work request */
	bzero(&res[i]->read_wr, sizeof(res[i]->read_wr));
	res[i]->read_wr.sg_list = &res[i]->read_sge;
	res[i]->read_wr.num_sge = 1;
	res[i]->read_wr.opcode = IBV_WR_RDMA_READ;
	res[i]->read_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	res[i]->read_wr.wr.rdma.rkey = res[i]->server_metadata_attr.stag.remote_stag;
	res[i]->read_wr.wr.rdma.remote_addr =res[i]->server_metadata_attr.address;

	return 0;
}
static int post_send(int i)
{
	struct ibv_wc wc;
	int ret = -1;
	
	/* Now we post it */
	ret = ibv_post_send(res[i]->client_qp, 
		       &res[i]->send_wr,
	       &res[i]->bad_send_wr);
	if (ret) {
		rdma_error("Failed to send client metadata, errno: %d \n", 
				-errno);
		return -errno;
	}
	/* at this point we are expecting 2 work completion. One for our 
	 * send and one for recv that we will get from the server for 
	 * its buffer information */
	ret = process_work_completion_events(res[i]->io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}
	// debug("Server sent us its buffer location and credentials, showing \n");
	// printf("sended message:%s,len:%d\n",send_region,strlen(send_region));
	return 0;
}

/* This function does :
 * 1) Prepare memory buffers for RDMA operations 
 * 1) RDMA write from src -> remote buffer 
 * 2) RDMA read from remote bufer -> dst
 */ 
static int post_write(uint64_t offset,uint64_t record_len,int i){
	struct ibv_wc wc;
	int ret = -1;
	res[i]->write_sge.length = record_len;
	res[i]->write_wr.wr.rdma.remote_addr = res[i]->server_metadata_attr.address+offset;
	//printf("write offset:%ld\n",offset);
	/* Now we post it */
	ret = ibv_post_send(res[i]->client_qp, 
		       &res[i]->write_wr,
	       &res[i]->bad_write_wr);
	if (ret) {
		rdma_error("Failed to write client src buffer, errno: %d \n", 
				-errno);
		return -errno;
	}
		
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(res[i]->io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}
	//debug("Client side WRITE is complete \n");
	//printf("src:%s\n",src);
	return 0;
}
static int post_read(uint64_t offset,uint64_t record_len,int i){
	struct ibv_wc wc;
	int ret = -1;
	memset(res[i]->dst,0,BUFFER_SIZE);
	res[i]->read_sge.length = (uint32_t) record_len;
	//printf(" read offset=%ld\n",offset);
	res[i]->read_wr.wr.rdma.remote_addr = res[i]->server_metadata_attr.address+offset;
	/* Now we post it */
	ret = ibv_post_send(res[i]->client_qp, 
		       &res[i]->read_wr,
	       &res[i]->bad_read_wr);
	if (ret) {
		rdma_error("Failed to read client dst buffer from the master, errno: %d \n", 
				-errno);
		return -errno;
	}
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(res[i]->io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}
	//debug("Client side READ is complete \n");
	//printf("dst:%s\n",dst);
	return 0;
}



/* This function disconnects the RDMA connection from the server and cleans up 
 * all the resources.
 */
static int client_disconnect_and_clean(int i)
{  
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/* active disconnect from the client side */
	ret = rdma_disconnect(res[i]->cm_client_id);
	if (ret) {
		rdma_error("Failed to disconnect, errno: %d \n", -errno);
		//continuing anyways
	}
	ret = process_rdma_cm_event(res[i]->cm_event_channel, 
			RDMA_CM_EVENT_DISCONNECTED,
			&cm_event);
	if (ret) {
		rdma_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event, ret = %d\n",
				ret);
		//continuing anyways 
	}
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge cm event, errno: %d\n", 
			       -errno);
		//continuing anyways
	}
	/* Destroy QP */
	rdma_destroy_qp(res[i]->cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(res[i]->cm_client_id);
	if (ret) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(res[i]->client_cq);
	if (ret) {
		rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy completion channel */
	ret = ibv_destroy_comp_channel(res[i]->io_completion_channel);
	if (ret) {
		rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy memory buffers */
	ibv_dereg_mr(res[i]->send_mr);
	ibv_dereg_mr(res[i]->recv_mr);
    ibv_dereg_mr(res[i]->client_dst_mr);
    ibv_dereg_mr(res[i]->client_src_mr);
    ibv_dereg_mr(res[i]->server_metadata_mr);
	/* We free the buffers */
	free(res[i]->send_region);
	free(res[i]->recv_region);
    free(res[i]->dst);
    free(res[i]->src);
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(res[i]->pd);
	if (ret) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		// we continue anyways;
	}
	rdma_destroy_event_channel(res[i]->cm_event_channel);
	printf("Client resource clean up is complete \n");
    free(res[i]);
    res[i]=NULL;
	return 0;
}

void usage() {
	printf("Usage:\n");
	printf("rdma_client: [-a <server_addr>] [-p <server_port>] \n");
	printf("(default IP is 127.0.0.1 and port is %d)\n", DEFAULT_RDMA_PORT);
	exit(1);
}
int on_connection(int i){
	int ret = -1;
	struct ibv_wc wc;
    post_recv(i);
	post_send(i);	
	ret = process_work_completion_events(res[i]->io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
        printf("send failed!\n");
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}	
    if(strncmp("error",res[i]->recv_region,5)==0){ 
        printf("hash val error\n");
        exit(1);
    }
	//printf("recved message:%s,len:%d\n",recv_region,strlen(recv_region));
    return ret;
}



int get(char key[KEY_LEN],size_t *value_len,char ** value,int i){   
   // if(verbose){  printf("get start\n");}
	char linshi_lock[LOCK_LEN+1];
    snprintf(linshi_lock,LOCK_LEN+1,"%08x%2s\0",ioshu,IPbuffer+ip_offset);
	snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "0%-16s%10s\0",key,linshi_lock);
   // if(verbose){  printf("send :%s\n", res[i]->send_region);}
	on_connection(i);
  //  if(verbose){  printf("recv :%s\n", res[i]->recv_region);}
	int num = res[i]->recv_region[0]-'0';
    while(num!=1){
        if(num==0){
            *value=NULL;
            *value_len =0;
            return 0;
        }
        usleep(rand()%100000);//sleep < 100000us ,0.1s
        snprintf(linshi_lock,LOCK_LEN+1,"%08x%2s\0",ioshu,IPbuffer+ip_offset);
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "0%-16s%10s\0",key,linshi_lock);
       // if(verbose){   printf("send :%s\n", res[i]->send_region);}
        on_connection(i);
       // if(verbose){  printf("recv :%s\n", res[i]->recv_region);}
        num = res[i]->recv_region[0]-'0';
    }
    
	char * leftover;
	char  linshi_buffer[9];
	linshi_buffer[8]='\0';
	memcpy(linshi_buffer,res[i]->recv_region+1,8);
	size_t offset = strtoul (linshi_buffer, &leftover, 16);
	memcpy(linshi_buffer,res[i]->recv_region+9,8);
	size_t record_len = strtoul (linshi_buffer,&leftover, 16);
	//   printf("get offset:%u\n",offset);	
	//  	printf("get record_len:%d\n",record_len);	

	post_read(offset,record_len,i);

	memcpy(linshi_buffer,res[i]->dst,8);  
	*value_len = strtoul(linshi_buffer, &leftover, 16);  

	*value = res[i]->dst+24;

    snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "3%-16s%10s\0",key,linshi_lock);
  //  if(verbose){  printf("send :%s\n", res[i]->send_region);}
	on_connection(i);
   // if(verbose){  printf("recv :%s\n", res[i]->recv_region);}
    num = res[i]->recv_region[0]-'0';
    if(num==0){
        printf("error del_lock :key does not exist\n");
    }else if(num==2){
        printf("error del_lock :lock is now occupied\n");
    } 
   // if(verbose){   printf("get finish\n");}
	return 1;
}
int put(char* key,char * value,int i){
   // if(verbose){   printf("put start !\n");  }
	size_t record_len = strlen(value)+RECORD_FIX_LEN+5;
	//time_t 8bytes signed long 
	char linshi_lock[LOCK_LEN+1];
    snprintf(linshi_lock,LOCK_LEN+1,"%08x%2s\0",ioshu,IPbuffer+ip_offset);
	//snprintf(linshi_lock,LOCK_LEN+1,"%016lx%08x%2s%08x\0",time_now,ioshu,IPbuffer+ip_offset,pid);
	snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "1%-16s%08lx%10s\0",key,record_len,linshi_lock);
//	if(verbose){  printf("send :%s\n", res[i]->send_region); }
	on_connection(i);
    //if(verbose){  printf("recv :%s\n", res[i]->recv_region);}
	int num = res[i]->recv_region[0]-'0';
    if(num==9){
        printf("recv hash error!!");
        exit(1);
    }
    while(num!=1){
        if(num==0){
            return 0;
        }
        usleep(rand()%100000);//sleep < 100000us ,0.1s    
        snprintf(linshi_lock,LOCK_LEN+1,"%08x%2s\0",ioshu,IPbuffer+ip_offset);
	    snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "1%-16s%08lx%10s\0",key,record_len,linshi_lock);
        // if(verbose){  printf("while()\n"); }
        // if(verbose){  printf("send :%s\n", res[i]->send_region);   }
        on_connection(i);   
        // if(verbose){   printf("recv :%s\n", res[i]->recv_region);   }
        num = res[i]->recv_region[0]-'0';
    }

	uint64_t offset =  strtoul (res[i]->recv_region+1, NULL, 16);	
	// printf("put offset:%d\n",offset);	
	// printf("put record_len:%d\n",record_len);
	snprintf(res[i]->src,BUFFER_SIZE,"%08lx%-16s%s\0\0\0\0\0\0",strlen(value),key,value); 
	
	post_write(offset,record_len,i);
	// post_read(offset,record_len);
    // if(verbose){  printf("xiewanbi_jiesuo()\n");}
	snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "3%-16s%10s\0",key,linshi_lock);
    // if(verbose){printf("send :%s\n", res[i]->send_region);    }
    on_connection(i);
    // if(verbose){ printf("recv :%s\n", res[i]->recv_region);}
    num = res[i]->recv_region[0]-'0';
    if(num==0){
        printf("error del_lock :key does not exist\n");
    }else if(num==2){
        printf("error del_lock :lock is now occupied\n");
    }

	// if(strcmp(src,dst)!=0){
	// 	printf("src:%s\n",src);
	// 	printf("dst:%s\n",dst);
	// 	printf("errrrrrrrrrrrrrrrprrrrrr!!!!!1\n");
	// 	pause();
	// }
    // if(verbose){ printf("put finish!\n");}
 
	return 1;
}
int del(char key[KEY_LEN],int i){ 
	char linshi_lock[LOCK_LEN+1];
    snprintf(linshi_lock,LOCK_LEN+1,"%08x%2s\0",ioshu,IPbuffer+ip_offset);
	//snprintf(linshi_lock,LOCK_LEN+1,"%016lx%08x%2s%08x\0",time_now,ioshu,IPbuffer+ip_offset);
	snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "2%-16s%10s\0",key,linshi_lock);
	on_connection(i);
	int num = res[i]->recv_region[0]-'0';
    while(num==2){      
        usleep(rand()%100000);//sleep < 100000us ,0.1s
        snprintf(linshi_lock,LOCK_LEN+1,"%08x%2s\0",ioshu,IPbuffer+ip_offset);
	    snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "2%-16s%10s\0",key,linshi_lock);
        on_connection(i);
        num = res[i]->recv_region[0]-'0';
    }
	return num;
}

void  genRandomString(char * string,int length)  
{  
    int flag, i;   
    for (i = 0; i < length-1; i++)  
    {
        flag = rand() % 3;  
        switch (flag)  
        {  
            case 0:  
                string[i] = 'A' + rand() % 26;  
                break;  
            case 1:  
                string[i] = 'a' + rand() % 26;  
                break;  
            case 2:  
                string[i] = '0' + rand() % 10;  
                break;  
            default:  
                string[i] = 'x';  
                break;  
        }  
    } 
	string[length-1] ='\0';
}



int get_client_addr(){

	char hostbuffer[256];
	struct hostent *host_entry;
	int hostname;

	//接收主机名
	hostname = gethostname(hostbuffer, sizeof(hostbuffer));
	if(hostname == -1){
		perror("gethostname");
		exit(1);
	}

	//接收主机信息
	host_entry = gethostbyname(hostbuffer);
	if(host_entry == NULL){
		perror("gethostbyname");
		exit(1);
	}

	//转换网络地址
	IPbuffer = inet_ntoa(*((struct in_addr*)
	host_entry->h_addr_list[0]));
	printf("Hostname: %s\n", hostbuffer);
	printf("Host IP: %s\n", IPbuffer);
	ip_offset = strlen(IPbuffer)-2;
	//printf("Host hou liang wei: %s\n", IPbuffer+ip_offset);
	return 0;
}
int get_client_pid(){
	pid = getpid();
	printf("pid：%d\n", pid);
	return 0;
}
int init(){
	int i,ret;
	for( i = 0;i<server_num;i++){	
	    res[i]=(struct resourse *)malloc(sizeof(struct resourse));
		memset(res[i],0,sizeof(res[i]));
		bzero(&res[i]->server_sockaddr, sizeof (res[i]->server_sockaddr));
		res[i]->server_sockaddr.sin_family = AF_INET;
		res[i]->server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		res[i]->server_ip = server_ip[i];
		ret = get_addr(res[i]->server_ip, (struct sockaddr*) &res[i]->server_sockaddr);
		if (ret) {
			rdma_error("Invalid IP \n");
			return ret;
		}
		if (!res[i]->server_sockaddr.sin_port) {
			/* no port provided, use the default port */
			res[i]->server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);
		}		
		/* buffers are NULL */
			
		res[i]->send_region = (char*)malloc(SEND_REGION_MAX_LEN);
		res[i]->recv_region = (char*)malloc(BUFFER_SIZE);
		res[i]->src = (char*)malloc(BUFFER_SIZE);
		res[i]->dst =  (char*)malloc(BUFFER_SIZE);
	
		ret = client_prepare_connection(&res[i]->server_sockaddr,i);
		if (ret) { 
			rdma_error("Failed to setup client connection , ret = %d \n", ret);
			return ret;
		}	
		ret = client_connect_to_server(i);
		if (ret) { 
			rdma_error("Failed to setup client connection , ret = %d \n", ret);
			return ret;
		}
		recv_server_metadata(i);
		register_all(i);
   }
    return ret;
}
int main(int argc, char **argv) {	

	// server_ip[0] = "10.0.0.40";
	// server_ip[1] = "10.0.0.41";	
	// server_ip[2] = "10.0.0.42";
	// server_ip[3] = "10.0.0.43";	

	// server_ip[0] = "10.0.0.50";
	// server_ip[1] = "10.0.0.51";	
	// server_ip[2] = "10.0.0.52";
	// server_ip[3] = "10.0.0.53";	
    int option;
    while ((option = getopt(argc, argv, "v:")) != -1) {
        switch (option) {			
            case 'v':
                verbose = strtol(optarg, NULL, 0);
                break;
            default:
                usage();			
                break;
            }
	}
    server_ip[0] = "10.0.0.40";
	server_ip[1] = "10.0.0.42";	
	get_client_addr();
	get_client_pid();
	init();
	sleep(1);//sleep 1s

 	int ret,i;


    // srand((unsigned) time(NULL ));
	char _key[KEY_LEN+1] ; 
	char _value[VALUE_MAX_LEN+1]; 
	char * ans_value = NULL;
	int keylen;
	size_t value_len;
	size_t ans_value_len;
	int ans_error = 0;
//typedef clock_t long 4 bytes
	clock_t start, finish;  
    double  duration=0.0;
	for(  i = 0;i<80000;i++){
        
		if(itimes%10000==0){
			printf("i=%d-----------------------------------------------------------\n",itimes);
		}
		
		keylen = rand()%KEY_LEN+2;	       
		genRandomString(_key ,keylen) ;        
		//value_len = VALUE_MAX_LEN;
		value_len = rand()%VALUE_MAX_LEN+2;//2-VALUE_MAX_LEN+1
		genRandomString(&_value ,value_len) ;	
        // keylen=1;
        // snprintf(_key,KEY_LEN,"a\0");	
        // value_len=5;
        // snprintf(_value,VALUE_MAX_LEN,"aaabb\0");

       //     printf("%s:%s\n",_key,_value);
		start = clock(); 
		char keybuffer[KEY_LEN+1];
		snprintf(keybuffer,KEY_LEN+1,"%-16s",_key); 
		int myserverid = select_server(keybuffer);
		//printf("myserverid=%d\n",myserverid);

		int put_ret = put( _key,_value,myserverid);	
        ioshu++;
		int get_ret = get(_key,&ans_value_len,&ans_value,myserverid);
        ioshu++;
		finish = clock();  
		duration += (double)(finish - start);
		// printf("here\n");
		// printf("%s\n",_value);
		// printf("%s\n",ans_value);
		if(get_ret!=0&&strncmp(_value,ans_value,ans_value_len)){
		//if(strcmp(_value,ans_value)){
			ans_error++;
			printf("src:%s,src_size:%d\n",res[myserverid]->src+24,strlen(res[myserverid]->src));
			printf("here1\n");
			printf("value_len:%d,ans_value_len:%d\n",value_len,ans_value_len);
			printf("here2\n");
			printf("value:%s,strlen(value):%d\n",_value,strlen(_value));
			printf("here3\n");
			printf("ans_e:%s,strlen(ans_value):%d\n",ans_value,strlen(ans_value));
			printf("here4\n");			
		}
		if(rand()%2){
            start = clock(); 
			if(del(_key,myserverid)==0){
                ans_error++;
            }
            ioshu++;
			if(get(_key,&ans_value_len,&ans_value,myserverid)){
                ans_error++;
            }
            ioshu++;
            finish = clock();  
		    duration += (double)(finish - start);
		}
        itimes++;	
	}


	printf("ans_error : %d~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n",ans_error);
    printf( "itimes : %d t~~~~~~~~~~~~~~~~~~~\n", itimes); 
  	printf( "ioshu : %d t~~~~~~~~~~~~~~~~~~~\n", ioshu); 
  	printf( "time : %f/%d seconds~~~~~~~~~~~~~~~~~~~\n", duration ,CLOCKS_PER_SEC); 
	printf( "iops :%f t/s~~~~~~~~~~~~~~~~~~~\n", ioshu/(duration/CLOCKS_PER_SEC)); 
	


	for( i = 0;i<server_num;i++){
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "4\0");
        post_send(i);
    }
	for( i = 0;i<server_num;i++){
		ret = client_disconnect_and_clean(i);
		if (ret) {
			rdma_error("Failed to cleanly disconnect and clean up resources \n");
		}
	}
	return ret;
}

