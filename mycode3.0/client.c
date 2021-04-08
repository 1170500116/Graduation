#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include<time.h>
#include <rdma/rdma_cma.h>
#include "rdma_common.h"
#include "define.h"#include <sys/select.h>


static void sleep_ms(unsigned int secs)

{

    struct timeval tval;

    tval.tv_sec=secs/1000;

    tval.tv_usec=(secs*1000)%1000000;

    select(0,NULL,NULL,NULL,&tval);

}

/* These are basic RDMA resources */
/* These are RDMA connection related resources */
static struct rdma_event_channel *cm_event_channel = NULL;
static struct rdma_cm_id *cm_client_id = NULL;
static struct ibv_pd *pd = NULL;
static struct ibv_comp_channel *io_completion_channel = NULL;
static struct ibv_cq *client_cq = NULL;
static struct ibv_qp_init_attr qp_init_attr;
static struct ibv_qp *client_qp;
/* These are memory buffers related resources */
static struct ibv_mr *recv_mr = NULL,  *send_mr = NULL;

static struct ibv_send_wr send_wr, *bad_send_wr = NULL;
static struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
static struct ibv_sge send_sge, recv_sge;

static struct rdma_buffer_attr server_metadata_attr;



char *send_region;
char *recv_region;



/*****************/
static struct ibv_mr * client_dst_mr =NULL,*client_src_mr = NULL;
static struct ibv_send_wr write_wr, *bad_write_wr = NULL;
static struct ibv_send_wr read_wr, *bad_read_wr = NULL;
struct ibv_sge read_sge,write_sge;
char * dst;
char * src;
/*****************/
static int get(char* key,size_t* value_len,char ** value);
static int put(char* key,char * value);
static int del(char* key);
#define SEND_REGION_MAX_LEN 50
int itimes =0;
/* This function prepares client side connection resources for an RDMA connection */
static int client_prepare_connection(struct sockaddr_in *s_addr)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/*  Open a channel used to report asynchronous communication event */
	cm_event_channel = rdma_create_event_channel();
	if (!cm_event_channel) {
		rdma_error("Creating cm event channel failed, errno: %d \n", -errno);
		return -errno;
	}
	//debug("RDMA CM event channel is created at : %p \n", cm_event_channel);
	/* rdma_cm_id is the connection identifier (like socket) which is used 
	 * to define an RDMA connection. 
	 */
	ret = rdma_create_id(cm_event_channel, &cm_client_id, 
			NULL,
			RDMA_PS_TCP);
	if (ret) {
		rdma_error("Creating cm id failed with errno: %d \n", -errno); 
		return -errno;
	}
	/* Resolve destination and optional source addresses from IP addresses  to
	 * an RDMA address.  If successful, the specified rdma_cm_id will be bound
	 * to a local device. */
	ret = rdma_resolve_addr(cm_client_id, NULL, (struct sockaddr*) s_addr, 2000);
	if (ret) {
		rdma_error("Failed to resolve address, errno: %d \n", -errno);
		return -errno;
	}
	//debug("waiting for cm event: RDMA_CM_EVENT_ADDR_RESOLVED\n");
	ret  = process_rdma_cm_event(cm_event_channel, 
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
	ret = rdma_resolve_route(cm_client_id, 2000);
	if (ret) {
		rdma_error("Failed to resolve route, erno: %d \n", -errno);
	       return -errno;
	}
	//debug("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED\n");
	ret = process_rdma_cm_event(cm_event_channel, 
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
	pd = ibv_alloc_pd(cm_client_id->verbs);
	if (!pd) {
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
	io_completion_channel = ibv_create_comp_channel(cm_client_id->verbs);
	if (!io_completion_channel) {
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
	client_cq = ibv_create_cq(cm_client_id->verbs /* which device*/, 
			CQ_CAPACITY /* maximum capacity*/, 
			NULL /* user context, not used here */,
			io_completion_channel /* which IO completion channel */, 
			0 /* signaling vector, not used here*/);
	if (!client_cq) {
		rdma_error("Failed to create CQ, errno: %d \n", -errno);
		return -errno;
	}
	//debug("CQ created at %p with %d elements \n", client_cq, client_cq->cqe);
	ret = ibv_req_notify_cq(client_cq, 0);
	if (ret) {
		rdma_error("Failed to request notifications, errno: %d\n", -errno);
		return -errno;
	}
       /* Now the last step, set up the queue pair (send, recv) queues and their capacity.
         * The capacity here is define statically but this can be probed from the 
	 * device. We just use a small number as defined in rdma_common.h */
       bzero(&qp_init_attr, sizeof qp_init_attr);
       qp_init_attr.cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
       qp_init_attr.cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
       qp_init_attr.cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
       qp_init_attr.cap.max_send_wr = MAX_WR; /* Maximum send posting capacity */
       qp_init_attr.qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
       /* We use same completion queue, but one can use different queues */
       qp_init_attr.recv_cq = client_cq; /* Where should I notify for receive completion operations */
       qp_init_attr.send_cq = client_cq; /* Where should I notify for send completion operations */
       /*Lets create a QP */
       ret = rdma_create_qp(cm_client_id /* which connection id */,
		       pd /* which protection domain*/,
		       &qp_init_attr /* Initial attributes */);
	if (ret) {
		rdma_error("Failed to create QP, errno: %d \n", -errno);
	       return -errno;
	}
	client_qp = cm_client_id->qp;
	//debug("QP created at %p \n", client_qp);
	return 0;
}

/* Pre-posts a receive buffer before calling rdma_connect () */
static int post_recv()
{
	int ret = -1;
	memset(recv_region,0,BUFFER_SIZE);
	ret = ibv_post_recv(client_qp /* which QP */,
		      &recv_wr /* receive work request*/,
		      &bad_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
//	debug("Receive buffer pre-posting is successful \n");
	return 0;
}

/* Connects to the RDMA server */
static int client_connect_to_server() 
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	bzero(&conn_param, sizeof(conn_param));
	conn_param.initiator_depth = 3;
	conn_param.responder_resources = 3;
	conn_param.retry_count = 3; // if fail, then how many times to retry
	ret = rdma_connect(cm_client_id, &conn_param);
	if (ret) {
		rdma_error("Failed to connect to remote host , errno: %d\n", -errno);
		return -errno;
	}
	//debug("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED\n");
	ret = process_rdma_cm_event(cm_event_channel, 
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

static int recv_server_metadata(){
	struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr = NULL;
	struct ibv_sge  server_recv_sge;
	int ret = -1;
	static struct ibv_mr *server_metadata_mr = NULL;   
	server_metadata_mr = rdma_buffer_register(pd,
			&server_metadata_attr,
			sizeof(server_metadata_attr),
			(IBV_ACCESS_LOCAL_WRITE));
	if(!server_metadata_mr){
		rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
		return -ENOMEM;
	}
	server_recv_sge.addr = (uint64_t) server_metadata_mr->addr;
	server_recv_sge.length = (uint32_t) server_metadata_mr->length;
	server_recv_sge.lkey = (uint32_t) server_metadata_mr->lkey;
	/* now we link it to the request */
	bzero(&server_recv_wr, sizeof(server_recv_wr));
	server_recv_wr.sg_list = &server_recv_sge;
	server_recv_wr.num_sge = 1;
	ret = ibv_post_recv(client_qp /* which QP */,
		      &server_recv_wr /* receive work request*/,
		      &bad_server_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
	//debug("Receive buffer pre-posting is successful \n");
	struct ibv_wc wc;
	ret = process_work_completion_events(io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}	
	show_rdma_buffer_attr(&server_metadata_attr);
	return 0;
}
static int register_all(){
	send_mr = rdma_buffer_register(pd,
			send_region,
			SEND_REGION_MAX_LEN,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if(!send_mr){
		rdma_error("Failed to register the send region\n");
		return -1;
	}
	/* now we fill up SGE */
	send_sge.addr = (uint64_t) send_mr->addr;
	send_sge.length = (uint32_t) send_mr->length;
	send_sge.lkey = send_mr->lkey;
	/* now we link to the send work request */
	bzero(&send_wr, sizeof(send_wr));
	send_wr.sg_list = &send_sge;
	send_wr.num_sge = 1;
	send_wr.opcode = IBV_WR_SEND;
	send_wr.send_flags = IBV_SEND_SIGNALED;


	recv_mr = rdma_buffer_register(pd,
			recv_region,
			BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE));
	if(!recv_mr){
		rdma_error("Failed to register the recv region\n");
		return -1;
	}
	recv_sge.addr = (uint64_t) recv_mr->addr;
	recv_sge.length = (uint32_t) recv_mr->length;
	recv_sge.lkey = (uint32_t) recv_mr->lkey;
	/* now we link it to the request */
	bzero(&recv_wr, sizeof(recv_wr));
	recv_wr.sg_list = &recv_sge;
	recv_wr.num_sge = 1;

	client_src_mr = rdma_buffer_register(pd,
			src,
			BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if (!client_src_mr) {
		rdma_error("We failed to create the source buffer, -ENOMEM\n");
		return -ENOMEM;
	}
	client_dst_mr = rdma_buffer_register(pd,
			dst,
			BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE | 
			 IBV_ACCESS_REMOTE_WRITE | 
			 IBV_ACCESS_REMOTE_READ));
	
	if (!client_dst_mr) {
		rdma_error("We failed to create the destination buffer, -ENOMEM\n");
		return -ENOMEM;
	}
	/* Step 1: is to copy the local buffer into the remote buffer. We will 
	 * reuse the previous variables. */
	/* now we fill up SGE */
	write_sge.addr = (uint64_t) client_src_mr->addr;
	write_sge.length = (uint32_t) client_src_mr->length;
	write_sge.lkey = client_src_mr->lkey;
	/* now we link to the send work request */
	bzero(&write_wr, sizeof(write_wr));
	write_wr.sg_list = &write_sge;
	write_wr.num_sge = 1;
	write_wr.opcode = IBV_WR_RDMA_WRITE;
	write_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	write_wr.wr.rdma.rkey = server_metadata_attr.stag.remote_stag;
	write_wr.wr.rdma.remote_addr = server_metadata_attr.address;
	//printf("remote_addr:%p\n",write_wr.wr.rdma.remote_addr);




	read_sge.addr = (uint64_t) client_dst_mr->addr;
	read_sge.length = (uint32_t) client_dst_mr->length;
	read_sge.lkey = client_dst_mr->lkey;
	/* now we link to the send work request */
	bzero(&read_wr, sizeof(read_wr));
	read_wr.sg_list = &read_sge;
	read_wr.num_sge = 1;
	read_wr.opcode = IBV_WR_RDMA_READ;
	read_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	read_wr.wr.rdma.rkey = server_metadata_attr.stag.remote_stag;
	read_wr.wr.rdma.remote_addr = server_metadata_attr.address;

	return 0;
}
static int post_send()
{
	struct ibv_wc wc;
	int ret = -1;
	
	/* Now we post it */
	ret = ibv_post_send(client_qp, 
		       &send_wr,
	       &bad_send_wr);
	if (ret) {
		rdma_error("Failed to send client metadata, errno: %d \n", 
				-errno);
		return -errno;
	}
	/* at this point we are expecting 2 work completion. One for our 
	 * send and one for recv that we will get from the server for 
	 * its buffer information */
	ret = process_work_completion_events(io_completion_channel, 
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
static int post_write(uint64_t offset,uint64_t record_len){
	struct ibv_wc wc;
	int ret = -1;
	write_sge.length = record_len;
	write_wr.wr.rdma.remote_addr = server_metadata_attr.address+offset;
	//printf("write offset:%ld\n",offset);
	/* Now we post it */
	ret = ibv_post_send(client_qp, 
		       &write_wr,
	       &bad_write_wr);
	if (ret) {
		rdma_error("Failed to write client src buffer, errno: %d \n", 
				-errno);
		return -errno;
	}
		
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(io_completion_channel, 
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
static int post_read(uint64_t offset,uint64_t record_len){
	struct ibv_wc wc;
	int ret = -1;
	 memset(dst,0,BUFFER_SIZE);
	read_sge.length = (uint32_t) record_len;
	//printf(" read offset=%ld\n",offset);
	read_wr.wr.rdma.remote_addr = server_metadata_attr.address+offset;
	/* Now we post it */
	ret = ibv_post_send(client_qp, 
		       &read_wr,
	       &bad_read_wr);
	if (ret) {
		rdma_error("Failed to read client dst buffer from the master, errno: %d \n", 
				-errno);
		return -errno;
	}
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(io_completion_channel, 
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
static int client_disconnect_and_clean()
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/* active disconnect from the client side */
	ret = rdma_disconnect(cm_client_id);
	if (ret) {
		rdma_error("Failed to disconnect, errno: %d \n", -errno);
		//continuing anyways
	}
	ret = process_rdma_cm_event(cm_event_channel, 
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
	rdma_destroy_qp(cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(cm_client_id);
	if (ret) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(client_cq);
	if (ret) {
		rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy completion channel */
	ret = ibv_destroy_comp_channel(io_completion_channel);
	if (ret) {
		rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy memory buffers */
	ibv_dereg_mr(send_mr);
	ibv_dereg_mr(recv_mr);
	
	/* We free the buffers */
	free(send_region);
	free(recv_region);
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(pd);
	if (ret) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		// we continue anyways;
	}
	rdma_destroy_event_channel(cm_event_channel);
	printf("Client resource clean up is complete \n");
	return 0;
}

void usage() {
	printf("Usage:\n");
	printf("rdma_client: [-a <server_addr>] [-p <server_port>] \n");
	printf("(default IP is 127.0.0.1 and port is %d)\n", DEFAULT_RDMA_PORT);
	exit(1);
}
int on_connection(){
	int ret = -1;
	struct ibv_wc wc;
    post_recv();
	post_send();	
	ret = process_work_completion_events(io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}	
	//printf("recved message:%s,len:%d\n",recv_region,strlen(recv_region));
}
int get(char key[KEY_LEN],size_t *value_len,char ** value){ 

  snprintf(send_region, SEND_REGION_MAX_LEN, "0%-16s\0",key);
  on_connection();

  int num = recv_region[0]-'0';
  if(num==0){
	  *value=NULL;
	  return 0;
  }
  char * leftover;
  char  linshi_buffer[9];
  linshi_buffer[8]='\0';
  memcpy(linshi_buffer,recv_region+1,8);
  size_t offset = strtoul (linshi_buffer, &leftover, 16);
  memcpy(linshi_buffer,recv_region+9,8);
  size_t record_len = strtoul (linshi_buffer,&leftover, 16);
//   printf("get offset:%u\n",offset);	
//  	printf("get record_len:%d\n",record_len);	
 
  post_read(offset,record_len);

  memcpy(linshi_buffer,dst,8);  
  *value_len = strtoul (linshi_buffer, &leftover, 16);  
 
  *value = dst+24;
  return 1;
}
int put(char* key,char * value){
	
	size_t record_len = strlen(value)+RECORD_FIX_LEN+5;
	snprintf(send_region, SEND_REGION_MAX_LEN, "1%-16s%08x\0",key,record_len);
	on_connection();
	int num = recv_region[0]-'0';
	if(num==0){
		return 0;
	}
	uint64_t offset =  strtoul (recv_region+1, NULL, 16);	
	// printf("put offset:%d\n",offset);	
	// printf("put record_len:%d\n",record_len);
	snprintf(src,BUFFER_SIZE,"%08x%-16s%s\0\0\0\0\0\0",strlen(value),key,value); 
	
	post_write(offset,record_len);
	// post_read(offset,record_len);
	


	// if(strcmp(src,dst)!=0){
	// 	printf("src:%s\n",src);
	// 	printf("dst:%s\n",dst);
	// 	printf("errrrrrrrrrrrrrrrprrrrrr!!!!!1\n");
	// 	pause();
	// }
	return 1;
}
int del(char key[KEY_LEN]){ 

  snprintf(send_region, SEND_REGION_MAX_LEN, "2%-16s\0",key);
  on_connection();
  return 1;
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
int main(int argc, char **argv) {	
    srand((unsigned) time(NULL ));      
	struct sockaddr_in server_sockaddr;
	int ret, option;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET;
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	/* buffers are NULL */

	send_region = (char*)malloc(SEND_REGION_MAX_LEN);
	recv_region = (char*)malloc(BUFFER_SIZE);
	src= (char*)malloc(BUFFER_SIZE);
	dst =  (char*)malloc(BUFFER_SIZE);

	/* Parse Command Line Arguments */
	while ((option = getopt(argc, argv, "a:p:")) != -1) {
	switch (option) {			
		case 'a':
			/* remember, this overwrites the port info */
			ret = get_addr(optarg, (struct sockaddr*) &server_sockaddr);
			if (ret) {
				rdma_error("Invalid IP \n");
				return ret;
			}
			break;
		case 'p':
			/* passed port to listen on */
			server_sockaddr.sin_port = htons(strtol(optarg, NULL, 0)); 
			break;
		default:
			usage();
			break;
		}
	}
	if (!server_sockaddr.sin_port) {
	  /* no port provided, use the default port */
	  server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);
	  }
	
	ret = client_prepare_connection(&server_sockaddr);
	if (ret) { 
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}	
	ret = client_connect_to_server();
	if (ret) { 
		rdma_error("Failed to setup client connection , ret = %d \n", ret);
		return ret;
	}
	recv_server_metadata();
	register_all();
	char _key[KEY_LEN+1] ; 
	char _value[VALUE_MAX_LEN+1]; 
	char * ans_value = NULL;
	int keylen;
	size_t value_len;
	size_t ans_value_len;
	int ans_error = 0;

	clock_t start, finish;  
    double  duration=0.0;
//	sprintf(src,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") ; 
 // post_write( 10023872UL);
   
	int i;
	//433/2+2
	int ioshu = 0;
	for( i = 0;i<80000;i++){
		if(itimes%10000==0){
			printf("i=%d-----------------------------------------------------------\n",itimes);
		}
		
		itimes++;
		
		ioshu+=2;
		keylen = rand()%KEY_LEN+2;	
		genRandomString(_key ,keylen) ;	
		//value_len = VALUE_MAX_LEN;
		value_len = rand()%VALUE_MAX_LEN+2;//2-VALUE_MAX_LEN+1
		genRandomString(&_value ,value_len) ;	
		start = clock();  
		put( _key,_value);
	
		get(_key,&ans_value_len,&ans_value);
		finish = clock();  
		duration += (double)(finish - start);
		if(strncmp(_value,ans_value,ans_value_len)){
		//if(strcmp(_value,ans_value)){
			ans_error++;
			printf("src:%s,src_size:%d\n",src+24,strlen(src));
			printf("value_len:%d,ans_value_len:%d\n",value_len,ans_value_len);
			printf("value:%s,strlen(value):%d\n",_value,strlen(_value));
			printf("ans_e:%s,strlen(ans_value):%d\n",ans_value,strlen(ans_value));
			return;
		}
		// if(rand()%2){
		// 	if(del(_key)==0){ans_error++;};
		// 	if(get(_key,&ans_value_len,&ans_value)){ans_error++;};
		// }
	}


	printf("ans_error=%d~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n",ans_error);

  	printf( "%f/%d seconds~~~~~~~~~~~~~~~~~~~\n", duration ,CLOCKS_PER_SEC); 
	  
	printf( "iops :%f t/s~~~~~~~~~~~~~~~~~~~\n", ioshu/(duration/CLOCKS_PER_SEC)); 
	


	
	
	
	// get(_key,&ans_value);
	// printf("dst+24:%p;ans_value:%p\n",dst+24,ans_value);
	// ret = client_remote_memory_ops();
	// if (ret) {
	// 	rdma_error("Failed to finish remote memory ops, ret = %d \n", ret);
	// 	return ret;
	// }
	
	// ret = client_disconnect_and_clean();
	// if (ret) {
	// 	rdma_error("Failed to cleanly disconnect and clean up resources \n");
	// }
	return ret;
}

