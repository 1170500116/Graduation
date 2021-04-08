#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <rdma/rdma_cma.h>
#include "rdma_common.h"
#include <sys/types.h>  
#include <fcntl.h>
#include <sys/stat.h>
#include "define.h"
#include <libpmem.h>
#include<sys/mman.h>
#define SEND_REGION_MAX_LEN 20

int pmem_fd ;
/* These are the RDMA resources needed to setup an RDMA connection */
/* Event channel, where connection management (cm) related events are relayed */
static struct rdma_event_channel *cm_event_channel = NULL;
static struct rdma_cm_id *cm_server_id = NULL, *cm_client_id = NULL;
static struct ibv_pd *pd = NULL;
static struct ibv_comp_channel *io_completion_channel = NULL;
static struct ibv_cq *cq = NULL;
static struct ibv_qp_init_attr qp_init_attr;
static struct ibv_qp *client_qp = NULL;
/* RDMA memory resources */
char * send_region;
char * recv_region;
char * recv_buffer;
char * file_buffer;
static struct ibv_mr *send_mr = NULL;
static struct ibv_mr *recv_mr = NULL; 
static struct ibv_mr *server_buffer_mr = NULL; 
static struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
static struct ibv_send_wr send_wr, *bad_send_wr = NULL;
static struct ibv_sge recv_sge, send_sge;
static struct rdma_buffer_attr server_metadata_attr;
int itimes = 0;
void parse_command(char * str){ 	
  int  num = str[0]-'0';
  char * key= malloc(KEY_LEN);
  memcpy(key,str+1,KEY_LEN);
  //printf("%d :%s\n",num,key);
  // num = 0: get
  // num = 1: set
  // num = 2: del 
  memset(send_region,0,SEND_REGION_MAX_LEN);
  if(num==0){
    BLOCK_INDEX_TYPE block_index = 0;
    VALUE_LEN_TYPE record_len  = 0;
    int ret = nvm_Get(key,&block_index,&record_len);
	
    snprintf(send_region, SEND_REGION_MAX_LEN, "%1d%08x%08x\0",ret,block_index*BLOCK_SIZE,record_len); 
	//printf("send:%s",send_region);    
  }else if(num==1){
    char * leftover;
	// for(int t = 0 ;t<strlen(str);t++){
	// 	printf(" %d:%c,",*(str+t),*(str+t));
	// }	
	//printf("value_size:%s",str+KEY_LEN+1);
    VALUE_LEN_TYPE record_len = strtoul (str+KEY_LEN+1, &leftover, 16);	
	//printf("value_size:%d\n",value_size);
    BLOCK_INDEX_TYPE block_index = 0;
    int ret = nvm_Put(key,record_len,&block_index);
    snprintf(send_region, SEND_REGION_MAX_LEN, "%1d%08x\0",ret,block_index*BLOCK_SIZE);  
	//printf("send:%s",send_region); 
  }else {
    int ret = nvm_Del(key);
    snprintf(send_region, SEND_REGION_MAX_LEN, "%1d\0",ret); 
  }
}
/* When we call this function cm_client_id must be set to a valid identifier.
 * This is where, we prepare client connection before we accept it. This 
 * mainly involve pre-posting a receive buffer to receive client side 
 * RDMA credentials
 */
static int setup_client_resources()
{
	int ret = -1;
	if(!cm_client_id){
		rdma_error("Client id is still NULL \n");
		return -EINVAL;
	}
	/* We have a valid connection identifier, lets start to allocate 
	 * resources. We need: 
	 * 1. Protection Domains (PD)
	 * 2. Memory Buffers 
	 * 3. Completion Queues (CQ)
	 * 4. Queue Pair (QP)
	 * Protection Domain (PD) is similar to a "process abstraction" 
	 * in the operating system. All resources are tied to a particular PD. 
	 * And accessing recourses across PD will result in a protection fault.
	 */
	pd = ibv_alloc_pd(cm_client_id->verbs 
			/* verbs defines a verb's provider, 
			 * i.e an RDMA device where the incoming 
			 * client connection came */);
	if (!pd) {
		rdma_error("Failed to allocate a protection domain errno: %d\n",
				-errno);
		return -errno;
	}
	//debug("A new protection domain is allocated at %p \n", pd);
	/* Now we need a completion channel, were the I/O completion 
	 * notifications are sent. Remember, this is different from connection 
	 * management (CM) event notifications. 
	 * A completion channel is also tied to an RDMA device, hence we will 
	 * use cm_client_id->verbs. 
	 */
	io_completion_channel = ibv_create_comp_channel(cm_client_id->verbs);
	if (!io_completion_channel) {
		rdma_error("Failed to create an I/O completion event channel, %d\n",
				-errno);
		return -errno;
	}
	// debug("An I/O completion event channel is created at %p \n", 
	// 		io_completion_channel);
	/* Now we create a completion queue (CQ) where actual I/O 
	 * completion metadata is placed. The metadata is packed into a structure 
	 * called struct ibv_wc (wc = work completion). ibv_wc has detailed 
	 * information about the work completion. An I/O request in RDMA world 
	 * is called "work" ;) 
	 */
	cq = ibv_create_cq(cm_client_id->verbs /* which device*/, 
			CQ_CAPACITY /* maximum capacity*/, 
			NULL /* user context, not used here */,
			io_completion_channel /* which IO completion channel */, 
			0 /* signaling vector, not used here*/);
	if (!cq) {
		rdma_error("Failed to create a completion queue (cq), errno: %d\n",
				-errno);
		return -errno;
	}
	// debug("Completion queue (CQ) is created at %p with %d elements \n", 
	// 		cq, cq->cqe);
	/* Ask for the event for all activities in the completion queue*/
	ret = ibv_req_notify_cq(cq /* on which CQ */, 
			0 /* 0 = all event type, no filter*/);
	if (ret) {
		rdma_error("Failed to request notifications on CQ errno: %d \n",
				-errno);
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
       qp_init_attr.recv_cq = cq; /* Where should I notify for receive completion operations */
       qp_init_attr.send_cq = cq; /* Where should I notify for send completion operations */
       /*Lets create a QP */
       ret = rdma_create_qp(cm_client_id /* which connection id */,
		       pd /* which protection domain*/,
		       &qp_init_attr /* Initial attributes */);
       if (ret) {
	       rdma_error("Failed to create QP due to errno: %d\n", -errno);
	       return -errno;
       }
       /* Save the reference for handy typing but is not required */
       client_qp = cm_client_id->qp;
     //  debug("Client QP created at %p\n", client_qp);
       return ret;
}

/* Starts an RDMA server by allocating basic connection resources */
static int start_rdma_server(struct sockaddr_in *server_addr) 
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/*  Open a channel used to report asynchronous communication event */
	cm_event_channel = rdma_create_event_channel();
	if (!cm_event_channel) {
		rdma_error("Creating cm event channel failed with errno : (%d)", -errno);
		return -errno;
	}
	// debug("RDMA CM event channel is created successfully at %p \n", 
	// 		cm_event_channel);
	/* rdma_cm_id is the connection identifier (like socket) which is used 
	 * to define an RDMA connection. 
	 */
	ret = rdma_create_id(cm_event_channel, &cm_server_id, NULL, RDMA_PS_TCP);
	if (ret) {
		rdma_error("Creating server cm id failed with errno: %d ", -errno);
		return -errno;
	}
	// debug("A RDMA connection id for the server is created \n");
	/* Explicit binding of rdma cm id to the socket credentials */
	ret = rdma_bind_addr(cm_server_id, (struct sockaddr*) server_addr);
	if (ret) {
		rdma_error("Failed to bind server address, errno: %d \n", -errno);
		return -errno;
	}
	// debug("Server RDMA CM id is successfully binded \n");
	/* Now we start to listen on the passed IP and port. However unlike
	 * normal TCP listen, this is a non-blocking call. When a new client is 
	 * connected, a new connection management (CM) event is generated on the 
	 * RDMA CM event channel from where the listening id was created. Here we
	 * have only one channel, so it is easy. */
	ret = rdma_listen(cm_server_id, 8); /* backlog = 8 clients, same as TCP, see man listen*/
	if (ret) {
		rdma_error("rdma_listen failed to listen on server address, errno: %d ",
				-errno);
		return -errno;
	}
	printf("Server is listening successfully at: %s , port: %d \n",
			inet_ntoa(server_addr->sin_addr),
			ntohs(server_addr->sin_port));
	/* now, we expect a client to connect and generate a RDMA_CM_EVNET_CONNECT_REQUEST 
	 * We wait (block) on the connection management event channel for 
	 * the connect event. 
	 */
	ret = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_CONNECT_REQUEST,
			&cm_event);
	if (ret) {
		rdma_error("Failed to get cm event, ret = %d \n" , ret);
		return ret;
	}
	/* Much like TCP connection, listening returns a new connection identifier 
	 * for newly connected client. In the case of RDMA, this is stored in id 
	 * field. For more details: man rdma_get_cm_event 
	 */
	cm_client_id = cm_event->id;
	/* now we acknowledge the event. Acknowledging the event free the resources 
	 * associated with the event structure. Hence any reference to the event 
	 * must be made before acknowledgment. Like, we have already saved the 
	 * client id from "id" field before acknowledging the event. 
	 */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the cm event errno: %d \n", -errno);
		return -errno;
	}
	// debug("A new RDMA client connection id is stored at %p\n", cm_client_id);
	return ret;
}

/* Pre-posts a receive buffer and accepts an RDMA client connection */
static int accept_client_connection()
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;
	struct sockaddr_in remote_sockaddr; 
	int ret = -1;
	if(!cm_client_id || !client_qp) {
		rdma_error("Client resources are not properly setup\n");
		return -EINVAL;
	}	
	/* Now we accept the connection. Recall we have not accepted the connection 
	 * yet because we have to do lots of resource pre-allocation */
       memset(&conn_param, 0, sizeof(conn_param));
       /* this tell how many outstanding requests can we handle */
       conn_param.initiator_depth = 3; /* For this exercise, we put a small number here */
       /* This tell how many outstanding requests we expect other side to handle */
       conn_param.responder_resources = 3; /* For this exercise, we put a small number */
       ret = rdma_accept(cm_client_id, &conn_param);
       if (ret) {
	       rdma_error("Failed to accept the connection, errno: %d \n", -errno);
	       return -errno;
       }
       /* We expect an RDMA_CM_EVNET_ESTABLISHED to indicate that the RDMA  
	* connection has been established and everything is fine on both, server 
	* as well as the client sides.
	*/
        // debug("Going to wait for : RDMA_CM_EVENT_ESTABLISHED event \n");
       ret = process_rdma_cm_event(cm_event_channel, 
		       RDMA_CM_EVENT_ESTABLISHED,
		       &cm_event);
        if (ret) {
		rdma_error("Failed to get the cm event, errnp: %d \n", -errno);
		return -errno;
	}
	/* We acknowledge the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		return -errno;
	}
	/* Just FYI: How to extract connection information */
	memcpy(&remote_sockaddr /* where to save */, 
			rdma_get_peer_addr(cm_client_id) /* gives you remote sockaddr */, 
			sizeof(struct sockaddr_in) /* max size */);
	printf("A new connection is accepted from %s \n", 
			inet_ntoa(remote_sockaddr.sin_addr));
	return ret;
}
static int send_server_metadata(){
	int ret = -1;
	//  int offset = (max_block_index_+BLOCK_SIZE-1)/BLOCK_SIZE*BLOCK_SIZE;  
	server_buffer_mr = rdma_buffer_register(pd /* which protection domain */, 
			file_buffer,	
			FILE_SIZE /* what size to allocate */, 
			(IBV_ACCESS_LOCAL_WRITE|
			IBV_ACCESS_REMOTE_READ|
			IBV_ACCESS_REMOTE_WRITE) /* access permissions */);
	if(!server_buffer_mr){
		rdma_error("Server failed to create a buffer \n");
		/* we assume that it is due to out of memory error */
		return -ENOMEM;
	}
       /* This buffer is used to transmit information about the above 
	* buffer to the client. So this contains the metadata about the server 
	* buffer. Hence this is called metadata buffer. Since this is already 
	* on allocated, we just register it. 
        * We need to prepare a send I/O operation that will tell the 
	* client the address of the server buffer. 
	*/
	struct ibv_mr *server_metadata_mr = NULL;
	struct ibv_sge server_send_sge;
	struct ibv_send_wr server_send_wr, *bad_server_send_wr = NULL;
	server_metadata_attr.address = (uint64_t) server_buffer_mr->addr;
	server_metadata_attr.length = (uint32_t) server_buffer_mr->length;
	server_metadata_attr.stag.local_stag = (uint32_t) server_buffer_mr->lkey;
	server_metadata_mr = rdma_buffer_register(pd /* which protection domain*/, 
			&server_metadata_attr /* which memory to register */, 
			sizeof(server_metadata_attr) /* what is the size of memory */,
			IBV_ACCESS_LOCAL_WRITE /* what access permission */);
	if(!server_metadata_mr){
		rdma_error("Server failed to create to hold server metadata \n");
		/* we assume that this is due to out of memory error */
		return -ENOMEM;
	}
	/* We need to transmit this buffer. So we create a send request. 
	* A send request consists of multiple SGE elements. In our case, we only
	* have one 
	*/
	server_send_sge.addr = (uint64_t) &server_metadata_attr;
	server_send_sge.length = sizeof(server_metadata_attr);
	server_send_sge.lkey = server_metadata_mr->lkey;
	/* now we link this sge to the send request */
	bzero(&server_send_wr, sizeof(server_send_wr));
	server_send_wr.sg_list = &server_send_sge;
	server_send_wr.num_sge = 1; // only 1 SGE element in the array 
	server_send_wr.opcode = IBV_WR_SEND; // This is a send request 
	server_send_wr.send_flags = IBV_SEND_SIGNALED; // We want to get notification 
	/* This is a fast data path operation. Posting an I/O request */
	ret = ibv_post_send(client_qp /* which QP */, 
			&server_send_wr /* Send request that we prepared before */, 
			&bad_server_send_wr /* In case of error, this will contain failed requests */);
	if (ret) {
		rdma_error("Posting of server metdata failed, errno: %d \n",
				-errno);
		return -errno;
	}
	/* We check for completion notification */
	struct ibv_wc wc;
	ret = process_work_completion_events(io_completion_channel, &wc, 1);
	if (ret != 1) {
		rdma_error("Failed to send server metadata, ret = %d \n", ret);
		return ret;
	}
	// debug("Local buffer metadata has been sent to the client \n");
	return 0;
}
static int register_send_recv(){
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

	
	return 0;
}
/* This function sends server side buffer metadata to the connected client */
static int recv_data_and_post_next_recv() 
{
	
	struct ibv_wc wc;
	int ret = -1;
	/* Now, we first wait for the client to start the communication by 
	 * sending the server its metadata info. The server does not use it 
	 * in our example. We will receive a work completion notification for 
	 * our pre-posted receive request.
	 */

	ret = process_work_completion_events(io_completion_channel, &wc, 1);
	if (ret != 1) {
		rdma_error("Failed to receive , ret = %d \n", ret);
		return ret;
	}
	/* if all good, then we should have client's buffer information, lets see */
	//printf("recved message:%s,len:%d\n",recv_region,strlen(recv_region));
	sprintf(recv_buffer,recv_region);	
	memset(recv_region,0,sizeof(recv_region));

	ret = ibv_post_recv(client_qp /* which QP */,
		      &recv_wr /* receive work request*/,
		      &bad_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
	// debug("Receive buffer pre-posting is successful \n");
	return ret;
}
static int post_send()
{
	struct ibv_wc wc[1];
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
			wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}
	// debug("Server sent us its buffer location and credentials, showing \n");
	// printf("sended message:%s,len:%d\n",send_region,strlen(send_region));
	return 0;
}

/* This is server side logic. Server passively waits for the client to call 
 * rdma_disconnect() and then it will clean up its resources */
static int disconnect_and_cleanup()
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
       /* Now we wait for the client to send us disconnect event */
    //   debug("Waiting for cm event: RDMA_CM_EVENT_DISCONNECTED\n");
       ret = process_rdma_cm_event(cm_event_channel, 
		       RDMA_CM_EVENT_DISCONNECTED, 
		       &cm_event);
       if (ret) {
	       rdma_error("Failed to get disconnect event, ret = %d \n", ret);
	       return ret;
       }
	/* We acknowledge the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		return -errno;
	}
	printf("A disconnect event is received from the client...\n");
	/* We free all the resources */
	/* Destroy QP */
	rdma_destroy_qp(cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(cm_client_id);
	if (ret) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(cq);
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
	free(send_region);
	free(recv_region);
	
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(pd);
	if (ret) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy rdma server id */
	ret = rdma_destroy_id(cm_server_id);
	if (ret) {
		rdma_error("Failed to destroy server id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	rdma_destroy_event_channel(cm_event_channel);
	printf("Server shut-down is complete \n");
	return 0;
}


void usage() 
{
	printf("Usage:\n");
	printf("rdma_server: [-a <server_addr>] [-p <server_port>]\n");
	printf("(default port is %d)\n", DEFAULT_RDMA_PORT);
	exit(1);
}

void init_nvm_file( char * _name){
 
	//  int isPmem;
	// if ((file_buffer = pmem_map_file(_name,FILE_SIZE,
	// 			PMEM_FILE_CREATE,
	// 			0666, 0, &isPmem)) == NULL) {
	// 	perror("pmem_map_file");
	// 	exit(1);
	// }
	// printf("isPmem=%d\n",isPmem);
	// open(_name, O_RDWR|O_CREAT, 0666);
	// posix_fallocate(fd1, 0, FILE_SIZE);	
	// file_buffer = mmap(NULL, FILE_SIZE, PROT_READ | PROT_WRITE|PROT_EXEC, MAP_SHARED, fd1, 0);

// int pmem_fd;
pmem_fd = open(_name, O_RDWR|O_CREAT, 0666);
posix_fallocate(pmem_fd, 0, FILE_SIZE);	
file_buffer = mmap(0, FILE_SIZE, PROT_READ|PROT_WRITE, MAP_SHARED, pmem_fd, 0);
printf("pmem_fd=%d\n",pmem_fd);

	// int srcfd;

	// struct stbuf;
	// /* open src-file */
	// if ((srcfd = open(_name, O_RDWR)) < 0) {	
	// 	exit(1);
	// }

	// /* find the size of the src-file */
	// if (fstat(srcfd, &stbuf) < 0) {
	// 	printf("here");
	// 	exit(1);
	// }

	/* create a pmem file and memory map it */
	
}

int main(int argc, char **argv) 
{
//file_buffer = (char*)malloc(BUFFER_SIZE*BUFFER_SIZE);
 	//init_nvm_file("./db1");
	 init_nvm_file("../pmem6/db");
	printf("server:file_buffer:%p\n",file_buffer);
	init_hashmap();
	int ret, option;
	struct sockaddr_in server_sockaddr;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */
	/* Parse Command Line Arguments, not the most reliable code */

	send_region = (char*)malloc(SEND_REGION_MAX_LEN);
	recv_region = (char*)malloc(BUFFER_SIZE);
	recv_buffer = (char*)malloc(BUFFER_SIZE);
	//
	memset(send_region,0,sizeof(send_region));
	memset(recv_region,0,sizeof(recv_region));
	if(!server_sockaddr.sin_port) {
		/* If still zero, that mean no port info provided */
		server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT); /* use default port */
	 }
	ret = start_rdma_server(&server_sockaddr);
	if (ret) {
		rdma_error("RDMA server failed to start cleanly, ret = %d \n", ret);
		return ret;
	}
	ret = setup_client_resources();
	if (ret) { 
		rdma_error("Failed to setup client resources, ret = %d \n", ret);
		return ret;
	}
	ret = accept_client_connection();
	if (ret) {
		rdma_error("Failed to handle client cleanly, ret = %d \n", ret);
		return ret;
	}
	send_server_metadata();
	register_send_recv();


	// int now=clock();
	// for(;clock()-now<10;);
	// int i = 0;
	// // printf("dizhi:%p\n",file_buffer);
	// while(i<1000){
	// 	printf("file_buffer:%s\n",file_buffer+1000);
	// 	i++;
	// }

	//printf("0x%p\n",file_buffer);
	memset(recv_region,0,sizeof(recv_region));
	ret = ibv_post_recv(client_qp /* which QP */,
		      &recv_wr /* receive work request*/,
		      &bad_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
//	debug("Receive buffer pre-posting is successful \n");
	while(1){
		//printf("i=%d\n",itimes);
  		fdatasync(pmem_fd);
		ret = recv_data_and_post_next_recv();
		if (ret) {
			rdma_error("Failed to send server metadata to the client, ret = %d \n", ret);
			return ret;
		}		

		parse_command(recv_buffer);	

	
		post_send();

		//itimes++;
	
	}

		// 		int now=clock();
	// for(;clock()-now<1;);
	// ret = disconnect_and_cleanup();
	// if (ret) { 
	// 	rdma_error("Failed to clean up resources properly, ret = %d \n", ret);
	// 	return ret;
	// }
	return 0;
}
