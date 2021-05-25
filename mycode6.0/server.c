#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <rdma/rdma_cma.h>
#include "rdma_common.h"
#include <sys/types.h>  
#include<time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "define.h"
#include <libpmem.h>
#include<sys/mman.h>
#include <pthread.h>
#include <x86intrin.h> 
#define SEND_REGION_MAX_LEN 20
#define MAX_client_num 20

/* These are the RDMA resources needed to setup an RDMA connection */
/* Event channel, where connection management (cm) related events are relayed */
static struct rdma_event_channel *cm_event_channel = NULL;
static struct rdma_cm_id *cm_server_id = NULL;

char * file_buffer;
int pmem_fd ;
int itimes = 0;
int verbose=0;
int res_loc=0;
pthread_mutex_t mutex;
struct timespec ts;
typedef struct resourse
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
    
    
}resourse;
resourse* res[MAX_client_num];



int get_next_free_res(){
  int flag = 0;
  while(res[res_loc]!=NULL){
    res_loc++;
    if(res_loc==MAX_client_num){   
      flag++;   
      res_loc = 0; 
      if(flag==2){
        return -1;
      }    
    }
  }
  return res_loc;
}

int parse_command(int i){ 	
    char * str = res[i]->recv_buffer;
    int  num = str[0]-'0';
    char * key= malloc(KEY_LEN);
    memcpy(key,str+1,KEY_LEN);    
    memset(	res[i]->send_region,0,SEND_REGION_MAX_LEN);
    HASH_VALUE hash_val = check_hashval(key,0);
    if(hash_val==(ZONG_HASH_MAP_SIZE/server_num)){
        printf("hash qujian error!\n");
        check_hashval(key,1);
       
       snprintf(res[i]->send_region, SEND_REGION_MAX_LEN,"9\0");
       return 1;
   }
    //printf("%d :%s\n",num,key);
    // num = 0: get
    // num = 1: set
    // num = 2: del 
    // num = 3: del_lock
    if(num==0){
        // snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "0%-16s%10\0",key,linshi_lock);

        char * linshi_lock= malloc(LOCK_LEN);
        memcpy(linshi_lock,str+17,LOCK_LEN);    
     //   printf("linshi_lock:%s.len:%d\n",linshi_lock,strlen(linshi_lock));
        BLOCK_INDEX_TYPE block_index = 0;
        VALUE_LEN_TYPE record_len  = 0;

        int ret = nvm_Get(key,hash_val,&block_index,&record_len,linshi_lock);
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "%1d%08x%08lx\0",ret,block_index*BLOCK_SIZE,record_len); 
    }else if(num==1){


        //  snprintf(send_region, SEND_REGION_MAX_LEN, "1%-16s%08x%10s%\0",key,record_len,linshi_lock);

       
        char * linshi_lock= malloc(LOCK_LEN);    
        memcpy(linshi_lock,str+25,LOCK_LEN);
        char * record_len_str = malloc(8);
        memcpy(record_len_str,str+17,8);
       
        char * leftover;
        VALUE_LEN_TYPE record_len = strtoul (record_len_str, &leftover, 16);
      
        //printf("value_size:%d\n",value_size);
        BLOCK_INDEX_TYPE block_index = 0;
        int ret = nvm_Put(key,hash_val,record_len,&block_index,linshi_lock);
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "%1d%08x\0",ret,block_index*BLOCK_SIZE); 
    }else if(num==2){
        // snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "2%-16s%10s\0",key,linshi_lock);
        char * linshi_lock= malloc(LOCK_LEN);
        memcpy(linshi_lock,str+17,LOCK_LEN);
        int ret = nvm_Del(key,hash_val,linshi_lock);
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "%1d\0",ret); 
    }else if(num==3){
        // snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "3%-16s%10s\0",key,linshi_lock);
        char * linshi_lock= malloc(LOCK_LEN);
        memcpy(linshi_lock,str+17,LOCK_LEN);
        int ret = nvm_Del_lock(key,hash_val,linshi_lock);
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "%1d\0",ret); 
    }else if(num==4){
        return 1;
    }
    return 0;
}

/* Starts an RDMA server by allocating basic connection resources */
static int start_rdma_server() 
{

    int ret = -1;
    struct sockaddr_in server_sockaddr;
    struct sockaddr_in *server_addr = &server_sockaddr;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */	
	if(!server_sockaddr.sin_port) {
		/* If still zero, that mean no port info provided */
		server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT); /* use default port */
	}
	
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
	ret = rdma_listen(cm_server_id, MAX_client_num); /* backlog = 8 clients, same as TCP, see man listen*/
	if (ret) {
		rdma_error("rdma_listen failed to listen on server address, errno: %d ",
				-errno);
		return -errno;
	}
	printf("Server is listening successfully at: %s , port: %d \n",
			inet_ntoa(server_addr->sin_addr),
			ntohs(server_addr->sin_port));
    return ret;
}
/* Wait for an RDMA client connection */
static int wait_for_client(int i) {
    int ret = -1;
    struct rdma_cm_event *cm_event = NULL;
	
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
	res[i]->cm_client_id = cm_event->id;
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
/* When we call this function cm_client_id must be set to a valid identifier.
 * This is where, we prepare client connection before we accept it. This 
 * mainly involve pre-posting a receive buffer to receive client side 
 * RDMA credentials
 */
static int setup_client_resources(int i)
{
	int ret = -1;
	if(!res[i]->cm_client_id){
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
	res[i]->pd = ibv_alloc_pd(res[i]->cm_client_id->verbs 
			/* verbs defines a verb's provider, 
			 * i.e an RDMA device where the incoming 
			 * client connection came */);
	if (!res[i]->pd) {
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
	res[i]->io_completion_channel = ibv_create_comp_channel(res[i]->cm_client_id->verbs);
	if (!res[i]->io_completion_channel) {
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
	res[i]->cq = ibv_create_cq(res[i]->cm_client_id->verbs /* which device*/, 
			CQ_CAPACITY /* maximum capacity*/, 
			NULL /* user context, not used here */,
			res[i]->io_completion_channel /* which IO completion channel */, 
			0 /* signaling vector, not used here*/);
	if (!res[i]->cq) {
		rdma_error("Failed to create a completion queue (cq), errno: %d\n",
				-errno);
		return -errno;
	}
	// debug("Completion queue (CQ) is created at %p with %d elements \n", 
	// 		cq, cq->cqe);
	/* Ask for the event for all activities in the completion queue*/
	ret = ibv_req_notify_cq(res[i]->cq /* on which CQ */, 
			0 /* 0 = all event type, no filter*/);
	if (ret) {
		rdma_error("Failed to request notifications on CQ errno: %d \n",
				-errno);
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
       res[i]->qp_init_attr.recv_cq = res[i]->cq; /* Where should I notify for receive completion operations */
       res[i]->qp_init_attr.send_cq = res[i]->cq; /* Where should I notify for send completion operations */
       /*Lets create a QP */
       ret = rdma_create_qp(res[i]->cm_client_id /* which connection id */,
		       res[i]->pd /* which protection domain*/,
		       &res[i]->qp_init_attr /* Initial attributes */);
       if (ret) {
	       rdma_error("Failed to create QP due to errno: %d\n", -errno);
	       return -errno;
       }
       /* Save the reference for handy typing but is not required */
       res[i]->client_qp = res[i]->cm_client_id->qp;
     //  debug("Client QP created at %p\n", client_qp);
       return ret;
}

/* Pre-posts a receive buffer and accepts an RDMA client connection */
static int accept_client_connection(int i)
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;	
	int ret = -1;
	if(!res[i]->cm_client_id || !res[i]->client_qp) {
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
       ret = rdma_accept(res[i]->cm_client_id, &conn_param);
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
	memcpy(&res[i]->remote_sockaddr /* where to save */, 
			rdma_get_peer_addr(res[i]->cm_client_id) /* gives you remote sockaddr */, 
			sizeof(struct sockaddr_in) /* max size */);
	printf("A new connection is accepted from %s:%d \n", 
			inet_ntoa(res[i]->remote_sockaddr.sin_addr),res[i]->remote_sockaddr.sin_port);
    
	return ret;
}
static int send_server_metadata(int i){
	int ret = -1;
	res[i]->server_buffer_mr = rdma_buffer_register(res[i]->pd /* which protection domain */, 
			file_buffer,	
			FILE_SIZE /* what size to allocate */, 
			(IBV_ACCESS_LOCAL_WRITE|
			IBV_ACCESS_REMOTE_READ|
			IBV_ACCESS_REMOTE_WRITE) /* access permissions */);
    if(!res[i]->server_buffer_mr){
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
    

    res[i]->server_metadata_attr.address = (uint64_t) res[i]->server_buffer_mr->addr;
	res[i]->server_metadata_attr.length = (uint32_t) res[i]->server_buffer_mr->length;
	res[i]->server_metadata_attr.stag.local_stag = (uint32_t) res[i]->server_buffer_mr->lkey;



	struct ibv_sge server_send_sge;
	struct ibv_send_wr server_send_wr, *bad_server_send_wr = NULL;
	
	res[i]->server_metadata_mr = rdma_buffer_register(res[i]->pd /* which protection domain*/, 
			&res[i]->server_metadata_attr /* which memory to register */, 
			sizeof(res[i]->server_metadata_attr) /* what is the size of memory */,
			IBV_ACCESS_LOCAL_WRITE /* what access permission */);
	if(!res[i]->server_metadata_mr){
		rdma_error("Server failed to create to hold server metadata \n");
		/* we assume that this is due to out of memory error */
		return -ENOMEM;
	}
	/* We need to transmit this buffer. So we create a send request. 
	* A send request consists of multiple SGE elements. In our case, we only
	* have one 
	*/
	server_send_sge.addr = (uint64_t) &res[i]->server_metadata_attr;
	server_send_sge.length = sizeof(res[i]->server_metadata_attr);
	server_send_sge.lkey = res[i]->server_metadata_mr->lkey;
	/* now we link this sge to the send request */
	bzero(&server_send_wr, sizeof(server_send_wr));
	server_send_wr.sg_list = &server_send_sge;
	server_send_wr.num_sge = 1; // only 1 SGE element in the array 
	server_send_wr.opcode = IBV_WR_SEND; // This is a send request 
	server_send_wr.send_flags = IBV_SEND_SIGNALED; // We want to get notification 
	/* This is a fast data path operation. Posting an I/O request */
	ret = ibv_post_send(res[i]->client_qp /* which QP */, 
			&server_send_wr /* Send request that we prepared before */, 
			&bad_server_send_wr /* In case of error, this will contain failed requests */);
	if (ret) {
		rdma_error("Posting of server metdata failed, errno: %d \n",
				-errno);
		return -errno;
	}
	/* We check for completion notification */
	struct ibv_wc wc;
	ret = process_work_completion_events(res[i]->io_completion_channel, &wc, 1);
	if (ret != 1) {
		rdma_error("Failed to send server metadata, ret = %d \n", ret);
		return ret;
	}
	// debug("Local buffer metadata has been sent to the client \n");
	return 0;
}
static int register_send_recv(int i){
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
	res[i]->recv_sge.addr = (uint64_t) res[i]->recv_mr->addr;
	res[i]->recv_sge.length = (uint32_t) res[i]->recv_mr->length;
	res[i]->recv_sge.lkey = (uint32_t) res[i]->recv_mr->lkey;
	/* now we link it to the request */
	bzero(&res[i]->recv_wr, sizeof(res[i]->recv_wr));
	res[i]->recv_wr.sg_list = &res[i]->recv_sge;
	res[i]->recv_wr.num_sge = 1;

	
	return 0;
}
/* This function sends server side buffer metadata to the connected client */
static int recv_data_and_post_next_recv(int i) 
{
	
	struct ibv_wc wc;
	int ret = -1;
	/* Now, we first wait for the client to start the communication by 
	 * sending the server its metadata info. The server does not use it 
	 * in our example. We will receive a work completion notification for 
	 * our pre-posted receive request.
	 */

	ret = process_work_completion_events(res[i]->io_completion_channel, &wc, 1);
	if (ret != 1) {
		rdma_error("Failed to receive , ret = %d \n", ret);
		return ret;
	}
	/* if all good, then we should have client's buffer information, lets see */
    // if(verbose){
	//     printf("%s recved message:%s,len:%d\n",inet_ntoa(res[i]->remote_sockaddr.sin_addr),res[i]->recv_region,strlen(res[i]->recv_region));
    // }
	sprintf(res[i]->recv_buffer,res[i]->recv_region);	
	memset(res[i]->recv_region,0,sizeof(res[i]->recv_region));

	ret = ibv_post_recv(res[i]->client_qp /* which QP */,
		      &res[i]->recv_wr /* receive work request*/,
		      &res[i]->bad_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
	//debug("Receive buffer pre-posting is successful!!!!!!! \n");
	return ret;
}
static int post_send(int i)
{
	struct ibv_wc wc[1];
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
			wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}
	// debug("Server sent us its buffer location and credentials, showing \n");
    // if(verbose){
    //     printf("%s sended message:%s,len:%d\n",inet_ntoa(res[i]->remote_sockaddr.sin_addr), res[i]->send_region,strlen(res[i]->send_region));
    // }	
	return 0;
}

/* This is server side logic. Server passively waits for the client to call 
 * rdma_disconnect() and then it will clean up its resources */
static int disconnect_and_cleanup_client(int i)
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
	rdma_destroy_qp(res[i]->cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(res[i]->cm_client_id);
	if (ret) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(res[i]->cq);
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
    free(res[i]->send_region);
	free(res[i]->recv_region);
	free(res[i]->recv_buffer);   
	ibv_dereg_mr(res[i]->send_mr);
	ibv_dereg_mr(res[i]->recv_mr);
    ibv_dereg_mr(res[i]->server_buffer_mr);
    ibv_dereg_mr(res[i]->server_metadata_mr);
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(res[i]->pd);
	if (ret) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);    
		// we continue anyways;
	}

    free(res[i]);
    res[i]=NULL;

    return 0;
}
static int cleanup_server(){
    int ret = -1;
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
    printf("./server: [-v <1 or 0>] \n");
    printf("(default -v is 0 representing not printf\n");
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

}

int nano_delay(long delay)
{
    struct timespec req, rem;
    long nano_delay = delay;
    int ret = 0;
    while(nano_delay > 0)
    {
            rem.tv_sec = 0;
            rem.tv_nsec = 0;
            req.tv_sec = 0;
            req.tv_nsec = nano_delay;
            if(ret = (nanosleep(&req, &rem) == -1))
            {
                printf("nanosleep failed.\n");                
            }
            nano_delay = rem.tv_nsec;
    };
    return ret;
}
void task(int i){
	int ret = setup_client_resources(i);
	if (ret) { 
		rdma_error("Failed to setup client resources, ret = %d \n", ret);
		pthread_exit( (void *)ret);
	}
    
    /* Parse Command Line Arguments, not the most reliable code */

	
	ret = accept_client_connection(i);
	if (ret) {
		rdma_error("Failed to handle client cleanly, ret = %d \n", ret);
		pthread_exit( (void *)ret);
	}
	send_server_metadata(i);
	register_send_recv(i);

	//printf("0x%p\n",file_buffer);
	memset(res[i]->recv_region,0,sizeof(res[i]->recv_region));
	ret = ibv_post_recv(res[i]->client_qp /* which QP */,
		      &res[i]->recv_wr /* receive work request*/,
		      &res[i]->bad_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		pthread_exit( (void *)ret);
	}
	//debug("Receive buffer pre-posting is successful \n");
	while(1){
       
  	
		ret = recv_data_and_post_next_recv(i);
		if (ret) {
			rdma_error("Failed to send server metadata to the client, ret = %d \n", ret);
			pthread_exit( (void *)ret);
		}	
         pthread_mutex_lock (&mutex);
        int flag = parse_command(i);
       // printf("here %s:\n",inet_ntoa(res[i]->remote_sockaddr.sin_addr));
         pthread_mutex_unlock(&mutex);
        if(flag){
         //   printf("error flag=%d\n",flag);
            break;
        }
        //_mm_clflushopt(pmem_fd);
        fdatasync(pmem_fd);
		post_send(i);
       
        // nano_delay(1000);
        // pthread_delay_np(&ts);
	}

    ret = disconnect_and_cleanup_client(i);
	if (ret) { 
		rdma_error("Failed to clean up resources properly, ret = %d \n", ret);
		pthread_exit( (void *)ret);
	}
   
    
    pthread_exit( (void *)ret);
   
}
int main(int argc, char **argv) 
{
//file_buffer = (char*)malloc(BUFFER_SIZE*BUFFER_SIZE);
 	init_nvm_file("./db1");
	// init_nvm_file("../pmem6/db");
	printf("server:file_buffer:%p\n",file_buffer);
   
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
	init_hashmap();
    memset(res,NULL,sizeof(res));
    pthread_mutex_init(&mutex,NULL);
    
	
	int ret;
	ret = start_rdma_server();
	if (ret) {
		rdma_error("RDMA server failed to start cleanly, ret = %d \n", ret);
		return ret;
	}

    pthread_attr_t attr;
    pthread_attr_init(&attr);         //初始化线程属性结构
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);   //设置attr结构为分离
    pthread_t pids[MAX_client_num];
    ts.tv_sec = 2;
    ts.tv_nsec = 0;
    while(1){
        int i =get_next_free_res();
        res[i]=(struct resourse *)malloc(sizeof(struct resourse));
		memset(res[i],0,sizeof(res[i]));
        res[i]->send_region = (char*)malloc(SEND_REGION_MAX_LEN);
        res[i]->recv_region = (char*)malloc(BUFFER_SIZE);
        res[i]->recv_buffer = (char*)malloc(BUFFER_SIZE);
        //
        memset(res[i]->send_region,0,sizeof(res[i]->send_region));
        memset(res[i]->recv_region,0,sizeof(res[i]->recv_region));


        ret = wait_for_client(i);
        if (ret) {
            rdma_error("RDMA server failed to wait for client, ret = %d \n", ret);
            return ret;
	    }
        ret = pthread_create(&pids[i], &attr, task, i);  
        if (ret != 0) {
            printf("create thread failed.\n");
            exit(1);
        }
    }

       

	ret = cleanup_server();
	if (ret) { 
		rdma_error("Failed to clean up resources properly, ret = %d \n", ret);
		return ret;
	}
	return 0;
}
