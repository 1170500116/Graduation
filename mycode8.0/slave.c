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
#include <libpmem.h>
#include<sys/mman.h>
#include <pthread.h>
#include <x86intrin.h> 


#define SEND_REGION_MAX_LEN 20
static int DEFAULT_RDMA_PORT=20086;
/* These are the RDMA resources needed to setup an RDMA connection */
/* Event channel, where connection management (cm) related events are relayed */
static struct rdma_event_channel *cm_event_channel = NULL;
static struct rdma_cm_id *cm_server_id = NULL;


char * file_buffer;
char * start_buffer;
char * log_buffer;
int file_exist;
int pmem_fd ;
int plog_fd;
int verbose=0;
int log_offset=0;
int recv_num = 0;


resourse_server* res;



void myputline(char * src,int len){
    log_buffer[log_offset] = ',';
    log_offset++;
    memcpy(log_buffer+log_offset,src,len);
    log_offset+=len;
    log_buffer[log_offset] = '\0';
    //fdatasync(plog_fd);
}
int parse_command(){ 	
     //char * str = res->recv_buffer;
     char * str = res->recv_region;
    int  num = str[0]-'0';
    if(num==4){
        return 1;
    }
    // char * key= malloc(KEY_LEN);
    // memcpy(key,str+1,KEY_LEN);    
   
//     HASH_VALUE hash_val = check_hashval(key,0);
//     if(hash_val==(ZONG_HASH_MAP_SIZE/server_num)){
//         printf("hash qujian error!\n");
//         check_hashval(key,1);
       
//        snprintf(res->send_region, SEND_REGION_MAX_LEN,"9\0");
//        return 1;
//    }
  
    // num = 1: set
    // num = 2: del 
    //myputline(str,strlen(str));
    
    return 0;
}

/***************************************

server functions

***************************************/



/* Starts an RDMA server by allocating basic connection resources */
int start_rdma_server(int port) 
{

    int ret = -1;
    struct sockaddr_in server_sockaddr;
    struct sockaddr_in *server_addr = &server_sockaddr;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */	
	if(!server_sockaddr.sin_port) {
		/* If still zero, that mean no port info provided */
		server_sockaddr.sin_port = htons(port); /* use default port */
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
	ret = rdma_listen(cm_server_id, 1); /* backlog = 8 clients, same as TCP, see man listen*/
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
int wait_for_client() {
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
	res->cm_client_id = cm_event->id;
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
int setup_client_resources()
{
	int ret = -1;
	if(!res->cm_client_id){
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
	res->pd = ibv_alloc_pd(res->cm_client_id->verbs 
			/* verbs defines a verb's provider, 
			 * i.e an RDMA device where the incoming 
			 * client connection came */);
	if (!res->pd) {
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
	res->io_completion_channel = ibv_create_comp_channel(res->cm_client_id->verbs);
	if (!res->io_completion_channel) {
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
	res->cq = ibv_create_cq(res->cm_client_id->verbs /* which device*/, 
			CQ_CAPACITY /* maximum capacity*/, 
			NULL /* user context, not used here */,
			res->io_completion_channel /* which IO completion channel */, 
			0 /* signaling vector, not used here*/);
	if (!res->cq) {
		rdma_error("Failed to create a completion queue (cq), errno: %d\n",
				-errno);
		return -errno;
	}
	// debug("Completion queue (CQ) is created at %p with %d elements \n", 
	// 		cq, cq->cqe);
	/* Ask for the event for all activities in the completion queue*/
	ret = ibv_req_notify_cq(res->cq /* on which CQ */, 
			0 /* 0 = all event type, no filter*/);
	if (ret) {
		rdma_error("Failed to request notifications on CQ errno: %d \n",
				-errno);
		return -errno;
	}
	/* Now the last step, set up the queue pair (send, recv) queues and their capacity.
	 * The capacity here is define statically but this can be probed from the 
	 * device. We just use a small number as defined in rdma_common.h */
       bzero(&res->qp_init_attr, sizeof (res->qp_init_attr));
       res->qp_init_attr.cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
       res->qp_init_attr.cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
       res->qp_init_attr.cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
       res->qp_init_attr.cap.max_send_wr = MAX_WR; /* Maximum send posting capacity */
       res->qp_init_attr.qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
       /* We use same completion queue, but one can use different queues */
       res->qp_init_attr.recv_cq = res->cq; /* Where should I notify for receive completion operations */
       res->qp_init_attr.send_cq = res->cq; /* Where should I notify for send completion operations */
       /*Lets create a QP */
       ret = rdma_create_qp(res->cm_client_id /* which connection id */,
		       res->pd /* which protection domain*/,
		       &res->qp_init_attr /* Initial attributes */);
       if (ret) {
	       rdma_error("Failed to create QP due to errno: %d\n", -errno);
	       return -errno;
       }
       /* Save the reference for handy typing but is not required */
       res->client_qp = res->cm_client_id->qp;
     //  debug("Client QP created at %p\n", client_qp);
       return ret;
}

/* Pre-posts a receive buffer and accepts an RDMA client connection */
int accept_client_connection()
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;	
	int ret = -1;
	if(!res->cm_client_id || !res->client_qp) {
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
       ret = rdma_accept(res->cm_client_id, &conn_param);
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
	memcpy(&res->remote_sockaddr /* where to save */, 
			rdma_get_peer_addr(res->cm_client_id) /* gives you remote sockaddr */, 
			sizeof(struct sockaddr_in) /* max size */);
	printf("A new connection is accepted from %s:%d \n", 
			inet_ntoa(res->remote_sockaddr.sin_addr),res->remote_sockaddr.sin_port);
    
	return ret;
}
int send_server_metadata(char * filebuffer,uint32_t filesize){
	int ret = -1;
	res->server_buffer_mr = rdma_buffer_register(res->pd /* which protection domain */, 
			filebuffer,	
			filesize /* what size to allocate */, 
			(IBV_ACCESS_LOCAL_WRITE|
			IBV_ACCESS_REMOTE_READ|
			IBV_ACCESS_REMOTE_WRITE) /* access permissions */);
    if(!res->server_buffer_mr){
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
    

    res->server_metadata_attr.address = (uint64_t) res->server_buffer_mr->addr;
	res->server_metadata_attr.length = (uint32_t) res->server_buffer_mr->length;
	res->server_metadata_attr.stag.local_stag = (uint32_t) res->server_buffer_mr->lkey;



	struct ibv_sge server_send_sge;
	struct ibv_send_wr server_send_wr, *bad_server_send_wr = NULL;
	
	res->server_metadata_mr = rdma_buffer_register(res->pd /* which protection domain*/, 
			&res->server_metadata_attr /* which memory to register */, 
			sizeof(res->server_metadata_attr) /* what is the size of memory */,
			IBV_ACCESS_LOCAL_WRITE /* what access permission */);
	if(!res->server_metadata_mr){
		rdma_error("Server failed to create to hold server metadata \n");
		/* we assume that this is due to out of memory error */
		return -ENOMEM;
	}
	/* We need to transmit this buffer. So we create a send request. 
	* A send request consists of multiple SGE elements. In our case, we only
	* have one 
	*/
	server_send_sge.addr = (uint64_t) &res->server_metadata_attr;
	server_send_sge.length = sizeof(res->server_metadata_attr);
	server_send_sge.lkey = res->server_metadata_mr->lkey;
	/* now we link this sge to the send request */
	bzero(&server_send_wr, sizeof(server_send_wr));
	server_send_wr.sg_list = &server_send_sge;
	server_send_wr.num_sge = 1; // only 1 SGE element in the array 
	server_send_wr.opcode = IBV_WR_SEND; // This is a send request 
	server_send_wr.send_flags = IBV_SEND_SIGNALED; // We want to get notification 
	/* This is a fast data path operation. Posting an I/O request */
	ret = ibv_post_send(res->client_qp /* which QP */, 
			&server_send_wr /* Send request that we prepared before */, 
			&bad_server_send_wr /* In case of error, this will contain failed requests */);
	if (ret) {
		rdma_error("Posting of server metdata failed, errno: %d \n",
				-errno);
		return -errno;
	}
	/* We check for completion notification */
    struct ibv_wc *wc;
	ret = process_work_completion_events(res->io_completion_channel, &wc, 1);
	if (ret != 1) {
		rdma_error("Failed to send server metadata, ret = %d \n", ret);
		return ret;
	}
	// debug("Local buffer metadata has been sent to the client \n");
	return 0;
}
int register_send_recv(){
	res->send_mr = rdma_buffer_register(res->pd,
			res->send_region,
			SEND_REGION_MAX_LEN,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if(!res->send_mr){
		rdma_error("Failed to register the send region\n");
		return -1;
	}
	/* now we fill up SGE */
	res->send_sge.addr = (uint64_t) res->send_mr->addr;
	res->send_sge.length = (uint32_t) res->send_mr->length;
	res->send_sge.lkey = res->send_mr->lkey;
	/* now we link to the send work request */
	bzero(&res->send_wr, sizeof(res->send_wr));
	res->send_wr.sg_list = &res->send_sge;
	res->send_wr.num_sge = 1;
	res->send_wr.opcode = IBV_WR_SEND;
	res->send_wr.send_flags = IBV_SEND_SIGNALED;


	res->recv_mr = rdma_buffer_register(res->pd,
			res->recv_region,
			SEND_RECV_BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE));
	if(!res->recv_mr){
		rdma_error("Failed to register the recv region\n");
		return -1;
	}
	res->recv_sge.addr = (uint64_t) res->recv_mr->addr;
	res->recv_sge.length = (uint32_t) res->recv_mr->length;
	res->recv_sge.lkey = (uint32_t) res->recv_mr->lkey;
	/* now we link it to the request */
	bzero(&res->recv_wr, sizeof(res->recv_wr));
	res->recv_wr.sg_list = &res->recv_sge;
	res->recv_wr.num_sge = 1;

	
	return 0;
}
/* This function sends server side buffer metadata to the connected client */
int recv_data_and_post_next_recv() 
{
	
	struct ibv_wc wc;
	int ret = -1;
	/* Now, we first wait for the client to start the communication by 
	 * sending the server its metadata info. The server does not use it 
	 * in our example. We will receive a work completion notification for 
	 * our pre-posted receive request.
	 */
    ret = ibv_post_recv(res->client_qp /* which QP */,
		      &res->recv_wr /* receive work request*/,
		      &res->bad_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}

    recv_num++;
	ret = process_work_completion_events(res->io_completion_channel, &wc, 1);
	if (ret != 1) {
		rdma_error("Failed to receive , ret = %d \n", ret);
		return ret;
	}
	/* if all good, then we should have client's buffer information, lets see */
    if(verbose){
	    printf("%s recved message:%s,len:%d\n",inet_ntoa(res->remote_sockaddr.sin_addr),res->recv_region,strlen(res->recv_region));
    }
    recv_num--;
	// sprintf(res->recv_buffer,res->recv_region);	
	// memset(res->recv_region,0,sizeof(res->recv_region));

	
    
	//debug("Receive buffer pre-posting is successful!!!!!!! \n");
	return ret;
}


/* This is server side logic. Server passively waits for the client to call 
 * rdma_disconnect() and then it will clean up its resources */
int disconnect_and_cleanup_client()
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
	rdma_destroy_qp(res->cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(res->cm_client_id);
	if (ret) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(res->cq);
	if (ret) {
		rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy completion channel */
	ret = ibv_destroy_comp_channel(res->io_completion_channel);
	if (ret) {
		rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy memory buffers */
    free(res->send_region);
	free(res->recv_region);
	free(res->recv_buffer);   
	ibv_dereg_mr(res->send_mr);
	ibv_dereg_mr(res->recv_mr);
    ibv_dereg_mr(res->server_buffer_mr);
    ibv_dereg_mr(res->server_metadata_mr);
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(res->pd);
	if (ret) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);    
		// we continue anyways;
	}

    free(res);
    res=NULL;

    return 0;
}
int cleanup_server(){
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


int post_send_server()
{
	struct ibv_wc wc[1];
	int ret = -1;	
	/* Now we post it */
	ret = ibv_post_send(res->client_qp, 
		       &res->send_wr,
	       &res->bad_send_wr);
	if (ret) {
		rdma_error("Failed to send client metadata, errno: %d \n", 
				-errno);
		return -errno;
	}
	/* at this point we are expecting 2 work completion. One for our 
	 * send and one for recv that we will get from the server for 
	 * its buffer information */
	ret = process_work_completion_events(res->io_completion_channel, 
			wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}
	// debug("Server sent us its buffer location and credentials, showing \n");
    if(verbose){
        printf("%s sended message:%s,len:%d\n",inet_ntoa(res->remote_sockaddr.sin_addr), res->send_region,strlen(res->send_region));
    }	
	return 0;
}
void usage() 
{
	printf("Usage:\n");
    printf("./slave: [-v <1 or 0>][-p listening port] \n");
    printf("(default -v is 0 representing not printf\n");
    printf("(default -p is 20086\n");
    exit(1);
}


void init_nvm_file( char * _name,char * log_name){
 
	//  int isPmem;
	// if ((file_buffer = pmem_map_file(_name,FILE_SIZE,
	// 			PMEM_FILE_CREATE|PMEM_FILE_CREATE,
	// 			0666, 0, &isPmem)) == NULL) {
	// 	perror("pmem_map_file");
	// 	exit(1);
	// }
	// printf("isPmem=%d\n",isPmem);

// int pmem_fd;
    FILE *fp=fopen(_name ,"r");
    if(fp == NULL){
        file_exist = 0;
    }else{
        file_exist = 1;
        fclose(fp);
    }
    printf("file_exist:%d\n",file_exist);
    pmem_fd = open(_name, O_RDWR|O_CREAT, 0666);
    FILE_SIZE_qian =cal_start_offset();
    FILE_SIZE_zong = FILE_SIZE_qian+FILE_SIZE_hou;
    posix_fallocate(pmem_fd, 0, FILE_SIZE_zong);
   
    file_buffer = mmap(0, FILE_SIZE_zong, PROT_READ|PROT_WRITE, MAP_SHARED, pmem_fd, 0);
    start_buffer = file_buffer+FILE_SIZE_qian;
    printf("pmem_fd=%d\n",pmem_fd);



    int log_exist=0;
    fp=fopen(log_name ,"r");
    if(fp == NULL){
        log_exist = 0;
    }else{
        log_exist = 1;
        fclose(fp);
    }
    printf("log_file_exist:%d\n",file_exist);
    plog_fd = open(log_name, O_RDWR|O_CREAT, 0666);
    posix_fallocate(plog_fd, 0, FILE_SIZE_log);	
    log_buffer = mmap(0, FILE_SIZE_log, PROT_READ|PROT_WRITE, MAP_SHARED, plog_fd, 0);
    if(!log_exist){
        *log_buffer='\0';
        log_offset=0;
        fdatasync(plog_fd);
    }else{
        log_offset=strlen(log_buffer);
    }
    
}


void create_recv(){
    int ret=0;
    while(1){
        if(recv_num<15){
            memset(res->recv_region,0,sizeof(res->recv_region));   
            ret = ibv_post_recv(res->client_qp /* which QP */,
                    &res->recv_wr /* receive work request*/,
                    &res->bad_recv_wr /* error WRs */);
            if (ret) {
                rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
                pthread_exit( (void *)ret);
            }
            recv_num++;
        }
    }

}
void task(){
	int ret = setup_client_resources();
	if (ret) { 
		rdma_error("Failed to setup client resources, ret = %d \n", ret);
	}
    
    /* Parse Command Line Arguments, not the most reliable code */

	
	ret = accept_client_connection();
	if (ret) {
		rdma_error("Failed to handle client cleanly, ret = %d \n", ret);
	
	}
	send_server_metadata(start_buffer,FILE_SIZE_hou);
    send_server_metadata(log_buffer+log_offset,FILE_SIZE_log-log_offset);
	register_send_recv();

    pthread_attr_t attr;
    pthread_attr_init(&attr);         //初始化线程属性结构
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);   //设置attr结构为分离
    pthread_t pids;
    ret = pthread_create(&pids, &attr, create_recv, NULL); 
	//printf("0x%p\n",start_buffer);
	
    
	//debug("Receive buffer pre-posting is successful \n");
	while(1){
       
  
		ret = recv_data_and_post_next_recv();
		
     
        int flag = parse_command();

        if(flag){
         //   printf("error flag=%d\n",flag);
            break;
        }
        //_mm_clflushopt(pmem_fd);
        // fdatasync(pmem_fd);
        //post_send_server(i);
       
	}

    ret = disconnect_and_cleanup_client();
	if (ret) { 
		rdma_error("Failed to clean up resources properly, ret = %d \n", ret);
		pthread_exit( (void *)ret);
	}
   
    
    pthread_exit( (void *)ret);
   
}

int main(int argc, char **argv) 
{

 	init_nvm_file("/home/wanru/nvm/db1","/home/wanru/nvm/log.txt");
	// init_nvm_file("../pmem6/db");
	
    int option;
    while ((option = getopt(argc, argv, "v:p:")) != -1) {
        switch (option) {			
            case 'v':
                verbose = strtol(optarg, NULL, 0);
                break;
            case 'p':
                DEFAULT_RDMA_PORT = strtol(optarg, NULL, 0);
            default:
                usage();			
                break;
            }
	}
	init_hashmap(file_buffer);
    printf("slave:file_buffer:%p\n",file_buffer);
    printf("slave:log_buffer:%p\n",log_buffer);
    
	int ret;
	ret = start_rdma_server(DEFAULT_RDMA_PORT);

	if (ret) {
		rdma_error("RDMA server failed to start cleanly, ret = %d \n", ret);
		return ret;
	}

    res=(struct resourse_server *)malloc(sizeof(struct resourse_server));
    memset(res,0,sizeof(res));
    res->send_region = (char*)malloc(SEND_REGION_MAX_LEN);
    res->recv_region = (char*)malloc(SEND_RECV_BUFFER_SIZE);
    res->recv_buffer = (char*)malloc(SEND_RECV_BUFFER_SIZE);
    //
    memset(res->send_region,0,sizeof(res->send_region));
    memset(res->recv_region,0,sizeof(res->recv_region));


    ret = wait_for_client();
    if (ret) {
        rdma_error("RDMA server failed to wait for client, ret = %d \n", ret);
        return ret;
    }
    task();

	ret = cleanup_server();
	if (ret) { 
		rdma_error("Failed to clean up resources properly, ret = %d \n", ret);
		return ret;
	}
	return 0;
}
