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


#define SEND_REGION_MAX_LEN 50
#define MAX_client_num 200
#define slave_num 1
int slave_offset = 0;
static char * slave_ip[slave_num];
static int DEFAULT_RDMA_PORT=20086;

/* These are the RDMA resources needed to setup an RDMA connection */
/* Event channel, where connection management (cm) related events are relayed */
static struct rdma_event_channel *cm_event_channel = NULL;
static struct rdma_cm_id *cm_server_id = NULL;
pthread_attr_t attr;
pthread_t pids[MAX_client_num];

char * file_buffer;
char * start_buffer;
int file_exist;
int pmem_fd ;
int verbose=0;
pthread_mutex_t mutex;
int ioshu = 0;  
int now_client_num=0;
resourse_server* res[MAX_client_num];
resourse_client* res_slave[slave_num];

int get_next_free_res(){
  int i=0;
  for(i=0;i<MAX_client_num;i++){
      if(res[i]==NULL){
          return i;
      }
  }
  return -1;
}

int parse_command(int i){ 	
    ioshu++;
    char * str = res[i]->recv_buffer;
    int  num = str[0]-'0';
    if(num==4){
        return 1;
    }
    char * key=str+1;
    memset(	res[i]->send_region,0,SEND_REGION_MAX_LEN);
    HASH_VALUE hash_val = check_hashval(key,0);
    if(hash_val==(ZONG_HASH_MAP_SIZE/server_num)){
        printf("hash qujian error!\n");
        check_hashval(key,1);
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN,"9\0");
        return 1;
    }
   
     //time_t 8bytes signed long 
        time_t timep;
        time(&timep); //获取从1970至今过了多少秒，存入time_t类型的timep
        timep+=LOCK_TIME;
    // num = 0: get
    // num = 1: set
    // num = 2: del 
    // num = 3: del_lock
    if(num==0){
        // snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "0%-16s%14s\0",key,linshi_lock);
         char * linshi_lock =  str+17;
        BLOCK_INDEX_TYPE block_index = 0;
        VALUE_LEN_TYPE record_len  = 0;      
        int ret = nvm_Get(key,hash_val,&block_index,&record_len,linshi_lock,timep);
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "%1d%08x%08lx\0",ret,block_index*BLOCK_SIZE,record_len); 
        
    }else if(num==1){

        //  snprintf(send_region, SEND_REGION_MAX_LEN, "1%-16s%08x\n%14s%\0",key,record_len,linshi_lock);
        char * linshi_lock = str+26;
        
        VALUE_LEN_TYPE record_len = strtoul (str+17, NULL, 16);
       
        BLOCK_INDEX_TYPE block_index = 0;
        // printf("num==1:%s",str+1);
		// 经测验，在num==1处可能有segment fault 待解决
        int ret = nvm_Put(key,hash_val,record_len,&block_index,linshi_lock,timep,2);
       snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "%1d%08x\0",ret,block_index*BLOCK_SIZE); 
    }else if(num==2){
        // snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "2%-16s\0",key);      
        int ret = nvm_Del(key,hash_val,"",0);
        
        if(ret==1){
            memcpy(res_slave[0]->src,str,18);
            res_slave[0]->src[18]='\n';
            res_slave[0]->src[19]='\0';
            post_write_log(slave_offset,20,0);
            slave_offset+=19;
        }          
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "%1d\0",ret); 
    }else if(num==3){
        // snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "3%-16s%14s",key,linshi_lock);
  
        char * linshi_lock = str+17;
        BLOCK_INDEX_TYPE block_index = 0;
        VALUE_LEN_TYPE record_len  = 0; 
        int isput =0;
        if(*(str+31)!='\0'){
            isput = *(str+31)-'0';
        }
	
        int ret = nvm_Del_lock(key,hash_val,linshi_lock,isput,&record_len,&block_index);
        if(ret==1 && isput==1){  	printf("3\n");
            uint64_t offset = block_index*BLOCK_SIZE;      
            ibv_dereg_mr(res_slave[0]->client_src_mr);
            res_slave[0]->client_src_mr = rdma_buffer_register(res_slave[0]->pd,
			start_buffer+offset,
			record_len+1,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
            if (!res_slave[0]->client_src_mr) {
                rdma_error("We failed to create the source buffer, -ENOMEM\n");
                return -ENOMEM;
            }

            /* Step 1: is to copy the local buffer into the remote buffer. We will 
            * reuse the previous variables. */
            /* now we fill up SGE */
            res_slave[0]->write_sge.addr = (uint64_t) res_slave[0]->client_src_mr->addr;
            res_slave[0]->write_sge.length = (uint32_t) res_slave[0]->client_src_mr->length;	
            res_slave[0]->write_sge.lkey = res_slave[0]->client_src_mr->lkey;	

            // printf("write record\n"); 
	        post_write(offset,record_len,0);
			
            char mykey[KEY_LEN+1] ;
            memcpy(mykey,key,KEY_LEN);
			mykey[KEY_LEN]='\0';
            snprintf(res_slave[0]->src_log, SEND_REGION_MAX_LEN, "1%-16s%08x%08x\n\0",mykey,record_len,offset); 
    	    // printf("write log\n");
            post_write_log(slave_offset,35,0); 
            slave_offset+=34; 
        }         
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "%1d\0",ret); 
    }else if(num==4){
        return 1;
    }
    return 0;
}



/***************************************

client functions

***************************************/

int client_prepare_connection(struct sockaddr_in *s_addr,int i)
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/*  Open a channel used to report asynchronous communication event */
	res_slave[i]->cm_event_channel = rdma_create_event_channel();
	if (!res_slave[i]->cm_event_channel) {
		rdma_error("Creating cm event channel failed, errno: %d \n", -errno);
		return -errno;
	}
	//debug("RDMA CM event channel is created at : %p \n", cm_event_channel);
	/* rdma_cm_id is the connection identifier (like socket) which is used 
	 * to define an RDMA connection. 
	 */
	ret = rdma_create_id(res_slave[i]->cm_event_channel, &res_slave[i]->cm_client_id, 
			NULL,
			RDMA_PS_TCP);
	if (ret) {
		rdma_error("Creating cm id failed with errno: %d \n", -errno); 
		return -errno;
	}
	/* Resolve destination and optional source addresses from IP addresses  to
	 * an RDMA address.  If successful, the specified rdma_cm_id will be bound
	 * to a local device. */
	ret = rdma_resolve_addr(res_slave[i]->cm_client_id, NULL, (struct sockaddr*) s_addr, 2000);
	if (ret) {
		rdma_error("Failed to resolve address, errno: %d \n", -errno);
		return -errno;
	}
	//debug("waiting for cm event: RDMA_CM_EVENT_ADDR_RESOLVED\n");
	ret  = process_rdma_cm_event(res_slave[i]->cm_event_channel, 
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
	ret = rdma_resolve_route(res_slave[i]->cm_client_id, 2000);
	if (ret) {
		rdma_error("Failed to resolve route, erno: %d \n", -errno);
	       return -errno;
	}
	//debug("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED\n");
	ret = process_rdma_cm_event(res_slave[i]->cm_event_channel, 
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
	res_slave[i]->pd = ibv_alloc_pd(res_slave[i]->cm_client_id->verbs);
	if (!res_slave[i]->pd) {
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
	res_slave[i]->io_completion_channel = ibv_create_comp_channel(res_slave[i]->cm_client_id->verbs);
	if (!res_slave[i]->io_completion_channel) {
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
	res_slave[i]->client_cq = ibv_create_cq(res_slave[i]->cm_client_id->verbs /* which device*/, 
			CQ_CAPACITY /* maximum capacity*/, 
			NULL /* user context, not used here */,
			res_slave[i]->io_completion_channel /* which IO completion channel */, 
			0 /* signaling vector, not used here*/);
	if (!res_slave[i]->client_cq) {
		rdma_error("Failed to create CQ, errno: %d \n", -errno);
		return -errno;
	}
	//debug("CQ created at %p with %d elements \n", client_cq, client_cq->cqe);
	ret = ibv_req_notify_cq(res_slave[i]->client_cq, 0);
	if (ret) {
		rdma_error("Failed to request notifications, errno: %d\n", -errno);
		return -errno;
	}
       /* Now the last step, set up the queue pair (send, recv) queues and their capacity.
         * The capacity here is define statically but this can be probed from the 
	 * device. We just use a small number as defined in rdma_common.h */
	bzero(&res_slave[i]->qp_init_attr, sizeof (res_slave[i]->qp_init_attr));
	res_slave[i]->qp_init_attr.cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
	res_slave[i]->qp_init_attr.cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
	res_slave[i]->qp_init_attr.cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
	res_slave[i]->qp_init_attr.cap.max_send_wr = MAX_WR; /* Maximum send posting capacity */
	res_slave[i]->qp_init_attr.qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
	/* We use same completion queue, but one can use different queues */
	res_slave[i]->qp_init_attr.recv_cq = res_slave[i]->client_cq; /* Where should I notify for receive completion operations */
	res_slave[i]->qp_init_attr.send_cq = res_slave[i]->client_cq; /* Where should I notify for send completion operations */
	/*Lets create a QP */
	ret = rdma_create_qp(res_slave[i]->cm_client_id /* which connection id */,
			res_slave[i]->pd /* which protection domain*/,
			&res_slave[i]->qp_init_attr /* Initial attributes */);
	if (ret) {
		rdma_error("Failed to create QP, errno: %d \n", -errno);
	       return -errno;
	}
	res_slave[i]->client_qp = res_slave[i]->cm_client_id->qp;
	//debug("QP created at %p \n", client_qp);
	return 0;
}


/* Connects to the RDMA server */
int client_connect_to_server(int i) 
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	bzero(&conn_param, sizeof(conn_param));
	conn_param.initiator_depth = 3;
	conn_param.responder_resources = 3;
	conn_param.retry_count = 7; // if fail, then how many times to retry
	ret = rdma_connect(res_slave[i]->cm_client_id, &conn_param);
	if (ret) {
		rdma_error("Failed to connect to remote host , errno: %d\n", -errno);
		return -errno;
	}
	//debug("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED\n");
	ret = process_rdma_cm_event(res_slave[i]->cm_event_channel, 
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

int recv_server_metadata(int i){
	struct ibv_recv_wr server_recv_wr, *bad_server_recv_wr = NULL;
	struct ibv_sge  server_recv_sge;
	int ret = -1;

	res_slave[i]->server_metadata_mr = rdma_buffer_register(res_slave[i]->pd,
			&res_slave[i]->server_metadata_attr,
			sizeof(res_slave[i]->server_metadata_attr),
			(IBV_ACCESS_LOCAL_WRITE));
	if(!res_slave[i]->server_metadata_mr){
		rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
		return -ENOMEM;
	}
	server_recv_sge.addr = (uint64_t) res_slave[i]->server_metadata_mr->addr;
	server_recv_sge.length = (uint32_t) res_slave[i]->server_metadata_mr->length;
	server_recv_sge.lkey = (uint32_t) res_slave[i]->server_metadata_mr->lkey;
	/* now we link it to the request */
	bzero(&server_recv_wr, sizeof(server_recv_wr));
	server_recv_wr.sg_list = &server_recv_sge;
	server_recv_wr.num_sge = 1;
	ret = ibv_post_recv(res_slave[i]->client_qp /* which QP */,
		      &server_recv_wr /* receive work request*/,
		      &bad_server_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
	//debug("Receive buffer pre-posting is successful \n");
	struct ibv_wc wc;
	ret = process_work_completion_events(res_slave[i]->io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}	
    printf("db buffer attr:\n");
	show_rdma_buffer_attr(&res_slave[i]->server_metadata_attr);

    res_slave[i]->server_metadata_mr_log = rdma_buffer_register(res_slave[i]->pd,
			&res_slave[i]->server_metadata_attr_log,
			sizeof(res_slave[i]->server_metadata_attr_log),
			(IBV_ACCESS_LOCAL_WRITE));
	if(!res_slave[i]->server_metadata_mr_log){
		rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
		return -ENOMEM;
	}
	server_recv_sge.addr = (uint64_t) res_slave[i]->server_metadata_mr_log->addr;
	server_recv_sge.length = (uint32_t) res_slave[i]->server_metadata_mr_log->length;
	server_recv_sge.lkey = (uint32_t) res_slave[i]->server_metadata_mr_log->lkey;
	/* now we link it to the request */
	bzero(&server_recv_wr, sizeof(server_recv_wr));
	server_recv_wr.sg_list = &server_recv_sge;
	server_recv_wr.num_sge = 1;
	ret = ibv_post_recv(res_slave[i]->client_qp /* which QP */,
		      &server_recv_wr /* receive work request*/,
		      &bad_server_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
	//debug("Receive buffer pre-posting is successful \n");

	ret = process_work_completion_events(res_slave[i]->io_completion_channel, 
			&wc, 1);
	if(ret != 1) {
		rdma_error("We failed to get 1 work completions , ret = %d \n",
				ret);
		return ret;
	}	
    printf("log buffer attr:\n");
	show_rdma_buffer_attr(&res_slave[i]->server_metadata_attr_log);
	return 0;
}
int register_all(int i){
	res_slave[i]->send_mr = rdma_buffer_register(res_slave[i]->pd,
			res_slave[i]->send_region,
			SEND_REGION_MAX_LEN,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if(!res_slave[i]->send_mr){
		rdma_error("Failed to register the send region\n");
		return -1;
	}
	/* now we fill up SGE */
	res_slave[i]->send_sge.addr = (uint64_t) res_slave[i]->send_mr->addr;
	res_slave[i]->send_sge.length = (uint32_t) res_slave[i]->send_mr->length;
	res_slave[i]->send_sge.lkey = res_slave[i]->send_mr->lkey;
	/* now we link to the send work request */
	bzero(&res_slave[i]->send_wr, sizeof(res_slave[i]->send_wr));
	res_slave[i]->send_wr.sg_list = &res_slave[i]->send_sge;
	res_slave[i]->send_wr.num_sge = 1;
	res_slave[i]->send_wr.opcode = IBV_WR_SEND;
	res_slave[i]->send_wr.send_flags = IBV_SEND_SIGNALED;


	res_slave[i]->recv_mr = rdma_buffer_register(res_slave[i]->pd,
			res_slave[i]->recv_region,
			SEND_RECV_BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE));
	if(!res_slave[i]->recv_mr){
		rdma_error("Failed to register the recv region\n");
		return -1;
	}
	res_slave[i]->recv_sge.addr = (uint64_t)res_slave[i]->recv_mr->addr;
	res_slave[i]->recv_sge.length = (uint32_t) res_slave[i]->recv_mr->length;
	res_slave[i]->recv_sge.lkey = (uint32_t) res_slave[i]->recv_mr->lkey;
	/* now we link it to the request */
	bzero(&res_slave[i]->recv_wr, sizeof(res_slave[i]->recv_wr));
	res_slave[i]->recv_wr.sg_list = &res_slave[i]->recv_sge;
	res_slave[i]->recv_wr.num_sge = 1;




	res_slave[i]->client_src_mr = rdma_buffer_register(res_slave[i]->pd,
			res_slave[i]->src,
			SRC_DST_BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if (!res_slave[i]->client_src_mr) {
		rdma_error("We failed to create the source buffer, -ENOMEM\n");
		return -ENOMEM;
	}


	
	/* Step 1: is to copy the local buffer into the remote buffer. We will 
	 * reuse the previous variables. */
	/* now we fill up SGE */
	res_slave[i]->write_sge.addr = (uint64_t) res_slave[i]->client_src_mr->addr;
	res_slave[i]->write_sge.length = (uint32_t) res_slave[i]->client_src_mr->length;
	res_slave[i]->write_sge.lkey = res_slave[i]->client_src_mr->lkey;
	/* now we link to the send work request */
	bzero(&res_slave[i]->write_wr, sizeof(res_slave[i]->write_wr));
	res_slave[i]->write_wr.sg_list = &res_slave[i]->write_sge;
	res_slave[i]->write_wr.num_sge = 1;
	res_slave[i]->write_wr.opcode = IBV_WR_RDMA_WRITE;
	res_slave[i]->write_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	res_slave[i]->write_wr.wr.rdma.rkey = res_slave[i]->server_metadata_attr.stag.remote_stag;
	res_slave[i]->write_wr.wr.rdma.remote_addr = res_slave[i]->server_metadata_attr.address;
	//printf("remote_addr:%p\n",write_wr.wr.rdma.remote_addr);





	res_slave[i]->client_src_mr_log = rdma_buffer_register(res_slave[i]->pd,
			res_slave[i]->src_log,
			SRC_DST_BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if (!res_slave[i]->client_src_mr_log) {
		rdma_error("We failed to create the source buffer, -ENOMEM\n");
		return -ENOMEM;
	}
	
	/* Step 1: is to copy the local buffer into the remote buffer. We will 
	 * reuse the previous variables. */
	/* now we fill up SGE */
	res_slave[i]->write_sge_log.addr = (uint64_t) res_slave[i]->client_src_mr_log->addr;
	res_slave[i]->write_sge_log.length = (uint32_t) res_slave[i]->client_src_mr_log->length;
	res_slave[i]->write_sge_log.lkey = res_slave[i]->client_src_mr_log->lkey;
	/* now we link to the send work request */
	bzero(&res_slave[i]->write_wr_log, sizeof(res_slave[i]->write_wr_log));
	res_slave[i]->write_wr_log.sg_list = &res_slave[i]->write_sge_log;
	res_slave[i]->write_wr_log.num_sge = 1;
	res_slave[i]->write_wr_log.opcode = IBV_WR_RDMA_WRITE;
	res_slave[i]->write_wr_log.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	res_slave[i]->write_wr_log.wr.rdma.rkey = res_slave[i]->server_metadata_attr_log.stag.remote_stag;
	res_slave[i]->write_wr_log.wr.rdma.remote_addr = res_slave[i]->server_metadata_attr_log.address;
	//printf("remote_addr:%p\n",write_wr.wr.rdma.remote_addr);





    // bzero(&res_slave[i]->write_wr_log, sizeof(res_slave[i]->write_wr_log));
	// res_slave[i]->write_wr_log.sg_list = &res_slave[i]->write_sge;
	// res_slave[i]->write_wr_log.num_sge = 1;
	// res_slave[i]->write_wr_log.opcode = IBV_WR_RDMA_WRITE;
	// res_slave[i]->write_wr_log.send_flags = IBV_SEND_SIGNALED;
	// /* we have to tell server side info for RDMA */
	// res_slave[i]->write_wr_log.wr.rdma.rkey = res_slave[i]->server_metadata_attr_log.stag.remote_stag;
	// res_slave[i]->write_wr_log.wr.rdma.remote_addr = res_slave[i]->server_metadata_attr_log.address;



    res_slave[i]->client_dst_mr = rdma_buffer_register(res_slave[i]->pd,
			res_slave[i]->dst,
			SRC_DST_BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE | 
			 IBV_ACCESS_REMOTE_WRITE | 
			 IBV_ACCESS_REMOTE_READ));
	
	if (!res_slave[i]->client_dst_mr) {
		rdma_error("We failed to create the destination buffer, -ENOMEM\n");
		return -ENOMEM;
	}
	res_slave[i]->read_sge.addr = (uint64_t) res_slave[i]->client_dst_mr->addr;
	res_slave[i]->read_sge.length = (uint32_t) res_slave[i]->client_dst_mr->length;
	res_slave[i]->read_sge.lkey = res_slave[i]->client_dst_mr->lkey;
	/* now we link to the send work request */
	bzero(&res_slave[i]->read_wr, sizeof(res_slave[i]->read_wr));
	res_slave[i]->read_wr.sg_list = &res_slave[i]->read_sge;
	res_slave[i]->read_wr.num_sge = 1;
	res_slave[i]->read_wr.opcode = IBV_WR_RDMA_READ;
	res_slave[i]->read_wr.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	res_slave[i]->read_wr.wr.rdma.rkey = res_slave[i]->server_metadata_attr.stag.remote_stag;
	res_slave[i]->read_wr.wr.rdma.remote_addr =res_slave[i]->server_metadata_attr.address;



    res_slave[i]->client_dst_mr_log = rdma_buffer_register(res_slave[i]->pd,
			res_slave[i]->dst_log,
			SRC_DST_BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE | 
			 IBV_ACCESS_REMOTE_WRITE | 
			 IBV_ACCESS_REMOTE_READ));
	
	if (!res_slave[i]->client_dst_mr_log) {
		rdma_error("We failed to create the destination buffer, -ENOMEM\n");
		return -ENOMEM;
	}
	res_slave[i]->read_sge_log.addr = (uint64_t) res_slave[i]->client_dst_mr_log->addr;
	res_slave[i]->read_sge_log.length = (uint32_t) res_slave[i]->client_dst_mr_log->length;
	res_slave[i]->read_sge_log.lkey = res_slave[i]->client_dst_mr_log->lkey;
	/* now we link to the send work request */
	bzero(&res_slave[i]->read_wr_log, sizeof(res_slave[i]->read_wr_log));
	res_slave[i]->read_wr_log.sg_list = &res_slave[i]->read_sge_log;
	res_slave[i]->read_wr_log.num_sge = 1;
	res_slave[i]->read_wr_log.opcode = IBV_WR_RDMA_READ;
	res_slave[i]->read_wr_log.send_flags = IBV_SEND_SIGNALED;
	/* we have to tell server side info for RDMA */
	res_slave[i]->read_wr_log.wr.rdma.rkey = res_slave[i]->server_metadata_attr_log.stag.remote_stag;
	res_slave[i]->read_wr_log.wr.rdma.remote_addr =res_slave[i]->server_metadata_attr_log.address;



    // bzero(&res_slave[i]->read_wr_log, sizeof(res_slave[i]->read_wr_log));
	// res_slave[i]->read_wr_log.sg_list = &res_slave[i]->read_sge;
	// res_slave[i]->read_wr_log.num_sge = 1;
	// res_slave[i]->read_wr_log.opcode = IBV_WR_RDMA_READ;
	// res_slave[i]->read_wr_log.send_flags = IBV_SEND_SIGNALED;
	// /* we have to tell server side info for RDMA */
	// res_slave[i]->read_wr_log.wr.rdma.rkey = res_slave[i]->server_metadata_attr_log.stag.remote_stag;
	// res_slave[i]->read_wr_log.wr.rdma.remote_addr =res_slave[i]->server_metadata_attr_log.address;

	return 0;
}


/* This function disconnects the RDMA connection from the server and cleans up 
 * all the resources.
 */
int client_disconnect_and_clean(int i)
{  
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/* active disconnect from the client side */
	ret = rdma_disconnect(res_slave[i]->cm_client_id);
	if (ret) {
		rdma_error("Failed to disconnect, errno: %d \n", -errno);
		//continuing anyways
	}
	ret = process_rdma_cm_event(res_slave[i]->cm_event_channel, 
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
	rdma_destroy_qp(res_slave[i]->cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(res_slave[i]->cm_client_id);
	if (ret) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(res_slave[i]->client_cq);
	if (ret) {
		rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy completion channel */
	ret = ibv_destroy_comp_channel(res_slave[i]->io_completion_channel);
	if (ret) {
		rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy memory buffers */
	ibv_dereg_mr(res_slave[i]->send_mr);
	ibv_dereg_mr(res_slave[i]->recv_mr);
    ibv_dereg_mr(res_slave[i]->client_dst_mr);
    ibv_dereg_mr(res_slave[i]->client_src_mr);
    ibv_dereg_mr(res_slave[i]->server_metadata_mr);
	/* We free the buffers */
	free(res_slave[i]->send_region);
	free(res_slave[i]->recv_region);
    free(res_slave[i]->dst);
    free(res_slave[i]->src);
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(res_slave[i]->pd);
	if (ret) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		// we continue anyways;
	}
	rdma_destroy_event_channel(res_slave[i]->cm_event_channel);
	printf("Client resource clean up is complete \n");
    free(res_slave[i]);
    res_slave[i]=NULL;
	return 0;
}



int post_send_client(int i)
{ 
    long t = 10000000l;
    while(t>0){t--;}
    struct ibv_wc wc;
	int ret = -1;
	/* Now we post it */
	ret = ibv_post_send(res_slave[i]->client_qp, 
		       &res_slave[i]->send_wr,
	       &res_slave[i]->bad_send_wr);
	if (ret) {
		rdma_error("Failed to send client metadata, errno: %d \n", 
				-errno);
		return -errno;
	}
   
	/* at this point we are expecting 2 work completion. One for our 
	 * send and one for recv that we will get from the server for 
	 * its buffer information */
	ret = process_work_completion_events(res_slave[i]->io_completion_channel, 
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
/* Pre-posts a receive buffer before calling rdma_connect () */
int post_recv(int i)
{
	int ret = -1;
	memset(res_slave[i]->recv_region,0,SEND_RECV_BUFFER_SIZE);
	ret = ibv_post_recv(res_slave[i]->client_qp /* which QP */,
		      &res_slave[i]->recv_wr /* receive work request*/,
		      &res_slave[i]->bad_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
//	debug("Receive buffer pre-posting is successful \n");
	return 0;
}


int post_read(uint64_t offset,uint64_t record_len,int i){
	struct ibv_wc wc;
	int ret = -1;
	memset(res_slave[i]->dst,0,SRC_DST_BUFFER_SIZE);
	res_slave[i]->read_sge.length = (uint32_t) record_len;
	//printf(" read offset=%ld\n",offset);
	res_slave[i]->read_wr.wr.rdma.remote_addr = res_slave[i]->server_metadata_attr.address+offset;
	/* Now we post it */
	ret = ibv_post_send(res_slave[i]->client_qp, 
		       &res_slave[i]->read_wr,
	       &res_slave[i]->bad_read_wr);
	if (ret) {
		rdma_error("Failed to read client dst buffer from the master, errno: %d \n", 
				-errno);
		return -errno;
	}
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(res_slave[i]->io_completion_channel, 
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

/* This function does :
 * 1) Prepare memory buffers for RDMA operations 
 * 1) RDMA write from src -> remote buffer 
 * 2) RDMA read from remote bufer -> dst
 */ 
int post_write(uint64_t offset,uint64_t record_len,int i){
	struct ibv_wc wc;
	int ret = -1;
	res_slave[i]->write_sge.length = record_len;
	res_slave[i]->write_wr.wr.rdma.remote_addr = res_slave[i]->server_metadata_attr.address+offset;
	//printf("write offset:%ld\n",offset);
	/* Now we post it */
	ret = ibv_post_send(res_slave[i]->client_qp, 
		       &res_slave[i]->write_wr,
	       &res_slave[i]->bad_write_wr);
	if (ret) {
		rdma_error("Failed to write client src buffer, errno: %d \n", 
				-errno);
		return -errno;
	}
		
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(res_slave[i]->io_completion_channel, 
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

/* This function does :
 * 1) Prepare memory buffers for RDMA operations 
 * 1) RDMA write from src -> remote buffer 
 * 2) RDMA read from remote bufer -> dst
 */ 
int post_write_log(uint64_t offset,uint64_t record_len,int i){
	struct ibv_wc wc;
	int ret = -1;
	res_slave[i]->write_sge_log.length = record_len;
	res_slave[i]->write_wr_log.wr.rdma.remote_addr = res_slave[i]->server_metadata_attr_log.address+offset;
	//printf("write offset:%ld\n",offset);
	/* Now we post it */
	ret = ibv_post_send(res_slave[i]->client_qp, 
		       &res_slave[i]->write_wr_log,
	       &res_slave[i]->bad_write_wr_log);
	if (ret) {
		rdma_error("Failed to write client src buffer, errno: %d \n", 
				-errno);
		return -errno;
	}
		
	/* at this point we are expecting 1 work completion for the write */
	ret = process_work_completion_events(res_slave[i]->io_completion_channel, 
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

/* When we call this function cm_client_id must be set to a valid identifier.
 * This is where, we prepare client connection before we accept it. This 
 * mainly involve pre-posting a receive buffer to receive client side 
 * RDMA credentials
 */
int setup_client_resources(int i)
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
       struct rdma_conn_param conn_param;	if(!res[i]->cm_client_id || !res[i]->client_qp) {
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
    //  debug("Client QP created at %p\n", client_qp);
    return ret;
}


int send_server_metadata(char * filebuffer,uint32_t filesize,int i){
	int ret = -1;
	res[i]->server_buffer_mr = rdma_buffer_register(res[i]->pd /* which protection domain */, 
			filebuffer,	
			filesize /* what size to allocate */, 
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
    struct ibv_wc *wc;
	ret = process_work_completion_events(res[i]->io_completion_channel, &wc, 1);
	if (ret != 1) {
		rdma_error("Failed to send server metadata, ret = %d \n", ret);
		return ret;
	}
	// debug("Local buffer metadata has been sent to the client \n");
	return 0;
}
int register_send_recv(int i){
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
			SEND_RECV_BUFFER_SIZE,
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
int recv_data_and_post_next_recv(int i) 
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
    if(verbose){
	    printf("%s recved message:%s,len:%d\n",inet_ntoa(res[i]->remote_sockaddr.sin_addr),res[i]->recv_region,strlen(res[i]->recv_region));
    }
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


/* This is server side logic. Server passively waits for the client to call 
 * rdma_disconnect() and then it will clean up its resources */
int disconnect_and_cleanup_client(int i)
{
	// struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
       /* Now we wait for the client to send us disconnect event */
    //   debug("Waiting for cm event: RDMA_CM_EVENT_DISCONNECTED\n");
    //    ret = process_rdma_cm_event(cm_event_channel, 
	// 	       RDMA_CM_EVENT_DISCONNECTED, 
	// 	       &cm_event);
    //    if (ret) {
	//        rdma_error("Failed to get disconnect event, ret = %d \n", ret);
	//        return ret;
    //    }
	// /* We acknowledge the event */
	// ret = rdma_ack_cm_event(cm_event);
	// if (ret) {
	// 	rdma_error("Failed to acknowledge the cm event %d\n", -errno);
	// 	return -errno;
	// }
	// printf("A disconnect event is received from the client...\n");
    
	/* We free all the resources */
  
    /* Destroy QP */
    // if(res[i]->cm_client_id!=NULL){
      
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
            if(res[i]->send_region!=NULL){
            free(res[i]->send_region);}
             if(res[i]->recv_region!=NULL){
            free(res[i]->recv_region);}
            if(res[i]->recv_buffer!=NULL){
            free(res[i]->recv_buffer);  }
            if(res[i]->send_mr!=NULL){
                ibv_dereg_mr(res[i]->send_mr);
            } 
            if(res[i]->recv_mr!=NULL){
                ibv_dereg_mr(res[i]->recv_mr);
            } 
            if(res[i]->server_buffer_mr!=NULL){
                ibv_dereg_mr(res[i]->server_buffer_mr);
            } 
            if(res[i]->server_metadata_mr!=NULL){
                ibv_dereg_mr(res[i]->server_metadata_mr);
            } 
           
            /* Destroy protection domain */
            ret = ibv_dealloc_pd(res[i]->pd);
            if (ret) {
                rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);    
                // we continue anyways;
            }
            free(res[i]);
            res[i]=NULL;
    // }
   

   

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


int post_send_server(int i)
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
    if(verbose){
        printf("%s sended message:%s,len:%d\n",inet_ntoa(res[i]->remote_sockaddr.sin_addr), res[i]->send_region,strlen(res[i]->send_region));
    }	
	return 0;
}
void usage() 
{
	printf("Usage:\n");
    printf("./server: [-v <1 or 0>][-p listening port] \n");
    printf("(default -v is 0 representing not printf\n");
    printf("(default -p is 20086\n");
    exit(1);
}


void init_nvm_file( char * _name){
 
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
}




int init_slave_ip(){

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
	slave_ip[0] = inet_ntoa(*((struct in_addr*)
	host_entry->h_addr_list[0]));
	//printf("Hostname: %s\n", hostbuffer);
	slave_ip[0] [strlen(slave_ip[0])-2]++;
	printf("Slave Host IP: %s\n", slave_ip[0]);
	return 0;
}


void task(int i){
    int ret;
    
	
    
    /* Parse Command Line Arguments, not the most reliable code */

	
	send_server_metadata(start_buffer,FILE_SIZE_hou,i);
	register_send_recv(i);

	//printf("0x%p\n",start_buffer);
	memset(res[i]->recv_region,0,sizeof(res[i]->recv_region));
	ret = ibv_post_recv(res[i]->client_qp /* which QP */,
		      &res[i]->recv_wr /* receive work request*/,
		      &res[i]->bad_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		pthread_exit( (void *)ret);
	}
	//debug("Receive buffer pre-posting is successful \n");
	while(1){printf("000\n");
		ret = recv_data_and_post_next_recv(i);
		if (ret) {
			rdma_error("Failed to send server metadata to the client, ret = %d \n", ret);
			pthread_exit( (void *)ret);
		}printf("111\n");
        pthread_mutex_lock (&mutex);
        int flag = parse_command(i);
        pthread_mutex_unlock(&mutex);
		printf("222\n");
        if(flag){
            break;
        }printf("333\n");
        // fdatasync(pmem_fd);
		post_send_server(i);
       printf("444\n");
	}printf("555\n");
    pthread_exit( (void *)ret);
   
}

int on_my_event(struct rdma_cm_event *cm_event_1)
{     
    struct rdma_cm_event *cm_event = (struct rdma_cm_event *)malloc(sizeof(struct rdma_cm_event));
    
    memcpy(cm_event,cm_event_1,sizeof(struct rdma_cm_event));
    rdma_ack_cm_event(cm_event_1);
    // printf("A new %s type event is received \n", rdma_event_str(cm_event->event));

    int ret = 0;
    if (cm_event->event == RDMA_CM_EVENT_CONNECT_REQUEST){
        int i =get_next_free_res();
		if(i==-1){
			printf("client number is more than the max_client_num\n");
			return 0;
		}
        res[i]=(struct resourse_server *)malloc(sizeof(struct resourse_server));
        memset(res[i],0,sizeof(res[i]));
        res[i]->send_region = (char*)malloc(SEND_REGION_MAX_LEN);
        res[i]->recv_region = (char*)malloc(SEND_RECV_BUFFER_SIZE);
        res[i]->recv_buffer = (char*)malloc(SEND_RECV_BUFFER_SIZE);
        //
        memset(res[i]->send_region,0,sizeof(res[i]->send_region));
        memset(res[i]->recv_region,0,sizeof(res[i]->recv_region));
        res[i]->cm_client_id = cm_event->id;

        ret = setup_client_resources(i);
        if (ret) { 
            rdma_error("Failed to setup client resources, ret = %d \n", ret);		
        }
    }else if(cm_event->event == RDMA_CM_EVENT_ESTABLISHED){
        int i= 0;
        for(i = 0;i<MAX_client_num;i++){
            if(  res[i]!=NULL&& res[i]->cm_client_id == cm_event->id){               
            /* Just FYI: How to extract connection information */
                memcpy(&res[i]->remote_sockaddr /* where to save */, 
                        rdma_get_peer_addr(res[i]->cm_client_id) /* gives you remote sockaddr */, 
                        sizeof(struct sockaddr_in) /* max size */);
                printf("A new connection is accepted from %s:%d \n", 
                        inet_ntoa(res[i]->remote_sockaddr.sin_addr),res[i]->remote_sockaddr.sin_port);
                ret = pthread_create(&pids[i], &attr, task, i);  
                if (ret != 0) {
                    printf("create thread failed.\n");
                    exit(1);
                }
                break;
            }
        }
    }else if (cm_event->event == RDMA_CM_EVENT_DISCONNECTED){
        printf("A disconnect event is received from the client...\n");
        int i= 0;
        for(i = 0;i<MAX_client_num;i++){
            if( res[i]!=NULL&& res[i]->cm_client_id == cm_event->id){
           
            /* Just FYI: How to extract connection information */
                ret = disconnect_and_cleanup_client(i);
                if (ret) { 
                    rdma_error("Failed to clean up resources properly, ret = %d \n", ret);
                }   
                printf("Finish cleaning up resources !\n");
                break;
            }
        }
   
    }else{
      printf("A new %s type event is received \n", rdma_event_str(cm_event->event));
      printf("on_event: unknown event.\n");
      exit(1);
    }
   
  return ret;
}

int init_and_connect_to_slave(){
    
    init_slave_ip();
    
	int i,ret;
    
	for( i = 0;i<slave_num;i++){	
	    res_slave[i]=(struct resourse_client *)malloc(sizeof(struct resourse_client));
		memset(res_slave[i],0,sizeof(res_slave[i]));
		bzero(&res_slave[i]->server_sockaddr, sizeof (res_slave[i]->server_sockaddr));
		res_slave[i]->server_sockaddr.sin_family = AF_INET;
		res_slave[i]->server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		res_slave[i]->server_ip = slave_ip[i];
		ret = get_addr(res_slave[i]->server_ip, (struct sockaddr*) &res_slave[i]->server_sockaddr);
		if (ret) {
			rdma_error("Invalid IP \n");
			return ret;
		}
		if (!res_slave[i]->server_sockaddr.sin_port) {
			/* no port provided, use the default port */
			res_slave[i]->server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);
		}		
		/* buffers are NULL */

		res_slave[i]->send_region = (char*)malloc(SEND_REGION_MAX_LEN);
		res_slave[i]->recv_region = (char*)malloc(SEND_RECV_BUFFER_SIZE);
		res_slave[i]->src = (char*)malloc(SRC_DST_BUFFER_SIZE);
        res_slave[i]->dst = (char*)malloc(SRC_DST_BUFFER_SIZE);
        res_slave[i]->src_log = (char*)malloc(SRC_DST_BUFFER_SIZE); 
        res_slave[i]->dst_log = (char*)malloc(SRC_DST_BUFFER_SIZE); 
	
		ret = client_prepare_connection(&res_slave[i]->server_sockaddr,i);
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
int main(int argc, char **argv) 
{
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

    init_nvm_file("/home/wanru/nvm/db1");
    // init_nvm_file("../pmem6/db");
	init_hashmap(file_buffer);
    printf("server:file_buffer:%p\n",file_buffer);
    printf("server:start_buffer:%p\n",start_buffer);
 
    memset(res,NULL,sizeof(res));
    pthread_mutex_init(&mutex,NULL);
	
	int ret;

    ret = init_and_connect_to_slave();
    if (ret) {
        rdma_error("RDMA server failed to connect to slave, ret = %d \n", ret);
        return ret;
    }
	ret = start_rdma_server(DEFAULT_RDMA_PORT);
	if (ret) {
		rdma_error("RDMA server failed to start cleanly, ret = %d \n", ret);
		return ret;
	}

    pthread_attr_init(&attr);         //初始化线程属性结构
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);   //设置attr结构为分离
    struct rdma_cm_event *cm_event = NULL;

    while (1) {
        ret=process_rdma_cm_event1(cm_event_channel,&cm_event);
        ret = on_my_event(cm_event);

    }

	ret = cleanup_server();
	if (ret) { 
		rdma_error("Failed to clean up resources properly, ret = %d \n", ret);
		return ret;
	}
	return 0;

}
