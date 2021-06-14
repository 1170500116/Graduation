#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include<time.h>
#include <rdma/rdma_cma.h>
#include "rdma_common.h"
#include <sys/time.h>
#include <sys/select.h>
#include <math.h>



#define SEND_REGION_MAX_LEN 50
resourse_client ** res;

/*****************/

int verbose=0;
int itimes =0;
int ioshu = 0;
char *IPbuffer;
int ip_offset=0;
pid_t pid =0;
int period =3000;//ms
int usleep_max =10000;
int value_len____ = 1024 ;
int value_lens[100];
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

/***************************************

client functions

***************************************/

int client_prepare_connection(struct sockaddr_in *s_addr,int i)
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

int recv_server_metadata(int i){
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
int register_all(int i){
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
	res[i]->recv_sge.addr = (uint64_t)res[i]->recv_mr->addr;
	res[i]->recv_sge.length = (uint32_t) res[i]->recv_mr->length;
	res[i]->recv_sge.lkey = (uint32_t) res[i]->recv_mr->lkey;
	/* now we link it to the request */
	bzero(&res[i]->recv_wr, sizeof(res[i]->recv_wr));
	res[i]->recv_wr.sg_list = &res[i]->recv_sge;
	res[i]->recv_wr.num_sge = 1;

	res[i]->client_src_mr = rdma_buffer_register(res[i]->pd,
			res[i]->src,
			SRC_DST_BUFFER_SIZE,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if (!res[i]->client_src_mr) {
		rdma_error("We failed to create the source buffer, -ENOMEM\n");
		return -ENOMEM;
	}
	res[i]->client_dst_mr = rdma_buffer_register(res[i]->pd,
			res[i]->dst,
			value_len____+50,
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


/* This function disconnects the RDMA connection from the server and cleans up 
 * all the resources.
 */
int client_disconnect_and_clean(int i)
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
    // if(res[i]->server_metadata_mr_log!=NULL){
    //     ibv_dereg_mr(res[i]->server_metadata_mr_log);
    // }
   
	/* We free the buffers */
	free(res[i]->send_region);
	free(res[i]->recv_region);
    free(res[i]->dst);
    free(res[i]->src);
    // free(&res[i]->server_metadata_attr);
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



int post_send_client(int i)
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
/* Pre-posts a receive buffer before calling rdma_connect () */
int post_recv(int i)
{
	int ret = -1;
	memset(res[i]->recv_region,0,SEND_RECV_BUFFER_SIZE);
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


int post_read(uint64_t offset,uint64_t record_len,int i){
	struct ibv_wc wc;
	int ret = -1;
	memset(res[i]->dst,0,value_len____+50);
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

/* This function does :
 * 1) Prepare memory buffers for RDMA operations 
 * 1) RDMA write from src -> remote buffer 
 * 2) RDMA read from remote bufer -> dst
 */ 
int post_write(uint64_t offset,uint64_t record_len,int i){
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


void usage() {
	printf("Usage:\n");
	printf("./client [-v <verbose>]\n");
    printf("(default -v is 0 representing not printf\n");
	// printf("(default IP is 127.0.0.1 and port is %d)\n", DEFAULT_RDMA_PORT);
	exit(1);
}
int on_connection(int i){
	int ret = -1;
	struct ibv_wc wc;
    post_recv(i);
    if(verbose){ printf("sended message:%s,len:%d\n",res[i]->send_region,strlen(res[i]->send_region));}
    post_send_client(i);
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
    if(verbose){ printf("recved message:%s,len:%d\n",res[i]->recv_region,strlen(res[i]->recv_region));}

    return ret;
}


int get(size_t *value_len,char ** value,int i,char* shang_del_str,char*del_lock_str){   
   if(verbose){  printf("get start-------------\n");}

    memcpy(res[i]->send_region, shang_del_str,32);
	on_connection(i);
	int num = res[i]->recv_region[0]-'0';
    while(num!=1){
        if(num==0){
            //printf("ere!\n");
            *value=NULL;
            *value_len =0;
            return 0;
        }
        usleep(rand()%usleep_max);//sleep < 10000us ,0.01s
        on_connection(i);
        num = res[i]->recv_region[0]-'0';
    }
    
	
	
	size_t record_len = strtoul (res[i]->recv_region+9,NULL, 16);
    res[i]->recv_region[9]='\0';
    size_t offset = strtoul (res[i]->recv_region+1,NULL, 16);
    // printf("duq:::::\n");
    // printf("record_len=%d\n",record_len);
	post_read(offset,record_len,i);
    char  linshi_buffer[9];
	linshi_buffer[8]='\0';
	memcpy(linshi_buffer,res[i]->dst,8);  
	*value_len = strtoul(linshi_buffer, NULL, 16);  

	*value = res[i]->dst+RECORD_FIX_LEN;

    memcpy(res[i]->send_region, del_lock_str,32);
	on_connection(i);
    num = res[i]->recv_region[0]-'0';
    if(num==0){
        printf("error del_lock :key does not exist\n");
    }else if(num==2){
        printf("error del_lock :lock is now occupied\n");
    } 

    while(num==2){    
        usleep(rand()%usleep_max);//sleep < 10000us ,0.01s
        on_connection(i);
        num = res[i]->recv_region[0]-'0';
    }
  
  
	return 1;
}
int put(int record_len,int i,char* shang_lock_str,char * record,char * del_lock_str){

    memcpy(res[i]->send_region, shang_lock_str,41); 
	on_connection(i);
   
	int num = res[i]->recv_region[0]-'0';
    if(num==9){
        printf("recv hash error!!");
        return 9;
    }
    while(num!=1){
        if(num==0){
            return 0;
        }
        usleep(rand()%usleep_max);//sleep < 10000us ,0.01s           
        on_connection(i);     
        num = res[i]->recv_region[0]-'0';
    }
  
	uint64_t offset =  strtoul (res[i]->recv_region+1, NULL, 16);
    ibv_dereg_mr(res[i]->client_src_mr);
    res[i]->client_src_mr = rdma_buffer_register(res[i]->pd,
			record,
			record_len+1,
			(IBV_ACCESS_LOCAL_WRITE|
			 IBV_ACCESS_REMOTE_READ|
			 IBV_ACCESS_REMOTE_WRITE));
	if (!res[i]->client_src_mr) {
		rdma_error("We failed to create the source buffer, -ENOMEM\n");
		return -ENOMEM;
	}

	/* Step 1: is to copy the local buffer into the remote buffer. We will 
	 * reuse the previous variables. */
	/* now we fill up SGE */
	res[i]->write_sge.addr = (uint64_t) res[i]->client_src_mr->addr;
    res[i]->write_sge.length = (uint32_t) res[i]->client_src_mr->length;	
    res[i]->write_sge.lkey = res[i]->client_src_mr->lkey;	
    // memcpy(res[i]->src, record,record_len+1);
	post_write(offset,record_len,i);   
    memcpy(res[i]->send_region, del_lock_str,33);
    on_connection(i);    
    num = res[i]->recv_region[0]-'0';
    if(num==0){
        printf("error del_lock :key does not exist\n");
    }else if(num==2){
        printf("error del_lock :lock is now occupied\n");
    }
    while(num==2){       
        usleep(rand()%usleep_max);//sleep < 10000us ,0.01s
        on_connection(i);
        num = res[i]->recv_region[0]-'0';
    }
    if(verbose){   printf("put finish\n");}
	return 1;
}
int del(int i,char * del_str){ 
    // if(verbose){ printf("del start!--------------\n");}
    memcpy(res[i]->send_region, del_str,18);
	on_connection(i);
	int num = res[i]->recv_region[0]-'0';
    while(num==2){      
        usleep(rand()%usleep_max);//sleep < 10000us ,0.01s    
        on_connection(i);
        num = res[i]->recv_region[0]-'0';
    }
    if(verbose){ printf("del finish!\n");}
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
    res  = (struct resourse_client **)malloc(sizeof(struct resourse_client*)*server_num);
	for( i = 0;i<server_num;i++){	
	    res[i]=(struct resourse_client *)malloc(sizeof(struct resourse_client));
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
			res[i]->server_sockaddr.sin_port = htons(server_port[i]);
		}		
		/* buffers are NULL */
			
		res[i]->send_region = (char*)malloc(SEND_REGION_MAX_LEN);
		res[i]->recv_region = (char*)malloc(SEND_RECV_BUFFER_SIZE);
		res[i]->src = (char*)malloc(SRC_DST_BUFFER_SIZE);
		res[i]->dst =  (char*)malloc(value_len____+50);
	
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


int test_put(int put_ioshu,size_t value_len,char ** keys,char ** values){
    int i,t,put_ret;
    //typedef clock_t long 4 bytes
	clock_t start, finish;  
    double  duration=0.0;
   

    char *linshi_lock[put_ioshu];
    char *shang_lock_str[put_ioshu];
     char * record[put_ioshu];
    char * del_lock_str[put_ioshu];
    size_t record_len = value_len+RECORD_FIX_LEN+1;
    for(  t = 0;t<put_ioshu;t++){ 
        char * key = keys[t];
        char * value = values[t];
        linshi_lock[t] = ( char *)malloc(LOCK_LEN+1);
        snprintf(linshi_lock[t],LOCK_LEN+1,"%08x%2s%04x\0",ioshu,IPbuffer+ip_offset,pid);
        // printf(" linshi_lock[t] :%s\n", linshi_lock[t] );
        shang_lock_str[t] = ( char *)malloc(41);       
        snprintf(shang_lock_str[t], 41, "1%-16s%08x %14s",key,record_len,linshi_lock[t]);	   
        record[t] =  ( char *)malloc(record_len+1);       
        snprintf(record[t],record_len+1,"%08lx%-16s%s",strlen(value),key,value);
        del_lock_str[t] = ( char *) malloc(33);   
        snprintf(del_lock_str[t], 33, "3%-16s%14s1\0",key,linshi_lock[t]);           
        ioshu++;
    }
    start = clock();  
   
    for(  t = 0;t<put_ioshu;t++){ 
    if(verbose){  printf("put %s,%s,%d\n" ,keys[t],values[t],strlen(values[t]));}
        i = select_server(keys[t]); 

        put_ret = put( record_len,i,shang_lock_str[t],record[t],del_lock_str[t]);
        if(put_ret==9){
            return put_ret;
        }
	}     
    finish = clock();  
    duration += (double)(finish - start); 
 
  
    for(  t = 0;t<put_ioshu;t++){        
        free(linshi_lock[t]);
        free(shang_lock_str[t]);
        free(del_lock_str[t]);
        free(record[t]);
    }
  
    
  	// printf( "put_ioshu : %d t~~~~~~~~~~~~~~~~~~~\n", put_ioshu); 
  	// printf( "time : %f/%d seconds~~~~~~~~~~~~~~~~~~~\n", duration ,CLOCKS_PER_SEC); 
	// printf( "iops :%f t/s~~~~~~~~~~~~~~~~~~~\n", put_ioshu/(duration/CLOCKS_PER_SEC)); 
    printf( "put latency :%f us~~~~~~~~~~~~~~~~~~~\n", duration/put_ioshu); 
       	
    return put_ret;
}
void re_register(int i){
	free(res[i]->dst);
	res[i]->dst =  (char*)malloc(value_len____+50);

	ibv_dereg_mr(res[i]->client_dst_mr);
	res[i]->client_dst_mr = rdma_buffer_register(res[i]->pd,
			res[i]->dst,
			value_len____+50,
			(IBV_ACCESS_LOCAL_WRITE | 
			 IBV_ACCESS_REMOTE_WRITE | 
			 IBV_ACCESS_REMOTE_READ));
	
	if (!res[i]->client_dst_mr) {
		rdma_error("We failed to create the destination buffer, -ENOMEM\n");
		return -ENOMEM;
	}
 	res[i]->read_sge.addr = (uint64_t) res[i]->client_dst_mr->addr;
	res[i]->read_sge.length = (uint32_t) res[i]->client_dst_mr->length;
	res[i]->read_sge.lkey = res[i]->client_dst_mr->lkey;	
	return 0;
}
double test_get(int get_ioshu,char ** keys,char** values){
    int i,t,get_ret;
    //typedef clock_t long 4 bytes
	clock_t start, finish;  
    double  duration=0.0;
   
  

    char * ans_value = NULL;
	size_t ans_value_len;

 
     char *linshi_lock[get_ioshu];
    char *shang_lock_str[get_ioshu];
    char * del_lock_str[get_ioshu];
    for(  t = 0;t<get_ioshu;t++){ 
        char * key = keys[t];
     
        linshi_lock[t] = ( char *)malloc(LOCK_LEN+1);
        snprintf(linshi_lock[t],LOCK_LEN+1,"%08x%2s%04x",ioshu,IPbuffer+ip_offset,pid);
        shang_lock_str[t] = ( char *)malloc(32); 
        snprintf(shang_lock_str[t], 32, "0%-16s%14s",key,linshi_lock[t]);         
        del_lock_str[t] = ( char *) malloc(32);   
        snprintf(  del_lock_str[t], 32, "3%-16s%14s",key,linshi_lock[t]);        
        ioshu++;
    }
    start = clock();    
    int ans_error = 0;
    for(  t = 0;t<get_ioshu;t++){ 
      
        i = select_server(keys[t]); 
        get_ret = get(&ans_value_len,&ans_value,i,shang_lock_str[t],del_lock_str[t]);
        if(verbose){
            if(ans_value_len!=strlen(values[t])||strncmp(ans_value,values[t],ans_value_len)){
                printf("ans_value_len:%d,zhenshi:%d\n",ans_value_len,strlen(values[t]));
                printf("%s\n%s\n",ans_value,values[t]);
                ans_error++;
            }
            if(verbose){  printf("get %s,%s,%d\n" ,keys[t],ans_value,ans_value_len);}
        }	
       
        // printf("%s,%d\n",ans_value,strlen(ans_value));
	} 
    finish = clock();  
    duration += (double)(finish - start); 
  

    for(  t = 0;t<get_ioshu;t++){ 
        free(linshi_lock[t]);
        free(shang_lock_str[t]);
        free(del_lock_str[t]);       
    }
  

     


    // printf( "get_ioshu : %d t~~~~~~~~~~~~~~~~~~~\n", get_ioshu); 
  	// printf( "time : %f/%d seconds~~~~~~~~~~~~~~~~~~~\n", duration ,CLOCKS_PER_SEC); 
	// printf( "iops :%f t/s~~~~~~~~~~~~~~~~~~~\n", get_ioshu/(duration/CLOCKS_PER_SEC)); 
    printf( "get latency :%f us~~~~~~~~~~~~~~~~~~~\n", duration/get_ioshu); 
    // printf( "ans_error :%d ~~~~~~~~~~~~~~~~~~~\n", ans_error); 
    return duration;
}

double test_del_get(int get_ioshu,char ** keys,char** values){
    int i,t,get_ret;
    //typedef clock_t long 4 bytes
	clock_t start, finish;  
    double  duration=0.0;
   
  

    char * ans_value = NULL;
	size_t ans_value_len;

 
     char *linshi_lock[get_ioshu];
    char *shang_lock_str[get_ioshu];
    char * del_lock_str[get_ioshu];
    for(  t = 0;t<get_ioshu;t++){ 
        char * key = keys[t];
     
        linshi_lock[t] = ( char *)malloc(LOCK_LEN+1);
        snprintf(linshi_lock[t],LOCK_LEN+1,"%08x%2s%04x",ioshu,IPbuffer+ip_offset,pid);
        shang_lock_str[t] = ( char *)malloc(32); 
        snprintf(shang_lock_str[t], 32, "0%-16s%14s",key,linshi_lock[t]);         
        del_lock_str[t] = ( char *) malloc(32);   
        snprintf(  del_lock_str[t], 32, "3%-16s%14s",key,linshi_lock[t]);        
        ioshu++;
    }
    start = clock();    
    int ans_error = 0;
    for(  t = 0;t<get_ioshu;t++){ 
      
        i = select_server(keys[t]); 
        get_ret = get(&ans_value_len,&ans_value,i,shang_lock_str[t],del_lock_str[t]);
        if(verbose){
            if(ans_value_len!=0){
                printf("ans_value_len:%d,zhenshi:%d\n",ans_value_len,strlen(values[t]));
                printf("%s\n%s\n",ans_value,values[t]);
                ans_error++;
            }
            if(verbose){  printf("get %s,%s,%d\n" ,keys[t],ans_value,ans_value_len);}
        }	
       
        // printf("%s,%d\n",ans_value,strlen(ans_value));
	} 
    finish = clock();  
    duration += (double)(finish - start); 
  

    for(  t = 0;t<get_ioshu;t++){ 
        free(linshi_lock[t]);
        free(shang_lock_str[t]);
        free(del_lock_str[t]);       
    }

  
    // printf( "check after del, ans_error :%d ~~~~~~~~~~~~~~~~~~~\n", ans_error); 
    return duration;
}


double test_del(int del_ioshu,char ** keys){
      int i,t,del_ret;
    //typedef clock_t long 4 bytes
	clock_t start, finish;  
    double  duration=0.0;
   
  
 
    char * del_str[del_ioshu];
    for( t = 0;t<del_ioshu;t++){ 
        char * key = keys[t];
        del_str[t] = ( char *) malloc(18);   
        snprintf(del_str[t], 18, "2%-16s",key);
        ioshu++;
    }
   
    start = clock();    
    for(  t = 0;t<del_ioshu;t++){  
        if(verbose){printf("del %s\n",keys[t]);  }
        i = select_server(keys[t]); 
        del_ret = del(i,del_str[t]);  

	} 
    finish = clock();  
   


  
   duration += (double)(finish - start); 
    
    for(  t = 0;t<del_ioshu;t++){ 
        free(del_str[t]);       
    }

    
  	// printf( "del_ioshu : %d t~~~~~~~~~~~~~~~~~~~\n", del_ioshu); 
  	// printf( "time : %f/%d seconds~~~~~~~~~~~~~~~~~~~\n", duration ,CLOCKS_PER_SEC); 
	// printf( "iops :%f t/s~~~~~~~~~~~~~~~~~~~\n", del_ioshu/(duration/CLOCKS_PER_SEC)); 
    printf( "del latency :%f us~~~~~~~~~~~~~~~~~~~\n", duration/del_ioshu); 
    	
    return duration;
}

int init_ip( char * _name){
    FILE *fp;
    size_t len = 0;
    char *str = NULL;
    ssize_t read;
    if((fp = fopen(_name,"r")) == NULL) //判断文件是否存在及可读
    {
        printf("error,%s do not exist!",_name);
        exit(1);
    }
    int i = 0;

    while((read = getline(&str,&len,fp)) != -1)
    {
        if(read>5){
            server_ip[i]=(char*)malloc(10);
            snprintf(server_ip[i],10,"%s",str);

            server_port[i] =strtoul (str+10, NULL, 10); 
            printf("%s,%d\n",server_ip[i],server_port[i]); 
            i++;
        }
    }
    fclose(fp);                     //关闭文件    
	memset(value_lens,0,sizeof(value_lens));
    return i;
}
int main(int argc, char **argv) {	
    int ret,i;
    server_num = init_ip("./server_ip.txt");	
    int zong = 10000;
	int value_len_min = 0;
	int value_len_max = 0;
    int option;
	
    while ((option = getopt(argc, argv, "v:i:l:a:b:")) != -1) {
        switch (option) {			
            case 'v':
                verbose = strtol(optarg, NULL, 0);
                break;
            case 'i':
                zong = strtol(optarg, NULL, 0);
                break;
            case 'l':
                value_len____ = strtol(optarg, NULL, 0);
                break;
			case 'a':
				value_len_min = strtol(optarg, NULL, 0);
				break;
			case 'b':
				value_len_max = strtol(optarg, NULL, 0);
				break;
            default:
                usage();			
                break;
            }
	}
	printf("server_num = %d\n",server_num);

	get_client_addr();
	get_client_pid();
	ret = init();
    if(ret){
            exit(1);
    }
	// sleep(1);//sleep 1s

 

	if(value_len_min!=0&&value_len_max!=0&&value_len_min<=value_len_max){
		i = 0;
		while(value_len_min<=value_len_max){
			value_lens[i]=value_len_min;
			value_len_min*=2;
			i++;			
		}
	}else if(value_len_min==0&&value_len_max==0){
		value_lens[0]=value_len____;
	}else{
		 usage();	
	}

   
   //generate data;
	srand((unsigned) time(NULL ));
   int k = 0;
   while(value_lens[k]!=0){

		value_len____ = value_lens[k];
	
   		printf("value_len:%d\n",value_len____);
		for( i = 0;i<server_num;i++){re_register(i);}
		char *keys[zong];
		char *values[zong];
		char *_key = (char*)malloc(KEY_LEN+1);
		char *_value = (char*)malloc(value_len____+1);
		int keylen;
		for(i =0;i<zong;i++){
			keys[i]=(char*)malloc(KEY_LEN+1);
			values[i] =(char*)malloc(value_len____+1);


			
			// keylen = 2;
			// _key[0]='a';
			// _key[1]='\0';

	 
			keylen = rand()%KEY_LEN+2;	 //2~KEY_LEN+1
			
			genRandomString(_key ,keylen) ;
			
			genRandomString(_value ,value_len____+1);  

			snprintf(keys[i],KEY_LEN+1,"%-16s",_key); 
	
			snprintf(values[i],value_len____+1,"%s",_value); 
			// printf("%s,%s\n",keys[i],values[i]);
		}
	

		// printf("PUT %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n");

		int put_ret = test_put(zong,value_len____,keys,values);
		// printf("GET %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
		test_get(zong,keys,values);
		// printf("DEL %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n");
		test_del(zong,keys);
		test_del_get(zong,keys,values);

		free(_key);
		free(_value);
		k++;
    }






	for( i = 0;i<server_num;i++){
        snprintf(res[i]->send_region, SEND_REGION_MAX_LEN, "4");
        post_send_client(i);
    }
	for( i = 0;i<server_num;i++){
		ret = client_disconnect_and_clean(i);
		if (ret) {
			rdma_error("Failed to cleanly disconnect and clean up resources \n");
		}
	}
	return ret;
}

