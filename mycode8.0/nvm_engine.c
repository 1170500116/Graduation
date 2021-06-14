#include "nvm_engine.h"
#include <sys/resource.h>
#include <sys/types.h>
#include <unistd.h>
#include<string.h>
#include <stdio.h>
#include <stdint.h>
HASH_INDEX_TYPE glo_loc = 0;
uint8_t * arr; 
//BLOCK_INDEX_TYPE max_block_index_;
void Push(BLOCK_INDEX_TYPE _block_index, int _size)  {
    uint8_t * p = arr+_block_index;
    int i;
    for ( i = 0;i<_size;i++){
      *p = 0;
      p++;
    }      
}
int Pop(BLOCK_INDEX_TYPE* _block_index, int _size) {
    //printf("blocknum:%d\n",_size);
    uint8_t * p = arr;
    *_block_index =0;
    uint8_t *end = arr+max_block_index_;
    int now_size = 0;
    while(p!=end &&now_size<_size){
      if(*p==0){
        now_size++;        
      }else{
        now_size=0;
       *_block_index = p-arr+1;
      }
      p++;      
    }
    //printf("now_size = %d\n",now_size);
    if(now_size==_size){
       p = arr+(*_block_index);  
       memset(p,1,_size);       

       return 1;
   }
   return 0;    
}
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
  // printf("len:%d\n",len);
  for ( i = 0; i < len; ++_str, ++i) {
    hash = ((hash << 5) + hash) + (*_str);
  }
  return hash%ZONG_HASH_MAP_SIZE;
}


void init_freelist(){
  //max_block_index_=  FILE_SIZE /  BLOCK_SIZE;
    uint8_t * p;
    arr = malloc(max_block_index_);
    memset(arr,0,max_block_index_);  

    if(file_exist==1){
        int i;
        for(i=0;i<KV_NUM_MAX;i++){
            if(hash_flag[i]!=0){
                BLOCK_INDEX_TYPE loc = hashmap[i].loc;
                int block_num =(hashmap[i].record_len+ BLOCK_SIZE - 1) / BLOCK_SIZE;
                p = arr+loc;  
                memset(p,1,block_num);   
            }
        }
    }
    // int i;
    // for(i=0;i<max_block_index_;i++){
    //     printf("%d ",arr[i]);
    //     if(i>1000){break;}
    // }printf("\n");
}
int  New( BLOCK_INDEX_TYPE* _index,int _size) {
      return Pop(_index, _size);
}

int Delete( BLOCK_INDEX_TYPE _index,int _size) {
    Push(_index, _size);
    return 1;
}  
  

void init_hashmap(char *file_buffer){
    entry = (int*)file_buffer;
    hash_flag = (int*)file_buffer+HASH_MAP_SIZE;
    hashmap = (Node*)hash_flag+KV_NUM_MAX;
    
    if(file_exist==0){
         int i;
        for(i=0;i<HASH_MAP_SIZE;i++){
            entry[i]= -1;
        }
        memset(hash_flag,0,KV_NUM_MAX*sizeof(int));
    }
    // int i;
    // printf("___________________________\n");
    // for(i=0;i<HASH_MAP_SIZE;i++){
    //     printf("%d ",entry[i]);    if(i>1000){break;}
    // }printf("\n");
    //   printf("___________________________\n");
    // for(i=0;i<KV_NUM_MAX;i++){
    //     printf("%d ",hash_flag[i]);    if(i>1000){break;}
    // }printf("\n");
    //   printf("___________________________\n");
    init_freelist();
}
uint32_t cal_start_offset(){
    return (KV_NUM_MAX+HASH_MAP_SIZE)*sizeof(int)+KV_NUM_MAX*sizeof(Node);
}


BLOCK_INDEX_TYPE GetBlockIndex(VALUE_LEN_TYPE record_len) {
  int block_num =(record_len+ BLOCK_SIZE - 1) / BLOCK_SIZE;
      //(RECORD_FIX_LEN + data_len+5 + BLOCK_SIZE - 1) / BLOCK_SIZE;
  BLOCK_INDEX_TYPE block_index = UINT32_MAX;
  if (!New( &block_index,block_num)) {
    block_index = UINT32_MAX;
    printf( "Out of memory, when allocate an aep space.\n");
    abort();
  }
  return block_index;
}

int hashmap_push(HASH_VALUE hash_val,char * key,BLOCK_INDEX_TYPE loc,VALUE_LEN_TYPE record_len,char* _lock,time_t endtime,int last_node_index){
    HASH_INDEX_TYPE next_loc = get_next_free_hashnode();
    if(next_loc ==-1){
      printf("hashmap out of memory!!!!\n");
      return 0;
    }  
    HASH_INDEX_TYPE index = entry[hash_val];
    entry[hash_val] = next_loc;
    
    memcpy(hashmap[next_loc].key,key,KEY_LEN);
    hashmap[next_loc].key[KEY_LEN]='\0';
    // printf("mykey:%s\n", hashmap[next_loc].key);
    hashmap[next_loc].loc = loc;
    hashmap[next_loc].record_len = record_len;
    if(_lock!=NULL&&strlen(_lock)!=0){
        memcpy(hashmap[next_loc].lock,_lock,LOCK_LEN);
        hashmap[next_loc].lock[LOCK_LEN]='\0';
    }
    hashmap[next_loc].next=index;
    hashmap[next_loc].last_node_index=last_node_index;
    hashmap[next_loc].endtime=endtime;
    hash_flag[next_loc] = 1;

    return 1;
}
//0,key doesnot exists;
//1,add lock success;
//2,lock is now occupied;
int nvm_Get(char * _key, HASH_VALUE hash_val,BLOCK_INDEX_TYPE * ans_block_index,VALUE_LEN_TYPE* record_len,char* _lock,time_t endtime) {
    int ret = 0;
    BLOCK_INDEX_TYPE block_index = -1 ;
    int index = entry[hash_val];
   
    while(index!=-1){  
       
        if(hash_flag[index]==1&&strncmp(_key,hashmap[index].key,KEY_LEN)==0){
            if(hashmap[index].lock==NULL||strlen(hashmap[index].lock)==0){//lock is does not exist              
                block_index = hashmap[index].loc;               
                memcpy(hashmap[index].lock,_lock,LOCK_LEN);            
                hashmap[index].endtime = endtime;
                ret = 1;
            }else{      
                ret = 2;//lock is now occupied
            } 
            break;
        }
        index =  hashmap[index].next;
    }    
  
 
    if(block_index==-1){
        return ret;//0,key doesnot exists;2,lock is now occupied;
    }  
    *ans_block_index = block_index;
    *record_len = hashmap[index].record_len;
 
    return ret;
}
//0,hashmap out of memory;
//1,add lock success;
//2,lock is now occupied;
int nvm_Put(char * _key,HASH_VALUE hash_val, VALUE_LEN_TYPE record_len,BLOCK_INDEX_TYPE * ans_block_index,char * _lock,time_t endtime,int myflag){   
    int last_node_index=-1;
    int ret = nvm_Del(_key,hash_val,&last_node_index,myflag); 
    
    if (ret==2){//lock is occupied
        return ret;
    }   
    // ret==0,key does not exist 
    // ret==1,key exist ,lock is does not exist 
    BLOCK_INDEX_TYPE loc = GetBlockIndex(record_len);
    *ans_block_index =loc;
    // Update key buffer in memory
   
    return hashmap_push(hash_val,_key,loc,record_len,_lock,endtime,last_node_index);
}

//0,key does not exist;
//1,key exist and lock does not exist ,deleted the key and value;
//2,lock is now occupied;
int nvm_Del(char * _key,HASH_VALUE hash_val,int *last_node_index,int myflag){
    int ret=0;
    BLOCK_INDEX_TYPE block_index = -1 ;
    int index = entry[hash_val];
    int last_index = -1;
    // printf("111\n");
    while(index!=-1){
        //    printf("22\n");
        if(strlen(_key)>0&&strlen(hashmap[index].key)>0&&hash_flag[index]==1&&strncmp(_key,hashmap[index].key,KEY_LEN)==0){  
            //   printf("221\n");
            if(hashmap[index].lock==NULL||strlen(hashmap[index].lock)==0){//lock is does not exist 
                //   printf("2222\n");
                block_index = hashmap[index].loc;
                ret = 1;                
            }else{      
                // printf("2223\n");
                ret = 2;//lock is now occupied
            }
            break;     
        }      
        //    printf("33\n");
        last_index = index;
            //   printf("44\n");
        index =  hashmap[index].next;
    }     
    //   printf("22221\n");
    if(block_index!=-1){
        if(myflag==2){//put use the del ,we dont really del ,just make the falag = 2
        //   printf("33333\n");
            hash_flag[index]=2;
            //    printf("33111111\n");
            *last_node_index = index; 
        }else{
            //    printf("44444\n");
            int block_num =  (hashmap[index].record_len+ BLOCK_SIZE - 1) / BLOCK_SIZE;
            Delete(block_index,block_num);     
            hash_flag[index]=0;  
                    //    printf("444441111\n");        
            if(last_index==-1){          
                entry[hash_val] = hashmap[index].next;
            }else{               
                hashmap[last_index].next= hashmap[index].next;
            }            
        }
     
      return ret;//lock does not exist ,deleted the key and value,1
    }    
    return ret;//key does not exist,0;lock is now occupied,2;
}

//0,key does not exist;
//1,key exist and lock will be deleted;
//2,lock is now occupied;
int nvm_Del_lock(char * _key,HASH_VALUE hash_val,char* _lock,int is_put,VALUE_LEN_TYPE * record_len,BLOCK_INDEX_TYPE * ans_block_index){
     int ret=0;
     to_slave_loc=0;
    int index = entry[hash_val];
    int last_index = -1;
    while(index!=-1){
        //    printf("key:%s,%s\n",_key,hashmap[index].key);
       
        if(hash_flag[index]==1&&strncmp(_key,hashmap[index].key,KEY_LEN)==0){  
            if(hashmap[index].lock==NULL||strlen(hashmap[index].lock)==0){//lock is does not exist
                return 0;
            }  
            if(_lock!=NULL&&strncmp(_lock,hashmap[index].lock,LOCK_LEN)==0){
                int last_node_index = hashmap[index].last_node_index;  
                if(last_node_index!=-1&&hash_flag[last_node_index]==2){
                    int block_num =  (hashmap[last_node_index].record_len+ BLOCK_SIZE - 1) / BLOCK_SIZE;
                    Delete(hashmap[last_node_index].loc,block_num);  
                    hash_flag[last_node_index]=0;
                    int abc_index = entry[hash_val];
                    int abc_last_index = -1;
                    while(abc_index!=-1){
                        if(abc_index==last_node_index){
                            break;
                        }
                        abc_last_index = abc_index;
                        abc_index =  hashmap[abc_index].next;
                    }
                    if(abc_last_index==-1){         
                        entry[hash_val] = hashmap[last_node_index].next;
                    }else{
                        hashmap[abc_last_index].next= hashmap[last_node_index].next;
                    }
                }
                memset(hashmap[index].lock,'\0',LOCK_LEN);
                if(is_put){
                    *ans_block_index = hashmap[index].loc;
                    *record_len = hashmap[index].record_len;
                 
                }              
                return 1;//lock will be deleted             
            }else{ 
                // printf("lock:%s,%s\n",_lock,hashmap[index].lock);
                return 2;//lock is now occupied
            }  
            break;
        }
        last_index = index;
        index =  hashmap[index].next;
    }
    return 0;  //key does not exist  
}
HASH_INDEX_TYPE get_next_free_hashnode(){
  int flag = 0;
  while(hash_flag[glo_loc]!=0){
    glo_loc++;
    if(glo_loc==KV_NUM_MAX){   
      flag++;   
      glo_loc = 0; 
      if(flag==2){
        return -1;
      }    
    }
  }
  return glo_loc;
}
HASH_VALUE check_hashval(char *key,int verbose){
    HASH_VALUE hash_val =  DJBHash(key); 
    if(verbose){
        printf("hash_val=%d\n",hash_val);
        printf("HASH_MIN=%d\n",HASH_MIN);
        printf("HASH_MAX=%d\n",HASH_MAX);
    }
    
    if(!(hash_val>=HASH_MIN&&hash_val<HASH_MAX)){
        hash_val=HASH_MAX;
    }    
    hash_val-=HASH_MIN;
    return hash_val;
}







