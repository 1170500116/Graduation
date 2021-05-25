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
  arr = malloc(max_block_index_);
  memset(arr,0,max_block_index_); 

}
int  New( BLOCK_INDEX_TYPE* _index,int _size) {
      return Pop(_index, _size);
}

int Delete( BLOCK_INDEX_TYPE _index,int _size) {
    Push(_index, _size);
    return 1;
}  
  



void init_hashmap(){
  memset(hash_flag,0,sizeof(hash_flag));
  memset(entry,-1,sizeof(entry));
  init_freelist();
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
int hashmap_push(HASH_VALUE hash_val,char * key,BLOCK_INDEX_TYPE loc,VALUE_LEN_TYPE record_len,char* _lock){
    HASH_INDEX_TYPE next_loc = get_next_free_hashnode();
    if(next_loc ==-1){
      printf("hashmap out of memory!!!!\n");
      return 0;
    }  
    HASH_INDEX_TYPE index = entry[hash_val];
    entry[hash_val] = next_loc;
    memcpy(hashmap[next_loc].key,key,KEY_LEN);
    hashmap[next_loc].loc = loc;
    hashmap[next_loc].record_len = record_len;
    memcpy(hashmap[next_loc].lock,_lock,LOCK_LEN);
    hashmap[next_loc].next=index;
    hash_flag[next_loc] = 1;
    return 1;
}
//0,key doesnot exists;
//1,add lock success;
//2,lock is now occupied;
int nvm_Get(char * _key, HASH_VALUE hash_val,BLOCK_INDEX_TYPE * ans_block_index,VALUE_LEN_TYPE* record_len,char* _lock) {
    int ret = 0;
    BLOCK_INDEX_TYPE block_index = -1 ;
    int index = entry[hash_val];
    while(index!=-1){     
        if(strncmp(_key,hashmap[index].key,KEY_LEN)==0){
            if(strlen(hashmap[index].lock)==0){//lock is does not exist              
                block_index = hashmap[index].loc;               
                memcpy(hashmap[index].lock,_lock,LOCK_LEN);            
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
int nvm_Put(char * _key,HASH_VALUE hash_val, VALUE_LEN_TYPE record_len,BLOCK_INDEX_TYPE * ans_block_index,char * _lock){   
    int ret = nvm_Del(_key,hash_val,_lock);  
    if (ret==2){//lock is occupied
        return ret;
    }
    // ret==0,key does not exist 
    // ret==1,key exist ,lock is does not exist 
    BLOCK_INDEX_TYPE loc = GetBlockIndex(record_len);
    *ans_block_index =loc;
    // Update key buffer in memory
    return hashmap_push(hash_val,_key,loc,record_len,_lock);
}

//0,key does not exist;
//1,key exist and lock does not exist ,deleted the key and value;
//2,lock is now occupied;
int nvm_Del(char * _key,HASH_VALUE hash_val,char* _lock){
    int ret=0;
    BLOCK_INDEX_TYPE block_index = -1 ;
    int index = entry[hash_val];
    int last_index = -1;
    while(index!=-1){
        if(strncmp(_key,hashmap[index].key,KEY_LEN)==0){  
            if(strlen(hashmap[index].lock)==0){//lock is does not exist 
                block_index = hashmap[index].loc;
                ret = 1;                
            }else{
                 ret = 2;//lock is now occupied
            }
            break;     
        }   
        last_index = index;
        index =  hashmap[index].next;
    }    
    if(block_index!=-1){
      int block_num =  (hashmap[index].record_len+ BLOCK_SIZE - 1) / BLOCK_SIZE;
      Delete(block_index,block_num);     
      hash_flag[index]=0;
      if(last_index==-1){          
          entry[hash_val] = hashmap[index].next;
      }else{
           hashmap[last_index].next= hashmap[index].next;
      }
      return ret;//lock does not exist ,deleted the key and value,1
    }    
    return ret;//key does not exist,0;lock is now occupied,2;
}
//0,key does not exist;
//1,key exist and lock will be deleted;
//2,lock is now occupied;
int nvm_Del_lock(char * _key,HASH_VALUE hash_val,char* _lock){
     int ret=0;
    int index = entry[hash_val];
    int last_index = -1;
    while(index!=-1){
        if(strncmp(_key,hashmap[index].key,KEY_LEN)==0){  
            if(strlen(hashmap[index].lock)==0){//lock is does not exist 
                return 0;
            }
            if(strncmp(_lock,hashmap[index].lock,LOCK_LEN)==0){ 
                memset(hashmap[index].lock,'\0',LOCK_LEN);
                return 1;//lock will be deleted                
            }else{
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
  while(hash_flag[glo_loc]==1){
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







