// #include "nvm_engine.h"
// #include <sys/resource.h>
// #include <sys/types.h>
// #include <unistd.h>
// #include<string.h>
// #include <stdio.h>
// #include <stdint.h>
// HASH_INDEX_TYPE glo_loc = 0;
// char * glo_p ;
// char *end ;
// void Push(BLOCK_INDEX_TYPE _block_index, int _size)  {
//     char * p = file_buffer+_block_index;
//     for (int i = 0;i<_size;i++){
//       *p = '0';
//       p++;
//     }      
// }
// int Pop(BLOCK_INDEX_TYPE* _block_index, int _size) {
//     *_block_index =0;
//     int now_size = 0;
//     while(glo_p!=end &&now_size<_size){   
//         if(*glo_p=='0'){
//             now_size++;        
//         }else{
//             now_size=0;
//             *_block_index = glo_p-file_buffer+1;
//         }
//         glo_p++;      
//     } 
//     if(*_block_index ==0){
//         glo_p = file_buffer;
//         now_size = 0;
//         while(glo_p!=end &&now_size<_size){    
//             if(*glo_p=='0'){
//               now_size++;        
//             }else{
//               now_size=0;
//             *_block_index = glo_p-file_buffer+1;
//             }
//             glo_p++;      
//         } 
//     }
    
//     if(now_size==_size){    
//       memset(file_buffer+(*_block_index),'1',_size);  
//       //printf("pop:_block_index=%d\n",*_block_index);
//       return 1;
//    }
 

//     return 0;    
// }
// void init_freelist(){
//   //printf("max_block_index_=%d\n",max_block_index_); 
//   memset(file_buffer,'0',max_block_index_);   
//   BLOCK_INDEX_TYPE _index = 1;
//   int flag_block_num = (max_block_index_+BLOCK_SIZE-1)/BLOCK_SIZE;  
//   memset(file_buffer,'1',flag_block_num); 
//   // int ans = Pop(&_index,flag_block_num);
//   // if(_index!=0||ans==0){
//   //   printf("error~~!!!1\n");
//   // }
// }

// int  New( BLOCK_INDEX_TYPE* _index,int _size) {
//       return Pop(_index, _size);
// }

// int Delete( BLOCK_INDEX_TYPE _index,int _size) {
//     Push(_index, _size);
//     return 1;
// }  
  


      
      
   
  
  



// HASH_VALUE DJBHash(char* _str) {
//   unsigned int hash = 5381;
//   int len = KEY_LEN;
//   for(int i = KEY_LEN-1;i>=0;i--){
//       if(_str[i]!=' '){
//         len = i+1;
//         break;
//       }
//   }
//   for (int i = 0; i < len; ++_str, ++i) {
//     hash = ((hash << 5) + hash) + (*_str);
//   }
//   return hash%HASH_MAP_SIZE;
// }

// void init_hashmap(){
//   glo_p = file_buffer;
//   end = file_buffer+max_block_index_;
//   memset(hash_flag,0,sizeof(hash_flag));
//   memset(entry,-1,sizeof(entry));  
//   init_freelist();
 
// }


// BLOCK_INDEX_TYPE GetBlockIndex(VALUE_LEN_TYPE data_len) {

//   int block_num =
//       (RECORD_FIX_LEN + data_len+5 + BLOCK_SIZE - 1) / BLOCK_SIZE;

//   BLOCK_INDEX_TYPE block_index = UINT32_MAX;
 
//   if (!New( &block_index,block_num)) {
//     block_index = UINT32_MAX;
//     printf( "Out of memory, when allocate an aep space.\n");
//     abort();
//   }
//   return block_index;
// }
// int hashmap_push(HASH_VALUE hash_val,char * key,BLOCK_INDEX_TYPE loc,int block_num){
//     HASH_INDEX_TYPE next_loc = get_next_free_hashnode();
//     if(next_loc ==-1){
//       printf("hashmap out of memory!!!!\n");
//       return 0;
//     }  
//     HASH_INDEX_TYPE index = entry[hash_val];
//     entry[hash_val] = next_loc;
//     memcpy(hashmap[next_loc].key,key,KEY_LEN);
//     hashmap[next_loc].loc = loc;
//     hashmap[next_loc].block_num = block_num;
//     hashmap[next_loc].next=index;
//     hash_flag[next_loc] = 1;
//     return 1;
// }

// int nvm_Get(char * _key, BLOCK_INDEX_TYPE * ans_block_index,int* block_num) {

//     HASH_VALUE hash_val =  DJBHash(_key);     
//     BLOCK_INDEX_TYPE block_index = 0 ;
//     int index = entry[hash_val];
   
//     while(index!=-1){     
//       if(strncmp(_key,hashmap[index].key,KEY_LEN)==0){
//         block_index = hashmap[index].loc;
//         break;
//       }
//       index =  hashmap[index].next;
//     }    
//     if(block_index==0){
//         return 0;
//     } 
//     *ans_block_index = block_index;
//     *block_num = hashmap[index].block_num;
//     return 1;
// }
// int nvm_Put(char * _key, VALUE_LEN_TYPE data_len,BLOCK_INDEX_TYPE * ans_block_index){  
//   nvm_Del(_key); 
//   BLOCK_INDEX_TYPE loc = GetBlockIndex(data_len);
//    int block_num =
//       (RECORD_FIX_LEN + data_len +5+ BLOCK_SIZE - 1) / BLOCK_SIZE;
//   *ans_block_index =loc;
  

//   // Update key buffer in memory
//   HASH_VALUE hash_val =  DJBHash(_key); 
  
//   //printf("loc:%d\n",loc);
//   return hashmap_push(hash_val,_key,loc,block_num);
// }
// int nvm_Del(char * _key){
//     HASH_VALUE hash_val =  DJBHash(_key); 
//     BLOCK_INDEX_TYPE block_index = 0 ;
//     int index = entry[hash_val];
//     int last_index = -1;
//     while(index!=-1){
//       if(strncmp(_key,hashmap[index].key,KEY_LEN)==0){   
//         block_index = hashmap[index].loc;
//         break;
//       }
//       last_index = index;
//       index =  hashmap[index].next;
//     }    
//     if(block_index!=0){
//       Delete(block_index,hashmap[index].block_num);
//       hash_flag[index]=0;
//       if(last_index==-1){          
//           entry[hash_val] = hashmap[index].next;
//       }else{
//         hashmap[last_index].next= hashmap[index].next;
//       }
//       return 1;
//     }    
//     return 0;
// }
// HASH_INDEX_TYPE get_next_free_hashnode(){
//   int flag = 0;
//   while(hash_flag[glo_loc]==1){
//     glo_loc++;
//     if(glo_loc==KV_NUM_MAX){   
//       flag++;   
//       glo_loc = 0; 
//       if(flag==2){
//         return -1;
//       }    
//     }
//   }
//   return glo_loc;
// }


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
  


// HASH_VALUE DJBHash(char* _str) {
//   unsigned int hash = 5381;
//   int len = KEY_LEN;
//   for(int i = KEY_LEN-1;i>=0;i--){
//       if(_str[i]!=' '){
//         len = i+1;
//         break;
//       }
//   }
//   for (int i = 0; i < len; ++_str, ++i) {
//     hash = ((hash << 5) + hash) + (*_str);
//   }
//   return hash%HASH_MAP_SIZE;
// }

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
int hashmap_push(HASH_VALUE hash_val,char * key,BLOCK_INDEX_TYPE loc,VALUE_LEN_TYPE record_len){
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
    hashmap[next_loc].next=index;
    hash_flag[next_loc] = 1;
    return 1;
}

int nvm_Get(char * _key, BLOCK_INDEX_TYPE * ans_block_index,VALUE_LEN_TYPE* record_len) {

    HASH_VALUE hash_val =  DJBHash(_key); 
    if(!(hash_val>=HASH_MIN&&hash_val<HASH_MAX)){
      printf("error hash get\n");
        return 0;
    }    
    hash_val-=HASH_MIN;
    BLOCK_INDEX_TYPE block_index = -1 ;
    int index = entry[hash_val];
  
    while(index!=-1){     
      if(strncmp(_key,hashmap[index].key,KEY_LEN)==0){
        block_index = hashmap[index].loc;
        break;
      }
      index =  hashmap[index].next;
    }    
    if(block_index==-1){
        return 0;
    } 
    *ans_block_index = block_index;
    *record_len = hashmap[index].record_len;
    return 1;
}
int nvm_Put(char * _key, VALUE_LEN_TYPE record_len,BLOCK_INDEX_TYPE * ans_block_index){   
  nvm_Del(_key);  
  BLOCK_INDEX_TYPE loc = GetBlockIndex(record_len);
  
  *ans_block_index =loc;
  // Update key buffer in memory
  HASH_VALUE hash_val =  DJBHash(_key); 

  if(!(hash_val>=HASH_MIN&&hash_val<HASH_MAX)){
    printf("%d\n",HASH_MIN);
      printf("%d\n",HASH_MAX);
    
        printf("hash_val:%d\n",hash_val);
       printf("error hash get\n");
      return 0;
  }    
  hash_val-=HASH_MIN;
  
  //printf("loc:%d",loc);
  return hashmap_push(hash_val,_key,loc,record_len);
}
int nvm_Del(char * _key){
    HASH_VALUE hash_val =  DJBHash(_key); 
    if(!(hash_val>=HASH_MIN&&hash_val<HASH_MAX)){
         printf("error hash get\n");
        return 0;
    }    
    hash_val-=HASH_MIN;
    BLOCK_INDEX_TYPE block_index = -1 ;
    int index = entry[hash_val];
    int last_index = -1;
    while(index!=-1){
      if(strncmp(_key,hashmap[index].key,KEY_LEN)==0){   
        block_index = hashmap[index].loc;
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
      return 1;
    }    
    return 0;
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






