//#include <libpmem.h>



#include <math.h>
#include <stdio.h>
#include <string.h>
#include "define.h"

void init_freelist();

int  New( BLOCK_INDEX_TYPE* _index,int _size);
int Delete( BLOCK_INDEX_TYPE _index,int _size);

// hash setting

#define HASH_MAP_SIZE ZONG_HASH_MAP_SIZE/server_num
#define HASH_MIN 750
#define HASH_MAX HASH_MIN+HASH_MAP_SIZE
#define KV_NUM_MAX  10000000

typedef struct Node
{
  char key[KEY_LEN];
  BLOCK_INDEX_TYPE loc;
  VALUE_LEN_TYPE record_len;
  HASH_INDEX_TYPE next;
  char lock[LOCK_LEN];
 }Node;
Node hashmap[KV_NUM_MAX];
int hash_flag[KV_NUM_MAX];
int entry[HASH_MAP_SIZE];
void init_hashmap();

//0,key doesnot exists;
//1,add lock success;
//2,lock is now occupied;
int nvm_Get(char * _key, HASH_VALUE hash_val,BLOCK_INDEX_TYPE * ans_block_index,VALUE_LEN_TYPE* record_len,char* _lock);
//0,hashmap out of memory;
//1,add lock success;
//2,lock is now occupied;
int nvm_Put(char * _key, HASH_VALUE hash_val,VALUE_LEN_TYPE record_len,BLOCK_INDEX_TYPE * ans_block_index,char* _lock);
//0,key does not exist;
//1,key exist and lock does not exist ,deleted the key and value;
//2,lock is now occupied;
int nvm_Del(char * _key,HASH_VALUE hash_val,char* _lock);
//0,key does not exist;
//1,key exist and lock will be deleted;
//2,lock is now occupied;
int nvm_Del_lock(char * _key,HASH_VALUE hash_val,char* _lock);

HASH_INDEX_TYPE get_next_free_hashnode();
HASH_VALUE check_hashval(char *key);



