//#include <libpmem.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include "define.h"

void init_freelist();

int  New( BLOCK_INDEX_TYPE* _index,int _size);
int Delete( BLOCK_INDEX_TYPE _index,int _size);

// hash setting
//static const uint32_t KV_NUM_MAX = 16 * 24 * 1024 * 1024 * 0.60;

#define HASH_MAP_SIZE 1000
#define HASH_MIN 0
#define HASH_MAX HASH_MIN+HASH_MAP_SIZE
#define KV_NUM_MAX  10000000

typedef struct Node
{
  char key[KEY_LEN];
  BLOCK_INDEX_TYPE loc;
  VALUE_LEN_TYPE record_len;
  HASH_INDEX_TYPE next;
}Node;
Node hashmap[KV_NUM_MAX];
int hash_flag[KV_NUM_MAX];
int entry[HASH_MAP_SIZE];
void init_hashmap();
int nvm_Get(char * _key, BLOCK_INDEX_TYPE * ans_block_index,VALUE_LEN_TYPE* record_len);
int nvm_Put(char * _key, VALUE_LEN_TYPE record_len,BLOCK_INDEX_TYPE * ans_block_index);
int nvm_Del(char * _key);




