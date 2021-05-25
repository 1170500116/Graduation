//
// Created by andyshen on 1/15/21.
//
#include<stdlib.h>
#include<stdint.h>

typedef uint32_t HASH_VALUE;
typedef uint64_t VALUE_LEN_TYPE;
typedef uint32_t KEY_INDEX_TYPE;
typedef uint32_t BLOCK_INDEX_TYPE;
typedef int32_t HASH_INDEX_TYPE;
// size of record
#define KEY_LEN 16
static const uint8_t VAL_SIZE_LEN = 4;
static const uint8_t RECORD_FIX_LEN = 20;
static const uint64_t VALUE_MAX_LEN = 1000;
//static const uint64_t VALUE_MAX_LEN = 100000;

// offset of record
static const uint8_t VAL_SIZE_OFFSET = 0;
static const uint8_t KEY_OFFSET = 4;//VAL_SIZE_LEN;
static const uint8_t VALUE_OFFSET = 20;//VERSION_OFFSET + VERSION_LEN;

// aep setting


//static const uint64_t FILE_SIZE = 68719476736UL;
//static const uint64_t FILE_SIZE = 6871946UL;
//static const uint64_t FILE_SIZE = 6400000000UL;
static const uint32_t FILE_SIZE = 640000000U;

                                
static const uint8_t BLOCK_SIZE = 64;
static const BLOCK_INDEX_TYPE max_block_index_ = 10000000;//(BLOCK_INDEX_TYPE)(FILE_SIZE /  BLOCK_SIZE);
//static const BLOCK_INDEX_TYPE max_block_index_ = 10;

extern char * file_buffer;
#define server_num 2
#define ZONG_HASH_MAP_SIZE 1000
#define LOCK_LEN 10
static char * server_ip[4];


