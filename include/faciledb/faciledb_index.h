#ifndef __FACILEDB_INDEX_H__
#define __FACILEDB_INDEX_H__

#include <stdint.h>

typedef struct
{
    uint64_t data_tag;
    uint64_t start_db_block_tag; // The first block tag of the data.
} DB_INDEX_PAYLOAD_T;

#define INDEX_PAYLOAD_SIZE (sizeof(DB_INDEX_PAYLOAD_T))

#endif // __FACILEDB_INDEX_H__