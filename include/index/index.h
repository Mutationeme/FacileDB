#ifndef __INDEX_H__
#define __INDEX_H__

#include <stdint.h>
#include <stdbool.h>

#if !defined(INDEX_TEST)
#include "faciledb_index.h"
#endif

#ifndef INDEX_ORDER
#define INDEX_ORDER (5)
#endif

#ifndef INDEX_PAYLOAD_SIZE
// ((INDEX_PAYLOAD_SIZE + sizeof(index_id)) * INDEX_ORDER) should be divisible by 4.
#define INDEX_PAYLOAD_SIZE (16)
#endif

#ifndef INDEX_FILE_PATH_BUFFER_LENGTH
#define INDEX_FILE_PATH_BUFFER_LENGTH (256)
#endif

#define INDEX_CHILD_TAG_ORDER (INDEX_ORDER + 1)
#define INDEX_FILE_PATH_MAX_LENGTH (INDEX_FILE_PATH_BUFFER_LENGTH - 1)

typedef enum
{
#ifdef INDEX_ID_TYPE_CONFIG
#undef INDEX_ID_TYPE_CONFIG
#endif
#define INDEX_ID_TYPE_CONFIG(index_id_type, index_id_size) index_id_type,
#include "index_id_type_table.h"
#undef INDEX_ID_TYPE_CONFIG
    INDEX_ID_TYPE_NUM,
    INDEX_ID_TYPE_INVALID = INDEX_ID_TYPE_NUM
} INDEX_ID_TYPE_E;

void Index_Api_Init(char *p_index_directory_path);
bool Index_Api_Index_Key_Exist(char *p_index_key);
void Index_Api_Insert_Element(char *p_index_key, void *p_index_id, INDEX_ID_TYPE_E index_id_type, void *p_index_payload, uint32_t payload_size);
void *Index_Api_Search_Equal(char *p_index_key, void *p_target_index_id, INDEX_ID_TYPE_E index_id_type, uint32_t *p_result_length);
void Index_Api_Free_Search_Result(void *p_result);
void Index_Api_Close();

#endif // __INDEX_H__