#ifndef __FACILEDB_H__
#define __FACILEDB_H__

#include <stdint.h>
#include <stdio.h>

// TODO: compile error when block size < sizeof(DB_RECORD_PROPERTIES_T)
#ifndef FACILEDB_BLOCK_DATA_SIZE
#define FACILEDB_BLOCK_DATA_SIZE (1028)
#endif

#ifndef FACILEDB_FILE_PATH_BUFFER_LENGTH
#define FACILEDB_FILE_PATH_BUFFER_LENGTH (256) // linux definition: 255bytes
#endif

#define FACILEDB_FILE_PATH_MAX_LENGTH (FACILEDB_FILE_PATH_BUFFER_LENGTH - 1)

#ifndef ENABLE_DB_INDEX
#define ENABLE_DB_INDEX (0)
#endif

typedef enum
{
    FACILEDB_RECORD_VALUE_TYPE_UINT32,
    FACILEDB_RECORD_VALUE_TYPE_STRING,

    FACILEDB_RECORD_VALUE_TYPE_NUM
} FACILEDB_RECORD_VALUE_TYPE_E;

// user input format
typedef struct
{
    uint32_t key_size;
    uint32_t value_size;
    union
    {
        FACILEDB_RECORD_VALUE_TYPE_E record_value_type;
        uint32_t record_value_type_32;
    };
    void *p_key;
    void *p_value;
} FACILEDB_RECORD_T;

// user input format
typedef struct
{
    uint32_t record_num;
    FACILEDB_RECORD_T *p_data_records;
} FACILEDB_DATA_T;

void FacileDB_Api_Init(char *p_db_directory_path);
void FacileDB_Api_Close();
bool FacileDB_Api_Check_Set_Exist(char *p_db_set_name);
uint32_t FacileDB_Api_Insert_Data(char *p_db_set_name, FACILEDB_DATA_T *p_faciledb_data);
FACILEDB_DATA_T *FacileDB_Api_Search_Equal(char *p_db_set_name, FACILEDB_RECORD_T *p_faciledb_record, uint32_t *p_faciledb_data_num);
uint32_t FacileDB_Api_Delete_Equal(char *p_db_set_name, FACILEDB_RECORD_T *p_faciledb_record);

void FacileDB_Api_Free_Data_Buffer(FACILEDB_DATA_T *p_faciledb_data);
void FacileDB_Api_Free_Record_Buffer(FACILEDB_RECORD_T *p_facilledb_record);

#endif // __FACILEDB_H__
