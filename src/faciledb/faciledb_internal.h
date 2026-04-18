#ifndef __FACILEDB_INTERNAL_H__
#define __FACILEDB_INTERNAL_H__

#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <unistd.h>
#if defined(_POSIX_VERSION)
#define IS_POSIX_API_SUPPORT (1)
#else
#define IS_POSIX_API_SUPPORT (0)
#endif // _POSIX_VERSION
#endif // (__unix__) || ((__APPLE__) && (__MACH__))

#include <stdint.h>
#include <stdio.h>

#if IS_POSIX_API_SUPPORT
#include <fcntl.h>
#include <pthread.h>
#else // IS_POSIX_API_SUPPORT
#error "POSIX API is not supported."
#endif // IS_POSIX_API_SUPPORT

#include "faciledb_record_value_type.h"
#include "faciledb.h"

#if ENABLE_DB_INDEX
#include "faciledb_index.h"
#include "index.h"
#endif

// Enum definition
typedef enum
{
    DB_CONTEXT_STATUS_UNUSED,
    DB_CONTEXT_STATUS_INITIALIZING,
    DB_CONTEXT_STATUS_CLOSING,
    DB_CONTEXT_STATUS_READY
} DB_CONTEXT_STATUS_E;

typedef enum
{
    DB_SET_INFO_STATUS_RELEASED,
    DB_SET_INFO_STATUS_STARTING,
    DB_SET_INFO_STATUS_CLOSING,
    DB_SET_INFO_STATUS_READY,
    DB_SET_INFO_STATUS_WRITING,
    DB_SET_INFO_STATUS_READING,
} DB_SET_INFO_STATUS_E;

// Struct definition
typedef struct
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t mutex;
    pthread_cond_t read_cond;
    pthread_cond_t write_cond;
    pthread_cond_t close_cond;
#endif
    uint32_t writer_waiting_count;
    uint32_t reader_waiting_count;
    uint32_t reader_count;
} DB_SET_INFO_SYNC_T;

typedef struct
{
    uint64_t seq_num; // // Monotonically increasing sequence used to determine the latest valid header during recovery.
    uint64_t block_num;
    uint64_t created_time;
    uint64_t modified_time;
    uint64_t data_num;
    uint32_t crc32; // crc32 of the above values (doesn't include crc32 itself)
} DB_SET_PROPERTIES_T;

typedef struct
{
    DB_SET_INFO_STATUS_E status;
    DB_SET_INFO_SYNC_T db_set_info_sync;

    FILE *file;

    DB_SET_PROPERTIES_T db_set_properties;
    uint32_t set_name_size;
    void *p_set_name;
} DB_SET_INFO_T;

typedef struct
{
    uint32_t deleted;
    uint32_t key_size;
    uint32_t value_size;
    union
    {
        FACILEDB_RECORD_VALUE_TYPE_E record_value_type;
        uint32_t record_value_type_32;
    };
} DB_RECORD_PROPERTIES_T;

// Record: A key-value pair
typedef struct
{
    void *p_key;
    void *p_value;
} DB_RECORD_T;

typedef struct
{
    DB_RECORD_PROPERTIES_T db_record_properties;
    DB_RECORD_T db_record;
    off_t db_record_properties_offset; // The offset value from the block data starting address to the record properties address.
} DB_RECORD_INFO_T;

typedef struct
{
    uint64_t data_tag;
    uint64_t start_db_block_tag;
    uint64_t created_time;
    uint64_t modified_time;
    uint32_t record_num;
    uint32_t deleted;
    DB_RECORD_INFO_T *p_db_record_info;
} DB_DATA_INFO_T;

typedef struct DB_DATA_INFO_LIST_NODE_T
{
    DB_DATA_INFO_T db_data_info;
    struct DB_DATA_INFO_LIST_NODE_T *p_prev;
    struct DB_DATA_INFO_LIST_NODE_T *p_next;
} DB_DATA_INFO_LIST_NODE_T;

typedef struct
{
    uint32_t list_length;
    DB_DATA_INFO_LIST_NODE_T *p_head;
    DB_DATA_INFO_LIST_NODE_T *p_tail;
} DB_DATA_INFO_LIST_ENTRY_T;

typedef struct
{
    uint64_t block_tag; // 1-based number, block_tag = 0 means null
    uint64_t data_tag;  // 1-based number
    uint64_t prev_block_tag;
    uint64_t next_block_tag;
    uint64_t created_time;
    uint64_t modified_time;
    uint32_t deleted;
    uint32_t valid_record_num;   // data based
    uint32_t record_crc32;       // foreach (RECORD_PROPERTIES_T + RECORD_T) whose delete flag is not 0.
    uint32_t block_header_crc32; // crc32 of the above values (doesn't include block_crc32 itself)

    uint8_t block_data[FACILEDB_BLOCK_DATA_SIZE]; // store db_records.
} DB_BLOCK_T;

typedef struct DB_BLOCK_LIST_NODE_T
{
    DB_BLOCK_T db_block;
    struct DB_BLOCK_LIST_NODE_T *p_prev;
    struct DB_BLOCK_LIST_NODE_T *p_next;
} DB_BLOCK_LIST_NODE_T;

typedef struct DB_BLOCK_LIST_ENTRY_T
{
    uint32_t list_length;
    DB_BLOCK_LIST_NODE_T *p_head;
    DB_BLOCK_LIST_NODE_T *p_tail;
} DB_BLOCK_LIST_ENTRY_T;

// Function definition
void lock_db_context_sync();
void unlock_db_context_sync();
bool check_db_context_status(DB_CONTEXT_STATUS_E target_status);
void update_db_context_status(DB_CONTEXT_STATUS_E new_status);

bool set_db_directory_path(char *p_db_directory_path);
void clear_db_directory_path();

void get_db_set_file_path_by_db_set_name(char *p_db_set_name, char *p_db_set_file_path);
bool is_db_set_file_exist(char *p_db_set_file_path); // TODO: rename to check_xxx

DB_SET_INFO_T *load_and_lock_db_set_info(char *p_db_set_name);
void close_db_set_info_instances();
void sync_latest_db_set_info(DB_SET_INFO_T *p_db_set_info); // TODO: rename the name "sync"
void update_db_set_info_status(DB_SET_INFO_T *p_db_set_info, DB_SET_INFO_STATUS_E new_status);
void update_db_set_info_status_from_reading(DB_SET_INFO_T *p_db_set_info);
void lock_db_set_info_sync(DB_SET_INFO_T *p_db_set_info);
void unlock_db_set_info_sync(DB_SET_INFO_T *p_db_set_info);
void db_set_info_sync_write_wait(DB_SET_INFO_T *p_db_set_info);
void db_set_info_sync_write_unblock(DB_SET_INFO_T *p_db_set_info);
void db_set_info_sync_read_wait(DB_SET_INFO_T *p_db_set_info);
void db_set_info_sync_read_unblock(DB_SET_INFO_T *p_db_set_info);
void db_set_info_file_lock_write(DB_SET_INFO_T *p_db_set_info);
void db_set_info_file_unlock_write(DB_SET_INFO_T *p_db_set_info);
void db_set_info_file_lock_read(DB_SET_INFO_T *p_db_set_info);
void db_set_info_file_unlock_read(DB_SET_INFO_T *p_db_set_info);

size_t get_db_record_properties_size();
void update_to_db_set_properties_region(DB_SET_INFO_T *p_db_set_info);

void db_data_info_init(DB_DATA_INFO_T *p_db_data_info);
void db_data_info_list_entry_init(DB_DATA_INFO_LIST_ENTRY_T *p_entry);
void db_data_info_list_node_init(DB_DATA_INFO_LIST_NODE_T *p_node);
void free_db_data_info_list_node_resources(DB_DATA_INFO_LIST_NODE_T *p_node);
void free_db_data_info_resources(DB_DATA_INFO_T *p_db_data_info);
void free_db_data_info_list(DB_DATA_INFO_LIST_ENTRY_T *p_entry); // TODO: resource
bool shallow_assign_faciledb_data_to_db_data_info(DB_DATA_INFO_T *p_db_data_info, FACILEDB_DATA_T *p_faciledb_data);
bool shallow_assign_db_data_info_to_failedb_data(FACILEDB_DATA_T *p_faciledb_data, DB_DATA_INFO_T *p_db_data_info);
uint32_t insert_db_data(DB_SET_INFO_T *p_db_set_info, DB_DATA_INFO_T *p_db_data_info, uint64_t data_tag);
void search_db_data(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, DB_DATA_INFO_LIST_ENTRY_T *p_result_db_data_info_list_entry);
void delete_db_data(DB_SET_INFO_T *p_db_set_info, DB_DATA_INFO_LIST_ENTRY_T *p_db_data_info_list_entry);

void db_record_info_init(DB_RECORD_INFO_T *p_db_record_info);
void shallow_assign_faciledb_record_to_db_record_info(DB_RECORD_INFO_T *p_db_record_info, FACILEDB_RECORD_T *p_faciledb_record);

void db_block_init(DB_BLOCK_T *p_db_block);
void db_block_list_entry_init(DB_BLOCK_LIST_ENTRY_T *p_entry);
void db_block_list_node_init(DB_BLOCK_LIST_NODE_T *p_node);
void append_db_block_list_node_to_tail(DB_BLOCK_LIST_ENTRY_T *p_entry, DB_BLOCK_LIST_NODE_T *p_node);
void write_db_block(DB_BLOCK_T *p_db_block, DB_SET_INFO_T *p_db_set_info);
uint32_t read_db_block(DB_SET_INFO_T *p_db_set_info, uint64_t block_tag, DB_BLOCK_T *p_db_block);
uint32_t read_db_block_attributes(DB_SET_INFO_T *p_db_set_info, uint64_t block_tag, DB_BLOCK_T *p_db_block);
bool extract_db_data_info_from_db_blocks(DB_DATA_INFO_T *p_db_data_info, uint64_t start_block_tag, DB_SET_INFO_T *p_db_set_info);

#if ENABLE_DB_INDEX
bool get_db_index_directory_path(char *p_db_index_directory_path);
INDEX_ID_TYPE_E get_db_index_id_type(FACILEDB_RECORD_VALUE_TYPE_E record_value_type);
char *set_db_index_key(void *db_set_name, uint32_t set_name_size, void *p_key, uint32_t key_size);
void insert_db_record_index(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_db_record_info, DB_INDEX_PAYLOAD_T *p_db_index_payload);
#endif // ENABLE_DB_INDEX

#endif // __FACILEDB_INTERNAL_H__
