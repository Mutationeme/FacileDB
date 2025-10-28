#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stddef.h>
#include <errno.h>

#include "faciledb.h"
#include "faciledb_utils.h"
#include "faciledb_record_value_type.h"

#if defined(_POSIX_VERSION)
#define IS_POSIX_API_SUPPORT (1)
#else
#define IS_POSIX_API_SUPPORT (0)
#endif

#if IS_POSIX_API_SUPPORT
#include <fcntl.h>
#include <pthread.h>
#else
#error "POSIX API is not supported."
#endif

#if ENABLE_DB_INDEX
#include "faciledb_index.h"
#include "hash.h"
#include "index.h"
#endif

#ifndef DB_SET_INFO_INSTANCE_NUM
#define DB_SET_INFO_INSTANCE_NUM (1)
// TODO: FIFO, LRU
#endif // DB_SET_INFO_INSTANCE_NUM

#ifndef DB_SEARCH_DATA_INFO_BUFFER_LEN
#define DB_SEARCH_DATA_INFO_BUFFER_LEN (8)
#endif // DB_SEARCH_DATA_INFO_BUFFER_LEN

#define DB_FILE_OPEN_CHECK_TIMEOUT (30)
#define DB_FILE_OPEN_CHECK_INTERVAL_US (100000) // 100ms

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

// Structure definition
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

// in-memory structure
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

typedef struct
{
    uint64_t block_tag; // 1-based number, block_tag = 0 means null
    uint64_t data_tag;  // 1-based number
    uint64_t prev_block_tag;
    uint64_t next_block_tag;
    uint64_t created_time;
    uint64_t modified_time;
    uint32_t deleted;
    uint32_t valid_record_num;      // data based
    uint32_t record_properties_num; // numbers of record in the data block

    uint8_t block_data[FACILEDB_BLOCK_DATA_SIZE]; // contains lots of db records.
} DB_BLOCK_T;

typedef struct
{
    uint64_t block_num;
    uint64_t created_time;
    uint64_t modified_time;
    uint64_t valid_record_num;
    uint32_t set_name_size;
    void *p_set_name;
} DB_SET_PROPERTIES_T;

// in-memory structure
typedef struct
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t db_set_info_mutex;
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
    DB_SET_INFO_STATUS_E status;
    DB_SET_INFO_SYNC_T db_set_info_sync; // rename to sync_info

    FILE *file;
    DB_SET_PROPERTIES_T db_set_properties;
} DB_SET_INFO_T;

typedef struct
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t mutex;
#endif
    DB_CONTEXT_STATUS_E status; // TODO: move status to DB_CONTEXT_T
} DB_CONTEXT_SYNC_T;

// TODO: using context
typedef struct
{
    DB_CONTEXT_SYNC_T db_context_sync;
    DB_SET_INFO_T *p_db_set_info_instances_list;
    char db_directory_path[FACILEDB_FILE_PATH_BUFFER_LENGTH];
} DB_CONTEXT_T;
// End of structure definition

// static variables
static DB_CONTEXT_SYNC_T db_context_sync = {
#if IS_POSIX_API_SUPPORT
    .mutex = PTHREAD_MUTEX_INITIALIZER,
#endif
    .status = DB_CONTEXT_STATUS_UNUSED};

static DB_SET_INFO_T db_set_info_instance[DB_SET_INFO_INSTANCE_NUM] = {
    {.status = DB_SET_INFO_STATUS_RELEASED,
     .db_set_info_sync = {
#if IS_POSIX_API_SUPPORT
         .db_set_info_mutex = PTHREAD_MUTEX_INITIALIZER,
         .read_cond = PTHREAD_COND_INITIALIZER,
         .write_cond = PTHREAD_COND_INITIALIZER,
         .close_cond = PTHREAD_COND_INITIALIZER,
#endif
         .reader_waiting_count = 0,
         .writer_waiting_count = 0,
         .reader_count = 0}}};

static char db_directory_path[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};
// End of static vaiables

// Local function declaration
static inline void lock_db_context_sync();
static inline void unlock_db_context_sync();
void update_db_context_status(DB_CONTEXT_STATUS_E new_status);
static inline bool check_db_context_status(DB_CONTEXT_STATUS_E target_status);
bool set_db_directory_path(char *p_db_directory_path);
void clear_db_directory_path();

void db_set_info_instances_init();
DB_SET_INFO_T *request_and_lock_released_db_set_info_instance();
DB_SET_INFO_T *query_and_lock_db_set_info_loaded(char *p_db_set_name_string);
DB_SET_INFO_T *load_and_lock_db_set_info(char *p_db_set_name);
void close_db_set_info_instances();
void create_new_db_set_file_format(DB_SET_INFO_T *p_db_set_info, uint8_t *p_db_set_name, uint32_t db_set_name_size);
bool read_and_check_db_set_file_format(DB_SET_INFO_T *p_db_set_info, uint8_t *p_db_set_name, uint32_t db_set_name_size);

void db_set_info_init(DB_SET_INFO_T *p_db_set_info);
bool is_db_set_file_exist(char *p_db_set_file_path);
void get_db_set_file_path_by_db_set_name(char *p_db_set_name, char *p_db_set_file_path);
void free_db_set_info_resources(DB_SET_INFO_T *p_db_set_info);
void close_db_set_info(DB_SET_INFO_T *p_db_set_info);
void db_set_info_sync_init(DB_SET_INFO_SYNC_T *p_db_set_info_sync);
static inline void db_set_info_file_lock_write(DB_SET_INFO_T *p_db_set_info);
static inline void db_set_info_file_unlock_write(DB_SET_INFO_T *p_db_set_info);
static inline void db_set_info_file_lock_read(DB_SET_INFO_T *p_db_set_info);
static inline void db_set_info_file_unlock_read(DB_SET_INFO_T *p_db_set_info);
static inline void lock_db_set_info_sync(DB_SET_INFO_T *p_db_set_info);
static inline void unlock_db_set_info_sync(DB_SET_INFO_T *p_db_set_info);
static inline void db_set_info_sync_close_wait(DB_SET_INFO_T *p_db_set_info);
static inline void db_set_info_sync_write_wait(DB_SET_INFO_T *p_db_set_info);
static inline void db_set_info_sync_write_unblock(DB_SET_INFO_T *p_db_set_info);
static inline void db_set_info_sync_read_wait(DB_SET_INFO_T *p_db_set_info);
static inline void db_set_info_sync_read_unblock(DB_SET_INFO_T *p_db_set_info);
void update_db_set_info_status(DB_SET_INFO_T *p_db_set_info, DB_SET_INFO_STATUS_E new_status);
void update_db_set_info_status_from_reading(DB_SET_INFO_T *p_db_set_info);
static inline bool check_db_set_info_status(DB_SET_INFO_T *p_db_set_info, DB_SET_INFO_STATUS_E target_status);

void db_set_properties_init(DB_SET_PROPERTIES_T *p_db_set_properties);
size_t get_db_set_properties_size(DB_SET_PROPERTIES_T *p_db_set_properties);
bool allocate_db_set_properties_resources(DB_SET_PROPERTIES_T *p_db_set_properties, uint32_t set_name_size);
void free_db_set_properties_resources(DB_SET_PROPERTIES_T *p_db_set_properties);
void write_db_set_properties(DB_SET_INFO_T *p_db_set_info);
void read_db_set_properties(DB_SET_INFO_T *p_db_set_info);
static inline uint64_t add_db_set_properties_valid_record_num(DB_SET_PROPERTIES_T *p_db_set_properties);

void db_block_init(DB_BLOCK_T *p_db_block);
off_t get_db_block_offset(DB_SET_PROPERTIES_T *p_db_set_properties, uint64_t block_tag);
size_t get_db_block_size();
void write_db_block(DB_BLOCK_T *p_db_block, DB_SET_INFO_T *p_db_set_info);
void read_db_block(DB_SET_INFO_T *p_db_set_info, uint64_t block_tag, DB_BLOCK_T *p_db_block);
void read_db_block_attributes(DB_SET_INFO_T *p_db_set_info, uint64_t block_tag, DB_BLOCK_T *p_db_block);
void update_db_block_next_block_tag(uint64_t block_tag, uint64_t next_block_tag, DB_SET_INFO_T *p_db_set_info);

void db_record_info_init(DB_RECORD_INFO_T *p_db_record_info);
bool allocate_db_record_info_resources(DB_RECORD_INFO_T *p_db_record_info);
void free_db_record_info_resources(DB_RECORD_INFO_T *p_db_record_info);
bool faciledb_record_to_db_record_info(FACILEDB_RECORD_T *p_faciledb_record, DB_RECORD_INFO_T *p_db_record_info);
void shallow_assign_faciledb_record_to_db_record_info(DB_RECORD_INFO_T *p_db_record_info, FACILEDB_RECORD_T *p_faciledb_record);
void shallow_assign_db_record_info_to_faciledb_record(FACILEDB_RECORD_T *p_faciledb_record, DB_RECORD_INFO_T *p_db_record_info);
size_t get_db_record_properties_size();

void db_data_info_init(DB_DATA_INFO_T *p_db_data_info);
void free_db_data_info_resources(DB_DATA_INFO_T *p_db_data_info);
void shallow_copy_db_data_info(DB_DATA_INFO_T *p_dest_db_data_info, DB_DATA_INFO_T *p_src_db_data_info);
bool shallow_assign_db_data_info_to_failedb_data(FACILEDB_DATA_T *p_faciledb_data, DB_DATA_INFO_T *p_db_data_info);
bool shallow_assign_faciledb_data_to_db_data_info(DB_DATA_INFO_T *p_db_data_info, FACILEDB_DATA_T *p_faciledb_data);

void db_record_init(DB_RECORD_T *p_db_record);
bool allocate_db_record_resources(DB_RECORD_T *p_db_record, uint32_t key_size, uint32_t value_size);
void free_db_record_resources(DB_RECORD_T *p_db_record);

uint32_t insert_db_data(DB_SET_INFO_T *p_db_set_info, DB_DATA_INFO_T *p_db_data_info, uint64_t data_tag);
DB_DATA_INFO_T *search_db_data_sequential(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, uint32_t *p_result_db_data_info_num);
uint64_t insert_db_data_handler_write_new_db_block(DB_BLOCK_T *p_db_block, DB_SET_INFO_T *p_db_set_info);
void insert_db_data_handler_assign_db_block_value(DB_BLOCK_T *p_db_block, uint64_t prev_block_tag, uint32_t valid_record_num, uint64_t data_tag);
DB_DATA_INFO_T *search_db_data(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, uint32_t *p_result_db_data_info_num);
void delete_db_data_handler_write_delete_flag(DB_SET_INFO_T *p_db_set_info, uint64_t db_block_tag, uint32_t deleted);
void delete_db_data(DB_SET_INFO_T *p_db_set_info, DB_DATA_INFO_T *p_db_data_info, uint32_t db_data_num);

#if ENABLE_DB_INDEX
bool get_db_index_directory_path(char *p_db_index_directory_path);
char *set_db_index_key(void *db_set_name, uint32_t set_name_size, void *p_key, uint32_t key_size);
INDEX_ID_TYPE_E get_db_index_id_type(FACILEDB_RECORD_VALUE_TYPE_E record_value_type);
uint32_t make_db_record_index(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_db_record_info);
void insert_db_record_index(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_db_record_info, DB_INDEX_PAYLOAD_T *p_db_index_payload);
DB_DATA_INFO_T *search_db_data_indexed(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, uint32_t *p_result_db_data_info_num);
#endif

// End of local function declaration

void FacileDB_Api_Init(char *p_db_directory_path)
{
    char temp_db_directory_path[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};

    strncpy(temp_db_directory_path, p_db_directory_path, FACILEDB_FILE_PATH_MAX_LENGTH);
    temp_db_directory_path[FACILEDB_FILE_PATH_MAX_LENGTH] = '\0';

    lock_db_context_sync();
    // TODO: If re-init, close_db_set_info_instances and reset db_directory_path first.
    if (check_db_context_status(DB_CONTEXT_STATUS_UNUSED) == false)
    {
        // Already initialized.
        unlock_db_context_sync();
        return;
    }
    update_db_context_status(DB_CONTEXT_STATUS_INITIALIZING);

    set_db_directory_path(temp_db_directory_path);
    // db_set_info_instances_init();

#if ENABLE_DB_INDEX
    char temp_db_index_directory_path[INDEX_FILE_PATH_BUFFER_LENGTH] = {0};
    get_db_index_directory_path(temp_db_index_directory_path);
    // The db_index_directory_path must be set before calling this function.
    Index_Api_Init(temp_db_index_directory_path);
#endif

    update_db_context_status(DB_CONTEXT_STATUS_READY);
    unlock_db_context_sync();
}

void FacileDB_Api_Close()
{
    lock_db_context_sync();
    if (check_db_context_status(DB_CONTEXT_STATUS_READY) == false)
    {
        // Not initialized or already closed.
        unlock_db_context_sync();
        return;
    }
    update_db_context_status(DB_CONTEXT_STATUS_CLOSING);

    close_db_set_info_instances();
    clear_db_directory_path();

#if ENABLE_DB_INDEX
    Index_Api_Close();
#endif

    update_db_context_status(DB_CONTEXT_STATUS_UNUSED);
    unlock_db_context_sync();
}

bool FacileDB_Api_Check_Set_Exist(char *p_db_set_name)
{
    char db_set_file_path[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};
    bool existed = false;

    lock_db_context_sync();

    if (check_db_context_status(DB_CONTEXT_STATUS_READY) == true)
    {
        get_db_set_file_path_by_db_set_name(p_db_set_name, db_set_file_path);
        existed = is_db_set_file_exist(db_set_file_path);
    }

    unlock_db_context_sync();
    return existed;
}

// Return Value: Number of inserted data.
uint32_t FacileDB_Api_Insert_Data(char *p_db_set_name, FACILEDB_DATA_T *p_faciledb_data)
{
    char temp_db_set_name[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};
    DB_SET_INFO_T *p_db_set_info = NULL;
    uint64_t data_tag = 0;
    DB_DATA_INFO_T db_data_info;

    // Check input parameters
    if (p_db_set_name == NULL || p_faciledb_data == NULL || p_faciledb_data->record_num == 0)
    {
        return 0;
    }

    lock_db_context_sync();
    // If the db context is not ready, return directly.
    if (check_db_context_status(DB_CONTEXT_STATUS_READY) == false)
    {
        unlock_db_context_sync();
        return 0;
    }

    strncpy(temp_db_set_name, p_db_set_name, FACILEDB_FILE_PATH_MAX_LENGTH);
    temp_db_set_name[FACILEDB_FILE_PATH_MAX_LENGTH] = '\0';

    p_db_set_info = load_and_lock_db_set_info(temp_db_set_name);
    unlock_db_context_sync();

    // convert format from FACILEDB_DATA_T to DB_DATA_INFO_T
    db_data_info_init(&db_data_info);
    shallow_assign_faciledb_data_to_db_data_info(&db_data_info, p_faciledb_data);

    db_set_info_sync_write_wait(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_WRITING);
    db_set_info_file_lock_write(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    data_tag = add_db_set_properties_valid_record_num(&(p_db_set_info->db_set_properties));
    insert_db_data(p_db_set_info, &db_data_info, data_tag);

    // write due to add valid record number.
    // TODO: update when close operation.
    write_db_set_properties(p_db_set_info);

    // start of sync
    lock_db_set_info_sync(p_db_set_info);
    db_set_info_file_unlock_write(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY);
    db_set_info_sync_write_unblock(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);
    // end of sync

    // free dynamic resources allocated at shallow_assign_faciledb_data_to_db_data_info.
    free(db_data_info.p_db_record_info);

    return 1;
}

// Return value: FACILEDB_DATA_T array and *p_faciledb_data_num
FACILEDB_DATA_T *FacileDB_Api_Search_Equal(char *p_db_set_name, FACILEDB_RECORD_T *p_faciledb_record, uint32_t *p_faciledb_data_num)
{
    char temp_db_set_name[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};
    DB_SET_INFO_T *p_db_set_info = NULL;
    DB_RECORD_INFO_T target_db_record;
    DB_DATA_INFO_T *p_db_result_data = NULL;
    FACILEDB_DATA_T *p_faciledb_data_result_array = NULL;
    uint32_t result_data_num = 0;

    // Check input parameters
    if (p_db_set_name == NULL || p_faciledb_record == NULL ||
        Faciledb_Record_Value_Type_Check_Size_Valid(p_faciledb_record->record_value_type, p_faciledb_record->value_size) == false)
    {
        // invalid
        *p_faciledb_data_num = 0;
        return NULL;
    }

    strncpy(temp_db_set_name, p_db_set_name, FACILEDB_FILE_PATH_MAX_LENGTH);
    temp_db_set_name[FACILEDB_FILE_PATH_MAX_LENGTH] = '\0';

    lock_db_context_sync();

    if (check_db_context_status(DB_CONTEXT_STATUS_READY) == false)
    {
        // db context is not ready
        unlock_db_context_sync();
        *p_faciledb_data_num = 0;
        return NULL;
    }

    p_db_set_info = load_and_lock_db_set_info(temp_db_set_name);
    unlock_db_context_sync();

    db_record_info_init(&target_db_record);
    shallow_assign_faciledb_record_to_db_record_info(&target_db_record, p_faciledb_record);

    db_set_info_sync_read_wait(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READING);
    db_set_info_file_lock_read(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    p_db_result_data = search_db_data(p_db_set_info, &target_db_record, FACILEDB_RECORD_VALUE_TYPE_COMPARE_EQUAL, &result_data_num);

    lock_db_set_info_sync(p_db_set_info);
    db_set_info_file_unlock_read(p_db_set_info);
    update_db_set_info_status_from_reading(p_db_set_info);
    db_set_info_sync_read_unblock(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    // Fill to faciledb structure
    p_faciledb_data_result_array = calloc(result_data_num, sizeof(FACILEDB_DATA_T));
    for (uint32_t i = 0; i < result_data_num; i++)
    {
        // p_faciledb_data_result_array[i].p_data_records buffer will be freed in the below function.
        shallow_assign_db_data_info_to_failedb_data(&(p_faciledb_data_result_array[i]), &(p_db_result_data[i]));

        // free dynamic buffers with different data type, and the content is shallow assigned to p_faciledb_data_result_array[i].
        free(p_db_result_data[i].p_db_record_info);
    }

    // free reuslt buffer
    free(p_db_result_data);

    if (result_data_num == 0)
    {
        free(p_faciledb_data_result_array);
        p_faciledb_data_result_array = NULL;
    }

    *p_faciledb_data_num = result_data_num;
    return p_faciledb_data_result_array;
}

// Return value: delete data number
uint32_t FacileDB_Api_Delete_Equal(char *p_db_set_name, FACILEDB_RECORD_T *p_faciledb_record)
{
    char temp_db_set_name[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};
    DB_SET_INFO_T *p_db_set_info = NULL;
    DB_RECORD_INFO_T target_db_record;
    DB_DATA_INFO_T *p_target_db_data = NULL;
    uint32_t delete_data_num = 0;

    // Check input parameters
    if (p_db_set_name == NULL || p_faciledb_record == NULL || Faciledb_Record_Value_Type_Check_Size_Valid(p_faciledb_record->record_value_type, p_faciledb_record->value_size) == false)
    {
        // invalid
        return 0;
    }

    strncpy(temp_db_set_name, p_db_set_name, FACILEDB_FILE_PATH_MAX_LENGTH);
    temp_db_set_name[FACILEDB_FILE_PATH_MAX_LENGTH] = '\0';

    lock_db_context_sync();
    if (check_db_context_status(DB_CONTEXT_STATUS_READY) == false)
    {
        // db context is not ready
        unlock_db_context_sync();
        return 0;
    }

    p_db_set_info = load_and_lock_db_set_info(temp_db_set_name);
    unlock_db_context_sync();

    db_record_info_init(&target_db_record);
    shallow_assign_faciledb_record_to_db_record_info(&target_db_record, p_faciledb_record);

    db_set_info_sync_write_wait(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_WRITING);
    db_set_info_file_lock_write(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    p_target_db_data = search_db_data(p_db_set_info, &target_db_record, FACILEDB_RECORD_VALUE_TYPE_COMPARE_EQUAL, &delete_data_num);
    // Delete the target data
    delete_db_data(p_db_set_info, p_target_db_data, delete_data_num);

    lock_db_set_info_sync(p_db_set_info);
    db_set_info_file_unlock_write(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY);
    db_set_info_sync_write_unblock(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    return delete_data_num;
}

void FacileDB_Api_Free_Data_Buffer(FACILEDB_DATA_T *p_faciledb_data)
{
    uint32_t record_num = 0;

    if (p_faciledb_data == NULL)
    {
        return;
    }

    record_num = p_faciledb_data->record_num;
    for (uint32_t i = 0; i < record_num; i++)
    {
        FacileDB_Api_Free_Record_Buffer(&(p_faciledb_data->p_data_records[i]));
    }
}

void FacileDB_Api_Free_Record_Buffer(FACILEDB_RECORD_T *p_facilledb_record)
{
    if (p_facilledb_record == NULL)
    {
        return;
    }

    free(p_facilledb_record->p_key);
    free(p_facilledb_record->p_value);
}

static inline void lock_db_context_sync()
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_db_context_mutex = &(db_context_sync.mutex);
    pthread_mutex_lock(p_db_context_mutex);
#endif
}

static inline void unlock_db_context_sync()
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_db_context_mutex = &(db_context_sync.mutex);
    pthread_mutex_unlock(p_db_context_mutex);
#endif
}

void update_db_context_status(DB_CONTEXT_STATUS_E new_status)
{
    DB_CONTEXT_STATUS_E *p_db_context_sync_status = &(db_context_sync.status);
    DB_CONTEXT_STATUS_E current_status = *p_db_context_sync_status;
    bool is_valid_transition = false;

    switch (current_status)
    {
    case DB_CONTEXT_STATUS_UNUSED:
    {
        if (new_status == DB_CONTEXT_STATUS_INITIALIZING)
        {
            is_valid_transition = true;
        }
        break;
    }
    case DB_CONTEXT_STATUS_INITIALIZING:
    {
        if (new_status == DB_CONTEXT_STATUS_READY)
        {
            is_valid_transition = true;
        }
        break;
    }
    case DB_CONTEXT_STATUS_CLOSING:
    {
        if (new_status == DB_CONTEXT_STATUS_UNUSED)
        {
            is_valid_transition = true;
        }
        break;
    }

    case DB_CONTEXT_STATUS_READY:
    {
        if (new_status == DB_CONTEXT_STATUS_CLOSING)
        {
            is_valid_transition = true;
        }
        break;
    }
    }

    if (is_valid_transition)
    {
        *p_db_context_sync_status = new_status;
    }
    else
    {
        // Invalid status changes.
        assert(0);
    }
}

// Lock the db_context_mutex before using this function.
static inline bool check_db_context_status(DB_CONTEXT_STATUS_E target_status)
{
    return (db_context_sync.status == target_status);
}

bool set_db_directory_path(char *p_db_directory_path)
{
    struct stat db_directory_stat;
    size_t str_length = strlen(p_db_directory_path);
    char temp_db_directory_path[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};
    char temp_path_buffer[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};

    if (str_length == 0)
    {
        return false;
    }

    strncpy(temp_db_directory_path, p_db_directory_path, FACILEDB_FILE_PATH_MAX_LENGTH);
    temp_db_directory_path[FACILEDB_FILE_PATH_MAX_LENGTH] = '\0';

    // The last char is not '/'.
    if (temp_db_directory_path[str_length - 1] != '/')
    {
        if (str_length == FACILEDB_FILE_PATH_MAX_LENGTH)
        {
            // String length reaches the max buffer length, modify the last char to '/'.
            temp_db_directory_path[str_length - 1] = '/';
        }
        else
        {
            // Append '/' at the end of the path.
            temp_db_directory_path[str_length] = '/';
            temp_db_directory_path[str_length + 1] = '\0';
        }
    }

    // Check if db directory exists or not.
    if (!((stat(temp_db_directory_path, &db_directory_stat) == 0) && (S_ISDIR(db_directory_stat.st_mode))))
    {
        // Doesn't exist or is not a directory. Create a new direcotry.
        for (char *p = strchr(temp_db_directory_path + 1, '/'); p != NULL; p = strchr(p + 1, '/'))
        {
            strncpy(temp_path_buffer, temp_db_directory_path, p - temp_db_directory_path);
            temp_path_buffer[p - temp_db_directory_path] = '\0';

            if (stat(temp_path_buffer, &db_directory_stat) != 0 || !(S_ISDIR(db_directory_stat.st_mode)))
            {
                if (mkdir(temp_path_buffer, 0755) == -1)
                {
                    perror("Create index directory fail");
                    // clear the path buffer
                    return false;
                }
            }
        }
    }

    strncpy(db_directory_path, temp_db_directory_path, FACILEDB_FILE_PATH_MAX_LENGTH);
    db_directory_path[FACILEDB_FILE_PATH_MAX_LENGTH] = '\0';

    return true;
}

void clear_db_directory_path()
{
    memset(db_directory_path, '\0', sizeof(db_directory_path));
}

void db_set_info_instances_init()
{
    for (uint32_t i = 0; i < DB_SET_INFO_INSTANCE_NUM; i++)
    {
        DB_SET_INFO_T *p_db_set_info = &(db_set_info_instance[i]);
#if IS_POSIX_API_SUPPORT
        pthread_mutex_t *p_db_set_info_mutex = &(p_db_set_info->db_set_info_sync.db_set_info_mutex);
        pthread_mutex_lock(p_db_set_info_mutex);
#endif

        db_set_info_init(&(db_set_info_instance[i]));

        update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_RELEASED);
#if IS_POSIX_API_SUPPORT
        pthread_mutex_unlock(p_db_set_info_mutex);
#endif
    }
}

DB_SET_INFO_T *request_and_lock_released_db_set_info_instance()
{
    DB_SET_INFO_T *p_db_set_info = NULL;

    // Current: only one instance
#if (DB_SET_INFO_INSTANCE_NUM == 1)
    p_db_set_info = &(db_set_info_instance[0]);

    lock_db_set_info_sync(p_db_set_info);
    if (check_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_RELEASED) == false)
    {
        // not released, close it first.
        db_set_info_sync_close_wait(p_db_set_info);
        update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_CLOSING);
        close_db_set_info(p_db_set_info);
        update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_RELEASED);
    }
#else  // DB_SET_INFO_INSTANCE_NUM == 1
#endif // DB_SET_INFO_INSTANCE_NUM == 1

    return p_db_set_info;
}

// Lock the target db_set_info and check if the set_name matches.
// If matches, return the pointer of the target db_set_info.
// If not matches, unlock the target db_set_info and return NULL.
DB_SET_INFO_T *query_and_lock_db_set_info_loaded(char *p_db_set_name_string)
{
    DB_SET_INFO_T *p_target_db_set_info = NULL;
    uint32_t db_set_name_string_size = strnlen(p_db_set_name_string, FACILEDB_FILE_PATH_MAX_LENGTH);

#if (DB_SET_INFO_INSTANCE_NUM == 1)
    p_target_db_set_info = &(db_set_info_instance[0]);

    lock_db_set_info_sync(p_target_db_set_info);

    if (p_target_db_set_info->status >= DB_SET_INFO_STATUS_READY &&
        (p_target_db_set_info->db_set_properties.set_name_size == db_set_name_string_size) &&
        (memcmp(p_target_db_set_info->db_set_properties.p_set_name, p_db_set_name_string, db_set_name_string_size) == 0))
    {
        return p_target_db_set_info;
    }
    else
    {
        unlock_db_set_info_sync(p_target_db_set_info);
        return NULL;
    }
#endif
}

void create_new_db_set_file_format(DB_SET_INFO_T *p_db_set_info, uint8_t *p_db_set_name, uint32_t db_set_name_size)
{
    uint64_t current_time = get_current_time();

    // Setup set_name nad write new db properties into file.
    // store the db_set_name without the '\0'.
    allocate_db_set_properties_resources(&(p_db_set_info->db_set_properties), db_set_name_size);
    p_db_set_info->db_set_properties.set_name_size = db_set_name_size;
    memcpy(p_db_set_info->db_set_properties.p_set_name, p_db_set_name, p_db_set_info->db_set_properties.set_name_size);
    p_db_set_info->db_set_properties.created_time = current_time;
    p_db_set_info->db_set_properties.modified_time = current_time;

    write_db_set_properties(p_db_set_info);
}

bool read_and_check_db_set_file_format(DB_SET_INFO_T *p_db_set_info, uint8_t *p_db_set_name, uint32_t db_set_name_size)
{
    DB_SET_PROPERTIES_T *p_db_set_properties = &(p_db_set_info->db_set_properties);
    DB_SET_PROPERTIES_T expected_db_set_properties = {.set_name_size = db_set_name_size};
    size_t expected_db_set_properties_size = get_db_set_properties_size(&expected_db_set_properties);

#if IS_POSIX_API_SUPPORT
    // read db properties from file and check set_name_size & set_name

    int fd = fileno(p_db_set_info->file);
    off_t file_size = lseek(fd, 0, SEEK_END);

    if (file_size >= expected_db_set_properties_size)
    {
        read_db_set_properties(p_db_set_info);

        if ((p_db_set_properties->set_name_size == db_set_name_size) && (memcmp(p_db_set_properties->p_set_name, p_db_set_name, db_set_name_size) == 0))
        {
            return true;
        }
        else
        {
            free_db_set_properties_resources(p_db_set_properties);
            db_set_properties_init(p_db_set_properties);
            return false;
        }
    }
    else
    {
        return false;
    }
#endif
}

DB_SET_INFO_T *load_and_lock_db_set_info(char *p_db_set_name)
{
    DB_SET_INFO_T *p_db_set_info = NULL;
    char db_set_file_path[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};

    p_db_set_info = query_and_lock_db_set_info_loaded(p_db_set_name);
    if (p_db_set_info != NULL)
    {
        return p_db_set_info;
    }

    // db_set_info is not loaded, load it from file.
    p_db_set_info = request_and_lock_released_db_set_info_instance();
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_STARTING);

    db_set_info_init(p_db_set_info);
    get_db_set_file_path_by_db_set_name(p_db_set_name, db_set_file_path);

#if IS_POSIX_API_SUPPORT
    // create new db_set_file if not exist.
    int fd = open(db_set_file_path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd >= 0)
    {
        // File created successfully
        p_db_set_info->file = fdopen(fd, "wb+");
        if (p_db_set_info->file == NULL)
        {
            // fdopen failed
            perror("DB set file unavailable: ");

            close(fd);
            unlock_db_set_info_sync(p_db_set_info);
            return NULL;
        }

        // Write db_set_properties
        db_set_info_file_lock_write(p_db_set_info);
        create_new_db_set_file_format(p_db_set_info, (uint8_t *)p_db_set_name, strlen(p_db_set_name));
        db_set_info_file_unlock_write(p_db_set_info);
    }
    else if (errno == EEXIST)
    {
        bool timeout = true;

        // File exists
        fd = open(db_set_file_path, O_RDWR);
        if (fd < 0)
        {
            perror("DB set file unavailable: ");
            unlock_db_set_info_sync(p_db_set_info);
            return NULL;
        }

        p_db_set_info->file = fdopen(fd, "rb+");
        if (p_db_set_info->file == NULL)
        {
            // fdopen failed
            perror("DB set file unavailable: ");

            close(fd);
            unlock_db_set_info_sync(p_db_set_info);
            return NULL;
        }

        // Try to read db_properties and check if db_properties writed done
        for (uint32_t check_time = 0; check_time < DB_FILE_OPEN_CHECK_TIMEOUT; check_time++)
        {
            db_set_info_file_lock_read(p_db_set_info);

            if (read_and_check_db_set_file_format(p_db_set_info, (uint8_t *)p_db_set_name, strlen(p_db_set_name)) == true)
            {
                db_set_info_file_unlock_read(p_db_set_info);
                timeout = false;
                break;
            }
            else
            {
                db_set_info_file_unlock_read(p_db_set_info);
                usleep(DB_FILE_OPEN_CHECK_INTERVAL_US);
            }
        }

        if (timeout)
        {
            assert(0);
        }
    }
    else
    {
        // error
        perror("DB set file unavailable: ");
        unlock_db_set_info_sync(p_db_set_info);
        return NULL;
    }

#else
    if (is_db_set_file_exist(db_set_file_path))
    {
        // open existed db_set_file as read and write permission.
        if (!((access(db_set_file_path, R_OK | W_OK) == 0) && ((p_db_set_info->file = fopen(db_set_file_path, "rb+")) != NULL)))
        {
            // db set file unreadable or unwritable
            perror("DB set file unavailable:");
            return NULL;
        }

        // read db properties from file and check set_name
        read_db_set_properties(p_db_set_info);
        if (!((strlen(p_db_set_name) == p_db_set_info->db_set_properties.set_name_size) &&
              (memcmp(p_db_set_name, p_db_set_info->db_set_properties.p_set_name, p_db_set_info->db_set_properties.set_name_size) == 0)))
        {
            close_db_set_info(p_db_set_info);
            return NULL;
        }
    }
    else
    {
        uint64_t current_time = 0;

        // db set file doesn't exist. Create a new db set file.
        p_db_set_info->file = fopen(db_set_file_path, "wb+");
        if (p_db_set_info->file == NULL)
        {
            // create db set file failed.
            perror("DB set file unavailable: ");
            return NULL;
        }

        // Setup set_name nad write new db properties into file.
        // store the db_set_name without the '\0'.
        // TODO: Setup variables
        allocate_db_set_properties_resources(&(p_db_set_info->db_set_properties), strlen(p_db_set_name));
        current_time = get_current_time();
        p_db_set_info->db_set_properties.set_name_size = strlen(p_db_set_name);
        memcpy(p_db_set_info->db_set_properties.p_set_name, p_db_set_name, p_db_set_info->db_set_properties.set_name_size);
        p_db_set_info->db_set_properties.created_time = current_time;
        p_db_set_info->db_set_properties.modified_time = current_time;
        write_db_set_properties(p_db_set_info);
    }
#endif

    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY);
    return p_db_set_info;
}

void close_db_set_info_instances()
{
    for (uint32_t i = 0; i < DB_SET_INFO_INSTANCE_NUM; i++)
    {
        DB_SET_INFO_T *p_db_set_info = &(db_set_info_instance[i]);

        lock_db_set_info_sync(p_db_set_info);

        // check db_set_info_status released
        if (check_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_RELEASED))
        {
            // already released
            unlock_db_set_info_sync(p_db_set_info);
            continue;
        }

        db_set_info_sync_close_wait(p_db_set_info);
        update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_CLOSING);

        close_db_set_info(p_db_set_info);

        update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_RELEASED);
        unlock_db_set_info_sync(p_db_set_info);
    }
}

void db_set_info_init(DB_SET_INFO_T *p_db_set_info)
{
    p_db_set_info->file = NULL;
    db_set_properties_init(&(p_db_set_info->db_set_properties));
    db_set_info_sync_init(&(p_db_set_info->db_set_info_sync));
}

// check if set file is in the db_set_directory
bool is_db_set_file_exist(char *p_db_set_file_path)
{
    // unistd.h
    if (access(p_db_set_file_path, F_OK) == 0)
    {
        // db set file exists
        return true;
    }
    else
    {
        // db set file doesn't exist.
        return false;
    }
}

void get_db_set_file_path_by_db_set_name(char *p_db_set_name, char *p_db_set_file_path)
{
    // file path: /db/directory/path/set_name.faciledb
    // TODO: check p_db_set_name contains sensitive keywords. e.g. '/'

    char file_extension[] = ".faciledb";

    if ((strlen(db_directory_path) + strlen(p_db_set_name) + strlen(file_extension)) > FACILEDB_FILE_PATH_MAX_LENGTH)
    {
        p_db_set_file_path[0] = '\0';
    }
    else
    {
        strcpy(p_db_set_file_path, db_directory_path);
        strcat(p_db_set_file_path, p_db_set_name);
        strcat(p_db_set_file_path, file_extension);

        p_db_set_file_path[FACILEDB_FILE_PATH_MAX_LENGTH] = '\0';
    }
}

void free_db_set_info_resources(DB_SET_INFO_T *p_db_set_info)
{
    if (p_db_set_info->file != NULL)
    {
        fclose(p_db_set_info->file);
        p_db_set_info->file = NULL;
    }

    free_db_set_properties_resources(&(p_db_set_info->db_set_properties));
}

void close_db_set_info(DB_SET_INFO_T *p_db_set_info)
{
    // Free dynamic buffers
    free_db_set_info_resources(p_db_set_info);

    // // Reset db_set_info variables except db_set_info_status.
    db_set_info_init(p_db_set_info);
}

// This funtion doesn't change the status to RELEASED.
void db_set_info_sync_init(DB_SET_INFO_SYNC_T *p_db_set_info_sync)
{
    // p_db_set_info_sync->status = DB_SET_INFO_STATUS_RELEASED;
    p_db_set_info_sync->reader_waiting_count = 0;
    p_db_set_info_sync->reader_count = 0;
    p_db_set_info_sync->writer_waiting_count = 0;
}

static inline void db_set_info_file_lock_write(DB_SET_INFO_T *p_db_set_info)
{
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_info->file);
    struct flock fl = {
        .l_type = F_WRLCK, // write lock
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0, // l_start = 0 && l_len = 0 means lock the whole file
        .l_pid = 0  // unused
    };

    // file level synchronization
    // fcntl F_SETLKW will block until the lock is acquired.
    // return value:  -1 means error.
    if (fcntl(fd, F_SETLKW, &fl) == -1)
    {
        // TODO: error handling
        assert(0);
    }
#endif
}

static inline void db_set_info_file_unlock_write(DB_SET_INFO_T *p_db_set_info)
{
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_info->file);
    struct flock fl = {
        .l_type = F_UNLCK, // unlock
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0, // l_start = 0 && l_len = 0 means unlock the whole file
        .l_pid = 0  // unused
    };

    // unlock doesn't need to wait.
    // return value: -1 means error.
    if (fcntl(fd, F_SETLK, &fl) == -1)
    {
        // TODO: error handling
        assert(0);
    }
#endif
}

static inline void db_set_info_file_lock_read(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t *p_db_set_info_sync_reader_count = &(p_db_set_info->db_set_info_sync.reader_count);
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_info->file);
    struct flock fl = {
        .l_type = F_RDLCK, // read lock
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0, // l_start = 0 && l_len = 0 means lock the whole file
        .l_pid = 0  // unused
    };

    if (*p_db_set_info_sync_reader_count == 1)
    {
        // first reader, get file lock
        // fcntl F_SETLKW will block until the lock is acquired.
        // return value: -1 means error.
        if (fcntl(fd, F_SETLKW, &fl) == -1)
        {
            // TODO: error handling
            assert(0);
        }
    }
#endif
}

static inline void db_set_info_file_unlock_read(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t *p_db_set_info_sync_reader_count = &(p_db_set_info->db_set_info_sync.reader_count);
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_info->file);
    struct flock fl = {
        .l_type = F_UNLCK, // unlock
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0, // l_start = 0 && l_len = 0 means lock the whole file
        .l_pid = 0  // unused
    };

    if (*p_db_set_info_sync_reader_count == 1)
    {
        // last reader.
        // unlock file, return value: -1 means error.
        if (fcntl(fd, F_SETLK, &fl) == -1)
        {
            // TODO: error handling
            assert(0);
        }
    }
#endif
}

static inline void lock_db_set_info_sync(DB_SET_INFO_T *p_db_set_info)
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.db_set_info_mutex);
    pthread_mutex_lock(p_mutex);
#endif
}

static inline void unlock_db_set_info_sync(DB_SET_INFO_T *p_db_set_info)
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.db_set_info_mutex);
    pthread_mutex_unlock(p_mutex);
#endif
}

static inline void db_set_info_sync_close_wait(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t *p_writer_waiting_count = &(p_db_set_info->db_set_info_sync.writer_waiting_count);
    uint32_t *p_reader_count = &(p_db_set_info->db_set_info_sync.reader_count);
    uint32_t *p_reader_waiting_count = &(p_db_set_info->db_set_info_sync.reader_waiting_count);
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.db_set_info_mutex);
    pthread_cond_t *p_close_cond = &(p_db_set_info->db_set_info_sync.close_cond);

    // using while loop for spurious wakeup
    while (check_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY) == false || *p_writer_waiting_count > 0 || *p_reader_count > 0 || *p_reader_waiting_count > 0)
    {
        pthread_cond_wait(p_close_cond, p_mutex);
    }
#endif
}

static inline void db_set_info_sync_write_wait(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t *p_writer_waiting_count = &(p_db_set_info->db_set_info_sync.writer_waiting_count);

    (*p_writer_waiting_count)++;
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.db_set_info_mutex);
    pthread_cond_t *p_write_cond = &(p_db_set_info->db_set_info_sync.write_cond);

    while (check_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY) == false)
    {
        pthread_cond_wait(p_write_cond, p_mutex);
    }
#endif
    (*p_writer_waiting_count)--;
}

static inline void db_set_info_sync_write_unblock(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t *p_writer_waiting_count = &(p_db_set_info->db_set_info_sync.writer_waiting_count);
    uint32_t *p_reader_waiting_count = &(p_db_set_info->db_set_info_sync.reader_waiting_count);
#if IS_POSIX_API_SUPPORT
    pthread_cond_t *p_write_cond = &(p_db_set_info->db_set_info_sync.write_cond);
    pthread_cond_t *p_read_cond = &(p_db_set_info->db_set_info_sync.read_cond);
    pthread_cond_t *p_close_cond = &(p_db_set_info->db_set_info_sync.close_cond);

    // Priority: write > read > close
    if (*p_writer_waiting_count > 0)
    {
        // notify the next writer
        pthread_cond_signal(p_write_cond);
    }
    else if (*p_reader_waiting_count > 0)
    {
        // notify all readers
        pthread_cond_broadcast(p_read_cond);
    }
    else
    {
        // notify to close
        pthread_cond_signal(p_close_cond);
    }
#endif
}

static inline void db_set_info_sync_read_wait(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t *p_reader_waiting_count = &(p_db_set_info->db_set_info_sync.reader_waiting_count);
    uint32_t *p_reader_count = &(p_db_set_info->db_set_info_sync.reader_count);

    (*p_reader_waiting_count)++;
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.db_set_info_mutex);
    pthread_cond_t *p_read_cond = &(p_db_set_info->db_set_info_sync.read_cond);

    // priority: write > read
    while (check_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READING) == false && (check_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY) == false || *p_reader_count > 0))
    {
        pthread_cond_wait(p_read_cond, p_mutex);
    }
#endif
    (*p_reader_waiting_count)--;
    (*p_reader_count)++;
}

static inline void db_set_info_sync_read_unblock(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t *p_reader_count = &(p_db_set_info->db_set_info_sync.reader_count);
    uint32_t *p_writer_waiting_count = &(p_db_set_info->db_set_info_sync.writer_waiting_count);
    uint32_t *p_reader_waiting_count = &(p_db_set_info->db_set_info_sync.reader_waiting_count);

    (*p_reader_count)--;
#if IS_POSIX_API_SUPPORT
    pthread_cond_t *p_write_cond = &(p_db_set_info->db_set_info_sync.write_cond);
    pthread_cond_t *p_close_cond = &(p_db_set_info->db_set_info_sync.close_cond);

    if (*p_reader_count == 0)
    {
        if (*p_writer_waiting_count > 0)
        {
            pthread_cond_signal(p_write_cond);
        }
        else if (*p_reader_waiting_count == 0)
        {
            // reader_count = 0 && writer_waiting_count = 0 && reader_waiting_count = 0
            pthread_cond_signal(p_close_cond);
        }
    }
#endif
}

void update_db_set_info_status(DB_SET_INFO_T *p_db_set_info, DB_SET_INFO_STATUS_E new_status)
{
    DB_SET_INFO_STATUS_E *p_db_set_info_sync_status = &(p_db_set_info->status);
    DB_SET_INFO_STATUS_E current_status = *p_db_set_info_sync_status;
    bool is_valid_transition = false;

    switch (current_status)
    {
    case DB_SET_INFO_STATUS_RELEASED:
    {
        if (new_status == DB_SET_INFO_STATUS_STARTING)
        {
            is_valid_transition = true;
        }
        break;
    }
    case DB_SET_INFO_STATUS_STARTING:
    {
        if (new_status == DB_SET_INFO_STATUS_READY)
        {
            is_valid_transition = true;
        }
        break;
    }
    case DB_SET_INFO_STATUS_CLOSING:
    {
        if (new_status == DB_SET_INFO_STATUS_RELEASED)
        {
            is_valid_transition = true;
        }
        break;
    }
    case DB_SET_INFO_STATUS_READY:
    {
        if (new_status == DB_SET_INFO_STATUS_WRITING || new_status == DB_SET_INFO_STATUS_READING || new_status == DB_SET_INFO_STATUS_CLOSING)
        {
            is_valid_transition = true;
        }
        break;
    }
    case DB_SET_INFO_STATUS_WRITING:
    {
        if (new_status == DB_SET_INFO_STATUS_READY)
        {
            is_valid_transition = true;
        }
        break;
    }
    case DB_SET_INFO_STATUS_READING:
    {
        if (new_status == DB_SET_INFO_STATUS_READY || new_status == DB_SET_INFO_STATUS_READING)
        {
            is_valid_transition = true;
        }
        break;
    }
    }

    if (is_valid_transition)
    {
        *p_db_set_info_sync_status = new_status;
    }
    else
    {
        assert(0);
    }
}

// Update the status to ready when no more users are reading and update the status to reading when there is at least one user reading.
void update_db_set_info_status_from_reading(DB_SET_INFO_T *p_db_set_info)
{
    assert(check_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READING) == true);

    uint32_t *p_reader_count = &(p_db_set_info->db_set_info_sync.reader_count);

    if (*p_reader_count > 1)
    {
        update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READING);
    }
    else
    {
        update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY);
    }
}

static inline bool check_db_set_info_status(DB_SET_INFO_T *p_db_set_info, DB_SET_INFO_STATUS_E target_status)
{
    return (p_db_set_info->status == target_status);
}

void db_set_properties_init(DB_SET_PROPERTIES_T *p_db_set_properties)
{
    p_db_set_properties->block_num = 0;
    p_db_set_properties->created_time = 0;
    p_db_set_properties->modified_time = 0;
    p_db_set_properties->valid_record_num = 0;
    p_db_set_properties->set_name_size = 0;

    p_db_set_properties->p_set_name = NULL;
}

size_t get_db_set_properties_size(DB_SET_PROPERTIES_T *p_db_set_properties)
{
    size_t set_properties_size = 0;

    // static variable
    set_properties_size = sizeof(p_db_set_properties->block_num) + sizeof(p_db_set_properties->created_time) + sizeof(p_db_set_properties->modified_time) + sizeof(p_db_set_properties->valid_record_num) + sizeof(p_db_set_properties->set_name_size);
    // dynamic variables
    set_properties_size += p_db_set_properties->set_name_size;

    return set_properties_size;
}

bool allocate_db_set_properties_resources(DB_SET_PROPERTIES_T *p_db_set_properties, uint32_t set_name_size)
{
    void *ptr = NULL;

    ptr = calloc(set_name_size, sizeof(uint8_t));
    if (ptr != NULL)
    {
        p_db_set_properties->p_set_name = ptr;
        return true;
    }

    return false;
}

void free_db_set_properties_resources(DB_SET_PROPERTIES_T *p_db_set_properties)
{
    // free set_name buffer
    if (p_db_set_properties->p_set_name != NULL)
    {
        free(p_db_set_properties->p_set_name);
        p_db_set_properties->p_set_name = NULL;
    }
}

/*
** The file variable in db_set_info should be set before calling this function.
*/
void write_db_set_properties(DB_SET_INFO_T *p_db_set_info)
{
    FILE *p_db_set_file = p_db_set_info->file;
    DB_SET_PROPERTIES_T *p_db_set_properties = &(p_db_set_info->db_set_properties);

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);
    off_t offset = 0;

    // write static variables
    pwrite(fd, &(p_db_set_properties->block_num), sizeof(p_db_set_properties->block_num), offset);
    offset += sizeof(p_db_set_properties->block_num);
    pwrite(fd, &(p_db_set_properties->created_time), sizeof(p_db_set_properties->created_time), offset);
    offset += sizeof(p_db_set_properties->created_time);
    pwrite(fd, &(p_db_set_properties->modified_time), sizeof(p_db_set_properties->modified_time), offset);
    offset += sizeof(p_db_set_properties->modified_time);
    pwrite(fd, &(p_db_set_properties->valid_record_num), sizeof(p_db_set_properties->valid_record_num), offset);
    offset += sizeof(p_db_set_properties->valid_record_num);
    pwrite(fd, &(p_db_set_properties->set_name_size), sizeof(p_db_set_properties->set_name_size), offset);
    offset += sizeof(p_db_set_properties->set_name_size);

    // write dynamic variables
    pwrite(fd, p_db_set_properties->p_set_name, p_db_set_properties->set_name_size, offset);
#else  // IS_POSIX_API_SUPPORT
    fseek(p_db_set_file, 0, SEEK_SET);

    // write static variables
    fwrite(&(p_db_set_properties->block_num), sizeof(p_db_set_properties->block_num), 1, p_db_set_file);
    fwrite(&(p_db_set_properties->created_time), sizeof(p_db_set_properties->created_time), 1, p_db_set_file);
    fwrite(&(p_db_set_properties->modified_time), sizeof(p_db_set_properties->modified_time), 1, p_db_set_file);
    fwrite(&(p_db_set_properties->valid_record_num), sizeof(p_db_set_properties->valid_record_num), 1, p_db_set_file);
    fwrite(&(p_db_set_properties->set_name_size), sizeof(p_db_set_properties->set_name_size), 1, p_db_set_file);

    // write dynamic variables
    fwrite(p_db_set_properties->p_set_name, p_db_set_properties->set_name_size, 1, p_db_set_file);
#endif // IS_POSIX_API_SUPPORT
}

/*
** The file variable in db_set_info should be set before calling this function.
*/
void read_db_set_properties(DB_SET_INFO_T *p_db_set_info)
{
    FILE *p_db_set_file = p_db_set_info->file;
    DB_SET_PROPERTIES_T *p_db_set_properties = &(p_db_set_info->db_set_properties);

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);
    off_t offset = 0;

    pread(fd, &(p_db_set_properties->block_num), sizeof(p_db_set_properties->block_num), offset);
    offset += sizeof(p_db_set_properties->block_num);
    pread(fd, &(p_db_set_properties->created_time), sizeof(p_db_set_properties->created_time), offset);
    offset += sizeof(p_db_set_properties->created_time);
    pread(fd, &(p_db_set_properties->modified_time), sizeof(p_db_set_properties->modified_time), offset);
    offset += sizeof(p_db_set_properties->modified_time);
    pread(fd, &(p_db_set_properties->valid_record_num), sizeof(p_db_set_properties->valid_record_num), offset);
    offset += sizeof(p_db_set_properties->valid_record_num);
    pread(fd, &(p_db_set_properties->set_name_size), sizeof(p_db_set_properties->set_name_size), offset);
    offset += sizeof(p_db_set_properties->set_name_size);

    // allocate dynamic variable buffer and read dynamic variables from file
    allocate_db_set_properties_resources(p_db_set_properties, p_db_set_properties->set_name_size);
    pread(fd, p_db_set_properties->p_set_name, p_db_set_properties->set_name_size, offset);
#else  // IS_POSIX_API_SUPPORT
    fseek(p_db_set_file, 0, SEEK_SET);

    // read ststic variables
    fread(&(p_db_set_properties->block_num), sizeof(p_db_set_properties->block_num), 1, p_db_set_file);
    fread(&(p_db_set_properties->created_time), sizeof(p_db_set_properties->created_time), 1, p_db_set_file);
    fread(&(p_db_set_properties->modified_time), sizeof(p_db_set_properties->modified_time), 1, p_db_set_file);
    fread(&(p_db_set_properties->valid_record_num), sizeof(p_db_set_properties->valid_record_num), 1, p_db_set_file);
    fread(&(p_db_set_properties->set_name_size), sizeof(p_db_set_properties->set_name_size), 1, p_db_set_file);

    // allocate dynamic variable buffer and read dynamic variables from file
    allocate_db_set_properties_resources(p_db_set_properties, p_db_set_properties->set_name_size);
    fread(p_db_set_properties->p_set_name, p_db_set_properties->set_name_size, 1, p_db_set_file);
#endif // IS_POSIX_API_SUPPORT
}

// return the updated value
static inline uint64_t add_db_set_properties_valid_record_num(DB_SET_PROPERTIES_T *p_db_set_properties)
{
    // add and return the added value.
    return ++(p_db_set_properties->valid_record_num);
}

void db_block_init(DB_BLOCK_T *p_db_block)
{
    p_db_block->block_tag = 0;
    p_db_block->data_tag = 0;
    p_db_block->prev_block_tag = 0;
    p_db_block->next_block_tag = 0;
    p_db_block->created_time = 0;
    p_db_block->modified_time = 0;
    p_db_block->deleted = 0;
    p_db_block->valid_record_num = 0;
    p_db_block->record_properties_num = 0;

    memset(p_db_block->block_data, 0, FACILEDB_BLOCK_DATA_SIZE);
}

off_t get_db_block_offset(DB_SET_PROPERTIES_T *p_db_set_properties, uint64_t block_tag)
{
    size_t set_properties_size = get_db_set_properties_size(p_db_set_properties);
    size_t block_size = get_db_block_size();

    return (set_properties_size + ((block_tag - 1) * block_size));
}

size_t get_db_block_size()
{
    DB_BLOCK_T dummy_db_block;
    size_t attribute_size = sizeof(dummy_db_block.block_tag) + sizeof(dummy_db_block.data_tag) + sizeof(dummy_db_block.prev_block_tag) + sizeof(dummy_db_block.next_block_tag);
    size_t time_attribute_size = sizeof(dummy_db_block.created_time) + sizeof(dummy_db_block.modified_time);
    size_t other_attribute_size = sizeof(dummy_db_block.deleted) + sizeof(dummy_db_block.valid_record_num) + sizeof(dummy_db_block.record_properties_num);

    size_t block_data_size = sizeof(dummy_db_block.block_data);

    return (attribute_size + time_attribute_size + other_attribute_size + block_data_size);
}

void write_db_block(DB_BLOCK_T *p_db_block, DB_SET_INFO_T *p_db_set_info)
{
    FILE *p_db_set_file = p_db_set_info->file;
    DB_SET_PROPERTIES_T *p_db_set_properties = &(p_db_set_info->db_set_properties);
    uint64_t block_tag = p_db_block->block_tag;
    off_t block_offset = get_db_block_offset(p_db_set_properties, block_tag);

    assert((block_tag > 0) && (block_tag <= p_db_set_properties->block_num));

    // write static variables
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);

    // alignment makes sizeof and get_db_block_size() different.
    if(sizeof(DB_BLOCK_T) == get_db_block_size())
    {
        pwrite(fd, p_db_block, sizeof(DB_BLOCK_T), block_offset);
    }
    else
    {
        pwrite(fd, &(p_db_block->block_tag), sizeof(p_db_block->block_tag), block_offset);
        block_offset += sizeof(p_db_block->block_tag);
        pwrite(fd, &(p_db_block->data_tag), sizeof(p_db_block->data_tag), block_offset);
        block_offset += sizeof(p_db_block->data_tag);
        pwrite(fd, &(p_db_block->prev_block_tag), sizeof(p_db_block->prev_block_tag), block_offset);
        block_offset += sizeof(p_db_block->prev_block_tag);
        pwrite(fd, &(p_db_block->next_block_tag), sizeof(p_db_block->next_block_tag), block_offset);
        block_offset += sizeof(p_db_block->next_block_tag);
        pwrite(fd, &(p_db_block->created_time), sizeof(p_db_block->created_time), block_offset);
        block_offset += sizeof(p_db_block->created_time);
        pwrite(fd, &(p_db_block->modified_time), sizeof(p_db_block->modified_time), block_offset);
        block_offset += sizeof(p_db_block->modified_time);
        pwrite(fd, &(p_db_block->deleted), sizeof(p_db_block->deleted), block_offset);
        block_offset += sizeof(p_db_block->deleted);
        pwrite(fd, &(p_db_block->valid_record_num), sizeof(p_db_block->valid_record_num), block_offset);
        block_offset += sizeof(p_db_block->valid_record_num);
        pwrite(fd, &(p_db_block->record_properties_num), sizeof(p_db_block->record_properties_num), block_offset);
        block_offset += sizeof(p_db_block->record_properties_num);
        pwrite(fd, p_db_block->block_data, sizeof(p_db_block->block_data), block_offset);
    }
#else  // IS_POSIX_API_SUPPORT
    fseek(p_db_set_file, block_offset, SEEK_SET);

    if (sizeof(DB_BLOCK_T) == get_db_block_size())
    {
        fwrite(p_db_block, sizeof(DB_BLOCK_T), 1, p_db_set_file);
    }
    else
    {
        fwrite(&(p_db_block->block_tag), sizeof(p_db_block->block_tag), 1, p_db_set_file);
        fwrite(&(p_db_block->data_tag), sizeof(p_db_block->data_tag), 1, p_db_set_file);
        fwrite(&(p_db_block->prev_block_tag), sizeof(p_db_block->prev_block_tag), 1, p_db_set_file);
        fwrite(&(p_db_block->next_block_tag), sizeof(p_db_block->next_block_tag), 1, p_db_set_file);
        fwrite(&(p_db_block->created_time), sizeof(p_db_block->created_time), 1, p_db_set_file);
        fwrite(&(p_db_block->modified_time), sizeof(p_db_block->modified_time), 1, p_db_set_file);
        fwrite(&(p_db_block->deleted), sizeof(p_db_block->deleted), 1, p_db_set_file);
        fwrite(&(p_db_block->valid_record_num), sizeof(p_db_block->valid_record_num), 1, p_db_set_file);
        fwrite(&(p_db_block->record_properties_num), sizeof(p_db_block->record_properties_num), 1, p_db_set_file);

        fwrite(p_db_block->block_data, sizeof(p_db_block->block_data), 1, p_db_set_file);
    }
#endif // IS_POSIX_API_SUPPORT
}

void read_db_block(DB_SET_INFO_T *p_db_set_info, uint64_t block_tag, DB_BLOCK_T *p_db_block)
{
    assert((block_tag > 0) && (block_tag <= p_db_set_info->db_set_properties.block_num));

    FILE *p_db_set_file = p_db_set_info->file;
    off_t block_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), block_tag);

    // read static variables
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);

    // Alignment may make sizeof and get_db_block_size() different.
    if (sizeof(DB_BLOCK_T) == get_db_block_size())
    {
        pread(fd, p_db_block, sizeof(DB_BLOCK_T), block_offset);
    }
    else
    {
        pread(fd, &(p_db_block->block_tag), sizeof(p_db_block->block_tag), block_offset);
        block_offset += sizeof(p_db_block->block_tag);
        pread(fd, &(p_db_block->data_tag), sizeof(p_db_block->data_tag), block_offset);
        block_offset += sizeof(p_db_block->data_tag);
        pread(fd, &(p_db_block->prev_block_tag), sizeof(p_db_block->prev_block_tag), block_offset);
        block_offset += sizeof(p_db_block->prev_block_tag);
        pread(fd, &(p_db_block->next_block_tag), sizeof(p_db_block->next_block_tag), block_offset);
        block_offset += sizeof(p_db_block->next_block_tag);
        pread(fd, &(p_db_block->created_time), sizeof(p_db_block->created_time), block_offset);
        block_offset += sizeof(p_db_block->created_time);
        pread(fd, &(p_db_block->modified_time), sizeof(p_db_block->modified_time), block_offset);
        block_offset += sizeof(p_db_block->modified_time);
        pread(fd, &(p_db_block->deleted), sizeof(p_db_block->deleted), block_offset);
        block_offset += sizeof(p_db_block->deleted);
        pread(fd, &(p_db_block->valid_record_num), sizeof(p_db_block->valid_record_num), block_offset);
        block_offset += sizeof(p_db_block->valid_record_num);
        pread(fd, &(p_db_block->record_properties_num), sizeof(p_db_block->record_properties_num), block_offset);
        block_offset += sizeof(p_db_block->record_properties_num);
        pread(fd, p_db_block->block_data, sizeof(p_db_block->block_data), block_offset);
    }
#else // IS_POSIX_API_SUPPORT
    fseek(p_db_set_file, block_offset, SEEK_SET);

    if (sizeof(DB_BLOCK_T) == get_db_block_size())
    {
        fread(p_db_block, sizeof(DB_BLOCK_T), 1, p_db_set_file);
    }
    else
    {
        fread(&(p_db_block->block_tag), sizeof(p_db_block->block_tag), 1, p_db_set_file);
        fread(&(p_db_block->data_tag), sizeof(p_db_block->data_tag), 1, p_db_set_file);
        fread(&(p_db_block->prev_block_tag), sizeof(p_db_block->prev_block_tag), 1, p_db_set_file);
        fread(&(p_db_block->next_block_tag), sizeof(p_db_block->next_block_tag), 1, p_db_set_file);
        fread(&(p_db_block->created_time), sizeof(p_db_block->created_time), 1, p_db_set_file);
        fread(&(p_db_block->modified_time), sizeof(p_db_block->modified_time), 1, p_db_set_file);
        fread(&(p_db_block->deleted), sizeof(p_db_block->deleted), 1, p_db_set_file);
        fread(&(p_db_block->valid_record_num), sizeof(p_db_block->valid_record_num), 1, p_db_set_file);
        fread(&(p_db_block->record_properties_num), sizeof(p_db_block->record_properties_num), 1, p_db_set_file);

        fread(p_db_block->block_data, sizeof(p_db_block->block_data), 1, p_db_set_file);
    }
#endif
}

void read_db_block_attributes(DB_SET_INFO_T *p_db_set_info, uint64_t block_tag, DB_BLOCK_T *p_db_block)
{
    assert((block_tag > 0) && (block_tag <= p_db_set_info->db_set_properties.block_num));

    FILE *p_db_set_file = p_db_set_info->file;
    off_t block_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), block_tag);

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);

    pread(fd, &(p_db_block->block_tag), sizeof(p_db_block->block_tag), block_offset);
    block_offset += sizeof(p_db_block->block_tag);
    pread(fd, &(p_db_block->data_tag), sizeof(p_db_block->data_tag), block_offset);
    block_offset += sizeof(p_db_block->data_tag);
    pread(fd, &(p_db_block->prev_block_tag), sizeof(p_db_block->prev_block_tag), block_offset);
    block_offset += sizeof(p_db_block->prev_block_tag);
    pread(fd, &(p_db_block->next_block_tag), sizeof(p_db_block->next_block_tag), block_offset);
    block_offset += sizeof(p_db_block->next_block_tag);
    pread(fd, &(p_db_block->created_time), sizeof(p_db_block->created_time), block_offset);
    block_offset += sizeof(p_db_block->created_time);
    pread(fd, &(p_db_block->modified_time), sizeof(p_db_block->modified_time), block_offset);
    block_offset += sizeof(p_db_block->modified_time);
    pread(fd, &(p_db_block->deleted), sizeof(p_db_block->deleted), block_offset);
    block_offset += sizeof(p_db_block->deleted);
    pread(fd, &(p_db_block->valid_record_num), sizeof(p_db_block->valid_record_num), block_offset);
    block_offset += sizeof(p_db_block->valid_record_num);
    pread(fd, &(p_db_block->record_properties_num), sizeof(p_db_block->record_properties_num), block_offset);
#else  // IS_POSIX_API_SUPPORT
    fseek(p_db_set_file, block_offset, SEEK_SET);

    fread(&(p_db_block->block_tag), sizeof(p_db_block->block_tag), 1, p_db_set_file);
    fread(&(p_db_block->data_tag), sizeof(p_db_block->data_tag), 1, p_db_set_file);
    fread(&(p_db_block->prev_block_tag), sizeof(p_db_block->prev_block_tag), 1, p_db_set_file);
    fread(&(p_db_block->next_block_tag), sizeof(p_db_block->next_block_tag), 1, p_db_set_file);
    fread(&(p_db_block->created_time), sizeof(p_db_block->created_time), 1, p_db_set_file);
    fread(&(p_db_block->modified_time), sizeof(p_db_block->modified_time), 1, p_db_set_file);
    fread(&(p_db_block->deleted), sizeof(p_db_block->deleted), 1, p_db_set_file);
    fread(&(p_db_block->valid_record_num), sizeof(p_db_block->valid_record_num), 1, p_db_set_file);
    fread(&(p_db_block->record_properties_num), sizeof(p_db_block->record_properties_num), 1, p_db_set_file);
#endif // IS_POSIX_API_SUPPORT
}

// traverse backward
void update_db_block_next_block_tag(uint64_t block_tag, uint64_t next_block_tag, DB_SET_INFO_T *p_db_set_info)
{
    DB_BLOCK_T db_block;
    db_block_init(&db_block);

    read_db_block(p_db_set_info, block_tag, &db_block);
    db_block.next_block_tag = next_block_tag;
    // TODO: only write the next_block_tag value
    write_db_block(&db_block, p_db_set_info);

    if (db_block.prev_block_tag != 0)
    {
        update_db_block_next_block_tag(db_block.prev_block_tag, block_tag, p_db_set_info);
    }
}

void extract_db_data_info_from_db_blocks_handler_update_time(DB_DATA_INFO_T *p_db_data_info, DB_BLOCK_T *p_db_block)
{
    if (p_db_block->created_time > p_db_data_info->created_time)
    {
        p_db_data_info->created_time = p_db_block->created_time;
    }

    if (p_db_block->modified_time > p_db_data_info->modified_time)
    {
        p_db_data_info->modified_time = p_db_block->modified_time;
    }
}

void extract_db_data_info_from_db_blocks(DB_DATA_INFO_T *p_db_data_info, uint64_t start_block_tag, DB_SET_INFO_T *p_db_set_info)
{
    DB_BLOCK_T db_block;
    uint32_t record_num = 0;
    DB_RECORD_INFO_T *result = NULL;
    uint8_t *p_block_data = NULL;
    uint8_t *p_block_end_address = NULL;

    db_block_init(&db_block);
    read_db_block(p_db_set_info, start_block_tag, &db_block);

    p_db_data_info->data_tag = db_block.data_tag;
    p_db_data_info->start_db_block_tag = start_block_tag;
    extract_db_data_info_from_db_blocks_handler_update_time(p_db_data_info, &db_block);
    // update record_num at the end of this function.
    p_db_data_info->deleted = db_block.deleted;

    record_num = db_block.valid_record_num;
    result = malloc(record_num * sizeof(DB_RECORD_INFO_T));
    p_block_data = db_block.block_data;
    p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();

    if (result == NULL)
    {
        p_db_data_info->record_num = 0;
        p_db_data_info->p_db_record_info = NULL;
        return;
    }

    for (uint32_t i = 0; i < record_num; i++)
    {
        bool find_valid_db_record = false;
        uint32_t remaining_size = 0;

        db_record_info_init(&(result[i]));

        // check if valid record (deleted == false) and reaches to the valid one.
        while (find_valid_db_record == false)
        {
            if ((p_block_data + get_db_record_properties_size()) > p_block_end_address)
            {
                // clear the current block and read next block from next_block_tag and update the variables.
                int next_block_tag = db_block.next_block_tag;
                assert(next_block_tag != 0);

                db_block_init(&db_block);
                read_db_block(p_db_set_info, next_block_tag, &db_block);
                extract_db_data_info_from_db_blocks_handler_update_time(p_db_data_info, &db_block);
                p_block_data = db_block.block_data;
                p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();
            }

            // check if the record deleted or not.
            if (((DB_RECORD_PROPERTIES_T *)p_block_data)->deleted == 0)
            {
                find_valid_db_record = true;
                break;
            }
            else
            {
                // record was deleted, bypass it.
                uint32_t key_size = ((DB_RECORD_PROPERTIES_T *)p_block_data)->key_size;
                uint32_t value_size = ((DB_RECORD_PROPERTIES_T *)p_block_data)->value_size;

                find_valid_db_record = false;
                p_block_data += get_db_record_properties_size();

                remaining_size = key_size;
                while (remaining_size > 0)
                {
                    uint32_t remaining_block_size = p_block_end_address - p_block_data;
                    uint32_t forward_size = (remaining_block_size > remaining_size) ? (remaining_size) : (remaining_block_size);

                    p_block_data += forward_size;
                    if (forward_size == 0)
                    {
                        // p_block_data reaches the end of the block, load next block and update variables.
                        int next_block_tag = db_block.next_block_tag;
                        assert(next_block_tag != 0);

                        db_block_init(&db_block);
                        read_db_block(p_db_set_info, next_block_tag, &db_block);
                        extract_db_data_info_from_db_blocks_handler_update_time(p_db_data_info, &db_block);
                        p_block_data = db_block.block_data;
                        p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();
                    }
                    remaining_size -= forward_size;
                }

                remaining_size = value_size;
                while (remaining_size > 0)
                {
                    uint32_t remaining_block_size = p_block_end_address - p_block_data;
                    uint32_t forward_size = (remaining_block_size > remaining_size) ? (remaining_size) : (remaining_block_size);

                    p_block_data += forward_size;
                    if (forward_size == 0)
                    {
                        // p_block_data reaches the end of the block, load next block and update variables.
                        int next_block_tag = db_block.next_block_tag;
                        assert(next_block_tag != 0);

                        db_block_init(&db_block);
                        read_db_block(p_db_set_info, next_block_tag, &db_block);
                        extract_db_data_info_from_db_blocks_handler_update_time(p_db_data_info, &db_block);
                        p_block_data = db_block.block_data;
                        p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();
                    }
                    remaining_size -= forward_size;
                }
            }
        }
        // Setting record properties offset and copy db_record_properties from db_block_data.
        result[i].db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), db_block.block_tag) + (p_block_data - ((uint8_t *)&db_block));
        memcpy(&(result[i].db_record_properties), p_block_data, get_db_record_properties_size());
        p_block_data += get_db_record_properties_size();

        // Copy db_record from the db blocks.
        allocate_db_record_resources(&(result[i].db_record), result[i].db_record_properties.key_size, result[i].db_record_properties.value_size);

        // copy db_record key from db block data.
        remaining_size = result[i].db_record_properties.key_size;
        while (remaining_size > 0)
        {
            uint32_t remaining_block_size = p_block_end_address - p_block_data;
            uint32_t copy_size = (remaining_block_size > remaining_size) ? (remaining_size) : (remaining_block_size);
            uint8_t *p_key = NULL;

            if (copy_size == 0)
            {
                // read next block and update variables.
                int next_block_tag = db_block.next_block_tag;
                assert(next_block_tag != 0);

                db_block_init(&db_block);
                read_db_block(p_db_set_info, next_block_tag, &db_block);
                extract_db_data_info_from_db_blocks_handler_update_time(p_db_data_info, &db_block);
                p_block_data = db_block.block_data;
                p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();

                continue;
            }

            p_key = result[i].db_record.p_key + result[i].db_record_properties.key_size - remaining_size;
            memcpy(p_key, p_block_data, copy_size);
            p_block_data += copy_size;
            remaining_size -= copy_size;
        }

        remaining_size = result[i].db_record_properties.value_size;
        while (remaining_size > 0)
        {
            uint32_t remaining_block_size = p_block_end_address - p_block_data;
            uint32_t copy_size = (remaining_block_size > remaining_size) ? (remaining_size) : (remaining_block_size);
            uint8_t *p_value = NULL;

            if (copy_size == 0)
            {
                // read next block and update variables.
                int next_block_tag = db_block.next_block_tag;
                assert(next_block_tag != 0);

                db_block_init(&db_block);
                read_db_block(p_db_set_info, next_block_tag, &db_block);
                extract_db_data_info_from_db_blocks_handler_update_time(p_db_data_info, &db_block);
                p_block_data = db_block.block_data;
                p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();
            }

            p_value = result[i].db_record.p_value + result[i].db_record_properties.value_size - remaining_size;
            memcpy(p_value, p_block_data, copy_size);
            p_block_data += copy_size;
            remaining_size -= copy_size;
        }
    }

    p_db_data_info->p_db_record_info = result;
    p_db_data_info->record_num = record_num;
}

// currently, this function only work for unit test.
DB_RECORD_INFO_T *extract_db_record_info_from_db_blocks(uint64_t block_tag, DB_SET_INFO_T *p_db_set_info, uint32_t *p_record_num)
{
    DB_BLOCK_T db_block;
    uint32_t record_num = 0;
    DB_RECORD_INFO_T *result = NULL;
    uint64_t next_block_tag = 0;
    uint8_t *p_block_data = NULL;
    uint8_t *p_block_end_address = NULL;

    db_block_init(&db_block);
    read_db_block(p_db_set_info, block_tag, &db_block);
    record_num = db_block.valid_record_num;
    result = malloc(record_num * sizeof(DB_RECORD_INFO_T));
    next_block_tag = db_block.next_block_tag;
    p_block_data = db_block.block_data;
    p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();

    if (result == NULL)
    {
        *p_record_num = 0;
        return NULL;
    }

    for (uint32_t i = 0; i < record_num; i++)
    {
        bool find_valid_db_record = false;
        uint32_t remaining_size = 0;

        db_record_info_init(&(result[i]));

        // check if valid record (deleted == false) and reaches to the valid one.
        while (find_valid_db_record == false)
        {
            if ((p_block_data + get_db_record_properties_size()) > p_block_end_address)
            {
                // clear the current block and read next block from next_block_tag and update the variables.
                assert(next_block_tag != 0);

                db_block_init(&db_block);
                read_db_block(p_db_set_info, next_block_tag, &db_block);
                next_block_tag = db_block.next_block_tag;
                p_block_data = db_block.block_data;
                p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();
            }

            // check if the record deleted or not.
            if (((DB_RECORD_PROPERTIES_T *)p_block_data)->deleted == 0)
            {
                find_valid_db_record = true;
                break;
            }
            else
            {
                // record was deleted, bypass it.
                uint32_t key_size = ((DB_RECORD_PROPERTIES_T *)p_block_data)->key_size;
                uint32_t value_size = ((DB_RECORD_PROPERTIES_T *)p_block_data)->value_size;

                find_valid_db_record = false;
                p_block_data += get_db_record_properties_size();

                remaining_size = key_size;
                while (remaining_size > 0)
                {
                    uint32_t remaining_block_size = p_block_end_address - p_block_data;
                    uint32_t forward_size = (remaining_block_size > remaining_size) ? (remaining_size) : (remaining_block_size);

                    p_block_data += forward_size;
                    if (forward_size == 0)
                    {
                        // p_block_data reaches the end of the block, load next block and update variables.
                        assert(next_block_tag != 0);

                        db_block_init(&db_block);
                        read_db_block(p_db_set_info, next_block_tag, &db_block);
                        next_block_tag = db_block.next_block_tag;
                        p_block_data = db_block.block_data;
                        p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();
                    }
                    remaining_size -= forward_size;
                }

                remaining_size = value_size;
                while (remaining_size > 0)
                {
                    uint32_t remaining_block_size = p_block_end_address - p_block_data;
                    uint32_t forward_size = (remaining_block_size > remaining_size) ? (remaining_size) : (remaining_block_size);

                    p_block_data += forward_size;
                    if (forward_size == 0)
                    {
                        // p_block_data reaches the end of the block, load next block and update variables.
                        assert(next_block_tag != 0);

                        db_block_init(&db_block);
                        read_db_block(p_db_set_info, next_block_tag, &db_block);
                        next_block_tag = db_block.next_block_tag;
                        p_block_data = db_block.block_data;
                        p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();
                    }
                    remaining_size -= forward_size;
                }
            }
        }
        // Setting record properties offset and copy db_record_properties from db_block_data.
        result[i].db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), db_block.block_tag) + (p_block_data - ((uint8_t *)&db_block));
        memcpy(&(result[i].db_record_properties), p_block_data, get_db_record_properties_size());
        p_block_data += get_db_record_properties_size();

        // Copy db_record from the db blocks.
        allocate_db_record_resources(&(result[i].db_record), result[i].db_record_properties.key_size, result[i].db_record_properties.value_size);

        // copy db_record key from db block data.
        remaining_size = result[i].db_record_properties.key_size;
        while (remaining_size > 0)
        {
            uint32_t remaining_block_size = p_block_end_address - p_block_data;
            uint32_t copy_size = (remaining_block_size > remaining_size) ? (remaining_size) : (remaining_block_size);
            uint8_t *p_key = NULL;

            if (copy_size == 0)
            {
                // read next block and update variables.
                assert(next_block_tag != 0);

                db_block_init(&db_block);
                read_db_block(p_db_set_info, next_block_tag, &db_block);
                next_block_tag = db_block.next_block_tag;
                p_block_data = db_block.block_data;
                p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();

                continue;
            }

            p_key = result[i].db_record.p_key + result[i].db_record_properties.key_size - remaining_size;
            memcpy(p_key, p_block_data, copy_size);
            p_block_data += copy_size;
            remaining_size -= copy_size;
        }

        remaining_size = result[i].db_record_properties.value_size;
        while (remaining_size > 0)
        {
            uint32_t remaining_block_size = p_block_end_address - p_block_data;
            uint32_t copy_size = (remaining_block_size > remaining_size) ? (remaining_size) : (remaining_block_size);
            uint8_t *p_value = NULL;

            if (copy_size == 0)
            {
                // read next block and update variables.
                assert(next_block_tag != 0);

                db_block_init(&db_block);
                read_db_block(p_db_set_info, next_block_tag, &db_block);
                next_block_tag = db_block.next_block_tag;
                p_block_data = db_block.block_data;
                p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();
            }

            p_value = result[i].db_record.p_value + result[i].db_record_properties.value_size - remaining_size;
            memcpy(p_value, p_block_data, copy_size);
            p_block_data += copy_size;
            remaining_size -= copy_size;
        }
    }

    *p_record_num = record_num;
    return result;
}

void db_record_properties_init(DB_RECORD_PROPERTIES_T *p_db_record_properties)
{
    p_db_record_properties->deleted = 0;
    p_db_record_properties->key_size = 0;
    p_db_record_properties->value_size = 0;
    p_db_record_properties->record_value_type = FACILEDB_RECORD_VALUE_TYPE_INVALID;
}

void copy_db_record_properties(DB_RECORD_PROPERTIES_T *p_dest_db_record_properties, DB_RECORD_PROPERTIES_T *p_src_db_record_properties)
{
    memcpy(p_dest_db_record_properties, p_src_db_record_properties, sizeof(DB_RECORD_PROPERTIES_T));
}

void assign_db_record_properties_to_db_block_data(DB_BLOCK_T *p_db_block, DB_RECORD_INFO_T *p_db_record_info)
{
    uint8_t *p_write = p_db_block->block_data + p_db_record_info->db_record_properties_offset;

    memcpy(p_write, &(p_db_record_info->db_record_properties), sizeof(DB_RECORD_PROPERTIES_T));
}

size_t get_db_record_properties_size()
{
    return sizeof(DB_RECORD_PROPERTIES_T);
}

void db_data_info_init(DB_DATA_INFO_T *p_db_data_info)
{
    p_db_data_info->data_tag = 0;
    p_db_data_info->start_db_block_tag = 0;
    p_db_data_info->created_time = 0;
    p_db_data_info->modified_time = 0;
    p_db_data_info->record_num = 0;
    p_db_data_info->deleted = 0;
    p_db_data_info->p_db_record_info = NULL;
}

void free_db_data_info_resources(DB_DATA_INFO_T *p_db_data_info)
{
    uint32_t data_num = p_db_data_info->record_num;
    for (uint32_t i = 0; i < data_num; i++)
    {
        free_db_record_info_resources(&(p_db_data_info->p_db_record_info[i]));
    }
}

// shallow copy: only copy the first-layer values and pointers of db_record_info.
void shallow_copy_db_data_info(DB_DATA_INFO_T *p_dest_db_data_info, DB_DATA_INFO_T *p_src_db_data_info)
{
    p_dest_db_data_info->data_tag = p_src_db_data_info->data_tag;
    p_dest_db_data_info->start_db_block_tag = p_src_db_data_info->start_db_block_tag;
    p_dest_db_data_info->created_time = p_src_db_data_info->created_time;
    p_dest_db_data_info->modified_time = p_src_db_data_info->modified_time;
    p_dest_db_data_info->record_num = p_src_db_data_info->record_num;
    p_dest_db_data_info->deleted = p_src_db_data_info->deleted;
    p_dest_db_data_info->p_db_record_info = p_src_db_data_info->p_db_record_info;
}

// shallow assign values and pointers. Only allocate memory for different data type and assign their values and pointers.
bool shallow_assign_db_data_info_to_failedb_data(FACILEDB_DATA_T *p_faciledb_data, DB_DATA_INFO_T *p_db_data_info)
{
    p_faciledb_data->record_num = p_db_data_info->record_num;
    p_faciledb_data->p_data_records = calloc(p_db_data_info->record_num, sizeof(FACILEDB_RECORD_T));

    if (p_faciledb_data->p_data_records == NULL)
    {
        return false;
    }

    for (uint32_t i = 0; i < p_db_data_info->record_num; i++)
    {
        shallow_assign_db_record_info_to_faciledb_record(&((p_faciledb_data->p_data_records)[i]), &((p_db_data_info->p_db_record_info)[i]));
    }

    return true;
}

// shallow assign values and pointers. Only allocate memory for different data type and assign their values and pointers.
bool shallow_assign_faciledb_data_to_db_data_info(DB_DATA_INFO_T *p_db_data_info, FACILEDB_DATA_T *p_faciledb_data)
{
    p_db_data_info->record_num = p_faciledb_data->record_num;
    p_db_data_info->p_db_record_info = calloc(p_db_data_info->record_num, sizeof(DB_RECORD_INFO_T));

    if (p_db_data_info->p_db_record_info == NULL)
    {
        return false;
    }

    // assign db_data_info content
    for (uint32_t i = 0; i < p_faciledb_data->record_num; i++)
    {
        shallow_assign_faciledb_record_to_db_record_info(&(p_db_data_info->p_db_record_info[i]), &(p_faciledb_data->p_data_records[i]));
    }

    return true;
}

void db_record_init(DB_RECORD_T *p_db_record)
{
    p_db_record->p_key = NULL;
    p_db_record->p_value = NULL;
}

bool allocate_db_record_resources(DB_RECORD_T *p_db_record, uint32_t key_size, uint32_t value_size)
{
    if (p_db_record->p_key || p_db_record->p_value)
    {
        free_db_record_resources(p_db_record);
    }

    p_db_record->p_key = calloc(1, key_size * sizeof(uint8_t));
    if (p_db_record->p_key == NULL)
    {
        return false;
    }

    p_db_record->p_value = calloc(1, value_size * sizeof(uint8_t));
    if (p_db_record->p_value == NULL)
    {
        free_db_record_resources(p_db_record);
        return false;
    }

    return true;
}

void free_db_record_resources(DB_RECORD_T *p_db_record)
{
    if (p_db_record->p_key != NULL)
    {
        free(p_db_record->p_key);
        p_db_record->p_key = NULL;
    }

    if (p_db_record->p_value != NULL)
    {
        free(p_db_record->p_value);
        p_db_record->p_value = NULL;
    }
}

void copy_db_record(DB_RECORD_T *p_dest_db_record, DB_RECORD_T *p_src_db_record, uint32_t key_size, uint32_t value_size)
{
    assert(p_dest_db_record->p_key != NULL && p_dest_db_record->p_value != NULL);

    memcpy(p_dest_db_record->p_key, p_src_db_record->p_key, key_size);
    memcpy(p_dest_db_record->p_value, p_src_db_record->p_value, value_size);
}

void db_record_info_init(DB_RECORD_INFO_T *p_db_record_info)
{
    db_record_properties_init(&(p_db_record_info->db_record_properties));
    db_record_init(&(p_db_record_info->db_record));
    p_db_record_info->db_record_properties_offset = 0;
}

// Setup key_size and value_size before calling this function.
bool allocate_db_record_info_resources(DB_RECORD_INFO_T *p_db_record_info)
{
    uint32_t key_size = p_db_record_info->db_record_properties.key_size;
    uint32_t value_size = p_db_record_info->db_record_properties.value_size;

    return allocate_db_record_resources(&(p_db_record_info->db_record), key_size, value_size);
}

void free_db_record_info_resources(DB_RECORD_INFO_T *p_db_record_info)
{
    free_db_record_resources(&(p_db_record_info->db_record));
}

bool faciledb_record_to_db_record_info(FACILEDB_RECORD_T *p_faciledb_record, DB_RECORD_INFO_T *p_db_record_info)
{
    DB_RECORD_PROPERTIES_T *p_db_record_properties = &(p_db_record_info->db_record_properties);
    DB_RECORD_T *p_db_record = &(p_db_record_info->db_record);

    p_db_record_properties->deleted = 0;
    p_db_record_properties->key_size = p_faciledb_record->key_size;
    p_db_record_properties->value_size = p_faciledb_record->value_size;
    p_db_record_properties->record_value_type = p_faciledb_record->record_value_type;

    allocate_db_record_resources(p_db_record, p_db_record_properties->key_size, p_db_record_properties->value_size);
    memcpy(p_db_record->p_key, p_faciledb_record->p_key, p_db_record_properties->key_size);
    memcpy(p_db_record->p_value, p_faciledb_record->p_value, p_db_record_properties->value_size);

    return true;
}

// shallow assign values and pointers, would not allocate space for dynamic resources.
void shallow_assign_faciledb_record_to_db_record_info(DB_RECORD_INFO_T *p_db_record_info, FACILEDB_RECORD_T *p_faciledb_record)
{
    DB_RECORD_PROPERTIES_T *p_db_record_properties = &(p_db_record_info->db_record_properties);
    DB_RECORD_T *p_db_record = &(p_db_record_info->db_record);

    p_db_record_properties->deleted = 0;
    p_db_record_properties->key_size = p_faciledb_record->key_size;
    p_db_record_properties->value_size = p_faciledb_record->value_size;
    p_db_record_properties->record_value_type = p_faciledb_record->record_value_type;

    p_db_record->p_key = p_faciledb_record->p_key;
    p_db_record->p_value = p_faciledb_record->p_value;

    p_db_record_info->db_record_properties_offset = 0; // invalid
}

// shallow assgin values and pointers, would not allocate memory for dynamic resources.
void shallow_assign_db_record_info_to_faciledb_record(FACILEDB_RECORD_T *p_faciledb_record, DB_RECORD_INFO_T *p_db_record_info)
{
    p_faciledb_record->key_size = p_db_record_info->db_record_properties.key_size;
    p_faciledb_record->value_size = p_db_record_info->db_record_properties.value_size;
    p_faciledb_record->record_value_type = p_db_record_info->db_record_properties.record_value_type;

    p_faciledb_record->p_key = p_db_record_info->db_record.p_key;
    p_faciledb_record->p_value = p_db_record_info->db_record.p_value;
}

// p_db_data_info is a pointer to a DB_DATA_INFO_T, not a pointer to an array.
// return value: the number of inserted data.
uint32_t insert_db_data(DB_SET_INFO_T *p_db_set_info, DB_DATA_INFO_T *p_db_data_info, uint64_t data_tag)
{
    DB_BLOCK_T new_db_block;
    uint8_t *p_db_block_write = new_db_block.block_data;
    uint8_t *p_db_block_end = new_db_block.block_data + FACILEDB_BLOCK_DATA_SIZE;
    size_t db_record_properties_size = get_db_record_properties_size();
    uint64_t first_db_block_tag = 0;
    uint64_t last_db_block_tag = 0;

    db_block_init(&new_db_block);
    // Set first block prev/next block tag as 0.
    insert_db_data_handler_assign_db_block_value(&new_db_block, 0, p_db_data_info->record_num, data_tag);
    for (uint32_t i = 0; i < (p_db_data_info->record_num); i++)
    {
        uint32_t remaining_size = 0;
        DB_RECORD_INFO_T *p_current_db_record_info = &p_db_data_info->p_db_record_info[i];

        if ((p_db_block_write + db_record_properties_size) > p_db_block_end)
        {
            // new_db_block is full, write new_db_block into file.
            uint64_t write_block_tag = insert_db_data_handler_write_new_db_block(&new_db_block, p_db_set_info);
            if (first_db_block_tag == 0)
            {
                // first block tag is not set, assign it.
                first_db_block_tag = write_block_tag;
            }

            // Clear and reassgin the new_block and local variables for new data.
            db_block_init(&new_db_block);
            insert_db_data_handler_assign_db_block_value(&new_db_block, write_block_tag, p_db_data_info->record_num, data_tag);
            // update local variables.
            p_db_block_write = new_db_block.block_data;
            p_db_block_end = new_db_block.block_data + FACILEDB_BLOCK_DATA_SIZE;
        }

        // copy record properties to block data.
        memcpy(p_db_block_write, &(p_current_db_record_info->db_record_properties), db_record_properties_size);
        new_db_block.record_properties_num++;
        p_db_block_write += db_record_properties_size;

        // copy record key to block data.
        remaining_size = p_current_db_record_info->db_record_properties.key_size;
        while (remaining_size > 0)
        {
            assert(p_db_block_end >= p_db_block_write);

            uint32_t remaining_block_size = (p_db_block_end - p_db_block_write);
            uint32_t copy_size = (remaining_block_size < remaining_size) ? (remaining_block_size) : (remaining_size);
            uint8_t *p_key = NULL;

            if (copy_size == 0)
            {
                // Current db_block is full, write db_block into file.
                uint64_t write_block_tag = insert_db_data_handler_write_new_db_block(&new_db_block, p_db_set_info);
                if (first_db_block_tag == 0)
                {
                    first_db_block_tag = write_block_tag;
                }

                // Clear and reassign the current block data and local variables for new data.
                db_block_init(&new_db_block);
                insert_db_data_handler_assign_db_block_value(&new_db_block, write_block_tag, p_db_data_info->record_num, data_tag);
                p_db_block_write = new_db_block.block_data;
                p_db_block_end = new_db_block.block_data + FACILEDB_BLOCK_DATA_SIZE;

                continue;
            }

            p_key = p_current_db_record_info->db_record.p_key + p_current_db_record_info->db_record_properties.key_size - remaining_size;
            memcpy(p_db_block_write, p_key, copy_size);
            p_db_block_write += copy_size;
            remaining_size -= copy_size;
        }

        // copy record value to block data
        remaining_size = p_current_db_record_info->db_record_properties.value_size;
        while (remaining_size > 0)
        {
            assert(p_db_block_end >= p_db_block_write);

            uint32_t remaining_block_size = p_db_block_end - p_db_block_write;
            uint32_t copy_size = (remaining_block_size < remaining_size) ? (remaining_block_size) : (remaining_size);
            uint8_t *p_value = NULL;

            if (copy_size == 0)
            {
                // block is full, write into file and clear the current block and pointer for new data.
                uint64_t write_block_tag = insert_db_data_handler_write_new_db_block(&new_db_block, p_db_set_info);
                if (first_db_block_tag == 0)
                {
                    first_db_block_tag = write_block_tag;
                }

                // Clear and reassign the current block data and local variables for new data.
                db_block_init(&new_db_block);
                insert_db_data_handler_assign_db_block_value(&new_db_block, write_block_tag, p_db_data_info->record_num, data_tag);
                // update local variables / pointers
                p_db_block_write = new_db_block.block_data;
                p_db_block_end = new_db_block.block_data + FACILEDB_BLOCK_DATA_SIZE;

                continue;
            }

            p_value = p_current_db_record_info->db_record.p_value + p_current_db_record_info->db_record_properties.value_size - remaining_size;
            memcpy(p_db_block_write, p_value, copy_size);
            p_db_block_write += copy_size;
            remaining_size -= copy_size;
        }
    }

    // write the last db_block
    last_db_block_tag = insert_db_data_handler_write_new_db_block(&new_db_block, p_db_set_info);
    if (first_db_block_tag == 0)
    {
        first_db_block_tag = last_db_block_tag;
    }

    // recurssively update the next block tag of each db blocks.
    update_db_block_next_block_tag(last_db_block_tag, 0, p_db_set_info);

#if ENABLE_DB_INDEX
    // insert index if existed
    for (uint32_t i = 0; i < (p_db_data_info->record_num); i++)
    {
        DB_RECORD_INFO_T *p_current_db_record_info = &p_db_data_info->p_db_record_info[i];
        char *p_index_key = set_db_index_key(p_db_set_info->db_set_properties.p_set_name, p_db_set_info->db_set_properties.set_name_size, p_current_db_record_info->db_record.p_key, p_current_db_record_info->db_record_properties.key_size);
        DB_INDEX_PAYLOAD_T db_index_payload = {
            .data_tag = data_tag,
            .start_db_block_tag = first_db_block_tag};

        // If p_key index has been created, insert new index element.
        if (Index_Api_Index_Key_Exist(p_index_key))
        {
            insert_db_record_index(p_db_set_info, p_current_db_record_info, &db_index_payload);
        }

        free(p_index_key);
    }
#endif

    // free db block resources if needed.
    // currently there is no dynamic resources in db block structure.

    // return the number of inserted data.
    return 1;
}

// return value: new block_tag
uint64_t insert_db_data_handler_write_new_db_block(DB_BLOCK_T *p_db_block, DB_SET_INFO_T *p_db_set_info)
{
    uint64_t block_tag = 0;
    uint64_t current_time = 0;

    p_db_set_info->db_set_properties.block_num++;
    block_tag = p_db_set_info->db_set_properties.block_num;
    p_db_block->block_tag = block_tag;

    current_time = (uint64_t)get_current_time();
    p_db_block->created_time = current_time;
    p_db_block->modified_time = current_time;
    // update db_set_properties time records
    p_db_set_info->db_set_properties.modified_time = current_time;

    write_db_block(p_db_block, p_db_set_info);

    return block_tag;
}

void insert_db_data_handler_assign_db_block_value(DB_BLOCK_T *p_db_block, uint64_t prev_block_tag, uint32_t valid_record_num, uint64_t data_tag)
{
    p_db_block->data_tag = data_tag;
    p_db_block->prev_block_tag = prev_block_tag;
    p_db_block->next_block_tag = 0;
    p_db_block->deleted = 0;
    p_db_block->record_properties_num = 0;
    p_db_block->valid_record_num = valid_record_num;
}

// return value: DB_DATA_INFO_T array whose length is *p_result_db_data_info_num
DB_DATA_INFO_T *search_db_data(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, uint32_t *p_result_db_data_info_num)
{
#if ENABLE_DB_INDEX
    // check if index existed and call search_db_data_indexed.
    char *p_index_key = set_db_index_key(p_db_set_info->db_set_properties.p_set_name, p_db_set_info->db_set_properties.set_name_size, p_target_db_record_info->db_record.p_key, p_target_db_record_info->db_record_properties.key_size);
    if (Index_Api_Index_Key_Exist(p_index_key))
    {
        free(p_index_key);
        return search_db_data_indexed(p_db_set_info, p_target_db_record_info, compare_type, p_result_db_data_info_num);
    }
    else
    {
        free(p_index_key);
    }
#endif
    // General sequential search
    return search_db_data_sequential(p_db_set_info, p_target_db_record_info, compare_type, p_result_db_data_info_num);
}

// General sequential search
DB_DATA_INFO_T *search_db_data_sequential(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, uint32_t *p_result_db_data_info_num)
{
    uint64_t block_num = p_db_set_info->db_set_properties.block_num;

    void *p_target_key = p_target_db_record_info->db_record.p_key;
    uint32_t target_key_size = p_target_db_record_info->db_record_properties.key_size;
    void *p_target_value = p_target_db_record_info->db_record.p_value;
    FACILEDB_RECORD_VALUE_TYPE_E target_value_type = p_target_db_record_info->db_record_properties.record_value_type;

    DB_DATA_INFO_T *p_result_db_data_infos = malloc(DB_SEARCH_DATA_INFO_BUFFER_LEN * sizeof(DB_DATA_INFO_T));
    uint32_t result_db_data_infos_buffer_len = DB_SEARCH_DATA_INFO_BUFFER_LEN;
    uint32_t result_db_data_info_num = 0;

    if (p_result_db_data_infos == NULL)
    {
        *p_result_db_data_info_num = 0;
        return NULL;
    }

    for (uint64_t block_tag = 1; block_tag <= block_num; block_tag++)
    {
        DB_DATA_INFO_T db_data_info;
        DB_BLOCK_T db_block;
        bool record_match = false;

        db_data_info_init(&db_data_info);
        db_block_init(&db_block);

        // read attribute only for checking delete flag and first block flag.
        read_db_block_attributes(p_db_set_info, block_tag, &db_block);

        if (db_block.deleted || db_block.prev_block_tag != 0)
        {
            continue;
        }

        // Read the whole block and next blocks if they exists. The buffers will be allocated, and the record content will be copied into the record_info
        extract_db_data_info_from_db_blocks(&db_data_info, block_tag, p_db_set_info);

        // Search if the target record matched or not.
        for (uint32_t record_idx = 0; record_idx < db_data_info.record_num; record_idx++)
        {
            if (target_key_size == db_data_info.p_db_record_info[record_idx].db_record_properties.key_size &&
                memcmp(db_data_info.p_db_record_info[record_idx].db_record.p_key, p_target_key, target_key_size) == 0 &&
                target_value_type == db_data_info.p_db_record_info[record_idx].db_record_properties.record_value_type &&
                // value size comparison doesn't need (?)
                (compare_type == FACILEDB_RECORD_VALUE_TYPE_COMPARE_ANY || Faciledb_Record_Value_Type_Compare(target_value_type, db_data_info.p_db_record_info[record_idx].db_record.p_value, p_target_value) == compare_type))
            {
                record_match = true;
                break;
            }
        }

        // Copy the matched key and value to p_result_db_data_infos array.
        if (record_match)
        {
            // Check if buffer length enough to store new matched data.
            if (result_db_data_info_num == result_db_data_infos_buffer_len)
            {
                DB_DATA_INFO_T *tmp = NULL;

                // TODO: buffer size optimization
                result_db_data_infos_buffer_len *= 2;
                tmp = realloc(p_result_db_data_infos, result_db_data_infos_buffer_len * sizeof(DB_DATA_INFO_T));
                if (tmp == NULL)
                {
                    // Not enough memory
                    // Return current results even if there are more matches data.
                    // TODO: error handling
                    break;
                }
                p_result_db_data_infos = tmp;
            }

            db_data_info_init(&(p_result_db_data_infos[result_db_data_info_num]));
            shallow_copy_db_data_info(&(p_result_db_data_infos[result_db_data_info_num]), &db_data_info);
            result_db_data_info_num++;

            // Do not free db_data_info here, because the dynamic resources are still used in p_result_db_data_infos.
        }
        else
        {
            free_db_data_info_resources(&db_data_info);
        }
    }

    *p_result_db_data_info_num = result_db_data_info_num;
    return p_result_db_data_infos;
}

void delete_db_data_handler_write_delete_flag(DB_SET_INFO_T *p_db_set_info, uint64_t db_block_tag, uint32_t deleted)
{
    FILE *p_db_set_file = p_db_set_info->file;
    DB_SET_PROPERTIES_T *p_db_set_properties = &(p_db_set_info->db_set_properties);
    off_t delete_flag_offset = get_db_block_offset(p_db_set_properties, db_block_tag) + offsetof(DB_BLOCK_T, deleted);
    uint64_t current_time = (uint64_t)get_current_time();
    off_t modified_time_offset = get_db_block_offset(p_db_set_properties, db_block_tag) + offsetof(DB_BLOCK_T, modified_time);

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);

    // write delete flag
    pwrite(fd, &deleted, sizeof(deleted), delete_flag_offset);
    // write modified time
    pwrite(fd, &current_time, sizeof(current_time), modified_time_offset);
#else  // IS_POSIX_API_SUPPORT
    fseek(p_db_set_file, delete_flag_offset, SEEK_SET);
    fwrite(&deleted, sizeof(deleted), 1, p_db_set_file);

    fseek(p_db_set_file, modified_time_offset, SEEK_SET);
    fwrite(&current_time, sizeof(current_time), 1, p_db_set_file);
#endif // IS_POSIX_API_SUPPORT
}

void delete_db_data(DB_SET_INFO_T *p_db_set_info, DB_DATA_INFO_T *p_db_data_info, uint32_t db_data_num)
{
    DB_BLOCK_T db_block;
    uint64_t block_tag = 0;
    db_block_init(&db_block);

    for (uint32_t i = 0; i < db_data_num; i++)
    {
        block_tag = p_db_data_info[i].start_db_block_tag;

        while (block_tag != 0)
        {
            read_db_block(p_db_set_info, block_tag, &db_block);
            delete_db_data_handler_write_delete_flag(p_db_set_info, db_block.block_tag, 1);
            // continue to the next block.
            block_tag = db_block.next_block_tag;
        }
    }
}

#if ENABLE_DB_INDEX
bool FacileDB_Api_Make_Record_Index(char *p_db_set_name, FACILEDB_RECORD_T *p_faciledb_record)
{
    DB_SET_INFO_T *p_db_set_info = NULL;
    DB_RECORD_INFO_T target_db_record;
    uint32_t result_data_num = 0;

    if (p_faciledb_record == NULL)
    {
        // invalid input
        return false;
    }

    lock_db_context_sync();
    if (check_db_context_status(DB_CONTEXT_STATUS_READY) == false)
    {
        // db context is not ready
        unlock_db_context_sync();
        return false;
    }

    p_db_set_info = load_and_lock_db_set_info(p_db_set_name);
    unlock_db_context_sync();

    db_record_info_init(&target_db_record);
    shallow_assign_faciledb_record_to_db_record_info(&target_db_record, p_faciledb_record);

    db_set_info_sync_read_wait(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READING);
    db_set_info_file_lock_read(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    result_data_num = make_db_record_index(p_db_set_info, &target_db_record);

    lock_db_set_info_sync(p_db_set_info);
    db_set_info_file_unlock_read(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY);
    db_set_info_sync_read_unblock(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    if (result_data_num > 0)
    {
        return true;
    }
    else
    {
        return false;
    }
}

// This function must be called after setting db_directory_path.
bool get_db_index_directory_path(char *p_db_index_directory_path)
{
    const char index_directory_suffix[] = "index/";

    if ((strlen(db_directory_path) + strlen(index_directory_suffix)) > INDEX_FILE_PATH_MAX_LENGTH)
    {
        p_db_index_directory_path[0] = '\0';
        return false;
    }

    strncpy(p_db_index_directory_path, db_directory_path, INDEX_FILE_PATH_MAX_LENGTH);
    strcat(p_db_index_directory_path, index_directory_suffix);
    p_db_index_directory_path[INDEX_FILE_PATH_MAX_LENGTH] = '\0';

    return true;
}

// index key defined as the file that contains the index data.
// Default index key is db_set_name + "_" + p_key (DB_RECORD_T).
// TODO: db_set_name is a fake string that doesn't contain '\0' at the end, and p_key is not expected to be a string all the time.
// TODO: to_printable or toString
char *set_db_index_key(void *db_set_name, uint32_t set_name_size, void *p_key, uint32_t key_size)
{
    // TODO: toString(p_key)
    char *p_db_index_key = calloc(set_name_size + key_size + 2, sizeof(char));

    if (p_db_index_key == NULL)
    {
        return NULL;
    }

    memcpy(p_db_index_key, db_set_name, set_name_size);
    p_db_index_key[set_name_size] = '_';
    memcpy(p_db_index_key + set_name_size + 1, p_key, key_size);

    p_db_index_key[set_name_size + key_size + 1] = '\0';

    return p_db_index_key;
}

INDEX_ID_TYPE_E get_db_index_id_type(FACILEDB_RECORD_VALUE_TYPE_E record_value_type)
{
    switch (record_value_type)
    {
    case FACILEDB_RECORD_VALUE_TYPE_UINT32:
        return INDEX_ID_TYPE_UINT32;
    case FACILEDB_RECORD_VALUE_TYPE_STRING:
        return INDEX_ID_TYPE_HASH;
    default:
        return INDEX_ID_TYPE_INVALID;
    }
}

uint32_t make_db_record_index(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_db_record_info)
{
    char *p_index_key = NULL;
    // array of db_data_info
    DB_DATA_INFO_T *p_db_result_data = NULL;
    uint32_t result_data_num = 0;

    // check if index existed.
    p_index_key = set_db_index_key(p_db_set_info->db_set_properties.p_set_name, p_db_set_info->db_set_properties.set_name_size, p_db_record_info->db_record.p_key, p_db_record_info->db_record_properties.key_size);
    if (!Index_Api_Index_Key_Exist(p_index_key))
    {
        // search for all matched db_records
        p_db_result_data = search_db_data(p_db_set_info, p_db_record_info, FACILEDB_RECORD_VALUE_TYPE_COMPARE_ANY, &result_data_num);

        for (uint32_t i = 0; i < result_data_num; i++)
        {
            for (uint32_t j = 0; j < p_db_result_data[i].record_num; j++)
            {
                if ((p_db_result_data[i].p_db_record_info[j].db_record_properties.key_size == p_db_record_info->db_record_properties.key_size) &&
                    (memcmp(p_db_result_data[i].p_db_record_info[j].db_record.p_key, p_db_record_info->db_record.p_key, p_db_record_info->db_record_properties.key_size) == 0) &&
                    (p_db_result_data[i].p_db_record_info[j].db_record_properties.record_value_type == p_db_record_info->db_record_properties.record_value_type))
                {
                    DB_INDEX_PAYLOAD_T db_index_payload = {
                        .data_tag = p_db_result_data[i].data_tag,
                        .start_db_block_tag = p_db_result_data[i].start_db_block_tag};
                    insert_db_record_index(p_db_set_info, &(p_db_result_data[i].p_db_record_info[j]), &db_index_payload);

                    break;
                }
            }

            // free resource
            free_db_data_info_resources(&(p_db_result_data[i]));
        }
    }

    free(p_index_key);
    free(p_db_result_data);

    return result_data_num;
}

void insert_db_record_index(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_db_record_info, DB_INDEX_PAYLOAD_T *p_db_index_payload)
{
    // TODO: toString(p_set_name) and toString(p_key)
    char *p_index_key = set_db_index_key(p_db_set_info->db_set_properties.p_set_name, p_db_set_info->db_set_properties.set_name_size, p_db_record_info->db_record.p_key, p_db_record_info->db_record_properties.key_size);
    void *p_record_value = p_db_record_info->db_record.p_value;
    void *p_index_id = NULL;
    INDEX_ID_TYPE_E index_id_type = get_db_index_id_type(p_db_record_info->db_record_properties.record_value_type);
    HASH_VALUE_T hash_value = 0;

    if (index_id_type != INDEX_ID_TYPE_INVALID)
    {
        // Setup p_index_id based on the index_id_type.
        if (index_id_type == INDEX_ID_TYPE_HASH)
        {
            // hash the value
            hash_value = Hash(p_record_value, p_db_record_info->db_record_properties.value_size);
            p_index_id = &hash_value;
        }
        else
        {
            p_index_id = p_record_value;
        }

        /*
        **  p_index_key: p_db_set_name + p_key (DB_RECORD_T)
        **  p_index_id: p_value (DB_RECORD_T)
        **  index_id_type: type (uint32 / string / ...)
        **  p_index_payload: a structure with the data offset / data tag / data start block tag / ...
        **  payload_size: sizeof the payload
        **  return value: pointer of the payload array, size of each element size is INDEX_PAYLOAD_SIZE
        */
        Index_Api_Insert_Element(p_index_key, p_index_id, index_id_type, p_db_index_payload, sizeof(DB_INDEX_PAYLOAD_T));
    }

    free(p_index_key);
}

// return value: an array of DB_DATA_INFO_T, whose length is *p_result_db_data_info_num.
DB_DATA_INFO_T *search_db_data_indexed(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, uint32_t *p_result_db_data_info_num)
{
    char *p_index_key = set_db_index_key(p_db_set_info->db_set_properties.p_set_name, p_db_set_info->db_set_properties.set_name_size, p_target_db_record_info->db_record.p_key, p_target_db_record_info->db_record_properties.key_size);
    void *p_record_value = p_target_db_record_info->db_record.p_value;
    void *p_index_id = NULL;
    HASH_VALUE_T hash_value = 0;
    INDEX_ID_TYPE_E index_id_type = get_db_index_id_type(p_target_db_record_info->db_record_properties.record_value_type);
    DB_INDEX_PAYLOAD_T *p_result_index_payloads = NULL;
    DB_DATA_INFO_T *p_result_db_data_infos = NULL;
    uint32_t result_length = 0;
    uint32_t match_length = 0;

    if (index_id_type != INDEX_ID_TYPE_INVALID)
    {
        // Setup p_index_id based on the index_id_type.
        if (index_id_type == INDEX_ID_TYPE_HASH)
        {
            // hash the value
            hash_value = Hash(p_record_value, p_target_db_record_info->db_record_properties.value_size);
            p_index_id = &hash_value;
        }
        else
        {
            p_index_id = p_record_value;
        }

        // if(compare_type == FACILEDB_RECORD_VALUE_TYPE_COMPARE_ANY)
        // {
        //     // TODO: compare_any, aka all value
        // }
        if (compare_type == FACILEDB_RECORD_VALUE_TYPE_COMPARE_EQUAL)
        {
            p_result_index_payloads = (DB_INDEX_PAYLOAD_T *)Index_Api_Search_Equal(p_index_key, p_index_id, index_id_type, &result_length);
        }

        // Transfer db_index_payloads to db_data_infos
        if (result_length > 0)
        {
            p_result_db_data_infos = calloc(result_length, sizeof(DB_DATA_INFO_T));

            for (uint32_t i = 0; i < result_length; i++)
            {
                DB_BLOCK_T db_block;
                DB_DATA_INFO_T read_db_data_info;
                bool record_match = false;

                db_data_info_init(&read_db_data_info);
                db_block_init(&db_block);

                // read attribute only for checking delete flag and first block flag.
                read_db_block_attributes(p_db_set_info, p_result_index_payloads[i].start_db_block_tag, &db_block);

                if (db_block.deleted || db_block.prev_block_tag != 0)
                {
                    continue;
                }

                // Read the whole block and next blocks if they exists. The buffers will be allocated, and the record content will be copied into the record_info
                extract_db_data_info_from_db_blocks(&read_db_data_info, p_result_index_payloads[i].start_db_block_tag, p_db_set_info);

                // Compare again to prevent collision.
                for (uint32_t record_idx = 0; record_idx < read_db_data_info.record_num; record_idx++)
                {
                    if (p_target_db_record_info->db_record_properties.key_size == read_db_data_info.p_db_record_info[record_idx].db_record_properties.key_size &&
                        memcmp(read_db_data_info.p_db_record_info[record_idx].db_record.p_key, p_target_db_record_info->db_record.p_key, p_target_db_record_info->db_record_properties.key_size) == 0 &&
                        p_target_db_record_info->db_record_properties.record_value_type == read_db_data_info.p_db_record_info[record_idx].db_record_properties.record_value_type &&
                        // value size comparison doesn't need (?)
                        (compare_type == FACILEDB_RECORD_VALUE_TYPE_COMPARE_ANY || Faciledb_Record_Value_Type_Compare(p_target_db_record_info->db_record_properties.record_value_type, read_db_data_info.p_db_record_info[record_idx].db_record.p_value, p_target_db_record_info->db_record.p_value) == compare_type))
                    {
                        record_match = true;
                        break;
                    }
                }

                if (record_match)
                {
                    db_data_info_init(&(p_result_db_data_infos[match_length]));
                    shallow_copy_db_data_info(&(p_result_db_data_infos[match_length]), &read_db_data_info);
                    match_length++;

                    // Because the data info resources still in-used for result, don't free data info resources here.
                }
                else
                {
                    free_db_data_info_resources(&read_db_data_info);
                }
            }

            free(p_result_index_payloads);
            result_length = 0;
        }
    }

    free(p_index_key);
    *p_result_db_data_info_num = match_length;
    return p_result_db_data_infos;
}
#endif // ENABLE_DB_INDEX
