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

#include "faciledb_utils.h"
#include "mema.h"
#include "faciledb_record_value_type.h"
#include "crc.h"
#include "faciledb_internal.h"

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

#ifndef DB_SET_PROPERTIES_NUM
#define DB_SET_PROPERTIES_NUM (2)
#endif

#ifndef DB_SEARCH_DATA_INFO_BUFFER_LEN
#define DB_SEARCH_DATA_INFO_BUFFER_LEN (8)
#endif // DB_SEARCH_DATA_INFO_BUFFER_LEN

#define DB_FILE_OPEN_CHECK_TIMEOUT (30)
#define DB_FILE_OPEN_CHECK_INTERVAL_US (100000) // 100ms

// Structure definition
typedef struct
{
    DB_SET_PROPERTIES_T db_set_properties_list[DB_SET_PROPERTIES_NUM];

    uint32_t set_name_size;
    void *p_set_name;
} DB_SET_PROPERTIES_REGION_T;

typedef struct
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t mutex;
#endif
} DB_CONTEXT_SYNC_T;

typedef struct
{
    DB_CONTEXT_STATUS_E status;
    DB_CONTEXT_SYNC_T sync;
    DB_SET_INFO_T *p_db_set_info_instances_list; // TODO
    char db_directory_path[FACILEDB_FILE_PATH_BUFFER_LENGTH];
} DB_CONTEXT_T;
// End of structure definition

// static variables
static DB_CONTEXT_T db_context = {
    .status = DB_CONTEXT_STATUS_UNUSED,
    .sync = {
#if IS_POSIX_API_SUPPORT
        .mutex = PTHREAD_MUTEX_INITIALIZER,
#endif
    },
    .db_directory_path = {0}};

static DB_SET_INFO_T db_set_info_instance[DB_SET_INFO_INSTANCE_NUM] = {
    {.status = DB_SET_INFO_STATUS_RELEASED,
     .db_set_info_sync = {
#if IS_POSIX_API_SUPPORT
         .mutex = PTHREAD_MUTEX_INITIALIZER,
         .read_cond = PTHREAD_COND_INITIALIZER,
         .write_cond = PTHREAD_COND_INITIALIZER,
         .close_cond = PTHREAD_COND_INITIALIZER,
#endif
         .reader_waiting_count = 0,
         .writer_waiting_count = 0,
         .reader_count = 0}}};
// End of static vaiables

// Local function declaration
void db_set_info_instances_init();
DB_SET_INFO_T *request_and_lock_released_db_set_info_instance();
DB_SET_INFO_T *query_and_lock_db_set_info_loaded(char *p_db_set_name_string);
bool load_and_lock_db_set_info_handler_check_and_deep_copy_set_name(DB_SET_INFO_T *p_db_set_info, uint8_t *p_set_name, uint32_t set_name_size);
void create_new_db_set_file_format(DB_SET_INFO_T *p_db_set_info, uint8_t *p_db_set_name, uint32_t db_set_name_size);
bool read_and_check_db_set_file_format(DB_SET_INFO_T *p_db_set_info);

void db_set_info_init(DB_SET_INFO_T *p_db_set_info);
void allocate_db_set_info_resources(DB_SET_INFO_T *p_db_set_info);
void free_db_set_info_resources(DB_SET_INFO_T *p_db_set_info);
// void close_db_set_info(DB_SET_INFO_T *p_db_set_info);
void db_set_info_sync_init(DB_SET_INFO_SYNC_T *p_db_set_info_sync);
static inline void db_set_info_sync_close_wait(DB_SET_INFO_T *p_db_set_info);
static inline bool check_db_set_info_status(DB_SET_INFO_T *p_db_set_info, DB_SET_INFO_STATUS_E target_status);

void db_set_properties_init(DB_SET_PROPERTIES_T *p_db_set_properties);
void db_set_properties_region_init(DB_SET_PROPERTIES_REGION_T *p_db_set_properties_region);
static inline size_t get_db_set_properties_size();
static inline size_t get_db_set_properties_region_size(DB_SET_INFO_T *p_db_set_info);
void write_db_set_properties(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_T *p_db_set_properties, uint32_t db_set_properties_index);
void write_raw_db_set_properties(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_T *p_db_set_properties, uint32_t db_set_properties_index);
void write_db_set_properties_region(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_REGION_T *p_db_set_properties_region);
uint32_t read_db_set_properties(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_T *p_db_set_properties, uint32_t db_set_properties_index);
uint32_t get_earlist_db_set_properties_index(DB_SET_INFO_T *p_db_set_info);
bool get_latest_db_set_properties(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_T *p_db_set_properties);
bool get_previous_db_set_properties(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_T *p_db_set_properties, uint64_t seq_num);
bool check_db_set_name(DB_SET_INFO_T *p_db_set_info);

off_t get_db_block_offset(DB_SET_INFO_T *p_db_set_info, uint64_t block_tag);
size_t get_db_block_size();

void db_sys_record_init(DB_SYS_RECORD_T *p_sys_record);
uint32_t load_db_sys_data_info_list(DB_SET_INFO_T *p_db_set_info);
uint32_t extract_db_sys_data_info_list_from_db_blocks(DB_SET_INFO_T *p_db_set_info, uint64_t start_block_tag);
void merge_sorted_delete_sys_info_list(DB_SYS_DATA_INFO_LIST_ENTRY_T *p_list_dest, DB_SYS_DATA_INFO_LIST_ENTRY_T *p_list_src);
void merge_sorted_index_checkpoint_sys_info_list(DB_SYS_DATA_INFO_LIST_ENTRY_T *p_list_dest, DB_SYS_DATA_INFO_LIST_ENTRY_T *p_list_src);

bool allocate_db_record_info_resources(DB_RECORD_INFO_T *p_db_record_info);
void free_db_record_info_resources(DB_RECORD_INFO_T *p_db_record_info);
bool faciledb_record_to_db_record_info(FACILEDB_RECORD_T *p_faciledb_record, DB_RECORD_INFO_T *p_db_record_info);
void shallow_assign_db_record_info_to_faciledb_record(FACILEDB_RECORD_T *p_faciledb_record, DB_RECORD_INFO_T *p_db_record_info);

void shallow_copy_db_data_info(DB_DATA_INFO_T *p_dest_db_data_info, DB_DATA_INFO_T *p_src_db_data_info);
bool check_last_db_data_info(DB_SET_INFO_T *p_db_set_info);
// End of local function declaration

void lock_db_context_sync()
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_db_context_mutex = &(db_context.sync.mutex);
    pthread_mutex_lock(p_db_context_mutex);
#endif
}

void unlock_db_context_sync()
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_db_context_mutex = &(db_context.sync.mutex);
    pthread_mutex_unlock(p_db_context_mutex);
#endif
}

void update_db_context_status(DB_CONTEXT_STATUS_E new_status)
{
    DB_CONTEXT_STATUS_E *p_db_context_sync_status = &(db_context.status);
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
bool check_db_context_status(DB_CONTEXT_STATUS_E target_status)
{
    return (db_context.status == target_status);
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

    strncpy(db_context.db_directory_path, temp_db_directory_path, FACILEDB_FILE_PATH_MAX_LENGTH);
    db_context.db_directory_path[FACILEDB_FILE_PATH_MAX_LENGTH] = '\0';

    return true;
}

void clear_db_directory_path()
{
    memset(db_context.db_directory_path, '\0', sizeof(db_context.db_directory_path));
}

void db_set_info_instances_init()
{
    for (uint32_t i = 0; i < DB_SET_INFO_INSTANCE_NUM; i++)
    {
        DB_SET_INFO_T *p_db_set_info = &(db_set_info_instance[i]);
#if IS_POSIX_API_SUPPORT
        pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.mutex);
        pthread_mutex_lock(p_mutex);
#endif

        db_set_info_init(&(db_set_info_instance[i]));

        update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_RELEASED);
#if IS_POSIX_API_SUPPORT
        pthread_mutex_unlock(p_mutex);
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
        // close_db_set_info(p_db_set_info);
        free_db_set_info_resources(p_db_set_info);
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
        (p_target_db_set_info->set_name_size == db_set_name_string_size) &&
        (memcmp(p_target_db_set_info->p_set_name, p_db_set_name_string, db_set_name_string_size) == 0))
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
    DB_SET_PROPERTIES_REGION_T new_region;

    // Update to db_set_info.
    // store the db_set_name without the '\0'.
    p_db_set_info->set_name_size = db_set_name_size;
    allocate_db_set_info_resources(p_db_set_info);
    memcpy(p_db_set_info->p_set_name, p_db_set_name, db_set_name_size);
    p_db_set_info->db_set_properties.created_time = current_time;
    p_db_set_info->db_set_properties.modified_time = current_time;
    p_db_set_info->db_set_properties.seq_num = 1; // set first db_set_properties.seq_num as 1
    p_db_set_info->db_set_properties.latest_sys_block_tag = 0;

    // assign value to db_set_properties_region
    db_set_properties_region_init(&new_region);
    memcpy(&(new_region.db_set_properties_list[0]), &(p_db_set_info->db_set_properties), sizeof(DB_SET_PROPERTIES_T));
    // shallow assign p_set_name
    new_region.set_name_size = db_set_name_size;
    new_region.p_set_name = p_db_set_info->p_set_name;

    write_db_set_properties_region(p_db_set_info, &new_region);
}

bool read_and_check_db_set_file_format(DB_SET_INFO_T *p_db_set_info)
{
    size_t expected_db_set_properties_region_size = get_db_set_properties_region_size(p_db_set_info);
    uint64_t temp_seq_num = UINT64_MAX;
    bool valid_format = false;

#if IS_POSIX_API_SUPPORT
    // read db properties from file and check set_name_size & set_name
    int fd = fileno(p_db_set_info->file);
    off_t file_size = lseek(fd, 0, SEEK_END);

    if (file_size < expected_db_set_properties_region_size)
    {
        return false;
    }
#endif

    // check db_set_name is equal or not.
    if (check_db_set_name(p_db_set_info) == false)
    {
        return false;
    }

    for (uint32_t i = 0; i < DB_SET_PROPERTIES_NUM; i++)
    {
        db_set_properties_init(&(p_db_set_info->db_set_properties));
        if (i == 0)
        {
            if (get_latest_db_set_properties(p_db_set_info, &(p_db_set_info->db_set_properties)) == false)
            {
                // no valid db_set_properties
                break;
            }
        }
        else
        {
            if (get_previous_db_set_properties(p_db_set_info, &(p_db_set_info->db_set_properties), temp_seq_num) == false)
            {
                // no other valid db_set_properties (expcept the latest one, but the db_block check fail using the latest one)
                break;
            }
        }
        temp_seq_num = p_db_set_info->db_set_properties.seq_num;

        // check the latest db data valid or not.
        if (check_last_db_data_info(p_db_set_info) == true)
        {
            valid_format = true;
            break;
        }
    }

    if (valid_format == false)
    {
        // clear db set properties
        db_set_properties_init(&(p_db_set_info->db_set_properties));
    }
    return valid_format;
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

            // close(fd);
            // unlock_db_set_info_sync(p_db_set_info);
            // return NULL;
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
        }

        // Check the db_set_name. If valid, assign db_set_name and set_name size into db_set_info.
        if (load_and_lock_db_set_info_handler_check_and_deep_copy_set_name(p_db_set_info, (uint8_t *)p_db_set_name, strlen(p_db_set_name)) == false)
        {
            // db_set_name format error
            // TODO: need review the following error handling flow.
            // close_db_set_info(p_db_set_info);
            free_db_set_info_resources(p_db_set_info);
            unlock_db_set_info_sync(p_db_set_info);
            return NULL;
        }

        // Try to read db_properties and check if db_properties writed done
        for (uint32_t check_time = 0; check_time < DB_FILE_OPEN_CHECK_TIMEOUT; check_time++)
        {
            bool check_result = false;

            db_set_info_file_lock_read(p_db_set_info);
            check_result = read_and_check_db_set_file_format(p_db_set_info);
            load_db_sys_data_info_list(p_db_set_info);
            db_set_info_file_unlock_read(p_db_set_info);

            if (check_result == true)
            {
                timeout = false;
                break;
            }
            else
            {
                usleep(DB_FILE_OPEN_CHECK_INTERVAL_US);
            }
        }

        if (timeout)
        {
            // TODO: need review the following error operations
            // close_db_set_info(p_db_set_info);
            free_db_set_info_resources(p_db_set_info);
            unlock_db_set_info_sync(p_db_set_info);
            return NULL;
        }
    }
    else
    {
        // error: DB set file unavailable
        unlock_db_set_info_sync(p_db_set_info);
        return NULL;
    }
#endif

    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY);
    return p_db_set_info;
}

bool load_and_lock_db_set_info_handler_check_and_deep_copy_set_name(DB_SET_INFO_T *p_db_set_info, uint8_t *p_set_name, uint32_t set_name_size)
{
    bool valid = true;
    // TODO: add checkers

    p_db_set_info->set_name_size = set_name_size;
    allocate_db_set_info_resources(p_db_set_info);
    memcpy(p_db_set_info->p_set_name, p_set_name, set_name_size);

    return valid;
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

        // close_db_set_info(p_db_set_info);
        free_db_set_info_resources(p_db_set_info);

        update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_RELEASED);
        unlock_db_set_info_sync(p_db_set_info);
    }
}

void db_set_info_init(DB_SET_INFO_T *p_db_set_info)
{
    p_db_set_info->file = NULL;
    p_db_set_info->set_name_size = 0;
    p_db_set_info->p_set_name = NULL;
    db_sys_data_info_list_entry_init(&(p_db_set_info->delete_list_entry));
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

    if ((strlen(db_context.db_directory_path) + strlen(p_db_set_name) + strlen(file_extension)) > FACILEDB_FILE_PATH_MAX_LENGTH)
    {
        p_db_set_file_path[0] = '\0';
    }
    else
    {
        strcpy(p_db_set_file_path, db_context.db_directory_path);
        strcat(p_db_set_file_path, p_db_set_name);
        strcat(p_db_set_file_path, file_extension);

        p_db_set_file_path[FACILEDB_FILE_PATH_MAX_LENGTH] = '\0';
    }
}

// assign the set_name_size before using this function.
void allocate_db_set_info_resources(DB_SET_INFO_T *p_db_set_info)
{
    p_db_set_info->p_set_name = Mema_Api_Alloc(MEMA_USER_FACILEDB, p_db_set_info->set_name_size);
}

void free_db_set_info_resources(DB_SET_INFO_T *p_db_set_info)
{
    if (p_db_set_info->p_set_name != NULL)
    {
        Mema_Api_Free(MEMA_USER_FACILEDB, p_db_set_info->p_set_name);
        p_db_set_info->p_set_name = NULL;
        p_db_set_info->set_name_size = 0;
    }

    free_db_sys_data_info_list(&(p_db_set_info->delete_list_entry));
    free_db_sys_data_info_list(&(p_db_set_info->index_checkpoint_list_entry));

    if (p_db_set_info->file != NULL)
    {
        fclose(p_db_set_info->file);
        p_db_set_info->file = NULL;
    }
}

// void close_db_set_info(DB_SET_INFO_T *p_db_set_info)
// {
//     // Free dynamic buffers
//     free_db_set_info_resources(p_db_set_info);
//     free_db_sys_data_info_list(&(p_db_set_info->sys_data_list_entry));
//     fclose(p_db_set_info->file);
//     p_db_set_info->file = NULL;

//     // Reset db_set_info variables except db_set_info_status.
//     // db_set_info_init(p_db_set_info);
// }

// This funtion doesn't change the status to RELEASED.
void db_set_info_sync_init(DB_SET_INFO_SYNC_T *p_db_set_info_sync)
{
    // p_db_set_info_sync->status = DB_SET_INFO_STATUS_RELEASED;
    p_db_set_info_sync->reader_waiting_count = 0;
    p_db_set_info_sync->reader_count = 0;
    p_db_set_info_sync->writer_waiting_count = 0;
}

void sync_latest_db_set_info(DB_SET_INFO_T *p_db_set_info)
{
    uint64_t seq_num_before = p_db_set_info->db_set_properties.seq_num;
    if (p_db_set_info->file)
    {
        get_latest_db_set_properties(p_db_set_info, &(p_db_set_info->db_set_properties));
    }

    // reload sys_data_list_entry from db file.
    if (p_db_set_info->db_set_properties.seq_num != seq_num_before)
    {
        free_db_sys_data_info_list(&(p_db_set_info->delete_list_entry));
        free_db_sys_data_info_list(&(p_db_set_info->index_checkpoint_list_entry));

        load_db_sys_data_info_list(p_db_set_info);
    }
}

void db_set_info_file_lock_write(DB_SET_INFO_T *p_db_set_info)
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

void db_set_info_file_unlock_write(DB_SET_INFO_T *p_db_set_info)
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

void db_set_info_file_lock_read(DB_SET_INFO_T *p_db_set_info)
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

void db_set_info_file_unlock_read(DB_SET_INFO_T *p_db_set_info)
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

void lock_db_set_info_sync(DB_SET_INFO_T *p_db_set_info)
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.mutex);
    pthread_mutex_lock(p_mutex);
#endif
}

void unlock_db_set_info_sync(DB_SET_INFO_T *p_db_set_info)
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.mutex);
    pthread_mutex_unlock(p_mutex);
#endif
}

static inline void db_set_info_sync_close_wait(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t *p_writer_waiting_count = &(p_db_set_info->db_set_info_sync.writer_waiting_count);
    uint32_t *p_reader_count = &(p_db_set_info->db_set_info_sync.reader_count);
    uint32_t *p_reader_waiting_count = &(p_db_set_info->db_set_info_sync.reader_waiting_count);
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.mutex);
    pthread_cond_t *p_close_cond = &(p_db_set_info->db_set_info_sync.close_cond);

    // using while loop for spurious wakeup
    while (check_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY) == false || *p_writer_waiting_count > 0 || *p_reader_count > 0 || *p_reader_waiting_count > 0)
    {
        pthread_cond_wait(p_close_cond, p_mutex);
    }
#endif
}

void db_set_info_sync_write_wait(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t *p_writer_waiting_count = &(p_db_set_info->db_set_info_sync.writer_waiting_count);

    (*p_writer_waiting_count)++;
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.mutex);
    pthread_cond_t *p_write_cond = &(p_db_set_info->db_set_info_sync.write_cond);

    while (check_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY) == false)
    {
        pthread_cond_wait(p_write_cond, p_mutex);
    }
#endif
    (*p_writer_waiting_count)--;
}

void db_set_info_sync_write_unblock(DB_SET_INFO_T *p_db_set_info)
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

void db_set_info_sync_read_wait(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t *p_reader_waiting_count = &(p_db_set_info->db_set_info_sync.reader_waiting_count);
    uint32_t *p_reader_count = &(p_db_set_info->db_set_info_sync.reader_count);

    (*p_reader_waiting_count)++;
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_db_set_info->db_set_info_sync.mutex);
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

void db_set_info_sync_read_unblock(DB_SET_INFO_T *p_db_set_info)
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
    p_db_set_properties->seq_num = 0;
    p_db_set_properties->block_num = 0;
    p_db_set_properties->created_time = 0;
    p_db_set_properties->modified_time = 0;
    p_db_set_properties->data_num = 0;
    p_db_set_properties->latest_sys_block_tag = 0;
    p_db_set_properties->crc32 = Crc32_Api_Init();
}

void db_set_properties_region_init(DB_SET_PROPERTIES_REGION_T *p_db_set_properties_region)
{
    for (uint32_t i = 0; i < DB_SET_PROPERTIES_NUM; i++)
    {
        db_set_properties_init(&(p_db_set_properties_region->db_set_properties_list[i]));
    }

    p_db_set_properties_region->set_name_size = 0;
    p_db_set_properties_region->p_set_name = NULL;
}

static inline size_t get_db_set_properties_size()
{
    DB_SET_PROPERTIES_T temp;

    // static variable
    return (sizeof(temp.seq_num) + sizeof(temp.block_num) + sizeof(temp.created_time) + sizeof(temp.modified_time) + sizeof(temp.data_num) + sizeof(temp.latest_sys_block_tag) + sizeof(temp.crc32));
}

static inline size_t get_db_set_properties_region_size(DB_SET_INFO_T *p_db_set_info)
{
    return ((DB_SET_PROPERTIES_NUM * get_db_set_properties_size()) + sizeof(p_db_set_info->set_name_size) + p_db_set_info->set_name_size);
}

/*
** The file variable in db_set_info should be set before calling this function.
*/
void write_db_set_properties(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_T *p_db_set_properties, uint32_t db_set_properties_index)
{
    assert(db_set_properties_index < DB_SET_PROPERTIES_NUM);

    FILE *p_db_set_file = p_db_set_info->file;

    // update seq_num
    p_db_set_properties->seq_num += 1;

    // update crc32 value
    p_db_set_properties->crc32 = Crc32_Api_Init();
    p_db_set_properties->crc32 = Crc32_Api_Calc(p_db_set_properties->crc32, (void *)p_db_set_properties, offsetof(DB_SET_PROPERTIES_T, crc32));

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);
    off_t offset = 0 + (get_db_set_properties_size() * db_set_properties_index);

    // write static variables
    pwrite(fd, &(p_db_set_properties->seq_num), sizeof(p_db_set_properties->seq_num), offset);
    offset += sizeof(p_db_set_properties->seq_num);
    pwrite(fd, &(p_db_set_properties->block_num), sizeof(p_db_set_properties->block_num), offset);
    offset += sizeof(p_db_set_properties->block_num);
    pwrite(fd, &(p_db_set_properties->created_time), sizeof(p_db_set_properties->created_time), offset);
    offset += sizeof(p_db_set_properties->created_time);
    pwrite(fd, &(p_db_set_properties->modified_time), sizeof(p_db_set_properties->modified_time), offset);
    offset += sizeof(p_db_set_properties->modified_time);
    pwrite(fd, &(p_db_set_properties->data_num), sizeof(p_db_set_properties->data_num), offset);
    offset += sizeof(p_db_set_properties->data_num);
    pwrite(fd, &(p_db_set_properties->latest_sys_block_tag), sizeof(p_db_set_properties->latest_sys_block_tag), offset);
    offset += sizeof(p_db_set_properties->latest_sys_block_tag);
    pwrite(fd, &(p_db_set_properties->crc32), sizeof(p_db_set_properties->crc32), offset);
    offset += sizeof(p_db_set_properties->crc32);

    // ensure data is written to disk
    fsync(fd);
#endif // IS_POSIX_API_SUPPORT
}

/*
** The file variable in db_set_info should be set before calling this function.
** This funtion write the db_set_properties to the file without calculate the CRC and increase seq_num.
*/
void write_raw_db_set_properties(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_T *p_db_set_properties, uint32_t db_set_properties_index)
{
    assert(db_set_properties_index < DB_SET_PROPERTIES_NUM);

    FILE *p_db_set_file = p_db_set_info->file;

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);
    off_t offset = 0 + (get_db_set_properties_size() * db_set_properties_index);

    // write static variables
    pwrite(fd, &(p_db_set_properties->seq_num), sizeof(p_db_set_properties->seq_num), offset);
    offset += sizeof(p_db_set_properties->seq_num);
    pwrite(fd, &(p_db_set_properties->block_num), sizeof(p_db_set_properties->block_num), offset);
    offset += sizeof(p_db_set_properties->block_num);
    pwrite(fd, &(p_db_set_properties->created_time), sizeof(p_db_set_properties->created_time), offset);
    offset += sizeof(p_db_set_properties->created_time);
    pwrite(fd, &(p_db_set_properties->modified_time), sizeof(p_db_set_properties->modified_time), offset);
    offset += sizeof(p_db_set_properties->modified_time);
    pwrite(fd, &(p_db_set_properties->data_num), sizeof(p_db_set_properties->data_num), offset);
    offset += sizeof(p_db_set_properties->data_num);
    pwrite(fd, &(p_db_set_properties->latest_sys_block_tag), sizeof(p_db_set_properties->latest_sys_block_tag), offset);
    offset += sizeof(p_db_set_properties->latest_sys_block_tag);
    pwrite(fd, &(p_db_set_properties->crc32), sizeof(p_db_set_properties->crc32), offset);
    offset += sizeof(p_db_set_properties->crc32);

    // ensure data is written to disk
    fsync(fd);
#endif // IS_POSIX_API_SUPPORT
}

// Only use when write db_set_properties_region into an empty file.
// Use write_raw_db_set_properties.
void write_db_set_properties_region(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_REGION_T *p_db_set_properties_region)
{
    FILE *f = p_db_set_info->file;

    for (uint32_t i = 0; i < DB_SET_PROPERTIES_NUM; i++)
    {
        write_raw_db_set_properties(p_db_set_info, &(p_db_set_properties_region->db_set_properties_list[i]), i);
    }

    // write set name size and set name
#if IS_POSIX_API_SUPPORT
    size_t db_set_properties_list_size = 0 + (get_db_set_properties_size() * DB_SET_PROPERTIES_NUM);
    int fd = fileno(f);

    pwrite(fd, &(p_db_set_properties_region->set_name_size), sizeof(p_db_set_properties_region->set_name_size), db_set_properties_list_size);
    pwrite(fd, p_db_set_properties_region->p_set_name, p_db_set_properties_region->set_name_size, db_set_properties_list_size + sizeof(p_db_set_properties_region->set_name_size));

    // ensure data is written to disk
    fsync(fd);
#endif
}

/*
** The file variable in db_set_info should be set before calling this function.
** return value: number of db_set_properties read from file.
** Fill in p_db_set_properties when crc32 check passed. Otherwise, keep p_db_set_properties unchanged and return 0 for crc32 check failed.
*/
uint32_t read_db_set_properties(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_T *p_db_set_properties, uint32_t db_set_properties_index)
{
    assert(db_set_properties_index < DB_SET_PROPERTIES_NUM);

    FILE *p_db_set_file = p_db_set_info->file;
    DB_SET_PROPERTIES_T temp;
    uint32_t expected_crc32 = Crc32_Api_Init();

    // read db_set_properties from file to temp variable and check crc32 value for data integrity first.
    db_set_properties_init(&temp);
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);
    off_t offset = 0 + (get_db_set_properties_size() * db_set_properties_index);

    pread(fd, &(temp.seq_num), sizeof(temp.seq_num), offset);
    offset += sizeof(temp.seq_num);
    pread(fd, &(temp.block_num), sizeof(temp.block_num), offset);
    offset += sizeof(temp.block_num);
    pread(fd, &(temp.created_time), sizeof(temp.created_time), offset);
    offset += sizeof(temp.created_time);
    pread(fd, &(temp.modified_time), sizeof(temp.modified_time), offset);
    offset += sizeof(temp.modified_time);
    pread(fd, &(temp.data_num), sizeof(temp.data_num), offset);
    offset += sizeof(temp.data_num);
    pread(fd, &(temp.latest_sys_block_tag), sizeof(temp.latest_sys_block_tag), offset);
    offset += sizeof(temp.latest_sys_block_tag);
    pread(fd, &(temp.crc32), sizeof(temp.crc32), offset);
#endif // IS_POSIX_API_SUPPORT

    // copy to db_set_properties when crc32 check passed. Otherwise, keep db_set_properties unchanged and return false for crc32 check failed.
    expected_crc32 = Crc32_Api_Calc(expected_crc32, (void *)&temp, offsetof(DB_SET_PROPERTIES_T, crc32));

    if (temp.crc32 == expected_crc32)
    {
        memcpy(p_db_set_properties, &temp, sizeof(DB_SET_PROPERTIES_T));
        return 1;
    }

    return 0;
}

// return the invalid first, then earliest valid db_set_properties index.
// Doesn't consider the case that time wrapped around.
uint32_t get_earlist_db_set_properties_index(DB_SET_INFO_T *p_db_set_info)
{
    DB_SET_PROPERTIES_T temp;
    uint64_t earlist_seq_num = UINT64_MAX;
    uint32_t earlist_index = 0;

    for (uint32_t i = 0; i < DB_SET_PROPERTIES_NUM; i++)
    {
        db_set_properties_init(&temp);

        if (read_db_set_properties(p_db_set_info, &temp, i) > 0)
        {
            if (i == 0)
            {
                earlist_seq_num = temp.seq_num;
                earlist_index = 0;
            }
            else if (earlist_seq_num > temp.seq_num)
            {
                earlist_seq_num = temp.seq_num;
                earlist_index = i;
            }
        }
        else
        {
            // invalid db_set_properties.
            return i;
        }
    }

    return earlist_index;
}

void update_to_db_set_properties_region(DB_SET_INFO_T *p_db_set_info)
{
    uint32_t index = get_earlist_db_set_properties_index(p_db_set_info);

    write_db_set_properties(p_db_set_info, &(p_db_set_info->db_set_properties), index);
}

// return if there is the latest valid db_set_properties existed.
bool get_latest_db_set_properties(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_T *p_db_set_properties)
{
    DB_SET_PROPERTIES_T temp;
    bool has_valid_db_set_properties = false;
    uint64_t *p_latest_seq_num = &(p_db_set_properties->seq_num);

    for (uint32_t i = 0; i < DB_SET_PROPERTIES_NUM; i++)
    {
        db_set_properties_init(&temp);

        // check if read sucessfully and no crc fails.
        if (read_db_set_properties(p_db_set_info, &temp, i) > 0)
        {
            has_valid_db_set_properties = true;

            if ((i == 0) || (*p_latest_seq_num < temp.seq_num))
            {
                memcpy(p_db_set_properties, &temp, sizeof(DB_SET_PROPERTIES_T));
            }
        }
    }

    return has_valid_db_set_properties;
}

bool get_previous_db_set_properties(DB_SET_INFO_T *p_db_set_info, DB_SET_PROPERTIES_T *p_db_set_properties, uint64_t seq_num)
{
    DB_SET_PROPERTIES_T temp;
    bool has_valid_db_set_properties = false;
    uint64_t *p_previous_seq_num = &(p_db_set_properties->seq_num);

    for (uint32_t i = 0; i < DB_SET_PROPERTIES_NUM; i++)
    {
        db_set_properties_init(&temp);

        // check if read sucessfully and no crc fails.
        if (read_db_set_properties(p_db_set_info, &temp, i) > 0)
        {
            if (seq_num > temp.seq_num)
            {
                if (has_valid_db_set_properties == false || temp.seq_num > *p_previous_seq_num)
                {
                    has_valid_db_set_properties = true;
                    memcpy(p_db_set_properties, &temp, sizeof(DB_SET_PROPERTIES_T));
                }
            }
        }
    }

    return has_valid_db_set_properties;
}

// Read db_set_name from db_set_properties_region and check if it match the inputted db_set_name.
bool check_db_set_name(DB_SET_INFO_T *p_db_set_info)
{
    uint8_t *p_temp_set_name;
    uint32_t temp_set_name_size = 0;
    bool match = false;

    // read set name size and set name
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_info->file);
    size_t db_set_properties_list_size = 0 + (get_db_set_properties_size() * DB_SET_PROPERTIES_NUM);

    pread(fd, &temp_set_name_size, sizeof(temp_set_name_size), db_set_properties_list_size);
    p_temp_set_name = Mema_Api_Alloc(MEMA_USER_FACILEDB, temp_set_name_size);
    pread(fd, p_temp_set_name, temp_set_name_size, db_set_properties_list_size + sizeof(temp_set_name_size));
#endif

    // check if set name read from file matches with the input set name.
    if (temp_set_name_size == p_db_set_info->set_name_size && memcmp(p_temp_set_name, p_db_set_info->p_set_name, temp_set_name_size) == 0)
    {
        match = true;
    }
    else
    {
        // set name check failed.
        match = false;
    }

    Mema_Api_Free(MEMA_USER_FACILEDB, p_temp_set_name);
    return match;
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
    p_db_block->record_crc32 = Crc32_Api_Init();
    p_db_block->block_header_crc32 = Crc32_Api_Init();

    memset(p_db_block->block_data, 0, FACILEDB_BLOCK_DATA_SIZE);
}

void db_block_list_entry_init(DB_BLOCK_LIST_ENTRY_T *p_entry)
{
    p_entry->p_head = NULL;
    p_entry->p_tail = NULL;
    p_entry->list_length = 0;
}

void db_block_list_node_init(DB_BLOCK_LIST_NODE_T *p_node)
{
    db_block_init(&(p_node->db_block));
    p_node->p_prev = NULL;
    p_node->p_next = NULL;
}

void append_db_block_list_node_to_tail(DB_BLOCK_LIST_ENTRY_T *p_entry, DB_BLOCK_LIST_NODE_T *p_node)
{
    if (p_entry->p_head == NULL)
    {
        assert(p_entry->p_tail == NULL);
        p_entry->p_head = p_node;
        p_entry->p_tail = p_node;
    }
    else
    {
        assert(p_entry->p_tail != NULL);
        p_entry->p_tail->p_next = p_node;
        p_node->p_prev = p_entry->p_tail;
        p_entry->p_tail = p_node;
    }

    p_entry->list_length++;
}

off_t get_db_block_offset(DB_SET_INFO_T *p_db_set_info, uint64_t block_tag)
{
    size_t set_properties_region_size = get_db_set_properties_region_size(p_db_set_info);
    size_t block_size = get_db_block_size();

    return (set_properties_region_size + ((block_tag - 1) * block_size));
}

size_t get_db_block_size()
{
    DB_BLOCK_T dummy_db_block;
    size_t attribute_size = sizeof(dummy_db_block.block_tag) + sizeof(dummy_db_block.data_tag) + sizeof(dummy_db_block.prev_block_tag) + sizeof(dummy_db_block.next_block_tag);
    size_t time_attribute_size = sizeof(dummy_db_block.created_time) + sizeof(dummy_db_block.modified_time);
    size_t other_attribute_size = sizeof(dummy_db_block.deleted) + sizeof(dummy_db_block.valid_record_num) + sizeof(dummy_db_block.record_crc32) + sizeof(dummy_db_block.block_header_crc32);

    size_t block_data_size = sizeof(dummy_db_block.block_data);

    return (attribute_size + time_attribute_size + other_attribute_size + block_data_size);
}

void write_db_block(DB_BLOCK_T *p_db_block, DB_SET_INFO_T *p_db_set_info)
{
    FILE *p_db_set_file = p_db_set_info->file;
    DB_SET_PROPERTIES_T *p_db_set_properties = &(p_db_set_info->db_set_properties);
    uint64_t block_tag = p_db_block->block_tag;
    off_t block_offset = get_db_block_offset(p_db_set_info, block_tag);
    uint32_t header_crc_value = Crc32_Api_Init();

    assert((block_tag > 0) && (block_tag <= p_db_set_properties->block_num));

    header_crc_value = Crc32_Api_Calc(header_crc_value, (void *)p_db_block, offsetof(DB_BLOCK_T, block_header_crc32));
    p_db_block->block_header_crc32 = header_crc_value;
    // write static variables
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);

    // alignment makes sizeof and get_db_block_size() different.
    if (sizeof(DB_BLOCK_T) == get_db_block_size())
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
        pwrite(fd, &(p_db_block->record_crc32), sizeof(p_db_block->record_crc32), block_offset);
        block_offset += sizeof(p_db_block->record_crc32);
        pwrite(fd, &(p_db_block->block_header_crc32), sizeof(p_db_block->block_header_crc32), block_offset);
        block_offset += sizeof(p_db_block->block_header_crc32);
        pwrite(fd, p_db_block->block_data, sizeof(p_db_block->block_data), block_offset);
    }

    // ensure data is written to disk
    fsync(fd);

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
        fwrite(&(p_db_block->record_crc32), sizeof(p_db_block->record_crc32), 1, p_db_set_file);
        fwrite(&(p_db_block->block_header_crc32), sizeof(p_db_block->block_header_crc32), 1, p_db_set_file);

        fwrite(p_db_block->block_data, sizeof(p_db_block->block_data), 1, p_db_set_file);
    }
#endif // IS_POSIX_API_SUPPORT
}

// return value: number of db_block read from file.
uint32_t read_db_block(DB_SET_INFO_T *p_db_set_info, uint64_t block_tag, DB_BLOCK_T *p_db_block)
{
    assert((block_tag > 0) && (block_tag <= p_db_set_info->db_set_properties.block_num));

    FILE *p_db_set_file = p_db_set_info->file;
    off_t block_offset = get_db_block_offset(p_db_set_info, block_tag);
    DB_BLOCK_T temp;
    uint32_t expected_header_crc32 = Crc32_Api_Init();

    // read db_block from file to temp variable and check crc32 value for integrity first.
    db_block_init(&temp);
    // read static variables
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);

    // Alignment may make sizeof and get_db_block_size() different.
    if (sizeof(DB_BLOCK_T) == get_db_block_size())
    {
        pread(fd, &temp, sizeof(DB_BLOCK_T), block_offset);
    }
    else
    {
        pread(fd, &(temp.block_tag), sizeof(temp.block_tag), block_offset);
        block_offset += sizeof(temp.block_tag);
        pread(fd, &(temp.data_tag), sizeof(temp.data_tag), block_offset);
        block_offset += sizeof(temp.data_tag);
        pread(fd, &(temp.prev_block_tag), sizeof(temp.prev_block_tag), block_offset);
        block_offset += sizeof(temp.prev_block_tag);
        pread(fd, &(temp.next_block_tag), sizeof(temp.next_block_tag), block_offset);
        block_offset += sizeof(temp.next_block_tag);
        pread(fd, &(temp.created_time), sizeof(temp.created_time), block_offset);
        block_offset += sizeof(temp.created_time);
        pread(fd, &(temp.modified_time), sizeof(temp.modified_time), block_offset);
        block_offset += sizeof(temp.modified_time);
        pread(fd, &(temp.deleted), sizeof(temp.deleted), block_offset);
        block_offset += sizeof(temp.deleted);
        pread(fd, &(temp.valid_record_num), sizeof(temp.valid_record_num), block_offset);
        block_offset += sizeof(temp.valid_record_num);
        pread(fd, &(temp.record_crc32), sizeof(temp.record_crc32), block_offset);
        block_offset += sizeof(temp.record_crc32);
        pread(fd, &(temp.block_header_crc32), sizeof(temp.block_header_crc32), block_offset);
        block_offset += sizeof(temp.block_header_crc32);
        pread(fd, temp.block_data, sizeof(p_db_block->block_data), block_offset);
    }
#else // IS_POSIX_API_SUPPORT
    fseek(p_db_set_file, block_offset, SEEK_SET);

    if (sizeof(DB_BLOCK_T) == get_db_block_size())
    {
        fread(&temp, sizeof(DB_BLOCK_T), 1, p_db_set_file);
    }
    else
    {
        fread(&(temp.block_tag), sizeof(temp.block_tag), 1, p_db_set_file);
        fread(&(temp.data_tag), sizeof(temp.data_tag), 1, p_db_set_file);
        fread(&(temp.prev_block_tag), sizeof(temp.prev_block_tag), 1, p_db_set_file);
        fread(&(temp.next_block_tag), sizeof(temp.next_block_tag), 1, p_db_set_file);
        fread(&(temp.created_time), sizeof(temp.created_time), 1, p_db_set_file);
        fread(&(temp.modified_time), sizeof(temp.modified_time), 1, p_db_set_file);
        fread(&(temp.deleted), sizeof(temp.deleted), 1, p_db_set_file);
        fread(&(temp.valid_record_num), sizeof(temp.valid_record_num), 1, p_db_set_file);
        fread(&(temp.record_crc32), sizeof(temp.record_crc32), 1, p_db_set_file);
        fread(&(temp.block_header_crc32), sizeof(temp.block_header_crc32), 1, p_db_set_file);

        fread(p_db_block->block_data, sizeof(p_db_block->block_data), 1, p_db_set_file);
    }
#endif

    // copy temp db block to p_db_block after header crc32 check passed.
    expected_header_crc32 = Crc32_Api_Calc(expected_header_crc32, (void *)&temp, offsetof(DB_BLOCK_T, block_header_crc32));
    if (temp.block_header_crc32 == expected_header_crc32)
    {
        memcpy(p_db_block, &temp, sizeof(DB_BLOCK_T));
        return 1;
    }
    else
    {
        assert(0);
        return 0;
    }
}

// return value: number of db_block read from file.
uint32_t read_db_block_attributes(DB_SET_INFO_T *p_db_set_info, uint64_t block_tag, DB_BLOCK_T *p_db_block)
{
    assert((block_tag > 0) && (block_tag <= p_db_set_info->db_set_properties.block_num));

    FILE *p_db_set_file = p_db_set_info->file;
    DB_BLOCK_T temp;
    off_t block_offset = get_db_block_offset(p_db_set_info, block_tag);
    uint32_t expected_header_crc32 = Crc32_Api_Init();

    db_block_init(&temp);
#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_db_set_file);

    pread(fd, &(temp.block_tag), sizeof(temp.block_tag), block_offset);
    block_offset += sizeof(temp.block_tag);
    pread(fd, &(temp.data_tag), sizeof(temp.data_tag), block_offset);
    block_offset += sizeof(temp.data_tag);
    pread(fd, &(temp.prev_block_tag), sizeof(temp.prev_block_tag), block_offset);
    block_offset += sizeof(temp.prev_block_tag);
    pread(fd, &(temp.next_block_tag), sizeof(temp.next_block_tag), block_offset);
    block_offset += sizeof(temp.next_block_tag);
    pread(fd, &(temp.created_time), sizeof(temp.created_time), block_offset);
    block_offset += sizeof(temp.created_time);
    pread(fd, &(temp.modified_time), sizeof(temp.modified_time), block_offset);
    block_offset += sizeof(temp.modified_time);
    pread(fd, &(temp.deleted), sizeof(temp.deleted), block_offset);
    block_offset += sizeof(temp.deleted);
    pread(fd, &(temp.valid_record_num), sizeof(temp.valid_record_num), block_offset);
    block_offset += sizeof(temp.valid_record_num);
    pread(fd, &(temp.record_crc32), sizeof(temp.record_crc32), block_offset);
    block_offset += sizeof(temp.record_crc32);
    pread(fd, &(temp.block_header_crc32), sizeof(temp.block_header_crc32), block_offset);
#else  // IS_POSIX_API_SUPPORT
    fseek(p_db_set_file, block_offset, SEEK_SET);

    fread(&(temp.block_tag), sizeof(temp.block_tag), 1, p_db_set_file);
    fread(&(temp.data_tag), sizeof(temp.data_tag), 1, p_db_set_file);
    fread(&(temp.prev_block_tag), sizeof(temp.prev_block_tag), 1, p_db_set_file);
    fread(&(temp.next_block_tag), sizeof(temp.next_block_tag), 1, p_db_set_file);
    fread(&(temp.created_time), sizeof(temp.created_time), 1, p_db_set_file);
    fread(&(temp.modified_time), sizeof(temp.modified_time), 1, p_db_set_file);
    fread(&(temp.deleted), sizeof(temp.deleted), 1, p_db_set_file);
    fread(&(temp.valid_record_num), sizeof(temp.valid_record_num), 1, p_db_set_file);
    fread(&(temp.record_crc32), sizeof(temp.record_crc32), 1, p_db_set_file);
    fread(&(temp.block_header_crc32), sizeof(temp.block_header_crc32), 1, p_db_set_file);
#endif // IS_POSIX_API_SUPPORT

    expected_header_crc32 = Crc32_Api_Calc(expected_header_crc32, (void *)&temp, offsetof(DB_BLOCK_T, block_header_crc32));
    if (temp.block_header_crc32 == expected_header_crc32)
    {
        memcpy(p_db_block, &temp, offsetof(DB_BLOCK_T, block_data));
        return 1;
    }
    else
    {
        assert(0);
        return 0;
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

bool extract_db_data_info_from_db_blocks(DB_DATA_INFO_T *p_db_data_info, uint64_t start_block_tag, DB_SET_INFO_T *p_db_set_info)
{
    DB_DATA_INFO_T temp_data_info;
    DB_BLOCK_T db_block;
    uint8_t *p_block_data = NULL;
    uint8_t *p_block_end_address = NULL;
    uint32_t expected_record_crc32 = 0; // value that stored in db block
    uint32_t calculated_record_crc32 = Crc32_Api_Init();

    db_data_info_init(&temp_data_info);
    db_block_init(&db_block);

    if (read_db_block(p_db_set_info, start_block_tag, &db_block) <= 0)
    {
        return false;
    }

    temp_data_info.data_tag = db_block.data_tag;
    temp_data_info.start_db_block_tag = start_block_tag;
    extract_db_data_info_from_db_blocks_handler_update_time(&temp_data_info, &db_block);
    // update record_num at the end of this function.
    temp_data_info.deleted = db_block.deleted;
    expected_record_crc32 = db_block.record_crc32;

    temp_data_info.record_num = db_block.valid_record_num;
    // allocate db data info resources
    temp_data_info.p_db_record_info = Mema_Api_Alloc(MEMA_USER_FACILEDB, temp_data_info.record_num * sizeof(DB_RECORD_INFO_T));
    p_block_data = db_block.block_data;
    p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();

    if (temp_data_info.p_db_record_info == NULL)
    {
        // TODO: handling out-of memory
        return false;
    }
    else
    {
        // Initialize p_db_record_info array
        for (uint32_t i = 0; i < temp_data_info.record_num; i++)
        {
            db_record_info_init(&(temp_data_info.p_db_record_info[i]));
        }
    }

    for (uint32_t i = 0; i < temp_data_info.record_num; i++)
    {
        bool find_valid_db_record = false;
        uint32_t remaining_size = 0;

        // db_record_info_init(&(temp_data_info.p_db_record_info[i]));

        // check if valid record (deleted == false) and reaches to the valid one.
        while (find_valid_db_record == false)
        {
            if ((p_block_data + get_db_record_properties_size()) > p_block_end_address)
            {
                // clear the current block and read next block from next_block_tag and update the variables.
                int next_block_tag = db_block.next_block_tag;
                assert(next_block_tag != 0);

                db_block_init(&db_block);

                if (read_db_block(p_db_set_info, next_block_tag, &db_block) <= 0)
                {
                    free_db_data_info_resources(&temp_data_info);
                    return false;
                }
                extract_db_data_info_from_db_blocks_handler_update_time(&temp_data_info, &db_block);
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
                        if (read_db_block(p_db_set_info, next_block_tag, &db_block) <= 0)
                        {
                            free_db_data_info_resources(&temp_data_info);
                            return false;
                        }
                        extract_db_data_info_from_db_blocks_handler_update_time(&temp_data_info, &db_block);
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
                        if (read_db_block(p_db_set_info, next_block_tag, &db_block) <= 0)
                        {
                            free_db_data_info_resources(&temp_data_info);
                            return false;
                        }
                        extract_db_data_info_from_db_blocks_handler_update_time(&temp_data_info, &db_block);
                        p_block_data = db_block.block_data;
                        p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();
                    }
                    remaining_size -= forward_size;
                }
            }
        }
        // Setting record properties offset and copy db_record_properties from db_block_data.
        temp_data_info.p_db_record_info[i].db_record_properties_offset = get_db_block_offset(p_db_set_info, db_block.block_tag) + (p_block_data - ((uint8_t *)&db_block));
        memcpy(&(temp_data_info.p_db_record_info[i].db_record_properties), p_block_data, get_db_record_properties_size());
        p_block_data += get_db_record_properties_size();

        // Prepare buffer to copy db_record from the db blocks.
        allocate_db_record_info_resources(&(temp_data_info.p_db_record_info[i]));

        // copy db_record key from db block data.
        remaining_size = temp_data_info.p_db_record_info[i].db_record_properties.key_size;
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
                if (read_db_block(p_db_set_info, next_block_tag, &db_block) <= 0)
                {
                    free_db_data_info_resources(&temp_data_info);
                    return false;
                }
                extract_db_data_info_from_db_blocks_handler_update_time(&temp_data_info, &db_block);
                p_block_data = db_block.block_data;
                p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();

                continue;
            }

            p_key = temp_data_info.p_db_record_info[i].db_record.p_key + temp_data_info.p_db_record_info[i].db_record_properties.key_size - remaining_size;
            memcpy(p_key, p_block_data, copy_size);
            p_block_data += copy_size;
            remaining_size -= copy_size;
        }

        // Copy db_record value from db block data.
        remaining_size = temp_data_info.p_db_record_info[i].db_record_properties.value_size;
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
                if (read_db_block(p_db_set_info, next_block_tag, &db_block) <= 0)
                {
                    free_db_data_info_resources(&temp_data_info);
                    return false;
                }
                extract_db_data_info_from_db_blocks_handler_update_time(&temp_data_info, &db_block);
                p_block_data = db_block.block_data;
                p_block_end_address = ((uint8_t *)&(db_block)) + get_db_block_size();
            }

            p_value = temp_data_info.p_db_record_info[i].db_record.p_value + temp_data_info.p_db_record_info[i].db_record_properties.value_size - remaining_size;
            memcpy(p_value, p_block_data, copy_size);
            p_block_data += copy_size;
            remaining_size -= copy_size;
        }
    }

    // cehck record crc
    calculated_record_crc32 = Crc32_Api_Init();
    for (uint32_t i = 0; i < temp_data_info.record_num; i++)
    {
        calculated_record_crc32 = Crc32_Api_Calc(calculated_record_crc32, &(temp_data_info.p_db_record_info[i].db_record_properties), sizeof(DB_RECORD_PROPERTIES_T));
        calculated_record_crc32 = Crc32_Api_Calc(calculated_record_crc32, temp_data_info.p_db_record_info[i].db_record.p_key, temp_data_info.p_db_record_info[i].db_record_properties.key_size);
        calculated_record_crc32 = Crc32_Api_Calc(calculated_record_crc32, temp_data_info.p_db_record_info[i].db_record.p_value, temp_data_info.p_db_record_info[i].db_record_properties.value_size);
    }

    if (calculated_record_crc32 != expected_record_crc32)
    {
        free_db_data_info_resources(&temp_data_info);
        return false;
    }

    shallow_copy_db_data_info(p_db_data_info, &temp_data_info);
    return true;
}

void db_record_properties_init(DB_RECORD_PROPERTIES_T *p_db_record_properties)
{
    p_db_record_properties->deleted = 0;
    p_db_record_properties->key_size = 0;
    p_db_record_properties->value_size = 0;
    p_db_record_properties->record_value_type = FACILEDB_RECORD_VALUE_TYPE_INVALID;
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

size_t get_db_sys_record_size()
{
    DB_SYS_RECORD_T temp;

    return sizeof(temp.type) + sizeof(temp.payload);
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
    for (uint32_t i = 0; i < p_db_data_info->record_num; i++)
    {
        free_db_record_info_resources(&(p_db_data_info->p_db_record_info[i]));
    }

    if (p_db_data_info->record_num > 0)
    {
        Mema_Api_Free(MEMA_USER_FACILEDB, p_db_data_info->p_db_record_info);
    }

    p_db_data_info->record_num = 0;
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
    p_faciledb_data->p_data_records = Mema_Api_Alloc(MEMA_USER_FACILEDB, p_db_data_info->record_num * sizeof(FACILEDB_RECORD_T));

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
    p_db_data_info->p_db_record_info = Mema_Api_Alloc(MEMA_USER_FACILEDB, p_db_data_info->record_num * sizeof(DB_RECORD_INFO_T));

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

bool check_last_db_data_info(DB_SET_INFO_T *p_db_set_info)
{
    // get the start block tag from the last db block and check block tag backward simultaneously.
    DB_BLOCK_T temp_db_block;
    DB_DATA_INFO_T temp_db_info;
    uint64_t current_block_tag = p_db_set_info->db_set_properties.block_num;
    uint64_t next_block_tag = 0;
    bool check_result = true;

    do
    {
        db_block_init(&temp_db_block);
        if (read_db_block_attributes(p_db_set_info, current_block_tag, &temp_db_block) <= 0)
        {
            return false;
        }

        if ((temp_db_block.block_tag != current_block_tag) ||
            (temp_db_block.data_tag != p_db_set_info->db_set_properties.data_num) ||
            (temp_db_block.next_block_tag != next_block_tag))
        {
            return false;
        }

        // move backward
        next_block_tag = current_block_tag;
        current_block_tag = temp_db_block.prev_block_tag;
    } while (current_block_tag != 0);

    // temp_db_block.block_tag is the first block tag.
    db_data_info_init(&temp_db_info);
    check_result = extract_db_data_info_from_db_blocks(&temp_db_info, temp_db_block.block_tag, p_db_set_info);

    free_db_data_info_resources(&temp_db_info);
    return check_result;
}

void db_data_info_list_entry_init(DB_DATA_INFO_LIST_ENTRY_T *p_entry)
{
    p_entry->p_head = NULL;
    p_entry->p_tail = NULL;
    p_entry->list_length = 0;
}

void db_data_info_list_node_init(DB_DATA_INFO_LIST_NODE_T *p_node)
{
    db_data_info_init(&(p_node->db_data_info));
    p_node->p_prev = NULL;
    p_node->p_next = NULL;
}

void free_db_data_info_list(DB_DATA_INFO_LIST_ENTRY_T *p_entry)
{
    DB_DATA_INFO_LIST_NODE_T *p_current_node = p_entry->p_head;
    DB_DATA_INFO_LIST_NODE_T *p_next_node = NULL;

    for (; p_entry->list_length > 0; p_entry->list_length--)
    {
        p_next_node = p_current_node->p_next;
        free_db_data_info_list_node_resources(p_current_node);
        Mema_Api_Free(MEMA_USER_FACILEDB, p_current_node);
        p_current_node = p_next_node;
    }

    p_entry->p_head = NULL;
    p_entry->p_tail = NULL;
}

void free_db_data_info_list_node_resources(DB_DATA_INFO_LIST_NODE_T *p_node)
{
    free_db_data_info_resources(&(p_node->db_data_info));
}

void db_sys_data_info_init(DB_SYS_DATA_INFO_T *p_db_sys_data_info)
{
    p_db_sys_data_info->block_tag = 0;
    p_db_sys_data_info->created_time = 0;
    db_sys_record_init(&(p_db_sys_data_info->sys_record));
}

void db_sys_data_info_list_entry_init(DB_SYS_DATA_INFO_LIST_ENTRY_T *p_entry)
{
    p_entry->p_head = NULL;
    p_entry->p_tail = NULL;
    p_entry->list_length = 0;
}

void db_sys_data_info_list_node_init(DB_SYS_DATA_INFO_LIST_NODE_T *p_node)
{
    db_sys_data_info_init(&(p_node->sys_data_info));
    p_node->p_prev = NULL;
    p_node->p_next = NULL;
}

void free_db_sys_data_info_list(DB_SYS_DATA_INFO_LIST_ENTRY_T *p_entry)
{
    for (DB_SYS_DATA_INFO_LIST_NODE_T *p_current_node = p_entry->p_head; p_entry->list_length > 0; p_entry->list_length--)
    {
        DB_SYS_DATA_INFO_LIST_NODE_T *p_next_node = p_current_node->p_next;
        Mema_Api_Free(MEMA_USER_FACILEDB, p_current_node);
        p_current_node = p_next_node;
    }

    p_entry->p_head = NULL;
    p_entry->p_tail = NULL;
}

void db_record_info_init(DB_RECORD_INFO_T *p_db_record_info)
{
    db_record_properties_init(&(p_db_record_info->db_record_properties));

    // Init db recrod
    p_db_record_info->db_record.p_key = NULL;
    p_db_record_info->db_record.p_value = NULL;

    p_db_record_info->db_record_properties_offset = 0;
}

// Setup key_size and value_size before calling this function.
bool allocate_db_record_info_resources(DB_RECORD_INFO_T *p_db_record_info)
{
    uint32_t key_size = p_db_record_info->db_record_properties.key_size;
    uint32_t value_size = p_db_record_info->db_record_properties.value_size;

    p_db_record_info->db_record.p_key = Mema_Api_Alloc(MEMA_USER_FACILEDB, key_size);
    if (p_db_record_info->db_record.p_key == NULL)
    {
        return false;
    }

    p_db_record_info->db_record.p_value = Mema_Api_Alloc(MEMA_USER_FACILEDB, value_size);
    if (p_db_record_info->db_record.p_value == NULL)
    {
        Mema_Api_Free(MEMA_USER_FACILEDB, p_db_record_info->db_record.p_key);
        return false;
    }

    return true;
}

void free_db_record_info_resources(DB_RECORD_INFO_T *p_db_record_info)
{
    if (p_db_record_info->db_record_properties.key_size > 0)
    {
        Mema_Api_Free(MEMA_USER_FACILEDB, p_db_record_info->db_record.p_key);
        p_db_record_info->db_record_properties.key_size = 0;
    }

    if (p_db_record_info->db_record_properties.value_size > 0)
    {
        Mema_Api_Free(MEMA_USER_FACILEDB, p_db_record_info->db_record.p_value);
        p_db_record_info->db_record_properties.value_size = 0;
    }
}

bool faciledb_record_to_db_record_info(FACILEDB_RECORD_T *p_faciledb_record, DB_RECORD_INFO_T *p_db_record_info)
{
    DB_RECORD_PROPERTIES_T *p_db_record_properties = &(p_db_record_info->db_record_properties);
    DB_RECORD_T *p_db_record = &(p_db_record_info->db_record);

    p_db_record_properties->deleted = 0;
    p_db_record_properties->key_size = p_faciledb_record->key_size;
    p_db_record_properties->value_size = p_faciledb_record->value_size;
    p_db_record_properties->record_value_type = p_faciledb_record->record_value_type;

    allocate_db_record_info_resources(p_db_record_info);
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

void db_sys_record_init(DB_SYS_RECORD_T *p_sys_record)
{
    memset(p_sys_record, 0, sizeof(DB_SYS_RECORD_T));
    p_sys_record->type = DB_SYS_RECORD_TYPE_INAVLID;
}

// return value: the list length
uint32_t load_db_sys_data_info_list(DB_SET_INFO_T *p_db_set_info)
{
    uint64_t sys_block_tag = p_db_set_info->db_set_properties.latest_sys_block_tag;

    db_sys_data_info_list_entry_init(&(p_db_set_info->delete_list_entry));
    db_sys_data_info_list_entry_init(&(p_db_set_info->index_checkpoint_list_entry));

    extract_db_sys_data_info_list_from_db_blocks(p_db_set_info, sys_block_tag);

    return p_db_set_info->delete_list_entry.list_length + p_db_set_info->index_checkpoint_list_entry.list_length;
}

// db_sys_data_info_list is a doubly linked list (not circular)
// sys_data_block is a series of db blocks that data_tag is 0 and use prev_data_tag to link to ther previous sys_data_block
// sys data block linked list is a single linked list start from the latest sys data block to the previous.
uint32_t extract_db_sys_data_info_list_from_db_blocks(DB_SET_INFO_T *p_db_set_info, uint64_t start_block_tag)
{
    uint64_t block_tag = start_block_tag;
    uint32_t sys_data_num = 0;

    while (block_tag != 0)
    {
        DB_BLOCK_T db_block;
        uint32_t calcualte_crc = Crc32_Api_Init();
        DB_SYS_DATA_INFO_LIST_ENTRY_T delete_sys_data_info_in_block, index_checkpoint_sys_data_info_in_block;
        DB_SYS_DATA_INFO_LIST_NODE_T *p_block_list_node;

        DB_SYS_DATA_INFO_LIST_ENTRY_T *p_target_list_entry = NULL;

        db_sys_data_info_list_entry_init(&delete_sys_data_info_in_block);
        db_sys_data_info_list_entry_init(&index_checkpoint_sys_data_info_in_block);
        read_db_block(p_db_set_info, block_tag, &db_block);

        for (uint32_t i = 0; i < db_block.valid_record_num; i++)
        {
            DB_SYS_DATA_INFO_LIST_NODE_T *p_node = Mema_Api_Alloc(MEMA_USER_FACILEDB, sizeof(DB_SYS_DATA_INFO_LIST_NODE_T));
            uint8_t *p_sys_record = db_block.block_data + (i * get_db_sys_record_size());

            db_sys_data_info_list_node_init(p_node);

            p_node->sys_data_info.block_tag = block_tag;
            p_node->sys_data_info.created_time = db_block.created_time;

            memcpy(&(p_node->sys_data_info.sys_record.type_32), p_sys_record, sizeof(p_node->sys_data_info.sys_record.type_32));
            p_sys_record += sizeof(p_node->sys_data_info.sys_record.type_32);
            memcpy(&(p_node->sys_data_info.sys_record.payload), p_sys_record, sizeof(p_node->sys_data_info.sys_record.payload));

            // check valid
            assert(p_node->sys_data_info.block_tag <= p_db_set_info->db_set_properties.block_num);
            assert(p_node->sys_data_info.sys_record.type < DB_SYS_RECORD_TYPE_NUM);

            calcualte_crc = Crc32_Api_Calc(calcualte_crc, &(p_node->sys_data_info.sys_record.type_32), sizeof(p_node->sys_data_info.sys_record.type_32));
            calcualte_crc = Crc32_Api_Calc(calcualte_crc, &(p_node->sys_data_info.sys_record.payload), sizeof(p_node->sys_data_info.sys_record.payload));

            if (p_node->sys_data_info.sys_record.type == DB_SYS_RECORD_TYPE_DELETE)
            {
                assert(p_node->sys_data_info.sys_record.payload.data_tag <= p_db_set_info->db_set_properties.data_num);
                p_target_list_entry = &delete_sys_data_info_in_block;

                // append node to block specific list, insertion sort, ascending order (p_next)
                for (p_block_list_node = delete_sys_data_info_in_block.p_tail; p_block_list_node != NULL; p_block_list_node = p_block_list_node->p_prev)
                {
                    if (p_block_list_node->sys_data_info.sys_record.payload.data_tag < p_node->sys_data_info.sys_record.payload.data_tag)
                    {
                        break;
                    }
                }
            }
            else if (p_node->sys_data_info.sys_record.type == DB_SYS_RECORD_TYPE_INDEX_SEQ_NUM_CHECKPOINT)
            {
                // append node to block specific list, insertion sort, ascending order (p_next)
                // if node with same index key existed, keep the largest seq_num one.

                p_target_list_entry = &index_checkpoint_sys_data_info_in_block;

                for (p_block_list_node = index_checkpoint_sys_data_info_in_block.p_tail; p_block_list_node != NULL; p_block_list_node = p_block_list_node->p_prev)
                {
                    HASH_VALUE_COMPARE_RESULT_E compare_result = Hash_Api_Compare(p_block_list_node->sys_data_info.sys_record.payload.index_key_hash, p_node->sys_data_info.sys_record.payload.index_key_hash);
                    if (compare_result == HASH_VALUE_COMPARE_EQUAL)
                    {
                        if (p_block_list_node->sys_data_info.sys_record.payload.index_seq_num > p_node->sys_data_info.sys_record.payload.index_seq_num)
                        {
                            // current seq_num is smaller, release it.
                            Mema_Api_Free(MEMA_USER_FACILEDB, p_node);
                            continue;
                        }
                        else
                        {
                            // current seq_num is bigger, release the existed node
                            DB_SYS_DATA_INFO_LIST_NODE_T *p_block_list_prev_node = p_block_list_node->p_prev;

                            if (p_block_list_node->p_prev)
                            {
                                p_block_list_node->p_prev->p_next = p_block_list_node->p_next;
                            }
                            else
                            {
                                // p_block_list_node is the head node
                                index_checkpoint_sys_data_info_in_block.p_head = p_block_list_node->p_next;
                            }

                            if (p_block_list_node->p_next)
                            {
                                p_block_list_node->p_next->p_prev = p_block_list_node->p_prev;
                            }
                            else
                            {
                                // p_block_list_node is the tail node
                                index_checkpoint_sys_data_info_in_block.p_tail = p_block_list_node->p_prev;
                            }

                            Mema_Api_Free(MEMA_USER_FACILEDB, p_block_list_node);
                            p_block_list_node = p_block_list_prev_node;
                            index_checkpoint_sys_data_info_in_block.list_length--;
                        }
                    }
                    else if (compare_result == HASH_VALUE_COMPARE_RIGHT_GREATER)
                    {
                        break;
                    }
                }
            }
            else
            {
                continue;
            }

            if (p_block_list_node == NULL)
            {
                // append to head
                p_node->p_next = p_target_list_entry->p_head;
                if (p_target_list_entry->p_head != NULL)
                {
                    p_target_list_entry->p_head->p_prev = p_node;
                }
                p_target_list_entry->p_head = p_node;

                // no existed node
                if (p_target_list_entry->p_tail == NULL)
                {
                    p_target_list_entry->p_tail = p_node;
                }
            }
            else
            {
                // append the p_node to next
                p_node->p_next = p_block_list_node->p_next;
                p_node->p_prev = p_block_list_node;

                if (p_block_list_node->p_next == NULL)
                {
                    // append to tail
                    p_target_list_entry->p_tail = p_node;
                }
                else
                {
                    p_block_list_node->p_next->p_prev = p_node;
                }
                p_block_list_node->p_next = p_node;
            }
            p_target_list_entry->list_length++;
        }

        // check record crc
        if (calcualte_crc != db_block.record_crc32)
        {
            free_db_sys_data_info_list(&delete_sys_data_info_in_block);
            free_db_sys_data_info_list(&index_checkpoint_sys_data_info_in_block);
        }
        else
        {
            // delete sys info list
            merge_sorted_delete_sys_info_list(&(p_db_set_info->delete_list_entry), &delete_sys_data_info_in_block);
            sys_data_num += delete_sys_data_info_in_block.list_length;

            // index_checkpoint sys info list
            merge_sorted_index_checkpoint_sys_info_list(&(p_db_set_info->index_checkpoint_list_entry), &index_checkpoint_sys_data_info_in_block);
            sys_data_num += index_checkpoint_sys_data_info_in_block.list_length;
        }

        block_tag = db_block.prev_block_tag;
    }

    return sys_data_num;
}

// append source delete list (ascending order) to destination delete list (ascending order)
void merge_sorted_delete_sys_info_list(DB_SYS_DATA_INFO_LIST_ENTRY_T *p_list_dest, DB_SYS_DATA_INFO_LIST_ENTRY_T *p_list_src)
{
    DB_SYS_DATA_INFO_LIST_NODE_T *p_dest_list_node, *p_src_list_node;

    p_dest_list_node = p_list_dest->p_head;
    p_src_list_node = p_list_src->p_head;

    while (p_dest_list_node && p_src_list_node)
    {
        if (p_src_list_node->sys_data_info.sys_record.payload.data_tag < p_dest_list_node->sys_data_info.sys_record.payload.data_tag)
        {
            DB_SYS_DATA_INFO_LIST_NODE_T *p_src_list_next_node = p_src_list_node->p_next;

            // append to p_dest_list_node->p_prev
            if (p_dest_list_node->p_prev == NULL)
            {
                // append as start node
                p_src_list_node->p_prev = NULL;
                p_src_list_node->p_next = p_dest_list_node;
                p_dest_list_node->p_prev = p_src_list_node;

                p_list_dest->p_head = p_src_list_node;
            }
            else
            {
                p_dest_list_node->p_prev->p_next = p_src_list_node;
                p_src_list_node->p_prev = p_dest_list_node->p_prev;
                p_dest_list_node->p_prev = p_src_list_node;
                p_src_list_node->p_next = p_dest_list_node;
            }

            p_src_list_node = p_src_list_next_node;
        }
        else
        {
            p_dest_list_node = p_dest_list_node->p_next;
        }
    }

    if (p_src_list_node != NULL)
    {
        // append to the tail of the result list
        if (p_list_dest->p_head == NULL)
        {
            // result list is empty
            p_list_dest->p_head = p_list_src->p_head;
            p_list_dest->p_tail = p_list_src->p_tail;
        }
        else
        {
            p_list_dest->p_tail->p_next = p_src_list_node;
            p_src_list_node->p_prev = p_list_dest->p_tail;
            p_list_dest->p_tail = p_list_src->p_tail;
        }
    }

    p_list_dest->list_length += p_list_src->list_length;
}

// append source delete list (ascending order) to destination delete list (ascending order)
void merge_sorted_index_checkpoint_sys_info_list(DB_SYS_DATA_INFO_LIST_ENTRY_T *p_list_dest, DB_SYS_DATA_INFO_LIST_ENTRY_T *p_list_src)
{
    DB_SYS_DATA_INFO_LIST_NODE_T *p_dest_list_node, *p_src_list_node;

    p_dest_list_node = p_list_dest->p_head;
    p_src_list_node = p_list_src->p_head;

    while (p_dest_list_node && p_src_list_node)
    {
        HASH_VALUE_COMPARE_RESULT_E compare_result;
        compare_result = Hash_Api_Compare(p_dest_list_node->sys_data_info.sys_record.payload.index_key_hash, p_src_list_node->sys_data_info.sys_record.payload.index_key_hash);

        if (compare_result == HASH_VALUE_COMPARE_EQUAL)
        {
            if (p_dest_list_node->sys_data_info.sys_record.payload.index_seq_num > p_src_list_node->sys_data_info.sys_record.payload.index_seq_num)
            {
                // release src list node
                DB_SYS_DATA_INFO_LIST_NODE_T *p_src_next_list_node = p_src_list_node->p_next;

                Mema_Api_Free(MEMA_USER_FACILEDB, p_src_list_node);
                p_list_src->list_length--;

                p_src_list_node = p_src_next_list_node;
            }
            else
            {
                // release dest list node
                DB_SYS_DATA_INFO_LIST_NODE_T *p_dest_next_list_node = p_dest_list_node->p_next;

                if (p_dest_list_node->p_next)
                {
                    p_dest_list_node->p_next->p_prev = p_dest_list_node->p_prev;
                }

                if (p_dest_list_node->p_prev)
                {
                    p_dest_list_node->p_prev->p_next = p_dest_list_node->p_next;
                }

                Mema_Api_Free(MEMA_USER_FACILEDB, p_dest_list_node);
                p_list_dest->list_length--;

                p_dest_list_node = p_dest_next_list_node;
            }
        }
        else if (compare_result == HASH_VALUE_COMPARE_LEFT_GREATER)
        {
            // append p_prev of the dest list node
            DB_SYS_DATA_INFO_LIST_NODE_T *p_src_next_list_node = p_src_list_node->p_next;

            p_src_list_node->p_next = p_dest_list_node;
            p_src_list_node->p_prev = p_dest_list_node->p_prev;

            if (p_dest_list_node->p_prev)
            {
                p_dest_list_node->p_prev->p_next = p_src_list_node;
            }
            else
            {
                p_list_dest->p_head = p_src_list_node;
            }
            p_dest_list_node->p_prev = p_src_list_node;

            p_list_src->list_length--;
            p_list_dest->list_length++;
            p_src_list_node = p_src_next_list_node;
        }
        else
        {
            p_dest_list_node = p_dest_list_node->p_next;
        }
    }

    if (p_src_list_node)
    {
        if (p_list_dest->p_head)
        {
            p_src_list_node->p_prev = p_list_dest->p_tail;
            p_list_dest->p_tail->p_next = p_src_list_node;
            p_list_dest->p_tail = p_list_src->p_tail;
        }
        else
        {
            p_list_dest->p_head = p_src_list_node;
            p_list_dest->p_tail = p_list_src->p_tail;
        }

        p_list_dest->list_length += p_list_src->list_length;
    }
}

#if ENABLE_DB_INDEX
// This function must be called after setting db_directory_path.
bool get_db_index_directory_path(char *p_db_index_directory_path)
{
    const char index_directory_suffix[] = "index/";

    if ((strlen(db_context.db_directory_path) + strlen(index_directory_suffix)) > INDEX_FILE_PATH_MAX_LENGTH)
    {
        p_db_index_directory_path[0] = '\0';
        return false;
    }

    strncpy(p_db_index_directory_path, db_context.db_directory_path, INDEX_FILE_PATH_MAX_LENGTH);
    strcat(p_db_index_directory_path, index_directory_suffix);
    p_db_index_directory_path[INDEX_FILE_PATH_MAX_LENGTH] = '\0';

    return true;
}

// Default index key is db_set_name (DB_SET_INFO) + "_" + p_key (DB_RECORD_T).
// Align with "set_db_index_key" function
size_t get_db_index_key_size(uint32_t set_name_size, uint32_t record_key_size)
{
    // index key =  db_set_name + "_" + p_key (DB_RECORD_T).
    return (set_name_size + record_key_size + 2) * sizeof(uint8_t);
}

// index key is the identity of file that contains the index data.
// Default index key is db_set_name (DB_SET_INFO) + "_" + p_key (DB_RECORD_T).
// TODO: db_set_name is a fake string that doesn't contain '\0' at the end, and p_key is not expected to be a string all the time.
// TODO: to_printable or toString
void set_db_index_key(char *p_dest, void *p_db_set_name, uint32_t set_name_size, void *p_key, uint32_t key_size)
{
    memcpy(p_dest, p_db_set_name, set_name_size);
    p_dest[set_name_size] = '_';
    // TODO: toString(p_key)
    memcpy(p_dest + set_name_size + 1, p_key, key_size);

    p_dest[set_name_size + key_size + 1] = '\0';
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
    uint32_t result_data_num = 0;
    size_t index_key_size = get_db_index_key_size(p_db_set_info->set_name_size, p_db_record_info->db_record_properties.key_size);
    char *p_index_key = Mema_Api_Alloc(MEMA_USER_FACILEDB, index_key_size);

    set_db_index_key(p_index_key, p_db_set_info->p_set_name, p_db_set_info->set_name_size, p_db_record_info->db_record.p_key, p_db_record_info->db_record_properties.key_size);
    // check if index existed.
    if (!Index_Api_Index_Key_Exist(p_index_key))
    {
        DB_DATA_INFO_LIST_ENTRY_T db_data_info_list_entry;
        DB_DATA_INFO_LIST_NODE_T *p_current_node = NULL;

        db_data_info_list_entry_init(&db_data_info_list_entry);

        // search for all matched db_records
        search_db_data(p_db_set_info, p_db_record_info, DB_RECORD_VALUE_TYPE_COMPARE_ALL, &db_data_info_list_entry);
        result_data_num = db_data_info_list_entry.list_length;

        if (result_data_num > 0)
        {
            DB_SYS_DATA_INFO_LIST_NODE_T *p_db_index_checkpoint_sys_data_info_list_node = NULL;
            DB_SYS_DATA_INFO_T *p_db_index_checkpoint_sys_data_info = NULL;

            // index checkpoint
            if ((p_db_index_checkpoint_sys_data_info = get_db_index_checkpoint_sys_info(&(p_db_set_info->index_checkpoint_list_entry), p_index_key)))
            {
                // TODO: build & inherent existed index checkpoint
                assert(0);
            }
            else
            {
                p_db_index_checkpoint_sys_data_info_list_node = Mema_Api_Alloc(MEMA_USER_FACILEDB, sizeof(DB_SYS_DATA_INFO_LIST_NODE_T));
                db_sys_data_info_list_node_init(p_db_index_checkpoint_sys_data_info_list_node);

                p_db_index_checkpoint_sys_data_info = &(p_db_index_checkpoint_sys_data_info_list_node->sys_data_info);
                p_db_index_checkpoint_sys_data_info->sys_record.type = DB_SYS_RECORD_TYPE_INDEX_SEQ_NUM_CHECKPOINT;
                p_db_index_checkpoint_sys_data_info->sys_record.payload.index_key_hash = Hash((uint8_t *)p_index_key, strlen(p_index_key));
            }

            p_current_node = db_data_info_list_entry.p_head;
            for (uint32_t i = 0; i < db_data_info_list_entry.list_length; i++)
            {
                for (uint32_t j = 0; j < p_current_node->db_data_info.record_num; j++)
                {
                    if ((p_current_node->db_data_info.p_db_record_info[j].db_record_properties.key_size == p_db_record_info->db_record_properties.key_size) &&
                        (memcmp(p_current_node->db_data_info.p_db_record_info[j].db_record.p_key, p_db_record_info->db_record.p_key, p_db_record_info->db_record_properties.key_size) == 0) &&
                        (p_current_node->db_data_info.p_db_record_info[j].db_record_properties.record_value_type == p_db_record_info->db_record_properties.record_value_type))
                    {
                        DB_INDEX_PAYLOAD_T db_index_payload = {
                            .data_tag = p_current_node->db_data_info.data_tag,
                            .start_db_block_tag = p_current_node->db_data_info.start_db_block_tag};
                        insert_db_record_index(p_index_key, p_db_index_checkpoint_sys_data_info, &(p_current_node->db_data_info.p_db_record_info[j]), &db_index_payload);
                    }
                }

                p_current_node = p_current_node->p_next;
            }

            // update index checkpoint
            if (p_db_index_checkpoint_sys_data_info->block_tag != 0)
            {
                // TODO: build & inherent the index checkpoint seq_num
                assert(0);
            }
            else
            {
                // insert the list node into index checkpoint list and write the new index checkpoint sys data
                write_db_record_index_checkpoint_sys_data(p_db_set_info, p_db_index_checkpoint_sys_data_info);
                append_db_record_index_checkpoint_list_node(p_db_set_info, p_db_index_checkpoint_sys_data_info_list_node);

                update_to_db_set_properties_region(p_db_set_info);
            }
        }

        free_db_data_info_list(&db_data_info_list_entry);
    }

    Mema_Api_Free(MEMA_USER_FACILEDB, p_index_key);

    return result_data_num;
}

void insert_db_record_index(char *p_index_key, DB_SYS_DATA_INFO_T *p_db_index_checkpoint_sys_data_info, DB_RECORD_INFO_T *p_db_record_info, DB_INDEX_PAYLOAD_T *p_db_index_payload)
{
    // TODO: toString(p_set_name) and toString(p_key)
    void *p_record_value = p_db_record_info->db_record.p_value;
    void *p_index_id = NULL;
    INDEX_ID_TYPE_E index_id_type = get_db_index_id_type(p_db_record_info->db_record_properties.record_value_type);
    HASH_VALUE_T hash_value = 0;

    if (index_id_type != INDEX_ID_TYPE_INVALID)
    {
        uint32_t index_seq_num_result = 0;

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
        **  p_index_seq_num: the current index seq_num
        **  p_index_id: p_value (DB_RECORD_T)
        **  index_id_type: type (uint32 / string / ...)
        **  p_index_payload: a structure with the data offset / data tag / data start block tag / ...
        **  payload_size: sizeof the payload
        **  return value: the latest seq_num
        */
        index_seq_num_result = Index_Api_Insert_Element(p_index_key, p_db_index_checkpoint_sys_data_info->sys_record.payload.index_seq_num, p_index_id, index_id_type, p_db_index_payload, sizeof(DB_INDEX_PAYLOAD_T));

        if (index_seq_num_result == 0)
        {
            // TODO: rebuild and insert to index again
            assert(0);
        }
        else
        {
            p_db_index_checkpoint_sys_data_info->sys_record.payload.index_seq_num = index_seq_num_result;
        }
    }
}

DB_SYS_DATA_INFO_T *get_db_index_checkpoint_sys_info(DB_SYS_DATA_INFO_LIST_ENTRY_T *p_entry, char *p_index_key)
{
    HASH_VALUE_T index_key_hash = Hash((uint8_t *)p_index_key, strlen(p_index_key));
    DB_SYS_DATA_INFO_LIST_NODE_T *p_node = p_entry->p_head;

    for (uint32_t i = 0; i < p_entry->list_length && p_node; i++)
    {
        HASH_VALUE_COMPARE_RESULT_E compare_result = Hash_Api_Compare(index_key_hash, p_node->sys_data_info.sys_record.payload.index_key_hash);
        if (compare_result == HASH_VALUE_COMPARE_EQUAL)
        {
            return &(p_node->sys_data_info);
        }
        else if (compare_result == HASH_VALUE_COMPARE_RIGHT_GREATER)
        {
            break;
        }
        p_node = p_node->p_next;
    }

    return NULL;
}

void write_db_record_index_checkpoint_sys_data(DB_SET_INFO_T *p_db_set_info, DB_SYS_DATA_INFO_T *p_index_checkpoint_sys_info)
{
    DB_BLOCK_T db_block;
    uint64_t current_time = (uint64_t)get_current_time();
    uint8_t *p_block_data = db_block.block_data;

    // write sys data to disk
    db_block_init(&db_block);

    db_block.record_crc32 = Crc32_Api_Init();
    memcpy(p_block_data, &(p_index_checkpoint_sys_info->sys_record.type_32), sizeof(p_index_checkpoint_sys_info->sys_record.type_32));
    db_block.record_crc32 = Crc32_Api_Calc(db_block.record_crc32, &(p_index_checkpoint_sys_info->sys_record.type_32), sizeof(p_index_checkpoint_sys_info->sys_record.type_32));
    p_block_data += sizeof(p_index_checkpoint_sys_info->sys_record.type_32);
    memcpy(p_block_data, &(p_index_checkpoint_sys_info->sys_record.payload), sizeof(p_index_checkpoint_sys_info->sys_record.payload));
    db_block.record_crc32 = Crc32_Api_Calc(db_block.record_crc32, &(p_index_checkpoint_sys_info->sys_record.payload), sizeof(p_index_checkpoint_sys_info->sys_record.payload));

    db_block.block_tag = ++(p_db_set_info->db_set_properties.block_num);
    db_block.data_tag = 0; // sys block data tag is 0
    db_block.deleted = 0;
    db_block.valid_record_num = 1;
    db_block.created_time = current_time;
    db_block.modified_time = current_time;
    db_block.prev_block_tag = p_db_set_info->db_set_properties.latest_sys_block_tag;

    write_db_block(&db_block, p_db_set_info);

    p_db_set_info->db_set_properties.latest_sys_block_tag = db_block.block_tag;
    // update the modified time of the db_set_properties
    p_db_set_info->db_set_properties.modified_time = db_block.modified_time;

    // assign sys_data_info block_tag and create_time
    p_index_checkpoint_sys_info->block_tag = db_block.block_tag;
    p_index_checkpoint_sys_info->created_time = db_block.created_time;
}

void append_db_record_index_checkpoint_list_node(DB_SET_INFO_T *p_db_set_info, DB_SYS_DATA_INFO_LIST_NODE_T *p_index_checkpoint_sys_data_list_node)
{
    DB_SYS_DATA_INFO_LIST_ENTRY_T *p_entry = &(p_db_set_info->index_checkpoint_list_entry);
    DB_SYS_DATA_INFO_LIST_NODE_T *p_node = p_entry->p_head;

    // append to index checkpoint list
    while (p_node)
    {
        HASH_VALUE_COMPARE_RESULT_E compare = Hash_Api_Compare(p_node->sys_data_info.sys_record.payload.index_key_hash, p_index_checkpoint_sys_data_list_node->sys_data_info.sys_record.payload.index_key_hash);
        if (compare == HASH_VALUE_COMPARE_LEFT_GREATER)
        {
            break;
        }

        p_node = p_node->p_next;
    }

    if (p_node)
    {
        // append to p_node->p_prev
        p_index_checkpoint_sys_data_list_node->p_next = p_node;
        p_index_checkpoint_sys_data_list_node->p_prev = p_node->p_prev;

        if (p_node->p_prev)
        {
            p_node->p_prev->p_next = p_index_checkpoint_sys_data_list_node;
        }
        else
        {
            p_entry->p_head = p_index_checkpoint_sys_data_list_node;
        }
        p_node->p_prev = p_index_checkpoint_sys_data_list_node;
    }
    else
    {
        // append to tail
        p_index_checkpoint_sys_data_list_node->p_prev = p_entry->p_tail;
        if (p_entry->p_tail)
        {
            p_entry->p_tail->p_next = p_index_checkpoint_sys_data_list_node;
        }
        else
        {
            // append to empty list
            p_entry->p_head = p_index_checkpoint_sys_data_list_node;
        }
        p_entry->p_tail = p_index_checkpoint_sys_data_list_node;
    }
    p_entry->list_length++;
}

#endif // ENABLE_DB_INDEX
