#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

#if defined(_POSIX_VERSION)
#define IS_POSIX_API_SUPPORT (1)
#else
#define IS_POSIX_API_SUPPORT (0)
#endif

#if IS_POSIX_API_SUPPORT
#include <fcntl.h>
#include <pthread.h>
#else
// #error "POSIX API is not supported. Please use a POSIX compliant system."
#endif

#include "index.h"
#include "index_id_type.h"

#ifndef INDEX_INFO_INSTANCE_NUM
#define INDEX_INFO_INSTANCE_NUM (1)
#endif

#define INDEX_FILE_OPEN_CHECK_TIMEOUT (30)
#define INDEX_FILE_OPEN_CHECK_INTERVAL_US (100000) // 100ms

// Enumeration definition
typedef enum
{
    INDEX_INFO_STATUS_RELEASED,
    INDEX_INFO_STATUS_STARTING,
    INDEX_INFO_STATUS_CLOSING,
    INDEX_INFO_STATUS_READY,
    INDEX_INFO_STATUS_WRITING,
    INDEX_INFO_STATUS_READING,
} INDEX_INFO_STATUS_E;

typedef enum
{
    INDEX_CONTEXT_STATUS_UNUSED,
    INDEX_CONTEXT_STATUS_INITIALIZING,
    INDEX_CONTEXT_STATUS_CLOSING,
    INDEX_CONTEXT_STATUS_READY
} INDEX_CONTEXT_STATUS_E;

// Structure definition
typedef struct
{
    void *p_index_id;
    uint8_t index_payload[INDEX_PAYLOAD_SIZE / sizeof(uint8_t)];
} INDEX_ELEMENT_T;

typedef struct
{
    // tag is a 1-based number, tag=0 means null
    uint32_t tag;

    uint32_t level;
    uint32_t length; // length of elements array

    uint32_t parent_tag;
    uint32_t next_tag;
    // child_tag is a 1-based number and initilized as 0.
    uint32_t child_tag[INDEX_CHILD_TAG_ORDER];

    INDEX_ELEMENT_T elements[INDEX_ORDER];
} INDEX_NODE_T;

typedef struct
{
    // tag_num == max(tag)
    uint32_t tag_num;
    uint32_t root_tag;
    union
    {
        INDEX_ID_TYPE_E index_id_type;
        uint32_t index_id_type_32; // write and read as uint32
    };
    // HASH_VALUE_T integrity;

    uint32_t key_size; // bytes
    uint8_t *p_key;    // size = key_size
} INDEX_PROPERTIES_T;

typedef struct
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t mutex;
    pthread_cond_t read_cond;
    pthread_cond_t write_cond;
    pthread_cond_t close_cond;
#endif
    uint32_t writer_waiting_count; // number of writers is writing (at most one) or waiting to write
    uint32_t reader_waiting_count;
    uint32_t reader_count; // readers can read simultaneously
} INDEX_INFO_SYNC_T;

typedef struct
{
    FILE *index_file;
    INDEX_PROPERTIES_T index_properties;
    INDEX_INFO_SYNC_T index_info_sync;

    INDEX_INFO_STATUS_E status;
} INDEX_INFO_T;

typedef struct
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t mutex; // TODO: scenario condition. e.g. close wait for all load
#endif
    INDEX_CONTEXT_STATUS_E status; // TODO: move to INDEX_CONTEXT_T
} INDEX_CONTEXT_SYNC_T;

// TODO: using singleton context
typedef struct
{
    INDEX_CONTEXT_STATUS_E status;
    INDEX_INFO_T *p_index_info;
    char index_directory_path[INDEX_FILE_PATH_BUFFER_LENGTH];
} INDEX_CONTEXT_T;

// End of structure definition

// Static Varialbes
static INDEX_INFO_T index_info_instance[INDEX_INFO_INSTANCE_NUM] = {
    {.index_info_sync = {
#if IS_POSIX_API_SUPPORT
         .mutex = PTHREAD_MUTEX_INITIALIZER,
         .read_cond = PTHREAD_COND_INITIALIZER,
         .write_cond = PTHREAD_COND_INITIALIZER,
         .close_cond = PTHREAD_COND_INITIALIZER
#endif
     },
     .status = INDEX_INFO_STATUS_RELEASED}};
static char index_directory_path[INDEX_FILE_PATH_BUFFER_LENGTH] = {0};
static INDEX_CONTEXT_SYNC_T index_context_sync = {
#if IS_POSIX_API_SUPPORT
    .mutex = PTHREAD_MUTEX_INITIALIZER,
#endif
    .status = INDEX_CONTEXT_STATUS_UNUSED};

// End of Static Variables

// Local function declaration
static inline void lock_index_context_sync();
static inline void unlock_index_context_sync();
void update_index_context_status(INDEX_CONTEXT_STATUS_E new_status);
static inline bool check_index_context_status(INDEX_CONTEXT_STATUS_E status);
bool set_index_directory_path(char *p_index_directory_path);
bool is_index_key_file_exists(char *p_index_key);
void get_index_file_path_by_index_key(char *p_index_file_path, char *p_index_key);

void index_info_instances_init();
void index_info_instances_close();
INDEX_INFO_T *request_and_lock_released_index_info_instance();
void create_new_index_file_format(INDEX_INFO_T *p_index_info, uint8_t *p_key, uint32_t key_size, INDEX_ID_TYPE_E index_id_type);

void index_info_init(INDEX_INFO_T *p_index_info);
void close_index_info(INDEX_INFO_T *index_info);
INDEX_INFO_T *query_and_lock_index_info_loaded(char *p_index_key, uint32_t index_key_size, INDEX_ID_TYPE_E index_id_type);
INDEX_INFO_T *load_and_lock_index_info(char *p_key, INDEX_ID_TYPE_E index_id_type);

void index_info_sync_init(INDEX_INFO_SYNC_T *p_index_info_sync);
static void inline index_info_file_lock_write(INDEX_INFO_T *p_index_info);
static void inline index_info_file_unlock_write(INDEX_INFO_T *p_index_info);
static void inline index_info_file_lock_read(INDEX_INFO_T *p_index_info);
static void inline index_info_file_unlock_read(INDEX_INFO_T *p_index_info);
static void lock_index_info_sync(INDEX_INFO_T *p_index_info);
static void unlock_index_info_sync(INDEX_INFO_T *p_index_info);
void update_index_info_status(INDEX_INFO_T *p_index_info, INDEX_INFO_STATUS_E new_status);
void update_index_info_status_from_reading(INDEX_INFO_T *p_index_info);
static inline bool check_index_info_status(INDEX_INFO_T *p_index_info, INDEX_INFO_STATUS_E target_status);
static inline bool check_index_info_status_available(INDEX_INFO_T *p_index_info);
static inline void index_info_sync_close_wait(INDEX_INFO_T *p_index_info);
static inline void index_info_sync_write_wait(INDEX_INFO_T *p_index_info);
static inline void index_info_sync_read_wait(INDEX_INFO_T *p_index_info);
static inline void index_info_sync_write_unblock(INDEX_INFO_T *p_index_info);
static inline void index_info_sync_read_unblock(INDEX_INFO_T *p_index_info);

void index_properties_init(INDEX_PROPERTIES_T *p_index_properties);
void allocate_index_properties_resources(INDEX_PROPERTIES_T *p_index_properties, uint32_t key_size);
void read_index_properties(INDEX_INFO_T *p_index_info);
void write_index_properties(INDEX_INFO_T *p_index_info);
size_t get_index_properties_size(INDEX_PROPERTIES_T *p_index_properties);
void free_index_properties_resources(INDEX_PROPERTIES_T *p_index_properties);
void close_index_properties(INDEX_PROPERTIES_T *p_index_properties);

void index_node_init(INDEX_NODE_T *p_index_node, uint32_t tag);
void write_index_node(INDEX_INFO_T *p_index_info, INDEX_NODE_T *p_index_node);
bool read_index_node(INDEX_INFO_T *p_index_info, uint32_t tag, INDEX_NODE_T *p_index_node);
off_t get_node_offset(INDEX_PROPERTIES_T *p_index_properties, uint32_t tag);
void free_index_node_resources(INDEX_NODE_T *p_index_node);

void index_element_init(INDEX_ELEMENT_T *p_index_element);
void setup_index_element(INDEX_ELEMENT_T *p_index_element, void *p_target, INDEX_ID_TYPE_E index_id_type, void *p_payload, uint32_t payload_size);
void write_index_elements(INDEX_INFO_T *p_index_info, uint32_t tag, uint32_t index_element_position, uint32_t write_length, INDEX_ELEMENT_T *p_index_element);
void read_index_elements(INDEX_INFO_T *p_index_info, const uint32_t tag, const uint32_t index_element_position, const uint32_t read_length, INDEX_ELEMENT_T *p_index_element);
off_t get_index_element_offset(INDEX_PROPERTIES_T *p_index_properties, uint32_t tag, uint32_t index_element_position);
void allocate_index_element_resources(INDEX_ELEMENT_T *p_index_element, uint32_t index_id_size);
void free_index_element_resources(INDEX_ELEMENT_T *p_index_element);
void deep_copy_index_element(INDEX_ELEMENT_T *p_dest_index_element, INDEX_ELEMENT_T *p_src_index_element, INDEX_ID_TYPE_E index_id_type);
uint32_t find_element_position_in_the_node(INDEX_NODE_T *p_index_node, INDEX_ELEMENT_T *p_index_element, INDEX_ID_TYPE_E index_id_type);
void split_index_elements_into_two_index_node(INDEX_ELEMENT_T *p_index_element_buffer, uint32_t index_element_buffer_length, INDEX_NODE_T *p_index_node_current, INDEX_NODE_T *p_index_node_sibling);
void split_child_tags_into_two_index_node(INDEX_INFO_T *p_index_info, uint32_t *p_child_tag_buffer, uint32_t child_tag_buffer_length, INDEX_NODE_T *p_index_node_current, INDEX_NODE_T *p_index_node_sibling);
void insert_index_element_handler(INDEX_INFO_T *p_index_info, INDEX_NODE_T *p_index_node, INDEX_ELEMENT_T *p_index_element, uint32_t new_child_tag);
void insert_index_element(INDEX_INFO_T *p_index_info, uint32_t tag, INDEX_ELEMENT_T *p_index_element);
void *search_index_element(INDEX_INFO_T *p_index_info, uint32_t tag, INDEX_ELEMENT_T *p_target_index_element, uint32_t *result_length);
uint8_t *search_index_element_handler(INDEX_INFO_T *p_index_info, INDEX_NODE_T *p_index_node, INDEX_ELEMENT_T *p_target_index_element, uint32_t *result_length);
// End of local function declaration

void Index_Api_Init(char *p_index_directory_path)
{
    char temp_index_directory_path[INDEX_FILE_PATH_BUFFER_LENGTH] = {0};

    strncpy(temp_index_directory_path, p_index_directory_path, INDEX_FILE_PATH_MAX_LENGTH);
    temp_index_directory_path[INDEX_FILE_PATH_MAX_LENGTH] = '\0';

    // lock_index_context_init();
    lock_index_context_sync();
    update_index_context_status(INDEX_CONTEXT_STATUS_INITIALIZING);

    // Initialize index dorectory path.
    set_index_directory_path(temp_index_directory_path);
    // Initialize index info instances.
    index_info_instances_init();

    // unlock_index_context_init();
    update_index_context_status(INDEX_CONTEXT_STATUS_READY);
    unlock_index_context_sync();
}

void Index_Api_Close()
{
    // lock_index_context_close();
    lock_index_context_sync();
    update_index_context_status(INDEX_CONTEXT_STATUS_CLOSING);

    index_info_instances_close();

    // unlock_index_context_close();
    update_index_context_status(INDEX_CONTEXT_STATUS_UNUSED);
    unlock_index_context_sync();
}

bool Index_Api_Index_Key_Exist(char *p_index_key)
{
    bool result = false;
    // INDEX_CONTEXT_STATUS_E *p_index_context_status = &(index_context_sync.status);
    char temp_index_key[INDEX_FILE_PATH_BUFFER_LENGTH] = {0};

    strncpy(temp_index_key, p_index_key, INDEX_FILE_PATH_MAX_LENGTH);
    temp_index_key[INDEX_FILE_PATH_MAX_LENGTH] = '\0';

    lock_index_context_sync();
    if (check_index_context_status(INDEX_CONTEXT_STATUS_READY) == true)
    {
        result = is_index_key_file_exists(temp_index_key);
    }
    unlock_index_context_sync();

    return result;
}

void Index_Api_Insert_Element(char *p_index_key, void *p_index_id, INDEX_ID_TYPE_E index_id_type, void *p_index_payload, uint32_t payload_size)
{
    INDEX_INFO_T *p_index_info = NULL;
    INDEX_ELEMENT_T index_element;

    index_element_init(&index_element);
    setup_index_element(&index_element, p_index_id, index_id_type, p_index_payload, payload_size);

    lock_index_context_sync();
    if (check_index_context_status(INDEX_CONTEXT_STATUS_READY) == false)
    {
        unlock_index_context_sync();
        free_index_element_resources(&index_element);
        return;
    }

    p_index_info = load_and_lock_index_info(p_index_key, index_id_type);
    unlock_index_context_sync();

    index_info_sync_write_wait(p_index_info);
    update_index_info_status(p_index_info, INDEX_INFO_STATUS_WRITING);
    index_info_file_lock_write(p_index_info);
    unlock_index_info_sync(p_index_info);

    insert_index_element(p_index_info, p_index_info->index_properties.root_tag, &index_element);

    lock_index_info_sync(p_index_info);
    index_info_file_unlock_write(p_index_info);
    update_index_info_status(p_index_info, INDEX_INFO_STATUS_READY);
    index_info_sync_write_unblock(p_index_info);
    unlock_index_info_sync(p_index_info);

    free_index_element_resources(&index_element);
}

// return value: result array
// result_length: integer, number of results in result array
void *Index_Api_Search_Equal(char *p_index_key, void *p_target_index_id, INDEX_ID_TYPE_E index_id_type, uint32_t *p_result_length)
{
    INDEX_INFO_T *p_index_info = NULL;
    uint32_t root_tag = 0;
    INDEX_ELEMENT_T target_index_element;
    void *result = NULL;

    index_element_init(&target_index_element);
    setup_index_element(&target_index_element, p_target_index_id, index_id_type, NULL, 0);

    lock_index_context_sync();

    // Check if index key exists or not.
    if (!(is_index_key_file_exists(p_index_key)))
    {
        unlock_index_context_sync();

        free_index_element_resources(&target_index_element);
        *p_result_length = 0;
        return NULL;
    }

    p_index_info = load_and_lock_index_info(p_index_key, index_id_type);
    unlock_index_context_sync();

    index_info_sync_read_wait(p_index_info);
    index_info_file_lock_read(p_index_info);
    update_index_info_status(p_index_info, INDEX_INFO_STATUS_READING);
    unlock_index_info_sync(p_index_info);

    root_tag = p_index_info->index_properties.root_tag;
    result = search_index_element(p_index_info, root_tag, &target_index_element, p_result_length);

    lock_index_info_sync(p_index_info);
    index_info_file_unlock_read(p_index_info);
    update_index_info_status_from_reading(p_index_info);
    index_info_sync_read_unblock(p_index_info);
    unlock_index_info_sync(p_index_info);

    free_index_element_resources(&target_index_element);

    return result;
}

void Index_Api_Free_Search_Result(void *p_result)
{
    free(p_result);
}

static inline void lock_index_context_sync()
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(index_context_sync.mutex);
    pthread_mutex_lock(p_mutex);
#endif
}

static inline void unlock_index_context_sync()
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(index_context_sync.mutex);
    pthread_mutex_unlock(p_mutex);
#endif
}

// Lock the index_context before using this funtion.
void update_index_context_status(INDEX_CONTEXT_STATUS_E new_status)
{
    INDEX_CONTEXT_STATUS_E *p_status = &(index_context_sync.status);
    INDEX_CONTEXT_STATUS_E current_status = *p_status;
    bool is_valid_transition = false;

    switch (current_status)
    {
    case INDEX_CONTEXT_STATUS_UNUSED:
    {
        if (new_status == INDEX_CONTEXT_STATUS_INITIALIZING)
        {
            is_valid_transition = true;
        }
        break;
    }
    case INDEX_CONTEXT_STATUS_INITIALIZING:
    {
        if (new_status == INDEX_CONTEXT_STATUS_READY)
        {
            is_valid_transition = true;
        }
        break;
    }
    case INDEX_CONTEXT_STATUS_CLOSING:
    {
        if (new_status == INDEX_CONTEXT_STATUS_UNUSED)
        {
            is_valid_transition = true;
        }
        break;
    }
    case INDEX_CONTEXT_STATUS_READY:
    {
        if (new_status == INDEX_CONTEXT_STATUS_CLOSING)
        {
            is_valid_transition = true;
        }
        break;
    }
    }

    if (is_valid_transition)
    {
        *p_status = new_status;
    }
    else
    {
        assert(0);
    }
}

static inline bool check_index_context_status(INDEX_CONTEXT_STATUS_E status)
{
    return (index_context_sync.status == status);
}

bool set_index_directory_path(char *p_index_directory_path)
{
    struct stat index_directory_stat;

    strncpy(index_directory_path, p_index_directory_path, INDEX_FILE_PATH_MAX_LENGTH);
    index_directory_path[INDEX_FILE_PATH_MAX_LENGTH] = '\0';

    // add (or replace the last char) '/' at the end of the path if the last char is not '/'
    if (index_directory_path[strlen(index_directory_path) - 1] != '/')
    {
        if (strlen(index_directory_path) == INDEX_FILE_PATH_MAX_LENGTH)
        {
            index_directory_path[INDEX_FILE_PATH_MAX_LENGTH - 1] = '/';
            index_directory_path[INDEX_FILE_PATH_MAX_LENGTH] = '\0';
        }
        else
        {
            strcat(index_directory_path, "/");
        }
    }

    // Check if index directory exists or not.
    if ((stat(index_directory_path, &index_directory_stat) != 0) || (!(S_ISDIR(index_directory_stat.st_mode))))
    {
        char temp_path_buffer[INDEX_FILE_PATH_BUFFER_LENGTH] = {0};

        // Index directory doesen't exist, create it.
        for (char *p = strchr(index_directory_path + 1, '/'); p != NULL; p = strchr(p + 1, '/'))
        {
            strncpy(temp_path_buffer, index_directory_path, p - index_directory_path);
            temp_path_buffer[p - index_directory_path + 1] = '\0';

            if (stat(temp_path_buffer, &index_directory_stat) != 0 || !(S_ISDIR(index_directory_stat.st_mode)))
            {
                if (mkdir(temp_path_buffer, 0755) == -1)
                {
                    perror("Create index directory fail");
                    return false;
                }
            }
        }
    }

    return true;
}

bool is_index_key_file_exists(char *p_index_key)
{
    char index_file_path[INDEX_FILE_PATH_BUFFER_LENGTH] = {0};
    get_index_file_path_by_index_key(index_file_path, p_index_key);

    if ((index_file_path[0] != '\0') && (access(index_file_path, F_OK) == 0))
    {
        // Index file exists
        return true;
    }
    else
    {
        // Index file doesn't exist.
        return false;
    }
}

void get_index_file_path_by_index_key(char *p_index_file_path, char *p_index_key)
{
    // Default: index_directory_path/index_key.index
    // TODO: check p_key contains sensitive keywords. e.g. '/'

    char file_extension[] = ".faciledb_index";

    if (strlen(index_directory_path) + strlen(p_index_key) + strlen(file_extension) > INDEX_FILE_PATH_MAX_LENGTH)
    {
        p_index_file_path[0] = '\0';
    }
    else
    {
        strcpy(p_index_file_path, index_directory_path);
        strcat(p_index_file_path, p_index_key);
        strcat(p_index_file_path, file_extension);
        p_index_file_path[INDEX_FILE_PATH_MAX_LENGTH] = '\0';
    }
}

// Initialize all index info instances when index_context_status is INDEX_CONTEXT_STATUS_UNUSED.
void index_info_instances_init()
{
    for (uint32_t i = 0; i < INDEX_INFO_INSTANCE_NUM; i++)
    {
        lock_index_info_sync(&(index_info_instance[i]));

        index_info_init(&(index_info_instance[i]));

        unlock_index_info_sync(&(index_info_instance[i]));
    }
}

void index_info_instances_close()
{
    for (uint32_t i = 0; i < INDEX_INFO_INSTANCE_NUM; i++)
    {
        INDEX_INFO_T *p_index_info = &(index_info_instance[i]);

        lock_index_info_sync(p_index_info);

        if (check_index_info_status(p_index_info, INDEX_INFO_STATUS_RELEASED))
        {
            unlock_index_info_sync(p_index_info);
            continue;
        }

        index_info_sync_close_wait(p_index_info);
        update_index_info_status(p_index_info, INDEX_INFO_STATUS_CLOSING);

        close_index_info(p_index_info);

        update_index_info_status(p_index_info, INDEX_INFO_STATUS_RELEASED);
        unlock_index_info_sync(p_index_info);
    }
}

// todo: using fifo queue or lru algorithm.
INDEX_INFO_T *request_and_lock_released_index_info_instance()
{
    INDEX_INFO_T *p_index_info = NULL;
#if INDEX_INFO_INSTANCE_NUM == 1
    p_index_info = &(index_info_instance[0]);
    lock_index_info_sync(p_index_info);

    if (check_index_info_status(p_index_info, INDEX_INFO_STATUS_RELEASED) == false)
    {
        index_info_sync_close_wait(p_index_info);
        update_index_info_status(p_index_info, INDEX_INFO_STATUS_CLOSING);
        close_index_info(p_index_info);
        update_index_info_status(p_index_info, INDEX_INFO_STATUS_RELEASED);
    }

    return p_index_info;
#endif
}

void create_new_index_file_format(INDEX_INFO_T *p_index_info, uint8_t *p_key, uint32_t key_size, INDEX_ID_TYPE_E index_id_type)
{
    INDEX_PROPERTIES_T *p_index_properties = &(p_index_info->index_properties);
    INDEX_NODE_T first_node;

    allocate_index_properties_resources(p_index_properties, key_size);

    p_index_properties->p_key = malloc(key_size * sizeof(uint8_t));
    memcpy(p_index_properties->p_key, p_key, key_size);
    p_index_properties->key_size = key_size;
    p_index_properties->root_tag = 0;
    // insert an empty node with node tag: 1
    p_index_properties->tag_num = 1;
    p_index_properties->index_id_type = index_id_type;

    index_node_init(&first_node, p_index_properties->tag_num);
    p_index_properties->root_tag = first_node.tag;

    // write to file
    write_index_properties(p_index_info);
    write_index_node(p_index_info, &first_node);

    free_index_node_resources(&first_node);
}

bool read_and_check_index_file_format(INDEX_INFO_T *p_index_info, uint8_t *p_key, uint32_t key_size, INDEX_ID_TYPE_E index_id_type)
{
    INDEX_PROPERTIES_T *p_index_properties = &(p_index_info->index_properties);
    // create a temporary index_properties to calcualte the size.
    INDEX_PROPERTIES_T expected_index_properties = {.key_size = key_size};
    off_t expected_index_properties_size = get_index_properties_size(&expected_index_properties);

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_index_info->index_file);
    off_t file_size = lseek(fd, 0, SEEK_END);

    if (file_size >= expected_index_properties_size)
    {
        read_index_properties(p_index_info);

        if ((p_index_properties->key_size == key_size) && (memcmp(p_index_properties->p_key, p_key, key_size) == 0) && (p_index_properties->index_id_type == index_id_type))
        {
            return true;
        }
        else
        {
            free_index_properties_resources(p_index_properties);
            index_properties_init(p_index_properties);
            return false;
        }
    }
    else
    {
        return false;
    }
#endif
}

void index_info_init(INDEX_INFO_T *p_index_info)
{
    p_index_info->index_file = NULL;

    index_properties_init(&(p_index_info->index_properties));
    index_info_sync_init(&(p_index_info->index_info_sync));
}

void close_index_info(INDEX_INFO_T *p_index_info)
{
    // writeback_index_properties();
    close_index_properties(&(p_index_info->index_properties));

    if (p_index_info->index_file != NULL)
    {
        fclose(p_index_info->index_file);
        p_index_info->index_file = NULL;
    }
}

INDEX_INFO_T *query_and_lock_index_info_loaded(char *p_index_key, uint32_t index_key_size, INDEX_ID_TYPE_E index_id_type)
{
    INDEX_INFO_T *p_index_info = NULL;

#if (INDEX_INFO_INSTANCE_NUM == 1)
    p_index_info = &(index_info_instance[0]);

    lock_index_info_sync(p_index_info);
    if (check_index_info_status_available(p_index_info))
    {
        uint32_t min_key_compare_length = (index_key_size > p_index_info->index_properties.key_size) ? (p_index_info->index_properties.key_size) : (index_key_size);
        if ((index_key_size != p_index_info->index_properties.key_size) || (memcmp(p_index_info->index_properties.p_key, p_index_key, min_key_compare_length) != 0) || (index_info_instance[0].index_properties.index_id_type != index_id_type))
        {
            unlock_index_info_sync(p_index_info);
            p_index_info = NULL;
        }
    }
    else
    {
        unlock_index_info_sync(p_index_info);
        p_index_info = NULL;
    }

    return p_index_info;
#endif
}

INDEX_INFO_T *load_and_lock_index_info(char *p_key, INDEX_ID_TYPE_E index_id_type)
{
    INDEX_INFO_T *p_index_info = NULL;
    char index_file_path[INDEX_FILE_PATH_BUFFER_LENGTH] = {0};

    p_index_info = query_and_lock_index_info_loaded(p_key, strlen(p_key), index_id_type);
    if (p_index_info != NULL)
    {
        return p_index_info;
    }

    p_index_info = request_and_lock_released_index_info_instance();
    index_info_init(p_index_info);
    update_index_info_status(p_index_info, INDEX_INFO_STATUS_STARTING);

    get_index_file_path_by_index_key(index_file_path, p_key);

#if IS_POSIX_API_SUPPORT
    // Create index file if not exist.
    int fd = open(index_file_path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd >= 0)
    {
        // File created successfully
        p_index_info->index_file = fdopen(fd, "wb+");
        if (p_index_info->index_file == NULL)
        {
            // fdopen failed
            perror("Index file unavailable: ");

            close(fd);
            unlock_index_info_sync(p_index_info);
            return NULL;
        }

        // Write index_properties and first_node
        index_info_file_lock_write(p_index_info);
        create_new_index_file_format(p_index_info, (uint8_t *)p_key, strlen(p_key), index_id_type);
        index_info_file_unlock_write(p_index_info);
    }
    else if (errno == EEXIST)
    {
        bool timeout = true;

        // File exists
        fd = open(index_file_path, O_RDWR);
        if (fd < 0)
        {
            // Open existing file failed
            perror("Index file unavailable: ");

            unlock_index_info_sync(p_index_info);
            return NULL;
        }

        // Check if index_properties and first node write done.
        p_index_info->index_file = fdopen(fd, "rb+");
        if (p_index_info->index_file == NULL)
        {
            // fdopen failed
            perror("Index file unavailable: ");

            close(fd);
            unlock_index_info_sync(p_index_info);
            return NULL;
        }

        // Read index_properties from file.
        // Sleep a while and try again
        for (uint32_t check_time = 0; check_time < INDEX_FILE_OPEN_CHECK_TIMEOUT; check_time++)
        {
            index_info_file_lock_read(p_index_info);
            if (read_and_check_index_file_format(p_index_info, (uint8_t *)p_key, strlen(p_key), index_id_type) == true)
            {
                index_info_file_unlock_read(p_index_info);
                timeout = false;
                break;
            }
            else
            {
                // wait and retry
                index_info_file_unlock_read(p_index_info);
                usleep(INDEX_FILE_OPEN_CHECK_INTERVAL_US);
            }
        }

        if (timeout)
        {
            // TODO: error message
            assert(0);
        }
    }
    else
    {
        // Open file failed
        perror("Index file unavailable: ");

        unlock_index_info_sync(p_index_info);
        return NULL;
    }
#else  // IS_POSIX_API_SUPPORT
    // TODO: TOCTOU（Time Of Check To Time Of Use）issue here
    if (access(index_file_path, F_OK) == 0)
    {
        // Index file exists
        if (access(index_file_path, R_OK | W_OK) == 0)
        {
            // Index file readable & writable
            p_index_info->index_file = fopen(index_file_path, "rb+");
        }
        else
        {
            // index file unreadable or unwritable
            perror("Index file unavailable");

            unlock_index_info_sync(p_index_info);
            return NULL;
        }

        index_info_file_lock_read(p_index_info);
        read_index_properties(p_index_info);
        index_info_file_unlock_read(p_index_info);
    }
    else
    {
        // Index file doesn't exist. Create a new index file.
        p_index_info->index_file = fopen(index_file_path, "wb+");
        if (p_index_info->index_file == NULL)
        {
            // create index file failed.
            perror("Index file unavailable: ");

            unlock_index_info_sync(p_index_info);
            return NULL;
        }

        // Create new index file and write index_properties and first_node into it.
        index_info_file_lock_write(p_index_info);
        create_new_index_file_format(p_index_info, (uint8_t *)p_key, strlen(p_key), index_id_type);
        index_info_file_unlock_write(p_index_info);
    }
#endif // IS_POSIX_API_SUPPORT

    update_index_info_status(p_index_info, INDEX_INFO_STATUS_READY);
    return p_index_info;
}

// This function doesn't change index_info_status.
void index_info_sync_init(INDEX_INFO_SYNC_T *p_index_info_sync)
{
    // p_index_info_sync->index_info_status = INDEX_INFO_STATUS_RELEASED;
    p_index_info_sync->writer_waiting_count = 0;
    p_index_info_sync->reader_waiting_count = 0;
    p_index_info_sync->reader_count = 0;
}

static void inline index_info_file_lock_write(INDEX_INFO_T *p_index_info)
{
#if IS_POSIX_API_SUPPORT
    struct flock fl = {
        .l_type = F_WRLCK, // write lock
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0, // l_start = 0 && l_len = 0 means lock the whole file
        .l_pid = 0  // unused
    };
    int fd = fileno(p_index_info->index_file);

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

static void inline index_info_file_unlock_write(INDEX_INFO_T *p_index_info)
{
#if IS_POSIX_API_SUPPORT
    struct flock fl = {
        .l_type = F_UNLCK, // unlock
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0, // l_start = 0 && l_len = 0 means lock the whole file
        .l_pid = 0  // unused
    };
    int fd = fileno(p_index_info->index_file);

    // unlock doesn't need to wait.
    // return value: -1 means error.
    if (fcntl(fd, F_SETLK, &fl) == -1)
    {
        // TODO: error handling
        assert(0);
    }
#endif
}

static void inline index_info_file_lock_read(INDEX_INFO_T *p_index_info)
{
    uint32_t *p_reader_count = &(p_index_info->index_info_sync.reader_count);
#if IS_POSIX_API_SUPPORT
    struct flock fl = {
        .l_type = F_RDLCK, // read lock
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0, // l_start = 0 && l_len = 0 means lock the whole file
        .l_pid = 0  // unused
    };
    int fd = fileno(p_index_info->index_file);

    if (*p_reader_count == 1)
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

static void inline index_info_file_unlock_read(INDEX_INFO_T *p_index_info)
{
    uint32_t *p_reader_count = &(p_index_info->index_info_sync.reader_count);
#if IS_POSIX_API_SUPPORT
    struct flock fl = {
        .l_type = F_UNLCK, // unlock
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0, // l_start = 0 && l_len = 0 means lock the whole file
        .l_pid = 0  // unused
    };
    int fd = fileno(p_index_info->index_file);

    if (*p_reader_count == 1)
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

void update_index_info_status(INDEX_INFO_T *p_index_info, INDEX_INFO_STATUS_E new_status)
{
    INDEX_INFO_STATUS_E *p_status = &(p_index_info->status);
    INDEX_INFO_STATUS_E current_status = *p_status;
    bool is_valid_transition = false;

    switch (current_status)
    {
    case INDEX_INFO_STATUS_RELEASED:
    {
        if (new_status == INDEX_INFO_STATUS_STARTING)
        {
            is_valid_transition = true;
        }
        break;
    }
    case INDEX_INFO_STATUS_STARTING:
    {
        if (new_status == INDEX_INFO_STATUS_READY)
        {
            is_valid_transition = true;
        }
        break;
    }
    case INDEX_INFO_STATUS_CLOSING:
    {
        if (new_status == INDEX_INFO_STATUS_RELEASED)
        {
            is_valid_transition = true;
        }
        break;
    }
    case INDEX_INFO_STATUS_READY:
    {
        if (new_status == INDEX_INFO_STATUS_CLOSING || new_status == INDEX_INFO_STATUS_WRITING || new_status == INDEX_INFO_STATUS_READING)
        {
            is_valid_transition = true;
        }
        break;
    }
    case INDEX_INFO_STATUS_WRITING:
    {
        if (new_status == INDEX_INFO_STATUS_READY)
        {
            is_valid_transition = true;
        }
        break;
    }
    case INDEX_INFO_STATUS_READING:
    {
        if (new_status == INDEX_INFO_STATUS_READING || new_status == INDEX_INFO_STATUS_READY)
        {
            is_valid_transition = true;
        }
        break;
    }
    }

    if (is_valid_transition)
    {
        *p_status = new_status;
    }
    else
    {
        assert(0);
    }
}

void update_index_info_status_from_reading(INDEX_INFO_T *p_index_info)
{
    assert(check_index_info_status(p_index_info, INDEX_INFO_STATUS_READING) == true);

    uint32_t *p_reader_count = &(p_index_info->index_info_sync.reader_count);
    if (*p_reader_count > 1)
    {
        update_index_info_status(p_index_info, INDEX_INFO_STATUS_READING);
    }
    else
    {
        update_index_info_status(p_index_info, INDEX_INFO_STATUS_READY);
    }
}

static inline bool check_index_info_status(INDEX_INFO_T *p_index_info, INDEX_INFO_STATUS_E target_status)
{
    return (p_index_info->status == target_status);
}

static inline bool check_index_info_status_available(INDEX_INFO_T *p_index_info)
{
    return (p_index_info->status >= INDEX_INFO_STATUS_READY);
}

static void lock_index_info_sync(INDEX_INFO_T *p_index_info)
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_index_info_mutex = &(p_index_info->index_info_sync.mutex);
    pthread_mutex_lock(p_index_info_mutex);
#endif
}

static void unlock_index_info_sync(INDEX_INFO_T *p_index_info)
{
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_index_info_mutex = &(p_index_info->index_info_sync.mutex);
    pthread_mutex_unlock(p_index_info_mutex);
#endif
}

static inline void index_info_sync_close_wait(INDEX_INFO_T *p_index_info)
{
    uint32_t *p_writer_waiting_count = &(p_index_info->index_info_sync.writer_waiting_count);
    uint32_t *p_reader_count = &(p_index_info->index_info_sync.reader_count);
    uint32_t *p_reader_waiting_count = &(p_index_info->index_info_sync.reader_waiting_count);
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_index_info_mutex = &(p_index_info->index_info_sync.mutex);
    pthread_cond_t *p_close_cond = &(p_index_info->index_info_sync.close_cond);

    // using while loop for spurious wakeup
    while (check_index_info_status(p_index_info, INDEX_INFO_STATUS_READY) == false || *p_writer_waiting_count > 0 || *p_reader_count > 0 || *p_reader_waiting_count > 0)
    {
        pthread_cond_wait(p_close_cond, p_index_info_mutex);
    }
#endif
}

static inline void index_info_sync_write_wait(INDEX_INFO_T *p_index_info)
{
    uint32_t *p_writer_waiting_count = &(p_index_info->index_info_sync.writer_waiting_count);
    (*p_writer_waiting_count)++;
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_index_info->index_info_sync.mutex);
    pthread_cond_t *p_write_cond = &(p_index_info->index_info_sync.write_cond);

    while (check_index_info_status(p_index_info, INDEX_INFO_STATUS_READY) == false)
    {
        pthread_cond_wait(p_write_cond, p_mutex);
    }
#endif
    (*p_writer_waiting_count)--;
}

static inline void index_info_sync_read_wait(INDEX_INFO_T *p_index_info)
{
    uint32_t *p_reader_waiting_count = &(p_index_info->index_info_sync.reader_waiting_count);
    uint32_t *p_reader_count = &(p_index_info->index_info_sync.reader_count);
    uint32_t *p_writer_waiting_count = &(p_index_info->index_info_sync.writer_waiting_count);

    (*p_reader_waiting_count)++;
#if IS_POSIX_API_SUPPORT
    pthread_mutex_t *p_mutex = &(p_index_info->index_info_sync.mutex);
    pthread_cond_t *p_read_cond = &(p_index_info->index_info_sync.read_cond);

    // priority: write > read
    while ((check_index_info_status(p_index_info, INDEX_INFO_STATUS_READING) == false) && (check_index_info_status(p_index_info, INDEX_INFO_STATUS_READY) == false || *p_writer_waiting_count > 0))
    {
        pthread_cond_wait(p_read_cond, p_mutex);
    }
#endif

    (*p_reader_waiting_count)--;
    (*p_reader_count)++;
}

static inline void index_info_sync_write_unblock(INDEX_INFO_T *p_index_info)
{
    uint32_t *p_writer_waiting_count = &(p_index_info->index_info_sync.writer_waiting_count);
    uint32_t *p_reader_waiting_count = &(p_index_info->index_info_sync.reader_waiting_count);

#if IS_POSIX_API_SUPPORT
    pthread_cond_t *p_write_cond = &(p_index_info->index_info_sync.write_cond);
    pthread_cond_t *p_read_cond = &(p_index_info->index_info_sync.read_cond);
    pthread_cond_t *p_close_cond = &(p_index_info->index_info_sync.close_cond);

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

static inline void index_info_sync_read_unblock(INDEX_INFO_T *p_index_info)
{
    uint32_t *p_reader_count = &(p_index_info->index_info_sync.reader_count);
    uint32_t *p_reader_waiting_count = &(p_index_info->index_info_sync.reader_waiting_count);
    uint32_t *p_writer_waiting_count = &(p_index_info->index_info_sync.writer_waiting_count);

    *p_reader_count -= 1;
#if IS_POSIX_API_SUPPORT
    pthread_cond_t *p_write_cond = &(p_index_info->index_info_sync.write_cond);
    pthread_cond_t *p_reader_cond = &(p_index_info->index_info_sync.read_cond);

    if (*p_reader_count == 0)
    {
        if (*p_writer_waiting_count > 0)
        {
            pthread_cond_signal(p_write_cond);
        }
        else if (*p_reader_waiting_count == 0)
        {
            // reader_count = 0 && writer_waiting_count = 0 && reader_waiting_count = 0
            pthread_cond_signal(p_reader_cond);
        }
    }
#endif
}

void index_properties_init(INDEX_PROPERTIES_T *p_index_properties)
{
    p_index_properties->p_key = NULL;
    p_index_properties->key_size = 0;
    p_index_properties->root_tag = 0;
    p_index_properties->tag_num = 0;
    p_index_properties->index_id_type = INDEX_ID_TYPE_INVALID;
}

void allocate_index_properties_resources(INDEX_PROPERTIES_T *p_index_properties, uint32_t key_size)
{
    p_index_properties->p_key = malloc(key_size * sizeof(uint8_t));
    p_index_properties->key_size = key_size;
}

void read_index_properties(INDEX_INFO_T *p_index_info)
{
    FILE *p_index_file = p_index_info->index_file;
    INDEX_PROPERTIES_T *p_index_properties = &(p_index_info->index_properties);

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_index_file);
    off_t offset = 0;

    // Read tag_num and root_tag
    pread(fd, &(p_index_properties->tag_num), sizeof(p_index_properties->tag_num), offset);
    offset += sizeof(p_index_properties->tag_num);
    pread(fd, &(p_index_properties->root_tag), sizeof(p_index_properties->root_tag), offset);
    offset += sizeof(p_index_properties->root_tag);

    // Read index_id_type (save as uint32)
    pread(fd, &(p_index_properties->index_id_type_32), sizeof(p_index_properties->index_id_type_32), offset);
    offset += sizeof(p_index_properties->index_id_type_32);

    // Read key_size
    pread(fd, &(p_index_properties->key_size), sizeof(p_index_properties->key_size), offset);
    offset += sizeof(p_index_properties->key_size);

    // Allocate buffer
    allocate_index_properties_resources(p_index_properties, p_index_properties->key_size);
    // Read key
    pread(fd, p_index_properties->p_key, p_index_properties->key_size, offset);

#else  // IS_POSIX_API_SUPPORT
    fseek(p_index_file, 0, SEEK_SET);
    // Read tag_num and root_tag
    fread(&(p_index_properties->tag_num), sizeof(p_index_properties->tag_num), 1, p_index_file);
    fread(&(p_index_properties->root_tag), sizeof(p_index_properties->root_tag), 1, p_index_file);

    // Read index_id_type (save as uint32)
    fread(&(p_index_properties->index_id_type_32), sizeof(p_index_properties->index_id_type_32), 1, p_index_file);
    assert(p_index_properties->index_id_type <= INDEX_ID_TYPE_NUM);

    // Read key_size
    fread(&(p_index_properties->key_size), sizeof(p_index_properties->key_size), 1, p_index_file);
    // Read key
    p_index_properties->p_key = malloc(p_index_properties->key_size);
    fread(p_index_properties->p_key, p_index_properties->key_size, 1, p_index_file);
#endif // IS_POSIX_API_SUPPORT
}

void write_index_properties(INDEX_INFO_T *p_index_info)
{
    FILE *p_index_file = p_index_info->index_file;
    INDEX_PROPERTIES_T *p_index_properties = &(p_index_info->index_properties);

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_index_file);
    off_t offset = 0;

    // Write tag_num & root_tag
    pwrite(fd, &(p_index_properties->tag_num), sizeof(p_index_properties->tag_num), offset);
    offset += sizeof(p_index_properties->tag_num);
    pwrite(fd, &(p_index_properties->root_tag), sizeof(p_index_properties->root_tag), offset);
    offset += sizeof(p_index_properties->root_tag);

    // write index_id_type as uint32
    pwrite(fd, &(p_index_properties->index_id_type_32), sizeof(p_index_properties->index_id_type_32), offset);
    offset += sizeof(p_index_properties->index_id_type_32);

    // Write key_size
    pwrite(fd, &(p_index_properties->key_size), sizeof(p_index_properties->key_size), offset);
    offset += sizeof(p_index_properties->key_size);
    // Write key
    pwrite(fd, p_index_properties->p_key, p_index_properties->key_size, offset);

#else  // IS_POSIX_API_SUPPORT
    fseek(p_index_file, 0, SEEK_SET);

    // Write tag_num & root_tag
    fwrite(&(p_index_properties->tag_num), sizeof(p_index_properties->tag_num), 1, p_index_file);
    fwrite(&(p_index_properties->root_tag), sizeof(p_index_properties->root_tag), 1, p_index_file);

    // write index_id_type as uint32
    fwrite(&(p_index_properties->index_id_type_32), sizeof(p_index_properties->index_id_type_32), 1, p_index_file);

    // Write key_size
    fwrite(&(p_index_properties->key_size), sizeof(p_index_properties->key_size), 1, p_index_file);
    // Write key
    fwrite(p_index_properties->p_key, p_index_properties->key_size, 1, p_index_file);

    fflush(p_index_file);
#endif // IS_POSIX_API_SUPPORT
}

size_t get_index_properties_size(INDEX_PROPERTIES_T *p_index_properties)
{
    size_t index_properties_size = 0;
    index_properties_size += sizeof(p_index_properties->tag_num) + sizeof(p_index_properties->root_tag) + sizeof(p_index_properties->index_id_type);
    // key_size & key
    index_properties_size += sizeof(p_index_properties->key_size) + p_index_properties->key_size;

    return index_properties_size;
}

void free_index_properties_resources(INDEX_PROPERTIES_T *p_index_properties)
{
    if (p_index_properties->p_key)
    {
        free(p_index_properties->p_key);
        p_index_properties->p_key = NULL;
    }
}

void close_index_properties(INDEX_PROPERTIES_T *p_index_properties)
{
    p_index_properties->root_tag = 0;
    p_index_properties->tag_num = 0;
    p_index_properties->key_size = 0;
    free_index_properties_resources(p_index_properties);
}

void index_node_init(INDEX_NODE_T *p_index_node, uint32_t tag)
{
    memset(p_index_node, 0, sizeof(INDEX_NODE_T));
    for (uint32_t i = 0; i < INDEX_ORDER; i++)
    {
        index_element_init(&(p_index_node->elements[i]));
    }

    p_index_node->tag = tag;
}

void write_index_node(INDEX_INFO_T *p_index_info, INDEX_NODE_T *p_index_node)
{
    uint32_t node_tag = p_index_node->tag;
    off_t node_offset = get_node_offset(&(p_index_info->index_properties), node_tag);
    FILE *p_index_file = p_index_info->index_file;

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_index_file);

    // write static fields.
    pwrite(fd, &(p_index_node->tag), sizeof(p_index_node->tag), node_offset);
    node_offset += sizeof(p_index_node->tag);
    pwrite(fd, &(p_index_node->level), sizeof(p_index_node->level), node_offset);
    node_offset += sizeof(p_index_node->level);
    pwrite(fd, &(p_index_node->length), sizeof(p_index_node->length), node_offset);
    node_offset += sizeof(p_index_node->length);
    pwrite(fd, &(p_index_node->parent_tag), sizeof(p_index_node->parent_tag), node_offset);
    node_offset += sizeof(p_index_node->parent_tag);
    pwrite(fd, &(p_index_node->next_tag), sizeof(p_index_node->next_tag), node_offset);
    node_offset += sizeof(p_index_node->next_tag);
    pwrite(fd, p_index_node->child_tag, sizeof(p_index_node->child_tag[0]) * INDEX_CHILD_TAG_ORDER, node_offset);

    write_index_elements(p_index_info, node_tag, 0, INDEX_ORDER, p_index_node->elements);
#else
    fseek(p_index_file, node_offset, SEEK_SET);

    // write static fields.
    fwrite(&(p_index_node->tag), sizeof(p_index_node->tag), 1, p_index_file);
    fwrite(&(p_index_node->level), sizeof(p_index_node->length), 1, p_index_file);
    fwrite(&(p_index_node->length), sizeof(p_index_node->length), 1, p_index_file);
    fwrite(&(p_index_node->parent_tag), sizeof(p_index_node->parent_tag), 1, p_index_file);
    fwrite(&(p_index_node->next_tag), sizeof(p_index_node->next_tag), 1, p_index_file);
    fwrite(p_index_node->child_tag, sizeof(p_index_node->child_tag[0]), INDEX_CHILD_TAG_ORDER, p_index_file);

    // write dynamic fields
    // TODO: only write the changed elements.
    write_index_elements(p_index_info, node_tag, 0, INDEX_ORDER, p_index_node->elements);

    fflush(p_index_file);
#endif // IS_POSIX_API_SUPPORT
}

// return sucessful or not
bool read_index_node(INDEX_INFO_T *p_index_info, uint32_t tag, INDEX_NODE_T *p_index_node)
{
    // tag is a 1-index number and it should smaller than the tag number.
    if (p_index_info->index_properties.tag_num < tag)
    {
        // Index node doesn't exist
        return false;
    }

    FILE *p_index_file = p_index_info->index_file;
    off_t node_offset = get_node_offset(&(p_index_info->index_properties), tag);

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_index_file);

    // read static fields
    pread(fd, &(p_index_node->tag), sizeof(p_index_node->tag), node_offset);
    node_offset += sizeof(p_index_node->tag);
    pread(fd, &(p_index_node->level), sizeof(p_index_node->level), node_offset);
    node_offset += sizeof(p_index_node->level);
    pread(fd, &(p_index_node->length), sizeof(p_index_node->length), node_offset);
    node_offset += sizeof(p_index_node->length);
    pread(fd, &(p_index_node->parent_tag), sizeof(p_index_node->parent_tag), node_offset);
    node_offset += sizeof(p_index_node->parent_tag);
    pread(fd, &(p_index_node->next_tag), sizeof(p_index_node->next_tag), node_offset);
    node_offset += sizeof(p_index_node->next_tag);
    pread(fd, p_index_node->child_tag, sizeof(p_index_node->child_tag[0]) * INDEX_CHILD_TAG_ORDER, node_offset);

#else  // IS_POSIX_API_SUPPORT
    fseek(p_index_file, node_offset, SEEK_SET);
    // read static fields
    fread(&(p_index_node->tag), sizeof(p_index_node->tag), 1, p_index_file);
    fread(&(p_index_node->level), sizeof(p_index_node->level), 1, p_index_file);
    fread(&(p_index_node->length), sizeof(p_index_node->length), 1, p_index_file);
    fread(&(p_index_node->parent_tag), sizeof(p_index_node->parent_tag), 1, p_index_file);
    fread(&(p_index_node->next_tag), sizeof(p_index_node->next_tag), 1, p_index_file);
    fread(p_index_node->child_tag, sizeof(p_index_node->child_tag[0]), INDEX_CHILD_TAG_ORDER, p_index_file);
#endif // IS_POSIX_API_SUPPORT

    // read dynamic fields
    read_index_elements(p_index_info, tag, 0, INDEX_ORDER, p_index_node->elements);

    return true;
}

off_t get_node_offset(INDEX_PROPERTIES_T *p_index_properties, uint32_t tag)
{
    size_t index_properties_size = get_index_properties_size(p_index_properties);
    size_t index_element_size = Index_Id_Type_Get_Size(p_index_properties->index_id_type) + INDEX_PAYLOAD_SIZE;
    // tag + level + length + parent_tag + next_tag + child_tag[INDEX_CHILD_TAG_ORDER] + elements[INDEX_ORDER]
    size_t index_node_size = (sizeof(uint32_t) * (INDEX_CHILD_TAG_ORDER + 5)) + (index_element_size * INDEX_ORDER);

    // tag is a 1-based number.
    return (index_properties_size + ((tag - 1) * index_node_size));
}

void free_index_node_resources(INDEX_NODE_T *p_index_node)
{
    for (uint32_t i = 0; i < INDEX_ORDER; i++)
    {
        free_index_element_resources(&(p_index_node->elements[i]));
    }
}

void index_element_init(INDEX_ELEMENT_T *p_index_element)
{
    p_index_element->p_index_id = NULL;
    memset(p_index_element->index_payload, 0, INDEX_PAYLOAD_SIZE);
}

void setup_index_element(INDEX_ELEMENT_T *p_index_element, void *p_index_id, INDEX_ID_TYPE_E index_id_type, void *p_payload, uint32_t payload_size)
{
    uint32_t index_id_size = Index_Id_Type_Get_Size(index_id_type);
    payload_size = (payload_size > INDEX_PAYLOAD_SIZE) ? INDEX_PAYLOAD_SIZE : payload_size;
    allocate_index_element_resources(p_index_element, index_id_size);

    // index_id
    memcpy(p_index_element->p_index_id, p_index_id, index_id_size);
    // payload
    // clear the existed data first.
    memset(p_index_element->index_payload, 0, INDEX_PAYLOAD_SIZE);
    if (payload_size > 0 && p_payload != NULL)
    {
        memcpy(p_index_element->index_payload, p_payload, payload_size);
    }
}

/*
** Write numbers of index_elements to file.
** p_index_element array length should greater or equal to the write_length.
*/
void write_index_elements(INDEX_INFO_T *p_index_info, uint32_t tag, uint32_t index_element_position, uint32_t write_length, INDEX_ELEMENT_T *p_index_element)
{
    assert((index_element_position + write_length) <= INDEX_ORDER);

    INDEX_PROPERTIES_T *p_index_properties = &(p_index_info->index_properties);
    FILE *p_index_file = p_index_info->index_file;
    off_t index_element_offset = get_index_element_offset(p_index_properties, tag, index_element_position);
    uint32_t index_id_size = Index_Id_Type_Get_Size(p_index_properties->index_id_type);

    uint8_t zero_buffer[index_id_size];
    memset(zero_buffer, 0, index_id_size);

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_index_file);

    for (uint32_t i = 0; i < write_length; i++)
    {
        // write index_id
        if (p_index_element[i].p_index_id != NULL)
        {
            pwrite(fd, p_index_element[i].p_index_id, index_id_size, index_element_offset);
        }
        else
        {
            pwrite(fd, zero_buffer, index_id_size, index_element_offset);
        }
        index_element_offset += index_id_size;

        // write payload
        pwrite(fd, p_index_element[i].index_payload, INDEX_PAYLOAD_SIZE, index_element_offset);
        index_element_offset += INDEX_PAYLOAD_SIZE;
    }

#else  // IS_POSIX_API_SUPPORT
    fseek(p_index_file, index_element_offset, SEEK_SET);
    for (uint32_t i = 0; i < write_length; i++)
    {
        if (p_index_element[i].p_index_id != NULL)
        {
            fwrite(p_index_element[i].p_index_id, index_id_size, 1, p_index_file);
        }
        else
        {
            fwrite(zero_buffer, index_id_size, 1, p_index_file);
        }
        fwrite(p_index_element[i].index_payload, INDEX_PAYLOAD_SIZE, 1, p_index_file);
    }

    // flush in write_index_node
    // fflush(p_index_file);
#endif // IS_POSIX_API_SUPPORT
}

/*
** Read numbers of index_elements from file.
** p_index_element array length should greater or equal to the read_length.
*/
void read_index_elements(INDEX_INFO_T *p_index_info, const uint32_t tag, const uint32_t index_element_position, const uint32_t read_length, INDEX_ELEMENT_T *p_index_element)
{
    assert((index_element_position + read_length) <= INDEX_ORDER);

    INDEX_PROPERTIES_T *p_index_properties = &(p_index_info->index_properties);
    FILE *p_index_file = p_index_info->index_file;
    off_t index_element_offset = get_index_element_offset(p_index_properties, tag, index_element_position);
    uint32_t index_id_size = Index_Id_Type_Get_Size(p_index_properties->index_id_type);

#if IS_POSIX_API_SUPPORT
    int fd = fileno(p_index_file);

    for (uint32_t i = 0; i < read_length; i++)
    {
        allocate_index_element_resources(&(p_index_element[i]), index_id_size);
        // read index_id
        pread(fd, p_index_element[i].p_index_id, index_id_size, index_element_offset);
        index_element_offset += index_id_size;
        // read index_paylolad
        pread(fd, p_index_element[i].index_payload, INDEX_PAYLOAD_SIZE, index_element_offset);
        index_element_offset += INDEX_PAYLOAD_SIZE;
    }
#else // IS_POSIX_API_SUPPORT
    fseek(p_index_file, index_element_offset, SEEK_SET);
    for (uint32_t i = 0; i < read_length; i++)
    {
        allocate_index_element_resources(&(p_index_element[i]), index_id_size);
        // read index_id
        fread(p_index_element[i].p_index_id, index_id_size, 1, p_index_file);
        // read index_paylolad
        fread(p_index_element[i].index_payload, INDEX_PAYLOAD_SIZE, 1, p_index_file);
    }
#endif
}

off_t get_index_element_offset(INDEX_PROPERTIES_T *p_index_properties, uint32_t tag, uint32_t index_element_position)
{
    uint32_t index_id_size = Index_Id_Type_Get_Size(p_index_properties->index_id_type);
    off_t index_node_offset = get_node_offset(p_index_properties, tag);
    // tag + level + length + parent_tag + next_tag + child_tag[INDEX_CHILD_TAG_ORDER]
    size_t index_before_fields_size = sizeof(uint32_t) * (INDEX_CHILD_TAG_ORDER + 5);
    size_t index_element_offset_of_node = index_element_position * (index_id_size + (INDEX_PAYLOAD_SIZE * sizeof(uint8_t)));

    return index_node_offset + index_before_fields_size + index_element_offset_of_node;
}

void allocate_index_element_resources(INDEX_ELEMENT_T *p_index_element, uint32_t index_id_size)
{
    if (p_index_element->p_index_id)
    {
        free_index_element_resources(p_index_element);
    }

    p_index_element->p_index_id = calloc(1, index_id_size);
}

void free_index_element_resources(INDEX_ELEMENT_T *p_index_element)
{
    if (p_index_element->p_index_id != NULL)
    {
        free(p_index_element->p_index_id);
        p_index_element->p_index_id = NULL;
    }
}

// Deep copy index element
void deep_copy_index_element(INDEX_ELEMENT_T *p_dest_index_element, INDEX_ELEMENT_T *p_src_index_element, INDEX_ID_TYPE_E index_id_type)
{
    uint32_t index_id_size = Index_Id_Type_Get_Size(index_id_type);

    free_index_element_resources(p_dest_index_element);
    allocate_index_element_resources(p_dest_index_element, index_id_type);

    memcpy(p_dest_index_element->p_index_id, p_src_index_element->p_index_id, index_id_size);
    memcpy(p_dest_index_element->index_payload, p_src_index_element->index_payload, INDEX_PAYLOAD_SIZE);
}

// Find the array position in the node where the new element should insert into.
// Return the minimum element position where the value is equal or greater than the inputed value.
uint32_t find_element_position_in_the_node(INDEX_NODE_T *p_index_node, INDEX_ELEMENT_T *p_index_element, INDEX_ID_TYPE_E index_id_type)
{
    // binary search index_element position.
    // find equal or upper bound (ceiling child_tag in the node)
    uint32_t start = 0;
    uint32_t end = p_index_node->length; // [start, end)

    while (start < end)
    {
        uint32_t mid = start + ((end - start) / 2);
        INDEX_ID_COMPARE_RESULT_E cmp_result = Index_Id_Type_Compare(index_id_type, p_index_element->p_index_id, p_index_node->elements[mid].p_index_id);

        if (cmp_result == INDEX_ID_COMPARE_LEFT_GREATER)
        {
            // grater than [mid]
            start = mid + 1;
        }
        else
        {
            // Smaller or equals to [mid]
            end = mid;
        }
    }
    assert((start == end) && (start <= p_index_node->length));
    return start;
}

void split_index_elements_into_two_index_node(INDEX_ELEMENT_T *p_index_element_buffer, uint32_t index_element_buffer_length, INDEX_NODE_T *p_index_node_current, INDEX_NODE_T *p_index_node_sibling)
{
    assert(index_element_buffer_length <= (2 * INDEX_ORDER));

    uint32_t start_position, copy_length;
    bool is_leaf_node = (p_index_node_current->child_tag[0] == 0) ? true : false;

    // Copy first half of index elements into current node.
    start_position = 0;
    copy_length = index_element_buffer_length / 2;
    // p_index_id: shallow copy
    memcpy(p_index_node_current->elements, p_index_element_buffer, sizeof(INDEX_ELEMENT_T) * copy_length);
    for (uint32_t i = copy_length; i < INDEX_ORDER; i++)
    {
        p_index_node_current->elements[i].p_index_id = NULL;
        memset(p_index_node_current->elements[i].index_payload, 0, INDEX_PAYLOAD_SIZE);
    }
    // TODO: Set the unused index elements into default value.
    p_index_node_current->length = copy_length;

    // Copy second half of index elements into the sibling node.
    if (is_leaf_node)
    {
        start_position = copy_length;
        copy_length = index_element_buffer_length - copy_length;
    }
    else
    {
        // If current node and sibling node are not leaf nodes, it doesn't need to copy the [mid] element to sibling node.
        // The [mid] element should be inserted to parent node.
        start_position = copy_length + 1;
        copy_length = index_element_buffer_length - copy_length - 1;
    }
    // p_index_id: shallow copy
    memcpy(p_index_node_sibling->elements, &(p_index_element_buffer[start_position]), sizeof(INDEX_ELEMENT_T) * copy_length);
    for (uint32_t i = copy_length; i < INDEX_ORDER; i++)
    {
        p_index_node_sibling->elements[i].p_index_id = NULL;
        memset(p_index_node_sibling->elements[i].index_payload, 0, INDEX_PAYLOAD_SIZE);
    }
    // Set unused elements into default value.
    p_index_node_sibling->length = copy_length;
}

void split_child_tags_into_two_index_node(INDEX_INFO_T *p_index_info, uint32_t *p_child_tag_buffer, uint32_t child_tag_buffer_length, INDEX_NODE_T *p_index_node_current, INDEX_NODE_T *p_index_node_sibling)
{
    uint32_t first_half_length = (child_tag_buffer_length / 2) + (child_tag_buffer_length % 2);
    uint32_t second_half_length = child_tag_buffer_length - first_half_length;
    INDEX_NODE_T second_half_child_index_node;

    // Copy first half of child tags to current node.
    memcpy(p_index_node_current->child_tag, p_child_tag_buffer, sizeof(uint32_t) * first_half_length);
    // Copy second half of child tags to sibling node.
    memcpy(p_index_node_sibling->child_tag, &(p_child_tag_buffer[first_half_length]), sizeof(uint32_t) * second_half_length);
    // update the parent tag of the second half child nodes to sibling node tag.
    for (uint32_t i = 0; i < second_half_length; i++)
    {
        index_node_init(&second_half_child_index_node, p_index_node_sibling->child_tag[i]);
        read_index_node(p_index_info, p_index_node_sibling->child_tag[i], &second_half_child_index_node);
        second_half_child_index_node.parent_tag = p_index_node_sibling->tag;
        write_index_node(p_index_info, &second_half_child_index_node);
        free_index_node_resources(&second_half_child_index_node);
    }
}

void insert_index_element_handler(INDEX_INFO_T *p_index_info, INDEX_NODE_T *p_index_node, INDEX_ELEMENT_T *p_index_element, uint32_t new_child_tag)
{
    INDEX_ID_TYPE_E index_id_type = p_index_info->index_properties.index_id_type;
    uint32_t position = find_element_position_in_the_node(p_index_node, p_index_element, index_id_type);
    uint32_t tag_position = position + 1;
    INDEX_PROPERTIES_T *p_index_properties = &(p_index_info->index_properties);

    if (p_index_node->length >= INDEX_ORDER)
    {
        // index node is full.
        // split current node into two nodes and insert the [INDEX_ORDER / 2] element to the parent node.
        INDEX_NODE_T parent_node, new_sibling_node;
        INDEX_ELEMENT_T index_elements_buffer[INDEX_ORDER + 1];
        uint32_t child_tags_buffer[INDEX_CHILD_TAG_ORDER + 1];
        uint32_t mid_position = (INDEX_ORDER + 1) / 2;
        bool is_leaf_node = (p_index_node->child_tag[0] == 0) ? true : false;

        // init buffers
        for (uint32_t i = 0; i < INDEX_ORDER + 1; i++)
        {
            index_element_init(&(index_elements_buffer[i]));
        }
        for (uint32_t i = 0; i < INDEX_CHILD_TAG_ORDER + 1; i++)
        {
            child_tags_buffer[i] = 0;
        }

        // insert the current node and the new element into buffer by order.
        if (position > 0)
        {
            // p_index_id: shallow copy
            memcpy(index_elements_buffer, &(p_index_node->elements[0]), sizeof(INDEX_ELEMENT_T) * position);
        }
        // deep copy the inserted element.
        deep_copy_index_element(&(index_elements_buffer[position]), p_index_element, p_index_properties->index_id_type);
        if (position < INDEX_ORDER)
        {
            // p_index_id: shallow copy
            memcpy(&(index_elements_buffer[position + 1]), &(p_index_node->elements[position]), sizeof(INDEX_ELEMENT_T) * (INDEX_ORDER - position));
        }

        // Insert the current child tags and new_child_tag into buffer by order.
        // new_child_tag is the child tag that greater than the inputted element.
        if (tag_position > 0)
        {
            memcpy(child_tags_buffer, &(p_index_node->child_tag[0]), sizeof(uint32_t) * tag_position);
        }
        child_tags_buffer[tag_position] = new_child_tag;
        if (tag_position < INDEX_CHILD_TAG_ORDER)
        {
            memcpy(&(child_tags_buffer[tag_position + 1]), &(p_index_node->child_tag[tag_position]), sizeof(uint32_t) * (INDEX_CHILD_TAG_ORDER - tag_position));
        }

        // Create and initialize the sibling Node
        p_index_properties->tag_num++;
        index_node_init(&new_sibling_node, p_index_properties->tag_num);
        new_sibling_node.level = p_index_node->level;
        new_sibling_node.parent_tag = p_index_node->parent_tag;
        new_sibling_node.next_tag = p_index_node->next_tag;
        p_index_node->next_tag = new_sibling_node.tag;

        // Create or load parent node.
        if (p_index_node->parent_tag == 0)
        {
            // no parent node, create a new one.
            p_index_properties->tag_num++;
            index_node_init(&parent_node, p_index_properties->tag_num);
            parent_node.child_tag[0] = p_index_node->tag;
            parent_node.level = p_index_node->level + 1;
            p_index_properties->root_tag = parent_node.tag;

            p_index_node->parent_tag = parent_node.tag;
            new_sibling_node.parent_tag = parent_node.tag;
        }
        else
        {
            index_node_init(&parent_node, p_index_node->parent_tag);
            read_index_node(p_index_info, p_index_node->parent_tag, &parent_node);
        }

        // Insert first half of the elements in the buffer into current node and insert the second half of the elements into sibling node.
        // If the current node is a leaf node, copy the [mid] element to new sibling node to keep it in the leaf.
        // If current node is not a leaf node, we don't have to keep it in any node in the current node level.
        split_index_elements_into_two_index_node(index_elements_buffer, INDEX_ORDER + 1, p_index_node, &new_sibling_node);
        // Because all the child tag in the leaef node is 0, no need to set non-0 value to them.
        if (is_leaf_node == false)
        {
            split_child_tags_into_two_index_node(p_index_info, child_tags_buffer, INDEX_CHILD_TAG_ORDER + 1, p_index_node, &new_sibling_node);
        }

        // write sibling node into file
        write_index_node(p_index_info, &new_sibling_node);

        // write current node into file.
        write_index_node(p_index_info, p_index_node);

        // TODO: update index properties only when close index.
        write_index_properties(p_index_info);

        // Insert the [mid] element to the parent node.
        insert_index_element_handler(p_index_info, &parent_node, &(index_elements_buffer[mid_position]), new_sibling_node.tag);
        // the above function will write parent node into file.

        // free resources of sibling, parnet node, and [mid] element if current node is not a leaf node.
        free_index_node_resources(&new_sibling_node);
        free_index_node_resources(&parent_node);
        // If current node is not a leaf node, the [mid] element would not existed in the current node or the sibling node.
        // And the resource would not be freed.
        if (is_leaf_node == false)
        {
            free_index_element_resources(&(index_elements_buffer[mid_position]));
        }
    }
    else
    {
        // index_node isn't full
        // insertion sort
        for (uint32_t i = p_index_node->length; i > position; i--)
        {
            void *temp_ptr = NULL;

            // move the elements in the node which greater than the inserted element to the next position in the array.
            // swap dynamic resources (malloc): p_index_id.
            temp_ptr = p_index_node->elements[i].p_index_id;
            p_index_node->elements[i].p_index_id = p_index_node->elements[i - 1].p_index_id;
            p_index_node->elements[i - 1].p_index_id = temp_ptr;
            // copy static resources: index_payload.
            memcpy(&(p_index_node->elements[i].index_payload), &(p_index_node->elements[i - 1].index_payload), INDEX_PAYLOAD_SIZE);
            // move child_tags
            p_index_node->child_tag[i + 1] = p_index_node->child_tag[i];
        }
        // deep copy inserted index element
        deep_copy_index_element(&(p_index_node->elements[position]), p_index_element, p_index_properties->index_id_type);
        p_index_node->child_tag[tag_position] = new_child_tag;
        p_index_node->length++;

        // write current node into file.
        write_index_node(p_index_info, p_index_node);
    }
}

void insert_index_element(INDEX_INFO_T *p_index_info, uint32_t tag, INDEX_ELEMENT_T *p_index_element)
{
    INDEX_NODE_T index_node;
    index_node_init(&index_node, tag);

    if (read_index_node(p_index_info, tag, &index_node) == false)
    {
        return;
    }

    // Check if the leaf node existed or not by child_tag[0], and process if reached leaf node.
    if (index_node.child_tag[0] != 0)
    {
        // Current node is not a leaf node.
        // find the insert position (and also the child_tag position).
        // Position is in the range of [0, index_node.length].
        INDEX_ID_TYPE_E index_id_type = p_index_info->index_properties.index_id_type;
        uint32_t position = find_element_position_in_the_node(&index_node, p_index_element, index_id_type);
        insert_index_element(p_index_info, index_node.child_tag[position], p_index_element);
    }
    else
    {
        // Current node is a leaf node.
        insert_index_element_handler(p_index_info, &index_node, p_index_element, 0);
    }

    free_index_node_resources(&index_node);
}

void *search_index_element(INDEX_INFO_T *p_index_info, uint32_t tag, INDEX_ELEMENT_T *p_target_index_element, uint32_t *result_length)
{
    INDEX_NODE_T index_node;
    index_node_init(&index_node, tag);

    if (read_index_node(p_index_info, tag, &index_node) == false)
    {
        *result_length = 0;
        return NULL;
    }

    if (index_node.child_tag[0] != 0)
    {
        // non-leaf
        INDEX_ID_TYPE_E index_id_type = p_index_info->index_properties.index_id_type;
        uint32_t position = find_element_position_in_the_node(&index_node, p_target_index_element, index_id_type);
        return search_index_element(p_index_info, index_node.child_tag[position], p_target_index_element, result_length);
    }
    else
    {
        // leaf-node
        return search_index_element_handler(p_index_info, &index_node, p_target_index_element, result_length);
    }

    free_index_node_resources(&index_node);
}

uint8_t *search_index_element_handler(INDEX_INFO_T *p_index_info, INDEX_NODE_T *p_index_node, INDEX_ELEMENT_T *p_target_index_element, uint32_t *result_length)
{
    uint8_t *p_search_result = NULL;
    INDEX_ID_TYPE_E index_id_type = p_index_info->index_properties.index_id_type;
    uint32_t position = find_element_position_in_the_node(p_index_node, p_target_index_element, index_id_type);
    uint32_t compare_equal_length = 0, i = 0, match_end_position = 0;
    bool only_current_node_result = true;

    for (i = position; i < p_index_node->length; i++)
    {
        if (Index_Id_Type_Compare(index_id_type, p_target_index_element->p_index_id, p_index_node->elements[i].p_index_id) == INDEX_ID_COMPARE_EQUAL)
        {
            match_end_position = i;
            compare_equal_length++;
        }
        else
        {
            break;
        }
    }

    if ((i == p_index_node->length) && (p_index_node->next_tag != 0))
    {
        // The target node may also existed in the next node, search next node.
        INDEX_NODE_T next_index_node;
        uint8_t *next_index_node_search_result = NULL;

        index_node_init(&next_index_node, p_index_node->next_tag);

        if (read_index_node(p_index_info, p_index_node->next_tag, &next_index_node) == true)
        {
            next_index_node_search_result = search_index_element_handler(p_index_info, &next_index_node, p_target_index_element, result_length);
            compare_equal_length += *result_length;
            p_search_result = realloc(next_index_node_search_result, compare_equal_length * INDEX_PAYLOAD_SIZE);
            if (p_search_result == NULL)
            {
                // allocate more memory error.
                // Error handling: return next search result, and doesn't attach current result.
                return next_index_node_search_result;
            }
            only_current_node_result = false;
        }
        else
        {
            // read next node error
            only_current_node_result = true;
        }

        free_index_node_resources(&next_index_node);
    }

    if (only_current_node_result == true)
    {
        // The right most element is not equal to the target.
        // No need to search next node.
        // Or next node doesn't exist.
        // Or next node read error.
        *result_length = 0;
        if (compare_equal_length > 0)
        {
            p_search_result = malloc(compare_equal_length * INDEX_PAYLOAD_SIZE);
            if (p_search_result == NULL)
            {
                // Allocate memory error
                // return NULL pointer and length = 0
                return NULL;
            }
        }
    }

    for (i = *result_length; i < compare_equal_length; i++, match_end_position--)
    {
        memcpy(p_search_result + (INDEX_PAYLOAD_SIZE * i), p_index_node->elements[match_end_position].index_payload, INDEX_PAYLOAD_SIZE);
    }

    *result_length = compare_equal_length;
    return p_search_result;
}
