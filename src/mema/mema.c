#include <stdatomic.h>
#include <assert.h>
#include "mema.h"

typedef struct
{
    _Atomic(size_t) total_user_size; // doesn't inlcude header_size
    _Atomic(uint32_t) allocate_count;
    // TODO: listed list that records each allocation block
    // TODO: store the max total size
} MEMA_USER_INFO_T;

typedef struct
{
    union
    {
        MEMA_USER_E user;
        uint64_t user_64;
    };

    uint64_t size;
    void *user_address;
    // uint32_t head_guard;
} MEMA_MEM_HEADER;

// typedef enum
// {
// #ifdef MEMA_USER_CONFIG
// #undef MEMA_USER_CONFIG
// #endif
// #define MEMA_USER_CONFIG(username, size) username##_MEMORY_SIZE = size,
// #include "mema_user_table.h"
// #undef MEMA_USER_CONFIG
// } MEMA_USER_MEMORY_SIZE_E;

// typedef enum
// {
//     MEM_BLOCK_STATUS_ALLOCATING,
//     MEM_BLOCK_STATUS_ALLOCATED,
//     MEM_BLOCK_STATUS_FREED
// } MEM_BLOCK_STATUS_E;

// typedef struct MEM_BLOCK_LIST
// {
//     MEMA_USER_E mema_user;
//     size_t mem_size;

//     struct MEM_BLOCK_LIST *next;
// } MEM_BLOCK_LIST_T;

static MEMA_USER_INFO_T mema_user_info[MEMA_USER_NUM];

// static uint8_t mema_user_index_memory_pool[MEMA_USER_INDEX_MEMORY_SIZE];
// static uint8_t mema_user_faciledb_memory_pool[MEMA_USER_FACILEDB_MEMORY_SIZE];

// static MEM_BLOCK_LIST_T *mema_mem_block_list[MEMA_USER_NUM] = {NULL};

void Mema_Api_Init(MEMA_USER_E user)
{
    assert(user < MEMA_USER_NUM);

    atomic_store_explicit(&(mema_user_info[user].total_user_size), 0, memory_order_relaxed);
    atomic_store_explicit(&(mema_user_info[user].allocate_count), 0, memory_order_relaxed);
}

void *Mema_Api_Alloc(MEMA_USER_E user, size_t user_size)
{
    assert(user < MEMA_USER_NUM && user_size > 0);

    const size_t header_size = sizeof(MEMA_MEM_HEADER);
    size_t total_size = header_size + user_size;

    uint8_t *p_buffer = calloc(total_size, sizeof(uint8_t));
    uint8_t *p_user_buffer = NULL;

    if (p_buffer != NULL)
    {
        MEMA_MEM_HEADER *p_header = (MEMA_MEM_HEADER *)p_buffer;
        p_header->user = user;
        p_header->size = total_size;
        p_header->user_address = ((uint8_t *)p_buffer) + header_size;
        p_user_buffer = p_header->user_address;

        atomic_fetch_add_explicit(&(mema_user_info[user].allocate_count), 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&(mema_user_info[user].total_user_size), user_size, memory_order_relaxed);
    }
    else
    {
        assert(0);
    }

    return (void *)p_user_buffer;
}

void Mema_Api_Free(MEMA_USER_E user, void *p_user_buffer)
{
    assert(user < MEMA_USER_NUM && p_user_buffer != NULL);

    const size_t header_size = sizeof(MEMA_MEM_HEADER);
    uint8_t *p_buffer = ((uint8_t *)p_user_buffer) - header_size;
    MEMA_MEM_HEADER *p_header = (MEMA_MEM_HEADER *)p_buffer;
    size_t total_size = (size_t)p_header->size;
    size_t user_size = total_size - header_size;

    assert((p_header->user_address == p_user_buffer) && (total_size > 0) && (p_header->user == user));
    free(p_buffer);

    atomic_fetch_sub_explicit(&(mema_user_info[user].total_user_size), user_size, memory_order_relaxed);
    atomic_fetch_sub_explicit(&(mema_user_info[user].allocate_count), 1, memory_order_relaxed);
}

void *Mema_Api_Realloc(MEMA_USER_E user, void *p_user_buffer, size_t new_size)
{
    assert(new_size > 0 && p_user_buffer != NULL);

    const size_t header_size = sizeof(MEMA_MEM_HEADER);
    uint8_t *p_buffer = ((uint8_t *)p_user_buffer) - header_size;
    MEMA_MEM_HEADER *p_header = (MEMA_MEM_HEADER *)p_buffer;
    size_t old_total_size = (size_t)p_header->size;

    assert(p_header->user_address == p_user_buffer && p_header->user == user);

    size_t new_total_size = header_size + new_size;
    uint8_t *p_new_buffer = realloc(p_buffer, new_total_size);
    uint8_t *p_new_user_buffer = NULL;

    if (p_new_buffer != NULL)
    {
        MEMA_MEM_HEADER *p_new_header = (MEMA_MEM_HEADER *)p_new_buffer;
        p_new_header->user = user;
        p_new_header->size = new_total_size;
        p_new_header->user_address = ((uint8_t *)p_new_buffer) + header_size;

        p_new_user_buffer = p_new_buffer + header_size;

        if (new_total_size > old_total_size)
        {
            atomic_fetch_add_explicit(&(mema_user_info[user].total_user_size), (new_total_size - old_total_size), memory_order_relaxed);
        }
        else if (new_total_size < old_total_size)
        {
            atomic_fetch_sub_explicit(&(mema_user_info[user].total_user_size), (old_total_size - new_total_size), memory_order_relaxed);
        }
    }
    else
    {
        assert(0);
    }

    return (void *)p_new_user_buffer;
}

size_t Mema_Api_Get_User_Usage_Size(MEMA_USER_E user)
{
    assert(user < MEMA_USER_NUM);

    return atomic_load_explicit(&(mema_user_info[user].total_user_size), memory_order_relaxed);
}

size_t Mema_Api_Get_User_Usage_Count(MEMA_USER_E user)
{
    assert(user < MEMA_USER_NUM);

    return atomic_load_explicit(&(mema_user_info[user].allocate_count), memory_order_relaxed);
}