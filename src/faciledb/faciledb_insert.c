#include <stdint.h>
#include <assert.h>
#include <string.h>

#include "faciledb.h"
#include "faciledb_internal.h"
#include "crc.h"
#include "mema.h"
#include "faciledb_utils.h"

#if ENABLE_DB_INDEX
#include "faciledb_index.h"
#include "index.h"
#endif

uint32_t insert_db_data_handler_write_db_block_list(DB_SET_INFO_T *p_db_set_info, DB_BLOCK_LIST_ENTRY_T *p_db_block_list_entry);
void insert_db_data_handler_assign_block_value(DB_BLOCK_T *p_db_block, DB_SET_INFO_T *p_info, uint32_t valid_record_num, uint64_t data_tag, uint32_t record_crc32);
void insert_db_data_handler_assign_prev_next_block_tag(DB_BLOCK_LIST_ENTRY_T *p_entry, DB_BLOCK_LIST_NODE_T *p_node);
void insert_db_data_handler_free_db_block_list(DB_BLOCK_LIST_ENTRY_T *p_db_block_list_entry);

// p_db_data_info is a pointer to a DB_DATA_INFO_T, not a pointer to an array.
// return value: the number of inserted data.
uint32_t insert_db_data(DB_SET_INFO_T *p_db_set_info, DB_DATA_INFO_T *p_db_data_info, uint64_t data_tag)
{
    DB_BLOCK_LIST_ENTRY_T db_block_list_entry;
    DB_BLOCK_LIST_NODE_T *p_db_block_list_node = NULL;
    DB_BLOCK_T *p_db_block = NULL;
    uint8_t *p_db_block_write = NULL;
    uint8_t *p_db_block_end = NULL;
    uint32_t calculated_record_crc32 = Crc32_Api_Init();
    const size_t db_record_properties_size = get_db_record_properties_size();

    db_block_list_entry_init(&db_block_list_entry);
    p_db_block_list_node = Mema_Api_Alloc(MEMA_USER_FACILEDB, sizeof(DB_BLOCK_LIST_NODE_T));
    db_block_list_node_init(p_db_block_list_node);
    p_db_block = &(p_db_block_list_node->db_block);
    p_db_block_write = p_db_block->block_data;
    p_db_block_end = p_db_block_write + FACILEDB_BLOCK_DATA_SIZE;

    // calculate the record crc32 value
    for (uint32_t i = 0; i < (p_db_data_info->record_num); i++)
    {
        calculated_record_crc32 = Crc32_Api_Calc(calculated_record_crc32, &(p_db_data_info->p_db_record_info[i].db_record_properties), sizeof(DB_RECORD_PROPERTIES_T));
        calculated_record_crc32 = Crc32_Api_Calc(calculated_record_crc32, p_db_data_info->p_db_record_info[i].db_record.p_key, p_db_data_info->p_db_record_info[i].db_record_properties.key_size);
        calculated_record_crc32 = Crc32_Api_Calc(calculated_record_crc32, p_db_data_info->p_db_record_info[i].db_record.p_value, p_db_data_info->p_db_record_info[i].db_record_properties.value_size);
    }

    for (uint32_t i = 0; i < (p_db_data_info->record_num); i++)
    {
        uint32_t remaining_size = 0;
        DB_RECORD_INFO_T *p_current_db_record_info = &p_db_data_info->p_db_record_info[i];

        if ((p_db_block_write + db_record_properties_size) > p_db_block_end)
        {
            // Current db_block is full, append db_block_list_node into the end of the list.
            insert_db_data_handler_assign_block_value(p_db_block, p_db_set_info, p_db_data_info->record_num, data_tag, calculated_record_crc32);
            insert_db_data_handler_assign_prev_next_block_tag(&db_block_list_entry, p_db_block_list_node);
            append_db_block_list_node_to_tail(&db_block_list_entry, p_db_block_list_node);

            // assign new block list node for next block
            p_db_block_list_node = Mema_Api_Alloc(MEMA_USER_FACILEDB, sizeof(DB_BLOCK_LIST_NODE_T));
            db_block_list_node_init(p_db_block_list_node);
            // assign local variables
            p_db_block = &(p_db_block_list_node->db_block);
            p_db_block_write = p_db_block->block_data;
            p_db_block_end = p_db_block_write + FACILEDB_BLOCK_DATA_SIZE;
        }

        // copy record properties to block data.
        memcpy(p_db_block_write, &(p_current_db_record_info->db_record_properties), db_record_properties_size);
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
                // Current db_block is full, append db_block_list_node into the end of the list.
                insert_db_data_handler_assign_block_value(p_db_block, p_db_set_info, p_db_data_info->record_num, data_tag, calculated_record_crc32);
                insert_db_data_handler_assign_prev_next_block_tag(&db_block_list_entry, p_db_block_list_node);
                append_db_block_list_node_to_tail(&db_block_list_entry, p_db_block_list_node);

                // assign new block list node for next block
                p_db_block_list_node = Mema_Api_Alloc(MEMA_USER_FACILEDB, sizeof(DB_BLOCK_LIST_NODE_T));
                db_block_list_node_init(p_db_block_list_node);
                // assign local variables
                p_db_block = &(p_db_block_list_node->db_block);
                p_db_block_write = p_db_block->block_data;
                p_db_block_end = p_db_block_write + FACILEDB_BLOCK_DATA_SIZE;

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
                // Current db_block is full, append db_block_list_node into the end of the list.
                insert_db_data_handler_assign_block_value(p_db_block, p_db_set_info, p_db_data_info->record_num, data_tag, calculated_record_crc32);
                insert_db_data_handler_assign_prev_next_block_tag(&db_block_list_entry, p_db_block_list_node);
                append_db_block_list_node_to_tail(&db_block_list_entry, p_db_block_list_node);

                // assign new block list node for next block
                p_db_block_list_node = Mema_Api_Alloc(MEMA_USER_FACILEDB, sizeof(DB_BLOCK_LIST_NODE_T));
                db_block_list_node_init(p_db_block_list_node);
                // assign local variables
                p_db_block = &(p_db_block_list_node->db_block);
                p_db_block_write = p_db_block->block_data;
                p_db_block_end = p_db_block_write + FACILEDB_BLOCK_DATA_SIZE;

                continue;
            }

            p_value = p_current_db_record_info->db_record.p_value + p_current_db_record_info->db_record_properties.value_size - remaining_size;
            memcpy(p_db_block_write, p_value, copy_size);
            p_db_block_write += copy_size;
            remaining_size -= copy_size;
        }
    }

    // Append the last db_block_list_node into the end of the list.
    insert_db_data_handler_assign_block_value(p_db_block, p_db_set_info, p_db_data_info->record_num, data_tag, calculated_record_crc32);
    insert_db_data_handler_assign_prev_next_block_tag(&db_block_list_entry, p_db_block_list_node);
    append_db_block_list_node_to_tail(&db_block_list_entry, p_db_block_list_node);

    // update the modified time of the db_set_properties
    p_db_set_info->db_set_properties.modified_time = db_block_list_entry.p_tail->db_block.modified_time;
    // write the db block list into file.
    insert_db_data_handler_write_db_block_list(p_db_set_info, &db_block_list_entry);
    update_to_db_set_properties_region(p_db_set_info);

#if ENABLE_DB_INDEX
    // insert index if existed
    uint64_t first_db_block_tag = db_block_list_entry.p_head->db_block.block_tag;
    for (uint32_t i = 0; i < (p_db_data_info->record_num); i++)
    {
        DB_RECORD_INFO_T *p_current_db_record_info = &p_db_data_info->p_db_record_info[i];
        char *p_index_key = set_db_index_key(p_db_set_info->p_set_name, p_db_set_info->set_name_size, p_current_db_record_info->db_record.p_key, p_current_db_record_info->db_record_properties.key_size);

        // If p_key index has been created, insert new index element.
        if (Index_Api_Index_Key_Exist(p_index_key))
        {
            DB_INDEX_PAYLOAD_T db_index_payload = {
                .data_tag = data_tag,
                .start_db_block_tag = first_db_block_tag};

            insert_db_record_index(p_db_set_info, p_current_db_record_info, &db_index_payload);
        }

        Mema_Api_Free(MEMA_USER_FACILEDB, p_index_key);
    }
#endif

    // free db block list
    insert_db_data_handler_free_db_block_list(&db_block_list_entry);

    // return the number of inserted data.
    return 1;
}

// return value: number of written db blocks
uint32_t insert_db_data_handler_write_db_block_list(DB_SET_INFO_T *p_db_set_info, DB_BLOCK_LIST_ENTRY_T *p_db_block_list_entry)
{
    DB_BLOCK_LIST_NODE_T *p_current_node = p_db_block_list_entry->p_head;
    uint32_t block_num;

    for (block_num = 0; block_num < p_db_block_list_entry->list_length; block_num++)
    {
        write_db_block(&(p_current_node->db_block), p_db_set_info);
        p_current_node = p_current_node->p_next;
    }

    return block_num;
}

void insert_db_data_handler_assign_block_value(DB_BLOCK_T *p_db_block, DB_SET_INFO_T *p_info, uint32_t valid_record_num, uint64_t data_tag, uint32_t record_crc32)
{
    uint64_t current_time = (uint64_t)get_current_time();

    p_db_block->block_tag = ++(p_info->db_set_properties.block_num);
    p_db_block->data_tag = data_tag;
    p_db_block->deleted = 0;
    p_db_block->valid_record_num = valid_record_num;
    p_db_block->created_time = current_time;
    p_db_block->modified_time = current_time;

    p_db_block->record_crc32 = record_crc32;
}

void insert_db_data_handler_assign_prev_next_block_tag(DB_BLOCK_LIST_ENTRY_T *p_entry, DB_BLOCK_LIST_NODE_T *p_node)
{
    // update the next block tag of the last block in the list and the prev block tag of the current block if exists.
    if (p_entry->p_tail != NULL)
    {
        p_entry->p_tail->db_block.next_block_tag = p_node->db_block.block_tag;
        p_node->db_block.prev_block_tag = p_entry->p_tail->db_block.block_tag;
    }

    p_node->db_block.next_block_tag = 0;
}

void insert_db_data_handler_free_db_block_list(DB_BLOCK_LIST_ENTRY_T *p_db_block_list_entry)
{
    DB_BLOCK_LIST_NODE_T *p_current_node = p_db_block_list_entry->p_head;
    DB_BLOCK_LIST_NODE_T *p_next_node = NULL;

    for (uint32_t list_length = p_db_block_list_entry->list_length; list_length > 0; list_length--)
    {
        p_next_node = p_current_node->p_next;
        Mema_Api_Free(MEMA_USER_FACILEDB, p_current_node);
        p_current_node = p_next_node;
    }

    p_db_block_list_entry->list_length = 0;
    p_db_block_list_entry->p_head = NULL;
    p_db_block_list_entry->p_tail = NULL;
}
