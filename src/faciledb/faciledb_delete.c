#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>

#include "faciledb_internal.h"
#include "faciledb_utils.h"
#include "mema.h"
#include "crc.h"

void delete_db_data_handler_assign_block_value(DB_BLOCK_T *p_db_block, DB_SET_INFO_T *p_info, uint32_t valid_record_num, uint32_t record_crc32);
uint32_t delete_db_data_handler_append_sys_data_list(DB_SET_INFO_T *p_db_set_info, DB_SYS_DATA_INFO_T *p_db_sys_data_info_array, uint32_t array_length);

void delete_db_data(DB_SET_INFO_T *p_db_set_info, DB_DATA_INFO_LIST_ENTRY_T *p_db_data_info_list_entry)
{
    DB_DATA_INFO_LIST_NODE_T *p_current_node = p_db_data_info_list_entry->p_head;
    DB_SYS_DATA_INFO_T *p_db_sys_data_info_array = NULL;
    uint32_t block_num = 0;
    uint32_t db_sys_data_info_num = p_db_data_info_list_entry->list_length;
    const uint32_t full_sys_record_num_per_block = FACILEDB_BLOCK_DATA_SIZE / get_db_sys_record_size();

    if(db_sys_data_info_num == 0)
    {
        return;
    }

    // assign sys data info array
    p_db_sys_data_info_array = Mema_Api_Alloc(MEMA_USER_FACILEDB, db_sys_data_info_num * sizeof(DB_SYS_DATA_INFO_T));
    for (uint32_t i = 0; i < db_sys_data_info_num; i++)
    {
        db_sys_data_info_init(&(p_db_sys_data_info_array[i]));
        p_db_sys_data_info_array[i].sys_record.data_tag = p_current_node->db_data_info.data_tag;
        p_db_sys_data_info_array[i].sys_record.action = DB_SYS_ACTION_TYPE_DELETE;
        
        p_current_node = p_current_node->p_next;
    }

    // ceiling
    block_num = (db_sys_data_info_num + (full_sys_record_num_per_block - 1)) / full_sys_record_num_per_block;
    // insert db_sys_record to db_block and write into disk.
    for (uint32_t i = 0; i < block_num; i++)
    {
        DB_BLOCK_T db_block;
        db_block_init(&db_block);
        uint32_t record_crc = Crc32_Api_Init();

        uint32_t sys_record_num_in_block = (i == (block_num - 1)) ? (db_sys_data_info_num - (i * full_sys_record_num_per_block)) : (full_sys_record_num_per_block);

        for (uint32_t j = 0; j < sys_record_num_in_block; j++)
        {
            uint8_t *p_db_block_data_dest = db_block.block_data + (j * get_db_sys_record_size());
            DB_SYS_RECORD_T *p_db_sys_record_src = &(p_db_sys_data_info_array[(i * full_sys_record_num_per_block) + j].sys_record);

            memcpy(p_db_block_data_dest, &(p_db_sys_record_src->data_tag), sizeof(p_db_sys_record_src->data_tag));
            record_crc = Crc32_Api_Calc(record_crc, p_db_block_data_dest, sizeof(p_db_sys_record_src->data_tag));

            p_db_block_data_dest += sizeof(p_db_sys_record_src->data_tag);
            memcpy(p_db_block_data_dest, &(p_db_sys_record_src->action_32), sizeof(p_db_sys_record_src->action_32));
            record_crc = Crc32_Api_Calc(record_crc, p_db_block_data_dest, sizeof(p_db_sys_record_src->action_32));
        }

        delete_db_data_handler_assign_block_value(&db_block, p_db_set_info, sys_record_num_in_block, record_crc);
        // link new sys block to the sys block list (linked by using prev_block_tag)
        db_block.prev_block_tag = p_db_set_info->db_set_properties.latest_sys_block_tag;
        p_db_set_info->db_set_properties.latest_sys_block_tag = db_block.block_tag;

        // update the modified time of the db_set_properties
        p_db_set_info->db_set_properties.modified_time = db_block.modified_time;
        write_db_block(&db_block, p_db_set_info);
    }
    update_to_db_set_properties_region(p_db_set_info);

    // append to system data info list
    // assume the sys data info array (from search) is sorted
    delete_db_data_handler_append_sys_data_list(p_db_set_info, p_db_sys_data_info_array, db_sys_data_info_num);

    Mema_Api_Free(MEMA_USER_FACILEDB, p_db_sys_data_info_array);
}

void delete_db_data_handler_assign_block_value(DB_BLOCK_T *p_db_block, DB_SET_INFO_T *p_info, uint32_t valid_record_num, uint32_t record_crc32)
{
    uint64_t current_time = (uint64_t)get_current_time();

    p_db_block->block_tag = ++(p_info->db_set_properties.block_num);
    p_db_block->data_tag = 0; // sys block data tag is 0
    p_db_block->deleted = 0;
    p_db_block->valid_record_num = valid_record_num;
    p_db_block->created_time = current_time;
    p_db_block->modified_time = current_time;

    p_db_block->record_crc32 = record_crc32;
}

// append to system data info list
// assume the p_db_sys_data_info_array is sorted
uint32_t delete_db_data_handler_append_sys_data_list(DB_SET_INFO_T *p_db_set_info, DB_SYS_DATA_INFO_T *p_db_sys_data_info_array, uint32_t array_length)
{
    DB_SYS_DATA_INFO_LIST_ENTRY_T *p_entry = &(p_db_set_info->sys_data_list_entry);
    DB_SYS_DATA_INFO_LIST_NODE_T *p_node = p_entry->p_head;

    for (uint32_t i = 0; i < array_length; i++)
    {
        DB_SYS_DATA_INFO_LIST_NODE_T *p_new_node = Mema_Api_Alloc(MEMA_USER_FACILEDB, sizeof(DB_SYS_DATA_INFO_LIST_NODE_T));
        db_sys_data_info_list_node_init(p_new_node);
        memcpy(&(p_new_node->sys_data_info), &(p_db_sys_data_info_array[i]), sizeof(DB_SYS_DATA_INFO_T));

        if (p_entry->list_length == 0)
        {
            p_entry->p_head = p_new_node;
            p_entry->p_tail = p_new_node;
            p_node = p_entry->p_head;
        }
        else
        {
            while (p_node && (p_node->sys_data_info.sys_record.data_tag < p_db_sys_data_info_array[i].sys_record.data_tag))
            {
                p_node = p_node->p_next;
            }

            if(p_node)
            {
                // append to front
                if(p_node->p_prev)
                {
                    p_new_node->p_prev = p_node->p_prev;
                    p_new_node->p_next = p_node;
                    p_node->p_prev->p_next = p_new_node;
                    p_node->p_prev = p_new_node;
                }
                else
                {
                    // append to linked-list head
                    p_new_node->p_next = p_node;
                    p_node->p_prev = p_new_node;
                    p_entry->p_head = p_new_node;
                }
            }
            else
            {
                // append to tail and reset p_tail to new tail node
                p_new_node->p_prev = p_entry->p_tail;
                p_entry->p_tail->p_next = p_new_node;
                p_entry->p_tail = p_new_node;
            }
        }

        (p_entry->list_length)++;
    }
    return array_length;
}