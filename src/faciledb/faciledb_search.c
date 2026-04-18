#include <stdint.h>
#include <assert.h>
#include <string.h>

#include "faciledb.h"
#include "faciledb_internal.h"
#include "mema.h"
#include "index.h"
#include "hash.h"

static void search_db_data_sequential(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, DB_DATA_INFO_LIST_ENTRY_T *p_result_db_data_info_list_entry);
#if ENABLE_DB_INDEX
static void search_db_data_indexed(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, DB_DATA_INFO_LIST_ENTRY_T *p_result_db_data_list_entry);
#endif

// return value: DB_DATA_INFO_T array whose length is *p_result_db_data_info_num
void search_db_data(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, DB_DATA_INFO_LIST_ENTRY_T *p_result_db_data_info_list_entry)
{
#if ENABLE_DB_INDEX
    // check if index existed and call search_db_data_indexed.
    char *p_index_key = set_db_index_key(p_db_set_info->p_set_name, p_db_set_info->set_name_size, p_target_db_record_info->db_record.p_key, p_target_db_record_info->db_record_properties.key_size);
    if (Index_Api_Index_Key_Exist(p_index_key))
    {
        Mema_Api_Free(MEMA_USER_FACILEDB, p_index_key);
        return search_db_data_indexed(p_db_set_info, p_target_db_record_info, compare_type, p_result_db_data_info_list_entry);
    }
    else
    {
        Mema_Api_Free(MEMA_USER_FACILEDB, p_index_key);
    }
#endif
    // General sequential search
    return search_db_data_sequential(p_db_set_info, p_target_db_record_info, compare_type, p_result_db_data_info_list_entry);
}

// General sequential search
static void search_db_data_sequential(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, DB_DATA_INFO_LIST_ENTRY_T *p_result_db_data_info_list_entry)
{
    uint64_t block_num = p_db_set_info->db_set_properties.block_num;

    void *p_target_key = p_target_db_record_info->db_record.p_key;
    uint32_t target_key_size = p_target_db_record_info->db_record_properties.key_size;
    void *p_target_value = p_target_db_record_info->db_record.p_value;
    FACILEDB_RECORD_VALUE_TYPE_E target_value_type = p_target_db_record_info->db_record_properties.record_value_type;

    for (uint64_t block_tag = 1; block_tag <= block_num; block_tag++)
    {
        DB_DATA_INFO_LIST_NODE_T *p_db_data_info_list_node = NULL;
        DB_BLOCK_T db_block;
        bool record_match = false;

        db_block_init(&db_block);

        // read attribute only for checking delete flag and first block flag.
        if (read_db_block_attributes(p_db_set_info, block_tag, &db_block) <= 0)
        {
            assert(false);
        }

        if (db_block.deleted || db_block.prev_block_tag != 0)
        {
            continue;
        }

        p_db_data_info_list_node = Mema_Api_Alloc(MEMA_USER_FACILEDB, sizeof(DB_DATA_INFO_LIST_NODE_T));
        db_data_info_list_node_init(p_db_data_info_list_node);
        // Read the whole block and next blocks if they exists. The buffers will be allocated, and the record content will be copied into the record_info
        if (extract_db_data_info_from_db_blocks(&(p_db_data_info_list_node->db_data_info), block_tag, p_db_set_info) == false)
        {
            // invalid data (crc check fail / db block read fail / ...)
            Mema_Api_Free(MEMA_USER_FACILEDB, p_db_data_info_list_node);
            continue;
        }

        // Search if the target record matched or not.
        for (uint32_t record_idx = 0; record_idx < p_db_data_info_list_node->db_data_info.record_num; record_idx++)
        {
            if (target_key_size == p_db_data_info_list_node->db_data_info.p_db_record_info[record_idx].db_record_properties.key_size &&
                memcmp(p_db_data_info_list_node->db_data_info.p_db_record_info[record_idx].db_record.p_key, p_target_key, target_key_size) == 0 &&
                target_value_type == p_db_data_info_list_node->db_data_info.p_db_record_info[record_idx].db_record_properties.record_value_type &&
                // value size comparison doesn't need (?)
                (compare_type == DB_RECORD_VALUE_TYPE_COMPARE_ALL || db_record_value_type_compare(target_value_type, p_db_data_info_list_node->db_data_info.p_db_record_info[record_idx].db_record.p_value, p_target_value) == compare_type))
            {
                record_match = true;
                break;
            }
        }

        // Copy the matched key and value to p_result_db_data_infos array.
        if (record_match)
        {
            // append the node to the result list entry.
            if (p_result_db_data_info_list_entry->list_length == 0)
            {
                p_db_data_info_list_node->p_prev = p_db_data_info_list_node;
                p_db_data_info_list_node->p_next = p_db_data_info_list_node;

                p_result_db_data_info_list_entry->p_head = p_db_data_info_list_node;
                p_result_db_data_info_list_entry->p_tail = p_db_data_info_list_node;
            }
            else
            {
                p_db_data_info_list_node->p_prev = p_result_db_data_info_list_entry->p_tail;
                p_db_data_info_list_node->p_next = p_result_db_data_info_list_entry->p_head;

                p_result_db_data_info_list_entry->p_tail->p_next = p_db_data_info_list_node;
                p_result_db_data_info_list_entry->p_head->p_prev = p_db_data_info_list_node;

                p_result_db_data_info_list_entry->p_tail = p_db_data_info_list_node;
            }
            p_result_db_data_info_list_entry->list_length++;
        }
        else
        {
            free_db_data_info_list_node_resources(p_db_data_info_list_node);
            Mema_Api_Free(MEMA_USER_FACILEDB, p_db_data_info_list_node);
        }
    }
}

#if ENABLE_DB_INDEX
static void search_db_data_indexed(DB_SET_INFO_T *p_db_set_info, DB_RECORD_INFO_T *p_target_db_record_info, DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E compare_type, DB_DATA_INFO_LIST_ENTRY_T *p_result_db_data_list_entry)
{
    char *p_index_key = set_db_index_key(p_db_set_info->p_set_name, p_db_set_info->set_name_size, p_target_db_record_info->db_record.p_key, p_target_db_record_info->db_record_properties.key_size);
    void *p_record_value = p_target_db_record_info->db_record.p_value;
    void *p_index_id = NULL;
    HASH_VALUE_T hash_value = 0;
    INDEX_ID_TYPE_E index_id_type = get_db_index_id_type(p_target_db_record_info->db_record_properties.record_value_type);
    INDEX_SEARCH_RESULT_T *p_index_search_result = NULL;
    DB_INDEX_PAYLOAD_T *p_result_index_payloads = NULL;

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

        // if(compare_type == FACILEDB_RECORD_VALUE_TYPE_COMPARE_ALL)
        // {
        //     // TODO
        // }
        if (compare_type == DB_RECORD_VALUE_TYPE_COMPARE_EQUAL)
        {
            p_index_search_result = Index_Api_Search_Equal(p_index_key, p_index_id, index_id_type);
            p_result_index_payloads = (DB_INDEX_PAYLOAD_T *)p_index_search_result->p_result_array;
        }
        else
        {
            assert(0);
        }

        // Transfer db_index_payloads to db_data_infos
        for (uint32_t i = 0; i < p_index_search_result->result_length; i++)
        {
            DB_BLOCK_T db_block;
            DB_DATA_INFO_LIST_NODE_T *p_db_data_info_list_node = NULL;
            bool record_match = false;

            db_block_init(&db_block);

            // read attribute only for checking delete flag and first block flag.
            if (read_db_block_attributes(p_db_set_info, p_result_index_payloads[i].start_db_block_tag, &db_block) <= 0)
            {
                assert(false);
            }

            if (db_block.deleted || db_block.prev_block_tag != 0)
            {
                continue;
            }

            p_db_data_info_list_node = Mema_Api_Alloc(MEMA_USER_FACILEDB, sizeof(DB_DATA_INFO_LIST_NODE_T));
            db_data_info_list_node_init(p_db_data_info_list_node);
            // Read the whole block and next blocks if they exists. The buffers will be allocated, and the record content will be copied into the record_info
            extract_db_data_info_from_db_blocks(&(p_db_data_info_list_node->db_data_info), p_result_index_payloads[i].start_db_block_tag, p_db_set_info);

            // Compare again to prevent collision.
            for (uint32_t record_idx = 0; record_idx < (p_db_data_info_list_node->db_data_info.record_num); record_idx++)
            {
                if (p_target_db_record_info->db_record_properties.key_size == p_db_data_info_list_node->db_data_info.p_db_record_info[record_idx].db_record_properties.key_size &&
                    memcmp(p_db_data_info_list_node->db_data_info.p_db_record_info[record_idx].db_record.p_key, p_target_db_record_info->db_record.p_key, p_target_db_record_info->db_record_properties.key_size) == 0 &&
                    p_target_db_record_info->db_record_properties.record_value_type == p_db_data_info_list_node->db_data_info.p_db_record_info[record_idx].db_record_properties.record_value_type &&
                    (compare_type == DB_RECORD_VALUE_TYPE_COMPARE_ALL || db_record_value_type_compare(p_target_db_record_info->db_record_properties.record_value_type, p_db_data_info_list_node->db_data_info.p_db_record_info[record_idx].db_record.p_value, p_target_db_record_info->db_record.p_value) == compare_type))
                {
                    record_match = true;
                    break;
                }
            }

            if (record_match)
            {
                // insert the new node to the result list entry.
                if (p_result_db_data_list_entry->list_length == 0)
                {
                    p_result_db_data_list_entry->p_head = p_db_data_info_list_node;
                    p_result_db_data_list_entry->p_tail = p_db_data_info_list_node;

                    p_db_data_info_list_node->p_prev = p_db_data_info_list_node;
                    p_db_data_info_list_node->p_next = p_db_data_info_list_node;
                }
                else
                {
                    // append to the tail.
                    p_db_data_info_list_node->p_prev = p_result_db_data_list_entry->p_tail;
                    p_db_data_info_list_node->p_next = p_result_db_data_list_entry->p_head;

                    p_result_db_data_list_entry->p_tail->p_next = p_db_data_info_list_node;
                    p_result_db_data_list_entry->p_head->p_prev = p_db_data_info_list_node;

                    p_result_db_data_list_entry->p_tail = p_db_data_info_list_node;
                }
                p_result_db_data_list_entry->list_length++;

                // Because the data info resources still in-used for result, don't free data info resources here.
            }
            else
            {
                free_db_data_info_list_node_resources(p_db_data_info_list_node);
                Mema_Api_Free(MEMA_USER_FACILEDB, p_db_data_info_list_node);
            }
        }

        Index_Api_Free_Search_Result(p_index_search_result);
    }

    Mema_Api_Free(MEMA_USER_FACILEDB, p_index_key);
}
#endif