#include <stdint.h>
#include <stdbool.h>
#include <assert.h>

#include "faciledb_internal.h"

void delete_db_data(DB_SET_INFO_T *p_db_set_info, DB_DATA_INFO_LIST_ENTRY_T *p_db_data_info_list_entry)
{
    DB_BLOCK_T db_block;
    uint64_t block_tag = 0;
    db_block_init(&db_block);
    DB_DATA_INFO_LIST_NODE_T *p_current_node = p_db_data_info_list_entry->p_head;

    for (uint32_t i = 0; i < p_db_data_info_list_entry->list_length; i++)
    {
        block_tag = p_current_node->db_data_info.start_db_block_tag;

        while (block_tag != 0)
        {
            if (read_db_block(p_db_set_info, block_tag, &db_block) <= 0)
            {
                assert(false);
            }

            db_block.deleted = 1;
            write_db_block(&db_block, p_db_set_info);
            // continue to the next block.
            block_tag = db_block.next_block_tag;
        }

        p_current_node = p_current_node->p_next;
    }
}
