#include <string.h>
#include <stdbool.h>

#include "mema.h"
#include "faciledb_record_value_type.h"
#include "faciledb_internal.h"
#include "faciledb.h"

#if ENABLE_DB_INDEX
#include "index.h"
#endif

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

    Mema_Api_Init(MEMA_USER_FACILEDB);
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
        // TODO: replace to load
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
    if(p_db_set_info == NULL)
    {
        return 0;
    }

    // convert format from FACILEDB_DATA_T to DB_DATA_INFO_T
    db_data_info_init(&db_data_info);
    shallow_assign_faciledb_data_to_db_data_info(&db_data_info, p_faciledb_data);

    db_set_info_sync_write_wait(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_WRITING);
    db_set_info_file_lock_write(p_db_set_info);
    // read db_set_properties from disk to ensure data consistency
    // Future: mmap (?)
    sync_latest_db_set_info(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    data_tag = ++(p_db_set_info->db_set_properties.data_num);
    insert_db_data(p_db_set_info, &db_data_info, data_tag);

    lock_db_set_info_sync(p_db_set_info);
    db_set_info_file_unlock_write(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY);
    db_set_info_sync_write_unblock(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    // free dynamic resources allocated at shallow_assign_faciledb_data_to_db_data_info.
    Mema_Api_Free(MEMA_USER_FACILEDB, db_data_info.p_db_record_info);

    return 1;
}

// return value: search result, the p_data_array will be NULL if no data found (data_num == 0).
FACILEDB_DATA_SEARCH_RESULT_T *FacileDB_Api_Search_Equal(char *p_db_set_name, FACILEDB_RECORD_T *p_faciledb_record)
{
    char temp_db_set_name[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};
    FACILEDB_DATA_SEARCH_RESULT_T *p_faciledb_search_result = Mema_Api_Alloc(MEMA_USER_FACILEDB, sizeof(FACILEDB_DATA_SEARCH_RESULT_T));

    DB_SET_INFO_T *p_db_set_info = NULL;
    DB_RECORD_INFO_T target_db_record;
    DB_DATA_INFO_LIST_ENTRY_T db_data_info_list_entry;

    db_data_info_list_entry_init(&db_data_info_list_entry);

    // default return value
    p_faciledb_search_result->data_num = 0;
    p_faciledb_search_result->p_data_array = NULL;

    // Check input parameters
    if (p_db_set_name == NULL || p_faciledb_record == NULL ||
        db_record_value_type_check_size_valid(p_faciledb_record->record_value_type, p_faciledb_record->value_size) == false)
    {
        // invalid
        return p_faciledb_search_result;
    }

    strncpy(temp_db_set_name, p_db_set_name, FACILEDB_FILE_PATH_MAX_LENGTH);
    temp_db_set_name[FACILEDB_FILE_PATH_MAX_LENGTH] = '\0';

    lock_db_context_sync();

    if (check_db_context_status(DB_CONTEXT_STATUS_READY) == false)
    {
        // db context is not ready
        unlock_db_context_sync();
        return p_faciledb_search_result;
    }

    p_db_set_info = load_and_lock_db_set_info(temp_db_set_name);
    unlock_db_context_sync();
    if(p_db_set_info == NULL)
    {
        return p_faciledb_search_result;
    }

    db_record_info_init(&target_db_record);
    shallow_assign_faciledb_record_to_db_record_info(&target_db_record, p_faciledb_record);

    db_set_info_sync_read_wait(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READING);
    db_set_info_file_lock_read(p_db_set_info);
    sync_latest_db_set_info(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    search_db_data(p_db_set_info, &target_db_record, DB_RECORD_VALUE_TYPE_COMPARE_EQUAL, &db_data_info_list_entry);

    lock_db_set_info_sync(p_db_set_info);
    db_set_info_file_unlock_read(p_db_set_info);
    update_db_set_info_status_from_reading(p_db_set_info);
    db_set_info_sync_read_unblock(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    // Fill to faciledb structure
    if (db_data_info_list_entry.list_length > 0)
    {
        p_faciledb_search_result->p_data_array = Mema_Api_Alloc(MEMA_USER_FACILEDB, db_data_info_list_entry.list_length * sizeof(FACILEDB_DATA_T));

        DB_DATA_INFO_LIST_NODE_T *p_current_node = db_data_info_list_entry.p_head;
        DB_DATA_INFO_LIST_NODE_T *p_freed_node = NULL;

        for (uint32_t i = 0; i < db_data_info_list_entry.list_length; i++)
        {
            shallow_assign_db_data_info_to_failedb_data(&(p_faciledb_search_result->p_data_array[i]), &(p_current_node->db_data_info));
            p_freed_node = p_current_node;
            p_current_node = p_current_node->p_next;

            // free linked-list node and record_info array, but keep db_record_info dynamic resources alive for faciledb search result.
            Mema_Api_Free(MEMA_USER_FACILEDB, p_freed_node->db_data_info.p_db_record_info);
            Mema_Api_Free(MEMA_USER_FACILEDB, p_freed_node);
        }

        p_faciledb_search_result->data_num = db_data_info_list_entry.list_length;
    }

    return p_faciledb_search_result;
}

// Return value: delete data number
uint32_t FacileDB_Api_Delete_Equal(char *p_db_set_name, FACILEDB_RECORD_T *p_faciledb_record)
{
    char temp_db_set_name[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};
    DB_SET_INFO_T *p_db_set_info = NULL;
    DB_RECORD_INFO_T target_db_record;
    DB_DATA_INFO_LIST_ENTRY_T db_data_info_list_entry;
    uint32_t delete_data_num = 0;

    db_data_info_list_entry_init(&db_data_info_list_entry);

    // Check input parameters
    if (p_db_set_name == NULL || p_faciledb_record == NULL || db_record_value_type_check_size_valid(p_faciledb_record->record_value_type, p_faciledb_record->value_size) == false)
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
    if(p_db_set_info == NULL)
    {
        return 0;
    }

    db_record_info_init(&target_db_record);
    shallow_assign_faciledb_record_to_db_record_info(&target_db_record, p_faciledb_record);

    db_set_info_sync_write_wait(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_WRITING);
    db_set_info_file_lock_write(p_db_set_info);
    sync_latest_db_set_info(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    search_db_data(p_db_set_info, &target_db_record, DB_RECORD_VALUE_TYPE_COMPARE_EQUAL, &db_data_info_list_entry);
    // Delete the target data
    delete_db_data(p_db_set_info, &db_data_info_list_entry);

    lock_db_set_info_sync(p_db_set_info);
    db_set_info_file_unlock_write(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY);
    db_set_info_sync_write_unblock(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    delete_data_num = db_data_info_list_entry.list_length;
    // free result linked list
    free_db_data_info_list(&db_data_info_list_entry);

    return delete_data_num;
}

void FacileDB_Api_Free_Search_Result(FACILEDB_DATA_SEARCH_RESULT_T *p_faciledb_search_result)
{
    if (p_faciledb_search_result == NULL)
    {
        return;
    }

    for (uint32_t i = 0; i < p_faciledb_search_result->data_num; i++)
    {
        for (uint32_t j = 0; j < p_faciledb_search_result->p_data_array[i].record_num; j++)
        {
            // TODO: free faciledb_record dynamic buffer
            Mema_Api_Free(MEMA_USER_FACILEDB, p_faciledb_search_result->p_data_array[i].p_data_records[j].p_key);
            Mema_Api_Free(MEMA_USER_FACILEDB, p_faciledb_search_result->p_data_array[i].p_data_records[j].p_value);
        }
        // if data exists, record_num > 0 and p_data_records is allocated.
        Mema_Api_Free(MEMA_USER_FACILEDB, p_faciledb_search_result->p_data_array[i].p_data_records);
    }

    if (p_faciledb_search_result->data_num != 0)
    {
        Mema_Api_Free(MEMA_USER_FACILEDB, p_faciledb_search_result->p_data_array);
    }

    Mema_Api_Free(MEMA_USER_FACILEDB, p_faciledb_search_result);
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
    if (p_db_set_info == NULL)
    {
        return false;
    }

    db_record_info_init(&target_db_record);
    shallow_assign_faciledb_record_to_db_record_info(&target_db_record, p_faciledb_record);

    // use write lock for make index (insert new sys data blocks)
    db_set_info_sync_write_wait(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_WRITING);
    db_set_info_file_lock_write(p_db_set_info);
    sync_latest_db_set_info(p_db_set_info);
    unlock_db_set_info_sync(p_db_set_info);

    result_data_num = make_db_record_index(p_db_set_info, &target_db_record);

    lock_db_set_info_sync(p_db_set_info);
    db_set_info_file_unlock_write(p_db_set_info);
    update_db_set_info_status(p_db_set_info, DB_SET_INFO_STATUS_READY);
    db_set_info_sync_write_unblock(p_db_set_info);
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
#endif
