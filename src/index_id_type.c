#include <stdint.h>
#include <assert.h>

#include "index_id_type.h"
#include "hash.h"

INDEX_ID_COMPARE_RESULT_E index_id_type_hash_compare(void *value1, void *value2);
INDEX_ID_COMPARE_RESULT_E index_id_type_uint32_compare(void *value1, void *value2);

static const uint32_t index_id_type_size[] = {
#ifdef INDEX_ID_TYPE_CONFIG
#undef INDEX_ID_TYPE_CONFIG
#endif
#define INDEX_ID_TYPE_CONFIG(index_id_type, index_id_size) index_id_size,
#include "index_id_type_table.h"
#undef INDEX_ID_TYPE_CONFIG
};

INDEX_ID_COMPARE_RESULT_E Index_Id_Type_Compare(INDEX_ID_TYPE_E index_id_type, void *value1, void *value2)
{
    switch(index_id_type)
    {
        case INDEX_ID_TYPE_HASH:
            return index_id_type_hash_compare(value1, value2);
        case INDEX_ID_TYPE_UINT32:
            return index_id_type_uint32_compare(value1, value2);
        default:
            assert(false);
            break;
    }
}

uint32_t Index_Id_Type_Get_Size(INDEX_ID_TYPE_E index_id_type)
{
    if(index_id_type >= INDEX_ID_TYPE_NUM)
    {
        return 0;
    }
    else
    {
        return index_id_type_size[index_id_type];
    }
}

INDEX_ID_COMPARE_RESULT_E index_id_type_hash_compare(void *value1, void *value2)
{
    HASH_VALUE_T hash_1 = *(HASH_VALUE_T *)value1, hash_2 = *(HASH_VALUE_T *)value2;
    HASH_VALUE_COMPARE_RESULT_E result = Hash_Api_Compare(hash_1, hash_2);
    if (result == HASH_VALUE_COMPARE_LEFT_GREATER)
    {
        return INDEX_ID_COMPARE_LEFT_GREATER;
    }
    else if (result == HASH_VALUE_COMPARE_RIGHT_GREATER)
    {
        return INDEX_ID_COMPARE_RIGHT_GREATER;
    }
    else
    {
        // HASH_VALUE_COMPARE_EQUAL
        return INDEX_ID_COMPARE_EQUAL;
    }
}

INDEX_ID_COMPARE_RESULT_E index_id_type_uint32_compare(void *value1, void *value2)
{
    uint32_t val_1 = *(uint32_t *)value1, val_2 = *(uint32_t *)value2;

    if (val_1 > val_2)
    {
        return INDEX_ID_COMPARE_LEFT_GREATER;
    }
    else if (val_1 < val_2)
    {
        return INDEX_ID_COMPARE_RIGHT_GREATER;
    }
    else
    {
        // val_1 == val_2
        return INDEX_ID_COMPARE_EQUAL;
    }
}
