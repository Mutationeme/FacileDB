#include <stdint.h>
#include <string.h>
#include <assert.h>

#include "record_value_type.h"
#include "hash.h"

#define RECORD_VALUE_TYPE_DYNAMIC_SIZE (0)

typedef struct
{
    uint32_t size;
    RECORD_VALUE_TYPE_COMPARE_RESULT_E (*p_compare_func)(void *, void *);
} RECORD_VALUE_TYPE_HANDLE_TBL_T;

RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_hash_compare(void *value1, void *value2);
RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_uint32_compare(void *value1, void *value2);
RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_string_compare(void *value1, void *value2);

static const RECORD_VALUE_TYPE_HANDLE_TBL_T record_value_type_handle_table[RECORD_VALUE_TYPE_NUM] = {
#ifdef RECORD_VALUE_TYPE_CONFIG
#undef RECORD_VALUE_TYPE_CONFIG
#endif
#define RECORD_VALUE_TYPE_CONFIG(record_value_type, record_value_type_size, record_value_type_compare_function) {record_value_type_size, record_value_type_compare_function},
#include "record_value_type_table.h"
#undef RECORD_VALUE_TYPE_CONFIG
};

RECORD_VALUE_TYPE_COMPARE_RESULT_E Record_Value_Type_Api_Compare(RECORD_VALUE_TYPE_E record_value_type, void *value1, void *value2)
{
    assert(record_value_type < RECORD_VALUE_TYPE_NUM);
    return record_value_type_handle_table[record_value_type].p_compare_func(value1, value2);
}

bool Record_Value_Type_Api_Check_Size_Valid(RECORD_VALUE_TYPE_E record_value_type, uint32_t value_size)
{
    if(record_value_type >= RECORD_VALUE_TYPE_NUM)
    {
        return false;
    }
    else if(record_value_type_handle_table[record_value_type].size == RECORD_VALUE_TYPE_DYNAMIC_SIZE)
    {
        return true;
    }
    else
    {
        return (record_value_type_handle_table[record_value_type].size == value_size);
    }
}

// uint32_t Record_Value_Type_Api_Get_Size(FACILEDB_RECORD_VALUE_TYPE_E record_value_type)
// {
//     assert(record_value_type < FACILEDB_RECORD_VALUE_TYPE_NUM);
//     return db_record_value_type_handle_table[record_value_type].size;
// }

RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_hash_compare(void *value1, void *value2)
{
    HASH_VALUE_T hash_1 = *(HASH_VALUE_T *)value1, hash_2 = *(HASH_VALUE_T *)value2;
    HASH_VALUE_COMPARE_RESULT_E result = Hash_Api_Compare(hash_1, hash_2);

    switch(result)
    {
        case HASH_VALUE_COMPARE_LEFT_GREATER:
            return RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;

        case HASH_VALUE_COMPARE_RIGHT_GREATER:
            return RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;

        case HASH_VALUE_COMPARE_EQUAL:
            return RECORD_VALUE_TYPE_COMPARE_EQUAL;

        default:
            // Should not reach here
            assert(false);
            break;
    }
}

RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_uint32_compare(void *value1, void *value2)
{
    uint32_t val_1 = *(uint32_t *)value1, val_2 = *(uint32_t *)value2;

    if (val_1 > val_2)
    {
        return RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;
    }
    else if (val_1 < val_2)
    {
        return RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;
    }
    else
    {
        // val_1 == val_2
        return RECORD_VALUE_TYPE_COMPARE_EQUAL;
    }
}

RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_string_compare(void *value1, void *value2)
{
    int32_t str_cmp_result = strcmp((char *)value1, (char *)value2);

    if(str_cmp_result == 0)
    {
        return RECORD_VALUE_TYPE_COMPARE_EQUAL;
    }
    else if(str_cmp_result > 0)
    {
        return RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;
    }
    else
    {
        return RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;
    }
}
