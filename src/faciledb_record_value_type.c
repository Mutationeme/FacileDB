#include <stdint.h>
#include <string.h>
#include <assert.h>
#include "faciledb_record_value_type.h"
#include "hash.h"

typedef struct
{
    uint32_t size;
    FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E (*p_compare_func)(void *, void *);
} DB_RECORD_VALUE_TYPE_HANDLE_TBL_T;

FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E faciledb_record_value_type_hash_compare(void *value1, void *value2);
FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E faciledb_record_value_type_uint32_compare(void *value1, void *value2);
FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E faciledb_record_value_type_string_compare(void *value1, void *value2);

static const DB_RECORD_VALUE_TYPE_HANDLE_TBL_T db_record_value_type_handle_table[FACILEDB_RECORD_VALUE_TYPE_NUM] = {
#ifdef FACILEDB_RECORD_VALUE_TYPE_CONFIG
#undef FACILEDB_RECORD_VALUE_TYPE_CONFIG
#endif
#define FACILEDB_RECORD_VALUE_TYPE_CONFIG(faciledb_record_value_type, faciledb_record_value_type_size, faciledb_record_value_type_compare_function) {faciledb_record_value_type_size, faciledb_record_value_type_compare_function},
#include "faciledb_record_value_type_table.h"
#undef FACILEDB_RECORD_VALUE_TYPE_CONFIG
};

FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E FacileDB_Record_Value_Type_Api_Compare(FACILEDB_RECORD_VALUE_TYPE_E record_value_type, void *value1, void *value2)
{
    assert(record_value_type < FACILEDB_RECORD_VALUE_TYPE_NUM);
    return db_record_value_type_handle_table[record_value_type].p_compare_func(value1, value2);
}

uint32_t FacileDB_Record_Value_Type_Api_Get_Size(FACILEDB_RECORD_VALUE_TYPE_E record_value_type)
{
    assert(record_value_type < FACILEDB_RECORD_VALUE_TYPE_NUM);
    return db_record_value_type_handle_table[record_value_type].size;
}

FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E faciledb_record_value_type_hash_compare(void *value1, void *value2)
{
    HASH_VALUE_T hash_1 = *(HASH_VALUE_T *)value1, hash_2 = *(HASH_VALUE_T *)value2;
    HASH_VALUE_COMPARE_RESULT_E result = Hash_Api_Compare(hash_1, hash_2);

    switch(result)
    {
        case HASH_VALUE_COMPARE_LEFT_GREATER:
            return FACILEDB_RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;

        case HASH_VALUE_COMPARE_RIGHT_GREATER:
            return FACILEDB_RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;

        case HASH_VALUE_COMPARE_EQUAL:
            return FACILEDB_RECORD_VALUE_TYPE_COMPARE_EQUAL;

        default:
            // Should not reach here
            assert(false);
            break;
    }
}

FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E faciledb_record_value_type_uint32_compare(void *value1, void *value2)
{
    uint32_t val_1 = *(uint32_t *)value1, val_2 = *(uint32_t *)value2;

    if (val_1 > val_2)
    {
        return FACILEDB_RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;
    }
    else if (val_1 < val_2)
    {
        return FACILEDB_RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;
    }
    else
    {
        // val_1 == val_2
        return FACILEDB_RECORD_VALUE_TYPE_COMPARE_EQUAL;
    }
}

FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E faciledb_record_value_type_string_compare(void *value1, void *value2)
{
    int32_t str_cmp_result = strcmp((char *)value1, (char *)value2);

    if(str_cmp_result == 0)
    {
        return FACILEDB_RECORD_VALUE_TYPE_COMPARE_EQUAL;
    }
    else if(str_cmp_result > 0)
    {
        return FACILEDB_RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;
    }
    else
    {
        return FACILEDB_RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;
    }
}
