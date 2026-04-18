#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>

#include "hash.h"
#include "faciledb.h"
#include "faciledb_record_value_type.h"

#define FACILEDB_RECORD_VALUE_TYPE_DYNAMIC_SIZE (0)

DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_hash_compare(void *value1, void *value2);
DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_uint32_compare(void *value1, void *value2);
DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_int32_compare(void *value1, void *value2);
DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_string_compare(void *value1, void *value2);

static const uint32_t record_value_type_size_table[] = {
#ifdef FACILEDB_RECORD_VALUE_TYPE_CONFIG
#undef FACILEDB_RECORD_VALUE_TYPE_CONFIG
#endif
#define FACILEDB_RECORD_VALUE_TYPE_CONFIG(record_value_type, record_value_type_size) record_value_type_size,
#include "faciledb_record_value_type_table.h"
#undef FACILEDB_RECORD_VALUE_TYPE_CONFIG
};

DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E db_record_value_type_compare(FACILEDB_RECORD_VALUE_TYPE_E record_value_type, void *value1, void *value2)
{
    switch (record_value_type)
    {
        case FACILEDB_RECORD_VALUE_TYPE_HASH:
            return record_value_type_hash_compare(value1, value2);
        case FACILEDB_RECORD_VALUE_TYPE_UINT32:
            return record_value_type_uint32_compare(value1, value2);
        case FACILEDB_RECORD_VALUE_TYPE_STRING:
            return record_value_type_string_compare(value1, value2);
        default:
            assert(false);
            break;
    }
}

bool db_record_value_type_check_size_valid(FACILEDB_RECORD_VALUE_TYPE_E record_value_type, uint32_t value_size)
{
    if (record_value_type >= FACILEDB_RECORD_VALUE_TYPE_NUM)
    {
        return false;
    }
    else if (record_value_type_size_table[record_value_type] == FACILEDB_RECORD_VALUE_TYPE_DYNAMIC_SIZE)
    {
        return true;
    }
    else
    {
        return (record_value_type_size_table[record_value_type] == value_size);
    }
}

DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_hash_compare(void *value1, void *value2)
{
    HASH_VALUE_T hash_1 = *(HASH_VALUE_T *)value1, hash_2 = *(HASH_VALUE_T *)value2;
    HASH_VALUE_COMPARE_RESULT_E result = Hash_Api_Compare(hash_1, hash_2);

    switch (result)
    {
    case HASH_VALUE_COMPARE_LEFT_GREATER:
        return DB_RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;

    case HASH_VALUE_COMPARE_RIGHT_GREATER:
        return DB_RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;

    case HASH_VALUE_COMPARE_EQUAL:
        return DB_RECORD_VALUE_TYPE_COMPARE_EQUAL;

    default:
        // Should not reach here
        assert(false);
        break;
    }
}

DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_uint32_compare(void *value1, void *value2)
{
    uint32_t val_1 = *(uint32_t *)value1, val_2 = *(uint32_t *)value2;

    if (val_1 > val_2)
    {
        return DB_RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;
    }
    else if (val_1 < val_2)
    {
        return DB_RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;
    }
    else
    {
        // val_1 == val_2
        return DB_RECORD_VALUE_TYPE_COMPARE_EQUAL;
    }
}

DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_int32_compare(void *value1, void *value2)
{
    int32_t val_1 = *(int32_t *)value1, val_2 = *(int32_t *)value2;

    if (val_1 > val_2)
    {
        return DB_RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;
    }
    else if (val_1 < val_2)
    {
        return DB_RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;
    }
    else
    {
        // val_1 == val_2
        return DB_RECORD_VALUE_TYPE_COMPARE_EQUAL;
    }
}

DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_uint64_compare(void *value1, void *value2)
{
    uint64_t val_1 = *(uint64_t *)value1, val_2 = *(uint64_t *)value2;

    if (val_1 > val_2)
    {
        return DB_RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;
    }
    else if (val_1 < val_2)
    {
        return DB_RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;
    }
    else
    {
        // val_1 == val_2
        return DB_RECORD_VALUE_TYPE_COMPARE_EQUAL;
    }
}

DB_RECORD_VALUE_TYPE_COMPARE_RESULT_E record_value_type_string_compare(void *value1, void *value2)
{
    int32_t str_cmp_result = strcmp((char *)value1, (char *)value2);

    if (str_cmp_result == 0)
    {
        return DB_RECORD_VALUE_TYPE_COMPARE_EQUAL;
    }
    else if (str_cmp_result > 0)
    {
        return DB_RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER;
    }
    else
    {
        return DB_RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER;
    }
}
