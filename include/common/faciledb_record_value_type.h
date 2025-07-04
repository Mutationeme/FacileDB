#ifndef __FACILEDB_RECORD_VALUE_TYPE_H__
#define __FACILEDB_RECORD_VALUE_TYPE_H__

#define FACILEDB_RECORD_VALUE_TYPE_DYNAMIC_SIZE (0)

typedef enum
{
    FACILEDB_RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER = -1,
    FACILEDB_RECORD_VALUE_TYPE_COMPARE_EQUAL = 0,
    FACILEDB_RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER = 1,

    FACILEDB_RECORD_VALUE_TYPE_COMPARE_GREATER_THAN = FACILEDB_RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER,
    FACILEDB_RECORD_VALUE_TYPE_COMPARE_SMALLER_THAN = FACILEDB_RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER,
} FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E;

typedef enum
{
#ifdef FACILEDB_RECORD_VALUE_TYPE_CONFIG
#undef FACILEDB_RECORD_VALUE_TYPE_CONFIG
#endif
#define FACILEDB_RECORD_VALUE_TYPE_CONFIG(faciledb_record_value_type, faciledb_record_value_type_size, faciledb_record_value_type_compare_function) faciledb_record_value_type,
#include "faciledb_record_value_type_table.h"
#undef FACILEDB_RECORD_VALUE_TYPE_CONFIG
    FACILEDB_RECORD_VALUE_TYPE_NUM,
    FACILEDB_RECORD_VALUE_TYPE_INVALID
} FACILEDB_RECORD_VALUE_TYPE_E;

FACILEDB_RECORD_VALUE_TYPE_COMPARE_RESULT_E FacileDB_Record_Value_Type_Api_Compare(FACILEDB_RECORD_VALUE_TYPE_E record_value_type, void *value1, void *value2);
uint32_t FacileDB_Record_Value_Type_Api_Get_Size(FACILEDB_RECORD_VALUE_TYPE_E record_value_type);
#endif //__FACILEDB_RECORD_VALUE_TYPE_H__