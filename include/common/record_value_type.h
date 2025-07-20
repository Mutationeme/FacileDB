#ifndef __RECORD_VALUE_TYPE_H__
#define __RECORD_VALUE_TYPE_H__

#include <stdint.h>
#include <stdbool.h>

typedef enum
{
    RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER = -1,
    RECORD_VALUE_TYPE_COMPARE_EQUAL = 0,
    RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER = 1,

    RECORD_VALUE_TYPE_COMPARE_GREATER_THAN = RECORD_VALUE_TYPE_COMPARE_LEFT_GREATER,
    RECORD_VALUE_TYPE_COMPARE_SMALLER_THAN = RECORD_VALUE_TYPE_COMPARE_RIGHT_GREATER,
} RECORD_VALUE_TYPE_COMPARE_RESULT_E;

typedef enum
{
#ifdef RECORD_VALUE_TYPE_CONFIG
#undef RECORD_VALUE_TYPE_CONFIG
#endif
#define RECORD_VALUE_TYPE_CONFIG(record_value_type, record_value_type_size, record_value_type_compare_function) record_value_type,
#include "record_value_type_table.h"
#undef RECORD_VALUE_TYPE_CONFIG
    RECORD_VALUE_TYPE_NUM,
    RECORD_VALUE_TYPE_INVALID
} RECORD_VALUE_TYPE_E;

RECORD_VALUE_TYPE_COMPARE_RESULT_E Record_Value_Type_Api_Compare(RECORD_VALUE_TYPE_E record_value_type, void *value1, void *value2);
// uint32_t Record_Value_Type_Api_Get_Size(RECORD_VALUE_TYPE_E record_value_type);
bool Record_Value_Type_Api_Check_Size_Valid(RECORD_VALUE_TYPE_E record_value_type, uint32_t value_size);


#endif //__RECORD_VALUE_TYPE_H__