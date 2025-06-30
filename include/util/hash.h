#ifndef __HASH_H__
#define __HASH_H__

#include <stdint.h>
#include <stdbool.h>

#define HASH_VALUE_T uint32_t

typedef enum
{
    HASH_VALUE_COMPARE_RIGHT_GREATER = -1,
    HASH_VALUE_COMPARE_EQUAL = 0,
    HASH_VALUE_COMPARE_LEFT_GREATER = 1
} HASH_VALUE_COMPARE_RESULT_E;

HASH_VALUE_T Hash(uint8_t *str, uint32_t length);
HASH_VALUE_COMPARE_RESULT_E Hash_Api_Compare(HASH_VALUE_T val1, HASH_VALUE_T val2);

#endif