#include <stdint.h>
#include <stdbool.h>

#include "hash.h"

HASH_VALUE_T djb_hash(uint8_t *p_str, uint32_t length);


HASH_VALUE_T Hash(uint8_t *p_str, uint32_t length)
{
    return djb_hash(p_str, length);
}

HASH_VALUE_T djb_hash(uint8_t *p_str, uint32_t length)
{
    HASH_VALUE_T hash_value = 5381;

    for (uint32_t i = 0; i < length; i++)
    {
        // hashValue = hashValue * 33 + str[i]
        hash_value = (hash_value << 5) + hash_value + p_str[i];
    }

    return hash_value;
}

HASH_VALUE_COMPARE_RESULT_E Hash_Api_Compare(HASH_VALUE_T val1, HASH_VALUE_T val2)
{    
    if(val1 > val2)
    {
        return HASH_VALUE_COMPARE_LEFT_GREATER;
    }
    else if(val1 < val2)
    {
        return HASH_VALUE_COMPARE_RIGHT_GREATER;
    }
    else
    {
        return HASH_VALUE_COMPARE_EQUAL;
    }
}
