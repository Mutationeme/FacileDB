#include <stdint.h>

#include "index_id_type_compare.h"
#include "hash.h"

INDEX_ID_COMPARE_RESULT_E Index_Id_Type_Api_Hash_Compare(void *value1, void *value2)
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

INDEX_ID_COMPARE_RESULT_E Index_Id_Type_Api_Uint32_Compare(void *value1, void *value2)
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
