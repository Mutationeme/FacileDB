/*
Format: INDEX_ID_TYPE_CONFIG(type_name, sizeof_type, compare_function)

type_name: enum name of index id type
sizeof_type: size of the index id, bytes.
*/

INDEX_ID_TYPE_CONFIG(INDEX_ID_TYPE_HASH, sizeof(HASH_VALUE_T))
INDEX_ID_TYPE_CONFIG(INDEX_ID_TYPE_UINT32, sizeof(uint32_t))
INDEX_ID_TYPE_CONFIG(INDEX_ID_TYPE_INT32, sizeof(int32_t))
INDEX_ID_TYPE_CONFIG(INDEX_ID_TYPE_UINT64, sizeof(uint64_t))
INDEX_ID_TYPE_CONFIG(INDEX_ID_TYPE_INT64, sizeof(int64_t))
INDEX_ID_TYPE_CONFIG(INDEX_ID_TYPE_FLOAT, sizeof(float))
INDEX_ID_TYPE_CONFIG(INDEX_ID_TYPE_DOUBLE, sizeof(double))