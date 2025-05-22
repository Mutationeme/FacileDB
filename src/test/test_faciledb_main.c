#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>

#define __FACILEDB_TEST__
// #define __PRINT_DETAILS__
#define DB_SET_INFO_INSTANCE_NUM (1)
// #define DB_BLOCK_DATA_SIZE ((16 + 6 + 6) * 2 - 4) // 52
#define DB_BLOCK_DATA_SIZE (50) // 49 ~

#include "faciledb.c"

void test_start(char *case_name)
{
    printf("Test Case: %s START!\n", case_name);
}

void test_end(char *case_name)
{
    printf("Test Case: %s END!\n\n", case_name);
}

void check_faciledb_properties(DB_SET_PROPERTIES_T *p_db_set_properties_1, DB_SET_PROPERTIES_T *p_db_set_properties_2)
{
    assert(p_db_set_properties_1->block_num == p_db_set_properties_2->block_num);
    assert(p_db_set_properties_1->valid_record_num == p_db_set_properties_2->valid_record_num);
    assert(p_db_set_properties_1->set_name_size == p_db_set_properties_2->set_name_size);
    assert(memcmp(p_db_set_properties_1->p_set_name, p_db_set_properties_2->p_set_name, p_db_set_properties_1->set_name_size) == 0);

#if defined(__PRINT_DETAILS__)
    DB_SET_PROPERTIES_T *p_db_set_properties_print = p_db_set_properties_1;
    char *p_set_name_buffer = NULL;

    printf("block_num: %" PRIu64 "\n", p_db_set_properties_print->block_num);
    printf("created_time: %" PRIu64 "\n", p_db_set_properties_print->created_time);
    printf("modified_time: %" PRIu64 "\n", p_db_set_properties_print->modified_time);
    printf("valid_record_num: %" PRIu64 "\n", p_db_set_properties_print->valid_record_num);
    printf("set_name_size: %" PRIu32 "\n", p_db_set_properties_print->set_name_size);

    p_set_name_buffer = calloc(p_db_set_properties_print->set_name_size + 1, sizeof(uint8_t));
    memcpy(p_set_name_buffer, p_db_set_properties_print->p_set_name, p_db_set_properties_print->set_name_size);
    printf("p_set_name: %s\n", p_set_name_buffer);

    free(p_set_name_buffer);
#endif
}

void check_faciledb_block(DB_BLOCK_T *p_db_block_1, DB_BLOCK_T *p_db_block_2)
{
    assert(p_db_block_1->block_tag == p_db_block_2->block_tag);
    assert(p_db_block_1->data_tag == p_db_block_2->data_tag);
    assert(p_db_block_1->prev_block_tag == p_db_block_2->prev_block_tag);
    assert(p_db_block_1->next_block_tag == p_db_block_2->next_block_tag);
    assert(p_db_block_1->deleted == p_db_block_2->deleted);
    assert(p_db_block_1->valid_record_num == p_db_block_2->valid_record_num);
    assert(p_db_block_1->record_properties_num == p_db_block_2->record_properties_num);

    // assert(memcmp(p_db_block_1->block_data, p_db_block_2->block_data, DB_BLOCK_DATA_SIZE) == 0);

#if defined(__PRINT_DETAILS__)
    DB_BLOCK_T *p_db_block_print = p_db_block_1;
    printf("block_tag: %" PRIu64 "\n", p_db_block_print->block_tag);
    printf("data_tag: %" PRId64 "\n", p_db_block_print->data_tag);
    printf("prev_block_tag: %" PRIu64 "\n", p_db_block_print->prev_block_tag);
    printf("next_block_tag: %" PRIu64 "\n", p_db_block_print->next_block_tag);
    printf("created_time: %" PRIu64 "\n", p_db_block_print->created_time);
    printf("modified_time: %" PRIu64 "\n", p_db_block_print->modified_time);
    printf("deleted: %" PRIu32 "\n", p_db_block_print->deleted);
    printf("valid_record_num: %" PRIu32 "\n", p_db_block_print->valid_record_num);
    printf("record_properties_number: %" PRIu32 "\n", p_db_block_print->record_properties_num);
#endif
}

void check_faciledb_records(DB_RECORD_INFO_T *p_db_record_info_1, uint32_t db_record_length_1, DB_RECORD_INFO_T *p_db_record_info_2, uint32_t db_record_length_2)
{
    assert(db_record_length_1 == db_record_length_2);
    for (uint32_t i = 0; i < db_record_length_1; i++)
    {
        assert(p_db_record_info_1[i].db_record_properties_offset == p_db_record_info_2[i].db_record_properties_offset);

        assert(p_db_record_info_1[i].db_record_properties.deleted == p_db_record_info_2[i].db_record_properties.deleted);
        assert(p_db_record_info_1[i].db_record_properties.key_size == p_db_record_info_2[i].db_record_properties.key_size);
        assert(p_db_record_info_1[i].db_record_properties.value_size == p_db_record_info_2[i].db_record_properties.value_size);
        assert(p_db_record_info_1[i].db_record_properties.record_value_type == p_db_record_info_2[i].db_record_properties.record_value_type);

        assert(memcmp(p_db_record_info_1[i].db_record.p_key, p_db_record_info_2[i].db_record.p_key, p_db_record_info_1->db_record_properties.key_size) == 0);
        assert(memcmp(p_db_record_info_1[i].db_record.p_value, p_db_record_info_2[i].db_record.p_value, p_db_record_info_1->db_record_properties.value_size) == 0);

#if defined(__PRINT_DETAILS__)
        DB_RECORD_INFO_T *p_db_record_info_print = &(p_db_record_info_1[i]);
        char key_string[p_db_record_info_print->db_record_properties.key_size + 1];

        printf("\t--Record: %d--\n", i);
        printf("record_properties_offset: %llu\n", p_db_record_info_print->db_record_properties_offset);
        printf("deleted: %" PRIu32 "\n", p_db_record_info_print->db_record_properties.deleted);
        printf("key_size: %" PRIu32 "\n", p_db_record_info_print->db_record_properties.key_size);
        printf("value_size: %" PRIu32 "\n", p_db_record_info_print->db_record_properties.value_size);
        printf("record_value_type: %" PRIu32 "\n", p_db_record_info_print->db_record_properties.record_value_type);

        memset(key_string, 0, p_db_record_info_print->db_record_properties.key_size + 1);
        memcpy(key_string, p_db_record_info_print->db_record.p_key, p_db_record_info_print->db_record_properties.key_size);
        printf("key: %s\n", key_string);
        if (p_db_record_info_print->db_record_properties.record_value_type == FACILEDB_RECORD_VALUE_TYPE_UINT32)
        {
            printf("value: %" PRIu32 "\n", *((uint32_t *)p_db_record_info_print->db_record.p_value));
        }
        else if (p_db_record_info_print->db_record_properties.record_value_type == FACILEDB_RECORD_VALUE_TYPE_STRING)
        {
            printf("value: %s\n", (char *)p_db_record_info_print->db_record.p_value);
        }
#endif
    }
}

void check_faciledb_search_result(FACILEDB_DATA_T *p_faciledb_data_array_1, uint32_t data_array_1_length, FACILEDB_DATA_T *p_faciledb_data_array_2, uint32_t data_array_2_length)
{
    assert(data_array_1_length == data_array_2_length);

    for(uint32_t i = 0; i < data_array_1_length; i++)
    {
        assert(p_faciledb_data_array_1[i].data_num == p_faciledb_data_array_2[i].data_num);
        
        for(uint32_t j = 0; j < p_faciledb_data_array_1[i].data_num; j++)
        {
            assert(p_faciledb_data_array_1[i].p_data_records[j].key_size == p_faciledb_data_array_2[i].p_data_records[j].key_size);
            assert(p_faciledb_data_array_1[i].p_data_records[j].value_size == p_faciledb_data_array_2[i].p_data_records[j].value_size);
            assert(p_faciledb_data_array_1[i].p_data_records[j].record_value_type == p_faciledb_data_array_2[i].p_data_records[j].record_value_type);
            assert(memcmp(p_faciledb_data_array_1[i].p_data_records[j].p_key, p_faciledb_data_array_2[i].p_data_records[j].p_key, p_faciledb_data_array_1[i].p_data_records[j].key_size) == 0);
            assert(memcmp(p_faciledb_data_array_1[i].p_data_records[j].p_value, p_faciledb_data_array_2[i].p_data_records[j].p_value, p_faciledb_data_array_1[i].p_data_records[j].value_size) == 0);
        }
    }
}

/////////////////////////

char test_db_directory[] = "./bin/test_db_files/";

void test_faciledb_init()
{
    char case_name[] = "test_faciledb_init";
    test_start(case_name);

    FacileDB_Api_Init(test_db_directory);

    assert(strcmp(test_db_directory, db_directory_path) == 0);

    FacileDB_Api_Close();

    test_end(case_name);
}

void test_faciledb_close()
{
    char case_name[] = "test_faciledb_init";
    test_start(case_name);

    FacileDB_Api_Init(test_db_directory);
    FacileDB_Api_Close();

    assert(db_set_info_instance[0].file == NULL);
    assert(strcmp(db_directory_path, "") == 0);

    test_end(case_name);
}

// one data each with one block.
void test_faciledb_insert_case1()
{
    char case_name[] = "test_faciledb_insert_case1";
    test_start(case_name);

    char db_set_name[] = "test_db_insert_case1";
    // clang-format off
    FACILEDB_DATA_T data = {
        .data_num = 1,
        .p_data_records = (FACILEDB_RECORD_T[]){
            {
                .key_size = 2, // 'a' and '\0'
                .p_key = (void *)"a",
                .value_size = sizeof(uint32_t),
                .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                .p_value = (void *)&(uint32_t){1}
            }
        }
    };
    // clang-format on

    FacileDB_Api_Init(test_db_directory);
    FacileDB_Api_Insert_Element(db_set_name, &data);
    FacileDB_Api_Close();

    // check
    FacileDB_Api_Init(test_db_directory);

    DB_SET_INFO_T *p_db_set_info = NULL;
    // check db_set_info
    p_db_set_info = load_db_set_info(db_set_name);
    assert((p_db_set_info != NULL) && (p_db_set_info->file != NULL));

    // check db_set_properties.
    // clang-format off
    DB_SET_PROPERTIES_T expect_db_set_properties = {
        .block_num = 1,
        .valid_record_num = 1,
        .set_name_size = strlen(db_set_name),
        .p_set_name = db_set_name
    };
    // clang-format on
    check_faciledb_properties(&(p_db_set_info->db_set_properties), &expect_db_set_properties);

    // check the db block.
    DB_BLOCK_T db_block;
    // clang-format off
    DB_BLOCK_T expected_db_block = {
        .block_tag = 1,
        .data_tag = 1,
        .prev_block_tag = 0,
        .next_block_tag = 0,
        .deleted = 0,
        .valid_record_num = 1,
        .record_properties_num = 1
    };
    // clang-format on
    // block_data
    // memcpy()
    db_block_init(&db_block);
    read_db_block(p_db_set_info, 1, &db_block);
    check_faciledb_block(&db_block, &expected_db_block);

    // check the db records
    // clang-format off
    uint32_t record_num = 0;
    DB_RECORD_INFO_T *p_db_records_info = extract_db_record_info_from_db_blocks(1, p_db_set_info, &record_num);
    DB_RECORD_INFO_T expected_db_record = {
        .db_record = (DB_RECORD_T){
            .p_key = data.p_data_records->p_key,
            .p_value = data.p_data_records->p_value
        },
        .db_record_properties = (DB_RECORD_PROPERTIES_T){
            .deleted = 0,
            .key_size = data.p_data_records->key_size,
            .record_value_type = data.p_data_records->record_value_type,
            .value_size = data.p_data_records->value_size
        },
        .db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), 1) + (((uint64_t)&expected_db_block.block_data) - ((uint64_t)&expected_db_block))
    };
    // clang-format on
    check_faciledb_records(p_db_records_info, record_num, &expected_db_record, 1);

    for (uint32_t i = 0; i < record_num; i++)
    {
        free_db_record_info_resources(&(p_db_records_info[i]));
    }
    FacileDB_Api_Close();

    test_end(case_name);
}

// One data each with one block that record value type is string.
void test_faciledb_insert_case2()
{
    char case_name[] = "test_faciledb_insert_case2";
    test_start(case_name);

    char db_set_name[] = "test_db_insert_case2";
    // clang-format off
    FACILEDB_DATA_T data = {
        .data_num = 1,
        .p_data_records = (FACILEDB_RECORD_T[]){
            {
                .key_size = (1 + 1),
                .p_key = (void *)"a",
                .value_size = (2 + 1),
                .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                .p_value = (void *)"aa"
            }
        }
    };
    // clang-format on

    FacileDB_Api_Init(test_db_directory);
    FacileDB_Api_Insert_Element(db_set_name, &data);
    FacileDB_Api_Close();

    // check
    FacileDB_Api_Init(test_db_directory);

    DB_SET_INFO_T *p_db_set_info = NULL;
    // check db_set_info
    p_db_set_info = load_db_set_info(db_set_name);
    assert((p_db_set_info != NULL) && (p_db_set_info->file != NULL));

    // check db_set_properties.
    // clang-format off
    DB_SET_PROPERTIES_T expect_db_set_properties = {
        .block_num = 1,
        .valid_record_num = 1,
        .set_name_size = strlen(db_set_name),
        .p_set_name = db_set_name
    };
    // clang-format on
    check_faciledb_properties(&(p_db_set_info->db_set_properties), &expect_db_set_properties);

    // check the db block.
    DB_BLOCK_T db_block;
    // clang-format off
    DB_BLOCK_T expected_db_block = {
        .block_tag = 1,
        .data_tag = 1,
        .prev_block_tag = 0,
        .next_block_tag = 0,
        .deleted = 0,
        .valid_record_num = 1,
        .record_properties_num = 1
    };
    // clang-format on
    // block_data
    // memcpy()
    db_block_init(&db_block);
    read_db_block(p_db_set_info, 1, &db_block);
    check_faciledb_block(&db_block, &expected_db_block);

    // check the db records
    // clang-format off
    uint32_t record_num = 0;
    DB_RECORD_INFO_T *p_db_records_info = extract_db_record_info_from_db_blocks(1, p_db_set_info, &record_num);
    DB_RECORD_INFO_T expected_db_record = {
        .db_record = (DB_RECORD_T){
            .p_key = data.p_data_records->p_key,
            .p_value = data.p_data_records->p_value
        },
        .db_record_properties = (DB_RECORD_PROPERTIES_T){
            .deleted = 0,
            .key_size = data.p_data_records->key_size,
            .record_value_type = data.p_data_records->record_value_type,
            .value_size = data.p_data_records->value_size
        },
        .db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), 1) + (((uint64_t)&expected_db_block.block_data) - ((uint64_t)&expected_db_block))
    };
    // clang-format on
    check_faciledb_records(p_db_records_info, record_num, &expected_db_record, 1);

    for (uint32_t i = 0; i < record_num; i++)
    {
        free_db_record_info_resources(&(p_db_records_info[i]));
    }
    FacileDB_Api_Close();

    test_end(case_name);
}

// One data each with one block.
void test_faciledb_insert_case4()
{
    char case_name[] = "test_faciledb_insert_case4";
    test_start(case_name);

    char db_set_name[] = "test_db_insert_case4";
    // clang-format off
    FACILEDB_DATA_T data1 = {
        .data_num = 1,
        .p_data_records = (FACILEDB_RECORD_T[]){
            {
                .key_size = (1 + 1),
                .p_key = (void *)"a",
                .value_size = sizeof(uint32_t),
                .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                .p_value = (void *)&(uint32_t){1}
            }
        }
    };
    FACILEDB_DATA_T data2 = {
        .data_num = 2,
        .p_data_records = (FACILEDB_RECORD_T[]){
            {
                // [0]
                .key_size = 2,
                .p_key = (void *)"b",
                .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                .value_size = 4,
                .p_value = (void *)&(uint32_t){2}
            },
            {
                // [1]
                .key_size = 2,
                .p_key = (void *)"c",
                .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                .value_size = 4,
                .p_value = (void *)&(uint32_t){3}
            }
        }
    };
    // clang-format on

    FacileDB_Api_Init(test_db_directory);
    FacileDB_Api_Insert_Element(db_set_name, &data1);
    // delay 2ms
    // sleep(2);
    FacileDB_Api_Insert_Element(db_set_name, &data2);
    FacileDB_Api_Close();

    // check
    FacileDB_Api_Init(test_db_directory);

    DB_SET_INFO_T *p_db_set_info = NULL;
    // check db_set_info
    p_db_set_info = load_db_set_info(db_set_name);
    assert((p_db_set_info != NULL) && (p_db_set_info->file != NULL));

    // check db_set_properties.
    // clang-format off
    DB_SET_PROPERTIES_T expect_db_set_properties = {
        // might be 3
        .block_num = 2,
        .valid_record_num = 2,
        .set_name_size = strlen(db_set_name),
        .p_set_name = db_set_name
    };
    // clang-format on
    check_faciledb_properties(&(p_db_set_info->db_set_properties), &expect_db_set_properties);

    // check the db block.
    DB_BLOCK_T db_block;
    // clang-format off
    DB_BLOCK_T expected_db_blocks[2] = {
        {
            // [0]
            .block_tag = 1,
            .data_tag = 1,
            .prev_block_tag = 0,
            .next_block_tag = 0,
            .deleted = 0,
            .valid_record_num = 1,
            .record_properties_num = 1
        },
        {
            // [1]
            .block_tag = 2,
            .data_tag = 2,
            .prev_block_tag = 0,
            .next_block_tag = 0,
            .deleted = 0,
            .valid_record_num = 2,
            .record_properties_num = 2
        }
    };
    // block_data
    // memcpy()
    DB_RECORD_INFO_T expected_db_records[3] = {
        {
            .db_record = {
                .p_key = data1.p_data_records->p_key,
                .p_value = data1.p_data_records->p_value
            },
            .db_record_properties = {
                .deleted = 0,
                .key_size = data1.p_data_records->key_size,
                .record_value_type = data1.p_data_records->record_value_type,
                .value_size = data1.p_data_records->value_size
            },
            .db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), 1) + (((uint64_t)&(expected_db_blocks[0].block_data)) - ((uint64_t)&(expected_db_blocks[0])))
        },
        {
            .db_record = (DB_RECORD_T){
                .p_key = data2.p_data_records[0].p_key,
                .p_value = data2.p_data_records[0].p_value
            },
            .db_record_properties = (DB_RECORD_PROPERTIES_T){
                .deleted = 0,
                .key_size = data2.p_data_records[0].key_size,
                .record_value_type = data2.p_data_records[0].record_value_type,
                .value_size = data2.p_data_records[0].value_size
            },
            .db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), 2) + (((uint64_t)&(expected_db_blocks[1].block_data)) - ((uint64_t)&(expected_db_blocks[1])))
        },
        {
            .db_record = {
                .p_key = data2.p_data_records[1].p_key,
                .p_value = data2.p_data_records[1].p_value
            },
            .db_record_properties = {
                .deleted = 0,
                .key_size = data2.p_data_records[1].key_size,
                .record_value_type = data2.p_data_records[1].record_value_type,
                .value_size = data2.p_data_records[1].value_size
            },
            .db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), 2) + (((uint64_t)&(expected_db_blocks[1].block_data)) - ((uint64_t)&(expected_db_blocks[1]))) + get_db_record_properties_size() + data2.p_data_records[0].key_size + data2.p_data_records[0].value_size
        }
    };
    // clang-format on

    // check db blocks
    for (uint32_t i = 0; i < 2; i++)
    {
        db_block_init(&db_block);
        read_db_block(p_db_set_info, i + 1, &db_block);
        check_faciledb_block(&db_block, &(expected_db_blocks[i]));

        // check the db records
        uint32_t record_num = 0;
        uint32_t expected_record_num = (i == 0) ? (1) : (2); // 1 record in block1 and 2 records in block2.
        DB_RECORD_INFO_T *p_db_records_info = extract_db_record_info_from_db_blocks(expected_db_blocks[i].block_tag, p_db_set_info, &record_num);

        check_faciledb_records(p_db_records_info, record_num, &(expected_db_records[i]), expected_record_num);

        for (uint32_t i = 0; i < record_num; i++)
        {
            free_db_record_info_resources(&(p_db_records_info[i]));
        }
    }

    FacileDB_Api_Close();

    test_end(case_name);
}

// one data each with two blocks.
void test_faciledb_insert_case3()
{
    char case_name[] = "test_faciledb_insert_case3";
    test_start(case_name);

    char db_set_name[] = "test_db_insert_case3";
    // clang-format off
    FACILEDB_DATA_T data = {
        .data_num = 1,
        .p_data_records = (FACILEDB_RECORD_T[]){
            {
                .key_size = 2, // 'a' + '\0'
                .p_key = (void *)"a",
                .value_size = (26 * 3 + 1), // strlen + '\0'
                .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                .p_value = (void *)"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
            }
        }
    };
    // clang-format on
    FacileDB_Api_Init(test_db_directory);
    FacileDB_Api_Insert_Element(db_set_name, &data);
    FacileDB_Api_Close();

    // check
    FacileDB_Api_Init(test_db_directory);

    DB_SET_INFO_T *p_db_set_info = NULL;
    // check db_set_info
    p_db_set_info = load_db_set_info(db_set_name);
    assert((p_db_set_info != NULL) && (p_db_set_info->file != NULL));

    // check db_set_properties.
    uint32_t expect_block_num = (get_db_record_properties_size() + data.p_data_records->key_size + data.p_data_records->value_size) / DB_BLOCK_DATA_SIZE;
    expect_block_num += (((get_db_record_properties_size() + data.p_data_records->key_size + data.p_data_records->value_size) % DB_BLOCK_DATA_SIZE) != 0) ? (1) : (0);
    // clang-format off
    DB_SET_PROPERTIES_T expect_db_set_properties = {
        .block_num = expect_block_num,
        .valid_record_num = 1,
        .set_name_size = strlen(db_set_name),
        .p_set_name = db_set_name
    };
    // clang-format on
    check_faciledb_properties(&(p_db_set_info->db_set_properties), &expect_db_set_properties);

    // check the db block.
    DB_BLOCK_T db_block;
    // clang-format off
    DB_BLOCK_T expected_db_block[2] = {
        {
            .block_tag = 1,
            .data_tag = 1,
            .prev_block_tag = 0,
            .next_block_tag = 2,
            .deleted = 0,
            .valid_record_num = 1,
            .record_properties_num = 1
        },
        {
            .block_tag = 2,
            .data_tag = 1,
            .prev_block_tag = 1,
            .next_block_tag = 0,
            .deleted = 0,
            .valid_record_num = 1,
            .record_properties_num = 0
        }
    };
    // clang-format on
    // block_data
    // memcpy()
    for (uint32_t i = 0; i < 2; i++)
    {
        db_block_init(&db_block);
        read_db_block(p_db_set_info, i + 1, &db_block);
        check_faciledb_block(&db_block, &(expected_db_block[i]));
    }

    // check the db records
    // clang-format off
    uint32_t record_num = 0;
    DB_RECORD_INFO_T *p_db_records_info = extract_db_record_info_from_db_blocks(1, p_db_set_info, &record_num);
    DB_RECORD_INFO_T expected_db_record = {
        .db_record = (DB_RECORD_T){
            .p_key = data.p_data_records->p_key,
            .p_value = data.p_data_records->p_value
        },
        .db_record_properties = (DB_RECORD_PROPERTIES_T){
            .deleted = 0,
            .key_size = data.p_data_records->key_size,
            .record_value_type = data.p_data_records->record_value_type,
            .value_size = data.p_data_records->value_size
        },
        .db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), 1) + (((uint64_t)&(expected_db_block[0].block_data)) - ((uint64_t)&(expected_db_block[0])))
    };
    // clang-format on
    check_faciledb_records(p_db_records_info, record_num, &expected_db_record, 1);

    for (uint32_t i = 0; i < record_num; i++)
    {
        free_db_record_info_resources(&(p_db_records_info[i]));
    }
    FacileDB_Api_Close();

    test_end(case_name);
}

// One data with one block, and one data with two blocks.
void test_faciledb_insert_case5()
{
    char case_name[] = "test_faciledb_insert_case5";
    test_start(case_name);

    char db_set_name[] = "test_db_insert_case5";
    // clang-format off
    FACILEDB_DATA_T data[] ={
        {
            .data_num = 1,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    .key_size = 2, // 'a' + '\0'
                    .p_key = (void *)"a",
                    .value_size = (26 * 3 + 1), // strlen + '\0'
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                    .p_value = (void *)"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
                }
            }
        },
        {
            .data_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = 4,
                    .p_value = (void *)&(uint32_t){2}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"c",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = 4,
                    .p_value = (void *)&(uint32_t){3}
                }
            }
        }
    };
    // clang-format on

    FacileDB_Api_Init(test_db_directory);
    FacileDB_Api_Insert_Element(db_set_name, &(data[0]));
    FacileDB_Api_Insert_Element(db_set_name, &(data[1]));
    FacileDB_Api_Close();

    // check
    FacileDB_Api_Init(test_db_directory);

    DB_SET_INFO_T *p_db_set_info = NULL;
    // check db_set_info
    p_db_set_info = load_db_set_info(db_set_name);
    assert((p_db_set_info != NULL) && (p_db_set_info->file != NULL));

    // check db_set_properties.
    // clang-format off
    DB_SET_PROPERTIES_T expect_db_set_properties = {
        .block_num = 3,
        .valid_record_num = 2,
        .set_name_size = strlen(db_set_name),
        .p_set_name = db_set_name
    };
    // clang-format on
    check_faciledb_properties(&(p_db_set_info->db_set_properties), &expect_db_set_properties);

    // check the db block.
    DB_BLOCK_T db_block;
    // clang-format off
    DB_BLOCK_T expected_db_blocks[3] = {
        {
            // [0]
            .block_tag = 1,
            .data_tag = 1,
            .prev_block_tag = 0,
            .next_block_tag = 2,
            .deleted = 0,
            .valid_record_num = 1,
            .record_properties_num = 1
        },
        {
            // [1]
            .block_tag = 2,
            .data_tag = 1,
            .prev_block_tag = 1,
            .next_block_tag = 0,
            .deleted = 0,
            .valid_record_num = 1,
            .record_properties_num = 0
        },
        {
            // [2]
            .block_tag = 3,
            .data_tag = 2,
            .prev_block_tag = 0,
            .next_block_tag = 0,
            .deleted = 0,
            .valid_record_num = 2,
            .record_properties_num = 2
        }
    };
    // block_data
    // memcpy()
    DB_RECORD_INFO_T expected_db_records[3] = {
        {
            .db_record = {
                .p_key = data[0].p_data_records->p_key,
                .p_value = data[0].p_data_records->p_value
            },
            .db_record_properties = {
                .deleted = 0,
                .key_size = data[0].p_data_records->key_size,
                .record_value_type = data[0].p_data_records->record_value_type,
                .value_size = data[0].p_data_records->value_size
            },
            .db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), 1) + (((uint64_t)&(expected_db_blocks[0].block_data)) - ((uint64_t)&(expected_db_blocks[0])))
        },
        {
            .db_record = (DB_RECORD_T){
                .p_key = data[1].p_data_records[0].p_key,
                .p_value = data[1].p_data_records[0].p_value
            },
            .db_record_properties = (DB_RECORD_PROPERTIES_T){
                .deleted = 0,
                .key_size = data[1].p_data_records[0].key_size,
                .record_value_type = data[1].p_data_records[0].record_value_type,
                .value_size = data[1].p_data_records[0].value_size
            },
            .db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), 3) + (((uint64_t)&(expected_db_blocks[3].block_data)) - ((uint64_t)&(expected_db_blocks[3])))
        },
        {
            .db_record = {
                .p_key = data[1].p_data_records[1].p_key,
                .p_value = data[1].p_data_records[1].p_value
            },
            .db_record_properties = {
                .deleted = 0,
                .key_size = data[1].p_data_records[1].key_size,
                .record_value_type = data[1].p_data_records[1].record_value_type,
                .value_size = data[1].p_data_records[1].value_size
            },
            .db_record_properties_offset = get_db_block_offset(&(p_db_set_info->db_set_properties), 3) + (((uint64_t)&(expected_db_blocks[3].block_data)) - ((uint64_t)&(expected_db_blocks[3]))) + get_db_record_properties_size() + data[1].p_data_records[0].key_size + data[1].p_data_records[0].value_size
        }
    };
    // clang-format on

    // check db blocks
    for (uint32_t i = 0; i < 3; i++)
    {
        db_block_init(&db_block);
        read_db_block(p_db_set_info, i + 1, &db_block);
        check_faciledb_block(&db_block, &(expected_db_blocks[i]));
    }

    // check the db records
    {
        // Record [0]
        uint32_t record_num;
        uint32_t expected_record_num;
        DB_RECORD_INFO_T *p_db_records_info = NULL;

        // Record [0]
        record_num = 0;
        expected_record_num = 1; // 1 record in block_tag: 1.
        p_db_records_info = extract_db_record_info_from_db_blocks(expected_db_blocks[0].block_tag, p_db_set_info, &record_num);

        check_faciledb_records(p_db_records_info, record_num, &(expected_db_records[0]), expected_record_num);

        for (uint32_t i = 0; i < record_num; i++)
        {
            free_db_record_info_resources(&(p_db_records_info[i]));
        }

        // Record [1] and [2]
        record_num = 0;
        expected_record_num = 2; // 2 records in block_tag: 3.
        p_db_records_info = extract_db_record_info_from_db_blocks(expected_db_blocks[2].block_tag, p_db_set_info, &record_num);

        check_faciledb_records(p_db_records_info, record_num, &(expected_db_records[1]), expected_record_num);

        for (uint32_t i = 0; i < record_num; i++)
        {
            free_db_record_info_resources(&(p_db_records_info[i]));
        }
    }

    FacileDB_Api_Close();

    test_end(case_name);
}

void test_faciledb_search_case1()
{
    char case_name[] = "test_faciledb_search_case1";
    test_start(case_name);

    char db_set_name[] = "test_db_search_case1";
    // clang-format off
    FACILEDB_DATA_T data = {
        .data_num = 1,
        .p_data_records = (FACILEDB_RECORD_T[]){
            {
                .key_size = 2, // 'a' and '\0'
                .p_key = (void *)"a",
                .value_size = sizeof(uint32_t),
                .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                .p_value = (void *)&(uint32_t){1}
            }
        }
    };
    FACILEDB_RECORD_T search_record[3] = {
        // match: 1
        {
            .key_size = 2,
            .p_key = (void *)"a",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){1}
        },
        // match: 0
        {
            .key_size = 2,
            .p_key = (void *)"a",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){2}
        },
        // match: 0
        {
            .key_size = 2,
            .p_key = (void *)"b",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){1}
        }
    };
    // clang-format on
    uint32_t data_num[3] = {0};
    FACILEDB_DATA_T *p_faciledb_data_array[3];

    FacileDB_Api_Init(test_db_directory);
    // insert 1 data
    FacileDB_Api_Insert_Element(db_set_name, &data);
    // search
    p_faciledb_data_array[0] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[0]), &(data_num[0]));
    p_faciledb_data_array[1] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[1]), &(data_num[1]));
    p_faciledb_data_array[2] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[2]), &(data_num[2]));

    FacileDB_Api_Close();

    // Check
    // clang-format off
    FACILEDB_DATA_T expected_data_result[3] = {
        {
            .data_num = 1,
            .p_data_records = (FACILEDB_RECORD_T *)&(
                (FACILEDB_RECORD_T){
                    .key_size = 2,
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){1}
                }
            )
        },
        {
            .data_num = 0,
            .p_data_records = NULL
        },
        {
            .data_num = 0,
            .p_data_records = NULL
        }
    };
    // clang-format on
    uint32_t expected_data_result_length[3] = {1, 0, 0};

    for(uint32_t i = 0; i < 3; i++)
    {
        check_faciledb_search_result(p_faciledb_data_array[i], data_num[i], &(expected_data_result[i]), expected_data_result_length[i]);
    }

    // End
    for (uint32_t i = 0; i < 3; i++)
    {
        for (uint32_t j = 0; j < data_num[i]; j++)
        {
            FacileDB_Api_Free_Data_Buffer(&(p_faciledb_data_array[i][j]));
        }
        free(p_faciledb_data_array[i]);
    }

    test_end(case_name);
}

int main()
{
    test_faciledb_init();
    test_faciledb_close();
    test_faciledb_insert_case1();
    test_faciledb_insert_case2();
    test_faciledb_insert_case3();
    test_faciledb_insert_case4();
    test_faciledb_insert_case5();

    test_faciledb_search_case1();
}
