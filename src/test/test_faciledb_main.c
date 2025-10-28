#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>

#define __FACILEDB_TEST__
// #define __PRINT_DETAILS__

#define ENABLE_DB_INDEX (1)
#define DB_SET_INFO_INSTANCE_NUM (1)
// #define FACILEDB_BLOCK_DATA_SIZE ((16 + 6 + 6) * 2 - 4) // 52
#define FACILEDB_BLOCK_DATA_SIZE (50) // 49 ~
// Definition for buffer length in search operation.
#define DB_SEARCH_DATA_INFO_BUFFER_LEN (1)

#include "faciledb.c"

char test_faciledb_directory[] = "./bin/test_db_files/";

void test_start(char *case_name)
{
    printf("Test Case: %s START!\n", case_name);
}

void test_end(char *case_name)
{
    printf("Test Case: %s END!\n\n", case_name);
}

void get_test_faciledb_file_path(char *p_faciledb_file_path, char *p_db_set_name)
{
    strcpy(p_faciledb_file_path, test_faciledb_directory);
    strcat(p_faciledb_file_path, p_db_set_name);
    strcat(p_faciledb_file_path, ".faciledb");
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

    // assert(memcmp(p_db_block_1->block_data, p_db_block_2->block_data, FACILEDB_BLOCK_DATA_SIZE) == 0);

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

    for (uint32_t i = 0; i < data_array_1_length; i++)
    {
        assert(p_faciledb_data_array_1[i].record_num == p_faciledb_data_array_2[i].record_num);

        for (uint32_t j = 0; j < p_faciledb_data_array_1[i].record_num; j++)
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

void test_faciledb_init_and_close()
{
    char case_name[] = "test_faciledb_init";
    test_start(case_name);

    FacileDB_Api_Init(test_faciledb_directory);
    FacileDB_Api_Close();

    test_end(case_name);
}

// one data each with one block.
void test_faciledb_insert_case1()
{
    char case_name[] = "test_faciledb_insert_case1";
    char db_set_name[] = "test_db_insert_case1";

    // clang-format off
    FACILEDB_DATA_T data = {
        .record_num = 1,
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

    test_start(case_name);

    FacileDB_Api_Init(test_faciledb_directory);
    FacileDB_Api_Insert_Data(db_set_name, &data);
    FacileDB_Api_Close();

    {
        // check
        DB_SET_INFO_T db_set_info;
        char db_set_file_path[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};

        db_set_info_init(&db_set_info);
        get_test_faciledb_file_path(db_set_file_path, db_set_name);
        db_set_info.file = fopen(db_set_file_path, "rb");

        // check db_set_info
        assert(db_set_info.file != NULL);

        // check db_set_properties.
        read_db_set_properties(&db_set_info);
        // clang-format off
        DB_SET_PROPERTIES_T expect_db_set_properties = {
            .block_num = 1,
            .valid_record_num = 1,
            .set_name_size = strlen(db_set_name),
            .p_set_name = db_set_name
        };
        // clang-format on
        check_faciledb_properties(&(db_set_info.db_set_properties), &expect_db_set_properties);

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
        // block_data and memcpy()
        db_block_init(&db_block);
        read_db_block(&db_set_info, 1, &db_block);
        check_faciledb_block(&db_block, &expected_db_block);

        // check the db records
        // clang-format off
        uint32_t record_num = 0;
        DB_RECORD_INFO_T *p_db_records_info = extract_db_record_info_from_db_blocks(1, &db_set_info, &record_num);
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
            .db_record_properties_offset = get_db_block_offset(&(db_set_info.db_set_properties), 1) + (((uint64_t)&expected_db_block.block_data) - ((uint64_t)&expected_db_block))
        };
        // clang-format on
        check_faciledb_records(p_db_records_info, record_num, &expected_db_record, 1);
        for (uint32_t i = 0; i < record_num; i++)
        {
            free_db_record_info_resources(&(p_db_records_info[i]));
        }
        free(p_db_records_info);

        fclose(db_set_info.file);
        free_db_set_info_resources(&db_set_info);
    }

    test_end(case_name);
}

// One data each with one block that record value type is string.
void test_faciledb_insert_case2()
{
    char case_name[] = "test_faciledb_insert_case2";
    char db_set_name[] = "test_db_insert_case2";

    test_start(case_name);

    // clang-format off
    FACILEDB_DATA_T data = {
        .record_num = 1,
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

    FacileDB_Api_Init(test_faciledb_directory);
    FacileDB_Api_Insert_Data(db_set_name, &data);
    FacileDB_Api_Close();

    // check
    {
        DB_SET_INFO_T db_set_info;
        char db_set_file_path[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};

        db_set_info_init(&db_set_info);
        get_test_faciledb_file_path(db_set_file_path, db_set_name);
        db_set_info.file = fopen(db_set_file_path, "rb");

        // check db_set_info
        assert(db_set_info.file != NULL);

        // check db_set_properties.
        read_db_set_properties(&db_set_info);
        // clang-format off
        DB_SET_PROPERTIES_T expect_db_set_properties = {
            .block_num = 1,
            .valid_record_num = 1,
            .set_name_size = strlen(db_set_name),
            .p_set_name = db_set_name
        };
        // clang-format on
        check_faciledb_properties(&(db_set_info.db_set_properties), &expect_db_set_properties);

        // check the db blocks.
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
        read_db_block(&db_set_info, 1, &db_block);
        check_faciledb_block(&db_block, &expected_db_block);

        // check the db records
        // clang-format off
        uint32_t record_num = 0;
        DB_RECORD_INFO_T *p_db_records_info = extract_db_record_info_from_db_blocks(1, &db_set_info, &record_num);
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
            .db_record_properties_offset = get_db_block_offset(&(db_set_info.db_set_properties), 1) + (((uint64_t)&expected_db_block.block_data) - ((uint64_t)&expected_db_block))
        };
        // clang-format on
        check_faciledb_records(p_db_records_info, record_num, &expected_db_record, 1);
        // free resources
        for (uint32_t i = 0; i < record_num; i++)
        {
            free_db_record_info_resources(&(p_db_records_info[i]));
        }
        free(p_db_records_info);

        fclose(db_set_info.file);
        free_db_set_info_resources(&db_set_info);
    }

    test_end(case_name);
}

// One data each with one block.
void test_faciledb_insert_case4()
{
    char case_name[] = "test_faciledb_insert_case4";
    char db_set_name[] = "test_db_insert_case4";

    test_start(case_name);

    // clang-format off
    FACILEDB_DATA_T data1 = {
        .record_num = 1,
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
        .record_num = 2,
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

    FacileDB_Api_Init(test_faciledb_directory);
    FacileDB_Api_Insert_Data(db_set_name, &data1);
    // delay 2ms
    // sleep(2);
    FacileDB_Api_Insert_Data(db_set_name, &data2);
    FacileDB_Api_Close();

    // check
    {
        DB_SET_INFO_T db_set_info;
        char db_set_file_path[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};

        db_set_info_init(&db_set_info);
        get_test_faciledb_file_path(db_set_file_path, db_set_name);
        db_set_info.file = fopen(db_set_file_path, "rb");

        // check db_set_info
        assert(db_set_info.file != NULL);

        // check db_set_properties.
        read_db_set_properties(&db_set_info);
        // clang-format off
        DB_SET_PROPERTIES_T expect_db_set_properties = {
            // might be 3
            .block_num = 2,
            .valid_record_num = 2,
            .set_name_size = strlen(db_set_name),
            .p_set_name = db_set_name
        };
        // clang-format on
        check_faciledb_properties(&(db_set_info.db_set_properties), &expect_db_set_properties);

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
                .db_record_properties_offset = get_db_block_offset(&(db_set_info.db_set_properties), 1) + (((uint64_t)&(expected_db_blocks[0].block_data)) - ((uint64_t)&(expected_db_blocks[0])))
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
                .db_record_properties_offset = get_db_block_offset(&(db_set_info.db_set_properties), 2) + (((uint64_t)&(expected_db_blocks[1].block_data)) - ((uint64_t)&(expected_db_blocks[1])))
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
                .db_record_properties_offset = get_db_block_offset(&(db_set_info.db_set_properties), 2) + (((uint64_t)&(expected_db_blocks[1].block_data)) - ((uint64_t)&(expected_db_blocks[1]))) + get_db_record_properties_size() + data2.p_data_records[0].key_size + data2.p_data_records[0].value_size
            }
        };
        // clang-format on

        // check db blocks and records
        for (uint32_t i = 0; i < 2; i++)
        {
            db_block_init(&db_block);
            read_db_block(&db_set_info, i + 1, &db_block);
            check_faciledb_block(&db_block, &(expected_db_blocks[i]));

            // check the db records
            uint32_t record_num = 0;
            uint32_t expected_record_num = (i == 0) ? (1) : (2); // 1 record in block1 and 2 records in block2.
            DB_RECORD_INFO_T *p_db_records_info = extract_db_record_info_from_db_blocks(expected_db_blocks[i].block_tag, &db_set_info, &record_num);

            check_faciledb_records(p_db_records_info, record_num, &(expected_db_records[i]), expected_record_num);

            for (uint32_t i = 0; i < record_num; i++)
            {
                free_db_record_info_resources(&(p_db_records_info[i]));
            }
            free(p_db_records_info);
        }

        fclose(db_set_info.file);
        free_db_set_info_resources(&db_set_info);
    }

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
        .record_num = 1,
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
    FacileDB_Api_Init(test_faciledb_directory);
    FacileDB_Api_Insert_Data(db_set_name, &data);
    FacileDB_Api_Close();

    // check
    {
        char faciledb_set_file_path[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};
        DB_SET_INFO_T db_set_info;

        get_test_faciledb_file_path(faciledb_set_file_path, db_set_name);
        db_set_info_init(&db_set_info);
        db_set_info.file = fopen(faciledb_set_file_path, "rb");

        // check db_set_info
        assert(db_set_info.file != NULL);

        // check db_set_properties.
        read_db_set_properties(&db_set_info);
        uint32_t expect_block_num = (get_db_record_properties_size() + data.p_data_records->key_size + data.p_data_records->value_size) / FACILEDB_BLOCK_DATA_SIZE;
        expect_block_num += (((get_db_record_properties_size() + data.p_data_records->key_size + data.p_data_records->value_size) % FACILEDB_BLOCK_DATA_SIZE) != 0) ? (1) : (0);
        // clang-format off
        DB_SET_PROPERTIES_T expect_db_set_properties = {
            .block_num = expect_block_num,
            .valid_record_num = 1,
            .set_name_size = strlen(db_set_name),
            .p_set_name = db_set_name
        };
        // clang-format on
        check_faciledb_properties(&(db_set_info.db_set_properties), &expect_db_set_properties);

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
            read_db_block(&db_set_info, i + 1, &db_block);
            check_faciledb_block(&db_block, &(expected_db_block[i]));
        }

        // check the db records
        uint32_t record_num = 0;
        DB_RECORD_INFO_T *p_db_records_info = extract_db_record_info_from_db_blocks(1, &db_set_info, &record_num);
        // clang-format off
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
            .db_record_properties_offset = get_db_block_offset(&(db_set_info.db_set_properties), 1) + (((uint64_t)&(expected_db_block[0].block_data)) - ((uint64_t)&(expected_db_block[0])))
        };
        // clang-format on
        check_faciledb_records(p_db_records_info, record_num, &expected_db_record, 1);
        // free resources
        for (uint32_t i = 0; i < record_num; i++)
        {
            free_db_record_info_resources(&(p_db_records_info[i]));
        }
        free(p_db_records_info);

        fclose(db_set_info.file);
        free_db_set_info_resources(&db_set_info);
    }

    test_end(case_name);
}

// One data with one block, and one data with two blocks.
void test_faciledb_insert_case5()
{
    char case_name[] = "test_faciledb_insert_case5";
    char db_set_name[] = "test_db_insert_case5";

    test_start(case_name);
    // clang-format off
    FACILEDB_DATA_T data[] ={
        {
            .record_num = 1,
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
            .record_num = 2,
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

    FacileDB_Api_Init(test_faciledb_directory);
    FacileDB_Api_Insert_Data(db_set_name, &(data[0]));
    FacileDB_Api_Insert_Data(db_set_name, &(data[1]));
    FacileDB_Api_Close();

    // check
    {
        char faciledb_set_file_path[FACILEDB_FILE_PATH_BUFFER_LENGTH] = {0};
        DB_SET_INFO_T db_set_info;

        get_test_faciledb_file_path(faciledb_set_file_path, db_set_name);
        db_set_info_init(&db_set_info);
        db_set_info.file = fopen(faciledb_set_file_path, "rb");

        // check db_set_info
        assert(db_set_info.file != NULL);

        // check db_set_properties.
        read_db_set_properties(&db_set_info);
        // clang-format off
        DB_SET_PROPERTIES_T expect_db_set_properties = {
            .block_num = 3,
            .valid_record_num = 2,
            .set_name_size = strlen(db_set_name),
            .p_set_name = db_set_name
        };
        // clang-format on
        check_faciledb_properties(&(db_set_info.db_set_properties), &expect_db_set_properties);

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
                .db_record_properties_offset = get_db_block_offset(&(db_set_info.db_set_properties), 1) + (((uint64_t)&(expected_db_blocks[0].block_data)) - ((uint64_t)&(expected_db_blocks[0])))
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
                .db_record_properties_offset = get_db_block_offset(&(db_set_info.db_set_properties), 3) + (((uint64_t)&(expected_db_blocks[3].block_data)) - ((uint64_t)&(expected_db_blocks[3])))
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
                .db_record_properties_offset = get_db_block_offset(&(db_set_info.db_set_properties), 3) + (((uint64_t)&(expected_db_blocks[3].block_data)) - ((uint64_t)&(expected_db_blocks[3]))) + get_db_record_properties_size() + data[1].p_data_records[0].key_size + data[1].p_data_records[0].value_size
            }
        };
        // clang-format on

        // check db blocks
        for (uint32_t i = 0; i < 3; i++)
        {
            db_block_init(&db_block);
            read_db_block(&db_set_info, i + 1, &db_block);
            check_faciledb_block(&db_block, &(expected_db_blocks[i]));
        }

        // check the db records

        // Record [0]
        uint32_t record_num;
        uint32_t expected_record_num;
        DB_RECORD_INFO_T *p_db_records_info = NULL;

        // Record [0]
        record_num = 0;
        expected_record_num = 1; // 1 record in block_tag: 1.
        p_db_records_info = extract_db_record_info_from_db_blocks(expected_db_blocks[0].block_tag, &db_set_info, &record_num);

        check_faciledb_records(p_db_records_info, record_num, &(expected_db_records[0]), expected_record_num);

        for (uint32_t i = 0; i < record_num; i++)
        {
            free_db_record_info_resources(&(p_db_records_info[i]));
        }
        free(p_db_records_info);

        // Record [1] and [2]
        record_num = 0;
        expected_record_num = 2; // 2 records in block_tag: 3.
        p_db_records_info = extract_db_record_info_from_db_blocks(expected_db_blocks[2].block_tag, &db_set_info, &record_num);

        check_faciledb_records(p_db_records_info, record_num, &(expected_db_records[1]), expected_record_num);

        for (uint32_t i = 0; i < record_num; i++)
        {
            free_db_record_info_resources(&(p_db_records_info[i]));
        }
        free(p_db_records_info);

        fclose(db_set_info.file);
        free_db_set_info_resources(&db_set_info);
    }
    test_end(case_name);
}

void test_faciledb_search_case1()
{
    char case_name[] = "test_faciledb_search_case1";
    test_start(case_name);

    char db_set_name[] = "test_db_search_case1";
    // clang-format off
    FACILEDB_DATA_T data = {
        .record_num = 1,
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

    FacileDB_Api_Init(test_faciledb_directory);
    // insert 1 data
    FacileDB_Api_Insert_Data(db_set_name, &data);
    // search
    p_faciledb_data_array[0] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[0]), &(data_num[0]));
    p_faciledb_data_array[1] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[1]), &(data_num[1]));
    p_faciledb_data_array[2] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[2]), &(data_num[2]));

    FacileDB_Api_Close();

    // Check
    // clang-format off
    FACILEDB_DATA_T expected_data_result[3] = {
        {
            .record_num = 1,
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
            .record_num = 0,
            .p_data_records = NULL
        },
        {
            .record_num = 0,
            .p_data_records = NULL
        }
    };
    // clang-format on
    uint32_t expected_data_result_length[3] = {1, 0, 0};

    for (uint32_t i = 0; i < 3; i++)
    {
        check_faciledb_search_result(p_faciledb_data_array[i], data_num[i], &(expected_data_result[i]), expected_data_result_length[i]);
    }

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

void test_faciledb_search_case2()
{
    char case_name[] = "test_faciledb_search_case2";
    test_start(case_name);

    char db_set_name[] = "test_db_search_case2";
    // clang-format off
    FACILEDB_DATA_T data[4] = 
    {
        {
            .record_num = 1,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){1}
                }
            }
        },
        {
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2,
                    .p_key = (void *)"a",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){2}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){3}
                }
            }  
        },
        {
            .record_num = 3,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2,
                    .p_key = (void *)"a",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){1}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){2}
                },
                {
                    // [2]
                    .key_size = 2,
                    .p_key = (void *)"c",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){3}
                }
            }
        },
        {
            .record_num = 1,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){1}
                }
            }
        }
    };
    FACILEDB_RECORD_T search_record[1] = {
        // match: 3
        {
            .key_size = 2,
            .p_key = (void *)"a",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){1}
        }
    };
    // clang-format on
    uint32_t data_num[1] = {0};
    FACILEDB_DATA_T *p_faciledb_data_array[1];

    FacileDB_Api_Init(test_faciledb_directory);
    // insert 4 data
    for (uint32_t i = 0; i < 4; i++)
    {
        FacileDB_Api_Insert_Data(db_set_name, &(data[i]));
    }
    // search
    p_faciledb_data_array[0] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[0]), &(data_num[0]));
    FacileDB_Api_Close();

    // Check
    // clang-format off
    FACILEDB_DATA_T expected_data_result[1][3] = {
        {
            {
                .record_num = 1,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){1}
                    }
                }
            },
            {
                .record_num = 3,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        // [0]
                        .key_size = 2,
                        .p_key = (void *)"a",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        // [1]
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){2}
                    },
                    {
                        // [2]
                        .key_size = 2,
                        .p_key = (void *)"c",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){3}
                    }
                }
            },
            {
                .record_num = 1,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){1}
                    }
                }
            }
        }
    };
    // clang-format on
    uint32_t expected_data_result_length[1] = {3};

    for (uint32_t i = 0; i < 1; i++)
    {
        check_faciledb_search_result(p_faciledb_data_array[i], data_num[i], expected_data_result[i], expected_data_result_length[i]);
    }

    for (uint32_t i = 0; i < 1; i++)
    {
        for (uint32_t j = 0; j < data_num[i]; j++)
        {
            FacileDB_Api_Free_Data_Buffer(&(p_faciledb_data_array[i][j]));
        }
        free(p_faciledb_data_array[i]);
    }

    test_end(case_name);
}

void test_faciledb_search_case3()
{
    char case_name[] = "test_faciledb_search_case3";
    test_start(case_name);

    char db_set_name[] = "test_db_search_case3";
    // clang-format off
    FACILEDB_DATA_T data[4] = 
    {
        {
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){1}
                },
                {
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"b",
                    .value_size = strlen("This is a test string.") + 1,
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                    .p_value = (void*) "This is a test string."

                }
            }
        },
        {
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2,
                    .p_key = (void *)"a",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){1}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                    .value_size = strlen("This is a test string.") + 1,
                    .p_value = (void *) "This is a test string."
                }
            }  
        },
        {
            .record_num = 3,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2,
                    .p_key = (void *)"a",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){1}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                    .value_size = strlen("This is not a test string.") + 1,
                    .p_value = (void *) "This is not a test string."
                },
                {
                    // [2]
                    .key_size = 2,
                    .p_key = (void *)"c",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){3}
                }
            }
        },
        {
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){1}
                },
                {
                    .key_size = 2,
                    .p_key = (void *)"c",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                    .value_size = strlen("Another test string.") + 1,
                    .p_value = (void *) "Another test string."
                }
            }
        }
    };
    FACILEDB_RECORD_T search_record[4] = {
        {
            // match: 4(all)
            .key_size = 2,
            .p_key = (void *)"a",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){1}
        },
        {
            // match: 2
            .key_size = 2,
            .p_key = (void *)"b",
            .value_size = strlen("This is a test string.") + 1,
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
            .p_value = (void *) "This is a test string."
        },
        {
            // match: 1
            .key_size = 2,
            .p_key = (void *)"c",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){3}
        },
        {
            // match: 1
            .key_size = 2,
            .p_key = (void *)"c",
            .value_size = strlen("Another test string.") + 1,
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
            .p_value = (void *) "Another test string."
        }
    };
    // clang-format on
    uint32_t data_num[4] = {0};
    FACILEDB_DATA_T *p_faciledb_data_array[4];

    FacileDB_Api_Init(test_faciledb_directory);
    // insert 4 data
    for (uint32_t i = 0; i < 4; i++)
    {
        FacileDB_Api_Insert_Data(db_set_name, &(data[i]));
    }
    // search
    for (uint32_t i = 0; i < 4; i++)
    {
        p_faciledb_data_array[i] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[i]), &(data_num[i]));
    }
    FacileDB_Api_Close();

    // Check
    // clang-format off
    FACILEDB_DATA_T expected_data_result[4][4] = {
        {
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"b",
                        .value_size = strlen("This is a test string.") + 1,
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .p_value = (void*) "This is a test string."
    
                    }
                }
            },
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        .key_size = 2,
                        .p_key = (void *)"a",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .value_size = strlen("This is a test string.") + 1,
                        .p_value = (void *) "This is a test string."
                    }
                }  
            },
            {
                .record_num = 3,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        .key_size = 2,
                        .p_key = (void *)"a",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .value_size = strlen("This is not a test string.") + 1,
                        .p_value = (void *) "This is not a test string."
                    },
                    {
                        .key_size = 2,
                        .p_key = (void *)"c",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){3}
                    }
                }
            },
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        .key_size = 2,
                        .p_key = (void *)"c",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .value_size = strlen("Another test string.") + 1,
                        .p_value = (void *) "Another test string."
                    }
                }
            }
        },
        {
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"b",
                        .value_size = strlen("This is a test string.") + 1,
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .p_value = (void*) "This is a test string."
    
                    }
                }
            },
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        // [0]
                        .key_size = 2,
                        .p_key = (void *)"a",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        // [1]
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .value_size = strlen("This is a test string.") + 1,
                        .p_value = (void *) "This is a test string."
                    }
                }  
            }
        },
        {
            {
                .record_num = 3,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        .key_size = 2,
                        .p_key = (void *)"a",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .value_size = strlen("This is not a test string.") + 1,
                        .p_value = (void *) "This is not a test string."
                    },
                    {
                        .key_size = 2,
                        .p_key = (void *)"c",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){3}
                    }
                }
            }
        },
        {
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        .key_size = 2,
                        .p_key = (void *)"c",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .value_size = strlen("Another test string.") + 1,
                        .p_value = (void *) "Another test string."
                    }
                }
            }
        }
    };
    // clang-format on
    uint32_t expected_data_result_length[4] = {4, 2, 1, 1};

    for (uint32_t i = 0; i < 4; i++)
    {
        check_faciledb_search_result(p_faciledb_data_array[i], data_num[i], expected_data_result[i], expected_data_result_length[i]);
    }

    for (uint32_t i = 0; i < 1; i++)
    {
        for (uint32_t j = 0; j < data_num[i]; j++)
        {
            FacileDB_Api_Free_Data_Buffer(&(p_faciledb_data_array[i][j]));
        }
        free(p_faciledb_data_array[i]);
    }

    test_end(case_name);
}

void test_faciledb_delete_case1()
{
    char case_name[] = "test_faciledb_delete_case1";
    test_start(case_name);

    char db_set_name[] = "test_db_delete_case1";
    // clang-format off
    FACILEDB_DATA_T data = {
        .record_num = 1,
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
    FACILEDB_RECORD_T search_record[1] = {
        // match: 1
        {
            .key_size = 2,
            .p_key = (void *)"a",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){1}
        }
    };
    // clang-format on
    uint32_t delete_data_num[1] = {0};
    uint32_t data_num[1] = {0};
    FACILEDB_DATA_T *p_faciledb_data_array[1];

    FacileDB_Api_Init(test_faciledb_directory);
    // insert 1 data
    FacileDB_Api_Insert_Data(db_set_name, &data);
    // delete
    delete_data_num[0] = FacileDB_Api_Delete_Equal(db_set_name, &(search_record[0]));
    // search
    p_faciledb_data_array[0] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[0]), &(data_num[0]));
    FacileDB_Api_Close();

    // check
    assert(delete_data_num[0] == 1);
    assert(p_faciledb_data_array[0] == NULL);
    assert(data_num[0] == 0);

    free(p_faciledb_data_array[0]);

    test_end(case_name);
}

void test_faciledb_delete_case2()
{
    char case_name[] = "test_faciledb_delete_case2";
    test_start(case_name);

    char db_set_name[] = "test_db_delete_case2";
    // clang-format off
    FACILEDB_DATA_T data[4] = 
    {
        {
            .record_num = 1,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){1}
                }
            }
        },
        {
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2,
                    .p_key = (void *)"a",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){2}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){3}
                }
            }  
        },
        {
            .record_num = 3,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2,
                    .p_key = (void *)"a",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){1}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){2}
                },
                {
                    // [2]
                    .key_size = 2,
                    .p_key = (void *)"c",
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .value_size = sizeof(uint32_t),
                    .p_value = (void *)&(uint32_t){3}
                }
            }
        },
        {
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){1}
                },
                {
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){3}
                }
            }
        }
    };
    FACILEDB_RECORD_T search_record[1] = {
        // before: 3
        // after: 2
        {
            .key_size = 2,
            .p_key = (void *)"a",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){1}
        }
    };
    FACILEDB_RECORD_T delete_record[1] = {
        // match: 2
        {
            .key_size = 2,
            .p_key = (void *)"b",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){3}
        }
    };
    // clang-format on
    uint32_t data_num[1] = {0};
    uint32_t delete_num[1] = {0};
    FACILEDB_DATA_T *p_faciledb_data_array[1];

    FacileDB_Api_Init(test_faciledb_directory);
    // insert 4 data
    for (uint32_t i = 0; i < 4; i++)
    {
        FacileDB_Api_Insert_Data(db_set_name, &(data[i]));
    }
    // delete
    delete_num[0] = FacileDB_Api_Delete_Equal(db_set_name, &(delete_record[0]));
    // search
    p_faciledb_data_array[0] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[0]), &(data_num[0]));
    FacileDB_Api_Close();

    // Check
    assert(delete_num[0] == 2);

    // clang-format off
    FACILEDB_DATA_T expected_data_result[1][2] = {
        {
            {
                .record_num = 1,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){1}
                    }
                }
            },
            {
                .record_num = 3,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        // [0]
                        .key_size = 2,
                        .p_key = (void *)"a",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        // [1]
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){2}
                    },
                    {
                        // [2]
                        .key_size = 2,
                        .p_key = (void *)"c",
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .value_size = sizeof(uint32_t),
                        .p_value = (void *)&(uint32_t){3}
                    }
                }
            }
        }
    };
    // clang-format on
    uint32_t expected_data_result_length[1] = {2};

    for (uint32_t i = 0; i < 1; i++)
    {
        check_faciledb_search_result(p_faciledb_data_array[i], data_num[i], expected_data_result[i], expected_data_result_length[i]);
    }

    for (uint32_t i = 0; i < 1; i++)
    {
        for (uint32_t j = 0; j < data_num[i]; j++)
        {
            FacileDB_Api_Free_Data_Buffer(&(p_faciledb_data_array[i][j]));
        }
        free(p_faciledb_data_array[i]);
    }

    test_end(case_name);
}

#if ENABLE_DB_INDEX
void test_faciledb_make_index_and_search_case1()
{
    char case_name[] = "test_faciledb_make_index_and_search_case1";
    test_start(case_name);

    char db_set_name[] = "test_faciledb_make_index_and_search_case1";

    FACILEDB_DATA_T data[1] =
        {
            {// [0]
             .record_num = 2,
             .p_data_records = (FACILEDB_RECORD_T[]){
                 {               // [0]
                  .key_size = 2, // 'a' and '\0'
                  .p_key = (void *)"a",
                  .value_size = sizeof(uint32_t),
                  .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                  .p_value = (void *)&(uint32_t){1}},
                 {// [1]
                  .key_size = 2,
                  .p_key = (void *)"b",
                  .value_size = 3,
                  .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                  .p_value = (void *)"bb"}}}};
    FACILEDB_RECORD_T search_record[1] = {
        {// match: 1
         .key_size = 2,
         .p_key = (void *)"a",
         .value_size = sizeof(uint32_t),
         .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
         .p_value = (void *)&(uint32_t){1}}};
    // clang-format on
    uint32_t data_num[4] = {0};
    FACILEDB_DATA_T *p_faciledb_data_array[4];

    FacileDB_Api_Init(test_faciledb_directory);
    for (uint32_t i = 0; i < 1; i++)
    {
        FacileDB_Api_Insert_Data(db_set_name, &(data[i]));
    }

    // make index
    FacileDB_Api_Make_Record_Index(db_set_name, &(data[0].p_data_records[0])); // a

    // search
    for (uint32_t i = 0; i < 1; i++)
    {
        p_faciledb_data_array[i] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[i]), &(data_num[i]));
    }
    FacileDB_Api_Close();

    // Check
    // clang-format off
    FACILEDB_DATA_T expected_data_result[1][1] = {
        {
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        // [0]
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        // [1]
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .value_size = 3,
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .p_value = (void *)"bb"
                    }
                }
            }
        }
    };
    // clang-format on
    uint32_t expected_data_result_length[1] = {1};

    for (uint32_t i = 0; i < 1; i++)
    {
        check_faciledb_search_result(p_faciledb_data_array[i], data_num[i], expected_data_result[i], expected_data_result_length[i]);
    }

    for (uint32_t i = 0; i < 1; i++)
    {
        for (uint32_t j = 0; j < data_num[i]; j++)
        {
            FacileDB_Api_Free_Data_Buffer(&(p_faciledb_data_array[i][j]));
        }
        free(p_faciledb_data_array[i]);
    }

    test_end(case_name);
}

void test_faciledb_make_index_and_search_case2()
{
    char case_name[] = "test_faciledb_make_index_and_search_case2";
    test_start(case_name);

    char db_set_name[] = "test_faciledb_make_index_and_search_case2";

    // clang-format off
    FACILEDB_DATA_T data[2] = {
        {
            // [0]
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){1}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .value_size = 3,
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                    .p_value = (void *)"bb"
                }
            }
        },
        {
            // [1]
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){2}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .value_size = 3,
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                    .p_value = (void *)"bb"
                }
            }
        }
    };
    FACILEDB_RECORD_T search_record[1] = {
        {
            // match: 1
            .key_size = 2,
            .p_key = (void *)"a",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){2}
        }
    };
    // clang-format on
    uint32_t data_num[1] = {0};
    FACILEDB_DATA_T *p_faciledb_data_array[1];

    FacileDB_Api_Init(test_faciledb_directory);
    for (uint32_t i = 0; i < 2; i++)
    {
        FacileDB_Api_Insert_Data(db_set_name, &(data[i]));
    }

    // make index
    FacileDB_Api_Make_Record_Index(db_set_name, &(data[0].p_data_records[0])); // a

    // search
    for (uint32_t i = 0; i < 1; i++)
    {
        p_faciledb_data_array[i] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[i]), &(data_num[i]));
    }
    FacileDB_Api_Close();

    // Check
    // clang-format off
    FACILEDB_DATA_T expected_data_result[1][1] = {
        {
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        // [0]
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){2}
                    },
                    {
                        // [1]
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .value_size = 3,
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .p_value = (void *)"bb"
                    }
                }
            }
        }
    };
    // clang-format on
    uint32_t expected_data_result_length[1] = {1};

    for (uint32_t i = 0; i < 1; i++)
    {
        check_faciledb_search_result(p_faciledb_data_array[i], data_num[i], expected_data_result[i], expected_data_result_length[i]);
    }

    for (uint32_t i = 0; i < 1; i++)
    {
        for (uint32_t j = 0; j < data_num[i]; j++)
        {
            FacileDB_Api_Free_Data_Buffer(&(p_faciledb_data_array[i][j]));
        }
        free(p_faciledb_data_array[i]);
    }

    test_end(case_name);
}

void test_faciledb_make_index_insert_and_search_case1()
{
    char case_name[] = "test_faciledb_make_index_insert_and_search_case1";
    test_start(case_name);

    char db_set_name[] = "test_faciledb_make_index_insert_and_search_case1";

    // clang-format off
    FACILEDB_DATA_T data[] = {
        {
            // [0]
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){1}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .value_size = 3,
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                    .p_value = (void *)"bb"
                }
            }
        },
        {
            // [1]
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){2}
                },
                {
                    // [1]
                    .key_size = 2,
                    .p_key = (void *)"b",
                    .value_size = 3,
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                    .p_value = (void *)"bb"
                }
            }
        },
        {
            // [2]
            .record_num = 2,
            .p_data_records = (FACILEDB_RECORD_T[]){
                {
                    // [0]
                    .key_size = 2, // 'a' and '\0'
                    .p_key = (void *)"a",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){2}
                },
                {
                    // [1]
                    .key_size = 3,
                    .p_key = (void *)"c3",
                    .value_size = sizeof(uint32_t),
                    .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                    .p_value = (void *)&(uint32_t){3}
                }
            }
        }
    };
    FACILEDB_RECORD_T search_record[] = {
        {
            // match: 1
            .key_size = 2,
            .p_key = (void *)"a",
            .value_size = sizeof(uint32_t),
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
            .p_value = (void *)&(uint32_t){2}
        },
        {
            // match: 2
            .key_size = 2,
            .p_key = (void *)"b",
            .value_size = 3,
            .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
            .p_value = (void *)"bb"
        }
    };
    // clang-format on
    uint32_t data_num[2] = {0};
    FACILEDB_DATA_T *p_faciledb_data_array[2];

    FacileDB_Api_Init(test_faciledb_directory);
    FacileDB_Api_Insert_Data(db_set_name, &(data[0]));

    // make index
    FacileDB_Api_Make_Record_Index(db_set_name, &(data[0].p_data_records[0])); // a
    FacileDB_Api_Make_Record_Index(db_set_name, &(data[0].p_data_records[1])); // b

    for (uint32_t i = 1; i < (sizeof(data) / sizeof(data[0])); i++)
    {
        FacileDB_Api_Insert_Data(db_set_name, &(data[i]));
    }

    // search
    for (uint32_t i = 0; i < (sizeof(search_record) / sizeof(search_record[0])); i++)
    {
        p_faciledb_data_array[i] = FacileDB_Api_Search_Equal(db_set_name, &(search_record[i]), &(data_num[i]));
    }
    FacileDB_Api_Close();

    // Check
    // clang-format off
    FACILEDB_DATA_T expected_data_result[2][2] = {
        {
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        // [0]
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){2}
                    },
                    {
                        // [1]
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .value_size = 3,
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .p_value = (void *)"bb"
                    }
                }
            },
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        // [0]
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){2}
                    },
                    {
                        // [1]
                        .key_size = 3,
                        .p_key = (void *)"c3",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){3}
                    }
                }
            }
        },
        {
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        // [0]
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){1}
                    },
                    {
                        // [1]
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .value_size = 3,
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .p_value = (void *)"bb"
                    }
                }
            },
            {
                .record_num = 2,
                .p_data_records = (FACILEDB_RECORD_T[]){
                    {
                        // [0]
                        .key_size = 2, // 'a' and '\0'
                        .p_key = (void *)"a",
                        .value_size = sizeof(uint32_t),
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_UINT32,
                        .p_value = (void *)&(uint32_t){2}
                    },
                    {
                        // [1]
                        .key_size = 2,
                        .p_key = (void *)"b",
                        .value_size = 3,
                        .record_value_type = FACILEDB_RECORD_VALUE_TYPE_STRING,
                        .p_value = (void *)"bb"
                    }
                }
            }
        }
    };
    // clang-format on
    uint32_t expected_data_result_length[2] = {2, 2};
    for (uint32_t i = 0; i < (sizeof(search_record) / sizeof(search_record[0])); i++)
    {
        check_faciledb_search_result(p_faciledb_data_array[i], data_num[i], expected_data_result[i], expected_data_result_length[i]);
    }

    for (uint32_t i = 0; i < (sizeof(search_record) / sizeof(search_record[0])); i++)
    {
        for (uint32_t j = 0; j < data_num[i]; j++)
        {
            FacileDB_Api_Free_Data_Buffer(&(p_faciledb_data_array[i][j]));
        }
        free(p_faciledb_data_array[i]);
    }

    test_end(case_name);
}

#endif

int main()
{
    test_faciledb_init_and_close();
    test_faciledb_insert_case1();
    test_faciledb_insert_case2();
    test_faciledb_insert_case3();
    test_faciledb_insert_case4();
    test_faciledb_insert_case5();

    test_faciledb_search_case1();
    test_faciledb_search_case2();
    test_faciledb_search_case3();

    test_faciledb_delete_case1();
    test_faciledb_delete_case2();

#if ENABLE_DB_INDEX
    test_faciledb_make_index_and_search_case1();
    test_faciledb_make_index_and_search_case2();

    test_faciledb_make_index_insert_and_search_case1();
#endif
}
