#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>

#define INDEX_TEST (1)
#define INDEX_ORDER (3)
#define INDEX_PAYLOAD_SIZE (8)

#include "index.c"

char test_index_directory[] = "./bin/test_index_files/";

void test_start(char *case_name)
{
    printf("Test Case: %s START!\n", case_name);
}

void test_end(char *case_name)
{
    printf("Test Case: %s END!\n\n", case_name);
}

void get_test_index_file_path(char *p_index_file_path, char *p_index_key)
{
    strcpy(p_index_file_path, test_index_directory);
    strcat(p_index_file_path, p_index_key);
    strcat(p_index_file_path, ".faciledb_index");
}

void check_index_properties(INDEX_PROPERTIES_T *p_index_properties_1, INDEX_PROPERTIES_T *p_index_properties_2)
{
    assert(p_index_properties_1->tag_num == p_index_properties_2->tag_num);
    assert(p_index_properties_1->root_tag == p_index_properties_2->root_tag);
    assert(p_index_properties_1->index_id_type == p_index_properties_2->index_id_type);
    assert(p_index_properties_1->key_size == p_index_properties_2->key_size);
    assert(memcmp(p_index_properties_1->p_key, p_index_properties_2->p_key, p_index_properties_1->key_size) == 0);
}

void check_index_node(INDEX_NODE_T *p_index_node_1, INDEX_NODE_T *p_index_node_2, INDEX_ID_TYPE_E index_id_type)
{
    uint32_t index_id_size = Index_Id_Type_Get_Size(index_id_type);
    bool is_leaf_node = (p_index_node_1->child_tag[0] == 0) ? (true) : (false);

    assert(p_index_node_1->tag == p_index_node_2->tag);
    assert(p_index_node_1->level == p_index_node_2->level);
    assert(p_index_node_1->length == p_index_node_2->length);
    assert(p_index_node_1->parent_tag == p_index_node_2->parent_tag);
    assert(p_index_node_1->next_tag == p_index_node_2->next_tag);

    assert(p_index_node_1->child_tag[0] == p_index_node_2->child_tag[0]);
    for (uint32_t i = 0; i < p_index_node_1->length; i++)
    {
        assert(memcmp(p_index_node_1->elements[i].p_index_id, p_index_node_2->elements[i].p_index_id, index_id_size) == 0);
        // No need to check the element payload of non-leaf node.
        if (is_leaf_node)
        {
            assert(memcmp(p_index_node_1->elements[i].index_payload, p_index_node_2->elements[i].index_payload, INDEX_PAYLOAD_SIZE) == 0);
        }
        assert((p_index_node_1->child_tag[i + 1]) == (p_index_node_2->child_tag[i + 1]));
    }
}

// void print_index_node(INDEX_NODE_T *p_index_node, INDEX_ID_TYPE_E index_id_type)
// {
//     printf("\ntag: %d\n", p_index_node->tag);
//     printf("level: %d\n", p_index_node->level);
//     printf("length: %d\n", p_index_node->length);
//     printf("parnet_tag: %d\n", p_index_node->parent_tag);
//     printf("next_tag: %d\n", p_index_node->next_tag);
//     for (uint32_t i = 0; i < p_index_node->length + 1; i++)
//     {
//         printf("child_tag[%d]: %d\n", i, p_index_node->child_tag[i]);
//     }
//     for (uint32_t i = 0; i < p_index_node->length; i++)
//     {
//         if (index_id_type == INDEX_ID_TYPE_UINT32)
//         {
//             uint32_t index_id = *((uint32_t *)p_index_node->elements[i].p_index_id);
//             printf("element_index_id[%d]: %d\n", i, index_id);
//         }

//         if (INDEX_PAYLOAD_SIZE > 0)
//         {
//             printf("\telement_payload_first_char: %c\n", *((char *)(p_index_node->elements[i].index_payload)));
//         }
//     }
// }

/*----------------------------*/

void test_index_init_and_close()
{
    char case_name[] = "test_index_init_and_close";
    test_start(case_name);

    Index_Api_Init(test_index_directory);
    Index_Api_Close();

    test_end(case_name);
}

void test_index_insert_case1()
{
    char case_name[] = "test_index_insert_case1";
    test_start(case_name);

    char index_key[] = "test_index_insert_case1";
    uint32_t target = 1;
    INDEX_ID_TYPE_E index_id_type = INDEX_ID_TYPE_UINT32;
    char payload[INDEX_PAYLOAD_SIZE / sizeof(char)] = "aaaaa";

    Index_Api_Init(test_index_directory);
    Index_Api_Insert_Element(index_key, (void *)&target, index_id_type, (void *)payload, strlen(payload) * sizeof(char));
    Index_Api_Close();

    // check
    {
        char index_file_path[INDEX_FILE_PATH_BUFFER_LENGTH] = {0};
        INDEX_INFO_T index_info;

        INDEX_PROPERTIES_T expected_index_properties;
        INDEX_NODE_T index_node, expected_index_node;

        index_info_init(&index_info);
        get_test_index_file_path(index_file_path, index_key);
        index_info.index_file = fopen(index_file_path, "rb");
        assert(index_info.index_file != NULL);

        // check index properties
        read_index_properties(&index_info);

        expected_index_properties.tag_num = 1;
        expected_index_properties.root_tag = 1;
        expected_index_properties.index_id_type = index_id_type;
        expected_index_properties.key_size = strlen(index_key);
        allocate_index_properties_resources(&expected_index_properties, expected_index_properties.key_size);
        memcpy(expected_index_properties.p_key, index_key, strlen(index_key) + 1);
        check_index_properties(&(index_info.index_properties), &expected_index_properties);

        // check index node
        index_node_init(&index_node, 1);
        read_index_node(&index_info, 1, &index_node);

        index_node_init(&expected_index_node, 1);
        expected_index_node.level = 0;
        expected_index_node.length = 1;
        setup_index_element(&(expected_index_node.elements[0]), &target, index_id_type, payload, strlen(payload) * sizeof(char));
        check_index_node(&index_node, &expected_index_node, index_id_type);

        // free check items
        close_index_info(&index_info);
        free_index_properties_resources(&expected_index_properties);
        free_index_node_resources(&index_node);
        free_index_node_resources(&expected_index_node);
    } // check

    test_end(case_name);
}

void test_index_insert_case11()
{
    char case_name[] = "test_index_insert_case11";
    test_start(case_name);

    char index_key[] = "test_index_insert_case11";
    uint32_t target[11] = {1, 4, 9, 10, 11, 12, 13, 15, 16, 20, 25};
    INDEX_ID_TYPE_E index_id_type = INDEX_ID_TYPE_UINT32;
    char payload[11][100];

    // 11 elements
    Index_Api_Init(test_index_directory);
    for (uint32_t i = 0; i < 11; i++)
    {
        payload[i][0] = (i % 26) + 'a';
        payload[i][1] = '\0';

        Index_Api_Insert_Element(index_key, &(target[i]), index_id_type, payload[i], strlen(payload[i]) * sizeof(char));
    }
    Index_Api_Close();

    // check
    {
        char test_index_file_path[INDEX_FILE_PATH_BUFFER_LENGTH] = {0};
        INDEX_INFO_T index_info;
        INDEX_PROPERTIES_T expected_index_properties;
        INDEX_NODE_T index_node, expected_index_node;

        get_test_index_file_path(test_index_file_path, index_key);
        index_info.index_file = fopen(test_index_file_path, "rb");
        assert(index_info.index_file != NULL);

        // check index properties
        read_index_properties(&index_info);
        expected_index_properties.tag_num = 8;
        expected_index_properties.root_tag = 8;
        expected_index_properties.index_id_type = index_id_type;
        expected_index_properties.key_size = strlen(index_key);
        allocate_index_properties_resources(&expected_index_properties, expected_index_properties.key_size);
        memcpy(expected_index_properties.p_key, index_key, strlen(index_key) + 1);
        check_index_properties(&(index_info.index_properties), &expected_index_properties);

        // check index nodes
        // Index node tag 1 (leaf node)
        index_node_init(&index_node, 1);
        read_index_node(&index_info, 1, &index_node);
        // print_index_node(&index_node, index_id_type);
        index_node_init(&expected_index_node, 1);
        expected_index_node.level = 0;
        expected_index_node.length = 2;
        expected_index_node.parent_tag = 3;
        expected_index_node.next_tag = 2;
        setup_index_element(&(expected_index_node.elements[0]), &(target[0]), index_id_type, payload[0], strlen(payload[0]) * sizeof(char));
        setup_index_element(&(expected_index_node.elements[1]), &(target[1]), index_id_type, payload[1], strlen(payload[1]) * sizeof(char));
        check_index_node(&index_node, &expected_index_node, index_id_type);

        free_index_node_resources(&index_node);
        free_index_node_resources(&expected_index_node);

        // Index node tag 2 (leaf node)
        index_node_init(&index_node, 2);
        index_node_init(&expected_index_node, 2);
        read_index_node(&index_info, 2, &index_node);
        expected_index_node.level = 0;
        expected_index_node.length = 2;
        expected_index_node.parent_tag = 3;
        expected_index_node.next_tag = 4;
        setup_index_element(&(expected_index_node.elements[0]), &(target[2]), index_id_type, payload[2], strlen(payload[2]) * sizeof(char));
        setup_index_element(&(expected_index_node.elements[1]), &(target[3]), index_id_type, payload[3], strlen(payload[3]) * sizeof(char));
        check_index_node(&index_node, &expected_index_node, index_id_type);

        free_index_node_resources(&index_node);
        free_index_node_resources(&expected_index_node);

        // Index node tag 3 (non-leaf node)
        index_node_init(&index_node, 3);
        index_node_init(&expected_index_node, 3);
        read_index_node(&index_info, 3, &index_node);
        expected_index_node.level = 1;
        expected_index_node.length = 2;
        expected_index_node.parent_tag = 8;
        expected_index_node.next_tag = 7;
        expected_index_node.child_tag[0] = 1;
        expected_index_node.child_tag[1] = 2;
        expected_index_node.child_tag[2] = 4;
        setup_index_element(&(expected_index_node.elements[0]), &(target[2]), index_id_type, NULL, 0);
        setup_index_element(&(expected_index_node.elements[1]), &(target[4]), index_id_type, NULL, 0);
        check_index_node(&index_node, &expected_index_node, index_id_type);

        free_index_node_resources(&index_node);
        free_index_node_resources(&expected_index_node);

        // Index node tag 4 (leaf node)
        index_node_init(&index_node, 4);
        read_index_node(&index_info, 4, &index_node);
        index_node_init(&expected_index_node, 4);
        expected_index_node.level = 0;
        expected_index_node.length = 2;
        expected_index_node.parent_tag = 3;
        expected_index_node.next_tag = 5;
        setup_index_element(&(expected_index_node.elements[0]), &(target[4]), index_id_type, payload[4], strlen(payload[4]) * sizeof(char));
        setup_index_element(&(expected_index_node.elements[1]), &(target[5]), index_id_type, payload[5], strlen(payload[5]) * sizeof(char));
        check_index_node(&index_node, &expected_index_node, index_id_type);

        free_index_node_resources(&index_node);
        free_index_node_resources(&expected_index_node);

        // Index node tag 5 (leaf node)
        index_node_init(&index_node, 5);
        read_index_node(&index_info, 5, &index_node);
        index_node_init(&expected_index_node, 5);
        expected_index_node.level = 0;
        expected_index_node.length = 2;
        expected_index_node.parent_tag = 7;
        expected_index_node.next_tag = 6;
        setup_index_element(&(expected_index_node.elements[0]), &(target[6]), index_id_type, payload[6], strlen(payload[6]) * sizeof(char));
        setup_index_element(&(expected_index_node.elements[1]), &(target[7]), index_id_type, payload[7], strlen(payload[7]) * sizeof(char));
        check_index_node(&index_node, &expected_index_node, index_id_type);

        free_index_node_resources(&index_node);
        free_index_node_resources(&expected_index_node);

        // Index node tag 6 (leaf node)
        index_node_init(&index_node, 6);
        read_index_node(&index_info, 6, &index_node);
        index_node_init(&expected_index_node, 6);
        expected_index_node.level = 0;
        expected_index_node.length = 3;
        expected_index_node.parent_tag = 7;
        expected_index_node.next_tag = 0;
        setup_index_element(&(expected_index_node.elements[0]), &(target[8]), index_id_type, payload[8], strlen(payload[8]) * sizeof(char));
        setup_index_element(&(expected_index_node.elements[1]), &(target[9]), index_id_type, payload[9], strlen(payload[9]) * sizeof(char));
        setup_index_element(&(expected_index_node.elements[2]), &(target[10]), index_id_type, payload[10], strlen(payload[10]) * sizeof(char));
        check_index_node(&index_node, &expected_index_node, index_id_type);

        free_index_node_resources(&index_node);
        free_index_node_resources(&expected_index_node);

        // Index node tag 7 (non-leaf node)
        index_node_init(&index_node, 7);
        read_index_node(&index_info, 7, &index_node);
        index_node_init(&expected_index_node, 7);
        expected_index_node.level = 1;
        expected_index_node.length = 1;
        expected_index_node.parent_tag = 8;
        expected_index_node.next_tag = 0;
        expected_index_node.child_tag[0] = 5;
        expected_index_node.child_tag[1] = 6;
        setup_index_element(&(expected_index_node.elements[0]), &(target[8]), index_id_type, payload[8], strlen(payload[8]) * sizeof(char));
        check_index_node(&index_node, &expected_index_node, index_id_type);

        free_index_node_resources(&index_node);
        free_index_node_resources(&expected_index_node);

        // Index node tag 8 (root node)
        index_node_init(&index_node, 8);
        read_index_node(&index_info, 8, &index_node);
        index_node_init(&expected_index_node, 8);
        expected_index_node.level = 2;
        expected_index_node.length = 1;
        expected_index_node.parent_tag = 0;
        expected_index_node.next_tag = 0;
        expected_index_node.child_tag[0] = 3;
        expected_index_node.child_tag[1] = 7;
        setup_index_element(&(expected_index_node.elements[0]), &(target[6]), index_id_type, payload[6], strlen(payload[6]) * sizeof(char));
        check_index_node(&index_node, &expected_index_node, index_id_type);

        free_index_node_resources(&index_node);
        free_index_node_resources(&expected_index_node);

        close_index_info(&index_info);
        free_index_properties_resources(&expected_index_properties);
    } // check

    test_end(case_name);
}

void test_index_key_exists()
{
    char case_name[] = "test_index_key_exists";
    test_start(case_name);

    char p_index_key[] = "test_index_key_exists";
    char p_fake_index_key[] = "test_index_key_exists_fake";
    uint32_t target = 1;
    char payload[100] = "aaa";

    Index_Api_Init(test_index_directory);
    Index_Api_Insert_Element(p_index_key, &target, INDEX_ID_TYPE_UINT32, &payload, strlen(payload));

    // check
    assert(Index_Api_Index_Key_Exist(p_index_key) == true);
    assert(Index_Api_Index_Key_Exist(p_fake_index_key) == false);

    Index_Api_Close();

    test_end(case_name);
}

void test_index_search_case1()
{
    char case_name[] = "test_index_search_case1";
    test_start(case_name);

    char p_index_key[] = "test_index_search_case1";
    uint32_t target = 1;
    INDEX_ID_TYPE_E index_id_type = INDEX_ID_TYPE_UINT32;
    char payload[INDEX_PAYLOAD_SIZE / sizeof(char)] = "aaaaa";

    uint32_t result_length = 0;
    void *result = NULL;

    Index_Api_Init(test_index_directory);
    Index_Api_Insert_Element(p_index_key, &target, index_id_type, payload, strlen(payload) * sizeof(char));
    result = Index_Api_Search_Equal(p_index_key, &target, index_id_type, &result_length);
    Index_Api_Close();

    // check
    {
        assert(result_length == 1);
        assert(memcmp(payload, result, INDEX_PAYLOAD_SIZE) == 0);
    } // check

    Index_Api_Free_Search_Result(result);

    test_end(case_name);
}

void test_index_search_case11()
{
    char case_name[] = "test_index_search_case11";
    test_start(case_name);

    char p_index_key[] = "test_index_search_case11";
    uint32_t target[11] = {1, 9, 9, 10, 11, 12, 12, 12, 12, 20, 20};
    INDEX_ID_TYPE_E index_id_type = INDEX_ID_TYPE_UINT32;
    char payload[11][INDEX_PAYLOAD_SIZE];

    void *result[6];
    uint32_t reuslt_length[6];

    // 11 elements
    Index_Api_Init(test_index_directory);
    for (uint32_t i = 0; i < 11; i++)
    {
        memset(payload[i], 0, INDEX_PAYLOAD_SIZE);
        payload[i][0] = (i % 26) + 'a';
        payload[i][1] = '\0';
        Index_Api_Insert_Element(p_index_key, &(target[i]), index_id_type, payload[i], strlen(payload[i]) * sizeof(char));
    }

    result[0] = Index_Api_Search_Equal(p_index_key, &(target[0]), index_id_type, &(reuslt_length[0]));
    result[1] = Index_Api_Search_Equal(p_index_key, &(target[1]), index_id_type, &(reuslt_length[1]));
    result[2] = Index_Api_Search_Equal(p_index_key, &(target[3]), index_id_type, &(reuslt_length[2]));
    result[3] = Index_Api_Search_Equal(p_index_key, &(target[4]), index_id_type, &(reuslt_length[3]));
    result[4] = Index_Api_Search_Equal(p_index_key, &(target[5]), index_id_type, &(reuslt_length[4]));
    result[5] = Index_Api_Search_Equal(p_index_key, &(target[9]), index_id_type, &(reuslt_length[5]));

    Index_Api_Close();

    // check
    {
        assert(reuslt_length[0] == 1);
        assert(memcmp(result[0], payload[0], INDEX_PAYLOAD_SIZE) == 0);

        // check if result[1][0] == result[1][1] == target payload
        assert(reuslt_length[1] == 2);
        assert(
            (memcmp(result[1], payload[1], INDEX_PAYLOAD_SIZE) == 0 && memcmp(result[1] + INDEX_PAYLOAD_SIZE, payload[2], INDEX_PAYLOAD_SIZE) == 0) ||
            (memcmp(result[1], payload[2], INDEX_PAYLOAD_SIZE) == 0 && memcmp(result[1] + INDEX_PAYLOAD_SIZE, payload[1], INDEX_PAYLOAD_SIZE) == 0));

        assert(reuslt_length[2] == 1);
        assert(memcmp(result[2], payload[3], INDEX_PAYLOAD_SIZE) == 0);

        assert(reuslt_length[3] == 1);
        assert(memcmp(result[3], payload[4], INDEX_PAYLOAD_SIZE) == 0);

        assert(reuslt_length[4] == 4);
        assert(memcmp(result[4] + INDEX_PAYLOAD_SIZE * 0, payload[5], INDEX_PAYLOAD_SIZE) == 0);
        assert(memcmp(result[4] + INDEX_PAYLOAD_SIZE * 1, payload[6], INDEX_PAYLOAD_SIZE) == 0);
        assert(memcmp(result[4] + INDEX_PAYLOAD_SIZE * 2, payload[7], INDEX_PAYLOAD_SIZE) == 0);
        assert(memcmp(result[4] + INDEX_PAYLOAD_SIZE * 3, payload[8], INDEX_PAYLOAD_SIZE) == 0);

        assert(reuslt_length[5] == 2);
        assert(memcmp(result[5] + INDEX_PAYLOAD_SIZE * 0, payload[9], INDEX_PAYLOAD_SIZE) == 0);
        assert(memcmp(result[5] + INDEX_PAYLOAD_SIZE * 1, payload[10], INDEX_PAYLOAD_SIZE) == 0);
    } // check

    for(uint32_t i = 0; i < 6; i++)
    {
        Index_Api_Free_Search_Result(result[i]);
    }

    test_end(case_name);
}

int main()
{
    test_index_init_and_close();
    test_index_insert_case1();
    test_index_insert_case11();
    test_index_key_exists();
    test_index_search_case1();
    test_index_search_case11();

    return 0;
}
