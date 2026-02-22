#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "crc.c"

void test_start(char *case_name)
{
    printf("Test Case: %s START!\n", case_name);
}

void test_end(char *case_name)
{
    printf("Test Case: %s END!\n\n", case_name);
}

void test_crc_init()
{
    char case_name[] = "crc_init";

    test_start(case_name);

    uint32_t crc_value = Crc32_Api_Init();
    uint32_t expected_crc_value = 0L;

    assert(crc_value == expected_crc_value);

    test_end(case_name);
}

void test_crc_calc_case1()
{
    char case_name[] = "crc_calc";

    test_start(case_name);

    uint8_t input[] = "Hello";
    uint32_t expected_crc_value = 0xf7d18982;

    uint32_t crc_value = Crc32_Api_Init();
    crc_value = Crc32_Api_Calc(crc_value, input, strlen((char *)input));

    assert(crc_value == expected_crc_value);

    test_end(case_name);
}

void test_crc_calc_case2()
{
    char case_name[] = "crc_calc_case2";
    test_start(case_name);

    uint8_t input[] = {0x1, 0x2, 0x3, 0x4, 0x5};
    uint32_t expected_crc_value = 0x470B99F4;

    uint32_t crc_value = Crc32_Api_Init();
    crc_value = Crc32_Api_Calc(crc_value, input, 3);
    crc_value = Crc32_Api_Calc(crc_value, input + 3, 2);

    assert(crc_value == expected_crc_value);

    test_end(case_name);
}

int main()
{
    test_crc_init();
    test_crc_calc_case1();
    test_crc_calc_case2();
}
