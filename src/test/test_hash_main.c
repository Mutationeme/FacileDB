#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>

#include "hash.c"

void test_start(char *case_name)
{
    printf("Test Case: %s START!\n", case_name);
}

void test_end(char *case_name)
{
    printf("Test Case: %s END!\n\n", case_name);
}

void test_hash()
{
    char case_name[] = "hash";

    test_start(case_name);

    uint8_t s[] = "dog";
    HASH_VALUE_T result = Hash(s, strlen((char *)s));
    HASH_VALUE_T expected = 0x0b886aff;

    // printf("0x%llx\n", result);
    assert(Hash_Api_Compare(result, expected) == HASH_VALUE_COMPARE_EQUAL);

    test_end(case_name);
}

int main()
{
    test_hash();
}
