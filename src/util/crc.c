#include <stdint.h>
#include <zlib.h>

#include "crc.h"

uint32_t Crc32_Api_Init()
{
    return crc32(0L, Z_NULL, 0);
}

uint32_t Crc32_Api_Calc(uint32_t crc_value, void *data, uint32_t length)
{
    return crc32((uLong)crc_value, (const Bytef *)data, (uInt)length);
}
