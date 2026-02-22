#ifndef __CRC_H__
#define __CRC_H__

#include <stdint.h>

uint32_t Crc32_Api_Init();
uint32_t Crc32_Api_Calc(uint32_t crc_value, void *data, uint32_t length);

#endif // __CRC_H__