#ifndef __MEMA_H__
#define __MEMA_H__

#include <stdlib.h>

typedef enum
{
#ifdef MEMA_USER_CONFIG
#undef MEMA_USER_CONFIG
#endif // MEMA_USER_CONFIG
#define MEMA_USER_CONFIG(username, size) username,
#include "mema_user_table.h"
#undef MEMA_USER_CONFIG
    MEMA_USER_NUM
} MEMA_USER_E;

void Mema_Api_Init(MEMA_USER_E user);
void *Mema_Api_Alloc(MEMA_USER_E user, size_t user_size);
void Mema_Api_Free(MEMA_USER_E user, void *p_user_buffer);
void *Mema_Api_Realloc(MEMA_USER_E user, void *p_user_buffer, size_t new_size);
size_t Mema_Api_Get_User_Usage_Size(MEMA_USER_E user);
size_t Mema_Api_Get_User_Usage_Count(MEMA_USER_E user);

#endif // __MEMA_H__
