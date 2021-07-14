/*
 * common.h
 *
 *  Created on: 10 Nov 2018
 *      Author: root
 */

#pragma once

#include <stdarg.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#if defined(DEBUG) || !defined(NDEBUG)
#include "readies/cetara/diag/gdb.h"
#endif

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

void SetId(char* finalId, char* idBuf, char* idStrBuf, long long* lastID);
int rg_vasprintf(char **__restrict __ptr, const char *__restrict __fmt, va_list __arg);
int rg_asprintf(char **__ptr, const char *__restrict __fmt, ...);
char* ArrToStr(void** arr, size_t len, char*(*toStr)(void*));
const char* GetShardUniqueId();
int IsKeyMatch(const char* prefix, const char* key, size_t prefixLen);
