#ifndef DB_API_H
#define DB_API_H

#include <cstdint>

#include "bpt.h"

int open_table(char* pathname);
int db_insert(int64_t key, char* value);
int db_find(int64_t key, char* ret_val);
int db_delete(int64_t key);

#endif