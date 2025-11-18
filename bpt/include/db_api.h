#ifndef DB_API_H
#define DB_API_H

#include "bpt.h"
#include <fcntl.h>
#include <sys/types.h>

extern const char DB_NAME[];
extern int global_table_id; // for future extension

int open_table(char *pathname);
int db_insert(int64_t key, char *value);
int db_find(int64_t key, char *ret_val);
int db_delete(int64_t key);

#endif