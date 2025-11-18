#include "db_api.h"
#include "bpt.h"

extern int fd;
const char DB_NAME[] = "test.db";
int global_table_id = -1;

/**
 * @brief Open existing data file using ‘pathname’ or create one if not existed
 • If success, return the table id,
 * which represents the own table in this database.
 * Otherwise, return negative value
 */
int open_table(char *pathname) {
  mode_t mode = 0600; // only owner rw
  if ((fd = open(DB_NAME, O_RDWR | O_CREAT, mode)) == -1) {
    return -1;
  }
  return global_table_id; // 추후에 table_id는 구현할 기능
}

/**
 * @brief  Insert input ‘key/value’ (record) to data file at the right place.
 * If success, return 0
 * Otherwise, return non-zero value
 */
int db_insert(int64_t key, char *value) {
  int result = insert(2, key, value);
  return result > 0 ? 0 : result;
}

/**
 * @brief Find the record containing input key
 * If found matching ‘key’, store matched ‘value’ string in ret_val and return 0
 * Otherwise, return non zero value
 * Memory allocation for ret_val should occur in caller
 */
int db_find(int64_t key, char *ret_val) {
  if (find(2, key, ret_val)) {
    return 0;
  }
  return -1;
}

/**
 * @brief Find the matching record and delete it if found
 * If success, return 0. Otherwise, return non-zero value
 */
int db_delete(int64_t key) {
  int result = delete (2, key);
  return result > 0 ? 0 : -1;
}