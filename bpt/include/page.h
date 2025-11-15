#ifndef PAGE_H
#define PAGE_H

#include <stdint.h>

typedef uint64_t pagenum_t;
typedef uint64_t magicnum_t;

#define PAGE_SIZE 4096
#define ORIGIN_HEADER_PAGE_RESERVED 4072
#define HEADER_PAGE_RESERVED 4040
#define NON_HEADER_PAGE_RESERVED 104
#define VALUE_SIZE 120
#define RECORDS_CNT 31
#define ENTRY_CNT 248
#define UNUSED_SIZE 4088
#define MAGIC_NUM 0x20251113
#define LEAF 1
#define INTERNAL 0
#define INITIAL_INTERNAL_CAPACITY 1000
#define PAGE_NULL 0
#define HEADER_PAGE_POS 1

// // other's header page
// typedef struct {
//   pagenum_t free_page_num;
//   pagenum_t root_page_num;
//   pagenum_t num_of_pages;
//   char reserved[HEADER_PAGE_RESERVED]; // not used
// } header_page_t;

// my header
typedef struct {
  // Legacy Fields (for compatibility with original Requirements)
  pagenum_t free_page_num; // 0 if end of free list
  pagenum_t root_page_num;
  pagenum_t num_of_pages;
  // New Fields (Custom Design & Optimization)
  magicnum_t magic_num; // Field to distinguish from legacy version
  pagenum_t free_internal_head;
  pagenum_t free_leaf_head;
  pagenum_t internal_next_alloc;
  char reserved[HEADER_PAGE_RESERVED];
} header_page_t;

// free page
typedef struct {
  pagenum_t next_free_page_num;
  char unused[UNUSED_SIZE];
} free_page_t;

// key-value record
typedef struct {
  int64_t key;
  char value[VALUE_SIZE];
} record_t;

// key-pagenum entry
typedef struct {
  int64_t key;
  pagenum_t page_num;
} entry_t;

// leaf page
typedef struct {
  // header
  pagenum_t parent_page_num;
  uint32_t is_leaf; // 1
  uint32_t num_of_keys;
  char reserved[NON_HEADER_PAGE_RESERVED]; // not used
  pagenum_t right_sibling_page_num;        // if rihgtmost, 0

  record_t records[RECORDS_CNT];
} leaf_page_t;

// internal page
typedef struct {
  // header
  pagenum_t parent_page_num;
  int32_t is_leaf; // 0
  int32_t num_of_keys;
  char reserved[NON_HEADER_PAGE_RESERVED]; // not used
  pagenum_t one_more_page_num; // leftmost page num to know key ranges

  entry_t entries[ENTRY_CNT];
} internal_page_t;

// page header - for referencing header
typedef struct {
  pagenum_t parent_page_num;
  uint32_t is_leaf;
  uint32_t num_of_keys;
  char reserved[NON_HEADER_PAGE_RESERVED];
} page_header_t;

// raw page for type casting
typedef struct {
  char data[PAGE_SIZE];
} page_t;

#endif