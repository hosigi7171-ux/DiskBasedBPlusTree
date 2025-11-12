#ifndef PAGE_H
#define PAGE_H

#include <stdint.h>

typedef uint64_t pagenum_t;

#define PAGE_SIZE 4096
#define HEADER_PAGE_RESERVED 4072
#define NON_HEADER_PAGE_RESERVED 104
#define VALUE_SIZE 120
#define RECORDS_CNT 31
#define ENTRY_CNT 248

// header page
typedef struct {
  pagenum_t free_page_num;
  pagenum_t root_page_num;
  pagenum_t num_of_pages;
  char reserved[HEADER_PAGE_RESERVED];
} header_page_t;

// free page
typedef struct {
  pagenum_t next_free_page_num;
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
  uint32_t is_leaf;
  uint32_t num_of_keys;
  char reserved[NON_HEADER_PAGE_RESERVED];
  pagenum_t right_sibling_page_num;

  record_t records[RECORDS_CNT];
} leaf_page_t;

// internal page
typedef struct {
  // header
  pagenum_t parent_page_num;
  int32_t is_leaf;
  int32_t num_of_keys;
  char reserved[NON_HEADER_PAGE_RESERVED];
  pagenum_t one_more_page_num;

  entry_t entries[ENTRY_CNT];
} internal_page_t;

// raw page for type casting
typedef struct {
  char data[PAGE_SIZE];
} page_t;

#endif