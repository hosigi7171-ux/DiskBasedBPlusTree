/*
 *  bpt.c
 */
#define Version "1.14"
/*
 *
 *  bpt:  B+ Tree Implementation
 *  Copyright (C) 2010-2016  Amittai Aviram  http://www.amittai.com
 *  All rights reserved.
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice,
 *  this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice,
 *  this list of conditions and the following disclaimer in the documentation
 *  and/or other materials provided with the distribution.

 *  3. Neither the name of the copyright holder nor the names of its
 *  contributors may be used to endorse or promote products derived from this
 *  software without specific prior written permission.

 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.

 *  Author:  Amittai Aviram
 *    http://www.amittai.com
 *    amittai.aviram@gmail.edu or afa13@columbia.edu
 *  Original Date:  26 June 2010
 *  Last modified: 17 June 2016
 *
 *  This implementation demonstrates the B+ tree data structure
 *  for educational purposes, includin insertion, deletion, search, and display
 *  of the search path, the leaves, or the whole tree.
 *
 *  Must be compiled with a C99-compliant C compiler such as the latest GCC.
 *
 *  Usage:  bpt [order]
 *  where order is an optional argument
 *  (integer MIN_ORDER <= order <= MAX_ORDER)
 *  defined as the maximal number of pointers in any node.
 *
 */

#include "bpt.h"

/**
 * Declaration of helper functions used only here
 */
record_t *prepare_records_for_split(leaf_page_t *leaf_page, int64_t key,
                                    const char *value);
int64_t distribute_records_to_leaves(leaf_page_t *leaf_page,
                                     leaf_page_t *new_leaf_page,
                                     record_t *temp_records,
                                     pagenum_t new_leaf_num);
entry_t *prepare_entries_for_split(internal_page_t *old_node_page,
                                   int64_t left_index, int64_t key,
                                   pagenum_t right);
int64_t distribute_entries_and_update_children(pagenum_t old_node_num,
                                               internal_page_t *old_node_page,
                                               pagenum_t new_node_num,
                                               internal_page_t *new_node_page,
                                               entry_t *temp_entries);
void coalesce_internal_nodes(page_t *neighbor_buf, page_t *target_buf,
                             int neighbor_num, int64_t k_prime);
void coalesce_leaf_nodes(page_t *neighbor_buf, page_t *target_buf);
void redistribute_from_left(pagenum_t target_num, page_t *target_buf,
                            page_t *neighbor_buf, internal_page_t *parent_page,
                            int k_prime_index, int k_prime);
void redistribute_internal_from_left(pagenum_t target_num, page_t *target_buf,
                                     page_t *neighbor_buf,
                                     internal_page_t *parent_page,
                                     int k_prime_index, int k_prime);
void redistribute_leaf_from_left(page_t *target_buf, page_t *neighbor_buf,
                                 internal_page_t *parent_page,
                                 int k_prime_index);
void redistribute_from_right(pagenum_t target_num, page_t *target_buf,
                             page_t *neighbor_buf, internal_page_t *parent_page,
                             int k_prime_index, int k_prime);
void redistribute_internal_from_right(pagenum_t target_num, page_t *target_buf,
                                      page_t *neighbor_buf,
                                      internal_page_t *parent_page,
                                      int k_prime_index, int k_prime);
void redistribute_leaf_from_right(page_t *target_buf, page_t *neighbor_buf,
                                  internal_page_t *parent_page,
                                  int k_prime_index);
int find_neighbor_and_kprime(pagenum_t target_node,
                             internal_page_t *parent_page,
                             page_header_t *target_header,
                             pagenum_t *neighbor_num_out,
                             int *k_prime_key_index_out);
int handle_underflow(pagenum_t target_node);

// GLOBALS.

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
queue *q_head;

// OUTPUT AND UTILITIES

/* Copyright and license notice to user at startup.
 */
void license_notice(void) {
  printf("bpt version %s -- Copyright (C) 2010  Amittai Aviram "
         "http://www.amittai.com\n",
         Version);
  printf("This program comes with ABSOLUTELY NO WARRANTY; for details "
         "type `show w'.\n"
         "This is free software, and you are welcome to redistribute it\n"
         "under certain conditions; type `show c' for details.\n\n");
}

/* Routine to print portion of GPL license to stdout.
 */
void print_license(int license_part) {
  int start, end, line;
  FILE *fp;
  char buffer[0x100];

  switch (license_part) {
  case LICENSE_WARRANTEE:
    start = LICENSE_WARRANTEE_START;
    end = LICENSE_WARRANTEE_END;
    break;
  case LICENSE_CONDITIONS:
    start = LICENSE_CONDITIONS_START;
    end = LICENSE_CONDITIONS_END;
    break;
  default:
    return;
  }

  fp = fopen(LICENSE_FILE, "r");
  if (fp == NULL) {
    perror("print_license: fopen");
    exit(EXIT_FAILURE);
  }
  for (line = 0; line < start; line++)
    fgets(buffer, sizeof(buffer), fp);
  for (; line < end; line++) {
    fgets(buffer, sizeof(buffer), fp);
    printf("%s", buffer);
  }
  fclose(fp);
}

void usage(void) {
  printf("Simple DBMS (B+ Tree based) - Commands:\n\n");
  printf("  o <filename>   Open (or create) a database file\n");
  printf("  i <key>        Insert key (value is automatically generated as "
         "\"<key>_value\")\n");
  printf("  f <key>        Find and print the value for <key>\n");
  printf("  d <key>        Delete <key> and its value\n");
  printf("  r <k1> <k2>    Print all keys and values in range [min(k1,k2) ... "
         "max(k1,k2)] (max range is 10000)\n");
  printf("  t              Print the entire B+ tree structure\n");
  printf("  q              Quit the program (closes the current table)\n");
  printf("  ?              Show this help message\n\n");
  printf("> ");
}

/* Helper function for printing the
 * tree out.  See print_tree.
 */
void enqueue(pagenum_t new_page_num, int level) {
  if (q_head == NULL) {
    q_head = (queue *)malloc(sizeof(queue));
    q_head->page_num = new_page_num;
    q_head->level = level;
    q_head->next = NULL;
  } else {
    queue *cur = q_head;
    while (cur->next != NULL) {
      cur = cur->next;
    }
    queue *new_node = (queue *)malloc(sizeof(queue));
    new_node->page_num = new_page_num;
    new_node->level = level;
    new_node->next = NULL;

    cur->next = new_node;
  }
}

/* Helper function for printing the
 * tree out.  See print_tree.
 */
queue *dequeue(void) {
  queue *next_node = q_head;
  q_head = q_head->next;
  next_node->next = NULL;
  return next_node;
}

/* Prints the bottom row of keys
 * of the tree
 */
void print_leaves(void) {
  page_t header_buf, current_buf;

  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header = (header_page_t *)&header_buf;
  pagenum_t current_page_num = header->root_page_num;

  if (current_page_num == PAGE_NULL) {
    printf("empty tree.\n");
    return;
  }

  // find left most leaf page
  while (1) {
    file_read_page(current_page_num, &current_buf);
    page_header_t *header = (page_header_t *)&current_buf;

    if (header->is_leaf == LEAF) {
      break;
    } else {
      internal_page_t *internal_page = (internal_page_t *)&current_buf;
      current_page_num = internal_page->one_more_page_num;

      if (current_page_num == PAGE_NULL) {
        printf("error: Internal node with no children.\n");
        return;
      }
    }
  }

  // print leaf pages
  do {
    if (current_page_num == PAGE_NULL) {
      break;
    }
    file_read_page(current_page_num, &current_buf);
    leaf_page_t *leaf_page = (leaf_page_t *)&current_buf;
    page_header_t *header = (page_header_t *)&current_buf;

    for (int i = 0; i < header->num_of_keys; i++) {
      printf("%" PRId64 " ", leaf_page->records[i].key);
    }

    current_page_num = leaf_page->right_sibling_page_num;

    if (current_page_num != PAGE_NULL) {
      printf("| ");
    }

  } while (current_page_num != PAGE_NULL);

  printf("\n");
}

/* Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
int height(pagenum_t header_page_num) {
  int h = 0;
  page_t header_buf, current_buf;

  file_read_page(header_page_num, &header_buf);
  header_page_t *header = (header_page_t *)&header_buf;
  pagenum_t current_page_num = header->root_page_num;

  if (current_page_num == PAGE_NULL) {
    return 0;
  }

  while (1) {
    file_read_page(current_page_num, &current_buf);
    page_header_t *header = (page_header_t *)&current_buf;

    if (header->is_leaf == LEAF) {
      break;
    } else {
      internal_page_t *internal_page = (internal_page_t *)&current_buf;
      current_page_num = internal_page->one_more_page_num;
      h++;

      if (current_page_num == PAGE_NULL) {
        return -1;
      }
    }
  }
  return h;
}

/**
 * @brief bfs로 b+tree를 level 별로 탐색 및 출력
 */
void print_tree() {
  queue *now_node_ptr = NULL;
  int i = 0;
  int current_level = 0;

  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header = (header_page_t *)&header_buf;
  pagenum_t root = header->root_page_num;

  if (root == PAGE_NULL) {
    printf("empty tree.\n");
    return;
  }

  enqueue(root, 0);

  printf("--- level %d ---\n", current_level);

  while (q_head != NULL) {
    now_node_ptr = dequeue();
    pagenum_t now_page_num = now_node_ptr->page_num;

    page_t now_buf;
    file_read_page(now_page_num, &now_buf);
    page_header_t *now_header = (page_header_t *)&now_buf;

    if (now_node_ptr->level != current_level) {
      current_level = now_node_ptr->level;
      printf("\n--- level %d ---\n", current_level);
    }

    printf("[");
    if (now_header->is_leaf == LEAF) {
      leaf_page_t *leaf_page = (leaf_page_t *)&now_buf;

      // Leaf Node: 키 출력
      for (i = 0; i < leaf_page->num_of_keys; i++) {
        printf("%" PRId64 " ", leaf_page->records[i].key);
      }
    } else {
      internal_page_t *internal_page = (internal_page_t *)&now_buf;
      int next_level = now_node_ptr->level + 1;

      // one_more_page_num 삽입
      if (internal_page->one_more_page_num != PAGE_NULL) {
        enqueue(internal_page->one_more_page_num, next_level);
      }

      // entry 삽입
      for (i = 0; i < internal_page->num_of_keys; i++) {
        printf("%" PRId64 " ", internal_page->entries[i].key);

        if (internal_page->entries[i].page_num != PAGE_NULL) {
          enqueue(internal_page->entries[i].page_num, next_level);
        }
      }
    }
    printf("] | ");

    free(now_node_ptr);
  }
  printf("\n");
}

/* Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
void find_and_print(int64_t key) {
  char result_buf[VALUE_SIZE];

  int result = find(key, result_buf);

  if (result == SUCCESS) {
    printf("Record -- key %" PRId64 ", value %s.\n", key, result_buf);
  } else {
    // 실패 시
    printf("Record not found key %" PRId64 "\n", key);
  }
}

/* Finds and prints the keys, pointers, and values within a range
 * of keys between key_start and key_end, including both bounds.
 */
int find_and_print_range(int64_t key_start, int64_t key_end) {
  int i;
  int64_t returned_keys[MAX_RANGE_SIZE];
  pagenum_t returned_pages[MAX_RANGE_SIZE];
  int returned_indices[MAX_RANGE_SIZE];

  if (key_end - key_start > MAX_RANGE_SIZE) {
    return FAILURE;
  }

  int num_found = find_range(key_start, key_end, returned_keys, returned_pages,
                             returned_indices);

  if (!num_found) {
    printf("not found.\n");
  } else {
    printf("found %d records in range [%" PRId64 ", %" PRId64 "]:\n", num_found,
           key_start, key_end);

    for (i = 0; i < num_found; i++) {
      page_t temp_buf;
      file_read_page(returned_pages[i], &temp_buf);
      leaf_page_t *temp_leaf = (leaf_page_t *)&temp_buf;

      int64_t key = returned_keys[i];
      int index = returned_indices[i];

      char *value_ptr = temp_leaf->records[index].value;

      printf("Key: %" PRId64 "  Location: page %" PRId64
             ", index %d  Value: %s\n",
             key, returned_pages[i], index, value_ptr);
    }
  }
  return SUCCESS;
}

/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
int find_range(int64_t key_start, int64_t key_end, int64_t returned_keys[],
               pagenum_t returned_pages[], int returned_indices[]) {

  int i = 0;
  int num_found = 0;
  page_t leaf_buf;
  leaf_page_t *leaf_page;

  pagenum_t current_leaf_num = find_leaf(key_start);

  if (current_leaf_num == PAGE_NULL) {
    return 0;
  }

  file_read_page(current_leaf_num, &leaf_buf);
  leaf_page = (leaf_page_t *)&leaf_buf;

  for (i = 0; i < leaf_page->num_of_keys; i++) {
    if (leaf_page->records[i].key >= key_start) {
      break;
    }
  }

  while (current_leaf_num != PAGE_NULL) {
    for (; i < leaf_page->num_of_keys; i++) {
      int64_t current_key = leaf_page->records[i].key;

      if (current_key > key_end) {
        return num_found;
      }

      returned_keys[num_found] = current_key;
      returned_pages[num_found] = current_leaf_num;
      returned_indices[num_found] = i;
      num_found++;
    }

    current_leaf_num = leaf_page->right_sibling_page_num;
    i = 0;

    if (current_leaf_num != PAGE_NULL) {
      file_read_page(current_leaf_num, &leaf_buf);
      leaf_page = (leaf_page_t *)&leaf_buf;
    }
  }

  return num_found;
}

/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 * This function finds the location where the key
 * should be, regardless of whether the key exists.
 */
pagenum_t find_leaf(int64_t key) {
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;

  pagenum_t cur_num = header_page->root_page_num;
  page_t page_buf;

  // leaf를 찾을때까지 계속해서 읽어나감
  while (true) {
    file_read_page(cur_num, &page_buf);
    page_header_t *page_header = (page_header_t *)&page_buf;
    uint32_t is_leaf = page_header->is_leaf;

    if (is_leaf == LEAF) {
      return cur_num;
    }

    int index = 0;
    internal_page_t *internal_page = (internal_page_t *)&page_buf;
    while (index < internal_page->num_of_keys &&
           key >= internal_page->entries[index].key) {
      index++;
    }

    if (index == 0) {
      cur_num = internal_page->one_more_page_num;
    } else {
      cur_num = internal_page->entries[index - 1].page_num;
    }
    if (cur_num == PAGE_NULL) {
      // 이거는 실행 안되어야 함
      perror("find_leaf");
      return PAGE_NULL;
    }
  }
}

/* Finds and returns success(0) or fail(1)
 */
int find(int64_t key, char *result_buf) {
  pagenum_t leaf_num = find_leaf(key);
  if (leaf_num == PAGE_NULL) {
    return FAILURE;
  }

  // leaf_page 에서 키에 해당하는 값 찾기
  page_t tmp_page;
  file_read_page(leaf_num, &tmp_page);
  leaf_page_t *leaf_page = (leaf_page_t *)&tmp_page;

  int index = 0;

  for (index = 0; index < leaf_page->num_of_keys; index++) {
    if (leaf_page->records[index].key == key) {
      break;
    }
  }
  // 해당하는 키를 찾았으면
  if (index != leaf_page->num_of_keys) {
    copy_value(result_buf, leaf_page->records[index].value, VALUE_SIZE);
    return SUCCESS;
  }

  return FAILURE;
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut(int length) {
  if (length % 2 == 0)
    return length / 2;
  else
    return length / 2 + 1;
}

// INSERTION

void copy_value(char *dest, const char *src, size_t size) {
  strncpy(dest, src, size - 1);
  dest[size - 1] = '\0';
}

void init_leaf_page(page_t *page) {
  leaf_page_t *leaf_page = (leaf_page_t *)page;
  leaf_page->parent_page_num = PAGE_NULL;
  leaf_page->is_leaf = LEAF;
  leaf_page->num_of_keys = 0;
  leaf_page->right_sibling_page_num = PAGE_NULL;
}

void init_internal_page(page_t *page) {
  internal_page_t *internal_page = (internal_page_t *)page;
  internal_page->parent_page_num = PAGE_NULL;
  internal_page->is_leaf = INTERNAL;
  internal_page->num_of_keys = 0;
  internal_page->one_more_page_num = PAGE_NULL;
}

/* Creates a new general node, which can be adapted
 * to serve as either a leaf or an internal node.
 */
pagenum_t make_node(uint32_t isleaf) {
  pagenum_t new_page_num;
  new_page_num = file_alloc_page();
  if (new_page_num == PAGE_NULL) {
    perror("Node creation.");
    exit(EXIT_FAILURE);
  }
  page_t page;
  memset(&page, 0, PAGE_SIZE);
  switch (isleaf) {
  case LEAF:
    init_leaf_page(&page);
    break;
  case INTERNAL:
    init_internal_page(&page);
    break;
  default:
    perror("make_node");
    exit(EXIT_FAILURE);
    break;
  }
  file_write_page(new_page_num, &page);

  return new_page_num;
}

// /* Creates a new leaf by creating a node
//  * and then adapting it appropriately.
//  */
pagenum_t make_leaf(void) { return make_node(LEAF); }

/* Finds the index within the parent's 'entries' array
 * where the new key should be inserted, based on the position
 * of the left child node (left_num).
 */
int get_index_after_left_child(page_t *parent_buffer, pagenum_t left_num) {
  internal_page_t *parent = (internal_page_t *)parent_buffer;
  // left_num이 leftmost인 경우 entries[0]
  if (parent->one_more_page_num == left_num) {
    return 0;
  }

  // left_num이 entries[index].page_num인 경우 entries[index+1]
  int index = 0;
  for (index = 0; index < parent->num_of_keys; index++) {
    if (parent->entries[index].page_num == left_num) {
      return index + 1;
    }
  }

  // 못찾으면 에러
  perror("get_left_index");
  return index;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 */
int insert_into_leaf(pagenum_t leaf_num, page_t *leaf_buffer, int64_t key,
                     char *value) {
  int index, insertion_point;
  leaf_page_t *leaf = (leaf_page_t *)leaf_buffer;

  insertion_point = 0;
  while (insertion_point < leaf->num_of_keys &&
         leaf->records[insertion_point].key < key) {
    insertion_point++;
  }
  for (index = leaf->num_of_keys; index > insertion_point; index--) {
    leaf->records[index] = leaf->records[index - 1];
  }

  leaf->records[insertion_point].key = key;
  copy_value(leaf->records[insertion_point].value, value, VALUE_SIZE);
  leaf->num_of_keys++;

  file_write_page(leaf_num, (page_t *)leaf);
  return SUCCESS;
}

/**
 * helper function for insert_into_leaf_after_splitting
 * Create a temporary array by combining the existing record and the new record
 * and return it
 */
record_t *prepare_records_for_split(leaf_page_t *leaf_page, int64_t key,
                                    const char *value) {

  record_t *temp_records = (record_t *)malloc(RECORD_CNT * sizeof(record_t));
  if (temp_records == NULL) {
    perror("Memory allocation for temporary records failed.");
    exit(EXIT_FAILURE);
  }

  int insertion_index = 0;
  while (insertion_index < RECORD_CNT &&
         leaf_page->records[insertion_index].key < key) {
    insertion_index++;
  }

  int i, j;
  for (i = 0, j = 0; i < leaf_page->num_of_keys; i++, j++) {
    if (j == insertion_index) {
      j++;
    }
    temp_records[j] = leaf_page->records[i];
  }

  // insert new record
  temp_records[insertion_index].key = key;
  copy_value(temp_records[insertion_index].value, value, VALUE_SIZE);

  return temp_records;
}

/**
 * helper function for insert_into_leaf_after_splitting
 * Distributes records in the temporary array to old_leaf and new_leaf and
 * returns k_prime
 */
int64_t distribute_records_to_leaves(leaf_page_t *leaf_page,
                                     leaf_page_t *new_leaf_page,
                                     record_t *temp_records,
                                     pagenum_t new_leaf_num) {

  const int split = cut(RECORD_CNT);

  int i, j;

  // Allocate to old_leaf_page until split point
  leaf_page->num_of_keys = 0;
  for (i = 0; i < split; i++) {
    leaf_page->records[i] = temp_records[i];
    leaf_page->num_of_keys++;
  }
  for (int k = split; k < RECORD_CNT; k++) {
    memset(&(leaf_page->records[k]), 0, sizeof(record_t));
  }

  // Records after the split point are allocated to new_leaf_page
  new_leaf_page->num_of_keys = 0;
  for (j = 0; i < LEAF_ORDER; i++, j++) {
    new_leaf_page->records[j] = temp_records[i];
    new_leaf_page->num_of_keys++;
  }
  for (int k = new_leaf_page->num_of_keys; k < RECORD_CNT; k++) {
    memset(&(new_leaf_page->records[k]), 0, sizeof(record_t));
  }

  // Connect sibling nodes and set parent nodes
  new_leaf_page->right_sibling_page_num = leaf_page->right_sibling_page_num;
  leaf_page->right_sibling_page_num = new_leaf_num;
  new_leaf_page->parent_page_num = leaf_page->parent_page_num;

  return new_leaf_page->records[0].key;
}

/**
 * Splits a node into two by inserting a new key and record into the leaf and
 * passing the split information to the parent
 */
int insert_into_leaf_after_splitting(pagenum_t leaf_num, int64_t key,
                                     char *value) {
  pagenum_t new_leaf_num;
  int64_t new_key;
  record_t *temp_records;

  new_leaf_num = make_leaf();

  page_t tmp_old_page;
  file_read_page(leaf_num, &tmp_old_page);
  leaf_page_t *leaf_page = (leaf_page_t *)&tmp_old_page;

  temp_records = prepare_records_for_split(leaf_page, key, value);

  page_t tmp_new_page;
  file_read_page(new_leaf_num, &tmp_new_page);
  leaf_page_t *new_leaf_page = (leaf_page_t *)&tmp_new_page;

  new_key = distribute_records_to_leaves(leaf_page, new_leaf_page, temp_records,
                                         new_leaf_num);

  free(temp_records);

  file_write_page(leaf_num, (page_t *)leaf_page);
  file_write_page(new_leaf_num, (page_t *)new_leaf_page);

  return insert_into_parent(leaf_num, new_key, new_leaf_num);
}

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
int insert_into_node(pagenum_t page_num, int64_t left_index, int64_t key,
                     pagenum_t right) {
  int index;

  page_t tmp_page;
  file_read_page(page_num, &tmp_page);
  internal_page_t *page = (internal_page_t *)&tmp_page;

  for (index = page->num_of_keys; index > left_index; index--) {
    page->entries[index] = page->entries[index - 1];
  }
  page->entries[left_index].page_num = right;
  page->entries[left_index].key = key;
  page->num_of_keys++;

  file_write_page(page_num, (page_t *)page);
  return SUCCESS;
}

/**
 * helper function for insert_into_node_after_splitting
 * Returns a temporary array created by combining the existing entries and the
 * new entries
 */
entry_t *prepare_entries_for_split(internal_page_t *old_node_page,
                                   int64_t left_index, int64_t key,
                                   pagenum_t right) {

  entry_t *temp_entries = (entry_t *)malloc((ENTRY_CNT) * sizeof(entry_t));
  if (temp_entries == NULL) {
    perror("Temporary entries array.");
    exit(EXIT_FAILURE);
  }

  int i;
  for (i = 0; i < old_node_page->num_of_keys; i++) {
    temp_entries[i] = old_node_page->entries[i];
  }

  // insert new entry
  for (i = old_node_page->num_of_keys; i > left_index; i--) {
    temp_entries[i] = temp_entries[i - 1];
  }
  temp_entries[left_index].key = key;
  temp_entries[left_index].page_num = right;

  return temp_entries;
}

/**
 * helper function for insert_into_node_after_splitting
 * Distribute temp_entries to old_node and new_node, and return k_prime
 */
int64_t distribute_entries_and_update_children(pagenum_t old_node_num,
                                               internal_page_t *old_node_page,
                                               pagenum_t new_node_num,
                                               internal_page_t *new_node_page,
                                               entry_t *temp_entries) {

  const int split = cut(INTERNAL_ORDER);
  int i, j;

  // key to send to parents
  const int64_t k_prime = temp_entries[split - 1].key;

  // Reassign entries to Old Node
  old_node_page->num_of_keys = 0;
  for (i = 0; i < split - 1; i++) {
    old_node_page->entries[i] = temp_entries[i];
    old_node_page->num_of_keys++;
  }
  for (int k = split - 1; k < ENTRY_CNT; k++) {
    memset(&(old_node_page->entries[k]), 0, sizeof(entry_t));
  }

  // Set the P0 pointer of the new node (the right pointer of k_prime)
  pagenum_t new_node_p0 = temp_entries[split - 1].page_num;
  new_node_page->one_more_page_num = new_node_p0;

  // Assigning entries to new nodes
  new_node_page->num_of_keys = 0;
  for (i = split, j = 0; i < INTERNAL_ORDER; i++, j++) {
    new_node_page->entries[j] = temp_entries[i];
    new_node_page->num_of_keys++;
  }
  for (int k = new_node_page->num_of_keys; k < ENTRY_CNT; k++) {
    memset(&(new_node_page->entries[k]), 0, sizeof(entry_t));
  }

  new_node_page->parent_page_num = old_node_page->parent_page_num;

  // Update the parent of a child node
  pagenum_t child = new_node_page->one_more_page_num;
  page_t tmp_child_page;
  if (child != PAGE_NULL) {
    file_read_page(child, &tmp_child_page);
    page_header_t *child_page_header = (page_header_t *)&tmp_child_page;
    child_page_header->parent_page_num = new_node_num;
    file_write_page(child, (page_t *)child_page_header);
  }
  for (i = 0; i < new_node_page->num_of_keys; i++) {
    child = new_node_page->entries[i].page_num;
    if (child != PAGE_NULL) {
      file_read_page(child, &tmp_child_page);
      page_header_t *child_page_header = (page_header_t *)&tmp_child_page;
      child_page_header->parent_page_num = new_node_num;
      file_write_page(child, (page_t *)child_page_header);
    }
  }

  return k_prime;
}

/**
 * Splits a node into two by inserting a new key and pointer into the internal
 * node and passes the split information to the parent
 */
int insert_into_node_after_splitting(pagenum_t old_node, int64_t left_index,
                                     int64_t key, pagenum_t right) {

  pagenum_t new_node_num;
  int64_t k_prime;
  entry_t *temp_entries;

  page_t tmp_old_page;
  file_read_page(old_node, &tmp_old_page);
  internal_page_t *old_node_page = (internal_page_t *)&tmp_old_page;

  temp_entries =
      prepare_entries_for_split(old_node_page, left_index, key, right);

  new_node_num = make_node(INTERNAL);
  page_t tmp_new_page;
  file_read_page(new_node_num, &tmp_new_page);
  internal_page_t *new_node_page = (internal_page_t *)&tmp_new_page;

  k_prime = distribute_entries_and_update_children(
      old_node, old_node_page, new_node_num, new_node_page, temp_entries);

  free(temp_entries);

  file_write_page(old_node, (page_t *)old_node_page);
  file_write_page(new_node_num, (page_t *)new_node_page);

  return insert_into_parent(old_node, k_prime, new_node_num);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
int insert_into_parent(pagenum_t left, int64_t key, pagenum_t right) {
  int left_index;
  pagenum_t parent;

  page_t tmp_left_page;
  file_read_page(left, &tmp_left_page);
  page_header_t *left_page_header = (page_header_t *)&tmp_left_page;

  parent = left_page_header->parent_page_num;

  /* Case: new root. */
  if (parent == PAGE_NULL) {
    return insert_into_new_root(left, key, right);
  }

  /* Case: leaf or node. (Remainder of
   * function body.)
   */
  page_t tmp_parent_page;
  file_read_page(parent, &tmp_parent_page);
  page_header_t *parent_page_header = (page_header_t *)&tmp_parent_page;

  /* Find the parent's pointer to the left
   * node.
   */
  left_index = get_index_after_left_child(&tmp_parent_page, left);

  /* Simple case: the new key fits into the node.
   */
  if (parent_page_header->num_of_keys < INTERNAL_ORDER - 1) {
    return insert_into_node(parent, left_index, key, right);
  }

  /* Harder case:  split a node in order
   * to preserve the B+ tree properties.
   */
  return insert_into_node_after_splitting(parent, left_index, key, right);
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
int insert_into_new_root(pagenum_t left, int64_t key, pagenum_t right) {
  pagenum_t root = make_node(INTERNAL);

  // root 처리
  page_t tmp_root_page;
  file_read_page(root, &tmp_root_page);
  internal_page_t *root_page = (internal_page_t *)&tmp_root_page;

  root_page->one_more_page_num = left;
  root_page->entries[0].key = key;
  root_page->entries[0].page_num = right;
  root_page->num_of_keys = 1;
  root_page->parent_page_num = PAGE_NULL;

  file_write_page(root, (page_t *)root_page);

  // left right 처리
  page_t tmp_left_page;
  file_read_page(left, &tmp_left_page);
  page_header_t *left_header = (page_header_t *)&tmp_left_page;
  left_header->parent_page_num = root;
  file_write_page(left, (page_t *)left_header);

  page_t tmp_right_page;
  file_read_page(right, &tmp_right_page);
  page_header_t *right_header = (page_header_t *)&tmp_right_page;
  right_header->parent_page_num = root;
  file_write_page(right, (page_t *)right_header);

  // 헤더 페이지 갱신
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;
  header_page->root_page_num = root;
  file_write_page(HEADER_PAGE_POS, (page_t *)header_page);

  return SUCCESS;
}

/* First insertion:
 * start a new tree.
 */
int start_new_tree(int64_t key, char *value) {
  // make root page
  pagenum_t root = make_node(LEAF);
  page_t tmp_root_page;
  file_read_page(root, &tmp_root_page);
  leaf_page_t *root_page = (leaf_page_t *)&tmp_root_page;

  root_page->parent_page_num = PAGE_NULL;
  root_page->is_leaf = LEAF;
  root_page->num_of_keys = 1;
  root_page->right_sibling_page_num = PAGE_NULL;
  root_page->records[0].key = key;
  copy_value(root_page->records[0].value, value, VALUE_SIZE);

  link_header_page(root);

  file_write_page(root, (page_t *)root_page);
  return SUCCESS;
}

/**
 * @brief init header page
 * must be used before insert
 */
void init_header_page() {
  page_t header_buf;
  memset(&header_buf, 0, PAGE_SIZE);
  header_page_t *header_page = (header_page_t *)&header_buf;
  header_page->num_of_pages = HEADER_PAGE_POS + 1;

  file_write_page(HEADER_PAGE_POS, (page_t *)header_page);
}

void link_header_page(pagenum_t root) {
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;
  header_page->root_page_num = root;

  file_write_page(HEADER_PAGE_POS, (page_t *)header_page);
}

/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
int insert(int64_t key, char *value) {
  pagenum_t leaf;

  /* The current implementation ignores
   * duplicates.
   */

  char result_buf[VALUE_SIZE];
  if (find(key, result_buf) == SUCCESS) {
    return FAILURE;
  }

  /* Case: the tree does not exist yet.
   * Start a new tree.
   */
  page_t header_page_buf;
  file_read_page(HEADER_PAGE_POS, &header_page_buf);
  header_page_t *h_page = (header_page_t *)&header_page_buf;

  pagenum_t root_num = h_page->root_page_num;
  if (root_num == PAGE_NULL) {
    return start_new_tree(key, value);
  }

  /* Case: the tree already exists.
   * (Rest of function body.)
   */
  leaf = find_leaf(key);

  /* Case: leaf has room for key and pointer.
   */
  page_t tmp_leaf_page;
  file_read_page(leaf, &tmp_leaf_page);
  leaf_page_t *leaf_page = (leaf_page_t *)&tmp_leaf_page;

  if (leaf_page->num_of_keys < RECORD_CNT) {
    return insert_into_leaf(leaf, &tmp_leaf_page, key, value);
  }

  /* Case:  leaf must be split.
   */
  return insert_into_leaf_after_splitting(leaf, key, value);
}

// DELETION.

/* Return the index of the key to the left
 * of the pointer in the parent pointing
 * to n. If not (the node is the leftmost child),
 * returns -1 to signify this special case.
 * If target_node has no parent, return -2 (CANNOT_ROOT)
 */
int get_kprime_index(pagenum_t target_node) {
  page_t target_buf, parent_buf;
  file_read_page(target_node, &target_buf);
  page_header_t *target_header = (page_header_t *)&target_buf;

  pagenum_t parent_num = target_header->parent_page_num;
  if (parent_num == PAGE_NULL) {
    return CANNOT_ROOT;
  }

  file_read_page(parent_num, &parent_buf);
  internal_page_t *parent_page = (internal_page_t *)&parent_buf;

  // 왼쪽 형제가 없는 경우
  if (parent_page->one_more_page_num == target_node) {
    return -1;
  }

  for (int index = 0; index < parent_page->num_of_keys; index++) {
    if (parent_page->entries[index].page_num == target_node) {
      return index;
    }
  }

  // Error state.
  printf("Search for nonexistent pointer to node in parent.\n");
  printf("Node:  %#lx\n", (unsigned long)target_node);
  exit(EXIT_FAILURE);
}

pagenum_t adjust_root(pagenum_t root) {
  page_t root_buf;
  file_read_page(root, &root_buf);
  page_header_t *root_header = (page_header_t *)&root_buf;

  /* Case: nonempty root.
   * Key and pointer have already been deleted,
   * so nothing to be done.
   */

  if (root_header->num_of_keys > 0) {
    return root;
  }

  /* Case: empty root.
   */

  // If it has a child, promote
  // the first (only) child
  // as the new root.
  pagenum_t new_root;
  internal_page_t *root_internal = (internal_page_t *)&root_buf;
  if (root_header->is_leaf == INTERNAL) {
    new_root = root_internal->one_more_page_num;

    if (new_root != PAGE_NULL) {
      page_t new_root_buf;
      file_read_page(new_root, &new_root_buf);
      page_header_t *new_root_header = (page_header_t *)&new_root_buf;

      new_root_header->parent_page_num = PAGE_NULL;
      file_write_page(new_root, &new_root_buf);
    }
  } else {
    new_root = PAGE_NULL;
  }

  file_free_page(root);

  // update header
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;
  header_page->root_page_num = new_root;
  file_write_page(HEADER_PAGE_POS, &header_buf);

  return new_root;
}

/**
 * helper function for coalesce nodes
 * @brief Handles the merging logic of internal nodes
 * Insert k_prime, copy target's entry, and update the child's parent pointer
 */
void coalesce_internal_nodes(page_t *neighbor_buf, page_t *target_buf,
                             int neighbor_num, int64_t k_prime) {
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;
  internal_page_t *neighbor_internal = (internal_page_t *)neighbor_buf;
  internal_page_t *target_internal = (internal_page_t *)target_buf;

  int neighbor_insertion_index = neighbor_header->num_of_keys;

  // Append k_prime and the target's one_more_page_num pointer
  neighbor_internal->entries[neighbor_insertion_index].key = k_prime;
  neighbor_internal->entries[neighbor_insertion_index].page_num =
      target_internal->one_more_page_num;
  neighbor_header->num_of_keys++;

  // Append all pointers and keys from target (excluding target's
  // one_more_page_num
  for (int i = neighbor_insertion_index + 1, j = 0;
       j < target_internal->num_of_keys; i++, j++) {
    neighbor_internal->entries[i] = target_internal->entries[j];
    neighbor_header->num_of_keys++;
  }
  target_internal->num_of_keys = 0;

  // Update parent pointers for all children copied from target
  pagenum_t child_num =
      neighbor_internal->entries[neighbor_insertion_index].page_num;
  if (child_num != PAGE_NULL) {
    page_t child_buf;
    file_read_page(child_num, &child_buf);
    ((page_header_t *)&child_buf)->parent_page_num = neighbor_num;
    file_write_page(child_num, &child_buf);
  }
  for (int i = neighbor_insertion_index + 1; i < neighbor_header->num_of_keys;
       i++) {
    child_num = neighbor_internal->entries[i].page_num;
    if (child_num != PAGE_NULL) {
      page_t child_buf;
      file_read_page(child_num, &child_buf);
      ((page_header_t *)&child_buf)->parent_page_num = neighbor_num;
      file_write_page(child_num, &child_buf);
    }
  }
}

/**
 * helper function for coalesce nodes
 * @brief Handles the merging logic of leaf nodes
 * Copy records from target and update right_sibling_page_num
 */
void coalesce_leaf_nodes(page_t *neighbor_buf, page_t *target_buf) {
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;
  leaf_page_t *neighbor_leaf = (leaf_page_t *)neighbor_buf;
  leaf_page_t *target_leaf = (leaf_page_t *)target_buf;

  int neighbor_insertion_index = neighbor_header->num_of_keys;

  // Append all records from target to neighbor
  for (int i = neighbor_insertion_index, j = 0; j < target_leaf->num_of_keys;
       i++, j++) {
    neighbor_leaf->records[i] = target_leaf->records[j];
    neighbor_header->num_of_keys++;
  }

  // Update neighbor's right sibling pointer
  neighbor_leaf->right_sibling_page_num = target_leaf->right_sibling_page_num;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int coalesce_nodes(pagenum_t target_num, pagenum_t neighbor_num,
                   int kprime_index_from_get, int64_t k_prime) {
  // Swap neighbor with target if target is on the extreme left
  if (kprime_index_from_get == -1) {
    pagenum_t tmp_num = target_num;
    target_num = neighbor_num;
    neighbor_num = tmp_num;
  }

  page_t neighbor_buf, target_buf;
  file_read_page(neighbor_num, &neighbor_buf);
  file_read_page(target_num, &target_buf);

  page_header_t *neighbor_header = (page_header_t *)&neighbor_buf;
  page_header_t *target_header = (page_header_t *)&target_buf;

  pagenum_t parent_num = target_header->parent_page_num;

  if (target_header->is_leaf == INTERNAL) {
    coalesce_internal_nodes(&neighbor_buf, &target_buf, neighbor_num, k_prime);
  } else {
    coalesce_leaf_nodes(&neighbor_buf, &target_buf);
  }

  file_write_page(neighbor_num, (page_t *)&neighbor_buf);
  file_free_page(target_num);

  // Remove the separator key from the parent
  return delete_entry(parent_num, k_prime, NULL);
}

/**
 * helper function for redistribute nodes
 * @brief Redistributes entries from the left neighbor node to the target node
 */
void redistribute_from_left(pagenum_t target_num, page_t *target_buf,
                            page_t *neighbor_buf, internal_page_t *parent_page,
                            int k_prime_index, int k_prime) {
  page_header_t *target_header = (page_header_t *)target_buf;

  if (target_header->is_leaf == INTERNAL) {
    redistribute_internal_from_left(target_num, target_buf, neighbor_buf,
                                    parent_page, k_prime_index, k_prime);
  } else {
    redistribute_leaf_from_left(target_buf, neighbor_buf, parent_page,
                                k_prime_index);
  }
}

/**
 * helper function for redistribute nodes
 * @brief Move the last entry of the left neighbor node from the internal node
 * to the first * position of the target node
 */
void redistribute_internal_from_left(pagenum_t target_num, page_t *target_buf,
                                     page_t *neighbor_buf,
                                     internal_page_t *parent_page,
                                     int k_prime_index, int k_prime) {
  page_header_t *target_header = (page_header_t *)target_buf;
  internal_page_t *target_internal = (internal_page_t *)target_buf;
  internal_page_t *neighbor_internal = (internal_page_t *)neighbor_buf;
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;

  for (int index = target_header->num_of_keys; index > 0; index--) {
    target_internal->entries[index] = target_internal->entries[index - 1];
  }
  target_internal->entries[0].page_num = target_internal->one_more_page_num;

  target_internal->entries[0].key = k_prime;

  pagenum_t last_num_neighbor =
      neighbor_internal->entries[neighbor_header->num_of_keys - 1].page_num;
  target_internal->one_more_page_num = last_num_neighbor;

  if (last_num_neighbor != PAGE_NULL) {
    page_t child_buf;
    file_read_page(last_num_neighbor, &child_buf);
    ((page_header_t *)&child_buf)->parent_page_num = target_num;
    file_write_page(last_num_neighbor, &child_buf);
  }

  parent_page->entries[k_prime_index].key =
      neighbor_internal->entries[neighbor_header->num_of_keys - 1].key;

  memset(&neighbor_internal->entries[neighbor_header->num_of_keys - 1], 0,
         sizeof(entry_t));
}

/**
 * helper function for redistribute nodes
 * @brief Move the last record of the left neighboring node from the leaf node
 * to the first position of the target node
 */
void redistribute_leaf_from_left(page_t *target_buf, page_t *neighbor_buf,
                                 internal_page_t *parent_page,
                                 int k_prime_index) {
  page_header_t *target_header = (page_header_t *)target_buf;
  leaf_page_t *target_leaf = (leaf_page_t *)target_buf;
  leaf_page_t *neighbor_leaf = (leaf_page_t *)neighbor_buf;
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;

  for (int i = target_header->num_of_keys; i > 0; i--) {
    target_leaf->records[i] = target_leaf->records[i - 1];
  }

  target_leaf->records[0] =
      neighbor_leaf->records[neighbor_header->num_of_keys - 1];

  parent_page->entries[k_prime_index].key = target_leaf->records[0].key;

  memset(&neighbor_leaf->records[neighbor_header->num_of_keys - 1], 0,
         sizeof(record_t));
}

/**
 * helper function for redistribute nodes
 * @brief Redistributes entries from the right neighbor node to the target node
 */
void redistribute_from_right(pagenum_t target_num, page_t *target_buf,
                             page_t *neighbor_buf, internal_page_t *parent_page,
                             int k_prime_index, int k_prime) {
  page_header_t *target_header = (page_header_t *)target_buf;

  if (target_header->is_leaf == INTERNAL) {
    redistribute_internal_from_right(target_num, target_buf, neighbor_buf,
                                     parent_page, k_prime_index, k_prime);
  } else {
    redistribute_leaf_from_right(target_buf, neighbor_buf, parent_page,
                                 k_prime_index);
  }
}

/**
 * helper function for redistribute nodes
 * @brief Move the first entry of the right neighbor node from the internal node
 * to the last position of the target node
 */
void redistribute_internal_from_right(pagenum_t target_num, page_t *target_buf,
                                      page_t *neighbor_buf,
                                      internal_page_t *parent_page,
                                      int k_prime_index, int k_prime) {
  page_header_t *target_header = (page_header_t *)target_buf;
  internal_page_t *target_internal = (internal_page_t *)target_buf;
  internal_page_t *neighbor_internal = (internal_page_t *)neighbor_buf;
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;

  target_internal->entries[target_header->num_of_keys].key = k_prime;

  pagenum_t num_from_neighbor = neighbor_internal->one_more_page_num;
  target_internal->entries[target_header->num_of_keys].page_num =
      num_from_neighbor;

  if (num_from_neighbor != PAGE_NULL) {
    page_t child_buf;
    file_read_page(num_from_neighbor, &child_buf);
    ((page_header_t *)&child_buf)->parent_page_num = target_num;
    file_write_page(num_from_neighbor, &child_buf);
  }

  parent_page->entries[k_prime_index].key = neighbor_internal->entries[0].key;

  neighbor_internal->one_more_page_num = neighbor_internal->entries[0].page_num;
  for (int i = 0; i < neighbor_header->num_of_keys - 1; i++) {
    neighbor_internal->entries[i] = neighbor_internal->entries[i + 1];
  }

  memset(&neighbor_internal->entries[neighbor_header->num_of_keys - 1], 0,
         sizeof(entry_t));
}

/**
 * helper function for redistribute nodes
 * @brief Move the first record of the right neighboring node from the leaf node
 * to the last position of the target node
 */
void redistribute_leaf_from_right(page_t *target_buf, page_t *neighbor_buf,
                                  internal_page_t *parent_page,
                                  int k_prime_index) {
  page_header_t *target_header = (page_header_t *)target_buf;
  leaf_page_t *target_leaf = (leaf_page_t *)target_buf;
  leaf_page_t *neighbor_leaf = (leaf_page_t *)neighbor_buf;
  page_header_t *neighbor_header = (page_header_t *)neighbor_buf;

  target_leaf->records[target_header->num_of_keys] = neighbor_leaf->records[0];

  parent_page->entries[k_prime_index].key = neighbor_leaf->records[1].key;

  for (int i = 0; i < neighbor_header->num_of_keys - 1; i++) {
    neighbor_leaf->records[i] = neighbor_leaf->records[i + 1];
  }

  memset(&neighbor_leaf->records[neighbor_header->num_of_keys - 1], 0,
         sizeof(record_t));
}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
int redistribute_nodes(pagenum_t target_num, pagenum_t neighbor_num,
                       int kprime_index_from_get, int k_prime_index,
                       int k_prime) {
  page_t target_buf, neighbor_buf, parent_buf;
  file_read_page(target_num, &target_buf);
  file_read_page(neighbor_num, &neighbor_buf);

  page_header_t *target_header = (page_header_t *)&target_buf;
  page_header_t *neighbor_header = (page_header_t *)&neighbor_buf;
  pagenum_t parent_num = target_header->parent_page_num;

  file_read_page(parent_num, &parent_buf);
  internal_page_t *parent_page = (internal_page_t *)&parent_buf;

  /// target is not leftmost, so neighbor is to the left
  if (kprime_index_from_get != -1) {
    redistribute_from_left(target_num, &target_buf, &neighbor_buf, parent_page,
                           k_prime_index, k_prime);
  }
  // target is leftmost, so neighbor is to the right
  else {
    redistribute_from_right(target_num, &target_buf, &neighbor_buf, parent_page,
                            k_prime_index, k_prime);
  }

  // Update key counts and write back pages
  target_header->num_of_keys++;
  neighbor_header->num_of_keys--;

  file_write_page(target_num, &target_buf);
  file_write_page(neighbor_num, &neighbor_buf);
  file_write_page(parent_num, &parent_buf);

  return SUCCESS;
}

/**
 * @brief remove record from leaf node, if success return SUCCESS(0) else
 * FAILURE(-1)
 */
int remove_record_from_node(leaf_page_t *target_page, int64_t key,
                            char *value) {
  // Remove the record and shift other records accordingly.
  int index = 0;
  while (target_page->records[index].key != key) {
    index++;
  }
  if (index == target_page->num_of_keys) {
    return FAILURE;
  }

  for (++index; index < target_page->num_of_keys; index++) {
    target_page->records[index - 1] = target_page->records[index];
  }
  // One key fewer.
  target_page->num_of_keys--;

  memset(&target_page->records[target_page->num_of_keys], 0, sizeof(record_t));

  return SUCCESS;
}

/**
 * @brief remove entry from internal node, if success return SUCCESS(0) else
 * FAILURE(-1)
 */
int remove_entry_from_node(internal_page_t *target_page, int64_t key) {
  // Remove the key and shift other keys accordingly.
  int index = 0;
  while (target_page->entries[index].key != key) {
    index++;
  }
  if (index == target_page->num_of_keys) {
    return FAILURE;
  }

  for (++index; index < target_page->num_of_keys; index++) {
    target_page->entries[index - 1] = target_page->entries[index];
  }
  // One key fewer.
  target_page->num_of_keys--;

  memset(&target_page->entries[target_page->num_of_keys], 0, sizeof(entry_t));

  return SUCCESS;
}

/**
 * helper function for delete entry
 * @brief Find the distinguishing key information of neighboring nodes and
 * parents to handle underflow
 */
int find_neighbor_and_kprime(pagenum_t target_node,
                             internal_page_t *parent_page,
                             page_header_t *target_header,
                             pagenum_t *neighbor_num_out,
                             int *k_prime_key_index_out) {

  int kprime_index_from_get = get_kprime_index(target_node);

  if (kprime_index_from_get == -1) {
    // target is P0 neighbor P1
    *neighbor_num_out = parent_page->entries[0].page_num;
    *k_prime_key_index_out = 0;
  } else {
    // target is Pi neighbor Pi-1.
    int target_pointer_index =
        kprime_index_from_get; // Index of the pointer to target

    *k_prime_key_index_out = target_pointer_index;

    if (target_pointer_index == 0) {
      // target is P1 (entries[0].page_num) neighbor P0
      *neighbor_num_out = parent_page->one_more_page_num;
    } else {
      // target is Pi+1 (entries[i].page_num, i > 0) neighbor is Pi
      *neighbor_num_out =
          parent_page->entries[target_pointer_index - 1].page_num;
    }
  }
  return kprime_index_from_get;
}

/**
 * helper function for delete entry
 * @brief Handles node underflow.
 * Finds neighboring nodes and decides whether to merge or redistribute them and
 * call
 */
int handle_underflow(pagenum_t target_node) {
  page_t node_buf, parent_buf;
  file_read_page(target_node, &node_buf);
  page_header_t *node_header = (page_header_t *)&node_buf;
  pagenum_t parent_num = node_header->parent_page_num;

  file_read_page(parent_num, &parent_buf);
  internal_page_t *parent_page = (internal_page_t *)&parent_buf;

  pagenum_t neighbor_num;
  int k_prime_key_index;

  int kprime_index_from_get = find_neighbor_and_kprime(
      target_node, parent_page, node_header, &neighbor_num, &k_prime_key_index);

  int64_t k_prime = parent_page->entries[k_prime_key_index].key;

  page_t neighbor_buf;
  file_read_page(neighbor_num, &neighbor_buf);
  page_header_t *neighbor_header = (page_header_t *)&neighbor_buf;

  int capacity = node_header->is_leaf ? RECORD_CNT : ENTRY_CNT - 1;

  if (neighbor_header->num_of_keys + node_header->num_of_keys < capacity) {
    return coalesce_nodes(target_node, neighbor_num, kprime_index_from_get,
                          k_prime);
  } else {
    return redistribute_nodes(target_node, neighbor_num, kprime_index_from_get,
                              k_prime_key_index, k_prime);
  }
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
int delete_entry(pagenum_t target_node, int64_t key, char *value) {
  page_t node_buf;
  file_read_page(target_node, &node_buf);
  page_header_t *node_header = (page_header_t *)&node_buf;

  // Case: Remove key and pointer from node
  int remove_result = FAILURE;
  switch (node_header->is_leaf) {
  case LEAF:
    remove_result =
        remove_record_from_node((leaf_page_t *)&node_buf, key, value);
    break;
  case INTERNAL:
    remove_result = remove_entry_from_node((internal_page_t *)&node_buf, key);
    break;
  default:
    perror("delete_entry error: Unknown node type");
    return FAILURE;
  }

  if (remove_result != SUCCESS) {
    return FAILURE;
  }
  file_write_page(target_node, (page_t *)&node_buf);

  // Case: Deletion from the root
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;

  if (target_node == header_page->root_page_num) {
    return adjust_root(header_page->root_page_num);
  }

  // Case: Node stays at or above minimum. (The simple case)
  if (node_header->num_of_keys >= MIN_KEYS) {
    return SUCCESS;
  }

  // Case: Node falls below minimum (underflow)
  return handle_underflow(target_node);
}

/* Master deletion function.
 */
int delete (int64_t key) {
  pagenum_t leaf;

  char value_buf[VALUE_SIZE];
  // if not exists fail
  if (find(key, value_buf) != SUCCESS) {
    return FAILURE;
  }

  leaf = find_leaf(key);

  if (leaf != PAGE_NULL) {
    return delete_entry(leaf, key, value_buf);
  }
  return FAILURE;
}

void destroy_tree_nodes(pagenum_t root) {
  if (root == PAGE_NULL) {
    return;
  }

  page_t page_buf;
  file_read_page(root, &page_buf);
  page_header_t *page_header = (page_header_t *)&page_buf;

  if (page_header->is_leaf == INTERNAL) {
    internal_page_t *internal_page = (internal_page_t *)&page_buf;

    destroy_tree_nodes(internal_page->one_more_page_num);
    for (int index = 0; index < internal_page->num_of_keys; index++) {
      destroy_tree_nodes(internal_page->entries[index].page_num);
    }
  }

  file_free_page(root);
}

void destroy_tree() {
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;

  pagenum_t root_num = header_page->root_page_num;
  if (root_num != PAGE_NULL) {
    destroy_tree_nodes(root_num);
  }

  header_page->root_page_num = PAGE_NULL;
  file_write_page(HEADER_PAGE_POS, (page_t *)&header_buf);
}
