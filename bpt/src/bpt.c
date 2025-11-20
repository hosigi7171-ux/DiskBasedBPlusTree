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
// GLOBALS.

/* The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
int order = DEFAULT_ORDER;

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
queue *q_head;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
bool verbose_output = false;

// FUNCTION DEFINITIONS.

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

/* First message to the user.
 */
void usage_1(void) {
  printf("B+ Tree of Order %d.\n", order);
  printf("Following Silberschatz, Korth, Sidarshan, Database Concepts, "
         "5th ed.\n\n"
         "To build a B+ tree of a different order, start again and enter "
         "the order\n"
         "as an integer argument:  bpt <order>  ");
  printf("(%d <= order <= %d).\n", MIN_ORDER, MAX_ORDER);
  printf("To start with input from a file of newline-delimited integers, \n"
         "start again and enter the order followed by the filename:\n"
         "bpt <order> <inputfile> .\n");
}

/* Second message to the user.
 */
void usage_2(void) {
  printf(
      "Enter any of the following commands after the prompt > :\n"
      "\ti <k>  -- Insert <k> (an integer) as both key and value).\n"
      "\tf <k>  -- Find the value under key <k>.\n"
      "\tp <k> -- Print the path from the root to key k and its associated "
      "value.\n"
      "\tr <k1> <k2> -- Print the keys and values found in the range "
      "[<k1>, <k2>\n"
      "\td <k>  -- Delete key <k> and its associated value.\n"
      "\tx -- Destroy the whole tree.  Start again with an empty tree of the "
      "same order.\n"
      "\tt -- Print the B+ tree.\n"
      "\tl -- Print the keys of the leaves (bottom row of the tree).\n"
      "\tv -- Toggle output of pointer addresses (\"verbose\") in tree and "
      "leaves.\n"
      "\tq -- Quit. (Or use Ctl-D.)\n"
      "\t? -- Print this help message.\n");
}

/* Brief usage note.
 */
void usage_3(void) {
  printf("Usage: ./bpt [<order>]\n");
  printf("\twhere %d <= order <= %d .\n", MIN_ORDER, MAX_ORDER);
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
void find_and_print_range(int64_t key_start, int64_t key_end) {
  int i;
  int64_t returned_keys[MAX_RANGE_SIZE];
  pagenum_t returned_pages[MAX_RANGE_SIZE];
  int returned_indices[MAX_RANGE_SIZE];

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

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
int insert_into_leaf_after_splitting(pagenum_t leaf_num, int64_t key,
                                     char *value) {
  pagenum_t new_leaf_num;
  int64_t new_key;
  record_t *temp_records;
  int insertion_index, split, i, j;

  new_leaf_num = make_leaf();

  temp_records = (record_t *)malloc(RECORD_CNT * sizeof(record_t));
  if (temp_records == NULL) {
    perror("Temporary records array.");
    exit(EXIT_FAILURE);
  }

  // leaf_page 읽기
  page_t tmp_page;
  file_read_page(leaf_num, &tmp_page);
  leaf_page_t *leaf_page = (leaf_page_t *)&tmp_page;

  // 삽입 위치 찾기
  insertion_index = 0;
  while (insertion_index < RECORD_CNT &&
         leaf_page->records[insertion_index].key < key) {
    insertion_index++;
  }

  // temp_records에 leaf_page의 레코드 내용 옮기기 + 삽입 내용 세팅
  for (i = 0, j = 0; i < leaf_page->num_of_keys; i++, j++) {
    if (j == insertion_index) {
      j++;
    }
    temp_records[j] = leaf_page->records[i];
  }
  temp_records[insertion_index].key = key;
  copy_value(temp_records[insertion_index].value, value, VALUE_SIZE);

  // 분할 지점 계산
  split = cut(RECORD_CNT);

  // 분할 지점까지 old_leaf_page에 할당
  leaf_page->num_of_keys = 0;
  for (i = 0; i < split; i++) {
    leaf_page->records[i] = temp_records[i];
    leaf_page->num_of_keys++;
  }
  for (i = split; i < RECORD_CNT; i++) {
    memset(&(leaf_page->records[i]), 0, sizeof(record_t));
  }

  // new_leaf_page 읽기
  page_t tmp_new_page;
  file_read_page(new_leaf_num, &tmp_new_page);
  leaf_page_t *new_leaf_page = (leaf_page_t *)&tmp_new_page;

  // 분할 지점 이후의 records는 new_leaf_page에 할당
  for (i = split, j = 0; i < LEAF_ORDER; i++, j++) {
    new_leaf_page->records[j] = temp_records[i];
    new_leaf_page->num_of_keys++;
  }
  for (i = new_leaf_page->num_of_keys; i < RECORD_CNT; i++) {
    memset(&(new_leaf_page->records[i]), 0, sizeof(record_t));
  }

  free(temp_records);
  // 형제 노드 연결 및 부모 노드 설정
  new_leaf_page->right_sibling_page_num = leaf_page->right_sibling_page_num;
  leaf_page->right_sibling_page_num = new_leaf_num;
  new_leaf_page->parent_page_num = leaf_page->parent_page_num;

  new_key = new_leaf_page->records[0].key;

  file_write_page(leaf_num, (page_t *)leaf_page);
  file_write_page(new_leaf_num, (page_t *)new_leaf_page);

  // 부모 페이지에 반영
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

/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
int insert_into_node_after_splitting(pagenum_t old_node, int64_t left_index,
                                     int64_t key, pagenum_t right) {
  int i, j, split;
  int64_t k_prime;
  pagenum_t new_node, child;
  entry_t *temp_entries;

  /* First create a temporary set of keys and pointers
   * to hold everything in order, including
   * the new key and pointer, inserted in their
   * correct places.
   * Then create a new node and copy half of the
   * keys and pointers to the old node and
   * the other half to the new.
   */

  // old_node 페이지 읽기
  page_t tmp_old_page;
  file_read_page(old_node, &tmp_old_page);
  internal_page_t *old_node_page = (internal_page_t *)&tmp_old_page;

  // temporary entry 할당
  temp_entries = (entry_t *)malloc((ENTRY_CNT) * sizeof(entry_t));
  if (temp_entries == NULL) {
    perror("Temporary entries array.");
    exit(EXIT_FAILURE);
  }

  // 기존 엔트리를 임시 배열에 복사
  for (i = 0; i < old_node_page->num_of_keys; i++) {
    temp_entries[i] = old_node_page->entries[i];
  }

  // 새로운 엔트리 삽입 위치 확보 및 삽입
  for (i = old_node_page->num_of_keys; i > left_index; i--) {
    temp_entries[i] = temp_entries[i - 1];
  }
  temp_entries[left_index].key = key;
  temp_entries[left_index].page_num = right;

  /* Create the new node and copy
   * half the keys and pointers to the
   * old and half to the new.
   */
  split = cut(INTERNAL_ORDER);
  new_node = make_node(INTERNAL);

  k_prime = temp_entries[split - 1].key;

  old_node_page->num_of_keys = 0;
  for (i = 0; i < split - 1; i++) {
    old_node_page->entries[i] = temp_entries[i];
    old_node_page->num_of_keys++;
  }
  for (i = split - 1; i < ENTRY_CNT; i++) {
    memset(&(old_node_page->entries[i]), 0, sizeof(entry_t));
  }

  // new_node 페이지 읽기
  page_t tmp_new_page;
  file_read_page(new_node, &tmp_new_page);
  internal_page_t *new_node_page = (internal_page_t *)&tmp_new_page;

  // New node의 P0가 될 포인터는
  // k_prime의 오른쪽 포인터
  pagenum_t new_node_p0 = temp_entries[split - 1].page_num;
  new_node_page->one_more_page_num = new_node_p0;

  for (i = split, j = 0; i < INTERNAL_ORDER; i++, j++) {
    new_node_page->entries[j] = temp_entries[i];
    new_node_page->num_of_keys++;
  }
  for (i = new_node_page->num_of_keys; i < ENTRY_CNT; i++) {
    memset(&(new_node_page->entries[i]), 0, sizeof(entry_t));
  }

  free(temp_entries);

  new_node_page->parent_page_num = old_node_page->parent_page_num;
  child = new_node_page->one_more_page_num;
  page_t tmp_child_page;
  if (child != PAGE_NULL) {
    file_read_page(child, &tmp_child_page);
    page_header_t *child_page_header = (page_header_t *)&tmp_child_page;
    child_page_header->parent_page_num = new_node;
    file_write_page(child, (page_t *)child_page_header);
  }

  // New node의 엔트리 자식 업데이트
  for (i = 0; i < new_node_page->num_of_keys; i++) {
    child = new_node_page->entries[i].page_num;
    if (child != PAGE_NULL) {
      file_read_page(child, &tmp_child_page);
      page_header_t *child_page_header = (page_header_t *)&tmp_child_page;
      child_page_header->parent_page_num = new_node;
      file_write_page(child, (page_t *)child_page_header);
    }
  }

  /* Insert a new key into the parent of the two
   * nodes resulting from the split, with
   * the old node to the left and the new to the right.
   */

  file_write_page(old_node, (page_t *)old_node_page);
  file_write_page(new_node, (page_t *)new_node_page);

  return insert_into_parent(old_node, k_prime, new_node);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
int insert_into_parent(pagenum_t left, int64_t key, pagenum_t right) {
  int left_index;
  pagenum_t parent;

  // left 페이지 읽기
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

  file_write_page(HEADER_PAGE_POS, (header_page_t *)header_page);
}

void link_header_page(pagenum_t root) {
  page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;
  header_page->root_page_num = root;

  file_write_page(HEADER_PAGE_POS, (header_page_t *)header_page);
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
  header_page_t header_page_buf;
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

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int coalesce_nodes(pagenum_t target_num, pagenum_t neighbor_num,
                   int kprime_index_from_get, int64_t k_prime) {

  /* Swap neighbor with node if node is on the
   * extreme left and neighbor is to its right.
   */
  int k_prime_index;
  if (kprime_index_from_get == -1) {
    pagenum_t tmp_num = target_num;
    target_num = neighbor_num;
    neighbor_num = tmp_num;
    k_prime_index = 0;
  } else {
    k_prime_index = kprime_index_from_get;
  }

  /* Starting point in the neighbor for copying
   * keys and pointers from target.
   * Recall that target and neighbor have swapped places
   * in the special case of target being a leftmost child.
   */

  page_t neighbor_buf, target_buf;
  pagenum_t parent_num;
  file_read_page(neighbor_num, &neighbor_buf);
  file_read_page(target_num, &target_buf);

  page_header_t *neighbor_header = (page_header_t *)&neighbor_buf;
  page_header_t *target_header = (page_header_t *)&target_buf;

  int neighbor_insertion_index = neighbor_header->num_of_keys;

  parent_num = target_header->parent_page_num;

  /* Case:  nonleaf node.
   * Append k_prime and the following pointer.
   * Append all pointers and keys from the neighbor.
   */

  if (target_header->is_leaf == INTERNAL) {
    internal_page_t *neighbor_internal = (internal_page_t *)&neighbor_buf;
    internal_page_t *target_internal = (internal_page_t *)&target_buf;

    /* Append k_prime. neighbor <= target
     */

    neighbor_internal->entries[neighbor_insertion_index].key = k_prime;
    neighbor_internal->entries[neighbor_insertion_index].page_num =
        target_internal->one_more_page_num;
    neighbor_header->num_of_keys++;

    for (int i = neighbor_insertion_index + 1, j = 0;
         j < target_internal->num_of_keys; i++, j++) {
      neighbor_internal->entries[i] = target_internal->entries[j];
      neighbor_header->num_of_keys++;
    }
    target_internal->num_of_keys = 0;

    /* All children must now point up to the same parent.
     */
    // make neightbor's one_more_page_num point to same parent
    pagenum_t child_num =
        neighbor_internal->entries[neighbor_insertion_index].page_num;
    if (child_num != PAGE_NULL) {
      page_t child_buf;
      file_read_page(child_num, &child_buf);
      ((page_header_t *)&child_buf)->parent_page_num = neighbor_num;
      file_write_page(child_num, &child_buf);
    }
    // make rest point to same parent
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

  /* In a leaf, append the keys and pointers of
   * target to the neighbor.
   * Set the neighbor's last pointer to point to
   * what had been target's right neighbor.
   */

  else {
    leaf_page_t *neighbor_leaf = (leaf_page_t *)&neighbor_buf;
    leaf_page_t *target_leaf = (leaf_page_t *)&target_buf;

    for (int i = neighbor_insertion_index, j = 0; j < target_leaf->num_of_keys;
         i++, j++) {
      neighbor_leaf->records[i] = target_leaf->records[j];
      neighbor_header->num_of_keys++;
    }

    neighbor_leaf->right_sibling_page_num = target_leaf->right_sibling_page_num;
  }
  file_write_page(neighbor_num, &neighbor_buf);
  file_free_page(target_num);

  return delete_entry(parent_num, k_prime, NULL);
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

  /* Case: target has a neighbor to the left.
   * Pull the neighbor's last key-pointer pair over
   * from the neighbor's right end to target's left end.
   */

  if (kprime_index_from_get != -1) {
    if (target_header->is_leaf == INTERNAL) {
      internal_page_t *target_internal = (internal_page_t *)&target_buf;
      internal_page_t *neighbor_internal = (internal_page_t *)&neighbor_buf;

      // target's left 공간 확보
      for (int index = target_header->num_of_keys; index > 0; index--) {
        target_internal->entries[index] = target_internal->entries[index - 1];
      }
      target_internal->entries[0].page_num = target_internal->one_more_page_num;

      // target의 K0는 k_prime(부모키)으로
      target_internal->entries[0].key = k_prime;
      // target의 첫번째 값을 neighbor의 마지막 값으로 설정
      pagenum_t last_num_neighbor =
          neighbor_internal->entries[neighbor_header->num_of_keys - 1].page_num;
      target_internal->one_more_page_num = last_num_neighbor;

      if (last_num_neighbor != PAGE_NULL) {
        page_t child_buf;
        file_read_page(last_num_neighbor, &child_buf);
        ((page_header_t *)&child_buf)->parent_page_num = target_num;
        file_write_page(last_num_neighbor, &child_buf);
      }
      // target의 부모 키를 neightbor의 마지막 키로 설정
      parent_page->entries[k_prime_index].key =
          neighbor_internal->entries[neighbor_header->num_of_keys - 1].key;

      memset(&neighbor_internal->entries[neighbor_header->num_of_keys - 1], 0,
             sizeof(entry_t));
    } else {
      // target is not leftmost && leaf case
      leaf_page_t *target_leaf = (leaf_page_t *)&target_buf;
      leaf_page_t *neighbor_leaf = (leaf_page_t *)&neighbor_buf;

      for (int i = target_header->num_of_keys; i > 0; i--) {
        target_leaf->records[i] = target_leaf->records[i - 1];
      }

      target_leaf->records[0] =
          neighbor_leaf->records[neighbor_header->num_of_keys - 1];

      parent_page->entries[k_prime_index].key = target_leaf->records[0].key;

      memset(&neighbor_leaf->records[neighbor_header->num_of_keys - 1], 0,
             sizeof(record_t));
    }
  }

  /* Case: target is the leftmost child.
   * Take a key-pointer pair from the neighbor to the right.
   * Move the neighbor's leftmost key-pointer pair
   * to target's rightmost position.
   */

  else {
    if (target_header->is_leaf == INTERNAL) {
      internal_page_t *target_internal = (internal_page_t *)&target_buf;
      internal_page_t *neighbor_internal = (internal_page_t *)&neighbor_buf;

      // target의 마지막 key에 k_prime(부모키) 설정
      target_internal->entries[target_header->num_of_keys].key = k_prime;

      // target의 마지막 entry에 neighbor의 onemorepagenum으로
      pagenum_t num_from_neighbor = neighbor_internal->one_more_page_num;
      target_internal->entries[target_header->num_of_keys].page_num =
          num_from_neighbor;

      // 자식의 부모 포인터 업데이트
      if (num_from_neighbor != PAGE_NULL) {
        page_t child_buf;
        file_read_page(num_from_neighbor, &child_buf);
        ((page_header_t *)&child_buf)->parent_page_num = target_num;
        file_write_page(num_from_neighbor, &child_buf);
      }

      // target의 부모 키(k_prime 위치)를 neighbor의 첫번째 키로
      parent_page->entries[k_prime_index].key =
          neighbor_internal->entries[0].key;

      // neighbor 한칸씩 왼쪽으로 당기기
      neighbor_internal->one_more_page_num =
          neighbor_internal->entries[0].page_num;
      for (int i = 0; i < neighbor_header->num_of_keys - 1; i++) {
        neighbor_internal->entries[i] = neighbor_internal->entries[i + 1];
      }

      memset(&neighbor_internal->entries[neighbor_header->num_of_keys - 1], 0,
             sizeof(entry_t));

    } else { // leaf nod

      leaf_page_t *target_leaf = (leaf_page_t *)&target_buf;
      leaf_page_t *neighbor_leaf = (leaf_page_t *)&neighbor_buf;

      target_leaf->records[target_header->num_of_keys] =
          neighbor_leaf->records[0];

      parent_page->entries[k_prime_index].key = neighbor_leaf->records[1].key;

      for (int i = 0; i < neighbor_header->num_of_keys - 1; i++) {
        neighbor_leaf->records[i] = neighbor_leaf->records[i + 1];
      }

      memset(&neighbor_leaf->records[neighbor_header->num_of_keys - 1], 0,
             sizeof(record_t));
    }
  }

  /* n now has one more key and one more pointer;
   * the neighbor has one fewer of each.
   */

  target_header->num_of_keys++;
  neighbor_header->num_of_keys--;

  file_write_page(target_num, &target_buf);
  file_write_page(neighbor_num, &neighbor_buf);
  file_write_page(parent_num, &parent_buf);

  return SUCCESS;
}

/**
 * @brief remove record from leaf node, if success return SUCCESS(0) else
 * FAILURE(1)
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
 * FAILURE(1)
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

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
int delete_entry(pagenum_t target_node, int64_t key, char *value) {
  int min_keys;
  int k_prime_index;

  // Remove key and pointer from node.

  page_t node_buf;
  file_read_page(target_node, &node_buf);
  page_header_t *node_header = (page_header_t *)&node_buf;

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
    perror("delete_entry error");
    break;
  }
  if (remove_result != SUCCESS) {
    return FAILURE;
  }
  file_write_page(target_node, (page_t *)&node_buf);

  /* Case:  deletion from the root.
   */
  header_page_t header_buf;
  file_read_page(HEADER_PAGE_POS, &header_buf);
  header_page_t *header_page = (header_page_t *)&header_buf;

  if (target_node == header_page->root_page_num) {
    return adjust_root(header_page->root_page_num);
  }

  /* Case:  deletion from a node below the root.
   * (Rest of function body.)
   */

  /* Determine minimum allowable size of node,
   * to be preserved after deletion.
   */

  /* Case:  node stays at or above minimum.
   * (The simple case.)
   */

  if (node_header->num_of_keys >= MIN_KEYS) {
    return SUCCESS;
  }

  /* Case:  node falls below minimum.
   * Either coalescence or redistribution
   * is needed.
   */
  /* Find the appropriate neighbor node with which
   * to coalesce.
   * Also find the key (k_prime) in the parent
   * between the pointer to node n and the pointer
   * to the neighbor.
   */
  pagenum_t neighbor_num_correct;
  int kprime_index_from_get = get_kprime_index(target_node);
  int k_prime_key_index; // Index of the key K that separates the two nodes

  pagenum_t parent_num = node_header->parent_page_num;
  page_t parent_buf;
  file_read_page(parent_num, &parent_buf);
  internal_page_t *parent_page = (internal_page_t *)&parent_buf;

  if (kprime_index_from_get == -1) {
    // target is P0 (one_more_page_num) must use right neighbor P1.
    neighbor_num_correct = parent_page->entries[0].page_num;
    k_prime_key_index = 0;
  } else {
    // target is Pi (i >= 0, P1~) use left neighbor Pi-1.
    k_prime_key_index = kprime_index_from_get;

    if (k_prime_key_index == 0) {
      // target is P1 (entries[0].page_num) neighbor is P0.
      neighbor_num_correct = parent_page->one_more_page_num;
    } else {
      // target is Pi+1 (entries[i].page_num, i > 0) neighbor is Pi.
      neighbor_num_correct =
          parent_page->entries[k_prime_key_index - 1].page_num;
    }
  }

  int64_t k_prime = parent_page->entries[k_prime_key_index].key;

  page_t neighbor_buf;
  file_read_page(neighbor_num_correct, &neighbor_buf);
  page_header_t *neighbor_header = (page_header_t *)&neighbor_buf;

  /* Coalescence. */
  int capacity = node_header->is_leaf ? RECORD_CNT : ENTRY_CNT - 1;
  if (neighbor_header->num_of_keys + node_header->num_of_keys < capacity) {
    return coalesce_nodes(target_node, neighbor_num_correct,
                          kprime_index_from_get, k_prime);
  }
  /* Redistribution. */
  else {
    // k_prime_index_from_get is the index of the pointer to the target node
    // k_prime_key_index is the index of the separating key in the parent
    return redistribute_nodes(target_node, neighbor_num_correct,
                              kprime_index_from_get, k_prime_key_index,
                              k_prime);
  }
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
