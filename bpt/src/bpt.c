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
node *queue = NULL;

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
void enqueue(node *new_node) {
  node *c;
  if (queue == NULL) {
    queue = new_node;
    queue->next = NULL;
  } else {
    c = queue;
    while (c->next != NULL) {
      c = c->next;
    }
    c->next = new_node;
    new_node->next = NULL;
  }
}

/* Helper function for printing the
 * tree out.  See print_tree.
 */
node *dequeue(void) {
  node *n = queue;
  queue = queue->next;
  n->next = NULL;
  return n;
}

/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */
void print_leaves(node *root) {
  int i;
  node *c = root;
  if (root == NULL) {
    printf("Empty tree.\n");
    return;
  }
  while (!c->is_leaf)
    c = c->pointers[0];
  while (true) {
    for (i = 0; i < c->num_keys; i++) {
      if (verbose_output)
        printf("%lx ", (unsigned long)c->pointers[i]);
      printf("%d ", c->keys[i]);
    }
    if (verbose_output)
      printf("%lx ", (unsigned long)c->pointers[order - 1]);
    if (c->pointers[order - 1] != NULL) {
      printf(" | ");
      c = c->pointers[order - 1];
    } else
      break;
  }
  printf("\n");
}

/* Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
int height(node *root) {
  int h = 0;
  node *c = root;
  while (!c->is_leaf) {
    c = c->pointers[0];
    h++;
  }
  return h;
}

/* Utility function to give the length in edges
 * of the path from any node to the root.
 */
int path_to_root(node *root, node *child) {
  int length = 0;
  node *c = child;
  while (c != root) {
    c = c->parent;
    length++;
  }
  return length;
}

/* Prints the B+ tree in the command
 * line in level (rank) order, with the
 * keys in each node and the '|' symbol
 * to separate nodes.
 * With the verbose_output flag set.
 * the values of the pointers corresponding
 * to the keys also appear next to their respective
 * keys, in hexadecimal notation.
 */
void print_tree(node *root) {
  node *n = NULL;
  int i = 0;
  int rank = 0;
  int new_rank = 0;

  if (root == NULL) {
    printf("Empty tree.\n");
    return;
  }
  queue = NULL;
  enqueue(root);
  while (queue != NULL) {
    n = dequeue();
    if (n->parent != NULL && n == n->parent->pointers[0]) {
      new_rank = path_to_root(root, n);
      if (new_rank != rank) {
        rank = new_rank;
        printf("\n");
      }
    }
    if (verbose_output)
      printf("(%lx)", (unsigned long)n);
    for (i = 0; i < n->num_keys; i++) {
      if (verbose_output)
        printf("%lx ", (unsigned long)n->pointers[i]);
      printf("%d ", n->keys[i]);
    }
    if (!n->is_leaf)
      for (i = 0; i <= n->num_keys; i++)
        enqueue(n->pointers[i]);
    if (verbose_output) {
      if (n->is_leaf)
        printf("%lx ", (unsigned long)n->pointers[order - 1]);
      else
        printf("%lx ", (unsigned long)n->pointers[n->num_keys]);
    }
    printf("| ");
  }
  printf("\n");
}

/* Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
void find_and_print(node *root, int key, bool verbose) {
  record *r = find(root, key, verbose);
  if (r == NULL)
    printf("Record not found under key %d.\n", key);
  else
    printf("Record at %lx -- key %d, value %d.\n", (unsigned long)r, key,
           r->value);
}

/* Finds and prints the keys, pointers, and values within a range
 * of keys between key_start and key_end, including both bounds.
 */
void find_and_print_range(node *root, int key_start, int key_end,
                          bool verbose) {
  int i;
  int array_size = key_end - key_start + 1;
  int returned_keys[array_size];
  void *returned_pointers[array_size];
  int num_found = find_range(root, key_start, key_end, verbose, returned_keys,
                             returned_pointers);
  if (!num_found)
    printf("None found.\n");
  else {
    for (i = 0; i < num_found; i++)
      printf("Key: %d   Location: %lx  Value: %d\n", returned_keys[i],
             (unsigned long)returned_pointers[i],
             ((record *)returned_pointers[i])->value);
  }
}

/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
int find_range(node *root, int key_start, int key_end, bool verbose,
               int returned_keys[], void *returned_pointers[]) {
  int i, num_found;
  num_found = 0;
  node *n = find_leaf(root, key_start, verbose);
  if (n == NULL)
    return 0;
  for (i = 0; i < n->num_keys && n->keys[i] < key_start; i++)
    ;
  if (i == n->num_keys)
    return 0;
  while (n != NULL) {
    for (; i < n->num_keys && n->keys[i] <= key_end; i++) {
      returned_keys[num_found] = n->keys[i];
      returned_pointers[num_found] = n->pointers[i];
      num_found++;
    }
    n = n->pointers[order - 1];
    i = 0;
  }
  return num_found;
}

/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
pagenum_t find_leaf(pagenum_t root, int64_t key) {
  if (root == PAGE_NULL) {
    return PAGE_NULL;
  }

  pagenum_t cur_num = root;
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

/* Finds and returns the record to which
 * a key refers.
 */
char *find(pagenum_t root, int64_t key, char *result_buf) {
  pagenum_t leaf_num = find_leaf(root, key);
  if (leaf_num == PAGE_NULL) {
    return PAGE_NULL;
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
    return result_buf;
  }

  return PAGE_NULL;
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

/* Creates a new record to hold the value
 * to which a key refers.
 */
// record_t *make_record(int64_t key, char *value) {
//   record_t *new_record = (record_t *)malloc(sizeof(record_t));
//   if (new_record == NULL) {
//     perror("Record creation.");
//     exit(EXIT_FAILURE);
//   } else {
//     new_record->key = key;
//     copy_value(new_record, value, VALUE_SIZE);
//   }
//   return new_record;
// }

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
  new_page_num = my_file_alloc_page(isleaf);
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
 * Returns the altered leaf.
 */
pagenum_t insert_into_leaf(pagenum_t leaf_num, page_t *leaf_buffer, int64_t key,
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
  return leaf_num;
}

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
pagenum_t insert_into_leaf_after_splitting(pagenum_t root, pagenum_t leaf_num,
                                           int64_t key, char *value) {
  pagenum_t new_leaf_num;
  int64_t new_key;
  record_t *temp_records;
  int insertion_index, split, i, j;

  new_leaf_num = make_leaf();

  temp_records = (record_t *)malloc(LEAF_ORDER * sizeof(record_t));
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
  while (insertion_index < RECORDS_CNT &&
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
  split = cut(LEAF_ORDER);

  // 분할 지점까지 old_leaf_page에 할당
  leaf_page->num_of_keys = 0;
  for (i = 0; i < split; i++) {
    leaf_page->records[i] = temp_records[i];
    leaf_page->num_of_keys++;
  }
  for (i = split; i < RECORDS_CNT; i++) {
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
  for (i = new_leaf_page->num_of_keys; i < RECORDS_CNT; i++) {
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
  return insert_into_parent(root, leaf_num, new_key, new_leaf_num);
}

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
pagenum_t insert_into_node(pagenum_t root, pagenum_t page_num,
                           int64_t left_index, int64_t key, pagenum_t right) {
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
  return root;
}

/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
pagenum_t insert_into_node_after_splitting(pagenum_t root, pagenum_t old_node,
                                           int64_t left_index, int64_t key,
                                           pagenum_t right) {
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
  temp_entries = (entry_t *)malloc((INTERNAL_ORDER) * sizeof(entry_t));
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
  file_read_page(child, &tmp_child_page);
  page_header_t *child_page_header = (page_header_t *)&tmp_child_page;
  child_page_header->parent_page_num = new_node;
  file_write_page(child, (page_t *)child_page_header);

  // New node의 엔트리 자식 업데이트
  for (i = 0; i < new_node_page->num_of_keys; i++) {
    child = new_node_page->entries[i].page_num;
    file_read_page(child, &tmp_child_page);
    child_page_header = (page_header_t *)&tmp_child_page;
    child_page_header->parent_page_num = new_node;
    file_write_page(child, (page_t *)child_page_header);
  }

  /* Insert a new key into the parent of the two
   * nodes resulting from the split, with
   * the old node to the left and the new to the right.
   */

  file_write_page(old_node, (page_t *)old_node_page);
  file_write_page(new_node, (page_t *)new_node_page);

  return insert_into_parent(root, old_node, k_prime, new_node);
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
pagenum_t insert_into_parent(pagenum_t root, pagenum_t left, int64_t key,
                             pagenum_t right) {
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

  if (parent_page_header->num_of_keys < ENTRY_CNT) {
    return insert_into_node(root, parent, left_index, key, right);
  }

  /* Harder case:  split a node in order
   * to preserve the B+ tree properties.
   */

  return insert_into_node_after_splitting(root, parent, left_index, key, right);
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
pagenum_t insert_into_new_root(pagenum_t left, int64_t key, pagenum_t right) {
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

  return root;
}

/* First insertion:
 * start a new tree.
 */
pagenum_t start_new_tree(int64_t key, char *value) {
  init_header_page();

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
  return root;
}

void init_header_page() {
  page_t header_buf;
  memset(&header_buf, 0, PAGE_SIZE);
  header_page_t *header_page = (header_page_t *)&header_buf;
  header_page->magic_num = MAGIC_NUM;
  header_page->internal_next_alloc = HEADER_PAGE_POS + 1;
  header_page->num_of_pages = INITIAL_INTERNAL_CAPACITY;

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
pagenum_t insert(pagenum_t root, int64_t key, char *value) {
  pagenum_t leaf;

  /* The current implementation ignores
   * duplicates.
   */

  char result_buf[VALUE_SIZE];
  if (find(root, key, result_buf) != PAGE_NULL) {
    return root;
  }

  /* Create a new record for the
   * value.
   */

  // pointer = make_record(value);

  /* Case: the tree does not exist yet.
   * Start a new tree.
   */

  if (root == PAGE_NULL) {
    return start_new_tree(key, value);
  }

  /* Case: the tree already exists.
   * (Rest of function body.)
   */

  leaf = find_leaf(root, key);

  /* Case: leaf has room for key and pointer.
   */

  page_t tmp_leaf_page;
  file_read_page(leaf, &tmp_leaf_page);
  leaf_page_t *leaf_page = (leaf_page_t *)&tmp_leaf_page;

  if (leaf_page->num_of_keys < RECORDS_CNT) {
    leaf = insert_into_leaf(leaf, &tmp_leaf_page, key, value);
    return root;
  }

  /* Case:  leaf must be split.
   */

  return insert_into_leaf_after_splitting(root, leaf, key, value);
}

// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int get_neighbor_index(node *n) {
  int i;

  /* Return the index of the key to the left
   * of the pointer in the parent pointing
   * to n.
   * If n is the leftmost child, this means
   * return -1.
   */
  for (i = 0; i <= n->parent->num_keys; i++)
    if (n->parent->pointers[i] == n)
      return i - 1;

  // Error state.
  printf("Search for nonexistent pointer to node in parent.\n");
  printf("Node:  %#lx\n", (unsigned long)n);
  exit(EXIT_FAILURE);
}

node *remove_entry_from_node(node *n, int key, node *pointer) {
  int i, num_pointers;

  // Remove the key and shift other keys accordingly.
  i = 0;
  while (n->keys[i] != key)
    i++;
  for (++i; i < n->num_keys; i++)
    n->keys[i - 1] = n->keys[i];

  // Remove the pointer and shift other pointers accordingly.
  // First determine number of pointers.
  num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
  i = 0;
  while (n->pointers[i] != pointer)
    i++;
  for (++i; i < num_pointers; i++)
    n->pointers[i - 1] = n->pointers[i];

  // One key fewer.
  n->num_keys--;

  // Set the other pointers to NULL for tidiness.
  // A leaf uses the last pointer to point to the next leaf.
  if (n->is_leaf)
    for (i = n->num_keys; i < order - 1; i++)
      n->pointers[i] = NULL;
  else
    for (i = n->num_keys + 1; i < order; i++)
      n->pointers[i] = NULL;

  return n;
}

node *adjust_root(node *root) {
  node *new_root;

  /* Case: nonempty root.
   * Key and pointer have already been deleted,
   * so nothing to be done.
   */

  if (root->num_keys > 0)
    return root;

  /* Case: empty root.
   */

  // If it has a child, promote
  // the first (only) child
  // as the new root.

  if (!root->is_leaf) {
    new_root = root->pointers[0];
    new_root->parent = NULL;
  }

  // If it is a leaf (has no children),
  // then the whole tree is empty.

  else
    new_root = NULL;

  free(root->keys);
  free(root->pointers);
  free(root);

  return new_root;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
node *coalesce_nodes(node *root, node *n, node *neighbor, int neighbor_index,
                     int k_prime) {
  int i, j, neighbor_insertion_index, n_end;
  node *tmp;

  /* Swap neighbor with node if node is on the
   * extreme left and neighbor is to its right.
   */

  if (neighbor_index == -1) {
    tmp = n;
    n = neighbor;
    neighbor = tmp;
  }

  /* Starting point in the neighbor for copying
   * keys and pointers from n.
   * Recall that n and neighbor have swapped places
   * in the special case of n being a leftmost child.
   */

  neighbor_insertion_index = neighbor->num_keys;

  /* Case:  nonleaf node.
   * Append k_prime and the following pointer.
   * Append all pointers and keys from the neighbor.
   */

  if (!n->is_leaf) {
    /* Append k_prime.
     */

    neighbor->keys[neighbor_insertion_index] = k_prime;
    neighbor->num_keys++;

    n_end = n->num_keys;

    for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
      neighbor->keys[i] = n->keys[j];
      neighbor->pointers[i] = n->pointers[j];
      neighbor->num_keys++;
      n->num_keys--;
    }

    /* The number of pointers is always
     * one more than the number of keys.
     */

    neighbor->pointers[i] = n->pointers[j];

    /* All children must now point up to the same parent.
     */

    for (i = 0; i < neighbor->num_keys + 1; i++) {
      tmp = (node *)neighbor->pointers[i];
      tmp->parent = neighbor;
    }
  }

  /* In a leaf, append the keys and pointers of
   * n to the neighbor.
   * Set the neighbor's last pointer to point to
   * what had been n's right neighbor.
   */

  else {
    for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
      neighbor->keys[i] = n->keys[j];
      neighbor->pointers[i] = n->pointers[j];
      neighbor->num_keys++;
    }
    neighbor->pointers[order - 1] = n->pointers[order - 1];
  }

  root = delete_entry(root, n->parent, k_prime, n);
  free(n->keys);
  free(n->pointers);
  free(n);
  return root;
}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
node *redistribute_nodes(node *root, node *n, node *neighbor,
                         int neighbor_index, int k_prime_index, int k_prime) {
  int i;
  node *tmp;

  /* Case: n has a neighbor to the left.
   * Pull the neighbor's last key-pointer pair over
   * from the neighbor's right end to n's left end.
   */

  if (neighbor_index != -1) {
    if (!n->is_leaf)
      n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
    for (i = n->num_keys; i > 0; i--) {
      n->keys[i] = n->keys[i - 1];
      n->pointers[i] = n->pointers[i - 1];
    }
    if (!n->is_leaf) {
      n->pointers[0] = neighbor->pointers[neighbor->num_keys];
      tmp = (node *)n->pointers[0];
      tmp->parent = n;
      neighbor->pointers[neighbor->num_keys] = NULL;
      n->keys[0] = k_prime;
      n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
    } else {
      n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
      neighbor->pointers[neighbor->num_keys - 1] = NULL;
      n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
      n->parent->keys[k_prime_index] = n->keys[0];
    }
  }

  /* Case: n is the leftmost child.
   * Take a key-pointer pair from the neighbor to the right.
   * Move the neighbor's leftmost key-pointer pair
   * to n's rightmost position.
   */

  else {
    if (n->is_leaf) {
      n->keys[n->num_keys] = neighbor->keys[0];
      n->pointers[n->num_keys] = neighbor->pointers[0];
      n->parent->keys[k_prime_index] = neighbor->keys[1];
    } else {
      n->keys[n->num_keys] = k_prime;
      n->pointers[n->num_keys + 1] = neighbor->pointers[0];
      tmp = (node *)n->pointers[n->num_keys + 1];
      tmp->parent = n;
      n->parent->keys[k_prime_index] = neighbor->keys[0];
    }
    for (i = 0; i < neighbor->num_keys - 1; i++) {
      neighbor->keys[i] = neighbor->keys[i + 1];
      neighbor->pointers[i] = neighbor->pointers[i + 1];
    }
    if (!n->is_leaf)
      neighbor->pointers[i] = neighbor->pointers[i + 1];
  }

  /* n now has one more key and one more pointer;
   * the neighbor has one fewer of each.
   */

  n->num_keys++;
  neighbor->num_keys--;

  return root;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
node *delete_entry(node *root, node *n, int key, void *pointer) {
  int min_keys;
  node *neighbor;
  int neighbor_index;
  int k_prime_index, k_prime;
  int capacity;

  // Remove key and pointer from node.

  n = remove_entry_from_node(n, key, pointer);

  /* Case:  deletion from the root.
   */

  if (n == root)
    return adjust_root(root);

  /* Case:  deletion from a node below the root.
   * (Rest of function body.)
   */

  /* Determine minimum allowable size of node,
   * to be preserved after deletion.
   */

  min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1;

  /* Case:  node stays at or above minimum.
   * (The simple case.)
   */

  if (n->num_keys >= min_keys)
    return root;

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

  neighbor_index = get_neighbor_index(n);
  k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
  k_prime = n->parent->keys[k_prime_index];
  neighbor = neighbor_index == -1 ? n->parent->pointers[1]
                                  : n->parent->pointers[neighbor_index];

  capacity = n->is_leaf ? order : order - 1;

  /* Coalescence. */

  if (neighbor->num_keys + n->num_keys < capacity)
    return coalesce_nodes(root, n, neighbor, neighbor_index, k_prime);

  /* Redistribution. */

  else
    return redistribute_nodes(root, n, neighbor, neighbor_index, k_prime_index,
                              k_prime);
}

/* Master deletion function.
 */
node *delete (node *root, int key) {
  node *key_leaf;
  record *key_record;

  key_record = find(root, key, false);
  key_leaf = find_leaf(root, key, false);
  if (key_record != NULL && key_leaf != NULL) {
    root = delete_entry(root, key_leaf, key, key_record);
    free(key_record);
  }
  return root;
}

void destroy_tree_nodes(node *root) {
  int i;
  if (root->is_leaf)
    for (i = 0; i < root->num_keys; i++)
      free(root->pointers[i]);
  else
    for (i = 0; i < root->num_keys + 1; i++)
      destroy_tree_nodes(root->pointers[i]);
  free(root->pointers);
  free(root->keys);
  free(root);
}

node *destroy_tree(node *root) {
  destroy_tree_nodes(root);
  return NULL;
}
