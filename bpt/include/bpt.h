#ifndef __BPT_H__
#define __BPT_H__

#include "file.h"

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

// Default order is 4.
#define DEFAULT_ORDER 4

// Minimum order is necessarily 3.  We set the maximum
// order arbitrarily.  You may change the maximum order.
#define MIN_ORDER 3
#define MAX_ORDER 20

#define LEAF_ORDER RECORDS_CNT + 1
#define INTERNAL_ORDER ENTRY_CNT + 1

// Constants for printing part or all of the GPL license.
#define LICENSE_FILE "LICENSE.txt"
#define LICENSE_WARRANTEE 0
#define LICENSE_WARRANTEE_START 592
#define LICENSE_WARRANTEE_END 624
#define LICENSE_CONDITIONS 1
#define LICENSE_CONDITIONS_START 70
#define LICENSE_CONDITIONS_END 625

// TYPES.

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
extern int order;

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
// extern pagenum_t queue;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
extern bool verbose_output;

// FUNCTION PROTOTYPES.

// Output and utility.

void license_notice(void);
void print_license(int licence_part);
void usage_1(void);
void usage_2(void);
void usage_3(void);
void enqueue(pagenum_t new_node);
pagenum_t dequeue(void);
int height(pagenum_t root);
int path_to_root(pagenum_t root, pagenum_t child);
void print_leaves(pagenum_t root);
void print_tree(pagenum_t root);
void find_and_print(pagenum_t root, int key, bool verbose);
void find_and_print_range(pagenum_t root, int range1, int range2, bool verbose);
int find_range(pagenum_t root, int key_start, int key_end, bool verbose,
               int returned_keys[], void *returned_pointers[]);
pagenum_t find_leaf(pagenum_t root, int64_t key);
char *find(pagenum_t root, int64_t key, char *result_buf);
int cut(int length);

// Insertion.

record_t *make_record(char *value);
pagenum_t make_node(uint32_t isleaf);
// pagenum_t make_node(void);
// pagenum_t make_leaf(void);
pagenum_t make_leaf(void);
int get_index_after_left_child(page_t *parent_buffer, pagenum_t left_num);
pagenum_t insert_into_leaf(pagenum_t leaf_num, page_t *leaf_buffer, int64_t key,
                           char *value);
pagenum_t insert_into_leaf_after_splitting(pagenum_t root, pagenum_t leaf,
                                           int64_t key, char *value);
pagenum_t insert_into_node(pagenum_t root, pagenum_t parent, int64_t left_index,
                           int64_t key, pagenum_t right);
pagenum_t insert_into_node_after_splitting(pagenum_t root, pagenum_t parent,
                                           int64_t left_index, int64_t key,
                                           pagenum_t right);
pagenum_t insert_into_parent(pagenum_t root, pagenum_t left, int64_t key,
                             pagenum_t right);
pagenum_t insert_into_new_root(pagenum_t left, int64_t key, pagenum_t right);
pagenum_t start_new_tree(int64_t key, char *value);
void init_header_page();
void link_header_page(pagenum_t root);
pagenum_t insert(pagenum_t root, int64_t key, char *value);

// Deletion.

int get_neighbor_index(pagenum_t n);
pagenum_t adjust_root(pagenum_t root);
pagenum_t coalesce_nodes(pagenum_t root, pagenum_t n, pagenum_t neighbor,
                         int neighbor_index, int k_prime);
pagenum_t redistribute_nodes(pagenum_t root, pagenum_t n, pagenum_t neighbor,
                             int neighbor_index, int k_prime_index,
                             int k_prime);
pagenum_t delete_entry(pagenum_t root, pagenum_t n, int key, void *pointer);
pagenum_t delete (pagenum_t root, int key);

void destroy_tree_nodes(pagenum_t root);
pagenum_t destroy_tree(pagenum_t root);

#endif /* __BPT_H__*/
