### b+tree insert의 call chain

```c
node *insert(node *root, int key, int value) {}
```

```
insert
  find
  make_record
  start_new_tree => [if root is NULL]
  find_leaf => [if root is not NULL]
    insert_into_leaf => [if leaf has room]
    insert_into_leaf_after_splitting => [if leaf full]
      make_leaf
      cut
      [can be recursive] : insert_into_parent
        insert_into_new_root => [if parent is new root]
        get_left_index
        insert_into_node => [if parent has room]
        insert_into_node_after_splitting => [if parent full]
          cut
          make_node
          [can be recursive] : insert_into_parent

```

### b+tree delete의 call chain

```c
node *delete (node *root, int key) {}
```

```
delete
  find : to find record
    find_leaf
  find_leaf : to find leaf node
  delete_entry
    remove_entry_from_node
    adjust_root => [if delete from the root, adjust]
    (if delete from a node below the root)
    cut
    return; => [if node above threshold, done]
    get_neightbor_index : to get left neightbor for coalesecing
    coalesce_nodes => [if merged node not full]
      delete_entry : to delete node in parent
    redistribute_nodes
```

### architecture design

```
[Application Layer]
(main.c)
- 사용자 입력 처리

=>

[Stroage Engine API]
(db_api.c)
- open_table()
- db_insert()
- db_find()
- db_delete()

=>

[Index Layer]
(bpt.c)
- B+tree 탐색/수정
- file API 호출

=>

[File and Disk Space Management Layer]
(file.c)
- file_alloc_page()
- file_free_page()
- file_read/write_page()
- fsync()

=>

[Disk]
(data file)
```
