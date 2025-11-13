#include "file.h";
#include <stdlib.h>
#include <unistd.h>

int fd = -1; // temp file discripter

// 헬퍼 함수 -----------------------------------------------------------
// ---------------------------------------------------------------------
pagenum_t *get_free_list_head(header_page_t *header, uint32_t page_type);
pagenum_t allocate_new_page(header_page_t *header, uint32_t page_type);
pagenum_t pop_free_page(pagenum_t *free_list_head);
void link_new_free_page(header_page_t *header, pagenum_t pagenum,
                        uint32_t isleaf, free_page_t *new_free_page);

off_t get_offset(pagenum_t pagenum) { return (off_t)pagenum * PAGE_SIZE; }

uint32_t get_isleaf_flag(const page_t *page) {
  return *(uint32_t *)(page->data + sizeof(pagenum_t));
}

void handle_error(const char *msg) {
  perror(msg);
  exit(EXIT_FAILURE);
}

// 페이지 할당 ----------------------------------------------------------
// ----------------------------------------------------------------------

/**
 * @brief Allocate an on-disk page from the free page list
 * legacy version
 */
pagenum_t file_alloc_page() {
  header_page_t header;
  free_page_t free_page;
  pagenum_t allocated_page_num;
  pagenum_t *current_head_ptr;

  file_read_page(0, &header);
  current_head_ptr = &header.free_page_num;
  allocated_page_num = *current_head_ptr;

  if (allocated_page_num == 0) {
    pagenum_t new_page_num = header.num_of_pages;
    header.num_of_pages += 1;
    file_write_page(0, &header);

    return new_page_num;
  }

  file_read_page(allocated_page_num, &free_page);
  *current_head_ptr = free_page.next_free_page_num;
  file_write_page(0, &header);

  return allocated_page_num;
}

/**
 * @brief Allocate an on-disk page from the free page list
 * my db version
 */
pagenum_t my_file_alloc_page(uint32_t page_type) {
  header_page_t header;
  free_page_t free_page;
  pagenum_t allocated_page_num;
  pagenum_t *current_head_ptr;

  file_read_page(0, (page_t *)&header);

  pagenum_t *free_list_head = get_free_list_head(&header, page_type);
  allocated_page_num = pop_free_page(free_list_head);

  // if there is no free list
  if (allocated_page_num == 0) {
    allocated_page_num = allocate_new_page(&header, page_type);
  }

  file_write_page(0, (page_t *)&header);
  return allocated_page_num;
}

/**
 * @brief Get the free list head object
 */
pagenum_t *get_free_list_head(header_page_t *header, uint32_t page_type) {
  switch (page_type) {
  case LEAF:
    return &header->free_leaf_head;
  case INTERNAL:
    return &header->free_internal_head;
  default:
    handle_error("leaf or internal only");
    return NULL;
  }
}

/**
 * @brief allocate new page by page_type
 */
pagenum_t allocate_new_page(header_page_t *header, uint32_t page_type) {
  pagenum_t new_page_num;

  if (page_type == INTERNAL &&
      header->internal_next_alloc < INITIAL_INTERNAL_CAPACITY) {
    new_page_num = header->internal_next_alloc;
    header->internal_next_alloc += 1;
    return new_page_num;
  }

  // num_of_pages는 leaf page의 다음 할당 번호 역할
  new_page_num = header->num_of_pages;
  header->num_of_pages += 1;
  return new_page_num;
}

/**
 * @brief pop free page from free list
 */
pagenum_t pop_free_page(pagenum_t *free_list_head) {
  if (*free_list_head == 0)
    return 0;

  pagenum_t allocated_page_num = *free_list_head;
  free_page_t free_page;
  file_read_page(allocated_page_num, (page_t *)&free_page);
  *free_list_head = free_page.next_free_page_num;

  return allocated_page_num;
}

// 페이지 해제 ----------------------------------------------------------
// ----------------------------------------------------------------------

/**
 * @brief Free an on-disk page to the free page list
 */
void file_free_page(pagenum_t pagenum) {
  header_page_t header;
  page_t removing_page;
  free_page_t new_free_page;

  // 헤더 페이지를 읽어와서 프리 페이지 리스트 참조
  file_read_page(0, &header);
  file_read_page(pagenum, &removing_page);
  uint32_t isleaf = get_isleaf_flag(&removing_page);
  link_new_free_page(&header, pagenum, isleaf, &new_free_page);

  // 삭제할 자리에 새로 작성할 프리 페이지 작성
  file_write_page(pagenum, (page_t *)&new_free_page);
  file_write_page(0, (page_t *)&header);
}

/**
 * @brief file_free_page 함수에서 사용하는 헬퍼 함수
 * 새로운 free page를 header page에 연결
 */
void link_new_free_page(header_page_t *header, pagenum_t pagenum,
                        uint32_t isleaf, free_page_t *new_free_page) {
  switch (isleaf) {
  case LEAF:
    new_free_page->next_free_page_num = header->free_leaf_head;
    header->free_leaf_head = pagenum;
    break;
  case INTERNAL:
    new_free_page->next_free_page_num = header->free_internal_head;
    header->free_internal_head = pagenum;
    break;
  default: // 이 경우는 에러일 수도 있으니까 나중에 다시 한 번 점검하기
    new_free_page->next_free_page_num = header->free_page_num;
    header->free_page_num = pagenum;
    break;
  }
}

// 페이지 읽기 쓰기 -----------------------------------------------------
// ----------------------------------------------------------------------

/**
 * @brief Read an on-disk page into the in-memory page structure(dest)
 */
void file_read_page(pagenum_t pagenum, page_t *dest) {
  off_t offset = get_offset(pagenum);

  if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
    handle_error("lseek error");
  }

  ssize_t bytes_read = read(fd, dest, PAGE_SIZE);
  if (bytes_read != PAGE_SIZE) {
    handle_error("read error");
  }
}

/**
 * @brief Write an in-memory page(src) to the on-disk page
 */
void file_write_page(pagenum_t pagenum, const page_t *src) {
  off_t offset = get_offset(pagenum);

  if (lseek(fd, offset, SEEK_SET) == (off_t)-1) {
    handle_error("lseek error");
  }

  ssize_t bytes_written = write(fd, src, PAGE_SIZE);
  if (bytes_written != PAGE_SIZE) {
    handle_error("write error");
  }

  if (fsync(fd) != 0) {
    handle_error("fsync error");
  }
}
