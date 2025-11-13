#include "file.h";
#include <stdlib.h>
#include <unistd.h>

int fd = -1; // temp file discripter

// 헬퍼 함수 -----------------------------------------------------------
// ---------------------------------------------------------------------

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

// Allocate an on-disk page from the free page list
// original version
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

// my db version
pagenum_t my_file_alloc_page(int page_type) {
  header_page_t header;
  free_page_t free_page;
  pagenum_t allocated_page_num;
  pagenum_t *current_head_ptr;

  file_read_page(0, (page_t *)&header);

  // 리프페이지냐 내부페이지냐에 따라 프리 페이지 번호 할당
  if (page_type == LEAF) {
    current_head_ptr = &header.free_leaf_head;
  } else if (page_type == INTERNAL) {
    current_head_ptr = &header.free_internal_head;
  } else {
    handle_error("leaf or internal only");
  }
  allocated_page_num = *current_head_ptr;

  // 프리 리스트가 비어있는 경우: 새 페이지 할당
  if (allocated_page_num == 0) {
    pagenum_t new_page_num = header.num_of_pages;
    header.num_of_pages += 1;
    file_write_page(0, (page_t *)&header);

    return new_page_num;
  }

  // 프리 리스트가 존재하는 경우:
  // 사용한 프리 페이지의 다음 페이지로 리스트 헤더 변경
  file_read_page(allocated_page_num, (page_t *)&free_page);
  *current_head_ptr = free_page.next_free_page_num;
  file_write_page(0, (page_t *)&header);

  return allocated_page_num;
}

// 페이지 해제 ----------------------------------------------------------
// ----------------------------------------------------------------------

// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum) {
  header_page_t header;
  page_t removing_page;
  free_page_t new_free_page;

  // 헤더 페이지를 읽어와서 프리 페이지 리스트 참조
  // 삭제할 페이지가 리프인지 내부인지에 따라 처리를 다르게 함
  file_read_page(0, &header);
  file_read_page(pagenum, &removing_page);
  uint32_t isleaf = get_isleaf_flag(&removing_page);
  if (isleaf == LEAF) {
    new_free_page.next_free_page_num = header.free_leaf_head;
    header.free_leaf_head = pagenum;
  } else if (isleaf == INTERNAL) {
    new_free_page.next_free_page_num = header.free_internal_head;
    header.free_internal_head = pagenum;
  } else {
    // 이러면 에러일 확률이 높음
    new_free_page.next_free_page_num = header.free_page_num;
    header.free_page_num = pagenum;
  }

  // 삭제할 자리에 새로 작성할 프리 페이지 작성
  // 헤더 페이지 및 프리페이지 쓰기
  file_write_page(pagenum, (page_t *)&new_free_page);
  file_write_page(0, (page_t *)&header);
}

// 페이지 읽기 쓰기 -----------------------------------------------------
// ----------------------------------------------------------------------

// Read an on-disk page into the in-memory page structure(dest)
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

// Write an in-memory page(src) to the on-disk page
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
