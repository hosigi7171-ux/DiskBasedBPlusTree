#include "helper_mock.h"
#include <string.h>

page_t MOCK_PAGES[MAX_MOCK_PAGES];

void setup_data_store(void) { memset(MOCK_PAGES, 0, sizeof(MOCK_PAGES)); }

void MOCK_file_read_page(pagenum_t pagenum, page_t *dest, int num_calls) {
  if (pagenum > 0 && pagenum < MAX_MOCK_PAGES) {
    memcpy(dest, &MOCK_PAGES[pagenum], PAGE_SIZE);
  }
}

void MOCK_file_write_page(pagenum_t pagenum, const page_t *src, int num_calls) {
  if (pagenum > 0 && pagenum < MAX_MOCK_PAGES) {
    memcpy(&MOCK_PAGES[pagenum], src, PAGE_SIZE);
  }
}