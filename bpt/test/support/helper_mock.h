#include "page.h"

#define MAX_MOCK_PAGES 10

// mock data store
extern page_t MOCK_PAGES[MAX_MOCK_PAGES];

// must called within test's setUp function for using mock read/write page
void setup_data_store(void);

void MOCK_file_read_page(pagenum_t pagenum, page_t *dest, int num_calls);
void MOCK_file_write_page(pagenum_t pagenum, const page_t *src, int num_calls);