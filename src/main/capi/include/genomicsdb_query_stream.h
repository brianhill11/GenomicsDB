/**
 * The MIT License (MIT)
 * Copyright (c) 2018 Omics Data Automation Incorporated and Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#ifndef _GENOMICSDB_C_API_QUERY_STREAM_H_
#define _GENOMICSDB_C_API_QUERY_STREAM_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

  typedef struct gdb_config {
    const char *loader_config_file;
    const char *query_config_file;
    const char *chr;
    int start;
    int end;
    // Optional fields
    int rank=0;
    uint64_t buffer_capacity=1048576u;
    uint64_t segment_size=1048576u;
    int is_bcf=true;
    int produce_header_only=false;
    int use_missing_values_only_not_vector_end=false;
    int keep_idx_fields_in_bcf_header=true;
  } gdb_config_t;

  /* C Interface to GenomicsDBBCFGenerator */
  void *genomicsdb_connect(void *config);

  int genomicsdb_close(void *handle);
  
  uint64_t genomicsdb_get_total_bytes_available(void *handle);

  uint8_t genomicsdb_read_next_byte(void *handle);
  
  int genomicsdb_read(void *handle, uint8_t *byte_array, uint64_t offset, uint64_t n);

  uint64_t genomicsdb_skip(void *handle, long n);

  /* Helpers to construct gdb_config_t to pass to GenomicsDBBCFGenerator */
  void *genomicsdb_create_config(const char *loader_config_file, const char *query_config_file, const char *chr, int start, int end);

  void genomicsdb_print_config(void *config);

  void genomicsdb_config_set_rank(void *config, int rank);

  void genomicsdb_config_set_buffer_capacity(void *config, uint64_t buffer_capacity);

  void genomicsdb_config_set_segment_size(void *config, uint64_t segment_size);

  void genomicsdb_config_set_is_bcf(void *config, int is_bcf);

  void genomicsdb_config_set_produce_header_only(void *config, int produce_header_only);

  void genomicsdb_config_set_use_missing_values_only_not_vector_end(void *config, int use_missing_values_only_not_vector_end);

  void genomicsdb_config_set_keep_idx_fields_in_bcf_header(void *config, int keep_idx_fields_in_bcf_header);

  void genomicsdb_config_delete(void *config);
}

#endif /* _GENOMICSDB_C_API_QUERY_STREAM_H_ */
