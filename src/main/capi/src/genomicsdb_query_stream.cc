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

#include "genomicsdb_bcf_generator.h"
#include "genomicsdb_query_stream.h"

#include <assert.h>
#include <iostream>

#define GET_GDB_CONFIG_FROM_PTR(X) (reinterpret_cast<gdb_config_t *>(static_cast<void *>(X)))
#define GET_BCF_READER_FROM_HANDLE(X) (reinterpret_cast<GenomicsDBBCFGenerator*>(static_cast<void *>(X)))

void *genomicsdb_connect(void **config) {
  // Sanity Check
  assert(GET_GDB_CONFIG_FROM_PTR(*config)->loader_config_file && "Loader config file not specified");
  assert(GET_GDB_CONFIG_FROM_PTR(*config)->query_config_file && "Query config file not specified");
  assert(GET_GDB_CONFIG_FROM_PTR(*config)->chr && "chr not specified");
  
  //Create object
  auto output_format = GET_GDB_CONFIG_FROM_PTR(*config)->is_bcf ? "bu" : "";
  GenomicsDBBCFGenerator *bcf_reader_obj = new GenomicsDBBCFGenerator(
      GET_GDB_CONFIG_FROM_PTR(*config)->loader_config_file,
      GET_GDB_CONFIG_FROM_PTR(*config)->query_config_file,
      GET_GDB_CONFIG_FROM_PTR(*config)->chr,
      GET_GDB_CONFIG_FROM_PTR(*config)->start,
      GET_GDB_CONFIG_FROM_PTR(*config)->end,
      GET_GDB_CONFIG_FROM_PTR(*config)->rank,
      GET_GDB_CONFIG_FROM_PTR(*config)->buffer_capacity,
      GET_GDB_CONFIG_FROM_PTR(*config)->segment_size,
      output_format,
      GET_GDB_CONFIG_FROM_PTR(*config)->produce_header_only,
      GET_GDB_CONFIG_FROM_PTR(*config)->is_bcf && GET_GDB_CONFIG_FROM_PTR(*config)->use_missing_values_only_not_vector_end,
      GET_GDB_CONFIG_FROM_PTR(*config)->is_bcf &&  GET_GDB_CONFIG_FROM_PTR(*config)->keep_idx_fields_in_bcf_header);

  //Cast pointer to void * and return
  return static_cast<void *>(bcf_reader_obj);
}

int genomicsdb_close(void **handle)
{
  auto bcf_reader_obj = GET_BCF_READER_FROM_HANDLE(*handle);
  if (bcf_reader_obj) //not NULL
    delete bcf_reader_obj;
  return 0;
}

uint64_t genomicsdb_get_total_bytes_available(void **handle)
{
  auto bcf_reader_obj = GET_BCF_READER_FROM_HANDLE(*handle);
  return (bcf_reader_obj) ? bcf_reader_obj->get_buffer_capacity() : 0;
}

uint8_t genomicsdb_read_next_byte(void **handle)
{
  auto bcf_reader_obj = GET_BCF_READER_FROM_HANDLE(*handle);
  return (bcf_reader_obj) ? bcf_reader_obj->read_next_byte() : -1;
}

int genomicsdb_read(void **handle, uint8_t *byte_array, uint64_t offset, uint64_t n)
{
  auto bcf_reader_obj = GET_BCF_READER_FROM_HANDLE(*handle);
  if(bcf_reader_obj == 0)
    return 0;
  
  uint64_t total_num_bytes_read = 0;
  while(total_num_bytes_read < n && !(bcf_reader_obj->end())) {
    auto& buffer_obj = bcf_reader_obj->get_read_batch();
    uint64_t num_bytes_to_copy = std::min<uint64_t>(buffer_obj.get_num_remaining_bytes(), n-total_num_bytes_read);
    //Handle this as a special case as read_and_advance will not advance anything if num_bytes_to_copy == 0u
    if(num_bytes_to_copy == 0u) {
      num_bytes_to_copy = SIZE_MAX;     //forces bcf_reader to produce the next batch of records
    } else {
      memcpy(byte_array+offset+total_num_bytes_read, buffer_obj.get_pointer_at_read_position(), num_bytes_to_copy);
      total_num_bytes_read += num_bytes_to_copy;
    }
    bcf_reader_obj->read_and_advance(0, 0u, num_bytes_to_copy);
  }
  return total_num_bytes_read;
}

uint64_t genomicsdb_skip(void **handle, long n)
{
  auto bcf_reader_obj = GET_BCF_READER_FROM_HANDLE(*handle);
  return (bcf_reader_obj) ? bcf_reader_obj->read_and_advance(0, 0, n) : 0;
}


/* Helpers to construct gdb_config_t to pass to GenomicsDBBCFGenerator */

void *genomicsdb_create_config(const char *loader_config_file, const char *query_config_file, const char *chr, int start, int end) {
  gdb_config_t *gdb_config = new (std::nothrow) gdb_config_t;
  gdb_config->loader_config_file = loader_config_file;
  gdb_config->query_config_file = query_config_file;
  gdb_config->chr = chr;
  gdb_config->start = start;
  gdb_config->end = end;
  return static_cast<void *>(gdb_config);
}

void genomicsdb_print_config(void **config) {
  if (config != NULL) {
    std::cout << "loader config = " << GET_GDB_CONFIG_FROM_PTR(*config)->loader_config_file << std::endl;
    std::cout << "query config = " << GET_GDB_CONFIG_FROM_PTR(*config)->query_config_file << std::endl;
    std::cout << "chr = " << GET_GDB_CONFIG_FROM_PTR(*config)->chr << std::endl;
    std::cout << "start = " << GET_GDB_CONFIG_FROM_PTR(*config)->start << std::endl;
    std::cout << "end =" << GET_GDB_CONFIG_FROM_PTR(*config)->end << std::endl;
    std::cout << "rank =" << GET_GDB_CONFIG_FROM_PTR(*config)->rank << std::endl;
    std::cout << "buffer capacity = " << GET_GDB_CONFIG_FROM_PTR(*config)->buffer_capacity << std::endl;
    std::cout << "segment_size = " << GET_GDB_CONFIG_FROM_PTR(*config)->segment_size << std::endl;
    std::cout << "is_bcf = " << GET_GDB_CONFIG_FROM_PTR(*config)->is_bcf << std::endl;
    std::cout << "produce_header_only = " << GET_GDB_CONFIG_FROM_PTR(*config)->produce_header_only << std::endl;
    std::cout << "use_missing_values_only_not_vector_end = " << GET_GDB_CONFIG_FROM_PTR(*config)->use_missing_values_only_not_vector_end << std::endl;
    std::cout << "keep_idx_fields_in_bcf_header = " << GET_GDB_CONFIG_FROM_PTR(*config)->keep_idx_fields_in_bcf_header << std::endl;
  }
}

void genomicsdb_config_set_rank(void **config, int rank) {
  if (*config != NULL) {
    GET_GDB_CONFIG_FROM_PTR(*config)->rank = rank;
  }
}

void genomicsdb_config_set_buffer_capacity(void **config, uint64_t buffer_capacity) {
  if (*config != NULL) {
    GET_GDB_CONFIG_FROM_PTR(*config)->buffer_capacity = buffer_capacity;
  }
}
        
void genomicsdb_config_set_segment_size(void **config, uint64_t segment_size) {
  if (*config != NULL) {
    GET_GDB_CONFIG_FROM_PTR(*config)->segment_size = segment_size;
  }
}

void genomicsdb_config_set_is_bcf(void **config, int is_bcf) {
  if (*config != NULL) {
    GET_GDB_CONFIG_FROM_PTR(*config)->is_bcf = is_bcf;
  }
}

void genomicsdb_config_set_produce_header_only(void **config, int produce_header_only) {
  if (*config != NULL) {
    GET_GDB_CONFIG_FROM_PTR(*config)->produce_header_only = produce_header_only;
  }
}

void genomicsdb_config_set_use_missing_values_only_not_vector_end(void **config, int use_missing_values_only_not_vector_end) {
  if (*config != NULL) {
    GET_GDB_CONFIG_FROM_PTR(*config)->use_missing_values_only_not_vector_end = use_missing_values_only_not_vector_end;
  }
}

void genomicsdb_config_set_keep_idx_fields_in_bcf_header(void **config, int keep_idx_fields_in_bcf_header) {
  if (*config != NULL) {
    GET_GDB_CONFIG_FROM_PTR(*config)-> keep_idx_fields_in_bcf_header =  keep_idx_fields_in_bcf_header;
  }
}

void genomicsdb_config_delete(void **config) {
  if (*config != NULL) {
    delete GET_GDB_CONFIG_FROM_PTR(*config);
  }
}
