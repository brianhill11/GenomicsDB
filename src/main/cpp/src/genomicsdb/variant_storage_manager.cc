/**
 * The MIT License (MIT)
 * Copyright (c) 2016-2017 Intel Corporation
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

#include "variant_storage_manager.h"
#include "variant_field_data.h"
#include "json_config.h"

#include <mutex>

#define VERIFY_OR_THROW(X) if(!(X)) throw VariantStorageManagerException(#X);
#define GET_METADATA_PATH(workspace, array) ((workspace)+'/'+(array)+"/genomicsdb_meta.json")

const std::unordered_map<std::string, int> VariantStorageManager::m_mode_string_to_int = {
  { "r", TILEDB_ARRAY_READ },
  { "w", TILEDB_ARRAY_WRITE },
  { "a", TILEDB_ARRAY_WRITE }
};

std::vector<const char*> VariantStorageManager::m_metadata_attributes = std::vector<const char*>({ "max_valid_row_idx_in_array" });

// Cache of metadata json to minimize reading from storage.
static std::mutex metadata_cache_mtx;
static std::unordered_map<std::string, int64_t> metadata_cache;

//ceil(buffer_size/field_size)*field_size
#define GET_ALIGNED_BUFFER_SIZE(buffer_size, field_size) ((((buffer_size)+(field_size)-1u)/(field_size))*(field_size))

//VariantArrayCellIterator functions
VariantArrayCellIterator::VariantArrayCellIterator(TileDB_CTX* tiledb_ctx, const VariantArraySchema& variant_array_schema,
        const std::string& array_path, const int64_t* range, const std::vector<int>& attribute_ids, const size_t buffer_size)
  : m_num_queried_attributes(attribute_ids.size()), m_tiledb_ctx(tiledb_ctx),
  m_variant_array_schema(&variant_array_schema), m_cell(variant_array_schema, attribute_ids)
#ifdef DO_PROFILING
  , m_tiledb_timer()
  , m_tiledb_to_buffer_cell_timer()
#endif
{
  m_buffers.clear();
  std::vector<const char*> attribute_names(attribute_ids.size()+1u);  //+1 for the COORDS
  for(auto i=0ull;i<attribute_ids.size();++i)
  {
    //Buffer size must be resized to be a multiple of the field size
    auto curr_buffer_size = buffer_size;
    attribute_names[i] = variant_array_schema.attribute_name(attribute_ids[i]).c_str();
    //For varible length attributes, need extra buffer for maintaining offsets
    if(variant_array_schema.is_variable_length_field(attribute_ids[i]))
    {
      curr_buffer_size = GET_ALIGNED_BUFFER_SIZE(buffer_size, sizeof(size_t));
      m_buffers.emplace_back(curr_buffer_size);
    }
    else
      curr_buffer_size = GET_ALIGNED_BUFFER_SIZE(buffer_size, m_cell.get_field_size_in_bytes(i));
    m_buffers.emplace_back(curr_buffer_size);
  }
  //Co-ordinates
  attribute_names[attribute_ids.size()] = TILEDB_COORDS;
  m_buffers.emplace_back(GET_ALIGNED_BUFFER_SIZE(buffer_size, variant_array_schema.dim_size_in_bytes()));
  //Initialize pointers to buffers
  m_buffer_pointers.resize(m_buffers.size());
  m_buffer_sizes.resize(m_buffers.size());
  for(auto i=0ull;i<m_buffers.size();++i)
  {
    m_buffer_pointers[i] = reinterpret_cast<void*>(&(m_buffers[i][0]));
    m_buffer_sizes[i] = m_buffers[i].size();
  }
  /* Initialize the array in READ mode. */
  auto status = tiledb_array_iterator_init(
      tiledb_ctx, 
      &m_tiledb_array_iterator,
      array_path.c_str(),
      TILEDB_ARRAY_READ,
      reinterpret_cast<const void*>(range), // range, 
      &(attribute_names[0]),           
      attribute_names.size(),
      const_cast<void**>(&(m_buffer_pointers[0])),
      &(m_buffer_sizes[0]));      
  VERIFY_OR_THROW(status == TILEDB_OK && "Error while initializing TileDB iterator");
#ifdef DEBUG
  m_last_row = -1;
  m_last_column = -1;
  m_num_cells_iterated_over = 0ull;
#endif
#ifdef DO_PROFILING
  m_tiledb_timer.stop();
#endif
}

const BufferVariantCell& VariantArrayCellIterator::operator*()
{
#ifdef DO_PROFILING
  m_tiledb_to_buffer_cell_timer.start();
#endif
  const uint8_t* field_ptr = 0;
  size_t field_size = 0u;
  for(auto i=0u;i<m_num_queried_attributes;++i)
  {
    auto status = tiledb_array_iterator_get_value(m_tiledb_array_iterator, i,
        reinterpret_cast<const void**>(&field_ptr), &field_size);
    VERIFY_OR_THROW(status == TILEDB_OK);
    m_cell.set_field_ptr_for_query_idx(i, field_ptr);
    m_cell.set_field_size_in_bytes(i, field_size);
  }
  //Co-ordinates
  auto status = tiledb_array_iterator_get_value(m_tiledb_array_iterator, m_num_queried_attributes,
        reinterpret_cast<const void**>(&field_ptr), &field_size);
  VERIFY_OR_THROW(status == TILEDB_OK);
  assert(field_size == m_variant_array_schema->dim_size_in_bytes());
  auto coords_ptr = reinterpret_cast<const int64_t*>(field_ptr);
  m_cell.set_coordinates(coords_ptr[0], coords_ptr[1]);
#ifdef DO_PROFILING
  m_tiledb_to_buffer_cell_timer.stop();
#endif
  return m_cell;
}

//VariantArrayInfo functions
VariantArrayInfo::VariantArrayInfo(int idx, int mode,
    const std::string& workspace, const std::string& name,
    const VidMapper* vid_mapper,
    const VariantArraySchema& schema,
    TileDB_CTX* tiledb_ctx,
    TileDB_Array* tiledb_array, const std::string& metadata_filename,
    const size_t buffer_size)
: m_idx(idx), m_mode(mode),
  m_workspace(workspace), m_name(name),
  m_vid_mapper(vid_mapper), m_schema(schema),m_tiledb_ctx(tiledb_ctx), m_cell(m_schema), m_tiledb_array(tiledb_array),
  m_metadata_filename(metadata_filename)
{
  //If writing, allocate buffers
  if(mode == TILEDB_ARRAY_WRITE || mode == TILEDB_ARRAY_WRITE_UNSORTED)
  {
    m_buffers.clear();
    for(auto i=0ull;i<schema.attribute_num();++i)
    {
      //For varible length attributes, need extra buffer for maintaining offsets
      if(m_schema.is_variable_length_field(i))
        m_buffers.emplace_back(buffer_size);
      m_buffers.emplace_back(buffer_size);
    }
    //Co-ordinates
    m_buffers.emplace_back(buffer_size);
    //Initialize pointers to buffers
    m_buffer_pointers.resize(m_buffers.size());
    m_buffer_offsets.resize(m_buffers.size());
    for(auto i=0ull;i<m_buffers.size();++i)
    {
      m_buffer_pointers[i] = reinterpret_cast<void*>(&(m_buffers[i][0]));
      m_buffer_offsets[i] = 0ull; //will be modified during a write
    }
  }
  read_row_bounds_from_metadata();
#ifdef DEBUG
  m_last_row = m_last_column = -1;
#endif
}

//Move constructor
VariantArrayInfo::VariantArrayInfo(VariantArrayInfo&& other)
  : m_schema(std::move(other.m_schema)), m_cell(std::move(other.m_cell))
{
  m_idx = other.m_idx;
  m_mode = other.m_mode;
  m_workspace = std::move(other.m_workspace);
  m_name = std::move(other.m_name);
  m_metadata_filename = std::move(other.m_metadata_filename);
  //Pointer handling
  m_tiledb_array = other.m_tiledb_array;
  other.m_tiledb_array = 0;
  m_vid_mapper = other.m_vid_mapper;
  other.m_vid_mapper = 0;
  //Point array schema to this array schema
  m_cell.set_variant_array_schema(m_schema);
  //Move other members
  m_buffers = std::move(other.m_buffers);
  m_buffer_offsets = std::move(other.m_buffer_offsets);
  m_buffer_pointers = std::move(other.m_buffer_pointers);
  for(auto i=0ull;i<m_buffer_pointers.size();++i)
    m_buffer_pointers[i] = reinterpret_cast<void*>(&(m_buffers[i][0]));
  m_metadata_contains_max_valid_row_idx_in_array = other.m_metadata_contains_max_valid_row_idx_in_array;
  m_max_valid_row_idx_in_array = other.m_max_valid_row_idx_in_array;
#ifdef DEBUG
  m_last_row = other.m_last_row;
  m_last_column = other.m_last_column;
#endif
}

void VariantArrayInfo::write_cell(const void* ptr)
{
  m_cell.set_cell(ptr);
#ifdef DEBUG
  assert((m_cell.get_begin_column() > m_last_column) || (m_cell.get_begin_column() == m_last_column && m_cell.get_row() > m_last_row));
  m_last_row = m_cell.get_row();
  m_last_column = m_cell.get_begin_column();
#endif
  auto buffer_idx = 0ull;
  auto overflow = false;
  //First check if the current cell will fit into the buffers
  for(auto i=0ull;i<m_schema.attribute_num();++i)
  {
    assert(buffer_idx < m_buffer_pointers.size());
    //Specify offsets in the buffer for variable length fields
    if(m_schema.is_variable_length_field(i))
    {
      if(m_buffer_offsets[buffer_idx]+sizeof(size_t) > m_buffers[buffer_idx].size())
      {
        overflow = true;
        break;
      }
      ++buffer_idx;
    }
    if(m_buffer_offsets[buffer_idx]+m_cell.get_field_size_in_bytes(i) > m_buffers[buffer_idx].size())
    {
      overflow = true;
      break;
    }
    ++buffer_idx;
  }
  //Check if co-ordinates buffer overflows
  auto coords_buffer_idx = m_buffers.size()-1u;
  auto coords_size = m_schema.dim_size_in_bytes();
  overflow = overflow || (m_buffer_offsets[coords_buffer_idx]+coords_size > m_buffers[coords_buffer_idx].size());
  //write to array and reset sizes
  if(overflow)
  {
    auto status = tiledb_array_write(m_tiledb_array, const_cast<const void**>(&(m_buffer_pointers[0])), &(m_buffer_offsets[0]));
    VERIFY_OR_THROW(status == TILEDB_OK);
    memset(&(m_buffer_offsets[0]), 0, m_buffer_offsets.size()*sizeof(size_t));
  }
  buffer_idx = 0;
  for(auto i=0ull;i<m_schema.attribute_num();++i)
  {
    assert(buffer_idx < m_buffer_pointers.size());
    //Specify offsets in the buffer for variable length fields
    if(m_schema.is_variable_length_field(i))
    {
      assert(buffer_idx+1u < m_buffer_offsets.size());
      //This could happen if segment_size in the loader JSON is so small that the data for a single cell
      //does not fit into the buffer
      if(overflow && m_buffer_offsets[buffer_idx]+sizeof(size_t) > m_buffers[buffer_idx].size())
      {
        m_buffers[buffer_idx].resize(m_buffer_offsets[buffer_idx]+sizeof(size_t));
        m_buffer_pointers[buffer_idx] = reinterpret_cast<void*>(&(m_buffers[buffer_idx][0u]));
      }
      assert(m_buffer_offsets[buffer_idx]+sizeof(size_t) <= m_buffers[buffer_idx].size());
      //Offset buffer - new entry starts after last entry
      *(reinterpret_cast<size_t*>(&(m_buffers[buffer_idx][m_buffer_offsets[buffer_idx]]))) = m_buffer_offsets[buffer_idx+1u];
      m_buffer_offsets[buffer_idx] += sizeof(size_t);
      ++buffer_idx;
    }
    auto field_size = m_cell.get_field_size_in_bytes(i);
    //This could happen if segment_size in the loader JSON is so small that the data for a single cell
    //does not fit into the buffer
    if(overflow && m_buffer_offsets[buffer_idx]+field_size > m_buffers[buffer_idx].size())
    {
      m_buffers[buffer_idx].resize(m_buffer_offsets[buffer_idx]+field_size);
      m_buffer_pointers[buffer_idx] = reinterpret_cast<void*>(&(m_buffers[buffer_idx][0u]));
    }
    assert(m_buffer_offsets[buffer_idx]+field_size <= m_buffers[buffer_idx].size());
    memcpy(&(m_buffers[buffer_idx][m_buffer_offsets[buffer_idx]]), m_cell.get_field_ptr_for_query_idx<void>(i), field_size);
    m_buffer_offsets[buffer_idx] += field_size;
    ++buffer_idx;
  }
  //Co-ordinates
  assert(buffer_idx == coords_buffer_idx);
  assert(m_buffer_offsets[coords_buffer_idx]+coords_size <= m_buffers[coords_buffer_idx].size());
  memcpy(&(m_buffers[coords_buffer_idx][m_buffer_offsets[coords_buffer_idx]]), ptr, coords_size);
  m_buffer_offsets[coords_buffer_idx] += coords_size;
}

void VariantArrayInfo::read_row_bounds_from_metadata()
{
  //Compute value from array schema
  m_metadata_contains_max_valid_row_idx_in_array = false;
  const auto& dim_domains = m_schema.dim_domains();
  m_max_valid_row_idx_in_array = dim_domains[0].second;

  // Check if metadata is cached
  auto search = metadata_cache.find(m_metadata_filename);
  if(search != metadata_cache.end()) {
    m_max_valid_row_idx_in_array = search->second;
    m_metadata_contains_max_valid_row_idx_in_array = true;
    return;
  }
  
  //Try reading from metadata
  if (is_file(m_tiledb_ctx, m_metadata_filename)) {
    size_t size = file_size(m_tiledb_ctx, m_metadata_filename);
    if (size > 0) {
       char *buffer = (char *)malloc(size+1);
       if (!buffer) 
         throw VariantStorageManagerException(std::string("Out-of-memory exception while allocating memory"));
       memset(buffer, 0, size+1); 

       VERIFY_OR_THROW(read_from_file(m_tiledb_ctx, m_metadata_filename, 0, buffer, size) == TILEDB_OK);
       VERIFY_OR_THROW(close_file(m_tiledb_ctx, m_metadata_filename) == TILEDB_OK);

       rapidjson::Document json_doc;
       json_doc.Parse(buffer);
       if(json_doc.HasParseError())
         throw VariantStorageManagerException(std::string("Syntax error in corrupted JSON metadata file ")+m_metadata_filename);

       if(json_doc.HasMember("max_valid_row_idx_in_array") && json_doc["max_valid_row_idx_in_array"].IsInt64())
       {
         m_max_valid_row_idx_in_array = json_doc["max_valid_row_idx_in_array"].GetInt64();
         m_metadata_contains_max_valid_row_idx_in_array = true;
         
         // Update Metadata Cache
         metadata_cache_mtx.lock();
         auto search = metadata_cache.find(m_metadata_filename);
         if(search == metadata_cache.end()) {
           metadata_cache.insert({m_metadata_filename, m_max_valid_row_idx_in_array});
         }
         metadata_cache_mtx.unlock();
       }
       free(buffer);
    }
  }
}

void VariantArrayInfo::update_row_bounds_in_array(TileDB_CTX* tiledb_ctx, const std::string& metadata_filename,
    const int64_t lb_row_idx, const int64_t max_valid_row_idx_in_array)
{
  //Update metadata if:
  // (it did not exist and (#valid rows is set to large value or  num_rows_seen > num rows as defined in schema
  //                (old implementation))  OR
  // (num_rows_seen > #valid rows in metadata (this part implies that metadata file exists)
  if((!m_metadata_contains_max_valid_row_idx_in_array &&
        (m_max_valid_row_idx_in_array == INT64_MAX-1 || max_valid_row_idx_in_array > m_max_valid_row_idx_in_array))
      || (max_valid_row_idx_in_array > m_max_valid_row_idx_in_array))
  {
    m_max_valid_row_idx_in_array = max_valid_row_idx_in_array;
    
    rapidjson::Document json_doc;
    json_doc.SetObject();
    json_doc.AddMember("lb_row_idx", lb_row_idx, json_doc.GetAllocator());
    json_doc.AddMember("max_valid_row_idx_in_array", max_valid_row_idx_in_array, json_doc.GetAllocator());
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    json_doc.Accept(writer);

    if (is_file(m_tiledb_ctx, metadata_filename)) {
        VERIFY_OR_THROW(delete_file(m_tiledb_ctx, metadata_filename) == TILEDB_OK);
    }

    VERIFY_OR_THROW(write_to_file(m_tiledb_ctx, metadata_filename, buffer.GetString(), strlen(buffer.GetString())) == TILEDB_OK);
    VERIFY_OR_THROW(close_file(m_tiledb_ctx, metadata_filename) == TILEDB_OK);

    // Update metadata cache
    metadata_cache_mtx.lock();
    auto search = metadata_cache.find(m_metadata_filename);
    if(search != metadata_cache.end()) {
      metadata_cache.insert({m_metadata_filename, m_max_valid_row_idx_in_array});
    } else {
      metadata_cache[m_metadata_filename] = m_max_valid_row_idx_in_array;
    }
    metadata_cache_mtx.unlock();
  }
}

void VariantArrayInfo::close_array(const bool consolidate_tiledb_array)
{
  auto coords_buffer_idx = m_buffers.size()-1u;
  if((m_mode == TILEDB_ARRAY_WRITE || m_mode == TILEDB_ARRAY_WRITE_UNSORTED))
  {
    //Flush cells in buffer
    if(m_buffer_offsets[coords_buffer_idx] > 0ull)
    {
      auto status = tiledb_array_write(m_tiledb_array, const_cast<const void**>(&(m_buffer_pointers[0])), &(m_buffer_offsets[0]));
      if(status != TILEDB_OK)
        throw VariantStorageManagerException("Error while writing to array "+m_name);
      memset(&(m_buffer_offsets[0]), 0, m_buffer_offsets.size()*sizeof(size_t));
    }
    auto sync_status = tiledb_array_sync(m_tiledb_array);
    if(sync_status != TILEDB_OK)
      throw VariantStorageManagerException("Error while syncing array "+m_name+" to disk");
  }
  if(m_tiledb_array)
  {
    auto status = tiledb_array_finalize(m_tiledb_array);
    if(status != TILEDB_OK)
      throw VariantStorageManagerException("Error while finalizing TileDB array "+m_name);
    if(consolidate_tiledb_array)
    {
      auto status = tiledb_array_consolidate(m_tiledb_ctx, (m_workspace + '/' + m_name).c_str());
      if(status != TILEDB_OK)
        throw VariantStorageManagerException("Error while consolidating TileDB array "+m_name);
    }
  }
  m_tiledb_array = 0;
  m_name.clear();
  m_mode = -1;
}

int initialize_storage(TileDB_CTX **ptiledb_ctx, const std::string& workspace)
{
  int rc;

  if (is_hdfs_path(workspace)) 
  {
    TileDB_Config tiledb_config;
    bzero(&tiledb_config, sizeof(TileDB_Config));
    
    std::string parent = parent_dir(workspace);
    tiledb_config.home_ = new char [parent.length()+1];
    std::strcpy ((char *)tiledb_config.home_, parent.c_str());
    tiledb_config.read_method_ = TILEDB_IO_READ;
    tiledb_config.write_method_ = TILEDB_IO_WRITE;
    rc = tiledb_ctx_init(ptiledb_ctx, &tiledb_config);
    delete tiledb_config.home_;
  } else {
    rc = tiledb_ctx_init(ptiledb_ctx, NULL);
  }

  if (rc == TILEDB_OK && !is_workspace(*ptiledb_ctx, workspace))
  {
    rc = tiledb_workspace_create(*ptiledb_ctx, workspace.c_str());
    if (rc != TILEDB_OK) {
      finalize_storage(*ptiledb_ctx);
    }
  }

  return rc;
}

int finalize_storage(TileDB_CTX *tiledb_ctx)
{
  return tiledb_ctx_finalize(tiledb_ctx);
}

//VariantStorageManager functions
VariantStorageManager::VariantStorageManager(const std::string& workspace, const unsigned segment_size)
{
  m_workspace = workspace;
  m_segment_size = segment_size;

  VERIFY_OR_THROW(initialize_storage(&m_tiledb_ctx, workspace) == TILEDB_OK && "Failure to initialize TileDB storage");
  VERIFY_OR_THROW(m_tiledb_ctx != NULL && "Could not get TileDB context");
}

int VariantStorageManager::open_array(const std::string& array_name, const VidMapper* vid_mapper, const char* mode,
    const int tiledb_zlib_compression_level)
{
  auto mode_iter = VariantStorageManager::m_mode_string_to_int.find(mode);
  VERIFY_OR_THROW(mode_iter != VariantStorageManager::m_mode_string_to_int.end() && "Unknown mode of opening an array");
  auto mode_int = (*mode_iter).second;
  if(is_array(m_tiledb_ctx, m_workspace + '/' + array_name))
  {
    //Try to open the array
    TileDB_Array* tiledb_array;
    auto status = tiledb_array_init(
        m_tiledb_ctx, 
        &tiledb_array,
        (m_workspace+'/'+array_name).c_str(),
        mode_int, NULL,
        0, 0);
    if(status == TILEDB_OK)
    {
      auto idx = m_open_arrays_info_vector.size();
      //Schema
      VariantArraySchema tmp_schema;
      get_array_schema(array_name, &tmp_schema);     
      m_open_arrays_info_vector.emplace_back(idx, mode_int,
          m_workspace, array_name,
          vid_mapper, tmp_schema,
          m_tiledb_ctx, tiledb_array,
          GET_METADATA_PATH(m_workspace, array_name), m_segment_size);
      tiledb_array_set_zlib_compression_level(tiledb_array, tiledb_zlib_compression_level);
      return idx;
    }
  }
  return -1;
}

void VariantStorageManager::close_array(const int ad, const bool consolidate_tiledb_array)
{
  VERIFY_OR_THROW(static_cast<size_t>(ad) < m_open_arrays_info_vector.size() &&
      m_open_arrays_info_vector[ad].get_array_name().length());
  m_open_arrays_info_vector[ad].close_array(consolidate_tiledb_array);
}

int VariantStorageManager::define_array(const VariantArraySchema* variant_array_schema, const size_t num_cells_per_tile)
{
  //Attribute attributes
  std::vector<const char*> attribute_names(variant_array_schema->attribute_num());
  std::vector<int> cell_val_num(variant_array_schema->attribute_num());
  std::vector<int> types(variant_array_schema->attribute_num()+1u);  //+1 for the co-ordinates
  std::vector<int> compression(variant_array_schema->attribute_num()+1u);  //+1 for the co-ordinates
  for(auto i=0ull;i<variant_array_schema->attribute_num();++i)
  {
    attribute_names[i] = variant_array_schema->attribute_name(i).c_str();
    cell_val_num[i] = variant_array_schema->val_num(i);
    types[i] = VariantFieldTypeUtil::get_tiledb_type_for_variant_field_type(variant_array_schema->type(i));
    compression[i] = variant_array_schema->compression(i);
  }
  //Co-ordinates
  types[variant_array_schema->attribute_num()] = VariantFieldTypeUtil::get_tiledb_type_for_variant_field_type(
      variant_array_schema->dim_type());
  compression[variant_array_schema->attribute_num()] = variant_array_schema->dim_compression_type();
  std::vector<const char*> dim_names(variant_array_schema->dim_names().size());
  std::vector<int64_t> dim_domains(2u*dim_names.size());
  for(auto i=0ull;i<dim_names.size();++i)
  {
    dim_names[i] = variant_array_schema->dim_names()[i].c_str();
    dim_domains[2u*i] = variant_array_schema->dim_domains()[i].first;
    dim_domains[2u*i+1u] = variant_array_schema->dim_domains()[i].second;
  }
  //TileDB C API
  TileDB_ArraySchema array_schema;
  memset(&array_schema, 0, sizeof(TileDB_ArraySchema));
  tiledb_array_set_schema(
      // The array schema struct
      &array_schema,
      // Array name
      (m_workspace+'/'+variant_array_schema->array_name()).c_str(),
      // Attributes
      &(attribute_names[0]),
      // Number of attributes
      attribute_names.size(),
      // Capacity
      num_cells_per_tile,
      // Cell order
      TILEDB_COL_MAJOR,
      // Number of cell values per attribute (NULL means 1 everywhere)
      &(cell_val_num[0]),
      // Compression
      &(compression[0]),
      // Sparse array
      0,
      // Dimensions
      &(dim_names[0]),
      // Number of dimensions
      dim_names.size(),
       // Domain
      &(dim_domains[0]),
      // Domain length in bytes
      dim_domains.size()*sizeof(int64_t),
      // Tile extents (no regular tiles defined)
      NULL,
      // Tile extents in bytes
      0, 
      // Tile order (0 means ignore in sparse arrays and default in dense)
      0,
      // Types
      &(types[0])
  );
  /* Create the array schema */
  auto status = tiledb_array_create(m_tiledb_ctx, &array_schema);
  if(status == TILEDB_OK)
  {
    status = tiledb_array_free_schema(&array_schema);
  }
  return status;
}

void VariantStorageManager::delete_array(const std::string& array_name)
{
  if(is_array(m_tiledb_ctx, m_workspace + '/' + array_name))
  {
    if (is_file(m_tiledb_ctx, GET_METADATA_PATH(m_workspace, array_name)))
        VERIFY_OR_THROW(delete_file(m_tiledb_ctx, GET_METADATA_PATH(m_workspace, array_name)) == TILEDB_OK);
    VERIFY_OR_THROW(tiledb_delete(m_tiledb_ctx, (m_workspace+"/"+array_name).c_str()) == TILEDB_OK);
  }
}

int VariantStorageManager::get_array_schema(const int ad, VariantArraySchema* variant_array_schema)
{
  VERIFY_OR_THROW(static_cast<size_t>(ad) < m_open_arrays_info_vector.size() &&
      m_open_arrays_info_vector[ad].get_array_name().length());
  auto status = get_array_schema(m_open_arrays_info_vector[ad].get_array_name(), variant_array_schema);
  if(status == TILEDB_OK)
    m_open_arrays_info_vector[ad].set_schema(*variant_array_schema);
  return status;
}

int VariantStorageManager::get_array_schema(const std::string& array_name, VariantArraySchema* variant_array_schema)
{
  // Get array schema
  TileDB_ArraySchema tiledb_array_schema;
  memset(&tiledb_array_schema, 0, sizeof(TileDB_ArraySchema));
  auto status = tiledb_array_load_schema(m_tiledb_ctx, (m_workspace+'/'+array_name).c_str(),
    &tiledb_array_schema);
  if(status != TILEDB_OK)
    return -1;
  //Attribute attributes
  std::vector<std::string> attribute_names(tiledb_array_schema.attribute_num_);
  std::vector<int> val_num(tiledb_array_schema.attribute_num_);
  std::vector<std::type_index> attribute_types(tiledb_array_schema.attribute_num_+1u, std::type_index(typeid(void)));//+1 for co-ordinates
  std::vector<int> compression(tiledb_array_schema.attribute_num_+1u);  //+1 for co-ordinates
  for(auto i=0u;i<attribute_names.size();++i)
  {
    attribute_names[i] = tiledb_array_schema.attributes_[i];
    val_num[i] = tiledb_array_schema.cell_val_num_[i];
    attribute_types[i] = VariantFieldTypeUtil::get_variant_field_type_for_tiledb_type(tiledb_array_schema.types_[i]);
    compression[i] = tiledb_array_schema.compression_[i];
  }
  //Co-ords
  auto coords_idx = tiledb_array_schema.attribute_num_;
  attribute_types[coords_idx] = std::type_index(typeid(int64_t));
  compression[coords_idx] = tiledb_array_schema.compression_[coords_idx];
  std::vector<std::string> dim_names(tiledb_array_schema.dim_num_);
  auto dim_domains = std::vector<std::pair<int64_t,int64_t>>(tiledb_array_schema.dim_num_);
  auto dim_domains_ptr = reinterpret_cast<const int64_t*>(tiledb_array_schema.domain_);
  for(auto i=0;i<tiledb_array_schema.dim_num_;++i)
  {
    dim_names[i] = tiledb_array_schema.dimensions_[i];
    dim_domains[i].first = dim_domains_ptr[2*i];
    dim_domains[i].second = dim_domains_ptr[2*i+1];
  }
  *variant_array_schema = std::move(VariantArraySchema(
        array_name,
        attribute_names,
        dim_names,
        dim_domains,
        attribute_types,
        val_num, 
        compression,
        TILEDB_COL_MAJOR));
  // Free array schema
  tiledb_array_free_schema(&tiledb_array_schema);
  return TILEDB_OK;
}

VariantArrayCellIterator* VariantStorageManager::begin(
    int ad, const int64_t* range, const std::vector<int>& attribute_ids) const
{
  VERIFY_OR_THROW(static_cast<size_t>(ad) < m_open_arrays_info_vector.size() &&
      m_open_arrays_info_vector[ad].get_array_name().length());
  auto& curr_elem = m_open_arrays_info_vector[ad];
  return new VariantArrayCellIterator(m_tiledb_ctx, curr_elem.get_schema(), m_workspace+'/'+curr_elem.get_array_name(),
      range, attribute_ids, m_segment_size);   
}

SingleCellTileDBIterator* VariantStorageManager::begin_columnar_iterator(
    int ad, const VariantQueryConfig& query_config, const bool use_common_array_object) const
{
  VERIFY_OR_THROW(static_cast<size_t>(ad) < m_open_arrays_info_vector.size() &&
      m_open_arrays_info_vector[ad].get_array_name().length());
  auto& curr_elem = m_open_arrays_info_vector[ad];
  return new SingleCellTileDBIterator(m_tiledb_ctx,
      use_common_array_object ? m_open_arrays_info_vector[ad].get_tiledb_array() : 0,
      curr_elem.get_vid_mapper(), curr_elem.get_schema(),
      m_workspace+'/'+curr_elem.get_array_name(),
      query_config, m_segment_size);
}

void VariantStorageManager::write_cell_sorted(const int ad, const void* ptr)
{
  assert(static_cast<size_t>(ad) < m_open_arrays_info_vector.size() &&
      m_open_arrays_info_vector[ad].get_array_name().length());
  m_open_arrays_info_vector[ad].write_cell(ptr);
}

int64_t VariantStorageManager::get_num_valid_rows_in_array(const int ad) const
{
  assert(static_cast<size_t>(ad) < m_open_arrays_info_vector.size() &&
      m_open_arrays_info_vector[ad].get_array_name().length());
  return m_open_arrays_info_vector[ad].get_num_valid_rows_in_array();
}

void VariantStorageManager::update_row_bounds_in_array(const int ad, const int64_t lb_row_idx, const int64_t max_valid_row_idx_in_array)
{
  assert(static_cast<size_t>(ad) < m_open_arrays_info_vector.size() &&
      m_open_arrays_info_vector[ad].get_array_name().length());
  m_open_arrays_info_vector[ad].update_row_bounds_in_array(m_tiledb_ctx,
      GET_METADATA_PATH(m_workspace,m_open_arrays_info_vector[ad].get_array_name()), lb_row_idx, max_valid_row_idx_in_array);
}
