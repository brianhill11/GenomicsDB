from ctypes import *
gdb_dll = CDLL("libtiledbgenomicsdb.so")

def connect(
    loader_config_file,
    query_config_file,
    chr,
    start,
    end,
    rank=None,
    buffer_capacity=None,
    segment_size=None,
    is_bcf=None,
    produce_header_only=None,
    use_missing_values_only_not_vector_end=None,
    keep_idx_fields_in_bcf_header=None):
        config = gdb_dll.genomicsdb_create_config(c_char_p(loader_config_file), c_char_p(query_config_file), c_char_p(chr), c_int(start), c_int(end))
        if (rank is not None):
                gdb_dll.genomicsdb_config_set_rank(config, c_int(rank))
        if (buffer_capacity is not None):
                gdb_dll.genomicsdb_config_set_buffer_capacity(config, c_ulonglong(buffer_capacity))
        if (segment_size is not None):
                gdb_dll.genomicsdb_config_set_segment_size(config, c_ulonglong(segment_size))
        if (is_bcf is not None):
                gdb_dll.genomicsdb_config_set_is_bcf(config, c_int(is_bcf))
        if (produce_header_only is not None):
                gdb_dll.genomicsdb_config_set_produce_header_only(config, c_int(produce_header_only))
        if (use_missing_values_only_not_vector_end is not None):
                gdb_dll.genomicsdb_config_set_use_missing_values_only_not_vector_end(config, c_int(use_missing_values_only_not_vector_end))
        gdb_dll.genomicsdb_print_config(config)
	return gdb_dll.genomicsdb_connect(config)

def close(handle):
	gdb_dll.genomicsdb_close(handle)
	
def total_bytes_available(handle):
	return gdb_dll.genomicsdb_get_total_bytes_available(handle)

def read_next_byte(handle):
	return gdb_dll.genomicsdb_read_next_byte(handle)

def read(handle, byte_array, offset, n):
	return gdb_dll.genomicsdb_read(handle, c_char_p(byte_array), c_ulong_long(offset), c_int(n))

def skip(handle, n):
	return gdb_dll.genomicsdb_skip(handle, c_int(n))
