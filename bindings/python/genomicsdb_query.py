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
        # set return type for library function calls
        gdb_dll.genomicsdb_create_config.restype = c_void_p
        gdb_dll.genomicsdb_connect.restype = c_void_p
        # get pointer to config instance 
        config = gdb_dll.genomicsdb_create_config(byref(create_string_buffer(loader_config_file)),
                                                  byref(create_string_buffer(query_config_file)),
                                                  byref(create_string_buffer(chr)),
                                                  byref(c_int(start)), byref(c_int(end)))
        if (rank is not None):
                gdb_dll.genomicsdb_config_set_rank(byref(c_void_p(config)), c_int(rank))
        if (buffer_capacity is not None):
                gdb_dll.genomicsdb_config_set_buffer_capacity(byref(c_void_p(config)), c_ulonglong(buffer_capacity))
        if (segment_size is not None):
                gdb_dll.genomicsdb_config_set_segment_size(byref(c_void_p(config)), c_ulonglong(segment_size))
        if (is_bcf is not None):
                gdb_dll.genomicsdb_config_set_is_bcf(byref(c_void_p(config)), c_int(is_bcf))
        if (produce_header_only is not None):
                gdb_dll.genomicsdb_config_set_produce_header_only(config, c_int(produce_header_only))
        if (use_missing_values_only_not_vector_end is not None):
                gdb_dll.genomicsdb_config_set_use_missing_values_only_not_vector_end(byref(c_void_p(config)),
                                                                    c_int(use_missing_values_only_not_vector_end))
        gdb_dll.genomicsdb_print_config(byref(c_void_p(config)))
        return gdb_dll.genomicsdb_connect(byref(c_void_p(config)))


def close(handle):
    gdb_dll.genomicsdb_close(byref(c_void_p(handle)))


def total_bytes_available(handle):
    gdb_dll.genomicsdb_get_total_bytes_available.restype = c_uint64
    return gdb_dll.genomicsdb_get_total_bytes_available(byref(c_void_p(handle)))


def read_next_byte(handle):
    return gdb_dll.genomicsdb_read_next_byte(byref(c_void_p(handle)))


def read(handle, byte_array, offset, n):
    return gdb_dll.genomicsdb_read(byref(c_void_p(handle)), c_char_p(byte_array), c_ulong_long(offset), c_int(n))


def skip(handle, n):
    return gdb_dll.genomicsdb_skip(byref(c_void_p(handle)), c_int(n))
