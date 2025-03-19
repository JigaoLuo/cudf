# import pylibcudf as plc
# file = "/raid/jluo/parquet_sf100/lineitem_l_orderkey_sf100_SNAPPY.parquet"
# options = plc.io.parquet.ParquetReaderOptions.builder(plc.io.SourceInfo([file])).build()
# plc.io.parquet.read_parquet(options)
# print("END")

# import pylibcudf as plc
# import rmm
# import os
# from rmm.pylibrmm.stream import Stream, DEFAULT_STREAM
# import threading

# pool = rmm.mr.PoolMemoryResource(rmm.mr.CudaMemoryResource(),initial_pool_size="10GiB", maximum_pool_size="30GiB")
# rmm.mr.set_current_device_resource(pool)
# file = "/raid/jluo/lineitem/parquet/comp-snappy_rowgroup-1000000_pagerow-50000_dict-false_stats-chunk.parquet"

# def thread_read(file):
#     th_stream = Stream()
#     for i in range(10):
#         options = plc.io.parquet.ParquetReaderOptions.builder(
#             plc.io.SourceInfo([file])
#         ).build()
#         options.set_columns(["l_orderkey"])
#         plc.io.parquet.read_parquet(options, th_stream)
#     th_stream.synchronize()
#     DEFAULT_STREAM.synchronize()

# t1 = threading.Thread(target=thread_read, args=(file,))
# t2 = threading.Thread(target=thread_read, args=(file,))
# t1.start()
# t2.start()
# t1.join()
# t2.join()
# print("END")


import pylibcudf as plc
import rmm
import os
from rmm.pylibrmm.stream import Stream, DEFAULT_STREAM
import threading

pool = rmm.mr.PoolMemoryResource(rmm.mr.CudaMemoryResource(),initial_pool_size="10GiB", maximum_pool_size="30GiB")
rmm.mr.set_current_device_resource(pool)
file = "/raid/jluo/lineitem/parquet/comp-snappy_rowgroup-1000000_pagerow-50000_dict-false_stats-chunk.parquet"

source = plc.io.SourceInfo([file])
meta = plc.io.parquet_metadata.read_parquet_metadata(source)
ptr = meta.get_aggregate_reader_metadata_ptr()

def thread_read(file, ptr):
    th_stream = Stream()
    for i in range(10):
        options = plc.io.parquet.ParquetReaderOptions.builder(
            plc.io.SourceInfo([file])
        ).build()
        options.set_columns(["l_orderkey"])
        options.set_aggregate_reader_metadata(ptr)
        plc.io.parquet.read_parquet(options, th_stream)
    th_stream.synchronize()
    DEFAULT_STREAM.synchronize()

t1 = threading.Thread(target=thread_read, args=(file, ptr,))
t2 = threading.Thread(target=thread_read, args=(file, ptr,))
t1.start()
t2.start()
t1.join()
t2.join()
print("END")



# import pylibcudf as plc
# # import rmm
# # import os
# # from rmm.pylibrmm.stream import Stream, DEFAULT_STREAM
# # import threading

# # pool = rmm.mr.PoolMemoryResource(rmm.mr.CudaMemoryResource(),initial_pool_size="1GiB", maximum_pool_size="20GiB")
# # rmm.mr.set_current_device_resource(pool)
# file = "/raid/jluo/parquet_sf100/lineitem_l_orderkey_sf100_SNAPPY.parquet"
# # file = "/raid/jluo/lineitem/parquet/comp-snappy_rowgroup-1000000_pagerow-50000_dict-false_stats-chunk.parquet"
# # source = plc.io.SourceInfo([file])
# # meta = plc.io.parquet_metadata.read_parquet_metadata(source)
# # ptr = meta.get_aggregate_reader_metadata_ptr()

# # for i in range(10):
# #     options = plc.io.parquet.ParquetReaderOptions.builder(
# #         plc.io.SourceInfo([file])
# #     ).build()
# #     options.set_columns(["l_orderkey"])
# #     plc_table_w_meta = plc.io.parquet.read_parquet(options)

# # for i in range(10):
# #     options = plc.io.parquet.ParquetReaderOptions.builder(
# #         plc.io.SourceInfo([file])
# #     ).build()
# #     options.set_columns(["l_orderkey"])
# #     options.set_aggregate_reader_metadata(ptr)
# #     plc_table_w_meta = plc.io.parquet.read_parquet(options)

# # def thread_read(file):
# #     th_stream = Stream()
# #     for i in range(10):
# #         options = plc.io.parquet.ParquetReaderOptions.builder(
# #             plc.io.SourceInfo([file])
# #         ).build()
# #         options.set_columns(["l_orderkey"])
# #         plc_table_w_meta0 = plc.io.parquet.read_parquet(options, th_stream)
# #     th_stream.synchronize()

# # t1 = threading.Thread(target=thread_read, args=(file,))
# # t2 = threading.Thread(target=thread_read, args=(file,))
# # t1.start()
# # t2.start()
# # t1.join()
# # t2.join()

# # th_stream = Stream()
# # th_stream = DEFAULT_STREAM
# options = plc.io.parquet.ParquetReaderOptions.builder(plc.io.SourceInfo([file])).build()
# # options.set_columns(["l_orderkey"])
# plc.io.parquet.read_parquet(options)
# # plc_table_w_meta = plc.io.parquet.read_parquet(options, th_stream)
# # th_stream.synchronize()

# # DEFAULT_STREAM.synchronize()
# print("END")