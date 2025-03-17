import pylibcudf as plc
import rmm
import os

# Force GDS
os.environ["KVIKIO_COMPAT_MODE"] = "OFF"
os.environ["KVIKIO_NTHREADS"] = "32"

pool = rmm.mr.PoolMemoryResource(rmm.mr.CudaMemoryResource(),initial_pool_size="24GiB", maximum_pool_size="39GiB")
rmm.mr.set_current_device_resource(pool)

file = "/raid/jluo/lineitem/parquet/comp-snappy_rowgroup-1000000_pagerow-50000_dict-false_stats-chunk.parquet"
source = plc.io.SourceInfo([file])
meta = plc.io.parquet_metadata.read_parquet_metadata(source)
ptr = meta.get_aggregate_reader_metadata_ptr()

for i in range(10):
    options = plc.io.parquet.ParquetReaderOptions.builder(
        plc.io.SourceInfo([file])
    ).build()
    options.set_columns(["l_orderkey"])
    plc_table_w_meta = plc.io.parquet.read_parquet(options)

for i in range(10):
    options = plc.io.parquet.ParquetReaderOptions.builder(
        plc.io.SourceInfo([file])
    ).build()
    options.set_columns(["l_orderkey"])
    options.set_aggregate_reader_metadata(ptr)
    plc_table_w_meta = plc.io.parquet.read_parquet(options)

print("END")