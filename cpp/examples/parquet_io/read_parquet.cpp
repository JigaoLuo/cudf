// export KVIKIO_COMPAT_MODE=OFF
// export KVIKIO_NTHREADS=16

#include "../utilities/timer.hpp"
#include "common_utils.hpp"
#include "io_source.hpp"

#include <cudf/io/parquet.hpp>
#include <cudf/sorting.hpp>
#include <rmm/mr/device/pool_memory_resource.hpp>
#include <rmm/cuda_stream_pool.hpp>
#include <cuda_runtime.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <filesystem>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <regex>
#include <thread>
#include <random>

void print_parquet_metadata(const cudf::io::parquet_metadata& metadata) {
    // HARD TO PRINT
    //std::cout << "Schema: " << std::endl;
    //auto schema = metadata.schema();

    std::cout << "Number of rows: " << metadata.num_rows() << std::endl;

    std::cout << "Number of row groups: " << metadata.num_rowgroups() << std::endl;

    // Print file-level metadata (key-value pairs)
    std::cout << "File metadata (key-value):" << std::endl;
    auto file_metadata = metadata.metadata();
    for (const auto& kv : file_metadata) {
        std::cout << kv.first << ": " << kv.second << std::endl;
    }

    // Print row group metadata (vector of maps)
    std::cout << "Row group metadata:" << std::endl;
    auto rowgroup_metadata = metadata.rowgroup_metadata();
    for (size_t i = 0; i < 10; ++i) {
    //for (size_t i = 0; i < rowgroup_metadata.size(); ++i) {
        std::cout << "-- Row group " << i + 1 << " metadata:" << std::endl;
        for (const auto& kv : rowgroup_metadata[i]) {
            std::cout << "---- " << kv.first << ": " << kv.second << std::endl;
        }
    }
}

float get_file_size_gb(const std::string& filepath) {
    std::filesystem::path p{filepath};
    return 1.0 * std::filesystem::file_size(p) / (1024 * 1024 * 1024);
}

std::string read_parquet_file(const std::string& filepath)
{
    cudf::examples::timer timer;
    auto source_info = cudf::io::source_info(filepath);
    auto options = cudf::io::parquet_reader_options::builder(source_info).build();
    timer.reset();
    auto [result, metadata] = cudf::io::read_parquet(options);
    auto elapsed_milis = std::chrono::duration_cast<std::chrono::milliseconds>(timer.elapsed()).count();
    auto bw = get_file_size_gb(filepath) / elapsed_milis * 1000; 
    std::cout << bw << std::endl;
    return std::to_string(bw);
    //std::cout << "Successfully read Parquet file: " << filepath << std::endl;
    //std::cout << "Number of columns: " << result->num_columns() << std::endl;
    //std::cout << "Number of rows: " << result->num_rows() << std::endl;
}

std::string read_parquet_file(cudf::io::parquet_reader_options& options, const std::string& filepath, rmm::cuda_stream_view stream, size_t iterations)
{
    cudf::io::read_parquet(options);  // first run
    
    cudf::examples::timer timer;
    for (auto i = 0; i < iterations; i++) {
        cudf::io::read_parquet(options);
        stream.synchronize();
    }
    auto elapsed_milis = std::chrono::duration_cast<std::chrono::milliseconds>(timer.elapsed()).count();
    std::cout << elapsed_milis << " ms" << std::endl;
    auto bw = get_file_size_gb(filepath) * iterations / elapsed_milis * 1000; 
    std::cout << bw << std::endl;
    return std::to_string(bw);
    //std::cout << "Successfully read Parquet file: " << filepath << std::endl;
    //std::cout << "Number of columns: " << result->num_columns() << std::endl;
    //std::cout << "Number of rows: " << result->num_rows() << std::endl;
}

class Barrier {
private:
    std::atomic<int> counter;       // Tracks the number of threads at the barrier
    int total_threads;              // Total number of threads expected
    std::mutex mtx;                 // Mutex for condition variable
    std::condition_variable cv;     // Condition variable for synchronization
    int generation;                 // Tracks barrier phase to prevent spurious wakeups

public:
    explicit Barrier(int num_threads) 
        : counter(0), total_threads(num_threads), generation(0) {}

    void wait() {
        std::unique_lock<std::mutex> lock(mtx);
        int local_gen = generation; // Capture the current phase

        // Increment counter atomically
        if (counter.fetch_add(1, std::memory_order_acq_rel) == total_threads - 1) {
            // Last thread arrives: reset counter and advance generation
            counter.store(0, std::memory_order_release);
            generation++; 
            cv.notify_all();
        } else {
            // Other threads wait until the generation changes
            cv.wait(lock, [&] { return local_gen != generation; });
        }
    }
};

struct read_fn {
  cudf::io::parquet_reader_options& options;
  rmm::cuda_stream_view stream;
  int const thread_id;
  int32_t const thread_count;
  size_t const repeat_num;
  std::chrono::milliseconds& first_time;
  // Barrier &barrier;
  void operator()()
  {
      auto start = std::chrono::high_resolution_clock::now();
      cudf::io::read_parquet(options, stream);
        // std::mt19937_64 eng{std::random_device{}()};  // or seed however you want
        // std::uniform_int_distribution<> dist{500, 1000};
        // std::this_thread::sleep_for(std::chrono::milliseconds{dist(eng)});
      auto end = std::chrono::high_resolution_clock::now();
      first_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
      for (int i=0; i < repeat_num;i++) {          
        auto [input, metadata] = cudf::io::read_parquet(options, stream);

        // computation: SORTING
        auto sorted_table_ascending = cudf::sort(cudf::table_view({input->view().column(0)}), {cudf::order::ASCENDING}, {cudf::null_order::BEFORE}, stream);
        //barrier.wait(); // FUCK a barrier leads to the ISSUE!!!
      }
    //   stream.synchronize_no_throw();
  }
};

std::string read_parquet_file_mt(std::vector<cudf::io::parquet_reader_options>& options_vec, const std::string& filepath, int32_t thread_count, rmm::cuda_stream_pool& stream_pool, rmm::cuda_stream_view stream, const size_t repeat_num = 1)
{
    std::vector<read_fn> read_tasks;
    read_tasks.reserve(thread_count);
    //Barrier barrier(thread_count);
    std::vector<std::chrono::milliseconds> results(thread_count);
    // Create the read tasks
    std::for_each(
        thrust::make_counting_iterator(0), thrust::make_counting_iterator(thread_count), [&](auto tid) {
            read_tasks.emplace_back(read_fn{options_vec[tid], stream_pool.get_stream(), tid, thread_count, repeat_num, results[tid]});
    });
    // Create threads with tasks
    std::vector<std::thread> threads;
    threads.reserve(thread_count); 
    cudf::examples::timer timer;
    for (auto& c : read_tasks) {
        threads.emplace_back(c);
    }
    for (auto& t : threads) {
        t.join();
    }
    stream.synchronize();
    
    auto maxFirstDuration = *std::max_element(results.begin(), results.end());
    auto elapsed_milis = std::chrono::duration_cast<std::chrono::milliseconds>(timer.elapsed()).count() - maxFirstDuration.count();
    std::cout << elapsed_milis << " ms" << std::endl;
    auto bw = get_file_size_gb(filepath) * thread_count * repeat_num / elapsed_milis * 1000;
    std::cout << bw << std::endl;
    return std::to_string(bw);
    //std::cout << "Successfully read Parquet file: " << filepath << std::endl;
    //std::cout << "Number of columns: " << result->num_columns() << std::endl;
    //std::cout << "Number of rows: " << result->num_rows() << std::endl;
}

int main(int argc, char const** argv)
{
    if (argc != 6) {
        std::cerr << "Usage: " << argv[0] << " <input parquet file> <repeat-iterations> <threadcount> <io_type: FILEPATH,HOST_BUFFER, PINNED_BUFFER, DEVICE_BUFFER" << std::endl;
        return 1;
    }
    std::string input_filepath = argv[1];
    const size_t iterations = std::stoi(argv[2]);
    const size_t thread_count = std::stoi(argv[3]); 
    const std::string io_type = std::string(argv[4]); 
    const size_t repeat_num = std::stoi(argv[5]);
    std::cout << "Iterations: " << iterations << std::endl;
    std::cout << "Thread Count: " << thread_count << std::endl;
    std::cout << "IO Type: " << io_type << std::endl; 
    io_source_type io_source_type = get_io_source_type(io_type); 
    std::cout << "Repeated Number as Batch: " << repeat_num << std::endl;

    std::regex pattern("_([A-Z0-9]+)\\.");
    std::smatch match;
    std::string compression;
    if (std::regex_search(input_filepath, match, pattern)) {
        compression = match[1]; // match[1] is the first captured group
    } else {
        std::cout << "COMPRESION not found in the filepath." << std::endl;
    }
    std::cout << "Compression: " << compression << std::endl;

    auto stream = cudf::get_default_stream();
    cudaSetDevice(0); 
    cudaFree(0); // Force device initialization

    //std::cout << "Reading Parquet file: " << input_filepath << std::endl; 
    //for (auto i = 0; i < iterations; i++) {
    //	try {
    //	    read_parquet_file(input_filepath);
    //	} catch (const std::exception& e) {
    //	    std::cerr << "Error reading Parquet file: " << e.what() << std::endl;
    //	    return 1;
    //	}
    //}
    //std::cout << "========================================" << std::endl;

    //std::cout << "Reading Parquet file on GPU Memory: " << input_filepath << std::endl; 
    //for (auto i = 0; i < iterations; i++) {
    //	try {
    //	    read_parquet_file_gpubuffer(input_filepath, stream);
    //	} catch (const std::exception& e) {
    //	    std::cerr << "Error reading Parquet file on GPU Memory: " << e.what() << std::endl;
    //	    return 1;
    //	}
    //}
    //std::cout << "========================================" << std::endl;

    rmm::mr::cuda_memory_resource cuda_mr;
    // Construct a resource that uses a coalescing best-fit pool allocator
    auto initial_size = rmm::percent_of_free_device_memory(95);
    rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource> pool_mr{&cuda_mr, initial_size};
    rmm::mr::set_current_device_resource(&pool_mr); // Updates the current device resource pointer to `pool_mr`
    rmm::mr::device_memory_resource* mr = rmm::mr::get_current_device_resource(); // Points to `pool_mr`
    
    io_source parquet_source(input_filepath, io_source_type, stream);
    auto source_info = parquet_source.get_source_info();
    auto metadata = cudf::io::read_parquet_metadata(source_info);
    const auto num_rowgroups = metadata.num_rowgroups();
    std::cout << "NUM ROW GROUPS: " << num_rowgroups << std::endl;
    print_parquet_metadata(metadata);

    // {
    //     //auto options = cudf::io::parquet_reader_options::builder(source_info).build();  // move parquet_reader_options member once it’s built. 
    //     auto options = cudf::io::parquet_reader_options::builder(source_info).columns({"l_orderkey"}).build();  // move parquet_reader_options member once it’s built. 
    //     std::cout << "Reading Parquet file on Pooled GPU Memory: " << input_filepath << std::endl; 
    //     std::string output;
    //     try {
    //         output = read_parquet_file(options, input_filepath, stream, iterations);
    //     } catch (const std::exception& e) {
    //         std::cerr << "Error reading Parquet file on Pool-ed GPU Memory: " << e.what() << std::endl;
    //         return 1;
    //     }
    //     std::cout << "SingleRead," << compression << "," << io_type << "," << output << std::endl;
    //     std::cout << "========================================" << std::endl;
    //     stream.synchronize();
    // }
    
    /// Each thread&stream reads the same whole file
    auto stream_pool = rmm::cuda_stream_pool(thread_count);
    // {
    //    std::vector<cudf::io::parquet_reader_options> options_vec(thread_count);
    //    io_source parquet_source_mt(input_filepath, io_source_type, stream);
    //    stream.synchronize();
    //    for (int32_t i = 0; i < thread_count; ++i) {
    //        auto source_info_mt = parquet_source_mt.get_source_info(); // returns a copy of source_info
    //        // then the source_info is move-ed
    //        options_vec[i] = cudf::io::parquet_reader_options::builder(source_info_mt).columns({"l_orderkey"}).build();  // move parquet_reader_options member once it’s built. 
    //        //    options_vec[i] = cudf::io::parquet_reader_options::builder(source_info_mt).build();  // move parquet_reader_options member once it’s built. 
    //    } 
    //    std::cout << "Reading Parquet file **N times** on Pooled GPU Memory Multithreading&streams: " << input_filepath << std::endl; 
    //    // TODO: I just disabled iteration
    //    //for (auto i = 0; i < iterations; i++) {
    //    std::string output;
    //        try {
    //    	    output = read_parquet_file_mt(options_vec, input_filepath, thread_count, stream_pool, stream, repeat_num);
    //    	} catch (const std::exception& e) {
    //    	    std::cerr << "Error reading Parquet file on Pool-ed GPU Memory: " << e.what() << std::endl;
    //    	    return 1;
    //    	}
    //    //}
    //    std::cout << "BatchedRead," << compression << "," << io_type << "," << output << std::endl;
    //    std::cout << "========================================" << std::endl;
    //    stream.synchronize();
    // }

    /// METADATA CACHING
    {
       auto parquet_metadata   = cudf::io::read_parquet_metadata(cudf::io::source_info{input_filepath});
       auto aggregate_reader_metadata = parquet_metadata.get_aggregate_reader_metadata();

       std::vector<cudf::io::parquet_reader_options> options_vec(thread_count);
       io_source parquet_source_mt(input_filepath, io_source_type, stream);
       stream.synchronize();
       std::vector<std::string> columns = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber"};
       for (int32_t i = 0; i < thread_count; ++i) {
           auto source_info_mt = parquet_source_mt.get_source_info(); // returns a copy of source_info
           // then the source_info is move-ed
           options_vec[i] = cudf::io::parquet_reader_options::builder(source_info_mt).columns({columns[i % columns.size()]}).build();  // move parquet_reader_options member once it’s built. 
            //   options_vec[i] = cudf::io::parquet_reader_options::builder(source_info_mt).build();  // move parquet_reader_options member once it’s built. 
           options_vec[i].set_aggregate_reader_metadata(aggregate_reader_metadata);
       } 
       std::cout << "[META CACHING] Reading Parquet file **N times** on Pooled GPU Memory Multithreading&streams: " << input_filepath << std::endl; 
       // TODO: I just disabled iteration
       //for (auto i = 0; i < iterations; i++) {
       std::string output;
           try {
       	    output = read_parquet_file_mt(options_vec, input_filepath, thread_count, stream_pool, stream, repeat_num);
       	} catch (const std::exception& e) {
       	    std::cerr << "Error reading Parquet file on Pool-ed GPU Memory: " << e.what() << std::endl;
       	    return 1;
       	}
       //}
       std::cout << "BatchedRead," << compression << "," << io_type << "," << output << std::endl;
       std::cout << "========================================" << std::endl;
       stream.synchronize();
    }










    /// Each thread&stream reads only part of the file. 
    /// All threads&streams read the whole file once.    
    //{
    //    // Assign sub-file to read as some row groups
    //    std::vector<std::vector<std::vector<int32_t>>> rg_vec(thread_count);
    //    for (int32_t i = 0, start = 0; i < thread_count; ++i) {
	//        std::vector<int32_t> rg(num_rowgroups / thread_count + (i < num_rowgroups % thread_count));
    //            std::iota (std::begin(rg), std::end(rg), start);
	//        start += rg.size();	
	//        rg_vec[i] = {std::move(rg)}; 
	//        
	//        /// Print each row-group-assignment
    //        //std::cout << "[" << i << "]" << std::endl;
	//        //for (auto ii = 0; ii < rg_vec[i][0].size(); ++ii) {
	//        //    std::cout << rg_vec[i][0][ii] << " ";
	//        //}
	//        //std::cout << std::endl;
    //    }

    //    std::vector<cudf::io::parquet_reader_options> options_vec(thread_count);
    //    io_source parquet_source_mt(input_filepath, io_source_type::DEVICE_BUFFER, stream);  // loading to GPU buffer
    //    stream.synchronize();
    //    for (int32_t i = 0; i < thread_count; ++i) {
    //        auto source_info_mt = parquet_source_mt.get_source_info(); // returns a copy of source_info
    //        // then the source_info is move-ed
    //        options_vec[i] = cudf::io::parquet_reader_options::builder(source_info_mt).row_groups(rg_vec[i]).build();  // move parquet_reader_options member once it’s built. 
    //    } 
    //    //auto rgs = options.get_row_groups();
    //    //std::cout << rgs.size() << std::endl;
    //    
    //    std::cout << "Reading Parquet file on Pooled GPU Memory Multithreading&streams: " << input_filepath << std::endl; 
    //    for (auto i = 0; i < iterations; i++) {
    //        try {
    //    	    read_parquet_file_gpubuffer_mt(options_vec, input_filepath, thread_count, stream_pool, stream);
    //    	} catch (const std::exception& e) {
    //    	    std::cerr << "Error reading Parquet file on Pool-ed GPU Memory: " << e.what() << std::endl;
    //    	    return 1;
    //    	}
    //    }
    //    std::cout << "========================================" << std::endl;
    //}

    return 0;
}

