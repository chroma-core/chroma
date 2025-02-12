#include "../../hnswlib/hnswlib.h"
#include <thread>

// Multithreaded executor
// The helper function copied from python_bindings/bindings.cpp (and that itself is copied from nmslib)
// An alternative is using #pragme omp parallel for or any other C++ threading
template <class Function>
inline void ParallelFor(size_t start, size_t end, size_t numThreads, Function fn)
{
    if (numThreads <= 0)
    {
        numThreads = std::thread::hardware_concurrency();
    }

    if (numThreads == 1)
    {
        for (size_t id = start; id < end; id++)
        {
            fn(id, 0);
        }
    }
    else
    {
        std::vector<std::thread> threads;
        std::atomic<size_t> current(start);

        // keep track of exceptions in threads
        // https://stackoverflow.com/a/32428427/1713196
        std::exception_ptr lastException = nullptr;
        std::mutex lastExceptMutex;

        for (size_t threadId = 0; threadId < numThreads; ++threadId)
        {
            threads.push_back(std::thread([&, threadId]
                                          {
                while (true) {
                    size_t id = current.fetch_add(1);

                    if (id >= end) {
                        break;
                    }

                    try {
                        fn(id, threadId);
                    } catch (...) {
                        std::unique_lock<std::mutex> lastExcepLock(lastExceptMutex);
                        lastException = std::current_exception();
                        /*
                         * This will work even when current is the largest value that
                         * size_t can fit, because fetch_add returns the previous value
                         * before the increment (what will result in overflow
                         * and produce 0 instead of current + 1).
                         */
                        current = end;
                        break;
                    }
                } }));
        }
        for (auto &thread : threads)
        {
            thread.join();
        }
        if (lastException)
        {
            std::rethrow_exception(lastException);
        }
    }
}

int main()
{
    int dim = 16;              // Dimension of the elements
    int max_elements = 10000;  // Maximum number of elements, should be known beforehand
    int M = 16;                // Tightly connected with internal dimensionality of the data
                               // strongly affects the memory consumption
    int ef_construction = 200; // Controls index search speed/build speed tradeoff
    int num_threads = 20;      // Number of threads for operations with index

    // Initing index with allow_replace_deleted=true
    int seed = 100;
    hnswlib::L2Space space(dim);
    hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, max_elements, M, ef_construction, seed, true);

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    float *data = new float[dim * max_elements];
    for (int i = 0; i < dim * max_elements; i++)
    {
        data[i] = distrib_real(rng);
    }

    // Add data to index
    ParallelFor(0, max_elements, num_threads, [&](size_t row, size_t threadId)
                { alg_hnsw->addPoint((void *)(data + dim * row), row); });

    // Mark first half of elements as deleted
    int num_deleted = max_elements / 2;
    ParallelFor(0, num_deleted, num_threads, [&](size_t row, size_t threadId)
                { alg_hnsw->markDelete(row); });

    // Generate additional random data
    float *add_data = new float[dim * num_deleted];
    for (int i = 0; i < dim * num_deleted; i++)
    {
        add_data[i] = distrib_real(rng);
    }

    // Replace deleted data with new elements
    // Maximum number of elements is reached therefore we cannot add new items,
    // but we can replace the deleted ones by using replace_deleted=true
    ParallelFor(0, num_deleted, num_threads, [&](size_t row, size_t threadId)
                {
        hnswlib::labeltype label = max_elements + row;
        alg_hnsw->addPoint((void*)(add_data + dim * row), label, true); });

    delete[] data;
    delete[] add_data;
    delete alg_hnsw;
    return 0;
}
