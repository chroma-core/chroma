#include "../../hnswlib/hnswlib.h"

#include <assert.h>

#include <vector>
#include <iostream>

namespace
{

    using idx_t = hnswlib::labeltype;

    void testReadUnormalizedData()
    {
        int d = 4;
        idx_t n = 100;
        idx_t nq = 10;
        size_t k = 10;

        std::vector<float> data(n * d);

        std::mt19937 rng;
        rng.seed(47);
        std::uniform_real_distribution<> distrib;

        for (idx_t i = 0; i < n * d; i++)
        {
            data[i] = distrib(rng);
        }

        hnswlib::InnerProductSpace space(d);
        hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, 2 * n, 16, 200, 100, false, true);

        for (size_t i = 0; i < n; i++)
        {
            alg_hnsw->addPoint(data.data() + d * i, i);
        }

        // Check that all data is the same
        for (size_t i = 0; i < n; i++)
        {
            std::vector<float> actual = alg_hnsw->template getDataByLabel<float>(i);
            for (size_t j = 0; j < d; j++)
            {
                // Check that abs difference is less than 1e-6
                assert(std::abs(actual[j] - data[d * i + j]) < 1e-6);
            }
        }

        delete alg_hnsw;
    }

    void testSaveAndLoadUnormalizedData()
    {
        int d = 4;
        idx_t n = 100;
        idx_t nq = 10;
        size_t k = 10;

        std::vector<float> data(n * d);

        std::mt19937 rng;
        rng.seed(47);
        std::uniform_real_distribution<> distrib;

        for (idx_t i = 0; i < n * d; i++)
        {
            data[i] = distrib(rng);
        }

        hnswlib::InnerProductSpace space(d);
        hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, 2 * n, 16, 200, 100, false, true);

        for (size_t i = 0; i < n; i++)
        {
            alg_hnsw->addPoint(data.data() + d * i, i);
        }

        alg_hnsw->saveIndex("test.bin");

        hnswlib::HierarchicalNSW<float> *alg_hnsw2 = new hnswlib::HierarchicalNSW<float>(&space, "test.bin", false, 2 * n, false, true);

        // Check that all data is the same
        for (size_t i = 0; i < n; i++)
        {
            std::vector<float> actual = alg_hnsw2->template getDataByLabel<float>(i);
            for (size_t j = 0; j < d; j++)
            {
                // Check that abs difference is less than 1e-6
                assert(std::abs(actual[j] - data[d * i + j]) < 1e-6);
            }
        }

        delete alg_hnsw;
    }

    void testUpdateUnormalizedData()
    {
        int d = 4;
        idx_t n = 100;
        idx_t nq = 10;
        size_t k = 10;

        std::vector<float> data(n * d);

        std::mt19937 rng;
        rng.seed(47);
        std::uniform_real_distribution<> distrib;

        for (idx_t i = 0; i < n * d; i++)
        {
            data[i] = distrib(rng);
        }

        hnswlib::InnerProductSpace space(d);
        hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, 2 * n, 16, 200, 100, false, true);

        for (size_t i = 0; i < n; i++)
        {
            alg_hnsw->addPoint(data.data() + d * i, i);
        }

        // Check that all data is the same
        for (size_t i = 0; i < n; i++)
        {
            std::vector<float> actual = alg_hnsw->template getDataByLabel<float>(i);
            for (size_t j = 0; j < d; j++)
            {
                // Check that abs difference is less than 1e-6
                assert(std::abs(actual[j] - data[d * i + j]) < 1e-6);
            }
        }

        // Generate new data
        std::vector<float> data2(n * d);
        for (idx_t i = 0; i < n * d; i++)
        {
            data2[i] = distrib(rng);
        }

        // Update data
        for (size_t i = 0; i < n; i++)
        {
            alg_hnsw->addPoint(data2.data() + d * i, i);
        }

        // Check that all data is the same
        for (size_t i = 0; i < n; i++)
        {
            std::vector<float> actual = alg_hnsw->template getDataByLabel<float>(i);
            for (size_t j = 0; j < d; j++)
            {
                // Check that abs difference is less than 1e-6
                assert(std::abs(actual[j] - data2[d * i + j]) < 1e-6);
            }
        }

        delete alg_hnsw;
    }

} // namespace

void testResizeUnormalizedData()
{
    int d = 4;
    idx_t n = 100;
    idx_t nq = 10;
    size_t k = 10;

    std::vector<float> data(n * d);

    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;

    for (idx_t i = 0; i < n * d; i++)
    {
        data[i] = distrib(rng);
    }

    hnswlib::InnerProductSpace space(d);
    hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, n, 16, 200, 100, false, true);

    for (size_t i = 0; i < n; i++)
    {
        alg_hnsw->addPoint(data.data() + d * i, i);
    }

    // Expect add to throw exception
    try
    {
        alg_hnsw->addPoint(data.data(), n);
        assert(false);
    }
    catch (std::runtime_error &e)
    {
        // Pass
    }

    // Resize the index
    alg_hnsw->resizeIndex(2 * n);

    // Check that all data is the same
    for (size_t i = 0; i < n; i++)
    {
        std::vector<float> actual = alg_hnsw->template getDataByLabel<float>(i);
        for (size_t j = 0; j < d; j++)
        {
            // Check that abs difference is less than 1e-6
            assert(std::abs(actual[j] - data[d * i + j]) < 1e-6);
        }
    }

    // Update / Add new data
    std::vector<float> data2(n * 2 * d);
    for (idx_t i = 0; i < n * 2 * d; i++)
    {
        data2[i] = distrib(rng);
    }
    for (size_t i = 0; i < n; i++)
    {
        alg_hnsw->addPoint(data2.data() + d * i, i);
    }

    // Check that all data is the same
    for (size_t i = 0; i < n; i++)
    {
        std::vector<float> actual = alg_hnsw->template getDataByLabel<float>(i);
        for (size_t j = 0; j < d; j++)
        {
            // Check that abs difference is less than 1e-6
            assert(std::abs(actual[j] - data2[d * i + j]) < 1e-6);
        }
    }
}

int main()
{
    std::cout << "Testing ..." << std::endl;
    testReadUnormalizedData();
    std::cout << "Test testReadUnormalizedData ok" << std::endl;
    testSaveAndLoadUnormalizedData();
    std::cout << "Test testSaveAndLoadUnormalizedData ok" << std::endl;
    testUpdateUnormalizedData();
    std::cout << "Test testUpdateUnormalizedData ok" << std::endl;
    testResizeUnormalizedData();
    std::cout << "Test testResizeUnormalizedData ok" << std::endl;

    return 0;
}
