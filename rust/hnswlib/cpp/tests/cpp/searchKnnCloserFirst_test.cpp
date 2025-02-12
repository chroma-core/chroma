// This is a test file for testing the interface
//  >>> virtual std::vector<std::pair<dist_t, labeltype>>
//  >>>    searchKnnCloserFirst(const void* query_data, size_t k) const;
// of class AlgorithmInterface

#include "../../hnswlib/hnswlib.h"

#include <assert.h>

#include <vector>
#include <iostream>

namespace
{

    using idx_t = hnswlib::labeltype;

    void test()
    {
        int d = 4;
        idx_t n = 100;
        idx_t nq = 10;
        size_t k = 10;

        std::vector<float> data(n * d);
        std::vector<float> query(nq * d);

        std::mt19937 rng;
        rng.seed(47);
        std::uniform_real_distribution<> distrib;

        for (idx_t i = 0; i < n * d; ++i)
        {
            data[i] = distrib(rng);
        }
        for (idx_t i = 0; i < nq * d; ++i)
        {
            query[i] = distrib(rng);
        }

        hnswlib::L2Space space(d);
        hnswlib::AlgorithmInterface<float> *alg_brute = new hnswlib::BruteforceSearch<float>(&space, 2 * n);
        hnswlib::AlgorithmInterface<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, 2 * n);

        for (size_t i = 0; i < n; ++i)
        {
            alg_brute->addPoint(data.data() + d * i, i);
            alg_hnsw->addPoint(data.data() + d * i, i);
        }

        // test searchKnnCloserFirst of BruteforceSearch
        for (size_t j = 0; j < nq; ++j)
        {
            const void *p = query.data() + j * d;
            auto gd = alg_brute->searchKnn(p, k);
            auto res = alg_brute->searchKnnCloserFirst(p, k);
            assert(gd.size() == res.size());
            size_t t = gd.size();
            while (!gd.empty())
            {
                assert(gd.top() == res[--t]);
                gd.pop();
            }
        }
        for (size_t j = 0; j < nq; ++j)
        {
            const void *p = query.data() + j * d;
            auto gd = alg_hnsw->searchKnn(p, k);
            auto res = alg_hnsw->searchKnnCloserFirst(p, k);
            assert(gd.size() == res.size());
            size_t t = gd.size();
            while (!gd.empty())
            {
                assert(gd.top() == res[--t]);
                gd.pop();
            }
        }

        delete alg_brute;
        delete alg_hnsw;
    }

} // namespace

int main()
{
    std::cout << "Testing ..." << std::endl;
    test();
    std::cout << "Test ok" << std::endl;

    return 0;
}
