// This is a test file for testing the filtering feature

#include "../../hnswlib/hnswlib.h"

#include <assert.h>

#include <vector>
#include <iostream>

namespace
{

    using idx_t = hnswlib::labeltype;

    class PickDivisibleIds : public hnswlib::BaseFilterFunctor
    {
        unsigned int divisor = 1;

    public:
        PickDivisibleIds(unsigned int divisor) : divisor(divisor)
        {
            assert(divisor != 0);
        }
        bool operator()(idx_t label_id)
        {
            return label_id % divisor == 0;
        }
    };

    class PickNothing : public hnswlib::BaseFilterFunctor
    {
    public:
        bool operator()(idx_t label_id)
        {
            return false;
        }
    };

    void test_some_filtering(hnswlib::BaseFilterFunctor &filter_func, size_t div_num, size_t label_id_start)
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
            // `label_id_start` is used to ensure that the returned IDs are labels and not internal IDs
            alg_brute->addPoint(data.data() + d * i, label_id_start + i);
            alg_hnsw->addPoint(data.data() + d * i, label_id_start + i);
        }

        // test searchKnnCloserFirst of BruteforceSearch with filtering
        for (size_t j = 0; j < nq; ++j)
        {
            const void *p = query.data() + j * d;
            auto gd = alg_brute->searchKnn(p, k, &filter_func);
            auto res = alg_brute->searchKnnCloserFirst(p, k, &filter_func);
            assert(gd.size() == res.size());
            size_t t = gd.size();
            while (!gd.empty())
            {
                assert(gd.top() == res[--t]);
                assert((gd.top().second % div_num) == 0);
                gd.pop();
            }
        }

        // test searchKnnCloserFirst of hnsw with filtering
        for (size_t j = 0; j < nq; ++j)
        {
            const void *p = query.data() + j * d;
            auto gd = alg_hnsw->searchKnn(p, k, &filter_func);
            auto res = alg_hnsw->searchKnnCloserFirst(p, k, &filter_func);
            assert(gd.size() == res.size());
            size_t t = gd.size();
            while (!gd.empty())
            {
                assert(gd.top() == res[--t]);
                assert((gd.top().second % div_num) == 0);
                gd.pop();
            }
        }

        delete alg_brute;
        delete alg_hnsw;
    }

    void test_none_filtering(hnswlib::BaseFilterFunctor &filter_func, size_t label_id_start)
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
            // `label_id_start` is used to ensure that the returned IDs are labels and not internal IDs
            alg_brute->addPoint(data.data() + d * i, label_id_start + i);
            alg_hnsw->addPoint(data.data() + d * i, label_id_start + i);
        }

        // test searchKnnCloserFirst of BruteforceSearch with filtering
        for (size_t j = 0; j < nq; ++j)
        {
            const void *p = query.data() + j * d;
            auto gd = alg_brute->searchKnn(p, k, &filter_func);
            auto res = alg_brute->searchKnnCloserFirst(p, k, &filter_func);
            assert(gd.size() == res.size());
            assert(0 == gd.size());
        }

        // test searchKnnCloserFirst of hnsw with filtering
        for (size_t j = 0; j < nq; ++j)
        {
            const void *p = query.data() + j * d;
            auto gd = alg_hnsw->searchKnn(p, k, &filter_func);
            auto res = alg_hnsw->searchKnnCloserFirst(p, k, &filter_func);
            assert(gd.size() == res.size());
            assert(0 == gd.size());
        }

        delete alg_brute;
        delete alg_hnsw;
    }

} // namespace

class CustomFilterFunctor : public hnswlib::BaseFilterFunctor
{
    std::unordered_set<idx_t> allowed_values;

public:
    explicit CustomFilterFunctor(const std::unordered_set<idx_t> &values) : allowed_values(values) {}

    bool operator()(idx_t id)
    {
        return allowed_values.count(id) != 0;
    }
};

int main()
{
    std::cout << "Testing ..." << std::endl;

    // some of the elements are filtered
    PickDivisibleIds pickIdsDivisibleByThree(3);
    test_some_filtering(pickIdsDivisibleByThree, 3, 17);
    PickDivisibleIds pickIdsDivisibleBySeven(7);
    test_some_filtering(pickIdsDivisibleBySeven, 7, 17);

    // all of the elements are filtered
    PickNothing pickNothing;
    test_none_filtering(pickNothing, 17);

    // functor style which can capture context
    CustomFilterFunctor pickIdsDivisibleByThirteen({26, 39, 52, 65});
    test_some_filtering(pickIdsDivisibleByThirteen, 13, 21);

    std::cout << "Test ok" << std::endl;

    return 0;
}
