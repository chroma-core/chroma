#include "../../hnswlib/hnswlib.h"

#include <assert.h>

#include <vector>
#include <iostream>

namespace
{

    using idx_t = hnswlib::labeltype;

    void testPersistentIndex()
    {
        int d = 1536;
        idx_t n = 100;
        idx_t nq = 10;
        size_t k = 10;

        std::vector<float> data(n * d);
        std::vector<float> query(nq * d);

        std::mt19937 rng;
        rng.seed(47);
        std::uniform_real_distribution<> distrib;

        for (idx_t i = 0; i < n * d; i++)
        {
            data[i] = distrib(rng);
        }
        for (idx_t i = 0; i < nq * d; ++i)
        {
            query[i] = distrib(rng);
        }

        hnswlib::InnerProductSpace space(d);
        hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, 2 * n, 16, 200, 100, false, false, true, ".");

        for (size_t i = 0; i < n; i++)
        {
            alg_hnsw->addPoint(data.data() + d * i, i);
            if (i % 10 == 0)
                alg_hnsw->persistDirty();
        }
        alg_hnsw->persistDirty();

        hnswlib::HierarchicalNSW<float> *alg_hnsw2 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, 2 * n, false, false, true);

        // Check that all data is the same
        for (size_t i = 0; i < n; i++)
        {
            std::vector<float> actual = alg_hnsw2->template getDataByLabel<float>(i);
            for (size_t j = 0; j < d; j++)
            {
                // Check that abs difference is less than 1e-6
                if (!(std::abs(actual[j] - data[d * i + j]) < 1e-6))
                {
                    std::cout << "actual: " << actual[j] << " expected: " << data[d * i + j] << std::endl;
                }
                assert(std::abs(actual[j] - data[d * i + j]) < 1e-6);
            }
        }

        // Compare to in-memory index
        for (size_t j = 0; j < nq; ++j)
        {
            const void *p = query.data() + j * d;
            auto gd = alg_hnsw->searchKnn(p, k);
            auto res = alg_hnsw2->searchKnn(p, k);
            assert(gd.size() == res.size());
            int missed = 0;
            for (size_t i = 0; i < gd.size(); i++)
            {
                assert(std::abs(gd.top().first - res.top().first) < 1e-6);
                assert(gd.top().second == res.top().second);
                gd.pop();
                res.pop();
            }
        }

        delete alg_hnsw;
    }

    void testResizePersistentIndex()
    {
        int d = 1536;
        idx_t n = 400;
        idx_t nq = 10;
        size_t k = 10;

        std::vector<float> data(n * d);
        std::vector<float> query(nq * d);

        std::mt19937 rng;
        rng.seed(47);
        std::uniform_real_distribution<> distrib;

        for (idx_t i = 0; i < n * d; i++)
        {
            data[i] = distrib(rng);
        }
        for (idx_t i = 0; i < nq * d; ++i)
        {
            query[i] = distrib(rng);
        }

        hnswlib::InnerProductSpace space(d);
        hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, n / 4, 16, 200, 100, false, false, true, ".");

        // Add a quarter of the data
        for (size_t i = 0; i < n / 4; i++)
        {
            alg_hnsw->addPoint(data.data() + d * i, i);
            if (i % 9 == 0)
                alg_hnsw->persistDirty();
        }
        alg_hnsw->persistDirty();

        // Resize index and another quarter of the data
        alg_hnsw->resizeIndex(n / 2);
        for (size_t i = n / 4; i < n / 2; i++)
        {
            alg_hnsw->addPoint(data.data() + d * i, i);
            if (i % 9 == 0)
                alg_hnsw->persistDirty();
        }
        alg_hnsw->persistDirty();

        // Load the resized index with n / 2 elements
        hnswlib::HierarchicalNSW<float> *alg_hnsw2 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, n / 2, false, false, true);
        // Check that the added half of the data is the same
        for (size_t i = 0; i < n / 2; i++)
        {
            std::vector<float> actual = alg_hnsw2->template getDataByLabel<float>(i);
            for (size_t j = 0; j < d; j++)
            {
                assert(std::abs(actual[j] - data[d * i + j]) < 1e-6);
            }
        }

        // Resize the index and add all the data
        alg_hnsw2->resizeIndex(n);
        for (size_t i = n / 2; i < n; i++)
        {
            alg_hnsw2->addPoint(data.data() + d * i, i);
            if (i % 9 == 0)
                alg_hnsw2->persistDirty();
        }
        alg_hnsw2->persistDirty();

        // Load the resized index with n elements
        hnswlib::HierarchicalNSW<float> *alg_hnsw3 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, n, false, false, true);
        // Check that all the data is the same
        for (size_t i = 0; i < n; i++)
        {
            std::vector<float> actual = alg_hnsw3->template getDataByLabel<float>(i);
            for (size_t j = 0; j < d; j++)
            {
                assert(std::abs(actual[j] - data[d * i + j]) < 1e-6);
            }
        }

        delete alg_hnsw;
        delete alg_hnsw2;
        delete alg_hnsw3;
    }

    void testAddUpdatePersistentIndex()
    {
        int d = 1536;
        idx_t n = 100;
        idx_t nq = 10;
        size_t k = 10;

        std::vector<float> data(n * d);
        std::vector<float> query(nq * d);

        std::mt19937 rng;
        rng.seed(47);
        std::uniform_real_distribution<> distrib;

        for (idx_t i = 0; i < n * d; i++)
        {
            data[i] = distrib(rng);
        }
        for (idx_t i = 0; i < nq * d; ++i)
        {
            query[i] = distrib(rng);
        }

        hnswlib::InnerProductSpace space(d);
        hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, n, 16, 200, 100, false, false, true, ".");

        for (size_t i = 0; i < n; i++)
        {
            alg_hnsw->addPoint(data.data() + d * i, i);
            if (i % 10 == 0)
                alg_hnsw->persistDirty();
        }
        alg_hnsw->persistDirty();

        // Generate random updates to the index
        float update_prob = 0.2;
        for (size_t i = 0; i < n; i++)
        {
            if (distrib(rng) < update_prob)
            {
                std::vector<float> new_data(d);
                for (size_t j = 0; j < d; j++)
                {
                    new_data[j] = distrib(rng);
                }
                alg_hnsw->addPoint(new_data.data(), i);
                if (i % 10 == 0)
                    alg_hnsw->persistDirty();
            }
        }
        alg_hnsw->persistDirty();

        // Load the index with n elements
        hnswlib::HierarchicalNSW<float> *alg_hnsw2 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, n, false, false, true);

        // Check that all the data is the same
        for (size_t i = 0; i < n; i++)
        {
            std::vector<float> actual = alg_hnsw2->template getDataByLabel<float>(i);
            std::vector<float> expected = alg_hnsw->template getDataByLabel<float>(i);
            for (size_t j = 0; j < d; j++)
            {
                assert(std::abs(actual[j] - expected[j]) < 1e-6);
            }
        }
    }

    void testDeletePersistentIndex()
    {
        int d = 1536;
        idx_t n = 100;
        idx_t nq = 10;
        size_t k = 10;

        std::vector<float> data(n * d);
        std::vector<float> query(nq * d);

        std::mt19937 rng;
        rng.seed(47);
        std::uniform_real_distribution<> distrib;

        for (idx_t i = 0; i < n * d; i++)
        {
            data[i] = distrib(rng);
        }
        for (idx_t i = 0; i < nq * d; ++i)
        {
            query[i] = distrib(rng);
        }

        hnswlib::InnerProductSpace space(d);
        hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, n, 16, 200, 100, false, false, true, ".");

        for (size_t i = 0; i < n; i++)
        {
            alg_hnsw->addPoint(data.data() + d * i, i);
            if (i % 10 == 0)
                alg_hnsw->persistDirty();
        }
        alg_hnsw->persistDirty();

        // Generate random deletes to the index
        float delete_prob = 0.2;
        std::set<idx_t> deleted;
        for (idx_t i = 0; i < n; i++)
        {
            if (distrib(rng) < delete_prob)
            {
                alg_hnsw->markDelete(i);
                deleted.insert(i);
                if (i % 10 == 0)
                    alg_hnsw->persistDirty();
            }
        }
        alg_hnsw->persistDirty();

        // Load the index with n elements
        hnswlib::HierarchicalNSW<float> *alg_hnsw2 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, n, false, false, true);

        // Query for all the elements and make sure that the deleted ones are not returned by the persisted index
        for (idx_t i = 0; i < n; i++)
        {
            if (deleted.count(i) != 0)
            {
                std::priority_queue<std::pair<float, idx_t>> result = alg_hnsw2->searchKnn(data.data() + d * i, k);
                for (size_t j = 0; j < k; j++)
                {
                    assert(result.top().second != i);
                    result.pop();
                };
            }
        }
    }
}

void test_persist_empty() {
    int d = 1;
    idx_t n = 0;
    idx_t nq = 1;

    std::vector<float> data(n * d);
    std::vector<float> query(nq * d);
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;

    for (idx_t i = 0; i < n * d; i++)
    {
        data[i] = distrib(rng);
    }
    for (idx_t i = 0; i < nq * d; ++i)
    {
        query[i] = distrib(rng);
    }

    hnswlib::InnerProductSpace space(d);
    hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, n, 16, 200, 100, false, false, true, ".");

    alg_hnsw->persistDirty();

    hnswlib::HierarchicalNSW<float> *alg_hnsw2 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, n, false, false, true);
    // query and expect no result
    std::priority_queue<std::pair<float, idx_t>> result = alg_hnsw2->searchKnn(query.data(), 10);
    assert(result.size() == 0);
}

void test_persist_size(int n) {
    int d = 1;
    idx_t nq = 1;
    size_t k = 10;

    std::vector<float> data(n * d);
    std::vector<float> query(nq * d);
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;


    for (idx_t i = 0; i < n * d; i++)
    {
        data[i] = distrib(rng);
    }
    for (idx_t i = 0; i < nq * d; ++i)
    {
        query[i] = distrib(rng);
    }

    hnswlib::InnerProductSpace space(d);
    hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, n, 16, 200, 100, false, false, true, ".");

    for (size_t i = 0; i < n; i++)
    {
        alg_hnsw->addPoint(data.data() + d * i, i);
        alg_hnsw->persistDirty();
    }
    alg_hnsw->persistDirty();

    hnswlib::HierarchicalNSW<float> *alg_hnsw2 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, n, false, false, true);


    // Check that all data is the same
    for (size_t i = 0; i < n; i++)
    {
        std::vector<float> actual = alg_hnsw2->template getDataByLabel<float>(i);
        for (size_t j = 0; j < d; j++)
        {
            // Check that abs difference is less than 1e-6
            if (!(std::abs(actual[j] - data[d * i + j]) < 1e-6))
            {
                std::cout << "actual: " << actual[j] << " expected: " << data[d * i + j] << std::endl;
            }
            assert(std::abs(actual[j] - data[d * i + j]) < 1e-6);
        }
    }

    // Compare to in-memory index
    for (size_t j = 0; j < nq; ++j)
    {
        const void *p = query.data() + j * d;
        auto gd = alg_hnsw->searchKnn(p, k);
        auto res = alg_hnsw2->searchKnn(p, k);
        assert(gd.size() == res.size());
        int missed = 0;
        for (size_t i = 0; i < gd.size(); i++)
        {
            assert(std::abs(gd.top().first - res.top().first) < 1e-6);
            assert(gd.top().second == res.top().second);
            gd.pop();
            res.pop();
        }
    }
}

void test_persist_then_delete_size(int n) {
    int d = 1;
    idx_t nq = 1;
    size_t k = 10;

    std::vector<float> data(n * d);
    std::vector<float> query(nq * d);
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;

    for (idx_t i = 0; i < n * d; i++)
    {
        data[i] = distrib(rng);
    }
    for (idx_t i = 0; i < nq * d; ++i)
    {
        query[i] = distrib(rng);
    }

    hnswlib::InnerProductSpace space(d);
    hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, n, 16, 200, 100, false, false, true, ".");

    for (size_t i = 0; i < n; i++)
    {
        alg_hnsw->addPoint(data.data() + d * i, i);
        alg_hnsw->persistDirty();
    }
    alg_hnsw->persistDirty();

    // Delete the inserted data and then persist
    for (size_t i = 0; i < n; i++)
    {
        alg_hnsw->markDelete(i);
        alg_hnsw->persistDirty();
    }
    alg_hnsw->persistDirty();

    hnswlib::HierarchicalNSW<float> *alg_hnsw2 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, n, false, false, true);

    // query and expect no result
    std::priority_queue<std::pair<float, idx_t>> result = alg_hnsw2->searchKnn(query.data(), 10);
    assert(result.size() == 0);
}

void test_persist_size_then_add(int n, int second_n) {
    int d = 1536;
    idx_t nq = 1;
    size_t k = 10;

    std::vector<float> data(n * d);
    std::vector<float> query(nq * d);
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;


    for (idx_t i = 0; i < n * d; i++)
    {
        data[i] = distrib(rng);
    }
    for (idx_t i = 0; i < nq * d; ++i)
    {
        query[i] = distrib(rng);
    }

    hnswlib::InnerProductSpace space(d);
    hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, n, 16, 200, 100, false, false, true, ".");

    for (size_t i = 0; i < n; i++)
    {
        alg_hnsw->addPoint(data.data() + d * i, i);
        alg_hnsw->persistDirty();
    }
    alg_hnsw->persistDirty();

    hnswlib::HierarchicalNSW<float> *alg_hnsw2 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, n + second_n, false, false, true);

    std::vector<float> data2(second_n * d);
    for (idx_t i = 0; i < second_n * d; i++)
    {
        data2[i] = distrib(rng);
    }

    for (size_t i = n; i < n + second_n; i++)
    {
        alg_hnsw2->addPoint(data2.data() + d * (i - n), i);
        alg_hnsw2->persistDirty();
    }
    alg_hnsw2->persistDirty();

    // Load alg_hnsw3
    hnswlib::HierarchicalNSW<float> *alg_hnsw3 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, n + second_n, false, false, true);

    // Check that all data is the same
    for (size_t i = 0; i < n + second_n; i++)
    {
        std::vector<float> actual = alg_hnsw3->template getDataByLabel<float>(i);
        for (size_t j = 0; j < d; j++)
        {
            // Check that abs difference is less than 1e-6
            if (!(std::abs(actual[j] - (i < n ? data[d * i + j] : data2[d * (i - n) + j])) < 1e-6))
            {
                std::cout << "actual: " << actual[j] << " expected: " << (i < n ? data[d * i + j] : data2[d * (i - n) + j]) << std::endl;
            }
            assert(std::abs(actual[j] - (i < n ? data[d * i + j] : data2[d * (i - n) + j]) < 1e-6));
        }
    }

    // Compare to in-memory index
    for (size_t j = 0; j < nq; ++j)
    {
        const void *p = query.data() + j * d;
        auto gd = alg_hnsw2->searchKnn(p, k);
        auto res = alg_hnsw3->searchKnn(p, k);
        assert(gd.size() == res.size());
        int missed = 0;
        for (size_t i = 0; i < gd.size(); i++)
        {
            assert(std::abs(gd.top().first - res.top().first) < 1e-6);
            assert(gd.top().second == res.top().second);
            gd.pop();
            res.pop();
        }
    }
}

int main()
{
    std::cout << "Testing ..." << std::endl;
    testPersistentIndex();
    std::cout << "Test testPersistentIndex ok" << std::endl;
    testResizePersistentIndex();
    std::cout << "Test testResizePersistentIndex ok" << std::endl;
    testAddUpdatePersistentIndex();
    std::cout << "Test testAddUpdatePersistentIndex ok" << std::endl;
    testDeletePersistentIndex();
    std::cout << "Test testDeletePersistentIndex ok" << std::endl;
    test_persist_empty();
    std::cout << "Test test_persist_empty ok" << std::endl;
    test_persist_size(1);
    std::cout << "Test test_persist_size(1) ok" << std::endl;
    test_persist_size(2);
    std::cout << "Test test_persist_size(2) ok" << std::endl;
    test_persist_size(3);
    std::cout << "Test test_persist_size(3) ok" << std::endl;
    test_persist_then_delete_size(1);
    std::cout << "Test test_persist_then_delete_size(1) ok" << std::endl;
    test_persist_then_delete_size(2);
    std::cout << "Test test_persist_then_delete_size(2) ok" << std::endl;
    test_persist_size_then_add(1, 1);
    std::cout << "Test test_persist_size_then_add(1, 1) ok" << std::endl;
    test_persist_size_then_add(2, 1);
    std::cout << "Test test_persist_size_then_add(2, 1) ok" << std::endl;
    test_persist_size_then_add(1, 1000);
    std::cout << "Test test_persist_size_then_add(1, 1000) ok" << std::endl;
    test_persist_size_then_add(2, 1000);
    std::cout << "Test test_persist_size_then_add(2, 1000) ok" << std::endl;
    return 0;
}
