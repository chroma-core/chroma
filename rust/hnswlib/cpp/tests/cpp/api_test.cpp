#include "../../hnswlib/hnswlib.h"

#include <assert.h>

void testListAllLabels()
{
    int d = 1536;
    hnswlib::labeltype n = 1000;

    std::vector<float> data(n * d);

    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib;

    for (auto i = 0; i < n * d; i++)
    {
        data[i] = distrib(rng);
    }

    hnswlib::InnerProductSpace space(d);
    hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, n, 16, 200, 100, false, false, true, ".");

    for (size_t i = 0; i < n; i++)
    {
        alg_hnsw->addPoint(data.data() + d * i, i);
    }
    // Delete odd points.
    for (size_t i = 1; i < n; i += 2)
    {
        alg_hnsw->markDelete(i);
    }
    // Get all data.
    auto res = alg_hnsw->getAllLabels();
    auto non_deleted = res.first;
    auto deleted = res.second;
    assert(non_deleted.size() == n / 2);
    assert(deleted.size() == n / 2);

    for (auto idx : non_deleted)
    {
        assert(idx % 2 == 0);
    }
    for (auto idx : deleted)
    {
        assert(idx % 2 == 1);
    }

    // After persisting and reloading the data should be the same.
    alg_hnsw->persistDirty();

    // Load the index with n elements
    hnswlib::HierarchicalNSW<float> *alg_hnsw2 = new hnswlib::HierarchicalNSW<float>(&space, ".", false, n, false, false, true);

    // Check that all the data is the same
    auto res2 = alg_hnsw2->getAllLabels();
    auto non_deleted2 = res2.first;
    auto deleted2 = res2.second;
    assert(non_deleted2.size() == n / 2);
    assert(deleted2.size() == n / 2);

    for (auto idx : non_deleted2)
    {
        assert(idx % 2 == 0);
    }
    for (auto idx : deleted2)
    {
        assert(idx % 2 == 1);
    }
}

int main()
{
    std::cout << "Testing ..." << std::endl;
    testListAllLabels();
    std::cout << "Test testListAllLabels ok" << std::endl;
    return 0;
}
