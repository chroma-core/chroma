#include "../../hnswlib/hnswlib.h"

int main()
{
    int dim = 16;              // Dimension of the elements
    int max_elements = 10000;  // Maximum number of elements, should be known beforehand
    int M = 16;                // Tightly connected with internal dimensionality of the data
                               // strongly affects the memory consumption
    int ef_construction = 200; // Controls index search speed/build speed tradeoff

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
    for (int i = 0; i < max_elements; i++)
    {
        alg_hnsw->addPoint(data + i * dim, i);
    }

    // Mark first half of elements as deleted
    int num_deleted = max_elements / 2;
    for (int i = 0; i < num_deleted; i++)
    {
        alg_hnsw->markDelete(i);
    }

    // Generate additional random data
    float *add_data = new float[dim * num_deleted];
    for (int i = 0; i < dim * num_deleted; i++)
    {
        add_data[i] = distrib_real(rng);
    }

    // Replace deleted data with new elements
    // Maximum number of elements is reached therefore we cannot add new items,
    // but we can replace the deleted ones by using replace_deleted=true
    for (int i = 0; i < num_deleted; i++)
    {
        hnswlib::labeltype label = max_elements + i;
        alg_hnsw->addPoint(add_data + i * dim, label, true);
    }

    delete[] data;
    delete[] add_data;
    delete alg_hnsw;
    return 0;
}
