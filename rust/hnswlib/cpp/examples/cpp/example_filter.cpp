#include "../../hnswlib/hnswlib.h"

// Filter that allows labels divisible by divisor
class PickDivisibleIds : public hnswlib::BaseFilterFunctor
{
    unsigned int divisor = 1;

public:
    PickDivisibleIds(unsigned int divisor) : divisor(divisor)
    {
        assert(divisor != 0);
    }
    bool operator()(hnswlib::labeltype label_id)
    {
        return label_id % divisor == 0;
    }
};

int main()
{
    int dim = 16;              // Dimension of the elements
    int max_elements = 10000;  // Maximum number of elements, should be known beforehand
    int M = 16;                // Tightly connected with internal dimensionality of the data
                               // strongly affects the memory consumption
    int ef_construction = 200; // Controls index search speed/build speed tradeoff

    // Initing index
    hnswlib::L2Space space(dim);
    hnswlib::HierarchicalNSW<float> *alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, max_elements, M, ef_construction);

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

    // Create filter that allows only even labels
    PickDivisibleIds pickIdsDivisibleByTwo(2);

    // Query the elements for themselves with filter and check returned labels
    int k = 10;
    for (int i = 0; i < max_elements; i++)
    {
        std::vector<std::pair<float, hnswlib::labeltype>> result = alg_hnsw->searchKnnCloserFirst(data + i * dim, k, &pickIdsDivisibleByTwo);
        for (auto item : result)
        {
            if (item.second % 2 == 1)
                std::cout << "Error: found odd label\n";
        }
    }

    delete[] data;
    delete alg_hnsw;
    return 0;
}
