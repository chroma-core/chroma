#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <omp.h>

// Each worker accumulates contiguous rows into its own two-group sum.
extern "C" void centroid_cpu(const float* x, const std::int32_t* labels,
                             float* out, std::size_t rows, int dim) {
    const int threads = omp_get_max_threads();
    std::vector<double> partial(static_cast<std::size_t>(threads) * 2 * dim, 0.0);
    std::vector<std::size_t> counts(static_cast<std::size_t>(threads) * 2, 0);
#pragma omp parallel
    {
        const int worker = omp_get_thread_num();
        double* local = partial.data() + static_cast<std::size_t>(worker) * 2 * dim;
#pragma omp for schedule(static)
        for (std::size_t row = 0; row < rows; ++row) {
            const int group = labels[row];
            ++counts[worker * 2 + group];
            const float* input = x + row * dim;
            double* sum = local + group * dim;
#pragma omp simd
            for (int col = 0; col < dim; ++col) sum[col] += input[col];
        }
    }
    for (int group = 0; group < 2; ++group) {
        std::size_t count = 0;
        for (int worker = 0; worker < threads; ++worker) count += counts[worker * 2 + group];
        for (int col = 0; col < dim; ++col) {
            double sum = 0;
            for (int worker = 0; worker < threads; ++worker)
                sum += partial[static_cast<std::size_t>(worker) * 2 * dim + group * dim + col];
            out[group * dim + col] = static_cast<float>(sum / std::max<std::size_t>(count, 1));
        }
    }
}
