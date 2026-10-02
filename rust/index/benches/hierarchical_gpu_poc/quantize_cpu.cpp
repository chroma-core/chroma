#include <cmath>
#include <cstddef>
#include <cstdint>

// A fused, multithreaded CPU reference for the 1-bit writer code and header.
extern "C" void quantize_cpu(const float* vectors, const float* center,
                              std::uint8_t* codes, float* headers,
                              std::size_t rows, int dim) {
#pragma omp parallel for schedule(static)
    for (std::size_t row = 0; row < rows; ++row) {
        float sum_abs = 0.0f;
        float sum_sq = 0.0f;
        float dot = 0.0f;
        int ones = 0;
        for (int byte = 0; byte < dim / 8; ++byte) {
            unsigned packed = 0;
            for (int bit = 0; bit < 8; ++bit) {
                int col = byte * 8 + bit;
                float residual = vectors[row * dim + col] - center[col];
                packed |= static_cast<unsigned>(residual >= 0.0f) << bit;
                sum_abs += std::fabs(residual);
                sum_sq += residual * residual;
                dot += residual * center[col];
            }
            codes[row * (dim / 8) + byte] = static_cast<std::uint8_t>(packed);
            ones += __builtin_popcount(packed);
        }
        float norm = std::sqrt(sum_sq);
        headers[row * 4] = norm < 1.1920929e-7f ? 1.0f : 0.5f * sum_abs / norm;
        headers[row * 4 + 1] = norm;
        headers[row * 4 + 2] = dot;
        headers[row * 4 + 3] = static_cast<float>(2 * ones - dim);
    }
}
