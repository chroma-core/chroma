#include <cuda_runtime.h>
#include <stdint.h>

__global__ void score_codes(const uint8_t* codes, const uint64_t* planes,
                            const int32_t* leaf_indices, const float* params,
                            float* output, int count) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const uint8_t* row = codes + (size_t)i * 144;
    int leaf = leaf_indices[i];
    const uint64_t* q = planes + (size_t)leaf * 64;
    const float* p = params + (size_t)leaf * 6;
    float correction = *(const float*)row;
    float norm = *(const float*)(row + 4);
    float radial = *(const float*)(row + 8);
    int signed_sum = *(const int*)(row + 12);
    const uint64_t* bits = (const uint64_t*)(row + 16);
    uint32_t dot = 0;
    for (int j = 0; j < 16; ++j) {
        uint64_t x = bits[j];
        dot += __popcll(x & q[j]);
        dot += 2 * __popcll(x & q[16 + j]);
        dot += 4 * __popcll(x & q[32 + j]);
        dot += 8 * __popcll(x & q[48 + j]);
    }
    float signed_dot = 2.0f * (float)dot - p[0];
    float g_dot_r_q = 0.5f * (p[2] * signed_dot + p[1] * (float)signed_sum);
    float r_dot_r_q = norm * g_dot_r_q / correction;
    float d_dot_q = p[4] + radial + r_dot_r_q;
    float d_norm_sq = p[3] * p[3] + 2.0f * radial + norm * norm;
    output[i] = d_norm_sq + p[5] * p[5] - 2.0f * d_dot_q;
}

extern "C" int hspann_score_codes(const uint8_t* codes, const uint8_t* planes,
                                  const int32_t* leaf_indices, const float* params,
                                  float* output, int count, int leaves) {
    if (count == 0) return 0;
    uint8_t* d_codes = nullptr;
    uint8_t* d_planes = nullptr;
    int32_t* d_indices = nullptr;
    float* d_params = nullptr;
    float* d_output = nullptr;
    cudaError_t error = cudaSuccess;
#define CHECK(call) do { error = (call); if (error != cudaSuccess) goto done; } while (0)
    CHECK(cudaMalloc(&d_codes, (size_t)count * 144));
    CHECK(cudaMalloc(&d_planes, (size_t)leaves * 512));
    CHECK(cudaMalloc(&d_indices, (size_t)count * sizeof(int32_t)));
    CHECK(cudaMalloc(&d_params, (size_t)leaves * 6 * sizeof(float)));
    CHECK(cudaMalloc(&d_output, (size_t)count * sizeof(float)));
    CHECK(cudaMemcpy(d_codes, codes, (size_t)count * 144, cudaMemcpyHostToDevice));
    CHECK(cudaMemcpy(d_planes, planes, (size_t)leaves * 512, cudaMemcpyHostToDevice));
    CHECK(cudaMemcpy(d_indices, leaf_indices, (size_t)count * sizeof(int32_t), cudaMemcpyHostToDevice));
    CHECK(cudaMemcpy(d_params, params, (size_t)leaves * 6 * sizeof(float), cudaMemcpyHostToDevice));
    score_codes<<<(count + 255) / 256, 256>>>(d_codes, (const uint64_t*)d_planes,
                                              d_indices, d_params, d_output, count);
    CHECK(cudaGetLastError());
    CHECK(cudaMemcpy(output, d_output, (size_t)count * sizeof(float), cudaMemcpyDeviceToHost));
done:
    cudaFree(d_output);
    cudaFree(d_params);
    cudaFree(d_indices);
    cudaFree(d_planes);
    cudaFree(d_codes);
    return (int)error;
#undef CHECK
}
