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

struct Buffers {
    uint8_t* codes = nullptr;
    uint8_t* planes = nullptr;
    int32_t* indices = nullptr;
    float* params = nullptr;
    float* output = nullptr;
    int count_capacity = 0;
    int leaf_capacity = 0;

    ~Buffers() {
        cudaFree(output);
        cudaFree(params);
        cudaFree(indices);
        cudaFree(planes);
        cudaFree(codes);
    }

    cudaError_t ensure(int count, int leaves) {
        if (count > count_capacity) {
            int capacity = 1;
            while (capacity < count) capacity *= 2;
            cudaFree(codes);
            cudaFree(indices);
            cudaFree(output);
            codes = nullptr;
            indices = nullptr;
            output = nullptr;
            cudaError_t error = cudaMalloc(&codes, (size_t)capacity * 144);
            if (error != cudaSuccess) return error;
            error = cudaMalloc(&indices, (size_t)capacity * sizeof(int32_t));
            if (error != cudaSuccess) return error;
            error = cudaMalloc(&output, (size_t)capacity * sizeof(float));
            if (error != cudaSuccess) return error;
            count_capacity = capacity;
        }
        if (leaves > leaf_capacity) {
            int capacity = 1;
            while (capacity < leaves) capacity *= 2;
            cudaFree(planes);
            cudaFree(params);
            planes = nullptr;
            params = nullptr;
            cudaError_t error = cudaMalloc(&planes, (size_t)capacity * 512);
            if (error != cudaSuccess) return error;
            error = cudaMalloc(&params, (size_t)capacity * 6 * sizeof(float));
            if (error != cudaSuccess) return error;
            leaf_capacity = capacity;
        }
        return cudaSuccess;
    }
};

extern "C" int hspann_score_codes(const uint8_t* codes, const uint8_t* planes,
                                  const int32_t* leaf_indices, const float* params,
                                  float* output, int count, int leaves) {
    if (count == 0) return 0;
    static thread_local Buffers buffers;
    cudaError_t error = buffers.ensure(count, leaves);
    if (error != cudaSuccess) return (int)error;
#define CHECK(call) do { error = (call); if (error != cudaSuccess) return (int)error; } while (0)
    CHECK(cudaMemcpy(buffers.codes, codes, (size_t)count * 144, cudaMemcpyHostToDevice));
    CHECK(cudaMemcpy(buffers.planes, planes, (size_t)leaves * 512, cudaMemcpyHostToDevice));
    CHECK(cudaMemcpy(buffers.indices, leaf_indices, (size_t)count * sizeof(int32_t), cudaMemcpyHostToDevice));
    CHECK(cudaMemcpy(buffers.params, params, (size_t)leaves * 6 * sizeof(float), cudaMemcpyHostToDevice));
    score_codes<<<(count + 255) / 256, 256>>>(buffers.codes, (const uint64_t*)buffers.planes,
                                              buffers.indices, buffers.params, buffers.output, count);
    CHECK(cudaGetLastError());
    CHECK(cudaMemcpy(output, buffers.output, (size_t)count * sizeof(float), cudaMemcpyDeviceToHost));
    return 0;
#undef CHECK
}
