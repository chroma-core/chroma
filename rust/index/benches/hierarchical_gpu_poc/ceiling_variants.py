"""Streaming kernels for grouped centroid and coalesced quantization probes."""

import cupy as cp

GROUP_SUM = cp.RawKernel(
    r"""
extern "C" __global__ void group_sum(const float* x, const int* labels,
 float* partial, int rows, int dim, int tile) {
 int col = blockIdx.x * blockDim.x + threadIdx.x;
 int start = blockIdx.y * tile;
 if (col >= dim) return;
 float a = 0, b = 0;
 for (int row = start; row < rows && row < start + tile; ++row) {
   float value = x[(unsigned long long)row * dim + col];
   if (labels[row] == 0) a += value; else b += value;
 }
 partial[((unsigned long long)blockIdx.y * 2) * dim + col] = a;
 partial[((unsigned long long)blockIdx.y * 2 + 1) * dim + col] = b;
}
""",
    "group_sum",
)

QUANTIZE_STREAM = cp.RawKernel(
    r"""
extern "C" __global__ void quantize_stream(const float* x, const float* center,
 unsigned char* codes, float* stats, int rows, int dim) {
 unsigned long long row = blockIdx.x;
 int lane = threadIdx.x;
 float a=0, b=0, c=0;
 int ones=0;
 for (int col=lane; col<dim; col+=128) {
   float residual=x[row*dim+col]-center[col];
   unsigned mask=__ballot_sync(0xffffffff, residual>=0);
   if ((lane & 31)==0) ((unsigned*)codes)[row*(dim/32)+col/32]=mask;
   a+=fabsf(residual); b+=residual*residual; c+=residual*center[col];
   ones+=residual>=0;
 }
 __shared__ float aa[128], bb[128], cc[128];
 __shared__ int oo[128];
 aa[lane]=a; bb[lane]=b; cc[lane]=c; oo[lane]=ones;
 __syncthreads();
 for(int stride=64;stride>0;stride>>=1) {
   if(lane<stride) {aa[lane]+=aa[lane+stride];bb[lane]+=bb[lane+stride];cc[lane]+=cc[lane+stride];oo[lane]+=oo[lane+stride];}
   __syncthreads();
 }
 if(lane==0) {
   float norm=sqrtf(bb[0]);
   stats[row*4]=norm<1.1920929e-7f?1.0f:0.5f*aa[0]/norm;
   stats[row*4+1]=norm;stats[row*4+2]=cc[0];stats[row*4+3]=2*oo[0]-dim;
 }
}
""",
    "quantize_stream",
)
