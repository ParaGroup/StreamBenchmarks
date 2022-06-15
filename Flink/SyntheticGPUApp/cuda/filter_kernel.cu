 extern "C"
__global__ void filter_kernel(float *v1, float *v2, float *v3, float *v4, int *flags, int size)
{
    int id = threadIdx.x + blockIdx.x * blockDim.x; // id of the thread in the kernel
    int num_threads = gridDim.x * blockDim.x; // number of threads in the kernel
    for (int i=id; i<size; i+=num_threads) {
        if (v1[i] < 0.9 && v2[i] < 0.9 && v3[i] < 0.9 && v4[i] < 0.9) {
            flags[i] = 1;
            v1[i] = (v1[i] + v2[i] + v3[i] + v4[i])/4;
            v2[i] = v3[i] = v4[i] = 0;
        }
        else {
            flags[i] = 0;
        }
    }
}
