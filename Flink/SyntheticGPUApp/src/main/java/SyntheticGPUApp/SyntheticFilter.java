/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *  
 *  This file is part of StreamBenchmarks.
 *  
 *  StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *  
 *  StreamBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

package SyntheticGPUApp;

import jcuda.*;
import java.util.*;
import java.lang.Math;
import jcuda.driver.*;
import jcuda.runtime.*;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.externalresource.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

// class SyntheticFilter
public class SyntheticFilter extends RichFlatMapFunction<Batch, Batch> {
    private final String MODE = "GPU";
    private static final String RESOURCE_NAME = "a30";
    private int received;
    private int batch_size;
    private transient Pointer v1_gpu;
    private transient Pointer v2_gpu;
    private transient Pointer v3_gpu;
    private transient Pointer v4_gpu;
    private transient Pointer flags_gpu;
    private transient CUfunction kernel_function;
    private transient cudaStream_t stream;
    private transient CUstream cuStream;

    // Constructor
    public SyntheticFilter(int _batch_size) {
        batch_size = _batch_size;
        received = 0;
    }

    // open method
    @Override
    public void open(Configuration cfg) {
        final String originTempDir = System.getProperty("java.io.tmpdir");
        final String newTempDir = originTempDir + "/jcuda-" + UUID.randomUUID();
        System.setProperty("java.io.tmpdir", newTempDir);
        // get the GPU information and select the first GPU
        final Set<ExternalResourceInfo> externalResourceInfos = getRuntimeContext().getExternalResourceInfos(RESOURCE_NAME);
        Preconditions.checkState(!externalResourceInfos.isEmpty(), "The program needs at least one GPU device while finding 0 GPU");
        final Optional<String> firstIndexOptional = externalResourceInfos.iterator().next().getProperty("index");
        Preconditions.checkState(firstIndexOptional.isPresent());
        // initialize JCublas with the selected GPU
        JCuda.cudaSetDevice(Integer.parseInt(firstIndexOptional.get()));
        // allocate GPU arrays
        v1_gpu = new Pointer();
        v2_gpu = new Pointer();
        v3_gpu = new Pointer();
        v4_gpu = new Pointer();
        flags_gpu = new Pointer();
        JCuda.cudaMalloc(v1_gpu, Sizeof.FLOAT * batch_size);
        JCuda.cudaMalloc(v2_gpu, Sizeof.FLOAT * batch_size);
        JCuda.cudaMalloc(v3_gpu, Sizeof.FLOAT * batch_size);
        JCuda.cudaMalloc(v4_gpu, Sizeof.FLOAT * batch_size);
        JCuda.cudaMalloc(flags_gpu, Sizeof.INT * batch_size);
        // load the ptx file of the kernel
        CUmodule module = new CUmodule();
        JCudaDriver.cuModuleLoad(module, "/home/mencagli/StreamBenchmarks/Flink/SyntheticGPUApp/cuda/filter_kernel.ptx");
        // obtain a function pointer to the kernel function
        kernel_function = new CUfunction();
        JCudaDriver.cuModuleGetFunction(kernel_function, module, "filter_kernel");
        // create the CUDA stream
        stream = new cudaStream_t();
        JCuda.cudaStreamCreate(stream);
        cuStream = new CUstream(stream);
    }

    // flatMap method
    @Override
    public void flatMap(Batch input, Collector<Batch> ctx) {
        if (MODE.equals("CPU")) { // CPU version
            for (int i=0; i<input.getSize(); i++) {
                if (input.v1[i] < 0.9 && input.v2[i] < 0.9 && input.v3[i] < 0.9 && input.v4[i] < 0.9) {
                    input.flags[i] = 1; // 1 means true
                    input.v1[i] = (input.v1[i] + input.v2[i] + input.v3[i] + input.v4[i]) / 4;
                    input.v2[i] = 0;
                    input.v3[i] = 0;
                    input.v4[i] = 0;
                }
            }
            ctx.collect(input);
        }
        else if (MODE.equals("GPU")) { // GPU version
            Set<ExternalResourceInfo> externalResourceInfos = getRuntimeContext().getExternalResourceInfos(RESOURCE_NAME);
            // copy batch arrays on the GPU arrays
            JCuda.cudaMemcpyAsync(v1_gpu, Pointer.to(input.v1), Sizeof.FLOAT*batch_size, cudaMemcpyKind.cudaMemcpyHostToDevice, stream);
            JCuda.cudaMemcpyAsync(v2_gpu, Pointer.to(input.v2), Sizeof.FLOAT*batch_size, cudaMemcpyKind.cudaMemcpyHostToDevice, stream);
            JCuda.cudaMemcpyAsync(v3_gpu, Pointer.to(input.v3), Sizeof.FLOAT*batch_size, cudaMemcpyKind.cudaMemcpyHostToDevice, stream);
            JCuda.cudaMemcpyAsync(v4_gpu, Pointer.to(input.v4), Sizeof.FLOAT*batch_size, cudaMemcpyKind.cudaMemcpyHostToDevice, stream);
            JCuda.cudaMemcpyAsync(flags_gpu, Pointer.to(input.flags), Sizeof.INT*batch_size, cudaMemcpyKind.cudaMemcpyHostToDevice, stream);
            // set up the kernel parameters
            Pointer kernelParameters = Pointer.to(Pointer.to(v1_gpu), Pointer.to(v2_gpu), Pointer.to(v3_gpu), Pointer.to(v4_gpu), Pointer.to(flags_gpu), Pointer.to(new int[]{batch_size}));
            int num_blocks = (int) Math.ceil(((float) batch_size) / 256.0);
            // call the kernel function
            JCudaDriver.cuLaunchKernel(kernel_function, 
                num_blocks,  1, 1, // grid dimension 
                256, 1, 1, // block dimension 
                0, cuStream, // shared memory size and stream 
                kernelParameters, null // kernel- and extra parameters
            );
            // copy on the host side the batch arrays
            JCuda.cudaMemcpyAsync(Pointer.to(input.v1), v1_gpu, Sizeof.FLOAT*batch_size, cudaMemcpyKind.cudaMemcpyDeviceToHost, stream);
            JCuda.cudaMemcpyAsync(Pointer.to(input.v2), v2_gpu, Sizeof.FLOAT*batch_size, cudaMemcpyKind.cudaMemcpyDeviceToHost, stream);
            JCuda.cudaMemcpyAsync(Pointer.to(input.v3), v3_gpu, Sizeof.FLOAT*batch_size, cudaMemcpyKind.cudaMemcpyDeviceToHost, stream);
            JCuda.cudaMemcpyAsync(Pointer.to(input.v4), v4_gpu, Sizeof.FLOAT*batch_size, cudaMemcpyKind.cudaMemcpyDeviceToHost, stream);
            JCuda.cudaMemcpyAsync(Pointer.to(input.flags), flags_gpu, Sizeof.INT*batch_size, cudaMemcpyKind.cudaMemcpyDeviceToHost, stream);
            // synchronization
            JCuda.cudaStreamSynchronize(stream);
            ctx.collect(input);
        }
        else {
            System.out.println("Unknown mode for operator SyntheticFilter");
            System.exit(1);
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        // de-allocate GPU arrays
        if (v1_gpu != null) {
            JCuda.cudaFree(v1_gpu);
        }
        if (v2_gpu != null) {
            JCuda.cudaFree(v2_gpu);
        }
        if (v3_gpu != null) {
            JCuda.cudaFree(v3_gpu);
        }
        if (v4_gpu != null) {
            JCuda.cudaFree(v4_gpu);
        }
        if (stream != null) {
            JCuda.cudaStreamDestroy(stream);
        }   
    }
}
