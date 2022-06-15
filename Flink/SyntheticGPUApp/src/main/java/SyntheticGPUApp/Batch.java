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

// class Tuple
public class Batch {
    int size;
    int[] identifiers;
    float[] v1;
    float[] v2;
    float[] v3;
    float[] v4;
    int[] flags;

    // Default Constructor
    public Batch(int _capacity) {
        size = 0;
        identifiers = new int[_capacity];
        v1 = new float[_capacity];
        v2 = new float[_capacity];
        v3 = new float[_capacity];
        v4 = new float[_capacity];
        flags = new int[_capacity];
    }

    // add a new element at the end of the Batch
    public void append(int _id, float _v1, float _v2, float _v3, float _v4) {
        identifiers[size] = _id;
        v1[size] = _v1;
        v2[size] = _v2;
        v3[size] = _v3;
        v4[size] = _v4;
        flags[size] = 0; // 0 means false
        size++;
    }

    // get the number of valid elements of the batch
    public int getSize() {
        return size;
    }
}
