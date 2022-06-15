/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Andrea Cardaci
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

package common;

/**
 * {@link AvgValue} is an class that helps to compute an average.
 *
 * @author mjsax
 */
public final class AvgValue {
    /**
     * The current sum over all values.
     */
    private int sum;

    /**
     * The current number of comm values.
     */
    private int count;


    /**
     * Instantiates a new {@link AvgValue} object with initial value.
     *
     * @param initalValue the first value of the average
     */
    public AvgValue(int initalValue) {
        this.sum = initalValue;
        this.count = 1;
    }


    /**
     * Adds a new value to the average.
     *
     * @param value the value to be added to the average
     */
    public void updateAverage(int value) {
        this.sum += value;
        ++this.count;
    }

    /**
     * Returns the current average.
     *
     * @return the current average
     */
    public Integer getAverage() {
        return this.sum / this.count;
    }

}
