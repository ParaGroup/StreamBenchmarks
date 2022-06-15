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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ScorerMap {
    public final static int CT24 = 0;
    public final static int GlobalACD = 1;
    public final static int ECR24 = 2;
    public final static int RCR = 3;
    public final static int ECR = 4;
    public final static int ENCR = 5;
    public final static int FoFiR = 6;
    public final static int ACD = 7;
    public final static int URL = 8;

    public static class Entry {
        public int[] fields;
        public double[] values;

        private Entry(int[] fields) {
            this.fields = fields;

            values = new double[fields.length];
            Arrays.fill(values, Double.NaN);
        }

        public void set(int src, double rate) {
            values[pos(src)] = rate;
        }

        public double get(int src) {
            return values[pos(src)];
        }

        public boolean isFull() {
            for (double value : values) {
                if (Double.isNaN(value)) {
                    return false;
                }
            }
            return true;
        }

        private int pos(int src) {
            for (int i = 0; i < fields.length; i++) {
                if (fields[i] == src) {
                    return i;
                }
            }
            return -1;
        }

        public double[] getValues() {
            return values;
        }
    }

    private Map<String, Entry> map = new HashMap<>();
    public int[] fields;

    public ScorerMap(int[] fields) {
        this.fields = fields;
    }

    public Map<String, Entry> getMap() {
        return map;
    }

    public Entry newEntry() {
        return new Entry(fields);
    }

    public static double score(double v1, double v2, double vi) {
        double score = vi / (v1 + (v2 - v1));
        if (score < 0) {
            score = 0;
        }
        if (score > 1) {
            score = 1;
        }
        return score;
    }

}
