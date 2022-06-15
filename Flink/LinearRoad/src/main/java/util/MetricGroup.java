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

package util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Stack;

// TODO instead of a static class one could pass the object around
public class MetricGroup {
    private static HashMap<String, Stack<Sampler>> map;

    static {
        map = new HashMap<>();
    }

    // XXX this is not time critical, making the whole method synchronized is good enough
    public static synchronized void add(String name, Sampler sampler) {
        Stack<Sampler> samplers = map.computeIfAbsent(name, key -> new Stack<>());
        samplers.add(sampler);
    }

    // XXX this consumes the groups
    public static void dumpAll() throws IOException {
        for (String name : map.keySet()) {
            Metric metric = getMetric(name);
            metric.dump();
        }
    }

    private static Metric getMetric(String name) {
        Metric metric = new Metric(name);

        // consume all the groups
        long total = 0;
        Stack<Sampler> samplers = map.get(name);
        while (!samplers.empty()) {
            Sampler sampler = samplers.pop();

            // add all the values from the sampler
            total += sampler.getTotal();
            for (double value : sampler.getValues()) {
                metric.add(value);
            }
        }

        // set total
        metric.setTotal(total);
        return metric;
    }
}
