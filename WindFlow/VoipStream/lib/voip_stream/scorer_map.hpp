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

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace voip_stream {

class ScorerMap
{
public:

    class Entry
    {
        friend class ScorerMap;

    public:

        void set(int src, double rate);
        double get(int src) const;
        bool is_full() const;
        int pos(int src) const;
        const std::vector<double> &get_values() const;

    private:

        Entry(const std::vector<int> &fields);

    private:

        const std::vector<int> &fields_;
        std::vector<double> values_;
    };

public:

    ScorerMap(std::vector<int> fields);
    std::unordered_map<std::string, Entry> &get_map();
    Entry new_entry() const;

public:

    static double score(double v1, double v2, double vi);

private:

    std::unordered_map<std::string, Entry> map_;
    std::vector<int> fields_;
};

}
