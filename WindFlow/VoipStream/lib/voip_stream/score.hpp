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

#include "filter_tuple.hpp"
#include "score_tuple.hpp"
#include "scorer_map.hpp"
#include <array>
#include <windflow.hpp>

namespace voip_stream {

class Score
{
public:

    Score();

    ScoreTuple operator ()(const FilterTuple &tuple, wf::RuntimeContext &rc);

private:

    template <typename Values>
    double sum(const Values &values, const std::array<double, 3> &weights) const
    {
        double sum = 0.0;

        for (int i = 0; i < 3; ++i) {
            sum += (values[i] * weights[i]);
        }

        return sum;
    }

private:

    ScorerMap scorer_map_;
    std::array<double, 3> weights_;
};

}
