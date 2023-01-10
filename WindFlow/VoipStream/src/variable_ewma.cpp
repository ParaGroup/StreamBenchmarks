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

#include<voip_stream/variable_ewma.hpp>

namespace voip_stream{

VariableEWMA::VariableEWMA(double age)
    : decay_(2 / (age + 1))
    , average_(0)
    , count_(0)
{}

void VariableEWMA::add(double value)
{
    if (average_ < WARMUP_SAMPLES) {
        count_++;
        average_ += value;
    } else if (average_ == WARMUP_SAMPLES) {
        average_ = average_ / WARMUP_SAMPLES;
        count_++;
    } else {
        average_ = (value * decay_) + (average_ * (1 - decay_));
    }
}

double VariableEWMA::get_average() const
{
    if (average_ <= WARMUP_SAMPLES) {
        return 0.0;
    }

    return average_;
}

}
