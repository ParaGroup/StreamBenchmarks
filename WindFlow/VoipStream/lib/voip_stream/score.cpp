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

#include "score.hpp"
#include "constants.hpp"
#include "util/log.hpp"

namespace voip_stream {

Score::Score()
    : scorer_map_({SOURCE_FoFiR, SOURCE_URL, SOURCE_ACD})
    , weights_({FOFIR_WEIGHT, URL_WEIGHT, ACD_WEIGHT})
{}

ScoreTuple Score::operator ()(const FilterTuple &tuple, wf::RuntimeContext &rc)
{
    DEBUG_LOG("score::tuple " << tuple);

    auto key = tuple.cdr.calling_number + ':' + std::to_string(tuple.cdr.answer_timestamp);
    auto &map = scorer_map_.get_map();
    auto it = map.find(key);
    ScoreTuple result;

    if (it != map.end()) {
        auto &entry = it->second;

        if (entry.is_full()) {
            double main_score = sum(entry.get_values(), weights_);

            result.ts = tuple.ts;
            result.cdr = tuple.cdr;
            result.score = main_score;
        } else {
            entry.set(tuple.source, tuple.rate);

            result.ts = tuple.ts;
            result.cdr = tuple.cdr;
            result.score = 0;
        }
    } else {
        ScorerMap::Entry entry = scorer_map_.new_entry();
        entry.set(tuple.source, tuple.rate);
        map.insert(std::make_pair(key, entry));

        result.ts = tuple.ts;
        result.cdr = tuple.cdr;
        result.score = 0;
    }
    return result;
}

}
