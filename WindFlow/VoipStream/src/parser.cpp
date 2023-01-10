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

#include<voip_stream/parser.hpp>
#include<util/log.hpp>
//#include <charconv>
#include<sstream>

namespace voip_stream {

std::ostream &operator <<(std::ostream &os, const Parser::Tuple &tuple)
{
    return os << tuple.ts
              << " cdr.calling_number=" << tuple.cdr.calling_number
              << " cdr.called_number=" << tuple.cdr.called_number
              << " cdr.answer_timestamp=" << tuple.cdr.answer_timestamp
              << " cdr.call_established=" << tuple.cdr.call_established
              << " ...";
}

Parser::Tuple Parser::operator ()(const util::LineReaderSource::Tuple &tuple, wf::RuntimeContext &rc)
{
    DEBUG_LOG("parser::tuple " << tuple);

    Parser::Tuple result;
    result.ts = tuple.ts;
    result.cdr = CallDetailRecord::parse(*tuple.line);
    return result;
}

}
