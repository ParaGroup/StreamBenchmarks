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

namespace voip_stream {

constexpr int SOURCE_CT24 = 0;
constexpr int SOURCE_GlobalACD = 1;
constexpr int SOURCE_ECR24 = 2;
constexpr int SOURCE_RCR = 3;
constexpr int SOURCE_ECR = 4;
constexpr int SOURCE_ENCR = 5;
constexpr int SOURCE_FoFiR = 6;
constexpr int SOURCE_ACD = 7;
constexpr int SOURCE_URL = 8;

constexpr int VAR_DETECT_APROX_SIZE = 180000;
constexpr double VAR_DETECT_ERROR_RATE = 0.05;

constexpr double ACD_THRESHOLD_MIN = 5.0;
constexpr double ACD_THRESHOLD_MAX = 10.0;
constexpr double ACD_DECAY_FACTOR = 86400.0;
constexpr double ACD_WEIGHT = 3.0;

constexpr double URL_THRESHOLD_MIN = 0.5;
constexpr double URL_THRESHOLD_MAX = 1.0;
constexpr double URL_WEIGHT = 3.0;

constexpr double FOFIR_THRESHOLD_MIN = 2.0;
constexpr double FOFIR_THRESHOLD_MAX = 10.0;
constexpr double FOFIR_WEIGHT = 2.0;

constexpr int CT24_NUM_ELEMENTS = 180000;
constexpr int CT24_BUCKETS_PER_ELEMENT = 10;
constexpr int CT24_BUCKETS_PER_WORD = 16;
constexpr double CT24_BETA = 0.9917;

constexpr int ECR24_NUM_ELEMENTS = 180000;
constexpr int ECR24_BUCKETS_PER_ELEMENT = 10;
constexpr int ECR24_BUCKETS_PER_WORD = 16;
constexpr double ECR24_BETA = 0.9917;

constexpr int ECR_NUM_ELEMENTS = 180000;
constexpr int ECR_BUCKETS_PER_ELEMENT = 10;
constexpr int ECR_BUCKETS_PER_WORD = 16;
constexpr double ECR_BETA = 0.9672;

constexpr int ENCR_NUM_ELEMENTS = 180000;
constexpr int ENCR_BUCKETS_PER_ELEMENT = 10;
constexpr int ENCR_BUCKETS_PER_WORD = 16;
constexpr double ENCR_BETA = 0.9672;

constexpr int RCR_NUM_ELEMENTS = 180000;
constexpr int RCR_BUCKETS_PER_ELEMENT = 10;
constexpr int RCR_BUCKETS_PER_WORD = 16;
constexpr double RCR_BETA = 0.9672;

}
