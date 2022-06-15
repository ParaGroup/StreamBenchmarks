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

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class CallDetailRecord {
    public String callingNumber;
    public String calledNumber;
    public long answerTimestamp;
    public int callDuration;
    public boolean callEstablished;

    // needs to be public to be POJO
    public CallDetailRecord() {
    }

    public CallDetailRecord(String str) {
        String[] psb = str.split(",");

        callingNumber = psb[0];
        calledNumber = psb[1];

        String dateTime = psb[2];
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        answerTimestamp = DateTime.parse(dateTime, formatter).getMillis() / 1000;
        callDuration = Integer.parseInt(psb[3]);
        callEstablished = Boolean.parseBoolean(psb[4]);
    }
}
