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

package VoipStream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import common.CallDetailRecord;
import util.Log;

import java.util.Map;

class PreRCR extends BaseRichBolt {
    private static final Logger LOG = Log.get(PreRCR.class);

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "timestamp", "calling_number", "called_number", "answer_timestamp", "new_callee", "cdr", "ecr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");

        // fetch values from the tuple
        String callingNumber = (String) tuple.getValueByField("calling_number");
        String calledNumber = (String) tuple.getValueByField("called_number");
        long answerTimestamp = (long) tuple.getValueByField("answer_timestamp");
        boolean newCallee = (Boolean) tuple.getValueByField("new_callee");
        CallDetailRecord cdr = (CallDetailRecord) tuple.getValueByField("cdr");
        double ecr = (double) tuple.getValueByField("ecr");

        // emits the tuples twice, the key is calling then called number
        Values values = new Values(callingNumber, timestamp, callingNumber, calledNumber, answerTimestamp, newCallee, cdr, ecr);
        outputCollector.emit(values);
        LOG.debug("tuple out: {}", values);

        values = new Values(calledNumber, timestamp, callingNumber, calledNumber, answerTimestamp, newCallee, cdr, ecr);
        outputCollector.emit(values);
        LOG.debug("tuple out: {}", values);

        //outputCollector.ack(tuple);
    }
}
