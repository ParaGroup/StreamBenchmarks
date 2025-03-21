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
import common.Constants;
import common.ODTDBloomFilter;
import common.ScorerMap;
import util.Log;

import java.util.Map;

class RCR extends BaseRichBolt {
    private static final Logger LOG = Log.get(RCR.class);

    private OutputCollector outputCollector;

    private ODTDBloomFilter filter;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;

        int numElements = Constants.RCR_NUM_ELEMENTS;
        int bucketsPerElement = Constants.RCR_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.RCR_BUCKETS_PER_WORD;
        double beta = Constants.RCR_BETA;
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source", "timestamp", "calling_number", "answer_timestamp", "rate", "cdr", "ecr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");
        CallDetailRecord cdr = (CallDetailRecord) tuple.getValueByField("cdr");

        if (cdr.callEstablished) {
            String key = (String) tuple.getValueByField("key");

            // default stream
            if (key.equals(cdr.callingNumber)) {
                String callee = cdr.calledNumber;
                filter.add(callee, 1, cdr.answerTimestamp);
            }
            // backup stream
            else {
                String caller = cdr.callingNumber;
                double rcr = filter.estimateCount(caller, cdr.answerTimestamp);

                double ecr = (double) tuple.getValueByField("ecr");
                Values out = new Values(ScorerMap.RCR, timestamp, caller, cdr.answerTimestamp, rcr, cdr, ecr);
                outputCollector.emit(out);
                LOG.debug("tuple out: {}", out);
            }
        }

        //outputCollector.ack(tuple);
    }
}
