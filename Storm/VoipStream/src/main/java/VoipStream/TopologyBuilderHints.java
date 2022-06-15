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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.storm.topology.*;
import org.slf4j.Logger;
import util.Configuration;
import util.Log;

public class TopologyBuilderHints extends TopologyBuilder {
    private static final Logger LOG = Log.get(TopologyBuilderHints.class);

    private Configuration configuration;

    public TopologyBuilderHints(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public SpoutDeclarer setSpout(String id, IRichSpout spout) throws IllegalArgumentException {
        int parallelismHint = getParallelismHint(id);
        LOG.info("SPOUT: {} ({})", id, parallelismHint);
        return super.setSpout(id, spout, parallelismHint);
    }

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt) throws IllegalArgumentException {
        int parallelismHint = getParallelismHint(id);
        LOG.info("BOLT: {} ({})", id, parallelismHint);
        return super.setBolt(id, bolt, parallelismHint);
    }

    private int getParallelismHint(String id) {
        JsonNode jsonNode = configuration.getTree().get(id);
        if (jsonNode == null) {
            return 1;
        } else {
            return jsonNode.numberValue().intValue();
        }
    }
}
