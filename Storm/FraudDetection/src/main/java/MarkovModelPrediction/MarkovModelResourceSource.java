/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Alessandra Fais
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

package MarkovModelPrediction;

import org.apache.storm.shade.com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

/**
 *
 * @author mayconbordin
 */
public class MarkovModelResourceSource implements IMarkovModelSource {
    private static final Logger LOG = LoggerFactory.getLogger(MarkovModelResourceSource.class);
    private Charset charset;

    public MarkovModelResourceSource() {
        charset = Charset.defaultCharset();
    }

    @Override
    public String getModel(String key) {
        try {
            URL url = Resources.getResource(key);
            return Resources.toString(url, charset);
        } catch (IOException ex) {
            LOG.error("Unable to load markov model from resource " + key, ex);
            return null;
        }
    }

}
