/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
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

package YSB;

public class YSB_Event {
	public String uuid1;
	public String uuid2;
	public String ad_id;
	public String ad_type;
	public String event_type;
	public long timestamp;
	public String ip;

	public YSB_Event() {
		uuid1 = "";
		uuid2 = "";
		ad_id = "";
		ad_type = "";
		event_type = "";
		timestamp = 0;
		ip = "";
	}

	public YSB_Event(String _uuid1, String _uuid2, String _ad_id, String _ad_type, String _event_type, long _ts, String _ip) {
		uuid1 = _uuid1;
		uuid2 = _uuid2;
		ad_id = _ad_id;
		ad_type = _ad_type;
		event_type = _event_type;
		timestamp = _ts;
		ip = _ip;
	}
}
