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

package TrafficMonitoring;

public class Source_Event {
	public String vehicleID;
	public double latitude;
	public double longitude;
	public double speed;
	public int bearing;
	public long ts;

	public Source_Event() {
		vehicleID = "";
		latitude = 0d;
		longitude = 0d;
		speed = 0d;
		bearing = 0;
		ts = 0L;
	}

	public Source_Event(String _vehicleID, double _latitude, double _longitude, double _speed, int _bearing, long _ts) {
		vehicleID = _vehicleID;
		latitude = _latitude;
		longitude = _longitude;
		speed = _speed;
		bearing = _bearing;
		ts = _ts;
	}
}
