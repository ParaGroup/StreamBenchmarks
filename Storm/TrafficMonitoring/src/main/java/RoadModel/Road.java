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

package RoadModel;

import Util.collections.FixedSizeQueue;

/**
 *  @author Alessandra Fais
 *  @version June 2019
 *
 *  The class defines a road.
 */
public class Road {
    private final int roadID;
    private final FixedSizeQueue<Integer> roadSpeed;
    private int averageSpeed;
    private int count;

    public Road(int roadID) {
        this.roadID = roadID;
        this.roadSpeed = new FixedSizeQueue<>(30);
    }

    public int getRoadID() {
        return roadID;
    }

    public int getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(int averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
    
    public void incrementCount() {
        this.count++;
    }

    public FixedSizeQueue<Integer> getRoadSpeed() {
        return roadSpeed;
    }

    public boolean addRoadSpeed(int speed) {
        return roadSpeed.add(speed);
    }
    
    public int getRoadSpeedSize() {
        return roadSpeed.size();
    }
}
