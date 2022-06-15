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

package Util.data;

/**
 * Generic Pair class
 * @author pranab
 *
 * @param <L>
 * @param <R>
 */
public class Pair<L,R>  {
    protected  L left;
    protected  R right;

    public Pair() {
    }

    public Pair(L left, R right) {
      this.left = left;
      this.right = right;
    }

    public L getLeft() {
        return left;
    }

    public void setLeft(L left) {
        this.left = left;
    }

    public R getRight() {
        return right;
    }

    public void setRight(R right) {
        this.right = right;
    }

    @Override
    public int hashCode() { 
        return left.hashCode() ^ right.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        boolean isEqual = false;
        if (null != other && other instanceof Pair) {
            Pair pairOther = (Pair) other;
            isEqual =  this.left.equals(pairOther.left) &&
                   this.right.equals(pairOther.right); 
        }
        return isEqual;
    }

}
