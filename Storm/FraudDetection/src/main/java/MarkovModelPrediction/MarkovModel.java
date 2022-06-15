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

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 *
 * @author mayconbordin
 */
public class MarkovModel {
    private List<String> states;
    private double[][] stateTransitionProb;
    private int numStates;

    public MarkovModel(String model) {
        Scanner scanner = new Scanner(model);
        int lineCount = 0;
        int row = 0;

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (lineCount == 0) {
                //states
                String[] items = line.split(",");
                states = Arrays.asList(items);
                numStates = items.length;
                stateTransitionProb = new double[numStates][numStates];
            } else {
                //populate state transtion probability
                deseralizeTableRow(stateTransitionProb, line, ",", row, numStates);
                ++row;
            }
            ++lineCount;
        }
        scanner.close();
    }

    /**
     * @param table
     * @param data
     * @param delim
     * @param row
     * @param numCol
     */
    private void deseralizeTableRow(double[][] table, String data, String delim, int row, int numCol) {
        String[] items = data.split(delim);
        if (items.length != numCol) {
            throw new IllegalArgumentException("SchemaRecord serialization failed, number of tokens in string does not match with number of columns");
        }
        for (int c = 0; c < numCol; ++c) {
            table[row][c] = Double.parseDouble(items[c]);
        }
    }

    public List<String> getStates() {
        return states;
    }

    public double[][] getStateTransitionProb() {
        return stateTransitionProb;
    }

    public int getNumStates() {
        return numStates;
    }


}

