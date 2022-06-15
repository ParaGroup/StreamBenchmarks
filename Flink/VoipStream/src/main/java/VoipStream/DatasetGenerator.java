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

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class DatasetGenerator {
    private List<String> numberPool;
    private long calls;

    private static String randomTelephoneNumber() {
        return "" + (10_000_000_000L + ThreadLocalRandom.current().nextLong(90_000_000_000L));
    }

    public DatasetGenerator(long numbers, long calls) {
        assert numbers > 1;

        this.calls = calls;
        numberPool = new ArrayList<>();
        for (long i = 0; i < numbers; i++) {
            numberPool.add(randomTelephoneNumber());
        }
    }

    private String pickRandomNumber() {
        return pickRandomNumber(null);
    }

    private String pickRandomNumber(String not) {
        String number;
        do {
            int index = ThreadLocalRandom.current().nextInt(numberPool.size());
            number = numberPool.get(index);
        } while (number.equals(not));
        return number;
    }

    public void generate() {
        for (long i = 0; i < calls; i++) {
            String calling = pickRandomNumber();
            String called = pickRandomNumber(calling);
            DateTime answerTimestamp = DateTime.now();
            int callDuration = ThreadLocalRandom.current().nextInt(3600 * 24);
            boolean callEstablished = ThreadLocalRandom.current().nextDouble() >= 0.05;

            System.out.printf("%s,%s,%s,%d,%b\n", calling, called, answerTimestamp, callDuration, callEstablished);
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: <numbers> <calls>");
            System.exit(1);
        }

        long numbers = Long.parseUnsignedLong(args[0]);
        long calls = Long.parseUnsignedLong(args[1]);
        DatasetGenerator generator = new DatasetGenerator(numbers, calls);
        generator.generate();
    }
}
