/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.affinity.impl;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.BitSet;

/*
 * Created by andre on 20/06/15.
 */
public final class Utilities {
    private Utilities() {
        throw new InstantiationError("Must not instantiate this class");
    }

    /**
     * Creates a hexademical representation of the bit set
     *
     * @param set the bit set to convert
     * @return the hexademical string representation
     */
    public static String toHexString(final BitSet set) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(out);
        final long[] longs = set.toLongArray();
        for (long aLong : longs) {
            writer.write(Long.toHexString(aLong));
        }
        writer.flush();

        return new String(out.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
    }

    public static String toBinaryString(BitSet set) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(out);
        final long[] longs = set.toLongArray();
        for (long aLong : longs) {
            writer.write(Long.toBinaryString(aLong));
        }
        writer.flush();

        return new String(out.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
    }
}
