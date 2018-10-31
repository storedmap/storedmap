/*
 * Copyright 2018 Fyodor Kravchenko <fedd@vsetec.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.vsetec.storedmap;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.CollationKey;
import java.text.Collator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.SerializationUtils;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class MapData implements Serializable {

    private final LinkedHashMap<String, Object> _map = new LinkedHashMap<>();
    private final Object[] _sorterObject = new Object[1];
    private final List<String> _tags = new ArrayList<>(4);

    public MapData() {
    }

    LinkedHashMap<String, Object> getMap() {
        return _map;
    }

    synchronized void putTags(String[] tags) {
        _tags.clear();
        _tags.addAll(Arrays.asList(tags));
    }

    String[] getTags() {
        return _tags.toArray(new String[_tags.size()]);
    }

    void putSorter(Object sorter) {
        _sorterObject[0] = sorter;
    }

    Object getSorter() {
        return _sorterObject[0];
    }

    byte[] getSorterAsBytes(Collator collator, int maximumSorterLength) {
        if (_sorterObject[0] == null) {
            return null;
        } else if (_sorterObject[0] instanceof String) {
            String sorter = (String) _sorterObject[0];
            CollationKey ck = collator.getCollationKey(sorter);
            byte[] arr = ck.toByteArray();
            return arr;
        } else if (_sorterObject[0] instanceof Instant) {
            Instant sorter = (Instant) _sorterObject[0];

            String timeString = ((Instant) sorter).toString();
            _sorterObject[0] = sorter;
            byte[] bytes = timeString.getBytes(StandardCharsets.US_ASCII);
            return bytes;

        } else if (_sorterObject[0] instanceof Number) {
            Number sorter = (Number) _sorterObject[0];

            byte[] gazillionByteRepresentation = new byte[maximumSorterLength - 1];
            gazillionByteRepresentation[0] = Byte.MAX_VALUE;
            for (int i = 1; i < gazillionByteRepresentation.length; i++) {
                gazillionByteRepresentation[i] = -1;
            } // making 7fffffffffffffffffffffff...
            BigInteger biggestInteger = new BigInteger(gazillionByteRepresentation);

            Number number = (Number) sorter;
            BigDecimal bd = new BigDecimal(number.toString());  //123.456
            bd = bd.movePointRight(maximumSorterLength / 2);     //123456000000. 
            BigInteger bi = bd.toBigInteger();                  //123456000000 - discard everything after decimal point
            if (bi.signum() > 1 && bi.compareTo(biggestInteger) > 1) { // ignore values too big
                bi = biggestInteger;
            } else if (bi.signum() < 1 && bi.abs().compareTo(biggestInteger) > 1) { // or too negative big
                bi = biggestInteger.negate();
            }

            bi = bi.add(biggestInteger);                        // now it's always positive
            byte[] bytes = bi.toByteArray();
            byte[] bytesB = new byte[maximumSorterLength];     // byte array of the desired length
            int latestZero = maximumSorterLength;
            boolean metNonZero = false;
            for (int i = bytes.length - 1, y = bytesB.length - 1; y >= 0; i--, y--) { // fill it from the end; leading zeroes will remain
                bytesB[y] = bytes[i];
                // look for latest zero
                if (!metNonZero) {
                    if (bytes[i] == 0) {
                        latestZero = y;
                    } else {
                        metNonZero = true;
                    }
                }
            }
            // crop the trailing zeroes (if we have some)
            byte[] bytesRet = new byte[latestZero];
            System.arraycopy(bytesB, 0, bytesRet, 0, latestZero);
            return bytesRet;
        } else if (_sorterObject[0] instanceof Serializable) {
            return SerializationUtils.serialize((Serializable) _sorterObject[0]);
        } else {
            return new byte[0];
        }
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 19 * hash + Objects.hashCode(this._map);
        //hash = 19 * hash + Objects.hashCode(this._locales);
        hash = 19 * hash + Objects.hashCode(this._sorterObject[0]);
        hash = 19 * hash + Objects.hashCode(this._tags);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MapData other = (MapData) obj;
        if (!Objects.equals(this._map, other._map)) {
            return false;
        }
//        if (!Objects.equals(this._locales, other._locales)) {
//            return false;
//        }
        if (!Objects.equals(this._sorterObject[0], other._sorterObject[0])) {
            return false;
        }
        return Objects.equals(this._tags, other._tags);
    }

}
