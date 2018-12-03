/*
 * Copyright 2018 Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}.
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
package org.storedmap;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.CollationKey;
import java.text.Collator;
import java.time.Instant;
import java.util.UUID;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.lang3.SerializationUtils;

/**
 *
 * @author Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}
 */
public class Util {

    private static final Base32 _b = new Base32(true, (byte) '*');

    public static byte[] translateSorterIntoBytes(Object sorterObject, Collator collator, int maximumSorterLength) {
        if (sorterObject == null) {
            return null;
        } else if (sorterObject instanceof String) {
            String sorter = (String) sorterObject;
            CollationKey ck = collator.getCollationKey(sorter);
            byte[] arr = ck.toByteArray();
            return arr;
        } else if (sorterObject instanceof Instant) {
            Instant sorter = (Instant) sorterObject;

            String timeString = ((Instant) sorter).toString();
            sorterObject = sorter;
            byte[] bytes = timeString.getBytes(StandardCharsets.US_ASCII);
            return bytes;

        } else if (sorterObject instanceof Number) {
            Number sorter = (Number) sorterObject;

            byte[] gazillionByteRepresentation = new byte[maximumSorterLength - 1];
            gazillionByteRepresentation[0] = Byte.MAX_VALUE;
            for (int i = 1; i < gazillionByteRepresentation.length; i++) {
                gazillionByteRepresentation[i] = -1;
            } // making 7fffffffffffffffffffffff...
            BigInteger biggestInteger = new BigInteger(gazillionByteRepresentation);
            int biggestIntegerLength = biggestInteger.toString().length();

            Number number = (Number) sorter;
            BigDecimal bd = new BigDecimal(number.toString());  //123.456
            bd = bd.movePointRight(biggestIntegerLength / 2);     //123456000000.  - move point right for the half of the allowed number size
            BigInteger bi = bd.toBigInteger();                  //123456000000 - discard everything after decimal point as it is too far from the half of the allowed number size
            if (bi.signum() > 1 && bi.compareTo(biggestInteger) > 1) { // ignore values too big
                bi = biggestInteger;
            } else if (bi.signum() < 1 && bi.abs().compareTo(biggestInteger) > 1) { // or too negative big
                bi = biggestInteger.negate();
            }

            bi = bi.add(biggestInteger);                        // now it's always positive. we are no afraid to overspill as the biggest integer is one byte shorter then the really allowed
            byte[] bytes = bi.toByteArray();
            byte[] bytesB = new byte[maximumSorterLength];     // byte array of the desired length
            //int latestZero = maximumSorterLength;
            //boolean metNonZero = false;
            for (int i = bytes.length - 1, y = bytesB.length - 1; i >= 0; i--, y--) { // fill it from the end; leading zeroes will remain
                bytesB[y] = bytes[i];
                // look for latest zero
                //if (!metNonZero) {
                //    if (bytes[i] == 0) {
                //        latestZero = y;
                //    } else {
                //        metNonZero = true;
                //    }
                //}
            }

            return bytesB;
            // crop the trailing zeroes (if we have some)
            //byte[] bytesRet = new byte[latestZero];
            //System.arraycopy(bytesB, 0, bytesRet, 0, latestZero);
            //return bytesRet;
        } else if (sorterObject instanceof Serializable) {
            return SerializationUtils.serialize((Serializable) sorterObject);
        } else {
            return new byte[0];
        }
    }

    public static String getRidOfNonLatin(String string) {
        String trString;
        if (!string.matches("^[a-z][a-z0-9_]*$") || string.endsWith("w32")) {

            trString = _b.encodeAsString(string.getBytes(StandardCharsets.UTF_8));
            // strip the hell the padding
            int starPos = trString.indexOf("*");
            if (starPos > 0) {
                trString = trString.substring(0, starPos);
            }

            trString = trString + "w32";
        } else {

            trString = string;

        }
        return trString.toLowerCase();
    }

    public static String restoreNonLatin(String trString) {
        if (trString.endsWith("w32")) {
            trString = trString.substring(0, trString.length() - 3).toUpperCase();
            String string = new String(_b.decode(trString), StandardCharsets.UTF_8);
            return string;
        } else {
            return trString;
        }
    }

    public static String transformIndexNameToCategoryName(Driver driver, Store store, Object con, String translated) {

        String ret;

        String trAppCode = getRidOfNonLatin(store.applicationCode()) + "_";
        String indexIndexStorageName = trAppCode + "_indices";

        if (!translated.startsWith(trAppCode)) {
            return null;
        }
        // remove the app prefix
        ret = translated.substring(trAppCode.length());

        // an this point it may not start with "_"
        if (ret.startsWith("_")) {
            return null;
        }

        // first look in the indices register
        byte[] cand = driver.get(ret, indexIndexStorageName, con);
        if (cand != null) {
            ret = new String(cand, StandardCharsets.UTF_8);
            // remove app prefix again
            ret = ret.substring(trAppCode.length());
        }

        // restore if it was base32ed
        ret = restoreNonLatin(ret);
        return ret;
    }

    public static String transformCategoryNameToIndexName(Driver driver, Store store, Object con, String categoryName, String notTranslated) {
        String trAppCode = getRidOfNonLatin(store.applicationCode());
        String indexIndexStorageName = trAppCode + "__indices";

        String trCatName = getRidOfNonLatin(categoryName);

        String indexName = trAppCode + "_" + trCatName;
        if (indexName.length() > driver.getMaximumIndexNameLength(con)) {
            String indexId = null;

            long waitForLock;
            while ((waitForLock = driver.tryLock("100", indexIndexStorageName, con, 10000)) > 0) {
                try {
                    Thread.sleep(waitForLock > 100 ? 100 : waitForLock);
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption", ex);
                }
            }
            Iterable<String> indexIndices = driver.get(indexIndexStorageName, con);
            for (String indexIndexKey : indexIndices) {
                byte[] indexIndex = driver.get(indexIndexKey, indexIndexStorageName, con);
                String indexIndexCandidate = new String(indexIndex, StandardCharsets.UTF_8);
                if (notTranslated.equals(indexIndexCandidate)) {
                    indexId = indexIndexKey;
                    //break; -- don't break to deplete the iterable so it closes
                }
            }

            if (indexId != null) {

                driver.unlock("100", indexIndexStorageName, con);

            } else {
                indexId = UUID.randomUUID().toString().replace("-", "");
                driver.put(indexId, indexIndexStorageName, con, notTranslated.getBytes(StandardCharsets.UTF_8), () -> {
                }, () -> {
                    driver.unlock("100", indexIndexStorageName, con);
                });

            }
            indexName = trAppCode + "_" + indexId;
        }
        return indexName;
    }

}
