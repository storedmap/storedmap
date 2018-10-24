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
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.CollationKey;
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class MapData implements Serializable {

    private static final RuleBasedCollator DEFAULTCOLLATOR;
    private static final BigDecimal BIGGESTNUMBER;

    static {
        String rules = ((RuleBasedCollator) Collator.getInstance(new Locale("ru"))).getRules()
                + ((RuleBasedCollator) Collator.getInstance(Locale.US)).getRules()
                + ((RuleBasedCollator) Collator.getInstance(Locale.PRC)).getRules();
        try {
            DEFAULTCOLLATOR = new RuleBasedCollator(rules);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        char[] gaz = new char[196];
        for (int i = 0; i < gaz.length; i++) {
            gaz[i] = '9';
        }
        String gazStr = new String(gaz);
        BigDecimal gazillion = new BigDecimal(gazStr);
        BIGGESTNUMBER = gazillion.movePointLeft(98);
    }

    private final LinkedHashMap<String, Object> _map = new LinkedHashMap<>();
    private final List<Locale> _locales = new ArrayList(3);
    private final List<Byte> _sorterAsBytes = new ArrayList<>();
    private final Object[] _sorterAsObject = new Object[1];
    private final List<String> _tags = new ArrayList<>(4);

    LinkedHashMap<String, Object> getMap() {
        return _map;
    }

    List<Locale> getLocales() {
        return Collections.unmodifiableList(_locales);
    }

    synchronized void putLocales(List<Locale> locales) {
        _locales.clear();
        _locales.addAll(locales);
        // recompute collation key
        if (_sorterAsObject[0] instanceof String) {
            putSorter((String) _sorterAsObject[0]);
        }
    }

    synchronized void putSorter(String sorter) {
        try {
            Collator common;
            if (_locales.isEmpty()) {
                common = DEFAULTCOLLATOR;
            } else {
                String rules = "";
                for (Locale locale : Collections.unmodifiableList(_locales)) {
                    RuleBasedCollator coll = (RuleBasedCollator) Collator.getInstance(locale);
                    rules = rules + coll.getRules();
                }
                common = new RuleBasedCollator(rules);
            }

            CollationKey ck = common.getCollationKey(sorter);
            byte[] arr = ck.toByteArray();
            Byte[] arrB = new Byte[arr.length];
            for (int i = 0; i < arr.length; i++) {
                arrB[i] = arr[i];
            }
            _sorterAsBytes.clear();
            _sorterAsBytes.addAll(Arrays.asList(arrB));
            _sorterAsObject[0] = sorter;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    synchronized void putSorter(Instant sorter) {
        String timeString = ((Instant) sorter).toString();
        _sorterAsObject[0] = sorter;
        byte[] bytes = timeString.getBytes(StandardCharsets.US_ASCII);
        Byte[] bytesB = new Byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            bytesB[i] = bytes[i];
        }
        _sorterAsBytes.clear();
        _sorterAsBytes.addAll(Arrays.asList(bytesB));
    }

    synchronized void putSorter(Number sorter) {
        Number number = (Number) sorter;
        BigDecimal bd = new BigDecimal(number.toString());  //123.456
        if (bd.signum() > 1 && bd.compareTo(BIGGESTNUMBER) > 1) {
            bd = BIGGESTNUMBER;                             //999.9999999
        } else if (bd.signum() < 1 && bd.abs().compareTo(BIGGESTNUMBER) > 1) {
            bd = BIGGESTNUMBER.negate();                    //-999.9999999
        }

        bd = bd.add(BIGGESTNUMBER);                        // now it's always positive
        bd = bd.movePointRight(98);
        BigInteger bi = bd.toBigInteger();
        byte[] bytes = bi.toByteArray();
        Byte[] bytesB = new Byte[200];
        for (int i = bytes.length - 1, y = bytesB.length - 1; i >= 0; i--, y--) {
            bytesB[y] = bytes[i];
        }
        _sorterAsBytes.clear();
        _sorterAsBytes.addAll(Arrays.asList(bytesB));
        _sorterAsObject[0] = sorter;
    }

    Object getSorter() {
        return _sorterAsObject[0];
    }

    List<Byte> getSorterAsBytes() {
        return _sorterAsBytes;
    }

    List<String> getTags() {
        return _tags;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 19 * hash + Objects.hashCode(this._map);
        hash = 19 * hash + Objects.hashCode(this._locales);
        hash = 19 * hash + Objects.hashCode(this._sorterAsObject[0]);
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
        if (!Objects.equals(this._locales, other._locales)) {
            return false;
        }
        if (!Objects.equals(this._sorterAsObject[0], other._sorterAsObject[0])) {
            return false;
        }
        return Objects.equals(this._tags, other._tags);
    }

}
