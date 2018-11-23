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
    private transient boolean _scheduledForDelete = false;

    public MapData() {
    }

    public boolean isScheduledForDelete() {
        return _scheduledForDelete;
    }

    public void setScheduledForDelete(boolean scheduledForDelete) {
        _scheduledForDelete = scheduledForDelete;
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
        return Util.translateSorterIntoBytes(_sorterObject[0], collator, maximumSorterLength);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 19 * hash + Objects.hashCode(this._map);
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
