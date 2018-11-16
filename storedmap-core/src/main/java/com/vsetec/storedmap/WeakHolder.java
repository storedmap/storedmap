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

import java.lang.ref.WeakReference;
import java.util.Objects;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class WeakHolder {

    private final String _key;
    private final Category _category;
    private WeakReference<MapData> _wr = new WeakReference<>(null);
    private final int _hash;

    WeakHolder(String key, Category category) {
        _key = key;
        _category = category;
        int hash = 3;
        hash = 89 * hash + Objects.hashCode(this._key);
        hash = 89 * hash + Objects.hashCode(this._category);
        _hash = hash;
    }

    String getKey() {
        return _key;
    }

    public Category getCategory() {
        return _category;
    }

    synchronized void put(MapData map) {
        if (map == null) {
            _wr = null;
        } else {
            MapData curMap;
            if (_wr != null) {
                curMap = _wr.get();
            } else {
                curMap = null;
            }
            if (curMap != map) { // don't replace the object if we're reputting it
                _wr = new WeakReference<>(map);
            }
        }
    }

    synchronized MapData get() {
        if (_wr == null) {
            return null;
        } else {
            return _wr.get();
        }
    }

    @Override
    public int hashCode() {
        return _hash;
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
        final WeakHolder other = (WeakHolder) obj;
        if (!Objects.equals(this._key, other._key)) {
            return false;
        }
        return Objects.equals(this._category, other._category);
    }

}
