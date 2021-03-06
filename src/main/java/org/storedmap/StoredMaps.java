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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 *
 * @author Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}
 */
public class StoredMaps implements Iterable<StoredMap> {

    private final Iterable<String> _iterable;
    private final Category _category;
    private final boolean _includeKeyOrSecondaryKayCached; // true - include normal key cached, false - secondary kay cached
    private final String _secondaryKey;

    StoredMaps(Category category, Iterable<String> iterable, String secondaryKey) {
        _iterable = iterable;
        _category = category;
        _includeKeyOrSecondaryKayCached = false;
        _secondaryKey = secondaryKey;
    }

    StoredMaps(Category category, Iterable<String> iterable) {
        _iterable = iterable;
        _category = category;
        _includeKeyOrSecondaryKayCached = true;
        _secondaryKey = null;
    }

    @Override
    public Iterator<StoredMap> iterator() {

        Iterator<String> keyIter = keyIterator();

        return new Iterator<StoredMap>() {
            @Override
            public boolean hasNext() {
                return keyIter.hasNext();
            }

            @Override
            public StoredMap next() {
                return _category.map(keyIter.next());
            }

        };

    }

    Iterator<String> keyIterator() {

        return new Iterator<String>() {

            private Iterator<String> _i = _iterable.iterator();
            private final Set<String> _keyCache;
            boolean _switched = false;

            {
                if (_includeKeyOrSecondaryKayCached) {
                    _keyCache = _category._keyCache();
                } else {
                    _keyCache = _category._secondaryKeyCache(_secondaryKey);
                }
            }

            @Override
            public boolean hasNext() {
                boolean ret = _i.hasNext();
                if (!ret && !_switched) {
                    _switched = true;
                    _i = _keyCache.iterator();
                    ret = _i.hasNext();
                }
                return ret;
            }

            @Override
            public String next() {
                String nextKey;
                try {

                    nextKey = _i.next();
                    if (!_switched && !_keyCache.isEmpty()) { // skipping all cached
                        _keyCache.remove(nextKey);
                    }

                } catch (NoSuchElementException e) {
                    if (!_switched) {
                        _switched = true;
                        _i = _keyCache.iterator();
                        nextKey = _i.next();
                    } else {
                        throw e;
                    }
                }
                return nextKey;
            }
        };

    }
}
