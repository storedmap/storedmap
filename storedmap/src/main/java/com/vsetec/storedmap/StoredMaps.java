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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class StoredMaps implements Iterable<StoredMap> {

    private final Iterable<String> _iterable;
    private final Category _category;
    private final boolean _includeCached;

    StoredMaps(Category category, Iterable<String> iterable) {
        _iterable = iterable;
        _category = category;
        _includeCached = false;
    }

    StoredMaps(Category category, Iterable<String> iterable, boolean includeCachedInCategory) {
        _iterable = iterable;
        _category = category;
        _includeCached = includeCachedInCategory;
    }

    @Override
    public Iterator<StoredMap> iterator() {

        return new Iterator<StoredMap>() {

            private Iterator<String> _i = _iterable.iterator();
            private final Set<String> _keyCache;
            boolean _switched = false;

            {
                if (_includeCached) {
                    _keyCache = _category.keyCache();
                } else {
                    _keyCache = Collections.EMPTY_SET;
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
            public StoredMap next() {
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
                return _category.map(nextKey);
            }
        };

    }

}
