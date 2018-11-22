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

import java.util.Iterator;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class StoredMaps implements Iterable<StoredMap> {

    private final Iterable<String> _iterable;
    private final Category _category;

    StoredMaps(Category category, Iterable<String> iterable) {
        _iterable = iterable;
        _category = category;
    }

    @Override
    public Iterator<StoredMap> iterator() {

        Iterator<String> i = _iterable.iterator();

        return new Iterator<StoredMap>() {
            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public StoredMap next() {
                String nextKey = i.next();
                return _category.map(nextKey);
            }
        };

    }

}
