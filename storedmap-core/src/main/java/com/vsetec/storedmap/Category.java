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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.WeakHashMap;
import org.apache.commons.codec.binary.Base32;

/**
 * A database table or index representation
 *
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Category {

    private final Store _store;
    private final Driver _driver;
    private final Object _connection;
    private final String _name;
    private final String _indexName;
    private final WeakHashMap<String, WeakReference<WeakHolder>> _cache = new WeakHashMap<>();

    private Category() {
        throw new UnsupportedOperationException();
    }

    Category(Store store, String name) {

        _name = name;
        _store = store;

        _connection = store.getConnection();
        _driver = store.getDriver();

        String indexName = store.getApplicationCode() + "_" + name;
        _indexName = _translateIndexName(indexName);

    }

    private String _translateIndexName(String notTranslated) {
        String appCode = _store.getApplicationCode();
        String trAppCode;
        if (!appCode.matches("^[a-z][a-z0-9]*$")) {
            Base32 b = new Base32(true);
            trAppCode = b.encodeAsString(appCode.getBytes(StandardCharsets.UTF_8));
        } else {
            trAppCode = appCode;
        }

        String indexIndexStorageName = trAppCode + "__indices";

        String trCatName;
        if (!_name.matches("^[a-z][a-z0-9]*$")) {
            Base32 b = new Base32(true);
            trCatName = b.encodeAsString(_name.getBytes(StandardCharsets.UTF_8));
        } else {
            trCatName = _name;
        }

        String indexName = trAppCode + "_" + trCatName;
        if (indexName.length() > _driver.getMaximumIndexNameLength()) {
            String indexId = null;
            Iterable<String> indexIndices = _driver.get(indexIndexStorageName, _connection);

            long waitForLock;
            while ((waitForLock = _driver.tryLock(indexId, indexIndexStorageName, _connection, 10000)) > 0) {
                try {
                    Thread.sleep(waitForLock > 100 ? 100 : waitForLock);
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption", ex);
                }
            }

            for (String indexIndexKey : indexIndices) {
                byte[] indexIndex = _driver.get(indexIndexKey, indexName, _connection);
                Map<String, Object> indexIndexMap = (Map<String, Object>) Util.bytes2object(indexIndex);
                if (notTranslated.equals(indexIndexMap.get("name"))) {
                    indexId = (String) indexIndexMap.get("id");
                    break;
                }
            }

            if (indexId != null) {

                _driver.unlock(indexId, indexIndexStorageName, _connection);

            } else {
                final String indexIdFinal = UUID.randomUUID().toString();
                indexId = indexIdFinal;
                Map<String, Object> indexIndex = new HashMap();
                indexIndex.put("name", notTranslated);
                indexIndex.put("id", indexId);
                _driver.put(indexId, indexIndexStorageName, _connection, Util.object2bytes(indexIndex), new Callback() {
                    @Override
                    public void call() {
                        _driver.unlock(indexIdFinal, indexIndexStorageName, _connection);
                    }
                }, null, null, null, null, null);

            }
            indexName = indexId;
        }
        return indexName;
    }

    public Store getStore() {
        return _store;
    }

    public String getName() {
        return _name;
    }

    String getIndexName() {
        return _indexName;
    }

    // simple
    public StoredMap get(String key) {
        StoredMap ret;
        synchronized (_cache) {
            WeakHolder cached;
            WeakReference<WeakHolder> wr = _cache.get(key);
            if (wr != null) {
                cached = wr.get();
            } else {
                cached = null;
            }
            if (cached == null) {
                WeakHolder holder = new WeakHolder(key, this);
                ret = new StoredMap(this, holder);
                _cache.put(key, new WeakReference<>(cached));
            } else {
                ret = new StoredMap(this, cached);
            }
        }
        return ret;
    }

    public StoredMaps get() {
        return new StoredMaps(this, _driver.get(_indexName, _connection));
    }

    public void remove(String key) {

    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + Objects.hashCode(this._store);
        hash = 53 * hash + Objects.hashCode(this._name);
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
        final Category other = (Category) obj;
        if (!Objects.equals(this._name, other._name)) {
            return false;
        }
        if (!Objects.equals(this._store, other._store)) {
            return false;
        }
        return true;
    }

}
