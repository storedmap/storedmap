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
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.WeakHashMap;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.lang3.SerializationUtils;

/**
 * A database table or index representation
 *
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Category {

    private static final RuleBasedCollator DEFAULTCOLLATOR;

    static {
        String rules = ((RuleBasedCollator) Collator.getInstance(new Locale("ru"))).getRules()
                + ((RuleBasedCollator) Collator.getInstance(Locale.US)).getRules()
                + ((RuleBasedCollator) Collator.getInstance(Locale.PRC)).getRules();
        try {
            DEFAULTCOLLATOR = new RuleBasedCollator(rules);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private final Store _store;
    private final Driver _driver;
    private final Object _connection;
    private final String _name;
    private final String _indexName;
    private final List<Locale> _locales = new ArrayList<>();
    private RuleBasedCollator _collator;
    private final WeakHashMap<String, WeakReference<WeakHolder>> _cache = new WeakHashMap<>();
    private final int _hash;

    private Category() {
        throw new UnsupportedOperationException();
    }

    Category(Store store, String name) {

        _name = name;
        _store = store;

        int hash = 5;
        hash = 53 * hash + Objects.hashCode(this._store);
        hash = 53 * hash + Objects.hashCode(this._name);
        _hash = hash;

        _connection = store.getConnection();
        _driver = store.getDriver();

        String indexName = store.getApplicationCode() + "_" + name;
        _indexName = _translateIndexName(indexName);

        // get category locales
        String localesIndexStorageName = _translate(_store.getApplicationCode()) + "__locales";
        byte[] localesB = _driver.get(_indexName, localesIndexStorageName, _connection);
        Locale[] locales;
        if (localesB != null && localesB.length > 0) {
            locales = (Locale[]) SerializationUtils.deserialize(localesB);
            setLocales(locales);
        }
    }

    public final synchronized void setLocales(Locale[] locales) {
        _locales.clear();
        _locales.addAll(Arrays.asList(locales));

        byte[] localesB = SerializationUtils.serialize(locales);
        String localesIndexStorageName = _translate(_store.getApplicationCode()) + "__locales";
        _driver.put(_indexName, localesIndexStorageName, _connection, localesB, () -> {
        }, () -> {
        });

        if (locales.length == 0) {
            _collator = DEFAULTCOLLATOR;
        } else {
            String rules = "";
            for (Locale locale : locales) {
                RuleBasedCollator coll = (RuleBasedCollator) Collator.getInstance(locale);
                rules = rules + coll.getRules();
            }
            try {
                _collator = new RuleBasedCollator(rules);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

        // TODO: recollate all objects in this category =)
    }

    public Locale[] getLocales() {
        return _locales.toArray(new Locale[_locales.size()]);
    }

    public RuleBasedCollator getCollator() {
        return _collator;
    }

    private String _translate(String string) {
        String trString;
        if (!string.matches("^[a-z][a-z0-9_]*$")) {
            Base32 b = new Base32(true, (byte) '*');
            trString = b.encodeAsString(string.getBytes(StandardCharsets.UTF_8));
            // strip the hell the padding
            int starPos = trString.indexOf("*");
            if (starPos > 0) {
                trString = trString.substring(0, starPos);
            }
        } else {
            trString = string;
        }
        return trString.toLowerCase();
    }

    private String _translateIndexName(String notTranslated) {
        String trAppCode = _translate(_store.getApplicationCode());
        String indexIndexStorageName = trAppCode + "__indices";

        String trCatName = _translate(_name);

        String indexName = trAppCode + "_" + trCatName;
        if (indexName.length() > _driver.getMaximumIndexNameLength(_connection)) {
            String indexId = null;

            long waitForLock;
            while ((waitForLock = _driver.tryLock("100", indexIndexStorageName, _connection, 10000)) > 0) {
                try {
                    Thread.sleep(waitForLock > 100 ? 100 : waitForLock);
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption", ex);
                }
            }
            Iterable<String> indexIndices = _driver.get(indexIndexStorageName, _connection);
            for (String indexIndexKey : indexIndices) {
                byte[] indexIndex = _driver.get(indexIndexKey, indexIndexStorageName, _connection);
                String indexIndexCandidate = new String(indexIndex, StandardCharsets.UTF_8);
                if (notTranslated.equals(indexIndexCandidate)) {
                    indexId = indexIndexKey;
                    //break; -- don't break to deplete the iterable so it closes
                }
            }

            if (indexId != null) {

                _driver.unlock("100", indexIndexStorageName, _connection);

            } else {
                indexId = UUID.randomUUID().toString().replace("-", "");
                _driver.put(indexId, indexIndexStorageName, _connection, notTranslated.getBytes(StandardCharsets.UTF_8), () -> {
                }, () -> {
                    _driver.unlock("100", indexIndexStorageName, _connection);
                });

            }
            indexName = trAppCode + "_" + indexId;
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
    public StoredMap getMap(String key) {
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
                _cache.put(key, new WeakReference<>(holder));
            } else {
                ret = new StoredMap(this, cached);
            }
        }
        return ret;
    }

    void removeFromCache(String key) {
        synchronized (_cache) {
            _cache.remove(key);
        }
    }

    public StoredMaps getMaps() {
        return new StoredMaps(this, _driver.get(_indexName, _connection));
    }

    public StoredMaps getMaps(String[] anyOfTags) {
        return new StoredMaps(this, _driver.get(_indexName, _connection, anyOfTags));
    }

    public StoredMaps getMaps(byte[] minSorter, byte[] maxSorter, boolean ascending) {
        return new StoredMaps(this, _driver.get(_indexName, _connection, minSorter, maxSorter, ascending));
    }

    public StoredMaps getMaps(String textQuery) {
        return new StoredMaps(this, _driver.get(_indexName, _connection, textQuery));
    }

    public StoredMaps getMaps(byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        return new StoredMaps(this, _driver.get(_indexName, _connection, minSorter, maxSorter, anyOfTags, ascending));
    }

    public StoredMaps getMaps(String textQuery, String[] anyOfTags) {
        return new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, anyOfTags));
    }

    public StoredMaps getMaps(String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        return new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, minSorter, maxSorter, anyOfTags, ascending));
    }

    public StoredMaps getMaps(String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        return new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, minSorter, maxSorter, ascending));
    }

    public List<StoredMap> getMaps(int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size * .8));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    public List<StoredMap> getMaps(String[] anyOfTags, int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, anyOfTags, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size * .8));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    public List<StoredMap> getMaps(byte[] minSorter, byte[] maxSorter, boolean ascending, int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, minSorter, maxSorter, ascending, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size * .8));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    public List<StoredMap> getMaps(String textQuery, int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size * .8));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    public List<StoredMap> getMaps(byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending, int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, minSorter, maxSorter, anyOfTags, ascending, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size * .8));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    public List<StoredMap> getMaps(String textQuery, String[] anyOfTags, int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, anyOfTags, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size * .8));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    public List<StoredMap> getMaps(String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending, int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, minSorter, maxSorter, anyOfTags, ascending, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size * .8));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    public List<StoredMap> getMaps(String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending, int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, minSorter, maxSorter, ascending, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size * .8));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
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
        final Category other = (Category) obj;
        if (!Objects.equals(this._name, other._name)) {
            return false;
        }
        return Objects.equals(this._store, other._store);
    }

}
