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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.lang3.SerializationUtils;

/**
 * A named group of {@link StoredMap}s with similar structure.
 *
 * <p>
 * This class is similar to a relational database table or a key value store
 * index</p>
 *
 * <p>
 * The Category implements {@link Map} interface.</p>
 *
 * <p>
 * This class provides additional methods for retrieving metadata and StoredMaps
 * creation, retrieval and removal. They intentionally avoid the Java bean
 * getter and setter naming style for conventional use in environments that
 * treat Map values as bean properties</p>
 *
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Category implements Map<String, Map<String, Object>> {

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

        String indexName = store.applicationCode() + "_" + name;
        _indexName = _translateIndexName(indexName);

        // get category locales
        String localesIndexStorageName = _translate(_store.applicationCode()) + "__locales";
        byte[] localesB = _driver.get(_indexName, localesIndexStorageName, _connection);
        Locale[] locales;
        if (localesB != null && localesB.length > 0) {
            locales = (Locale[]) SerializationUtils.deserialize(localesB);
            locales(locales);
        }
    }

    /**
     * Sets the language information that will be used for sorting.
     *
     * <p>
     * This method allows to provide one or more {@link Locale}s which will be
     * used for generating collation codes for string data, or for hinting the
     * storing mechanism about languages used in the {@link StoredMap}. The
     * {@link Driver} may use this information for indexing the stored
     * structure</p>
     *
     * <p>
     * The order of Locale objects provided does matter. The collation rules
     * take it into account and attempt to provide collation code that will
     * allow certain languages go before others, if they use different
     * alphabets</p>
     *
     * @param locales An array of {@link Locale}s to associate with this
     * Category. The order matters
     */
    public final synchronized void locales(Locale[] locales) {
        _locales.clear();
        _locales.addAll(Arrays.asList(locales));

        byte[] localesB = SerializationUtils.serialize(locales);
        String localesIndexStorageName = _translate(_store.applicationCode()) + "__locales";
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

    /**
     * The language information associated with this Category
     *
     *
     * @return Locale objects
     */
    public Locale[] locales() {
        return _locales.toArray(new Locale[_locales.size()]);
    }

    public RuleBasedCollator collator() {
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
        String trAppCode = _translate(_store.applicationCode());
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

    /**
     * Returns the {@link Store} this Category belongs to
     *
     * @return The Store
     */
    public Store store() {
        return _store;
    }

    /**
     * Returns this Category's name
     *
     * @return the name of this Category
     */
    public String name() {
        return _name;
    }

    String getIndexName() {
        return _indexName;
    }

    void removeFromCache(String key) {
        synchronized (_cache) {
            _cache.remove(key);
        }
    }

    Set<String> keyCache() {
        synchronized (_cache) {
            return new HashSet(_cache.keySet());
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
        final Category other = (Category) obj;
        if (!Objects.equals(this._name, other._name)) {
            return false;
        }
        return Objects.equals(this._store, other._store);
    }

    // ***************************************************
    //             basic data manipulations
    // ***************************************************
    /**
     * The main method to get a {@link StoredMap} by it's identifier
     *
     * <p>
     * The StoredMap with the provided identifier may or may not be present in
     * the store, in the latter case it will be created on the fly.</p>
     *
     * <p>
     * The StoredMaps are identified by the key, which can be any string in any
     * language and containing any characters. The only limitation is it's
     * length. This key size is determined by the underlying storage and
     * reported by the driver's method
     * {@link Driver#getMaximumKeyLength(java.lang.Object)}</p>
     *
     * @param key the StoredMap identifier
     * @return the StoredMap, either new or previously persisted
     */
    public StoredMap map(String key) {
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

    /**
     * Gets an iterable of all StoredMaps in this Category.
     *
     * <p>
     * This method will include items that were just inserted (and scheduled for
     * asynchronous persist)</p>
     *
     * @return all StoredMaps of this Category
     */
    public Iterable<StoredMap> maps() {
        return new StoredMaps(this, _driver.get(_indexName, _connection), true);
    }

    /**
     * Gets the persisted Maps that are associated with any of the specified
     * tags set by {@link StoredMap#tags(java.lang.String[])}.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param anyOfTags array of tag Strings
     * @return iterable of Stored Maps
     */
    public Iterable<StoredMap> maps(String[] anyOfTags) {
        return new StoredMaps(this, _driver.get(_indexName, _connection, anyOfTags));
    }

    /**
     * Gets the StoredMaps of this Category that have a
     * {@link StoredMap#sorter(java.lang.Object)} set to a value in the
     * specified range.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @param ascending false if the results should be ordered from maximum to
     * minimum Sorter value, true otherwise
     * @return iterable of Stored Maps
     */
    public Iterable<StoredMap> maps(Object minSorter, Object maxSorter, boolean ascending) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        return new StoredMaps(this, _driver.get(_indexName, _connection, min, max, ascending));
    }

    /**
     * Gets the StoredMaps of this Category that conform with the
     * database-specific query.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param textQuery a database-specific query in textual form
     * @return iterable of Stored Maps
     */
    public Iterable<StoredMap> maps(String textQuery) {
        return new StoredMaps(this, _driver.get(_indexName, _connection, textQuery));
    }

    /**
     * Gets the iterable collection of StoredMaps that have a
     * {@link StoredMap#sorter(java.lang.Object)} set to a value in the
     * specified range and that are associated with any of the specified tags
     * set by {@link StoredMap#tags(java.lang.String[])}.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @param anyOfTags array of tag Strings
     * @param ascending false if the results should be ordered from maximum to
     * @return iterable of Stored Maps
     */
    public Iterable<StoredMap> maps(Object minSorter, Object maxSorter, String[] anyOfTags, boolean ascending) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        return new StoredMaps(this, _driver.get(_indexName, _connection, min, max, anyOfTags, ascending));
    }

    /**
     * Gets the iterable collection of StoredMaps that conform with the
     * database-specific query and that are associated with any of the specified
     * tags set by {@link StoredMap#tags(java.lang.String[])}.
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     *
     * @param textQuery a database-specific query in textual form
     * @param anyOfTags array of tag Strings
     * @return iterable of Stored Maps
     */
    public Iterable<StoredMap> maps(String textQuery, String[] anyOfTags) {
        return new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, anyOfTags));
    }

    /**
     * Gets the iterable collection of StoredMaps that conform with the
     * database-specific query, have a
     * {@link StoredMap#sorter(java.lang.Object)} set to a value in the
     * specified range and that are associated with any of the specified tags
     * set by {@link StoredMap#tags(java.lang.String[])}.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param textQuery a database-specific query in textual form
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @param anyOfTags array of tag Strings
     * @param ascending false if the results should be ordered from maximum to
     * @return iterable of Stored Maps
     */
    public Iterable<StoredMap> maps(String textQuery, Object minSorter, Object maxSorter, String[] anyOfTags, boolean ascending) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        return new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, min, max, anyOfTags, ascending));
    }

    /**
     * Gets the iterable collection of StoredMaps that conform with the
     * database-specific query and have a
     * {@link StoredMap#sorter(java.lang.Object)} set to a value in the
     * specified range.
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     *
     * @param textQuery a database-specific query in textual form
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @param ascending false if the results should be ordered from maximum to
     * @return iterable of Stored Maps
     */
    public Iterable<StoredMap> maps(String textQuery, Object minSorter, Object maxSorter, boolean ascending) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        return new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, min, max, ascending));
    }

    /**
     * Gets the {@link List} of StoredMaps of the specified size, starting from
     * a specified item.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param from the starting item
     * @param size the maximum size of the returned list
     * @return list of Stored Maps
     */
    public List<StoredMap> maps(int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size > 1000 ? 1000 : size * 1.7));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    /**
     * Gets the {@link List} of StoredMaps of the specified size, starting from
     * a specified item, that are associated with any of the specified tags set
     * by {@link StoredMap#tags(java.lang.String[])}.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param anyOfTags array of tag Strings
     * @param from the starting item
     * @param size the maximum size of the returned list
     * @return list of Stored Maps
     */
    public List<StoredMap> maps(String[] anyOfTags, int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, anyOfTags, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size > 1000 ? 1000 : size * 1.7));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    /**
     * Gets the {@link List} of StoredMaps of the specified size, starting from
     * a specified item, that have a {@link StoredMap#sorter(java.lang.Object)}
     * set to a value in the specified range.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @param ascending false if the results should be ordered from maximum to
     * @param from the starting item
     * @param size the maximum size of the returned list
     * @return list of Stored Maps
     */
    public List<StoredMap> maps(Object minSorter, Object maxSorter, boolean ascending, int from, int size) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, min, max, ascending, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size > 1000 ? 1000 : size * 1.7));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    /**
     * Gets the {@link List} of StoredMaps of the specified size, starting from
     * a specified item, that conform to the database-specific query.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param textQuery a database-specific query in textual form
     * @param from the starting item
     * @param size the maximum size of the returned list
     * @return list of Stored Maps
     */
    public List<StoredMap> maps(String textQuery, int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size > 1000 ? 1000 : size * 1.7));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    /**
     * Gets the {@link List} of StoredMaps of the specified size, starting from
     * a specified item, that have a {@link StoredMap#sorter(java.lang.Object)}
     * set to a value in the specified range and that are associated with any of
     * the specified tags set by {@link StoredMap#tags(java.lang.String[])}.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @param anyOfTags array of tag Strings
     * @param ascending false if the results should be ordered from maximum to
     * @param from the starting item
     * @param size the maximum size of the returned list
     * @return list of Stored Maps
     */
    public List<StoredMap> maps(Object minSorter, Object maxSorter, String[] anyOfTags, boolean ascending, int from, int size) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, min, max, anyOfTags, ascending, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size > 1000 ? 1000 : size * 1.7));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    /**
     * Gets the {@link List} of StoredMaps of the specified size, starting from
     * a specified item, that conform to the database-specific query and are
     * associated with any of the specified tags set by
     * {@link StoredMap#tags(java.lang.String[])}.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param textQuery a database-specific query in textual form
     * @param anyOfTags array of tag Strings
     * @param from the starting item
     * @param size the maximum size of the returned list
     * @return list of Stored Maps
     */
    public List<StoredMap> maps(String textQuery, String[] anyOfTags, int from, int size) {
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, anyOfTags, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size > 1000 ? 1000 : size * 1.7));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    /**
     * Gets the {@link List} of StoredMaps of the specified size, starting from
     * a specified item, that conform to the database-specific query, have a
     * {@link StoredMap#sorter(java.lang.Object)} set to a value in the
     * specified range and that are associated with any of the specified tags
     * set by {@link StoredMap#tags(java.lang.String[])}.
     *
     * @param textQuery a database-specific query in textual form
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @param anyOfTags array of tag Strings
     * @param ascending false if the results should be ordered from maximum to
     * @param from the starting item
     * @param size the maximum size of the returned list
     * @return list of Stored Maps
     */
    public List<StoredMap> maps(String textQuery, Object minSorter, Object maxSorter, String[] anyOfTags, boolean ascending, int from, int size) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, min, max, anyOfTags, ascending, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size > 1000 ? 1000 : size * 1.7));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    /**
     * Gets the {@link List} of StoredMaps of the specified size, starting from
     * a specified item, that conform to the database-specific query and have a
     * {@link StoredMap#sorter(java.lang.Object)} set to a value in the
     * specified range.
     *
     * <p>
     * This method may omit the most recently added StoredMaps if they weren't
     * yet persisted asynchronously</p>
     *
     * @param textQuery a database-specific query in textual form
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @param ascending false if the results should be ordered from maximum to
     * @param from the starting item
     * @param size the maximum size of the returned list
     * @return list of Stored Maps
     */
    public List<StoredMap> maps(String textQuery, Object minSorter, Object maxSorter, boolean ascending, int from, int size) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        StoredMaps maps = new StoredMaps(this, _driver.get(_indexName, _connection, textQuery, min, max, ascending, from, size));
        ArrayList<StoredMap> ret = new ArrayList<>((int) (size > 1000 ? 1000 : size * 1.7));
        for (StoredMap map : maps) {
            ret.add(map);
        }
        return Collections.unmodifiableList(ret);
    }

    // **********************************
    //             counts
    // **********************************
    /**
     * Counts all StoredMaps in this Category.
     *
     * <p>
     * The number may miss the StoredMaps added most recently</p>
     *
     * @return number of StoredMaps in the Category
     */
    public int count() {
        return _driver.count(_indexName, _connection);
    }

    /**
     * Counts all StoredMaps in this Category that conform with the
     * database-specific query, have a
     * {@link StoredMap#sorter(java.lang.Object)} set to a value in the
     * specified range and that are associated with any of the specified tags
     * set by {@link StoredMap#tags(java.lang.String[])}.
     *
     * <p>
     * The number may miss the StoredMaps added most recently</p>
     *
     * @param textQuery a database-specific query in textual form
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @param anyOfTags array of tag Strings
     * @return number of StoredMaps in the Category
     */
    public int count(String textQuery, Object minSorter, Object maxSorter, String[] anyOfTags) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        return _driver.count(_indexName, _connection, textQuery, min, max, anyOfTags);
    }

    /**
     * Counts all StoredMaps in this Category that have a
     * {@link StoredMap#sorter(java.lang.Object)} set to a value in the
     * specified range and are associated with any of the specified tags set by
     * {@link StoredMap#tags(java.lang.String[])}.
     *
     * <p>
     * The number may miss the StoredMaps added most recently</p>
     *
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @param anyOfTags array of tag Strings
     * @return number of StoredMaps in the Category
     */
    public int count(Object minSorter, Object maxSorter, String[] anyOfTags) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        return _driver.count(_indexName, _connection, min, max, anyOfTags);
    }

    /**
     * Counts all StoredMaps in this Category that conform with the
     * database-specific query and are associated with any of the specified tags
     * set by {@link StoredMap#tags(java.lang.String[])}.
     *
     * <p>
     * The number may miss the StoredMaps added most recently</p>
     *
     * @param textQuery a database-specific query in textual form
     * @param anyOfTags array of tag Strings
     * @return number of StoredMaps in the Category
     */
    public int count(String textQuery, String[] anyOfTags) {
        return _driver.count(_indexName, _connection, textQuery, anyOfTags);
    }

    /**
     * Counts all StoredMaps in this Category that conform with the
     * database-specific query and have a
     * {@link StoredMap#sorter(java.lang.Object)} set to a value in the
     * specified range.
     *
     * <p>
     * The number may miss the StoredMaps added most recently</p>
     *
     * @param textQuery a database-specific query in textual form
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @return number of StoredMaps in the Category
     */
    public int count(String textQuery, Object minSorter, Object maxSorter) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        return _driver.count(_indexName, _connection, textQuery, min, max);
    }

    /**
     * Counts all StoredMaps in this Category that are associated with any of
     * the specified tags set by {@link StoredMap#tags(java.lang.String[])}.
     *
     * <p>
     * The number may miss the StoredMaps added most recently</p>
     *
     * @param anyOfTags array of tag Strings
     * @return number of StoredMaps in the Category
     */
    public int count(String[] anyOfTags) {
        return _driver.count(_indexName, _connection, anyOfTags);
    }

    /**
     * Counts all StoredMaps in this Category that conform with the
     * database-specific query.
     *
     * <p>
     * The number may miss the StoredMaps added most recently</p>
     *
     * @param textQuery a database-specific query in textual form
     * @return number of StoredMaps in the Category
     */
    public int count(String textQuery) {
        return _driver.count(_indexName, _connection, textQuery);
    }

    /**
     * Counts all StoredMaps in this Category that have a
     * {@link StoredMap#sorter(java.lang.Object)} set to a value in the
     * specified range.
     *
     * <p>
     * The number may miss the StoredMaps added most recently</p>
     *
     * @param minSorter minimum value of Sorter
     * @param maxSorter maximum value of Sorter
     * @return number of StoredMaps in the Category
     */
    public int count(Object minSorter, Object maxSorter) {
        byte[] min = MapData.translateSorterIntoBytes(minSorter, _collator, _driver.getMaximumSorterLength(_connection));
        byte[] max = MapData.translateSorterIntoBytes(maxSorter, _collator, _driver.getMaximumSorterLength(_connection));
        return _driver.count(_indexName, _connection, min, max);
    }

    // **********************************
    //              map api
    // **********************************
    @Override
    public int size() {
        return count();
    }

    @Override
    public boolean isEmpty() {
        return size() <= 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key instanceof String) {
            return _driver.get((String) key, _indexName, _connection) != null;
        } else {
            return false;
        }
    }

    @Override
    public boolean containsValue(Object value) {
        if (value instanceof StoredMap) {
            return ((StoredMap) value).category().equals(Category.this);
        } else {
            return false;
        }
    }

    @Override
    public Map<String, Object> get(Object key) {
        if (key instanceof String) {
            StoredMap sm = map((String) key);
            if (sm.isEmpty()) {
                return null;
            } else {
                return sm;
            }
        } else {
            return null;
        }
    }

    @Override
    public synchronized Map<String, Object> put(String key, Map<String, Object> value) {
        StoredMap sm = map(key);
        Map<String, Object> ret;
        if (!sm.isEmpty()) {
            ret = Collections.unmodifiableMap(new HashMap<>(sm));
            sm.clear();
        } else {
            ret = null;
        }
        sm.putAll(value);
        return ret;
    }

    @Override
    public synchronized Map<String, Object> remove(Object key) {
        if (key instanceof String) {
            StoredMap sm = map((String) key);
            Map<String, Object> ret;
            if (!sm.isEmpty()) {
                ret = Collections.unmodifiableMap(new HashMap<>(sm));
            } else {
                ret = null;
            }
            sm.remove();
            return ret;
        } else {
            return null;
        }

    }

    @Override
    public void putAll(Map<? extends String, ? extends Map<String, Object>> m) {
        for (Entry<? extends String, ? extends Map<String, Object>> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public synchronized void clear() {
        _driver.removeAll(_indexName, _connection);
    }

    @Override
    public Set<String> keySet() {
        return new Set<String>() {
            @Override
            public int size() {
                return Category.this.size();
            }

            @Override
            public boolean isEmpty() {
                return Category.this.isEmpty();
            }

            @Override
            public boolean contains(Object o) {
                return Category.this.containsKey(o);
            }

            @Override
            public Iterator<String> iterator() {

                Iterator<StoredMap> i = Category.this.maps().iterator();

                return new Iterator<String>() {

                    @Override
                    public boolean hasNext() {
                        return i.hasNext();
                    }

                    @Override
                    public String next() {
                        return i.next().key();
                    }
                };
            }

            @Override
            public Object[] toArray() {
                ArrayList ret = new ArrayList();
                Iterator<String> i = iterator();
                while (i.hasNext()) {
                    ret.add(i.next());
                }
                return ret.toArray();
            }

            @Override
            public <T> T[] toArray(T[] arg0) {
                ArrayList<T> ret = new ArrayList<>();
                Iterator<String> i = iterator();
                while (i.hasNext()) {
                    ret.add((T) i.next());
                }
                return ret.toArray(arg0);
            }

            @Override
            public boolean add(String e) {
                return true;
            }

            @Override
            public boolean remove(Object o) {
                return Category.this.remove(o) != null;
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                for (Object o : c) {
                    if (!Category.this.containsKey(o)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean addAll(Collection<? extends String> c) {
                return true;
            }

            @Override
            public boolean retainAll(Collection<?> c) {

                String[] currentKeys = toArray(new String[0]);
                boolean ret = false;
                for (String key : currentKeys) {
                    if (!c.contains(key)) {
                        Category.this.remove(key);
                        ret = true;
                    }
                }
                return ret;
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                boolean ret = false;
                for (Object o : c) {
                    if (Category.this.remove(o) != null) {
                        ret = true;
                    }
                }
                return ret;
            }

            @Override
            public void clear() {
                Category.this.clear();
            }
        };
    }

    @Override
    public Collection<Map<String, Object>> values() {
        return new Collection<Map<String, Object>>() {
            @Override
            public int size() {
                return Category.this.size();
            }

            @Override
            public boolean isEmpty() {
                return Category.this.isEmpty();
            }

            @Override
            public boolean contains(Object o) {
                return Category.this.containsValue(o);
            }

            @Override
            public Iterator<Map<String, Object>> iterator() {
                Iterator<StoredMap> i = Category.this.maps().iterator();

                return new Iterator<Map<String, Object>>() {

                    @Override
                    public boolean hasNext() {
                        return i.hasNext();
                    }

                    @Override
                    public Map<String, Object> next() {
                        return i.next();
                    }
                };
            }

            @Override
            public Object[] toArray() {
                ArrayList ret = new ArrayList();
                Iterator<Map<String, Object>> i = iterator();
                while (i.hasNext()) {
                    ret.add(i.next());
                }
                return ret.toArray();
            }

            @Override
            public <T> T[] toArray(T[] arg0) {
                ArrayList<T> ret = new ArrayList<>();
                Iterator<Map<String, Object>> i = iterator();
                while (i.hasNext()) {
                    ret.add((T) i.next());
                }
                return ret.toArray(arg0);
            }

            @Override
            public boolean add(Map<String, Object> e) {
                throw new UnsupportedOperationException("Adding a map to category without any key is not supported.");
            }

            @Override
            public boolean remove(Object o) {
                boolean ret = false;
                if (o instanceof StoredMap) {
                    StoredMap sm = (StoredMap) o;
                    if (sm.category().equals(Category.this)) {
                        sm.remove();
                        ret = true;
                    }
                }
                return ret;
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                for (Object o : c) {
                    if (!Category.this.containsValue(o)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean addAll(Collection<? extends Map<String, Object>> c) {
                throw new UnsupportedOperationException("Adding maps to category without keys is not supported.");
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                boolean ret = false;
                for (Object o : c) {
                    if (o instanceof StoredMap) {
                        StoredMap sm = (StoredMap) o;
                        if (sm.category().equals(Category.this)) {
                            sm.remove();
                            ret = true;
                        }
                    }
                }
                return ret;
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                HashSet<String> keysToRetain = new HashSet<>();
                for (Object o : c) {
                    if (!(o instanceof StoredMap)) {
                        return false;
                    } else {
                        keysToRetain.add(((StoredMap) o).key());
                    }
                }
                StoredMap[] currentValues = toArray(new StoredMap[0]);
                boolean ret = false;
                for (StoredMap map : currentValues) {
                    if (!keysToRetain.contains(map.key())) {
                        map.remove();
                        ret = true;
                    }
                }
                return ret;
            }

            @Override
            public void clear() {
                Category.this.clear();
            }
        };
    }

    @Override
    public Set<Entry<String, Map<String, Object>>> entrySet() {
        return new Set<Entry<String, Map<String, Object>>>() {
            @Override
            public int size() {
                return Category.this.size();
            }

            @Override
            public boolean isEmpty() {
                return Category.this.isEmpty();
            }

            @Override
            public boolean contains(Object o) {
                if (o instanceof Entry) {
                    Entry e = (Entry) o;
                    Object v = e.getValue();
                    if (v instanceof StoredMap) {
                        StoredMap sm = (StoredMap) v;
                        if (sm.category().equals(Category.this)) {
                            return sm.key().equals(e.getKey());
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            @Override
            public Iterator<Entry<String, Map<String, Object>>> iterator() {

                Iterator<Map<String, Object>> i = Category.this.values().iterator();

                return new Iterator<Entry<String, Map<String, Object>>>() {
                    @Override
                    public boolean hasNext() {
                        return i.hasNext();
                    }

                    @Override
                    public Entry<String, Map<String, Object>> next() {

                        StoredMap sm = (StoredMap) i.next();

                        return new Entry<String, Map<String, Object>>() {
                            @Override
                            public String getKey() {
                                return sm.key();
                            }

                            @Override
                            public Map<String, Object> getValue() {
                                return sm;
                            }

                            @Override
                            public Map<String, Object> setValue(Map<String, Object> value) {
                                return Category.this.put(sm.key(), value);
                            }
                        };
                    }
                };
            }

            @Override
            public Object[] toArray() {
                ArrayList ret = new ArrayList();
                for (Entry<String, Map<String, Object>> entry : entrySet()) {
                    ret.add(entry);
                }
                return ret.toArray();
            }

            @Override
            public <T> T[] toArray(T[] arg0) {
                ArrayList<T> ret = new ArrayList<>();
                for (Entry<String, Map<String, Object>> entry : entrySet()) {
                    ret.add((T) entry);
                }
                return ret.toArray(arg0);
            }

            @Override
            public boolean add(Entry<String, Map<String, Object>> e) {
                Category.this.put(e.getKey(), e.getValue());
                return true;
            }

            @Override
            public boolean remove(Object o) {
                if (o instanceof Entry) {
                    Category.this.remove(((Entry) o).getKey());
                    return true;
                }
                return false;
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                for (Object o : c) {
                    if (!Category.this.values().contains(o)) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean addAll(Collection<? extends Entry<String, Map<String, Object>>> c) {
                boolean ret = false;
                for (Entry e : c) {
                    if (add(e)) {
                        ret = true;
                    }
                }
                return ret;
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                boolean ret = false;
                for (Object o : c) {
                    if (o instanceof Entry) {
                        Entry e = (Entry) o;
                        if (Category.this.remove(e.getKey()) != null) {
                            ret = true;
                        }
                    }
                }
                return ret;
            }

            @Override
            public void clear() {
                Category.this.clear();
            }
        };
    }

}
