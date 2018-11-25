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
package org.storedmap;

import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Interface that a connector to a StoredMaps underlying storage should
 * implement.
 *
 * <p>
 * First the StoredMap library invokes the
 * {@link #openConnection(java.util.Properties)} method and obtains an object
 * that will be provided for every subsequent calls to retrieval and store
 * methods. At the end when the {@link Store} is about to be closed, the library
 * calls {@link #closeConnection(java.lang.Object)} with the same connection
 * object.</p>
 *
 * <p>
 * The {@code indexName} parameter that is expected at the most of data
 * manipulation methods is guaranteed to be a {@link String} containing only the
 * basic Latin characters, numbers and, possibly, underscore signs ('_'), and to
 * be no longer then the return value of
 * {@link #getMaximumIndexNameLength(java.lang.Object)}. It is shortened and
 * transformed to a Base32 string by the Library if needed.</p>
 *
 * <p>
 * The Driver is responsible for automatic creation of the actual tables or
 * indices in the underlying data storage at the first attempt of accessing
 * them. The list of index names already created for the connection is retrieved
 * with the {@link #getIndices(java.lang.Object)} method.</p>
 *
 * <p>
 * The library doesn't do anything with the connection object except providing
 * it intact to the get, put and remove methods of the Driver, so it can be an
 * object of any type that will let the Driver implementation perform the needed
 * command.</p>
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 *
 * @param <T> Type of the connection object
 */
public interface Driver<T> {

    /**
     * Creates a connection to the underlying database
     *
     * @param properties
     * @return object representing a connection
     */
    T openConnection(Properties properties);

    /**
     * Closes the connection
     *
     * @param connection the connection object
     */
    void closeConnection(T connection);

    /**
     * Returns the maximum number of {@link Character}s (Unicode code units)
     * that can be in an underlying database index name.
     *
     * <p>
     * The index may be a relational database table or a key value storage
     * index.</p>
     *
     * <p>
     * While the {@link Category} name roughly corresponds to the underlying
     * data store index name, the library still allows the String of any length
     * to be a Category name. Long index names get shortened inside the library,
     * and the short forms are used when accessing the data store</p>
     *
     * @param connection connection object
     * @return the maximum number of characters allowed in an index name
     */
    int getMaximumIndexNameLength(T connection);

    /**
     * Returns the maximum number of {@link Character}s allowed in the StoredMap
     * key string.
     *
     * <p>
     * This is a maximum length of a string that can be a primary key in the
     * index (or table) of the underlying data store</p>
     *
     *
     * @param connection connection object
     * @return the maximum number of characters in key
     */
    int getMaximumKeyLength(T connection);

    /**
     * Gets the maximum number of {@link Character}s in the StoredMaps tag.
     *
     * <p>
     * This is the longest string that the underlying data store can index</p>
     *
     * @param connection
     * @return the maximum number of characters in a tag
     */
    int getMaximumTagLength(T connection);

    /**
     * Gets the maximum number of bytes in the StoredMap's sorter value.
     *
     * <p>
     * This is the maximum length of the byte array the underlying data store
     * can effectively index</p>
     *
     * @param connection connection object
     * @return the number of bytes the sorting value
     */
    int getMaximumSorterLength(T connection);

    /**
     * Retrieves the {@link StoredMap} data in a binary form by its key.
     *
     * <p>
     * The library serializes the contents of a StoredMap into the binary
     * format, so that it is unchanged when retrieved</p>
     *
     * @param key the StoredMap's key
     * @param indexName index name, shortened if needed
     * @param connection connection object
     * @return the StoredMap binary representation
     */
    byte[] get(String key, String indexName, T connection);

    long count(String indexName, T connection);

    long count(String indexName, T connection, String[] anyOfTags);

    long count(String indexName, T connection, byte[] minSorter, byte[] maxSorter);

    long count(String indexName, T connection, String textQuery);

    long count(String indexName, T connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags);

    long count(String indexName, T connection, String textQuery, String[] anyOfTags);

    long count(String indexName, T connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags);

    long count(String indexName, T connection, String textQuery, byte[] minSorter, byte[] maxSorter);

    /**
     * Gets keys of all {@link StoredMap}s in the index.
     *
     * <p>
     * This method is invoked to get all StoredMaps in the {@link Category}
     *
     * @param indexName index name, shortened if needed
     * @param connection connection object
     * @return an object that can be iterated to get all relevant keys
     */
    Iterable<String> get(String indexName, T connection);

    /**
     * Gets keys of all {@link StoredMap}s that are associated with any or all
     * of the specified tags.
     *
     * <p>
     * Tags is a number of Strings attached to a StoredMap</p>
     *
     * @param indexName index name, shortened if needed
     * @param connection connection object
     * @param anyOfTags an array of the tag Strings
     * @return an object that can be iterated to get all relevant keys
     */
    Iterable<String> get(String indexName, T connection, String[] anyOfTags);

    Iterable<String> get(String indexName, T connection, byte[] minSorter, byte[] maxSorter, boolean ascending);

    Iterable<String> get(String indexName, T connection, String textQuery);

    Iterable<String> get(String indexName, T connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending);

    Iterable<String> get(String indexName, T connection, String textQuery, String[] anyOfTags);

    Iterable<String> get(String indexName, T connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending);

    Iterable<String> get(String indexName, T connection, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending);

    Iterable<String> get(String indexName, T connection, int from, int size);

    Iterable<String> get(String indexName, T connection, String[] anyOfTags, int from, int size);

    Iterable<String> get(String indexName, T connection, byte[] minSorter, byte[] maxSorter, boolean ascending, int from, int size);

    Iterable<String> get(String indexName, T connection, String textQuery, int from, int size);

    Iterable<String> get(String indexName, T connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending, int from, int size);

    Iterable<String> get(String indexName, T connection, String textQuery, String[] anyOfTags, int from, int size);

    Iterable<String> get(String indexName, T connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending, int from, int size);

    Iterable<String> get(String indexName, T connection, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending, int from, int size);

    /**
     * Tries to lock the key in a specified index or returns the value
     * indicating that the key is already locked.
     *
     * <p>
     * The driver should first check the existing lock in the underlying
     * database, and if found, return the time remaining for this lock to be
     * automatically released.</p>
     *
     * <p>
     * If the remaining time is less or equals to zero, the lock should be
     * considered expired and replaced with the new one for the specified amount
     * of time. The method should still return zero or negative value to
     * indicate that the key was locked as result of this operation.</p>
     *
     * <p>
     * The lock can be removed before the specified amount of time passes using
     * the method {@link #unlock(String, String, Object)}</p>
     *
     *
     * @param key identifier of a record to lock
     * @param indexName name of the underlying database index or table
     * @param connection object that represents the connection
     * @param milliseconds the maximum amount of time the new lock is active
     * before it will be automatically considered to be released
     * @return number of milliseconds left to wait or a zero or negative value
     * if lock was performed
     */
    int tryLock(String key, String indexName, T connection, int milliseconds);

    /**
     * Unlock the key in the specified index.
     *
     * <p>
     * Commands the driver to remove the lock regardless of the maximum active
     * time set in
     * {@link #tryLock(java.lang.String, java.lang.String, java.lang.Object, int) }</p>
     *
     * @param key
     * @param indexName
     * @param connection
     */
    void unlock(String key, String indexName, T connection);

    void put(
            String key,
            String indexName,
            T connection,
            byte[] value,
            Runnable callbackBeforeIndex,
            Runnable callbackAfterIndex);

    void put(
            String key,
            String indexName,
            T connection,
            Map<String, Object> map,
            Locale[] locales,
            byte[] sorter,
            String[] tags,
            Runnable callbackAfterAdditionalIndex);

    void remove(String key, String indexName, T connection, Runnable callback);

    void removeAll(String indexName, T connection);

    /**
     * Return the index names that can be provided as a parameter of data
     * manipulation methods.
     *
     * <p>
     * This will be an internal index names that conform with the general rule:
     * only basic Latin charachters are used and the number of characters is no
     * longer then the number returned by
     * {@link #getMaximumIndexNameLength(java.lang.Object)} method.</p>
     *
     * @param connection object that represents the connection
     * @return iterable of index names
     */
    Iterable<String> getIndices(T connection);

}
