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

import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public interface Driver {

    Object openConnection(String connectionString, Properties properties);

    void closeConnection(Object connection);

    int getMaximumIndexNameLength();

    int getMaximumKeyLength();

    int getMaximumTagLength();

    int getMaximumSorterLength();

    byte[] get(String key, String indexName, Object connection);

    Iterable<String> get(String indexName, Object connection);

    Iterable<String> get(String indexName, Object connection, String[] anyOfTags);

    Iterable<String> get(String indexName, Object connection, byte[] minSorter, byte[] maxSorter, boolean ascending);

    Iterable<String> get(String indexName, Object connection, String textQuery);

    Iterable<String> get(String indexName, Object connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending);

    Iterable<String> get(String indexName, Object connection, String textQuery, String[] anyOfTags);

    Iterable<String> get(String indexName, Object connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending);

    Iterable<String> get(String indexName, Object connection, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending);

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
     * indicate that the key was locked.</p>
     *
     * <p>
     * The lock can be removed before the specified amount of time passes using
     * the method {@link unlock(String, String, Object)}</p>
     *
     *
     * @param key identifier of a record to lock
     * @param indexName name of the underlying database index or table
     * @param connection object that represents the connection
     * @param milliseconds the maximum amount of time the new lock is active
     * before it will be automatically considered to be released
     * @return
     */
    int tryLock(String key, String indexName, Object connection, int milliseconds);

    /**
     * Unlock the key in the specified index.
     *
     * <p>
     * Commands the driver to remove the lock regardless of the maximum active
     * time set in
     * {@link com.vsetec.storedmap.Driver#tryLock(String, String, Object, long)}</p>
     *
     * @param key
     * @param indexName
     * @param connection
     */
    void unlock(String key, String indexName, Object connection);

    void put(
            String key,
            String indexName,
            Object connection,
            byte[] value,
            Runnable callbackOnIndex,
            Map<String, Object> map,
            Locale[] locales,
            byte[] sorter,
            String[] tags,
            Runnable callbackOnAdditionalIndex);

    void remove(String key, String indexName, Object connection, Runnable callback);

}
