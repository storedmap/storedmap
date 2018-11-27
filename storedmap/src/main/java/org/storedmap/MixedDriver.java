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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class MixedDriver implements Driver<MixedDriver.MixedConnection> {

    private final Map<String, Driver> _drivers = new HashMap<>();

    @Override
    public synchronized MixedConnection openConnection(Properties properties) {
        String driverClassName = properties.getProperty("driver.main");
        Driver mainDriver = _drivers.get(driverClassName);
        if (mainDriver == null) {
            try {
                Class<Driver> mainDriverClass = (Class<Driver>) Class.forName(driverClassName);
                mainDriver = mainDriverClass.newInstance();
                _drivers.put(driverClassName, mainDriver);
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }

        driverClassName = properties.getProperty("driver.additional");
        Driver ftDriver = _drivers.get(driverClassName);
        if (ftDriver == null) {
            try {
                Class<Driver> driverClass = (Class<Driver>) Class.forName(driverClassName);
                ftDriver = driverClass.newInstance();
                _drivers.put(driverClassName, ftDriver);
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }

        boolean lockWithMain = "true".equals(properties.getProperty("driver.lock.with.main", "true"));

        Object mainConnection = mainDriver.openConnection(properties);
        Object addConnection = ftDriver.openConnection(properties);
        Object lockConn = lockWithMain ? mainConnection : addConnection;

        return new MixedConnection(
                mainDriver,
                mainConnection,
                ftDriver,
                addConnection,
                lockWithMain ? mainDriver : ftDriver,
                lockConn
        );
    }

    @Override
    public void closeConnection(MixedConnection connection) {
        connection._mainDriver.closeConnection(connection._mainConnection);
        connection._fulltextDriver.closeConnection(connection._fulltextConnection);
    }

    @Override
    public int getMaximumIndexNameLength(MixedConnection connection) {
        return Math.min(connection._fulltextDriver.getMaximumIndexNameLength(connection._fulltextConnection), connection._mainDriver.getMaximumIndexNameLength(connection._mainConnection));
    }

    @Override
    public int getMaximumKeyLength(MixedConnection connection) {
        return Math.min(connection._fulltextDriver.getMaximumKeyLength(connection._fulltextConnection), connection._mainDriver.getMaximumKeyLength(connection._mainConnection));
    }

    @Override
    public int getMaximumTagLength(MixedConnection connection) {
        return connection._fulltextDriver.getMaximumTagLength(connection._fulltextConnection);
    }

    @Override
    public int getMaximumSorterLength(MixedConnection connection) {
        return connection._fulltextDriver.getMaximumSorterLength(connection._fulltextConnection);
    }

    @Override
    public byte[] get(String key, String indexName, MixedConnection connection) {
        return connection._mainDriver.get(key, indexName, connection._mainConnection);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection) {
        return connection._mainDriver.get(indexName, connection._mainConnection);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, String[] anyOfTags) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, anyOfTags);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, minSorter, maxSorter, ascending);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, String textQuery) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, textQuery);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, minSorter, maxSorter, anyOfTags, ascending);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, String textQuery, String[] anyOfTags) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, textQuery, anyOfTags);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, textQuery, minSorter, maxSorter, anyOfTags, ascending);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, textQuery, minSorter, maxSorter, ascending);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, int from, int size) {
        return connection._mainDriver.get(indexName, connection._mainConnection, from, size);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, String[] anyOfTags, int from, int size) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, anyOfTags, from, size);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, byte[] minSorter, byte[] maxSorter, boolean ascending, int from, int size) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, minSorter, maxSorter, ascending, from, size);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, String textQuery, int from, int size) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, textQuery, from, size);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending, int from, int size) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, minSorter, maxSorter, anyOfTags, ascending, from, size);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, String textQuery, String[] anyOfTags, int from, int size) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, textQuery, anyOfTags, from, size);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags, boolean ascending, int from, int size) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, textQuery, minSorter, maxSorter, anyOfTags, ascending, from, size);
    }

    @Override
    public Iterable<String> get(String indexName, MixedConnection connection, String textQuery, byte[] minSorter, byte[] maxSorter, boolean ascending, int from, int size) {
        return connection._fulltextDriver.get(indexName, connection._fulltextConnection, textQuery, minSorter, maxSorter, ascending, from, size);
    }

    @Override
    public int tryLock(String key, String indexName, MixedConnection connection, int milliseconds) {
        return connection._lockDriver.tryLock(key, indexName, connection._lockConnection, milliseconds);
    }

    @Override
    public void unlock(String key, String indexName, MixedConnection connection) {
        connection._lockDriver.unlock(key, indexName, connection._lockConnection);
    }

    @Override
    public void put(String key, String indexName, MixedConnection connection, byte[] value, Runnable callbackBeforeIndex, Runnable callbackAfterIndex) {
        connection._mainDriver.put(key, indexName, connection._mainConnection, value, callbackBeforeIndex, callbackAfterIndex);
    }

    @Override
    public void put(String key, String indexName, MixedConnection connection, Map<String, Object> map, Locale[] locales, byte[] sorter, String[] tags, Runnable callbackAfterAdditionalIndex) {
        connection._fulltextDriver.put(key, indexName, connection._fulltextConnection, map, locales, sorter, tags, callbackAfterAdditionalIndex);
    }

    @Override
    public void remove(String key, String indexName, MixedConnection connection, Runnable callback) {
        connection._mainDriver.remove(key, indexName, connection._mainConnection, () -> {
            connection._fulltextDriver.remove(key, indexName, connection._fulltextConnection, callback);
        });
    }

    @Override
    public Iterable<String> getIndices(MixedConnection connection) {
        return connection._mainDriver.getIndices(connection);
    }

    @Override
    public long count(String indexName, MixedConnection connection) {
        return connection._fulltextDriver.count(indexName, connection._mainConnection);
    }

    @Override
    public long count(String indexName, MixedConnection connection, String[] anyOfTags) {
        return connection._fulltextDriver.count(indexName, connection._mainConnection, anyOfTags);
    }

    @Override
    public long count(String indexName, MixedConnection connection, byte[] minSorter, byte[] maxSorter) {
        return connection._fulltextDriver.count(indexName, connection._mainConnection, minSorter, maxSorter);
    }

    @Override
    public long count(String indexName, MixedConnection connection, String textQuery) {
        return connection._fulltextDriver.count(indexName, connection._mainConnection, textQuery);

    }

    @Override
    public long count(String indexName, MixedConnection connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags) {
        return connection._fulltextDriver.count(indexName, connection._mainConnection, minSorter, maxSorter, anyOfTags);
    }

    @Override
    public long count(String indexName, MixedConnection connection, String textQuery, String[] anyOfTags) {
        return connection._fulltextDriver.count(indexName, connection._mainConnection, textQuery, anyOfTags);

    }

    @Override
    public long count(String indexName, MixedConnection connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags) {
        return connection._fulltextDriver.count(indexName, connection._mainConnection, textQuery, minSorter, maxSorter, anyOfTags);
    }

    @Override
    public long count(String indexName, MixedConnection connection, String textQuery, byte[] minSorter, byte[] maxSorter) {
        return connection._fulltextDriver.count(indexName, connection._mainConnection, textQuery, minSorter, maxSorter);

    }

    @Override
    public synchronized void removeAll(String indexName, MixedConnection connection) {
        connection._fulltextDriver.removeAll(indexName, connection);
        connection._mainDriver.removeAll(indexName, connection);

    }

    public static class MixedConnection<I, F> {

        private final Driver<I> _mainDriver;
        private final I _mainConnection;
        private final Driver<F> _fulltextDriver;
        private final F _fulltextConnection;
        private final Driver _lockDriver;
        private final Object _lockConnection;

        public MixedConnection(
                Driver<I> mainDriver,
                I mainConnection,
                Driver<F> fulltextDriver,
                F fulltextConnection,
                Driver lockDriver,
                Object lockConnection) {
            _mainDriver = mainDriver;
            _mainConnection = mainConnection;
            _fulltextDriver = fulltextDriver;
            _fulltextConnection = fulltextConnection;
            _lockDriver = lockDriver;
            _lockConnection = lockConnection;
        }

    }

}
