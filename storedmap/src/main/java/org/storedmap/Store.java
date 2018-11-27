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

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * A database representation.
 *
 * <p>
 * The class holds static references to multiple instances of itself, unique for
 * the database type, database connections string that defines a database of the
 * type, properties that affect the database connection, and the application
 * code which is used to distinguish indexes or tables created in the database
 * that belong to different applications.</p>
 *
 * The specific Store is retrieved or created by the static
 * {@link #getStore(Properties)} method.
 *
 * <p>
 * The two main properties are:</p>
 *
 * <ul><li><b>driver</b>: the name of the class that implements
 * {@link org.storedmap.Driver}</li>
 *
 * <li><b>applicationCode</b>: the substring that prefixes all underlying data
 * store artifacts (tables, indices etc)</li></ul>
 *
 * <p>
 * The same properties list may contain the configuration items that the driver
 * will use. It is advised that these additional properties have some driver
 * specific prefix</p>
 *
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Store implements Closeable {

    private final static Map<Properties, Store> STORES = new HashMap<>();

    /**
     * Gets the source object for all categories of stored maps, aka Store
     *
     *
     * @param properties Database connection details including application
     * specific prefix for all indices, the StoredMap database driver class name
     * and additional database connection characteristics
     * @return the Store either previously created or new
     */
    public static Store getStore(Properties properties) {
        Store ret;
        synchronized (STORES) {
            ret = STORES.get(properties);
            if (ret == null) {
                ret = new Store(properties);
                STORES.put(properties, ret);
            }
        }
        return ret;
    }

    private final Map<String, Category> _categories = new HashMap<>();

    private final String _appCode;
    private final Driver _driver;
    private final Object _connection;
    private final Properties _properties;
    private final Persister _persister = new Persister(this);
    private final int _hash;

    private Store() {
        throw new UnsupportedOperationException();
    }

    private Store(Properties properties) {
        Driver driver;
        String driverClassname = properties.getProperty("driver", "com.vsetec.storedmap.jdbc.GenericJdbcDriver");
        try {

            Class driverClass = Class.forName(driverClassname);
            driver = (Driver) driverClass.newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new StoredMapException("Couldn't load the driver " + driverClassname, e);
        }

        _appCode = properties.getProperty("applicationCode", "storedmap");
        _driver = driver;
        _properties = properties;

        try {
            _connection = driver.openConnection(properties);
        } catch (Exception e) {
            throw new StoredMapException("Couldn't connect to the underlying storage with properties " + properties.toString(), e);
        }

        int hash = 7;
        hash = 19 * hash + Objects.hashCode(this._properties);
        _hash = hash;
    }

    /**
     * Closes the resources associated with this Store
     */
    @Override
    public void close() {
        synchronized (STORES) {
            _persister.stop();
            _driver.closeConnection(_connection);
            STORES.remove(_properties);
        }
    }

    Persister getPersister() {
        return _persister;
    }

    Driver getDriver() {
        return _driver;
    }

    Object getConnection() {
        return _connection;
    }

    /**
     * Gets the application code - the string prefix used to distinguish the
     * underlying store artifacts that belong to StoredMap
     *
     * @return the application specific prefix
     */
    public String applicationCode() {
        return _appCode;
    }

    /**
     * Returns the configuration parameters that were used to create this Store
     *
     * @return the configuration parameters
     */
    public Properties properties() {
        return _properties;
    }

    /**
     * Gets the named {@link Category} - a group of {@link StoredMap}s with
     * mostly similar structure
     *
     * @param categoryName name of a {@link Category}
     * @return a {@link Category} by that name
     */
    public synchronized Category get(String categoryName) {
        Category ret = _categories.get(categoryName);
        if (ret == null) {
            ret = new Category(this, categoryName);
            _categories.put(categoryName, ret);
        }
        return ret;
    }

    public Set<Category> categories() {
        Iterable<String> cats = _driver.getIndices(_connection);
        Set<Category> ret = new HashSet<>();
        for (String cat : cats) {
            cat = Util.transformIndexNameToCategoryName(_driver, this, _connection, cat);
            if (cat != null) {
                ret.add(get(cat));
            }
        }
        return ret;
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
        final Store other = (Store) obj;
        return Objects.equals(this._properties, other._properties);
    }

}
