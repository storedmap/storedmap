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

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * A database representation. The class holds static references to multiple
 * instances of itself, unique for the database type, database connections
 * string that defines a database of the type, properties that affect the
 * database connection, and the application code which is used to distinguish
 * indexes or tables created in the database that belong to different
 * applications
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Store implements Closeable {

    // stores cache: driverClass - connectionString - properties - appCode --- store
    private final static Map<String, Map<String, Map<Properties, Map<String, Store>>>> STORES = new HashMap<>();

    /**
     * Gets the source object for all categories of stored maps
     *
     *
     * @param driverClassName implementation of
     * {@link com.vsetec.storedmap.Driver}
     * @param connectionString is used by Driver to connect to the underlying
     * database
     * @param properties more connection details
     * @param appCode an application short name which is a short string to be
     * used as prefix for all index name of this connection,
     * @return
     */
    public static Store get(String driverClassName, String connectionString, Properties properties, String appCode) {
        Map<String, Map<Properties, Map<String, Store>>> ofDriver;
        synchronized (STORES) {
            ofDriver = STORES.get(driverClassName);
            if (ofDriver == null) {
                ofDriver = new HashMap<>();
                STORES.put(driverClassName, ofDriver);
            }
        }

        Map<Properties, Map<String, Store>> ofConnection;
        synchronized (ofDriver) {
            ofConnection = ofDriver.get(connectionString);
            if (ofConnection == null) {
                ofConnection = new HashMap<>(3);
                ofDriver.put(connectionString, ofConnection);
            }
        }

        Map<String, Store> ofProperties;
        synchronized (ofConnection) {
            ofProperties = ofConnection.get(properties);
            if (ofProperties == null) {
                ofProperties = new HashMap<>(3);
                ofConnection.put(properties, ofProperties);
            }
        }

        Store ret;
        synchronized (ofProperties) {
            ret = ofProperties.get(appCode);
            if (ret == null) {
                ret = new Store(driverClassName, connectionString, properties, appCode);
                ofProperties.put(appCode, ret);
            }
        }
        return ret;
    }

    private final Map<String, Category> _categories = new HashMap<>();
    
    private final String _appCode;
    private final Driver _driver;
    private final String _driverClassName;
    private final Object _connection;
    private final String _connectionString;
    private final Properties _properties;
    private final Persister _persister = new Persister(this);

    private Store() {
        throw new UnsupportedOperationException();
    }

    private Store(String driverClassname, String connectionString, Properties properties, String appCode) {
        _driverClassName = driverClassname;
        Driver driver;
        try {
            Class driverClass = Class.forName(driverClassname);
            driver = (Driver) driverClass.newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new StoredMapException("Couldn't load the driver " + driverClassname, e);
        }

        _appCode = appCode;
        _connectionString = connectionString;
        _driver = driver;
        _properties = properties;

        try {
            _connection = driver.openConnection(connectionString, properties);
        } catch (Exception e) {
            throw new StoredMapException("Couldn't connect to the underlying storage with connection " + connectionString, e);
        }
    }

    @Override
    public void close() {
        _persister.stop();
        _driver.closeConnection(_connection);
    }

    public Persister getPersister() {
        return _persister;
    }

    public Driver getDriver() {
        return _driver;
    }

    public Object getConnection() {
        return _connection;
    }

    public String getConnectionString() {
        return _connectionString;
    }

    public String getApplicationCode() {
        return _appCode;
    }

    public Properties getProperties() {
        return _properties;
    }

    public synchronized Category getCategory(String categoryName) {
        Category ret = _categories.get(categoryName);
        if (ret == null) {
            ret = new Category(this, categoryName);
            _categories.put(categoryName, ret);
        }
        return ret;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 19 * hash + Objects.hashCode(this._appCode);
        hash = 19 * hash + Objects.hashCode(this._driverClassName);
        hash = 19 * hash + Objects.hashCode(this._connectionString);
        hash = 19 * hash + Objects.hashCode(this._properties);
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
        final Store other = (Store) obj;
        if (!Objects.equals(this._appCode, other._appCode)) {
            return false;
        }
        if (!Objects.equals(this._driverClassName, other._driverClassName)) {
            return false;
        }
        if (!Objects.equals(this._connectionString, other._connectionString)) {
            return false;
        }
        return Objects.equals(this._properties, other._properties);
    }

}
