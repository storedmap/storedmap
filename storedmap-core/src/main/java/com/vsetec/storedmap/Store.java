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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A database representation
 *
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Store {

    // stores cache: driverClass - connectionString - properties - appCode --- store
    private final static Map<String, Map<String, Map<Properties, Map<String, Store>>>> STORES = new HashMap<>();

    /**
     * Gets the source object for all categories of stored maps
     *
     *
     * @param driverClassName
     * @param connectionString
     * @param properties
     * @param appCode
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
    private final Object _connection;
    private final String _connectionString;
    private final Properties _properties;

    private Store() {
        throw new UnsupportedOperationException();
    }

    private Store(String driverClassname, String connectionString, Properties properties, String appCode) {
        Driver driver;
        try {
            Class driverClass = Class.forName(driverClassname);
            driver = (Driver) driverClass.newInstance();
        } catch (Exception e) {
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
    protected void finalize() throws Throwable {
        _driver.closeConnection(_connection);
        super.finalize(); //To change body of generated methods, choose Tools | Templates.
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

}
