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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;
import org.apache.commons.codec.binary.Base32;

/**
 * A database table or index representation
 *
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Category implements Serializable {

    private final Store _store;
    private final Driver _driver;
    private final Object _connection;
    private final String _name;
    private final String _indexName;
    private final String _lockIndexName;
    private boolean _lockIndexIsCreated = false;
    private final WeakHashMap<String, Holder>_cache = new WeakHashMap<>();
    private final HashMap<String, Object> _lockedInStore = new HashMap<>();    

    private Category() {
        throw new UnsupportedOperationException();
    }
    
    Category(Store store, String name) {

        _name = name;
        _store = store;

        _connection = store.getConnection();
        _driver = store.getDriver();

        String indexName = store.getApplicationCode() + "_" + name + "_main";
        _indexName = _translateIndexName(indexName);
        _driver.createIndexIfDoesNotExist(_connection, _indexName);

        String lockIndexName = store.getApplicationCode() + "_" + name + "_lock";
        _lockIndexName = _translateIndexName(lockIndexName);
    }

    
    private String _translateIndexName(String notTranslated){
        String appCode = _store.getApplicationCode();
        String trAppCode;
        if (!appCode.matches("^[a-z][a-z0-9]*$")) {
            Base32 b = new Base32(true);
            trAppCode = b.encodeAsString(appCode.getBytes(StandardCharsets.UTF_8));
        } else {
            trAppCode = appCode;
        }
        
        String indexIndexStorageName = trAppCode + "__indices";
        _driver.createIndexIfDoesNotExist(_connection, indexIndexStorageName);

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
            for (String indexIndexKey : indexIndices) {
                byte[]indexIndex = _driver.get(indexIndexKey, indexName, _connection);
                Map<String,Object>indexIndexMap = (Map<String,Object>) Util.bytes2object(indexIndex);
                if (notTranslated.equals(indexIndexMap.get("name"))) {
                    indexId = (String) indexIndexMap.get("id");
                    break;
                }
            }
            if (indexId == null) {
                indexId = UUID.randomUUID().toString();
                Map<String, Object> indexIndex = new HashMap();
                indexIndex.put("name", notTranslated);
                indexIndex.put("id", indexId);
                _driver.put(indexId, indexIndexStorageName, _connection, Util.object2bytes(indexIndex), true);
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
    
    void lockInStore(String key, long maxLock){
        if(!_lockIndexIsCreated){
            _driver.createIndexIfDoesNotExist(_connection, _lockIndexName);
            _lockIndexIsCreated = true;
        }
        
        
        while (true) {

            long now = java.lang.System.currentTimeMillis();
            final Long lock;

            synchronized (_lockedInStore) {
                
                byte[]lockMillisB = _driver.get(key, _lockIndexName, _connection);
                if(lockMillisB!=null){
                    lock = (Long) Util.bytes2object(lockMillisB);
                }else{
                    lock = null;
                }

                if (lock == null || lock < now) { // no lock or the lock is in the past

                    Long newLock = now + maxLock;
                    lockMillisB = Util.object2bytes(newLock);
                    _driver.put(key, _lockIndexName, _connection, lockMillisB, true);
                    _lockedInStore.put(key, newLock);

                    break;
                }
            }

            synchronized (lock) { // it is still locked there in the storage
                long whatsleft = lock - now;
                if (whatsleft <= 0) { // though it was checked above
                    whatsleft = 5;
                } else if (whatsleft > 1000) { // for not to wait for too long, because it may have been locked on another machine, and it may have finished
                    whatsleft = 1000;
                }
                try {
                    lock.wait(whatsleft);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Unexpected interruption", e);
                }
            }
        }
    }

    public void unlockInStore(String key) {
        Object lock;
        synchronized (_lockedInStore) {
            _driver.removeTagsSorterAndText(key, _lockIndexName, _connection);
            lock = _lockedInStore.remove(key);
            if (lock != null) {
                synchronized (lock) {
                    lock.notify(); // TODO: notifyAll???  // if somebody is waiting on this machine
                }
            }
        }
    }

    
    
    // simple
    
    public StoredMap get(String key){
        Holder cached;
        synchronized(_cache){
            cached = _cache.get(key);
            if(cached==null){
                cached = new Holder(key);
                _cache.put(key, cached);
            }
        }
        StoredMap ret = new StoredMap(this, cached);
        return ret;
    }
    
    public Maps get(){
        return new Maps(this, _driver.get(_indexName, _connection));
    }
    
    public void remove(String key){
        
    }
    
    Map<String,Object> getOrLoad(Holder cached){
        synchronized(cached){
            Map<String,Object>map = cached.get();
            if(map==null){
                String key = cached.getKey();
                byte[] mapB = _driver.get(key, _indexName, _connection);
                if(mapB!=null){
                    map = (Map<String, Object>) Util.bytes2object(mapB);
                }else{
                    map = new LinkedHashMap<>(3);
                }
                cached.put(map);
            }
            return map;
        }
    }
    
    void persist(Holder holder){
        synchronized(holder){
            String key = holder.getKey();
            Map<String, Object>map = holder.get();
            assert map!=null;
            byte[]mapB = Util.object2bytes(map);
            _driver.put(key, _indexName, _connection, mapB, false);
        }
    }



    
    
}
