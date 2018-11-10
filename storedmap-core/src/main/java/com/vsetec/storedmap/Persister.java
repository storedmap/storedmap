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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SerializationUtils;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Persister {

    private final Store _store;
    private final ConcurrentHashMap<WeakHolder, ScheduledFuture> _inWork = new ConcurrentHashMap<>();
    //private final ThreadLocal<Boolean>_lockedInThisThread = new ThreadLocal(){
    //   @Override
    //    protected Object initialValue() {
    //        return false;
    //    }
    //    
    //};
    private final ScheduledExecutorService _mainIndexer = Executors.newScheduledThreadPool(5, new ThreadFactory() {
        private int _num = 0;

        @Override
        public Thread newThread(Runnable r) {
            _num++;
            return new Thread(r, "Indexer-" + _num);
        }
    });
    private final ExecutorService _additionalIndexer = Executors.newSingleThreadExecutor((Runnable r) -> new Thread(r, "AdditionalIndexPersister"));

    Persister(Store store) {
        _store = store;
    }

    void stop() {
        _mainIndexer.shutdown();
        try {
            _mainIndexer.awaitTermination(3, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected termination", e);
        }
        _additionalIndexer.shutdown();
        try {
            _additionalIndexer.awaitTermination(3, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected termination", e);
        }
    }

    MapData scheduleForPersist(StoredMap storedMap) {
        WeakHolder holder = storedMap.holder();
        synchronized (holder) {
            ScheduledFuture oldCommand = _inWork.remove(holder);
            if (oldCommand != null) {
                oldCommand.cancel(false);
            } else {
                long waitForLock;
                // wait for releasing on other machines then lock for ourselves
                while ((waitForLock = _store.getDriver().tryLock(holder.getKey(), storedMap.category().getIndexName(), _store.getConnection(), 100000)) > 0) {
                    try {
                        holder.wait(waitForLock > 5000 ? 2000 : waitForLock); // check every 2 seconds
                    } catch (InterruptedException ex) {
                        throw new RuntimeException("Unexpected interruption", ex);
                    }
                }
            }

            MapData mapData = storedMap.getMapData();
            if (storedMap.isRemoved) {
                return mapData;
            }
            String key = holder.getKey();
            Category category = holder.getCategory();

            ScheduledFuture newCommand = _mainIndexer.schedule(() -> {

                synchronized (holder) {
                    byte[] mapB = SerializationUtils.serialize(mapData);
                    Driver driver = _store.getDriver();
                    Object connection = _store.getConnection();
                    String indexName = category.getIndexName();

                    // data for additional index
                    Map<String, Object> mapDataMap = mapData.getMap();
                    byte[] sorter = mapData.getSorterAsBytes(category.getCollator(), driver.getMaximumSorterLength());
                    String[] tags = mapData.getTags();

                    driver.put(key, indexName, connection, mapB, () -> {

                        _additionalIndexer.submit(() -> {
                            driver.put(
                                    key,
                                    indexName,
                                    connection,
                                    mapDataMap,
                                    category.getLocales(),
                                    sorter,
                                    tags, () -> {
                                        synchronized (holder) {
                                            driver.unlock(key, indexName, connection);
                                            holder.notify();
                                        }
                                    });

                        });

                    });

                }

                _inWork.remove(holder);
            }, 3, TimeUnit.SECONDS);
            _inWork.put(holder, newCommand);
            return mapData;
        }
    }

}
