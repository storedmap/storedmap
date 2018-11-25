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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SerializationUtils;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Persister {

    private final Store _store;
    private final ConcurrentHashMap<WeakHolder, SaveOrReschedule> _inWork = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<WeakHolder, SaveOrReschedule> _inLongWork = new ConcurrentHashMap<>();
    private final ScheduledExecutorService _mainIndexer = Executors.newScheduledThreadPool(5, new ThreadFactory() {
        private int _num = 0;

        @Override
        public Thread newThread(Runnable r) {
            _num++;
            return new Thread(r, "StoredMapIndexer-" + _num);
        }
    });

    Persister(Store store) {
        _store = store;
    }

    void stop() {

        while (!_inLongWork.isEmpty()) {
            try {
                Thread.currentThread().sleep(100);
            } catch (InterruptedException e) {

            }
        }

        _mainIndexer.shutdown();
        try {
            _mainIndexer.awaitTermination(3, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected termination", e);
        }

    }

    boolean isInWork(WeakHolder holder) {
        return _inWork.contains(holder);
    }

    MapData scheduleForPersist(StoredMap storedMap) {
        WeakHolder holder = storedMap.holder();
        synchronized (holder) {
            SaveOrReschedule command = _inWork.get(holder);
            if (command != null) {
                command._reschedule = true;
                return command._mapData;
            } else {
                long waitForLock;
                // wait for releasing on other machines then lock for ourselves
                while ((waitForLock = _store.getDriver().tryLock(holder.getKey(), storedMap.category().internalIndexName(), _store.getConnection(), 100000)) > 0) {
                    try {
                        System.out.println("Waiting " + storedMap.category().internalIndexName() + " for " + waitForLock);
                        holder.wait(waitForLock > 5000 ? 2000 : waitForLock); // check every 2 seconds
                    } catch (InterruptedException ex) {
                        throw new RuntimeException("Unexpected interruption", ex);
                    }
                }

                MapData mapData = storedMap.getMapData();
                if (storedMap.isRemoved) {
                    return mapData;
                }
                command = new SaveOrReschedule(storedMap, mapData);
                _inWork.put(holder, command);
                _inLongWork.put(holder, command);
                _mainIndexer.schedule(command, 3, TimeUnit.SECONDS);
                return mapData;
            }
        }
    }

    private class SaveOrReschedule implements Runnable {

        private boolean _reschedule = false;
        private final StoredMap _sm;
        private final WeakHolder _holder;
        private final MapData _mapData;

        public SaveOrReschedule(StoredMap sm, MapData md) {
            _sm = sm;
            _mapData = md;
            _holder = sm.holder();
        }

        @Override
        public void run() {

            synchronized (_holder) {

                if (_reschedule) {
                    _reschedule = false;
                    _mainIndexer.schedule(this, 2, TimeUnit.SECONDS);
                    //System.out.println("*** rescheduling as added new info ***");
                    return;
                }

                Category category = _sm.category();

                byte[] mapB = SerializationUtils.serialize(_mapData);
                Driver driver = _store.getDriver();
                Object connection = _store.getConnection();
                String indexName = category.internalIndexName();

                // data for additional index
                Map<String, Object> mapDataMap = _mapData.getMap();
                byte[] sorter = _mapData.getSorterAsBytes(category.collator(), driver.getMaximumSorterLength(connection));
                String[] tags = _mapData.getTags();

                driver.put(_holder.getKey(), indexName, connection, mapB, () -> {
                    _inWork.remove(_holder);
                }, () -> {
                    driver.put(
                            _holder.getKey(),
                            indexName,
                            connection,
                            mapDataMap,
                            category.locales(),
                            sorter,
                            tags, () -> {
                                synchronized (_holder) {
                                    driver.unlock(_holder.getKey(), indexName, connection);
                                    _inLongWork.remove(_holder);
                                    _holder.notify();
                                }
                            });

                });

            }

        }

    }

}
