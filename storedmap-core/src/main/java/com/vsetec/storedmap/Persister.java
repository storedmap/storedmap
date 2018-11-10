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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SerializationUtils;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Persister {

    private final Store _store;
    private final Map<WeakHolder, Thread> _threadsByStoredMap = new HashMap<>();
    private final ThreadLocal<WaitAndPersist> _storerThreadForMainThread = new ThreadLocal<>();
    private final ThreadPoolExecutor _pool = (ThreadPoolExecutor) Executors.newCachedThreadPool(new ThreadFactory() {
        private int _num = 1;

        @Override
        public Thread newThread(Runnable r) {
            _num++;
            return new Thread(r, "MainIndexPersister-" + _num);
        }
    });
    private final ExecutorService _additionalIndexer = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "AdditionalIndexPersister");
        }
    });
    private final ExecutorService _remover = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "IndexRemover");
        }
    });

    Persister(Store store) {
        _store = store;
    }

    void stop() {
        _pool.shutdown();
        try {
            _pool.awaitTermination(3, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected termination", e);
        }
        _additionalIndexer.shutdown();
        try {
            _additionalIndexer.awaitTermination(3, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected termination", e);
        }
        _remover.shutdown();
        try {
            _remover.awaitTermination(3, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected termination", e);
        }
    }

    Store getStore() {
        return _store;
    }

//    void scheduleForRemove(StoredMap storedMap){
//        
//        // TODO: put in the same virtual queue as persisting (see below)
//        
//        WeakHolder holder = storedMap.holder();
//        synchronized(holder){
//            _remover.submit(() -> {
//                synchronized(holder){
//                    _store.getDriver().remove(holder.getKey(), storedMap.category().getIndexName(), _store.getConnection(), () -> {
//                        System.out.println("Callback of removal or " + holder.getKey());
//                    });
//                }
//            });
//        }
//    }
    MapData scheduleForPersist(StoredMap storedMap, boolean actuallyRemove) {
        Thread curThread = Thread.currentThread();
        WeakHolder holder = storedMap.holder();
        final MapData mapData;

        synchronized (holder) { // TODO: review the nested synchronized block
            while (true) {
                // check if already scheduled for persist
                Thread whereScheduled;
                synchronized (_threadsByStoredMap) {
                    whereScheduled = _threadsByStoredMap.get(holder);
                }
                if (whereScheduled == null) { // no thread is working with this sm

                    {
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

                    // mark that this thread is working with this map
                    synchronized (_threadsByStoredMap) {
                        _threadsByStoredMap.put(holder, curThread);
                    }

                    // do we have a storer thread for this main thread?
                    WaitAndPersist storerThreadRunnable = _storerThreadForMainThread.get();
                    if (storerThreadRunnable == null || storerThreadRunnable._finished) { // start a new thread

                        mapData = storedMap.getMapData();
                        mapData.setScheduledForDelete(actuallyRemove);
                        storerThreadRunnable = new WaitAndPersist();
                        storerThreadRunnable._mapDatas.put(holder, mapData);
                        _storerThreadForMainThread.set(storerThreadRunnable);

                        _pool.execute(storerThreadRunnable);

                    } else {

                        if (storerThreadRunnable._tooLateToAddMapData) {
                            continue;
                        }

                        synchronized (storerThreadRunnable) {

                            mapData = storedMap.getMapData();
                            mapData.setScheduledForDelete(actuallyRemove);
                            storerThreadRunnable._mapDatas.put(holder, mapData);
                            // notify the thread that it has to wait another N seconds
                            storerThreadRunnable._continueWaiting = true;
                            storerThreadRunnable.notify();

                        }
                    }

                    break;
                } else if (whereScheduled != curThread) { // if somebody on this machine has taken it
                    try {
                        holder.wait(); // wait for them to notify us, then move on with this loop
                    } catch (InterruptedException ex) {
                        throw new RuntimeException("Unexpected interruption", ex);
                    }
                } else { // it was us! 
                    WaitAndPersist storerThreadRunnable = _storerThreadForMainThread.get();
                    if (storerThreadRunnable._tooLateToAddMapData) {
                        continue;
                    }
                    synchronized (storerThreadRunnable) {
                        mapData = storedMap.getMapData();
                        mapData.setScheduledForDelete(actuallyRemove);
                        storerThreadRunnable._mapDatas.put(holder, mapData);
                        storerThreadRunnable._continueWaiting = true;
                        storerThreadRunnable.notify();
                        break;
                    }
                }
            }
        }

        //System.out.println("loaded for persist: " + holder.getKey() +", there was sorter " + mapData.getSorter() + " and map " + mapData.getMap());
        
        return mapData;

    }

    private class WaitAndPersist implements Runnable {

        private final HashMap<WeakHolder, MapData> _mapDatas = new HashMap<>();
        private boolean _continueWaiting = true;
        private boolean _tooLateToAddMapData = false;
        private boolean _finished = false;

        WaitAndPersist() {
        }

        @Override
        public synchronized void run() {
            while (_continueWaiting) {
                try {
                    _continueWaiting = false;
                    this.wait(3000);
                } catch (InterruptedException e) {
                    break;
                }
            }

            _tooLateToAddMapData = true;

            for (Map.Entry<WeakHolder, MapData> km : _mapDatas.entrySet()) {
                WeakHolder holder = km.getKey();
                MapData mapData = km.getValue();

                synchronized (holder) {
                    String key = holder.getKey();
                    Category category = holder.getCategory();

                    if (mapData.isScheduledForDelete()) {

                        _remover.submit(() -> {
                            synchronized (holder) {
                                _store.getDriver().remove(key, category.getIndexName(), _store.getConnection(), () -> {
                                    //System.out.println("Callback of removal or " + holder.getKey());
                                });
                            }
                        });

                    } else {

                        byte[] mapB = SerializationUtils.serialize(mapData);  // Util.object2bytes(mapData);
                        Driver driver = _store.getDriver();
                        Object connection = _store.getConnection();
                        String indexName = category.getIndexName();

                        // data for additional index
                        Map<String, Object> mapDataMap = mapData.getMap();
                        byte[] sorter = mapData.getSorterAsBytes(category.getCollator(), driver.getMaximumSorterLength());
                        String[] tags = mapData.getTags();

                        driver.put(key, indexName, connection, mapB, () -> {

                            synchronized (holder) {
                                synchronized (_threadsByStoredMap) {
                                    _threadsByStoredMap.remove(holder);
                                }
                                driver.unlock(key, indexName, connection);
                                holder.notify();
                            }

                            _additionalIndexer.submit(() -> {
                                driver.put(
                                        key,
                                        indexName,
                                        connection,
                                        mapDataMap,
                                        category.getLocales(),
                                        sorter,
                                        tags, () -> {
                                            // do nothing for now
                                        });
                            });

                        });
                    }
                }
            }

            _finished = true;

        }

    }

}
