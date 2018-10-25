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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Persister {

    private final ThreadPoolExecutor _pool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    private final Store _store;
    //private final Map<Thread, Set<MapData>> _toPersistByThread = new HashMap<>();
    //private final Map<Thread, Set<MapData>> _storerThreads = new HashMap<>();
    private final Map<StoredMap, Thread> _threadsByStoredMap = new HashMap<>();
    private final Map<Thread, WaitAndPersist> _storerThreadForMainThread = new HashMap<>();

    public Persister(Store store) {
        _store = store;
    }

    public Store getStore() {
        return _store;
    }

    MapData scheduleForPersist(StoredMap storedMap) {
        Thread curThread = Thread.currentThread();
        WeakHolder holder = storedMap.holder();
        final MapData mapData;

        synchronized (holder) { // TODO: review the nested synchronized block
            while (true) {
                // check if already scheduled for persist
                Thread whereScheduled = _threadsByStoredMap.get(storedMap);
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
                    _threadsByStoredMap.put(storedMap, curThread);

                    // do we have a storer thread for this main thread?
                    WaitAndPersist storerThreadRunnable = _storerThreadForMainThread.get(curThread);
                    if (storerThreadRunnable == null) { // start a new thread

                        mapData = storedMap.getMapData();
                        storerThreadRunnable = new WaitAndPersist(curThread);
                        storerThreadRunnable._mapDatas.put(holder, mapData);
                        _storerThreadForMainThread.put(curThread, storerThreadRunnable);

                        _pool.execute(storerThreadRunnable);

                    } else {
                        synchronized (storerThreadRunnable) {

                            if (storerThreadRunnable._tooLateToAddMapData) {
                                continue;
                            }

                            mapData = storedMap.getMapData();
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
                    WaitAndPersist storerThreadRunnable = _storerThreadForMainThread.get(curThread);
                    synchronized (storerThreadRunnable) {
                        if (storerThreadRunnable._tooLateToAddMapData) {
                            continue;
                        }
                        mapData = storedMap.getMapData();
                        storerThreadRunnable._mapDatas.put(holder, mapData);
                        storerThreadRunnable._continueWaiting = true;
                        storerThreadRunnable.notify();
                    }
                    break;
                }
            }
        }

        return mapData;

    }

    private class WaitAndPersist implements Runnable {

        private final Thread _mainThread;
        private final HashMap<WeakHolder, MapData> _mapDatas = new HashMap<>();
        private boolean _continueWaiting = true;
        private boolean _tooLateToAddMapData = false;

        WaitAndPersist(Thread mainThread) {
            _mainThread = mainThread;
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
            _storerThreadForMainThread.remove(_mainThread);

            for (Map.Entry<WeakHolder, MapData> km : _mapDatas.entrySet()) {
                WeakHolder holder = km.getKey();
                MapData mapData = km.getValue();

                synchronized (holder) {
                    String key = holder.getKey();
                    Category category = holder.getCategory();
                    byte[] mapB = Util.object2bytes(mapData);
                    Driver driver = _store.getDriver();
                    Object connection = _store.getConnection();
                    String indexName = category.getIndexName();

                    driver.put(key, indexName, connection, mapB, () -> {

                        synchronized(holder){
                            driver.unlock(key, indexName, connection);
                            holder.notify();
                        }

                    }, mapData.getMap(), mapData.getLocales(), mapData.getSorterAsBytes(), mapData.getTags(), () -> {
                        // do nothing for now
                    });
                }
            }

        }

    }

}
