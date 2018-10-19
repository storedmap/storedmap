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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Persister {

    private final Store _store;
    private final Map<Thread, Set<MapData>> _toPersistByThread = new HashMap<>();
    private final Map<StoredMap, Thread> _threadsByHolder = new HashMap<>();

    public Persister(Store store) {
        _store = store;
    }

    public Store getStore() {
        return _store;
    }

    MapData scheduleForPersist(StoredMap sm) {
        Thread curThread = Thread.currentThread();
        WeakHolder holder = sm.holder();
        final MapData md;

        synchronized (holder) {
            while (true) {
                // check if already scheduled for persist
                Thread whereScheduled = _threadsByHolder.get(sm);
                if (whereScheduled == null) { // no thread works with this sm

                    long waitForLock;
                    // wait for releasing on other machines then lock for ourselves
                    while ((waitForLock = _store.getDriver().tryLock(holder.getKey(), sm.category().getIndexName(), _store.getConnection(), 100000)) > 0) {
                        try {
                            holder.wait(waitForLock > 1000 ? 1000 : waitForLock);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException("Unexpected interruption", ex);
                        }
                    }

                    _threadsByHolder.put(sm, curThread);
                    Set<MapData> toPersist = _toPersistByThread.get(curThread);
                    if (toPersist == null) {
                        toPersist = new HashSet<>();
                        _toPersistByThread.put(curThread, toPersist);
                    }
                    md = sm.getMapData();
                    break;
                } else if (whereScheduled != curThread) { // if somebody on this machine has taken it
                    try {
                        holder.wait(); // wait for them to notify us, then move on with this loop
                    } catch (InterruptedException ex) {
                        throw new RuntimeException("Unexpected interruption", ex);
                    }
                } else { // it was us! 
                    // just nothing to do as it's already marked for us
                    md = sm.getMapData();
                    break;
                }
            }
        }

        return md;

    }

    private void _persist(MapData mal) {
        WeakHolder holder = mal.getTmpHolder();
        synchronized (holder) {
            String key = holder.getKey();
            Category category = holder.getCategory();
            byte[] mapB = Util.object2bytes(mal);
            Store store = category.getStore();
            Driver driver = store.getDriver();
            Object connection = store.getConnection();
            String indexName = category.getIndexName();

            driver.put(key, indexName, connection, mapB, () -> {

                driver.unlock(key, indexName, connection);
                holder.unlockForPersist();

            }, mal.getMap(), mal.getLocales(), mal.getSorter(), mal.getTags(), () -> {
                // do nothing for now
            });
        }
    }

}
