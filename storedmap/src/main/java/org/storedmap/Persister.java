/*
 * Copyright 2018 Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}.
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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}
 */
public class Persister {

    private static final Logger LOG = LoggerFactory.getLogger(StoredMap.class);

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

    void cancelSave(WeakHolder holder) {
        synchronized (holder) {
            SaveOrReschedule sor = _inLongWork.get(holder);
            if (sor != null) {
                sor._cancelSave = true;
//                Driver driver = _store.getDriver();
//                Object connection = _store.getConnection();
//                String indexName = sor._sm.category().internalIndexName();
//                _inWork.remove(holder);
//                driver.unlock(holder.getKey(), indexName, connection);
//                _inLongWork.remove(holder);
//                holder.notify();
                LOG.debug("Cancelling save of {}-{}", holder.getCategory().name(), holder.getKey());
            }
        }
    }

    MapData scheduleForPersist(StoredMap storedMap) {
        WeakHolder holder = storedMap.holder();
        synchronized (holder) {
            LOG.debug("Planning to save {}-{}", holder.getCategory().name(), holder.getKey());
            SaveOrReschedule command = _inWork.get(holder);
            if (command != null) {
                command._reschedule = true;
                LOG.debug("Skipping saving {}-{} as rescheduled", holder.getCategory().name(), holder.getKey());
                return command._mapData;
            } else {
                long waitForLock;
                // wait for releasing on other machines then lock for ourselves
                while ((waitForLock = _store.getDriver().tryLock(holder.getKey(), storedMap.category().internalIndexName(), _store.getConnection(), 100000)) > 0) {
                    try {
                        LOG.warn("Waiting " + storedMap.category().internalIndexName() + " for " + waitForLock);
                        holder.wait(waitForLock > 5000 ? 2000 : waitForLock); // check every 2 seconds
                    } catch (InterruptedException ex) {
                        throw new RuntimeException("Unexpected interruption", ex);
                    }
                }

                MapData mapData = storedMap.getMapData();
                if (storedMap.isRemoved) {
                    LOG.warn("Map {}-{} turned out to be removed, exiting", holder.getCategory().name(), holder.getKey());
                    return mapData;
                }
                command = new SaveOrReschedule(storedMap, mapData);
                _inWork.put(holder, command);
                _inLongWork.put(holder, command);
                _mainIndexer.schedule(command, 3, TimeUnit.SECONDS);
                //_mainIndexer.
                LOG.debug("Planned to save {}-{}; queue size={}", holder.getCategory().name(), holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());
                return mapData;
            }
        }
    }

    private class SaveOrReschedule implements Runnable {

        private boolean _reschedule = false;
        private final StoredMap _sm;
        private final WeakHolder _holder;
        private final MapData _mapData;
        private boolean _cancelSave = false;

        public SaveOrReschedule(StoredMap sm, MapData md) {
            _sm = sm;
            _mapData = md;
            _holder = sm.holder();
        }

        @Override
        public void run() {

            LOG.debug("Ready to save map data {}-{}; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());

            try {
                synchronized (_holder) {

                    if (_reschedule) {
                        _reschedule = false;
                        //SaveOrReschedule.this
                        SaveOrReschedule sor = new SaveOrReschedule(_sm, _mapData);
                        sor._cancelSave = _cancelSave;
                        _mainIndexer.schedule(sor, 2, TimeUnit.SECONDS);
                        LOG.debug("Rescheduling saving {}-{} as new info came. Queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());
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
                    String secondaryKey = _mapData.getSecondarKey();

                    if (!_cancelSave) {
                        LOG.debug("Sending to save map data {}-{}; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());
                        driver.put(_holder.getKey(), indexName, connection, mapB, () -> {
                            LOG.debug("Sent to saved map data for {}-{}, proceed for index; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());
                            _inWork.remove(_holder);
                        }, () -> {
                            if (!_cancelSave) {
                                LOG.debug("Sending to save index for {}-{}; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());
                                driver.put(
                                        _holder.getKey(),
                                        indexName,
                                        connection,
                                        mapDataMap,
                                        category.locales(),
                                        secondaryKey,
                                        sorter,
                                        tags, () -> {
                                            LOG.debug("Fully finished saving {}-{}; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());
                                            synchronized (_holder) {
                                                driver.unlock(_holder.getKey(), indexName, connection);
                                                _inLongWork.remove(_holder);
                                                _holder.notify();
                                                LOG.debug("Unlocked after full save of {}-{}", _holder.getCategory().name(), _holder.getKey());
                                            }
                                        });
                            } else {
                                LOG.debug("Cancelled saving after main data and before indexing {}-{}, unlocking; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());
                                driver.unlock(_holder.getKey(), indexName, connection);
                                _inLongWork.remove(_holder);
                                _holder.notify();
                            }
                        });
                    } else {
                        LOG.debug("Cancelled saving before main data is sent to save {}-{}, unlocking; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());
                        _inWork.remove(_holder);
                        driver.unlock(_holder.getKey(), indexName, connection);
                        _inLongWork.remove(_holder);
                        _holder.notify();
                    }

                }
            } catch (Exception e) {
                LOG.error("Couldn't persist {" + _holder.getCategory().name() + "-" + _holder.getKey() + "}", e);
                synchronized (_holder) {
                    Category category = _sm.category();
                    Driver driver = _store.getDriver();
                    Object connection = _store.getConnection();
                    String indexName = category.internalIndexName();

                    driver.unlock(_holder.getKey(), indexName, connection);
                    _inLongWork.remove(_holder);
                    _holder.notify();
                    LOG.debug("Unlocking after failing to save of {}-{}", _holder.getCategory().name(), _holder.getKey());
                }
                throw e;
            }

        }

    }

}
