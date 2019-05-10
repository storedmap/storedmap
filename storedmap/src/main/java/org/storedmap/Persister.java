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

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final ScheduledThreadPoolExecutor _mainIndexer = new ScheduledThreadPoolExecutor(100, new ThreadFactory() {

        private int _num = 0;

        @Override
        public Thread newThread(Runnable r) {
            _num++;
            return new Thread(r, "StoredMapIndexer-" + _num);
        }
    });

    Persister(Store store) {

        _mainIndexer.setKeepAliveTime(1, TimeUnit.MINUTES);
        _mainIndexer.allowCoreThreadTimeOut(true);
        _mainIndexer.setMaximumPoolSize(Integer.MAX_VALUE);

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

    private void _remove(WeakHolder holder, StoredMap storedMap, Runnable callback) {
        _store.getDriver().remove(holder.getKey(), storedMap.category().internalIndexName(), _store.getConnection(), new Runnable() {
            @Override
            public void run() {
                synchronized (holder) {
                    _store.getDriver().unlock(holder.getKey(), storedMap.category().internalIndexName(), _store.getConnection());
                    holder.notify();
                    LOG.debug("Removed {}-{}", holder.getCategory().name(), holder.getKey());
                    if (callback != null) {
                        callback.run();
                    }
                }
            }
        });

    }

    MapData scheduleForPersist(StoredMap storedMap, Runnable callback, boolean deferredCreate, boolean needRemove) {
        WeakHolder holder = storedMap.holder();
        synchronized (holder) {
            LOG.debug("Planning to {} {}-{}", needRemove ? "remove" : "save", holder.getCategory().name(), holder.getKey());
            SaveOrReschedule command = _inWork.get(holder);
            if (command != null) {

                if (needRemove) {
                    command._needRemove = true;
                    _remove(holder, storedMap, callback);
                    MapData mapData = new MapData();
                    holder.put(mapData);
                    _inWork.remove(holder);
                    _inLongWork.remove(holder);
                    return mapData;
                } else {
                    command._reschedule = true;
                }

                if (callback != null) {
                    command._callbacks.add(callback);
                }

                LOG.debug("Skipping saving {}-{} as rescheduled", holder.getCategory().name(), holder.getKey());
                return command._mapData;
            } else {

                MapData mapData;

                if (!deferredCreate || needRemove) {

                    if (!needRemove) {
                        SaveOrReschedule anotherSor = _inLongWork.get(holder);
                        if (anotherSor != null) { // we are working with it! let's keep it locked for ourselves
                            // add a followup
                            mapData = anotherSor._mapData; // this doesn't go to database as it is kept in long work command

                            if (anotherSor._followup != null) {
                                command = anotherSor._followup;
                            } else {
                                command = new SaveOrReschedule(storedMap, mapData, deferredCreate);
                                anotherSor._followup = command;
                            }
                            command._callbacks.add(callback);
                            // no need to check for lock
                            return mapData;
                        }
                    }

                    Driver.Lock waitForLock;
                    // wait for releasing on other machines then lock for ourselves
                    while ((waitForLock = _store.getDriver().tryLock(holder.getKey(), storedMap.category().internalIndexName(), _store.getConnection(), 100000, _store.sessionId)).getWaitTime() > 0) {
                        try {
                            LOG.warn("Waiting {} for {} in main persist schedule{}", storedMap.category().internalIndexName(), waitForLock, needRemove ? " for remove" : "");
                            holder.wait(waitForLock.getWaitTime() > 5000 ? 2000 : waitForLock.getWaitTime()); // check every 2 seconds
                        } catch (InterruptedException ex) {
                            throw new RuntimeException("Unexpected interruption", ex);
                        }
                    }

                    if (needRemove) {
                        _remove(holder, storedMap, callback);
                        mapData = new MapData();
                        holder.put(mapData);
                        return mapData;
                    } else {
                        mapData = storedMap.getMapData();
                    }

                } else {
                    mapData = new MapData();
                    holder.put(mapData);
                }

                command = new SaveOrReschedule(storedMap, mapData, deferredCreate);
                command._callbacks.add(callback);

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
        private boolean _needRemove = false;
        private final boolean _deferredCreate;
        private final StoredMap _sm;
        private final WeakHolder _holder;
        private final MapData _mapData;
        //private boolean _cancelSave = false;
        private final ArrayList<Runnable> _callbacks = new ArrayList<>(2);
        private boolean _lockedInFirstReschedule = false;
        private SaveOrReschedule _followup = null;

        public SaveOrReschedule(StoredMap sm, MapData md, boolean deferredCreate) {
            _deferredCreate = deferredCreate;
            _sm = sm;
            _mapData = md;
            _holder = sm.holder();
        }

        private boolean _tryReschedule() {
            if (_reschedule) {
                //_reschedule = false;

                SaveOrReschedule sor = new SaveOrReschedule(_sm, _mapData, _deferredCreate);
                sor._callbacks.addAll(_callbacks);
                sor._lockedInFirstReschedule = _lockedInFirstReschedule;
                sor._needRemove = _needRemove;
                _inWork.put(_holder, sor);
                _inLongWork.put(_holder, sor);
                _mainIndexer.schedule(sor, 2, TimeUnit.SECONDS);
                LOG.debug("Rescheduling saving {}-{} as new info came. Queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());

                return true;
            } else {
                return false;
            }
        }

        @Override
        public void run() {

            LOG.debug("Ready to save map data {}-{}; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());

            try {
                synchronized (_holder) {

                    if (_needRemove) {
                        return;
                    }

                    if (_tryReschedule()) {
                        return;
                    }

                    Driver driver = _store.getDriver();
                    Object connection = _store.getConnection();
                    Category category = _sm.category();
                    String indexName = category.internalIndexName();

                    if (_deferredCreate && !_lockedInFirstReschedule) { // lock only once in case of rescheduling a deferred saver

                        LOG.debug("Locking to create map data {}-{}; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());

                        Driver.Lock waitForLock;
                        // wait for releasing on other machines then lock for ourselves
                        while ((waitForLock = _store.getDriver().tryLock(_holder.getKey(), _sm.category().internalIndexName(), _store.getConnection(), 100000, _store.sessionId)).getWaitTime() > 0) {
                            try {
                                LOG.warn("Waiting {} for {} in deferred creation", _sm.category().internalIndexName(), waitForLock);
                                _holder.wait(waitForLock.getWaitTime() > 5000 ? 2000 : waitForLock.getWaitTime()); // check every 2 seconds
                            } catch (InterruptedException ex) {
                                throw new RuntimeException("Unexpected interruption", ex);
                            }
                        }

                        _lockedInFirstReschedule = true;

                    }

                    byte[] mapB = SerializationUtils.serialize(_mapData);

                    // data for additional index
                    Map<String, Object> mapDataMap = _mapData.getMap();
                    byte[] sorter = _mapData.getSorterAsBytes(category.collator(), driver.getMaximumSorterLength(connection));
                    String[] tags = _mapData.getTags();
                    String secondaryKey = _mapData.getSecondarKey();

                    LOG.debug("Sending to save map data {}-{}; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());
                    driver.put(_holder.getKey(), indexName, connection, mapB, () -> {
                        LOG.debug("Sent to saved map data for {}-{}, proceed for index; queue size={}", _holder.getCategory().name(), _holder.getKey(), ((ScheduledThreadPoolExecutor) _mainIndexer).getQueue().size());
                        synchronized (_holder) {
                            if (!_needRemove) {
                                if (!_tryReschedule()) {
                                    _inWork.remove(_holder);
                                }
                            }
                        }
                    }, () -> {
                        synchronized (_holder) {
                            if (!_needRemove) {
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

                                                if (_followup != null) {
                                                    //don't  unlock but run a followup!
                                                    _inWork.put(_holder, _followup);
                                                    _inLongWork.put(_holder, _followup);
                                                    _mainIndexer.schedule(_followup, 3, TimeUnit.SECONDS);
                                                    LOG.debug("Fully saved {}-{} but have a followup. Keep the lock", _holder.getCategory().name(), _holder.getKey());
                                                } else {
                                                    driver.unlock(_holder.getKey(), indexName, connection);
                                                    _inLongWork.remove(_holder);
                                                    LOG.debug("Unlocked after full save of {}-{}", _holder.getCategory().name(), _holder.getKey());
                                                }
                                                _holder.notify();
                                                for (Runnable callback : _callbacks) {
                                                    callback.run();
                                                }
                                            }
                                        });
                            }
                        }
                    });

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
