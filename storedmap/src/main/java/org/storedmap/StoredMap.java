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

import static org.storedmap.MapData.NOTAGSMAGICAL;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * A persisted, data store backed {@link java.util.Map}
 *
 * @author Fyodor Kravchenko {@literal(<fedd@vsetec.com>)}
 */
public class StoredMap implements Map<String, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(StoredMap.class);

    private final Category _category;
    private final WeakHolder _holder;
    private final int _hash;
    transient boolean isRemoved = false;

    StoredMap(Category category, WeakHolder holder) {
        _category = category;
        _holder = holder;
        int hash = 7;
        hash = 19 * hash + Objects.hashCode(this._category);
        hash = 19 * hash + Objects.hashCode(this._holder);
        _hash = hash;
    }

    public Category category() {
        return _category;
    }

    public String key() {
        return _holder.getKey();
    }

    WeakHolder holder() {
        return _holder;
    }

//    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
//
//        LOG.debug("Starting deserializing some StoredMap");
//        Properties properties = (Properties) in.readObject();
//        Store store = Store.getStore(properties);
//
//        String categoryName = (String) in.readObject();
//        Category category = store.get(categoryName);
//
//        String key = (String) in.readObject();
//        StoredMap another = category.map(key);
//
//        try {
//            Field fld = this.getClass().getDeclaredField("_category");
//            fld.setAccessible(true);
//            fld.set(this, category);
//            fld.setAccessible(false);
//
//            fld = this.getClass().getDeclaredField("_holder");
//            fld.setAccessible(true);
//            fld.set(this, another._holder);
//            fld.setAccessible(false);
//        } catch (IllegalAccessException | IllegalArgumentException | NoSuchFieldException | SecurityException e) {
//            throw new StoredMapException("Couldn't deserialize stored map with key " + key, e);
//        }
//
//        in.close();
//
//    }
//
//    private void writeObject(ObjectOutputStream out) throws IOException {
//        Store store = _category.store();
//        out.writeObject(store.properties());
//        out.writeChars(_category.name());
//        out.writeChars(_holder.getKey());
//    }
    private MapData _getOrLoadForPersist() {

        MapData md = _category.store().getPersister().scheduleForPersist(this, null);
        return md;

    }

    private MapData _getOrLoadForPersist(Runnable callback) {

        MapData md = _category.store().getPersister().scheduleForPersist(this, callback);
        return md;

    }

    public void remove() {
        // immediate remove
        synchronized (_holder) {

            LOG.debug("Planning to remove {}-{}", _holder.getCategory().name(), _holder.getKey());

            Store store = _category.store();

            store.getPersister().cancelSave(_holder);

            Driver driver = store.getDriver();

            //if (!_category.store().getPersister().isInWork(_holder)) 
            {
                long waitForLock;
                // wait for releasing on other machines then lock for ourselves
                while ((waitForLock = driver.tryLock(_holder.getKey(), _category.internalIndexName(), store.getConnection(), 100000)) > 0) {
                    try {
                        _holder.wait(waitForLock > 5000 ? 2000 : waitForLock); // check every 2 seconds
                    } catch (InterruptedException ex) {
                        throw new RuntimeException("Unexpected interruption", ex);
                    }
                }
            }

            isRemoved = true;

            _category.removeFromCache(_holder.getKey());
            driver.remove(_holder.getKey(), _category.internalIndexName(), store.getConnection(), new Runnable() {
                @Override
                public void run() {
                    synchronized (_holder) {
                        driver.unlock(_holder.getKey(), _category.internalIndexName(), store.getConnection());
                        _holder.notify();
                        LOG.debug("Removed {}-{}", _holder.getCategory().name(), _holder.getKey());
                    }
                }
            });

        }
    }

    MapData getMapData() {
        synchronized (_holder) {
            MapData map = _holder.get();
            if (map == null) {
                String key = _holder.getKey();
                Store store = _category.store();
                byte[] mapB = store.getDriver().get(key, _category.internalIndexName(), store.getConnection());
                if (mapB != null) {
                    LOG.debug("Went for map with id {} in {}, found", key, _category.name());
                    map = (MapData) SerializationUtils.deserialize(mapB);
                } else {
                    LOG.debug("Went for map with id {} in {}, DID NOT found", key, _category.name());
                    map = new MapData();
                }
                _holder.put(map);
            }
            return map;
        }
    }

    // sorting and filtering metadata
    public Object sorter() {
        return getMapData().getSorter();
    }

    public void sorter(Object sorter, Runnable callback) {
        synchronized (_holder) {
            MapData map = _getOrLoadForPersist(callback);
            //System.out.println("putting sorter: " + sorter.toString() + " for key " + _holder.getKey());
            map.setSorter(sorter);
        }
    }

    public String secondaryKey() {
        return getMapData().getSecondarKey();
    }

    public void secondaryKey(String secondaryKey, Runnable callback) {
        synchronized (_holder) {
            MapData map = _getOrLoadForPersist(callback);
            //System.out.println("putting sorter: " + sorter.toString() + " for key " + _holder.getKey());
            map.setSecondaryKey(secondaryKey);
        }
    }

    public String[] tags() {
        String[] _tags = getMapData().getTags();
        if (_tags.length == 1 && _tags[0].equals(NOTAGSMAGICAL)) {
            return MapData.NOTAGSACTUAL;
        } else {
            return _tags;
        }
    }

    public void tags(String[] tags, Runnable callback) {
        synchronized (_holder) {
            MapData map = _getOrLoadForPersist(callback);
            map.setTags(tags);
        }
    }

    // simple
    @Override
    public int size() {
        return getMapData().getMap().size();
    }

    @Override
    public boolean isEmpty() {
        return getMapData().getMap().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return getMapData().getMap().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return getMapData().getMap().containsValue(value);
    }

    @Override
    public Set<String> keySet() {
        return getMapData().getMap().keySet();
    }

    // special atomic modifying
    public Long increaseBy(String key, long by) {
        synchronized (_holder) {
            MapData md = _getOrLoadForPersist();
            Map<String, Object> map = md.getMap();
            Object value = map.get(key);
            Long ret = null;
            if (value instanceof Number) {
                Number number = (Number) value;
                ret = number.longValue() + by;
                map.put(key, ret);
            }
            return ret;
        }
    }

    public Long adjustGetDifference(String key, long target) {
        synchronized (_holder) {
            MapData md = _getOrLoadForPersist();
            Map<String, Object> map = md.getMap();
            Object value = map.get(key);
            Long ret = null;
            if (value instanceof Number) {
                long cur = ((Number) value).longValue();
                ret = target - cur;
                map.put(key, target);
            }
            return ret;
        }
    }

    public Long decreaseBy(String key, long by) {
        synchronized (_holder) {
            MapData md = _getOrLoadForPersist();
            Map<String, Object> map = md.getMap();
            Object value = map.get(key);
            Long ret = null;
            if (value instanceof Number) {
                Number number = (Number) value;
                ret = number.longValue() - 1;
                map.put(key, ret);
            }
            return ret;
        }
    }

    // simple modifying
    @Override
    public Object put(String key, Object value) {
        synchronized (_holder) {
            MapData md = _getOrLoadForPersist();
            Map<String, Object> map = md.getMap();
            Object ret = map.put(key, _ensureSimpleCollections(value));
            return ret;
        }
    }

    public Object putAndReturnBacked(String key, Object value) {
        synchronized (_holder) {
            MapData map = _getOrLoadForPersist();
            value = _ensureSimpleCollections(value);
            map.getMap().put(key, value);
            //_persist(map);
            return _backupWithMe(value, key);
        }
    }

    @Override
    public Object remove(Object key) {
        synchronized (_holder) {
            MapData map = _getOrLoadForPersist();
            Object ret = map.getMap().remove(key);
            ///_persist(map);
            return ret;
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        synchronized (_holder) {
            MapData map = _getOrLoadForPersist();
            m = (Map<? extends String, ? extends Object>) _ensureSimpleCollections(m);
            map.getMap().putAll(m);
            //_persist(map);
        }
    }

    @Override
    public void clear() {
        synchronized (_holder) {
            MapData map = _getOrLoadForPersist();
            map.getMap().clear();
            //_persist(map);
        }
    }

    // stored backed results
    // back maps and lists
    @Override
    public Object get(Object key) {
        if (!(key instanceof String)) {
            return null;
        }
        synchronized (_holder) {
            MapData md = getMapData();
            Map<String, Object> map = md.getMap();
            Object ret = map.get(key);
            ret = _backupWithMe(ret, (String) key);
            return ret;
        }
    }

    @Override
    public Collection<Object> values() {
        synchronized (_holder) {
            Map map = getMapData().getMap();
            Set<String> keys = map.keySet();
            ArrayList backed = new ArrayList(keys.size() + 3);
            for (String key : keys) {
                Object val = map.get(key);
                backed.add(_backupWithMe(val, key));
            }
            return backed;
        }
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return new EntrySetMain();
    }

    ////// entryset backed by this map
    private class EntrySetMain implements Set<Entry<String, Object>> {

        @Override
        public int size() {
            return StoredMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return StoredMap.this.isEmpty();
        }

        @Override
        public Iterator<Entry<String, Object>> iterator() {
            final Map map = getMapData().getMap();
            final Iterator<Entry<String, Object>> it = map.entrySet().iterator();

            return new Iterator<Entry<String, Object>>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Entry<String, Object> next() {
                    final Entry<String, Object> entry = it.next();
                    return new Entry<String, Object>() {
                        @Override
                        public String getKey() {
                            return entry.getKey();
                        }

                        @Override
                        public Object getValue() {
                            return _backupWithMe(entry.getValue(), entry.getKey());
                        }

                        @Override
                        public Object setValue(Object value) {
                            return StoredMap.this.put(entry.getKey(), value);
                        }
                    };
                }
            };
        }

        @Override
        public Object[] toArray() {
            synchronized (StoredMap.this._holder) { // make any sence?
                ArrayList<Entry<String, Object>> ret = new ArrayList<>();
                Iterator<Entry<String, Object>> it = iterator();
                while (it.hasNext()) {
                    ret.add(it.next());
                }
                return ret.toArray();
            }
        }

        @Override
        public <T> T[] toArray(T[] arg0) {
            synchronized (StoredMap.this._holder) { // make any sence?
                ArrayList<T> ret = new ArrayList<>();
                Iterator<Entry<String, Object>> it = iterator();
                while (it.hasNext()) {
                    ret.add((T) it.next());
                }
                return ret.toArray(arg0);
            }
        }

        @Override
        public boolean add(Entry<String, Object> e) {
            Object ret = StoredMap.this.put(e.getKey(), e.getValue());
            return ret != null;
        }

        @Override
        public void clear() {
            StoredMap.this.clear();
        }

        ///// no need to implement:
        @Override
        public boolean contains(Object o) {
            return getMapData().getMap().entrySet().contains(o);
        }

        @Override
        public boolean remove(Object o) {
            if (o instanceof Entry) {
                Entry kv = (Entry) o;
                return StoredMap.this.remove(kv.getKey(), kv.getValue());
            }
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return getMapData().getMap().entrySet().containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends Entry<String, Object>> c) {
            synchronized (StoredMap.this._holder) {
                boolean ret;
                MapData m = _getOrLoadForPersist();
                c = (Collection<? extends Entry<String, Object>>) _ensureSimpleCollections(c);
                ret = m.getMap().entrySet().addAll(c);
                if (ret) {
                    //_persist(m);
                } else {
                    //sdfs//_cache._synced = true;
                }
                return ret;
            }
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            synchronized (StoredMap.this._holder) {
                boolean ret;
                Map m = _getOrLoadForPersist().getMap();
                ret = m.entrySet().removeAll(c);
                if (ret) {
                    //_persist();
                } else {
                    //_cache._synced = true;
                }
                return ret;
            }
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            synchronized (StoredMap.this._holder) {
                boolean ret;
                Map m = _getOrLoadForPersist().getMap();
                c = (Collection<?>) _ensureSimpleCollections(c);
                ret = m.entrySet().retainAll(c);
                if (ret) {
                    //_persist();
                } else {
                    //_cache._synced = true;
                }
                return ret;
            }
        }

    }

    /////// magic
    private Object _backupWithMe(final Object value, String key) {
        return _backupWithMe(value, key, null, null);
    }

    private Object _backupWithMe(final Object value, String key, Object[] address, Object newKey) {

        if (address == null) {
            address = new Object[0];
        } else {
            Object[] newAddress = new Object[address.length + 1];
            java.lang.System.arraycopy(address, 0, newAddress, 0, address.length);
            newAddress[address.length] = newKey;
            address = newAddress;
        }

        if (value instanceof Map) {
            return new BackedMap(key, address);
        }

        if (value instanceof Collection) {
            return new BackedList(key, address);
        }

        return value;
    }

    private <T> T _getByAddress(Map m, Object key, Object... address) {
        Object ret = m.get(key);

        for (Object subKey : address) {
            try {
                if (ret instanceof Map) {
                    ret = ((Map) ret).get(subKey);
                } else if (ret instanceof List) {
                    ret = ((List) ret).get((int) subKey);
                } else {

                    Object tmp = new HashMap(1);
                    try {
                        ret = (T) tmp;
                    } catch (ClassCastException ee) {
                        ret = new ArrayList(1);
                    }

                    break; // TODO: restore error, probably                   
                    //throw new RuntimeException("In a map " + Arrays.toString(_multiId._category) + " Wrong Address: " + subKey + "," + Arrays.toString(address));
                }
            } catch (ClassCastException e) {
                throw new RuntimeException("In a map " + _holder.getKey() + " Problem with address: " + subKey + "," + Arrays.toString(address), e);
            }
        }
        return (T) ret;
    }

    private Object _ensureSimpleCollections(Object obj) {
        if (obj instanceof Collection) {
            Collection c = (Collection) obj;
            Object[] objects = new Object[c.size()];
            objects = c.toArray(objects);
            for (int i = 0; i < objects.length; i++) {
                objects[i] = _ensureSimpleCollections(objects[i]);
            }
            obj = new ArrayList(Arrays.asList(objects));
        } else if (obj instanceof Map) {
            Map<? extends Object, ? extends Object> m1 = (Map) obj;
            Map<? extends Object, ? extends Object> m2 = new LinkedHashMap<>(m1);

            for (Map.Entry entry : m2.entrySet()) {
                entry.setValue(_ensureSimpleCollections(entry.getValue()));
            }

            obj = m2;
        } else if (obj instanceof Entry) {
            ((Entry) obj).setValue(_ensureSimpleCollections(((Entry) obj).getValue()));
        }

        return obj;
    }

    private Object _ensureUnbacked(Object obj) {
        if (obj instanceof Backed) {
            obj = ((Backed) obj).unbacked();
        } else if (obj instanceof Collection) {
            Collection c = (Collection) obj;
            Object[] objects = new Object[c.size()];
            objects = c.toArray(objects);
            for (int i = 0; i < objects.length; i++) {
                objects[i] = _ensureUnbacked(objects[i]);
            }
            obj = new ArrayList(Arrays.asList(objects));
        } else if (obj instanceof Map) {
            Map<? extends Object, ? extends Object> m1 = (Map) obj;
            Map<? extends Object, ? extends Object> m2 = new LinkedHashMap<>(m1);

            for (Map.Entry entry : m2.entrySet()) {
                entry.setValue(_ensureUnbacked(entry.getValue()));
            }

            obj = m2;
        } else if (obj instanceof Entry) {
            ((Entry) obj).setValue(_ensureUnbacked(((Entry) obj).getValue()));
        }

        return obj;
    }

    public class BackedList implements List<Object>, Serializable, Backed<List> {

        private static final long serialVersionUID = 1L;

        private final String _key;
        private final Object[] _address;
        private final int _from, _to;

        private BackedList(String key, Object... address) {
            _key = key;
            _address = address;
            _from = -1;
            _to = -1;
        }

        private BackedList(String key, int from, int to, Object... address) {
            _key = key;
            _address = address;
            _from = from;
            _to = to;
        }

//        @Override
//        public void lockInStore(long millis) {
//            StoredMap.this.lockInStore(millis);
//        }
//
//        @Override
//        public void unlockInStore() {
//            StoredMap.this.unlockInStore();
//        }
//
//        @Override
//        public void lockOnMachine() {
//            StoredMap.this.lockOnMachine();
//        }
//
//        @Override
//        public void unlockOnMachine() {
//            StoredMap.this.unlockOnMachine();
//        }
        @Override
        public StoredMap storedMap() {
            return StoredMap.this;
        }

        @Override
        public List unbacked() {
            List ret = _getByAddress(getMapData().getMap(), _key, _address);
            if (_from > 0 || _to > 0) {
                ret = ret.subList(_from, _to);
            }
            return ret;
        }

        // simple
        @Override
        public int size() {
            return unbacked().size();
        }

        @Override
        public boolean isEmpty() {
            return unbacked().isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return unbacked().contains(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return unbacked().containsAll(c);
        }

        @Override
        public int indexOf(Object o) {
            return unbacked().indexOf(o);
        }

        @Override
        public int lastIndexOf(Object o) {
            return unbacked().lastIndexOf(o);
        }

        //// modify
        @Override
        public Object set(int index, Object element) {
            synchronized (StoredMap.this._holder) {

                element = _ensureUnbacked(element);

                Map m = _getOrLoadForPersist().getMap();
                List l = _getByAddress(m, _key, _address);
                Object ret = l.set(index, element);
                //_persist();
                return ret;
            }
        }

        @Override
        public boolean add(Object e) { // TODO: synch, lock//// - resolved?
            synchronized (StoredMap.this._holder) {

                e = _ensureUnbacked(e);

                Map m = _getOrLoadForPersist().getMap();
                List l = _getByAddress(m, _key, _address);
                boolean ret = l.add(e);
                //_persist();
                return ret;
            }
        }

        @Override
        public boolean remove(Object o) {
            synchronized (StoredMap.this._holder) {
                Map m = _getOrLoadForPersist().getMap();
                List l = _getByAddress(m, _key, _address);
                boolean ret = l.remove(o);
                if (ret) {
                    //_persist();
                } else {
                    //_cache._synced = true;
                }
                return ret;
            }
        }

        @Override
        public boolean addAll(Collection<? extends Object> c) {

            c = (Collection<?>) _ensureUnbacked(c);

            synchronized (StoredMap.this._holder) {
                Map m = _getOrLoadForPersist().getMap();
                List l = _getByAddress(m, _key, _address);
                boolean ret = l.addAll(c);
                if (ret) {
                    //_persist();
                } else {
                    //_cache._synced = true;
                }
                return ret;
            }
        }

        @Override
        public boolean addAll(int index, Collection<? extends Object> c) {
            synchronized (StoredMap.this._holder) {
                c = (Collection<?>) _ensureUnbacked(c);

                Map m = _getOrLoadForPersist().getMap();
                List l = _getByAddress(m, _key, _address);
                boolean ret = l.addAll(index, c);
                if (ret) {
                    //_persist();
                }
                return ret;
            }
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            synchronized (StoredMap.this._holder) {
                Map m = _getOrLoadForPersist().getMap();
                List l = _getByAddress(m, _key, _address);
                boolean ret = l.removeAll(c);
                if (ret) {
                    //_persist();
                } else {
                    //_cache._synced = true;
                }
                return ret;
            }
        }

        @Override
        public void add(int index, Object element) {
            synchronized (StoredMap.this._holder) {
                element = _ensureUnbacked(element);
                Map m = _getOrLoadForPersist().getMap();
                List l = _getByAddress(m, _key, _address);
                l.add(index, element);
                //_persist();
            }
        }

        @Override
        public Object remove(int index) {
            synchronized (StoredMap.this._holder) {
                Map m = _getOrLoadForPersist().getMap();
                List l = _getByAddress(m, _key, _address);
                Object ret = l.remove(index);
                if (ret != null) {
                    //_persist();
                } else {
                    //_cache._synced = true;
                }
                return ret;
            }
        }

        @Override
        public void clear() {
            synchronized (StoredMap.this._holder) {
                Map m = _getOrLoadForPersist().getMap();
                List l = _getByAddress(m, _key, _address);
                l.clear();
                //_persist();
            }
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            synchronized (StoredMap.this._holder) {
                c = (Collection<?>) _ensureUnbacked(c);
                Map m = _getOrLoadForPersist().getMap();
                List l = _getByAddress(m, _key, _address);
                boolean ret = l.retainAll(c);
                if (ret) {
                    //_persist();
                } else {
                    //_cache._synced = true;
                }
                return ret;
            }
        }

        ////// backed value
        @Override
        public Object get(int index) {
            synchronized (StoredMap.this._holder) {
                Map m = getMapData().getMap();
                List l = _getByAddress(m, _key, _address);
                Object ret = l.get(index);
                ret = _backupWithMe(ret, _key, _address, index);
                return ret;
            }
        }

        @Override
        public Iterator<Object> iterator() {
            synchronized (StoredMap.this._holder) {
                Map m = getMapData().getMap();
                List l = _getByAddress(m, _key, _address);
                final Iterator<Object> it = l.iterator();
                return new Iterator() {

                    int _i = 0;

                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Object next() {
                        Object ret = it.next();
                        ret = _backupWithMe(ret, _key, _address, _i);
                        _i++;
                        return ret;
                    }

                };
            }
        }

        @Override
        public Object[] toArray() {
            synchronized (StoredMap.this._holder) {
                ArrayList<Object> ret = new ArrayList<>();
                Iterator<Object> it = iterator();
                while (it.hasNext()) {
                    ret.add(it.next());
                }
                return ret.toArray();
            }
        }

        @Override
        public <T> T[] toArray(T[] arg0) {
            synchronized (StoredMap.this._holder) {
                ArrayList<T> ret = new ArrayList<>();
                Iterator<Object> it = iterator();
                while (it.hasNext()) {
                    ret.add((T) it.next());
                }
                return ret.toArray(arg0);
            }
        }

        @Override
        public ListIterator<Object> listIterator(int index) {
            synchronized (StoredMap.this._holder) {
                final Map m = getMapData().getMap();
                List l = _getByAddress(m, _key, _address);
                final ListIterator<Object> it = l.listIterator(index);
                return new ListIterator() {

                    int _i = 0;

                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Object next() {
                        Object ret = it.next();
                        ret = _backupWithMe(ret, _key, _address, _i);
                        _i++;
                        return ret;
                    }

                    @Override
                    public boolean hasPrevious() {
                        return it.hasPrevious();
                    }

                    @Override
                    public Object previous() {
                        Object ret = it.previous();
                        _i--;
                        ret = _backupWithMe(ret, _key, _address, _i);
                        return ret;
                    }

                    @Override
                    public int nextIndex() {
                        return it.nextIndex();
                    }

                    @Override
                    public int previousIndex() {
                        return it.previousIndex();
                    }

                    @Override
                    public void remove() {
                        // TODO: can't synch here :( consider throwing exception?
                        throw new UnsupportedOperationException();
                        //it.remove();
                        //_storeMap(m);
                    }

                    @Override
                    public void set(Object e) {
                        // TODO: can't synch here :( consider throwing exception?
                        throw new UnsupportedOperationException();

                        //e = _ensureUnbacked(e);
                        //it.set(e);
                        //_storeMap(m);
                    }

                    @Override
                    public void add(Object e) {
                        // TODO: can't synch here :( consider throwing exception?
                        throw new UnsupportedOperationException();

                        //e = _ensureUnbacked(e);
                        //it.add(e);
                        //_storeMap(m);
                    }

                };
            }
        }

        @Override
        public ListIterator<Object> listIterator() {
            return listIterator(0);
        }

        @Override
        public List<Object> subList(int fromIndex, int toIndex) {
            int from, to;
            if (_from > 0) {
                from = _from + fromIndex;
                to = from + toIndex;
            } else {
                from = fromIndex;
                to = toIndex;
            }
            if (_to > 0 && to > _to) {
                to = _to;
            }

            return new BackedList(_key, from, to, _address);
        }

    }

    public interface Backed<T> {

        static final long serialVersionUID = 1L; // TODO: do we need this?? h.z.

        public StoredMap storedMap();

        public T unbacked();

//        public void lockInStore(long millis);
//
//        public void unlockInStore();
//
//        public void lockOnMachine();
//
//        public void unlockOnMachine();
    }

    public class BackedMap implements Map<String, Object>, Serializable, Backed<Map> {

        private final String _key;
        private final Object[] _address;

        private BackedMap(String key, Object... address) {
            _key = key;
            _address = address;
        }

//        @Override
//        public void lockInStore(long millis) {
//            StoredMap.this.lockInStore(millis);
//        }
//
//        @Override
//        public void unlockInStore() {
//            StoredMap.this.unlockInStore();
//        }
//
//        @Override
//        public void lockOnMachine() {
//            StoredMap.this.lockOnMachine();
//        }
//
//        @Override
//        public void unlockOnMachine() {
//            StoredMap.this.unlockOnMachine();
//        }
        @Override
        public StoredMap storedMap() {
            return StoredMap.this;
        }

        @Override
        public Map unbacked() {
            synchronized (StoredMap.this._holder) {
                Map ret = _getByAddress(getMapData().getMap(), _key, _address);
                return ret;
            }
        }

        // simple
        @Override
        public int size() {
            return unbacked().size();
        }

        @Override
        public boolean isEmpty() {
            return unbacked().isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return unbacked().containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            return unbacked().containsValue(value);
        }

        @Override
        public Set keySet() {
            return unbacked().keySet();
        }

        ///// modifying
        @Override
        public Object put(String key, Object value) {
            synchronized (StoredMap.this._holder) {
                value = _ensureUnbacked(value);
                Map m = _getOrLoadForPersist().getMap();
                Map l = _getByAddress(m, _key, _address);
                Object ret = l.put(key, value);
                //_persist();
                return ret;
            }
        }

        @Override
        public Object remove(Object key) {
            synchronized (StoredMap.this._holder) {
                Map m = _getOrLoadForPersist().getMap();
                Map l = _getByAddress(m, _key, _address);
                Object ret = l.remove(key);
                if (ret != null) {
                    //_persist();
                } else {
                    //_cache._synced = true;
                }
                return ret;
            }
        }

        @Override
        public void putAll(Map map) {
            synchronized (StoredMap.this._holder) {
                map = (Map) _ensureUnbacked(map);
                Map m = _getOrLoadForPersist().getMap();
                Map l = _getByAddress(m, _key, _address);
                l.putAll(map);
                //_persist();
            }
        }

        @Override
        public void clear() {
            synchronized (StoredMap.this._holder) {
                Map m = _getOrLoadForPersist().getMap();
                Map l = _getByAddress(m, _key, _address);
                l.clear();
                //_persist();
            }
        }

        ///// backed values
        @Override
        public Object get(Object key) {
            synchronized (StoredMap.this._holder) {
                Map m = getMapData().getMap();
                Map l = _getByAddress(m, _key, _address);
                Object ret = l.get(key);
                ret = _backupWithMe(ret, _key, _address, key);
                return ret;
            }
        }

        @Override
        public Collection values() {
            synchronized (StoredMap.this._holder) {
                Map m = getMapData().getMap();
                Map l = _getByAddress(m, _key, _address);
                Set<String> keys = l.keySet();
                ArrayList<Object> backed = new ArrayList<>(keys.size() + 3);
                for (String key : keys) {
                    Object val = l.get(key);
                    backed.add(_backupWithMe(val, _key, _address, key));
                }
                return backed;
            }
        }

        @Override
        public Set entrySet() {
            return new EntrySetBacked();
        }

        private class EntrySetBacked implements Set<Entry<String, Object>> {

            @Override
            public int size() {
                return BackedMap.this.size();
            }

            @Override
            public boolean isEmpty() {
                return BackedMap.this.isEmpty();
            }

            @Override
            public Iterator<Entry<String, Object>> iterator() {
                synchronized (StoredMap.this._holder) {
                    Map m = getMapData().getMap();
                    Map l = _getByAddress(m, _key, _address);
                    final Iterator<Entry<String, Object>> it = l.entrySet().iterator();

                    return new Iterator<Entry<String, Object>>() {
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Entry<String, Object> next() {
                            final Entry<String, Object> entry = it.next();
                            return new Entry<String, Object>() {
                                @Override
                                public String getKey() {
                                    return entry.getKey();
                                }

                                @Override
                                public Object getValue() {
                                    return _backupWithMe(entry.getValue(), _key, _address, entry.getKey());
                                }

                                @Override
                                public Object setValue(Object value) {
                                    return BackedMap.this.put(entry.getKey(), value);
                                }
                            };
                        }
                    };
                }
            }

            @Override
            public Object[] toArray() {
                synchronized (StoredMap.this._holder) {
                    ArrayList<Entry<String, Object>> ret = new ArrayList<>();
                    Iterator<Entry<String, Object>> it = iterator();
                    while (it.hasNext()) {
                        ret.add(it.next());
                    }
                    return ret.toArray();
                }
            }

            @Override
            public <T> T[] toArray(T[] arg0) {
                synchronized (StoredMap.this._holder) {
                    ArrayList<T> ret = new ArrayList<>();
                    Iterator<Entry<String, Object>> it = iterator();
                    while (it.hasNext()) {
                        ret.add((T) it.next());
                    }
                    return ret.toArray(arg0);
                }
            }

            @Override
            public boolean add(Entry<String, Object> e) {
                Object ret = BackedMap.this.put(e.getKey(), e.getValue());
                return ret != null;
            }

            @Override
            public void clear() {
                BackedMap.this.clear();
            }

            ///// no need to implement:
            @Override
            public boolean contains(Object o) {
                return unbacked().entrySet().contains(o);
            }

            @Override
            public boolean remove(Object o) {
                if (o instanceof Entry) {
                    Entry kv = (Entry) o;
                    return BackedMap.this.remove(kv.getKey(), kv.getValue());
                }
                return false;
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                synchronized (StoredMap.this._holder) {
                    return unbacked().entrySet().containsAll(c);
                }
            }

            @Override
            public boolean addAll(Collection<? extends Entry<String, Object>> c) {
                synchronized (StoredMap.this._holder) {
                    Map m = _getOrLoadForPersist().getMap();
                    Map l = _getByAddress(m, _key, _address);

                    //Set<Entry<Object,Object>>entrySet = l.entrySet();
                    boolean ret = false;
                    for (Entry<String, Object> entry : c) {
                        if (l.put(entry.getKey(), _ensureUnbacked(entry.getValue())) != null) {
                            ret = true;
                        }
                    }

                    //ret = l.entrySet().addAll(c);
                    if (ret) {
                        //_persist();
                    } else {
                        //_cache._synced = true;
                    }
                    return ret;
                }
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                synchronized (StoredMap.this._holder) {
                    boolean ret;
                    Map m = _getOrLoadForPersist().getMap();
                    Map l = _getByAddress(m, _key, _address);
                    ret = l.entrySet().removeAll(c);
                    if (ret) {
                        //_persist();
                    } else {
                        //_cache._synced = true;
                    }
                    return ret;
                }
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                synchronized (StoredMap.this._holder) {
                    boolean ret;
                    Map m = _getOrLoadForPersist().getMap();
                    Map l = _getByAddress(m, _key, _address);
                    ret = l.entrySet().retainAll(c);
                    if (ret) {
                        //_persist();
                    } else {
                        //_cache._synced = true;
                    }
                    return ret;
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return _hash;
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
        final StoredMap other = (StoredMap) obj;
        if (!Objects.equals(this._category, other._category)) {
            return false;
        }
        return Objects.equals(this._holder, other._holder);
    }

}
