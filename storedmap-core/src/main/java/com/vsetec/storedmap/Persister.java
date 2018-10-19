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

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Persister {
    
    private final ConcurrentLinkedQueue<MapData>_q = new ConcurrentLinkedQueue<>();
    
    void addToQueue(MapData mal){
        _q.add(mal);
    }
    
    
    private void _persist(MapData mal) {
        WeakHolder holder = mal.getTmpHolder();
        synchronized (holder) {
            String key = holder.getKey();
            Category category = holder.getCategory();
            assert mal!=null;
            byte[] mapB = Util.object2bytes(mal);
            Store store = category.getStore();
            store.getDriver().put(key, category.getIndexName(), store.getConnection(), mapB, () -> {

                store.getDriver().unlock(key, category.getIndexName(), store.getConnection());
                holder.setTakenForPersistIn(null);
                mal.setTmpHolder(null);
                holder.notify();

            }, mal.getMap(), mal.getLocales(), mal.getSorter(), mal.getTags(), () -> {
                // do nothing for now
            });
        }
    }

    
}
