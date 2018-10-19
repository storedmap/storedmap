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

import java.lang.ref.WeakReference;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class WeakHolder {

    private final String _key;
    private final Category _category;
    //private boolean _shortLock = false;
    private WeakReference<MapData> _wr = new WeakReference<>(null);
    private transient Thread _takenForPersistIn = null; 

    WeakHolder(String key, Category category) {
        _key = key;
        _category = category;
    }

    String getKey() {
        return _key;
    }

    public Category getCategory() {
        return _category;
    }

    synchronized void put(MapData map) {
        if (map == null) {
            _wr = null;
        } else {
            MapData curMap = _wr.get();
            if (curMap != map) { // don't replace the object if we're reputting it
                _wr = new WeakReference<>(map);
            }
        }
    }

    synchronized MapData get() {
        if (_wr == null) {
            return null;
        } else {
            return _wr.get();
        }
    }

//    synchronized void lockOnMachine() {
//        while (true) {
//            if (!_shortLock) {
//                break;
//            }
//            try {
//                this.wait();
//            } catch (InterruptedException ex) {
//                throw new RuntimeException("Unexpected interruption", ex);
//            }
//        }
//        _shortLock = true;
//    }
//
//    synchronized void unlockOnMachine() {
//        _shortLock = false;
//        this.notify();
//    }

    Thread getTakenForPersistIn(){
        return _takenForPersistIn;
    }

    public void setTakenForPersistIn(Thread _takenForPersistIn) {
        this._takenForPersistIn = _takenForPersistIn;
    }
    
}
