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
import java.util.Locale;
import java.util.Map;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Holder {

    private final String _key;
    private boolean _shortLock = false;
    private WeakReference<MapAndLocale> _wr = new WeakReference<>(null);
    private Locale _locale = null;
            

    Holder(String key, Locale locale) {
        _key = key;
        _locale = locale;
    }

    String getKey() {
        return _key;
    }
    
    Locale getLocale(){
        return _locale;
    }
    
    synchronized void put(MapAndLocale map) {
        if (map == null) {
            _wr = null;
        } else {
            MapAndLocale curMap = _wr.get();
            if (curMap != map) { // don't replace the object if we're reputting it
                _wr = new WeakReference<>(map);
                _locale = map.getLocale();
            }
        }
    }

    synchronized MapAndLocale get() {
        if (_wr == null) {
            return null;
        } else {
            return _wr.get();
        }
    }

    synchronized void lockOnMachine() {
        while (true) {
            if (!_shortLock) {
                break;
            }
            try {
                this.wait();
            } catch (InterruptedException ex) {
                throw new RuntimeException("Unexpected interruption", ex);
            }
        }
        _shortLock = true;
    }

    synchronized void unlockOnMachine() {
        _shortLock = false;
        this.notify();
    }

}
