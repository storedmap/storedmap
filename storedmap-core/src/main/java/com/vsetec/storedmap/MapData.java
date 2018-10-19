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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class MapData implements Serializable {

    private final LinkedHashMap<String, Object> _map = new LinkedHashMap<>();
    private final List<Locale> _locales = new ArrayList(3);
    private final List<Byte> _sorter = new ArrayList<>();
    private final List<String>_tags = new ArrayList<>(4);
    private transient WeakHolder _tmpHolder = null;
    
    LinkedHashMap<String, Object> getMap() {
        return _map;
    }

    List<Locale> getLocales() {
        return _locales;
    }

    List<Byte> getSorter() {
        return _sorter;
    }

    List<String> getTags() {
        return _tags;
    }
    
    void setTmpHolder(WeakHolder holder){
        _tmpHolder = holder;
    }
    
    WeakHolder getTmpHolder(){
        return _tmpHolder;
    }
    
}
