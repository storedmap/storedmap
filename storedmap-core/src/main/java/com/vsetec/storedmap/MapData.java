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
import java.text.CollationKey;
import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class MapData implements Serializable {

    private final LinkedHashMap<String, Object> _map = new LinkedHashMap<>();
    private final List<Locale> _locales = new ArrayList(3);
    private final List<Byte> _sorterAsBytes = new ArrayList<>();
    private final Object[] _sorterAsObject = new Object[1];
    private final List<String> _tags = new ArrayList<>(4);

    LinkedHashMap<String, Object> getMap() {
        return _map;
    }

    List<Locale> getLocales() {
        return Collections.unmodifiableList(_locales);
    }
    
    synchronized void putLocales(List<Locale> locales){
        _locales.clear();
        _locales.addAll(locales);
        // recompute collation key
        putSorter(_sorterAsObject);
    }

    synchronized void putSorter(Object sorter){
        // convert to a sortable bytes
        if(sorter instanceof String){
            
            String rules = "";
            if(_locales.isEmpty()){
                rules = ((RuleBasedCollator) Collator.getInstance(new Locale("ru"))).getRules() + 
                        ((RuleBasedCollator) Collator.getInstance(Locale.US)).getRules() + 
                        ((RuleBasedCollator) Collator.getInstance(Locale.PRC)).getRules();
            }else{
                rules = "";
                for(Locale locale: Collections.unmodifiableList(_locales)){
                    RuleBasedCollator coll = (RuleBasedCollator) Collator.getInstance(locale);
                    rules = rules + coll.getRules();
                }
            }
            try{
                Collator common = new RuleBasedCollator(rules);
                CollationKey ck = common.getCollationKey((String) sorter);
                byte[]arr = ck.toByteArray();
                Byte[]arrB = new Byte[arr.length];
                for(int i=0;i<arr.length;i++){
                    arrB[i] = arr[i];
                }
                _sorterAsBytes.clear();
                _sorterAsBytes.addAll(Arrays.asList(arrB));
                _sorterAsObject[0] = sorter;
            }catch(ParseException e){
                throw new RuntimeException(e);
            }
            
            
        }//else if()
    }
    
    Object getSorter(){
        return _sorterAsObject[0];
    }

    List<Byte> getSorterAsBytes() {
        return _sorterAsBytes;
    }
    

    List<String> getTags() {
        return _tags;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 19 * hash + Objects.hashCode(this._map);
        hash = 19 * hash + Objects.hashCode(this._locales);
        hash = 19 * hash + Objects.hashCode(this._sorter);
        hash = 19 * hash + Objects.hashCode(this._tags);
        return hash;
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
        final MapData other = (MapData) obj;
        if (!Objects.equals(this._map, other._map)) {
            return false;
        }
        if (!Objects.equals(this._locales, other._locales)) {
            return false;
        }
        if (!Objects.equals(this._sorter, other._sorter)) {
            return false;
        }
        return Objects.equals(this._tags, other._tags);
    }

}
