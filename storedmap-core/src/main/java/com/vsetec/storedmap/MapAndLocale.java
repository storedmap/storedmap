/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
public class MapAndLocale implements Serializable {

    private final LinkedHashMap<String, Object> _map = new LinkedHashMap<>();
    private final List<Locale> _locales = new ArrayList(3);
    private final List<Byte> _sorter = new ArrayList<>();
    private final List<String>_tags = new ArrayList<>(4);
    private transient Thread _takenForPersistIn = null;

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
    
    Thread getTakenForPersistIn(){
        return _takenForPersistIn;
    }

    public void setTakenForPersistIn(Thread _takenForPersistIn) {
        this._takenForPersistIn = _takenForPersistIn;
    }
    
}
