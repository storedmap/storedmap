/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vsetec.storedmap;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class MapAndLocale {
    
    private final LinkedHashMap<String,Object>_map;
    private final Locale _locale;

    MapAndLocale(LinkedHashMap<String, Object> _map, Locale _locale) {
        this._map = _map;
        this._locale = _locale;
    }

    public LinkedHashMap<String, Object> getMap() {
        return _map;
    }

    public Locale getLocale() {
        return _locale;
    }
    
    
}
