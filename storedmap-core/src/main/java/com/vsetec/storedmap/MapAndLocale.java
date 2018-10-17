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

    public LinkedHashMap<String, Object> getMap() {
        return _map;
    }

    public List<Locale> getLocales() {
        return _locales;
    }

    public List<Byte> getSorter() {
        return _sorter;
    }

    public List<String> getTags() {
        return _tags;
    }

}
