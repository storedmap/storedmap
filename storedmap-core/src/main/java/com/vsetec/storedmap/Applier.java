/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vsetec.storedmap;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public class Applier {

    private final ObjectMapper _om = new ObjectMapper();
    private final Store _store;
    private final LinkedBlockingQueue<WeakHolder> _fulltexts = new LinkedBlockingQueue<>(1000);
    private final LinkedBlockingQueue<Object[]>_sorterandtags = new LinkedBlockingQueue<>(1000);

    Applier(Store store) {
        _store = store;
    }

    void indexFulltext(StoredMap map) {
        WeakHolder holder = map.holder();
        MapAndLocale mal = holder.get();
        if (mal == null) {
            mal = map.category().getOrLoadMapAndLocale(holder);
        }
        try {
            _fulltexts.put(mal);
        } catch (InterruptedException ex) {
            throw new RuntimeException("Unexpected interrupt", ex);
        }
    }

    void applySorterAndTags(StoredMap map) {
        WeakHolder holder = map.holder();
        MapAndLocale mal = holder.get();
        if (mal == null) {
            mal = map.category().getOrLoadMapAndLocale(holder);
        }
        try {
            _fulltexts.put(mal);
        } catch (InterruptedException ex) {
            throw new RuntimeException("Unexpected interrupt", ex);
        }
    }

}
