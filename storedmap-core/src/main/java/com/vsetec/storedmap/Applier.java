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
    private final LinkedBlockingQueue<String> _fulltexts = new LinkedBlockingQueue<>(1000);
    private final LinkedBlockingQueue _sorterandtags = new LinkedBlockingQueue(1000);
    
    Applier(Store store){
        _store = store;
    }
    
    void applyFulltext(StoredMap map){
        //_om.writeValueAsString(map);
    }
    
}
