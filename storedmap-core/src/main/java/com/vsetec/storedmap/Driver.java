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

import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author Fyodor Kravchenko <fedd@vsetec.com>
 */
public interface Driver {

    Object openConnection(String connectionString, Properties properties);

    void closeConnection(Object connection);
    

    int getMaximumIndexNameLength();

    int getMaximumKeyLength();

    int getMaximumTagLength();

    int getMaximumSorterLength();

    void createIndexIfDoesNotExist(Object connection, String indexName);


    byte[] get(String key, String indexName, Object connection);

    Iterable<String> get(String indexName, Object connection);

    Iterable<String> get(String indexName, Object connection, String[] anyOfTags);

    Iterable<String> get(String indexName, Object connection, byte[] minSorter, byte[] maxSorter);
    
    Iterable<String> get(String indexName, Object connection, String textQuery);
    
    Iterable<String> get(String indexName, Object connection, byte[] minSorter, byte[] maxSorter, String[] anyOfTags);

    Iterable<String> get(String indexName, Object connection, String textQuery, String[] anyOfTags);

    Iterable<String> get(String indexName, Object connection, String textQuery, byte[] minSorter, byte[] maxSorter, String[] anyOfTags);

    Iterable<String> get(String indexName, Object connection, String textQuery, byte[] minSorter, byte[] maxSorter);


    void put(String key, String indexName, Object connection, byte[] value, boolean doSynchronously);

    void apply(String key, String indexName, Object connection, byte[] sorter, String... tags);
    
    void applyFulltext(String key, String indexName, Object connection, Map<String,Object> map, Locale locale);


    String[] getTags(String key, String indexName, Object connection);

    byte[] getSorter(String key, String indexName, Object connection);


    void removeTagsSorterAndFulltext(String key, String indexName, Object connection);

}
