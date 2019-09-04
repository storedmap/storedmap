# storedmap
A database-backed java.util.Map

StoredMap as a database-backed map is a library able to persist data, represent it in a form 
of standard Java Map, useful in scripting and expression environments. Once obtained with the unique 
identifier, the map can be modified and populated with simple values, Lists and nested Maps, and 
the subsequent retrieval will provide access to that data. The library lets accompany the persisted 
data structures with limited sorting and filtering information. 

It uses a relational database or a key-value store as its persistence mechanism if the suitable Driver 
is provided. The library expects the underlying database to be able to store binary data as well as 
specifying String tags, sortable objects and Json representation to provide an ability to search 
and order the StoredMap instances by the selected information bits.

Drivers
-------

See https://github.com/storedmap for the generic JDBC driver and an Elasticsearch driver.

Examples
--------

Create an Elasticsearch-backed StoredMap:

        Properties elasticsearch = new Properties();
        elasticsearch.setProperty("applicationCode", "law");
        elasticsearch.setProperty("driver", ElasticsearchDriver.class.getName());
        elasticsearch.setProperty("elasticsearch.host", "localhost");
        elasticsearch.setProperty("elasticsearch.port", "9200");
        Store store = new Store(elasticsearch);
        
Get a category - a set of maps with similar structure:

        Category constitution = store.get("constitution");
        
Get a stored map:

        StoredMap article = constitution.map("1");
        
Manipulate data in the map:

        article.put("title", 1);
        article.get("contents");

Close connection:

        store.close();
        
Advanced manipulations and iterations:

        store = new Store(elasticsearch);
        constitution = store.get("constitution");
        article = constitution.map("1amd");
        article.tags(new String[]{"amendment"});
        article.sorter(1);
        article.put("title", "First Amendment");
        //.....
        // find all entries with a certain tag sorted by a value provided with `sorter` method:
        for(StoredMap article: constitution.maps(null, null, null, new String[]{"amendment"}, true, null)){
           System.out.println(article.get("title"));
           //.....
        }
        store.close();
        
        
        
