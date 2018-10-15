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
