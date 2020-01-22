package org.infinispan.api.collection;

import java.util.Map;

import org.infinispan.api.Experimental;
import org.infinispan.api.collection.listener.KeyValueStoreListener;
import org.infinispan.api.collection.query.QueryRequest;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * A Reactive Key Value Store provides a highly concurrent and distributed data structure, non blocking and using
 * reactive streams and <a href="https://smallrye.io/smallrye-mutiny/">Mutiny</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @see <a href="http://www.infinispan.org">Infinispan documentation</a>
 * @since 10.0
 */
@Experimental
public interface KeyValueStore<K, V> {

   /**
    * Get the value of the Key if such exists
    *
    * @param key
    * @return the value
    */
   Uni<V> get(K key);

   /**
    * Insert the key/value if such key does not exist
    *
    * @param key
    * @param value
    * @return Void
    */
   Uni<Boolean> insert(K key, V value);

   /**
    * Save the key/value. If the key exists will replace the value
    *
    * @param key
    * @param value
    * @return Void
    */
   Uni<Void> save(K key, V value);

   /**
    * Delete the key
    *
    * @param key
    * @return Void
    */
   Uni<Void> delete(K key);

   /**
    * Retrieve all keys
    *
    * @return Publisher
    */
   Multi<K> keys();

   /**
    * Retrieve all entries
    *
    * @return
    */
   Multi<? extends Map.Entry<K, V>> entries();

   /**
    * Estimate the size of the store
    *
    * @return Long, estimated size
    */
   Uni<Long> estimateSize();

   /**
    * Clear the store. If a concurrent operation puts data in the store the clear might not properly work
    *
    * @return Void
    */
   Uni<Void> clear();

   /**
    * Executes the query and returns a reactive streams Publisher with the results
    *
    * @param ickleQuery query String
    * @return Publisher reactive streams
    */
   Multi<KeyValueEntry<K, V>> find(String ickleQuery);

   /**
    * Find by QueryRequest.
    *
    * @param queryRequest
    * @return
    */
   Multi<KeyValueEntry<K, V>> find(QueryRequest queryRequest);

   /**
    * Executes the query and returns a reactive streams Publisher with the results
    *
    * @param ickleQuery query String
    * @return Publisher reactive streams
    */
   Multi<KeyValueEntry<K, V>> findContinuously(String ickleQuery);

   /**
    * Executes the query and returns a reactive streams Publisher with the results
    *
    * @param queryRequest
    * @return Publisher reactive streams
    */
   <T> Multi<KeyValueEntry<K, T>> findContinuously(QueryRequest queryRequest);

   /**
    * Listens to the {@link KeyValueStoreListener}
    *
    * @param listener
    * @return Publisher reactive streams
    */
   Multi<KeyValueEntry<K, V>> listen(KeyValueStoreListener listener);

}
