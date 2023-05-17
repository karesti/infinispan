package org.infinispan.api.sync;

/**
 * @since 15.0
 **/
public interface SyncStructures {
   <V> SyncList<V> list(String container, String listName);

   <V> SyncSet<V> set(String container, String setName);

   <V> SyncSortedSet<V> sortedSet(String container, String sortedSetName);
}
