package org.infinispan.api.sync;

/**
 * @since 15.0
 **/
public interface SyncSortedSet<V> {

   String name();

   SyncContainer container();

   void add(V value);

   void remove(V value);

   long size();

   //
}
