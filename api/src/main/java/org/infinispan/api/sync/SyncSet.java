package org.infinispan.api.sync;

/**
 * @since 15.0
 **/
public interface SyncSet<V> {

   String name();

   SyncContainer container();

   void add(V value);

   long size();

   //
}
