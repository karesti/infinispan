package org.infinispan.api.sync;

import java.util.Collection;

/**
 * @since 15.0
 **/
public interface SyncList<V> {

   String name();

   SyncContainer container();

   void offerLast(V value);

   void offerFirst(V value);

   long size();

   Collection<V> subList(int from, int to);

   //
}
