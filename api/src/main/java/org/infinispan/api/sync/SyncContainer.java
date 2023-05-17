package org.infinispan.api.sync;

import java.util.function.Function;

import org.infinispan.api.Infinispan;
import org.infinispan.api.common.events.container.ContainerListenerEventType;
import org.infinispan.api.sync.events.container.SyncContainerListener;

/**
 * @since 14.0
 **/
public interface SyncContainer extends Infinispan {

   // quick access -> default config used if not exists
   SyncCache getCache(String name);

   // quick access ?
   SyncList getList(String name);

   SyncSet getSet(String name);

   SyncSortedSet getSortedSet(String name);

   SyncMultimap getMultimap(String name); // array lists or sets

   SyncStructures structures();

   SyncCaches caches();

   SyncMultimaps multimaps();

   SyncStrongCounters strongCounters();

   SyncWeakCounters weakCounters();

   SyncLocks locks();

   void listen(SyncContainerListener listener, ContainerListenerEventType... types);

   <T> T batch(Function<SyncContainer, T> function);
}
