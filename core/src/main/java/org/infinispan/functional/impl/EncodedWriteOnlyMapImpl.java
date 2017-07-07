package org.infinispan.functional.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.cache.impl.CacheEncoders;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.cache.impl.EncodingClasses;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.functional.Listeners.WriteListeners;
import org.infinispan.functional.Param;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Write-only map implementation.
 *
 * @since 9.2
 */
@Experimental
public final class EncodedWriteOnlyMapImpl<K, V> extends AbstractFunctionalMap<K, V> implements WriteOnlyMap<K, V> {
   private static final Log log = LogFactory.getLog(EncodedWriteOnlyMapImpl.class);

   private EncodingClasses encodingClasses;
   private CacheEncoders cacheEncoders;

   private EncodedWriteOnlyMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(params, functionalMap);
   }

   public static <K, V> WriteOnlyMap<K, V> create(FunctionalMapImpl<K, V> functionalMap) {
      return create(Params.from(functionalMap.params.params), functionalMap);
   }

   private static <K, V> WriteOnlyMap<K, V> create(Params params, FunctionalMapImpl<K, V> functionalMap) {
      if (!functionalMap.isEncoded()) {
         throw new IllegalStateException("Cache is not encoded");
      }
      EncodedWriteOnlyMapImpl<K, V> encodedWriteOnlyMap = new EncodedWriteOnlyMapImpl<>(params, functionalMap);
      EncoderCache encoderCache = (EncoderCache) functionalMap.cache;

      Encoder keyEncoder = encoderCache.getKeyEncoder();
      Wrapper keyWrapper = encoderCache.getKeyWrapper();
      Encoder valueEncoder = encoderCache.getValueEncoder();
      Wrapper valueWrapper = encoderCache.getValueWrapper();
      encodedWriteOnlyMap.cacheEncoders = new CacheEncoders(keyEncoder, keyWrapper, valueEncoder, valueWrapper);
      encodedWriteOnlyMap.encodingClasses = encoderCache.getEncodingClasses();

      return encodedWriteOnlyMap;
   }

   @Override
   public CompletableFuture<Void> eval(K key, Consumer<WriteEntryView<V>> f) {
      log.tracef("Invoked eval(k=%s, %s)", key, params);
      Object keyEncoded = cacheEncoders.keyToStorage(key);
      WriteOnlyKeyCommand cmd = fmap.commandsFactory.buildWriteOnlyKeyCommand(keyEncoded, f, params, encodingClasses);
      InvocationContext ctx = getInvocationContext(true, 1);
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> eval(K key, V value, BiConsumer<V, WriteEntryView<V>> f) {
      log.tracef("Invoked eval(k=%s, v=%s, %s)", key, value, params);
      Object keyEncoded = cacheEncoders.keyToStorage(key);
      Object valueEncoded = cacheEncoders.valueToStorage(value);
      WriteOnlyKeyValueCommand cmd = fmap.commandsFactory.buildWriteOnlyKeyValueCommand(keyEncoded, valueEncoded, (BiConsumer) f, params, encodingClasses);
      InvocationContext ctx = getInvocationContext(true, 1);
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> evalMany(Map<? extends K, ? extends V> entries, BiConsumer<V, WriteEntryView<V>> f) {
      log.tracef("Invoked evalMany(entries=%s, %s)", entries, params);
      Map encodedEntries = new HashMap<>();
      entries.entrySet().forEach(e -> {
         Object keyEncoded = cacheEncoders.keyToStorage(e.getKey());
         Object valueEncoded = cacheEncoders.valueToStorage(e.getValue());
         encodedEntries.put(keyEncoded, valueEncoded);
      });

      WriteOnlyManyEntriesCommand cmd = fmap.commandsFactory.buildWriteOnlyManyEntriesCommand(encodedEntries, f, params, encodingClasses);
      InvocationContext ctx = getInvocationContext(true, entries.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> evalMany(Set<? extends K> keys, Consumer<WriteEntryView<V>> f) {
      log.tracef("Invoked evalMany(keys=%s, %s)", keys, params);
      Set encodedKeys = keys.stream().map(k -> cacheEncoders.keyToStorage(k)).collect(Collectors.toSet());
      WriteOnlyManyCommand cmd = fmap.commandsFactory.buildWriteOnlyManyCommand(encodedKeys, f, params, encodingClasses);
      InvocationContext ctx = getInvocationContext(true, keys.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> evalAll(Consumer<WriteEntryView<V>> f) {
      log.tracef("Invoked evalAll(%s)", params);
      // TODO: during commmand execution the set is iterated multiple times, and can execute remote operations
      // therefore we should rather have separate command (or different semantics for keys == null)
      Set<K> keys = new HashSet<>(fmap.cache.keySet());
      Set<Object> encodedKeys = keys.stream().map(k -> cacheEncoders.keyToStorage(k)).collect(Collectors.toSet());
      WriteOnlyManyCommand cmd = fmap.commandsFactory.buildWriteOnlyManyCommand(encodedKeys, f, params, encodingClasses);
      InvocationContext ctx = getInvocationContext(true, encodedKeys.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public CompletableFuture<Void> truncate() {
      log.tracef("Invoked truncate(%s)", params);
      return fmap.cache.clearAsync();
   }

   @Override
   public WriteOnlyMap<K, V> withParams(Param<?>... ps) {
      if (ps == null || ps.length == 0)
         return this;

      if (params.containsAll(ps))
         return this; // We already have all specified params

      return create(params.addAll(ps), fmap);
   }

   @Override
   public WriteListeners<K, V> listeners() {
      return fmap.notifier;
   }

}
