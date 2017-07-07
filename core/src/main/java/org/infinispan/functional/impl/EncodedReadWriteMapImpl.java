package org.infinispan.functional.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.cache.impl.CacheEncoders;
import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.cache.impl.EncodingClasses;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.Experimental;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.Listeners.ReadWriteListeners;
import org.infinispan.functional.Param;
import org.infinispan.functional.Traversable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Read-write map implementation with encoding
 *
 * @since 9.2
 */
@Experimental
public final class EncodedReadWriteMapImpl<K, V> extends AbstractFunctionalMap<K, V> implements ReadWriteMap<K, V> {
   private static final Log log = LogFactory.getLog(EncodedReadWriteMapImpl.class);

   private EncodedReadWriteMapImpl(Params params, FunctionalMapImpl<K, V> functionalMap) {
      super(params, functionalMap);
   }

   private EncodingClasses encodingClasses;
   private CacheEncoders cacheEncoders;

   public static <K, V> ReadWriteMap<K, V> create(FunctionalMapImpl<K, V> functionalMap) {
      return create(Params.from(functionalMap.params.params), functionalMap);
   }

   static <K, V> ReadWriteMap<K, V> create(Params params, FunctionalMapImpl<K, V> functionalMap) {
      if (!functionalMap.isEncoded()) {
         throw new IllegalStateException("Cache is not encoded");
      }
      EncodedReadWriteMapImpl readWriteMap = new EncodedReadWriteMapImpl<>(params, functionalMap);
      EncoderCache encoderCache = (EncoderCache) functionalMap.cache;

      Encoder keyEncoder = encoderCache.getKeyEncoder();
      Wrapper keyWrapper = encoderCache.getKeyWrapper();
      Encoder valueEncoder = encoderCache.getValueEncoder();
      Wrapper valueWrapper = encoderCache.getValueWrapper();
      readWriteMap.cacheEncoders = new CacheEncoders(keyEncoder, keyWrapper, valueEncoder, valueWrapper);
      readWriteMap.encodingClasses = encoderCache.getEncodingClasses();

      return readWriteMap;
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, Function<ReadWriteEntryView<K, V>, R> f) {
      Object keyEncoded = cacheEncoders.keyToStorage(key);

      log.tracef("Invoked eval(k=%s, %s)", key, params);

      ReadWriteKeyCommand cmd = fmap.commandsFactory.buildReadWriteKeyCommand(keyEncoded, (Function) f, params, encodingClasses);

      InvocationContext ctx = getInvocationContext(true, 1);
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }

      return invokeAsync(ctx, cmd);
   }

   @Override
   public <R> CompletableFuture<R> eval(K key, V value, BiFunction<V, ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked eval(k=%s, v=%s, %s)", key, value, params);
      Object keyEncoded = cacheEncoders.keyToStorage(key);
      Object valueEncoded = cacheEncoders.valueToStorage(value);
      ReadWriteKeyValueCommand cmd = fmap.commandsFactory.buildReadWriteKeyValueCommand(keyEncoded, valueEncoded, (BiFunction) f, params, encodingClasses);
      InvocationContext ctx = getInvocationContext(true, 1);
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return invokeAsync(ctx, cmd);
   }

   @Override
   public <R> Traversable<R> evalMany(Map<? extends K, ? extends V> entries, BiFunction<V, ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked evalMany(entries=%s, %s)", entries, params);
      Map encodedEntries = new HashMap<>();
      entries.entrySet().forEach(e -> {
         Object keyEncoded = cacheEncoders.keyToStorage(e.getKey());
         Object valueEncoded = cacheEncoders.valueToStorage(e.getValue());
         encodedEntries.put(keyEncoded, valueEncoded);
      });
      ReadWriteManyEntriesCommand cmd = fmap.commandsFactory.buildReadWriteManyEntriesCommand(encodedEntries, f, params, encodingClasses);
      InvocationContext ctx = getInvocationContext(true, entries.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return Traversables.of(((List<R>) invokeAsync(ctx, cmd).join()).stream());
   }

   @Override
   public <R> Traversable<R> evalMany(Set<? extends K> keys, Function<ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked evalMany(keys=%s, %s)", keys, params);
      Set<Object> encodedKeys = keys.stream().map(k -> cacheEncoders.keyToStorage(k)).collect(Collectors.toSet());
      ReadWriteManyCommand cmd = fmap.commandsFactory.buildReadWriteManyCommand(encodedKeys, (Function) f, params, encodingClasses);
      InvocationContext ctx = getInvocationContext(true, keys.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return Traversables.of(((List<R>) invokeAsync(ctx, cmd).join()).stream());
   }

   @Override
   public <R> Traversable<R> evalAll(Function<ReadWriteEntryView<K, V>, R> f) {
      log.tracef("Invoked evalAll(%s)", params);
      // TODO: during commmand execution the set is iterated multiple times, and can execute remote operations
      // therefore we should rather have separate command (or different semantics for keys == null)
      Set<K> keys = new HashSet<>(fmap.cache.keySet());
      Set<Object> encodedKeys = keys.stream().map(k -> cacheEncoders.keyToStorage(k)).collect(Collectors.toSet());
      ReadWriteManyCommand cmd = fmap.commandsFactory.buildReadWriteManyCommand(encodedKeys, (Function) f, params, encodingClasses);
      InvocationContext ctx = getInvocationContext(true, encodedKeys.size());
      if (ctx.getLockOwner() == null) {
         ctx.setLockOwner(cmd.getKeyLockOwner());
      }
      return Traversables.of(((List<R>) invokeAsync(ctx, cmd).join()).stream());
   }

   @Override
   public ReadWriteListeners<K, V> listeners() {
      return fmap.notifier;
   }

   @Override
   public ReadWriteMap<K, V> withParams(Param<?>... ps) {
      if (ps == null || ps.length == 0)
         return this;

      if (params.containsAll(ps))
         return this; // We already have all specified params

      return create(params.addAll(ps), fmap);
   }

}
