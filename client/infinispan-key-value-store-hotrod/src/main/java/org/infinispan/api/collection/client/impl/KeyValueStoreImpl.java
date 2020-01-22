package org.infinispan.api.collection.client.impl;

import java.util.Map;

import org.infinispan.api.client.listener.ClientKeyValueStoreListener;
import org.infinispan.api.collection.KeyValueEntry;
import org.infinispan.api.collection.KeyValueStore;
import org.infinispan.api.collection.client.impl.listener.ClientListenerImpl;
import org.infinispan.api.collection.listener.KeyValueStoreListener;
import org.infinispan.api.collection.query.QueryRequest;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;


/**
 * Implements the {@link KeyValueStore} interface
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public class KeyValueStoreImpl<K, V> implements KeyValueStore<K, V> {
   protected final RemoteCache<K, V> cache;
   protected RemoteCache<K, V> cacheReturnValues;

   public KeyValueStoreImpl(RemoteCache<K, V> cache, RemoteCache<K, V> cacheReturnValues) {
      this.cache = cache;
      this.cacheReturnValues = cacheReturnValues;
   }

   @Override
   public Uni<V> get(K key) {
      return Uni.createFrom().completionStage(cache.getAsync(key));
   }

   @Override
   public Uni<Boolean> insert(K key, V value) {
      return Uni.createFrom().completionStage(cacheReturnValues.putIfAbsentAsync(key, value).thenApply(v -> v == null));
   }

   @Override
   public Uni<Void> save(K key, V value) {
      // We don't return the value here
      return Uni.createFrom().completionStage(cache.putAsync(key, value).thenApply(v -> null));
   }

   @Override
   public Uni<Void> delete(K key) {
      return Uni.createFrom().completionStage(cache.removeAsync(key).thenApply(v -> null));
   }

   @Override
   public Multi<K> keys() {
      return Multi.createFrom().iterable(cache.keySet());
   }

   @Override
   public Multi<? extends Map.Entry<K, V>> entries() {
      return Multi.createFrom().iterable(cache.entrySet());
   }

   @Override
   public Uni<Long> estimateSize() {
      return Uni.createFrom().completionStage(cache.sizeAsync());
   }

   @Override
   public Uni<Void> clear() {
      return Uni.createFrom().completionStage(cache.clearAsync());
   }

   @Override
   public Multi<KeyValueEntry<K, V>> find(String ickleQuery) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(ickleQuery);
      return new QueryPublisherImpl(query, cache.getRemoteCacheManager().getAsyncExecutorService()).toMulti();
   }

   @Override
   public Multi<KeyValueEntry<K, V>> find(QueryRequest queryRequest) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(queryRequest.getIckleQuery());
      query.setParameters(queryRequest.getParams());
      query.startOffset(queryRequest.skip());
      query.maxResults(queryRequest.limit());
      return new QueryPublisherImpl(query, cache.getRemoteCacheManager().getAsyncExecutorService()).toMulti();
   }

   @Override
   public Multi<KeyValueEntry<K, V>> findContinuously(String ickleQuery) {
      ContinuousQuery<K, V> continuousQuery = Search.getContinuousQuery(cache);
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(ickleQuery);
      return new ContinuousQueryPublisherImpl(query, continuousQuery, true, true, true).toMulti();
   }

   @Override
   public <T> Multi<KeyValueEntry<K, T>> findContinuously(QueryRequest queryRequest) {
      ContinuousQuery<K, V> continuousQuery = Search.getContinuousQuery(cache);
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query query = queryFactory.create(queryRequest.getIckleQuery())
            .setParameters(queryRequest.getParams());
      query.startOffset(queryRequest.skip());
      return new ContinuousQueryPublisherImpl(query, continuousQuery, queryRequest.isCreated(), queryRequest.isUpdated(), queryRequest.isDeleted()).toMulti();
   }

   @Override
   public Multi<KeyValueEntry<K, V>> listen(KeyValueStoreListener listener) {
      // TODO CHECK CAST. Now there is a single class that implements
      return new ClientListenerImpl(cache, (ClientKeyValueStoreListener) listener).toMulti();
   }
}
