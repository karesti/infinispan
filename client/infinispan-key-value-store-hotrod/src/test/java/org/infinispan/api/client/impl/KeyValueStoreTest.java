package org.infinispan.api.client.impl;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.AbstractMap;

import org.infinispan.api.Infinispan;
import org.infinispan.api.collection.KeyValueStore;
import org.infinispan.api.collection.KeyValueStoreConfig;
import org.infinispan.api.collection.WriteResult;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.functional.FunctionalTestUtils;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.reactivestreams.Publisher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.Flowable;
import io.reactivex.subscribers.TestSubscriber;

@Test(groups = "functional", testName = "org.infinispan.api.client.impl.KeyyValueStoreSimpleTest")
public class KeyValueStoreTest extends SingleHotRodServerTest {

   public static final String CACHE_NAME = "test";
   private Infinispan infinispan;

   private KeyValueStore<Integer, String> store;

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      cacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(CACHE_NAME, new org.infinispan.configuration.cache.ConfigurationBuilder().build());
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      infinispan = new InfinispanClientImpl(remoteCacheManager);
      store = FunctionalTestUtils.await(infinispan.getKeyValueStore(CACHE_NAME, KeyValueStoreConfig.defaultConfig()));
   }

   @Override
   protected void teardown() {
      await(infinispan.stop());
      super.teardown();
   }

   @BeforeMethod
   public void clearStoreBeforeEachTest() {
      KeyValueStore<Integer, String> store = FunctionalTestUtils.await(infinispan.getKeyValueStore(CACHE_NAME, KeyValueStoreConfig.defaultConfig()));
      store.clear().await();
   }

   public void testGetNoValue() {
      assertNull(store.get(0).await().indefinitely());
   }

   public void testCreate() {
      Boolean writeResult = store.insert(1, "hi").await().indefinitely();
      assertTrue(writeResult);
      assertEquals("hi", store.get(1).await().indefinitely());
      Boolean writeResult2 = store.insert(1, "hi").await().indefinitely();
      assertFalse(writeResult2);
   }

   public void testEstimateSizeEmptyStore() {
      long estimatedSize = store.estimateSize().await().indefinitely();

      assertEquals(0, estimatedSize);
   }

   public void testEstimateSizeWithData() {
      for (int i = 0; i < 100; i++) {
         store.save(i, "" + i).await().indefinitely();
      }

      long estimatedSize = store.estimateSize().await().indefinitely();
      assertEquals(100, estimatedSize);
   }

   public void testDeleteNotExisting() {
      store.delete(0).await().indefinitely();
   }

   public void testDeleteExisting() {
      store.save(0, "hola").await().indefinitely();
      store.delete(0).await().indefinitely();
      String getRemovedValue = store.get(0).await().indefinitely();
      assertNull(getRemovedValue);
   }

   public void testKeys() {
      for (int i = 0; i < 100; i++) {
         store.save(i, "" + i).await().indefinitely();
      }

      TestSubscriber subscriber = new TestSubscriber();
      store.keys().subscribe(subscriber);

      subscriber.awaitCount(100);

      assertEquals(100, subscriber.valueCount());
   }

   public void testEntries() {
      for (int i = 0; i < 100; i++) {
         store.save(i, "" + i).await().indefinitely();
      }

      TestSubscriber subscriber = new TestSubscriber();
      store.entries().subscribe(subscriber);

      subscriber.awaitCount(100);

      assertEquals(100, subscriber.valueCount());
   }
}
