package org.infinispan.multimap.impl;

import static java.lang.String.format;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.infinispan.multimap.impl.MultimapTestUtils.assertMultimapCacheSize;
import static org.infinispan.multimap.impl.MultimapTestUtils.putValuesOnMultimapCache;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.EncodingUtils;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.MultimapCache;
import org.infinispan.multimap.api.MultimapCacheManager;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistributedMultimapCacheTest")
public class DistributedMultimapCacheTest extends BaseDistFunctionalTest<String, Collection<User>> {

   protected Map<Address, MultimapCache<String, User>> multimapCacheCluster = new HashMap<>();

   protected boolean fromOwner;

   public DistributedMultimapCacheTest fromOwner(boolean fromOwner) {
      this.fromOwner = fromOwner;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "fromOwner");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), fromOwner ? Boolean.TRUE : Boolean.FALSE);
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new DistributedMultimapCacheTest().fromOwner(false).cacheMode(CacheMode.DIST_SYNC).transactional(false),
            new DistributedMultimapCacheTest().fromOwner(true).cacheMode(CacheMode.DIST_SYNC).transactional(false),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();

      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         MultimapCacheManager multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cacheManager);
         multimapCacheCluster.put(cacheManager.getAddress(), multimapCacheManager.get(cacheName));
      }
   }

   @Override
   protected void initAndTest() {
      assertMultimapCacheSize(multimapCacheCluster, 0);
      putValuesOnMultimapCache(getMultimapCacheMember(), NAMES_KEY, OIHANA);
      assertValuesAndOwnership(NAMES_KEY, OIHANA);
   }

   public void testPut() {
      initAndTest();
      MultimapCache<String, User> multimapCache = getMultimapCacheMember(NAMES_KEY);

      putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);

      assertValuesAndOwnership(NAMES_KEY, JULIEN);
   }

   public void testRemoveKey() {
      initAndTest();
      MultimapCache<String, User> multimapCache = getMultimapCacheMember(NAMES_KEY);

      await(
            multimapCache.remove(NAMES_KEY, OIHANA).thenCompose(r1 -> {
               assertTrue(r1);
               return multimapCache.get(NAMES_KEY).thenAccept(v -> assertTrue(v.isEmpty()));
            })
      );

      assertRemovedOnAllCaches(NAMES_KEY);
   }

   public void testRemoveKeyValue() {
      initAndTest();
      MultimapCache<String, User> multimapCache = getMultimapCacheMember(NAMES_KEY);

      await(multimapCache.remove("unexistingKey", OIHANA).thenAccept(r -> assertFalse(r)));
      assertValuesAndOwnership(NAMES_KEY, OIHANA);

      await(
            multimapCache.remove(NAMES_KEY, OIHANA).thenCompose(r1 -> {
                     assertTrue(r1);
                     return multimapCache.get(NAMES_KEY).thenAccept(v -> assertTrue(v.isEmpty()));
                  }
            )
      );

      assertRemovedOnAllCaches(NAMES_KEY);
   }

   public void testRemoveWithPredicate() {
      MultimapCache<String, User> multimapCache = getMultimapCacheMember();

      await(
            multimapCache.put(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> multimapCache.put(NAMES_KEY, JULIEN))
                  .thenCompose(r2 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v -> assertEquals(2, v.size()))
      );

      assertValuesAndOwnership(NAMES_KEY, OIHANA);
      assertValuesAndOwnership(NAMES_KEY, JULIEN);

      MultimapCache<String, User> multimapCache2 = getMultimapCacheMember(NAMES_KEY);

      await(
            multimapCache2.remove(o -> o.getName().contains("Ka"))
                  .thenCompose(r1 -> multimapCache2.get(NAMES_KEY))
                  .thenAccept(v ->
                        assertEquals(2, v.size())

                  )
      );
      assertValuesAndOwnership(NAMES_KEY, OIHANA);
      assertValuesAndOwnership(NAMES_KEY, JULIEN);

      await(
            multimapCache.remove(o -> o.getName().contains("Ju"))
                  .thenCompose(r1 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v ->
                        assertEquals(1, v.size())
                  )
      );
      assertValuesAndOwnership(NAMES_KEY, OIHANA);
      assertKeyValueNotFoundInAllCaches(NAMES_KEY, JULIEN);


      await(
            multimapCache.remove(o -> o.getName().contains("Oi"))
                  .thenCompose(r1 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v ->
                        assertTrue(v.isEmpty())
                  )
      );

      assertRemovedOnAllCaches(NAMES_KEY);
   }

   public void testGetEmpty() {
      MultimapCache<String, User> multimapCache = getMultimapCacheMember();

      await(
            multimapCache.get(NAMES_KEY)
                  .thenAccept(v -> {
                           assertTrue(v.isEmpty());
                        }
                  )

      );
   }

   public void testGetAndModifyResults() {
      initAndTest();
      MultimapCache<String, User> multimapCache = getMultimapCacheMember(NAMES_KEY);

      User pepe = new User("Pepe", 17);

      await(
            multimapCache.get(NAMES_KEY)
                  .thenAccept(v -> {
                           List<User> modifiedList = new ArrayList<>(v);
                           modifiedList.add(pepe);
                        }
                  )

      );

      assertKeyValueNotFoundInAllCaches(NAMES_KEY, pepe);
   }

   public void testContainsKey() {
      initAndTest();

      multimapCacheCluster.values().forEach(mc -> {
         await(
               mc.containsKey("other")
                     .thenAccept(containsKey -> assertFalse(containsKey))
         );
         await(
               mc.containsKey(NAMES_KEY)
                     .thenAccept(containsKey -> assertTrue(containsKey))
         );
      });
   }

   public void testContainsValue() {
      initAndTest();

      multimapCacheCluster.values().forEach(mc -> {
         await(
               mc.containsValue(RAMON)
                     .thenAccept(containsValue -> assertFalse(containsValue))
         );
         await(
               mc.containsValue(OIHANA)
                     .thenAccept(containsValue -> assertTrue(containsValue))
         );
      });
   }

   public void testContainEntry() {
      initAndTest();

      multimapCacheCluster.values().forEach(mc -> {
         await(
               mc.containsEntry(NAMES_KEY, RAMON)
                     .thenAccept(containsValue -> assertFalse(containsValue))
         );
         await(
               mc.containsEntry(NAMES_KEY, OIHANA)
                     .thenAccept(containsValue -> assertTrue(containsValue))
         );
      });
   }

   public void testSize() {
      String anotherKey = "firstNames";
      MultimapCache<String, User> multimapCache = getMultimapCacheMember(NAMES_KEY);

      await(
            multimapCache.put(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> multimapCache.put(NAMES_KEY, JULIEN))
                  .thenCompose(r2 -> multimapCache.put(anotherKey, OIHANA))
                  .thenCompose(r3 -> multimapCache.put(anotherKey, JULIEN))
                  .thenCompose(r4 -> multimapCache.size())
                  .thenAccept(s -> {
                     assertEquals(4, s.intValue());
                     assertValuesAndOwnership(NAMES_KEY, JULIEN);
                     assertValuesAndOwnership(NAMES_KEY, OIHANA);
                     assertValuesAndOwnership(anotherKey, JULIEN);
                     assertValuesAndOwnership(anotherKey, OIHANA);
                  })
      );
   }

   public void testGetEntry() {
      MultimapCache<String, User> multimapCache = getMultimapCacheMember(NAMES_KEY);

      await(
            multimapCache.getEntry(NAMES_KEY)
                  .thenAccept(maybeEntry -> {
                           assertFalse(NAMES_KEY, maybeEntry.isPresent());
                        }
                  )

      );

      await(
            multimapCache.put(NAMES_KEY, JULIEN)
                  .thenCompose(r3 -> multimapCache.getEntry(NAMES_KEY))
                  .thenAccept(maybeEntry -> {
                           assertTrue(NAMES_KEY, maybeEntry.isPresent());
                        }
                  )
      );
   }

   protected MultimapCache getMultimapCacheMember() {
      return multimapCacheCluster.values().stream().findFirst().orElseThrow(() -> new IllegalStateException("Cluster is empty"));
   }

   protected MultimapCache getMultimapCacheMember(String key) {
      Cache<String, Collection<User>> cache = fromOwner ? getFirstOwner(key) : getFirstNonOwner(key);
      return multimapCacheCluster.get(cache.getCacheManager().getAddress());
   }

   protected MultimapCache getMultimapCacheFirstOwner(String key) {
      Cache<String, Collection<User>> cache = getFirstOwner(key);
      return multimapCacheCluster.get(cache.getCacheManager().getAddress());
   }

   protected void assertValuesAndOwnership(String key, User value) {
      assertOwnershipAndNonOwnership(key, l1CacheEnabled);
      assertOnAllCaches(key, value);
   }

   protected void assertKeyValueNotFoundInAllCaches(String key, User value) {
      for (Map.Entry<Address, MultimapCache<String, User>> entry : multimapCacheCluster.entrySet()) {
         await(entry.getValue().get(key).thenAccept(v -> {
                  assertNotNull(format("values on the key %s must be not null", key), v);
                  assertFalse(format("values on the key '%s' must not contain '%s' on node '%s'", key, value, entry.getKey()), v.contains(value));
               })

         );
      }
   }

   protected void assertKeyValueFoundInOwners(String key, User value) {
      Cache<String, Collection<User>> firstOwner = getFirstOwner(key);
      Cache<String, Collection<User>> secondNonOwner = getSecondNonOwner(key);

      MultimapCache<String, User> mcFirstOwner = multimapCacheCluster.get(firstOwner.getCacheManager().getAddress());
      MultimapCache<String, User> mcSecondOwner = multimapCacheCluster.get(secondNonOwner.getCacheManager().getAddress());


      await(mcFirstOwner.get(key).thenAccept(v -> {
               assertTrue(format("firstOwner '%s' must contain key '%s' value '%s' pair", firstOwner.getCacheManager().getAddress(), key, value), v.contains(value));
            })
      );

      await(mcSecondOwner.get(key).thenAccept(v -> {
               assertTrue(format("secondOwner '%s' must contain key '%s' value '%s' pair", secondNonOwner.getCacheManager().getAddress(), key, value), v.contains(value));
            })
      );
   }

   @Override
   protected void assertOwnershipAndNonOwnership(Object key, boolean allowL1) {
      for (Cache cache : caches) {
         Wrapper keyWrapper = cache.getAdvancedCache().getKeyWrapper();
         Encoder keyEncoder = cache.getAdvancedCache().getKeyEncoder();
         Object keyToBeChecked = keyEncoder != null && keyWrapper != null ? EncodingUtils.toStorage(key, keyEncoder, keyWrapper) : key;
         DataContainer dc = cache.getAdvancedCache().getDataContainer();
         InternalCacheEntry ice = dc.get(keyToBeChecked);
         if (isOwner(cache, keyToBeChecked)) {
            assertNotNull(ice);
            assertTrue(ice instanceof ImmortalCacheEntry);
         } else {
            if (allowL1) {
               assertTrue("ice is null or L1Entry", ice == null || ice.isL1Entry());
            } else {
               // Segments no longer owned are invalidated asynchronously
               eventuallyEquals("Fail on non-owner cache " + addressOf(cache) + ": dc.get(" + key + ")",
                     null, () -> dc.get(keyToBeChecked));
            }
         }
      }
   }

   protected void assertOnAllCaches(Object key, User value) {
      for (Map.Entry<Address, MultimapCache<String, User>> entry : multimapCacheCluster.entrySet()) {
         await(entry.getValue().get((String) key).thenAccept(v -> {
                  assertNotNull(format("values on the key %s must be not null", key), v);
                  assertTrue(format("values on the key '%s' must contain '%s' on node '%s'", key, value, entry.getKey()), v.contains(value));
               })

         );
      }
   }
}
