package org.infinispan.lock;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockConfiguration;
import org.infinispan.lock.api.ClusteredLockManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SnowCampTest extends BaseClusteredLockTest {

   protected static final String LOCK_NAME = "SnowCamp";

   @BeforeMethod(alwaysRun = true)
   public void createLock() throws Throwable {
      ClusteredLockManager m1 = clusteredLockManager(0);
      m1.defineLock(LOCK_NAME, new ClusteredLockConfiguration());
   }

   @AfterMethod(alwaysRun = true)
   protected void destroyLock() {
      ClusteredLockManager clusteredLockManager = clusteredLockManager(0);
      await(clusteredLockManager.remove(LOCK_NAME));
   }

   @Test
   public void test_tryLock(){
      StringBuilder message = new StringBuilder();

      ClusteredLock lock0 = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);


      await(lock0.tryLock().whenComplete((result, ex) -> {
         if (result) {
            message.append("0");
         }
      }));

      await(lock1.tryLock().whenComplete((result, ex) -> {
         if (result) {
            message.append("1");
         }
      }));
      assertEquals("0", message.toString());
   }

   @Test
   public void test_unlock(){
      StringBuilder message = new StringBuilder();

      ClusteredLock lock0 = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);


      await(lock0.tryLock().whenComplete((result, ex) -> {
         if (result) {
            message.append("0");
         }
      }));

      await(lock1.unlock());

      await(lock0.tryLock().whenComplete((result, ex) -> {
         if (result) {
            message.append("1");
         }
      }));

      await(lock1.tryLock().whenComplete((result, ex) -> {
         if (result) {
            message.append("2");
         }
      }));

      await(lock0.unlock());

      await(lock1.tryLock().whenComplete((result, ex) -> {
         if (result) {
            message.append("3");
         }
      }));

      assertEquals("03", message.toString());
   }


   @Test
   public void test_tryLock_with_timeout(){
      StringBuilder builder = new StringBuilder();

      ClusteredLock lock0 = clusteredLockManager(0).get(LOCK_NAME);
      ClusteredLock lock1 = clusteredLockManager(1).get(LOCK_NAME);

      CompletableFuture<Void> lockRes0 = lock0.tryLock(1000, TimeUnit.MILLISECONDS).thenAccept(r -> {
         if (r) {
            builder.append("0");
            await(lock0.unlock());
         }
      });

      CompletableFuture<Void> lockRes1 = lock1.tryLock(1000, TimeUnit.MILLISECONDS).thenAccept(r -> {
         if (r) {
            builder.append("1");
            await(lock1.unlock());
         }
      });

      await(lockRes0);
      await(lockRes1);

      assertTrue(builder.toString().contains("0"));
      assertTrue(builder.toString().contains("1"));
   }
}
