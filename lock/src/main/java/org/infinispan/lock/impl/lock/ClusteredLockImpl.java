package org.infinispan.lock.impl.lock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.exception.ClusteredLockException;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.functions.LockFunction;
import org.infinispan.lock.impl.functions.UnlockFunction;
import org.infinispan.lock.impl.log.Log;
import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.infinispan.remoting.RemoteException;

public class ClusteredLockImpl implements ClusteredLock {
   private static final Log log = LogFactory.getLog(ClusteredLockImpl.class, Log.class);

   private final ClusteredLockKey lockKey;
   private final EmbeddedClusteredLockManager clusteredLockManager;
   private final Object originator;

   public ClusteredLockImpl(ClusteredLockKey lockKey,
                            AdvancedCache<ClusteredLockKey, ClusteredLockValue> clusteredLockCache,
                            EmbeddedClusteredLockManager clusteredLockManager) {
      this.clusteredLockManager = clusteredLockManager;
      this.lockKey = lockKey;
      this.originator = clusteredLockCache.getCacheManager().getAddress();
   }

   @Override
   public CompletableFuture<Boolean> tryLock() {
      return null;
   }

   @Override
   public CompletableFuture<Void> unlock() {
      return null;
   }

   @Override
   public CompletableFuture<Boolean> tryLock(long time, TimeUnit unit) {
      return null;
   }


   @Override
   public CompletableFuture<Void> lock() {
      return null;
   }

   @Override
   public CompletableFuture<Boolean> isLocked() {
      return null;
   }

   @Override
   public CompletableFuture<Boolean> isLockedByMe() {
      return null;
   }

   private String createRequestId() {
      return Util.threadLocalRandomUUID().toString();
   }

   private Throwable handleException(Throwable ex) {
      Throwable lockException = ex;
      if (ex instanceof RemoteException) {
         lockException = ex.getCause();
      }
      if (!(lockException instanceof ClusteredLockException)) {
         lockException = new ClusteredLockException(ex);
      }
      return lockException;
   }

   public String getName() {
      return lockKey.getName().toString();
   }

   public Object getOriginator() {
      return originator;
   }
}
