package org.infinispan.lock.impl.lock;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import org.infinispan.lock.impl.entries.ClusteredLockState;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.functions.LockFunction;
import org.infinispan.lock.impl.functions.UnlockFunction;
import org.infinispan.lock.impl.log.Log;
import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.remoting.RemoteException;

public class ClusteredLockImpl implements ClusteredLock {
   private static final Log log = LogFactory.getLog(ClusteredLockImpl.class, Log.class);

   private final ClusteredLockKey lockKey;
   private final EmbeddedClusteredLockManager clusteredLockManager;
   private final Object originator;
   private final FunctionalMap.ReadWriteMap<ClusteredLockKey, ClusteredLockValue> readWriteMap;
   private final Queue<TryLockRequest> pendingRequest;

   public ClusteredLockImpl(ClusteredLockKey lockKey,
                            AdvancedCache<ClusteredLockKey, ClusteredLockValue> clusteredLockCache,
                            EmbeddedClusteredLockManager clusteredLockManager) {
      this.clusteredLockManager = clusteredLockManager;
      this.lockKey = lockKey;
      this.originator = clusteredLockCache.getCacheManager().getAddress();
      clusteredLockCache.addListener(new TryLockReleaseListener(), new ClusteredLockFilter(lockKey));
      readWriteMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(clusteredLockCache));
      pendingRequest = new ConcurrentLinkedQueue<>();
   }

   @Override
   public CompletableFuture<Boolean> tryLock() {
      return readWriteMap.eval(lockKey, new LockFunction(createRequestId(), originator));
   }

   @Override
   public CompletableFuture<Void> unlock() {
      return readWriteMap.eval(lockKey, new UnlockFunction(originator));
   }

   @Override
   public CompletableFuture<Boolean> tryLock(long time, TimeUnit unit) {
      CompletableFuture<Boolean> result = new CompletableFuture<>();
      TryLockRequest tryLockRequest = new TryLockRequest(originator, result, time, unit);
      pendingRequest.offer(tryLockRequest);
      readWriteMap.eval(lockKey, new LockFunction(tryLockRequest.requestId, tryLockRequest.requestor)).whenComplete((r, ex) ->
            tryLockRequest.handleLockResult(r, ex)
      );
      return result;
   }

   private void tryLock(TryLockRequest tryLockRequest) {
      if(tryLockRequest == null || tryLockRequest.request.isDone()) return;
      pendingRequest.offer(tryLockRequest);
      readWriteMap.eval(lockKey, new LockFunction(tryLockRequest.requestId, tryLockRequest.requestor)).whenComplete((r, ex) ->
            tryLockRequest.handleLockResult(r, ex)
      );
   }

   @Listener(clustered = true)
   private class TryLockReleaseListener {

      @CacheEntryModified
      public void entryModified(CacheEntryModifiedEvent event) {
         ClusteredLockValue value = (ClusteredLockValue) event.getValue();

         if(value.getState() == ClusteredLockState.RELEASED) {
            TryLockRequest next = null;

            while (!pendingRequest.isEmpty() && (next == null || !next.request.isDone()))
               next = pendingRequest.poll();

            TryLockRequest finalNext = next;
            clusteredLockManager.execute(() -> tryLock(finalNext));

         }
      }
   }


   private class TryLockRequest {
      final CompletableFuture<Boolean> request;
      final String requestId;
      final Object requestor;
      final long time;
      final TimeUnit unit;
      boolean isScheduled = false;

      public TryLockRequest(Object requestor, CompletableFuture<Boolean> request, long time, TimeUnit unit) {
         this.request = request;
         this.requestId = createRequestId();
         this.requestor = requestor;
         this.time = time;
         this.unit = unit;
      }

      public void handleLockResult(Boolean result, Throwable ex) {
         if (ex != null) {
            log.trace("Exception on lock request " + this, ex);
            request.completeExceptionally(ex);
            return;
         }

         if (result == null) {
            log.trace("Result is null on request " + this);
            request.completeExceptionally(new ClusteredLockException("Lock result is null, something is wrong"));
            return;
         }

        if(result) {
           request.complete(true);
        } else if (!isScheduled) {
           isScheduled = true;
           clusteredLockManager.schedule(() -> request.complete(false), time, unit);
        }
      }
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
