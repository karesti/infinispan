package org.infinispan.api.store.client.impl;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.infinispan.api.store.SinglePublisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class AsyncSinglePublisher<T> implements SinglePublisher<T> {

   final Supplier<CompletableFuture<T>> completionStageSupplier;

   public AsyncSinglePublisher(Supplier<CompletableFuture<T>> completionStageSupplier) {
      this.completionStageSupplier = completionStageSupplier;
   }

   @Override
   public void subscribe(Subscriber<? super T> subscriber) {
      subscriber.onSubscribe(new AsyncSingleSubscription<>(completionStageSupplier, subscriber));
   }

   final static class AsyncSingleSubscription<T> extends AtomicBoolean implements Subscription {

      final Supplier<CompletableFuture<T>> completionStageSupplier;
      final Subscriber<? super T> subscriber;

      CompletableFuture<T> future;
      volatile boolean cancelled;

      AsyncSingleSubscription(Supplier<CompletableFuture<T>> completionStageSupplier, Subscriber<? super T> subscriber) {
         this.completionStageSupplier = completionStageSupplier;
         this.subscriber = subscriber;
      }

      @Override
      public void request(long n) {
         if (cancelled) {
            return;
         }

         if (n <= 0) {
            cancel();
            subscriber.onError(new IllegalArgumentException("non positive request size"));
         }

         if (!get() && compareAndSet(false, true)) {
            try {
               future = Objects.requireNonNull(completionStageSupplier.get());
               future.whenComplete((t, throwable) -> {
                  if (throwable != null) {
                     if (cancelled) {
                        return;
                     }

                     subscriber.onError(throwable);
                     return;
                  }

                  if (t != null) {
                     if (cancelled) {
                        return;
                     }

                     subscriber.onNext(t);
                  }

                  if (cancelled) {
                     return;
                  }

                  subscriber.onComplete();
               });
            } catch (Throwable t) {
               if (cancelled) {
                  return;
               }

               subscriber.onError(t);
            }
         }
      }

      @Override
      public void cancel() {
         cancelled = true;

         if (get() && future != null) {
            future.cancel(true);
         }
      }
   }

}
