package org.infinispan.functional;

import static org.infinispan.test.Exceptions.assertException;
import static org.infinispan.test.Exceptions.assertExceptionNonStrict;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.concurrent.CompletionException;

import javax.transaction.RollbackException;
import javax.transaction.xa.XAException;

import org.infinispan.Cache;
import org.infinispan.cache.impl.CacheEncoders;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.EncodingUtils;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.remoting.RemoteException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "functional")
public class FunctionalTxOffHeapTest extends FunctionalTxInMemoryTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new FunctionalTxOffHeapTest().transactional(true).lockingMode(LockingMode.OPTIMISTIC).isolationLevel(IsolationLevel.READ_COMMITTED),
//            new FunctionalTxOffHeapTest().transactional(true).lockingMode(LockingMode.PESSIMISTIC).isolationLevel(IsolationLevel.READ_COMMITTED),
//            new FunctionalTxOffHeapTest().transactional(true).lockingMode(LockingMode.PESSIMISTIC).isolationLevel(IsolationLevel.REPEATABLE_READ),
      };
   }

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      transactional = true;
      builder.memory().storageType(StorageType.OFF_HEAP);
      super.configureCache(builder);
   }

   @Override
   protected boolean cacheContainsKey(Object key, Cache<Object, Object> cache) {
      Encoder keyEncoder = cache.getAdvancedCache().getKeyEncoder();
      Wrapper keyWrapper = cache.getAdvancedCache().getKeyWrapper();
      return super.cacheContainsKey(EncodingUtils.toStorage(key, keyEncoder, keyWrapper), cache);
   }
}
