package org.infinispan.functional;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalEncoderTest")
public class FunctionalEncoderTest extends FunctionalMapTest {
   StorageType storageType;

   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      builder.memory().storageType(storageType);
      super.configureCache(builder);
   }

   public Object[] factory() {
      return new Object[]{
            new FunctionalEncoderTest().storageType(StorageType.OFF_HEAP),
            new FunctionalEncoderTest().storageType(StorageType.BINARY),
            new FunctionalEncoderTest().storageType(StorageType.OBJECT),
      };
   }

   FunctionalEncoderTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }
}
