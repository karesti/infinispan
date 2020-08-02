package org.infinispan.integrationtests.caching.embedded;

import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;

@EnableCaching
@CacheConfig(cacheNames = "default")
public class CachingApp {

      @Cacheable
      public Person findById(int id) {
         return new Person(id, "Margarita");
      }
}
