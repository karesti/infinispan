package org.infinispan.rest.resources.secured;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestTestSCI;
import org.infinispan.rest.resources.CacheManagerResourceTest;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import static org.infinispan.security.AuthorizationPermission.ALL;
import static org.infinispan.security.AuthorizationPermission.EXEC;
import static org.infinispan.security.AuthorizationPermission.READ;
import static org.infinispan.security.AuthorizationPermission.WRITE;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

@Test(groups = "functional", testName = "rest.SecuredCacheManagerResourceTest")
public class SecuredCacheManagerResourceTest extends CacheManagerResourceTest {

   protected GlobalConfigurationBuilder getGlobalConfigForNode(int id) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      globalBuilder.cacheContainer().statistics(true);
      globalBuilder.serialization().addContextInitializer(RestTestSCI.INSTANCE);
      globalBuilder.security()
            .authorization().enable().principalRoleMapper(new IdentityRoleMapper())
            .role("admin").permission(ALL)
            .role("supervisor").permission(READ, WRITE, EXEC)
            .role("reader").permission(READ);
      return globalBuilder.clusteredDefault().cacheManagerName("default");
   }

   @Override
   public ConfigurationBuilder getDefaultCacheBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.security().authorization()
                           .enable()
                              .role("admin")
                              .role("supervisor")
                              .role("reader");
      return builder;
   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(GlobalConfigurationBuilder globalBuilder,
                                                                ConfigurationBuilder builder, TransportFlags flags) {
      EmbeddedCacheManager cm = createClusteredCacheManager(false, globalBuilder, builder, flags);
      amendCacheManagerBeforeStart(cm);
      cacheManagers.add(cm);
      cm.withSubject(Security.getSubject()).start();
      return cm;
   }
}
