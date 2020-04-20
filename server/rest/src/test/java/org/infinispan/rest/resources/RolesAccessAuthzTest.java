package org.infinispan.rest.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.resources.security.AuthClient;
import org.infinispan.security.Security;
import org.testng.annotations.Test;

import java.io.File;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.stream.Collectors;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "rest.RolesAccessAuthzTest")
public class RolesAccessAuthzTest extends AbstractRestResourceTest {

   private static final String PERSISTENT_LOCATION = tmpDirectory(CacheV2ResourceTest.class.getName());
   private ObjectMapper mapper = new ObjectMapper();
   private JsonWriter jsonWriter = new JsonWriter();

   @Override
   protected boolean isSecurityEnabled() {
      return true;
   }

   @Override
   protected void createCacheManagers() throws Exception {
      Util.recursiveFileRemove(PERSISTENT_LOCATION);
      super.createCacheManagers();
   }

   @Override
   protected GlobalConfigurationBuilder getGlobalConfigForNode(int id) {
      GlobalConfigurationBuilder config = super.getGlobalConfigForNode(id);
      config.globalState().enable().configurationStorage(ConfigurationStorage.OVERLAY)
            .persistentLocation(PERSISTENT_LOCATION + File.separator + id);
      return config;
   }

   @Test
   public void createAndRead() throws Exception {
      AuthClient authClient = (AuthClient) client;

      ConfigurationBuilder templateConfigBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      templateConfigBuilder.security().authorization().enable().security().authorization()
            .roles("user", "admin", "customer");

      Security.doAs(ADMIN_USER, (PrivilegedAction<Cache>) () -> getCacheManagers().get(0).administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache("default", templateConfigBuilder.build()));

      String baseURL = String.format("http://localhost:%d/rest/v2/caches/", restServer().getPort());
      String url = baseURL + "user_cache";

      ContentResponse response = authClient.newRequest(url + "?template=default").method(HttpMethod.POST)
            .header("Content-type", APPLICATION_JSON_TYPE).send();

      ResponseAssertion.assertThat(response).isOk();

      ContentResponse sizeResponse = authClient.newRequest(url + "?action=size")
            .header(HttpHeader.AUTHORIZATION, "Basic " + AuthClient.createCredentials("customer", "customer"))
            .send();
      ResponseAssertion.assertThat(sizeResponse).isForbidden();

      assertVisibility("user", "user_cache", true);
      assertVisibility("customer", "user_cache", false);
   }

   private void assertVisibility(String user, String cacheName, boolean visible) throws Exception {
      String accept = "text/plain; q=0.9, application/json; q=0.6";
      String url = String.format("http://localhost:%d/rest/v2/cache-managers/default/caches", restServer().getPort());
      ContentResponse response = client.newRequest(url).header("Accept", accept)
            .header(HttpHeader.AUTHORIZATION, "Basic " + AuthClient.createCredentials(user, user))
            .send();
      ResponseAssertion.assertThat(response).isOk();

      String json = response.getContentAsString();
      JsonNode jsonNode = mapper.readTree(json);
      List<String> names = asText(jsonNode.findValues("name"));
      assertEquals("'" + cacheName + "' cache visibility should be " + visible + " for user '" + user + "'", visible, names.contains(cacheName));
   }

   private List<String> asText(List<JsonNode> values) {
      return values.stream().map(JsonNode::asText).collect(Collectors.toList());
   }

}
