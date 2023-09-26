package org.infinispan.rest.resources;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.mappers.ClusterRoleMapper;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @since 15.0
 */
@Test(groups = "functional", testName = "rest.AccessManagementResourceTest")
public class AccessManagementResourceTest extends AbstractRestResourceTest {

   protected void addSecurity(GlobalConfigurationBuilder globalBuilder) {
      ClusterRoleMapper roleMapper = spy(ClusterRoleMapper.class);
      doReturn(Set.of("admin")).when(roleMapper).listPrincipals("admin");
      doReturn(Set.of("user1", "user2")).when(roleMapper).listPrincipals("user");
      globalBuilder.security().authorization().enable().groupOnlyMapping(false).principalRoleMapper(roleMapper)
            .role("ADMIN").permission(AuthorizationPermission.ALL)
            .role("USER").permission(AuthorizationPermission.WRITE, AuthorizationPermission.READ, AuthorizationPermission.EXEC, AuthorizationPermission.BULK_READ);
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new AccessManagementResourceTest().withSecurity(true).protocol(HTTP_11).ssl(false).browser(false),
            new AccessManagementResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(false),
      };
   }

   @Test
   public void testRolesList() {
      CompletionStage<RestResponse> response = adminClient.security().listRoles();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      Json jsonNode = Json.read(join(response).getBody());
      assertThat(jsonNode.isArray()).isTrue();
      assertThat(jsonNode.asList()).containsExactlyInAnyOrder("ADMIN", "USER");

      response = adminClient.security().listRoles(false);
      jsonNode = Json.read(join(response).getBody());
      assertThat(jsonNode.isArray()).isTrue();
      assertThat(jsonNode.asList()).containsExactlyInAnyOrder("ADMIN", "USER");
   }

   @Test
   public void testDescribeRole() {
      CompletionStage<RestResponse> response = adminClient.security().describeRole("ADMIN");
      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(join(response).getBody());
      assertThat(jsonNode.at("name").asString()).isEqualTo("ADMIN");
      assertThat(jsonNode.at("permissions").asList()).containsExactly("ALL");
   }

   @Test
   public void testDetailedRolesList() {
      CompletionStage<RestResponse> response = adminClient.security().listRoles(true);
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      Json jsonNode = Json.read(join(response).getBody());
      assertThat(jsonNode.asJsonMap()).hasSize(2);
      assertThat(jsonNode.at("ADMIN").at("permissions").asList()).containsExactly("ALL");
      assertThat(jsonNode.at("ADMIN").at("inheritable").asBoolean()).isTrue();
      assertThat(jsonNode.at("USER").at("permissions").asList())
            .containsExactlyInAnyOrder("READ", "WRITE", "BULK_READ", "EXEC");
      assertThat(jsonNode.at("USER").at("inheritable").asBoolean()).isTrue();
   }

   @Test
   public void testPrincipalListByRole() {
      CompletionStage<RestResponse> response = adminClient.security().listPrincipals("admin");
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      Json jsonNode = Json.read(join(response).getBody());
      assertThat(jsonNode.asList()).containsExactly("admin");
      response = adminClient.security().listPrincipals("user");
      jsonNode = Json.read(join(response).getBody());
      assertThat(jsonNode.asList()).containsExactlyInAnyOrder("user1", "user2");
   }
}
