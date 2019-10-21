package org.infinispan.rest.resources;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.rest.framework.Method.GET;

import java.util.concurrent.CompletionStage;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;

/**
 * Handler for the cluster resource.
 *
 * @since 10.0
 */
public class ClusterResource implements ResourceHandler {

   private final InvocationHelper invocationHelper;

   public ClusterResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            // Config
            .invocation().methods(GET).path("/v2/cluster").handleWith(this::getCluster)
            .create();
   }

   private CompletionStage<RestResponse> getCluster(RestRequest request) {
      invocationHelper.getServer().getCacheManager("fefault").getGlobalComponentRegistry().getNamedComponentRegistry("").getInternalDataContainer().
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      return completedFuture(responseBuilder.build());

   }
}
