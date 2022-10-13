package org.infinispan.server.core;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.group.ChannelMatcher;
import org.infinispan.server.core.configuration.MockServerConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.transport.NettyTransport;

public class MockProtocolServer extends AbstractProtocolServer {

   public MockProtocolServer(String protocolName, NettyTransport transport) {
      super(protocolName);
      configuration = new MockServerConfigurationBuilder()
            .defaultCacheName("dummyCache")
            .port(1245)
            .build();
      this.transport = transport;
   }

   public MockProtocolServer() {
      super(null);
   }

   @Override
   public ChannelOutboundHandler getEncoder() {
      return null;
   }

   @Override
   public ChannelInboundHandler getDecoder() {
      return null;
   }

   @Override
   public ChannelMatcher getChannelMatcher() {
      return channel -> true;
   }

   @Override
   public ProtocolServerConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      return null;
   }
}
