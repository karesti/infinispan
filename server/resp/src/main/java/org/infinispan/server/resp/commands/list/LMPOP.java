package org.infinispan.server.resp.commands.list;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.list.internal.POP;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @link https://redis.io/commands/lmpop/
 * Pops one or more elements from the first non-empty list key from the list of provided key names.
 *
 * See BLMPOP for the blocking variant of this command.
 *
 * Elements are popped from either the left or right of the first non-empty list based on the
 * passed argument. The number of returned elements is limited to the lower between the non-empty
 * list's length, and the count argument (which defaults to 1).
 * @since 15.0
 */
public class LMPOP extends RespCommand implements Resp3Command {

   public LMPOP() {
      super(-4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      return null;
   }
}
