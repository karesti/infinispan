package org.infinispan.server.resp.commands.list;

import org.infinispan.server.resp.commands.list.internal.POP;

/**
 * @link https://redis.io/commands/rpop/
 *
 * Removes and returns the last elements of the list stored at key.
 *
 * By default, the command pops a single element from the end of the list.
 * When provided with the optional count argument, the reply will consist of up to count elements,
 * depending on the list's length.
 *
 * @since 15.0
 */
public class RPOP extends POP {
   public RPOP() {
      super(false);
   }
}
