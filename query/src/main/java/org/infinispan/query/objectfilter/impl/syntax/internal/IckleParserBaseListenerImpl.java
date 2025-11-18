package org.infinispan.query.objectfilter.impl.syntax.internal;

import org.infinispan.query.objectfilter.impl.ql.parse.IckleParser;
import org.infinispan.query.objectfilter.impl.ql.parse.IckleParserBaseListener;

public class IckleParserBaseListenerImpl extends IckleParserBaseListener {

   @Override
   public void enterWhereClause(IckleParser.WhereClauseContext ctx) {
      super.enterWhereClause(ctx);
   }
}
