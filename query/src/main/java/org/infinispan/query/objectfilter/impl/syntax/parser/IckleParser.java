package org.infinispan.query.objectfilter.impl.syntax.parser;

import org.infinispan.query.objectfilter.impl.ql.QueryParser;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class IckleParser {

   private static final QueryParser queryParser = new QueryParser();

   private IckleParser() {
   }

   public static <TypeMetadata> IckleParsingResult<TypeMetadata> parse(String queryString, ObjectPropertyHelper<TypeMetadata> propertyHelper) {
      return null;
   }
}
