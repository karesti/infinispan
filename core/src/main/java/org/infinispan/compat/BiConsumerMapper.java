package org.infinispan.compat;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.function.SerializableConsumer;

public class BiConsumerMapper<K, V, R> implements SerializableConsumer<R> {

   private final BiConsumer<K, V> action;
   private transient TypeConverter converter;

   public BiConsumerMapper(BiConsumer<K, V> action) {
      this.action = action;
   }

   @Inject
   public void injectDependencies(TypeConverter converter) {
      this.converter = converter;
   }

   @Override
   public void accept(Object o) {
      if (o instanceof Map.Entry) {
         Map.Entry<K, V> entry = (Map.Entry) o;
         action.accept((K) converter.unboxKey(entry.getKey()), (V) converter.unboxValue(entry.getValue()));
      }
   }

   public static class Externalizer implements AdvancedExternalizer<BiConsumerMapper> {

      @Override
      public Set<Class<? extends BiConsumerMapper>> getTypeClasses() {
         return Collections.singleton(BiConsumerMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.BI_CONSUMER_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, BiConsumerMapper object) throws IOException {
         output.writeObject(object.action);
      }

      @Override
      public BiConsumerMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new BiConsumerMapper((BiConsumer) input.readObject());
      }
   }
}
