package org.infinispan.compat;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;

public class BiFunctionMapper<T, U, R> implements BiFunction<T, U, R> {

   private transient TypeConverter converter;
   private BiFunction bifunction;

   @Inject
   public void injectDependencies(TypeConverter converter) {
      this.converter = converter;
   }

   public BiFunctionMapper() {

   }

   public BiFunctionMapper(BiFunction remappingFunction) {
      this.bifunction = remappingFunction;
   }

   @Override
   public R apply(T t, U u) {
      Object oldVUnboxed = converter.unboxValue(t);
      return (R) bifunction.apply(oldVUnboxed, u);
   }

   public BiFunction getBifunction() {
      return bifunction;
   }

   public static class Externalizer implements AdvancedExternalizer<BiFunctionMapper> {

      @Override
      public Set<Class<? extends BiFunctionMapper>> getTypeClasses() {
         return Collections.singleton(BiFunctionMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.BI_FUNCTION_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, BiFunctionMapper object) throws IOException {
         output.writeObject(object.bifunction);
      }

      @Override
      public BiFunctionMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new BiFunctionMapper((BiFunction) input.readObject());
      }
   }
}
