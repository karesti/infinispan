package org.infinispan.compat;

import static org.infinispan.commons.dataconversion.EncodingUtils.fromStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;

import org.infinispan.cache.impl.EncodingClasses;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;

public class BiConsumerMapper implements BiConsumer {

   private EncodingClasses encodingClasses;

   private Encoder keyEncoder;
   private Encoder valueEncoder;
   private Wrapper keyWrapper;
   private Wrapper valueWrapper;
   private final BiConsumer biConsumer;

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry) {
      this.keyEncoder = encoderRegistry.getEncoder(encodingClasses.getKeyEncoderClass());
      this.valueEncoder = encoderRegistry.getEncoder(encodingClasses.getValueEncoderClass());
      this.keyWrapper = encoderRegistry.getWrapper(encodingClasses.getKeyWrapperClass());
      this.valueWrapper = encoderRegistry.getWrapper(encodingClasses.getValueWrapperClass());
   }

   public BiConsumerMapper(BiConsumer biConsumer,
                           EncodingClasses encodingClasses) {
      this.biConsumer = biConsumer;
      this.encodingClasses = encodingClasses;
   }

   @Override
   public void accept(Object k, Object v) {
      Object key = fromStorage(k, keyEncoder, keyWrapper);
      Object value = fromStorage(v, valueEncoder, valueWrapper);
      biConsumer.accept(key, value);
   }

   public static class Externalizer implements AdvancedExternalizer<BiConsumerMapper> {

      @Override
      public Set<Class<? extends BiConsumerMapper>> getTypeClasses() {
         return Collections.singleton(BiConsumerMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.BI_FUNCTION_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, BiConsumerMapper object) throws IOException {
         output.writeObject(object.biConsumer);
         output.writeObject(object.encodingClasses);
      }

      @Override
      public BiConsumerMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new BiConsumerMapper((BiConsumer) input.readObject(),
               (EncodingClasses) input.readObject());
      }
   }
}
