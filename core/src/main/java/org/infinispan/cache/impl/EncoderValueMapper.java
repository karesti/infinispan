package org.infinispan.cache.impl;

import static org.infinispan.commons.dataconversion.EncodingUtils.fromStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.util.function.RemovableFunction;

/**
 * {@link java.util.function.Function} that uses a valueEncoder to converter values from the configured storage format
 * to the requested format.
 *
 * @since 9.1
 */
public class EncoderValueMapper<V> implements RemovableFunction<V, V> {

   private final EncodingClasses encodingClasses;
   private transient Encoder valueEncoder;
   private transient Wrapper valueWrapper;

   public EncoderValueMapper(EncodingClasses encodingClasses) {
      this.encodingClasses = encodingClasses;
   }

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry) {
      this.valueEncoder = encoderRegistry.getEncoder(encodingClasses.getValueEncoderClass());
      this.valueWrapper = encoderRegistry.getWrapper(encodingClasses.getValueWrapperClass());
   }

   @Override
   @SuppressWarnings("unchecked")
   public V apply(V v) {
      return (V) fromStorage(v, valueEncoder, valueWrapper);
   }

   public static class Externalizer implements AdvancedExternalizer<EncoderValueMapper> {

      @Override
      public Set<Class<? extends EncoderValueMapper>> getTypeClasses() {
         return Collections.singleton(EncoderValueMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.ENCODER_VALUE_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, EncoderValueMapper object) throws IOException {
         output.writeObject(object.encodingClasses);
      }

      @Override
      @SuppressWarnings("unchecked")
      public EncoderValueMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new EncoderValueMapper((EncodingClasses) input.readObject());
      }
   }
}
