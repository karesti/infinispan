package org.infinispan.cache.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;

/**
 * Created by katiaaresti on 29/06/17.
 */
public class EncodingClasses implements Serializable {

   private final Class<? extends Encoder> keyEncoderClass;
   private final Class<? extends Encoder> valueEncoderClass;
   private final Class<? extends Wrapper> keyWrapperClass;
   private final Class<? extends Wrapper> valueWrapperClass;

   public EncodingClasses(Class<? extends Encoder> keyEncoderClass,
                          Class<? extends Encoder> valueEncoderClass,
                          Class<? extends Wrapper> keyWrapperClass,
                          Class<? extends Wrapper> valueWrapperClass) {
      this.keyEncoderClass = keyEncoderClass;
      this.valueEncoderClass = valueEncoderClass;
      this.keyWrapperClass = keyWrapperClass;
      this.valueWrapperClass = valueWrapperClass;
   }

   public Class<? extends Encoder> getKeyEncoderClass() {
      return keyEncoderClass;
   }

   public Class<? extends Encoder> getValueEncoderClass() {
      return valueEncoderClass;
   }

   public Class<? extends Wrapper> getKeyWrapperClass() {
      return keyWrapperClass;
   }

   public Class<? extends Wrapper> getValueWrapperClass() {
      return valueWrapperClass;
   }

   public static class Externalizer implements AdvancedExternalizer<EncodingClasses> {

      @Override
      public Set<Class<? extends EncodingClasses>> getTypeClasses() {
         return Collections.singleton(EncodingClasses.class);
      }

      @Override
      public Integer getId() {
         return Ids.ENCODING_CLASSES;
      }

      @Override
      public void writeObject(ObjectOutput output, EncodingClasses object) throws IOException {
         output.writeObject(object.keyEncoderClass);
         output.writeObject(object.valueEncoderClass);
         output.writeObject(object.keyWrapperClass);
         output.writeObject(object.valueWrapperClass);
      }

      @Override
      public EncodingClasses readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new EncodingClasses(
               (Class<? extends Encoder>) input.readObject(),
               (Class<? extends Encoder>) input.readObject(),
               (Class<? extends Wrapper>) input.readObject(),
               (Class<? extends Wrapper>) input.readObject());
      }
   }
}
