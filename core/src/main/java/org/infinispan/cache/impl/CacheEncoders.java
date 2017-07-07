package org.infinispan.cache.impl;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.EncodingUtils;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.marshall.core.EncoderRegistry;

/**
 * @since 9.2
 */
public class CacheEncoders {

   private Encoder keyEncoder;
   private Encoder valueEncoder;
   private Wrapper keyWrapper;
   private Wrapper valueWrapper;

   public CacheEncoders() {

   }

   public CacheEncoders(Encoder keyEncoder, Wrapper keyWrapper, Encoder valueEncoder, Wrapper valueWrapper) {
      this.keyEncoder = keyEncoder;
      this.keyWrapper = keyWrapper;
      this.valueEncoder = valueEncoder;
      this.valueWrapper = valueWrapper;
   }

   public void grabEncodersFromRegistry(EncoderRegistry encoderRegistry, EncodingClasses encodingClasses) {
      this.keyEncoder = encoderRegistry.getEncoder(encodingClasses.getKeyEncoderClass());
      this.valueEncoder = encoderRegistry.getEncoder(encodingClasses.getValueEncoderClass());
      this.keyWrapper = encoderRegistry.getWrapper(encodingClasses.getKeyWrapperClass());
      this.valueWrapper = encoderRegistry.getWrapper(encodingClasses.getValueWrapperClass());
   }

   public Encoder getKeyEncoder() {
      return keyEncoder;
   }

   public Encoder getValueEncoder() {
      return valueEncoder;
   }

   public Wrapper getKeyWrapper() {
      return keyWrapper;
   }

   public Wrapper getValueWrapper() {
      return valueWrapper;
   }

   public boolean isKeyEncodingActive() {
      return keyEncoder != null && keyWrapper != null;
   }

   public boolean isValueEncodingActive() {
      return valueEncoder != null && valueWrapper != null;
   }

   public Object keyFromStorage(Object keyFromStorage) {
      if (keyFromStorage == null) return null;
      return isKeyEncodingActive() ? keyEncoder.fromStorage(keyWrapper.unwrap(keyFromStorage)) : keyFromStorage;
   }

   public Object keyToStorage(Object key) {
      return isKeyEncodingActive() && !EncodingUtils.isWrapped(key, keyWrapper) ? keyWrapper.wrap(keyEncoder.toStorage(key)) : key;
   }

   public Object valueFromStorage(Object valueFromStorage) {
      if (valueFromStorage == null) return null;
      return isValueEncodingActive() ? valueEncoder.fromStorage(valueWrapper.unwrap(valueFromStorage)) : valueFromStorage;
   }

   public Object valueToStorage(Object value) {
      return isValueEncodingActive() && !EncodingUtils.isWrapped(value, valueWrapper) ? valueWrapper.wrap(valueEncoder.toStorage(value)) : value;
   }
}
