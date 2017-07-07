package org.infinispan.functional.impl;

import org.infinispan.functional.FunctionalMap;

/**
 * Created by katiaaresti on 29/06/17.
 */
public class MapFactory {

   public static <K, V> FunctionalMap.ReadOnlyMap<K, V> readOnlyMap(FunctionalMapImpl<K, V> functionalMap) {
      if (functionalMap.isEncoded()) {
         return EncodedReadOnlyMapImpl.create(functionalMap);
      } else {
         return ReadOnlyMapImpl.create(functionalMap);
      }
   }

   public static <K, V> FunctionalMap.WriteOnlyMap<K, V> writeOnlyMap(FunctionalMapImpl<K, V> functionalMap) {
      if (functionalMap.isEncoded()) {
         return EncodedWriteOnlyMapImpl.create(functionalMap);
      } else {
         return WriteOnlyMapImpl.create(functionalMap);
      }
   }

   public static <K, V> FunctionalMap.ReadWriteMap<K, V> readWriteMap(FunctionalMapImpl<K, V> functionalMap) {
      if (functionalMap.isEncoded()) {
         return EncodedReadWriteMapImpl.create(functionalMap);
      } else {
         return ReadWriteMapImpl.create(functionalMap);
      }
   }
}
