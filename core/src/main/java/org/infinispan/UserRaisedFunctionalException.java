package org.infinispan;

public class UserRaisedFunctionalException extends RuntimeException {

   public UserRaisedFunctionalException(Throwable cause) {
      super(cause);
   }

   public UserRaisedFunctionalException(String msg) {
      super(msg);
   }

   public UserRaisedFunctionalException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
