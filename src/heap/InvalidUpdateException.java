package heap;

import chainexception.ChainException;

public class InvalidUpdateException extends ChainException {

   public InvalidUpdateException(Exception e, String name)  
   {
      super(e, name); 
   }
}
