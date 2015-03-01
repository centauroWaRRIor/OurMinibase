package bufmgr;

import chainexception.ChainException;

public class LIRSFailureException extends ChainException{

   public LIRSFailureException(Exception e, String name)  
   { 
	  super(e, name); 
   }		
}