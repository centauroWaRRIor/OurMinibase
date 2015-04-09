package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
  
  /* Underlying iterator, be it FileScanner, KeyScan, another selection
   * etc.
   */
  private Iterator iterator;
  /* Remember the list of predicates logically connected by OR operators. 
   */
  private Predicate predicates[];
  
  /* Has the latest tuple produced being consumed?. 
   */
  private Boolean isNextTupleConsumed;
  
  /* Store latest tupe being produced. 
   */
  private Tuple nextTuple;
 
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
    /* Initialize references */
	iterator = iter;
	predicates = preds;
	isNextTupleConsumed = true;
	setSchema(iterator.getSchema());
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	super.indent(depth);
	System.out.println("Selects tuples if they meet any of the predicates\n" );
    /* tbd - check if this is the right usage */
    iterator.explain(depth);

  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
     iterator.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
     return iterator.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
     iterator.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
     if(!isNextTupleConsumed)
	    return true;
     else {
        // Produce next tuple if there is one
	    return produceNextTuple();
     }
  }
	 
   /**
    * Produces next tuple.
    */
   private boolean produceNextTuple() {
	   
	   Tuple t;
	   while (iterator.hasNext()) {
		   
          t = iterator.getNext();
			
		  for( int i = 0; i < predicates.length; i++ ) {
	   	     /* Iterate through predicates until finding
	   	      * the first one that evaluates to TRUE. This
	   	      * short cuircuit logic follows from the fact
	   	      * that predicates are logically connected by
	   	      * OR operators.
	   	      * 
	   	      * Ideally we'd check for operator validity before
	   	      * evaluating the predicate but A) its not required
	   	      * for this project and B) validate() method has a 
	   	      * bug.
	   	      */
	   	     //if(predicates[i].validate(getSchema()))
	   	     {
	   		    try {
	   			   if(predicates[i].evaluate(t)) {
	   				  nextTuple = t;
	   				  isNextTupleConsumed = false;
	   			      return true;
	   			   }
	   				   
	   		    } catch (IllegalStateException e2) {
	   		       throw new IllegalStateException("Error evaluating predicate");
	   		    }
	   	     }
	   	     //else {
	   	     //   throw new IllegalStateException("Projection failed due to invalid predicate");
	   	     //}
	      }
	   }
	   return false;
   }

/**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(!isNextTupleConsumed) {
    	isNextTupleConsumed = true;
    	return nextTuple;
    }
    else {
    	if(hasNext()) { // hasNext will produce next tuple and put it in nextTuple
    	
        	isNextTupleConsumed = true;
        	return nextTuple;    		
    	}
    	else
    		throw new IllegalStateException("Selection operator has no more tuples to select");
    }
  }

} // public class Selection extends Iterator
