package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 *
 * It is not complicated, but we need to solve the issue of mapping both
 * the schema and data from the original to the projected one.
 *
 * We create the schema at the beginning and set it in our superclass.
 * We remember the mapping since we need to use it to convert for every Tuple
 * 
 * An arguably more efficient way is to actually copy the byte data but the
 * solution here is a lot simpler and easier to read
 *  
 */
public class Projection extends Iterator {

  private Iterator iter;

    /* we need the mapping to convert the Tuple */
  private Integer mapping[];
	
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    /* save the iterator */
    this.iter = iter;
    this.mapping = fields;
    
    /* Create a new scheme with the fields given */
    Schema temp_schema = new Schema(fields.length);
    for( int i = 0; i < fields.length; i++ ) {
        temp_schema.initField(i, iter.getSchema(), fields[i] );
    }

    /* save this to our iterator */
    setSchema(temp_schema);
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	System.out.println("Projects given fields from the supplied iterator\n" );
	super.indent(depth);

    /* tbd - check if this is the right usage */
    iter.explain(depth);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    iter.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	try {  
	    Tuple t = iter.getNext();
	    Tuple tnew = new Tuple(getSchema());
		
        for( int i = 0; i < mapping.length; i++ ) {
           tnew.setField(i, t.getField(mapping[i]) );
        }
       return tnew;
	} catch (IllegalStateException e) {
	   throw new IllegalStateException("There are no more tuples to project");
	}
  }

} // public class Projection extends Iterator
