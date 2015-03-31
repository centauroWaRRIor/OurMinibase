package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    throw new UnsupportedOperationException("Not implemented");
  }

} // public class Projection extends Iterator
