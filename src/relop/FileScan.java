package relop;

import global.RID;
import heap.HeapFile;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
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

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    throw new UnsupportedOperationException("Not implemented");
  }

} // public class FileScan extends Iterator
