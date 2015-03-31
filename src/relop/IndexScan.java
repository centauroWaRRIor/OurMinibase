package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
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
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
    throw new UnsupportedOperationException("Not implemented");
  }

} // public class IndexScan extends Iterator
