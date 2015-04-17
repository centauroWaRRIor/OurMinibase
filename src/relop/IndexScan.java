package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;

import global.RID;
import index.BucketScan;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

  private HashIndex hashIndex;
  private BucketScan bucketScan;
  private HeapFile heapFile;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) throws Exception {
	setSchema(schema); // schema is protected in the parent class so need to use this setter
	hashIndex = index;
	heapFile = file;
	try {
		bucketScan = hashIndex.openScan();
	}
	catch (Exception e) {
		throw new Exception("*** Error opening scan");
	}    
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	super.indent(depth);
	System.out.println("Scans all the buckets of a hash index and produces tuples as output");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
     close();
 	 bucketScan = hashIndex.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
     return !(bucketScan == null);
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	if( bucketScan != null ) bucketScan.close();
	bucketScan = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
     return bucketScan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  byte[] recordData;
	  RID recordId;
	  try {
         recordId = bucketScan.getNext();
         recordData = heapFile.selectRecord(recordId);
         return new Tuple(getSchema(), recordData);
	  } catch (IllegalStateException exc) {
	     throw new IllegalStateException("No more tuples");
      }
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
     return bucketScan.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
    return bucketScan.getNextHash();
  }

} // public class IndexScan extends Iterator
