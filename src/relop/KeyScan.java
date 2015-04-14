package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

  /* A KeyScan is a HashScan that returns tuples instead of RIDs. The HashScan
   * is provided by the HashIndex which in turn is built around a given SearchKey. 
   * The HashScan uses this same SearchKey to scan the index.
   * Hence, we need to store a HashIndex reference in order to access
   * the HashScan and a schema reference in order to produce the tuples.
   * But the schema is already stored in the iterator parent. We also 
   * need to store a HeapFile reference in order to retrieve the record
   * once the RID has been obtained via the HashScan.
   */
  private HashIndex hashIndex;
  private HashScan hashScan;
  private SearchKey searchKey; // I may not need to store the key after all.
  private HeapFile heapFile;
	
  /**
   * Constructs an index scan, given the hash index and schema.
 * @throws Exception 
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) throws Exception {
	setSchema(schema); // schema is protected in the parent class so need to use this setter
	searchKey = key;
	hashIndex = index;
	heapFile = file;
	try {
		hashScan = hashIndex.openScan(searchKey);
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
	/* A FileScan is a leaf node so there is no need to call
	 * explain on the children because there are no children.
	 */
	super.indent(depth);
	System.out.println("Scans a hash index using a given search key and produces tuples as output");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
     close();
 	 hashScan = hashIndex.openScan(searchKey);
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return !(hashScan == null);
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	if( hashScan != null ) hashScan.close();
	hashScan = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
     return hashScan.hasNext();
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
         recordId = hashScan.getNext();
         recordData = heapFile.selectRecord(recordId);
         return new Tuple(getSchema(), recordData);
	  } catch (IllegalStateException exc) {
	     throw new IllegalStateException("No more tuples");
      }
  }

} // public class KeyScan extends Iterator
