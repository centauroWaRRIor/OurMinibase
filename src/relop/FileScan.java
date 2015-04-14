package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  /* A FileScan is a HeapScan that returns tuples instead of bytes[].
   * Hence, we need to store a HeapFile reference in order to access
   * the HeapScan and a schema reference in order to produce the tuples.
   * But the schema is already stored in the iterator parent.
   */
  protected HeapFile heapFile;
  private HeapScan heapScan;
  protected RID currentRid;
	
  /**
   * Constructs a file scan, given the schema and heap file.
 * @throws Exception 
   */
  public FileScan(Schema schema, HeapFile file) throws Exception {
	setSchema(schema); // schema is protected in the parent class so need to use this setter
	currentRid = new RID();
    this.heapFile = file;
	try {
	    heapScan = heapFile.openScan();
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
    System.out.println("Scans a heap file and produces tuples as output");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
     close();
	 heapScan = heapFile.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return !(heapScan == null);
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    if( heapScan != null ) heapScan.close();
    heapScan = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return heapScan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  byte[] recordData;

	  try {
            recordData = heapScan.getNext(currentRid);
            return new Tuple(getSchema(), recordData);
	  } catch (IllegalStateException exc) {
		  throw new IllegalStateException("No more tuples");
	  }
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    return currentRid;
  }

} // public class FileScan extends Iterator
