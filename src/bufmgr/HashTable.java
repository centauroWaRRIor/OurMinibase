package bufmgr;

import java.util.LinkedList;

import global.PageId;

public class HashTable {

	private Bucket [] directory;
	private Integer HTSIZE; 
	
    public HashTable(int tableSize) {
    	if(tableSize < 0) {
    		throw new IllegalArgumentException("Table size must be positive.");
    	}
    	// Allocate the table which is initially all null bucket references
    	HTSIZE = tableSize;
    	directory = new Bucket[HTSIZE];
    }
    
    /* Pagenumber goes IN for hash table lookup
       and a Pair containing Pagenumber, FrameNumber 
       comes OUT if found. Otherwise throws exception */
    public Pair hashKey(PageId pageNumber) 
        throws HashEntryNotFoundException {
 	   	
    	Pair result; // Empty Pair by default
    	Bucket directoryEntry;
    	directoryEntry = directory[h(pageNumber.pid)];
    	result = directoryEntry.search(pageNumber);
    	if(result.isPairEmpty())
    		throw new HashEntryNotFoundException(null,
    				"PageID is not found in the buffer pool");
    	else 
    	   return result;
    }
    
    public void insertEntry(Pair aPair) {
    	Bucket directoryEntry;
    	directoryEntry = directory[h(aPair.getPageNumber().pid)];
    	directoryEntry.insert(aPair);	    
    }
    
    public void deleteEntry(Pair aPair) 
    		throws HashEntryNotFoundException {
    	Bucket directoryEntry;
    	directoryEntry = directory[h(aPair.getPageNumber().pid)];
    	if(!directoryEntry.delete(aPair))
    		throw new HashEntryNotFoundException(null,
    				"PageID is not found in the buffer pool");
    }
    
    // hash function is internal to the class
    private Integer h(Integer value) {
    	final Integer a = 1; // TODO: Figure out a good a value
    	final Integer b = 1; // TODO: Figure out a good b value
    	return ((a*value + b) % HTSIZE);
    }
}

class Bucket {
	 /* Linked List Declaration */
    private LinkedList<Pair> linkedList;
    
    public Bucket() {
    	linkedList = new LinkedList<Pair>();
    }
    
    public Pair search(PageId aPageId) {
    	Pair nextPage;
    	// Look for the given pageId in this bucket
        for (int i = 0; i < linkedList.size(); i++) {
            nextPage = linkedList.get(i); 
            if(nextPage.equals(aPageId))
            	return nextPage;
        }
    	return new Pair(); // Returns empty pair
    }
    
    public void insert(Pair newPair) {
    	// Don't allow duplicates
    	if(!linkedList.contains(newPair))
    	   linkedList.add(newPair);
    }
    
    public Boolean delete(Pair newPair) {
    	return linkedList.remove(newPair);
    }
}

class Pair {
    private PageId pageNumber;
    private Integer frameNumber;

    public Pair(PageId pageNumber, Integer frameNumber) {
        this.pageNumber = pageNumber;
        this.frameNumber = frameNumber;
    }
    
    public Pair() {
    	// Create an empty Pair by default
    	pageNumber.pid = -1;
    	frameNumber = -1;
    }
    
    public void setPageNumber(PageId pageNumber) {
        this.pageNumber = pageNumber;
    }

    public void setFrameNumber(Integer frameNumber) {
        this.frameNumber = frameNumber;
    }

    public PageId getPageNumber() {
        return pageNumber;
    }

    public Integer getFrameNumber() {
        return frameNumber;
    }
    
    public Boolean isEqual(Pair aPair) {
    	return (pageNumber == aPair.pageNumber &&
    			frameNumber == aPair.frameNumber);
    }
    
    public Boolean isPairEmpty() {
    	return (pageNumber.pid == -1 && frameNumber == -1);
    }
}
