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
    	Pair page;
    	// Look for the given pageId in this bucket
    	int i = linkedList.indexOf(aPageId);
    	if(i != -1) {
           page = linkedList.get(i);
           return page;
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
    
    public Pair(Integer pageNumber, Integer frameNumber) {
        this.pageNumber = new PageId(pageNumber);
        this.frameNumber = frameNumber;
    }
    
    public Pair() {
    	// Create an empty Pair by default
    	pageNumber.pid = -1;
    	frameNumber = -1;
    }
    
    public Pair(Pair o) {
    	// Copy constructor
    	pageNumber = new PageId(o.pageNumber.pid);
    	frameNumber = o.frameNumber;
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
        
    public Boolean isPairEmpty() {
    	return (pageNumber.pid == -1 && frameNumber == -1);
    }
    
    // Overriding equals() to compare two LIRS_Pair objects
    public boolean equals(Object o) {
 
        // If the object is compared with itself then return true  
        if (o == this) {
            return true;
        }
 
        /* Check if o is an instance of LIRS_Pair or not
          "null instanceof [type]" also returns false */
        if (!(o instanceof Pair)) {
            return false;
        }
         
        // typecast o to Complex so that we can compare data members 
        Pair c = (Pair) o;
         
        // Compare the relevant data members and return accordingly 
        return c.pageNumber == this.pageNumber &&
        	   c.frameNumber == this.frameNumber;
    }
}
