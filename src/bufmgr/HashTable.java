package bufmgr;

import java.io.IOException;
import java.util.LinkedList;

import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
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
    
    /* This is the main API function. Pagenumber goes IN
       and a Pair containing Pagenumber, FrameNumber 
       comes OUT */
    public Pair HashThis(PageId pageNumber) 
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
