package bufmgr;

import java.util.LinkedList;

import global.PageId;

public class HashTable {

	private Bucket [] directory;
	
    public HashTable(int tableSize) {
    	if(tableSize < 0) {
    		throw new IllegalArgumentException("Table size must be positive.");
    	}
    	// Allocate the table which is initially all null bucket references
    	directory = new Bucket[tableSize];
    }
    
    // This is the main API function. Pagenumber goes IN
    // and FrameNumber comes OUT
    public int HashThis(PageId pageNumber) {
    	return 0;
    }
}

class Bucket {
	 /* Linked List Declaration */
    private LinkedList<Pair> linkedlist;
    
    Pair getFirst() {
    	// Placeholder
		return null;    	
    }
    
    Pair getNext() {
    	// Placeholder
		return null;    	
    }
    
    Boolean search(Pair aPair) {
    	//Placeholder
    	return false;
    }
}

class Pair {
    private PageId pageNumber;
    private Integer frameNumber;

    public Pair(PageId pageNumber, Integer frameNumber) {
        this.pageNumber = pageNumber;
        this.frameNumber = frameNumber;
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
}