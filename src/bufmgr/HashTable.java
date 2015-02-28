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
    	// Create the directory
    	directory = new Bucket[HTSIZE];
    	// Create the directory entries
    	for (int i = 0; i < directory.length; i++)
    		directory[i]= new Bucket();    	
    }
    
    /* Pagenumber goes IN for hash table lookup
       and a Pair containing Pagenumber, FrameNumber 
       comes OUT if found. Otherwise throws exception */
    public Pair hashKey(PageId pageNumber) 
        throws HashEntryNotFoundException {
 	   	
    	Pair result; // Empty Pair by default
    	Bucket directoryEntry;
		//System.out.println ("PageID to be looked up: " + pageNumber.pid);
		//System.out.println ("Hash table returns h(" + pageNumber.pid + ")= " + h(pageNumber.pid));
    	//System.out.println ("Size of hash table: " + HTSIZE);
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
    	directoryEntry = directory[h(aPair.getPageId().pid)];
    	directoryEntry.insert(aPair);	    
    }
    
    public void deleteEntry(Pair aPair) 
    		throws HashEntryNotFoundException {
    	Bucket directoryEntry;
    	directoryEntry = directory[h(aPair.getPageId().pid)];
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
    
    private class Bucket {
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
}