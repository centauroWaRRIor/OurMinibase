package bufmgr;

import global.PageId;
import java.util.LinkedList;

public class LIRS {

	/* Linked List Declaration */
    private LinkedList<LIRS_Stats> unpinnedList;
    private LinkedList<LIRS_Stats> pinnedList;
    private Integer globalCount; // Gets incremented every time a page gets accessed
	
	public LIRS(Integer numbufs) {
		
	   unpinnedList = new LinkedList<LIRS_Stats>();
	   pinnedList = new LinkedList<LIRS_Stats>();
	   globalCount = 0;
       // Add all the pages initially to the free pages list
       for(int i = 0; i < numbufs; i++) {
    	   LIRS_Stats tmpEntry = new LIRS_Stats(new Pair(-1, i));
    	   unpinnedList.add(tmpEntry);
       }
	}
	
	/* Returns a Pair type so that it can be easily 
	 * plugged into the hash table delete method 
	 */
	public Pair getReplacementCandidate(PageId targetPage)
			throws LIRSFailureException {
		LIRS_Stats victim = null;
		Integer oldpid;
		int candidateIndex;
		// Look under unpinned list only
		if(!unpinnedList.isEmpty()) {
			
		   candidateIndex = computeStats(); // Updates Stats for all candidates being tracked by LIRS
		   victim = unpinnedList.remove(candidateIndex);
		   oldpid = victim.getCandidateInfo().getPageId().pid;
		   // Track this entry under the pinned list now using target pid
		   victim.getCandidateInfo().setPageId(targetPage.pid);
		   // Reset stats for this new target pid
		   victim.reset(); // Could have relied on constructor but this way is more explicit
		   LIRS_Stats tmpLIRSEntry = new LIRS_Stats(victim);
		   pinnedList.add(tmpLIRSEntry);
		   // Return the old pid so that the pair entry can be
		   // removed from the hash table
		   victim.getCandidateInfo().setPageId(oldpid);
		   return new Pair(victim.getCandidateInfo());  
		}
		else { 
			throw new LIRSFailureException(null, "LIRS Failed to pick a replacement candidate");
		}
	}
	
	public void deleteFreeListEntry (Pair entry) 
			throws LIRSFailureException {	
		Boolean status = false;
		LIRS_Stats tmpTransferEntry = new LIRS_Stats(entry);
		int index = unpinnedList.indexOf(tmpTransferEntry);
		if(index != -1) {
			tmpTransferEntry = unpinnedList.remove(index);	
			// Track this page under different list
			status = pinnedList.add(tmpTransferEntry);
		}
		if(!status)
			throw new LIRSFailureException(null, "LIRS Failed to delete from free list");
	}
	
	public void insertFreeListEntry (Pair entry)
			throws LIRSFailureException {
		Boolean status = false;
		LIRS_Stats tmpTransferEntry = new LIRS_Stats(entry);
		int index = pinnedList.indexOf(tmpTransferEntry);
		if(index != -1) {
			tmpTransferEntry = pinnedList.remove(index);	
			// Track this page under different list
			status = unpinnedList.add(tmpTransferEntry);
		}
		if(!status)
			throw new LIRSFailureException(null, "LIRS Failed to delete from free list");
		
	}
	
	public void updatePageAccessStats(Pair entry) 
			throws LIRSFailureException {
    	LIRS_Stats lirsEntry = new LIRS_Stats(entry);
    	// Look for the given page
    	int i = pinnedList.indexOf(lirsEntry);
    	if(i != -1) {
    	   lirsEntry = pinnedList.get(i);
    	   lirsEntry.updateRecency();
    	   lirsEntry.updateLastAccessed(globalCount);
           incGlobalCount();
        }
    	else 
    		throw new LIRSFailureException(null, "LIR Policy failed to update stats");
	}
	
	public Integer getFreeListSize() {
		//return freeList.size();
		return unpinnedList.size();
	}
	
	/* Compute LIRS statistics for all candidates currently
	 * in the unpinned list and return the index for the resulting candidate
	 */
	private Integer computeStats() {
    	LIRS_Stats entry = null;
    	LIRS_Stats victim = null;
    	Integer candidateIndex = -1;
    	// Walk up the list
    	for(int i = 0; i < unpinnedList.size(); i++) {
    		entry = unpinnedList.get(i);
    		entry.computeWeight(globalCount); 
    		if(i == 0) {
    			victim = entry;
    			candidateIndex = 0;
    		}
    		else {
    			if(entry.getWeight() > victim.getWeight()) {
    				victim = entry;
    				candidateIndex = i;
    			}
    		}
    	}
    	return candidateIndex;
	}
	
	private void incGlobalCount() {
		if(globalCount == Integer.MAX_VALUE)
			globalCount = 0;
		globalCount++;	
	}
	
	private class LIRS_Stats {
		
	    Pair candidateInfo; // Stores page number and buffer's pool frame index
	    Integer reuseDistance;
	    Integer recency;
	    Integer weight;
	    Integer lastAccessed; // Value of globalCount last time 
	                            // this page was accessed.
	    
	    public LIRS_Stats(Pair candidateInfo) {
	    	reset();
	    	this.candidateInfo = new Pair(candidateInfo);
	    }
	    
	    // Copy constructor
	    public LIRS_Stats(LIRS_Stats other) {
	    	this.candidateInfo = new Pair(other.getCandidateInfo());
	    	this.reuseDistance = other.reuseDistance;
	    	this.recency = other.recency;
	    	this.weight = other.weight;
	    	this.lastAccessed = other.lastAccessed;
	    }
	    
	    public void reset() {
	    	reuseDistance = Integer.MAX_VALUE;
	    	recency = Integer.MAX_VALUE;
	    	weight = 0;
	    	lastAccessed = 0;    	
	    }
	    
	    public Pair getCandidateInfo() {
	    	return candidateInfo;
	    }
	    
	    public Integer getWeight() {
	    	return weight;
	    }
	    
	    public void updateRecency() {
	    	recency = 0;
	    }
	    
	    public void updateLastAccessed(Integer value) {
	    	lastAccessed = value;
	    }
	    
	    public void computeWeight(Integer globalCount) {
	    	
	    	recency += computeDeltaCount(globalCount);
	    	reuseDistance = computeDeltaCount(globalCount);
	    	weight = Math.max(recency, reuseDistance);
	    }
	    
	    public Integer computeDeltaCount(Integer globalCount) {
	    	return (globalCount - lastAccessed);// TODO: Account for wrapping
	    }
	    
	    // Overriding equals() to compare two LIRS_Pair objects
	    public boolean equals(Object o) {
	 
	        // If the object is compared with itself then return true  
	        if (o == this) {
	            return true;
	        }
	 
	        /* Check if o is an instance of LIRS_Pair or not
	          "null instanceof [type]" also returns false */
	        if (!(o instanceof LIRS_Stats)) {
	               return false;
	        }
	         
	        // typecast o to LIRS_Pair so that we can compare data members 
	        LIRS_Stats c = (LIRS_Stats) o;
	         
	        if(c.candidateInfo != null)
	           // Compare the relevant data members and return accordingly 
	           return c.candidateInfo.equals(this.candidateInfo); 
	        else
	        	return false;
	    }
	}
}

