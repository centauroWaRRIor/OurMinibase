package bufmgr;

import global.PageId;
import java.util.LinkedList;

public class LIRS {

	/* Linked List Declaration */
    private LinkedList<Pair> freeList; // LIRS looks here first
    private LinkedList<LIRS_Pair> candidateList; // LIRS looks here second
    private Integer globalCount; // Gets incremented every time a page gets accessed
	private LIRS_Pair victim;
	
	public LIRS() {
		
	   freeList = new LinkedList<Pair>();
	   candidateList = new LinkedList<LIRS_Pair>();
	   globalCount = 0;
	   victim = null;
	}
	
	/* We need to return a Pair object so that we can
	 * plug that object into HashTable.delete() in turn
	 */
	public Pair getReplacementCandidate(PageId targetPage) {
		Pair returnCandidate = null;
		// Look under free list first
		if(!freeList.isEmpty()) {
			returnCandidate = freeList.removeFirst();
			returnCandidate.setPageId(targetPage.pid);
			LIRS_Pair tmpLIRSEntry = new LIRS_Pair(returnCandidate);
			// Start tracking this page with the LIRS algorithm
			candidateList.add(tmpLIRSEntry);
		}
		else { // Now look for a candidate using LIRS
		   computeStats(); // Updates Stats for all candidates being tracked by LIRS
		   returnCandidate = victim.getCandidateInfo();
		   victim.reset(); // reset statistics for this victim
		}
		return new Pair(returnCandidate);
	}
	
	public void insertFreeListEntry (Pair entry) {
		// Remove from list of candidates being tracked by LIRS
		LIRS_Pair tempEntry = new LIRS_Pair(entry);
		// TODO: Add check for failure to remove
	    candidateList.remove(tempEntry);
	    // Now add to list of free pages
		freeList.add(entry);
	}
	
	public void updatePageAccessStats(Pair entry) {
    	LIRS_Pair candidate = new LIRS_Pair(entry);
    	// Look for the given page
    	int i = candidateList.indexOf(candidate);
    	if(i != -1) {
    	   candidate = candidateList.get(i);
           candidate.updateRecency();
           candidate.updateLastAccessed(globalCount);
           incGlobalCount();
        }
	}
	
	public Integer getFreeListSize() {
		return freeList.size();
	}
	
	/* Compute LIRS statistics for all candidates currently
	 * in the list
	 */
	private void computeStats() {
    	LIRS_Pair candidate;
    	// Walk up the list
    	for(int i = 0; i < candidateList.size(); i++) {
    		candidate = candidateList.get(i);
    		candidate.computeWeight(globalCount); 
    		if(i == 0)
    			victim = candidate;
    		else {
    			if(candidate.getWeight() > victim.getWeight())
    				victim = candidate;
    		}
    	}
	}
	
	private void incGlobalCount() {
		globalCount++; // TODO: Add wrapping around when spilled	
	}
	
	private class LIRS_Pair {
		
	    Pair candidateInfo; // Stores page number and buffer's pool frame index
	    Integer reuseDistance;
	    Integer recency;
	    Integer weight;
	    Integer lastAccessed; // Value of globalCount last time 
	                            // this page was accessed.
	    
	    public LIRS_Pair(Pair candidateInfo) {
	    	reset();
	    	this.candidateInfo = new Pair(candidateInfo);
	    }
	    
	    public void reset() {
	    	candidateInfo = null;
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
	    	weight = Integer.max(recency, reuseDistance);
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
	        if (!(o instanceof LIRS_Pair)) {
	               return false;
	        }
	         
	        // typecast o to LIRS_Pair so that we can compare data members 
	        LIRS_Pair c = (LIRS_Pair) o;
	         
	        if(c.candidateInfo != null)
	           // Compare the relevant data members and return accordingly 
	           return c.candidateInfo.equals(this.candidateInfo); 
	        else
	        	return false;
	    }
	}
}

