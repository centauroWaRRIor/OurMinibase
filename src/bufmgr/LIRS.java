package bufmgr;

import java.util.LinkedList;

public class LIRS {

	/* Linked List Declaration */
    private LinkedList<Pair> freeList; // Looks here first
    private LinkedList<LIRS_Pair> candidateList; // Looks here second
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
	public Pair getReplacementCandidate() {
		Pair returnCandidate = null;
		// Look under free list first
		if(!freeList.isEmpty()) {
			returnCandidate = freeList.getFirst();
		}
		else { // Now look for a candidate using LIRS
		   computeStats();
		   returnCandidate = victim.getCandidateInfo();
		   victim.reset(); // reset statistics for this victim
		}
		return returnCandidate;
	}
	
	public void insertFreeListEntry (Pair entry) {
		LIRS_Pair tempEntry = new LIRS_Pair();
		tempEntry.setCandidateInfo(entry); 
	    candidateList.remove(tempEntry);
		freeList.add(entry);
	}
	
	public void deleteFreeListEntry(Pair entry) {
		LIRS_Pair tempEntry = new LIRS_Pair();
		tempEntry.setCandidateInfo(entry); 
		if(freeList.remove(entry)) {
			// Start tracking this page with the LIRS algorithm
			candidateList.add(tempEntry);
		}		
	}
	
	public void updateEntryAccess(Pair entry) {
    	LIRS_Pair candidate;
    	// Look for the given page
    	int i = candidateList.indexOf(entry);
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
}

class LIRS_Pair {
	/* Linked List Declaration */
    Pair candidateInfo;
    Integer reuseDistance;
    Integer recency;
    Integer weight;
    Integer lastAccessed; // Value of globalCount last time 
                            // this page was accessed.
    
    public LIRS_Pair() {
    	reset();
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
    
    public void setCandidateInfo(Pair pair) {
    	candidateInfo = new Pair(pair);
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
//    public Pair search(PageId aPageId) {
//    	Pair nextPage;
//    	// Look for the given pageId in this bucket
//        for (int i = 0; i < linkedList.size(); i++) {
//            nextPage = linkedList.get(i); 
//            if(nextPage.equals(aPageId))
//            	return nextPage;
//        }
//    	return new Pair(); // Returns empty pair
//    }
//    
//    public void insert(Pair newPair) {
//    	// Don't allow duplicates
//    	if(!linkedList.contains(newPair))
//    	   linkedList.add(newPair);
//    }
//    
//    public Boolean delete(Pair newPair) {
//    	return linkedList.remove(newPair);
//    }
    
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
         
        // typecast o to Complex so that we can compare data members 
        LIRS_Pair c = (LIRS_Pair) o;
         
        if(c.candidateInfo != null)
           // Compare the relevant data members and return accordingly 
           return c.candidateInfo.equals(this.candidateInfo);
        else
        	return false;
    }
}