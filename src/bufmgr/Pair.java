package bufmgr;

import global.PageId;

class Pair {
    private PageId pageNumber;
    private Integer frameNumber;

    public Pair(PageId pageNumber, Integer frameNumber) {
        this.pageNumber.copyPageId(pageNumber);
        this.frameNumber = frameNumber;
    }
    
    public Pair(Integer pageNumber, Integer frameNumber) {
        this.pageNumber = new PageId(pageNumber);
        this.frameNumber = frameNumber;
    }
    
    public Pair() {
    	// Create an empty Pair by default
    	pageNumber = new PageId(-1);
    	frameNumber = -1;
    }
    
    public Pair(Pair o) {
    	// Copy constructor
    	pageNumber = new PageId(o.pageNumber.pid);
    	frameNumber = o.frameNumber;
    }
    
    public void setPageId(PageId pageNumber) {
        this.pageNumber.copyPageId(pageNumber);
    }

    public void setFrameNumber(Integer frameNumber) {
        this.frameNumber = frameNumber;
    }

    public PageId getPageId() {
    	// Keep our instance of PageId private
        return new PageId(pageNumber.pid);
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
 
        /* Check if o is an instance of Pair or not
          "null instanceof [type]" also returns false */
        if (!(o instanceof Pair)) {
            return false;
        }
         
        // typecast o to Pair so that we can compare data members 
        Pair c = (Pair) o;
         
        // Compare the relevant data members and return accordingly 
        return c.pageNumber.pid == this.pageNumber.pid &&
        	   c.frameNumber == this.frameNumber;
    }
}
