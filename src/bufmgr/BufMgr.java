package bufmgr;

import java.io.IOException;

import chainexception.ChainException;
import global.Page;
import global.PageId;

import global.Minibase;

public class BufMgr {
	
    private Frame frames[];
    private int numbufs;
    private String replacementPolicyString;
    private HashTable hashTable;
    private LIRS replacementLIRS;
	
	/**
	* Create the BufMgr object.
	* Allocate pages (frames) for the buffer pool in main memory and
	* make the buffer manage aware that the replacement policy is* specified by replacerArg (e.g., LH, Clock, LRU, MRU, LIRS, etc.).
	*
	* @param​numbufs number of buffers in the buffer pool
	* @param​lookAheadSize number of pages to be looked ahead
	* @param​replacementPolicy Name of the replacement policy
	*/
	public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {
        frames = new Frame[numbufs];
        hashTable = new HashTable(numbufs);
        replacementLIRS = new LIRS();
        this.numbufs = numbufs;
        this.replacementPolicyString = replacementPolicy;
        // Add all the pages initially to the free pages list
        for(int i = 0; i < numbufs; i++) {
        	replacementLIRS.insertFreeListEntry(new Pair(-1, i));
        }
    }
	/**
	* Pin a page.
	* First check if this page is already in the buffer pool.
	* If it is, increment the pin_count and return a pointer to this
	* page.
	* If the pin_count was 0 before the call, the page was a
	* replacement candidate, but is no longer a candidate.
	* If the page is not in the pool, choose a frame (from the
	* set of replacement candidates) to hold this page, read the
	* page (using the appropriate method from {\em diskmgr} package) and pin it.
	* Also, must write out the old page in chosen frame if it is dirty
	* before reading new page.__ (You can assume that emptyPage==false for
	* this assignment.)
	*
	* @param pageno page number in the Minibase.
	* @param page the pointer point to the page.
	* @param emptyPage true (empty page); false (non­empty page)
	*/
	public void pinPage(PageId pageno, Page page, boolean emptyPage) 
    {
		// TODO: Throw BufferPoolExceededException
		Pair mgmInfo = null;
		try {
			mgmInfo = hashTable.hashKey(pageno);
		} catch (HashEntryNotFoundException e) {
			//e.printStackTrace();
			// Find a candidate for replacement
			// TODO: line below may throw BufferPoolExceededException
			Pair replacementCandidate = replacementLIRS.getReplacementCandidate();
	        Integer replacementIndex = replacementCandidate.getFrameNumber();
	        // Flush replacement page before reusing
	        if(frames[replacementIndex].isFrameDirty())
	           flushPage(frames[replacementIndex].getPageId());
	        // Erase old entry from hashTable 
	        try {
				hashTable.deleteEntry(replacementCandidate);
			} catch (HashEntryNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	        // Add new entry to hashtable
	        replacementCandidate.setPageNumber(pageno);
	        hashTable.insertEntry(replacementCandidate);
	        // Update mgmInfo so control flow can continue as 
	        // if nothing happened 
	        mgmInfo = replacementCandidate;
		}
		// Assume mgmInfo contains the frame location
		// for the given page from now on
		
		// If the page was a replacement candidate, it no
		// longer is
		Integer frameIndex = mgmInfo.getFrameNumber(); 
		Frame frame = frames[frameIndex];
		if(frame.isReplacementCandidate())
			frame.setReplacementCandidate(false);
		// Therefore communicate LIRS to eliminate this page from
		// list of empty pages
		replacementLIRS.deleteFreeListEntry(new Pair(frame.getPageId(),
				frameIndex));
		
		// Increment pinCount
		frames[frameIndex].IncPinCount();
		
		// Update LIRS stats
		replacementLIRS.updateEntryAccess(mgmInfo);
		
		page.setPage(frames[frameIndex].getPage());		
    }
	/**
	* Unpin a page specified by a pageId.
	* This method should be called with dirty==true if the client has
	* modified the page.
	* If so, this call should set the dirty bit
	* for this frame.
	* Further, if pin_count>0, this method should
	* decrement it.
	*If pin_count=0 before this call, throw an exception
	* to report error.
	*(For testing purposes, we ask you to throw
	* an exception named PageUnpinnedException in case of error.)
	*
	* @param pageno page number in the Minibase.
	* @param dirty the dirty bit of the frame
	*/
	public void unpinPage(PageId pageno, boolean dirty) 
    {
//        System.out.print( "\n unpinPage::page id: [" + pageno.pid + "]" );
//        Frame f = getFrame(pageno);
//
//        if( f.pin_count > 0 ) 
//            f.pin_count--;
//
//        if( f.pin_count <= 0 )
//        {
//            if( f.dirty )
//                Minibase.DiskManager.write_page(pageno, f.pg);
//
//            // make it available:
//            f.pid = null;
//            f.pg = null;
//        }

    }
	/**
	* Allocate new pages.* Call DB object to allocate a run of new pages and
	* find a frame in the buffer pool for the first page
	* and pin it. (This call allows a client of the Buffer Manager
	* to allocate pages on disk.) If buffer is full, i.e., you
	* can't find a frame for the first page, ask DB to deallocate
	* all these pages, and return null.
	*
	* @param firstpage the address of the first page.
	* @param howmany total number of allocated new pages.
	*
	* @return the first page id of the new pages.__ null, if error.
	*/
	public PageId newPage(Page firstpage, int howmany) 
    {
        PageId pid = new PageId();
        try {
			Minibase.DiskManager.allocate_page(pid, howmany);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ChainException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        pinPage(pid, firstpage, false);
		return pid;
    }
	/**
	* This method should be called to delete a page that is on disk.
	* This routine must call the method in diskmgr package to
	* deallocate the page.
	*
	* @param globalPageId the page number in the data base.
	*/
	public void freePage(PageId globalPageId) {}
	/**
	* Used to flush a particular page of the buffer pool to disk.
	* This method calls the write_page method of the diskmgr package.
	*
	* @param pageid the page number in the database.
	*/
	public void flushPage(PageId pageid) {}
	/**
	* Used to flush all dirty pages in the buffer pool to disk
	*
	*/
	public void flushAllPages() {}
	/**
	* Returns the total number of buffer frames.
	*/
	public int getNumBuffers() {
		return numbufs;
	}
	/**
	* Returns the total number of unpinned buffer frames.
	*/
	public int getNumUnpinned() {
		return replacementLIRS.getFreeListSize();
	}

//    /* need to replace the following with a Priority Queue: */
//    Frame getLIRSFrame()
//    {
//        for( int i = 0; i < numbufs; i++ )
//        {
//            if( frames[i].page == null )
//                return frames[i];
//        }
//
//        System.out.println( "Whoops - out of frames!\n" );
//        return null;
//    }
//
//    /* need to replace the following with our hash table */
//    Frame getFrame(PageId pid)
//    {
//        for( int i = 0; i < numbuf; i++ )
//        {
//            if( frames[i].page != null && frames[i].pid == pid )
//                return frames[i];
//        }
//
//        return null;
//    }	
}

class Frame 
{
   private PageId pid;
   private Page pg;
   private Integer pinCount;
   private Boolean isDirty;
   private Boolean isReplacementCandidate;
    
   public Frame() {
      pid = new PageId();
      pg = new Page();
      resetFrame();
   }

   public void setPageId(PageId pid) {
      this.pid = pid;
   }
   
   public Page getPage() {
	      return pg;
   }
   
   public void IncPinCount() {
     pinCount++;
   }
   
   // TODO: Maybe throw an exception when pin_count is negative? 
   public void DecrPinCount() {
     pinCount--;
     if(pinCount == 0)
    	 isReplacementCandidate = true;
   }
   
   public void setReplacementCandidate(Boolean value) {
	   isReplacementCandidate = value;
   }
   
   public Boolean isFrameDirty() {
	   return isDirty;
   }
   
   public Boolean isReplacementCandidate() {
	   return isReplacementCandidate;
   }
   
   public Integer getPinCount() {
	   return pinCount;
   }
   
   public PageId getPageId() {
	   return pid;
   }
   
   public byte[] getFrameData() {
	   return pg.getpage();
   }
   
   public void resetFrame() {
	   pinCount = 0;
	   isDirty = false;
	   isReplacementCandidate = false;
   }
}
