package bufmgr;

import java.io.IOException;

import chainexception.ChainException;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.Page;
import global.PageId;

import global.Minibase;

public class BufMgr {
	
    private Frame frames[];
    private int numbufs;
    private String replacementPolicy;
    private HashTable hashTable;
    private LIRS lirsPolicy;
	
	/**
	* Create the BufMgr object.
	* Allocate pages (frames) for the buffer pool in main memory and
	* make the buffer manage aware that the replacement policy is* specified by replacerArg (e.g., LH, Clock, LRU, MRU, LIRS, etc.).
	 * @throws LIRSFailureException 
	*
	* @param​numbufs number of buffers in the buffer pool
	* @param​lookAheadSize number of pages to be looked ahead
	* @param​replacementPolicy Name of the replacement policy
	*/
	public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) 
			throws LIRSFailureException {

		// Create array of frames
        frames = new Frame[numbufs];
        // Allocate the frames
    	for (int i = 0; i < frames.length; i++)
    		frames[i]= new Frame();   
        hashTable = new HashTable(numbufs);
        lirsPolicy = new LIRS(numbufs);
        this.numbufs = numbufs;
        // TODO: Maybe throw exception for replacement policies we haven't implemented
        this.replacementPolicy = replacementPolicy;
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
	 * @throws HashEntryNotFoundException 
	 * @throws LIRSFailureException 
	 * @throws DiskMgrException 
	*/
	public void pinPage(PageId pageno, Page page, boolean emptyPage) 
		throws HashEntryNotFoundException, LIRSFailureException, DiskMgrException {
		// TODO: Throw BufferPoolExceededException
		Pair mgmInfo = null;
		Boolean hashEntryFound = true;
		try {
			mgmInfo = hashTable.hashKey(pageno);
		} catch (HashEntryNotFoundException e) {
			hashEntryFound = false;
			// Find a candidate for replacement
			// TODO: line below may throw BufferPoolExceededException
			// TODO: Only use this when replacementPolicy=LIRS
			Pair replacementCandidate = lirsPolicy.getReplacementCandidate(pageno);
	        Integer replacementIndex = replacementCandidate.getFrameNumber();
	        
	        // Flush replacement page before reusing
	        if(frames[replacementIndex].isFrameDirty()) {
	           flushPage(frames[replacementIndex].getPageId());
	           frames[replacementIndex].setIsFrameDirty(false);
	        }
			/* Need to remove this entry from the hash table.
			 * Exception only applies when the this is the 
			 * first time using this frame
			 */
	        if(frames[replacementIndex].isHashed()) {
	           try {
				   hashTable.deleteEntry(replacementCandidate);
			   } catch (HashEntryNotFoundException e1) {
			      throw new HashEntryNotFoundException(e1, 
					   "Attempted to delete a non existing entry in the hash table!");
			   }
	        }
			// Set this frame for use with new page id
			replacementCandidate.setPageId(pageno.pid);
	        // Add new entry to hash table
	        hashTable.insertEntry(replacementCandidate);
	        // Lower initial condition flag for this frame (always true after first time)
	        frames[replacementIndex].setIsHashed(true);
	        // Update mgmInfo so control flow can continue as 
	        // if nothing happened 
	        mgmInfo = replacementCandidate;
		}		
		Integer frameIndex = mgmInfo.getFrameNumber(); 
		Frame frame = frames[frameIndex];
		// Update pageid for this frame here
		frame.setPageId(pageno.pid);

		/* Delete from free list only in special case where 
		 * this frame was a replacement candidate but was
		 * still in the hash table
		 */
		if(hashEntryFound && frame.isReplacementCandidate()) {
			lirsPolicy.deleteFreeListEntry(mgmInfo); 
		}
			
		/* Increment pinCount, this will also remove the 
		 * replacement candidate flag if there was such.
		 */
		frame.incPinCount();
		
		// Update LIRS stats
		lirsPolicy.updatePageAccessStats(mgmInfo);
		
		// Return page stored in this frame
		page.setPage(frame.getPage());		
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
	 * @throws LIRSFailureException 
	*/
	public void unpinPage(PageId pageno, boolean dirty) 
	throws PageUnpinnedException, LIRSFailureException
    {
		Pair mgmInfo = null;
		try {
			mgmInfo = hashTable.hashKey(pageno);
		} catch (HashEntryNotFoundException e) {
			e.printStackTrace();
			throw new PageUnpinnedException(e,
    				"Trying to unpin a page not found in the buffer pool"); 
		}
		Integer frameIndex = mgmInfo.getFrameNumber(); 
		Frame frame = frames[frameIndex];
		// If pin_count was zero 
        if(frame.isReplacementCandidate())
        	throw new PageUnpinnedException(null,
    				"Trying to unpin a page not found in the buffer pool");
        else {
        	if(dirty)
        	   frame.setIsFrameDirty(true);
        	// Flushes the frame's page and raises flag for frame reuse 
        	// when pinCount = 0
        	frame.decrPinCount();
    		// Finally, communicate to LIRS to add this page to
    		// list of empty pages if appropriate
        	if(frame.isReplacementCandidate())
    		   lirsPolicy.insertFreeListEntry(new Pair(frame.getPageId().pid,
    				                                   frameIndex));
        }
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
	 * @throws ChainException 
	*/
	public PageId newPage(Page firstpage, int howmany) 
			throws ChainException 
    {
        PageId pid = new PageId();
        try {
			Minibase.DiskManager.allocate_page(pid, howmany);
			pinPage(pid, firstpage, false);
		} catch (HashEntryNotFoundException e1) {
			throw new HashEntryNotFoundException(e1, "Issue pinning pid (Hash Table)");
		} catch (LIRSFailureException e2) {
			throw new LIRSFailureException(e2, "Issue pinning pid (LIRS)");
		} catch (DiskMgrException e3) {
			throw new DiskMgrException(e3, "Issue allocating pid (DiskManager)");
		} catch (Exception e4) {
			throw new ChainException(e4, "Issue allocating pid (other)");
		}
		return pid;
    }
	/**
	* This method should be called to delete a page that is on disk.
	* This routine must call the method in diskmgr package to
	* deallocate the page.
	*
	* @param globalPageId the page number in the data base.
	 * @throws DiskMgrException 
	*/
	public void freePage(PageId globalPageId) 
			throws DiskMgrException
	{
		try {
			Minibase.DiskManager.deallocate_page(globalPageId);
		} catch (Exception e) {
			// Per the specs, throw exception caused by lower layer
			throw new DiskMgrException(e, "DiskManager failed to deallocate page");
		}
	}
	/**
	* Used to flush a particular page of the buffer pool to disk.
	* This method calls the write_page method of the diskmgr package.
	*
	* @param pageid the page number in the database.
	 * @throws DiskMgrException 
	 * @throws HashEntryNotFoundException 
	*/
	public void flushPage(PageId pageid) 
			throws DiskMgrException, HashEntryNotFoundException {
		Pair bufferPageInfo = null;
		try {
		   bufferPageInfo = hashTable.hashKey(pageid);
		   Integer frameIndex = bufferPageInfo.getFrameNumber(); 
		   Frame frame = frames[frameIndex];
		   Minibase.DiskManager.write_page(frame.getPageId(), frame.getPage());
		} catch (HashEntryNotFoundException e) {
			throw new HashEntryNotFoundException(e, "Page to flush not found in buffer pool");
		} catch (Exception e) {
			// Per the specs, throw exception caused by lower layer
			throw new DiskMgrException(e, "DiskManager failed to write to page");
		}
	}
	/**
	* Used to flush all dirty pages in the buffer pool to disk
	 * @throws DiskMgrException 
	*
	*/
	public void flushAllPages() 
			throws DiskMgrException { 
		Frame frame;
		for(int i = 0; i < numbufs; i++) {
			frame = frames[i];
			if(frame.isFrameDirty() && !frame.isReplacementCandidate()) {
				try {
					Minibase.DiskManager.write_page(frame.getPageId(), frame.getPage());
				} catch (Exception e) {
					// Per the specs, throw exception caused by lower layer
					throw new DiskMgrException(e, "DiskManager failed to write to page");
				}					
			}
		}
	}
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
		return lirsPolicy.getFreeListSize();
	}
}