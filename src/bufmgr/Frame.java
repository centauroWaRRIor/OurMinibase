package bufmgr;

import global.Minibase;
import global.Page;
import global.PageId;

import java.io.IOException;

import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;

class Frame 
{
   private PageId pageId;
   private Page pg;
   private Integer pinCount;
   private Boolean isDirty;
   private Boolean isReplacementCandidate;
    
   public Frame() {
      pageId = new PageId();
      pg = new Page();
      resetFrame();
   }

   public void setPageId(Integer pid) {
      this.pageId.pid = pid;
   }
   
   public Page getPage() {
	   // return reference so clients can modify it directly
	   return pg;
   }
   
   public void incPinCount() { // TODO: Deal with variable wrapping around
     pinCount++;
   }
   
   // TODO: Maybe throw an exception when pin_count is negative? 
   public void decrPinCount() {
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
   
   public void setIsFrameDirty(Boolean isDirty) {
	   this.isDirty = isDirty;
   }
   
   public Boolean isReplacementCandidate() {
	   return isReplacementCandidate;
   }
   
   public Integer getPinCount() {
	   return pinCount;
   }
   
   public PageId getPageId() {
	   return pageId;
   }
   
   public byte[] getFrameData() {
	   return pg.getpage();
   }
   
   public void resetFrame() {
	   pinCount = 0;
	   isDirty = false;
	   isReplacementCandidate = true;
   }
}
