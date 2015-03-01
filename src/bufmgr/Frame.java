package bufmgr;

import global.Page;
import global.PageId;

class Frame 
{
   private PageId pageId;
   private Page pg;
   private Integer pinCount;
   private Boolean isDirty;
   private Boolean isReplacementCandidate;
   private Boolean isInHashTable; // Deals with init condition
    
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
	 if(pinCount == 0)
	   	 isReplacementCandidate = false;	   
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
   
   public Boolean isHashed() {
	   return isInHashTable;
   }
   
   public void setIsHashed(Boolean value) {
	   isInHashTable = value;
   }
   
   public Integer getPinCount() {
	   return pinCount;
   }
   
   public PageId getPageId() {
	   return pageId;
   }
   
   public void resetFrame() {
	   pinCount = 0;
	   isDirty = false;
	   isReplacementCandidate = true;
	   isInHashTable = false;
   }
}
