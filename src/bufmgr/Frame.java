package bufmgr;

import global.Minibase;
import global.Page;
import global.PageId;

import java.io.IOException;

import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;

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
      this.pid.copyPageId(pid);
   }
   
   public Page getPage() {
	   // return reference so clients can modify it directly
	   return pg;
   }
   
   public void incPinCount() {
     pinCount++;
   }
   
   // TODO: Maybe throw an exception when pin_count is negative? 
   public void decrPinCount() {
     pinCount--;
     if(pinCount == 0) {
    	 isReplacementCandidate = true;
    	 // Flush the page
		try {
			Minibase.DiskManager.write_page(pid, pg);
		} catch (InvalidPageNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}						
    	 
     }
   }
   
   public void setReplacementCandidate(Boolean value) {
	   isReplacementCandidate = value;
   }
   
   public Boolean isFrameDirty() {
	   return isDirty;
   }
   
   public void setFrameDirty() {
	   isDirty = true;
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
