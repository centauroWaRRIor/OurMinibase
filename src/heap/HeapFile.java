package heap;

import java.util.TreeSet;
import java.util.Comparator;;

import global.RID;
import global.Minibase;
import global.Page;
import global.PageId;

import heap.Tuple;
import heap.HFPage;

import chainexception.ChainException;

class HeapFileUtil {
    /* convenience functions */
    static HFPage getHFPage(PageId pid) {
        Page page = new Page();
        Minibase.BufferManager.pinPage(pid, page, true);
        return HFPage(page);
    }
    
    static void releaseHFPage(HFPage hfp, bool dirty) {
        Minibase.BufferManager.unpinPage(hfp.getCurPage(), dirty);
    }

    static HFPage newHFPage() {
        Page page = new Page();
        PageId pid = Minibase.BufferManager.newPage(page, 1);
        return new HFPage(page);
    }
};

class DirectoryEntry {
    public DirectoryEntry( PageId pid, int capacity, int num_records ) {
        this.pid = pid;
        this.capacity = capacity;
        this.num_records = num_records;
        this.rid_dirent = new RID();

        /* we will save the page id (int), capacity, and num_records 
           3 integers */
        this.data = new byte[getRecSize()];
    }

    public static int getRecSize()    { return 4+4+4; }
    public int getPageCapacity()       { return capacity; }
    public PageId getPID()     { return pid; }

    public byte[] getByteArray() 
	    throws java.io.IOException {
	    /** convert this class objcet to a byte array
	    *  this is used when you want to write this object to a byte array
	    */
		    Convert.setIntValue (pid.pid, 0, data);
		    Convert.setIntValue (capacity, 4, data);
		    Convert.setStringValue (num_records, 8, data);

		    return data;
    }

    /* this is what we save and what the DirEnt points to */
    private PageId pid;
    private int capacity;
    private int num_records;

    private byte[] data;

    /* this is the RID of the Directory Entry itself */
    RID rid_dirent;
};

class CompareSizeDirectoryEntry implements Comparator<DirectoryEntry> {
    @Override 
    public int compare(DirectoryEntry dirent1, DirectoryEntry dirent2) {
        return( dirent1.getCapacity() - dirent2.getCapacity() );
    }
}

class Directory {
    public Directory(PageId pid) {
        startingPID = pid;

        /* create our tree to manage the capacity */
        page_capacity_tree = new TreeSet<DirectoryEntry> ( new CompareSizeDirectoryEntry() );
    }

    /*
        1) Check if we can locate a page that has enough capacity.
        2) If yes, return the Directory Entry that corresponds to this page.
        3) If no, go to 4.
        4) Create a new data page.
        5) Create a new Directory Entry to hold this page.
        6) Get a Directory Page to add this Directory Entry.
        7) Insert the newly created Directory Entry in this Directory Page.
        8) Unpin the Directory Page.
        9) Return this Directory Entry.

        TBD:
        - some inefficiency since we create a new page but do not return it.
        - we could return the page and take DirectoryEntry as a parameter.
    */
    public DirectoryEntry getDirectoryEntryWithCapacity(int size) {
        /* search in the tree for the Directory Entries with the closest size */
        boolean existing_page = false;
        DirectoryEntry dirent =  page_capacity_tree.ceiling( new DirectoryEntry(null, size, -1) );

        if( dirent != null ) {
            return dirent;
        }

        /* page does not exist.  create new data page */
        HFPage hfp_data = HeapFileUtil.newHFPage();

        /* create a new Directory Entry to hold this page */
        dirent = new DirectoryEntry(hfp_data.getCurPage(), hfp_data.getFreeSpace(), 0);

        /* get a Directory Page to host this Directory Entry */
        HFPage hfp_dir = getDirectoryPageForNewDirectoryEntry();

        /* Insert the Directory Entry into this page */
        hfp_dir.insertRecord( dirent.getByteArray() );

        /* Unpin this Directory Page - indicate that the page is Dirty */
        Minibase.BufferManager.unpinPage(hfp_dir.getCurPage(), true);

        return dirent;
    }

    /*
        Returns a HFPage that can hold one Directory Entry.
        1) Start from the root of the Directory.
        2) Check the free space;  if it is more than the size of the Directory Entry, return this page.
        3) Otherwise, go to the next Directory page, following the linked list.
        4) Do 2, 3 until done.  Return page if we find a page that has the capacity.
        5) If we cannot find a Directory page, go to 6).
        6) Create a new page.  Connect this with the last page in the Directory Pages Linked List.
        7) Return this new page.
    */
    public HFPage getDirectoryPageForNewDirectoryEntry() {
        PageId pid;
        /* Go through the Directory Pages, starting from the Root */
        for( PageId pid = startingPID; pid != null; ) {
            /* pin the page */
            HFPage hfp_dir = HeapFileUtil.getHFPage(pid);

            /* is there capacity? */
            if( hfpage.getFreeSpace() >= DirectoryEntry.getRecSize() ) {
                /* found it */
                /* caller needs to unpin the page */
                return hfpage;
            } else {
                pid = hfpage.getNextPage();
            }

            HeapFileUtil.releaseHFPage(hfp_dir, false);
        }

        /* if we are here, we did not find a Directory Page - need to allocate one */
        HFPage hfp_dir = HeapFileUtil.getHFPage(pid);
        HFPage new_hfp_dir = HeapFileUtil.newHFPage();

        hfp_dir.setNextPage(new_hfp_dir.getCurPage());
        new_hfpage_dir.setPrevPage(hfp_dir.getCurPage());

        /* caller needs to unpin this */
        return new_hfpage;
    }

    PageId getStartingPid() { return starting_pid; }

    private PageId startingPID;
    private TreeSet<DirectoryEntry> page_capacity_tree;
}

public class HeapFile {
    /*
        - create a PageId object for page 0 - this is our Directory Page
        - create a file entry in the DB using DiskMgr.
    */
    public HeapFile(String name) {
        directory = new Directory(0);

        /* for now, assume it is a new file */
        Minibase.DiskManager.add_file_entry(name, directory.getStartingPID() );

    }

    /*
        - get a Directory Entry that points to a page that we can insert to.
        - create Page and then a HFPage object.
        - insert the user record into this.
        - update the Directory Entry with the new information about the page.
    */
    public RID insertRecord(byte[] record) throws ChainException {

        /* get the DirectoryEntry that points to the page we want */
        DirectoryEntry dirent = directory.getDirectoryEntryWithCapacity( record.length );

        /* load the page from disk through the buffer manager */
        HFPage hfp_data  = HeapFileUtil.getHFPage(dirent.getPID());

        /* insert the data */
        RID rid =  hfp_data.insertRecord(record);

        /* update the Directory Entry with the new information */
        dirent.capacity = hfp_data.getFreeSpace();
        /* dirent.numRecords = hfp_data.... */

        /* unpin the data page */
        HeapFileUtil.releaseHFPage(hfp_data, true);

        /* update the directory with the new Directory Entry */
        hfp_dir = HeapFileUtil.getHFPage(dirent.getRID().pageno);

        /* insert the record */
        hfp_dir.updateRecord( dirent.getRID(), dirent.getByteArray() );

        HeapFileUtil.releaseHFPage(hfp_dir, true);

        return rid;
    }
    

    public int getRecCnt() {
        return -1;
    }

    public boolean updateRecord(RID rid, Tuple newRecord) throws ChainException {
        return false;
    }

    public boolean deleteRecord(RID rid) {
        return false;
    }

    public Tuple getRecord(RID rid) {
        return null;
    }

    public HeapScan openScan() { 
        return null;
    }

    Directory directory;
};


