package heap;

import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Comparator;;

import global.RID;
import global.Minibase;
import global.Page;
import global.PageId;
import global.Convert;
import global.GlobalConst;

import heap.Tuple;
import heap.HFPage;

import chainexception.ChainException;

class HeapFileUtil implements GlobalConst {
    /* convenience functions */
    static HFPage getHFPage(PageId pid) {
        /*
        Page page = new Page();
        System.out.print( String.format( "Trying to pin pageno %d\n", pid.pid ) );
        Minibase.BufferManager.pinPage(pid, page, PIN_DISKIO);
        return new HFPage(page);
        */
        HFPage hfp = new HFPage();
        hfp.setCurPage(pid);
        return hfp;
    }
    
    static void releaseHFPage(HFPage hfp, boolean dirty) {
        Minibase.BufferManager.unpinPage(hfp.getCurPage(), dirty);
    }

    static HFPage newHFPage() {
        Page page = new Page();
        PageId pid = Minibase.BufferManager.newPage(page, 1);
        System.out.print( String.format( "newHFPage -> page id [%d] \n", pid.pid ) );
        HFPage hfp = new HFPage();
        hfp.setCurPage(pid);
        System.out.println( "printing the HFPage" );
        hfp.print();

        System.out.print( String.format( "newHFPage -> free space [%d] \n", hfp.getFreeSpace() ) );

        return hfp;
    }

    static void printNumPinnedPages() {
        System.out.print( String.format( "Number of pinned pages [%d]\n", 
                    Minibase.BufferManager.getNumBuffers() - Minibase.BufferManager.getNumUnpinned() ) );
    }
};

class DirectoryEntry {
    public DirectoryEntry( PageId pid, int capacity, int num_records ) {
        this.pid = pid;
        this.capacity = capacity;
        this.num_records = num_records;
        this.rid = new RID();

        /* we will save the page id (int), capacity, and num_records 
           3 integers */
        this.data = new byte[getRecSize()];
    }

    public void setCapacity( int capacity ) { this.capacity = capacity; }
    public static int getRecSize()      { return 4+4+4; }
    public int getPageCapacity()        { return capacity; }
    public PageId getPageId()           { return pid; }

    public RID getRID() { return rid; }
    public void setRID(RID rid) { this.rid = rid; }
    public PageId getOriginDirectoryPage() { return rid.pageno; }

    public byte[] getByteArray() 
	    throws java.io.IOException {
	    /** convert this class objcet to a byte array
	    *  this is used when you want to write this object to a byte array
	    */
		    Convert.setIntValue (pid.pid, 0, data);
		    Convert.setIntValue (capacity, 4, data);
		    Convert.setIntValue (num_records, 8, data);

		    return data;
    }

    public Tuple getTuple() throws java.io.IOException {
        byte [] byteArray = getByteArray();
        return new Tuple( byteArray, 0, byteArray.length );
    }

    /* this is what we save and what the DirEnt points to */
    private PageId pid;
    private int capacity;
    private int num_records;

    private byte[] data;

    /* this is the RID of the Directory Entry itself */
    RID rid;
};

class CompareSizeDirectoryEntry implements Comparator<DirectoryEntry> {
    @Override 
    public int compare(DirectoryEntry dirent1, DirectoryEntry dirent2) {
        return( dirent1.getPageCapacity() - dirent2.getPageCapacity() );
    }
}

class ComparePIDDirectoryEntry implements Comparator<DirectoryEntry> {
    @Override 
    public int compare(DirectoryEntry dirent1, DirectoryEntry dirent2) {
        return( dirent1.getPageId().pid - dirent2.getPageId().pid );
    }
}

class Directory implements global.GlobalConst {
    public Directory(PageId pid) {
        startingPID = pid;

        /* create our tree to manage the capacity */
        page_capacity_array = new ArrayList<DirectoryEntry> ();
        page_id_tree = new TreeSet<DirectoryEntry> ( new ComparePIDDirectoryEntry() );
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
        9) Add the Directory Entry to the in memory data structures
        10) Return this Directory Entry.

        TBD:
        - some inefficiency since we create a new page but do not return it.
        - we could return the page and take DirectoryEntry as a parameter.
    */
    public DirectoryEntry getDirectoryEntryWithCapacity(int size) 
                throws java.io.IOException {
        /* search in the tree for the Directory Entries with the closest size */
        DirectoryEntry dirent = getFirstGreaterThan(size);
        if( dirent != null ) {
            return dirent;
        }

        System.out.println( "getDirectoryEntryWithCapacity\n" );
        HeapFileUtil.printNumPinnedPages();

        /* page does not exist.  create new data page */
        HFPage hfp_data = HeapFileUtil.newHFPage();

        System.out.println( "getDirectoryEntryWithCapacity after newHFPage\n" );
        HeapFileUtil.printNumPinnedPages();

        System.out.print( String.format( "getDirectoryEntryWithCapacity - created page [%d]\n", 
                        hfp_data.getCurPage().pid ) );

        /* create a new Directory Entry to hold this page */
        dirent = new DirectoryEntry(hfp_data.getCurPage(), hfp_data.getFreeSpace(), 0);

        /* get a Directory Page to host this Directory Entry */
        HFPage hfp_dir = getDirectoryPageForNewDirectoryEntry();

        System.out.println( "getDirectoryEntryWithCapacity after getDirectoryPage...\n" );
        HeapFileUtil.printNumPinnedPages();

        /* Insert the Directory Entry into this page */
        byte [] ba = dirent.getByteArray();
        System.out.print( String.format( "Length of the directory record [%d]\n", ba.length ) );

        RID rid = hfp_dir.insertRecord( dirent.getByteArray() );

        System.out.print( String.format( "After inserting directory entry pid [%d] slot [%d]\n", 
                        rid.pageno.pid, rid.slotno ) );

        System.out.println( "getDirectoryEntryWithCapacity: printing hfp_dir" );
        hfp_dir.print();
        
        /* Unpin this Directory Page - indicate that the page is Dirty */
        //Minibase.BufferManager.unpinPage(hfp_dir.getCurPage(), true);

        /* The Directory Entry needs to remember where it came from */
        dirent.setRID( rid );

        /* This dirent is added to in memory once the capacity is updated */

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
        for( pid = startingPID; pid != null; ) {
            /* pin the page */
            System.out.print( String.format( "before pinning startingPID [%d]\n", startingPID.pid ) );
            HeapFileUtil.printNumPinnedPages();

            HFPage hfp_dir = HeapFileUtil.getHFPage(pid);

            System.out.print( String.format( "free space from directory page:  [%d]\n", hfp_dir.getFreeSpace() ) );
            HeapFileUtil.printNumPinnedPages();

            /* is there capacity? */
            if( hfp_dir.getFreeSpace() >= DirectoryEntry.getRecSize() ) {
                /* found it */
                /* caller needs to unpin the page */
                return hfp_dir;
            } else {
                pid = hfp_dir.getNextPage();
            }

            /* release page to the buffer pool */
            HeapFileUtil.releaseHFPage(hfp_dir, false);
        }

        /* if we are here, we did not find a Directory Page - need to allocate one */
        /* pid is pointing to the last page in the Directory Pages Linked List */
        HFPage hfp_dir = HeapFileUtil.getHFPage(pid);
        HFPage new_hfp_dir = HeapFileUtil.newHFPage();

        hfp_dir.setNextPage(new_hfp_dir.getCurPage());
        new_hfp_dir.setPrevPage(hfp_dir.getCurPage());

        HeapFileUtil.releaseHFPage( hfp_dir, UNPIN_DIRTY );

        /* caller needs to unpin this */
        return new_hfp_dir;
    }

    public void addDirectoryEntryToInMemory(DirectoryEntry dirent) { 
        System.out.println( "Not used - error" );
    }

    public void updateDirectoryEntryInMemory(DirectoryEntry dirent) { 
        /* add in both the trees */
        page_id_tree.add(dirent);
        page_capacity_array.add(dirent);
    }

    /*
        Linear performance now.
        Needs to be optimized
    */
    public DirectoryEntry getFirstGreaterThan(int capacity) {
        int found_index = -1;
        int found_capacity = Integer.MAX_VALUE;
        DirectoryEntry dirent = null;
        for( int i = 0; i < page_capacity_array.size(); i++ ) {
            dirent = page_capacity_array.get(i);
            if( dirent.getPageCapacity() >= capacity ) {
                if( dirent.getPageCapacity() < found_capacity ) {
                    found_index = i;
                    found_capacity = dirent.getPageCapacity();
                }
            }
        }

        if( found_index > 0 ) { 
            page_capacity_array.remove(found_index);

            /* to be consistent, we will remove it from the page_id_set as well */
            page_id_tree.remove(dirent);

            return dirent;
        }

        return null;
    }

    /*
        Updates the Directory Entry;  this happens when capacity changes.

        1. Update the database.
        2. Update the internal memory structure.
    */
    public void updateDirectoryEntry(DirectoryEntry dirent) 
                throws java.io.IOException { 
        /* update the directory with the new Directory Entry */
        HFPage hfp_dir = HeapFileUtil.getHFPage(dirent.getOriginDirectoryPage() );

        System.out.println( "printing the HFPage\n" );
        hfp_dir.print();

        /* insert the record */
        Tuple t = dirent.getTuple();
        RID rid = dirent.getRID();
        System.out.print( String.format( "length of tuple [%d]\n", t.getLength() ) );
        System.out.print( String.format( "RID for update: pid [%d] slot [%d]\n", rid.pageno.pid, rid.slotno ) );
        hfp_dir.updateRecord( dirent.getRID(), dirent.getTuple() );

        HeapFileUtil.releaseHFPage(hfp_dir, true);

        updateDirectoryEntryInMemory(dirent);
    }

    public boolean doesPageIDExist(PageId pid) {
        return page_id_tree.contains( new DirectoryEntry(pid,-1,-1) );
    }

    PageId getStartingPID() { return startingPID; }

    private PageId startingPID;
    //private TreeSet<DirectoryEntry> page_capacity_tree;
    private TreeSet<DirectoryEntry> page_id_tree;
    private ArrayList<DirectoryEntry> page_capacity_array;
}

public class HeapFile {
    /*
        - create a PageId object for page 0 - this is our Directory Page
        - create a file entry in the DB using DiskMgr.
    */
    public HeapFile(String name) {
        PageId pid = Minibase.DiskManager.allocate_page();

        System.out.print( String.format( "Initializing Heapfile - allocating a new page %d\n", pid.pid ) );

        directory = new Directory( pid );

        /* for now, assume it is a new file */
        Minibase.DiskManager.add_file_entry(name, directory.getStartingPID() );

    }

    /*
        - get a Directory Entry that points to a page that we can insert to.
        - create Page and then a HFPage object.
        - insert the user record into this.
        - update the Directory Entry with the new information about the page.
    */
    public RID insertRecord(byte[] record) throws ChainException, java.io.IOException {

        /* get the DirectoryEntry that points to the page we want */
        DirectoryEntry dirent = directory.getDirectoryEntryWithCapacity( record.length );

        /* load the page from disk through the buffer manager */
        HFPage hfp_data  = HeapFileUtil.getHFPage(dirent.getPageId());

        /* insert the data */
        RID rid =  hfp_data.insertRecord(record);

        /* update the Directory Entry with the new information */
        dirent.setCapacity( hfp_data.getFreeSpace() );
        /* dirent.numRecords = hfp_data.... */

        System.out.println( "Printing the data page\n" );
        hfp_data.print();

        /* unpin the data page */
        //HeapFileUtil.releaseHFPage(hfp_data, true);

        directory.updateDirectoryEntry(dirent);

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

    /*
        1. check if we have the PageID in our directory.
        2. If yes, make a HFPage out of the PageID and get the record.
    */
    public Tuple getRecord(RID rid) {
        if( !directory.doesPageIDExist(rid.pageno) ) 
            return null;

        HFPage hfp_data = HeapFileUtil.getHFPage(rid.pageno);
        byte[] byteArray = hfp_data.selectRecord(rid);
        HeapFileUtil.releaseHFPage(hfp_data, false);

        return new Tuple(byteArray, 0, byteArray.length);
    }

    public HeapScan openScan() { 
        return null;
    }

    Directory directory;
};


