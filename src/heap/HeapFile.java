/*
TODO:

- In memory structure for checking capacity of pages
- Saving and re-running
- Exceptions
- Update and Insert
- HeapScan
- Increase the number of Inserts (more data pages;  more directory pages)

- Confirm if the number of pages I am getting is OK.  I got 7 pages for 100 records


NOTES:
- I get an insufficient space error when there is exact space left (32 bytes VS 32 bytes)
  So I resorted to checking for strictly greater than which seems to work


*/
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

enum LogLevel {
     VERBOSE(0), MOST(1), MORE(2), LESS(3), NONE(4);
     private int level;
     LogLevel(int level) { this.level = level; }
}

class Log
{
    static LogLevel current = LogLevel.MOST;

    public static boolean IsVerbose() { return current == LogLevel.VERBOSE ? true : false; }

    public static void log(LogLevel level, String format, Object...args) {
        if( level.ordinal() >= Log.current.ordinal() ) {
            System.out.printf(format, args);
        }
    }
}

class HeapFileUtil implements GlobalConst {

    static void printNumPinnedPages(String s) {
        Log.log( LogLevel.MOST, "%s: Number of pinned pages [%d]\n",  s,
                    Minibase.BufferManager.getNumBuffers() - Minibase.BufferManager.getNumUnpinned() ) ;
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

class RecCountEntry {
    public RecCountEntry( int reccnt ) { 
        this.reccnt = reccnt; 
        data = new byte[getRecSize()];
    }

    public byte [] getByteArray() throws java.io.IOException {
        Convert.setIntValue( reccnt, 0, data ); 
        return data;
    }

    public Tuple getTuple() throws java.io.IOException {
        byte [] ba = getByteArray();
        return new Tuple( ba, 0, ba.length ); 
    }

    public int getRecSize() { return 4; }

    int reccnt;
    private byte[] data;
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
    public Directory(PageId pid) throws java.io.IOException {
        startingPID = pid;

        /* create our tree to manage the capacity */
        page_capacity_array = new ArrayList<DirectoryEntry> ();
        page_id_tree = new TreeSet<DirectoryEntry> ( new ComparePIDDirectoryEntry() );

        /* remember the number of records */
        reccount = 0;

        /* insert a zero record entry */
        insertRecCount();
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

        String function_name = "getDirectoryEntryWithCapacity";
        /* search in the tree for the Directory Entries with the closest size */

        Log.log( LogLevel.MOST, "%s: Searching for a Directory Entry more than [%d]\n", function_name, size );
        DirectoryEntry dirent = getFirstGreaterThan(size);
        if( dirent != null ) {
            return dirent;
        }

        /* page does not exist.  create new data page */
        HFPage page_data = new HFPage();
        PageId pid_data = Minibase.BufferManager.newPage(page_data, 1);
        page_data.setCurPage(pid_data);
        Log.log( LogLevel.MOST, "%s: Directory Entry does not exist - creating new page [%d]\n", 
                        function_name, pid_data.pid );

        /* create a new Directory Entry to hold this page */
        Log.log( LogLevel.MOST, "%s: creating DirectoryEntry with pageno [%d] and free space [%d]\n", 
                    function_name, pid_data.pid, page_data.getFreeSpace() );
        dirent = new DirectoryEntry(page_data.getCurPage(), page_data.getFreeSpace(), 0);
        Minibase.BufferManager.unpinPage(pid_data, true);

        HeapFileUtil.printNumPinnedPages( function_name );

        Log.log( LogLevel.MOST, "%s: Getting Directory Page for Directory Entry\n", function_name );
        /* get a Directory Page to host this Directory Entry */
        HFPage page_dir = getDirectoryPageForNewDirectoryEntry();

        HeapFileUtil.printNumPinnedPages( function_name );

        /* Insert the Directory Entry into this page */
        byte [] ba = dirent.getByteArray();
        Log.log( LogLevel.MOST, "%s: Length of the directory record [%d]\n", function_name, ba.length );

        RID rid = page_dir.insertRecord( dirent.getByteArray() );

        Log.log( LogLevel.MOST, "%s: After inserting directory entry pid [%d] slot [%d]\n", 
                        function_name, rid.pageno.pid, rid.slotno ) ;

        if( Log.IsVerbose() ) {
            Log.log( LogLevel.VERBOSE, "%s: printing Directory Page\n", function_name );
            page_dir.print();
        }
        
        /* Unpin this Directory Page - indicate that the page is Dirty */
        Minibase.BufferManager.unpinPage(page_dir.getCurPage(), true);

        /* The Directory Entry needs to remember where it came from */
        dirent.setRID( rid );

        Log.log( LogLevel.MOST, "%s: Returning Directory Entry successfully!\n", function_name );
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
        String function_name = "getDirectoryPageForNewDirectoryEntry";

        Log.log( LogLevel.MOST, "%s: starting from starting page [%d]\n", function_name, startingPID.pid );
        PageId pid = startingPID;
        HFPage page = new HFPage();

        /* Go through the Directory Pages, starting from the Root */
        while( true ) { 
            Minibase.BufferManager.pinPage(pid, page, false);
            page.setCurPage(pid);
            if( page.getFreeSpace() >= DirectoryEntry.getRecSize() ) {
                Log.log( LogLevel.MOST, "%s: Found Directory Page [%d] for Directory Entry\n", function_name, pid.pid );
                /* found it */
                /* caller needs to unpin the page */
                return page;
            } else {
                PageId next_pid = page.getNextPage();
                if( next_pid.pid == -1 ) 
                    break;
                else {
                    Minibase.BufferManager.unpinPage(pid, false);
                    pid = next_pid;
                }
            }
        }

        /* if we are here, we did not find a Directory Page - need to allocate one */
        /* pid is pointing to the last page in the Directory Pages Linked List */
        Log.log( LogLevel.LESS, "%s: need to create new directory page!\n", function_name );

        HFPage newPage = new HFPage();
        PageId newPID = Minibase.BufferManager.newPage(newPage, 1);
        newPage.setCurPage(newPID);
        
        Log.log( LogLevel.MOST, "%s: created directory page [%d]\n", function_name, newPID.pid );

        page.setNextPage(newPID);
        newPage.setPrevPage(pid);

        Minibase.BufferManager.unpinPage(pid, true);

        /* caller needs to unpin this */
        return newPage;
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
        String function_name = "getFirstGreaterThan";
        Log.log( LogLevel.MOST, "%s: Number of Directory Entries [%d]\n", 
                function_name, page_capacity_array.size() );

        int found_index = -1;
        int found_capacity = Integer.MAX_VALUE;
        DirectoryEntry dirent = null;
        for( int i = 0; i < page_capacity_array.size(); i++ ) {
            dirent = page_capacity_array.get(i);

            Log.log( LogLevel.VERBOSE, "%s: Page Capacity [%d] Looking for Capacity [%d]\n", 
                    function_name, dirent.getPageCapacity(), capacity );

            if( dirent.getPageCapacity() > capacity ) {
                if( dirent.getPageCapacity() < found_capacity ) {
                    found_index = i;
                    found_capacity = dirent.getPageCapacity();
                }
            }
        }

        if( found_index >= 0 ) { 
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
        String function_name = "updateDirectoryEntry";

        Log.log( LogLevel.MOST, "%s:Setting up the Directory Page\n", function_name );
        /* update the directory with the new Directory Entry */
        HFPage page_dir = new HFPage();
        Log.log( LogLevel.MOST, "%s: Pinnning page [%d]\n", function_name, 
                            dirent.getOriginDirectoryPage().pid );
        Minibase.BufferManager.pinPage( dirent.getOriginDirectoryPage(), page_dir, false );
        page_dir.setCurPage( dirent.getOriginDirectoryPage() );

        if( Log.IsVerbose() ) {
            Log.log( LogLevel.VERBOSE, "%s: printing the Directory Page\n", function_name );
            page_dir.print();
        }

        /* insert the record */
        Tuple t = dirent.getTuple();
        RID rid = dirent.getRID();
        Log.log( LogLevel.MOST, "%s: length of tuple [%d]\n", function_name, t.getLength() );
        Log.log( LogLevel.MOST, "%s: RID for update: pid [%d] slot [%d]\n", function_name, rid.pageno.pid, rid.slotno );
        page_dir.updateRecord( dirent.getRID(), dirent.getTuple() );

        Log.log( LogLevel.MOST, "%s: Unpinning page [%d]\n", function_name, 
                            page_dir.getCurPage().pid );
        Minibase.BufferManager.unpinPage(page_dir.getCurPage(), true );

        Log.log( LogLevel.MOST, "%s: Updating In Memory Structures\n", function_name );

        updateDirectoryEntryInMemory(dirent);
    }

    public boolean doesPageIDExist(PageId pid) {
        return page_id_tree.contains( new DirectoryEntry(pid,-1,-1) );
    }

    public void insertRecCount() throws java.io.IOException { 
        String function_name = "insertRecCount";

        Log.log( LogLevel.MOST, "%s: inserting Record Count of zero\n", function_name );

        HFPage page = new HFPage();
        Minibase.BufferManager.pinPage(startingPID, page, false);
        page.setCurPage(startingPID);
        rid_reccount = page.insertRecord( (new RecCountEntry(0)).getByteArray() );
        Minibase.BufferManager.unpinPage(startingPID, true);
    }

    public void updateRecCount() throws java.io.IOException { 
        String function_name = "updateRecCount";

        Log.log( LogLevel.MOST, "%s: Updating RecCount to [%d]\n", function_name, reccount );
        HFPage page = new HFPage();
        Minibase.BufferManager.pinPage(startingPID, page, false);
        page.setCurPage(startingPID);
        RecCountEntry rce = new RecCountEntry(reccount);
        page.updateRecord(rid_reccount, rce.getTuple() );
        Minibase.BufferManager.unpinPage(startingPID, true);
    }

    public void incRecCount() throws java.io.IOException { 
        reccount++;
        updateRecCount();
    }

    PageId getStartingPID()     { return startingPID; }
    public int getRecCount()    { return reccount; }

    private int reccount;
    private RID rid_reccount;
    private PageId startingPID;
    private TreeSet<DirectoryEntry> page_id_tree;
    private ArrayList<DirectoryEntry> page_capacity_array;
}

public class HeapFile {
    /*
        - create a PageId object for page 0 - this is our Directory Page
        - create a file entry in the DB using DiskMgr.
    */
    public HeapFile(String name) throws java.io.IOException {

        /* create a new page for the Directory */
        HFPage page = new HFPage();
        PageId pid = Minibase.BufferManager.newPage(page, 1);
        Minibase.BufferManager.unpinPage(pid, false);

        Log.log( LogLevel.MOST, "Initializing Heapfile - allocating a new page [%d]\n", pid.pid ) ;

        directory = new Directory( pid );

        /* for now, assume it is a new file */
        Log.log( LogLevel.MOST, "Adding file entry [%s]\n", name );
        Minibase.DiskManager.add_file_entry(name, directory.getStartingPID() );

    }

    /*
        - get a Directory Entry that points to a page that we can insert to.
        - create Page and then a HFPage object.
        - insert the user record into this.
        - update the Directory Entry with the new information about the page.
    */
    public RID insertRecord(byte[] record) throws ChainException, java.io.IOException {

        String function_name = "insertRecord";
        Log.log( LogLevel.MOST, "%s: looking for Directory Entry with capacity [%d]\n", function_name, record.length );

        /* get the DirectoryEntry that points to the page we want */
        DirectoryEntry dirent = directory.getDirectoryEntryWithCapacity( record.length );

        HeapFileUtil.printNumPinnedPages( function_name );

        /* load the page from disk through the buffer manager */
        Log.log( LogLevel.MOST, "%s: Loading Data Page [%d]\n", function_name, dirent.getPageId().pid );
        HFPage page_data = new HFPage();
        Minibase.BufferManager.pinPage( dirent.getPageId(), page_data, false );
        page_data.setCurPage( dirent.getPageId() );

        /* insert the data */
        RID rid =  page_data.insertRecord(record);
        if( rid == null ) 
        {
            Log.log( LogLevel.NONE, "Error inserting record!\n" );
            Minibase.BufferManager.unpinPage( dirent.getPageId(), true );
            return null;
        } else {
            Log.log( LogLevel.MOST, "%s: After insertRecord RID.pid [%d] RID.slotno [%d]\n", 
                        function_name, rid.pageno.pid, rid.slotno );
        }

        /* update the Directory Entry with the new information */
        Log.log( LogLevel.MOST, "%s: Setting new capacity in Directory Entry [%d]\n", 
                        function_name, page_data.getFreeSpace() );
        dirent.setCapacity( page_data.getFreeSpace() );
        /* dirent.numRecords = hfp_data.... */

        if( Log.IsVerbose() ) {
            Log.log( LogLevel.VERBOSE, "%s: Printing the data page\n", function_name );
            page_data.print();
        }

        /* unpin the data page */
        //HeapFileUtil.releaseHFPage(hfp_data, true);

        Log.log( LogLevel.MOST, "%s: Updating Directory Entry\n", function_name );
        directory.updateDirectoryEntry(dirent);

        /* increment the number of records */
        directory.incRecCount();

        Log.log( LogLevel.MOST, "%s: Unpinning page [%d]\n", function_name, dirent.getPageId().pid );
        Minibase.BufferManager.unpinPage( dirent.getPageId(), true );

        return rid;
    }
    

    public int getRecCnt() {
        return directory.getRecCount();
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

        HFPage page_data = new HFPage();;
        Minibase.BufferManager.pinPage( rid.pageno, page_data, false ); 
        byte[] byteArray = page_data.selectRecord(rid);
        Minibase.BufferManager.unpinPage( rid.pageno, false );

        return new Tuple(byteArray, 0, byteArray.length);
    }

    public HeapScan openScan() { 
        return null;
    }

    Directory directory;
};


