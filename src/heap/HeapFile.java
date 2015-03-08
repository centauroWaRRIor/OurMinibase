/*
TODO:

- In memory structure for checking capacity of pages
- Saving and re-running: is this required??
- Exceptions
- Update and Delete
- HeapScan: DONE
- Increase the number of Inserts (more data pages;  more directory pages): DONE

- Confirm if the number of pages I am getting is OK.  I got 7 pages for 100 records

- In HeapScan(), why does getNext() have a RID?  Are we supposed to populate it? DONE
- I am being conservative in unpinning in HeapScan.  The requirements are that I pin at least one
  page by the time the constructor is called.
  Even after all the data is over, I need to hold onto one page!!
  I am keeping the Directory Page for now.
  We need to clean this up during finalize();
- Need to use a type to identify a page to be a Directory Page


NOTES:
- I get an insufficient space error when there is exact space left (32 bytes VS 32 bytes)
  So I resorted to checking for strictly greater than which seems to work
- We may want to think of more test cases.  For example, I had forgotten to update the free
  space in both the in memory as well as the disk structures!
- We may want to do some testing with the number of records, for example.
- Current updateRecord disallows updates with different lengths than what is present currently in the DB.

*/
package heap;

import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Comparator;

import global.RID;
import global.Minibase;
import global.PageId;
import global.Convert;
import global.GlobalConst;

import heap.Tuple;
import heap.HFPage;

import chainexception.ChainException;

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

    public DirectoryEntry(byte [] ba) {
        String function_name = "DirectoryEntry(byte[] ba)";
        this.data = ba;
        if( ba.length != getRecSize() ) {
            Log.log( LogLevel.MOST, "%s: incorrect length for Byte Array; expected [%d] received [%d]\n",
                    function_name, getRecSize(), ba.length );
            /* TBD - need to raise an exception */
        }
        pid = new PageId( Convert.getIntValue(0, ba) );
        capacity = Convert.getIntValue(4, ba);
        num_records = Convert.getIntValue(8, ba);
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

    public RecCountEntry(byte[] ba) {
        String function_name = "RecCountEntry Constructor";

        if( ba.length != getRecSize() ) {
            /* throw an exception */
            Log.log( LogLevel.MOST, "%s: expected length [%d] received [%d]\n", 
                function_name, getRecSize(), ba.length );
        }
        data = ba;
        reccnt = Convert.getIntValue(0,data);
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
    int getCount() { return reccnt; }

    int reccnt;
    private byte[] data;
};

class ComparePIDDirectoryEntry implements Comparator<DirectoryEntry> {
    public int compare(DirectoryEntry dirent1, DirectoryEntry dirent2) {
        return( dirent1.getPageId().pid - dirent2.getPageId().pid );
    }
}

class Directory implements global.GlobalConst {
    public Directory(String name) throws java.io.IOException {

        String function_name = "Directory constructor";
        Log.log(LogLevel.MOST, "%s: checking if file [%s] exists\n", function_name, name );

        initialize();

        PageId pid = Minibase.DiskManager.get_file_entry( name );

        Log.log(LogLevel.MOST, "%s: PID after get_file_entry\n", function_name );

        if( pid == null || pid.pid == -1 ) {
            createFile(name);
        } else {
            readFile(pid);
        }
    }

    private void initialize() throws java.io.IOException {
        /* create our tree to manage the capacity */
        page_capacity_tree = new TreeMap<Integer, LinkedList<DirectoryEntry>>();
        page_id_tree = new TreeSet<DirectoryEntry> ( new ComparePIDDirectoryEntry() );

        /* remember the number of records */
        reccount = 0;
    }

    private void createFile(String name) throws java.io.IOException {

        String function_name = "createFile";
        Log.log( LogLevel.MOST, "%s: creating file [%s]\n", function_name, name );
        /* create a new page for the Directory */
        HFPage page = new HFPage();
        PageId pid = Minibase.BufferManager.newPage(page, 1);
        Minibase.BufferManager.unpinPage(pid, false);

        Log.log( LogLevel.MOST, "Initializing Heapfile - allocating a new page [%d]\n", pid.pid ) ;

        /* remember the startingPID */
        startingPID = pid;

        /* for now, assume it is a new file */
        Log.log( LogLevel.MOST, "Adding file entry [%s]\n", name );
        Minibase.DiskManager.add_file_entry(name, getStartingPID() );

        /* insert a zero record entry */
        insertRecCount();
    }

    /*
        1. Go through all Directory Pages.
        2. For each Directory Page, read all the records.
        3. If it is the first Directory Page, read the record count as well.
    */
    protected void readFile( PageId startingPID ) {

        String function_name = "readFile";

        /* remember the starting pid */
        this.startingPID = startingPID;

        /* outer loop goes through the linked list of Directory Page Entries */
        /* start with the root */
        for( PageId pid = startingPID; pid.pid != -1; ) 
        {
            Log.log( LogLevel.MOST, "%s: reading page [%d]\n", function_name, pid.pid );

            HFPage page_dir = new HFPage();
            Minibase.BufferManager.pinPage( pid, page_dir, false );
            page_dir.setCurPage( pid );

            /* special treatment for first Directory Page */
            RID rid = null;
            if( pid.pid == startingPID.pid ) {
                /* read the number of records */
                /* this will set rid_reccount as well */
                Log.log( LogLevel.MOST, "%s: setting record count\n", function_name );
                readRecCount(page_dir);
                rid = page_dir.nextRecord(rid_reccount);
            } else {
                rid = page_dir.firstRecord();
            }

            for( ;rid != null; rid = page_dir.nextRecord(rid) ) {
                byte[] ba = page_dir.selectRecord(rid);
                DirectoryEntry dirent = new DirectoryEntry(ba);

                /* save the RID */
                dirent.rid = new RID();
                dirent.rid.copyRID(rid);

                /* update in memory structures */
                updateDirectoryEntryInMemory(dirent);
            }
            PageId pid_next = page_dir.getNextPage();
            Minibase.BufferManager.unpinPage(pid, false);
            pid = pid_next;
        }
        Log.log( LogLevel.MOST, "%s: Done reading file!\n", function_name );
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

    public void updateDirectoryEntryInMemory(DirectoryEntry dirent) { 
        /* add in page id tree */
        page_id_tree.add(dirent);
        /* add in page capacity tree */
        Map.Entry<Integer, LinkedList<DirectoryEntry>> entry;
        entry = page_capacity_tree.ceilingEntry(dirent.getPageCapacity());
        LinkedList<DirectoryEntry> list = null;
        if(entry != null && 
           entry.getKey() == dirent.getPageCapacity()) {
          /* If there is already an entry with the same page capacity key in the 
           * tree, then append to exiting linked list value. Note that a Linked List
           * is as this TreeMap value because TreeMaps don't allow repetitions. 
           * Also note that Addition and search are still O(log n) because 
           * insertion/addition to the head of a list is O(1)
           */
           list = entry.getValue();
           list.addFirst(dirent);
        }
        else {
        	/* If there is no entry in the tree with this exact capacity key then
        	 * create a new K,V entry for future insertions with this same
        	 * capacity key.
        	 */
        	list = new LinkedList<DirectoryEntry>();
        	list.add(dirent);
        	page_capacity_tree.put(dirent.getPageCapacity(), new LinkedList<DirectoryEntry>(list));
        }
    }

    /*
     * O(log n) for searching the tree plus O(1) for removing from head of list
     * equals O(log n) performance.
     */
    public DirectoryEntry getFirstGreaterThan(int capacity) {
        String function_name = "getFirstGreaterThan";
        Log.log( LogLevel.MOST, "%s: Number of Directory Entries [%d]\n", 
                function_name, page_capacity_tree.size() );

        DirectoryEntry resultDirent = null;
        Map.Entry<Integer, LinkedList<DirectoryEntry>> entry;
        entry = page_capacity_tree.ceilingEntry(capacity);
        /* Check if such key was found in the tree.
         * Important Note: CeilingEntry returns an entry
         * that is greater or equal to the key (capacity). 
         * However, we want to restrict the search to strictly 
         * greater because we need to account for the space needed 
         * for storing not only the record but also the RID
         * in the heap page (4 extra bytes).        
         */
        if(entry != null && entry.getKey() > capacity ) {
            LinkedList<DirectoryEntry> list = entry.getValue();
            resultDirent = list.removeFirst();
            /* Remove key from tree if this was the only value 
             * in the linked list.
             */
            if(list.isEmpty()) {
            	page_capacity_tree.remove(entry.getKey());
            }
            /* to be consistent, we will remove it from the page_id_tree as well */
            page_id_tree.remove(resultDirent);
            return resultDirent;
        }
        else 
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

    /*
        reads the record count from the file
    */
    private void readRecCount(HFPage page_dir) {
        String function_name = "readRecCount";

        Log.log( LogLevel.MOST, "%s: page id [%d]\n", function_name, page_dir.getCurPage().pid );
        this.rid_reccount = page_dir.firstRecord();
        if( rid_reccount == null ) {
            /* TBD - throw an exception */
            Log.log( LogLevel.MOST, "%s: got a NULL record count record!\n" );
            return;
        }

        byte[] ba = page_dir.selectRecord(rid_reccount);
        RecCountEntry rce = new RecCountEntry(ba);
        this.reccount = rce.getCount();

        Log.log( LogLevel.LESS, "%s: read count from file [%d]\n", function_name,  this.reccount );
    }

    public void incRecCount() throws java.io.IOException { 
        reccount++;
        updateRecCount();
    }

    public void decRecCount() throws java.io.IOException { 
        reccount--;
        updateRecCount();
    }

    /*
        Ideally we need to validate that the rid_dir belongs to the set of 
        Directory Pages - we will assume so now.

        1. Construct a HFPage from rid's pageno
        2. Read the data pointed by this rid as a Byte Array.
        3. Convert to a Directory Entry.
        4. Unpin HFPage
        5. Return the page id.
    */
    public PageId getPageIdFromDirectoryRID(RID rid_dir) {
        HFPage page = new HFPage();
        Minibase.BufferManager.pinPage(rid_dir.pageno, page, false);
        page.setCurPage(rid_dir.pageno);
        byte[] ba = page.selectRecord(rid_dir);
        DirectoryEntry dirent = new DirectoryEntry(ba);
        PageId pid = dirent.getPageId();
        Minibase.BufferManager.unpinPage(rid_dir.pageno, false);
        return pid;
    }

    /* 
        1. rid_current is a valid RID for a Directory Entry.
        2. Check if the current directory page has any more.
        3. If yes for 2, we are done.
        4. If no, check if there is a next page in the Directory 
        5. Repeat 2.
    */
    public RID getNextDirectoryRID( RID rid_dir ) {
        String function_name = "getNextDirectoryRID";

        RID rid = null;

        PageId pid = rid_dir.pageno;
        HFPage page_dir = new HFPage();
        Minibase.BufferManager.pinPage(pid, page_dir, false);
        page_dir.setCurPage(pid);

        /* if there is another Directory Entry in this page, we are done */
        if( page_dir.hasNext(rid_dir) ) {

            rid = page_dir.nextRecord(rid_dir);
            Log.log( LogLevel.MORE, "%s: Found another directory entry pageno [%d] slotno [%d]\n",
                        function_name, rid.pageno.pid, rid.slotno );
            Minibase.BufferManager.unpinPage(pid, false);
            return rid;
        }

        PageId next_pid = page_dir.getNextPage();
        /* unpin the old page */
        Minibase.BufferManager.unpinPage(pid, false);

        while( next_pid.pid != -1 ) {
            /* pin this page */
            page_dir = new HFPage();
            Minibase.BufferManager.pinPage(next_pid, page_dir, false);
            page_dir.setCurPage(next_pid);

            Log.log( LogLevel.MOST, "%s: checking next directory page [%d]\n", function_name, next_pid.pid );

            /* check if there is a single record in this Directory Page */
            rid = page_dir.firstRecord();
            if( rid != null ) {
                Minibase.BufferManager.unpinPage(next_pid, false);
                return rid;
            }
        }

        Log.log( LogLevel.MOST, "%s: Done with all directory entries\n", function_name );
        /* we are done now! */
        return null;
    }

    /*
        1. Locate the Page with this RID in the Directory using the In Memory Tree.
        2. If not found - error.
        3. Remove the data from the Page first.  This may make the Page empty, but we are not cleaning it.
        4. Update Directory Entry with the appropriate space.
           Note that this needs to be done for In Memory as well as the database.
        5. Decrement the number of objects and update Entry.
    */
    public boolean deleteRecord( RID rid ) throws java.io.IOException {
        String function_name = "deleteRecord";

        DirectoryEntry dirent = page_id_tree.ceiling( new DirectoryEntry( rid.pageno, -1, -1 ) );
        if( dirent == null  || (dirent.getPageId().pid != rid.pageno.pid) ) {
            /* TBD - need to raise an exception */
            Log.log( LogLevel.MOST, "%s: could not find [%d] to delete\n", function_name, rid.pageno.pid );
            return false;
        }

        Log.log( LogLevel.MOST, "%s: Found the Directory Entry for RID\n", function_name  );

        /* now, delete the entry in the data page */
        HFPage page_data = new HFPage();
        Minibase.BufferManager.pinPage( rid.pageno, page_data, false );
        page_data.setCurPage(rid.pageno);
        page_data.deleteRecord( rid );

        /* save the new free space */
        dirent.setCapacity( page_data.getFreeSpace() );

        Minibase.BufferManager.unpinPage( rid.pageno, true );
        Log.log( LogLevel.MORE, "%s: Deleted record [%d] slotno [%d]\n", 
                function_name, rid.pageno.pid, rid.slotno );

        /* now update the Directory Entry with the available space */
        HFPage page_dir = new HFPage();
        Minibase.BufferManager.pinPage( dirent.getOriginDirectoryPage(), page_dir, false );
        page_dir.setCurPage( dirent.getOriginDirectoryPage() );

        page_dir.updateRecord( dirent.getRID(), dirent.getTuple() );

        Log.log( LogLevel.MORE, "%s: Updated directory entry [%d] slotno [%d] with new freespace [%d]\n", 
                function_name, dirent.getRID().pageno.pid, dirent.getRID().slotno, dirent.getPageCapacity() );
        Minibase.BufferManager.unpinPage( dirent.getOriginDirectoryPage(), true );

        /* finally, decrement the counter for the number of records */
        decRecCount();

        return true;
    }

    /*
        1. Make sure this is a page we manage from the Directory.
        2. Check if the lengths are OK and then update the record.
        3. Nothing to update in the Directory Header or Record Count!

        We are not handling record sizes that are of different sizes i.e. you cannot 
        update a record with a different size for now.
    */
    public void updateRecord(RID rid, Tuple t)
    		throws InvalidUpdateException {
        String function_name = "updateRecord";

        DirectoryEntry dirent = page_id_tree.ceiling( new DirectoryEntry( rid.pageno, -1, -1 ) );
        if( dirent == null  || (dirent.getPageId().pid != rid.pageno.pid) ) {
            /* TBD - need to raise an exception */
            Log.log( LogLevel.MOST, "%s: could not find [%d] to delete\n", function_name, rid.pageno.pid );
            throw new InvalidUpdateException(null, new String("Could not find" + rid.pageno.pid));
        }

        Log.log( LogLevel.MOST, "%s: Found the Directory Entry for RID\n", function_name  );

        /* now, update the entry in the data page */
        HFPage page_data = new HFPage();
        Minibase.BufferManager.pinPage( rid.pageno, page_data, false );
        page_data.setCurPage(rid.pageno);

        /* check for length */
        byte[] ba = page_data.selectRecord(rid);
        if( ba.length != t.getLength() ) {
            Log.log( LogLevel.MOST, "%s: different record lengths: original: [%d], new: [%d]\n",
                    function_name, ba.length, t.getLength() );
            throw new InvalidUpdateException(null, new String("different record lengths: original: " + 
            		ba.length + ", new: " + t.getLength()));
            
        }

        page_data.updateRecord( rid, t );

        Minibase.BufferManager.unpinPage( rid.pageno, true );
        Log.log( LogLevel.MORE, "%s: Deleted record [%d] slotno [%d]\n", 
                function_name, rid.pageno.pid, rid.slotno );
    }

    public int getRecCount()    { return reccount; }
    PageId getStartingPID()        { return startingPID; }

    private int reccount;
    private RID rid_reccount;
    private PageId startingPID;
    private TreeSet<DirectoryEntry> page_id_tree;
    private TreeMap<Integer, LinkedList<DirectoryEntry>> page_capacity_tree;
}

public class HeapFile implements global.GlobalConst {
    /*
        - create a PageId object for page 0 - this is our Directory Page
        - create a file entry in the DB using DiskMgr.
    */
    public HeapFile(String name) throws java.io.IOException {
        String function_name = "HeapFile constructor";
        Log.log(LogLevel.MOST, "%s: checking if file [%s] exists\n", function_name, name );

        directory = new Directory(name);
    }

    /*
        - get a Directory Entry that points to a page that we can insert to.
        - create Page and then a HFPage object.
        - insert the user record into this.
        - update the Directory Entry with the new information about the page.
    */
    public RID insertRecord(byte[] record) throws ChainException, java.io.IOException {

        String function_name = "insertRecord";
        Log.log( LogLevel.MOST, "%s: looking for Directory Entry with capacity [%d]\n", 
                    function_name, record.length );

        /* check for the length of the record */
        if( record.length > MAX_TUPSIZE ) {
            throw new SpaceNotAvailableException( "insertRecord - Tuple too big." );
            //throw new HeapFileException(null, "hey");
        }

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
            Log.log( LogLevel.MOST, "Error inserting record!\n" );
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
        String function_name = "updateRecord";

        Log.log( LogLevel.MOST, "%s: updating RID pageno [%d] slotno [%d]\n", 
                function_name, rid.pageno.pid, rid.slotno );

        boolean success = false;
        try {
            directory.updateRecord(rid, newRecord);
            success = true;
        } catch( InvalidUpdateException e ) {
            throw(new InvalidUpdateException(e, "Error updating record." ));
        }

        return success;
    }

    public boolean deleteRecord(RID rid) throws ChainException {
        String function_name = "deleteRecord";

        Log.log( LogLevel.MOST, "%s: Deleting RID pid [%d] slotno [%d]\n", 
                function_name, rid.pageno.pid, rid.slotno );
        boolean success = false;
        try {
            success = directory.deleteRecord(rid);
        } catch( java.io.IOException e) {
            throw new HeapFileException(e, "Delete Record Failed." );
        }

        return success;
    }

    /*
        1. check if we have the PageID in our directory.
        2. If yes, make a HFPage out of the PageID and get the record.
    */
    public Tuple getRecord(RID rid) {
        if( !directory.doesPageIDExist(rid.pageno) ) 
            return null;

        HFPage page_data = new HFPage();
        Minibase.BufferManager.pinPage( rid.pageno, page_data, false ); 
        byte[] byteArray = page_data.selectRecord(rid);
        Minibase.BufferManager.unpinPage( rid.pageno, false );

        return new Tuple(byteArray, 0, byteArray.length);
    }

    public HeapScan openScan() { 
        return new HeapScan(this);
    }

    protected Directory directory;

};

