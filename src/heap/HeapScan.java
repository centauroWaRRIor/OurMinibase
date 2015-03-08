package heap;

import java.util.Arrays;
import global.RID;
import global.Minibase;
import global.PageId;
import heap.Tuple;
import heap.HFPage;

import chainexception.ChainException;

public class HeapScan {
    public HeapScan(HeapFile hf) {
        heapFile = hf;
        initialized = false;
        pinnedDirectoryHeader = false;

        initialize();
    }

    protected void finalize() throws Throwable {
        cleanup();
    }

    public void close() throws ChainException {
        if( heapFile == null ) {
            throw new ChainException(null, 
            		"Attempt to close a heap scan without calling clean first");
        }

        String function_name = "close";
        cleanup();
        Log.log( LogLevel.MOST, "%s: End of closing\n", function_name );
    }

    public boolean hasNext() {
        return next_ridData == null ? false : true;
    }

    public void cleanup() { 
        unpinDirectoryPage();
        if( next_ridData != null ) {
            Minibase.BufferManager.unpinPage(next_ridData.pageno, false);
        }

        next_ridData = null;
        next_ridDirectory = null;
        next_pageData = null;
        initialized = false;
        pinnedDirectoryHeader = false;
        heapFile = null;
    }

    public void unpinDirectoryPage() {
        if( pinnedDirectoryHeader ) {
            String function_name = "unpinDirectoryPage";

            Log.log( LogLevel.LESS, "%s: Unpinning the Directory Page\n", function_name );
            Minibase.BufferManager.unpinPage(heapFile.directory.getStartingPID(), false);

            pinnedDirectoryHeader = false;
        }
    }

    /*
        1. First, ensure that we are pointing to a valid next_ridData.
        2. Get the data from the page.
        3. We need to set next_ridData to a valid RID.
        4. If the data page has another record, we are done.
        5. Otherwise go to 6.
        6. Unpin the current page since we are done.
        7. Get the next Directory RID - note that this may mean following the linked list.
           And this could be null.
        8. Set the page data.
    */
    public Tuple getNext(RID rid) {
        String function_name = "getNext";

        // get the data and move the pointer:
        if( next_ridData == null ) {
            // we should raise an exception here.
            Log.log( LogLevel.NONE, "%s:next_ridData is null - there is no next data...\n", function_name );

            // this is a bit of a kludge: :-( 
            unpinDirectoryPage();
            return null;
        }

        if( next_pageData == null ) {
            Log.log( LogLevel.NONE, "%s:next_pageData is null - there is no next data...\n", function_name );
            // this is a bit of a kludge: :-( 
            unpinDirectoryPage();
            return null;
        }

        /* Return the RID to the user */
        rid.copyRID( next_ridData );

        // retrieve the data:
        Log.log( LogLevel.MOST, "%s: RIDs:\n", function_name );
        printRIDs();

        byte[] ba = next_pageData.selectRecord(next_ridData);
        Tuple t =  new Tuple(ba, 0, ba.length);

        Log.log( LogLevel.VERBOSE, "%s: Returning data [%s]\n", function_name, 
                Arrays.toString(t.getTupleByteArray()) );

        /* this is the part that may need some reworking */
        gotoNext();

        return t;
    }

    void gotoNext() {
        String function_name = "gotoNext";

        /* if there is another record in the same page, we are done */
        if( next_pageData.hasNext(next_ridData) ) {
            next_ridData = next_pageData.nextRecord(next_ridData);
            /* next_pageData remains the same! */
        } else {
            /* unpin the current page since we are done */
            Minibase.BufferManager.unpinPage(next_pageData.getCurPage(), false);
            Log.log( LogLevel.LESS, "%s: Done with Data Page [%d]\n", function_name, next_ridData.pageno.pid );
            next_pageData = null;
            next_ridData = null;

            next_ridDirectory = heapFile.directory.getNextDirectoryRID(next_ridDirectory);
            setNextPageData();
        }
    }

    /*
        1. Call this function after setting next_ridDirectory.
        2. Get the pageid from the directory entry.
        3. Pin the HFPage for this pageid.
        4. Check if we have a valid record.
        5. If we do, we are done.  If not, go to next step.
        6. We will go to the next directory and continue the process.
    */
    private void setNextPageData() {
        String function_name = "setNextPageData";

        while( next_ridDirectory != null ) {
            PageId pid = heapFile.directory.getPageIdFromDirectoryRID( next_ridDirectory );

            Log.log( LogLevel.MORE, "%s: Setting current data page to [%d]\n", 
                        function_name, pid.pid );

            next_pageData = new HFPage();
            Minibase.BufferManager.pinPage(pid, next_pageData, false);
            next_pageData.setCurPage(pid);
            next_ridData = next_pageData.firstRecord();
            if( next_ridData == null ) {
                Minibase.BufferManager.unpinPage(pid, false);
                next_pageData = null;

                next_ridDirectory = heapFile.directory.getNextDirectoryRID( next_ridDirectory );
            } else {
                /* we are done */
                break;
            }
        }

    }

    public void printRIDs() {
        if( next_ridDirectory != null ) {
            Log.log( LogLevel.MOST, "Directory RID: pageno [%d] slot [%d]\n", 
                 next_ridDirectory.pageno.pid, next_ridDirectory.slotno );
        }

        if( next_ridData != null ) {
            Log.log( LogLevel.MOST, "Data RID: pageno [%d] slot [%d]\n", 
                next_ridData.pageno.pid, next_ridData.slotno );
        }
    }

    /*
        1. Pin the Directory Page out of the startingPID.
        2. Skip the first record since it has the number of records.
        3. Set next_ridDirectory to the second entry (this can be null).
        4. Call setNextPageData()
           This is useful to handle the case of empty data pages.
        4. Unpin the Directory Page.
    */
    private void initialize() {
        if( initialized ) {
            return;
        }
        else {
            initialized = true;
        }

        next_ridData = null;
        next_ridDirectory = null;

        String function_name = "initialize";
        Log.log( LogLevel.MOST, "%s: Opening the Directory Page\n", function_name );
        HFPage page_dir = new HFPage();
        Minibase.BufferManager.pinPage(heapFile.directory.getStartingPID(), page_dir, false);
        this.pinnedDirectoryHeader = true;
        page_dir.setCurPage(heapFile.directory.getStartingPID());

        /* get the next record for the Directory */

        /* skip the first record since it has the count of records */
        RID first = page_dir.firstRecord();
        if( page_dir.hasNext(first) ) { 
            next_ridDirectory = page_dir.nextRecord(first);
            setNextPageData();
        }
        else {
            /* we are done */
            Log.log( LogLevel.NONE, "%s: cannot find first record in Directory!\n", function_name );
        }

        // Minibase.BufferManager.unpinPage(heapFile.directory.getStartingPID(), false);

        Log.log( LogLevel.MOST, "%s: RIDs:\n", function_name );
        printRIDs();

        if( Log.IsVerbose() ) {
            Log.log( LogLevel.MOST, "%s: Printing data page\n", function_name );
            next_pageData.print();
        }
    }

    private boolean initialized;
    private boolean pinnedDirectoryHeader;
    private HeapFile heapFile;
    private RID next_ridData;
    private RID next_ridDirectory;
    private HFPage next_pageData;
};

