package relop;

import global.AttrType;
import global.SearchKey;
import global.RID;

import heap.HeapFile;
import index.HashIndex;

import java.util.UUID;

/*
 *   This class centralizes all access points to the Index Scan object that we need.
 *
 *   There are three possibilities:
 *      1) The iterator passed is an Index Scan itself - we are done in this case.
 *      2) The iterator is a File Scan.  We need to create the HashIndex and then the Index Scan.
 *      3) Finally, the iterator is none of the above.  We need to create the HeapFile, the HashIndex, and the Index Scan
 */
class IndexScanCreator {
    private HeapFile heapFile;
    private HashIndex hashIndex;
    private IndexScan indexScan;
    private boolean ownHeapFile;
    private boolean ownHashIndex;
    private boolean ownIndexScan;

    public IndexScanCreator(Iterator iter, int colIndex) throws Exception {
        heapFile = null;
        hashIndex = null;

        ownHeapFile = false;
        ownHashIndex = false;
        ownIndexScan = false;

        if( colIndex < 0 || colIndex >= iter.getSchema().getCount() ) {
            throw new Exception( "Invalid column Index passed - please check the parameters!" );
        }

        if( iter instanceof IndexScan ) {
            /* we are done! */
            indexScan = (IndexScan)iter;
            return;
        }

        /* we need: Schema, HashIndex and HeapFile                          */
        /* if we have a FileScan, we do not need to create a HeapFile       */
        if( iter instanceof FileScan )  {
            /* we need to create a hashIndex alone */
            this.hashIndex = new HashIndex(null);
            ownHashIndex = true;
            FileScan fileScan = (FileScan) iter;

            while( fileScan.hasNext() ) {
                Tuple t = fileScan.getNext();
                this.hashIndex.insertEntry( new SearchKey( t.getField(colIndex) ), fileScan.currentRid );
            }

            indexScan = new IndexScan(fileScan.getSchema(), hashIndex, fileScan.heapFile);
            ownIndexScan = true;
        }
        else {
            /* create a HeapFile and populate with iter records */
            this.heapFile = new HeapFile(null);
            ownHeapFile = true;
            this.hashIndex = new HashIndex(null);
            ownHashIndex = true;

            while( iter.hasNext() ) {
                Tuple t = iter.getNext();
                RID rid = heapFile.insertRecord( t.getData() );
                this.hashIndex.insertEntry( new SearchKey( t.getField(colIndex) ), rid );
            }

            indexScan = new IndexScan(iter.getSchema(), hashIndex, heapFile);
            ownIndexScan = true;
        }
    }

    public void close() { 
        if( ownIndexScan ) { 
            //System.out.printf( "closing indexscan\n" );
            indexScan = null;
        }

        if( ownHashIndex ) { 
            //System.out.printf( "closing hashindex\n" );
            hashIndex = null;
        }

        if( ownHeapFile ) {
            //System.out.printf( "closing heapfile\n" );
            heapFile = null;
        }
    }

    public IndexScan getIndexScan() {
        return indexScan;
    }
}


public class HashJoin extends Iterator {

    private Iterator left;
    private int leftColIndex;

    private Iterator right;
    private int rightColIndex;

    private IndexScanCreator leftIndexScanCreator;
    private IndexScanCreator rightIndexScanCreator;

    // to indicate if we have initalized the joins 
    private boolean startJoin;

	// boolean variable to indicate whether the pre-fetched tuple is consumed or not
	private boolean nextTupleIsConsumed;

	// pre-fetched tuple
	private Tuple nextTuple; 

    /* these are used once we find matching entries in the currentHash for the rightTuple */
    private Tuple[] currentMatchedArray;
    private int currentMatchedArrayIndex;

    /* the tuple on the right iterator */
    private Tuple rightTuple;

    /* the current hash table */
    private HashTableDup currentHash;
    private int currentBucketIndex;

    private void init() {
        this.leftIndexScanCreator = null;
        this.rightIndexScanCreator = null;

        this.startJoin = false;

        this.nextTupleIsConsumed = true;

        this.nextTuple = null;

        this.currentMatchedArray = null;
        this.currentMatchedArrayIndex = -1;

        this.rightTuple = null;

        this.currentHash = null;
        this.currentBucketIndex = -1;
    }

    /**
     *  Constructs a HashJoin given a left iterator and right iterator and column
     *  indices into the iterators on which the equijoins are made.
     */
    public HashJoin( Iterator left, Iterator right, int leftColIndex, int rightColIndex ) {
        /* copy parameters */
        this.left = left;
        this.leftColIndex = leftColIndex;

        this.right = right;
        this.rightColIndex = rightColIndex;

		this.setSchema(Schema.join(left.getSchema(), right.getSchema())); 

        init();
	}
	
	/**
	 * Gives a one-line explanation of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
	    super.indent(depth);
	    System.out.println("Performs a HashJoin between the two iterators passed\n" );
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
        init();
        left.restart();
        right.restart();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
        if( startJoin && leftIndexScanCreator != null && rightIndexScanCreator != null )
            return true;
        return false;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
        /* close the iterators */
		this.left.close();
		this.right.close();

        this.rightIndexScanCreator.close();
        //System.out.printf( "closed rightindex..\n" );

        this.leftIndexScanCreator.close();
        //System.out.printf( "closed leftindex..\n" );
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 * 
	 */
	public boolean hasNext() {
        /* if the caller has not yet consumed the lookahead-fetch Tuple, simply return success */
        if( !this.nextTupleIsConsumed ) {
            // System.out.printf( "nextTuple is not consumed\n" );
            return true;
        }

        // if this is the first time, do the initialization:
        if( !startJoin ) { 
            // System.out.printf( "hasNext(): Initializing the HashJoin\n" );
            /* create the index scans */
            try {
                this.leftIndexScanCreator = new IndexScanCreator( left, leftColIndex );
                this.rightIndexScanCreator = new IndexScanCreator( right, rightColIndex );
            } catch( Exception e ) {
                /* tbd: what is the right thing here? */
                System.out.printf( "error while creating Index Scan: [%s]\n", e.getMessage() );
                return false;
            }

            currentMatchedArray = null;
            currentMatchedArrayIndex = -1;
        
            loadNextPartition();

            /* done with the initialization */
            startJoin = true;
        }

        /* 
         * normal course of action:
         *
         * state variables:
         *
         *  1) Left and Right Index Scan objects.
         *  2) Current Bucket Index.                            (currentBucketIndex)
         *  3) Current Bucket (Hash)                            (currentHash)
         *  4) Current matched array elements from Hash         (currentMatchedArray)
         *  5) Index into the current matched array elements    (currentMatchedArrayIndex)
         *  6) Right Tuple                                      (rightTuple)
         * 
         * Logic:
         *
         * 1) If the Left Index is less than the Right Index, we construct a new partition.
         * 2) If the Left Index is more than the Right Index, we throw away right tuples until we catch up 
         *    with the Left Index.
         * 3) Once we have the same bucket for both, we get ALL Tuples from the HashTable that match the 
         *    entry on the Right Index.
         * 4) We now go through this array to check if the keys match.
         * 5) If they match, we return SUCCESS and remember the state (the index into this matching array and others)
         * 6) If they do not match - we repeat from 1)
         *
         * Exit points:
         *
         * 1) If we found a match, we construct the matching Tuple and return TRUE.
         * 2) If the partition that we create from the Left Index is empty, we return FALSE.
         * 3) If hasNext() returns FALSE for the Right Index, we return FALSE.
         *
        */ 

        while( true ) {
            // array check:
            if( this.currentMatchedArray != null ) {
                this.currentMatchedArrayIndex++;
                for( ; this.currentMatchedArrayIndex < currentMatchedArray.length; 
                            this.currentMatchedArrayIndex++ ) {
                    // System.out.printf( "comparing Tuples\n" );
                    if( compareTuplesOnIndex(currentMatchedArray[currentMatchedArrayIndex], rightTuple) ) {
                        /* hit GOLD - matches */
				        nextTuple = Tuple.join(currentMatchedArray[currentMatchedArrayIndex], 
                                                rightTuple, this.getSchema());
                        nextTupleIsConsumed = false;
                        // System.out.printf( "Returning Successful Tuple\n" );
                        return true;
                    }
                }
            }

            // we need to construct the next "matching array"
            currentMatchedArray = null;
            currentMatchedArrayIndex = -1;

            // get next right tuple in the same bucket:
            IndexScan rightIndexScan = rightIndexScanCreator.getIndexScan();
            while( currentMatchedArray == null ) {
                /* Left has reached the end: we are done */
                if( !rightIndexScan.hasNext() ) return false;

                /* Left is ahead of right;  throw away tuples until right >= left */
                while( rightIndexScan.getNextHash() < currentBucketIndex ) {
                    if( !rightIndexScan.hasNext() ) return false;

                    rightIndexScan.getNext();
                } 
                
                /* Right is ahead of left;  get the next partition from the left */
                if( rightIndexScan.getNextHash() > currentBucketIndex ) {
                    /* if there is no partition, we are done */
                    if( !loadNextPartition() )
                        return false;
                } 

                /* left is on the same bucket as right;  get the next tuple from right */
                if( rightIndexScan.getNextHash() == currentBucketIndex ) {
                    if( !rightIndexScan.hasNext() ) return false;

                    rightTuple = rightIndexScan.getNext();
                    currentMatchedArray = currentHash.getAll( new SearchKey( rightTuple.getField(rightColIndex) ) );
                    currentMatchedArrayIndex = -1;
                }
            }
        }
	}


	/**
	 * loads the next partition into the Hash Table:
	 */
    private boolean loadNextPartition()
    {
        /* set the current bucket */
        IndexScan leftIndexScan = leftIndexScanCreator.getIndexScan();
        this.currentBucketIndex = leftIndexScan.getNextHash();

        /* create the current bucket */
        this.currentHash = new HashTableDup();

        while( leftIndexScan.hasNext() ) {
            if( leftIndexScan.getNextHash() > this.currentBucketIndex ) {
                break;
            }

            Tuple t = leftIndexScan.getNext();

            /* create an entry in the HashTableDup for this entry */
            currentHash.add( new SearchKey( t.getField(this.leftColIndex) ), t );
        }

        // System.out.printf("loadNextPartition: current bucket index %d\n", currentBucketIndex );

        return !currentHash.isEmpty();
    }

	/**
	 * Compares the two tuples based on the respective indexes:
     *
	 */
    private boolean compareTuplesOnIndex(Tuple leftTuple, Tuple rightTuple) { 
        if( leftTuple.schema.fieldType(leftColIndex) != rightTuple.schema.fieldType(rightColIndex) ) {
            /* we need to raise an execption */
            System.out.println( "Tuple Attributes do not match\n" );
            return false;
        }

        switch( leftTuple.schema.fieldType(leftColIndex) ) {
            case AttrType.INTEGER:
                // System.out.printf( "compareTuplesOnIndex: right Tuple Index [%d]\n", rightTuple.getIntFld(rightColIndex) );
                return leftTuple.getIntFld(leftColIndex) == rightTuple.getIntFld(rightColIndex);

            case AttrType.FLOAT:
                return leftTuple.getFloatFld(leftColIndex) == rightTuple.getFloatFld(rightColIndex);

            case AttrType.STRING:
                return leftTuple.getStringFld(leftColIndex).equals(rightTuple.getStringFld(rightColIndex));
        }

        return false;
    }

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
        if( !hasNext() ) {
            throw new IllegalStateException( "No more tuples" );
        }

        // System.out.printf( "getNext()\n" );

		nextTupleIsConsumed = true;
		return nextTuple;
	}

}


