package query;

import index.HashIndex;

import java.util.ArrayList;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Delete;
import relop.FileScan;
import relop.IndexScan;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {

  /** Name of the table to create. */
  protected String fileName;

  /** Schema of the table to create. */
  protected Schema schema;
	  
  /** Values of the tuple to insert. */
  protected relop.Predicate[][] predicates;
  
  /** Number of ANDed predicates */
  int andPredicates;
	
  /** Prints debug info when enabled */
  private static final boolean debug = true;  
  
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
  public Delete(AST_Delete tree) throws QueryException {

	/* Get table name */
    fileName = tree.getFileName();

    /* Get predicates in Conjunctive Normal Form 
     * (a.k.a. product of sums, i.e. AND expression
     *  of OR expressions).
     */
    predicates = tree.getPredicates();
	    
    /* Validate existence of table */
    schema = QueryCheck.tableExists(fileName);
    
    /* Capture number of anded predicates */
    if(predicates.length > 0) {
    	andPredicates = predicates.length;
    }
    
    /* Validate predicates */
    for(int i = 0; i < andPredicates; i++)
    	/* For each AND predicate loop through all OR predicates */
    	for(int j = 0; j < predicates[i].length; j++)
    		predicates[i][j].validate(schema);
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

	/* Open Heap File containing table */
	HeapFile fileHandle = new HeapFile(fileName);
	/* Open table scanner */
    FileScan scanner = new FileScan(schema, fileHandle);

    ArrayList<RID> deleteRecordsArray = new ArrayList<RID>();
    
    /* Tuples that qualify predicates get marked for deletion */
    while( scanner.hasNext() ) {
        Tuple t = scanner.getNext();
        RID rid = scanner.getLastRID();
        /* Evaluate CNF predicates */
        boolean isDelCandidate = false;
        for(int i = 0; i < andPredicates; i++) {
        	isDelCandidate = false;
           	for(int j = 0; j < predicates[i].length; j++) {
           		/* Found a true in series of OR predicates */
           		if(predicates[i][j].evaluate(t)) {
           			isDelCandidate = true;
           			break;
           		}
           	}
           	/* All AND predicates need to evaluate to TRUE */
           	if(!isDelCandidate)
           	   break;
        }
        /* Add to list of records for deletion */
        if(isDelCandidate)
        	deleteRecordsArray.add(rid);
    }
    
    /* Update indices (if any). Do this before deleting tuples since
     * need tuples for reference at this point */
    IndexDesc[] indexs = Minibase.SystemCatalog.getIndexes(fileName);
    HashIndex hashIndex = null;
    if(indexs.length > 0)    	
    {
    	/* Loop through all the indices */
    	for(int i = 0; i < indexs.length; i++) {
    	   /* Open the index */
    	   hashIndex = new HashIndex(indexs[i].indexName);
    	 
    	   /* Delete from this index all the RIDs that qualified */
    	   for(int j = 0; j < deleteRecordsArray.size(); j++) {
    		   
    		   Tuple tempTuple = new Tuple(schema,
    				   fileHandle.selectRecord(deleteRecordsArray.get(j)));
        	   hashIndex.deleteEntry( new SearchKey( tempTuple.getField(indexs[i].columnName) ), 
        			   deleteRecordsArray.get(j));
    	    	
    	    }      
    	}
    }
    
    /* actually delete records from Table  */
    for(int i = 0; i < deleteRecordsArray.size(); i++)
    {
    	fileHandle.deleteRecord(deleteRecordsArray.get(i));
    }
    
    /* Update catalog statistics */
    int tuplesCount;
    tuplesCount = Minibase.SystemCatalog.decRecCount(fileName);
    if(debug)
    	System.out.println("Number of tuples for this table in catalog = " +
                            tuplesCount);
    
    /* Print the debug info */
    IndexScan indexScan;
    if(debug) {
    	/* Print contents of table */
        schema.print();
        scanner.restart();
        while(scanner.hasNext()) {
        	scanner.getNext().print();
        }
        /* Print contents of indices */
        if(indexs.length > 0)    	
        { 
        	  for(int i = 0; i < indexs.length; i++) {
        	     /* Open the index */
        	     hashIndex = new HashIndex(indexs[i].indexName);
                 System.out.println("Contents of index " + indexs[i].indexName + 
           		   " [" + indexs[i].columnName + "]");
     	         indexScan = new IndexScan(schema, hashIndex, fileHandle);
                 while(indexScan.hasNext()) {
         	        indexScan.getNext().print();
                 }
              }
        }

    }
    
    // print the output message
    System.out.println(deleteRecordsArray.size() + " rows deleted from table " +
      fileName);

    /* Prevent exception due to page still pinned when dropping table */
    fileHandle = null;
    scanner = null; // Doesn't hurt
    deleteRecordsArray = null; // Doesn't hurt
    hashIndex = null; // Doesn't hurt
    indexScan = null; // Doesn't hurt
    System.gc();
  } // public void execute()

} // class Delete implements Plan
