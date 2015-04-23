package query;

import java.util.ArrayList;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Delete;
import relop.FileScan;
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
    
    /* actually delete records from heapfile */
    for(int i = 0; i < deleteRecordsArray.size(); i++)
    {
    	fileHandle.deleteRecord(deleteRecordsArray.get(i));
    }

    /* Update indices */
    //TODO:
    
    /* Update catalog statistics */
    //TODO: Don't know how to yet
    
    if(debug) {
        schema.print();
        //FileScan debugScan = new FileScan(schema, fileHandle);
        scanner.restart();
        while(scanner.hasNext()) {
        	scanner.getNext().print();
        }
    }
    
    // print the output message
    System.out.println(deleteRecordsArray.size() + " rows deleted from table " +
      fileName);

    /* Prevent exception due to page still pinned when dropping table */
    fileHandle = null;
    scanner = null; // Doesn't hurt
    deleteRecordsArray = null; // Doesn't hurt
    System.gc();
  } // public void execute()

} // class Delete implements Plan
