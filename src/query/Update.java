package query;

import global.RID;
import heap.HeapFile;

import java.util.ArrayList;

import parser.AST_Update;
import relop.FileScan;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {

  /** Name of the table to create. */
  protected String fileName;

  /** Name of the columns to update. */
  protected String[] columnsToUpdate;

  /** Schema of the table to update. */
  protected Schema schema;
		  
  /** Predicates to qualify before updating. */
  protected relop.Predicate[][] predicates;
  
  /** Number of ANDed predicates */
  int totalAndPredicates;
  
  /** Field numbers corresponding to columns to update */
  protected int[] fieldNumbers;
  
  /** Values of the tuple to update. */
  protected Object [] values;
	
  /** Prints debug info when enabled */
  private static final boolean debug = true;  
  
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if invalid column names, values, or pedicates
   */
  public Update(AST_Update tree) throws QueryException {

     /* Get table name */
	 fileName = tree.getFileName();
	 
	 /* Get columns. */
	 columnsToUpdate = tree.getColumns();

	 /* Get predicates */
	 predicates = tree.getPredicates();
	 
	 /* Capture number of anded predicates */
	 if(predicates.length > 0) {
	   	totalAndPredicates = predicates.length;
	 }
	 
	 /* Get tuple field values */
	 values = tree.getValues();
	    
	 /* Validate existence of table */
	 schema = QueryCheck.tableExists(fileName);
	 
	 /* Validate values */
	 // TODO: Not applicable?
	 //QueryCheck.insertValues(schema, values);
	 
	 /* Get field numbers of attributes to update */
	 fieldNumbers = QueryCheck.updateFields(schema, columnsToUpdate);
	 
	 /* Use field numbers to validate value objects */
	 QueryCheck.updateValues(schema, fieldNumbers, values);
	  
  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

	/* Open Heap File containing table */
	HeapFile fileHandle = new HeapFile(fileName);
	/* Open table scanner */
    FileScan scanner = new FileScan(schema, fileHandle);
	    
    /* Tuples that qualify predicates are updated */
    int countUpdatedRecords = 0;
    while( scanner.hasNext() ) {
        Tuple t = scanner.getNext();
        RID rid = scanner.getLastRID();
        /* Evaluate CNF predicates */
        boolean isUpdateCandidate = false;
        for(int i = 0; i < totalAndPredicates; i++) {
        	isUpdateCandidate = false;
           	for(int j = 0; j < predicates[i].length; j++) {
           		/* Found a true in series of OR predicates */
           		if(predicates[i][j].evaluate(t)) {
           			isUpdateCandidate = true;
           			break;
           		}
           	}
           	/* All AND predicates need to evaluate to TRUE */
           	if(!isUpdateCandidate)
           	   break;
        }
        /* Update this record */
        if(isUpdateCandidate) {
        	/* Update all the fields that need to be updated */
        	for(int k = 0; k < fieldNumbers.length; k++)
        	   t.setField(fieldNumbers[k], values[k]);
        	/* Update record using updated tuple */
        	fileHandle.updateRecord(rid, t.getData());
        	countUpdatedRecords++;
        }
    }
    
    if(debug) {
    	/* Reprint the table for debug */
        schema.print();
        scanner.restart();
        while(scanner.hasNext()) {
        	scanner.getNext().print();
        }
    }
    
    // print the output message
    System.out.println(countUpdatedRecords + " rows updated from table " +
      fileName);

    /* Prevent exception due to page still pinned when dropping table */
    fileHandle = null;
    scanner = null;
    System.gc();
  } // public void execute()

} // class Update implements Plan
