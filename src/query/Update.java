package query;

import index.HashIndex;

import java.util.ArrayList;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;

import parser.AST_Update;
import relop.FileScan;
import relop.IndexScan;
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
	    
    /* Keep track of indices associated with this table */
    IndexDesc[] indexs = Minibase.SystemCatalog.getIndexes(fileName);
    HashIndex hashIndex = null;
    
    /* Tuples that qualify predicates are updated */
    int updatedRows = 0;
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
        /* Do update this record */
        if(isUpdateCandidate) {
        	
          /* Update this record in index if applicable */	
          if(indexs.length > 0)    	
          {
          	/* Loop through all the indices */
          	for(int i = 0; i < indexs.length; i++) {
          	   /* Open the index */
          	   hashIndex = new HashIndex(indexs[i].indexName);
          	           		   
          	   /* Walk through the list of fields to be updated */ 
               for(int j = 0; j < fieldNumbers.length; j++) {
             	    /* see if the index is associated with this field */
            	   if(indexs[i].columnName == schema.fieldName(fieldNumbers[j])) {
            	      /* Delete this index key/entry */ 
            		   hashIndex.deleteEntry( new SearchKey( t.getField(indexs[i].columnName) ), 
                  			   rid);
            		   /* Insert new key/entry with updated value */
                       hashIndex.insertEntry( new SearchKey( values[j]), rid );
            	   }
               }          	       
          	}
          } /* End of index update */

          	/* Now update the record in the actual table */
        	/* Update all the fields that need to be updated */
        	for(int k = 0; k < fieldNumbers.length; k++)
        	   t.setField(fieldNumbers[k], values[k]);
        	/* Update record using updated tuple */
        	fileHandle.updateRecord(rid, t.getData());
            /* Count the number of rows affected */
            updatedRows++;
        }
    }
    
    /* Assuming there are no STATS update to make because
     * the row count for this table remains the same.
     */
    
    /* Print debug info */
    IndexScan indexScan;
    if(debug) {
    	/* Reprint the table for debug */
        schema.print();
        scanner.restart();
        while(scanner.hasNext()) {
        	scanner.getNext().print();
        }
        System.out.println("Number of tuples for this table in catalog = " +
        		             Minibase.SystemCatalog.getRecCount(fileName));
        
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
    System.out.println(updatedRows + " rows updated from table " +
      fileName);

    /* Prevent exception due to page still pinned when dropping table */
    fileHandle = null;
    scanner = null;
    hashIndex = null;
    indexScan = null;
    System.gc();
  } // public void execute()

} // class Update implements Plan
