package query;

import global.AttrType;
import global.Minibase;
import global.RID;
import heap.HeapFile;
import parser.AST_Insert;
import relop.FileScan;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

  /** Name of the table to create. */
  protected String fileName;

  /** Schema of the table to create. */
  protected Schema schema;
  
  /** Values of the tuple to insert. */
  protected Object [] values;
  
  /** Prints debug info when enabled */
  private static final boolean debug = true;
	
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {
    
	/* Get table name */
    fileName = tree.getFileName();
    
    /* Get tuple field values */
    values = tree.getValues();
    
    /* Validate existence of table */
    schema = QueryCheck.tableExists(fileName);
    
    /* Validate values */
    QueryCheck.insertValues(schema, values);
  	  
  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

	  /* Create tuple to insert */
	Tuple newTuple = new Tuple(schema, values);
	
    /* Open Heap File containing table */
    HeapFile fileHandle = new HeapFile(fileName);
 
    /* Keep track of tuples RID to store in the index later */
    RID tuplesRID;
    
    tuplesRID = fileHandle.insertRecord(newTuple.getData());
    
    /* Update index if there is one */
    IndexDesc[] indexs = Minibase.SystemCatalog.getIndexes(fileName);
    if(indexs.length > 0)    	
    {
    	System.out.println("The following indices were found");
    	for(int i = 0; i < indexs.length; i++) {
    		System.out.println(indexs[i].indexName);		
    	}
    }
    
    /* Update catalog statistics */
    
    /* print the output message */
    System.out.println("1 tuple insterted into " + fileName + " relation");

    if(debug) {
       schema.print();
       FileScan debugScan = new FileScan(schema, fileHandle);
       while(debugScan.hasNext()) {
          debugScan.getNext().print();
       }
       /* Prevent exception due to page still pinned when dropping table */
       debugScan = null;
       System.gc();
    }
    
  } // public void execute()

} // class Insert implements Plan
