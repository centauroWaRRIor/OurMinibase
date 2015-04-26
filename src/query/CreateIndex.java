package query;

import index.HashIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_CreateIndex;
import relop.FileScan;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

  /** Name of the index to create. */
  protected String indexName;

  /** Name of the table to index. */
  protected String ixTableName;

  /** Name of the column to index. */
  protected String ixColumnName;
  
  /** Schema of the table to index. */
  Schema ixTableSchema;
  
  /** Position of the column to index. */
  int ixColumnNumber;
  
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {

	Boolean okProceed = false;
    /* make sure the file doesn't already exist */
    indexName = tree.getFileName();
    try {
    	/* Makes sure there is no index with this name already */
       QueryCheck.indexExists(indexName);       
    } catch (QueryException exc) {
    	/* index doesn't exist yet, ok to proceed */
    	okProceed = true;
    }
    if(!okProceed)
        throw new QueryException("index " + indexName + " already exists");
    else
    	/* Makes sure there is no table with this name already */
        QueryCheck.fileNotExists(indexName);
    /* Parse table name to which this index belongs */
    ixTableName = tree.getIxTable();
    QueryCheck.tableExists(ixTableName);
    
    /* Collect index column, schema and column number */
    ixTableSchema = Minibase.SystemCatalog.getSchema(ixTableName);
    ixColumnName =  tree.getIxColumn();
    ixColumnNumber = QueryCheck.columnExists(ixTableSchema, ixColumnName);    	  
  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    /* File scan */
    HeapFile fileHandle = new HeapFile(ixTableName);
    FileScan scanner = new FileScan(ixTableSchema, fileHandle);
	    
    /* Create the hash index to build */
    HashIndex hashIndex = new HashIndex(indexName);

    int tupleCount = 0;
    
    /* Add existing table's tuples to index */ 
    while( scanner.hasNext() ) {
        Tuple t = scanner.getNext();
        RID rid = scanner.getLastRID();
        hashIndex.insertEntry( new SearchKey( t.getField(ixColumnNumber) ), rid );
        tupleCount++;
        
    }
    if(Global.DEBUG)
    	System.out.println(tupleCount + " entries added to newly created" + indexName + " index.");
    	

    /* add the index to the catalog */
    Minibase.SystemCatalog.createIndex(indexName, ixTableName, ixColumnName);
    
    /* Don't need to update STATS on an index. 
     * Assumption is that the amount of tuples is
     * equal to its respective table and that INSERT 
     * and DELETE do the STATS maintenance */

    /* print the output message */
    System.out.println("Index " + indexName + " created.");
    
  } // public void execute()

} // class CreateIndex implements Plan
