package query;

import global.Minibase;
import index.HashIndex;
import parser.AST_DropIndex;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {

  /** Name of the index to drop. */
  protected String indexName;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index doesn't exist
   */
  public DropIndex(AST_DropIndex tree) throws QueryException {

	// make sure the table exists
	indexName = tree.getFileName();
	QueryCheck.indexExists(indexName);

  } // public DropIndex(AST_DropIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // drop index
    HashIndex h = new HashIndex(indexName);
    h.deleteFile();
    Minibase.SystemCatalog.dropIndex(indexName);

    // print the output message
    System.out.println("Index " + indexName + " dropped.");
    h = null;
    System.gc(); // Prevents us from getting page currently pinned exception at random times
  } // public void execute()

} // class DropIndex implements Plan
