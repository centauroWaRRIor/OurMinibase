package query;

import parser.AST_CreateIndex;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {

  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println("(Not implemented)");

  } // public void execute()

} // class CreateIndex implements Plan
