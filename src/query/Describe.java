package query;

import parser.AST_Describe;

/**
 * Execution plan for describing tables.
 */
class Describe implements Plan {

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist
   */
  public Describe(AST_Describe tree) throws QueryException {

  } // public Describe(AST_Describe tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println("(Not implemented)");

  } // public void execute()

} // class Describe implements Plan
