package query;

import global.AttrType;
import parser.AST_Describe;
import relop.Schema;

/**
 * Execution plan for describing tables.
 */
class Describe implements Plan {

  /** Name of the table to describe. */
  protected String fileName;

  /** Schema of the table to describe. */
  protected Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist
   */
  public Describe(AST_Describe tree) throws QueryException {

	/* Get table name */
    fileName = tree.getFileName();
	    	    
	/* Validate existence of table */
    schema = QueryCheck.tableExists(fileName);
	  
  } // public Describe(AST_Describe tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

	/* Print the schema (name of each column) */
	schema.print();	
	
	String fieldName;
	int fieldLength;
	/* Print type of each column */
	for(int i = 0; i < schema.getCount(); i++) {
		
		/* Collect info for print padding */
		fieldName = schema.fieldName(i);
		fieldLength = schema.fieldLength(i);
		
		switch(schema.fieldType(i)) {
           case AttrType.INTEGER:
              printType("INTEGER", fieldName.length(), fieldLength);
              break;

           case AttrType.FLOAT:
        	   printType("FLOAT", fieldName.length(), fieldLength);
        	   break;

           case AttrType.STRING:
        	   printType("STRING", fieldName.length(), fieldLength);
        	   break;

           case AttrType.COLNAME:
        	   printType("COLNAME", fieldName.length(), fieldLength);
        	   break;

           case AttrType.FIELDNO:
        	   printType("FIELDNO", fieldName.length(), fieldLength);  
        	   break;
		}
	}
	System.out.println("\n\n1 table described");
  } // public void execute()
  
  private void printType(String type, int fieldNameLength, int fieldLength) {

	  int len = 0;
	  /** Minimum column width for output. */
	  final int MIN_WIDTH = 10;

	  // print and space the column names
      System.out.print(type);

      // figure out the padding
      int collen = Math.max(fieldNameLength, MIN_WIDTH);
      if (type == "STRING") {
        collen = Math.max(fieldLength, collen);
      }
      len += collen;

      // pad the output to the field length
      for (int j = 0; j < collen - type.length(); j++) {
        System.out.print(' ');
      }
      //System.out.print('|');
   }

} // class Describe implements Plan
