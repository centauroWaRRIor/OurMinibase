package query;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import global.Minibase;

import parser.AST_Select;

import heap.HeapFile;

import relop.Predicate;
import relop.Schema;
import relop.Iterator;
import relop.FileScan;
import relop.SimpleJoin;
import relop.Selection;
import relop.Projection;

/**
    This is the node in the Tree we construct with iterators.

    There are various constructors for each kind of iterators we support:
            File Scan, Selection, SimpleJoin, Projection.

    We differentiate between Selection and SelectionBeforeJoin

    SimpleJoin uses the left and right node. Others use the right alone.
    Except File Scan which uses neither.

    Each SelectNode encapsulates an Iterator.
**/
class SelectNode {
    public enum Type { FILESCAN, SELECTION, SELECTBEFOREJOIN, SIMPLEJOIN, PROJECTION };

    private Iterator iter;
    private SelectNode left;
    private SelectNode right;
    private String table_name;
    private Type type;
    private Schema schema;

    public SelectNode(String table_name, Schema schema) {
        this.table_name = table_name;
        this.type = Type.FILESCAN;
        this.iter = new FileScan( schema, new HeapFile(table_name) );
        this.schema = schema;

        this.left = null;
        this.right = null;
    }

    public SelectNode(SelectNode left, SelectNode right) { 
        this.left = left;
        this.right = right;
        schema = Schema.join(this.left.schema, this.right.schema);
        this.type = Type.SIMPLEJOIN;
        this.table_name = left.getName() + "|" + right.getName();

        this.iter = new SimpleJoin(left.iter, right.iter);
    }

    public SelectNode(SelectNode node, Predicate [] pred_array) {
        this.right = node;
        this.left = null;
        schema = node.schema;
        this.type = Type.SELECTION;
        this.table_name = "select" + "|" + node.getName();

        this.iter = new Selection(node.iter, pred_array);
    }

    public SelectNode(SelectNode node, ArrayList<Predicate> pred_arraylist) {
        this.right = node;
        this.left = null;
        schema = node.schema;
        this.type = Type.SELECTBEFOREJOIN;
        this.table_name = node.getName();

        /* convert the ArrayList into an Array */
        Predicate[] pred_array = new Predicate[ pred_arraylist.size() ];
        for( int i = 0; i < pred_arraylist.size(); i++ ) {
            pred_array[i] = pred_arraylist.get(i);
        }

        this.iter = new Selection(node.iter, pred_array);
    }

    public SelectNode(SelectNode node, String[] columns) {
        this.right = node;
        this.left = null;
        this.type = Type.PROJECTION;
        this.table_name = "projection" + "|" + node.getName();
        this.schema = new Schema(columns.length);

        Integer [] fields = new Integer[columns.length];
        for( int i = 0; i < columns.length; i++ ) {
            fields[i] = node.schema.fieldNumber(columns[i]);
            if( fields[i] == -1 ) { 
                System.out.printf( "cannot find column [%s] in schema!", columns[i] );
            }
            schema.initField(0, node.schema, fields[i] );
        }

        this.iter = new Projection(node.iter, fields);
    }

    public void close() {
        if( this.left != null ) this.left.close();
        if( this.right != null ) this.right.close();
        iter.close();
    }

    public Schema getSchema()       { return schema; }
    public String getName()         { return table_name; }
    public Iterator getIter()       { return iter; }
    public Type getType()           { return type; }
}

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

  private AST_Select tree;

  private HashMap<String, Schema> schemaTwoTables;
  private Schema [] schemaSingleTables;

  private ArrayList<SelectNode> nodeArray;
  private SelectNode currentTopNode;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
      this.tree = tree;
      currentTopNode = null;

      /* do checks that we have proper inputs */

      Schema s = new Schema(0);

      try { 
        /* validate the relations */
        String [] tables = tree.getTables();
        for( int i = 0; i < tables.length; i++ ) {
            Schema s2 = QueryCheck.tableExists(tables[i]);
            s = Schema.join(s, s2);
        }

        /* validate the columns */
        String [] cols = tree.getColumns();
        for( int i = 0; i < cols.length; i++ )  {
            QueryCheck.columnExists(s, cols[i]);
        }

        /* validate the predicates */
        Predicate [][] and_preds = tree.getPredicates();
        QueryCheck.predicates(s, and_preds);
      } catch( QueryException e ) {
            s = null;
            throw e;
      }

      /* Invoke the workhorse */
      buildTree();

  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    int nRows = 0;

    try { 
        /* We save the top node and start executing from there */
        checkCurrentTopNode();

        /* we explain always! */
        currentTopNode.getIter().explain(1);

        if( !tree.isExplain ) {
            nRows = currentTopNode.getIter().execute();
        } else {
            currentTopNode.close();
        }

    } catch( Exception e ) {
        System.out.println(e);
    }

    // check for JOINs and JOINLESS predicates:
    //printJoinInfo();

    //printPredicates();
    // print the output message

    System.out.printf( "[%d] rows fetched.", nRows );
  }

  private String keyForTwoTables(String table1_name, String table2_name) {
      return table1_name + "|" + table2_name;
  }

  private void checkCurrentTopNode() throws Exception {
      if( currentTopNode == null ) {
        throw new Exception ("null current top node!");
      }
  }

  /** 
        The sequence is:
            Build the File Scans. (Always! Everything else is optional)
            Check and add for Selects before File Scans.
            Add any SimpleJoins
            Add Selects that have not been pushed down yet.
            Add Projection (optional)
  **/
  private void buildTree() {
      initSingleTableSchemas();

      nodeArray = new ArrayList<SelectNode>();

      buildFileScans();

      addSelectsBeforeJoins();

      // currentTopNode is set after this:
      buildJoins();

      // add the selects now: This builds more nodes on top of currentTopNode:
      addSelects();

      // add the projects:
      addProjection();
  }

  private void dumpNodeArray() { 
      System.out.printf( "Number of nodes [%d]\n", this.nodeArray.size() );
  }

  /** 
    We get the list of tables from the Tree and create FileScan objects around them.

    Note that we store these in the nodeArray until they are assimilated by the SimpleJoins.
  **/
  private void buildFileScans() {
      String [] tables = tree.getTables();
      for( int i = 0; i < tables.length; i++ ) {
          nodeArray.add( new SelectNode( tables[i], Minibase.SystemCatalog.getSchema( tables[i] ) ) );
      }

      //System.out.printf( "End of FileScans\n" );
      //dumpNodeArray();
  }

  /**
     There is a row for each AND clause.
     Each row is a sequence of Predicates that have ORs.

     We look for rows that have Predicates that operate only on single Relations.

     We can push these Selects with the Predicates close to the FileScan.
  **/
  private void addSelectsBeforeJoins() {
    // go through the predicates:
    String [] tables = tree.getTables();
    Predicate [][] and_preds = tree.getPredicates();

    for(int row = 0; row < and_preds.length; row++ ) {
        Predicate [] or_pred = and_preds[row];

        // Keeps track of whether this row has columns from 2 different relations:
        boolean mixed = false;

        // we are going through one row of predicates:
        // each row is a sequence of ORs of individual predicates:
        for( int col = 0; col < or_pred.length; col++ ) {
            Predicate pred = or_pred[col];
            //System.out.printf ( "predicate [%d, %d] = [%s]\n", row, col, pred.toString() );

            int tableIndex = 0;
            for(tableIndex = 0; tableIndex < tables.length; tableIndex++ ) {
                if( pred.validate( schemaSingleTables[tableIndex] ) ) {
                    break;
                }
            }

            // if we did not find a match for a single table, look for two tables:
            if( tableIndex == tables.length ) { 
                mixed = true;
                break;
            }
        }

        //System.out.printf( "Predicate Row [%d] is [%s]\n", row, mixed ? "mixed" : "only single tables!" );
        if( !mixed ) {

            // we are being a little paranoid here:
            // we go through each relation and then pluck the Predicates for this relation.
            // Thus, we assume that a ROW may have relation specific predicates, but for different
            // relations!
            //
            // this is where we push the SELECTION inside the JOIN:
            for(int tableIndex = 0; tableIndex < tables.length; tableIndex++ ) {

                ArrayList<Predicate> pred_arraylist = new ArrayList<Predicate>();

                // go through each predicate and insert a SELECTION for that table:
                for( int col = 0; col < or_pred.length; col++ ) {
                    Predicate pred = or_pred[col];

                    /* if this predicate matches this table, store it */
                    if( pred.validate(schemaSingleTables[tableIndex]) ) {
                        pred_arraylist.add(pred);
                    }
                }

                if( pred_arraylist.size() > 0 ) {
                    createSelectNodeBeforeJoin(tableIndex, pred_arraylist);
                }
            }
        }
    } 

    //System.out.printf( "End of addSelectsBeforeJoins\n" );
    //dumpNodeArray();
  }

  /* manipulates the Node Array - we do not change the size - we just add another SelectNode layer */
  private void createSelectNodeBeforeJoin(int tableIndex, ArrayList<Predicate> pred_arraylist) {
      //System.out.printf( "SelectNode Before Join!\n" );

      if( nodeArray.size() > tableIndex ) { 
          SelectNode node = nodeArray.get(tableIndex);
          SelectNode newNode = new SelectNode(node, pred_arraylist);
          nodeArray.set(tableIndex, newNode);
      } else {
          System.out.printf( "Internal error - could not find node for FileScan!\n" );
      }
  }

  /* very similar to the SelectNodeBeforeJoin, but this occurs after a SIMPLEJOIN */
  private void createSelectNode(Predicate[] pred_array) {
      if( currentTopNode == null ) {
          System.out.printf( "no current node!\n" );
          return;
      }

      SelectNode newNode = new SelectNode( currentTopNode, pred_array);
      currentTopNode = newNode;
  }

  /* 
     we leverage the SelectNode to construct the parameter for Projection
     The only interesting thing is that we extract the field numbers from the Column Names
   */
  private void createProjectionNode(String [] columns) {
      if( currentTopNode == null ) {
          System.out.printf( "no current node!\n" );
          return;
      }

      SelectNode newNode = new SelectNode( currentTopNode, columns);
      currentTopNode = newNode;
  }

  /* 
    used for the second optimization where we keep track of the rec count of each relation

    each insert increments the counter and each delete decrements it 
  */
  private int getRecCount(int nodeIndex) {
      if( nodeIndex >= nodeArray.size() ) {
        System.out.printf( "nodeIndex is more than the size of the node array [%d]!\n", nodeIndex );
        return -1;
      }

      SelectNode node = nodeArray.get(nodeIndex);
      if( node.getType() == SelectNode.Type.FILESCAN || node.getType() == SelectNode.Type.SELECTBEFOREJOIN ) {
          /* these are the only types where we can use the record count */
          return Minibase.SystemCatalog.getRecCount(node.getName());
      }
      else {
          return -1;
      }
  }

  /*
    helper function to actually merge 2 nodes from the Node Array
    note that the size of the Node Array comes down by 1.

    Happens in a SIMPLEJOIN situation.

    Here is where we apply the second optimization and make sure that the first relation
    is the SMALLER one.

    This is valid currently only for FILESCAN and SELECTBEFOREJOIN nodes
  */
  private void mergeNodes(int outer, int inner) {
      //System.out.printf( "Size of Node Array [%d]\n", nodeArray.size() );

      /* Use the Catalog Stats to determine the order of the JOIN */
      int recCountInner = getRecCount(inner);
      int recCountOuter = getRecCount(outer);

      /*
      System.out.printf( "Merging [%s] (count: %d) with [%s] (count: %d)\n", 
                    nodeArray.get(outer).getName(), recCountOuter,
                    nodeArray.get(inner).getName(), recCountInner );
      */

      SelectNode newNode = null;
      SelectNode innerNode = nodeArray.get(inner);
      SelectNode outerNode = nodeArray.get(outer);

      if( recCountInner == -1 || recCountOuter == -1 ) {
        /* order does not matter in this case */
        newNode = new SelectNode( nodeArray.get(outer), nodeArray.get(inner) );
      } else {
        /* have the relation with smaller count to be the first one to merge */
        if( recCountInner < recCountOuter ) {
            newNode = new SelectNode( nodeArray.get(inner), nodeArray.get(outer) );
            if( Global.DEBUG ) {
                System.out.printf( "Merging [%s] (count:%d) with [%s] (count:%d)\n",
                            innerNode.getName(), recCountInner, outerNode.getName(), recCountOuter );
            }
        } else {
            newNode = new SelectNode( nodeArray.get(outer), nodeArray.get(inner) );
            if( Global.DEBUG ) {
                System.out.printf( "Merging [%s] (count:%d) with [%s] (count:%d)\n",
                            outerNode.getName(), recCountOuter, innerNode.getName(), recCountInner );
            }
        }
      }
      
      // remove the two elements and add this element:
      int first  = inner > outer ? inner : outer;
      int second = inner == first ? outer : inner;
      nodeArray.remove(first);
      nodeArray.remove(second);

      // inefficient: ok for now for readability:
      nodeArray.add(second, newNode);
  }

  /**
    We look for "mixed" rows and apply a SIMPLEJOIN that is a cross tab.

    We go through each predicate for a row:
        We use a combination of two relations from the Node Tree (since we may have merged earier)
        If we find a MATCH (as detected by the Predicate's Validate(), we merge those two.

  **/
  private void buildJoins() {
    //System.out.printf( "Inside buildJoins\n" );

    // go through the predicates:
    String [] tables = tree.getTables();
    Predicate [][] and_preds = tree.getPredicates();

    for(int row = 0; row < and_preds.length; row++ ) {
        Predicate [] or_pred = and_preds[row];

        boolean mixed = false;
        // we are going through one row of predicates:
        // each row is a sequence of ORs of individual predicates:
        for( int col = 0; col < or_pred.length; col++ ) {
            Predicate pred = or_pred[col];
            //System.out.printf ( "predicate [%d, %d] = [%s]\n", row, col, pred.toString() );

            int tableIndex = 0;
            for(tableIndex = 0; tableIndex < tables.length; tableIndex++ ) {
                if( pred.validate( schemaSingleTables[tableIndex] ) ) {
                    break;
                }
            }

            // if we did not find a match for a single table, look for two tables:
            if( tableIndex == tables.length ) { 
                mixed = true;

                /* see if we can have a join from the current nodes */
                for( int outer = 0; outer < nodeArray.size() - 1; outer ++ ) {
                    for( int inner = outer+1; inner < nodeArray.size(); inner++ ) {
                        Schema schema = Schema.join(nodeArray.get(outer).getSchema(), 
                                            nodeArray.get(inner).getSchema());

                        if( pred.validate(schema ) ) {
                            /* woohoo - found a match */
                            /* merge the two nodes */
                            mergeNodes(inner, outer);
                            break;
                        }
                    }
                }
            }
        }

        //System.out.printf( "Predicate Row [%d] is [%s]\n", row, mixed ? "mixed" : "only single tables!" );
        if( !mixed ) {
            // all predicates are single table predicates!
        }
    }

    //System.out.printf( "End of buildJoins -> number of nodes [%d]\n", nodeArray.size() );

    if( nodeArray.size() == 1 ) {
        currentTopNode = nodeArray.get(0);
    } else {
        System.out.printf( "buildJoins -> error -> more than one node in node array!\n" );
    }

  }

  private void addSelects() {

    // go through the predicates:
    String [] tables = tree.getTables();
    Predicate [][] and_preds = tree.getPredicates();

    for(int row = 0; row < and_preds.length; row++ ) {
        Predicate [] or_pred = and_preds[row];

        boolean mixed = false;
        // we are going through one row of predicates:
        // each row is a sequence of ORs of individual predicates:
        for( int col = 0; col < or_pred.length; col++ ) {
            Predicate pred = or_pred[col];
            //System.out.printf ( "predicate [%d, %d] = [%s]\n", row, col, pred.toString() );

            int tableIndex = 0;
            for(tableIndex = 0; tableIndex < tables.length; tableIndex++ ) {
                if( pred.validate( schemaSingleTables[tableIndex] ) ) {
                    break;
                }
            }

            // if we did not find a match for a single table, look for two tables:
            if( tableIndex == tables.length ) { 
                mixed = true;

                /* we use the entire row of predicates so we are done with this row */
                createSelectNode(or_pred);
                break;
            }
        }
    }
  }

  private void addProjection() {
    String [] columns = tree.getColumns();

    if( columns.length > 0 ) {
        createProjectionNode(columns);
    }
  }

  /* 
     we create single table schemas 
     we always check the Predicates against a single table before we check for JOINS
   */
  private void initSingleTableSchemas() { 
    // create a catalog object:
    String [] tables = tree.getTables();

    int tableIndex = 0;
    schemaSingleTables = new Schema [tables.length];
    for( tableIndex = 0; tableIndex < tables.length; tableIndex++ ) {
        schemaSingleTables[tableIndex] = Minibase.SystemCatalog.getSchema(tables[tableIndex]);
        if( schemaSingleTables[tableIndex].getCount() == 0 ) {
            System.out.printf( "Cannot get schema for table [%s]\n", tables[tableIndex] );
        }
    }
  }

  /*
    Some useful debugging functions:
  */
  private void printPredicates() {
    // print the predicates:
    String [] tables = tree.getTables();
    for(int i = 0; i < tables.length; i++ )
        System.out.printf( "Table [%d] = [%s]\n", i, tables[i] );

    String [] cols = tree.getColumns();
    for(int i = 0; i < cols.length; i++ )
        System.out.printf( "Column [%d] = [%s]\n", i, cols[i] );

    Predicate [][] preds_matrix = tree.getPredicates();
    for(int i = 0; i < preds_matrix.length; i++ ) {
        Predicate [] preds_rows = preds_matrix[i];
        for( int j = 0; j < preds_rows.length; j++ ) {
            Predicate pred = preds_rows[j];

            /*
            System.out.printf( "predicate [%d] left [%s] oper [%d] right [%s]\n",
                        i, pred.left.toString(), pred.right.toString(), pred.oper );
            */
            System.out.printf ( "predicate [%d, %d] = [%s]\n", i, j, pred.toString() );
        }
    }
  } // public void execute()

} // class Select implements Plan
