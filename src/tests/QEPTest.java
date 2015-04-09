package tests;

import index.HashIndex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import relop.FileScan;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;

// YOUR CODE FOR PART3 SHOULD GO HERE.

public class QEPTest extends TestDriver {

	/** Folder path where test tables data is stored. */
	private static String FILE_PATH;
	
	/** Department table schema. */
	private static Schema s_department;

	/** Employee table schema. */
	private static Schema s_employee;
	
	/** Department table. */
	private HeapFile t_department;
	
	/** Employee table. */
	private HeapFile t_employee;

	/**
	 * Test application entry point; runs all tests.
	 */
	public static void main(String argv[]) {

		if(argv.length != 1 || argv[0].isEmpty()) {
			System.out.println("Error: No file path provided to main");
			return;
		}
		else 
			FILE_PATH = new String(argv[0]);
		
		// create a clean Minibase instance
		QEPTest qepT = new QEPTest();
		qepT.create_minibase();
		
		// initialize schema for the "Department" table
		s_employee = new Schema(5);
		s_employee.initField(0, AttrType.INTEGER, 4, "EmpId");
		s_employee.initField(1, AttrType.STRING, 20, "Name");
		s_employee.initField(2, AttrType.FLOAT, 4, "Age");
		s_employee.initField(3, AttrType.FLOAT, 4, "Salary");
		s_employee.initField(4, AttrType.INTEGER, 4, "DeptID");

		// initialize schema for the "Employee" table
		s_department = new Schema(4);
		s_department.initField(0, AttrType.INTEGER, 4, "DeptID");
		s_department.initField(1, AttrType.STRING, 30, "Name");
		s_department.initField(2, AttrType.FLOAT, 4, "MinSalary");
		s_department.initField(3, AttrType.FLOAT, 4, "MaxSalary");
		
		try {
			qepT.loadHeapFile("Department.txt");
			qepT.loadHeapFile("Employee.txt");
		} catch (IOException e) {
			System.out.println("Error(s) encountered during Textfile I/O");
			e.printStackTrace();
			return;			
		}
		
		boolean status = PASS;
		status &= qepT.query1();
		status &= qepT.query2();
		
		// display the final results
		System.out.println();
		if (status != PASS) {
			System.out.println("Error(s) encountered during queries.");
		} else {
			System.out.println("All queries completed; verify output for correctness.");
		}

	}

	private void loadHeapFile(String filename) throws IOException {

	   Tuple tuple;
	   File file = new File(FILE_PATH + filename);
	   
	   if(filename == "Department.txt") {
		   t_department = new HeapFile("Department");
		   tuple = new Tuple(s_department);
	   }
	   else if (filename == "Employee.txt"){
		   t_employee = new HeapFile("Employee");
		   tuple = new Tuple(s_employee);
	   }
	   else
		   throw new IOException("Filename is invalid");
	   
		System.out.println("Loading " + filename + " table");
	   
	   BufferedReader br;
	   br = new BufferedReader(new FileReader(file));
	   String line;
	   String[] retval; // Stores the tokens inside a scanned line
  	   line = br.readLine(); // Skip this line as it contains the schema
  	   line = br.readLine();
	   while (line != null) {
			//System.out.println(line); // Prints scanned line for debug
			// Parse the line into a tuple
			if(filename == "Employee.txt") { // Department tuples
			   // Quick code snippet to print all tokens in a line
			   //for (String retval: line.split(", ", 5)){
			      //System.out.println(retval);
				retval = line.split(", ", 5);
				tuple.setField("EmpId", Integer.parseInt(retval[0]));
				tuple.setField("Name", retval[1]);
				tuple.setField("Age", Float.parseFloat(retval[2]));
				tuple.setField("Salary", Float.parseFloat(retval[3]));
				tuple.setField("DeptID", Integer.parseInt(retval[4]));
				tuple.print();
				t_employee.insertRecord(tuple.getData());
			}
			else { // Department tuples 
				retval = line.split(", ", 4);
				tuple.setField("DeptID", Integer.parseInt(retval[0]));
				tuple.setField("Name", retval[1]);
				tuple.setField("MinSalary", Float.parseFloat(retval[2]));
				tuple.setField("MaxSalary", Float.parseFloat(retval[3]));
				tuple.print();
				t_department.insertRecord(tuple.getData());
			}
			line = br.readLine();	
	   }
	   br.close();
	}
	
	/**
	 * Display for each employee his Name and Salary
	 * SELECT e.Name, e.Salary
	 * FROM Employee e;
	 */
	protected boolean query1() {
		try {

			System.out.println("\nQuery 1: Display for each employee his Name and Salary");
			initCounts();
			saveCounts(null);
			FileScan scan = new FileScan(s_employee, t_employee);
			Projection pro = new Projection(scan, 1, 3);
			pro.execute();
			saveCounts("query1");

			// destroy temp files before doing final counts
			pro = null;
			scan = null;
			System.gc();

			// that's all folks!
			System.out.print("\n\nQuery 1 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nQuery 1 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean query1()

	/**
	 * Display the Name for the departments with MinSalary = 1000
	 * SELECT d.Name, d.MinSalary
	 * FROM Department d;
	 * WHERE d.MinSalary = 1000
	 */
	protected boolean query2() {
		try {

			System.out.println("\nQuery 2: Display the Name for the departments with MinSalary = 1000");
			initCounts();
			saveCounts(null);
			Predicate[] preds = new Predicate[] {
			new Predicate(AttrOperator.EQ, AttrType.COLNAME, "MinSalary", AttrType.FLOAT,
					1000F) };
			FileScan scan = new FileScan(s_department, t_department);
			Selection sel = new Selection(scan, preds);
			Projection pro = new Projection(sel, 1);
			pro.execute();
			saveCounts("query2");
			
			// destroy temp files before doing final counts
			pro = null;
			scan = null;
			System.gc();

			// that's all folks!
			System.out.print("\n\nQuery 2 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nQuery 2 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean query2()

	
}

//
//// test selection operator
//saveCounts(null);
//Predicate[] preds = new Predicate[] {
//		new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FLOAT,
//				65F),
//				new Predicate(AttrOperator.LT, AttrType.FIELDNO, 3, AttrType.FLOAT,
//						15F) };
//FileScan scan = new FileScan(s_drivers, file);
//Selection sel = new Selection(scan, preds);
//sel.execute();
//saveCounts("select");
//
//// test projection operator
//saveCounts(null);
//scan = new FileScan(s_drivers, file);
//Projection pro = new Projection(scan, 3, 1);
//pro.execute();
//saveCounts("project");
//
//// test simple pipelining
//saveCounts(null);
//System.out.println("\n  ~> selection and projection (pipelined)...\n");
//scan = new FileScan(s_drivers, file);
//sel = new Selection(scan, preds);
//pro = new Projection(sel, 3, 1);
//pro.execute();
//saveCounts("both");
//
//// test join operator
//saveCounts(null);
//System.out.println("\n  ~> test simple (nested loops) join...\n");
//preds = new Predicate[] { new Predicate(AttrOperator.EQ,
//		AttrType.FIELDNO, 0, AttrType.FIELDNO, 5) };
//SimpleJoin join = new SimpleJoin(new FileScan(s_drivers, file),
//		new FileScan(s_drivers, file), preds);
//pro = new Projection(join, 0, 1, 5, 6);
//pro.execute();
//
//// destroy temp files before doing final counts
//join = null;
//pro = null;
//sel = null;
//scan = null;
//keyscan = null;
//index = null;
//file = null;
//System.gc();
//saveCounts("join");
//
//// that's all folks!
//System.out.print("\n\nTest 1 completed without exception.");
//return PASS;
//
//} catch (Exception exc) {
//
//exc.printStackTrace(System.out);
//System.out.print("\n\nTest 1 terminated because of exception.");
//return FAIL;
//
//} finally {
//printSummary(6);
//System.out.println();
//}
