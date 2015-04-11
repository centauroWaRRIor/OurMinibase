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
		status &= qepT.query3();
		status &= qepT.query4();
		status &= qepT.query5();
		status &= qepT.query6();
		status &= qepT.query7();
		status &= qepT.query8();
		
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
			sel = null;
			preds = null;
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

	/**
	 * Display the Name for the departments with MinSalary = MaxSalary
	 * SELECT d.Name
	 * FROM Department d;
	 * WHERE d.MinSalary = d.MaxSalary
	 */
	protected boolean query3() {
		try {

			System.out.println("\nQuery 3: Display the Name for the departments with MinSalary = MaxSalary");
			initCounts();
			saveCounts(null);
			Predicate[] preds = new Predicate[] {
			new Predicate(AttrOperator.EQ, AttrType.COLNAME, "MinSalary", AttrType.COLNAME,
					"MaxSalary") };
			FileScan scan = new FileScan(s_department, t_department);
			Selection sel = new Selection(scan, preds);
			Projection pro = new Projection(sel, 1);
			pro.execute();
			saveCounts("query3");
			
			// destroy temp files before doing final counts
			pro = null;
			scan = null;
			preds = null;
			sel = null;
			System.gc();

			// that's all folks!
			System.out.print("\n\nQuery 3 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nQuery 3 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean query3()
	
	/**
	 * Display the Name for employees whose Age > 30 and Salary < 1000
	 * SELECT e.Name
	 * FROM Employee e;
	 * WHERE e.Age = d.Salary
	 */
	protected boolean query4() {
		try {

			System.out.println("\nQuery 4: Display the Name for employees whose Age > 30 and Salary < 1000");
			initCounts();
			saveCounts(null);
			Predicate predAge = new Predicate(AttrOperator.GT, AttrType.COLNAME, "Age", AttrType.FLOAT,
					30F);
			Predicate predSalary = new Predicate(AttrOperator.LT, AttrType.COLNAME, "Salary", AttrType.FLOAT,
					1000F);
			
			FileScan scan = new FileScan(s_employee, t_employee);
			Selection selAge = new Selection(scan, predAge);
			Selection selSalary = new Selection(selAge, predSalary);
			Projection pro = new Projection(selSalary, 1);
			pro.execute();
			saveCounts("query4");
			
			// destroy temp files before doing final counts
			pro = null;
			scan = null;
			pro = null;
			scan = null;
			predAge = null;
			predSalary = null;
			selAge = null;
			selSalary = null;
			System.gc();

			// that's all folks!
			System.out.print("\n\nQuery 4 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nQuery 4 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean query4()
	
	/**
	 * For each employee, display his Salary and the name of his department
	 * SELECT e.Name, d.Name
	 * FROM Employee e, Department d;
	 * WHERE e.DeptId = d.DeptId
	 */
	protected boolean query5() {
		try {

			System.out.println("\nQuery 5: For each employee, display his Salary and the name of his department");
			initCounts();
			saveCounts(null);
			Predicate[] preds = new Predicate[] { new Predicate(AttrOperator.EQ,
			AttrType.FIELDNO, 4, AttrType.FIELDNO, 5) };
			FileScan scan1 = new FileScan(s_employee, t_employee);
			FileScan scan2 = new FileScan(s_department, t_department);
	        SimpleJoin join = new SimpleJoin(scan1, scan2, preds);
			Projection pro = new Projection(join, 3, 6);
			pro.execute();
			saveCounts("query5");
			
			// destroy temp files before doing final counts
			pro = null;
			scan1 = null;
			scan2 = null;
			preds = null;
			join = null;
			System.gc();

			// that's all folks!
			System.out.print("\n\nQuery 5 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nQuery 5 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean query5()
	
	/**
	 * Display the Name and Salary for employees who work in the department that has a DeptId = 3.
	 * SELECT e.Name, e.Salary
	 * FROM Employee e
	 * WHERE e.DeptId = 3
	 */
	protected boolean query6() {
		try {

			System.out.println("\nQuery 6: Display the Name and Salary for employees who work in the department that has a DeptId = 3.");
			initCounts();
			saveCounts(null);
			Predicate[] pred = new Predicate[] { new Predicate(AttrOperator.EQ,
			AttrType.COLNAME, "DeptId", AttrType.INTEGER, 3) };
			FileScan scan = new FileScan(s_employee, t_employee);
			Selection sel = new Selection(scan, pred);
			Projection pro = new Projection(sel, 1, 3);
			pro.execute();
			saveCounts("query6");
			
			// destroy temp files before doing final counts
			pro = null;
			scan = null;
			sel = null;
			System.gc();

			// that's all folks!
			System.out.print("\n\nQuery 6 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nQuery 6 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean query6()
	
	/**
	 * Display the Salary for each employee who works in a department that has MaxSalary > 100000
	 * SELECT e.Salary
	 * FROM Employee e, Department d;
	 * WHERE e.DeptId = d.DeptId &&
	 *       d.MaxSalary > 100000
	 */
	protected boolean query7() {
		try {

			System.out.println("\nQuery 7: Display the Salary for each employee who works in a department that has MaxSalary > 100000");
			initCounts();
			saveCounts(null);
			
			Predicate[] predD = new Predicate[] { new Predicate(AttrOperator.GT,
			AttrType.COLNAME, "MaxSalary", AttrType.FLOAT, 100000F) };
			FileScan scanD = new FileScan(s_department, t_department);
			Selection selD = new Selection(scanD, predD);
			
			Predicate[] predJoin = new Predicate[] { new Predicate(AttrOperator.EQ,
			AttrType.FIELDNO, 0, AttrType.FIELDNO, 8) }; // d.DeptId == e.DeptId
			FileScan scanE = new FileScan(s_employee, t_employee);
	        SimpleJoin join = new SimpleJoin(selD, scanE, predJoin);
			
			Projection pro = new Projection(join, 7);

			pro.execute();
			
			saveCounts("query7");
			
			// destroy temp files before doing final counts
			pro = null;
			selD = null;
			scanD = null;
			scanE = null;
			predD = null;
			predJoin = null;
			join = null;
			System.gc();

			// that's all folks!
			System.out.print("\n\nQuery 7 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nQuery 7 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean query7()

	/**
	 * Display the Name for each employee whose Salary is less than the MinSalary of his department
	 * SELECT e.Name
	 * FROM Employee e, Department d;
	 * WHERE e.DeptId = d.DeptId &&
	 *       e.Salary < d.MinSalary
	 */
	protected boolean query8() {
		try {

			System.out.println("\nQuery 8: Display the Name for each employee whose Salary is less than the MinSalary of his department");
			initCounts();
			saveCounts(null);
						
			Predicate[] predJoin = new Predicate[] { new Predicate(AttrOperator.EQ,
			AttrType.FIELDNO, 4, AttrType.FIELDNO, 5) }; // e.DeptId == d.DeptId
			FileScan scanE = new FileScan(s_employee, t_employee);
			FileScan scanD = new FileScan(s_department, t_department);
	        SimpleJoin join = new SimpleJoin(scanE, scanD, predJoin);
	        
			Predicate[] predSel = new Predicate[] { new Predicate(AttrOperator.LT,
			AttrType.FIELDNO, 3, AttrType.FIELDNO, 7) };
			Selection sel = new Selection(join, predSel);
			
			Projection pro = new Projection(sel, 1);

			pro.execute();
			
			saveCounts("query8");
			
			// destroy temp files before doing final counts
			pro = null;
			scanD = null;
			scanE = null;
			predSel = null;
			predJoin = null;
			join = null;
			sel = null;
			System.gc();

			// that's all folks!
			System.out.print("\n\nQuery 8 completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nQuery 8 terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean query8()
}