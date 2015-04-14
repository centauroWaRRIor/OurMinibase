package tests;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.HashJoin;
import relop.IndexScan;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;

/**
 * Test suite for the relop layer.
 */
class ROTest2 extends TestDriver {

	/** The display name of the test suite. */
	private static final String TEST_NAME = "Rel Operators mini mini test";

	/** Size of tables in test3. */
	private static final int SUPER_SIZE = 2000;

	/** Drivers table schema. */
	private static Schema s_drivers;

	/** Rides table schema. */
	private static Schema s_rides;

	/** Groups table schema. */
	private static Schema s_groups;

	// --------------------------------------------------------------------------

	/**
	 * Test application entry point; runs all tests.
	 */
	public static void main(String argv[]) {

		// create a clean Minibase instance
		ROTest2 rot = new ROTest2();
		rot.create_minibase();

		// initialize schema for the "Drivers" table
		s_drivers = new Schema(5);
		s_drivers.initField(0, AttrType.INTEGER, 4, "DriverId");
		s_drivers.initField(1, AttrType.STRING, 20, "FirstName");
		s_drivers.initField(2, AttrType.STRING, 20, "LastName");
		s_drivers.initField(3, AttrType.FLOAT, 4, "Age");
		s_drivers.initField(4, AttrType.INTEGER, 4, "NumSeats");

		// initialize schema for the "Rides" table
		s_rides = new Schema(4);
		s_rides.initField(0, AttrType.INTEGER, 4, "DriverId");
		s_rides.initField(1, AttrType.INTEGER, 4, "GroupId");
		s_rides.initField(2, AttrType.STRING, 10, "FromDate");
		s_rides.initField(3, AttrType.STRING, 10, "ToDate");

		// initialize schema for the "Groups" table
		s_groups = new Schema(2);
		s_groups.initField(0, AttrType.INTEGER, 4, "GroupId");
		s_groups.initField(1, AttrType.STRING, 10, "Country");

		// run all the test cases
		System.out.println("\n" + "Running " + TEST_NAME + "...");
		boolean status = PASS;
		
		status &= rot.testFileScan();
		status &= rot.testKeyScan();
		status &= rot.testIndexScan();
		status &= rot.testProjection();
        status &= rot.testSelection();

		// display the final results
		System.out.println();
		if (status != PASS) {
			System.out.println("Error(s) encountered during " + TEST_NAME + ".");
		} else {
			System.out.println("All " + TEST_NAME
					+ " completed; verify output for correctness.");
		}

	} // public static void main (String argv[])


    private void createTestData( HeapFile file, HashIndex index ) {
			initCounts();
			saveCounts(null);

			// create and populate a temporary Drivers file and index
			Tuple tuple = new Tuple(s_drivers);
			for (int i = 1; i <= 10; i++) {

				// create the tuple
				tuple.setIntFld(0, i);
				tuple.setStringFld(1, "f" + i);
				tuple.setStringFld(2, "l" + i);
				Float age = (float) (i * 7.7);
				tuple.setFloatFld(3, age);
				tuple.setIntFld(4, i + 100);

				// insert the tuple in the file and index
				RID rid = file.insertRecord(tuple.getData());
                if( index != null ) { 
				    index.insertEntry(new SearchKey(age), rid);
                }

			} // for
			saveCounts("insert");
    }

	protected boolean testFileScan() {
		try {

			System.out.println("\nTest testFileScan: Simple test for FileScan");
            HeapFile file = new HeapFile(null);

            createTestData( file, null );

			// test index scan
			saveCounts(null);
			System.out.println("\n  ~> test File Scan...\n");
			FileScan fileScan = new FileScan(s_drivers, file);
			fileScan.execute();
			saveCounts("filescan");
			
			// destroy temp files before doing final counts
			fileScan = null;
			file = null;
			System.gc();
			saveCounts("end");

			// that's all folks!
			System.out.print("\n\nTest testFileScan completed without exception.");
			return PASS;

		} catch (Exception exc) {

			exc.printStackTrace(System.out);
			System.out.print("\n\nTest testFileScan terminated because of exception.");
			return FAIL;

		} finally {
			printSummary(6);
			System.out.println();
		}
	} // protected boolean testFileScan()
	
	protected boolean testKeyScan() {
		try {

			System.out.println("\nTest testKeyScan: Simple test for KeyScan");
            HeapFile file = new HeapFile(null);
            HashIndex index = new HashIndex(null);

            createTestData( file, index );

			// test index scan
			saveCounts(null);
			System.out.println("\n  ~> test key scan (Age = 53.9)...\n");
			SearchKey key = new SearchKey(53.9F);
			KeyScan keyscan = new KeyScan(s_drivers, index, key, file);
			keyscan.execute();
			saveCounts("keyscan");
            
	        // destroy temp files before doing final counts
	        keyscan = null;
	        index = null;
	        file = null;
	        System.gc();
	        saveCounts("end");

	        // that's all folks!
	        System.out.print("\n\nTest testKeyScan completed without exception.");
	        return PASS;

        } catch (Exception exc) {

	       exc.printStackTrace(System.out);
	       System.out.print("\n\nTest testKeyScan terminated because of exception.");
	       return FAIL;

        } finally {
	       printSummary(6);
	       System.out.println();
        }
   } // protected boolean testKeyScan()

	
	protected boolean testIndexScan() {
		try {

			System.out.println("\nTest testIndexScan: Simple test for IndexScan");
            HeapFile file = new HeapFile(null);
            HashIndex index = new HashIndex(null);

            createTestData( file, index );

			// test index scan
			saveCounts(null);
			System.out.println("\n  ~> test index scan ...\n");
			IndexScan indexscan = new IndexScan(s_drivers, index, file);
			indexscan.execute();
			saveCounts("indexscan");
            
	        // destroy temp files before doing final counts
	        indexscan = null;
	        index = null;
	        file = null;
	        System.gc();
	        saveCounts("end");

	        // that's all folks!
	        System.out.print("\n\nTest testIndexScan completed without exception.");
	        return PASS;

        } catch (Exception exc) {

	       exc.printStackTrace(System.out);
	       System.out.print("\n\nTest testIndexScan terminated because of exception.");
	       return FAIL;

        } finally {
	       printSummary(6);
	       System.out.println();
        }
    }

	protected boolean testSelection() {
		try {

			System.out.println("\nTest testSelection: Simple test for Selection");
            HeapFile file = new HeapFile(null);
            HashIndex index = new HashIndex(null);

            createTestData( file, index );

			// test selection operator
			saveCounts(null);
			System.out.println("\n  ~> test selection (Age > 65 OR Age < 15)...\n");
			Predicate[] preds = new Predicate[] {
					new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FLOAT,
							65F),
							new Predicate(AttrOperator.LT, AttrType.FIELDNO, 3, AttrType.FLOAT,
									15F) };
			FileScan scan = new FileScan(s_drivers, file);
			Selection sel = new Selection(scan, preds);
			sel.execute();
			saveCounts("select");

			// destroy temp files before doing final counts
			sel = null;
			scan = null;
			file = null;
			System.gc();
			saveCounts("end");

	        // that's all folks!
	        System.out.print("\n\nTest testSelection completed without exception.");
	        return PASS;

        } catch (Exception exc) {

	       exc.printStackTrace(System.out);
	       System.out.print("\n\nTest testSelection terminated because of exception.");
	       return FAIL;

        } finally {
	       printSummary(6);
	       System.out.println();
        }
    }
	
	protected boolean testProjection() {
		try {

			System.out.println("\nTest testProjection: Simple test for Projection");
            HeapFile file = new HeapFile(null);
            HashIndex index = new HashIndex(null);

            createTestData( file, index );

			// test projection
			saveCounts(null);
			System.out.println("\n  ~> test projection ...\n");
			IndexScan indexscan = new IndexScan(s_drivers, index, file);
            Projection proj = new Projection(indexscan, 0, 1, 3 );
			proj.execute();
			saveCounts("proj");
            
	        // destroy temp files before doing final counts
	        indexscan = null;
	        index = null;
	        file = null;
            proj = null;
	        System.gc();
	        saveCounts("end");

	        // that's all folks!
	        System.out.print("\n\nTest testProjection completed without exception.");
	        return PASS;

        } catch (Exception exc) {

	       exc.printStackTrace(System.out);
	       System.out.print("\n\nTest testProjection terminated because of exception.");
	       return FAIL;

        } finally {
	       printSummary(6);
	       System.out.println();
        }
    }

} // class ROTest extends TestDriver



