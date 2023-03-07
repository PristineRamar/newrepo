package com.pristine.test.offermgmt;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.pristine.dataload.offermgmt.StorePriceExportV3;
import com.pristine.dto.fileformatter.gianteagle.ZoneDTO;
import com.pristine.dto.offermgmt.PriceExportDTO;

public class StorePriceExportTest {
	
	private static List<String> failedTests;
	private static List<String> skippedTests;
	private static List<String> passedTests;
	
	private StorePriceExportV3 storePriceExport;
	private StorePriceExportDataProvider dataProvider;

	@Rule
	  public TestWatcher watchman= new TestWatcher() {
	      @Override
	      protected void failed(Throwable e, Description description) {
	    	  if(null==failedTests) {
		    	  System.out.println("Failed tests list initialized.");
	    		  failedTests = new ArrayList<>();
	    	  }
	    	  System.out.println(description.getMethodName());
	    	  failedTests.add(description.getMethodName());
	      }
	
	      @Override
		protected void skipped(AssumptionViolatedException e, Description description) {
	    	  if(null==skippedTests) {
	    		  System.out.println("Skipped tests list initialized.");
	    		  skippedTests = new ArrayList<>();
	    	  }
	    	  skippedTests.add(description.getMethodName());
		}
	
		@Override
	      protected void succeeded(Description description) {
	    	  if(null==passedTests) {
	    		  System.out.println("Passed tests list initialized.");
	    		  passedTests = new ArrayList<>();
	    	  }
	    	  passedTests.add(description.getMethodName());
	         }
	     };

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUpBeforeTest() throws Exception {
		storePriceExport = new StorePriceExportV3(true);
		dataProvider = new StorePriceExportDataProvider();
	}

	@After
	public void tearDownAfterTest() throws Exception {
	}

	/**
	 * Reads in a json file with elements that can map to PriceExportDTO objects (signifying items approved for export)
	 * Expects the file to have 576 elements
	 * Of these there are 128 elements each of zones 66, 67, 49 and 1000.
	 * The file also has 32 elements for zone 4 such that their item codes are equal to 32 of the elements for zone 1000.
	 * 16 of these 32 are such that their approval dates are after the approval dates for the same items for zone 1000 
	 * while the other 16 are such that their approval dates are before the approval dates for the same items for zone 1000.
	 * The file also has 32 elements for zone 16 that satisfy the same conditions as those mentioned above for zone 4 elements.
	 */
	@Test
	public void testUnpackGlobalZoneApprovals() {
		List<PriceExportDTO> itemsFromQueue = dataProvider.generateExportQueue("com/pristine/test/offermgmt/sampleQueue.json");
		ListIterator<PriceExportDTO> itemsFromQueueIterator = itemsFromQueue.listIterator();
		PriceExportDTO queueItem;
		String queueItemZoneNumber;
//		Count number of Zone 4, Zone 16 and global zone (zone 1000) entries in the static test data input.
		
		List<ZoneDTO> globalZoneData = dataProvider.generateGlobalZoneData("com/pristine/test/offermgmt/globalZoneData.json");
		List<PriceExportDTO> allZonesMergedList = null;
		
//		Method being tested
		allZonesMergedList = storePriceExport.unpackGlobalZoneApprovals(itemsFromQueue, globalZoneData);
		
		int numberOfGlobalZoneEntriesAfter = 0;
		int numberOfZone4EntriesAfter = 0;
		int numberOfZone16EntriesAfter = 0;
		int numberOfOtherZoneEntriesAfter = 0;
		itemsFromQueueIterator = allZonesMergedList.listIterator();
//		Count number of Zone 4, Zone 16 and global zone (zone 1000) entries after executing applyGlobalZonePriceToAllZones.
		while(itemsFromQueueIterator.hasNext()) {
			queueItem = itemsFromQueueIterator.next();
			queueItemZoneNumber = queueItem.getPriceZoneNo();
			if(queueItemZoneNumber.equals("4"))
				numberOfZone4EntriesAfter++;
			else if(queueItemZoneNumber.equals("16"))
				numberOfZone16EntriesAfter++;
			else if(queueItemZoneNumber.equals("1000"))
				numberOfGlobalZoneEntriesAfter++;
			else
				numberOfOtherZoneEntriesAfter++;
		}
//		applyGlobalZonePriceToAllZones must NOT alter individual zone entries
		assertTrue(384==numberOfOtherZoneEntriesAfter);
//		applyGlobalZonePriceToAllZones must remove all global zone entries
		assertTrue(0==numberOfGlobalZoneEntriesAfter);
//		applyGlobalZonePriceToAllZones must add 1 zone 4 entry per zone 1000 entry
		assertTrue(128==numberOfZone4EntriesAfter);
//		applyGlobalZonePriceToAllZones must add 1 zone 16 entry per zone 1000 entry
		assertTrue(128==numberOfZone16EntriesAfter);
	}
	
	@Test
	public void testPopulateExportItemsBasedOnCEitems () {
		List<PriceExportDTO> approvedItemList = dataProvider.generateExportQueue("com/pristine/test/offermgmt/queue.json");
//		TODO Create a json file to supply to StorePriceExportDataProvider.generateEmergencyAndClearanceItems(String jsonDataFilePath)
		HashMap<String, List<PriceExportDTO>>emergencyAndClearanceItems = dataProvider.generateEmergencyAndClearanceItems("");
		HashMap<String, List<Long>>zoneTypeToRunIdsMap = dataProvider.generateZoneTypeToRunIdsMap("com/pristine/test/offermgmt/queueRunIds.json");
		try {
			List<PriceExportDTO> candidateItemList = storePriceExport.populateExportItemsBasedOnCEitems("ESH", 
					emergencyAndClearanceItems, approvedItemList, zoneTypeToRunIdsMap, "01011970");//java.util.Date calculates time in milliseconds from 1970 January 01 00:00:00 GMT
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			fail("CloneNotSupportedException encountered!");
		}
		fail("Not yet implemented");
	}
	
}
