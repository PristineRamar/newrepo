package com.pristine.test.fileValidator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleAllowanceDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleCostDTO;
import com.pristine.exception.GeneralException;
import com.pristine.feedvalidator.CostFileValidator;
import com.pristine.test.dataload.TestHelper;


public class GECostFileValidatorJunitTest {
	private static Logger logger = Logger.getLogger("GECostFileValidatorJunitTest");
	List<GiantEagleCostDTO> costList = null;
	List<GiantEagleAllowanceDTO> allowanceList = null;
	String currentWeekStartDate = "03/08/2018", pastWeek = "02/10/2016", currentWeekEndDate="03/14/2018", week1Start="03/15/2018", 
			week1End="03/21/2018", week2Start="03/22/2018", week2End="03/28/2018", week3Start="03/29/2018", week3End="04/04/2018",
			week4Start="04/05/2018", week4End="04/11/2018", pastStart="02/04/2016", pastEnd="02/10/2016", inCorrectDate="03/15/21", 
			week5Start="04/12/2018";
	HashMap<String, List<ItemDTO>> upcMap = null;
	HashMap<String, RetailCalendarDTO> everydayAndRespWeekCalId = null;
	
	@Before
	public void init() {
		costList = new ArrayList<>();
		allowanceList = new ArrayList<>();
		upcMap = new HashMap<>();
		everydayAndRespWeekCalId = new HashMap<>();
	}
	
	private HashMap<String, RetailCalendarDTO> setCalendarDetails(){
		HashMap<String, RetailCalendarDTO> calendarDetails = new HashMap<>();
		TestHelper.setRetailCalendar(calendarDetails,currentWeekStartDate, currentWeekStartDate, currentWeekEndDate);
		TestHelper.setRetailCalendar(calendarDetails,week1Start, week1Start, week1End);	
		TestHelper.setRetailCalendar(calendarDetails,week2Start, week2Start, week2End);	
		TestHelper.setRetailCalendar(calendarDetails,week3Start, week3Start, week3End);	
		TestHelper.setRetailCalendar(calendarDetails,week4Start, week4Start, week4End);
		TestHelper.setRetailCalendar(calendarDetails,week4End, week4Start, week4End);
		TestHelper.setRetailCalendar(calendarDetails,pastWeek, pastStart, pastEnd);
		TestHelper.setRetailCalendar(calendarDetails,week2End, week2Start, week2End);
		return calendarDetails;
	}
	
	
	/**
	 * Simple cost record with all the values assigned properly.
	 * Expected output: Given input records must be the same as output record. No error record should be available 
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void TestCase1() throws GeneralException, Exception {
		TestHelper.setUPCMap("007314388662","1",upcMap);
		everydayAndRespWeekCalId = setCalendarDetails();
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		
		CostFileValidator costFileValidator = new CostFileValidator(upcMap, everydayAndRespWeekCalId);
		
		// To get error records
		List<GiantEagleCostDTO> errorCostRecords = costFileValidator.getErrorRecordsAlone(costList);
		assertEquals("Not Matching", null, errorCostRecords);
		
		// To get Valid cost records
		List<GiantEagleCostDTO> validCostRecords = costFileValidator.getNonErrorRecords(costList);
		assertEquals("Not Matching", 1, validCostRecords.size());
		
		costList.forEach(expectedItem->{
			validCostRecords.stream().filter(s->expectedItem.getUPC().equals(s.getUPC())).forEach(actualItem->{
				assertEquals("Not matching", expectedItem.getUPC(), actualItem.getUPC());
				assertEquals("Not matching", expectedItem.getWHITEM_NO(), actualItem.getWHITEM_NO());
				assertEquals(expectedItem.getBS_CST_AKA_STORE_CST(), actualItem.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals("Not matching", expectedItem.getSTRT_DTE(), actualItem.getSTRT_DTE());
				assertEquals(expectedItem.getDLVD_CST_AKA_WHSE_CST(), actualItem.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", expectedItem.getCST_ZONE_NO(), actualItem.getCST_ZONE_NO());
				assertEquals("Not matching", expectedItem.getBNR_CD(), actualItem.getBNR_CD());
				assertEquals("Not matching", expectedItem.getLONG_TERM_REFLECT_FG(), actualItem.getLONG_TERM_REFLECT_FG());
				assertEquals("Not matching", expectedItem.getSPLR_NO(), actualItem.getSPLR_NO());
				assertEquals("Not matching", null, actualItem.getErrorMessage());
			});
		});
	}
	
	/**
	 * Totally 2 cost records with all the values assigned properly. but one cost record UPC is not matching with Presto
	 * Expected output: One record should be valid and another one is a error record.
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void TestCase2() throws GeneralException, Exception {
		TestHelper.setUPCMap("007314388662","1",upcMap);
		everydayAndRespWeekCalId = setCalendarDetails();
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setCostDetails(costList, "007314388666", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		CostFileValidator costFileValidator = new CostFileValidator(upcMap, everydayAndRespWeekCalId);
		
		// To get error records
		List<GiantEagleCostDTO> errorCostRecords = costFileValidator.getErrorRecordsAlone(costList);
		assertEquals("Not Matching", 1, errorCostRecords.size());
		costList.forEach(expectedItem->{
			errorCostRecords.stream().filter(s->expectedItem.getUPC().equals(s.getUPC())).forEach(actualItem->{
				assertEquals("Not matching", expectedItem.getUPC(), actualItem.getUPC());
				assertEquals("Not matching", expectedItem.getWHITEM_NO(), actualItem.getWHITEM_NO());
				assertEquals(expectedItem.getBS_CST_AKA_STORE_CST(), actualItem.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals("Not matching", expectedItem.getSTRT_DTE(), actualItem.getSTRT_DTE());
				assertEquals(expectedItem.getDLVD_CST_AKA_WHSE_CST(), actualItem.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", expectedItem.getCST_ZONE_NO(), actualItem.getCST_ZONE_NO());
				assertEquals("Not matching", expectedItem.getBNR_CD(), actualItem.getBNR_CD());
				assertEquals("Not matching", expectedItem.getLONG_TERM_REFLECT_FG(), actualItem.getLONG_TERM_REFLECT_FG());
				assertEquals("Not matching", expectedItem.getSPLR_NO(), actualItem.getSPLR_NO());
				assertEquals("Not matching", "The UPC value is not available in Presto ,", actualItem.getErrorMessage());
			});
		});
		// To get Valid cost records
		List<GiantEagleCostDTO> validCostRecords = costFileValidator.getNonErrorRecords(costList);
		assertEquals("Not Matching", 1, validCostRecords.size());
		
		costList.forEach(expectedItem->{
			validCostRecords.stream().filter(s->expectedItem.getUPC().equals(s.getUPC())).forEach(actualItem->{
				assertEquals("Not matching", expectedItem.getUPC(), actualItem.getUPC());
				assertEquals("Not matching", expectedItem.getWHITEM_NO(), actualItem.getWHITEM_NO());
				assertEquals(expectedItem.getBS_CST_AKA_STORE_CST(), actualItem.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals("Not matching", expectedItem.getSTRT_DTE(), actualItem.getSTRT_DTE());
				assertEquals(expectedItem.getDLVD_CST_AKA_WHSE_CST(), actualItem.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", expectedItem.getCST_ZONE_NO(), actualItem.getCST_ZONE_NO());
				assertEquals("Not matching", expectedItem.getBNR_CD(), actualItem.getBNR_CD());
				assertEquals("Not matching", expectedItem.getLONG_TERM_REFLECT_FG(), actualItem.getLONG_TERM_REFLECT_FG());
				assertEquals("Not matching", expectedItem.getSPLR_NO(), actualItem.getSPLR_NO());
				assertEquals("Not matching", null, actualItem.getErrorMessage());
			});
		});
	}
	
	/**
	 * One cost record with Invalid date is given
	 * Expected output: Error record should be returned with error message stating that Date column has unexpected format
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void TestCase3() throws GeneralException, Exception {
		TestHelper.setUPCMap("007314388662","1",upcMap);
		everydayAndRespWeekCalId = setCalendarDetails();
		TestHelper.setCostDetails(costList, "007314388662", "322792", inCorrectDate, "1", "", "C", 7.5000, 5.7, "N", "GE");
		CostFileValidator costFileValidator = new CostFileValidator(upcMap, everydayAndRespWeekCalId);
		
		// To get error records
		List<GiantEagleCostDTO> errorCostRecords = costFileValidator.getErrorRecordsAlone(costList);
		assertEquals("Not Matching", 1, errorCostRecords.size());
		costList.forEach(expectedItem->{
			errorCostRecords.stream().filter(s->expectedItem.getUPC().equals(s.getUPC())).forEach(actualItem->{
				assertEquals("Not matching", expectedItem.getUPC(), actualItem.getUPC());
				assertEquals("Not matching", expectedItem.getWHITEM_NO(), actualItem.getWHITEM_NO());
				assertEquals(expectedItem.getBS_CST_AKA_STORE_CST(), actualItem.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals("Not matching", expectedItem.getSTRT_DTE(), actualItem.getSTRT_DTE());
				assertEquals(expectedItem.getDLVD_CST_AKA_WHSE_CST(), actualItem.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", expectedItem.getCST_ZONE_NO(), actualItem.getCST_ZONE_NO());
				assertEquals("Not matching", expectedItem.getBNR_CD(), actualItem.getBNR_CD());
				assertEquals("Not matching", expectedItem.getLONG_TERM_REFLECT_FG(), actualItem.getLONG_TERM_REFLECT_FG());
				assertEquals("Not matching", expectedItem.getSPLR_NO(), actualItem.getSPLR_NO());
				assertEquals("Not matching", "The Column STRT_DTE is having date in unexpected format "
						+ ",The STRT_DTE value is not available in Presto ,", actualItem.getErrorMessage());
			});
		});
		// To get Valid cost records
		List<GiantEagleCostDTO> validCostRecords = costFileValidator.getNonErrorRecords(costList);
		assertEquals("Not Matching", null, validCostRecords);
	}
	
	/**
	 * One cost record with Invalid date and UPC isn't available in presto
	 * Expected output: Error record should be returned with error message stating that Date column has unexpected format 
	 * and UPC isn't available in presto message should be returned 
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void TestCase4() throws GeneralException, Exception {
		TestHelper.setUPCMap("007314388662","1",upcMap);
		everydayAndRespWeekCalId = setCalendarDetails();
		TestHelper.setCostDetails(costList, "007314388666", "322792", inCorrectDate, "1", "", "C", 7.5000, 5.7, "N", "GE");
		CostFileValidator costFileValidator = new CostFileValidator(upcMap, everydayAndRespWeekCalId);
		
		// To get error records
		List<GiantEagleCostDTO> errorCostRecords = costFileValidator.getErrorRecordsAlone(costList);
		assertEquals("Not Matching", 1, errorCostRecords.size());
		costList.forEach(expectedItem->{
			errorCostRecords.stream().filter(s -> expectedItem.getUPC().equals(s.getUPC())).forEach(actualItem -> {
				assertEquals("Not matching", expectedItem.getUPC(), actualItem.getUPC());
				assertEquals("Not matching", expectedItem.getWHITEM_NO(), actualItem.getWHITEM_NO());
				assertEquals(expectedItem.getBS_CST_AKA_STORE_CST(), actualItem.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals("Not matching", expectedItem.getSTRT_DTE(), actualItem.getSTRT_DTE());
				assertEquals(expectedItem.getDLVD_CST_AKA_WHSE_CST(), actualItem.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", expectedItem.getCST_ZONE_NO(), actualItem.getCST_ZONE_NO());
				assertEquals("Not matching", expectedItem.getBNR_CD(), actualItem.getBNR_CD());
				assertEquals("Not matching", expectedItem.getLONG_TERM_REFLECT_FG(), actualItem.getLONG_TERM_REFLECT_FG());
				assertEquals("Not matching", expectedItem.getSPLR_NO(), actualItem.getSPLR_NO());
				assertEquals("Not matching", "The UPC value is not available in Presto "
						+ ",The Column STRT_DTE is having date in unexpected format ,The STRT_DTE value is not available in Presto ,",
						actualItem.getErrorMessage());
			});
		});
		// To get Valid cost records
		List<GiantEagleCostDTO> validCostRecords = costFileValidator.getNonErrorRecords(costList);
		assertEquals("Not Matching", null, validCostRecords);
	}
	
	/**
	 * One cost record with valid date which is not available in presto
	 * Expected output: Error record should be returned with error message stating that Date isn't available in presto.
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void TestCase5() throws GeneralException, Exception {
		TestHelper.setUPCMap("007314388662","1",upcMap);
		everydayAndRespWeekCalId = setCalendarDetails();
		TestHelper.setCostDetails(costList, "007314388662", "322792", week5Start, "1", "", "C", 7.5000, 5.7, "N", "GE");
		CostFileValidator costFileValidator = new CostFileValidator(upcMap, everydayAndRespWeekCalId);
		
		// To get error records
		List<GiantEagleCostDTO> errorCostRecords = costFileValidator.getErrorRecordsAlone(costList);
		assertEquals("Not Matching", 1, errorCostRecords.size());
		costList.forEach(expectedItem->{
			errorCostRecords.stream().filter(s -> expectedItem.getUPC().equals(s.getUPC())).forEach(actualItem -> {
				assertEquals("Not matching", expectedItem.getUPC(), actualItem.getUPC());
				assertEquals("Not matching", expectedItem.getWHITEM_NO(), actualItem.getWHITEM_NO());
				assertEquals(expectedItem.getBS_CST_AKA_STORE_CST(), actualItem.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals("Not matching", expectedItem.getSTRT_DTE(), actualItem.getSTRT_DTE());
				assertEquals(expectedItem.getDLVD_CST_AKA_WHSE_CST(), actualItem.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", expectedItem.getCST_ZONE_NO(), actualItem.getCST_ZONE_NO());
				assertEquals("Not matching", expectedItem.getBNR_CD(), actualItem.getBNR_CD());
				assertEquals("Not matching", expectedItem.getLONG_TERM_REFLECT_FG(), actualItem.getLONG_TERM_REFLECT_FG());
				assertEquals("Not matching", expectedItem.getSPLR_NO(), actualItem.getSPLR_NO());
				assertEquals("Not matching", "The STRT_DTE value is not available in Presto ,",
						actualItem.getErrorMessage());
			});
		});
		// To get Valid cost records
		List<GiantEagleCostDTO> validCostRecords = costFileValidator.getNonErrorRecords(costList);
		assertEquals("Not Matching", null, validCostRecords);
	}
	
	/**
	 * Warehouse item has only one cost zone
	 * Expected output: Cost zone validator shouldn't have any issues in processing 
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void TestCase6() throws GeneralException, Exception {
		TestHelper.setUPCMap("007314388662","1",upcMap);
		everydayAndRespWeekCalId = setCalendarDetails();
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		
		CostFileValidator costFileValidator = new CostFileValidator(upcMap, everydayAndRespWeekCalId);
		
		// To get error records
		boolean itemsHasOnlyOneZone = costFileValidator.isWHSEItemHasOnlyOneZoneAtBnrLevel(costList);
		assertEquals("Not Matching", true, itemsHasOnlyOneZone);
		List<GiantEagleCostDTO> errorCostRecords = costFileValidator.getErrorRecordsAlone(costList);
		assertEquals("Not Matching", null, errorCostRecords);
		
		// To get Valid cost records
		List<GiantEagleCostDTO> validCostRecords = costFileValidator.getNonErrorRecords(costList);
		assertEquals("Not Matching", 1, validCostRecords.size());
		
		costList.forEach(expectedItem->{
			validCostRecords.stream().filter(s->expectedItem.getUPC().equals(s.getUPC())).forEach(actualItem->{
				assertEquals("Not matching", expectedItem.getUPC(), actualItem.getUPC());
				assertEquals("Not matching", expectedItem.getWHITEM_NO(), actualItem.getWHITEM_NO());
				assertEquals(expectedItem.getBS_CST_AKA_STORE_CST(), actualItem.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals("Not matching", expectedItem.getSTRT_DTE(), actualItem.getSTRT_DTE());
				assertEquals(expectedItem.getDLVD_CST_AKA_WHSE_CST(), actualItem.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", expectedItem.getCST_ZONE_NO(), actualItem.getCST_ZONE_NO());
				assertEquals("Not matching", expectedItem.getBNR_CD(), actualItem.getBNR_CD());
				assertEquals("Not matching", expectedItem.getLONG_TERM_REFLECT_FG(), actualItem.getLONG_TERM_REFLECT_FG());
				assertEquals("Not matching", expectedItem.getSPLR_NO(), actualItem.getSPLR_NO());
				assertEquals("Not matching", null, actualItem.getErrorMessage());
			});
		});
	}
	
	/**
	 * Warehouse item has two cost zone
	 * Expected output: Cost zone validation will return false which mean more than one cost zone found for an item
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void TestCase7() throws GeneralException, Exception {
		TestHelper.setUPCMap("007314388662","1",upcMap);
		everydayAndRespWeekCalId = setCalendarDetails();
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "2", "", "C", 7.5000, 5.7, "N", "GE");
		
		CostFileValidator costFileValidator = new CostFileValidator(upcMap, everydayAndRespWeekCalId);
		
		// To get error records
		boolean itemsHasOnlyOneZone = costFileValidator.isWHSEItemHasOnlyOneZoneAtBnrLevel(costList);
		assertEquals("Not Matching", false, itemsHasOnlyOneZone);
	}
}
