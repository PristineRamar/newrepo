package com.pristine.test.dataload;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.pristine.dataload.gianteagle.ScanBackLoader;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.ScanBackDTO;
import com.pristine.fileformatter.gianteagle.CostFileFormatter;
import com.pristine.util.PropertyManager;

public class GEScanbackJUnitTest {
	private static Logger logger = Logger.getLogger("GECostFileFormatterJUnit");
	CostFileFormatter costFileFormatter = null;
	List<ScanBackDTO> scanBackInputList = null;
	String currentWeekStartDate = "03/08/2018", pastWeek = "02/10/2016", currentWeekEndDate="03/14/2018", week1Start="03/15/2018", 
			week1End="03/21/2018", week2Start="03/22/2018", week2End="03/28/2018", week3Start="03/29/2018", week3End="04/04/2018",
			week4Start="04/05/2018", week4End="04/11/2018", pastStart="02/04/2016", pastEnd="02/10/2016", week5Start="04/12/2018", 
					week5End="04/28/2018";
	ScanBackLoader scanBackLoader = null;
	@Before
	public void init() {
		PropertyManager.initialize("GEDataloadAnalysis.properties");
		costFileFormatter = new CostFileFormatter();
		scanBackLoader = new ScanBackLoader();
		scanBackInputList = new ArrayList<>();

	}
	private HashMap<String, RetailCalendarDTO> setCalendarDetails(){
		HashMap<String, RetailCalendarDTO> calendarDetails = new HashMap<>();
		TestHelper.setRetailCalendar(calendarDetails,currentWeekStartDate, currentWeekStartDate, currentWeekEndDate);
		TestHelper.setRetailCalendar(calendarDetails,currentWeekEndDate, currentWeekStartDate, currentWeekEndDate);
		TestHelper.setRetailCalendar(calendarDetails,week1Start, week1Start, week1End);	
		TestHelper.setRetailCalendar(calendarDetails,week1End, week1Start, week1End);	
		TestHelper.setRetailCalendar(calendarDetails,week2Start, week2Start, week2End);	
		TestHelper.setRetailCalendar(calendarDetails,week3Start, week3Start, week3End);	
		TestHelper.setRetailCalendar(calendarDetails,week4Start, week4Start, week4End);
		TestHelper.setRetailCalendar(calendarDetails,week4End, week4Start, week4End);
		TestHelper.setRetailCalendar(calendarDetails,pastWeek, pastStart, pastEnd);
		TestHelper.setRetailCalendar(calendarDetails,week2End, week2Start, week2End);
		TestHelper.setRetailCalendar(calendarDetails,week5Start, week5Start, week5End);
		TestHelper.setRetailCalendar(calendarDetails,week5End, week5Start, week5End);
		return calendarDetails;
	}
	
	/**
	 * Item has one scan back values, which starts from Past week and end in week 4. 
	 * Expected output: From current week till week 4, scan back value must be available for that item
	 */
	@Test
	public void TestCase1() {
		CostFileFormatter.calendarDetails = setCalendarDetails();

		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", pastWeek, week4End);
		
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = scanBackLoader
				.findEachItemScanbackAmtInDiffWeek(currentWeekStartDate, scanBackInputList);
		
		HashMap<String, HashMap<String, ScanBackDTO>> expectedScanbackDetails = new HashMap<>();
		
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, currentWeekStartDate);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week1Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week2Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week3Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week4Start);
		
		
		expectedScanbackDetails.forEach((weekDate, values)->{
			assertEquals("Not matching", true,multiWeekScanbackDetails.get(weekDate)!=null);
			HashMap<String, ScanBackDTO> actualScanbackDetail = multiWeekScanbackDetails.get(weekDate);
			values.forEach((key, value)->{
				assertEquals("Not matching", true,actualScanbackDetail.containsKey(key));
				ScanBackDTO actaulSBDTO = actualScanbackDetail.get(key);
				assertEquals("Not matching", value.getRetailerItemCode(), actaulSBDTO.getRetailerItemCode());
				assertEquals("Not matching", value.getZoneNumber(), actaulSBDTO.getZoneNumber());
				assertEquals("Not matching", value.getSplrNo(), actaulSBDTO.getSplrNo());
				assertEquals("Not matching", value.getBnrCD(), actaulSBDTO.getBnrCD());
				assertEquals("Not matching", value.getScanBackAmt1(), actaulSBDTO.getScanBackAmt1());
				assertEquals("Not matching", value.getScanBackEndDate1(), actaulSBDTO.getScanBackEndDate1());
				assertEquals("Not matching", value.getScanBackStartDate1(), actaulSBDTO.getScanBackStartDate1());
				assertEquals("Not matching", value.getScanBackAmt2(), actaulSBDTO.getScanBackAmt2());
				assertEquals("Not matching", value.getScanBackEndDate2(), actaulSBDTO.getScanBackEndDate2());
				assertEquals("Not matching", value.getScanBackStartDate2(), actaulSBDTO.getScanBackStartDate2());
				assertEquals("Not matching", value.getScanBackAmt3(), actaulSBDTO.getScanBackAmt3());
				assertEquals("Not matching", value.getScanBackEndDate3(), actaulSBDTO.getScanBackEndDate3());
				assertEquals("Not matching", value.getScanBackStartDate3(), actaulSBDTO.getScanBackStartDate3());
				assertEquals(value.getScanBackTotalAmt(), actaulSBDTO.getScanBackTotalAmt(), 0);
			});
		});
	}
	
	/**
	 * Item has two scan back values, first scan back starts from Past week and end in week 4. Second scan back starts is valid for current week
	 * Expected output: From week 1 till week 4, scan back value must be based on First scan back and in Current week Scan back total Amt must be sum 
	 * of 1 & 2 Scan back Amt. And Scan back 1 & 2 related columns to be filled respectively in current week.
	 */
	@Test
	public void TestCase2() {
		CostFileFormatter.calendarDetails = setCalendarDetails();

		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", pastWeek, week4End);
		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", currentWeekStartDate, currentWeekEndDate);
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = scanBackLoader
				.findEachItemScanbackAmtInDiffWeek(currentWeekStartDate, scanBackInputList);
		
		HashMap<String, HashMap<String, ScanBackDTO>> expectedScanbackDetails = new HashMap<>();
		
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.1000, "0.0500", pastWeek, week4End, 
				"0.0500", currentWeekStartDate, currentWeekEndDate, null, null, null, currentWeekStartDate);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week1Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week2Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week3Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week4Start);
		
		
		expectedScanbackDetails.forEach((weekDate, values)->{
			assertEquals("Not matching", true,multiWeekScanbackDetails.get(weekDate)!=null);
			HashMap<String, ScanBackDTO> actualScanbackDetail = multiWeekScanbackDetails.get(weekDate);
			values.forEach((key, value)->{
				assertEquals("Not matching", true,actualScanbackDetail.containsKey(key));
				ScanBackDTO actaulSBDTO = actualScanbackDetail.get(key);
				assertEquals("Not matching", value.getRetailerItemCode(), actaulSBDTO.getRetailerItemCode());
				assertEquals("Not matching", value.getZoneNumber(), actaulSBDTO.getZoneNumber());
				assertEquals("Not matching", value.getSplrNo(), actaulSBDTO.getSplrNo());
				assertEquals("Not matching", value.getBnrCD(), actaulSBDTO.getBnrCD());
				assertEquals("Not matching", value.getScanBackAmt1(), actaulSBDTO.getScanBackAmt1());
				assertEquals("Not matching", value.getScanBackEndDate1(), actaulSBDTO.getScanBackEndDate1());
				assertEquals("Not matching", value.getScanBackStartDate1(), actaulSBDTO.getScanBackStartDate1());
				assertEquals("Not matching", value.getScanBackAmt2(), actaulSBDTO.getScanBackAmt2());
				assertEquals("Not matching", value.getScanBackEndDate2(), actaulSBDTO.getScanBackEndDate2());
				assertEquals("Not matching", value.getScanBackStartDate2(), actaulSBDTO.getScanBackStartDate2());
				assertEquals("Not matching", value.getScanBackAmt3(), actaulSBDTO.getScanBackAmt3());
				assertEquals("Not matching", value.getScanBackEndDate3(), actaulSBDTO.getScanBackEndDate3());
				assertEquals("Not matching", value.getScanBackStartDate3(), actaulSBDTO.getScanBackStartDate3());
				assertEquals(value.getScanBackTotalAmt(), actaulSBDTO.getScanBackTotalAmt(), 0);
			});
		});
	}
	
	/**
	 * Item has 3 scan back values, first scan back starts from Past week and end in week 4. Second scan back is valid from current week 
	 * till Week 2. And 3rd Scan back is valid only in Current week.
	 * Expected output: For week 3 & 4, scan back value must be based on First scan back and from Current week to week 2 Scan back total Amt 
	 * must be sum of Scan back 1, 2 Amt. Additionally 3rd scan back value to be added in Current week
	 * And Scan back 1, 2 & 3 related columns to be filled respectively in current week.
	 */
	@Test
	public void TestCase3() {
		CostFileFormatter.calendarDetails = setCalendarDetails();

		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", pastWeek, week4End);
		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", currentWeekStartDate, week2End);
		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", currentWeekStartDate, currentWeekEndDate);
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = scanBackLoader
				.findEachItemScanbackAmtInDiffWeek(currentWeekStartDate, scanBackInputList);
		
		HashMap<String, HashMap<String, ScanBackDTO>> expectedScanbackDetails = new HashMap<>();
		
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.1500, "0.0500", pastWeek, week4End, 
				"0.0500", currentWeekStartDate, week2End, "0.0500", currentWeekStartDate, currentWeekEndDate, currentWeekStartDate);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.1000, "0.0500", pastWeek, week4End, 
				"0.0500", currentWeekStartDate, week2End, null, null, null, week1Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.1000, "0.0500", pastWeek, week4End, 
				"0.0500", currentWeekStartDate, week2End, null, null, null, week2Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week3Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week4Start);
		
		
		expectedScanbackDetails.forEach((weekDate, values)->{
			assertEquals("Not matching", true,multiWeekScanbackDetails.get(weekDate)!=null);
			HashMap<String, ScanBackDTO> actualScanbackDetail = multiWeekScanbackDetails.get(weekDate);
			values.forEach((key, value)->{
				assertEquals("Not matching", true,actualScanbackDetail.containsKey(key));
				ScanBackDTO actaulSBDTO = actualScanbackDetail.get(key);
				assertEquals("Not matching", value.getRetailerItemCode(), actaulSBDTO.getRetailerItemCode());
				assertEquals("Not matching", value.getZoneNumber(), actaulSBDTO.getZoneNumber());
				assertEquals("Not matching", value.getSplrNo(), actaulSBDTO.getSplrNo());
				assertEquals("Not matching", value.getBnrCD(), actaulSBDTO.getBnrCD());
				assertEquals("Not matching", value.getScanBackAmt1(), actaulSBDTO.getScanBackAmt1());
				assertEquals("Not matching", value.getScanBackEndDate1(), actaulSBDTO.getScanBackEndDate1());
				assertEquals("Not matching", value.getScanBackStartDate1(), actaulSBDTO.getScanBackStartDate1());
				assertEquals("Not matching", value.getScanBackAmt2(), actaulSBDTO.getScanBackAmt2());
				assertEquals("Not matching", value.getScanBackEndDate2(), actaulSBDTO.getScanBackEndDate2());
				assertEquals("Not matching", value.getScanBackStartDate2(), actaulSBDTO.getScanBackStartDate2());
				assertEquals("Not matching", value.getScanBackAmt3(), actaulSBDTO.getScanBackAmt3());
				assertEquals("Not matching", value.getScanBackEndDate3(), actaulSBDTO.getScanBackEndDate3());
				assertEquals("Not matching", value.getScanBackStartDate3(), actaulSBDTO.getScanBackStartDate3());
				assertEquals(value.getScanBackTotalAmt(), actaulSBDTO.getScanBackTotalAmt(), 0);
			});
		});
	}
	
	/**
	 * Item has 4 scan back values, first scan back starts from Past week and end in week 4. Second scan back starts is valid from current week 
	 * till Week 2. 3rd Scan back is valid only in Current week. And final Scan back is started from past and ends in current week. 
	 * Expected output: For week 3 & 4, scan back value must be based on First scan back and from Current week to week 2 Scan back total Amt 
	 * must be sum of Scan back 1, 2 Amt. Additionally 3rd & 4th scan back value to be added in Current week
	 * And Scan back 1, 2 & 3 related columns to be filled respectively in current week.
	 */
	@Test
	public void TestCase4() {
		CostFileFormatter.calendarDetails = setCalendarDetails();

		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", pastWeek, week4End);
		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", currentWeekStartDate, week2End);
		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", currentWeekStartDate, currentWeekEndDate);
		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", pastWeek, currentWeekEndDate);
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = scanBackLoader
				.findEachItemScanbackAmtInDiffWeek(currentWeekStartDate, scanBackInputList);
		
		HashMap<String, HashMap<String, ScanBackDTO>> expectedScanbackDetails = new HashMap<>();
		
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.2000, "0.0500", pastWeek, week4End, 
				"0.0500", currentWeekStartDate, week2End, "0.1", currentWeekStartDate, currentWeekEndDate, currentWeekStartDate);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.1000, "0.0500", pastWeek, week4End, 
				"0.0500", currentWeekStartDate, week2End, null, null, null, week1Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.1000, "0.0500", pastWeek, week4End, 
				"0.0500", currentWeekStartDate, week2End, null, null, null, week2Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week3Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week4End, 
				null, null, null, null, null, null, week4Start);
		
		
		expectedScanbackDetails.forEach((weekDate, values)->{
			assertEquals("Not matching", true,multiWeekScanbackDetails.get(weekDate)!=null);
			HashMap<String, ScanBackDTO> actualScanbackDetail = multiWeekScanbackDetails.get(weekDate);
			values.forEach((key, value)->{
				assertEquals("Not matching", true,actualScanbackDetail.containsKey(key));
				ScanBackDTO actaulSBDTO = actualScanbackDetail.get(key);
				assertEquals("Not matching", value.getRetailerItemCode(), actaulSBDTO.getRetailerItemCode());
				assertEquals("Not matching", value.getZoneNumber(), actaulSBDTO.getZoneNumber());
				assertEquals("Not matching", value.getSplrNo(), actaulSBDTO.getSplrNo());
				assertEquals("Not matching", value.getBnrCD(), actaulSBDTO.getBnrCD());
				assertEquals("Not matching", value.getScanBackAmt1(), actaulSBDTO.getScanBackAmt1());
				assertEquals("Not matching", value.getScanBackEndDate1(), actaulSBDTO.getScanBackEndDate1());
				assertEquals("Not matching", value.getScanBackStartDate1(), actaulSBDTO.getScanBackStartDate1());
				assertEquals("Not matching", value.getScanBackAmt2(), actaulSBDTO.getScanBackAmt2());
				assertEquals("Not matching", value.getScanBackEndDate2(), actaulSBDTO.getScanBackEndDate2());
				assertEquals("Not matching", value.getScanBackStartDate2(), actaulSBDTO.getScanBackStartDate2());
				assertEquals("Not matching", value.getScanBackAmt3(), actaulSBDTO.getScanBackAmt3());
				assertEquals("Not matching", value.getScanBackEndDate3(), actaulSBDTO.getScanBackEndDate3());
				assertEquals("Not matching", value.getScanBackStartDate3(), actaulSBDTO.getScanBackStartDate3());
				assertEquals(value.getScanBackTotalAmt(), actaulSBDTO.getScanBackTotalAmt(), 0);
			});
		});
	}
	
	/**
	 * Item has 2 scan back values, first scan back valid in current week. Second scan back is available in week 4. 
	 * Expected output: For Current week and Week 4 scan back values must be available. Weeks in between doesn't have scan back amount.
	 */
	@Test
	public void TestCase5() {
		CostFileFormatter.calendarDetails = setCalendarDetails();

		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", week4Start, week4End);
		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", currentWeekStartDate, currentWeekEndDate);
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = scanBackLoader
				.findEachItemScanbackAmtInDiffWeek(currentWeekStartDate, scanBackInputList);
		
		HashMap<String, HashMap<String, ScanBackDTO>> expectedScanbackDetails = new HashMap<>();
		
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", currentWeekStartDate,
				currentWeekEndDate, null, null, null, null, null, null, currentWeekStartDate);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", week4Start, week4End, 
				null, null, null, null, null, null, week4Start);
		
		assertEquals("Not matching", 2,multiWeekScanbackDetails.size());
		
		expectedScanbackDetails.forEach((weekDate, values)->{
			assertEquals("Not matching", true,multiWeekScanbackDetails.get(weekDate)!=null);
			HashMap<String, ScanBackDTO> actualScanbackDetail = multiWeekScanbackDetails.get(weekDate);
			values.forEach((key, value)->{
				assertEquals("Not matching", true,actualScanbackDetail.containsKey(key));
				ScanBackDTO actaulSBDTO = actualScanbackDetail.get(key);
				assertEquals("Not matching", value.getRetailerItemCode(), actaulSBDTO.getRetailerItemCode());
				assertEquals("Not matching", value.getZoneNumber(), actaulSBDTO.getZoneNumber());
				assertEquals("Not matching", value.getSplrNo(), actaulSBDTO.getSplrNo());
				assertEquals("Not matching", value.getBnrCD(), actaulSBDTO.getBnrCD());
				assertEquals("Not matching", value.getScanBackAmt1(), actaulSBDTO.getScanBackAmt1());
				assertEquals("Not matching", value.getScanBackEndDate1(), actaulSBDTO.getScanBackEndDate1());
				assertEquals("Not matching", value.getScanBackStartDate1(), actaulSBDTO.getScanBackStartDate1());
				assertEquals("Not matching", value.getScanBackAmt2(), actaulSBDTO.getScanBackAmt2());
				assertEquals("Not matching", value.getScanBackEndDate2(), actaulSBDTO.getScanBackEndDate2());
				assertEquals("Not matching", value.getScanBackStartDate2(), actaulSBDTO.getScanBackStartDate2());
				assertEquals("Not matching", value.getScanBackAmt3(), actaulSBDTO.getScanBackAmt3());
				assertEquals("Not matching", value.getScanBackEndDate3(), actaulSBDTO.getScanBackEndDate3());
				assertEquals("Not matching", value.getScanBackStartDate3(), actaulSBDTO.getScanBackStartDate3());
				assertEquals(value.getScanBackTotalAmt(), actaulSBDTO.getScanBackTotalAmt(), 0);
			});
		});
	}
	
	/**
	 * Item has one scan back values, which starts from Past week and end in week 5(But based on configuration Week 4 is maximum). 
	 * Expected output: From current week till week 4, scan back value must be available for that item and week 5 will not be available
	 */
	@Test
	public void TestCase6() {
		CostFileFormatter.calendarDetails = setCalendarDetails();

		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", pastWeek, week5End);
		
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = scanBackLoader
				.findEachItemScanbackAmtInDiffWeek(currentWeekStartDate, scanBackInputList);
		
		HashMap<String, HashMap<String, ScanBackDTO>> expectedScanbackDetails = new HashMap<>();
		
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, currentWeekStartDate);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week1Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week2Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week3Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week4Start);
		
		assertEquals("Not matching", 5,multiWeekScanbackDetails.size());
		expectedScanbackDetails.forEach((weekDate, values)->{
			assertEquals("Not matching", true,multiWeekScanbackDetails.get(weekDate)!=null);
			HashMap<String, ScanBackDTO> actualScanbackDetail = multiWeekScanbackDetails.get(weekDate);
			values.forEach((key, value)->{
				assertEquals("Not matching", true,actualScanbackDetail.containsKey(key));
				ScanBackDTO actaulSBDTO = actualScanbackDetail.get(key);
				assertEquals("Not matching", value.getRetailerItemCode(), actaulSBDTO.getRetailerItemCode());
				assertEquals("Not matching", value.getZoneNumber(), actaulSBDTO.getZoneNumber());
				assertEquals("Not matching", value.getSplrNo(), actaulSBDTO.getSplrNo());
				assertEquals("Not matching", value.getBnrCD(), actaulSBDTO.getBnrCD());
				assertEquals("Not matching", value.getScanBackAmt1(), actaulSBDTO.getScanBackAmt1());
				assertEquals("Not matching", value.getScanBackEndDate1(), actaulSBDTO.getScanBackEndDate1());
				assertEquals("Not matching", value.getScanBackStartDate1(), actaulSBDTO.getScanBackStartDate1());
				assertEquals("Not matching", value.getScanBackAmt2(), actaulSBDTO.getScanBackAmt2());
				assertEquals("Not matching", value.getScanBackEndDate2(), actaulSBDTO.getScanBackEndDate2());
				assertEquals("Not matching", value.getScanBackStartDate2(), actaulSBDTO.getScanBackStartDate2());
				assertEquals("Not matching", value.getScanBackAmt3(), actaulSBDTO.getScanBackAmt3());
				assertEquals("Not matching", value.getScanBackEndDate3(), actaulSBDTO.getScanBackEndDate3());
				assertEquals("Not matching", value.getScanBackStartDate3(), actaulSBDTO.getScanBackStartDate3());
				assertEquals(value.getScanBackTotalAmt(), actaulSBDTO.getScanBackTotalAmt(), 0);
			});
		});
	}
	
	
	/**
	 * Item has long term scan back amount  
	 * Expected output: New list cost should be calculated by deducting the long term scan back  
	 */
	@Test
	public void TestCase7() {
		CostFileFormatter.calendarDetails = setCalendarDetails();

		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", pastWeek, week5End);
		
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = scanBackLoader
				.findEachItemScanbackAmtInDiffWeek(currentWeekStartDate, scanBackInputList);
		
		HashMap<String, HashMap<String, ScanBackDTO>> expectedScanbackDetails = new HashMap<>();
		
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, currentWeekStartDate);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week1Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week2Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week3Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week4Start);
		
		assertEquals("Not matching", 5,multiWeekScanbackDetails.size());
		expectedScanbackDetails.forEach((weekDate, values)->{
			assertEquals("Not matching", true,multiWeekScanbackDetails.get(weekDate)!=null);
			HashMap<String, ScanBackDTO> actualScanbackDetail = multiWeekScanbackDetails.get(weekDate);
			values.forEach((key, value)->{
				assertEquals("Not matching", true,actualScanbackDetail.containsKey(key));
				ScanBackDTO actaulSBDTO = actualScanbackDetail.get(key);
				assertEquals("Not matching", value.getRetailerItemCode(), actaulSBDTO.getRetailerItemCode());
				assertEquals("Not matching", value.getZoneNumber(), actaulSBDTO.getZoneNumber());
				assertEquals("Not matching", value.getSplrNo(), actaulSBDTO.getSplrNo());
				assertEquals("Not matching", value.getBnrCD(), actaulSBDTO.getBnrCD());
				assertEquals("Not matching", value.getScanBackAmt1(), actaulSBDTO.getScanBackAmt1());
				assertEquals("Not matching", value.getScanBackEndDate1(), actaulSBDTO.getScanBackEndDate1());
				assertEquals("Not matching", value.getScanBackStartDate1(), actaulSBDTO.getScanBackStartDate1());
				assertEquals("Not matching", value.getScanBackAmt2(), actaulSBDTO.getScanBackAmt2());
				assertEquals("Not matching", value.getScanBackEndDate2(), actaulSBDTO.getScanBackEndDate2());
				assertEquals("Not matching", value.getScanBackStartDate2(), actaulSBDTO.getScanBackStartDate2());
				assertEquals("Not matching", value.getScanBackAmt3(), actaulSBDTO.getScanBackAmt3());
				assertEquals("Not matching", value.getScanBackEndDate3(), actaulSBDTO.getScanBackEndDate3());
				assertEquals("Not matching", value.getScanBackStartDate3(), actaulSBDTO.getScanBackStartDate3());
				assertEquals(value.getScanBackTotalAmt(), actaulSBDTO.getScanBackTotalAmt(), 0);
			});
		});
	}
	
	
	/**
	 * Item has multiple long term scan back amount  
	 * Expected output: New list cost should be calculated by deducting the all long term scan back amounts  
	 */
	@Test
	public void TestCase8() {
		CostFileFormatter.calendarDetails = setCalendarDetails();

		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", pastWeek, week5End);
		
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = scanBackLoader
				.findEachItemScanbackAmtInDiffWeek(currentWeekStartDate, scanBackInputList);
		
		HashMap<String, HashMap<String, ScanBackDTO>> expectedScanbackDetails = new HashMap<>();
		
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, currentWeekStartDate);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week1Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week2Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week3Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week4Start);
		
		assertEquals("Not matching", 5,multiWeekScanbackDetails.size());
		expectedScanbackDetails.forEach((weekDate, values)->{
			assertEquals("Not matching", true,multiWeekScanbackDetails.get(weekDate)!=null);
			HashMap<String, ScanBackDTO> actualScanbackDetail = multiWeekScanbackDetails.get(weekDate);
			values.forEach((key, value)->{
				assertEquals("Not matching", true,actualScanbackDetail.containsKey(key));
				ScanBackDTO actaulSBDTO = actualScanbackDetail.get(key);
				assertEquals("Not matching", value.getRetailerItemCode(), actaulSBDTO.getRetailerItemCode());
				assertEquals("Not matching", value.getZoneNumber(), actaulSBDTO.getZoneNumber());
				assertEquals("Not matching", value.getSplrNo(), actaulSBDTO.getSplrNo());
				assertEquals("Not matching", value.getBnrCD(), actaulSBDTO.getBnrCD());
				assertEquals("Not matching", value.getScanBackAmt1(), actaulSBDTO.getScanBackAmt1());
				assertEquals("Not matching", value.getScanBackEndDate1(), actaulSBDTO.getScanBackEndDate1());
				assertEquals("Not matching", value.getScanBackStartDate1(), actaulSBDTO.getScanBackStartDate1());
				assertEquals("Not matching", value.getScanBackAmt2(), actaulSBDTO.getScanBackAmt2());
				assertEquals("Not matching", value.getScanBackEndDate2(), actaulSBDTO.getScanBackEndDate2());
				assertEquals("Not matching", value.getScanBackStartDate2(), actaulSBDTO.getScanBackStartDate2());
				assertEquals("Not matching", value.getScanBackAmt3(), actaulSBDTO.getScanBackAmt3());
				assertEquals("Not matching", value.getScanBackEndDate3(), actaulSBDTO.getScanBackEndDate3());
				assertEquals("Not matching", value.getScanBackStartDate3(), actaulSBDTO.getScanBackStartDate3());
				assertEquals(value.getScanBackTotalAmt(), actaulSBDTO.getScanBackTotalAmt(), 0);
			});
		});
	}
	
	
	/**
	 * Item has 2 long term scan back amount and 1 normal scan back  
	 * Expected output: New list cost should be calculated by deducting only long term scan back amounts  
	 */
	@Test
	public void TestCase9() {
		CostFileFormatter.calendarDetails = setCalendarDetails();

		TestHelper.setScanBackDetails(scanBackInputList, "114507", "1", "", "GE", "0.0500", pastWeek, week5End);
		
		HashMap<String, HashMap<String, ScanBackDTO>> multiWeekScanbackDetails = scanBackLoader
				.findEachItemScanbackAmtInDiffWeek(currentWeekStartDate, scanBackInputList);
		
		HashMap<String, HashMap<String, ScanBackDTO>> expectedScanbackDetails = new HashMap<>();
		
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, currentWeekStartDate);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week1Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week2Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week3Start);
		TestHelper.setScanbackExpectedOutput(expectedScanbackDetails, "114507", "1", "", "GE", (float) 0.0500, "0.0500", pastWeek, week5End, 
				null, null, null, null, null, null, week4Start);
		
		assertEquals("Not matching", 5,multiWeekScanbackDetails.size());
		expectedScanbackDetails.forEach((weekDate, values)->{
			assertEquals("Not matching", true,multiWeekScanbackDetails.get(weekDate)!=null);
			HashMap<String, ScanBackDTO> actualScanbackDetail = multiWeekScanbackDetails.get(weekDate);
			values.forEach((key, value)->{
				assertEquals("Not matching", true,actualScanbackDetail.containsKey(key));
				ScanBackDTO actaulSBDTO = actualScanbackDetail.get(key);
				assertEquals("Not matching", value.getRetailerItemCode(), actaulSBDTO.getRetailerItemCode());
				assertEquals("Not matching", value.getZoneNumber(), actaulSBDTO.getZoneNumber());
				assertEquals("Not matching", value.getSplrNo(), actaulSBDTO.getSplrNo());
				assertEquals("Not matching", value.getBnrCD(), actaulSBDTO.getBnrCD());
				assertEquals("Not matching", value.getScanBackAmt1(), actaulSBDTO.getScanBackAmt1());
				assertEquals("Not matching", value.getScanBackEndDate1(), actaulSBDTO.getScanBackEndDate1());
				assertEquals("Not matching", value.getScanBackStartDate1(), actaulSBDTO.getScanBackStartDate1());
				assertEquals("Not matching", value.getScanBackAmt2(), actaulSBDTO.getScanBackAmt2());
				assertEquals("Not matching", value.getScanBackEndDate2(), actaulSBDTO.getScanBackEndDate2());
				assertEquals("Not matching", value.getScanBackStartDate2(), actaulSBDTO.getScanBackStartDate2());
				assertEquals("Not matching", value.getScanBackAmt3(), actaulSBDTO.getScanBackAmt3());
				assertEquals("Not matching", value.getScanBackEndDate3(), actaulSBDTO.getScanBackEndDate3());
				assertEquals("Not matching", value.getScanBackStartDate3(), actaulSBDTO.getScanBackStartDate3());
				assertEquals(value.getScanBackTotalAmt(), actaulSBDTO.getScanBackTotalAmt(), 0);
			});
		});
	}
}
