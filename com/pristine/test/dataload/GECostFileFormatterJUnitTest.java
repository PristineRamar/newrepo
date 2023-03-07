package com.pristine.test.dataload;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleAllowanceDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleCostDTO;
import com.pristine.exception.GeneralException;
import com.pristine.fileformatter.gianteagle.CostFileFormatter;
import com.pristine.util.PropertyManager;

public class GECostFileFormatterJUnitTest {
	private static Logger logger = Logger.getLogger("GECostFileFormatterJUnit");
	List<GiantEagleCostDTO> costList = null;
	List<GiantEagleAllowanceDTO> allowanceList = null;
	String currentWeekStartDate = "03/08/2018", pastWeek = "02/10/2016", currentWeekEndDate="03/14/2018", week1Start="03/15/2018", 
			week1End="03/21/2018", week2Start="03/22/2018", week2End="03/28/2018", week3Start="03/29/2018", week3End="04/04/2018",
			week4Start="04/05/2018", week4End="04/11/2018", pastStart="02/04/2016", pastEnd="02/10/2016";
	CostFileFormatter costFileFormatter = null;
	
	@Before
	public void init() {
		PropertyManager.initialize("GEDataloadAnalysis.properties");
		costList = new ArrayList<>();
		allowanceList = new ArrayList<>();
		costFileFormatter = new CostFileFormatter();
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
	 * Item has Current cost record with Start Date effective from 02/10/2016. It has no allowance cost. 
	 * Expected output: For current Week and future week (4 weeks) same item with multiple entries should be created.
	 * Future record should future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase1() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, currentWeekStartDate, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000,
				5.7, "N", "GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week1Start, "007314388662", "322792", week1Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
				
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Current cost record with Start Date effective from 02/10/2016 and Future cost record in Week3. It has no allowance cost. 
	 * Expected output: For current Week and future week 1 and 2 current cost must be used. For Week 3 & 4 new cost must be applied.
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase2() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setCostDetails(costList, "007314388662", "322792", week3Start, "1", "", "F", 7.5000, 4.7, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, currentWeekStartDate, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000,
				5.7, "N", "GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week1Start, "007314388662", "322792", week1Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 4.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 4.7, "N",
				"GE", null, null, 0, 0);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
				
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Future cost record in Week3. It has no allowance cost. 
	 * Expected output: For current Week and future week 1 and 2 there must not be any entry for this item. For Week 3 & 4 new cost must be applied.
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase3() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", week3Start, "1", "", "F", 7.5000, 4.7, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 4.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 4.7, "N",
				"GE", null, null, 0, 0);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
				
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Future cost record in Week 2 & 4. It has no allowance cost. 
	 * Expected output: For current Week and future week 1 there must not be any entry for this item. For Week 2 & 3 week 2 cost must be applied.
	 * And in week 4 respective cost me applied
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase4() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", week2Start, "1", "", "F", 7.5000, 4.7, "N", "GE");
		TestHelper.setCostDetails(costList, "007314388662", "322792", week4Start, "1", "", "F", 7.5000, 5.7, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 4.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 4.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
				
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Current cost record with Start Date effective from 02/10/2016. It has allowance amt from past week till Week 4 (For all weeks). 
	 * Expected output: For current Week and future weeks Deal cost must be applied
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase5() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", pastWeek, week4End, "1", "", "C", 0, 3.2, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, currentWeekStartDate, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000,
				5.7, "N", "GE", pastWeek, week4End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week1Start, "007314388662", "322792", week1Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week4End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week4End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week4End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week4End, 3.2, 2.5);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
				
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Current cost record with Start Date effective from 02/10/2016. It has allowance amt from past week till Week 2 (For all weeks). 
	 * Expected output: For current Week and future week 1 & 2 Deal cost must be applied and rest of the weeks deal cost must be zero.
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase6() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", pastWeek, week2End, "1", "", "C", 2.5000, 0, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, currentWeekStartDate, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000,
				5.7, "N", "GE", pastWeek, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week1Start, "007314388662", "322792", week1Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
				
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Current cost record with Start Date effective from 02/10/2016. It has allowance amount for Week 2. 
	 * Expected output: For Week 2 Deal cost must be applied and rest of the weeks deal cost must be zero.
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase7() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", week2Start, week2End, "1", "", "F", 2.5000, 0, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, currentWeekStartDate, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000,
				5.7, "N", "GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week1Start, "007314388662", "322792", week1Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", week2Start, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
				
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Current cost record with Start Date effective from 02/10/2016. It has allowance amount from past Week till Week 4 as Nested type.
	 * And for Week 2 it has future Allowance record. 
	 * Expected output: For Week 2 Future Deal cost must be applied and rest of the weeks Nested type deal cost must be applied.
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase8() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", pastWeek, week4End, "1", "", "N", 2.7500, 0, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", week2Start, week2End, "1", "", "F", 2.5000, 0, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, currentWeekStartDate, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000,
				5.7, "N", "GE", pastWeek, week4End, 2.95, 2.75);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week1Start, "007314388662", "322792", week1Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week4End, 2.95, 2.75);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", week2Start, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week4End, 2.95, 2.75);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week4End, 2.95, 2.75);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Current cost record with Start Date effective from 02/10/2016. It has allowance amount from past Week till Week 4 as Nested type.
	 * And from Current Week till Week 2 it has Current Allowance record. 
	 * Expected output: From Current Week till Week 2 Current Deal cost must be applied and rest of the weeks Nested type deal cost must be applied.
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase9() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", pastWeek, week4End, "1", "", "N", 2.7500, 0, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", currentWeekStartDate, week2End, "1", "", "C", 2.5000, 0, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, currentWeekStartDate, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000,
				5.7, "N", "GE", currentWeekStartDate, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week1Start, "007314388662", "322792", week1Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", currentWeekStartDate, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", currentWeekStartDate, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week4End, 2.95, 2.75);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week4End, 2.95, 2.75);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Current cost record with Start Date effective from 02/10/2016. It has allowance amount from Current Week till Week 2 it has Current 
	 * Allowance record and Week 4 has future Allowance cost. 
	 * Expected output: From Current Week till Week 2 Current Deal cost must be applied and in week 4 future deal cost must be applied.
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase10() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", week4Start, week4End, "1", "", "F", 2.7500, 0, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", currentWeekStartDate, week2End, "1", "", "C", 2.5000, 0, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, currentWeekStartDate, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000,
				5.7, "N", "GE", currentWeekStartDate, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week1Start, "007314388662", "322792", week1Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", currentWeekStartDate, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", currentWeekStartDate, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", week4Start, week4End, 2.95, 2.75);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has future cost record with Start Date effective week 4. It has allowance amount from Current Week till Week 2 it has Current 
	 * Allowance record and Week 4 has future Allowance cost. 
	 * Expected output: Only week 4 future deal cost must be applied.
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase11() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", week4Start, "1", "", "F", 7.5000, 5.7, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", week4Start, week4End, "1", "", "F", 2.7500, 0, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", currentWeekStartDate, week2End, "1", "", "C", 2.5000, 0, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();

		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", week4Start, week4End, 2.95, 2.75);
		
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Current cost record with Start Date effective from 02/10/2016. It has allowance amount from Current Week till Week 2 it has Current 
	 * Allowance record and Week 4 has future Allowance cost. And from past week till week 4 Nested type allowance cost is available
	 * Expected output: From Current Week till Week 2 Current Deal cost must be applied and in week 4 future deal cost must be applied.
	 * Week 3 should have Nested Allowance amt
	 * Same item with multiple entries should be created.
	 * Future record must have future week start date.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase12() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", pastWeek, week4End, "1", "", "N", 2.0000, 0, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", week4Start, week4End, "1", "", "F", 2.7500, 0, "N", "GE");
		TestHelper.setAllwDetails(allowanceList, "007314388662", "322792", currentWeekStartDate, week2End, "1", "", "C", 2.5000, 0, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, currentWeekStartDate, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000,
				5.7, "N", "GE", currentWeekStartDate, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week1Start, "007314388662", "322792", week1Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", currentWeekStartDate, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", currentWeekStartDate, week2End, 3.2, 2.5);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", pastWeek, week4End, 3.7, 2);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", week4Start, week4End, 2.95, 2.75);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
	
	/**
	 * Item has Current cost record with Start Date effective from 02/10/2016 and Same item has future records with current week effective date
	 * and it has Non everyday item SPLR Number. It has no allowance cost. 
	 * Expected output: For current Week and future week (4 weeks) same item with multiple entries should be created.
	 * And second record should be considered in this process.
	 * @throws GeneralException 
	 */
	@Test
	public void TestCase13() throws GeneralException {
		TestHelper.setCostDetails(costList, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000, 5.7, "N", "GE");
		TestHelper.setCostDetails(costList, "007314388662", "", currentWeekStartDate, "1", "103", "C", 7.5000, 5.7, "N", "GE");
		CostFileFormatter.calendarDetails = setCalendarDetails();
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = costFileFormatter.processCurrentAndFutureWeeksCost(costList,
				allowanceList, currentWeekStartDate);

		HashMap<String, HashMap<String, GiantEagleCostDTO>> expectedMulWeekCostDetails = new HashMap<>();
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, currentWeekStartDate, "007314388662", "322792", pastWeek, "1", "", "C", 7.5000,
				5.7, "N", "GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week1Start, "007314388662", "322792", week1Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week2Start, "007314388662", "322792", week2Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week3Start, "007314388662", "322792", week3Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		TestHelper.setExpectedItemOutput(expectedMulWeekCostDetails, week4Start, "007314388662", "322792", week4Start, "1", "", "C", 7.5000, 5.7, "N",
				"GE", null, null, 0, 0);
		
		assertEquals("Not Matching", 5, multipleWeekCostDetails.size());
		expectedMulWeekCostDetails.forEach((weekDate,values)->{
			
			assertEquals("Not matching", true,multipleWeekCostDetails.get(weekDate)!=null);
			HashMap<String, GiantEagleCostDTO> actualCostDetail = multipleWeekCostDetails.get(weekDate);
			values.forEach((key,value)->{
				assertEquals("Not matching", true,actualCostDetail.containsKey(key));
				GiantEagleCostDTO actualObj = actualCostDetail.get(key);
				assertEquals("Not matching", value.getUPC(), actualObj.getUPC());
				assertEquals("Not matching", value.getWHITEM_NO(), actualObj.getWHITEM_NO());
				assertEquals(value.getBS_CST_AKA_STORE_CST(), actualObj.getBS_CST_AKA_STORE_CST(), 0);
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(), 0);
				assertEquals("Not matching", value.getSTRT_DTE(), actualObj.getSTRT_DTE());
				assertEquals(value.getDLVD_CST_AKA_WHSE_CST(), actualObj.getDLVD_CST_AKA_WHSE_CST(),0);
				assertEquals("Not matching", value.getCST_ZONE_NO(), actualObj.getCST_ZONE_NO());
				assertEquals(value.getDealCost(), actualObj.getDealCost(),0);
				assertEquals("Not matching", value.getBNR_CD(), actualObj.getBNR_CD());
				assertEquals("Not matching", value.getLONG_TERM_REFLECT_FG(), actualObj.getLONG_TERM_REFLECT_FG());
				assertEquals(value.getALLW_AMT(), actualObj.getALLW_AMT(),0);
				assertEquals("Not matching", value.getDealEndDate(), actualObj.getDealEndDate());
				assertEquals("Not matching", value.getDealStartDate(), actualObj.getDealStartDate());
				assertEquals("Not matching", value.getSPLR_NO(), actualObj.getSPLR_NO());
				
			});
			assertEquals(expectedMulWeekCostDetails.get(weekDate).values().toString(),values.values().toString());
		});
	}
}
