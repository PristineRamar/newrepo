package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRConstraintRounding;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class RoundingJUnitTest {

	PRConstraintRounding constraintRounding = new PRConstraintRounding();
	PRExplainLog explainLog = new PRExplainLog();
	PRItemDTO itemInfo = null;
	PRRange priceRange = null;
	int itemCode1 = 1000;
	Integer COST_NO_CHANGE = 0;
	
	@Before
	public void init() {
		explainLog = new PRExplainLog();
		constraintRounding.setRoundingTableId(1);
		constraintRounding.setRoundingTableContent(TestHelper.getRoundingTableTableAZ());
	}
	
	
	/***
	 * GE: Input Range 4.08 - 4.72
	 * Expected output 3.99, 4.29, 4.49, 4.79
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase1() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 2.79d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(4.08);
		priceRange.setEndVal(4.72);
		
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "4.29,4.49", actualRoundingDigits);
	}
	
	/***
	 * GE: Input Range 2.51 - 2.51
	 * Expected output 2.49
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase2() throws GeneralException, Exception, OfferManagementException {
		
		double[] roundingDigits = constraintRounding.getRange(2.51, 2.51, "");
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		assertEquals("JSON Not Matching", "2.49", actualRoundingDigits);
		
		roundingDigits = constraintRounding.getRange(2.51, 2.51, PRConstants.ROUND_UP);
		actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		assertEquals("JSON Not Matching", "2.59", actualRoundingDigits);
		
		roundingDigits = constraintRounding.getRange(2.51, 2.51, PRConstants.ROUND_DOWN);
		actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		assertEquals("JSON Not Matching", "2.49", actualRoundingDigits);
	}
	
	/***
	 * TOPS
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase3() throws GeneralException, Exception, OfferManagementException {
		
		constraintRounding.setRoundingTableId(1);
		constraintRounding.setRoundingTableContent(TestHelper.getRoundingTableTable1());
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 11.29d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(9.95);
		priceRange.setEndVal(9.95);
		
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "9.99", actualRoundingDigits);
	}
	
	/***
	 * TOPS
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase4() throws GeneralException, Exception, OfferManagementException {
		
		constraintRounding.setRoundingTableId(1);
		constraintRounding.setRoundingTableContent(TestHelper.getRoundingTableTable1());
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 2.79d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		itemInfo.setRoundingLogic(PRConstants.ROUND_UP);
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(3.14);
		priceRange.setEndVal(3.14);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setIsBreakingConstraint(true);
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 2.37, 3.21,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 3.14, 3.14, "");
		
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "3.19", actualRoundingDigits);
	}
	
	/***
	 * TOPS
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase5() throws GeneralException, Exception, OfferManagementException {
		
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "09/28/2014", "10/04/2014", false, -1, -1, -1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());
		
		constraintRounding.setRoundingTableId(1);
		constraintRounding.setRoundingTableContent(TestHelper.getRoundingTableTable1());
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 7.99d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, strategy, 0);
		PRRange prRange = new PRRange();
		prRange.setStartVal(8.6);
		prRange.setEndVal(9.22);
		
		itemInfo.setStoreBrandRelationRange(prRange);
		itemInfo.setCostChangeBehavior(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND);
		
		itemInfo.setRoundingLogic(PRConstants.ROUND_UP);
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(8.6);
		priceRange.setEndVal(8.6);
		
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "8.69", actualRoundingDigits);
	}
	
	@Test
	public void testCase7() throws GeneralException, Exception, OfferManagementException {
		
		double[] roundingDigits = constraintRounding.getRange(2.02, 2.02, PRConstants.ROUND_UP);
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		assertEquals("JSON Not Matching", "2.09", actualRoundingDigits);
		
		roundingDigits = constraintRounding.getRange(2.02, 2.02, PRConstants.ROUND_DOWN);
		actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		assertEquals("JSON Not Matching", "1.99", actualRoundingDigits);
	}
	
	@Test
	public void testCase8() throws GeneralException, Exception, OfferManagementException {
		
		double[] roundingDigits = constraintRounding.getRange(1.60, 1.68, "");
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		assertEquals("JSON Not Matching", "1.59,1.69", actualRoundingDigits);
		
	}
	
	@Test
	public void testCase9() throws GeneralException, Exception, OfferManagementException {
		
		constraintRounding.setRoundingTableId(1);
		constraintRounding.setRoundingTableContent(TestHelper.getRoundingTableTable1());
		
		double[] roundingDigits = constraintRounding.getRange(4.03, 4.03, "");
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		assertEquals("JSON Not Matching", "3.99", actualRoundingDigits);
		
		roundingDigits = constraintRounding.getRange(4.03, 4.03, PRConstants.ROUND_UP);
		actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		assertEquals("JSON Not Matching", "4.19", actualRoundingDigits);
		
	}
	
	@Test
	public void testCase10() throws GeneralException, Exception, OfferManagementException {
		
		constraintRounding.setRoundingTableId(1);
		constraintRounding.setRoundingTableContent(TestHelper.getRoundingTableTableTops());
		
		double[] roundingDigits = constraintRounding.getRange(2.33, 2.33, PRConstants.ROUND_UP);
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		assertEquals("JSON Not Matching", "2.39", actualRoundingDigits);
		
		roundingDigits = constraintRounding.getRange(2.33, 2.33, PRConstants.ROUND_DOWN);
		actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		assertEquals("JSON Not Matching", "2.29", actualRoundingDigits);
		
	}
	
	
	/***
	 * Test cases based on GE rounding table
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase11() throws GeneralException, Exception, OfferManagementException {
		
		constraintRounding.setRoundingTableId(1);
		constraintRounding.setRoundingTableContent(TestHelper.getRoundingTableTableGE());
		
		double[] roundingDigits;
		
		roundingDigits = constraintRounding.getRange(2.02, 2.02, "");
		assertEquals("JSON Not Matching", "1.99", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(2.02, 2.02, PRConstants.ROUND_UP);
		assertEquals("JSON Not Matching", "2.09", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(2.02, 2.02, PRConstants.ROUND_DOWN);
		assertEquals("JSON Not Matching", "1.99", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		/*****/
		
		roundingDigits = constraintRounding.getRange(2.05, 2.14, "");
		assertEquals("JSON Not Matching", "2.09", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(2.05, 2.14, PRConstants.ROUND_UP);
		assertEquals("JSON Not Matching", "2.09", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(2.05, 2.14, PRConstants.ROUND_DOWN);
		assertEquals("JSON Not Matching", "2.09", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		/*****/
		
		roundingDigits = constraintRounding.getRange(1.75, 2.14, "");
		assertEquals("JSON Not Matching", "1.79,1.89,1.99,2.09", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(1.75, 2.14, PRConstants.ROUND_UP);
		assertEquals("JSON Not Matching", "1.79,1.89,1.99,2.09", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(1.75, 2.14, PRConstants.ROUND_DOWN);
		assertEquals("JSON Not Matching", "1.79,1.89,1.99,2.09", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		
		roundingDigits = constraintRounding.getRange(4.65, 5.24, "");
		assertEquals("JSON Not Matching", "4.79,4.99", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(4.65, 5.24, PRConstants.ROUND_UP);
		assertEquals("JSON Not Matching", "4.79,4.99", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(4.65, 5.24, PRConstants.ROUND_DOWN);
		assertEquals("JSON Not Matching", "4.79,4.99", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(4.25, 4.25, PRConstants.ROUND_DOWN);
		assertEquals("JSON Not Matching", "3.99", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
	}
	
	@Test
	public void testCase12() throws GeneralException, Exception, OfferManagementException {
		
		constraintRounding.setRoundingTableId(1);
		constraintRounding.setRoundingTableContent(TestHelper.getRoundingTableTableGE());
		
		double[] roundingDigits;
		
		roundingDigits = constraintRounding.getRange(4.55, 4.89, "");
		assertEquals("JSON Not Matching", "4.79", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(3.6, 3.94, "");
		assertEquals("JSON Not Matching", "3.79", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(2.63, 2.63, "");
		assertEquals("JSON Not Matching", "2.59", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));

		roundingDigits = constraintRounding.getRange(2.63, 2.63, PRConstants.ROUND_UP);
		assertEquals("JSON Not Matching", "2.69", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
		
		roundingDigits = constraintRounding.getRange(2.63, 2.63, PRConstants.ROUND_DOWN);
		assertEquals("JSON Not Matching", "2.59", PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits));
	}
	
	@Test
	public void testCase13() throws GeneralException, Exception, OfferManagementException {
		constraintRounding.setRoundingTableId(1);
		constraintRounding.setRoundingTableContent(TestHelper.getRoundingTableTableGE());
		
		PRItemDTO itemDTO = new PRItemDTO();
		itemDTO.setRecommendedRegPrice(new MultiplePrice(1,2.74));
		constraintRounding.getNextAndPreviousPrice(itemDTO, false);
		
		//1.79, 1.89, 1.99, 2.09, 2.19, 2.29, 2.39, 2.49, 2.59, 2.69, 2.79, 2.89, 2.99, 3.29, 3.49, 2.74
	}
	
	/**
	 * There are no rounding digits available within the range.  Current Price: 3.99.  
	 * Rounding down is chosen, as rounding up will break the threshold.
	 * 
	 * Index: 4.39	4.39
	 * Threshold range:	1.40	4.19
	 * Final Range:	4.19	4.19
	 * Prev & Next Rounding Digit:	3.99	4.29
	 * Final Rounding Digit:	3.99
	 * Explanation:	Rounding up (4.29) will break the threshold constraint, rounded down to 3.99
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase14() throws GeneralException, Exception, OfferManagementException {
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 3.99, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		itemInfo.setRoundingLogic(PRConstants.ROUND_DOWN);
		priceRange = new PRRange();
		priceRange.setStartVal(4.19);
		priceRange.setEndVal(4.19);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1 = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog1, true, false, 0d, 4.39, 4.39, 2.28, "", new MultiplePrice(1, 2.56));
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setIsBreakingConstraint(true);
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.40, 4.19,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.19, 4.19, "");

		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);

		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);

		assertEquals("JSON Not Matching", "3.99", actualRoundingDigits);
	}
	
	/**
	 * There are no rounding digits available within the range.  
	 * Current Price: 3.99.  
	 * Rounding up is chosen, as both rounding up and down will break the threshold but rounding down will also break the cost constraint.  
	 * Importance is given to the cost constraint.
	 * 
	 * Index:4.39	4.39
	 * Threshold range:1.40	4.29
	 * Cost:4.19	
	 * Final Range:4.19	4.29
	 * Prev & Next Rounding Digit:3.99	4.29
	 * Final Rounding Digit:4.29
	 * Explanation: Here rounding up (4.29) will break the threshold constraint, 
	 * but rounding down (3.99) will also break the cost constraint, 
	 * but the importance will be given to cost constraint and it will be rounded up
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase15() throws GeneralException, Exception, OfferManagementException {
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 3.99d, 0d, 4.19, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		priceRange = new PRRange();
		priceRange.setStartVal(4.19);
		priceRange.setEndVal(4.29);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setIsBreakingConstraint(false);
		TestHelper.setThresholdLog(explainLog, guidelineAndConstraintLog, true, false, 1.40, 4.29,
				Constants.DEFAULT_NA, Constants.DEFAULT_NA, 4.19, 4.29, "");

		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);

		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);

		assertEquals("JSON Not Matching", "4.29", actualRoundingDigits);
	}
	
	/**
	 * There are no rounding digits available within the range.  
	 * Current Price: 2.19.  
	 * Rounding down is chosen even though rounding up is closer to the final range, 
	 * as it will break the first guideline in the strategy.  In this example, the Index guideline
	 * 
	 * Index:	--	2.28
	 * Final Range:	2.28	2.28
	 * Prev & Next Rounding Digit:2.19	2.29
	 * Final Rounding Digit:2.19
	 * Explanation	Here the 2.29 (rounded up) is closer to final range, 
	 * but breaking the first guideline (Index), so 2.19 is chosen as the final rounding digit
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase16() throws GeneralException, Exception, OfferManagementException {
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 2.19d, 0d, 2.19, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		priceRange = new PRRange();
		priceRange.setStartVal(2.28);
		priceRange.setEndVal(2.28);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1 = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog1, true, false, 0d, 2.28, 2.28, 2.28, "", new MultiplePrice(1, 2.56));


		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);

		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);

		assertEquals("JSON Not Matching", "2.19", actualRoundingDigits);
	}
	
	/**
	 * There are no rounding digits available within the range.  
	 * Current Price: 2.39.  Rounding up is chosen as it is closer to the final range
	 * 
	 * Index: --	2.49
	 * Final Range: 2.38	2.38
	 * Prev & Next Rounding Digit: 2.29	2.39
	 * Final Rounding Digit: 2.39
	 * Explanation:	Here the 2.39 (rounded up) is closer to the final range, so price point 2.39 is chosen as the final rounding digit
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase17() throws GeneralException, Exception, OfferManagementException {
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 2.39, 0d, 2.19, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		priceRange = new PRRange();
		priceRange.setStartVal(2.38);
		priceRange.setEndVal(2.38);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1 = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog1, true, false, 0d, 2.49, 2.28, 2.28, "", new MultiplePrice(1, 2.56));


		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);

		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);

		assertEquals("JSON Not Matching", "2.39", actualRoundingDigits);
	}
	
	/**
	 * There are no rounding digits available within the range.  
	 * Current Price: 2.29.  Both rounding up and rounding down is closer to the final price range.
	 * 
	 * Index	--	2.42
	 * Final Range	2.24	2.24
	 * Prev & Next Rounding Digit	2.19	2.29
	 * Final Rounding Digit	2.29
	 * Explanation	Here both 2.19 & 2.29 is closer to current price, but the preference is given to the price which meets the objective, so 2.39 is chosen as the final rounding digit

	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase18() throws GeneralException, Exception, OfferManagementException {
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 2.29, 0d, 2.19, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		priceRange = new PRRange();
		priceRange.setStartVal(2.24);
		priceRange.setEndVal(2.24);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1 = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog1, true, false, 0d, 2.42, 2.28, 2.28, "", new MultiplePrice(1, 2.56));


		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);

		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);

		assertEquals("JSON Not Matching", "2.29", actualRoundingDigits);
	}
	
	/**
	 * There are no rounding digits available within the range.  
	 * Current Price: 26.99.  Both rounding up and rounding down is breaking the index guideline
	 * 
	 * Index	27.28	27.28
	 * Final Range	27.00	27.97
	 * Prev & Next Rounding Digit	26.99	27.99
	 * Final Rounding Digit	26.99
	 * Explanation	Here 26.99 is closer to final price, but it is also breaking the first guideline in the strategy, 
	 * the index guideline. At the same time 27.99 is also breaking the index guideline. 
	 * In this case stay with the price point (26.99) which is closer to final price range (.01 difference vs .98 difference)
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase19() throws GeneralException, Exception, OfferManagementException {
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 26.99, 0d, 2.19, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		priceRange = new PRRange();
		priceRange.setStartVal(27.00);
		priceRange.setEndVal(27.97);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1 = new PRGuidelineAndConstraintLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog1, true, false, 27.28, 27.28, 2.28, 2.28, "", new MultiplePrice(1, 2.56));


		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);

		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);

		assertEquals("JSON Not Matching", "26.99", actualRoundingDigits);
	}
	
	/**
	 * There is only 1 rounding digit available within the range. Current Price: 2.19
	 * 
	 * Final Range: 2.28 2.35
	 * Rounding Choices: 2.29
	 * Final Rounding Digit: 2.29
	 * Explanation: Since 2.29 is available, that is selected irrespective of the objective.
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase20() throws GeneralException, Exception, OfferManagementException {
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 2.19, 0d, 2.19, 0d, COST_NO_CHANGE, 0, 0d, null, 0);

		priceRange = new PRRange();
		priceRange.setStartVal(2.28);
		priceRange.setEndVal(2.35);

		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog);

		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);

		assertEquals("JSON Not Matching", "2.29", actualRoundingDigits);
	}


	/***
	 * AZ: Input Range 11.8 - 11.8
	 * Expected output 11.49 for ROUND_CLOSEST AND ROUND_DOWN
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase21() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 71.49d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(11.8);
		priceRange.setEndVal(11.8);
		
		PRRange priceRange1 = new PRRange();
		priceRange1.setStartVal(8.85);
		priceRange1.setEndVal(11.8);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(1);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange1);
		List<PRGuidelineAndConstraintLog> logs = new ArrayList<>();
		logs.add(guidelineAndConstraintLog);
		explainLog.setGuidelineAndConstraintLogs(logs);
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog, PRConstants.ROUND_DOWN);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "11.49", actualRoundingDigits);
	}
	
	
	/***
	 * AZ: Input Range 11.8 - 11.8
	 * Expected output 11.99 for ROUND_UP
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase22() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 71.49d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(11.8);
		priceRange.setEndVal(11.8);
		
		PRRange priceRange1 = new PRRange();
		priceRange1.setStartVal(8.85);
		priceRange1.setEndVal(11.8);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(1);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange1);
		List<PRGuidelineAndConstraintLog> logs = new ArrayList<>();
		logs.add(guidelineAndConstraintLog);
		explainLog.setGuidelineAndConstraintLogs(logs);
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog, PRConstants.ROUND_UP);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "11.99", actualRoundingDigits);
	}
	
	/***
	 * AZ: Input Range 29.25 - 29.25
	 * Expected output 29.49 for ROUND_UP
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase23() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 71.49d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(29.25);
		priceRange.setEndVal(29.25);
		
		PRRange priceRange1 = new PRRange();
		priceRange1.setStartVal(21.94);
		priceRange1.setEndVal(29.25);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(1);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange1);
		List<PRGuidelineAndConstraintLog> logs = new ArrayList<>();
		logs.add(guidelineAndConstraintLog);
		explainLog.setGuidelineAndConstraintLogs(logs);
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog, PRConstants.ROUND_UP);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "29.49", actualRoundingDigits);
	}
	
	/***
	 * AZ: Input Range 29.25 - 29.25
	 * Expected output 28.99 for ROUND_DOWN and ROUND_CLOSEST
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase24() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 71.49d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(29.25);
		priceRange.setEndVal(29.25);
		
		PRRange priceRange1 = new PRRange();
		priceRange1.setStartVal(21.94);
		priceRange1.setEndVal(29.25);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(1);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange1);
		List<PRGuidelineAndConstraintLog> logs = new ArrayList<>();
		logs.add(guidelineAndConstraintLog);
		explainLog.setGuidelineAndConstraintLogs(logs);
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog, PRConstants.ROUND_CLOSEST);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "28.99", actualRoundingDigits);
	}
	
	
	/***
	 * AZ: Input Range 41.19 - 41.19
	 * Expected output 41.49 for ROUND_CLOSEST
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase25() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 71.49d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(41.19);
		priceRange.setEndVal(41.19);
		
		PRRange priceRange1 = new PRRange();
		priceRange1.setStartVal(41.19);
		priceRange1.setEndVal(41.19);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(1);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange1);
		List<PRGuidelineAndConstraintLog> logs = new ArrayList<>();
		logs.add(guidelineAndConstraintLog);
		explainLog.setGuidelineAndConstraintLogs(logs);
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog, PRConstants.ROUND_CLOSEST);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "41.49", actualRoundingDigits);
	}
	
	
	/***
	 * AZ: Input Range 41.19 - 41.19
	 * Expected output 40.49 for ROUND_CLOSEST
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase26() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 71.49d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(41.19);
		priceRange.setEndVal(41.19);
		
		PRRange priceRange1 = new PRRange();
		priceRange1.setStartVal(41.19);
		priceRange1.setEndVal(41.19);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(1);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange1);
		List<PRGuidelineAndConstraintLog> logs = new ArrayList<>();
		logs.add(guidelineAndConstraintLog);
		explainLog.setGuidelineAndConstraintLogs(logs);
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog, PRConstants.ROUND_DOWN);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "40.49", actualRoundingDigits);
	}
	
	/***
	 * AZ: Input Range 81.45 - 81.45
	 * Expected output 81.49 for ROUND_UP
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase27() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 71.49d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(81.45);
		priceRange.setEndVal(81.45);
		
		PRRange priceRange1 = new PRRange();
		priceRange1.setStartVal(81.45);
		priceRange1.setEndVal(81.45);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(1);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange1);
		List<PRGuidelineAndConstraintLog> logs = new ArrayList<>();
		logs.add(guidelineAndConstraintLog);
		explainLog.setGuidelineAndConstraintLogs(logs);
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog, PRConstants.ROUND_UP);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "81.49", actualRoundingDigits);
	}
	
	/***
	 * AZ: Input Range 81.45 - 81.45
	 * Expected output 81.49 for ROUND_UP
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase28() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 71.49d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(81.45);
		priceRange.setEndVal(81.45);
		
		PRRange priceRange1 = new PRRange();
		priceRange1.setStartVal(81.45);
		priceRange1.setEndVal(81.45);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(1);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange1);
		List<PRGuidelineAndConstraintLog> logs = new ArrayList<>();
		logs.add(guidelineAndConstraintLog);
		explainLog.setGuidelineAndConstraintLogs(logs);
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog, PRConstants.ROUND_CLOSEST);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "81.49", actualRoundingDigits);
	}
	
	
	/***
	 * AZ: Input Range 81.45 - 81.45
	 * Expected output 81.49 for ROUND_UP
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase29() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 71.49d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(81.45);
		priceRange.setEndVal(81.45);
		
		PRRange priceRange1 = new PRRange();
		priceRange1.setStartVal(81.45);
		priceRange1.setEndVal(81.45);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(1);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange1);
		List<PRGuidelineAndConstraintLog> logs = new ArrayList<>();
		logs.add(guidelineAndConstraintLog);
		explainLog.setGuidelineAndConstraintLogs(logs);
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog, PRConstants.ROUND_DOWN);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "80.49", actualRoundingDigits);
	}
	
	/***
	 * AZ: Input Range 81.45 - 81.45
	 * Expected output 81.49 for ROUND_UP
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase30() throws GeneralException, Exception, OfferManagementException {
		
		// Set item details
		itemInfo = TestHelper.getTestItem(itemCode1, 1, 71.49d, 0d, 0d, 0d, COST_NO_CHANGE, 0, 0d, null, 0);
		
		// Input range
		priceRange = new PRRange();
		priceRange.setStartVal(0.01);
		priceRange.setEndVal(0.01);
		
		PRRange priceRange1 = new PRRange();
		priceRange1.setStartVal(0.01);
		priceRange1.setEndVal(0.01);
		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setGuidelineTypeId(1);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange1);
		List<PRGuidelineAndConstraintLog> logs = new ArrayList<>();
		logs.add(guidelineAndConstraintLog);
		explainLog.setGuidelineAndConstraintLogs(logs);
		Double[] roundingDigits = constraintRounding.getRoundingDigits(itemInfo, priceRange, explainLog, PRConstants.ROUND_UP);
		
		String actualRoundingDigits = PRCommonUtil.getCommaSeperatedStringFromDouble(roundingDigits);
		
		assertEquals("JSON Not Matching", "0.19", actualRoundingDigits);
	}
}
