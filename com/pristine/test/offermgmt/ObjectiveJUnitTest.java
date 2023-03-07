package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.BrandClassLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ObjectiveService;
import com.pristine.service.offermgmt.PriceGroupService;
import com.pristine.service.offermgmt.PriceRollbackService;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class ObjectiveJUnitTest {
	private static Logger logger = Logger.getLogger("ObjectiveJUnitTest");

	String week1StartDate = "11/27/2016";
	String week2StartDate = "11/20/2016";
	String week3StartDate = "11/13/2016";
	String week4StartDate = "11/06/2016";
	String week5StartDate = "10/30/2016";
	String week6StartDate = "10/23/2016";
	String week7StartDate = "10/16/2016";
	String week8StartDate = "10/09/2016";
	String week9StartDate = "10/02/2016";
	String week15StartDate = "08/21/2016";

	String regEffectiveDate1 = "11/06/2016";
	ObjectiveService objectiveService = null;
	private int itemCode1 = 100001;
	private int itemCode2 = 100002;
	private int itemCode3 = 100003;

	// private static final Integer COST_DECREASE = -1;
	private static final Integer COST_UNCHANGED = 0;
	private int NO_OF_STORES_1 = 12;
	String recommendationWeekStartDate = "01/08/2017";
	String recRunningWeekStartDate = "01/01/2017";
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.getMultipleRecommendationRuleMapFull(); 

	double maxUnitPriceDiff = Double.parseDouble(PropertyManager.getProperty("REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE", "0"));
	
	@Before
	public void init() {
		// PropertyConfigurator.configure("log4j-pricing-engine.properties");
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClients.properties");

		objectiveService = new ObjectiveService();
	}

	/**
	 * Rule : R1 - If Reg retail was increased during last 13 weeks, then the retail will not be increased again unless such increase is
	 * warranted by a cost increase that took place within last 13 weeks or due to a cost increase that’s scheduled to take place
	 * in the next 2 weeks.
	 * 
	 * Retail increased, cost unchanged
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase001() throws Exception, GeneralException {
		
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R1", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.69, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.49,2.59,2.69";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R1 - If Reg retail was increased during last 13 weeks, then the retail will not be increased again unless such increase is
	 * warranted by a cost increase that took place within last 13 weeks or due to a cost increase that’s scheduled to take place
	 * in the next 2 weeks.
	 * 
	 * Retail increased, cost increased
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase002() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R1", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.69, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.49,2.59,2.69,2.79";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R2 - If Reg retail was decreased during last 13 weeks, then the retail will not be increased, unless such increase was
	 * necessitated by a cost increase
	 * 
	 * Retail decreased, cost unchanged
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase003() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79,2.89";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R2", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.69, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.79, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.49,2.59,2.69";
		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R2 - If Reg retail was decreased during last 13 weeks, then the retail will not be increased, unless such increase was
	 * necessitated by a cost increase
	 * 
	 * Retail decreased, cost increased
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase004() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79,2.89";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R2", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.69, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.79, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.49,2.59,2.69,2.79,2.89";
		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R5 - If Reg retail was decreased during last 13 weeks, then the retail will not be decreased, unless such decrease was supported
	 * by a cost decrease or to meet the Index/Competition guideline
	 * 
	 * Retail decreased, cost unchanged, there is no margin guideline
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase005() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79,2.89";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R5", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.69, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.79, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "";
		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R5 - If Reg retail was decreased during last 13 weeks, then the retail will not be decreased, unless such decrease was supported
	 * by a cost decrease or to meet the Index/Competition guideline
	 * 
	 * Retail decreased, cost decreased, there is no margin guideline
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase006() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79,2.89";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R5", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.69, 0d, -1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.79, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.49,2.59,2.69,2.79,2.89";
		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule R5 - If Reg retail was decreased during last 13 weeks, then the retail will not be decreased, unless such decrease was supported
	 * by a cost decrease or to meet the Index/Competition guideline
	 * 
	 * Retail decreased, cost unchanged, there is margin guideline
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase007() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79,2.89";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R5", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.69, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		PRExplainLog explainLog = new PRExplainLog();
		TestHelper.setPriceIndexLog(explainLog, guidelineAndConstraintLog, true, true, 12.62, 13.15, 12.02, 12.02, "", new MultiplePrice(1, 12.49));
		itemDTO.setExplainLog(explainLog);

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.79, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.49,2.59,2.69,2.79,2.89";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R3 - Retail is not increased greater than 5% than the highest Reg price point in last 52 weeks, unless there was a cost increase
	 * 
	 * Retail increased, cost unchanged
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase008() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79,2.89,2.99,3.19";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R3", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week15StartDate, 1, 2.69, 0d, week15StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.79,2.49,2.59,2.69";

		assertEquals("Mismatch", expPricePoints, filteredPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R3 - Retail is not increased greater than 5% than the highest Reg price point in last 52 weeks, unless there was a cost increase
	 * 
	 * Retail increased, decreased, increased in the past, cost unchanged
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase009() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "15.49,15.79,15.99,16.29,16.49,16.79,16.99,17.29,18.59";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R3", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 16.99, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 16.99, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 15.99, 0d, week3StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 15.99, 0d, week3StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 13.99, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 13.99, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week15StartDate, 1, 14.99, 0d, week15StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO,recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "16.29,16.79,17.29,15.79,15.49,16.49,16.99,15.99";
		
		

		assertEquals("Mismatch", expPricePoints, filteredPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R3 - Retail is not increased greater than 5% than the highest Reg price point in last 52 weeks, unless there was a cost increase
	 * 
	 * Retail increased, decreased, increased in the past, cost increased
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase010() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "15.49,15.79,15.99,16.29,16.49,16.79,16.99,17.29,18.59";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R3", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 16.99, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 16.99, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 15.99, 0d, week3StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 15.99, 0d, week3StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 13.99, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 13.99, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week15StartDate, 1, 14.99, 0d, week15StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO,recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "16.29,16.79,17.29,15.79,15.49,16.49,16.99,15.99,18.59";

		assertEquals("Mismatch", expPricePoints, filteredPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R4A - If item hasn’t gone through a Reg retail change during last 52 weeks Retail will not be increased more than 5%, unless
	 * warranted by a cost increase
	 * 
	 * Retail unchanged, cost unchanged
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase011() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79,2.89,2.99,3.19";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4A", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.49,2.59,2.69";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R4A - If item hasn’t gone through a Reg retail change during last 52 weeks Retail will not be increased more than 5%, unless
	 * warranted by a cost increase
	 * 
	 * Retail unchanged, cost increased
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase012() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79,2.89,2.99,3.19";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4A", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.49,2.59,2.69,2.79,2.89,2.99,3.19";
		System.out.println("filtered price " + filteredPricePoints + "  :: ");
		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R4B - If item hasn’t gone through a Reg retail change during last 52 weeks Retail will not be reduced below the highest sale/TPR
	 * price observed in last 52 weeks, unless there is a cost decrease.
	 * 
	 * Retail unchanged, cost increased
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase013() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "1.99,2.09,2.19,2.29";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4B", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 2, 0, 2.19, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 2, 0, 2.19, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "1.99,2.29,2.19,2.09";

		assertEquals("Mismatch", expPricePoints, filteredPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R4B - If item hasn’t gone through a Reg retail change during last 52 weeks Retail will not be reduced below the highest sale/TPR
	 * price observed in last 52 weeks, unless there is a cost decrease.
	 * 
	 * Retail unchanged, cost unchanged
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase014() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "1.99,2.09,2.19,2.29";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4B", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 1, 2.19, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 1, 2.29, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.29";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R4B - If item hasn’t gone through a Reg retail change during last 52 weeks Retail will not be reduced below the highest sale/TPR
	 * price observed in last 52 weeks, unless there is a cost decrease.
	 * 
	 * Retail unchanged, cost decreased
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase015() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "1.99,2.09,2.19,2.29";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4B", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, -1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 1, 2.19, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 1, 2.29, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.29";

		assertEquals("Mismatch", expPricePoints, filteredPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R4C - If item hasn’t gone through a Reg retail change during last 52 weeks If Item was never on sale during last 52 weeks, Retail
	 * will not be reduced by > 5%, unless there is a cost decrease or comp price decrease
	 * 
	 * Retail unchanged, no sale history, cost increased
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase016() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "1.99,2.09,2.19,2.29,2.59";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4C", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.59";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R4C - If item hasn’t gone through a Reg retail change during last 52 weeks If Item was never on sale during last 52 weeks, Retail
	 * will not be reduced by > 5%, unless there is a cost decrease or comp price decrease
	 * 
	 * Retail unchanged, no sale history, cost unchanged
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase017() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "1.99,2.09,2.19,2.29,2.59";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4C", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.59";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R4C - :  If an item was never on sale during the last 52 weeks, the retail not be reduced by >5% 
	 * unless there is a size/brand violation.  Price points which are less than 5% will be ignored.  
	 * If all of the price points are less than 5%, then the price point which is closest to 5% will be chosen.
	 * 
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase018() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "1.99,2.09,2.19,2.29,2.59";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4C", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, -1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.59";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R4C - If item hasn’t gone through a Reg retail change during last 52 weeks If Item was never on sale during last 52 weeks, Retail
	 * will not be reduced by > 5%, unless there is a cost decrease or comp price decrease
	 * 
	 * Retail unchanged, no sale history, cost unchanged, comp price decreased
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase019() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "1.99,2.09,2.19,2.29,2.59";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4C", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 0, priceRange);
		itemDTO.setCompPriceChgIndicator(-1);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.59";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R4D-1  - If item hasn’t gone through a Reg retail change during last 52 weeks d. For items priced < $15.00, i. While raising the
	 * retail, if new retail crosses the next $ range (e.g. going from $2.89 to $3.09) and there is another retail available
	 * without crossing the next $ range (e.g. $2.99), use the retail within the existing $ range, unless such retail increase is
	 * necessitated to maintain the per unit margin $ mandated by the Margin Guideline
	 * 
	 * Retail unchanged, no sale history, cost increased, no margin guideline
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase020() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.89,2.99,3.19";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4D-1", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.89,2.99";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R4D-1  - If item hasn’t gone through a Reg retail change during last 52 weeks d. For items priced < $15.00, i. While raising the
	 * retail, if new retail crosses the next $ range (e.g. going from $2.89 to $3.09) and there is another retail available
	 * without crossing the next $ range (e.g. $2.99), use the retail within the existing $ range, unless such retail increase is
	 * necessitated to maintain the per unit margin $ mandated by the Margin Guideline
	 * 
	 * Retail unchanged, no sale history, cost increased, there is margin guideline
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Ignore
	public void testCase021() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.89,2.99,3.19";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R4", 0, true));
		recommendationRules.add(new RecommendationRuleMapDTO("R4D-1", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		PRGuidelineAndConstraintLog guidelineAndConstraintLog;
		guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		PRExplainLog explainLog = new PRExplainLog();
		TestHelper.setMarginLog(explainLog, guidelineAndConstraintLog, true, false, 2.41, 2.41, 2.41, 2.41, "");
		itemDTO.setExplainLog(explainLog);

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.89,2.99,3.19";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Rule : R6 -	If a non-LIG or any one of the LIG member item is in short term promotion
	 *  (promotion ends before 6 weeks from Recommendation Week), then suggest a new retail with
	 *   effective date as next day after the promotion end date
	 * 
	 *  short term promotion
	 * 
	 * @throws GeneralException
	 * @throws ParseException
	 */
	@Test
	public void testCase022() throws GeneralException, ParseException {
		PricingEngineService pricingEngineService = new PricingEngineService();
		
		//PriceRollbackService priceRollbackService = new PriceRollbackService();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 1.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 0);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		item.setIsNewPriceRecommended(1);
		// Set price history
		//HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		// Set possible price points
		TestHelper.setPricePoints(item, "1.99");

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set prediction map
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 1.89, 130.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 1.99, 150.0, PredictionStatus.SUCCESS);
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set predictions
		item.setCurRegPricePredictedMovement(130d);
		item.setPredictedMovement(150d);

		// Set short term promotion
		item.getRecWeekAdInfo().setAdPageNo(3);
		item.getRecWeekAdInfo().setAdBlockNo(4);
		item.getRecWeekAdInfo().setWeeklyAdStartDate("02/12/2017");

		// Set calendar
		//RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		//objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R6", 0, true);
		
		itemListWithRecPrice.add(item);
		//priceRollbackService.validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
		//		NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap);

		pricingEngineService.setEffectiveDate(itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader, recommendationRuleMap);

		assertEquals("Mismatch", item.getRecPriceEffectiveDate(), null);
	}

	/**
	 * Rule : R7 -	If a non-LIG or any one of the LIG member item is in short term promotion
	 *  (promotion ends before 6 weeks from Recommendation Week), then suggest a new retail with
	 *   effective date as next day after the promotion end date
	 * 
	 * Future short term promotion promotion
	 * 
	 * @throws GeneralException
	 * @throws ParseException
	 */
	@Test
	public void testCase023() throws GeneralException, ParseException {
		PricingEngineService pricingEngineService = new PricingEngineService();
		
		//PriceRollbackService priceRollbackService = new PriceRollbackService();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		//HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 1.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 0);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		item.setIsNewPriceRecommended(1);
		
		// Set possible price points
		TestHelper.setPricePoints(item, "1.99");

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);
		
		// Set short term future promotion
		item.getFutWeekSaleInfo().setSalePrice(new MultiplePrice(2, 1.89));
		item.getFutWeekSaleInfo().setSaleStartDate("01/15/2017");
		item.getFutWeekSaleInfo().setSaleEndDate("02/11/2017");
				
		// Set sale details promotion
		TestHelper.setSaleDetails(saleDetails, item.getItemCode(), "01/15/2017", "02/11/2017");
		item.setOnGoingPromotion(false);
		item.setFuturePromotion(true);
		item.setIsNewPriceRecommended(1);
		// Set predictions
		item.setCurRegPricePredictedMovement(130d);
		item.setPredictedMovement(150d);
		

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R7", 0, true);
		
		itemListWithRecPrice.add(item);
		pricingEngineService.setEffectiveDate(itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader, 
				recommendationRuleMap);
		
		//MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 1.89);
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		Date promoEndDate = dateFormat.parse("2017-02-12");
		String expectedEffectiveDate = DateUtil.dateToString(promoEndDate , Constants.APP_DATE_FORMAT);

		System.out.println(item.getRecPriceEffectiveDate() + " ::: " + expectedEffectiveDate);
		
		assertEquals("Mismatch", item.getRecPriceEffectiveDate(), expectedEffectiveDate);
	}

	/***
	 * Current retail is not part of price range
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase024() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "6.29,6.49,6.59";

		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 6.49, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 6.49, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 6.49, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 6.49, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 6.49, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 6.29, 0d, week15StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "6.49,6.29,6.59";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/***
	 * last price change is more than 90 days ago
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase025() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "7.29";

		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 6.49, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 6.49, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 6.49, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 6.49, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 6.49, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 6.29, 0d, week15StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}

	/**
	 * Simple Test Maximize margin $ while maintaining current Sales $
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 */
	/**
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void testCase026() throws GeneralException, Exception {
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.69, null, 1.28, 1.28, COST_UNCHANGED, 0, 0d, strategy, 0);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());

		// Set price history
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.69, 0d, week5StartDate, 0, 0, 0, "");

		// Set possible price points
		TestHelper.setPricePoints(item, "2.59,2.69,2.79");

		// Set prediction map
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.59, 140.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.69, 110.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.79, 90.0, PredictionStatus.SUCCESS);
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);

		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.59);

		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);

	}

	/**
	 * Simple Test Use guidelines and constraints
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 */
	@Test
	public void testCase027() throws GeneralException, Exception {
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowGuidelinesAndConstraints(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.69, null, 1.28, 1.28, COST_UNCHANGED, 0, 0d, strategy, 0);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());

		// Set price history
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.69, 0d, week5StartDate, 0, 0, 0, "");

		// Set possible price points
		TestHelper.setPricePoints(item, "2.79, 2.89, 2.99");

		// Set prediction map
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.79, 140.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.89, 110.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.99, 90.0, PredictionStatus.SUCCESS);
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);

		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.79);

		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);

	}

	/**
	 * Simple Test - Highest Margin $
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 */
	@Test
	public void testCase028() throws GeneralException, Exception {
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.69, null, 1.28, 1.28, COST_UNCHANGED, 0, 0d, strategy, 0);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());

		// Set price history
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.69, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.69, 0d, week5StartDate, 0, 0, 0, "");

		// Set possible price points
		TestHelper.setPricePoints(item, "2.79, 2.89, 2.99");

		// Set prediction map
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.69, 130.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.79, 140.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.89, 110.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.99, 90.0, PredictionStatus.SUCCESS);
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);

		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.79);

		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);

	}

	/**
	 * Current price is retained, no valid prediction, non-lig
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 */
	@Test
	public void testCase029() throws GeneralException, Exception {
		PriceRollbackService priceRollbackService = new PriceRollbackService();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 1.89, null, 1.28, 1.28, COST_UNCHANGED, 0, 0d, strategy, 0);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());

		// Set price history
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 1.89, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 1.89, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 1.89, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 1.89, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 1.89, 0d, week5StartDate, 0, 0, 0, "");
		// Set possible price points
		TestHelper.setPricePoints(item, "1.89, 1.99, 2.19");

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");
				
		itemListWithRecPrice.add(item);
		priceRollbackService.validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Set prediction map
		item.setRegPricePredictionMap(regPricePredictionMap);

		

		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);

		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 1.89);

		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);
	}

	/**
	 * Current price is retained, not enough margin benefit, non-lig
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 */
	@Test
	public void testCase030() throws GeneralException, Exception {
		PriceRollbackService priceRollbackService = new PriceRollbackService();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 1.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 0);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());

		// Set price history
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		// Set possible price points
		TestHelper.setPricePoints(item, "1.99");

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set prediction map
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 1.89, 160.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 1.99, 130.0, PredictionStatus.SUCCESS);
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set predictions
		item.setCurRegPricePredictedMovement(160d);
		item.setPredictedMovement(130d);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R10", 0, true);
		
		itemListWithRecPrice.add(item);
		priceRollbackService.validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 1.89);

		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);
	}

	/**
	 * Rule : R9 - Current price is retained, long term promotion, non-lig
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 */
	@Test
	public void testCase031() throws GeneralException, Exception {
		PriceRollbackService priceRollbackService = new PriceRollbackService();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 1.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 0);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());

		// Set price history
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		// Set possible price points
		TestHelper.setPricePoints(item, "1.99");

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set prediction map
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 1.89, 130.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 1.99, 150.0, PredictionStatus.SUCCESS);
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set predictions
		item.setCurRegPricePredictedMovement(130d);
		item.setPredictedMovement(150d);

		// Set long term promotion
		item.getRecWeekSaleInfo().setSalePrice(new MultiplePrice(2, 1.89));
		item.getRecWeekSaleInfo().setSaleStartDate("01/01/2017");
		item.getRecWeekSaleInfo().setSaleEndDate("02/19/2017");

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R9", 0, true);
		
		itemListWithRecPrice.add(item);
		priceRollbackService.validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 1.99);

		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);
	}

	/**
	 * Current price is retained, future promotions, trying to reduce price, non-lig
	 * 
	 * @throws GeneralException
	 * @throws Exception 
	 */
	@Test
	public void testCase032() throws GeneralException, Exception {
		PriceRollbackService priceRollbackService = new PriceRollbackService();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 0);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());

		// Set price history
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		// Set possible price points
		TestHelper.setPricePoints(item, "2.59");

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set prediction map
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.89, 130.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.59, 180.0, PredictionStatus.SUCCESS);
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set predictions
		item.setCurRegPricePredictedMovement(130d);
		item.setPredictedMovement(180d);

		// Set sale details promotion
		TestHelper.setSaleDetails(saleDetails, item.getItemCode(), "01/22/2017", "02/11/2017");

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R8", 0, true);
		
		itemListWithRecPrice.add(item);
		priceRollbackService.validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.59);

		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);
	}

	/**
	 * Current price is retained as there is no valid prediction,
	 * even though the brand/size relation want a new price
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase033() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();

		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 0, 0, 0, 0, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
//		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.99);
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.89);
		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);
	}
	
	/**
	 * Current price is retained, brand/size relation is also broken
	 * when the objective is "Maximize Margin $ while maintaining Sales $" , non-lig
	 * 
	 * @throws GeneralException
	 * @throws OfferManagementException
	 * @throws Exception
	 */
	@Test
	public void testCase034() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveMaximizeMar$ByMaintaningSale$(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();

		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 0, 0, 0, 0, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R8", 0, true);
		
		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.89);
		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);
	}
	
	/**
	 * When current retail is in multiples and if the recommended price is unit price
	 * and if the difference between current unit price and recommended unit price is within <<5>> cents
	 * then retain current price
	 * @throws GeneralException
	 * @throws Exception 
	 */
	@Test
	public void testCase035() throws GeneralException, Exception {
		PriceRollbackService priceRollbackService = new PriceRollbackService();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 10, null, 10d, 0.8, 0.8, COST_UNCHANGED, 0, 0d, strategy, 20);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());

		// Set price history
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		// Set possible price points
		TestHelper.setPricePoints(item, "0.95, 0.99");

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);
		
		// Set prediction map
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 10, 10d, 130.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.59, 180.0, PredictionStatus.SUCCESS);
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set predictions
		item.setCurRegPricePredictedMovement(130d);
		item.setPredictedMovement(180d);

		itemListWithRecPrice.add(item);

		// Set prediction map
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R12", 0, true);

		priceRollbackService.validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, 0.05d, recommendationRuleMap, itemZonePriceHistory, curCalDTO);
		
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(10, 10d);

		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);
	}
	
	/**
	 * Validate Objective : Maximize margin while maintaining current movement
	 * 
	 * Predicted movement are not higher for higher retails
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase036() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);

		PRStrategyDTO strategyDTO = null;
		
		PRItemDTO itemDTO = null;
	
		itemDTO = TestHelper.getTestItem1(itemCode1, 1, 16.99, 16.99, 13.00, 13.00, 0, 0, null, strategyDTO ,
				13, 0, null, null, "01/08/2017", 0, false, 0, //prediction status
				657.56, 0, 600, 1, 16.79, // recommended reg price
				null);
		
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();
		
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 15.99, 800.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.29, 770.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.49, 720.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.79, 655.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.99, 648.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 17.29, 645.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 18.59, 570.00, PredictionStatus.SUCCESS);
		
		itemDTO.setRegPricePredictionMap(regPricePredictionMap);
		
		Double[] pricePoints = {15.99,16.29,16.49,16.79,16.99,17.29,18.59};
		
		MultiplePrice finalPricePoint = objectiveService.applyMaximizeMarginByMaintainingCurMov(itemDTO, pricePoints);
		
		MultiplePrice expectedPricePoint = new MultiplePrice(1, 16.99);

		assertEquals("Mismatch", finalPricePoint, expectedPricePoint);
		logger.debug("****************************");
	}

	
	/**
	 * Validate Objective : Maximize margin while maintaining current movement
	 * 
	 * Predicted movement are higher for higher retails
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase037() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);

		PRStrategyDTO strategyDTO = null;
		
		PRItemDTO itemDTO = null;
	
		itemDTO = TestHelper.getTestItem1(itemCode1, 1, 16.99, 16.99, 13.00, 13.00, 0, 0, null, strategyDTO ,
				13, 0, null, null, "01/08/2017", 0, false, 0, //prediction status
				657.56, 0, 600, 1, 16.79, // recommended reg price
				null);
		
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();
		
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 15.99, 800.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.29, 770.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.49, 720.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.79, 690.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.99, 648.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 17.29, 645.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 18.59, 649.00, PredictionStatus.SUCCESS);
		
		itemDTO.setRegPricePredictionMap(regPricePredictionMap);
		
		Double[] pricePoints = {15.99,16.29,16.49,16.79,16.99,17.29,18.59};
		
		MultiplePrice finalPricePoint = objectiveService.applyMaximizeMarginByMaintainingCurMov(itemDTO, pricePoints);
		
		MultiplePrice expectedPricePoint = new MultiplePrice(1, 18.59);

		assertEquals("Mismatch", finalPricePoint, expectedPricePoint);
		logger.debug("****************************");
	}
	
	/**
	 * To check if Processing item is on TPR
	 * Size related item price is within the given range
	 * Current price needs to be retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase038() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();

		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 0, 0, 1.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.89);
		assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
	}
	
	/**
	 * To check if Processing item is on TPR
	 * Size related item price is not within the given range
	 * Item with size relationship alone and it is violated – new retail is recommended
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase039() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();

		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
//		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.99);
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.89);
		assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
	}
	
	/**
	 * To check if Processing item is on TPR
	 * Brand related item price is within the given range
	 * Item with Brand relationship alone and it is not violated – Same retail needs to be retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase040() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set Size Relation
//		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
//				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, 2.79, 3.79,
				itemCode2, false);
//		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.89);
		assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
	}
	
	/**
	 * To check if Processing item is on TPR
	 * Brand related item price is not within the given range
	 * Item with Brand relationship alone and it is violated – new retail needs to be retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase041() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set Size Relation
//		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
//				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, 3.79, 4.79,
				itemCode2, false);
//		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
//		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 1.29);
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.89);
		assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
	}
	
	/**
	 * To check if Processing item is on TPR
	 * Size and Brand related item price is not within the given range
	 * Item with Size and Brand relationship alone and it is violated – new retail needs to be retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase042() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, 3.79, 4.79,
				itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1  = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog1, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
//		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.99);
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.89);
		assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
	}
	
	/**
	 * To check if Processing item is on TPR
	 * Size and Brand related item price is within the given range
	 * Item with Size and Brand relationship and it is not violated – Same retail needs to be retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase043() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, 2.19, 3.79,
				itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1  = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog1, true, false, 0, 0, 1.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.89);
		assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
	}
	
	/**
	 * To check if Processing item is on TPR
	 * Size relation is within the range and Brand related item price is not within the given range
	 * Item with Size is not violated and Brand relationship and it is violated – new retail needs to be recommended
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase044() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, 3.19, 3.79,
				itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1  = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog1, true, false, 0, 0, 1.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
		
		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
//		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.99);
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.89);
		assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
	}
	
	/**
	 * To check if Processing item is on TPR
	 * Size relation is not within the range and Brand related item price is within the given range
	 * Item with Size is violated and Brand relationship and it is not violated – new retail needs to be recommended
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase045() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.89, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		item.setIsTPR(1);
		String priceRange = "2.49,2.59,2.69,2.79,2.89,2.99,3.19";
		if (priceRange != null || priceRange == "") {
			String[] prices = priceRange.split(",");
			Double[] pricePoints = new Double[prices.length];
			for (int i = 0; i < prices.length; i++) {
				pricePoints[i] = Double.valueOf(prices[i]);
			}
			item.setPriceRange(pricePoints);
		}
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 2.79, 2.19, 3.79,
				itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1  = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog1, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.89, 0d, week4StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.89, 0d, week5StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

//		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R11", 0, true);
//		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R3", 0, true);
		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
//		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.99);
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.89);
		assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
	}
	
	/**
	 * Rule : R15. Ignore all price points which crosses to next dollar range of current price. 
	 * If all the price points are crossing to next dollar range, then stay with those price points
	 * 
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase046() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.89,2.99,3.19";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R15", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.99,2.89";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R15. Ignore all price points which crosses to next dollar range of current price. 
	 * If all the price points are crossing to next dollar range, then stay with those price points
	 * 
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase047() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "3.19,3.29";

		List<RecommendationRuleMapDTO> recommendationRules = new ArrayList<RecommendationRuleMapDTO>();
		recommendationRules.add(new RecommendationRuleMapDTO("R15", 0, true));
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setMultipleRecommendationRuleMap(recommendationRules);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 1, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");

		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "3.19,3.29";

		assertEquals("Mismatch", filteredPricePoints, expPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R3 - Retail is not increased greater than 5% than the highest Reg price point in last 52 weeks, unless there was a cost increase
	 * 
	 * Cost increased, Margin guideline is available and Brand and Size violated
	 * Output: Nearest Price point greater than 5% have to be considered
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase048() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.89,2.99,3.19";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R3", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week15StartDate, 1, 2.69, 0d, week15StartDate, 0, 0, 0, "");
		
		itemDTO.setCostChgIndicator(1);
		
		// Set price group
		TestHelper.setPriceGroup(itemDTO, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		TestHelper.setBrandRelation(itemDTO, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');

		// Set Size Relation
		TestHelper.setSizeRelation(itemDTO, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		
		// Set explain log
		PRExplainLog explainLog1 = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1 = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(itemDTO, explainLog1, guidelineAndConstraintLog1, true, false, Constants.DEFAULT_NA, 2.79, 2.19, 2.82, itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog2 = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(itemDTO, explainLog1, guidelineAndConstraintLog2, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog1, guidelineAndConstraintLog, true, false, 2.59, 2.59, 2.59, 2.59, "");
		itemDTO.setExplainLog(explainLog1);
		
		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.99,2.89";

		assertEquals("Mismatch", expPricePoints, filteredPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R3 - Retail is not increased greater than 5% than the highest Reg price point in last 52 weeks, unless there was a cost increase
	 * 
	 * Cost haven't changed
	 * Output: Price points which is within 5% were filtered
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase049() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79,2.89,2.99,3.19";

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R3", 0, true);
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 0, priceRange);
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week15StartDate, 1, 2.69, 0d, week15StartDate, 0, 0, 0, "");
		
		// Set price group
		TestHelper.setPriceGroup(itemDTO, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		TestHelper.setBrandRelation(itemDTO, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');

		// Set Size Relation
		TestHelper.setSizeRelation(itemDTO, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		
		// Set explain log
		PRExplainLog explainLog1 = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1 = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(itemDTO, explainLog1, guidelineAndConstraintLog1, true, false, Constants.DEFAULT_NA, 2.79, 2.19, 3.79, itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog2 = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(itemDTO, explainLog1, guidelineAndConstraintLog2, true, false, 0, 0, 2.91, 3.1, itemCode2, false, 10);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		TestHelper.setMarginLog(explainLog1, guidelineAndConstraintLog, true, false, 2.59, 2.59, 2.59, 2.59, "");
		itemDTO.setExplainLog(explainLog1);
		
		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.79,2.49,2.59,2.69";

		assertEquals("Mismatch", expPricePoints, filteredPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R4 - If an item hasn’t gone through a Reg retail change during the last 52 weeks,
	 * Rule : R4B - The retail will not be reduced below the highest sale/TPR price observed in the last 52 weeks 
	 * 			unless there is size/brand violation.  Price points which are below the highest sale/TPR will be ignored. 
	 * Item was on Sale, Brand and Size violated
	 * Output: The retail will be reduced below the highest Sale/TPR price
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase050() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.49,2.59,2.69,2.79,2.89,2.99,3.19";

		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 0, priceRange);
		itemDTO.setRecommendedRegPrice(new MultiplePrice(1, 2.99));
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 1, 2.49, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 1, 2.49, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 1, 2.49, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 1, 2.49, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week15StartDate, 1, 2.59, 0d, week15StartDate, 0, 0, 0, "");
		
		// Set price group
		TestHelper.setPriceGroup(itemDTO, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		TestHelper.setBrandRelation(itemDTO, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');

		// Set Size Relation
		TestHelper.setSizeRelation(itemDTO, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		
		// Set explain log
		PRExplainLog explainLog1 = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1 = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(itemDTO, explainLog1, guidelineAndConstraintLog1, true, false, Constants.DEFAULT_NA, 2.79, 2.19, 2.82, itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog2 = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(itemDTO, explainLog1, guidelineAndConstraintLog2, true, false, 0, 0, 2.19, 2.82, itemCode2, false, 10);
		itemDTO.setExplainLog(explainLog1);
		
		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.49,2.59,2.69";

		assertEquals("Mismatch", expPricePoints, filteredPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Rule : R4 - If an item hasn’t gone through a Reg retail change during the last 52 weeks,
	 * Rule : R4C - If an item was never on sale during the last 52 weeks, the retail not be reduced by >5% 
	 * unless there is a size/brand violation.  Price points which are less than 5% will be ignored.  
	 * If all of the price points are less than 5%, then the price point which is closest to 5% will be chosen
	 * 
	 * Item was never on sale and Brand/Size violated
	 * Output: Price points which are less than 5% will be ignored. And considered price points within Brand And Size price range
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase051() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		String priceRange = "2.79,2.89,2.99,3.19";
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new HashMap<>();
		recommendationRuleMap.put("R4", Arrays.asList(new RecommendationRuleMapDTO("R4", 0, true)));
		recommendationRuleMap.put("R4C", Arrays.asList(new RecommendationRuleMapDTO("R4C", 0, true)));
		PRItemDTO itemDTO = TestHelper.getTestItem(itemCode1, 1, 2.59, 0d, 0, priceRange);
		itemDTO.setRecommendedRegPrice(new MultiplePrice(1, 2.99));
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week1StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week2StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week3StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week4StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, week5StartDate, 1, 2.59, 0d, week5StartDate, 0, 0, 0, "");
		
		// Set price group
		TestHelper.setPriceGroup(itemDTO, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		TestHelper.setBrandRelation(itemDTO, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');

		// Set Size Relation
		TestHelper.setSizeRelation(itemDTO, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');
		
		// Set explain log
		PRExplainLog explainLog1 = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1 = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(itemDTO, explainLog1, guidelineAndConstraintLog1, true, false, Constants.DEFAULT_NA, 2.79, 2.19, 2.82, itemCode2,
				false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog2 = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(itemDTO, explainLog1, guidelineAndConstraintLog2, true, false, 0, 0, 2.19, 2.82, itemCode2, false, 10);
		itemDTO.setExplainLog(explainLog1);
		
		Double[] filterPricePoints = objectiveService.filterFinalPricePoints(itemDTO, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		String filteredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(filterPricePoints);
		String expPricePoints = "2.79";

		assertEquals("Mismatch", expPricePoints, filteredPricePoints);
		logger.debug("****************************");
	}
	
	/**
	 * Non LIG Item, item in relation, Item retail was changed on 02/22/2018, Assume recommendation week as 03/15/2018
	 * current retail satisfies brand & size guidelines & cost constraints
	 * 
	 * Expected output: Recommended Retail: Current retail, Additional Log must say current price retained
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase052() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails("03/15/2018", "");

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 2.19, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		String priceRange = "2.49,2.59,2.69,2.79,2.89,2.99,3.19";
		if (priceRange != null || priceRange == "") {
			String[] prices = priceRange.split(",");
			Double[] pricePoints = new Double[prices.length];
			for (int i = 0; i < prices.length; i++) {
				pricePoints[i] = Double.valueOf(prices[i]);
			}
			item.setPriceRange(pricePoints);
		}
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 1.29);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 1.79, 2.19, 3.79,
				itemCode2, false);
//		PRGuidelineAndConstraintLog guidelineAndConstraintLog1  = new PRGuidelineAndConstraintLog();
//		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog1, true, false, 0, 0, 1.79, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, "03/08/2018", 1, 2.89, 0d, "03/08/2018", 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, "02/22/2018", 1, 2.59, 0d, week4StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);
		
//		PricingEngineService pricingEngineService = new PricingEngineService(); 
// 		new PriceRollbackService().checkRetailRecommendedWithinXWeeks(item, itemListWithRecPrice, recommendationRunHeader,saleDetails, adDetails,
//				itemZonePriceHistory, recommendationRuleMap, pricingEngineService, curCalDTO);
		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.19);
		assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
	}
	
	/**
	 * Non LIG Item, item in relation, Item retail was changed on 02/22/2018, Assume recommendation week as 03/15/2018
	 * current retail satisfies brand & size guidelines & cost constraints, 
	 * but final rounding digits has 3 rounding digits other than current retail.
	 * 
	 * Brand/Size is violated
	 * Expected output: Recommended Retail: Recommended a new retail
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase053() throws GeneralException, Exception, OfferManagementException {

		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

		// Dummy variables
		HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
		HashMap<Integer, LocationKey> compIdMap = new HashMap<Integer, LocationKey>();
		HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails("03/15/2018", "");
	
		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);
		
		TestHelper.setSizeGuideline(strategy, 1);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 1, 1.99, null, 1.29, 1.28, COST_UNCHANGED, 0, 0d, strategy, 50);
		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);
		String priceRange = "2.49,2.59,2.69,2.79,2.89,2.99,3.19";
		if (priceRange != null || priceRange == "") {
			String[] prices = priceRange.split(",");
			Double[] pricePoints = new Double[prices.length];
			for (int i = 0; i < prices.length; i++) {
				pricePoints[i] = Double.valueOf(prices[i]);
			}
			item.setPriceRange(pricePoints);
		}
		
		// Price points movement
		HashMap<MultiplePrice, PricePointDTO> priceMovementPrediction = new HashMap<>();
		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO.setPredictedMovement(40.22);
		priceMovementPrediction.put(new MultiplePrice(1, 2.99), pricePointDTO);

		PricePointDTO pricePointDTO1 = new PricePointDTO();
		pricePointDTO1.setPredictionStatus(PredictionStatus.SUCCESS);
		pricePointDTO1.setPredictedMovement(140.22);
		priceMovementPrediction.put(new MultiplePrice(1, 1.99), pricePointDTO1);

		item.setRegPricePredictionMap(priceMovementPrediction);
		// Set price group
		TestHelper.setPriceGroup(item, 80, BrandClassLookup.NATIONAL.getBrandClassId(), 'X', ' ', ' ');

		// Set Related Item
		PRItemDTO parentItem = TestHelper.getRelatedItem(itemCode2, 2.89);
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, parentItem);

		TestHelper.setBrandRelation(item, itemCode2, 0, 'X', 16, PRConstants.BRAND_RELATION, Constants.DEFAULT_NA,
				Constants.DEFAULT_NA, PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM, PRConstants.RETAIL_TYPE_SHELF, ' ');
		
		
		// Set Size Relation
		TestHelper.setSizeRelation(item, itemCode2, 0, 'N', 40, PRConstants.SIZE_RELATION, Constants.DEFAULT_NA, Constants.DEFAULT_NA,
				PRConstants.PRICE_GROUP_EXPR_LESSER_SYM, PRConstants.RETAIL_TYPE_UNIT, ' ');

		// Set explain log
		PRExplainLog explainLog = new PRExplainLog();
		PRGuidelineAndConstraintLog guidelineAndConstraintLog  = new PRGuidelineAndConstraintLog();
		TestHelper.setBrandLog(item, explainLog, guidelineAndConstraintLog, true, false, Constants.DEFAULT_NA, 1.79, 2.19, 3.79,
				itemCode2, false);
		PRGuidelineAndConstraintLog guidelineAndConstraintLog1  = new PRGuidelineAndConstraintLog();
		TestHelper.setSizeLog(item, explainLog, guidelineAndConstraintLog1, true, false, 0, 0, 1.79, 3.1, itemCode2, false, 10);
		
		item.setExplainLog(explainLog);
		
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, "03/08/2018", 1, 2.89, 0d, "03/08/2018", 0, 0, 0, "");
		TestHelper.setItemZonePrice(itemZonePriceHistory, itemCode1, "02/22/2018", 1, 2.59, 0d, week4StartDate, 0, 0, 0, "");
		
		// put in item list
		itemListWithRecPrice.add(item);
		itemListWithRecPrice.add(parentItem);

		// apply objective and validations
		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		
		new PriceRollbackService().validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, maxUnitPriceDiff, recommendationRuleMap, itemZonePriceHistory, curCalDTO);

		// Recommend related item again
		new PriceGroupService().recommendPriceGroupRelatedItems(recommendationRunHeader, itemListWithRecPrice, itemDataMap, compIdMap,
				retLirConstraintMap, multiCompLatestPriceMap, recRunningWeekStartDate, leadZoneDetails, false, itemZonePriceHistory, curCalDTO,
				maxUnitPriceDiff, recommendationRuleMap);

		// Match recommended price
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(1, 2.99);
		assertEquals("Mismatch", expectedRecommendedPrice, item.getRecommendedRegPrice());
	}
	
	/**
	 * When current retail is in multiples and if the recommended price is unit price
	 * and if the difference between current unit price and recommended unit price is within <<5>> cents
	 * then retain current price
	 * @throws GeneralException
	 * @throws Exception 
	 */
	@Test
	public void testCase054() throws GeneralException, Exception {
		PriceRollbackService priceRollbackService = new PriceRollbackService();
		List<PRItemDTO> itemListWithRecPrice = new ArrayList<PRItemDTO>();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = new HashMap<Integer, List<PRItemSaleInfoDTO>>();
		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = new HashMap<Integer, List<PRItemAdInfoDTO>>();
		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();

		// Set Strategy
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/08/2017", "01/28/2017", false, -1, -1, -1);
		TestHelper.setObjectiveFollowHighestMarginDollar(strategy);

		// Set Item
		PRItemDTO item = TestHelper.getTestItem(itemCode1, 10, null, 10d, 0.8, 0.8, COST_UNCHANGED, 0, 0d, strategy, 20);

		item.setObjectiveTypeId(strategy.getObjective().getObjectiveTypeId());

		// Set price history
		HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<Integer, HashMap<String, RetailPriceDTO>>();

		// Set possible price points
		TestHelper.setPricePoints(item, "0.95, 0.99");

		// Set recommendation header
		recommendationRunHeader.setStartDate(recommendationWeekStartDate);
		
		// Set prediction map
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 10, 10d, 130.0, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMap, 1, 2.59, 180.0, PredictionStatus.SUCCESS);
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set predictions
		item.setCurRegPricePredictedMovement(130d);
		item.setPredictedMovement(180d);

		itemListWithRecPrice.add(item);

		// Set prediction map
		item.setRegPricePredictionMap(regPricePredictionMap);

		// Set calendar
		RetailCalendarDTO curCalDTO = TestHelper.getCalendarDetails(recRunningWeekStartDate, "");

		objectiveService.applyObjectiveAndSetRecPrice(item, itemZonePriceHistory, curCalDTO, recommendationRuleMap);
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = TestHelper.setRecommendationRuleMap("R12", 0, true);

		priceRollbackService.validateAndRetainCurrentRetail(item, itemListWithRecPrice, saleDetails, adDetails, recommendationRunHeader,
				NO_OF_STORES_1, 1.00d, recommendationRuleMap, itemZonePriceHistory, curCalDTO);
		
		MultiplePrice expectedRecommendedPrice = new MultiplePrice(10, 10d);

		assertEquals("Mismatch", item.getRecommendedRegPrice(), expectedRecommendedPrice);
	}

	/**
	 * Validate Objective : Maximize movement while maintaining current margin
	 * If the item does not have a valid cost, price and predicted units, 
	 * then return a price point which is closest to the current retail.
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase055() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);

		PRItemDTO[] itemDTOs = {
//				Invalid price (curRegPrice and curRegMPrice)
				TestHelper.getTestItem1(itemCode1, //Integer itemCode
						1, //int curRegMultiple
						null, //Double curRegPrice
						null, //Double curRegMPrice
						13.00, //Double curListCost
						13.00, //Double preListCost
						0, //Integer costChgIndicator
						0, //Integer compStrId
						null, //Double compPrice
						null, //PRStrategyDTO strategyDTO
						13, //long lastXWeeksMov
						0, //int curSaleMultipe
						null, //Double curSalePrice
						null, //Double curSaleMPrice
						"01/08/2017", //String promoWeekStartDate
						0, //int retLirId
						false, //boolean isLir
						0, //int predictionStatus
						657.56, //double predictedMov
						0, //int curPredictionStatus
						600, //double curPredictedMov
						1, //int recRegMultiple
						16.79, //Double recRegPrice
						null //Double recRegMPrice
						),
//				Invalid cost. (curListCost and VipCost)
				TestHelper.getTestItem1(itemCode2, //Integer itemCode
						1, //int curRegMultiple
						16.99, //Double curRegPrice
						16.99, //Double curRegMPrice
						null, //Double curListCost
						null, //Double preListCost
						0, //Integer costChgIndicator
						0, //Integer compStrId
						null, //Double compPrice
						null, //PRStrategyDTO strategyDTO
						13, //long lastXWeeksMov
						0, //int curSaleMultipe
						null, //Double curSalePrice
						null, //Double curSaleMPrice
						"01/08/2017", //String promoWeekStartDate
						0, //int retLirId
						false, //boolean isLir
						0, //int predictionStatus
						657.56, //double predictedMov
						0, //int curPredictionStatus
						600, //double curPredictedMov
						1, //int recRegMultiple
						16.79, //Double recRegPrice
						null //Double recRegMPrice
						),
//				Valid price and cost but paired with price points that have invalid movements
				TestHelper.getTestItem1(itemCode3, //Integer itemCode
						1, //int curRegMultiple
						16.99, //Double curRegPrice
						16.99, //Double curRegMPrice
						13.00, //Double curListCost
						13.00, //Double preListCost
						0, //Integer costChgIndicator
						0, //Integer compStrId
						null, //Double compPrice
						null, //PRStrategyDTO strategyDTO
						13, //long lastXWeeksMov
						0, //int curSaleMultipe
						null, //Double curSalePrice
						null, //Double curSaleMPrice
						"01/08/2017", //String promoWeekStartDate
						0, //int retLirId
						false, //boolean isLir
						0, //int predictionStatus
						657.56, //double predictedMov
						0, //int curPredictionStatus
						600, //double curPredictedMov
						1, //int recRegMultiple
						16.79, //Double recRegPrice
						null //Double recRegMPrice
						)};
		
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMapValid = new HashMap<MultiplePrice, PricePointDTO>();
		
		TestHelper.setRegPricePredictionMap(regPricePredictionMapValid , 1, 15.99, 800.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapValid , 1, 16.29, 770.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapValid , 1, 16.49, 720.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapValid , 1, 16.79, 690.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapValid , 1, 16.99, 600.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapValid , 1, 17.29, 645.00, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapValid , 1, 18.59, 649.00, PredictionStatus.SUCCESS);
		
		
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMapInvalid = new HashMap<MultiplePrice, PricePointDTO>();
		
		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 15.99, 800.00, null);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 16.29, 0.0, PredictionStatus.NO_RECENT_MOVEMENT);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 16.49, null, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 16.79, 0.0, PredictionStatus.SUCCESS);
//		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 16.99, 0.00, PredictionStatus.SUCCESS); //missing prediction
		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 17.29, -173.00, PredictionStatus.SUCCESS);

		itemDTOs[0].setRegPricePredictionMap(regPricePredictionMapValid);
		itemDTOs[1].setRegPricePredictionMap(regPricePredictionMapValid);
		itemDTOs[2].setRegPricePredictionMap(regPricePredictionMapInvalid);
		
		Double[] pricePoints = {15.99,16.29,16.49,16.79,16.99,17.29};
		
		MultiplePrice finalPricePoint = objectiveService.applyMaximizeMovByMaintainingCurMargin(itemDTOs[0], pricePoints);
		assert(null==finalPricePoint);
		
		finalPricePoint = objectiveService.applyMaximizeMovByMaintainingCurMargin(itemDTOs[1], pricePoints);
		double priceAfterApplyingObjective = finalPricePoint.getUnitPrice();
		assert(16.99 == priceAfterApplyingObjective);
		
		finalPricePoint = objectiveService.applyMaximizeMovByMaintainingCurMargin(itemDTOs[2], pricePoints);
		priceAfterApplyingObjective = finalPricePoint.getUnitPrice();
		assert(16.99 == priceAfterApplyingObjective);
		
	}

	/**
	 * Validate Objective : Maximize movement while maintaining current margin when 
	 * the item has a valid cost, price and predicted units
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase056() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		
		PRItemDTO itemDTO = TestHelper.getTestItem1(itemCode1, //Integer itemCode
				1, //int curRegMultiple
				16.99, //Double curRegPrice
				16.99, //Double curRegMPrice
				13.00, //Double curListCost
				13.00, //Double preListCost
				0, //Integer costChgIndicator
				0, //Integer compStrId
				null, //Double compPrice
				null, //PRStrategyDTO strategyDTO
				13, //long lastXWeeksMov
				0, //int curSaleMultipe
				null, //Double curSalePrice
				null, //Double curSaleMPrice
				"01/08/2017", //String promoWeekStartDate
				0, //int retLirId
				false, //boolean isLir
				0, //int predictionStatus
				657.56, //double predictedMov
				0, //int curPredictionStatus
				600, //double curPredictedMov
				1, //int recRegMultiple
				16.79, //Double recRegPrice
				null //Double recRegMPrice
				);
		
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMap = new HashMap<MultiplePrice, PricePointDTO>();
//		current margin = 3.99*600 = 2394
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 15.99, 800.00, PredictionStatus.SUCCESS);//margin = 2.99*800= 2392.00
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.29, 770.00, PredictionStatus.SUCCESS);//margin = 3.29*770= 2533.30
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.79, 655.00, PredictionStatus.SUCCESS);//margin = 3.79*655= 2482.45
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 16.99, 620.00, PredictionStatus.SUCCESS);//margin = 3.99*620= 2473.80
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 17.29, 645.00, PredictionStatus.SUCCESS);//margin = 4.29*645= 2767.05
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 17.49, 605.00, PredictionStatus.SUCCESS);//margin = 4.49*605= 2716.45
		TestHelper.setRegPricePredictionMap(regPricePredictionMap , 1, 18.59, 575.00, PredictionStatus.SUCCESS);//margin = 5.59*575= 3214.25
		
		itemDTO.setRegPricePredictionMap(regPricePredictionMap);
		
		Double[] pricePoints = {15.99,16.29,16.79,16.99,17.29,17.49,18.59};
		
		MultiplePrice finalPricePoint = objectiveService.applyMaximizeMovByMaintainingCurMargin(itemDTO, pricePoints);
		double priceAfterApplyingObjective = finalPricePoint.getUnitPrice();
		assert(16.29 == priceAfterApplyingObjective);
	}

	/**
	 * Validate Objective : Maximize movement while maintaining current margin.
	 * When none of the price points meet the objective, return the current price point  
	 * provided the current retail is equal to one of the price points, else 
	 * return the valid price point that yields the max margin.
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase057() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);
		
		PRItemDTO[] itemDTOs = {
				TestHelper.getTestItem1(itemCode1, //Integer itemCode
						1, //int curRegMultiple
						16.99, //Double curRegPrice
						16.99, //Double curRegMPrice
						13.00, //Double curListCost
						13.00, //Double preListCost
						0, //Integer costChgIndicator
						0, //Integer compStrId
						null, //Double compPrice
						null, //PRStrategyDTO strategyDTO
						13, //long lastXWeeksMov
						0, //int curSaleMultipe
						null, //Double curSalePrice
						null, //Double curSaleMPrice
						"01/08/2017", //String promoWeekStartDate
						0, //int retLirId
						false, //boolean isLir
						0, //int predictionStatus
						657.56, //double predictedMov
						0, //int curPredictionStatus
						600, //double curPredictedMov
						1, //int recRegMultiple
						16.79, //Double recRegPrice
						null //Double recRegMPrice
						),
				TestHelper.getTestItem1(itemCode2, //Integer itemCode
						1, //int curRegMultiple
						16.99, //Double curRegPrice
						16.99, //Double curRegMPrice
						13.00, //Double curListCost
						13.00, //Double preListCost
						0, //Integer costChgIndicator
						0, //Integer compStrId
						null, //Double compPrice
						null, //PRStrategyDTO strategyDTO
						13, //long lastXWeeksMov
						0, //int curSaleMultipe
						null, //Double curSalePrice
						null, //Double curSaleMPrice
						"01/08/2017", //String promoWeekStartDate
						0, //int retLirId
						false, //boolean isLir
						0, //int predictionStatus
						657.56, //double predictedMov
						0, //int curPredictionStatus
						600, //double curPredictedMov
						1, //int recRegMultiple
						16.79, //Double recRegPrice
						null //Double recRegMPrice
						)
		};
		
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMapWithCurPrice = new HashMap<MultiplePrice, PricePointDTO>();
//		current margin = 3.99*600 = 2394
		TestHelper.setRegPricePredictionMap(regPricePredictionMapWithCurPrice , 1, 15.99, 795.00, PredictionStatus.SUCCESS);//margin = 2377.05
		TestHelper.setRegPricePredictionMap(regPricePredictionMapWithCurPrice , 1, 16.29, 725.00, PredictionStatus.SUCCESS);//margin = 2385.25
		TestHelper.setRegPricePredictionMap(regPricePredictionMapWithCurPrice , 1, 16.79, 630.00, PredictionStatus.SUCCESS);//margin = 2387.70
		TestHelper.setRegPricePredictionMap(regPricePredictionMapWithCurPrice , 1, 16.99, 600.00, PredictionStatus.SUCCESS);//margin = 2394.00
		TestHelper.setRegPricePredictionMap(regPricePredictionMapWithCurPrice , 1, 17.29, 555.00, PredictionStatus.SUCCESS);//margin = 2380.95
		TestHelper.setRegPricePredictionMap(regPricePredictionMapWithCurPrice , 1, 17.49, 530.00, PredictionStatus.SUCCESS);//margin = 2379.70
		TestHelper.setRegPricePredictionMap(regPricePredictionMapWithCurPrice , 1, 18.59, 425.00, PredictionStatus.SUCCESS);//margin = 2375.75

		itemDTOs[0].setRegPricePredictionMap(regPricePredictionMapWithCurPrice);
		itemDTOs[1].setRegPricePredictionMap(regPricePredictionMapWithCurPrice);
		
		Double[] pricePointsWithCurrentPrice = {15.99,16.29,16.79,16.99,17.29,17.49,18.59};
		Double[] pricePointsWithoutCurrentPrice = {15.99,16.29,16.79,17.29,17.49,18.59};
		
		MultiplePrice finalPricePoint = objectiveService.applyMaximizeMovByMaintainingCurMargin(itemDTOs[0], pricePointsWithCurrentPrice);
		double priceAfterApplyingObjective = finalPricePoint.getUnitPrice();
		assert(16.99 == priceAfterApplyingObjective);
		
		finalPricePoint = objectiveService.applyMaximizeMovByMaintainingCurMargin(itemDTOs[1], pricePointsWithoutCurrentPrice);
		priceAfterApplyingObjective = finalPricePoint.getUnitPrice();
		assert(16.79 == priceAfterApplyingObjective);
		
	}
	
	/**
	 * Validate Objective : Maximize movement while maintaining current margin.
	 * If all price points have the same units predicted and margin predicted, 
	 * then expect current price as recommendation as there’s a forecast issue.
	 * 
	 * @throws Exception
	 * @throws GeneralException
	 */
	@Test
	public void testCase058() throws Exception, GeneralException {
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		curCalDTO.setStartDate(week1StartDate);

		PRItemDTO itemDTO = TestHelper.getTestItem1(itemCode1, //Integer itemCode
						1, //int curRegMultiple
						16.99, //Double curRegPrice
						16.99, //Double curRegMPrice
						13.00, //Double curListCost
						13.00, //Double preListCost
						0, //Integer costChgIndicator
						0, //Integer compStrId
						null, //Double compPrice
						null, //PRStrategyDTO strategyDTO
						13, //long lastXWeeksMov
						0, //int curSaleMultipe
						null, //Double curSalePrice
						null, //Double curSaleMPrice
						"01/08/2017", //String promoWeekStartDate
						0, //int retLirId
						false, //boolean isLir
						0, //int predictionStatus
						657.56, //double predictedMov
						0, //int curPredictionStatus
						600, //double curPredictedMov
						1, //int recRegMultiple
						16.79, //Double recRegPrice
						null //Double recRegMPrice
						);
		
		HashMap<MultiplePrice, PricePointDTO> regPricePredictionMapInvalid = new HashMap<MultiplePrice, PricePointDTO>();
		
		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 15.99, 800.00, null);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 16.29, 0.0, PredictionStatus.NO_RECENT_MOVEMENT);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 16.49, null, PredictionStatus.SUCCESS);
		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 16.79, 0.0, PredictionStatus.SUCCESS);
//		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 16.99, 0.00, PredictionStatus.SUCCESS); //missing prediction
		TestHelper.setRegPricePredictionMap(regPricePredictionMapInvalid , 1, 17.29, -173.00, PredictionStatus.SUCCESS);

		itemDTO.setRegPricePredictionMap(regPricePredictionMapInvalid);
		
		Double[] pricePoints = {15.99,16.29,16.49,16.79,16.99,17.29};
		
		MultiplePrice finalPricePoint = objectiveService.applyMaximizeMovByMaintainingCurMargin(itemDTO, pricePoints);
		double priceAfterApplyingObjective = finalPricePoint.getUnitPrice();
		assert(16.99 == priceAfterApplyingObjective);
		
	}
	
}
