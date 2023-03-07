package com.pristine.test.offermgmt;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.ItemDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.SubstituteDAO;
import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dto.ProductDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelnDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PRSubstituteItem;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionReportItemUIDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.dto.webservice.Strategy;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.service.PredictionComponent;
import com.pristine.service.offermgmt.DisplayTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.MostOccurrenceData;
import com.pristine.service.offermgmt.PriceAdjustment;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.RecommendationAnalysis;
import com.pristine.service.offermgmt.StrategyService;
import com.pristine.service.offermgmt.SubstituteService;
import com.pristine.service.offermgmt.oos.OOSCriteriaLookup;
import com.pristine.service.offermgmt.prediction.PredictionAnalysis;
import com.pristine.service.offermgmt.prediction.PredictionEngineInput;
import com.pristine.service.offermgmt.prediction.PredictionEngineItem;
import com.pristine.service.offermgmt.prediction.PredictionReportService;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class GeneralTest {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	
	
	/**
	 * @param args
	 * @throws GeneralException 
	 * @throws Exception 
	 * @throws OfferManagementException 
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) throws GeneralException, Exception, OfferManagementException {
		GeneralTest generalTest = new GeneralTest();
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("analysis.properties");
		Integer a = 20;
		logger.debug(a);
		test1(a);
		logger.debug(a);
		
		HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = new HashMap<String, RetailCalendarDTO>();
		List<RetailCalendarDTO> retailCalendarList = PRCommonUtil.getPreviousCalendars(allWeekCalendarDetails, "01/07/2017",
				1);
		
		
		
		
		
		long testx = Math.round(8301.65d);
		
		List<Double> listCost = new ArrayList<Double>();
		listCost.add(2.19d);
		listCost.add(2.19d);
		listCost.add(2.19d);
		listCost.add(2.39d);
		listCost.add(2.19d);
		
		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		Double maxOccurListCost = (Double) mostOccurrenceData.getMaxOccurance(listCost);
		
		Set<PRItemSaleInfoDTO> saleItems = new  HashSet<PRItemSaleInfoDTO>();
		PRItemSaleInfoDTO saleInfoDTO1 = new PRItemSaleInfoDTO();
		saleInfoDTO1.setSalePrice(new MultiplePrice(1,1.99));
		saleItems.add(saleInfoDTO1);
		
		saleInfoDTO1 = new PRItemSaleInfoDTO();
		saleInfoDTO1.setSalePrice(new MultiplePrice(1,1.99));
		saleItems.add(saleInfoDTO1);
		
		if(false && testDebugFunction()) {
			logger.debug("in");
		}
		
		Collection<PRItemDTO> allItemList = new ArrayList<PRItemDTO>();

		PRItemDTO itemDTO5 = new PRItemDTO();
		itemDTO5.setRecommendedRegPrice(new MultiplePrice(1,2.9));
		itemDTO5.setAvgMovement(20);
		allItemList.add(itemDTO5);
		
		itemDTO5 = new PRItemDTO();
		itemDTO5.setRecommendedRegPrice(new MultiplePrice(1,2.9));
		itemDTO5.setAvgMovement(25);
		allItemList.add(itemDTO5);
		
		itemDTO5 = new PRItemDTO();
		itemDTO5.setRecommendedRegPrice(new MultiplePrice(1,2.8));
		itemDTO5.setAvgMovement(50);
		allItemList.add(itemDTO5);
		
		itemDTO5 = new PRItemDTO();
		itemDTO5.setRecommendedRegPrice(new MultiplePrice(1,2.8));
		itemDTO5.setAvgMovement(10);
		allItemList.add(itemDTO5);
		
		MultiplePrice multiplePrice = null;
		multiplePrice = (MultiplePrice) new MostOccurrenceData().getMaxOccurance(allItemList, "RecRegPrice");	
		
		
		
		generalTest.intialSetup();
//		generalTest.getAllStrategies();
		
//		generalTest.connectionTest();
		
		
		String nextWkStartDate = DateUtil.getWeekStartDate(-1);
		RetailCalendarDTO curWkDTO = new RetailCalendarDAO().getCalendarId(generalTest.conn, nextWkStartDate,
				Constants.CALENDAR_WEEK);
		
		
		
		List<PRItemDTO> items1 = new ArrayList<PRItemDTO>();
		items1.add(null);
		
		for (PRItemDTO itemDTO : items1) {
			if (!itemDTO.isLir()) {

			}
		}
		
		DecimalFormat roundToOneDecimalDigit = new DecimalFormat("#############.#");
		DecimalFormat roundToTwoDecimalDigit = new DecimalFormat("#############.##"); 
		
		double orgPrice =  1.80d;
		double tempPrice = orgPrice;
		//make last digit as 9
		for (int i = 0; i < 10; i++) {
			double rounded = Double.valueOf(roundToOneDecimalDigit.format(tempPrice));
			double doubleDigit = Double.valueOf(roundToTwoDecimalDigit.format(tempPrice));
			if(Double.valueOf(roundToTwoDecimalDigit.format(rounded - doubleDigit)) == 0.01) {
				orgPrice = tempPrice;
				break;
			}
			tempPrice = tempPrice + 0.01d;
		}
		
		
		
		logger.debug("output:" + Double.valueOf(roundToTwoDecimalDigit.format(orgPrice)));
		
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
		LocalDate dat =  LocalDate.parse("02/07/2017", formatter);
		
		  HashMap<Integer, String> orderedHHImpactItemList = new HashMap<Integer, String>();
		  orderedHHImpactItemList.put(1, "a");
		  orderedHHImpactItemList.put(4, "a");
		  orderedHHImpactItemList.put(2, "a");
		  orderedHHImpactItemList.put(3, "a");
		  orderedHHImpactItemList.put(5, "a");
		  orderedHHImpactItemList.put(15, "a");
		  
		  for(Integer i: orderedHHImpactItemList.keySet()){
		   System.out.println("Ebaskjd : " + i);
		  }

		PRItemDTO ligItem = new PRItemDTO();
		ligItem.setCompPriceCheckDate((String) null);
		
		List<PRItemDTO> tempRelatedItemList = new ArrayList<PRItemDTO>();
		
		PRItemDTO tempItem = new PRItemDTO();
		tempItem.setRegMPack(1);
		tempItem.setRegPrice(0.49d);
		tempRelatedItemList.add(tempItem);
		
		tempItem = new PRItemDTO();
		//tempItem.setRegMPack(3);
		//tempItem.setRegPrice(1.59d);
		tempRelatedItemList.add(tempItem);
		
		tempItem = new PRItemDTO();
		tempItem.setRegMPack(2);
		tempItem.setRegPrice(1.69d);
		tempRelatedItemList.add(tempItem);
		
		Collections.sort(tempRelatedItemList, new Comparator<PRItemDTO>() {
			public int compare(PRItemDTO a, PRItemDTO b) {
				Double unitPrice1 = PRCommonUtil.getUnitPrice(a.getRegMPack(), a.getRegPrice(), a.getRegMPrice(), true);
				Double unitPrice2 = PRCommonUtil.getUnitPrice(b.getRegMPack(), b.getRegPrice(), b.getRegMPrice(), true);
				return unitPrice1.compareTo(unitPrice2);
			}
		});

		for (PRItemDTO itemDTO : tempRelatedItemList) {
			logger.debug("RegPrice:" + PRCommonUtil.getCurRegPrice(itemDTO));
		}
		
		Collections.sort(tempRelatedItemList, new Comparator<PRItemDTO>() {
			public int compare(PRItemDTO a, PRItemDTO b) {
				Double unitPrice1 = PRCommonUtil.getUnitPrice(a.getRegMPack(), a.getRegPrice(), a.getRegMPrice(), true);
				Double unitPrice2 = PRCommonUtil.getUnitPrice(b.getRegMPack(), b.getRegPrice(), b.getRegMPrice(), true);
				return unitPrice2.compareTo(unitPrice1);
			}
		});
		for (PRItemDTO itemDTO : tempRelatedItemList) {
			logger.debug("RegPrice:" + PRCommonUtil.getCurRegPrice(itemDTO));
		}
		
		logger.debug("test");
				
//		char runType1 = 'D';
//		Double aa = null;
//		long b = Math.round(aa);
		
//		PRItemDTO ligRepOrNonLigItem = new PRItemDTO();
//		ligRepOrNonLigItem.setPredictedMovement(0d);
//		if (ligRepOrNonLigItem.getPredictedMovement() == null || ligRepOrNonLigItem.getPredictedMovement() <= 0
//				|| ligRepOrNonLigItem.getCurRegPricePredictedMovement() == null || ligRepOrNonLigItem.getCurRegPricePredictedMovement() <= 0) {
//			logger.debug("Zero prediction");
//		}
//		
//		List<Double> tempPriceRange = new ArrayList<Double>();
//		tempPriceRange.add(1.2d);
//		tempPriceRange.add(1.8d);
//		
//		List<Double> filteredPriceRange = new ArrayList<Double>();
//		filteredPriceRange.add(1.2d);
//		
//		
//		String ignoredPricePoints = "";
//		List<Double> tempList = new ArrayList<>(tempPriceRange);
//		tempList.removeAll(filteredPriceRange);
//
//		ignoredPricePoints = PRCommonUtil.getCommaSeperatedStringFromDouble(tempList);
//
//		logger.debug(ignoredPricePoints);
//		
//		filteredPriceRange.clear();
//		filteredPriceRange.addAll(tempPriceRange);
//		tempPriceRange.clear();
//		
//		PRItemDTO itemDTO1 = new PRItemDTO();
//		itemDTO1.getRegMPack();		
//		itemDTO1.setRecommendedRegPrice(null);
//		logger.debug("Rec Price:" + PRCommonUtil.getPriceForLog(itemDTO1.getRecommendedRegPrice()));
//		
//		try{
//			int a = 1/0;
//		}catch(Exception ex) {
//			try {
//				//int b = 1/0;
//			} catch(Exception ex1) {
//				
//			}finally {
//				logger.debug("Finally-inner");
//			}
//		}finally {
//			logger.debug("Finally-outer");
//		}
//		String retailerItemCode = "24901.0";
//		retailerItemCode = retailerItemCode.replaceAll("[^\\d.]", "");
//		String tempretItemCode = String.valueOf(Integer.parseInt(retailerItemCode));
//		
//		DateFormat formatterMMddyy = new SimpleDateFormat("MM/dd/yy");
//		Date processingWeekDate = formatterMMddyy.parse("07/25/15");
//		Date tempWeekDate1 = formatterMMddyy.parse("07/25/15");
//		Date tempWeekStartDate2 = formatterMMddyy.parse("10/30/16");
//		
//		//if week start date <= 07/25/15
//		if(processingWeekDate.before(tempWeekDate1) || processingWeekDate.equals(tempWeekDate1)) {
//			logger.debug("1..");
//		} else if (processingWeekDate.equals(tempWeekStartDate2) || processingWeekDate.before(tempWeekStartDate2)) { 
//			logger.debug("2..");
//		} else {
//			logger.debug("3..");
//		}
//		
//		Date inputDate = formatterMMddyy.parse("11/18/16");
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(inputDate);//
//		int weekStartDayIndex = 4;
////		cal.set(Calendar.DAY_OF_WEEK, weekStartDayIndex + 1);
////		cal.setFirstDayOfWeek(weekStartDayIndex + 1);
//		int startDay = cal.get(Calendar.DAY_OF_WEEK) + ((cal.getFirstDayOfWeek() + 6) - (weekStartDayIndex + 1));
////		int startDay = (cal.get(Calendar.DAY_OF_WEEK) - cal.getFirstDayOfWeek());
////		int startDay = (cal.get(Calendar.DAY_OF_WEEK) - weekStartDayIndex + 1);
//		Date outputDate = null;
//		
//		outputDate = DateUtil.incrementDate(inputDate, -startDay);
//		logger.debug("outputDate:" + outputDate);
//		outputDate = DateUtil.incrementDate(inputDate, 7 - cal.get(Calendar.DAY_OF_WEEK));
//		logger.debug("outputDate:" + outputDate);
//		
//		Date tempDate = DateUtil.incrementDate(DateUtil.toDate("10/02/2016"), 6);
//		
//		String productName = "Fresh water mock test sdfd s";
//		productName = productName.length() >= 30 ? productName.substring(0, 29) : productName;
//		
//		generalTest.addMissingPointToLig(); 
//		ObjectMapper mapper = new ObjectMapper();
//		
//		PRItemDTO itemDTO = new PRItemDTO();
//		PRItemDTO item = new PRItemDTO();
//		itemDTO.getRecWeekAdInfo().setWeeklyAdStartDate("09/26/2016");
//		item.getRecWeekDisplayInfo().setDisplayWeekStartDate("09/26/2016");
//		if (itemDTO.getRecWeekAdInfo().getWeeklyAdStartDate() != null
//				&& itemDTO.getRecWeekAdInfo().getWeeklyAdStartDate().equals(item.getRecWeekDisplayInfo().getDisplayWeekStartDate())) {
//			logger.debug("in");
//		}
//		
//		long promoDuration = DateUtil.getDateDiff("11/12/16", "10/29/16");
//		
////		PRItemSaleInfoDTO itemSaleInfo = new PRItemSaleInfoDTO();
////		int a = itemSaleInfo.getSalePromoTypeLookup().getPromoTypeId();
//		
//		int shortTermPromoWeeks = 6;
//		promoDuration = DateUtil.getDateDiff("12/17/16", "11/06/16");
//
//		if ((promoDuration + 1) <= 7 * shortTermPromoWeeks) {
//			logger.debug("Short term promotion");
//		}
//		
//		int longTermPromoWeeks = 6;
//		
//		promoDuration = DateUtil.getDateDiff("12/17/16", "11/06/16");
//		if((promoDuration +1 ) > 7 * longTermPromoWeeks) {
//			logger.debug("Long term promotion");
//		}
		
//		Strategy strategy = new Strategy();
//		List<Long> strategyIds = new ArrayList<Long>();
//		strategyIds.add(10l);
//		strategyIds.add(20l);
//		strategy.strategyId = strategyIds;
//		logger.debug(mapper.writeValueAsString(strategy));
//		
//		
//		
//		PredictionDetailDTO tempPredictionDetailDTO = new PredictionDetailDTO();
//		tempPredictionDetailDTO.setPredictedMovement(0.98989);
//		Long t = (long) tempPredictionDetailDTO.getPredictedMovement();
//		
//		
//		
//		if(!(runType1 == PRConstants.RUN_TYPE_TEMP)) {
//			logger.debug("non temp run type");
//		}
//		
//		PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
//		recommendationRunHeader.setRunType(String.valueOf(PRConstants.RUN_TYPE_TEMP));
//		if(!recommendationRunHeader.getRunType().equals(String.valueOf(PRConstants.RUN_TYPE_TEMP))) {
//			logger.debug("temp run type");
//		}
//		
//		List<Integer> itemCodes = new ArrayList<Integer>();
//		itemCodes.add(1000);
//		itemCodes.add(1001);
//		itemCodes.add(1000);
//		itemCodes.add(1002);
//		itemCodes.add(1003);
//		itemCodes.add(1004);
//		itemCodes.add(1005);
//		itemCodes.add(1005);
//		itemCodes.add(1006);
//		itemCodes.add(1006);
//		itemCodes.add(1007);
//		
//		HashSet<Integer> totalDistinctItems = new HashSet<Integer>();
//		for (int i = 0; i < itemCodes.size(); i++) {
//			totalDistinctItems.add(itemCodes.get(i));
//		}
//		logger.debug("Total Distinct Items to be predicted: " + totalDistinctItems.size());
//		
//		HashSet<Integer> tempItemCodes = new HashSet<Integer>();
//		List<Integer> finalItemsList = new ArrayList<Integer>();
////		int distItemCount = 0;
//		// // Split the predictionEngineInput and call in batch mode
//		//Split the items and call in batch mode
//		int itemCount = 0;
//		for (Integer itemCode : totalDistinctItems) {
//			tempItemCodes.add(itemCode);
//			
//			//Total item count more than the batch count
//			if ((itemCount + 1) % 3 == 0) {
//				//Add the items
//				for (Integer ic : itemCodes) {
//					if(tempItemCodes.contains(ic)) {
//						finalItemsList.add(ic);
//					}
//				}
//				logger.debug("Total No of items in batch mode: " + tempItemCodes.size());
//				tempItemCodes.clear();
//				finalItemsList = new ArrayList<Integer>();
//			}
//			
//			
//			if (((totalDistinctItems.size() - itemCount) < 3)
//					&& ((itemCount + 1) == totalDistinctItems.size())) {
//				//Add the items
//				for (Integer ic : itemCodes) {
//					if(tempItemCodes.contains(ic)) {
//						finalItemsList.add(ic);
//					}
//				}
//				logger.debug("Remaning No of items in batch mode: " + tempItemCodes.size());
//			}
//			itemCount++;
//		}
		
//		Set<String> test = new HashSet<String>();
//		test.add("1");
//		test.add("2");
//		
//		String a = PRCommonUtil.getCommaSeperatedStringFromString(test);
//		logger.debug("a:" + a);
		
		generalTest.intialSetup();
		
		List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
		PredictionInputDTO predictionInputDTO = new PredictionInputDTO();
		predictionInputDTO.predictionItems = new ArrayList<PredictionItemDTO>();
		
		PredictionItemDTO predictionItemDTO  = new PredictionItemDTO();
		predictionItemDTO.itemCodeOrLirId = 1000;
		predictionItemDTO.lirInd = false;
		predictionInputDTO.predictionItems.add(predictionItemDTO);
		
		predictionItemDTO  = new PredictionItemDTO();
		predictionItemDTO.itemCodeOrLirId = 1001;
		predictionItemDTO.lirInd = false;
		predictionInputDTO.predictionItems.add(predictionItemDTO);
		
		predictionItemDTO  = new PredictionItemDTO();
		predictionItemDTO.itemCodeOrLirId = 1002;
		predictionItemDTO.lirInd = false;
		predictionInputDTO.predictionItems.add(predictionItemDTO);
		
		
		predictionInputDTOs.add(predictionInputDTO);
		
//		PredictionAnalysis predictionAnalysis = new PredictionAnalysis();
//		predictionAnalysis.newItemsList(generalTest.conn, predictionInputDTOs);
		
		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = new PricingEngineDAO().getRecommendationRules(generalTest.conn);
		
		boolean isRuleEnabled = new PricingEngineService().isRuleEnabled(recommendationRuleMap, "R4D-1", 1);
		
		logger.debug("isRuleEnabled:" + isRuleEnabled);
		
		
		List<PRProductGroupProperty> productGroupProperties = new ArrayList<PRProductGroupProperty>();
		List<ProductDTO> productList = new ArrayList<ProductDTO>();
		ProductDTO productDTO = new ProductDTO();
		productDTO.setProductLevelId(4);
		productDTO.setProductId(1348);
		productList.add(productDTO);
		
		productGroupProperties = new PricingEngineDAO().getProductGroupProperties(generalTest.conn, productList);
		
		
		PriceAdjustment priceAdjustment = new PriceAdjustment();
		List<PRItemDTO> allItemsOfRun = new ArrayList<PRItemDTO>();
		for (int i = 0; i < 1150; i++) {
			PRItemDTO itemDTO = new PRItemDTO();
			itemDTO.setItemCode(i);
			allItemsOfRun.add(itemDTO);
		}
		
//		priceAdjustment.updateItemWithPriceGroupDetail(generalTest.conn,allItemsOfRun);
		

		
 		
		
		/***** subs testing *******/
//		subsAdjTesting(generalTest.conn);
		/**** subs testing *******/
		
//		Set<String> itemsToBeChecked = new HashSet<>();
//		PromotionDAO promoDAO = new PromotionDAO(generalTest.conn);
//		itemsToBeChecked.add("16183");
//		promoDAO.getPromotionsByItems(generalTest.conn, itemsToBeChecked, 6238, 6244, 7, 43, 50);
//		
//		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
//		String curWkStartDate = DateUtil.getWeekStartDate(-1);
//		RetailCalendarDTO curWkDTO = retailCalendarDAO.getCalendarId(generalTest.conn, curWkStartDate,
//				Constants.CALENDAR_WEEK);
		
		/***** Get sale, ad and display details ***************/
		//Get sale, ad and display details
//		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
//		PricingEngineService pricingEngineService = new PricingEngineService();
//		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
//		//1266 - ICE CREAM, 1345-SNACKS
//		int noOfsaleAdDisplayWeeks = 3, productId = 1345, zoneId = 6, chainId = 50;
//		String curWeekStartDate = "9/25/2016";
//		String recWeekStartDate = "10/02/2016";
//		
//		PRItemDTO prItemDTO = new PRItemDTO();
//		prItemDTO.setItemCode(6938);
//		itemDataMap.put(new ItemKey(prItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR), prItemDTO);
		
//		prItemDTO = new PRItemDTO();
//		prItemDTO.setItemCode(13133);
//		itemDataMap.put(new ItemKey(prItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR), prItemDTO);
		
		
//		HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = pricingEngineDAO.getSaleDetails(generalTest.conn, Constants.CATEGORYLEVELID,
//				productId, chainId, zoneId, recWeekStartDate, noOfsaleAdDisplayWeeks);
//		HashMap<Integer, List<PRItemAdInfoDTO>> adDetails = pricingEngineDAO.getAdDetails(generalTest.conn, Constants.CATEGORYLEVELID, productId,
//				chainId, zoneId, recWeekStartDate, noOfsaleAdDisplayWeeks);
//		HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails = pricingEngineDAO.getDisplayDetails(generalTest.conn, Constants.CATEGORYLEVELID,
//				productId, chainId, zoneId, recWeekStartDate, noOfsaleAdDisplayWeeks);
//
//		pricingEngineService.fillSaleDetails(itemDataMap, saleDetails, curWeekStartDate, recWeekStartDate, noOfsaleAdDisplayWeeks);
//		pricingEngineService.fillAdDetails(itemDataMap, adDetails, curWeekStartDate, recWeekStartDate, noOfsaleAdDisplayWeeks);
//		pricingEngineService.fillDisplayDetails(itemDataMap, displayDetails, curWeekStartDate, recWeekStartDate, noOfsaleAdDisplayWeeks);
		/******* Get sale, ad and display details ********************/
		
		/****** Sale prediction ***********************************/
		PredictionComponent predictionComponent = new PredictionComponent();
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		PRRecommendationRunHeader recRunHeader = new PRRecommendationRunHeader();
		
		recRunHeader.setLocationLevelId(6);
		recRunHeader.setLocationId(6);
		recRunHeader.setProductLevelId(4);
		recRunHeader.setProductId(1345);
		recRunHeader.setCalendarId(6404);
		
		List<PRItemDTO> items = new ArrayList<PRItemDTO>();
		PRItemSaleInfoDTO curSaleInfo = new PRItemSaleInfoDTO();
		PRItemAdInfoDTO curAdInfo = new PRItemAdInfoDTO();
		PRItemDisplayInfoDTO curDisplayInfo = new PRItemDisplayInfoDTO();
		PRItemSaleInfoDTO recWeekSaleInfo = new PRItemSaleInfoDTO();
		PRItemAdInfoDTO recWeekAdInfo = new PRItemAdInfoDTO();
		PRItemDisplayInfoDTO recWeekDisplayInfo = new PRItemDisplayInfoDTO();
		
		curSaleInfo.setSalePrice(new MultiplePrice(2, 3.19));
		curSaleInfo.setPromoTypeId(PromoTypeLookup.BOGO.getPromoTypeId());
		curAdInfo.setAdPageNo(10);
		curAdInfo.setAdBlockNo(2);
		curDisplayInfo.setDisplayTypeLookup(DisplayTypeLookup.FAST_WALL);
		
		recWeekSaleInfo.setSalePrice(new MultiplePrice(2, 3.19));
		recWeekSaleInfo.setPromoTypeId(PromoTypeLookup.BOGO.getPromoTypeId());
		recWeekAdInfo.setAdPageNo(10);
		recWeekAdInfo.setAdBlockNo(2);
		recWeekDisplayInfo.setDisplayTypeLookup(DisplayTypeLookup.FAST_WALL);
		
		PRItemDTO prItemDTO1 = new PRItemDTO();
		prItemDTO1.setItemCode(6938);
		prItemDTO1.setUpc("498448494954");
		prItemDTO1.setRegPrice(3.19);
		prItemDTO1.setRecommendedRegPrice(new MultiplePrice(1, 3.19));
//		prItemDTO1.setCurSaleInfo(curSaleInfo);
//		prItemDTO1.setCurAdInfo(curAdInfo);
//		prItemDTO1.setCurDisplayInfo(curDisplayInfo);
		
		prItemDTO1.setRecWeekSaleInfo(recWeekSaleInfo);
		prItemDTO1.setRecWeekAdInfo(recWeekAdInfo);
		prItemDTO1.setRecWeekDisplayInfo(recWeekDisplayInfo);
		items.add(prItemDTO1);
		
//		predictionComponent.predictPromoItems(generalTest.conn, executionTimeLogs, recRunHeader, items, false);
		
		
		/****** Sale prediction ***********************************/
		
		/***************************************************************/
		/*RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		List<Integer> priceZoneStores = new ArrayList<Integer>();
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		ItemService itemService = new ItemService(executionTimeLogs);
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		long runId = 14290;
		
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		inputDTO.setLocationLevelId(6);
		inputDTO.setLocationId(6);
		inputDTO.setProductLevelId(4);
		inputDTO.setProductId(1303);
		
		PRItemDTO prItemDTO = new PRItemDTO();
		prItemDTO.setItemCode(151216);
		itemDataMap.put(new ItemKey(prItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR), prItemDTO);
		
		LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails = retailCalendarDAO.getAllPreviousWeeks(generalTest.conn,
				"02/14/2016", 52);
		
		priceZoneStores = itemService.getPriceZoneStores(generalTest.conn, inputDTO.getProductLevelId(), inputDTO.getProductId(),
				inputDTO.getLocationLevelId(), inputDTO.getLocationId());
		
		HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData = pricingEngineDAO.getMovementDataForZone(generalTest.conn, inputDTO,
				priceZoneStores, "02/14/2016", 52, itemDataMap);
		
		pricingEngineDAO.getMovementDataForZone(movementData, itemDataMap, previousCalendarDetails, 13);
		
		List<Long> runIds = new ArrayList<Long>();
		runIds.add(runId);
		HashMap<Long, List<PRItemDTO>> runAndItsRecommendedItems = pricingEngineDAO.getRecommendationItems(generalTest.conn, runIds);
		
		RecommendationAnalysis recommendationAnalysis = new RecommendationAnalysis();
		recommendationAnalysis.recommendationAnalysis(generalTest.conn, runId, runAndItsRecommendedItems, itemDataMap, previousCalendarDetails);
		PristineDBUtil.commitTransaction(generalTest.conn, "Commit Price Recommendation");*/
		/***************************************************************/
		
//		
//		long runId = 14288;
//		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
//		List<PRItemDTO> itemList = pricingEngineDAO.getRecommendationItems(generalTest.conn, runId);
//		RecommendationAnalysis recommendationAnalysis = new RecommendationAnalysis();
//		recommendationAnalysis.insertCompDetails(generalTest.conn, runId, itemList);
//		
//		
//		generalTest.getCompData();
//		
//		PredictionReportService prs = new PredictionReportService();
//		ObjectMapper mapper1 = new ObjectMapper();
//		String jsonOutput = "";
//		jsonOutput = mapper.writeValueAsString(prs.generatePredictionReportForUI(generalTest.conn, 6, 15, 4, 1297, 6379));
//		logger.debug("Output:" + jsonOutput);
//		
//		String inputPath = PropertyManager.getProperty("WEEKLY_CHAIN_PRED_ACCURACY_INPUT");
//		ArrayList<String> csvFiles = PrestoUtil.getAllFilesInADirectory(inputPath, "csv");
//		PrestoUtil.deleteFiles(csvFiles);
//		
//		DateFormat formatterMMddyy1 = new SimpleDateFormat("MM/dd/yy");
//		Date processingWeekDate = formatterMMddyy1.parse("08/25/15");
//		Date tempWeekDate1 = formatterMMddyy1.parse("07/25/15");
//		
//		//if week start date <= 07/25/15
//		if(processingWeekDate.before(tempWeekDate1) || processingWeekDate.equals(tempWeekDate1)) {
//			logger.debug("Week before");
//		} else {
//			logger.debug("Week after");
//		}
//		
//		int OOS_SHELF_CAPACITY_PCT = 50;
//		boolean cacheEnabled = true;
//		Date curDate = new Date();
//		DateFormat formatterMMddyy = new SimpleDateFormat("MMddyyHms");
//		logger.debug(formatterMMddyy.format(curDate));
//		HashSet<String> test = new HashSet<String>();
//		HashSet<String> test1 = new HashSet<String>();
//		HashSet<HashSet<String>> distinctSet = new HashSet<HashSet<String>>();
//		test.add("010");
//		test.add("020");
//		test1.add("020");
//		test1.add("010");
//		distinctSet.add(test);
//		distinctSet.add(test1);
//		logger.debug(distinctSet.toString());
//		
//		List<String> storesInZone = new ArrayList<String>();
//		HashSet<String> distinctStores = new HashSet<String>();
//		storesInZone.add("010");
//		storesInZone.add("020");
//		distinctStores.add("010");
//		distinctStores.add("020");
//		distinctStores.add("030");
//		if (storesInZone.size() > 0 && distinctStores.containsAll(storesInZone)) {
//			logger.debug("storesInZone:" + storesInZone.toString());
//		}
//		
//		
//		if ("FALSE".equalsIgnoreCase(PropertyManager.getProperty("EHCACHE_ENABLED"))) {
//			cacheEnabled = false;
//		}
//		
//		double minShelfCapacityMov = Double.valueOf(0)
//				* Double.valueOf((10 / 100d));
//		
//		List<OOSItemDTO> allCandidateItems = new ArrayList<OOSItemDTO>();
//		OOSItemDTO oosItemDTO = new OOSItemDTO();
//		oosItemDTO.getOOSCriteriaData().setForecastMovOfProcessingTimeSlotOfItemOrLig(20);
//		oosItemDTO.setIsOutOfStockItem(true);
//		oosItemDTO.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_1.getOOSCriteriaId());
//		allCandidateItems.add(oosItemDTO);
//
//		oosItemDTO = new OOSItemDTO();
//		oosItemDTO.getOOSCriteriaData().setForecastMovOfProcessingTimeSlotOfItemOrLig(10);
//		oosItemDTO.setIsOutOfStockItem(true);
//		oosItemDTO.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_6.getOOSCriteriaId());
//		allCandidateItems.add(oosItemDTO);
//		
//		oosItemDTO = new OOSItemDTO();
//		oosItemDTO.getOOSCriteriaData().setForecastMovOfProcessingTimeSlotOfItemOrLig(10);
//		oosItemDTO.setIsOutOfStockItem(true);
//		oosItemDTO.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_2.getOOSCriteriaId());
//		allCandidateItems.add(oosItemDTO);
//		
//		oosItemDTO = new OOSItemDTO();
//		oosItemDTO.getOOSCriteriaData().setForecastMovOfProcessingTimeSlotOfItemOrLig(10);
//		oosItemDTO.setIsOutOfStockItem(true);
//		oosItemDTO.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_6.getOOSCriteriaId());
//		allCandidateItems.add(oosItemDTO);
//		
//		int topItemCount = 1;
//		for (OOSItemDTO oosItem : allCandidateItems) {
//			if (oosItem.getIsOutOfStockItem()) {
//				if (topItemCount <= 2 || oosItemDTO.getOOSCriteriaId() == OOSCriteriaLookup.CRITERIA_6.getOOSCriteriaId()) {
//					oosItem.setIsSendToClient(true);
//					//Don't count criteria 6 - Potential OOS (always include that)
//					if (oosItem.getOOSCriteriaId() != OOSCriteriaLookup.CRITERIA_6.getOOSCriteriaId()) {
//						topItemCount = topItemCount + 1;
//					}
//				}
//			}
//		}
//		
//		OOSItemDTO oosCandidateItem = new OOSItemDTO();
//		oosCandidateItem.getOOSCriteriaData().setShelfCapacityOfItemOrLig(9);
//		double shelfCapacity = Double.valueOf(oosCandidateItem.getOOSCriteriaData().getShelfCapacityOfItemOrLig()) * 
//				Double.valueOf(OOS_SHELF_CAPACITY_PCT/100d);
//		
//		
//		 
//		Collections.sort(allCandidateItems, new Comparator<OOSItemDTO>() {
//			public int compare(OOSItemDTO a, OOSItemDTO b) {
//				if (a.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig() > b.getOOSCriteriaData()
//						.getForecastMovOfProcessingTimeSlotOfItemOrLig())
//					return 1;
//				if (a.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig() < b.getOOSCriteriaData()
//						.getForecastMovOfProcessingTimeSlotOfItemOrLig())
//					return -1;
//				return 0;
//			}
//		});
//		
//		int transBasedExpectation = (int) Math.round((Double.valueOf(34)
//				/ Double.valueOf(5319.0))* 387);
//		
//		double testx = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(123d));
//		
//		Integer a = Double.valueOf(0.01).intValue();
//		int  b = (int) Math.ceil(10.99);
//		
//		Integer tt = 10;
//		List<PRItemDTO> items = new ArrayList<PRItemDTO>();
//		PRItemDTO item = new PRItemDTO();
//		item.setItemCode(10);
//		items.add(item);
//		generalTest.test1(items, tt);
//		for(PRItemDTO itemDTO : items){
//			logger.debug("Item Code: " + itemDTO.getItemCode());
//		}
//		logger.debug("tt: " + tt);
		//PRPriceGroupRelnDTO test = new PRPriceGroupRelnDTO();
		//jsonTest();
		//logger.debug("Test" + test.getValueType());
		//test.setValueType('\0');
		//logger.debug("Test" + test.getValueType());
//		PRItemDTO item = new PRItemDTO();
//		if(item.getRegPrice() == null && item.getRegPrice()==0){
//			logger.debug("Test");
//		}
		
//		Double a = 10d;
//		int b = 2;
//		
//		
//		
//		Double c = a / b;
//		logger.debug("c:" + c);
//		PRItemDTO item = new PRItemDTO();
//		item.setRegMPack(null);
//		
//		if (item.getRegMPack() != null && item.getRegMPack() > 1) {
//			logger.debug("Test");
//		}
		
//		double minImpact = 3.9999;
//		//Object vId = null;
//		//Integer vendorId = (Integer) vId;
//		//PRItemDTO itemDTO = new PRItemDTO();		
//		//itemDTO.setItemSize((double)vId);		
//		logger.debug("Min Impact:" + PRFormatHelper.doubleToTwoDigitString(minImpact * 100));
//		
//		PRItemDTO itemDTO1 = new PRItemDTO();
//		//Cost, price is there
//		itemDTO1.setListCost(10.09d);
//		itemDTO1.setRegPrice(12d);
//		if ((itemDTO1.getListCost() == null || itemDTO1.getListCost() <= 0)
//				&& (itemDTO1.getRegPrice() != null && itemDTO1.getRegPrice() > 0)) {
//			logger.debug("1. Cost & Price is there");
//		}
//		
//		//No Cost, price is there
//		itemDTO1.setListCost(null);
//		itemDTO1.setRegPrice(12d);
//		if ((itemDTO1.getListCost() == null || itemDTO1.getListCost() <= 0)
//				&& (itemDTO1.getRegPrice() != null && itemDTO1.getRegPrice() > 0)) {
//			logger.debug("2. No Cost, price is there");
//		}
//		
//		// No Cost, no price is there
//		itemDTO1.setListCost(null);
//		itemDTO1.setRegPrice(null);
//		if ((itemDTO1.getListCost() == null || itemDTO1.getListCost() <= 0)
//				&& (itemDTO1.getRegPrice() != null && itemDTO1.getRegPrice() > 0)) {
//			logger.debug("3. No Cost, no price is there");
//		}
//		
//		// No Cost, price is there
//		itemDTO1.setListCost(null);
//		itemDTO1.setRegPrice(0.01d);
//		if ((itemDTO1.getListCost() == null || itemDTO1.getListCost() <= 0)
//				&& (itemDTO1.getRegPrice() != null && itemDTO1.getRegPrice() > 0)) {
//			logger.debug("4. No Cost, price is there");
//		}
//		
//		// Cost, no price is there
//		itemDTO1.setListCost(0.01d);
//		itemDTO1.setRegPrice(null);
//		if ((itemDTO1.getListCost() == null || itemDTO1.getListCost() <= 0)
//				&& (itemDTO1.getRegPrice() != null && itemDTO1.getRegPrice() > 0)) {
//			logger.debug("4. Cost is there no Price");
//		}
		
//		Double a = null;
//		double aa = a * 10;
//		logger.debug("aa:" + aa);
		
		//generalTest.intialSetup();		
		//generalTest.getPeriodDetail();
		
		//generalTest.getCompData();
		//generalTest.getItemAndLir();
		//HashMap<Integer, PRItemDTO> itemDataMap = generalTest.getAuthorizedItemZone();
		//generalTest.getAuthorizedItemStore(itemDataMap);
		//generalTest.getAllStrategies();
		//generalTest.getAuthorizedItemsOfZoneAndStore();
	}
	
	private static void test1(Integer a) {
		a = 10;
	}
	
	private static boolean testDebugFunction() {
		logger.debug("test123");
		return true;
	}
	
	private void connectionTest() throws SQLException {
		logger.debug(conn.isClosed());
		PristineDBUtil.close(conn);
		logger.debug(conn.isClosed());
		
		setConnection();
		connectionTest1(conn);
		
		logger.debug(conn);
	}
	
	private void connectionTest1(Connection conn) {
		logger.debug("Conn1: " + conn);
		PristineDBUtil.close(conn);
		conn = null;
		setConnection();
		conn = this.conn;
	}
	
	private static void subsAdjTesting(Connection conn) throws GeneralException {
		SubstituteDAO subsDAO = new SubstituteDAO();
		com.pristine.service.offermgmt.substitute.SubstituteService substituteService = 
				new com.pristine.service.offermgmt.substitute.SubstituteService(false);
		List<Integer> retLirIds = new ArrayList<Integer>();
		List<Integer> nonLigItems = new ArrayList<Integer>();
		List<PRItemDTO> recommendedItems = new ArrayList<PRItemDTO>();
		PRItemDTO itemDTO = null;
		
		retLirIds.add(12388);
		retLirIds.add(12389);
		
		//main item
		itemDTO = new PRItemDTO();
		itemDTO.setRetLirId(12388);
		itemDTO.setItemCode(458465);
		itemDTO.setRecommendedRegPrice(new MultiplePrice(1, 3.19));
		itemDTO.setPredictedMovement(2000d);
		itemDTO.setPredictionStatus(0);
		recommendedItems.add(itemDTO);
		
		itemDTO.setRetLirId(12388);
		itemDTO.setItemCode(458466);
		itemDTO.setRecommendedRegPrice(new MultiplePrice(1, 3.19));
		itemDTO.setPredictedMovement(1250d);
		itemDTO.setPredictionStatus(0);
		recommendedItems.add(itemDTO);
		
		//Subs item
		itemDTO = new PRItemDTO();
		itemDTO.setItemCode(12389);
		itemDTO.setLir(true);
		itemDTO.setRecommendedRegPrice(new MultiplePrice(1, 2.49));
		itemDTO.setRegMPack(1);
		itemDTO.setRegPrice(2.39);
		recommendedItems.add(itemDTO);
		
		itemDTO = new PRItemDTO();
		itemDTO.setItemCode(13651);
		itemDTO.setLir(true);
		itemDTO.setRecommendedRegPrice(new MultiplePrice(1, 2.19));
		itemDTO.setRegMPack(2);
		itemDTO.setRegMPrice(4d);
		recommendedItems.add(itemDTO);
		
		itemDTO = new PRItemDTO();
		itemDTO.setItemCode(13654);
		itemDTO.setLir(true);
		itemDTO.setRecommendedRegPrice(new MultiplePrice(1, 2.29));
		itemDTO.setRegMPack(1);
		itemDTO.setRegPrice(2.19);
		recommendedItems.add(itemDTO);
		
		itemDTO = new PRItemDTO();
		itemDTO.setItemCode(5083);
		itemDTO.setLir(true);
		itemDTO.setRecommendedRegPrice(new MultiplePrice(1, 2.39));
		itemDTO.setRegMPack(1);
		itemDTO.setRegPrice(2.49);
		recommendedItems.add(itemDTO);
		
		
		
//		HashMap<ItemKey, List<PRSubstituteItem>> substituteItems =  subsDAO.getSubstituteItemsNew(conn, 6, 15, retLirIds, nonLigItems);
//		substituteService.adjustPredWithSubsEffect(substituteItems, recommendedItems);
		
		
	}
	
	private void addMissingPointToLig() {
		PredictionComponent predictionComponent = new PredictionComponent();
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<ItemKey, PredictionItemDTO> itemsForPrediction = new HashMap<ItemKey, PredictionItemDTO>();
		PRItemDTO itemDTO = null;
		PredictionItemDTO predictionItemDTO = null;
		ItemKey itemKey = null;
		PricePointDTO pricePointDTO = null;
		
		List<PRItemDTO> items = new ArrayList<PRItemDTO>();
		itemDTO = new PRItemDTO();
		itemDTO.setItemCode(10000);
		items.add(itemDTO);
		
		itemDTO = new PRItemDTO();
		itemDTO.setItemCode(10001);
		items.add(itemDTO);
		retLirMap.put(100, items);
		
		predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();
		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(1);
		pricePointDTO.setRegPrice(2.19);
		predictionItemDTO.pricePoints.add(pricePointDTO);
		itemKey = new ItemKey(10000, 0);
		itemsForPrediction.put(itemKey, predictionItemDTO);
		
		predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();
		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(2);
		pricePointDTO.setRegPrice(3.19);
		predictionItemDTO.pricePoints.add(pricePointDTO);
		
		pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(2);
		pricePointDTO.setRegPrice(4.19);
		predictionItemDTO.pricePoints.add(pricePointDTO);
		itemKey = new ItemKey(10001, 0);
		itemsForPrediction.put(itemKey, predictionItemDTO);
		
//		predictionComponent.addMissingPricePointToLigMembers(retLirMap, itemsForPrediction, true);
	}
	
	private void test1(List<PRItemDTO> items, Integer test){
		test = 20;
		logger.debug("test");
		for(PRItemDTO itemDTO : items){
			itemDTO.setItemCode(20);
			logger.debug("Item Code: " + itemDTO.getItemCode());
		}
	}
	
	private static void jsonTest() throws JsonProcessingException{
		PredictionInputDTO predInput = new PredictionInputDTO();
		ObjectMapper mapper = new ObjectMapper();
		List<PredictionItemDTO> predItemList = new ArrayList<PredictionItemDTO>();
		
        predInput.locationLevelId = 1;
        predInput.locationId = 1;
        predInput.productLevelId = 1;
        predInput.productId = 1;
        predInput.startCalendarId = 1;
        predInput.endCalendarId = 1;
        predInput.predictedBy = "";
        predInput.runType = 'O';
        predInput.useSubstFlag = "Y";
        predInput.recommendationRunId = 1;
        predInput.isForcePrediction = true;
        
		PredictionItemDTO predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.itemCodeOrLirId = 1;
		predictionItemDTO.lirInd = false;
		predictionItemDTO.mainOrImpactFlag = 'M';
		predictionItemDTO.upc = "34343";
//
		List<PricePointDTO> ppList = new ArrayList<PricePointDTO>();
		PricePointDTO pricePointDTO = new PricePointDTO();
//		pricePointDTO.setRegPrice(2.19);
//		pricePointDTO.setRegQuantity(1);
		ppList.add(pricePointDTO);
		predictionItemDTO.pricePoints = ppList;
//
		predItemList.add(predictionItemDTO);

		predInput.predictionItems = predItemList;
        logger.debug(mapper.writeValueAsString(pricePointDTO));
	}
	private void getPeriodDetail() throws GeneralException{
		RetailCalendarDAO retailCalDAO = new RetailCalendarDAO();		
		retailCalDAO.getPeriodDetail(conn, "03/18/2015");
	}
	
	private void getAllStrategies() throws OfferManagementException{
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		StrategyService strategyService = new StrategyService(executionTimeLogs);
		PRStrategyDTO inputDTO = TestHelper.getStrategy(1, 6, 5, 4, 1266, "10/22/2017", "10/28/2017", false, -1, -1, -1);
		strategyService.getAllActiveStrategies(conn, inputDTO, 7);
	}
	
	private List<PRItemDTO> getAuthorizedItemsOfZoneAndStore() {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		ItemService itemService = new ItemService(executionTimeLogs);
		List<PRItemDTO> itemDataMap = new ArrayList<PRItemDTO>();
		PRStrategyDTO inputDTO = TestHelper
				.getStrategy(1, 6, 66, 4, 212, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		try {
////			itemDataMap = itemService.getAuthorizedItemsOfZoneAndStore(conn, inputDTO);
//		} catch (Exception | OfferManagementException e) {
//			if (e instanceof OfferManagementException)
//				logger.info(((OfferManagementException) e).getRecommendationErrorCode().getErrorCode());
//
//			logger.info(e);
//		}
		logger.debug("Stop Log");
		for(PRItemDTO item : itemDataMap){
			if(item.getItemCode() == 24588){
				logger.debug("Pre-Price Status of Item: " + item.getItemCode() + "--" + item.getIsPrePriced());
			}
		}
		return itemDataMap;
	}
	
//	private HashMap<Integer, PRItemDTO> getAuthorizedItemZone(){
//		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
//		ItemService itemService = new ItemService(executionTimeLogs);
//		HashMap<Integer, PRItemDTO> itemDataMap = new HashMap<Integer, PRItemDTO>();
//		PRStrategyDTO inputDTO = TestHelper.getStrategy(1, 6, 66, 4, 202, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		try {
//			itemDataMap = itemService.getAuthorizedItemsOfZone(conn, inputDTO);
//		} catch (Exception | OfferManagementException e) {
//			if (e instanceof OfferManagementException)			
//				logger.info(((OfferManagementException) e).getRecommendationErrorCode().getErrorCode());
//			
//			logger.info(e);
//		}
//		logger.debug("Stop Log");
//		return itemDataMap;
//	}
	
//	private void getAuthorizedItemStore(HashMap<Integer, PRItemDTO> itemDataMap) throws GeneralException, Exception, OfferManagementException{
//		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
//		ItemService itemService = new ItemService(executionTimeLogs);
//		List<PRItemDTO> storeItems;
//		HashMap<Integer, HashMap<Integer, PRItemDTO>> itemDataMapStore = new HashMap<Integer, HashMap<Integer, PRItemDTO>>();
//		PRStrategyDTO inputDTO = TestHelper.getStrategy(1, 6, 66, 4, 202, "12/28/2014", "01/03/2014", false, -1, -1, -1);
//		logger.debug("Start Log");
//		storeItems = itemService.getAuthorizedItemsOfStores(conn, inputDTO, false);
//		logger.debug(storeItems.size());
//		logger.debug("Stop Log");
//		logger.debug("Start Log");
//		itemService.populateStoreItemMap(conn, inputDTO, itemDataMap, storeItems, itemDataMapStore);
//		logger.debug("Stop Log");
//	}
private void testExecutionTimeLog(){
	List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
	PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();	
	long runId = 1254;
	int executionOrder = 0;
	
	ExecutionTimeLog overallExecutionTime = new ExecutionTimeLog(PRConstants.OVERALL_RECOMMENDATION);
		for (int i = 0; i < 100; i++) {
			System.out.print("test");
		}
	overallExecutionTime.setEndTime();
	executionTimeLogs.add(overallExecutionTime);
	
	PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
	pricingEngineDAO.insertExecutionTimeLog(conn, recommendationRunHeader, executionTimeLogs);
	try {
		PristineDBUtil.commitTransaction(conn,"");
	} catch (GeneralException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
	
	private void getItemAndLir() {
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		ArrayList<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> tRetLirMap ;
		
		RetailCalendarDTO runForWeek;
		try {
			runForWeek = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(-1), Constants.CALENDAR_WEEK);

			inputDTO.setStartCalendarId(runForWeek.getCalendarId());
			inputDTO.setEndCalendarId(runForWeek.getCalendarId());
			inputDTO.setStartDate(runForWeek.getStartDate());
			inputDTO.setEndDate(runForWeek.getEndDate());

			inputDTO.setLocationLevelId(6);
			inputDTO.setLocationLevelId(66); // Zone - 693

			inputDTO.setProductLevelId(4); // Category
			inputDTO.setProductId(149); // Trash bag(149), peanut butter(264)

//			itemList = pricingEngineDAO.getAllItems(conn, inputDTO);
//			tRetLirMap = pricingEngineDAO.populateRetLirDetailsInMap(itemList);
			
			logger.debug("Stop Log");
			
		} catch (GeneralException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private void getCompData() throws GeneralException, Exception{
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		PRStrategyDTO tempDTO = new PRStrategyDTO();
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		RetailCalendarDTO curCalDTO = new RetailCalendarDTO();
		RetailCalendarDTO resetCalDTO = new RetailCalendarDTO();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		ItemKey itemKey;
		
		// Week in which the batch runs
		curCalDTO = getCurCalDTO(retailCalendarDAO);
		String resetDate = null;
		if (resetDate == null) {
			String startDate = DateUtil.getWeekStartDate(DateUtil.toDate(curCalDTO.getStartDate()), 24);
			resetDate = startDate;
			resetCalDTO = retailCalendarDAO.getCalendarId(conn, startDate, Constants.CALENDAR_WEEK);
		}

		
		inputDTO.setLocationLevelId(6);
		inputDTO.setLocationLevelId(6); // Zone - 693

		inputDTO.setProductLevelId(4); // Category
		inputDTO.setProductId(1359); // Trash bag(149), peanut butter(264), frozen ice(156)
		
		tempDTO.copy(inputDTO);
		tempDTO.setLocationLevelId(Constants.STORE_LEVEL_ID);
		tempDTO.setLocationId(5772);
		
		
		PRItemDTO itemDTO = new PRItemDTO();
		itemDTO.setItemCode(596991);	
		itemDTO.setRetailerItemCode("471252");
		itemKey = PRCommonUtil.getItemKey(itemDTO);
		itemDataMap.put(itemKey, itemDTO);
		
		itemDTO = new PRItemDTO();
		itemDTO.setItemCode(601619);
		itemDTO.setRetailerItemCode("471252");
		itemKey = PRCommonUtil.getItemKey(itemDTO);
		itemDataMap.put(itemKey, itemDTO);
		
//		itemDTO = new PRItemDTO();
//		itemDTO.setItemCode(19301);	
//		itemDTO.setRetLirId(101443);
//		itemKey = PRCommonUtil.getItemKey(itemDTO);
//		itemDataMap.put(itemKey, itemDTO);
		
		pricingEngineDAO.getCompPriceData(conn, tempDTO, curCalDTO, resetCalDTO, 24, itemDataMap);
	}
	
	private RetailCalendarDTO getCurCalDTO(RetailCalendarDAO retailCalendarDAO) throws GeneralException {
		RetailCalendarDTO curCalDTO;
		curCalDTO = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(0), Constants.CALENDAR_WEEK);
		return curCalDTO;
	}
	
	public void intialSetup() {
		initialize();
	}
	
	protected void setConnection() {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	/**
	 * Initializes object
	 */
	protected void initialize() {
		setConnection();
	}
}
