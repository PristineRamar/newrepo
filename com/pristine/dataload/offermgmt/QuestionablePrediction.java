package com.pristine.dataload.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.RecommendationAnalysis;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class QuestionablePrediction {
	Connection conn = null;
	private static Logger logger = Logger.getLogger("QuestionablePrediction");
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
//	private static final String WEEK_START_DATE = "WEEK_START_DATE=";
	private int inpLocationLevelId = 0, inpLocationId = 0, inpProductLevelId = 0, inpProductId = 0;
//	private String inpWeekStartDate = "";

	public QuestionablePrediction() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ge) {
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-questionable-prediction.properties");
		PropertyManager.initialize("recommendation.properties");
		QuestionablePrediction questionablePrediction = new QuestionablePrediction();

		for (String arg : args) {
			if (arg.startsWith(LOCATION_ID)) {
				questionablePrediction.inpLocationId = Integer.parseInt(arg.substring(LOCATION_ID.length()));
			} else if (arg.startsWith(LOCATION_LEVEL_ID)) {
				questionablePrediction.inpLocationLevelId = Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length()));
			} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
				questionablePrediction.inpProductLevelId = Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length()));
			} else if (arg.startsWith(PRODUCT_ID)) {
				questionablePrediction.inpProductId = Integer.parseInt(arg.substring(PRODUCT_ID.length()));
			} /*else if (arg.startsWith(WEEK_START_DATE)) {
				questionablePrediction.inpWeekStartDate = arg.substring(WEEK_START_DATE.length());
			}*/
		}

		logger.info("**********************************************");
		questionablePrediction.findQuestionablePrediction();
		logger.info("**********************************************");
	}

	private void findQuestionablePrediction() {
		RecommendationAnalysis recommendationAnalysis = new RecommendationAnalysis();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		List<Long> runIds = new ArrayList<Long>();
		long runId = 0;
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		List<Integer> priceZoneStores = new ArrayList<Integer>();
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		ItemService itemService = new ItemService(executionTimeLogs);

		try {
			int noOfWeeksIMSData = Integer.parseInt(PropertyManager.getProperty("PR_NO_OF_WEEKS_IMS_DATA"));
			int noOfWeeksBehind = Integer.parseInt(PropertyManager.getProperty("PR_AVG_MOVEMENT"));

			// Get latest run id
			runId = pricingEngineDAO.getLatestRecommendationRunId(conn, inpLocationLevelId, inpLocationId, inpProductLevelId, inpProductId);
			runIds.add(runId);

			// Get recommended item details
			HashMap<Long, List<PRItemDTO>> runAndItsRecommendedItems = pricingEngineDAO.getRecommendationItems(conn, runIds);

			// Fill item data map
			for (PRItemDTO itemDTO : runAndItsRecommendedItems.get(runId)) {
				ItemKey itemKey = PRCommonUtil.getItemKey(itemDTO);
				itemDataMap.put(itemKey, itemDTO);
			}

			// Get stores in the zone
			priceZoneStores = itemService.getPriceZoneStores(conn, inpProductLevelId, inpProductId, inpLocationLevelId, inpLocationId);

			// Get recommendation header details
			PRRecommendationRunHeader recommendationRunHeader = pricingEngineDAO.getRecommendationRunHeader(conn, runId);

			// Get week detail on which recommendation ran
			RetailCalendarDTO curCalDTO = retailCalendarDAO.getWeekCalendarFromDate(conn, recommendationRunHeader.getStartRunTime());

			// Get all previous weeks calendar id's
			LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails = retailCalendarDAO.getAllPreviousWeeks(conn, curCalDTO.getStartDate(),
					noOfWeeksIMSData);

			// Get calendar details
			RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarDetail(conn, recommendationRunHeader.getCurRetailCalendarId());

			// Get movement data
			HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData = pricingEngineDAO.getMovementDataForZone(conn, null,
					priceZoneStores, calendarDTO.getStartDate(), noOfWeeksIMSData, itemDataMap);
			pricingEngineDAO.getMovementDataForZone(movementData, itemDataMap, previousCalendarDetails, noOfWeeksBehind);

			// Find questionable predictions
			recommendationAnalysis.doPredictionAnalysis(conn, runAndItsRecommendedItems.get(runId), itemDataMap, previousCalendarDetails, runId);

			PristineDBUtil.commitTransaction(conn, "Commit Transaction");
		} catch (Exception | GeneralException ex) {
			ex.printStackTrace();
			PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
			logger.error("Exception in questionable prediction process " + ex.toString() + ex + ex.getMessage());
		} finally {
			PristineDBUtil.close(conn);
		}
	}
}
