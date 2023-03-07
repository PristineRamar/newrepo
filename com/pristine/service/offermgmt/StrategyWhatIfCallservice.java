package com.pristine.service.offermgmt;

import java.sql.Connection;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dataload.offermgmt.mwr.RecommendationFlow;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class StrategyWhatIfCallservice {

	private Connection conn = null;
	private static Logger logger = Logger.getLogger("StrategyWhatIfCallservice");
	private static final String SUBMITTED_BY = "SUBMITTED_BY=";
	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	private static final String STRATEGY_ID = "STRATEGY_ID=";
	private static final String QUEUE_ID = "QUEUE_ID=";

	private static final String CONSIDER_ONLY_SCENARIO = "CONSIDER_ONLY_SCENARIO=";
	private static int locationLevelId;
	private static int locationId;
	private static int productLevelId;
	private static int productId;
	private static long queueID;
	private static String userID;
	private static Long strategyId;
	private static boolean runOnlytempStrat;

	public static void main(String[] args) throws GeneralException {

		PropertyManager.initialize("recommendation.properties");
		PropertyConfigurator.configure("log4j.properties");
		StrategyWhatIfCallservice strategyWhatIfCallservice = new StrategyWhatIfCallservice();
		logger.info("whatIf service called..");
		if (args.length > 0) {
			for (String arg : args) {
				if (arg.startsWith(SUBMITTED_BY)) {
					userID = arg.substring(SUBMITTED_BY.length());
				} else if (arg.startsWith(LOCATION_LEVEL_ID)) {
					locationLevelId = (Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length())));
				} else if (arg.startsWith(LOCATION_ID)) {
					locationId = (Integer.parseInt(arg.substring(LOCATION_ID.length())));
				} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
					productLevelId = (Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length())));
				} else if (arg.startsWith(PRODUCT_ID)) {
					productId = (Integer.parseInt(arg.substring(PRODUCT_ID.length())));
				} else if (arg.startsWith(PRODUCT_ID)) {
					productId = (Integer.parseInt(arg.substring(PRODUCT_ID.length())));
				} else if (arg.startsWith(STRATEGY_ID)) {
					strategyId = (Long.parseLong(arg.substring(STRATEGY_ID.length())));
				} else if (arg.startsWith(QUEUE_ID)) {
					queueID = (Long.parseLong(arg.substring(QUEUE_ID.length())));
				} else if (arg.startsWith(CONSIDER_ONLY_SCENARIO)) {
					if (Integer.parseInt(arg.substring(CONSIDER_ONLY_SCENARIO.length())) == 0) {
						runOnlytempStrat = false;
					} else
						runOnlytempStrat = true;
				}
			}
		}
		strategyWhatIfCallservice.setupStandardConnection();
		logger.info("StrategyWhatIfCallservice()-" + "ProductID:" + productId + "location: " + locationId
				+ " strategyId:" + strategyId + "queueID:" + queueID + "runOnlytempStrat:" + runOnlytempStrat);
		strategyWhatIfCallservice.callReccService(locationLevelId, locationId, productLevelId, productId, userID,
				strategyId, queueID, runOnlytempStrat);

	}

	/**
	 * Initializes connection
	 * 
	 * @throws GeneralException
	 */
	protected void setupStandardConnection() throws GeneralException {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	private void callReccService(int locationLevelId, int locationId, int productLevelId, int productId,
			String predictedUserId, Long strategyId, long queueId, boolean runOnlytempStrat) throws GeneralException {

		RecommendationInputDTO recommendationInputDTO = new RecommendationInputDTO();
		recommendationInputDTO.setLocationLevelId(locationLevelId);
		recommendationInputDTO.setLocationId(locationId);
		recommendationInputDTO.setProductLevelId(productLevelId);
		recommendationInputDTO.setProductId(productId);
		recommendationInputDTO.setRecType(PRConstants.MW_WEEK_RECOMMENDATION);
		recommendationInputDTO.setRunMode(PRConstants.RUN_TYPE_BATCH);
		recommendationInputDTO.setRunType(PRConstants.RUN_TYPE_TEMP);
		recommendationInputDTO.setNoOfWeeksInAdvance(1);
		recommendationInputDTO.setUserId(predictedUserId);
		recommendationInputDTO.setStrategyId(strategyId);
		recommendationInputDTO.setQueueId(queueId);
		recommendationInputDTO.setRunOnlyTempStrats(runOnlytempStrat);
		RecommendationFlow recommendationFlow = new RecommendationFlow();
		recommendationFlow.multiWeekRecommendation(conn, recommendationInputDTO);
	}

}
