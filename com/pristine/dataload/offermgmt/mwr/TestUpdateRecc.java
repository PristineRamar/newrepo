package com.pristine.dataload.offermgmt.mwr;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.mwr.core.MWRUpdateRecommendationService;
import com.pristine.util.PropertyManager;

public class TestUpdateRecc {

	private Connection conn = null;
	private static Logger logger = Logger.getLogger("TestUpdateRecc");
	private static final String RUN_ID = "RUN_ID=";
	private static final String USER_ID = "USER_ID=";

	public static void main(String[] args) throws GeneralException {
		PropertyManager.initialize("recommendation.properties");
		PropertyConfigurator.configure("log4j-UpdateRecommendation.properties");
		TestUpdateRecc updateRecService = new TestUpdateRecc();
		long run_id = 0;
		String userId = "BATCH";

		if (args.length > 0) {
			for (String arg : args) {
				if (arg.startsWith(RUN_ID)) {
					run_id = Long.parseLong(arg.substring(RUN_ID.length()));
				}
				if (arg.startsWith(USER_ID)) {
					userId = arg.substring(USER_ID.length());
				}
			}
		}

		if (run_id > 0)
			updateRecService.updateRecommendation(run_id, userId);
		else {
			logger.error(
					"USAGE: java com.pristine.dataload.offermgmt.mwr.UpdateRecommenationService RUN_ID=123 USER_ID=BATCH");
			System.exit(1);
		}
	}

	/**
	 * This method will call update recommendation for given run Id and user Id
	 * @param runId
	 * @param userId
	 * @throws GeneralException
	 */
	public void updateRecommendation(long runId, String userId) throws GeneralException {

		logger.info("updateRecommendation()- Starting Update recommendation for runId :  " + runId);
		setConnection();
		List<Long> runIds = new ArrayList<Long>();
		runIds.add(runId);
		try {
			new MWRUpdateRecommendationService().triggerUpdateRecommendation(conn, runIds, userId);
			logger.info("updateRecommendation()-  Update recommendation Completed for runId :  " + runId);
		} catch (Exception e) {
			logger.error(
					"updateRecommendation()-Error in updateRecommendation: for runId:" + runId + " user Id: " + userId,
					e);

		}

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

}
