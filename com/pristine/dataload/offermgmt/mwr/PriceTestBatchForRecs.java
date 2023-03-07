package com.pristine.dataload.offermgmt.mwr;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.PriceTestDAO;
import com.pristine.dto.PriceTestDTO;

import com.pristine.exception.GeneralException;

import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

public class PriceTestBatchForRecs {
	private static Logger logger = Logger.getLogger("PriceTestBatchForRecs");

	// Connection variables
	private static Connection conn = null;
	PriceTestDAO priceTestDAO = new PriceTestDAO();
	SimpleDateFormat sdf = new SimpleDateFormat(Constants.APP_DATE_FORMAT);

	public static void main(String[] args) throws GeneralException, ParseException, IOException, SQLException {

		PropertyManager.initialize("recommendation.properties");
		PropertyConfigurator.configure("log4j-ReccomendationsPriceTest.properties");
		PriceTestBatchForRecs priceTestBatchForRecs = new PriceTestBatchForRecs();

		logger.info("*****************************************");

		try {
			priceTestBatchForRecs.initiateRecommendations();
		} catch (Exception e) {
			logger.error("Exception in PriceTestBatch" + e);
		} finally {
			conn.close();
		}

		logger.info("*****************************************");

	}

	private void initiateRecommendations() throws GeneralException, ParseException, IOException {
		Initialize();
		logger.debug("initiateRecommendations()- Getting active PriceTest request for ....");
		List<PriceTestDTO> priceTestReccs = priceTestDAO.getPriceTestStatusForRecommendation(getConn());
		logger.info(
				"initiateRecommendations()- # active PriceTest request for recomendations : " + priceTestReccs.size());

		String reccPath = PropertyManager.getProperty("RECC_BATCH_PATH");
		int maxBatchPossible = Integer.parseInt(PropertyManager.getProperty("MAX_PARALLEL_RECCS_POSSIBLE", "5"));
		int runningReccs = priceTestDAO.getCountOfRunningrecommendations(getConn());
		logger.info("initiateRecommendations()- # running Reccs for PriceTest request for recomendations : "
				+ runningReccs);
		if (priceTestReccs != null && priceTestReccs.stream().count() > 0) {
			try {
				boolean isBatchTriggerPossible = true;

				if (runningReccs > maxBatchPossible)
					isBatchTriggerPossible = false;
				else
					maxBatchPossible = maxBatchPossible - runningReccs;

				if (isBatchTriggerPossible) {

					// Remove the entries, to trigger based on slot availability
					int count = 0;
					Iterator<PriceTestDTO> it = priceTestReccs.iterator();

					while (it.hasNext()) {
						it.next();
						count++;
						if (count > maxBatchPossible) {
							it.remove();
						}
					}

					for (PriceTestDTO item : priceTestReccs) {

						String productId = String.valueOf(item.getProductId());
						String locationID = String.valueOf(item.getLocationId());
						String locationLevelID = String.valueOf(item.getLocationLevelID());
						String productLevelId = String.valueOf(item.getProductLevelID());

						logger.info("Running Reccs For: " + item.getProductId() + " Location: " + item.getLocationId());

						Runtime.getRuntime().exec(new String[] { "cmd.exe", "/c", "start " + reccPath, locationLevelID,
								locationID, productLevelId, productId, item.getRequestorName() });

					}
				} else {
					logger.info("initiateRecommendations()-Recommendations queued since currently" + maxBatchPossible
							+ "reccs running...");
					
				}
			} catch (Exception e) {
				logger.info("initiateRecommendations()-Exception in execution :" + e);
				throw new GeneralException("initiateRecommendations()-Exception in execution :", e);
			}
		} else {
			logger.info("initiateRecommendations()-No new recommendation requests...");
		}
	}

	private void Initialize() throws GeneralException {

		// Standard Connection
		setupStandardConnection();
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

	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

}
