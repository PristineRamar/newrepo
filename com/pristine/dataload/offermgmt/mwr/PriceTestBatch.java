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

public class PriceTestBatch {

	private static Logger logger = Logger.getLogger("PriceTestBatch");

	// Connection variables
	private static Connection conn = null;
	PriceTestDAO priceTestDAO = new PriceTestDAO();
	SimpleDateFormat sdf = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	// DirectIMSWeeklyLoader directIMSWeeklyLoader = new DirectIMSWeeklyLoader();

	public static void main(String[] args) throws GeneralException, ParseException, IOException, SQLException {

		PropertyManager.initialize("recommendation.properties");
		PropertyConfigurator.configure("log4j-PriceTest.properties");
		PriceTestBatch priceTestBatch = new PriceTestBatch();

		logger.info("*****************************************");
		try {
			priceTestBatch.initiatePriceTest();
		} catch (Exception e) {
			logger.error("Exception in PriceTestBatch" + e);
		} finally {
			conn.close();
		}

		logger.info("*****************************************");

	}

	private void initiatePriceTest() throws GeneralException, ParseException, IOException {
		Initialize();
		logger.debug("initiatePriceTest()- Getting active PriceTest request started....");
		List<PriceTestDTO> priceTestList = priceTestDAO.getPriceTestRequest(getConn());
		
		logger.info("initiatePriceTest()- #PriceTest requests fetched :- " + priceTestList.size());

		String imsBatchPath = PropertyManager.getProperty("IMS_BAT_PATH");
		int maxBatchPossible = Integer.parseInt(PropertyManager.getProperty("MAX_PARALLEL_IMS_POSSIBLE", "5"));
		int runningIMSBatch = priceTestDAO.getCountOfRunningIMS(getConn());

		logger.info("initiatePriceTest()-currently # runningIMSBatch:  " + runningIMSBatch);

		if (priceTestList != null && priceTestList.stream().count() > 0) {
			try {
				boolean isBatchTriggerPossible = true;

				if (runningIMSBatch > maxBatchPossible)
					isBatchTriggerPossible = false;
				else
					maxBatchPossible = maxBatchPossible - runningIMSBatch;

				if (isBatchTriggerPossible) {

					// Remove the entries, to trigger based on slot availability
					int count = 0;
					Iterator<PriceTestDTO> it = priceTestList.iterator();

					while (it.hasNext()) {
						it.next();
						count++;
						if (count > maxBatchPossible) {
							it.remove();
						}
					}
				
					for (PriceTestDTO item : priceTestList) {

						String productId = String.valueOf(item.getProductId());
						String productLevelId = String.valueOf(item.getProductLevelID());

						logger.info("Running IMS For: " + item.getProductId() + " Location: " + item.getLocationId()
								+ " PriceTest Id: " + item.getPriceTestID());
						Runtime.getRuntime().exec(new String[] { "cmd.exe", "/c", "start " + imsBatchPath,
								item.getZoneNum(), productId, productLevelId });
					}
				}
				else {
					logger.info("Request is Queued as :  " + maxBatchPossible + "are running......");
				}

			} catch (Exception e) {
				logger.info("initiatePriceTest()-Exception in execution :" + e);
				throw new GeneralException("initiatePriceTest()-Exception in execution :", e);
			}
		} else {
			logger.info("initiatePriceTest()-No new priceTest request....");
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
