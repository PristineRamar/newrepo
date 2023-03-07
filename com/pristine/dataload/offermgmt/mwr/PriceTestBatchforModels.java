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
import com.pristine.lookup.offermgmt.PriceTestStatusLookUp;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

public class PriceTestBatchforModels {
	private static Logger logger = Logger.getLogger("InitiateModelBuildingForPriceTest");

	// Connection variables
	private static Connection conn = null;
	PriceTestDAO priceTestDAO = new PriceTestDAO();
	SimpleDateFormat sdf = new SimpleDateFormat(Constants.APP_DATE_FORMAT);

	public static void main(String[] args) throws GeneralException, ParseException, IOException, SQLException {

		PropertyManager.initialize("recommendation.properties");
		PropertyConfigurator.configure("log4j-ModelBuildingPriceTest.properties");
		PriceTestBatchforModels initiateIMS = new PriceTestBatchforModels();

		logger.info("*****************************************");

		try {
			initiateIMS.initiateModelBuilding();
		} catch (Exception e) {
			logger.error("exception in PriceTestBatchforModels");
		} finally {
			conn.close();
		}

		logger.info("*****************************************");

	}

	private void initiateModelBuilding() throws GeneralException, ParseException, IOException {
		Initialize();
		logger.debug("initiateModelBuilding()- Getting active PriceTest request for model building started....");
		List<PriceTestDTO> priceTestModels = priceTestDAO.getPriceTestStatusForModelBuilding(getConn());
		logger.info("initiateModelBuilding()-  #active PriceTest request for model building "+ priceTestModels.size());

		String modelsPath = PropertyManager.getProperty("MODELS_BATCH_PATH");
		int maxBatchPossible = Integer.parseInt(PropertyManager.getProperty("MAX_PARALLEL_MODELS_POSSIBLE", "5"));
		int runningModelsBatch = priceTestDAO.getCountOfRunningModels(getConn());
		logger.info("initiateModelBuilding()-  #Currently running model building :- " + runningModelsBatch);
		if (priceTestModels != null && priceTestModels.stream().count() > 0) {
			try {
				boolean isBatchTriggerPossible = true;

				if (runningModelsBatch > maxBatchPossible)
					isBatchTriggerPossible = false;
				else
					maxBatchPossible = maxBatchPossible - runningModelsBatch;

				if (isBatchTriggerPossible) {

					// Remove the entries, to trigger based on slot availability
					int count = 0;
					Iterator<PriceTestDTO> it = priceTestModels.iterator();

					while (it.hasNext()) {
						it.next();
						count++;
						if (count > maxBatchPossible) {
							it.remove();
						}
					}

					for (PriceTestDTO item : priceTestModels) {

						String productId = String.valueOf(item.getProductId());
						String locationID = String.valueOf(item.getLocationId());

						logger.info(
								"Running Models For: " + item.getProductId() + " Location: " + item.getLocationId());

						Runtime.getRuntime()
								.exec(new String[] { "cmd.exe", "/c", "start " + modelsPath, locationID, productId });

					}
				}else
				{
					logger.info("Model building queued since currently  "+ maxBatchPossible +" requests running...." );
				}

			} catch (Exception e) {
				logger.error("initiateModelBuilding()-Exception in execution :" + e);
				throw new GeneralException("initiateModelBuilding()-Exception in execution :", e);
			}
		} else {
			logger.info("initiateModelBuilding()-No new  Model building request....");
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
