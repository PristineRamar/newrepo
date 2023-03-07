package com.pristine.dataload.offermgmt.mwr;

import java.io.IOException;
import java.sql.Connection;
import java.util.*;

import com.pristine.dto.offermgmt.mwr.RecommendationQueueDTO;
import com.pristine.dao.offermgmt.mwr.RecommendationQueueDAO;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class RecommendationQueueBatch {

	private static Logger logger = Logger.getLogger("MultiWeekRecommendation");
	
	// Input variables
	private String locationLevelId;
	private String locationId;
	private String productLevelId;
	private String productId;
	private String queueId;
	private String strategyId;
	private String submittedBy;
	private String considerOnlyScenario;
	// Connection variables
	private Connection conn = null;
	
	public static void main(String[] args) {
		
		PropertyManager.initialize("recommendation.properties");
		PropertyConfigurator.configure("log4j-queue-recommendation.properties");
		RecommendationQueueBatch multiWeekRecQueueBatch = new RecommendationQueueBatch();
		
		logger.info("*****************************************");
		
		multiWeekRecQueueBatch.initiateMultiWeekRecommendation();
		
		logger.info("*****************************************");
		
	}

	private void initiateMultiWeekRecommendation() {
		try {
			
			RecommendationQueueDAO recomQueue = new RecommendationQueueDAO();
			
			// Initialize the connection
			Initialize();
			
			try {
				// Delete Queued Data with status - SUBMITTED older than 7 days
				logger.info("Deleting the Older Queued Recommendations");
				recomQueue.deleteOlderQueuedRecommendation(getConnection());
				
				PristineDBUtil.commitTransaction(conn, "Delete Older Queued Recommendation");
			} catch (Exception e) {
				PristineDBUtil.rollbackTransaction(conn, "Delete Older Queued Recommendation");
				logger.error("Exception in Deleting older queue Recommendations" + e);
			}
			
			
			// Get all the Queued Recommendation from table 
			logger.info("Checking for Available Slots");
			int runningRecommendations = recomQueue.getCountOfRunningRecommendations(getConnection());
			
			// Get all the Queued Recommendation from table 
			logger.info("Getting the Queued Recommendations");
			List<RecommendationQueueDTO> queuedRecommendations = recomQueue.getAllQueuedRecommendation(getConnection());
			
			// Loop through the records and trigger the batches
			logger.info("******Triggering the Queued Recommendations******");
			if(queuedRecommendations != null && queuedRecommendations.stream().count() > 0) {
				
				boolean isBatchTriggerPossible = true;
				
				// For AZ - 9, GE - 30, RA - 6
				int maxBatchPossible = Integer.parseInt(PropertyManager.getProperty("MAX_PARALLEL_BATCH_POSSIBLE", "4"));
				
				if(runningRecommendations > maxBatchPossible)
					isBatchTriggerPossible = false;
				else
					maxBatchPossible = maxBatchPossible - runningRecommendations;
				
				if(isBatchTriggerPossible) {
					
					// Remove the Recommendations, to trigger based on slot availability
			        int count = 0; 
			        Iterator<RecommendationQueueDTO> it = queuedRecommendations.iterator(); 
			        
			        while (it.hasNext()) { 
			            it.next(); 
			            count++;  
			            if (count > maxBatchPossible) { 
			                it.remove(); 
			            } 
			        }
					
					try {
						// Update the On Hold flag - Hold the Recommendations
						logger.info("******Updating the On Hold flag for each recommendation******");
						if(queuedRecommendations != null && queuedRecommendations.stream().count() > 0)
							recomQueue.updateOnHoldFlag(getConnection(), "Y", queuedRecommendations);
						
						PristineDBUtil.commitTransaction(conn, "Delete Older Queued Recommendation");
					} catch (Exception e) {
						PristineDBUtil.rollbackTransaction(conn, "Delete Older Queued Recommendation");
						logger.error("Exception in Deleting older queue Recommendations" + e);
						
					}
					
					triggermultiWeekRecommendationBatch(queuedRecommendations);
				}else {
					logger.info("******Batch could not be triggered for Queued Recommendations, as already "+runningRecommendations+" are running!******");
				}		
			}
			
		}catch(GeneralException | Exception e) {
			logger.error("Unable to execute muti week recommendation", e);
		}
	}

	private void triggermultiWeekRecommendationBatch(List<RecommendationQueueDTO> queuedRecommendations) {
		// Trigger MultiWeek recommendation batch
		
		try {
			
			// filter the records to see if it is Recommendation/what-if
			// and trigger the batch accordingly
			
			String queueRecomBatchPath = PropertyManager.getProperty("QUEUE_REC_BAT_PATH");
			String whatIfBatchPath = PropertyManager.getProperty("WHAT_IF_BAT_PATH");
			logger.info("Total Batches to be triggered: " + queuedRecommendations.size());
			for(RecommendationQueueDTO item : queuedRecommendations) {
				
				queueId = String.valueOf(item.getQueueId());
				productId = item.getProductId();
				productLevelId = item.getProductLevelId();
				locationId = item.getLocationId();
				locationLevelId = item.getLocationLevelId();
				submittedBy = item.getSubmittedBy();
				strategyId = String.valueOf(item.getStrategyId());
				considerOnlyScenario = String.valueOf(item.getConsiderScenario());
				if(item.getRecType().equalsIgnoreCase("D")) {
					// To trigger batch for Recommendation
					logger.info("Running recommendation for Product: " + productId + " Location: " + locationId + " Queue Id: "+ queueId);
					Runtime.getRuntime().exec(new String[] {"cmd.exe", "/c", "start " + queueRecomBatchPath, 
							locationLevelId, locationId, productLevelId, productId, queueId, submittedBy});	
				} else if(item.getRecType().equalsIgnoreCase("T")) {
					// To trigger batch for What-If
					// To trigger batch for Recommendation
					logger.info("Running what-if for Product: " + productId + " Location: " + locationId + " Queue Id: "+ queueId);
					Runtime.getRuntime().exec(new String[] {"cmd.exe", "/c", "start " + whatIfBatchPath, 
							locationLevelId, locationId, productLevelId, productId, queueId, submittedBy, strategyId, considerOnlyScenario});
				}
			}
			
		} catch (IOException e) {
			//e.printStackTrace();
			logger.error("Unable to execute muti week recommendation", e);
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
	
	protected Connection getConnection() {
		return conn;
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	public String getLocationLevelId() {
		return locationLevelId;
	}

	public void setLocationLevelId(String locationLevelId) {
		this.locationLevelId = locationLevelId;
	}

	public String getLocationId() {
		return locationId;
	}

	public void setLocationId(String locationId) {
		this.locationId = locationId;
	}

	public String getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(String productLevelId) {
		this.productLevelId = productLevelId;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getQueueId() {
		return queueId;
	}

	public void setQueueId(String queueId) {
		this.queueId = queueId;
	}

	public String getSubmittedBy() {
		return submittedBy;
	}

	public void setSubmittedBy(String submittedBy) {
		this.submittedBy = submittedBy;
	}

}
