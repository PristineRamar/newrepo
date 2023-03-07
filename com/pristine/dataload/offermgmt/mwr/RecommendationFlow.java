package com.pristine.dataload.offermgmt.mwr;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import com.pristine.service.offermgmt.AuditTrailService;
import com.pristine.service.offermgmt.NotificationService;
import com.pristine.service.offermgmt.multiWeekPrediction.CLPDLPPredictionService;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.pristine.dao.PriceTestDAO;
import com.pristine.dao.offermgmt.mwr.MWRRunHeaderDAO;
import com.pristine.dao.offermgmt.mwr.WeeklyRecDAO;
import com.pristine.dataload.offermgmt.AuditEngineWS;
import com.pristine.dataload.offermgmt.mwr.summary.MultiWeekRecSummary;
import com.pristine.dto.offermgmt.NotificationDetailInputDTO;
import com.pristine.dto.offermgmt.mwr.MWRRunHeader;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.AuditTrailStatusLookup;
import com.pristine.lookup.AuditTrailTypeLookup;
import com.pristine.lookup.offermgmt.PriceTestStatusLookUp;
import com.pristine.service.offermgmt.mwr.basedata.BaseDataService;
import com.pristine.service.offermgmt.mwr.core.CoreRecommendationService;
import com.pristine.service.offermgmt.mwr.itemattributes.ItemAttributeService;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

/**
 * Class defintion for entire multi week recommendation flow
 * 
 * @author Pradeepkumar
 * @version 1.0
 */

public class RecommendationFlow {

	private static Logger logger = Logger.getLogger("RecommendationFlow");
	
	/**
	 * Covers entire multi week recommendation flow
	 * @throws GeneralException 
	 */
	public void multiWeekRecommendation(Connection conn, 
			RecommendationInputDTO recommendationInputDTO) throws GeneralException {
		try {
			logger.info("multiWeekRecommendation() - Recommendation starts");
			long startTime = System.currentTimeMillis();
			CommonDataHelper commonDataHelper = new CommonDataHelper();
			MWRRunHeaderDAO mwrRunHeaderDAO = new MWRRunHeaderDAO();
			PriceTestDAO priceTestDAO = new PriceTestDAO();

			// Added for the PriceTest enhancement for AZ to identigy if the given zone is Test Zone
			commonDataHelper.setisPriceTestZone(conn, recommendationInputDTO);

			String reccStatus = mwrRunHeaderDAO.getReccomendationStatus(conn, recommendationInputDTO);
			
			
			if (reccStatus.equalsIgnoreCase("Y") || recommendationInputDTO.getQueueId() != 0
					|| recommendationInputDTO.getRunMode() == PRConstants.RUN_TYPE_ONLINE) {
				RecommendationWeek recommendationWeek = new RecommendationWeek(commonDataHelper, conn);

				// 1. Initialize common data which is going to be used in entire recommendation
				commonDataHelper.getCommonData(conn, recommendationInputDTO);

					// 2. Set recommendation weeks and corresponding start and end calendar ids
					recommendationWeek.setupRecommendationWeeks(recommendationInputDTO);

					// 3. log recommendation weeks detail
					recommendationWeek.logRecWeeksDetail(recommendationInputDTO);

					// 4. Insert run header
					mwrRunHeaderDAO.insertMultiWeekRecRunHeader(conn, recommendationInputDTO);

				// To update the status in Price Test
				if (recommendationInputDTO.isPriceTestZone()
						&& recommendationInputDTO.getRunMode() != PRConstants.RUN_TYPE_ONLINE
						&& recommendationInputDTO.getRunType() != PRConstants.RUN_TYPE_TEMP) {
					priceTestDAO.updatePriceTestStatus(conn,
							PriceTestStatusLookUp.RECOMMENDATION_IN_PROGRESS.getPriceTestTypeLookupId(),
							recommendationInputDTO.getLocationId(), Constants.ZONE_LEVEL_ID,
							recommendationInputDTO.getProductId(), recommendationInputDTO.getProductLevelId());

				}

					// Added for updating the rundId and queue status in recommendation que table
					if (recommendationInputDTO.getQueueId() != 0) {
						try {
							mwrRunHeaderDAO.updateRecQueueHeader(conn, recommendationInputDTO);
							PristineDBUtil.commitTransaction(conn, "commiting Update queue Status");
						} catch (Exception e) {
							PristineDBUtil.rollbackTransaction(conn, "Error commiting Update queue Status");
						}

					}

					if (recommendationInputDTO.getRunMode() == PRConstants.RUN_TYPE_ONLINE) {
						Runnable task = new Runnable() {
							@Override
							public void run() {
								try {
									executePriceRecommendation(conn, recommendationInputDTO, commonDataHelper,
											mwrRunHeaderDAO);
									logger.info("multiWeekRecommendation() - Recommendation ends. Run Id: "
											+ recommendationInputDTO.getRunId());
									long endTime = System.currentTimeMillis();

									logger.info(
											"multiWeekRecommendation() - Total time taken to complete recommendation: "
													+ PrestoUtil.getTimeTakenInMins(startTime, endTime));

								} catch (GeneralException e) {
									logger.error(
											"executeMultiWeekRecommendation() - Unable to execute multi week recommendation",
											e);
								}
							}
						};
						new Thread(task, "ServiceThread").start();
					} else {
						executePriceRecommendation(conn, recommendationInputDTO, commonDataHelper, mwrRunHeaderDAO);
						logger.info("multiWeekRecommendation() - Recommendation ends. Run Id: "
								+ recommendationInputDTO.getRunId());
						long endTime = System.currentTimeMillis();
						logger.info("multiWeekRecommendation() - Total time taken to complete recommendation: "
								+ PrestoUtil.getTimeTakenInMins(startTime, endTime));
					}

				} else {
					
				if (recommendationInputDTO.isPriceTestZone()) {
					logger.info("RecommendationFlow()-Recommendation is not done. Run Type: "
							+ recommendationInputDTO.getRunMode() + " .price Test is ended for " + "Product Level Id: "
							+ recommendationInputDTO.getProductLevelId() + ",Product Id: "
							+ recommendationInputDTO.getProductId() + ",Location Level Id: "
							+ recommendationInputDTO.getLocationLevelId() + ",Location Id:"
							+ recommendationInputDTO.getLocationId());
				} else

					logger.info("RecommendationFlow()-Recommendation is not done. Run Type: "
							+ recommendationInputDTO.getRunMode() + " .Automatic Recommendation is disabled for "
							+ "Product Level Id: " + recommendationInputDTO.getProductLevelId() + ",Product Id: "
							+ recommendationInputDTO.getProductId() + ",Location Level Id: "
							+ recommendationInputDTO.getLocationLevelId() + ",Location Id:"
							+ recommendationInputDTO.getLocationId());

			}
		
		}catch (GeneralException e) {
			logger.error("executeMultiWeekRecommendation() - Unable to execute multi week recommendation", e);
		}
	}
	
	private void callAudit(Connection conn, RecommendationInputDTO recommendationRunHeader) throws GeneralException {
		//Calling Audit process after pricing recommendation is successful
		AuditEngineWS auditEngine = new AuditEngineWS();
		auditEngine.isIntegratedProcess = true;
		logger.info("callAudit()-calling audit");
		long reportId = auditEngine.getAuditHeader(conn, recommendationRunHeader.getLocationLevelId(), recommendationRunHeader.getLocationId(),
				recommendationRunHeader.getProductLevelId(), recommendationRunHeader.getProductId(), PRConstants.BATCH_USER,recommendationRunHeader.getStartWeek());
		logger.info("callAudit()-Audit is completed for report id - " + reportId);
	}
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param commonDataHelper
	 * @param mwrRunHeaderDAO
	 * @throws GeneralException
	 */
	public void executePriceRecommendation(Connection conn, 
			RecommendationInputDTO recommendationInputDTO, 
			CommonDataHelper commonDataHelper, 
			MWRRunHeaderDAO mwrRunHeaderDAO) throws GeneralException {
		int completePct = 10;
		BaseData baseData = new BaseData();
		BaseDataService baseDataService = new BaseDataService();
		ItemAttributeService itemAttributeService = new ItemAttributeService();
		CoreRecommendationService coreRecommendationService = new CoreRecommendationService();
		MultiWeekRecSummary multiWeekRecSummary = new MultiWeekRecSummary();
		CLPDLPPredictionService clpdlpPredictionService = new CLPDLPPredictionService();
		try {
			updateRecommendationStatus(conn, recommendationInputDTO, "Fetching base data", completePct,
					mwrRunHeaderDAO);

			// 5. Setup base data using BaseDataService
			baseDataService.setupBaseData(conn, recommendationInputDTO, baseData, commonDataHelper);

			completePct = 30;
			updateRecommendationStatus(conn, recommendationInputDTO, "Setting up item attributes", completePct,
					mwrRunHeaderDAO);

			
			// 6. Set item level attributes
			/** Added connection for PROM-2223 **/
			itemAttributeService.setupItemAttributes(baseData, commonDataHelper, recommendationInputDTO, conn);
			baseData.cleanupCacheSet1();
			/** PROM-2223 End **/
			
			completePct = 40;
			updateRecommendationStatus(conn, recommendationInputDTO, "Recommending prices", completePct,
					mwrRunHeaderDAO);

			// 7. Core recommendation
			coreRecommendationService.recommendPrice(conn, baseData, commonDataHelper, recommendationInputDTO);
			baseData.cleanupCacheSet2();

			completePct = 70;
			updateRecommendationStatus(conn, recommendationInputDTO, "Saving recommendations", completePct,
					mwrRunHeaderDAO);

			// 9. Save recommendation
			saveRecommendation(conn, recommendationInputDTO, baseData,
					mwrRunHeaderDAO);

			if (Boolean.parseBoolean(PropertyManager.getProperty("CLP_DLP_MODEL_ENABLED", "FALSE"))) {
				completePct = 80;
				updateRecommendationStatus(conn, recommendationInputDTO, "Predicting at category level", completePct,
						mwrRunHeaderDAO);
				clpdlpPredictionService.getCLPDLPPredictions(commonDataHelper, baseData, recommendationInputDTO);
			}

			completePct = 90;
			updateRecommendationStatus(conn, recommendationInputDTO, "Updating Summary", completePct,
					mwrRunHeaderDAO);

			// 10. Update quarter level summary
			multiWeekRecSummary.calculateSummary(conn, baseData, recommendationInputDTO);

			if (!recommendationInputDTO.getRecType().equals(PRConstants.MW_WEEK_RECOMMENDATION)) {
				
				completePct = 95;
				updateRecommendationStatus(conn, recommendationInputDTO, "Auditing Recommnedation", completePct,
						mwrRunHeaderDAO);
				
				callAudit(conn,recommendationInputDTO);
				
				recommendationInputDTO.getMwrRunHeader().setRunStatus(PRConstants.RUN_STATUS_SUCCESS);
				
				completePct = 100;
				updateRecommendationStatus(conn, recommendationInputDTO, "Recommendation successful", completePct,
						mwrRunHeaderDAO);
			
				mwrRunHeaderDAO.updateEndTimeInHeader(conn, recommendationInputDTO.getRunId());
				
				mwrRunHeaderDAO.insertRecommendationStatus(conn, recommendationInputDTO.getMwrRunHeader(),
						recommendationInputDTO);
				
				
				// 12. Update dashboardData
				new QuarterlyDashboard().populateDashboardData(conn, recommendationInputDTO.getLocationLevelId(),
						recommendationInputDTO.getLocationId(), recommendationInputDTO.getProductLevelId(),
						recommendationInputDTO.getProductId());
				
				
				// update NotificationAnalysis using runID
				logger.info("Notification Analysis is Started...");
				addNotifications(conn, recommendationInputDTO.getRunId(), PRConstants.REC_COMPLETED, recommendationInputDTO);
				logger.info("Notification Analysis is Completed...");

				// call audit trail
				callAuditTrail(conn, recommendationInputDTO.getLocationLevelId(), recommendationInputDTO.getLocationId(),
						recommendationInputDTO.getProductLevelId(), recommendationInputDTO.getProductId(),
						AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(), PRConstants.REC_COMPLETED,
						recommendationInputDTO.getRunId(), recommendationInputDTO.getUserId(),AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),AuditTrailStatusLookup.AUDIT_SUB_TYPE_RECC.getAuditTrailTypeId(),AuditTrailStatusLookup.SUB_STATUS_TYPE_RECC.getAuditTrailTypeId());

			} else {
				recommendationInputDTO.getMwrRunHeader().setRunStatus(PRConstants.RUN_STATUS_SUCCESS);
				
				completePct = 100;
				updateRecommendationStatus(conn, recommendationInputDTO, "Recommendation successful", completePct,
						mwrRunHeaderDAO);
			
				mwrRunHeaderDAO.updateEndTimeInHeader(conn, recommendationInputDTO.getRunId());
				
				mwrRunHeaderDAO.insertRecommendationStatus(conn, recommendationInputDTO.getMwrRunHeader(),
						recommendationInputDTO);

				//Added for updating the status in recommendation que table 
				logger.debug("queueId :" + recommendationInputDTO.getQueueId() +" runType: " + recommendationInputDTO.getRunType() +" User: "+ recommendationInputDTO.getUserId());
				
				if (recommendationInputDTO.getQueueId() > 0
						&& recommendationInputDTO.getRunType() == PRConstants.RUN_TYPE_TEMP) {
					try {
					mwrRunHeaderDAO.insertScenario(conn, recommendationInputDTO, recommendationInputDTO.getQueueId());
					mwrRunHeaderDAO.updateRecHeaderUser(conn,recommendationInputDTO);
					mwrRunHeaderDAO.updateRecStatusUser(conn,recommendationInputDTO);
					
					PristineDBUtil.commitTransaction(conn, "commiting Update whatIf Status");
					} catch(Exception ex)
					{
						PristineDBUtil.rollbackTransaction(conn, " Error commiting Update whatIf Status");
						logger.error("Error in updating WhatIf "+ ex);
					}
				} else if (recommendationInputDTO.getQueueId() == 0
						&& recommendationInputDTO.getRunType() == PRConstants.RUN_TYPE_TEMP) {
					mwrRunHeaderDAO.updateScenario(conn, recommendationInputDTO.getRunId(),
							PRConstants.WHAT_IF_SUCCESS);
				}
			}
			
			//Temporary_added
			if (recommendationInputDTO.isPriceTestZone()
					&& recommendationInputDTO.getRunMode() != PRConstants.RUN_TYPE_ONLINE
					&& recommendationInputDTO.getRunType() != PRConstants.RUN_TYPE_TEMP) {
				PriceTestDAO priceTestDAO = new PriceTestDAO();
				priceTestDAO.updatePriceTestStatus(conn,
						PriceTestStatusLookUp.RECOMMENDATION_IN_COMPLETED.getPriceTestTypeLookupId(),
						recommendationInputDTO.getLocationId(), Constants.ZONE_LEVEL_ID,
						recommendationInputDTO.getProductId(), recommendationInputDTO.getProductLevelId());
				PristineDBUtil.commitTransaction(conn, "status update");
			}
			
		} catch (GeneralException | OfferManagementException | Exception e) {
			recommendationInputDTO.getMwrRunHeader().setRunStatus(PRConstants.RUN_STATUS_ERROR);
			
			updateRecommendationStatus(conn, recommendationInputDTO, "Error in recommendation", completePct,
					mwrRunHeaderDAO);
			
			// call audit trail
			callAuditTrail(conn, recommendationInputDTO.getLocationLevelId(), recommendationInputDTO.getLocationId(),
					recommendationInputDTO.getProductLevelId(), recommendationInputDTO.getProductId(),
					AuditTrailTypeLookup.RECOMMENDATION.getAuditTrailTypeId(), PRConstants.ERROR_REC,
					recommendationInputDTO.getRunId(), recommendationInputDTO.getUserId(),
					AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
					AuditTrailStatusLookup.AUDIT_SUB_TYPE_ERROR.getAuditTrailTypeId(),
					AuditTrailStatusLookup.SUB_STATUS_TYPE_ERROR.getAuditTrailTypeId());
			
			if (recommendationInputDTO.isPriceTestZone()
					&& recommendationInputDTO.getRunMode() != PRConstants.RUN_TYPE_ONLINE
					&& recommendationInputDTO.getRunType() != PRConstants.RUN_TYPE_TEMP) {
				PriceTestDAO priceTestDAO = new PriceTestDAO();
				priceTestDAO.updatePriceTestStatus(conn,
						PriceTestStatusLookUp.ERROR_IN_RECOMMENDATION.getPriceTestTypeLookupId(),
						recommendationInputDTO.getLocationId(), Constants.ZONE_LEVEL_ID,
						recommendationInputDTO.getProductId(), recommendationInputDTO.getProductLevelId());
			}
				
			if (recommendationInputDTO.getQueueId() == 0
					&& recommendationInputDTO.getRunType() == PRConstants.RUN_TYPE_TEMP) {
				mwrRunHeaderDAO.updateScenario(conn, recommendationInputDTO.getRunId(), PRConstants.WHAT_IF_ERROR);
			}

			throw new GeneralException("executePriceRecommendation() - Error recommending prices", e);
		} finally {
			baseData = null;
			PristineDBUtil.close(conn);
		}
	}
	
	/**
	 * Adds notificaiton for current recommendation
	 * 
	 * @param runId
	 * @param notificationTypeId
	 * @throws SQLException
	 * @throws GeneralException
	 */
	private void addNotifications(Connection conn, long runId, Integer notificationTypeId, 
			RecommendationInputDTO recommendationInputDTO) throws SQLException, GeneralException {
		NotificationService notificationService = new NotificationService();
		List<NotificationDetailInputDTO> notificationDetailDTOs = new ArrayList<NotificationDetailInputDTO>();
		NotificationDetailInputDTO notificationDetailInputDTO = new NotificationDetailInputDTO();
		notificationDetailInputDTO.setModuleId(PRConstants.REC_MODULE_ID);
		notificationDetailInputDTO.setNotificationTypeId(notificationTypeId);
		notificationDetailInputDTO.setNotificationKey1(runId);
		notificationDetailDTOs.add(notificationDetailInputDTO);
		boolean sendEmail = recommendationInputDTO.getRunMode() == PRConstants.RUN_TYPE_ONLINE;
		notificationService.addNotificationsBatch(conn, notificationDetailDTOs, sendEmail);
	}

	/**
	 * Adds details for audit trail
	 * 
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param auditTrailTypeId
	 * @param recStatusId
	 * @param runId
	 * @param userId
	 * @param auditSubStatus 
	 * @param auditSubType 
	 * @param auditType 
	 * @throws GeneralException
	 */
	private void callAuditTrail(Connection conn, int locationLevelId, int locationId, int productLevelId, int productId,
			int auditTrailTypeId, int recStatusId, long runId, String userId, int auditType, int auditSubType, int auditSubStatus) throws GeneralException {
		AuditTrailService auditTrailService = new AuditTrailService();
		auditTrailService.auditRecommendation(conn, locationLevelId, locationId, productLevelId, productId,
				auditTrailTypeId, recStatusId, runId, userId,auditType,auditSubType,auditSubStatus,0,0,0);
	}
	
	/**
	 * 
	 * @throws GeneralException
	 * @throws JsonProcessingException
	 */
	private void saveRecommendation(Connection conn, RecommendationInputDTO recommendationInputDTO, 
			BaseData baseData, MWRRunHeaderDAO mwrRunHeaderDAO) throws GeneralException, JsonProcessingException{
		
		// 1. Create weekly run headers
		mwrRunHeaderDAO.insertWeeklyRunHeader(conn, recommendationInputDTO,
				CommonDataHelper.getRecWeekSet(baseData.getWeeklyItemDataMap()));
		
		WeeklyRecDAO weeklyRecDAO = new WeeklyRecDAO();
		// 2. Save recommendation
		weeklyRecDAO.saveRecommendationDetails(conn, recommendationInputDTO, baseData.getWeeklyItemDataMap(),
				Constants.CALENDAR_WEEK);
	}
	
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param message
	 * @param percentCompleted
	 * @throws GeneralException 
	 */
	private void updateRecommendationStatus(Connection conn, RecommendationInputDTO recommendationInputDTO,
			String message, int percentCompleted, MWRRunHeaderDAO mwrRunHeaderDAO) throws GeneralException {

		try {
			MWRRunHeader mwrRunHeader = recommendationInputDTO.getMwrRunHeader();
			mwrRunHeader.setMessage(message);
			mwrRunHeader.setPercentCompleted(percentCompleted);

			mwrRunHeaderDAO.updateMWRRunHeader(conn, mwrRunHeader);
			//Commented this line,commit for the status should happen for all types of recc in the header table
			//This is done to track the status of reccs running from batch 
			// if (recommendationInputDTO.getQueueId() > 0) {
			PristineDBUtil.commitTransaction(conn, "Update recc status");

		} catch (Exception e) {
			logger.error("updateRecommendationStatus error " + e);
			PristineDBUtil.rollbackTransaction(conn, "Error in UpdateRecommendation status");
		}

	}
}
