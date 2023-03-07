package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.DashboardDAO;
//import com.pristine.dao.offermgmt.DashboardDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.StrategyDAO;
//import com.pristine.dataload.offermgmt.Dashboard;
//import com.pristine.dao.offermgmt.prediction.PredictionDAO;
//import com.pristine.dataload.offermgmt.PricingEngineWS;
//import com.pristine.dto.ItemDTO;
import com.pristine.dto.ProductDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRGuidelineMargin;
import com.pristine.dto.offermgmt.PRGuidelinePI;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
//import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
//import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.RunStatusDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
//import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

//import net.sf.ehcache.CacheOperationOutcomes.GetAllOutcome;

import org.apache.log4j.Logger;

public class PriceAdjustment {
	private static Logger logger = Logger.getLogger("PriceAdjustment");
	private Connection conn = null;
	private Context initContext;
	private Context envContext;
	private DataSource ds;
	private final String ADJUSTMENT_TYPE_PI = "PI";
	private final String ADJUSTMENT_TYPE_MAR_RATE = "MARGIN_RATE";
	
	/***
	 * Change the recommend price of items to balance PI and Margin towards goal.
	 * This is called from UI
	 * @param inpRunIds
	 * @return
	 */
	public List<RunStatusDTO> balancePIAndMargin(String inpRunIds) {
		List<RunStatusDTO> runStatusDTOs = new ArrayList<RunStatusDTO>();
		ObjectMapper mapper = new ObjectMapper();
		
		if (this.conn == null) {
			initializeForWS();
			this.conn = getConnection();
		}
		
		try{
			List<PRRecommendationRunHeader> runHeaders = new ArrayList<PRRecommendationRunHeader>();
			List<Long> runIds =  mapper.readValue(inpRunIds, new TypeReference<List<Long>>(){});
			for (Long runId : runIds) {
				PRRecommendationRunHeader runHeader = new PRRecommendationRunHeader();
				runHeader.setRunId(runId);
				runHeaders.add(runHeader);
			}
			runStatusDTOs = adjustPriceToBalancePIAndMargin(conn, runHeaders);
			PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
		}catch(Exception | GeneralException | OfferManagementException ex){
			//Update all run id with error status
			logger.error("Exception while adjusting the Price to balance PI and Margin" + ex.toString(), ex);
			PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
			for (RunStatusDTO runStatusDTO : runStatusDTOs) {
				updateRunStatusWithException(runStatusDTO, "Error while Adjusting the Price");		
			}
		}finally {
			PristineDBUtil.close(conn);					 
		}
		return runStatusDTOs;
	}
	
	/****
	 * Change the recommend price of items to balance PI and Margin towards goal.
	 * This is called from Recommendation Program
	 * @param conn
	 * @param runIds
	 * @return
	 * @throws OfferManagementException 
	 */
	public List<RunStatusDTO> balancePIAndMargin(Connection conn, Long runId, char runType) throws OfferManagementException {
		//Don't update dashboard data when strategy what-if is done
		List<PRRecommendationRunHeader> runHeaders = new ArrayList<PRRecommendationRunHeader>();
		PRRecommendationRunHeader runHeader = new PRRecommendationRunHeader();
		runHeader.setRunId(runId);
		runHeader.setRunType(String.valueOf(runType));
		runHeaders.add(runHeader);
		List<RunStatusDTO> runStatusDTO = adjustPriceToBalancePIAndMargin(conn, runHeaders);
		return runStatusDTO;
	}
	
	private List<RunStatusDTO> adjustPriceToBalancePIAndMargin(Connection conn, List<PRRecommendationRunHeader> runHeaders)
			throws OfferManagementException {
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		HashMap<Long, Integer> runAndItsStoreRecCnt = new HashMap<Long, Integer>();
		HashMap<Long, PRStrategyDTO> runAndItsCategoryLevelStrategy = new HashMap<Long, PRStrategyDTO>();
//		HashMap<Long, Long> runAndItsCategoryLevelStrategyId = new HashMap<Long, Long>();
		HashMap<Long, PRStrategyDTO> strategyMap = new HashMap<Long, PRStrategyDTO>();
		HashMap<Long, List<PRItemDTO>> runAndItsItem = new HashMap<Long, List<PRItemDTO>>();
		List<PRItemDTO> priceAdjustedItemsOfAllRuns = new ArrayList<PRItemDTO>();
		HashMap<Long, List<PRItemDTO>> runAndItsAlreadyAdjustedItem = new HashMap<Long, List<PRItemDTO>>();
		HashMap<Long, List<PRItemDTO>> runAndItsPriceAdjustedItem  = new HashMap<Long, List<PRItemDTO>>();
		HashMap<Long, PRRecommendationRunHeader> runAndItsRecommendationRunHeader = new HashMap<Long, PRRecommendationRunHeader>();
		List<RunStatusDTO> runStatusDTOs = new ArrayList<RunStatusDTO>();
		PricingEngineService engineService = new PricingEngineService();
		try {
			RetailPriceZoneDAO rpzDAO = new RetailPriceZoneDAO();
			DashboardDAO dashboardDAO = new DashboardDAO(conn);
			String chainId = new RetailPriceDAO().getChainId(conn);
			
			List<Long> runIds = new ArrayList<Long>();
			for (PRRecommendationRunHeader runHeader : runHeaders) {
				runIds.add(runHeader.getRunId());
			}
			
			runStatusDTOs = updateRunStatusWithRunId(runIds);
			
			String costChangeLogic = PropertyManager.getProperty("PR_COST_CHANGE_BEHAVIOR", "");
			
			// Get Store Recommendation count of each run's
			runAndItsStoreRecCnt = getStoreRecommendationCount(conn, runIds);

			// Get all items of each run's
			runAndItsItem = pricingEngineDAO.getRecItemsOfRunIds(conn, runIds);

			// Get Category level strategy id of each run's
			//runAndItsCategoryLevelStrategyId = pricingEngineDAO.getCategoryLevelStrategyId(conn, runIds);
			
			// Get Strategy definition for those strategies
			//getStrategyDefinition(conn, runIds, runAndItsCategoryLevelStrategyId, runAndItsCategoryLevelStrategy, strategyMap);
			
			// Get run and its already adjusted item
			runAndItsAlreadyAdjustedItem = getPriceAdjustedItems(runAndItsItem);
			
			for (PRRecommendationRunHeader runHeader : runHeaders) {
				long runId = runHeader.getRunId();
				
				if (runAndItsItem.get(runId) != null && runAndItsItem.get(runId).size() > 0) {
					Boolean isMarginRateAlreadyMeet = null, isPIRateAlreadyMeet = null;
					List<ProductDTO> productList = new ArrayList<ProductDTO>();
					// Get recommendation run header for each run
					PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
					recommendationRunHeader = pricingEngineDAO.getRecommendationRunHeader(conn, runId);
					runAndItsRecommendationRunHeader.put(runId, recommendationRunHeader);

					HashMap<Integer, Integer> zoneDivisionMap = rpzDAO.getZoneDivisionMap(conn, recommendationRunHeader.getLocationId());
					ArrayList<Integer> zoneIdList = dashboardDAO.getLocationListId(recommendationRunHeader.getLocationLevelId(),
							recommendationRunHeader.getLocationId());

					//Nagaraj:: Logic of getting the category level strategy id from dashboard table is changed on 11/21/2015
					//Get the category level strategy from the strategy id available in recommendation table
					//This is done, so that adjustment is done even from strategy what-if screen
					Long strategyId = setCategoryLevelStrategyId(chainId, zoneDivisionMap.get(recommendationRunHeader.getLocationId()), zoneIdList,
							recommendationRunHeader, runAndItsItem.get(runId));

					getStrategyDefinition(conn, runId, strategyId, runAndItsCategoryLevelStrategy, strategyMap);
					
					logger.debug("Category level strategy id : "
							+ (runAndItsCategoryLevelStrategy.get(runId) != null ? runAndItsCategoryLevelStrategy
									.get(runId).getStrategyId() : 0) + " for runid : " + runId);
					
					
					ProductDTO productDTO = new ProductDTO();
					productDTO.setProductLevelId(recommendationRunHeader.getProductLevelId());
					productDTO.setProductId(recommendationRunHeader.getProductId());
					productList.add(productDTO);

					// Get product group properties
					List<PRProductGroupProperty> productGroupProperties = pricingEngineDAO.getProductGroupProperties(
							conn, productList);
					// Get isUsePrediction flag of all runs
					boolean usePrediction = PricingEngineHelper.isUsePrediction(productGroupProperties,
							recommendationRunHeader.getProductLevelId(), recommendationRunHeader.getProductId(),
							recommendationRunHeader.getLocationLevelId(), recommendationRunHeader.getLocationId());

					// Get predicted results of all items
					HashMap<PredictionDetailKey, PredictionDetailDTO> predictions = engineService
							.getAlreadyPredictedValues(conn, runAndItsItem.get(runId), recommendationRunHeader,
									usePrediction);

					PRGuidelineMargin catLevelMarginGuideline = getMarginGuideline(runAndItsCategoryLevelStrategy
							.get(runId));
					PRGuidelinePI catLevelPIGuideline = getPIGuideline(runAndItsCategoryLevelStrategy.get(runId));

					double curMarginRate = computeFutureMarginRate(runAndItsItem.get(runId), recommendationRunHeader,
							predictions, usePrediction);
					isMarginRateAlreadyMeet = isMarginRateMeetsGoal(curMarginRate, catLevelMarginGuideline);

					double curPI = computePriceIndex(runAndItsItem.get(runId));
					isPIRateAlreadyMeet = isPriceIndexMeetsGoal(curPI, catLevelPIGuideline);

					logger.debug("PI before Balancing:" + curPI);
					logger.debug("Is PI Already Meet for RunId:" + runId + "--" + isPIRateAlreadyMeet);

					logger.debug("Margin Rate before Balancing:" + curMarginRate);
					logger.debug("Is Margin Rate Already Meet for RunId:" + runId + "--" + isMarginRateAlreadyMeet);

					// If both PI and Margin is below expectation, then don't do
					// any balancing
					if (isMarginRateAlreadyMeet != null && isPIRateAlreadyMeet != null && !isMarginRateAlreadyMeet
							&& !isPIRateAlreadyMeet) {
						logger.info("Balancing of PI & Margin is not Done for Run Id: " + runId
								+ ". Both Index & Margin is below expectation.");
						updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
								"Index & Margin adjustment not done. Both are below expectation.");
					} else {
						// Balance PI
						adjustRecPriceToMeetPI(runStatusDTOs, conn, runId, catLevelPIGuideline,
								runAndItsItem.get(runId), runAndItsStoreRecCnt.get(runId),
								runAndItsAlreadyAdjustedItem.get(runId), strategyMap, runAndItsPriceAdjustedItem,
								recommendationRunHeader, predictions, costChangeLogic, usePrediction,
								isMarginRateAlreadyMeet, catLevelMarginGuideline);

						// Balance Margin Rate
						adjustRecPriceToMeetMarginRate(runStatusDTOs, conn, runId, catLevelMarginGuideline,
								runAndItsItem.get(runId), runAndItsStoreRecCnt.get(runId),
								runAndItsAlreadyAdjustedItem.get(runId), strategyMap, runAndItsPriceAdjustedItem,
								recommendationRunHeader, predictions, costChangeLogic, usePrediction,
								isMarginRateAlreadyMeet, catLevelPIGuideline);

						if (runAndItsPriceAdjustedItem.get(runId) != null
								&& runAndItsPriceAdjustedItem.get(runId).size() > 0) {
							// Update conflicts
							updateConflicts(runId, runAndItsPriceAdjustedItem.get(runId));

							// Update Prediction
							updatePrediction(conn, runId, runAndItsPriceAdjustedItem.get(runId),
									recommendationRunHeader, predictions);

							// Update margin opportunity
							updateMarginOpportunity(runId, runAndItsPriceAdjustedItem.get(runId),
									recommendationRunHeader);

						}
					}
				} else {
					logger.info("Balancing of PI & Margin is not Done for Run Id: " + runId
							+ ". There are no recommended items");
					updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
							"Index & Margin adjustment not done. There are no recommended items.");
				}
			}
			
			// Update adjusted price to recommendation table
			for (List<PRItemDTO> items : runAndItsPriceAdjustedItem.values()) {
				for(PRItemDTO itemDTO : items)
					priceAdjustedItemsOfAllRuns.add(itemDTO);
			}
			// If there are item to update, perform following operation
			if (priceAdjustedItemsOfAllRuns.size() > 0) {
				// Update recommendation table for all run id
				updateAdjustedPriceToRecTable(conn, priceAdjustedItemsOfAllRuns);
				//Comment below line
				//PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
				//Update dashboard
				for (PRRecommendationRunHeader runHeader : runHeaders) {
					//Don't update dashboard if called from strategy what-if
					if (!runHeader.getRunType().equals(String.valueOf(PRConstants.RUN_TYPE_TEMP))) {
						PRRecommendationRunHeader recommendationRunHeader = runAndItsRecommendationRunHeader.get(runHeader.getRunId());
						engineService.updateDashboard(conn, recommendationRunHeader.getLocationLevelId(), recommendationRunHeader.getLocationId(),
								recommendationRunHeader.getProductLevelId(), recommendationRunHeader.getProductId(), runHeader.getRunId());
					}
				}
			}
		} catch (GeneralException | Exception e) {
			logger.error("Error while balancing PI and Margin. " + e.toString(), e);
			throw new OfferManagementException("Error while balancing PI and Margin ",
					RecommendationErrorCode.BALANCING_PI_MARGIN);
		}
		
		return runStatusDTOs;
	}
	
	private void adjustRecPriceToMeetPI(List<RunStatusDTO> runStatusDTOs, Connection conn, long runId,
			PRGuidelinePI catLevelPIGuideline, List<PRItemDTO> allItems, Integer storeRecCnt,
			List<PRItemDTO> alreadyAdjustedItem, HashMap<Long, PRStrategyDTO> strategyMap,
			HashMap<Long, List<PRItemDTO>> runAndItsPriceAdjustedItem,
			PRRecommendationRunHeader recommendationRunHeader,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, String costChangeLogic,
			Boolean usePredictionFlag, Boolean isMarginRateAlreadyMeet, PRGuidelineMargin marginGuideline)
			throws GeneralException {
		// Loop each run's
		logger.debug("Balancing of PI for Run Id: " + runId + " is Started...");

		int adjustedItemCount = alreadyAdjustedItem == null ? 0 : (alreadyAdjustedItem.size());
		List<PRItemDTO> priceAdjustedItemsOfRun = null;
		// Calculate current PI
		double curPI = computePriceIndex(allItems);

		// Ignore the run if it has store recommendation
		if (storeRecCnt != null) {
			logger.info("Balancing of PI is not Done for Run Id: " + runId + ". There are Store level Recommendations.");
			updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
					"Index Adjustment not done. There are store level recommendations.");
		} else if (adjustedItemCount > 0) {
			// If the run is already adjusted, mark the status and continue to
			// next run
			logger.info("Balancing of PI is not Done for Run Id: " + runId
					+ ". Price Adjustment was already done, No more price adjustment is allowed.");
			updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
					"Index Adjustment not done. Price Adjustment was already done, no more price adjustment is allowed.");
		} else if (catLevelPIGuideline == null) {
			// If the run has no PI goal, mark the status and continue to next
			// run
			logger.info("Balancing of PI is not Done for Run Id: " + runId
					+ ". Category level Price Index Guideline is not defined or only Max value is defined.");
			updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
					"Index Adjustment not done. Category level price index guideline is not defined or only max value is defined.");
		} else if (curPI == 0) {
			logger.info("Balancing of PI is not Done for Run Id: " + runId
					+ ". Competitor price is not present even for a Single Item.");
			updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
					"Index Adjustment not done. Competitor price is not present even for a single item.");
		} else if (isPriceIndexMeetsGoal(curPI, catLevelPIGuideline)) {
			// If the run is already meeting the PI goal, mark the status and
			// continue to next run
			logger.info("Balancing of PI is not Done for Run Id: " + runId + ". Price Index already meets the Goal.");
			updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
					"Index Adjustment not done. Price Index already meets the goal.");
		} else {
			List<PRItemDTO> opporutinityItems = new ArrayList<PRItemDTO>();

			// Update item with strategy object
			updateItemWithStrategyObject(conn, allItems, strategyMap);

			// Update item with price group detail
			updateItemWithPriceGroupDetail(conn, allItems);

			// Get items which are considered for Price Adjustment
			opporutinityItems = getOpportunityItems(allItems, costChangeLogic, ADJUSTMENT_TYPE_PI,
					recommendationRunHeader, predictions, usePredictionFlag);

			if (opporutinityItems.size() > 0) {
				priceAdjustedItemsOfRun = new ArrayList<PRItemDTO>();
				// Find if the price needs to be increased or decreased
				int priceChgInd = findPriceChgIndicator(curPI, catLevelPIGuideline);

				// Adjust price of each item till the goal is meet
				String adjustedItemCnt = adjustRecommendedPriceForPI(conn, priceChgInd, allItems,
						opporutinityItems, priceAdjustedItemsOfRun, catLevelPIGuideline, recommendationRunHeader,
						predictions, usePredictionFlag, isMarginRateAlreadyMeet, marginGuideline);

				runAndItsPriceAdjustedItem.put(runId, priceAdjustedItemsOfRun);
				logger.info("No of Items adjusted to Balance PI for Run Id: " + runId + " -- " + adjustedItemCnt);
				updateRunStatus(runId, runStatusDTOs, PRConstants.SUCCESS, "Index adjusted successfully for "
						+ adjustedItemCnt + " items.");
			} else {
				logger.info("Balancing of PI is not Done for Run Id: " + runId + ". No Opportunity Item Found.");
				updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
						"Index Adjustment not done. No opportunity item found.");
			}
		}
		logger.debug("Balancing of PI for Run Id: " + runId + " is Completed...");
	}

	private void adjustRecPriceToMeetMarginRate(List<RunStatusDTO> runStatusDTOs, Connection conn, long runId,
			PRGuidelineMargin catLevelMarginGuideline, List<PRItemDTO> allItems, Integer storeRecCnt,
			List<PRItemDTO> alreadyAdjustedItem, HashMap<Long, PRStrategyDTO> strategyMap,
			HashMap<Long, List<PRItemDTO>> runAndItsPriceAdjustedItem,
			PRRecommendationRunHeader recommendationRunHeader,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, String costChangeLogic,
			Boolean usePredictionFlag, Boolean isMarginRateAlreadyMeet, PRGuidelinePI catLevelPIGuideline)
			throws GeneralException {
		// Don't adjust the item which is already adjusted to meet PI goal
		logger.debug("Balancing of Margin Rate for Run Id: " + runId + " is Started...");

		int adjustedItemCount = alreadyAdjustedItem == null ? 0 : (alreadyAdjustedItem.size());

		// Calculate current Margin Rate
		double curMarginRate = computeFutureMarginRate(allItems, recommendationRunHeader, predictions,
				usePredictionFlag);
		
		// Calculate current PI
		double curPI = computePriceIndex(allItems);
				
		logger.debug("Margin Rate before Balancing Margin Rate:" + curMarginRate);

		// Ignore the run if it has store recommendation
		if (storeRecCnt != null) {
			logger.info("Balancing of Margin Rate is not Done for Run Id: " + runId
					+ ". There are Store level Recommendations.");
			updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
					"Margin Adjustment not done. There are store level recommendations.");
		} else if (adjustedItemCount > 0) {
			// If the run is already adjusted, mark the status and continue to
			// next run
			logger.info("Balancing of Margin Rate is not Done for Run Id: " + runId
					+ ". Price Adjustment was Already done, No More Price Adjustment is Allowed.");
			updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
					"Margin Adjustment not done. Price Adjustment was already done, no more price adjustment is allowed.");
		} else if (catLevelMarginGuideline == null) {
			// If the run has no PI goal, mark the status and continue to next
			// run
			logger.info("Balancing of Margin Rate is not done for Run Id: "
					+ runId
					+ ". Margin Adjustment not done. Category level Margin Rate is not defined or only Max value is defined.");
			updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
					"Margin Adjustment not done. Category level margin rate is not defined or only max value is defined.");
		} else if (isMarginRateMeetsGoal(curMarginRate, catLevelMarginGuideline)) {
			// If the run is already meeting the PI goal, mark the status and
			// continue to next run
			logger.info("Balancing of Margin Rate is not done for Run Id: " + runId
					+ ". Margin Rate already meets the Goal.");
			updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
					"Margin Adjustment not done. Margin Rate already meets the goal.");
		} else {
			List<PRItemDTO> opporutinityItems = new ArrayList<PRItemDTO>();

			// Update item with strategy object
			updateItemWithStrategyObject(conn, allItems, strategyMap);

			// Update item with price group detail
			updateItemWithPriceGroupDetail(conn, allItems);

			// Get items which are considered for Price Adjustment
			opporutinityItems = getOpportunityItems(allItems, costChangeLogic, ADJUSTMENT_TYPE_MAR_RATE,
					recommendationRunHeader, predictions, usePredictionFlag);

			if (opporutinityItems.size() > 0) {
				List<PRItemDTO> priceAdjustedItemsOfRun = new ArrayList<PRItemDTO>();

				// Adjust price of each item till the goal is meet
				String adjustedItemCnt = adjustRecommendedPriceForMarginRate(conn, allItems, opporutinityItems,
						priceAdjustedItemsOfRun, catLevelMarginGuideline, recommendationRunHeader, predictions,
						usePredictionFlag, isMarginRateAlreadyMeet, curPI, catLevelPIGuideline, costChangeLogic);

				if (runAndItsPriceAdjustedItem.get(runId) != null)
					runAndItsPriceAdjustedItem.get(runId).addAll(priceAdjustedItemsOfRun);
				else
					runAndItsPriceAdjustedItem.put(runId, priceAdjustedItemsOfRun);
				logger.info("No of Items adjusted to Balance Margin Rate for Run Id: " + runId + " -- "
						+ adjustedItemCnt);
				updateRunStatus(runId, runStatusDTOs, PRConstants.SUCCESS, "Margin adjusted successfully for "
						+ adjustedItemCnt + " items.");
			} else {
				logger.info("Balancing of Margin Rate is not done for Run Id: " + runId
						+ ". No Opportunity Item Found.");
				updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
						"Margin Adjustment not done. No opportunity item found.");
			}
		}
		logger.debug("Balancing of Margin Rate for Run Id: " + runId + " is Completed...");
	}
	
	private PRGuidelineMargin getMarginGuideline(PRStrategyDTO strategyDTO){
		PRGuidelineMargin categoryLevelMarginGuideline = null;
		
		if(strategyDTO != null && strategyDTO.getGuidelines() != null && strategyDTO.getGuidelines().getMarginGuideline() != null){
			List<PRGuidelineMargin> marginGuidelines =  strategyDTO.getGuidelines().getMarginGuideline();
			PRGuidelineMargin catLevelMargin = null;
			
			//Get category level pi guideline
			for(PRGuidelineMargin marGuideline : marginGuidelines){
				if(marGuideline.getItemLevelFlag() == Constants.NO &&
						marGuideline.getValueType() == PRConstants.VALUE_TYPE_PCT){
					catLevelMargin = marGuideline;
					break;
				}
			}
			
			//if max alone defined, then don't do adjustment
			if(catLevelMargin != null && catLevelMargin.getMinMarginPct() == Constants.DEFAULT_NA && 
					catLevelMargin.getMaxMarginPct() != Constants.DEFAULT_NA){
				categoryLevelMarginGuideline = null;
			//Either min or max must have mentioned
			} else if (catLevelMargin != null && (catLevelMargin.getMinMarginPct() != Constants.DEFAULT_NA
					|| catLevelMargin.getMaxMarginPct() != Constants.DEFAULT_NA)) {
				categoryLevelMarginGuideline = catLevelMargin;
			}
		}
		
		return categoryLevelMarginGuideline;
	}
	
	private PRGuidelinePI getPIGuideline(PRStrategyDTO strategyDTO){
		PRGuidelinePI categoryLevelPriceIndex = null;
		
		if(strategyDTO != null && strategyDTO.getGuidelines() != null && strategyDTO.getGuidelines().getPiGuideline() != null){
			List<PRGuidelinePI> piGuidelines =  strategyDTO.getGuidelines().getPiGuideline();
			PRGuidelinePI catLevelPI = null;
			
			//Get category level pi guideline
			for(PRGuidelinePI piGuideline : piGuidelines){
				if(piGuideline.getItemLevelFlag() == Constants.NO){
					catLevelPI = piGuideline;
					break;
				}
			}
			
			//if max alone defined, then don't do adjustment
			if(catLevelPI != null && catLevelPI.getMinValue() == Constants.DEFAULT_NA && 
					catLevelPI.getMaxValue() != Constants.DEFAULT_NA){
				categoryLevelPriceIndex = null;
			//Either min or max must have mentioned
			} else if (catLevelPI != null && (catLevelPI.getMinValue() != Constants.DEFAULT_NA
					|| catLevelPI.getMaxValue() != Constants.DEFAULT_NA)) {
				categoryLevelPriceIndex = catLevelPI;
			}
		}
		
		return categoryLevelPriceIndex;
	}
	
	private Long setCategoryLevelStrategyId(String chainId, Integer divisionId, ArrayList<Integer> locationList,
			PRRecommendationRunHeader recommendationRunHeader, List<PRItemDTO> runAndItems) {
		Long strategyId = null;
		for (PRItemDTO itemDTO : runAndItems) {
			// If available at zone and category level
			if (itemDTO.getStrategyDTO().getLocationLevelId() == recommendationRunHeader.getLocationLevelId()
					&& itemDTO.getStrategyDTO().getLocationId() == recommendationRunHeader.getLocationId()
					&& itemDTO.getStrategyDTO().getProductLevelId() == recommendationRunHeader.getProductLevelId()
					&& itemDTO.getStrategyDTO().getProductId() == recommendationRunHeader.getProductId() && itemDTO.getStrategyDTO().getApplyTo() <= 0
					&& itemDTO.getStrategyDTO().getStateId() <= 0 && itemDTO.getStrategyDTO().getVendorId() <= 0) {
				strategyId = itemDTO.getStrategyId();
				break;
			}
		}

		if (strategyId == null) {
			for (Integer locationListId : locationList) {
				for (PRItemDTO itemDTO : runAndItems) {
					// If available at zone list level
					if (itemDTO.getStrategyDTO().getLocationLevelId() == PRConstants.ZONE_LIST_LEVEL_TYPE_ID
							&& itemDTO.getStrategyDTO().getLocationId() == locationListId
							&& itemDTO.getStrategyDTO().getProductLevelId() == recommendationRunHeader.getProductLevelId()
							&& itemDTO.getStrategyDTO().getProductId() == recommendationRunHeader.getProductId()
							&& itemDTO.getStrategyDTO().getApplyTo() <= 0 && itemDTO.getStrategyDTO().getStateId() <= 0
							&& itemDTO.getStrategyDTO().getVendorId() <= 0) {
						strategyId = itemDTO.getStrategyId();
						break;
					}
				}
			}
		}

		if (strategyId == null) {
			if (divisionId != null) {
				for (PRItemDTO itemDTO : runAndItems) {
					// If available at division level
					if (itemDTO.getStrategyDTO().getLocationLevelId() == Constants.DIVISION_LEVEL_ID
							&& itemDTO.getStrategyDTO().getLocationId() == divisionId
							&& itemDTO.getStrategyDTO().getProductLevelId() == recommendationRunHeader.getProductLevelId()
							&& itemDTO.getStrategyDTO().getProductId() == recommendationRunHeader.getProductId()
							&& itemDTO.getStrategyDTO().getApplyTo() <= 0 && itemDTO.getStrategyDTO().getStateId() <= 0
							&& itemDTO.getStrategyDTO().getVendorId() <= 0) {
						strategyId = itemDTO.getStrategyId();
						break;
					}
				}
			}
		}

		if (strategyId == null) {
			for (PRItemDTO itemDTO : runAndItems) {
				// If available at chain level
				if (itemDTO.getStrategyDTO().getLocationLevelId() == Constants.CHAIN_LEVEL_ID
						&& itemDTO.getStrategyDTO().getLocationId() == Integer.valueOf(chainId)
						&& itemDTO.getStrategyDTO().getProductLevelId() == recommendationRunHeader.getProductLevelId()
						&& itemDTO.getStrategyDTO().getProductId() == recommendationRunHeader.getProductId()
						&& itemDTO.getStrategyDTO().getApplyTo() <= 0 && itemDTO.getStrategyDTO().getStateId() <= 0
						&& itemDTO.getStrategyDTO().getVendorId() <= 0) {
					strategyId = itemDTO.getStrategyId();
					break;
				}
			}
		}

		return strategyId;
	}
	
//	private void getStrategyDefinition(Connection conn, List<Long> runIds,
//			HashMap<Long, Long> runAndItsCategoryLevelStrategyId,
//			HashMap<Long, PRStrategyDTO> runAndItsCategoryLevelStrategy, HashMap<Long, PRStrategyDTO> strategyMap)
//			throws OfferManagementException {
//		StrategyDAO strategyDAO = new StrategyDAO();
//		for (Long runId : runIds) {
//			PRStrategyDTO strategyDTO = null;
//			if (strategyMap.get(runAndItsCategoryLevelStrategyId.get(runId)) != null) {
//				strategyDTO = strategyMap.get(runAndItsCategoryLevelStrategyId.get(runId));
//			} else {
//				strategyDTO = strategyDAO.getStrategyDefinition(conn, runAndItsCategoryLevelStrategyId.get(runId));
//				strategyMap.put(strategyDTO.getStrategyId(), strategyDTO);
//			}
//			runAndItsCategoryLevelStrategy.put(runId, strategyDTO);
//		}
//	}
	
	private void getStrategyDefinition(Connection conn, Long runId, Long strategyId, HashMap<Long, PRStrategyDTO> runAndItsCategoryLevelStrategy,
			HashMap<Long, PRStrategyDTO> strategyMap) throws OfferManagementException {
		StrategyDAO strategyDAO = new StrategyDAO();
		PRStrategyDTO strategyDTO = null;
		if (strategyId != null && strategyId > 0) {
			if (strategyMap.get(strategyId) != null) {
				strategyDTO = strategyMap.get(strategyId);
			} else {
				strategyDTO = strategyDAO.getStrategyDefinition(conn, strategyId);
				strategyMap.put(strategyDTO.getStrategyId(), strategyDTO);
			}
		}
		runAndItsCategoryLevelStrategy.put(runId, strategyDTO);
	}
	
	public List<RunStatusDTO> rollbackAdjustedPrice(String inpRunIds) {
		List<RunStatusDTO> runStatusDTOs = new ArrayList<RunStatusDTO>();
		ObjectMapper mapper = new ObjectMapper();
		HashMap<Long, List<PRItemDTO>> runAndItsAdjustedItem = new HashMap<Long, List<PRItemDTO>>();
		HashMap<Long, List<PRItemDTO>> runAndItsAllItems = new HashMap<Long, List<PRItemDTO>>();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		List<PRItemDTO> rollbackItems = new ArrayList<PRItemDTO>();
		HashMap<Long, PRStrategyDTO> strategyMap = new HashMap<Long, PRStrategyDTO>();
//		HashMap<Long, PRStrategyDTO> runAndItsPIGoal = new HashMap<Long, PRStrategyDTO>();
		HashMap<Long, PRRecommendationRunHeader> runAndItsRecommendationRunHeader = new HashMap<Long, PRRecommendationRunHeader>();
		PricingEngineService engineService = new PricingEngineService();
		try {
			if (this.conn == null) {
				initializeForWS();
				this.conn = getConnection();
			}
			List<Long> runIds =  mapper.readValue(inpRunIds, new TypeReference<List<Long>>(){});
			
			if(runIds.size() > 0 ){
				runStatusDTOs = updateRunStatusWithRunId(runIds);
				
				//Get Price Index Goal of each run
//				runAndItsPIGoal = pricingEngineDAO.getCategoryLevelPriceIndex(conn, runIds);
				
				//Get all the items of the runs
				runAndItsAllItems = pricingEngineDAO.getRecItemsOfRunIds(conn, runIds);
				
				//get items whose price is changed
				//runAndItsAdjustedItem = pricingEngineDAO.getPriceAdjustedItems(conn, runIds);
				runAndItsAdjustedItem = getPriceAdjustedItems(runAndItsAllItems);
				
				//Loop through each run's 
				for(Long runId : runIds){
					//If not items to rollback
					if(runAndItsAdjustedItem.get(runId) != null && runAndItsAdjustedItem.get(runId).size() > 0){
						//Get recommendation 
						PRRecommendationRunHeader recommendationRunHeader = new PRRecommendationRunHeader();
						recommendationRunHeader = pricingEngineDAO.getRecommendationRunHeader(conn, runId);
						runAndItsRecommendationRunHeader.put(runId, recommendationRunHeader);
						
						List<ProductDTO> productList = new ArrayList<ProductDTO>();
						ProductDTO productDTO = new ProductDTO();
						productDTO.setProductLevelId(recommendationRunHeader.getProductLevelId());
						productDTO.setProductId(recommendationRunHeader.getProductId());
						productList.add(productDTO);
						
						// Get product group properties
						List<PRProductGroupProperty> productGroupProperties = pricingEngineDAO
								.getProductGroupProperties(conn, productList);
						// Get isUsePrediction flag of all runs
						boolean usePrediction = PricingEngineHelper.isUsePrediction(productGroupProperties,
								recommendationRunHeader.getProductLevelId(), recommendationRunHeader.getProductId(),
								recommendationRunHeader.getLocationLevelId(), recommendationRunHeader.getLocationId());
						
						//Update item with strategy object
						updateItemWithStrategyObject(conn, runAndItsAdjustedItem.get(runId), strategyMap);

						String adjustedItemCnt = rollbackPrices(runAndItsAdjustedItem.get(runId), rollbackItems);

						//Update the conflicts of lig rep item and its member, non lig and explain log
						updateConflicts(runId, runAndItsAdjustedItem.get(runId));
						
						HashMap<PredictionDetailKey, PredictionDetailDTO> predictions = engineService
								.getAlreadyPredictedValues(conn, runAndItsAdjustedItem.get(runId),
										recommendationRunHeader, usePrediction);
						
						//Update the predictions
						updatePrediction(conn, runId, runAndItsAdjustedItem.get(runId), recommendationRunHeader, predictions);
						
						//Update the PI and Value in dashboard table
//						updateFuturePIAndValueInDashboard(conn, recommendationRunHeader, runAndItsAllItems.get(runId), 
//								runAndItsPIGoal.get(runId).getStrategyId());
						
						logger.debug("Price is rollbacked successfully for " + adjustedItemCnt + " items.");
						updateRunStatus(runId, runStatusDTOs, PRConstants.SUCCESS, 
								"Price is rollbacked successfully for " + adjustedItemCnt + " items.");
					}else{
						updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE,
								"Price rollback not done. No items to rollback.");
					}
				}

				if (rollbackItems.size() > 0) {
					// Update recommendation table for all run id
					updateAdjustedPriceToRecTable(conn, rollbackItems);

					for (Long runId : runIds) {
						PRRecommendationRunHeader recommendationRunHeader = runAndItsRecommendationRunHeader.get(runId);
						engineService.updateDashboard(conn, recommendationRunHeader.getLocationLevelId(),
								recommendationRunHeader.getLocationId(), recommendationRunHeader.getProductLevelId(),
								recommendationRunHeader.getProductId(), runId);
					}
					
					// Update recommendation table
					PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
				}
			}
		} catch (Exception | GeneralException | OfferManagementException ex) {
			// Update all run id with error status
			logger.error("Exception while rollbacking the Price" + ex.toString(), ex);
			PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
			for (RunStatusDTO runStatusDTO : runStatusDTOs) {
				updateRunStatusWithException(runStatusDTO, "Error while Rollbacking the Price");
			}
		}finally {
			PristineDBUtil.close(conn);					 
		}
		return runStatusDTOs;
	}
	
	private HashMap<Long, List<PRItemDTO>> getPriceAdjustedItems(HashMap<Long, List<PRItemDTO>> runAndItsAllItems){
		HashMap<Long, List<PRItemDTO>> runAndItsAdjustedItem = new HashMap<Long, List<PRItemDTO>>();
		
		for (Map.Entry<Long, List<PRItemDTO>> itemsOfARun : runAndItsAllItems.entrySet()) {
			List<PRItemDTO> items = new ArrayList<PRItemDTO>();
			for (PRItemDTO item : itemsOfARun.getValue()) {
				if(item.getIsPriceAdjusted())
					items.add(item);
			}
			if(items.size() > 0)
				runAndItsAdjustedItem.put(itemsOfARun.getKey(), items);
		}
		
		return runAndItsAdjustedItem;
	}
	
//	private void updateFuturePIAndValueInDashboard(Connection conn, PRRecommendationRunHeader recommendationRunHeader,
//			List<PRItemDTO> allItemsOfRun, long inpStrategyId) throws GeneralException, OfferManagementException{
//		DashboardDAO dashboardDAO = new DashboardDAO(conn);
//		PriceIndexCalculation priceIndexCalculation = new PriceIndexCalculation();
//		DashboardService dashboardService = new DashboardService();
//		int strategyId = (int) inpStrategyId;
//		
//		Double futurePI = priceIndexCalculation.getFutureSimpleIndex(allItemsOfRun, true);
//		Double value = dashboardService.calculateValue(conn, strategyId, futurePI, allItemsOfRun);
//		
//		dashboardDAO.updateFuturePIAndValue(recommendationRunHeader.getLocationLevelId(), recommendationRunHeader.getLocationId(), 
//				recommendationRunHeader.getProductLevelId(), recommendationRunHeader.getProductId(), futurePI, value);
//		
//	}
	
	private String rollbackPrices(List<PRItemDTO> allItemsOfRun, List<PRItemDTO> rollbackItems) {
		String noOfRollbackedPrice = "";
		int allAdjustedItem = 0;
		int distinctAdjustedItem = 0;
		HashMap<Integer, Integer> distinctRetLirPriceRollbacked = new HashMap<Integer, Integer>();
		
		for (PRItemDTO itemDTO : allItemsOfRun) {
			if (itemDTO.getRecPriceBeforeAdjustment() != null) {
				itemDTO.setRecommendedRegPrice(itemDTO.getRecPriceBeforeAdjustment());
				itemDTO.setIsPriceAdjusted(false);
				itemDTO.setRecPriceBeforeAdjustment(null);				
				
				rollbackItems.add(itemDTO);

				if (!itemDTO.isLir())
					allAdjustedItem = allAdjustedItem + 1;

				if (itemDTO.getRetLirId() <= 0)
					distinctAdjustedItem = distinctAdjustedItem + 1;
				
				if(itemDTO.getRetLirId() > 0 && itemDTO.isLir())
					distinctRetLirPriceRollbacked.put(itemDTO.getRetLirId(), 0);
				
			}
		}
		noOfRollbackedPrice = (distinctRetLirPriceRollbacked.size() +  distinctAdjustedItem) + "(" + allAdjustedItem + ")";
		return noOfRollbackedPrice;
	}
	
	private void updateRunStatus(long runId, List<RunStatusDTO> runStatusDTOs, int statusCode, String message) {
		for (RunStatusDTO runStatusDTO : runStatusDTOs) {
			if (runStatusDTO.runId == runId) {
				//If either pi or margin is success, then make it has success
				if(runStatusDTO.msgCnt == 0)
					runStatusDTO.statusCode = statusCode;
				else if(runStatusDTO.statusCode != PRConstants.SUCCESS)
					runStatusDTO.statusCode = statusCode;
				
				runStatusDTO.message = (runStatusDTO.message == null ? "" : runStatusDTO.message);
				runStatusDTO.message = runStatusDTO.message + " " + message;
				runStatusDTO.msgCnt = runStatusDTO.msgCnt + 1;
			}
		}
	}
	
	// Update RunStatusDTO with exception
	private void updateRunStatusWithException(RunStatusDTO runStatusDTO, String message) {
			runStatusDTO.statusCode = PRConstants.FAILURE;
			runStatusDTO.message = message;;
	}
	
	//Update RunStatusDTO with run Id
	private List<RunStatusDTO> updateRunStatusWithRunId(List<Long> runIds) throws GeneralException{
		List<RunStatusDTO> runStatusDTOs = new ArrayList<RunStatusDTO>();
		
		try{
			for (Long runId : runIds) {
				RunStatusDTO runStatusDTO = new RunStatusDTO();
				runStatusDTO.runId = runId;
				runStatusDTOs.add(runStatusDTO);
			}
		}catch(Exception ex){
			logger.error("Exception in updateRunStatusWithRunId()" + ex.toString(), ex);
			throw new GeneralException("", ex);
		}
		return runStatusDTOs;
	}
	
	//Get Store Recommendation Count
	private HashMap<Long, Integer> getStoreRecommendationCount(Connection conn, List<Long> runIds) throws GeneralException{
		HashMap<Long, Integer> runAndItsStoreRecCount = new HashMap<Long, Integer>();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		runAndItsStoreRecCount = pricingEngineDAO.getStoreRecommendationCount(conn, runIds);
		return runAndItsStoreRecCount;
	}
	
	//Find Price Index of the Category
	private Double computePriceIndex(List<PRItemDTO> allItemsOfRun){
		PriceIndexCalculation priceIndexCalculation = new PriceIndexCalculation();
		Double priceIndex = priceIndexCalculation.getFutureSimpleIndex(allItemsOfRun, true);
		return priceIndex == null ? 0 : priceIndex;
	}

	/***
	 * Check if the curPriceIndex meets the Category Level Price Index
	 * @param curPriceIndex
	 * @param categoryLevelPriceIndex
	 * @return
	 */
	private Boolean isPriceIndexMeetsGoal(double curPriceIndex, PRGuidelinePI categoryLevelPriceIndex) {
		PriceIndexCalculation priceIndexCalculation = new PriceIndexCalculation();
		Boolean isPIMeetsGoal = null;

		if (categoryLevelPriceIndex != null) 
			isPIMeetsGoal = priceIndexCalculation.isPriceIndexMeetsGoal(curPriceIndex, categoryLevelPriceIndex);			
		
		return isPIMeetsGoal;
	}
	
	/***
	 * Find if a price has to be increased or decreased in order to meet the price index goal
	 * @param curPriceIndex
	 * @param categoryLevelPriceIndex
	 * @return
	 */
	private int findPriceChgIndicator(double curPriceIndex, PRGuidelinePI categoryLevelPriceIndex){
		int priceChgIndicator = 0;

		// Min alone present
		if (categoryLevelPriceIndex.getMinValue() != Constants.DEFAULT_NA && categoryLevelPriceIndex.getMaxValue() == Constants.DEFAULT_NA) {
			if(curPriceIndex < categoryLevelPriceIndex.getMinValue()){
				priceChgIndicator = -1;
			}
		} else if (categoryLevelPriceIndex.getMaxValue() != Constants.DEFAULT_NA && categoryLevelPriceIndex.getMinValue() == Constants.DEFAULT_NA) {
			// Max alone present
			if(curPriceIndex > categoryLevelPriceIndex.getMaxValue()){
				priceChgIndicator = 1;
			}
		} else if (categoryLevelPriceIndex.getMinValue() != Constants.DEFAULT_NA && categoryLevelPriceIndex.getMaxValue() != Constants.DEFAULT_NA) {
			// Both present
			if(curPriceIndex < categoryLevelPriceIndex.getMinValue()){
				priceChgIndicator = -1;
			}else if (curPriceIndex > categoryLevelPriceIndex.getMaxValue()){
				priceChgIndicator = 1;
			}
		}
		return priceChgIndicator;
	}
	
	/***
	 * Update the strategy object to item
	 * @param conn
	 * @param allItemsOfRun
	 * @param strategyMap
	 * @throws GeneralException
	 */
	private void updateItemWithStrategyObject(Connection conn, List<PRItemDTO> allItemsOfRun,
			HashMap<Long, PRStrategyDTO> strategyMap) throws GeneralException {
		// Get Strategy definition
		// Didn't get strategy definition while getting items of run id due to query complexity and redundant data in the query
		
		PRStrategyDTO strategyDTO = null;
		StrategyDAO strategyDAO = new StrategyDAO();
		try {
			for (PRItemDTO itemDTO : allItemsOfRun) {
				if (strategyMap.get(itemDTO.getStrategyId()) != null) {
					strategyDTO = strategyMap.get(itemDTO.getStrategyId());
				} else {
					strategyDTO = strategyDAO.getStrategyDefinition(conn, itemDTO.getStrategyId());
					strategyMap.put(strategyDTO.getStrategyId(), strategyDTO);
				}

				itemDTO.setStrategyDTO(strategyDTO);
			}
		} catch (OfferManagementException | Exception ex) {
			logger.error("Exception in updateItemWithStrategyObject" + ex.toString(), ex);
			throw new GeneralException(ex.toString());
		}
	}
	
	/***
	 * Find and updates if a item is part of price group
	 * @param conn
	 * @param allItemsOfRun
	 * @throws GeneralException
	 */
	private void updateItemWithPriceGroupDetail(Connection conn, List<PRItemDTO> allItemsOfRun) throws GeneralException{
		//Get distinct ret lir id, non lig item
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		HashMap<Integer, Integer> distinctRetLirIdAndItemCodeMap = new HashMap<Integer, Integer>();
		List<Integer> distinctRetLirIdAndItemCode = new ArrayList<Integer>();
		List<PRItemDTO> priceGroupItems = new ArrayList<PRItemDTO>();
		int limitcount = 0;
		
		for (PRItemDTO itemDTO : allItemsOfRun) {
			if(itemDTO.getRetLirId() > 0){
				distinctRetLirIdAndItemCodeMap.put(itemDTO.getRetLirId(), 0);
			}else{
				distinctRetLirIdAndItemCodeMap.put(itemDTO.getItemCode(), 0);
			}
		}
		
//		for (Integer retLirIdOrItemCode: distinctRetLirIdAndItemCodeMap.keySet()) {
//			distinctRetLirIdAndItemCode.add(retLirIdOrItemCode);
//		}
		
		//NU:: 27th Feb 2017, bug fix: when a category has more than 1000 items
		// In query breaks, so splitted to run for every 1000
		
		for (Integer retLirIdOrItemCode: distinctRetLirIdAndItemCodeMap.keySet()) {
			distinctRetLirIdAndItemCode.add(retLirIdOrItemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
				List<PRItemDTO> tempPriceGroupItems = null;
				Object[] values = distinctRetLirIdAndItemCode.toArray();
				tempPriceGroupItems = pricingEngineDAO.getPriceGroupItems(conn, values);
				if(tempPriceGroupItems != null) {
					priceGroupItems.addAll(tempPriceGroupItems);
				}
				distinctRetLirIdAndItemCode.clear();
			}
		}
		if (distinctRetLirIdAndItemCode.size() > 0) {
			List<PRItemDTO> tempPriceGroupItems = null;
			Object[] values = distinctRetLirIdAndItemCode.toArray();
			tempPriceGroupItems = pricingEngineDAO.getPriceGroupItems(conn, values);
			if(tempPriceGroupItems != null) {
				priceGroupItems.addAll(tempPriceGroupItems);
			}
			distinctRetLirIdAndItemCode.clear();
		}
		
		
		
		// Get items which are part of price group
		// Didn't get price group information while getting items of run id due to query complexity and redundant data in the query
//		priceGroupItems = pricingEngineDAO.getPriceGroupItems(conn, distinctRetLirIdAndItemCode);

		//update item object 
		for (PRItemDTO priceGroupItem : priceGroupItems) {
			//If lig
			if(priceGroupItem.isLir()){
				for (PRItemDTO itemDTO : allItemsOfRun) {
					//Update all its member
					if(itemDTO.getRetLirId() > 0 && itemDTO.getRetLirId() == priceGroupItem.getItemCode()){
						itemDTO.setIsPartOfPriceGroup(true);
					}
				}	
			}else{
				int retLirId = -1;
				//Check if it is a lig member
				for (PRItemDTO itemDTO : allItemsOfRun) {
					if(itemDTO.getItemCode() == priceGroupItem.getItemCode()){
						retLirId = itemDTO.getRetLirId();
						break;
					}
				}
				
				if (retLirId > 0) {
					for (PRItemDTO itemDTO : allItemsOfRun) {
						//Update all its member
						if(itemDTO.getRetLirId() == priceGroupItem.getItemCode()){
							itemDTO.setIsPartOfPriceGroup(true);
						}
					}	
				} else {//non lig
					for (PRItemDTO itemDTO : allItemsOfRun) {
						if (itemDTO.getItemCode() == priceGroupItem.getItemCode()) {
							itemDTO.setIsPartOfPriceGroup(true);
							break;
						}
					}
				}
			}
		}
	}
	
	/***
	 * Get opportunity items based on different criteria
	 * @param allItemsOfRun
	 * @param costChangeLogic
	 * @param balanceType
	 * @return
	 */
	private List<PRItemDTO> getOpportunityItems(List<PRItemDTO> allItemsOfRun, String costChangeLogic,
			String balanceType, PRRecommendationRunHeader recommendationRunHeader,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, boolean isUsePrediction) {
		List<PRItemDTO> opportunityItems = new ArrayList<PRItemDTO>();
		/* Get items which are considered for Price Adjustment (if one lig member is ignored, then the entire lig has to be ignored)
		 * Don't consider Items part of price group, price check list, Cost Changed, without comp price,
		 * Pre priced, loc priced and has min/max constraint, already overridden price, items with no prediction or 0 prediction 
		 */
		HashMap<Integer, Integer> ligsToConsider = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> nonLigsToConsider = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> ligsToIgnore = new HashMap<Integer, Integer>();
		
		
		for (PRItemDTO itemDTO : allItemsOfRun) {
			//ignore lig item
			if (!itemDTO.isLir()) {
				boolean isPrePrice, isLockPrice, isMinMax, ignoreCostChangedItem = false, canConsiderItem = false;
				PRStrategyDTO strategyDTO = itemDTO.getStrategyDTO();
				int priceCheckListId = 0;

				isPrePrice = (itemDTO.getIsPrePriced() == 1) ? true : false;
				//isLockPrice = (strategyDTO.getConstriants().getLocPriceConstraint() != null) ? true : false;
				isLockPrice = (itemDTO.getIsLocPriced() == 1) ? true : false;
				isMinMax = (strategyDTO.getConstriants().getMinMaxConstraint() != null) ? true : false;
				priceCheckListId = itemDTO.getPriceCheckListId() == null ? 0 : itemDTO.getPriceCheckListId();
				
				//Get the predicted movement of the item
				Long predictedMov = null;
				PricingEngineService pricingEngineService = new PricingEngineService();
				
//				predictedMov = pricingEngineService.getPredictedMov(itemDTO.getItemCode(), recommendationRunHeader.getLocationLevelId(),
//						recommendationRunHeader.getLocationId(), itemDTO.getRecommendedRegMultiple(), itemDTO.getRecommendedRegPrice(),
//						predictions, isUsePrediction);
				
				predictedMov = pricingEngineService.getPredictedMov(itemDTO.getItemCode(), recommendationRunHeader.getLocationLevelId(),
						recommendationRunHeader.getLocationId(), itemDTO.getRecommendedRegPrice(), predictions, isUsePrediction);
				
				if (costChangeLogic.toUpperCase().equals(PRConstants.AHOLD)) {
					if (itemDTO.getCostChgIndicator() == 0)
						ignoreCostChangedItem = false;
					else
						ignoreCostChangedItem = true;
				}

				if (balanceType.equals("PI")) {
					if (priceCheckListId <= 0 && !itemDTO.getIsPartOfPriceGroup() && itemDTO.getCompPrice() != null
							&& !ignoreCostChangedItem && itemDTO.getOverrideRegPrice() == null && !isPrePrice
							&& !isLockPrice && !isMinMax && !itemDTO.getIsPartOfSubstituteGroup()
							&& (predictedMov != null && predictedMov > 0)) {
						canConsiderItem = true;
					}
				} else if (balanceType.equals("MARGIN_RATE")) {
					if (priceCheckListId <= 0 && !itemDTO.getIsPartOfPriceGroup() && itemDTO.getCompPrice() == null
							&& !ignoreCostChangedItem && itemDTO.getOverrideRegPrice() == null && !isPrePrice
							&& !isLockPrice && !isMinMax && !itemDTO.getIsPriceAdjusted()
							&& !itemDTO.getIsPartOfSubstituteGroup() && (predictedMov != null && predictedMov > 0)) {
						canConsiderItem = true;
					}
				}

				if (canConsiderItem) {
					if (itemDTO.getRetLirId() > 0) {
						ligsToConsider.put(itemDTO.getRetLirId(), 0);
					} else {
						nonLigsToConsider.put(itemDTO.getItemCode(), 0);
					}
				} else {
					//if(itemDTO.getIsPartOfSubstituteGroup())
						//logger.debug("Substitute Item is Ignored: " + itemDTO.getItemCode());
					// if any one of the member cost is changed, it will be
					// captured here
					if (itemDTO.getRetLirId() > 0) {
						ligsToIgnore.put(itemDTO.getRetLirId(), 0);
					}
				}
			}
		}
		
		//lig's
		for (Integer retLirId : ligsToConsider.keySet()) {
			// See it is not in the ignore list
			if (ligsToIgnore.get(retLirId) == null) {
				PRItemDTO itemDTO = new PRItemDTO();
				itemDTO.setRetLirId(retLirId);
				itemDTO.setLir(true);
				opportunityItems.add(itemDTO);
			}
		}
		//non lig's
		for(Integer itemCode : nonLigsToConsider.keySet()){
			PRItemDTO itemDTO = new PRItemDTO();
			itemDTO.setItemCode(itemCode);
			opportunityItems.add(itemDTO);
		}
		
		//Will have ret lir id or item code alone here
		return opportunityItems;
	}
	
	private String adjustRecommendedPriceForPI(Connection conn, int priceChgInd, List<PRItemDTO> allItemsOfRun,
			List<PRItemDTO> opportunityItems, List<PRItemDTO> priceAdjustedItems,
			PRGuidelinePI categoryLevelPriceIndex, PRRecommendationRunHeader recommendationRunHeader,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, boolean isUsePrediction,
			Boolean isMarginRateAlreadyMeet, PRGuidelineMargin categoryLevelMarginRate) throws GeneralException {
		String noOfPriceAdjustment = "";
		Integer allAdjustedItem = 0;
		Integer distinctAdjustedItem = 0;
		
		// Pick item first which has lesser margin impact
		sortByMarginBenefit(conn, priceChgInd, allItemsOfRun, opportunityItems, recommendationRunHeader, predictions, isUsePrediction);
		
		for (PRItemDTO opportunityItem : opportunityItems) {

			List<PRItemDTO> tempAdjustedItem = adjustPrice(priceChgInd, opportunityItem, allItemsOfRun,
					categoryLevelPriceIndex, categoryLevelMarginRate, ADJUSTMENT_TYPE_PI, isMarginRateAlreadyMeet);
			
			double newMarginRate = computeFutureMarginRate(allItemsOfRun, recommendationRunHeader, predictions,
					isUsePrediction);
			logger.debug("New margin rate after pi adjustment: " + newMarginRate);
			//If margin rate is already above the expectation, then make sure the price changes to balance index
			//doesn't break the margin rate at category level
			if (isMarginRateAlreadyMeet != null && isMarginRateAlreadyMeet
					&& !isMarginRateMeetsGoal(newMarginRate, categoryLevelMarginRate)) {
				// rollback the prices and remove it from priceAdjustedItems
				rollbackAdjustedPrice(tempAdjustedItem);
				logger.debug("Future PI adjustment is stopped as it will break the Margin Rate expectataion: "
						+ ". Also Margin Rate is above expectation"
						+ ".Margin Rate after adjustment: "
						+ newMarginRate
						+ " ,Target Rate:"
						+ (categoryLevelMarginRate != null ? (categoryLevelMarginRate.getMinMarginPct() + " to " + categoryLevelMarginRate
								.getMaxMarginPct()) : " No Margin Rate Goal Found."));
				break;
			} else {
				priceAdjustedItems.addAll(tempAdjustedItem);
				String adjustCnt = getPriceAdjustedCount(tempAdjustedItem);
				distinctAdjustedItem = distinctAdjustedItem + Integer.valueOf(adjustCnt.split(",")[0]);
				allAdjustedItem = allAdjustedItem + Integer.valueOf(adjustCnt.split(",")[1]);
				// Find the Price Index of the Category
				double newPriceIndex = computePriceIndex(allItemsOfRun);

				logger.debug("PI after price adjustment of opportuinty Item:"
						+ (opportunityItem.getRetLirId() > 0 ? "RetLirId:" + opportunityItem.getRetLirId()
								: "ItemCode:" + opportunityItem.getItemCode()) + "-->" + newPriceIndex + ". Target:"
						+ "Min:" + categoryLevelPriceIndex.getMinValue() + ",Max:"
						+ categoryLevelPriceIndex.getMaxValue());
				// Check if Price Index goal is meet
				if (isPriceIndexMeetsGoal(newPriceIndex, categoryLevelPriceIndex)) {
					// Come out of the loop (no more price changes)
					logger.debug("PI Goal Meet. No further price changes");
					break;
				}
			}
		}
		
		noOfPriceAdjustment = distinctAdjustedItem + "(" + allAdjustedItem + ")";
		return noOfPriceAdjustment;
	}
	
	//Rollback the adjusted price back to its original price
	//If it is a lig, it will do for all lig mem & lig, otherwise just the item
	private void rollbackAdjustedPrice(List<PRItemDTO> adjustedItems) {
		//Rollback prices
		for (PRItemDTO itemDTO : adjustedItems) {			
			itemDTO.setRecommendedRegPrice(itemDTO.getRecPriceBeforeAdjustment());
			itemDTO.setRecPriceBeforeAdjustment(null);
			itemDTO.setIsPriceAdjusted(false);
		}
	}

	private String adjustRecommendedPriceForMarginRate(Connection conn, List<PRItemDTO> allItemsOfRun,
			List<PRItemDTO> opportunityItems, List<PRItemDTO> priceAdjustedItems,
			PRGuidelineMargin categoryLevelMarginRate, PRRecommendationRunHeader recommendationRunHeader,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, boolean isUsePrediction,
			Boolean isMarginRateAlreadyMeet, double curPI, PRGuidelinePI catLevelPIGuideline, String costChangeLogic)
			throws GeneralException {
		String noOfPriceAdjustment = "";
		Integer allAdjustedItem = 0;
		Integer distinctAdjustedItem = 0;
		
		sortByMarginBenefit(conn, 1, allItemsOfRun, opportunityItems, recommendationRunHeader, predictions, isUsePrediction);	
		
		String adjustCnt = adjustPriceToAchieveMarginRate(opportunityItems, allItemsOfRun, catLevelPIGuideline,
				categoryLevelMarginRate, isMarginRateAlreadyMeet, priceAdjustedItems, recommendationRunHeader,
				predictions, isUsePrediction, false);
		distinctAdjustedItem = distinctAdjustedItem + Integer.valueOf(adjustCnt.split(",")[0]);
		allAdjustedItem = allAdjustedItem + Integer.valueOf(adjustCnt.split(",")[1]);
		
		logger.debug("Adjusting Margin Rate with PI opportunity items is started...");
		// Check if PI is meeting Goal and MarginRate is not meeting goal 
		// If above condition satisfies then try to reduce PI to get better MarginRate 
		if (catLevelPIGuideline != null) {
			double newMarginRate = computeFutureMarginRate(allItemsOfRun, recommendationRunHeader, predictions,
					isUsePrediction);
			if (!isMarginRateMeetsGoal(newMarginRate, categoryLevelMarginRate)
					&& isPriceIndexMeetsGoal(curPI, catLevelPIGuideline)) {
				List<PRItemDTO> piOpportunityItems = new ArrayList<PRItemDTO>();
				// Pick opportunity items of PI
				piOpportunityItems = getOpportunityItems(allItemsOfRun, costChangeLogic, ADJUSTMENT_TYPE_PI,
						recommendationRunHeader, predictions, isUsePrediction);
				// Sort by margin opportunity
				sortByMarginBenefit(conn, 1, allItemsOfRun, (List<PRItemDTO>) piOpportunityItems, recommendationRunHeader,
						predictions, isUsePrediction);

				adjustCnt = adjustPriceToAchieveMarginRate(piOpportunityItems, allItemsOfRun, catLevelPIGuideline,
						categoryLevelMarginRate, isMarginRateAlreadyMeet, priceAdjustedItems, recommendationRunHeader,
						predictions, isUsePrediction, true);
				distinctAdjustedItem = distinctAdjustedItem + Integer.valueOf(adjustCnt.split(",")[0]);
				allAdjustedItem = allAdjustedItem + Integer.valueOf(adjustCnt.split(",")[1]);
			}
		}
		logger.debug("Adjusting Margin Rate with PI opportunity items is completed...");
		
		noOfPriceAdjustment = distinctAdjustedItem + "(" + allAdjustedItem + ")";
		return noOfPriceAdjustment;
	}
	
	private String adjustPriceToAchieveMarginRate(List<PRItemDTO> opportunityItems, List<PRItemDTO> allItemsOfRun,
			PRGuidelinePI catLevelPIGuideline, PRGuidelineMargin categoryLevelMarginRate,
			Boolean isMarginRateAlreadyMeet, List<PRItemDTO> priceAdjustedItems,
			PRRecommendationRunHeader recommendationRunHeader,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, boolean isUsePrediction,
			boolean isAdjustWithPIItems) {

		double newMarginRate = 0;
		String finalAdjustCnt = "";
		Integer distinctAdjustedItem = 0;
		Integer allAdjustedItem = 0;
		
		for (PRItemDTO opportunityItem : opportunityItems) {
			// If the new price is going to result in decrease in margin $, then
			// do consider that item
			if (opportunityItem.getSumOfDifference() >= 0) {
				List<PRItemDTO> tempAdjustedItem = adjustPrice(1, opportunityItem, allItemsOfRun, null,
						categoryLevelMarginRate, ADJUSTMENT_TYPE_MAR_RATE, isMarginRateAlreadyMeet);

				double newPI = computePriceIndex(allItemsOfRun);
				
				//logger.debug("New margin rate after pi adjustment: " + newMarginRate);
				
				//IF PI is already meet, and margin rate is not meet in first phase,
				//then in seconde phase, pi items are considered and its adjusted till it
				//doesn't break the category level pi or margin rate is meet
				if (isAdjustWithPIItems && !isPriceIndexMeetsGoal(newPI, catLevelPIGuideline)) {
					// rollback the prices and remove it from priceAdjustedItems
					rollbackAdjustedPrice(tempAdjustedItem);
					logger.debug("Future Margin Rate adjustment is stopped as it will break the PI expectataion: "
							+ ".PI Rate after adjustment: "
							+ newPI
							+ " ,Target PI:"
							+ (catLevelPIGuideline != null ? (catLevelPIGuideline.getMinValue() + " to " + catLevelPIGuideline
									.getMaxValue()) : " No PI Goal Found."));
					break;
				} else {
					priceAdjustedItems.addAll(tempAdjustedItem);
					String adjustCnt = getPriceAdjustedCount(tempAdjustedItem);
					distinctAdjustedItem = distinctAdjustedItem + Integer.valueOf(adjustCnt.split(",")[0]);
					allAdjustedItem = allAdjustedItem + Integer.valueOf(adjustCnt.split(",")[1]);

					// Find the Margin Rate of the Category
					newMarginRate = computeFutureMarginRate(allItemsOfRun, recommendationRunHeader, predictions,
							isUsePrediction);

					logger.debug("Margin Rate after price adjustment of opportuinty Item:"
							+ (opportunityItem.getRetLirId() > 0 ? "RetLirId:" + opportunityItem.getRetLirId()
									: "ItemCode:" + opportunityItem.getItemCode()) + "-->" + newMarginRate
							+ ". Target:" + "Min:" + categoryLevelMarginRate.getMinMarginPct() + ",Max:"
							+ categoryLevelMarginRate.getMaxMarginPct());

					// Check if Margin Rate goal is meet
					if (isMarginRateMeetsGoal(newMarginRate, categoryLevelMarginRate)) {
						// Come out of the loop (no more price changes)
						logger.debug("Margin Rate Goal Meet. No further price changes");
						break;
					}
				}
			}
		}
		
		finalAdjustCnt = distinctAdjustedItem + "," + allAdjustedItem;
		return finalAdjustCnt;
	}
	
	/***
	 * Sort the items which has more beneficial in terms of margin $
	 * @param conn
	 * @param priceChgInd
	 * @param allItemsOfRun
	 * @param opportunityItems
	 * @param recommendationRunHeader
	 * @param predictions
	 * @param isUsePrediction
	 * @throws GeneralException
	 */
	private void sortByMarginBenefit(Connection conn, int priceChgInd, List<PRItemDTO> allItemsOfRun,
			List<PRItemDTO> opportunityItems, PRRecommendationRunHeader recommendationRunHeader,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, boolean isUsePrediction)
			throws GeneralException {

		// Find difference for lig
		for (PRItemDTO opportunityItem : opportunityItems) {
			Double recPriceMarginDollar = null, lowerHigherPriceMarginDollar = null;
			if (opportunityItem.getRetLirId() > 0) {
				// Get all its child
				for (PRItemDTO itemDTO : allItemsOfRun) {
					// ignore rep item
					if (itemDTO.getRetLirId() == opportunityItem.getRetLirId() && !itemDTO.isLir()) {
						double newPricePoint = getNewPricePoint(priceChgInd, itemDTO);
						Double localRecMarDollar, localLowerHigherMarDollar;
						Double cost = itemDTO.getCost();
						
//						localRecMarDollar = calculatePredMarginDollar(recommendationRunHeader,
//								itemDTO.getItemCode(), itemDTO.getRetLirId(), itemDTO.getRecommendedRegMultiple(),
//								itemDTO.getRecommendedRegPrice(), cost, predictions, isUsePrediction, itemDTO);
//						
//						localLowerHigherMarDollar = calculatePredMarginDollar(recommendationRunHeader,
//								itemDTO.getItemCode(), itemDTO.getRetLirId(), itemDTO.getRecommendedRegMultiple(),
//								newPricePoint, cost, predictions, isUsePrediction, itemDTO);
						
						localRecMarDollar = calculatePredMarginDollar(recommendationRunHeader, itemDTO.getItemCode(), itemDTO.getRetLirId(),
								itemDTO.getRecommendedRegPrice(), cost, predictions, isUsePrediction, itemDTO);

						localLowerHigherMarDollar = calculatePredMarginDollar(recommendationRunHeader, itemDTO.getItemCode(), itemDTO.getRetLirId(),
								new MultiplePrice(itemDTO.getRecommendedRegPrice().multiple, newPricePoint), cost, predictions, isUsePrediction,
								itemDTO);

						if (localRecMarDollar != null) {
							recPriceMarginDollar = (recPriceMarginDollar == null ? 0 : recPriceMarginDollar); 
							recPriceMarginDollar  = recPriceMarginDollar + localRecMarDollar;
						}
						if (localLowerHigherMarDollar != null) {
							lowerHigherPriceMarginDollar = (lowerHigherPriceMarginDollar == null ? 0 : lowerHigherPriceMarginDollar);
							lowerHigherPriceMarginDollar  = lowerHigherPriceMarginDollar + localLowerHigherMarDollar;
						}
						
						
					}
				}
				//If there is 
			} else {
				// non lig's
				for (PRItemDTO itemDTO : allItemsOfRun) {
					if (itemDTO.getItemCode() == opportunityItem.getItemCode()) {
						double newPricePoint = getNewPricePoint(priceChgInd, itemDTO);
						Double cost = itemDTO.getCost();
						
//						recPriceMarginDollar = calculatePredMarginDollar(recommendationRunHeader,
//								itemDTO.getItemCode(), itemDTO.getRetLirId(), itemDTO.getRecommendedRegMultiple(),
//								itemDTO.getRecommendedRegPrice(), cost, predictions, isUsePrediction, itemDTO);
//						
//						lowerHigherPriceMarginDollar = calculatePredMarginDollar(recommendationRunHeader,
//								itemDTO.getItemCode(), itemDTO.getRetLirId(), itemDTO.getRecommendedRegMultiple(),
//								newPricePoint, cost, predictions, isUsePrediction, itemDTO);
						
						recPriceMarginDollar = calculatePredMarginDollar(recommendationRunHeader, itemDTO.getItemCode(), itemDTO.getRetLirId(),
								itemDTO.getRecommendedRegPrice(), cost, predictions, isUsePrediction, itemDTO);

						lowerHigherPriceMarginDollar = calculatePredMarginDollar(recommendationRunHeader, itemDTO.getItemCode(),
								itemDTO.getRetLirId(), new MultiplePrice(itemDTO.getRecommendedRegPrice().multiple, newPricePoint), cost, predictions,
								isUsePrediction, itemDTO);
						
						break;
					}
				}
			}
			
			//If there is no predicted movement for the lig/ non-lig, gives those items least priority
			if (recPriceMarginDollar != null && lowerHigherPriceMarginDollar != null) {
				double marginDiff = 0;
				marginDiff = lowerHigherPriceMarginDollar - recPriceMarginDollar;
				opportunityItem.setSumOfDifference(marginDiff);
			} else {
				// Larger -negative no is set, so it is given least priority.
				// Code should not reach here, as items with no movement are already filtered in getopportunity function
				opportunityItem.setSumOfDifference(-999999d);
			}
			logger.debug((opportunityItem.getRetLirId() > 0 ? "RetLirId:" + opportunityItem.getRetLirId() : "ItemCode:"
					+ opportunityItem.getItemCode())
					+ ",Rec Price Margin:"
					+ recPriceMarginDollar
					+ ",LowerHigher Margin:"
					+ lowerHigherPriceMarginDollar + ",Margin Diff:" + opportunityItem.getSumOfDifference());
		}

		//Sort the items by the difference in descending order.
		Collections.sort(opportunityItems, new SumOfDifferenceComparator());
		
		/**Logging **/
		logger.debug("*** Opportunity item sorted by Margin Benefit ***");
		for(PRItemDTO itemDTO : opportunityItems){
			logger.debug((itemDTO.getRetLirId() > 0 ? "RetLirId:" + itemDTO.getRetLirId() : "ItemCode:"
					+ itemDTO.getItemCode()) + ",Margin Diff:" + itemDTO.getSumOfDifference());
		}
		logger.debug("*** Opportunity item sorted by Margin Benefit ***");
	}
	
	/***
	 * Determine whether a LIR/Item can be adjusted based on multiple criteria
	 * @param adjustType
	 * @param piGuideline
	 * @param marginGuideline
	 * @param opportunityItem
	 * @param allItemsOfRun
	 * @param priceChgInd
	 * @param isMarginRateAlreadyMeet
	 * @return
	 */
	private boolean isItemOrLirCanBeAdjusted(String adjustType, PRGuidelinePI piGuideline,
			PRGuidelineMargin marginGuideline, PRItemDTO opportunityItem, List<PRItemDTO> allItemsOfRun,
			int priceChgInd, Boolean isMarginRateAlreadyMeet) {
		List<PRItemDTO> nonLigOrLigMembers = new ArrayList<PRItemDTO>();
		boolean canConsiderItem = true;
		PricingEngineService pricingEngineService = new PricingEngineService();
		PriceIndexCalculation piCalculation = new PriceIndexCalculation();
		
		// If it is a lig
		if (opportunityItem.getRetLirId() > 0) {
			for (PRItemDTO itemDTO : allItemsOfRun) {
				if (itemDTO.getRetLirId() == opportunityItem.getRetLirId() && !itemDTO.isLir()) {
					nonLigOrLigMembers.add(itemDTO);
				}
			}
		} else {
			for (PRItemDTO itemDTO : allItemsOfRun) {
				if(itemDTO.getItemCode() == opportunityItem.getItemCode() && !itemDTO.isLir())
					nonLigOrLigMembers.add(itemDTO);
			}
		}
		
		//Even if one of the item is not satisfying then ignore entire list
		for (PRItemDTO itemDTO : nonLigOrLigMembers) {
			double newPricePoint = getNewPricePoint(priceChgInd, itemDTO);
			if (newPricePoint > 0) {
				PRItemDTO copyItem = new PRItemDTO();
				copyItem.setExplainLog(itemDTO.getExplainLog());
//				copyItem.setRecommendedRegMultiple(itemDTO.getRecommendedRegMultiple());
//				copyItem.setRecommendedRegPrice(newPricePoint);
				copyItem.setRecommendedRegPrice(new MultiplePrice(itemDTO.getRecommendedRegPrice().multiple, newPricePoint));
				// 1. Don't Pick item
				// If item is going to break cost/lower/higher/threshold
				// constraint
				if (pricingEngineService.isCostOrLowerHigherOrThresholdBroken(copyItem)) {
					canConsiderItem = false;
					logger.debug("Item is ignored as adjustment is going to break constraints: "
							+ itemDTO.getItemCode() + ", Ret Lir Id: " + itemDTO.getRetLirId());
					break;
					// While adjusting item to balance margin, Ignore if the item is
					// already meeting the category level margin goal
				} else if (adjustType == ADJUSTMENT_TYPE_MAR_RATE && isItemMeetsMarginRate(marginGuideline, itemDTO)) {
					canConsiderItem = false;
					logger.debug("Item is ignored as it is already meeting the category level Margin Rate goal: "
							+ itemDTO.getItemCode() + ", Ret Lir Id: " + itemDTO.getRetLirId());
					break;
					// While adjusting item to balance PI, 
				} else if (adjustType == ADJUSTMENT_TYPE_PI) {
					//When margin guideline is not defined at category level
					if(isMarginRateAlreadyMeet == null){
						if(piCalculation.isItemMeetFutureSimpleIndex(itemDTO, piGuideline)) {
							logger.debug("Item is ignored as it is already meeting the category level PI goal: "
									+ itemDTO.getItemCode() + ", Ret Lir Id: " + itemDTO.getRetLirId());
							canConsiderItem = false;
							break;
						}
					}
					//While adjusting price to balance PI, Ignore if the item is already meeting the category level PI goal
					else if (isMarginRateAlreadyMeet && piCalculation.isItemMeetFutureSimpleIndex(itemDTO, piGuideline)) {
						canConsiderItem = false;
						logger.debug("Item is ignored as it is already meeting the category level PI goal: "
								+ itemDTO.getItemCode() + ", Ret Lir Id: " + itemDTO.getRetLirId()
								+ ". Also Margin Rate is above expectation");
						break;
					} else if (!isMarginRateAlreadyMeet && (isItemMeetsMarginRate(marginGuideline, itemDTO) ||
							piCalculation.isItemMeetFutureSimpleIndex(itemDTO, piGuideline))) {
						//While adjusting price to balance PI, if the margin rate goal is not meet, then don't pick
						//items which is already meeting the margin rate or item which is already meeting the index goal
						canConsiderItem = false;
						logger.debug("Item is ignored as it is already meeting the category level PI or Margin goal: "
								+ itemDTO.getItemCode() + ", Ret Lir Id: " + itemDTO.getRetLirId()
								+ ". Also Margin Rate is under expectation");
						break;
					}
				}
			} else {
				canConsiderItem = false;
				break;
			}
			 
		}
		return canConsiderItem;
	}
	
	/***
	 * Adjust the price of each item
	 * @param priceChgInd
	 * @param opportunityItem
	 * @param allItemsOfRun
	 * @param priceAdjustedItems
	 * @param distinctRetLirPriceAdjusted
	 * @param piGuideline
	 * @param marginGuideline
	 * @param adjustType
	 * @return
	 */
	private List<PRItemDTO> adjustPrice(int priceChgInd, PRItemDTO opportunityItem, List<PRItemDTO> allItemsOfRun,
			PRGuidelinePI piGuideline, PRGuidelineMargin marginGuideline, String adjustType,
			Boolean isMarginRateAlreadyMeet) {
		List<PRItemDTO> nonLigOrLigMembers = new ArrayList<PRItemDTO>();
		List<PRItemDTO> priceAdjustedItems = new ArrayList<PRItemDTO>();
		
		boolean canItemOrLigConsidered = isItemOrLirCanBeAdjusted(adjustType, piGuideline, marginGuideline,
				opportunityItem, allItemsOfRun, priceChgInd, isMarginRateAlreadyMeet);
		if (canItemOrLigConsidered) {
			if (opportunityItem.getRetLirId() > 0) {
				for (PRItemDTO itemDTO : allItemsOfRun) {
					//both lig and its members
					if (itemDTO.getRetLirId() == opportunityItem.getRetLirId()
							|| (itemDTO.getItemCode() == opportunityItem.getRetLirId() && itemDTO.isLir())) {
						nonLigOrLigMembers.add(itemDTO);
					}
				}
			} else {
				for (PRItemDTO itemDTO : allItemsOfRun) {
					if(itemDTO.getItemCode() == opportunityItem.getItemCode() && !itemDTO.isLir())
						nonLigOrLigMembers.add(itemDTO);
				}
			}

			for (PRItemDTO itemDTO : nonLigOrLigMembers) {
				double newPricePoint = getNewPricePoint(priceChgInd, itemDTO);
				itemDTO.setRecPriceBeforeAdjustment(itemDTO.getRecommendedRegPrice());
//				itemDTO.setRecommendedRegPrice(newPricePoint);
				itemDTO.setRecommendedRegPrice(new MultiplePrice(itemDTO.getRecommendedRegPrice().multiple, newPricePoint));
				itemDTO.setIsPriceAdjusted(true);
				priceAdjustedItems.add(itemDTO);
			}
		} else {
			logger.debug("Opportunity Item : "
					+ (opportunityItem.getRetLirId() > 0 ? "LIG:" + opportunityItem.getRetLirId() : "Item: "
							+ opportunityItem) + " is ignored ");
		}
		
		return priceAdjustedItems;
	}

	private String getPriceAdjustedCount(List<PRItemDTO> adjustedItems){
		String adjustedCnt = "";
		Integer allAdjustedItem = 0, distinctAdjustedItem = 0;
		for (PRItemDTO itemDTO : adjustedItems) {			
			if (!itemDTO.isLir())
				allAdjustedItem = allAdjustedItem + 1;
			
			if (itemDTO.getRetLirId() < 1)
				distinctAdjustedItem = distinctAdjustedItem + 1;

		}
		adjustedCnt = distinctAdjustedItem + "," + allAdjustedItem;
		return adjustedCnt;
	}
	
	private Double computeFutureMarginRate(List<PRItemDTO> allItemsOfRun,
			PRRecommendationRunHeader recommendationRunHeader,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, boolean isUsePrediction) {
		PricingEngineService pricingEngineService = new PricingEngineService();
		double marginRate = 0;
		double sumOfPrice = 0, sumOfCost = 0;
		
		for (PRItemDTO item : allItemsOfRun) {
//			MultiplePrice multiplePrice = new MultiplePrice(item.getRecommendedRegMultiple(), item.getRecommendedRegPrice());
			MultiplePrice multiplePrice = item.getRecommendedRegPrice();
			double unitPrice = PRCommonUtil.getUnitPrice(multiplePrice, true) ;
			Long predictedMov = null;
			Double cost = item.getCost();
			
			//For items not adjusted, use predicted movement from recommendation table itself
			if (item.getIsPriceAdjusted()) {
//				predictedMov = pricingEngineService.getPredictedMov(item.getItemCode(),
//						recommendationRunHeader.getLocationLevelId(), recommendationRunHeader.getLocationId(),
//						item.getRecommendedRegMultiple(), item.getRecommendedRegPrice(), predictions, isUsePrediction);
				
				predictedMov = pricingEngineService.getPredictedMov(item.getItemCode(), recommendationRunHeader.getLocationLevelId(),
						recommendationRunHeader.getLocationId(), item.getRecommendedRegPrice(), predictions, isUsePrediction);
				
			} else {
				predictedMov = item.getPredictedMovement() != null ? Math.round(item.getPredictedMovement())  : 0;
			}
			
//			if (!item.isLir())
//				logger.debug("Item Code:" + item.getItemCode() + ",cost:" + cost + ",price:" + unitPrice + ",pred mov:"
//						+ predictedMov);
			if ((!item.isLir()) && unitPrice > 0 && cost != null && cost > 0 && predictedMov != null
					&& predictedMov > 0) {
				
				sumOfPrice = sumOfPrice + (unitPrice * predictedMov);
				sumOfCost = sumOfCost + (cost * predictedMov);
			}
		}
		logger.debug("Sum of Price: " + sumOfPrice + ", Sum of Cost: " + sumOfCost);
		if(sumOfPrice > 0)
			marginRate = ((sumOfPrice - sumOfCost) / sumOfPrice) * 100;
		
		return marginRate;
	}
	
	private Boolean isMarginRateMeetsGoal(double curMarginRate, PRGuidelineMargin guidelineMargin) {
		Boolean isMarginRateMeetsGoal = null;

		if (guidelineMargin != null) {
			isMarginRateMeetsGoal = true;
			// Min alone present
			if (guidelineMargin.getMinMarginPct() != Constants.DEFAULT_NA
					&& guidelineMargin.getMaxMarginPct() == Constants.DEFAULT_NA) {
				if (curMarginRate < guidelineMargin.getMinMarginPct()) {
					isMarginRateMeetsGoal = false;
				}
			} else if (guidelineMargin.getMaxMarginPct() != Constants.DEFAULT_NA
					&& guidelineMargin.getMinMarginPct() == Constants.DEFAULT_NA) {
				// Max alone present (margin is not pushed down)
				// if(curMarginRate > guidelineMargin.getMaxMarginPct()){
				if (curMarginRate < guidelineMargin.getMaxMarginPct()) {
					isMarginRateMeetsGoal = false;
				}
			} else if ((guidelineMargin.getMinMarginPct() != Constants.DEFAULT_NA && guidelineMargin.getMaxMarginPct() != Constants.DEFAULT_NA)
					&& (guidelineMargin.getMinMarginPct() == guidelineMargin.getMaxMarginPct())) {
				// Both present -- Equals (margin is not pushed down)
				// if(curMarginRate == guidelineMargin.getMinMarginPct()){
				if (curMarginRate < guidelineMargin.getMinMarginPct()) {
					isMarginRateMeetsGoal = false;
				}
			} else if (guidelineMargin.getMinMarginPct() != Constants.DEFAULT_NA
					&& guidelineMargin.getMaxMarginPct() != Constants.DEFAULT_NA) {
				// Both present (margin is not pushed down)
				// if(curMarginRate < guidelineMargin.getMinMarginPct() ||
				// curMarginRate > guidelineMargin.getMaxMarginPct()){
				if (curMarginRate < guidelineMargin.getMinMarginPct()) {
					isMarginRateMeetsGoal = false;
				}
			}
		}  

		return isMarginRateMeetsGoal;
	}
	
	private Boolean isItemMeetsMarginRate(PRGuidelineMargin marginGuideline, PRItemDTO item) {
		Boolean isItemMeetsMarginRate = false;

//		if (item.getRecommendedRegPrice() != null && item.getCost() != null && item.getRecommendedRegPrice() > 0
//				&& item.getCost() > 0) {
		if (item.getRecommendedRegPrice() != null && item.getCost() != null && item.getRecommendedRegPrice().price > 0
				&& item.getCost() > 0) {
			double itemLevelMarginRate;
//			MultiplePrice multiplePrice = new MultiplePrice(item.getRecommendedRegMultiple(),
//					item.getRecommendedRegPrice());
			MultiplePrice multiplePrice = item.getRecommendedRegPrice();
			double unitPrice = PRCommonUtil.getUnitPrice(multiplePrice, true);

			itemLevelMarginRate = ((unitPrice - item.getCost()) / unitPrice) * 100;
			isItemMeetsMarginRate = isMarginRateMeetsGoal(itemLevelMarginRate, marginGuideline);
			
			logger.debug("Item Level Margin Rate: "
					+ itemLevelMarginRate
					+ " ,Target Rate:"
					+ (marginGuideline != null ? (marginGuideline.getMinMarginPct() + " to " + marginGuideline
							.getMaxMarginPct()) : " No Margin Rate Goal Found.") + ",Item Margin Meet Status:"
					+ isItemMeetsMarginRate);
		} else {
			// don't consider item which doesn't have cost/price, as margin rate
			// can't be found for this
			isItemMeetsMarginRate = true;
		}
		return isItemMeetsMarginRate;
	}
	
//	private Double calculatePredMarginDollar(PRRecommendationRunHeader recommendationRunHeader, int itemCode,
//			int retLirId, int multiple, Double price, Double cost,
//			HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, boolean isUsePrediction,
//			PRItemDTO item) {
	private Double calculatePredMarginDollar(PRRecommendationRunHeader recommendationRunHeader, int itemCode, int retLirId,
			MultiplePrice multiplePrice, Double cost, HashMap<PredictionDetailKey, PredictionDetailDTO> predictions, boolean isUsePrediction,
			PRItemDTO item) {
		Double marginDollar = null;
		Long predictedMov = null;
		PricingEngineService pricingEngineService = new PricingEngineService();

		//if (item.getIsPriceAdjusted()) {
//			predictedMov = pricingEngineService.getPredictedMov(itemCode, recommendationRunHeader.getLocationLevelId(),
//					recommendationRunHeader.getLocationId(), multiple, price, predictions, isUsePrediction);
			
			predictedMov = pricingEngineService.getPredictedMov(itemCode, recommendationRunHeader.getLocationLevelId(),
					recommendationRunHeader.getLocationId(), multiplePrice, predictions, isUsePrediction);
			
//		} else {
//			predictedMov = item.getPredictedMovement() != null ? Math.round(item.getPredictedMovement()) : 0;
//		}
		
		// If prediction available for both
		if (predictedMov != null && cost != null && cost > 0) {
//			marginDollar = (price * predictedMov) - (cost * predictedMov);
			marginDollar = (multiplePrice.price * predictedMov) - (cost * predictedMov);
		}

		logger.debug("ItemCode:" + itemCode + ",RetLirId:" + retLirId + ",Price: " + multiplePrice.price + ",Prediced Mov:"
				+ predictedMov + ", Cost: " + cost + ",Margin:" + marginDollar);

		return marginDollar;
	}
	
	private double getNewPricePoint(int priceChgInd, PRItemDTO itemDTO){
		double newPricePoint = 0;
		
		if (priceChgInd == 1) {
//			newPricePoint = itemDTO.getStrategyDTO().getConstriants().getRoundingConstraint()
//					.roundupPriceToNextRoundingDigit(itemDTO.getRecommendedRegPrice() + 0.01);
			newPricePoint = itemDTO.getStrategyDTO().getConstriants().getRoundingConstraint()
					.roundupPriceToNextRoundingDigit(itemDTO.getRecommendedRegPrice().price + 0.01);
		}else if (priceChgInd == -1) {
//			newPricePoint = itemDTO.getStrategyDTO().getConstriants().getRoundingConstraint()
//					.roundupPriceToPreviousRoundingDigit(itemDTO.getRecommendedRegPrice() - 0.01);
			newPricePoint = itemDTO.getStrategyDTO().getConstriants().getRoundingConstraint()
					.roundupPriceToPreviousRoundingDigit(itemDTO.getRecommendedRegPrice().price - 0.01);
		}
		
		return newPricePoint;
	}
	
	//Note:: The items passed to this function must be of one run, otherwise it will not work
	private void updateConflicts(Long runId, List<PRItemDTO> priceAdjustedItems) throws GeneralException{
		//This function may need changes, when opportunity items includes cost changed items, price group items
		//as conflict works little different for cost changed and price group items
		HashMap<Integer, Integer> distinctRetLirId = new HashMap<Integer, Integer>();
		PricingEngineService pricingEngineService = new PricingEngineService();
		PRItemDTO ligRepresentingItem = null;
		LIGConstraint ligConstraint = new LIGConstraint();
		List<PRItemDTO> priceAdjustedItemsOfRun = new ArrayList<PRItemDTO>();
		HashMap<Integer, Integer> retLirIdAndItsExplainLogItem = new HashMap<Integer, Integer>();
		
		for (PRItemDTO itemDTO : priceAdjustedItems) {
			if (itemDTO.getRunId() == runId) {
				priceAdjustedItemsOfRun.add(itemDTO);
				// get distinct ret lir id
				if (itemDTO.getRetLirId() > 0) {
					distinctRetLirId.put(itemDTO.getRetLirId(), 0);
				}
			}
		}
		
		//Lig level explain log is one of its lig member, find which member log is used at lig
		//After the conflict are updated, pick that log and update to lig
		for (Integer retLirId : distinctRetLirId.keySet()) {
			List<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
			ligMembers = getLigMembers(retLirId, priceAdjustedItemsOfRun);
			ligRepresentingItem = getLigRepresentingItem(retLirId, priceAdjustedItemsOfRun);
			if (ligRepresentingItem != null){
				int itemCode = ligConstraint.getItemCodeWhoseLogIsUsedAsLigLevelLog(ligRepresentingItem, ligMembers);
				retLirIdAndItsExplainLogItem.put(retLirId, itemCode);
			}
		}
		
		for (PRItemDTO itemDTO : priceAdjustedItemsOfRun) {
			// process all items
			if(!itemDTO.isLir())
				pricingEngineService.updateConflictsExceptRounding(itemDTO);
		}
		
		//If it is lig representing mark it as conflict even one of its member is conflict
		for (Integer retLirId : distinctRetLirId.keySet()) {
			List<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
			ligMembers = getLigMembers(retLirId, priceAdjustedItemsOfRun);
			ligRepresentingItem = getLigRepresentingItem(retLirId, priceAdjustedItemsOfRun);
			if (ligRepresentingItem != null){
				ligRepresentingItem = ligConstraint.updateLIGExplainLog(retLirIdAndItsExplainLogItem.get(retLirId),
						ligRepresentingItem, ligMembers);
				ligRepresentingItem = ligConstraint.updateLIGConflict(ligRepresentingItem, ligMembers);
			}
		}
	}
	
	private void updateMarginOpportunity(Long runId, List<PRItemDTO> priceAdjustedItems,
			PRRecommendationRunHeader recommendationRunHeader) {
		List<PRItemDTO> priceAdjustedItemsOfRun = new ArrayList<PRItemDTO>();
		HashMap<Integer, Integer> distinctRetLirId = new HashMap<Integer, Integer>();
		PRItemDTO ligRepresentingItem = null;
		
		//Get only items of that run
		for (PRItemDTO itemDTO : priceAdjustedItems) {
			if (itemDTO.getRunId() == runId) {
				priceAdjustedItemsOfRun.add(itemDTO);
				// get distinct ret lir id
				if (itemDTO.getRetLirId() > 0) {
					distinctRetLirId.put(itemDTO.getRetLirId(), 0);
				}
			}
		}
		
		//Update margin oppr for non-lig 
		for (PRItemDTO itemDTO : priceAdjustedItemsOfRun) {
			if(!itemDTO.isLir() && itemDTO.getRetLirId() < 1){
				if (itemDTO.getIsOppurtunity() != null && itemDTO.getOppurtunityPrice() != null
						&& itemDTO.getIsOppurtunity().equals(String.valueOf(Constants.YES))
						&& itemDTO.getRecommendedRegPrice().price.equals(itemDTO.getOppurtunityPrice())) {
					itemDTO.setIsOppurtunity(null);
					itemDTO.setOppurtunityQty(null);
					itemDTO.setOppurtunityPrice(null);
				}			 
			}
		}
		
		//Update margin oppr for lig and lig members
		for (Integer retLirId : distinctRetLirId.keySet()) {
			List<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
			ligMembers = getLigMembers(retLirId, priceAdjustedItemsOfRun);
			ligRepresentingItem = getLigRepresentingItem(retLirId, priceAdjustedItemsOfRun);
			//If recommended price changed to margin oppor price
			if (ligRepresentingItem.getIsOppurtunity() != null && ligRepresentingItem.getOppurtunityPrice() != null
					&& ligRepresentingItem.getIsOppurtunity().equals(String.valueOf(Constants.YES))
					&& ligRepresentingItem.getIsOppurtunity().equals(String.valueOf(Constants.YES))
					&& ligRepresentingItem.getRecommendedRegPrice().price.equals(ligRepresentingItem.getOppurtunityPrice())) {
				ligRepresentingItem.setIsOppurtunity(null);
				ligRepresentingItem.setOppurtunityQty(null);
				ligRepresentingItem.setOppurtunityPrice(null);
				for (PRItemDTO ligMem : ligMembers) {
					ligMem.setIsOppurtunity(null);
					ligMem.setOppurtunityQty(null);
					ligMem.setOppurtunityPrice(null);
				}
			}
		}
	}
	
	//Note:: The items passed to this function must be of one run, otherwise it will not work
	private void updatePrediction(Connection conn, Long runId, List<PRItemDTO> priceAdjustedItems,
			PRRecommendationRunHeader recommendationRunHeader, HashMap<PredictionDetailKey, PredictionDetailDTO> predictions) throws GeneralException{
		HashMap<Integer, Integer> distinctRetLirId = new HashMap<Integer, Integer>();
		PRItemDTO ligRepresentingItem = null;
		LIGConstraint ligConstraint = new LIGConstraint();
		List<PRItemDTO> priceAdjustedItemsOfRun = new ArrayList<PRItemDTO>();
		
		//int locationLevelId = 0, locationId = 0, startCalendarId = 0, endCalendarId = 0;
		PredictionDetailKey predictionDetailKey;
		
		//Get only items of that run
		for (PRItemDTO itemDTO : priceAdjustedItems) {
			if (itemDTO.getRunId() == runId) {
				priceAdjustedItemsOfRun.add(itemDTO);
				// get distinct ret lir id
				if (itemDTO.getRetLirId() > 0) {
					distinctRetLirId.put(itemDTO.getRetLirId(), 0);
				}
			}
		}
		
		//Update items with prediction
		for (PRItemDTO itemDTO : priceAdjustedItemsOfRun) {
			if(!itemDTO.isLir()){
				//below variable is added to get the default values for parameter which is not applicable
				//here e.g. sale, page no, block no, display id, promo type id
				PredictionDetailDTO tempPredictionDetailDTO = new PredictionDetailDTO();
				
//				predictionDetailKey = new PredictionDetailKey(recommendationRunHeader.getLocationLevelId(),
//						recommendationRunHeader.getLocationId(), itemDTO.getItemCode(), 
//						itemDTO.getRecommendedRegMultiple(), itemDTO.getRecommendedRegPrice(), 
//						tempPredictionDetailDTO.getSaleQuantity(), tempPredictionDetailDTO.getSalePrice(),
//						tempPredictionDetailDTO.getAdPageNo(), tempPredictionDetailDTO.getBlockNo(),
//						tempPredictionDetailDTO.getPromoTypeId(), tempPredictionDetailDTO.getDisplayTypeId());
				predictionDetailKey = new PredictionDetailKey(recommendationRunHeader.getLocationLevelId(),
						recommendationRunHeader.getLocationId(), itemDTO.getItemCode(), 
						itemDTO.getRecommendedRegPrice().multiple, itemDTO.getRecommendedRegPrice().price, 
						tempPredictionDetailDTO.getSaleQuantity(), tempPredictionDetailDTO.getSalePrice(),
						tempPredictionDetailDTO.getAdPageNo(), tempPredictionDetailDTO.getBlockNo(),
						tempPredictionDetailDTO.getPromoTypeId(), tempPredictionDetailDTO.getDisplayTypeId());
				PredictionDetailDTO predictionDetailDTO = predictions.get(predictionDetailKey);
				if (predictionDetailDTO != null) {
					// if(predictionDetailDTO.getPredictedMovement() < 0)
					// itemDTO.setPredictedMovement(0d);
					// else
					itemDTO.setPredictedMovement(Double.valueOf(predictionDetailDTO.getPredictedMovement()));
					itemDTO.setPredictionStatus(PredictionStatus.get(predictionDetailDTO.getPredictionStatus())
							.getStatusCode());
				} else {
					itemDTO.setPredictedMovement(null);
					itemDTO.setPredictionStatus(null);
				}
			}
		}
		
		for (Integer retLirId : distinctRetLirId.keySet()) {
			List<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
			ligMembers = getLigMembers(retLirId, priceAdjustedItemsOfRun);
			ligRepresentingItem = getLigRepresentingItem(retLirId, priceAdjustedItemsOfRun);
			if (ligRepresentingItem != null){
				//Sum prediction at lig level
				ligRepresentingItem = ligConstraint.sumPredictionForLIG(ligRepresentingItem, ligMembers);
				ligRepresentingItem = ligConstraint.sumRecRetailSalesDollarForLIG(ligRepresentingItem, ligMembers);
				ligRepresentingItem = ligConstraint.sumRecRetailMarginDollarForLIG(ligRepresentingItem, ligMembers);
				//Update prediction status of lig
				ligRepresentingItem = ligConstraint.updatePredictionStatus(ligRepresentingItem, ligMembers);
			}
		}
	}
	
	private void updateAdjustedPriceToRecTable(Connection conn, List<PRItemDTO> priceAdjustedItems) throws GeneralException{
		//Update RECOMMENDED_REG_PRICE, IS_REC_PRICE_ADJUSTED, REC_PRICE_BEFORE_ADJUST, IS_CONFLICT, LOG, OVERRIDE_PRED_UPDATE_STATUS
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		pricingEngineDAO.updateRecTableAfterPriceAdjust(conn, priceAdjustedItems);
	}
	
	private List<PRItemDTO> getLigMembers(int retLirId, List<PRItemDTO> allItems){
		List<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
		for (PRItemDTO itemDTO : allItems) {
			if(itemDTO.getRetLirId() ==  retLirId && !itemDTO.isLir()){
				ligMembers.add(itemDTO);
			}
		}
		return ligMembers;
	}
	
	private PRItemDTO getLigRepresentingItem(int retLirId, List<PRItemDTO> allItems){
		PRItemDTO ligRepresentingItem = null;
		for (PRItemDTO itemDTO : allItems) {
			if(itemDTO.getItemCode() ==  retLirId && itemDTO.isLir()){
				ligRepresentingItem = itemDTO;
				break;
			}
		}
		return ligRepresentingItem;
	}
	
	public List<RunStatusDTO> balancePIAndMarginTest(Connection conn, String runIds) {
		List<RunStatusDTO> runStatusDTOs;
		this.conn = conn;
		runStatusDTOs = balancePIAndMargin(runIds);
		return runStatusDTOs;
	}
	
	public List<RunStatusDTO> rollbackAdjustedPriceTest(Connection conn, String runIds){
		List<RunStatusDTO> runStatusDTOs;
		this.conn = conn;
		runStatusDTOs = rollbackAdjustedPrice(runIds);
		return runStatusDTOs;
	}
	
	/**
	 * Initializes connection. Used when program is accessed through webservice
	 */
	protected void initializeForWS() {
		setConnection(getDSConnection());		
	}
	
	/**
	 * Returns Connection from datasource
	 * @return
	 */
	private Connection getDSConnection() {
		Connection connection = null;
		logger.info("WS Connection - " + PropertyManager.getProperty("WS_CONNECTION"));;
		try{
			if(ds == null){
				initContext = new InitialContext();
				envContext  = (Context)initContext.lookup("java:/comp/env");
				ds = (DataSource)envContext.lookup(PropertyManager.getProperty("WS_CONNECTION"));
			}
			connection = ds.getConnection();
		}catch(NamingException exception){
			logger.error("Error when creating connection from datasource " + exception.toString());
		}catch(SQLException exception){
			logger.error("Error when creating connection from datasource " + exception.toString());
		}
		return connection;
	}
	
	protected Connection getConnection(){
		return conn;
	}
	
	/**
	 * Sets database connection. Used when program runs in batch mode
	 */
	protected void setConnection(){
		if(conn == null){
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}
	
	protected void setConnection(Connection conn){
		this.conn = conn;
	}
}

class SumOfDifferenceComparator implements Comparator<PRItemDTO> {
    public int compare(PRItemDTO a, PRItemDTO b) {
    	return b.getSumOfDifference() > a.getSumOfDifference() ? 1 : (b.getSumOfDifference() < a.getSumOfDifference() ? -1 : 0);
    }
}
