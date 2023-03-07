package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pristine.dao.DBManager;
import com.pristine.dao.LocationCompetitorMapDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.DashboardDAO;
import com.pristine.dao.offermgmt.PriceGroupRelationDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.StrategyDAO;
import com.pristine.dao.offermgmt.SubstituteDAO;
import com.pristine.dataload.offermgmt.AuditEngineWS;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ProductDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRGuidelineSize;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PRSubstituteItem;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.prediction.RunStatusDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.AuditTrailStatusLookup;
import com.pristine.lookup.AuditTrailTypeLookup;
import com.pristine.service.PredictionComponent;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.service.offermgmt.substitute.SubstituteService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class RerecommendationService {
	private static Logger logger = Logger.getLogger("Re-Recommendation");
	private Connection conn = null;
	private Context initContext;
	private Context envContext;
	private DataSource ds;
	private String chainId = null;
	private boolean isOnline = true;
	private char runType = 'O';

	public RerecommendationService() {
		String runType = PropertyManager.getProperty("PR_RUN_TYPE");
		logger.debug("Run Type - " + runType);
		if (runType != null && PRConstants.RUN_TYPE_ONLINE == runType.charAt(0)) {
			isOnline = true;
		} else
			isOnline = false;
	}

	/**
	 * Re recommendation for the price overridden and related items
	 * 
	 * @param runIds
	 * @throws GeneralException
	 * @throws OfferManagementException
	 */
	public List<RunStatusDTO> updaterecommendUsingOverrideRetails(Connection conn, List<Long> runIds, String userId) throws GeneralException, OfferManagementException {
		this.conn = conn;

		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		StrategyDAO strategyDAO = new StrategyDAO();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		HashMap<Long, PRStrategyDTO> strategyFullDetails = null;
		int noOfWeeksBehind = Integer.parseInt(PropertyManager.getProperty("PR_AVG_MOVEMENT"));
		int noOfWeeksIMSData = Integer.parseInt(PropertyManager.getProperty("PR_NO_OF_WEEKS_IMS_DATA"));
		boolean doSubsAnalysis = Boolean.parseBoolean(PropertyManager.getProperty("PR_SUBSTITUTE_ANALYSIS", "TRUE"));
		int movHistoryWeeks = Integer.parseInt(PropertyManager.getProperty("PR_MOV_HISTORY_WEEKS"));
		double maxUnitPriceDiff = Double.parseDouble(PropertyManager.getProperty("REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE", "0"));
		HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = new HashMap<String, RetailCalendarDTO>();
		List<RunStatusDTO> runStatusDTOs = new ArrayList<RunStatusDTO>();

		// Get recommendation items of all runs --
		// pricingEngineDAO.getRecItemsOfRunIds()
		HashMap<Long, List<PRItemDTO>> runAndItItems = pricingEngineDAO.getRecItemsOfRunIds(conn, runIds);

		// Get List of Strategy Id from all the recommendation run id
		strategyFullDetails = getStrategyDetails(runAndItItems, strategyDAO);

		allWeekCalendarDetails = retailCalendarDAO.getAllWeeks(conn);

		HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = pricingEngineDAO.getRecommendationRules(conn);

		
		// Update all recs as started
		for(Long runId: runAndItItems.keySet()) {
			pricingEngineDAO.updateReRecommendationStatus(conn, runId, 0, "Yet to start");
		}
		
		
		Long runId = null;

		for (Map.Entry<Long, List<PRItemDTO>> entry : runAndItItems.entrySet()) {
			try {

				// NU Bug Fix: 14th Oct 2017, in other part of program getRegPrice() is used to get current price
				// when the current price is in multiple, this is not filled, so it's not working in other place (e.g. threshold)
				updateRegUnitPrice(entry.getValue());

				List<Integer> priceZoneStores = new ArrayList<Integer>();
				StrategyService strategyService = new StrategyService(executionTimeLogs);
				PricingEngineWS pricingEngineWS = new PricingEngineWS();
				PricingEngineService pricingEngineService = new PricingEngineService();
				PriceGroupService priceGroupService = new PriceGroupService();
				ItemService itemService = new ItemService(executionTimeLogs);
				LocationCompetitorMapDAO locCompMapDAO = new LocationCompetitorMapDAO();
				HashMap<Integer, HashMap<Integer, Character>> retLirConstraintMap = new HashMap<Integer, HashMap<Integer, Character>>();
				HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap = new HashMap<MultiCompetitorKey, CompetitiveDataDTO>();
				HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
				HashMap<Integer, List<PRItemDTO>> finalLigMap = new HashMap<Integer, List<PRItemDTO>>();
				PredictionComponent predictionComponent = new PredictionComponent();
				HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData = null;
				List<String> priceAndStrategyZoneNos = new ArrayList<String>();
				SubstituteDAO substituteDAO = new SubstituteDAO();

				RetailCalendarDTO curCalDTO = null;
				curCalDTO = pricingEngineWS.getCurCalDTO(conn, retailCalendarDAO);
				// MarginOpportunity marginOpportunity = new MarginOpportunity();
				// Lead zone details
				HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>>();
				PriceGroupRelationDAO pgDAO = new PriceGroupRelationDAO(conn);
				runId = entry.getKey();
				int leadZoneId = 0, leadZoneDivisionId = 0, divisionId = 0;
				PRStrategyDTO inputDTO = new PRStrategyDTO();
				PRStrategyDTO leadInputDTO = new PRStrategyDTO();

				// Rec Run header information for each run id
				PRRecommendationRunHeader recRunHeader = pricingEngineDAO.getRecommendationRunHeader(conn, runId);
				
				//NU:: 7th March 2018, Update user id
				recRunHeader.setPredictedBy(userId);

				setLog4jProperties(recRunHeader.getLocationId(), recRunHeader.getProductId());
				logger.info(" Re recommendation started for run Id: " + entry.getKey());

				pricingEngineDAO.updateReRecommendationStatus(conn, runId, 0, "Started");
				
				// Product group properties
				List<PRProductGroupProperty> productGroupProperties = getProductGroupProperties(conn, recRunHeader, pricingEngineDAO);

				// 5. Fill inputDTO object (Prod Id, Prod Level id, Loc ID, Loc Level Id, Start Calendar Id)
				getInputDTODetails(recRunHeader, inputDTO, leadInputDTO, leadZoneId, leadZoneDivisionId, divisionId, pricingEngineDAO,
						pricingEngineService);

				RetailCalendarDTO resetCalDTO = getResetCalDTO(inputDTO, retailCalendarDAO);
				if (inputDTO.getLocationLevelId() == PRConstants.ZONE_LEVEL_TYPE_ID) {
					divisionId = new RetailPriceZoneDAO().getDivisionIdForZone(getConnection(), inputDTO.getLocationId());
				}
				if (leadZoneId > 0) {
					pricingEngineService.copyForLeadStrategyDTO(inputDTO, leadInputDTO);
					leadInputDTO.setLocationId(leadZoneId);
					leadZoneDivisionId = new RetailPriceZoneDAO().getDivisionIdForZone(getConnection(), leadZoneId);
				}

				RetailCalendarDTO calDTO = pricingEngineWS.getLatestWeek(conn, inputDTO, retailCalendarDAO);
				// 6. Fill itemDataMap Hash map from recommendation items (fill all necessary properties).

				HashMap<ItemKey, PRItemDTO> itemDataMap = getItemDetails(entry.getValue());

				HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);

				// 7. Get price group details of the category and zone
				HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupRelation = pgDAO.getPriceGroupDetails(inputDTO, itemDataMap);

				// 8. Populate price group
				pricingEngineDAO.populatePriceGroupDetails(itemDataMap, priceGroupRelation);
				
				// To handle Object reference issue. 
				// Related item details were referred to same Memory. So when relation were changing it applied all the items
				// which has same related item.
				itemDataMap.forEach((key,value)->{
					if(value.getPgData()!= null && value.getPgData().getRelationList() != null){
						TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationList = new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>();
						relationList = value.getPgData().getRelationList();
						value.getPgData().setRelationList(new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>(relationList));
//						ArrayList<PRPriceGroupRelatedItemDTO> tempPGRelatedItem = new ArrayList<PRPriceGroupRelatedItemDTO>();
//						tempPGRelatedItem.add(relatedItemDTO);
//						relationList.put(relatedItemKeyChar, tempPGRelatedItem);
					}else if(value.getPgData()!= null && value.getPgData().getRelationList() == null){
						value.getPgData().setRelationList(new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>());
					}
				});
				
				
				// 9. Set the strategyDTO to each item in the overridden item data map
				updateItemWithStrategyDTO(itemDataMap, strategyFullDetails);
				priceGroupService.updatePriceRelationFromStrategy(itemDataMap);
				
				// 10. itemDataMap only with following items
				HashMap<ItemKey, PRItemDTO> overriddenItemDataMap = getOverriddenAndRelatedItemMap(itemDataMap, retLirMap);
				
				List<PRItemDTO> allItemDataList = new ArrayList<PRItemDTO>();
				allItemDataList.addAll(itemDataMap.values());

				// In a given run Id if there is no item overridden then don't process further steps
				if (overriddenItemDataMap != null && overriddenItemDataMap.size() > 0) {
					logger.debug("Size of Overridden item data Map: " + overriddenItemDataMap.size());
					logger.info("Processing overridden and related items...");
					
					pricingEngineDAO.updateReRecommendationStatus(conn, runId, 25, "Processing overridden and related items");
					
					// Set overridden price in recommended reg price and set isProcessed flag as true for overridden items
					updateIsprocessedForOverriddenItem(overriddenItemDataMap);
					// Update System override flag if current recommended price doesn't falls within the given based on parent
					// item
					logger.info("Adjusting Size and Brand Relations overridden related items");
					adjustSizeAndBrandRelation(overriddenItemDataMap, retLirMap);

					// To Change Operator text for Brand Relation for an item, if it has related item Brand relation
					changeBrandOpertorForRelationOverridden(overriddenItemDataMap);
					// To remove existing size relations if overridden size relation were applied
					removeExitingRelations(overriddenItemDataMap);

					logger.info("Updating System override flag is started");
					updateSystemOverrideFlag(overriddenItemDataMap, pricingEngineService, retLirMap);

					// Update LIG item, if any one of LIR Member is System Overridden
					updateSystemOverrideForLIG(overriddenItemDataMap);
					logger.info("Updating System override flag is completed");

					// Reset is processed flag of all item which require system override
					resetIsProcessedFlagForSysOverriddenItems(overriddenItemDataMap);

					// Update LIG item processed flag as True if all LIG Members processed flag are true
					updateLIGItemIsProcessedFlag(overriddenItemDataMap);
					
					// Debug log
					/*overriddenItemDataMap.forEach((key, value)->{
						if(value.getRetLirId() == 29467){
							logger.info("Item Code: "+value.getItemCode()+" Sys Overide flag: "+value.isSystemOverrideFlag()+" User override:"+
						value.getUserOverrideFlag()+" is processed flag: "+value.isProcessed());
						}
					});*/
					
					// To remove existing size relations if overridden size relation were applied
					// removeRelationListFromOverriddenItems(overriddenItemDataMap);
					HashMap<Integer, List<PRItemDTO>> processedLIGMap = getProcessedLIGAndMembersMap(overriddenItemDataMap);

					new LIGConstraint().applyLIGConstraint(processedLIGMap, overriddenItemDataMap, retLirConstraintMap);

					strategyMap = getStrategyMap(leadZoneId, leadZoneDivisionId, divisionId, inputDTO, leadInputDTO, strategyService);

					HashMap<Integer, LocationKey> compIdMap = locCompMapDAO.getCompetitors(conn, inputDTO.getLocationLevelId(),
							inputDTO.getLocationId(), inputDTO.getProductLevelId(), inputDTO.getProductId());

					// Add distinct competitor defined in strategy, so that those competition price also taken
					pricingEngineService.addDistinctCompStrId(overriddenItemDataMap, compIdMap);

					pricingEngineWS.applyMultiCompRetails(conn, strategyMap, curCalDTO,
							inputDTO.getProductLevelId(), inputDTO.getProductId(), itemDataMap);

					// if lead zone guideline is present
					if (pricingEngineService.isLeadZoneGuidelinePresent(overriddenItemDataMap)) {
						// Get Lead Zone Item Details of Zone
						leadZoneDetails = pricingEngineService.getLeadZoneDetails(conn, overriddenItemDataMap, inputDTO);
					}
					LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails = retailCalendarDAO.getAllPreviousWeeks(conn,
							curCalDTO.getStartDate(), noOfWeeksIMSData);

					priceZoneStores = itemService.getPriceZoneStores(conn, inputDTO.getProductLevelId(), inputDTO.getProductId(),
							inputDTO.getLocationLevelId(), inputDTO.getLocationId());

					movementData = pricingEngineDAO.getMovementDataForZone(conn, inputDTO, priceZoneStores, calDTO.getStartDate(), noOfWeeksIMSData,
							overriddenItemDataMap);

					pricingEngineDAO.getMovementDataForZone(movementData, overriddenItemDataMap, previousCalendarDetails, noOfWeeksBehind);

					pricingEngineDAO.getLastXWeeksMovForZone(conn, priceZoneStores, curCalDTO.getStartDate(), movHistoryWeeks, itemDataMap,
							retLirMap, inputDTO.getLocationId());
					// Get Authorized items of Zone and Store. Get items authorized for a location-product combination
					//allStoreItems = itemService.getAuthorizedItemsOfZoneAndStore(conn, inputDTO, priceZoneStores);

					// Get all zone no's
					priceAndStrategyZoneNos = itemService.getPriceAndStrategyZoneNos(entry.getValue());
					HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory = pricingEngineDAO.getPriceHistory(conn,
							Integer.parseInt(chainId), allWeekCalendarDetails, calDTO, resetCalDTO, overriddenItemDataMap, priceAndStrategyZoneNos,
							priceZoneStores, false);

					HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = pricingEngineService
							.getItemZonePriceHistory(Integer.parseInt(chainId), inputDTO.getLocationId(), itemPriceHistory);

					pricingEngineWS.processItems(overriddenItemDataMap, recRunHeader, retLirMap, retLirConstraintMap, compIdMap,
							multiCompLatestPriceMap, finalLigMap, leadZoneDetails, false, curCalDTO);

					List<PRItemDTO> overriddenItemList = new ArrayList<PRItemDTO>();
					overriddenItemDataMap.forEach((itemKey, prItemDTO) -> {
						overriddenItemList.add(prItemDTO);
					});
					
					logger.info("Reg price prediction is started..");
					pricingEngineDAO.updateReRecommendationStatus(conn, runId, 50, "Predicting new price points");
					PredictionService predictionService = new PredictionService(movementData, itemPriceHistory, retLirMap);
					// Zone level prediction
					predictionComponent.predictRegPricePoints(conn, overriddenItemDataMap, overriddenItemList, inputDTO, productGroupProperties,
							recRunHeader, executionTimeLogs, isOnline, retLirMap, predictionService);
					logger.info("Reg price prediction is completed..");
					
					//NU::7th Nov 2017, predictRegPricePoints has only user override and system override items,
					//as a result of it, other items don't have cur price and rec price prediction, which is
					//needed for substitute, otherwise substitute will again call the prediction and it will
					//further increases the timing. Here predictions are set again for those items
					updateRegPriceMap(itemDataMap);
					 
					logger.info("Apply actual objective is started..");

					// Now pick price points again based on the actual objective
					List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
					overriddenItemDataMap.forEach((key, itemDTO) -> {
						if (itemDTO.isSystemOverrideFlag()) {
							itemsToApplyActualObj.add(itemDTO);
						}
					});
					
					pricingEngineDAO.updateReRecommendationStatus(conn, runId, 75, "Recommending prices");
					
					new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory, curCalDTO,
							recommendationRuleMap);
					logger.info("Apply actual objective is completed..");

					// Apply lig constraint
					HashMap<Integer, List<PRItemDTO>> overriddenLIGMap = pricingEngineService.formLigMap(overriddenItemDataMap);
					new LIGConstraint().applyLIGConstraint(overriddenLIGMap, overriddenItemDataMap, retLirConstraintMap);
					
					// Dinesh:: 11/08/2017, After applying actual objective, ProcessItem function applies Guidelines and constraint
					// which is not correct to get final price. Instead call recommendPriceGroupRelatedItems function to apply price to related items
					// based on price is which is retained by actual objective. 
					
					logger.info("Recommending related items is Started....");
					// Recommended all related items.(Related items: Consider items only if it is not User overridden)
					List<PRItemDTO> itemListWithLIG= new ArrayList<PRItemDTO>();
					overriddenItemDataMap.forEach((key, prItemDTO)->{
						if(prItemDTO.isSystemOverrideFlag()){
							itemListWithLIG.add(prItemDTO);
						}
					});
					
					pricingEngineDAO.updateReRecommendationStatus(conn, runId, 75, "Updating system overrides");
					
					priceGroupService.recommendPriceGroupRelatedItems(recRunHeader, itemListWithLIG, itemDataMap, compIdMap,
							retLirConstraintMap, multiCompLatestPriceMap, curCalDTO.getStartDate(), leadZoneDetails, false,
							itemZonePriceHistory, curCalDTO, maxUnitPriceDiff, recommendationRuleMap);
					
					// TODO::SRIC Apply lead item price of same retail item code group to other item
						// TODO::Clear additional log and add log if other item price is different from lead item
					// TODO::SRIC Copy other attributes of lead to other items
					
					logger.info("Recommending related items is Completed....");
					// update item level attributes like current and recommended price prediction
					pricingEngineService.updateItemAttributes(overriddenItemList, recRunHeader);
					
					// Apply lig constraint
					HashMap<Integer, List<PRItemDTO>> ligMap = pricingEngineService.formLigMap(overriddenItemDataMap);
					new LIGConstraint().applyLIGConstraint(ligMap, overriddenItemDataMap, retLirConstraintMap);

					List<PRItemDTO> overriddenItemList1 = new ArrayList<PRItemDTO>();
					itemDataMap.forEach((itemKey, prItemDTO) -> {
						overriddenItemList1.add(prItemDTO);
					});
					
					updatePromoPredictions(executionTimeLogs, recRunHeader, overriddenItemList1, predictionService);
					
					// Update substitute effects
					updateSubstitueEffects(doSubsAnalysis, itemDataMap, recRunHeader, curCalDTO, substituteDAO, inputDTO, pricingEngineDAO, retLirMap,
							priceZoneStores, overriddenItemList1, executionTimeLogs, pricingEngineService);

					logger.info("Size of item list to insert:" + overriddenItemList1.size());
					
					pricingEngineDAO.updateReRecommendationStatus(conn, runId, 90, "Saving recommendation changes");
					
					String noOfItemUpdateStatus = pricingEngineDAO.updateReRecommendationDetails(conn, overriddenItemList1);

					pricingEngineDAO.updateSubPredMovForRerecommendedItems(conn, itemDataMap);
					pricingEngineDAO.updateOverridePredStatusInRunHeader(conn, recRunHeader.getRunId());

					// Update audit trail details
					AuditTrailService auditTrailService = new AuditTrailService();
					auditTrailService.auditRecommendation(conn, inputDTO.getLocationLevelId(), inputDTO.getLocationId(), inputDTO.getProductLevelId(),
							inputDTO.getProductId(), AuditTrailTypeLookup.RE_RECOMMENDATION.getAuditTrailTypeId(), PRConstants.REC_COMPLETED,
							recRunHeader.getRunId(), recRunHeader.getPredictedBy(),AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
							AuditTrailStatusLookup.AUDIT_SUB_TYPE_UPDATE_RECC.getAuditTrailTypeId(),
							AuditTrailStatusLookup.AUDIT_SUB_TYPE_UPDATE_RECC.getAuditTrailTypeId(),0,0,0);

					// Calling Audit process
					callAudit(recRunHeader);
					// Update Dash board numbers
					pricingEngineService.updateDashboard(conn, inputDTO.getLocationLevelId(), inputDTO.getLocationId(),
							inputDTO.getProductLevelId(), inputDTO.getProductId(), recRunHeader.getRunId());
					logger.info("Recommendation sucessfully completed for Run Id: " + recRunHeader.getRunId());

					pricingEngineDAO.updateReRecommendationStatus(conn, runId, 100, "Update recommendation is successful");
					pricingEngineDAO.updateRunStatus(conn, runId, PRConstants.RUN_STATUS_SUCCESS);
					
					PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
					
					String recStatus = "Re-Recommendation completed sucessfully " + noOfItemUpdateStatus.toString();
					updateRunStatus(recRunHeader.getRunId(), runStatusDTOs, PRConstants.SUCCESS, recStatus);
//					updateRunStatus(recRunHeader.getRunId(), runStatusDTOs, PRConstants.SUCCESS, noOfItemUpdateStatus);
				} else {
					updateRunStatus(recRunHeader.getRunId(), runStatusDTOs, PRConstants.SUCCESS, "No price overwritten items available to recommend");
				}

			} catch (Exception | GeneralException ex) {
				updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE, "Error in updating Recommendation");
				pricingEngineDAO.updateReRecommendationStatus(conn, runId, 100, "Error in updating Recommendation");
				pricingEngineDAO.updateRunStatus(conn, runId, PRConstants.RUN_STATUS_ERROR);
				PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");

				logger.error("Error in RerecommendUsingOverrideRetails() " + ex.toString(), ex);
				// throw new OfferManagementException("Error in Re-Recommendation process" + ex,
				// RecommendationErrorCode.GENERAL_EXCEPTION);
			}
		}
		return runStatusDTOs;
	}

	private void updatePromoPredictions(List<ExecutionTimeLog> executionTimeLogs, PRRecommendationRunHeader recRunHeader,
			List<PRItemDTO> itemListWithLIG, PredictionService predictionService) throws GeneralException {

		List<PRItemDTO> saleItemList = new ArrayList<PRItemDTO>();
		// itemListWithLIG.parallelStream().filter(prItemDTO -> prItemDTO.getRecWeekSaleInfo().getSalePrice() != null ||
		// prItemDTO.getIsOnAd() == 1)
		// .forEach(prItemDTO -> {
		// saleItemList.add(prItemDTO);
		// });
		// NU:: 13th Oct 2017, bug fix parallelstream inserts null objects also in to saleItemList list in some cases for some
		// reason, changed the code as below

		for (PRItemDTO itemDTO : itemListWithLIG) {
			if (itemDTO.getRecWeekSaleInfo().getSalePrice() != null || itemDTO.getIsOnAd() == 1) {
				saleItemList.add(itemDTO);
			}
		}

		if (saleItemList.size() > 0) {
			logger.info("Sale prediction is started..");
			PredictionComponent predictionComponent = new PredictionComponent();
			predictionComponent.predictPromoItems(conn, executionTimeLogs, recRunHeader, saleItemList, isOnline, predictionService);
			logger.info("Sale prediction is completed..");
		}
	}

	private void updateSubstitueEffects(boolean doSubsAnalysis, HashMap<ItemKey, PRItemDTO> itemDataMap, PRRecommendationRunHeader recRunHeader,
			RetailCalendarDTO curCalDTO, SubstituteDAO substituteDAO, PRStrategyDTO inputDTO, PricingEngineDAO pricingEngineDAO,
			HashMap<Integer, List<PRItemDTO>> retLirMap, List<Integer> priceZoneStores, List<PRItemDTO> itemListForInsert,
			List<ExecutionTimeLog> executionTimeLogs, PricingEngineService pricingEngineService)
			throws GeneralException, Exception, OfferManagementException {

		SubstituteService substituteService = new SubstituteService(isOnline);
		logger.info("Substitution effect application is started..");
		int noOfsaleAdDisplayWeeks = Integer.parseInt(PropertyManager.getProperty("PR_SALE_AD_DISP_NO_OF_WEEKS"));
		// No Store level recommendation
		if (doSubsAnalysis && runType != PRConstants.RUN_TYPE_TEMP) {
			String promoStartDate = curCalDTO.getStartDate();
			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails = pricingEngineDAO.getSaleDetails(conn, inputDTO.getProductLevelId(),
					inputDTO.getProductId(), Integer.valueOf(chainId), inputDTO.getLocationId(), promoStartDate, noOfsaleAdDisplayWeeks,
					priceZoneStores, false);
			// Assign Sale price to items
			pricingEngineService.fillSaleDetails(itemDataMap, saleDetails, promoStartDate, recRunHeader.getStartDate(), noOfsaleAdDisplayWeeks);
			List<PRSubstituteItem> substituteItems = substituteDAO.getSubstituteItemsNew(conn, inputDTO.getLocationLevelId(),
					inputDTO.getLocationId(), inputDTO.getProductLevelId(), inputDTO.getProductId(), retLirMap, itemDataMap);

			substituteService.adjustPredWithSubsEffect(conn, recRunHeader, curCalDTO, substituteItems, itemListForInsert, saleDetails,
					executionTimeLogs);
		}
		logger.info("Substitution effect application is completed..");
	}

	public void resetIsProcessedFlagForSysOverriddenItems(HashMap<ItemKey, PRItemDTO> overriddenItemDataMap) {
		overriddenItemDataMap.forEach((itemkey, prItemDTO) -> {
			if (prItemDTO.isSystemOverrideFlag()) {
				prItemDTO.setProcessed(false);
			}
		});
	}

	private HashMap<StrategyKey, List<PRStrategyDTO>> getStrategyMap(int leadZoneId, int leadZoneDivisionId, int divisionId, PRStrategyDTO inputDTO,
			PRStrategyDTO leadInputDTO, StrategyService strategyService) throws OfferManagementException {
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		if (leadZoneId > 0) {
			HashMap<StrategyKey, List<PRStrategyDTO>> leadStrategyMap = strategyService.getAllActiveStrategies(getConnection(), leadInputDTO,
					leadZoneDivisionId);
			HashMap<StrategyKey, List<PRStrategyDTO>> dependentStrategyMap = strategyService.getAllActiveStrategies(getConnection(), inputDTO,
					divisionId);
			for (Map.Entry<StrategyKey, List<PRStrategyDTO>> entry : leadStrategyMap.entrySet()) {
				List<PRStrategyDTO> strategyList = new ArrayList<PRStrategyDTO>();
				if (strategyMap.containsKey(entry.getKey())) {
					strategyList = strategyMap.get(entry.getKey());
				}
				strategyList.addAll(entry.getValue());
				strategyMap.put(entry.getKey(), strategyList);
			}
			for (Map.Entry<StrategyKey, List<PRStrategyDTO>> entry : dependentStrategyMap.entrySet()) {
				List<PRStrategyDTO> strategyList = new ArrayList<PRStrategyDTO>();
				if (strategyMap.containsKey(entry.getKey())) {
					strategyList = strategyMap.get(entry.getKey());
				}
				strategyList.addAll(entry.getValue());
				strategyMap.put(entry.getKey(), strategyList);
			}
		} else {
			strategyMap = strategyService.getAllActiveStrategies(conn, inputDTO, divisionId);
		}
		return strategyMap;
	}

	public HashMap<Integer, List<PRItemDTO>> getProcessedLIGAndMembersMap(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		HashMap<Integer, List<PRItemDTO>> processedLIGMap = new HashMap<Integer, List<PRItemDTO>>();
		HashMap<Integer, List<PRItemDTO>> itemDataGroupedByRetLirId = new HashMap<Integer, List<PRItemDTO>>();
		// Group LIG and LIG Members together based on Ret Lir id
		itemDataMap.forEach((key, pritemDTO) -> {
			if (pritemDTO.getRetLirId() > 0) {
				List<PRItemDTO> itemDataList = new ArrayList<PRItemDTO>();
				if (itemDataGroupedByRetLirId.containsKey(pritemDTO.getRetLirId())) {
					itemDataList = itemDataGroupedByRetLirId.get(pritemDTO.getRetLirId());
				}
				itemDataList.add(pritemDTO);
				itemDataGroupedByRetLirId.put(pritemDTO.getRetLirId(), itemDataList);
			}
		});

		// Check LIG and its member items were processed flag is true. If so then add those items in the final List
		itemDataGroupedByRetLirId.forEach((key, ligMembersList) -> {
			boolean isAllLigMembersProcessed = true;
			for (PRItemDTO prItemDTO : ligMembersList) {
				if (!prItemDTO.isProcessed()) {
					isAllLigMembersProcessed = false;
				}
			}
			if (isAllLigMembersProcessed) {
				processedLIGMap.put(key, ligMembersList);
			}
		});
		return processedLIGMap;
	}

	private HashMap<Long, PRStrategyDTO> getStrategyDetails(HashMap<Long, List<PRItemDTO>> runAndItItems, StrategyDAO strategyDAO)
			throws OfferManagementException {
		HashMap<Long, PRStrategyDTO> strategyFullDetails = new HashMap<Long, PRStrategyDTO>();
		HashSet<Long> strategyIdSet = new HashSet<Long>();
		for (Map.Entry<Long, List<PRItemDTO>> entry : runAndItItems.entrySet()) {
			for (PRItemDTO prItemDTO : entry.getValue()) {
				if (prItemDTO.getStrategyId() > 0) {
					strategyIdSet.add(prItemDTO.getStrategyId());
				}
			}
		}
		for (long strategyId : strategyIdSet) {
			PRStrategyDTO prStrategyDTO = strategyDAO.getStrategyDefinition(conn, strategyId);
			if (prStrategyDTO != null) {
				strategyFullDetails.put(strategyId, prStrategyDTO);
			}
		}
		return strategyFullDetails;
	}

	public boolean isLIGItemHasSameRelatedItem(PRItemDTO ligItemDTO, ItemKey relatedItemKey, Character relatedItemChar) {
		boolean ligItemHasSameRelation = false;
		if(ligItemDTO.getPgData()!= null && ligItemDTO.getPgData().getRelationList()!= null){
			NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = ligItemDTO.getPgData().getRelationList();
			for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
				if (entry.getKey().equals(relatedItemChar)) {
					for (PRPriceGroupRelatedItemDTO relatedItemDTO : entry.getValue()) {
						ItemKey itemKey = PRCommonUtil.getRelatedItemKey(relatedItemDTO);
						if (itemKey.getItemCodeOrRetLirId() == relatedItemKey.getItemCodeOrRetLirId()
								&& relatedItemKey.getLirIndicator() == itemKey.getLirIndicator()) {
							ligItemHasSameRelation = true;
						}
					}
				}

			}
		}
		
		return ligItemHasSameRelation;
	}
	
	public void adjustSizeAndBrandRelation(HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, List<PRItemDTO>> retLirMap) {

		int loopThresholdCnt = 0;
		while (true) {
			loopThresholdCnt++;
			HashMap<ItemKey, PRItemDTO> procItemDataMap = new HashMap<ItemKey, PRItemDTO>();
			for (Map.Entry<ItemKey, PRItemDTO> entry : itemDataMap.entrySet()) {
				if (entry.getValue().getOverrideRemoved() == 0 && !entry.getValue().isRelatedItemRelationChanged()
						&& (entry.getValue().getUserOverrideFlag() == 1 || entry.getValue().isRelationOverridden())) {
					procItemDataMap.put(entry.getKey(), entry.getValue());
				}
			}

			// When all items are processed
			if (procItemDataMap.size() == 0 || itemDataMap.size() < loopThresholdCnt)
				break;

			procItemDataMap.forEach((itemKey, prItemDTO) -> {
				// Process Only overridden items
				if (prItemDTO.getPgData() != null && prItemDTO.getPgData().getRelationList() != null) {
					NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = prItemDTO.getPgData().getRelationList();
					for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
						for (PRPriceGroupRelatedItemDTO relatedItemDTO : entry.getValue()) {
							
							// Check related item is LIG or LIG Member or non Lig Item.
							// If related item is LIG, then apply relationship at LIG level else apply at item level
							
							// If related item Is LIG, then check Overridden item has LIG item and then LIG item has same related item. 
							// If LIG Item doesn't have related item, then Apply relation at LIG member level itself
							ItemKey relatedItemKey = PRCommonUtil.getRelatedItemKey(relatedItemDTO);
							PRItemDTO relatedItemDetail = null;
							if (relatedItemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR && prItemDTO.getRetLirId()>0) {
								if (itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)) != null && isLIGItemHasSameRelatedItem(
										itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)), relatedItemKey, entry.getKey())) {

									// Check
									relatedItemDetail = itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true));
								}
							}
							// If Related item is Non LIG or LIG Member.
							if(relatedItemDetail == null && itemDataMap.get(relatedItemKey)!= null && 
									prItemDTO.getRetLirId()>0){
								if (itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)) != null  && isLIGItemHasSameRelatedItem(
										itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)), relatedItemKey, entry.getKey())) {
									relatedItemDetail = itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true));
								}
							}
							if(relatedItemDetail == null) {
								relatedItemDetail = prItemDTO;
							}

							if (relatedItemDTO.getRelatedItemCode() > 0 && relatedItemDTO.getRelatedItemCode() != prItemDTO.getItemCode()
									&& !relatedItemDTO.isOverriddenRelatedItem()) {
								prItemDTO.setRelatedItemRelationChanged(true);
								PRPriceGroupRelatedItemDTO prPriceGroupRelatedItemDTO = null;
								try {
									prPriceGroupRelatedItemDTO = (PRPriceGroupRelatedItemDTO) relatedItemDTO.clone();
								} catch (Exception e) {
									e.printStackTrace();
								}

								prPriceGroupRelatedItemDTO.setRelatedItemCode(relatedItemDetail.getItemCode());
								prPriceGroupRelatedItemDTO.setIsLig(relatedItemDetail.isLir());
								prPriceGroupRelatedItemDTO.setRelatedItemSize(relatedItemDetail.getPgData().getItemSize());
								prPriceGroupRelatedItemDTO.setRelatedItemPrice(relatedItemDetail.getRecommendedRegPrice());
								prPriceGroupRelatedItemDTO.setOverriddenRelatedItem(true);
								if (relatedItemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR
										&& retLirMap.get(relatedItemKey.getItemCodeOrRetLirId()) != null) {
									List<PRItemDTO> parentItemDTOList = retLirMap.get(relatedItemKey.getItemCodeOrRetLirId());
									// Adjust relation for LIG item. LIG item is a lead item for some other items.
									// Relation needs to be changed for LIG items
									if (itemDataMap.get(relatedItemKey) != null) {
										PRItemDTO parentItemDTO = itemDataMap.get(relatedItemKey);
//										logger.info("related item adjusting size and brand for item: " + parentItemDTO.getItemCode()
//												+ " related item:" + prItemDTO.getItemCode());
										if (parentItemDTO.getUserOverrideFlag() == 0 && parentItemDTO.getItemCode() != prItemDTO.getItemCode()
												&& parentItemDTO.getRetLirId() != prItemDTO.getItemCode()) {
											parentItemDTO.setRelationOverridden(true);
											  PRPriceGroupRelatedItemDTO prPriceGroupRelatedItem = null;
									           try {
									            prPriceGroupRelatedItem = (PRPriceGroupRelatedItemDTO) prPriceGroupRelatedItemDTO.clone();
									           } catch (CloneNotSupportedException e) {
									            e.printStackTrace();
									           }
											changeSizeBrandBasedOnOverrideItems(parentItemDTO, prPriceGroupRelatedItem, entry.getKey());
											parentItemDTO.getStrategyDTO().getGuidelines().getExecOrderMap();
										}
									}
									for (PRItemDTO parentItemDTO : parentItemDTOList) {
										if (parentItemDTO.getUserOverrideFlag() == 0 && parentItemDTO.getItemCode() != prItemDTO.getItemCode()
												&& parentItemDTO.getRetLirId() != prItemDTO.getItemCode()) {
											parentItemDTO.setRelationOverridden(true);
											PRPriceGroupRelatedItemDTO prPriceGroupRelatedItem = null;
											try {
												prPriceGroupRelatedItem = (PRPriceGroupRelatedItemDTO) prPriceGroupRelatedItemDTO.clone();
											} catch (Exception e) {
												e.printStackTrace();
											}
											changeSizeBrandBasedOnOverrideItems(parentItemDTO, prPriceGroupRelatedItem, entry.getKey());
											parentItemDTO.getStrategyDTO().getGuidelines().getExecOrderMap();
										}
									}
								} else if (itemDataMap.get(relatedItemKey) != null && itemDataMap.get(relatedItemKey).getUserOverrideFlag() == 0
										&& itemDataMap.get(relatedItemKey).getItemCode() != prPriceGroupRelatedItemDTO.getRelatedItemCode()) {
									PRItemDTO parentItemDTO = itemDataMap.get(relatedItemKey);
									parentItemDTO.setRelationOverridden(true);
									PRPriceGroupRelatedItemDTO prPriceGroupRelatedItem = null;
									try {
										prPriceGroupRelatedItem = (PRPriceGroupRelatedItemDTO) prPriceGroupRelatedItemDTO.clone();
									} catch (Exception e) {
										e.printStackTrace();
									}
									changeSizeBrandBasedOnOverrideItems(parentItemDTO, prPriceGroupRelatedItem, entry.getKey());
									parentItemDTO.getStrategyDTO().getGuidelines().getExecOrderMap();
								}
							}
						}
					}
				}
			});
		}
	}

	public void changeBrandOpertorForRelationOverridden(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		
		for(Map.Entry<ItemKey, PRItemDTO> entry : itemDataMap.entrySet()){
			PRItemDTO prItemDTO = entry.getValue();
			
			if (prItemDTO.getPgData() != null && prItemDTO.getPgData().getRelationList() != null && prItemDTO.isRelationOverridden()) {
				NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = prItemDTO.getPgData().getRelationList();
				navigableMap.forEach((relatedItemKeyChar, relatedItemList) -> {
					if (relatedItemKeyChar.equals(PRConstants.BRAND_RELATION)) {
						for (PRPriceGroupRelatedItemDTO prPriceGroupRelatedItemDTO : relatedItemList) {
							String operatorText = "";
							if (prPriceGroupRelatedItemDTO.isOverriddenRelatedItem()) {
								logger.debug("Processing item: " + prItemDTO.getItemCode());
								logger.debug("Related item: " + prPriceGroupRelatedItemDTO.getRelatedItemCode()
										+ ", Operator text: "
										+ prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText());
								
								if (prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText() != null) {
									if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
										operatorText = PRConstants.PRICE_GROUP_EXPR_BELOW;
									} else if (PRConstants.PRICE_GROUP_EXPR_BELOW
											.equals(prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
										operatorText = PRConstants.PRICE_GROUP_EXPR_ABOVE;
									} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM
											.equals(prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
										operatorText = PRConstants.PRICE_GROUP_EXPR_LESSER_SYM;
									} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM
											.equals(prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
										operatorText = PRConstants.PRICE_GROUP_EXPR_GREATER_SYM;
									} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM
											.equals(prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
										operatorText = PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM;
									} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN
											.equals(prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
										operatorText = PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN;
									} else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN
											.equals(prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
										operatorText = PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN;
									} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL
											.equals(prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
										operatorText = PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL;
									} else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL
											.equals(prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
										operatorText = PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL;
									}
									logger.debug("Related item: " + prPriceGroupRelatedItemDTO.getRelatedItemCode()
									+ ", Operator text: " + operatorText);
									prPriceGroupRelatedItemDTO.getPriceRelation().setOperatorText(operatorText);
								}
							}
						}
					}
				});
			}
		}
	}

	public void removeExitingRelations(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		itemDataMap.forEach((itemKey, prItemDTO) -> {
			if (prItemDTO.getPgData() != null && prItemDTO.getPgData().getRelationList() != null && prItemDTO.isRelationOverridden()) {
				TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> overridedRelationItemMap = new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>();
				NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = prItemDTO.getPgData().getRelationList();
				navigableMap.forEach((relatedItemKeyChar, relatedItemList) -> {
					ArrayList<PRPriceGroupRelatedItemDTO> tempPGRelatedItem = new ArrayList<PRPriceGroupRelatedItemDTO>();
					for (PRPriceGroupRelatedItemDTO prPriceGroupRelatedItemDTO : relatedItemList) {
						if (prPriceGroupRelatedItemDTO.isOverriddenRelatedItem()) {
							tempPGRelatedItem.add(prPriceGroupRelatedItemDTO);
						}
					}
					if (tempPGRelatedItem.size() > 0) {
						overridedRelationItemMap.put(relatedItemKeyChar, tempPGRelatedItem);
					}
				});
				if (overridedRelationItemMap.size() > 0) {
					prItemDTO.getPgData().setRelationList(overridedRelationItemMap);
				}
			}
		});

	}

	public void updateSystemOverrideFlag(HashMap<ItemKey, PRItemDTO> itemDataMap, PricingEngineService pricingEngineService,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		int loopThresholdCnt = 0;
		while (true) {
			loopThresholdCnt++;
			HashMap<ItemKey, PRItemDTO> procItemDataMap = new HashMap<ItemKey, PRItemDTO>();
			for (Map.Entry<ItemKey, PRItemDTO> entry : itemDataMap.entrySet()) {
				if (entry.getValue().getOverrideRemoved() == 0 && !entry.getValue().isProcessed()
						&& (entry.getValue().getUserOverrideFlag() == 0 || !entry.getValue().isSystemOverrideFlag())) {
					procItemDataMap.put(entry.getKey(), entry.getValue());
				}
			}
			// When all items are processed
			if (procItemDataMap.size() == 0 || itemDataMap.size() < loopThresholdCnt)
				break;

			procItemDataMap.forEach((itemKey, prItemDTO) -> {
				if (prItemDTO.getUserOverrideFlag() == 0) {
					// To update System override flag by checking the price
					// range based on related overridden item price
					checkPriceRangeUsingRelatedItem(prItemDTO, itemDataMap, pricingEngineService);
				}
			});
		}
	}

	private void changeSizeBrandBasedOnOverrideItems(PRItemDTO parentItemDTO, PRPriceGroupRelatedItemDTO relatedItemDTO,
			Character relatedItemKeyChar) {
		boolean ignoreFurtherProcessing = false;
		if (parentItemDTO.getPgData() != null) {
			if (parentItemDTO.getPgData().getRelationList() != null) {
				// Apply Size guide lines
				for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : parentItemDTO.getPgData().getRelationList().entrySet()) {
					for (PRPriceGroupRelatedItemDTO relItemDTO : entry.getValue()) {
						if (relItemDTO.getRelatedItemCode() == relatedItemDTO.getRelatedItemCode() || relItemDTO.isOverriddenRelatedItem()) {
							ignoreFurtherProcessing = true;
						}
					}
				}
			}

			if (!ignoreFurtherProcessing) {
				if (relatedItemKeyChar.equals(PRConstants.SIZE_RELATION)) {
					parentItemDTO.setRelationOverridden(true);
					if (parentItemDTO.getPgData().getRelationList().get(relatedItemKeyChar) != null) {
						parentItemDTO.getPgData().getRelationList().get(relatedItemKeyChar).add(relatedItemDTO);
					} else {
						// Even if Size relation exist, replace it with
						// Dependent item size relation
						ArrayList<PRPriceGroupRelatedItemDTO> tempPGRelatedItem = new ArrayList<PRPriceGroupRelatedItemDTO>();
						tempPGRelatedItem.add(relatedItemDTO);
						parentItemDTO.getPgData().getRelationList().put(relatedItemKeyChar, tempPGRelatedItem);
					}
				}
				// Apply Brand Guide lines
				else if (relatedItemKeyChar.equals(PRConstants.BRAND_RELATION)) {
					parentItemDTO.setRelationOverridden(true);
					// Since applying dependent item guideline, change the
					// conditions to opposite for parent item
					if (parentItemDTO.getPgData().getRelationList().get(relatedItemKeyChar) != null) {
						parentItemDTO.getPgData().getRelationList().get(relatedItemKeyChar).add(relatedItemDTO);
					} else {
						ArrayList<PRPriceGroupRelatedItemDTO> tempPGRelatedItem = new ArrayList<PRPriceGroupRelatedItemDTO>();
						tempPGRelatedItem.add(relatedItemDTO);
						parentItemDTO.getPgData().getRelationList().put(relatedItemKeyChar, tempPGRelatedItem);
					}

				}
			}
		} else {
			TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationList = new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>();
			ArrayList<PRPriceGroupRelatedItemDTO> tempPGRelatedItem = new ArrayList<PRPriceGroupRelatedItemDTO>();
			tempPGRelatedItem.add(relatedItemDTO);
			relationList.put(relatedItemKeyChar, tempPGRelatedItem);
			parentItemDTO.setRelationOverridden(true);
			parentItemDTO.getPgData().setRelationList(relationList);
		}
	}

	private boolean checkPriceRangeUsingRelatedItem(PRItemDTO itemInfo, HashMap<ItemKey, PRItemDTO> itemDataMap,
			PricingEngineService pricingEngineService) {
		boolean processsFurther = true;
		PRStrategyDTO strategyDTO = itemInfo.getStrategyDTO();
		if (itemInfo.getPgData() != null && itemInfo.getPgData().getRelationList() != null) {
			NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = itemInfo.getPgData().getRelationList();
			for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
				// List of Related items(Parent Item for current processing item)
				for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
					ItemKey relatedItemKey = PRCommonUtil.getRelatedItemKey(relatedItem);
					// If parent is overridden and processed set as true check the
					// price range
					logger.debug("Processing item :" + itemInfo.getItemCode() + " Related item: " + relatedItem.getRelatedItemCode());
					if (itemDataMap.get(relatedItemKey) != null && (itemDataMap.get(relatedItemKey).getUserOverrideFlag() == 1
							|| itemDataMap.get(relatedItemKey).isSystemOverrideFlag())) {
						PRItemDTO parentItemDTO = itemDataMap.get(relatedItemKey);
						if (parentItemDTO.isSystemOverrideFlag()) {
							logger.debug("System Override for an item: " + itemInfo.getItemCode() + " Since it's parent item is System overridden");
							processsFurther = false;
							itemInfo.setSystemOverrideFlag(true);
						} else {
							relatedItem.setRelatedItemPrice(parentItemDTO.getRecommendedRegPrice());
							double sizeShelfPCT = Constants.DEFAULT_NA;
							List<PRGuidelineSize> sizeGuidelines = null;
							if (strategyDTO.getGuidelines() != null && strategyDTO.getGuidelines().getSizeGuideline() != null) {
								sizeGuidelines = strategyDTO.getGuidelines().getSizeGuideline();
								if (sizeGuidelines.size() > 0) {
									sizeShelfPCT = sizeGuidelines.get(0).getShelfValue();
								}
							}
							PRRange priceRange = getPriceRangeBasedOnRelatedItem(relatedItem, itemInfo.getPgData(), entry.getKey(), sizeShelfPCT);
							boolean isPriceWithInRange = isRecommendedPriceWithInRange(priceRange, itemInfo);
							if (!isPriceWithInRange) {
								logger.debug("System Overridden for an Item: " + itemInfo.getItemCode() + " Rec reg Price: "
										+ itemInfo.getRecommendedRegPrice().price + " With price range start val: " + priceRange.getStartVal()
										+ " and End Val: " + priceRange.getEndVal());
								processsFurther = false;
								itemInfo.setSystemOverrideFlag(true);

								// Update System overridden for LIG item as well Since dependent item may have LIG item as related item
								if(itemInfo.getRetLirId()>0 && itemDataMap.get(PRCommonUtil.getItemKey(itemInfo.getRetLirId(), true))!=null){
									itemDataMap.get(PRCommonUtil.getItemKey(itemInfo.getRetLirId(), true)).setSystemOverrideFlag(true);
								}
							} else {
								logger.debug("System Overridden not done for an Item: " + itemInfo.getItemCode() + " Rec reg Price: "
										+ itemInfo.getRecommendedRegPrice().price + " With price range start val: " + priceRange.getStartVal()
										+ " and End Val: " + priceRange.getEndVal());
								processsFurther = false;
								itemInfo.setProcessed(true);
							}
						}
					}
				}
			}
		}

		return processsFurther;
	}

	private boolean isRecommendedPriceWithInRange(PRRange priceRange, PRItemDTO prItemDTO) {
		boolean isPriceWithRange = false;
		if (priceRange.getStartVal() != Constants.DEFAULT_NA && priceRange.getEndVal() != Constants.DEFAULT_NA) {
			if (prItemDTO.getRecommendedRegPrice().price >= priceRange.getStartVal()
					&& prItemDTO.getRecommendedRegPrice().price <= priceRange.getEndVal()) {
				isPriceWithRange = true;
			}
		}
		return isPriceWithRange;
	}

	private PRRange getPriceRangeBasedOnRelatedItem(PRPriceGroupRelatedItemDTO parentItemDTO, PRPriceGroupDTO pgItemDTO, char relationType,
			double sizeShelfPCT) {
		return parentItemDTO.getPriceRelation().getPriceRange(parentItemDTO.getRelatedItemPrice(), parentItemDTO.getRelatedItemSize(),
				pgItemDTO.getItemSize(), relationType, sizeShelfPCT);
	}

	public void updateIsprocessedForOverriddenItem(HashMap<ItemKey, PRItemDTO> overriddenItemDataMap) {
		overriddenItemDataMap.forEach((key, value) -> {
			if (value.getUserOverrideFlag() == 1 && value.getOverrideRemoved() == 0) {
				value.setPriceRange(new Double[] { value.getOverriddenRegularPrice().price });
				value.setRecommendedRegPrice(value.getOverriddenRegularPrice());
				value.setRegMPack(value.getOverriddenRegularPrice().multiple);
				if (value.getRegMPack() == 1) {
					value.setRegMPrice(value.getOverriddenRegularPrice().price);
				} else {
					value.setRegPrice(value.getOverriddenRegularPrice().price);
				}
				value.setProcessed(true);
			} else if (value.getOverrideRemoved() == 1) {
				value.setProcessed(true);
			}
		});
	}

	/**
	 * Apply Strategy DTO for each item
	 * 
	 * @param overriddenItemDataMap
	 * @param strategyFullDetails
	 */
	private void updateItemWithStrategyDTO(HashMap<ItemKey, PRItemDTO> overriddenItemDataMap, HashMap<Long, PRStrategyDTO> strategyFullDetails) {
		overriddenItemDataMap.forEach((key, value) -> {
			if (value.getStrategyId() > 0 && strategyFullDetails.containsKey(value.getStrategyId())) {
				value.setStrategyDTO(strategyFullDetails.get(value.getStrategyId()));
			} else if (value.getStrategyId() > 0) {
				logger.error("StrategyDTO not found for the strategy id:" + value.getStrategyId());
			} else {
				logger.error("Strategy id not found for the item code:" + key.getItemCodeOrRetLirId() + " is Lir id:" + key.getLirIndicator());
			}
		});
	}

	/**
	 * Get price overridden items and it's related items
	 * 
	 * @param itemDataMap
	 * @return
	 */
	private HashMap<ItemKey, PRItemDTO> getOverriddenAndRelatedItemMap(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {

		// Group all the items based on price group
		HashMap<Integer, List<PRItemDTO>> itemDataBasedOnPG = new HashMap<Integer, List<PRItemDTO>>();
		itemDataMap.forEach((key, prItemDTO) -> {
			if (prItemDTO.getPgData() != null) {
				List<PRItemDTO> tempItemDataList = new ArrayList<PRItemDTO>();
				if (itemDataBasedOnPG.containsKey(prItemDTO.getPgData().getPriceGroupId())) {
					tempItemDataList = itemDataBasedOnPG.get(prItemDTO.getPgData().getPriceGroupId());
				}
				tempItemDataList.add(prItemDTO);
				itemDataBasedOnPG.put(prItemDTO.getPgData().getPriceGroupId(), tempItemDataList);
			}
		});
		// Loop itemDataMap to find list of overridden Items and items related
		// to overridden item
		HashMap<ItemKey, PRItemDTO> overriddenItemMap = new HashMap<ItemKey, PRItemDTO>();
		itemDataMap.forEach((itemKey, value) -> {
			// To filter items were price has been overridden.
			// And if price overridden ITEM is LIG item or LIG member then
			// consider all the items from same LIG
			if (value != null && value.getUserOverrideFlag() == 1 && value.getOverriddenRegularPrice() != null
					&& value.getOverriddenRegularPrice().price > 0) {
				// To consider LIG item
				int retLirId = value.getRetLirId();
				if (itemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR) {
					retLirId = itemKey.getItemCodeOrRetLirId();
					overriddenItemMap.put(itemKey, value);
					overriddenItemMap.get(itemKey).setOverrideRemoved(0);
				}
				// Consider all the LIG members
				if (retLirId > 0 && retLirMap.containsKey(retLirId)) {
					List<PRItemDTO> lirItemList = retLirMap.get(retLirId);
					lirItemList.forEach(prItemDTO -> {
						overriddenItemMap.put(PRCommonUtil.getItemKey(prItemDTO), prItemDTO);
						overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO)).setOverrideRemoved(0);
					});
				} else {
					overriddenItemMap.put(itemKey, value);
					overriddenItemMap.get(itemKey).setOverrideRemoved(0);
				}

				// Consider PG items and its LIG members if available
				if (value.getPgData() != null) {
					List<PRItemDTO> pgItems = itemDataBasedOnPG.get(value.getPgData().getPriceGroupId());
					for (PRItemDTO prItemDTO : pgItems) {
						int retLirId1 = prItemDTO.getRetLirId();
						if (retLirId1 > 0) {
							ItemKey tempKey = PRCommonUtil.getItemKey(retLirId1, true);
							if (itemDataMap.containsKey(tempKey)) {
								PRItemDTO prItemDTO1 = itemDataMap.get(tempKey);
								overriddenItemMap.put(tempKey, prItemDTO1);
								if (overriddenItemMap.get(tempKey).getOverriddenRegularPrice() == null
										|| overriddenItemMap.get(tempKey).getOverriddenRegularPrice().price == 0) {
									overriddenItemMap.get(tempKey).setUserOverrideFlag(0);
									overriddenItemMap.get(tempKey).setOverrideRemoved(0);
								}
							}
						}
						if (retLirId1 > 0 && retLirMap.containsKey(retLirId1)) {
							List<PRItemDTO> lirItemList = retLirMap.get(retLirId1);
							lirItemList.forEach(ligItem -> {
								overriddenItemMap.put(PRCommonUtil.getItemKey(ligItem), ligItem);
								if (overriddenItemMap.get(PRCommonUtil.getItemKey(ligItem)).getOverriddenRegularPrice() == null
										|| overriddenItemMap.get(PRCommonUtil.getItemKey(ligItem)).getOverriddenRegularPrice().price == 0) {
									overriddenItemMap.get(PRCommonUtil.getItemKey(ligItem)).setUserOverrideFlag(0);
									overriddenItemMap.get(PRCommonUtil.getItemKey(ligItem)).setOverrideRemoved(0);
								}
							});
						} else {
							overriddenItemMap.put(PRCommonUtil.getItemKey(prItemDTO), prItemDTO);
							if (overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO)).getOverriddenRegularPrice() == null
									|| overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO)).getOverriddenRegularPrice().price == 0) {
								overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO)).setUserOverrideFlag(0);
								overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO)).setOverrideRemoved(0);
							}
						}
					}
				}
			}
			// If User override item is removed and there is no related item
			// with Overridden Price, then update Override removed flag
			else if (value.getUserOverrideFlag() == 1
					&& (value.getOverriddenRegularPrice() != null && value.getOverriddenRegularPrice().price == 0)) {
				int overrideRemoved = 1;
				int retLirId = value.getRetLirId();
				if (retLirId > 0) {
					ItemKey tempKey = PRCommonUtil.getItemKey(retLirId, true);
					// retLirId = itemKey.getItemCodeOrRetLirId();
					if (itemDataMap.containsKey(tempKey) && !overriddenItemMap.containsKey(tempKey)) {
						PRItemDTO prItemDTO1 = itemDataMap.get(tempKey);
						overriddenItemMap.put(tempKey, prItemDTO1);
						overriddenItemMap.get(tempKey).setOverrideRemoved(overrideRemoved);
					}
				}
				// Consider all the LIG members
				if (retLirId > 0 && retLirMap.containsKey(retLirId)) {
					List<PRItemDTO> lirItemList = retLirMap.get(retLirId);
					lirItemList.forEach(prItemDTO -> {
						// To remove override details

						if (!overriddenItemMap.containsKey(PRCommonUtil.getItemKey(prItemDTO))) {
							overriddenItemMap.put(PRCommonUtil.getItemKey(prItemDTO), prItemDTO);
							overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO)).setOverrideRemoved(overrideRemoved);
						}
					});
				} else {
					if (!overriddenItemMap.containsKey(itemKey)) {
						overriddenItemMap.put(itemKey, value);
						overriddenItemMap.get(itemKey).setOverrideRemoved(overrideRemoved);
					}
				}

				// Consider PG items and its LIG members if available
				if (value.getPgData() != null) {
					List<PRItemDTO> pgItems = itemDataBasedOnPG.get(value.getPgData().getPriceGroupId());
					for (PRItemDTO prItemDTO : pgItems) {
						int retLirId1 = prItemDTO.getRetLirId();
						// Add Lig Items
						if (retLirId1 > 0) {
							ItemKey tempKey = PRCommonUtil.getItemKey(retLirId1, true);
							if (itemDataMap.containsKey(tempKey)) {
								if (!overriddenItemMap.containsKey(tempKey)) {
									overriddenItemMap.put(tempKey, prItemDTO);
									overriddenItemMap.get(tempKey).setOverrideRemoved(overrideRemoved);
								}
							}
						}
						// Add Lig Members
						if (retLirId1 > 0 && retLirMap.containsKey(retLirId1)) {
							List<PRItemDTO> lirItemList = retLirMap.get(retLirId1);
							lirItemList.forEach(ligItem -> {
								if (!overriddenItemMap.containsKey(PRCommonUtil.getItemKey(ligItem))) {
									overriddenItemMap.put(PRCommonUtil.getItemKey(ligItem), ligItem);
									overriddenItemMap.get(PRCommonUtil.getItemKey(ligItem)).setOverrideRemoved(overrideRemoved);
								}
							});
						} else if (!overriddenItemMap.containsKey(PRCommonUtil.getItemKey(prItemDTO))) {
							overriddenItemMap.put(PRCommonUtil.getItemKey(prItemDTO), prItemDTO);
							overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO)).setOverrideRemoved(overrideRemoved);
						}
					}
				}
			}
		});
		return overriddenItemMap;

	}

	private HashMap<ItemKey, PRItemDTO> getItemDetails(List<PRItemDTO> itemDetails) {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		itemDetails.forEach((itemDTO) -> {
			if (itemDTO.isLir()) {
				itemDTO.setRetLirId(itemDTO.getItemCode());
			}
			itemDataMap.put(PRCommonUtil.getItemKey(itemDTO), itemDTO);
		});
		return itemDataMap;
	}

	/**
	 * Get product group properties
	 * 
	 * @param conn
	 * @param recRunHeader
	 * @param pricingEngineDAO
	 * @return
	 * @throws OfferManagementException
	 */
	private List<PRProductGroupProperty> getProductGroupProperties(Connection conn, PRRecommendationRunHeader recRunHeader,
			PricingEngineDAO pricingEngineDAO) throws OfferManagementException {
		List<PRProductGroupProperty> productGroupProperties = new ArrayList<PRProductGroupProperty>();
		// Get all items under input product
		List<ProductDTO> productList = new ArrayList<ProductDTO>();
		ProductDTO productDTO = new ProductDTO();
		productDTO.setProductLevelId(recRunHeader.getProductLevelId());
		productDTO.setProductId(new Integer(String.valueOf(recRunHeader.getProductId())));
		productList.add(productDTO);
		productGroupProperties = pricingEngineDAO.getProductGroupProperties(conn, productList);
		return productGroupProperties;

	}

	/**
	 * 
	 * @param recRunHeader
	 * @param inputDTO
	 * @param leadZoneId
	 * @param leadZoneDivisionId
	 * @param divisionId
	 * @param pricingEngineDAO
	 * @throws GeneralException
	 */
	public void getInputDTODetails(PRRecommendationRunHeader recRunHeader, PRStrategyDTO inputDTO, PRStrategyDTO leadInputDTO, int leadZoneId,
			int leadZoneDivisionId, int divisionId, PricingEngineDAO pricingEngineDAO, PricingEngineService pricingEngineService)
			throws GeneralException {

		if (chainId == null) {
			chainId = new RetailPriceDAO().getChainId(getConnection());
		}
		inputDTO.setLocationId(recRunHeader.getLocationId());
		inputDTO.setLocationLevelId(recRunHeader.getLocationLevelId());
		inputDTO.setProductId(recRunHeader.getProductId());
		inputDTO.setProductLevelId(recRunHeader.getProductLevelId());
		inputDTO.setStartCalendarId(recRunHeader.getCalendarId());
		inputDTO.setEndCalendarId(recRunHeader.getCalendarId());
		inputDTO.setStartDate(recRunHeader.getStartDate());
		inputDTO.setEndDate(recRunHeader.getEndDate());
		leadZoneId = pricingEngineDAO.getLeadAndDependentZone(conn, inputDTO.getLocationId());
	}

	protected void initializeForWS() {
		setConnection(getDSConnection());
		setConnection();
	}

	/**
	 * Returns Connection from datasource
	 * 
	 * @return
	 */
	private Connection getDSConnection() {
		Connection connection = null;
		logger.info("WS Connection - " + PropertyManager.getProperty("WS_CONNECTION"));
		;
		try {
			if (ds == null) {
				initContext = new InitialContext();
				envContext = (Context) initContext.lookup("java:/comp/env");
				ds = (DataSource) envContext.lookup(PropertyManager.getProperty("WS_CONNECTION"));
			}
			connection = ds.getConnection();
		} catch (NamingException exception) {
			logger.error("Error when creating connection from datasource " + exception.toString());
		} catch (SQLException exception) {
			logger.error("Error when creating connection from datasource " + exception.toString());
		}
		return connection;
	}

	protected Connection getConnection() {
		return conn;
	}

	/**
	 * Sets database connection. Used when program runs in batch mode
	 */
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

	protected void initialize() throws GeneralException {
		setConnection();
		// setLog4jProperties(getBatchLocationId(), getBatchProductId());
	}

	private RetailCalendarDTO getResetCalDTO(PRStrategyDTO inputDTO, RetailCalendarDAO retailCalendarDAO) throws GeneralException {
		DashboardDAO dashboardDAO = new DashboardDAO(conn);
		String resetDate = null;
		RetailCalendarDTO resetCalDTO = null;
		if (inputDTO.getProductLevelId() == Constants.CATEGORYLEVELID) {
			// Max of reset date in dashboard or last approval date
			// in recommendation
			resetDate = dashboardDAO.getDashboardResetDate(inputDTO.getLocationLevelId(), inputDTO.getLocationId(), inputDTO.getProductLevelId(),
					inputDTO.getProductId());
			if (resetDate != null) {
				logger.info("Reset Date - " + resetDate);
				resetCalDTO = retailCalendarDAO.getCalendarId(getConnection(), resetDate, Constants.CALENDAR_WEEK);
			}
		}
		return resetCalDTO;
	}

	private void callAudit(PRRecommendationRunHeader recommendationRunHeader) throws GeneralException {
		// Calling Audit process after pricing recommendation is successful
		AuditEngineWS auditEngine = new AuditEngineWS();
		auditEngine.isIntegratedProcess = true;

		long reportId = auditEngine.getAuditHeader(conn, recommendationRunHeader.getLocationLevelId(), recommendationRunHeader.getLocationId(),
				recommendationRunHeader.getProductLevelId(), recommendationRunHeader.getProductId(), recommendationRunHeader.getPredictedBy(),"");
		logger.info("Audit is completed for report id - " + reportId);

	}

	public List<RunStatusDTO> rerecommendRetails(Connection conn, String inpRunIds) {
		this.conn = conn;
		return rerecommendRetails(inpRunIds, PRConstants.BATCH_USER);
	}
	
	public List<RunStatusDTO> rerecommendRetails(String inpRunIds, String userId) {
		List<RunStatusDTO> runStatusDTOs = new ArrayList<RunStatusDTO>();
		ObjectMapper mapper = new ObjectMapper();
		List<Long> runIds = new ArrayList<Long>();
		try {
			logger.debug("Re-Recommendation Starteed..");
			runIds = mapper.readValue(inpRunIds, new TypeReference<List<Long>>() {
			});
			asyncServiceMethod(runIds, userId);
		} catch (Exception ex) {
			// Update all run id with error status
			logger.error("Error in rerecommendRetails() " + ex.toString(), ex);
			for (Long runId : runIds) {
				RunStatusDTO runStatusDTO = new RunStatusDTO();
				runStatusDTO.runId = runId;
				runStatusDTOs.add(runStatusDTO);

				updateRunStatusWithException(runStatusDTO, "Error while Re-Recommending overridden items");
			}
		} finally {
			PristineDBUtil.close(conn);
		}
		return runStatusDTOs;
	}

	private void asyncServiceMethod(List<Long> runIds, String userId) {
		Runnable task = new Runnable() {
			@Override
			public void run() {
				if (conn == null) {
					initializeForWS();
					conn = getConnection();
				}
				triggerUpdateRecommendation(conn, runIds, userId);
			}
		};
		new Thread(task, "ServiceThread").start();
	}
	
	
	private void triggerUpdateRecommendation(Connection conn, List<Long> runIds, String userId) {
		try {
			updaterecommendUsingOverrideRetails(conn, runIds, userId);
		} catch (GeneralException | OfferManagementException e) {
			logger.error("Error in rerecommendRetails() " + e.toString(), e);
			PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
		} finally {
			try {
				PristineDBUtil.commitTransaction(conn, "Update Recommendation");
			} catch (GeneralException e) {
				logger.error("Error in rerecommendRetails() " + e.toString(), e);
			}
		}
	}
	
	private void updateRunStatus(long runId, List<RunStatusDTO> runStatusDTOs, int statusCode, String message) {

//		boolean statusUpdated = false;
//		if (runStatusDTOs.size() > 0) {
//			for (RunStatusDTO runStatusDTO : runStatusDTOs) {
//				if (runStatusDTO.runId == runId) {
//					// If either pi or margin is success, then make it has success
//					runStatusDTO.statusCode = statusCode;
//					runStatusDTO.message = (runStatusDTO.message == null ? "" : runStatusDTO.message);
//					runStatusDTO.message = runStatusDTO.message + " " + message;
//					runStatusDTO.msgCnt = runStatusDTO.msgCnt + 1;
//					statusUpdated = true;
//				}
//			}
//		} else if (!statusUpdated) {
//			RunStatusDTO runStatusDTO = new RunStatusDTO();
//			runStatusDTO.runId = runId;
//			runStatusDTO.statusCode = statusCode;
//			runStatusDTO.message = (runStatusDTO.message == null ? "" : runStatusDTO.message);
//			runStatusDTO.message = runStatusDTO.message + " " + message;
//			runStatusDTO.msgCnt = runStatusDTO.msgCnt + 1;
//			runStatusDTOs.add(runStatusDTO);
//		}
		
		
			RunStatusDTO runStatusDTO = new RunStatusDTO();
			runStatusDTO.runId = runId;
			runStatusDTO.statusCode = statusCode;
			runStatusDTO.message = (runStatusDTO.message == null ? "" : runStatusDTO.message);
			runStatusDTO.message = runStatusDTO.message + " " + message;
//		runStatusDTO.msgCnt = runStatusDTO.msgCnt + 1;
			runStatusDTOs.add(runStatusDTO);
		}

	/**
	 * Update System override flag for LIG, if Any LIG member is system overridden
	 * 
	 * @param itemDataMap
	 */
	public void updateSystemOverrideForLIG(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		itemDataMap.forEach((itemKey, prItemDTO) -> {
			if (prItemDTO.isSystemOverrideFlag() && itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)) != null) {
				itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)).setSystemOverrideFlag(true);
				itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)).setOverrideRemoved(0);
				;
			}
		});
	}

	private void setLog4jProperties(int locationId, int productId) throws GeneralException {
		String logTypes = PropertyManager.getProperty("log4j.rootLogger");
		String appender = PropertyManager.getProperty("log4j.appender.logFile");
		String logPath = PropertyManager.getProperty("log4j.appender.logFile.File");
		String maxFileSize = PropertyManager.getProperty("log4j.appender.logFile.MaxFileSize");
		String patternLayout = PropertyManager.getProperty("log4j.appender.logFile.layout");
		String conversionPattern = PropertyManager.getProperty("log4j.appender.logFile.layout.ConversionPattern");

		String appenderConsole = PropertyManager.getProperty("log4j.appender.console");
		String appenderConsoleLayout = PropertyManager.getProperty("log4j.appender.console.layout");
		String appenderConsoleLayoutPattern = PropertyManager.getProperty("log4j.appender.console.layout.ConversionPattern");

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();

		// RetailCalendarDTO curWkDTO = retailCalendarDAO.getCalendarId(getConnection(), DateUtil.getWeekStartDate(-1),
		// Constants.CALENDAR_WEEK);

		String curWkStartDate = DateUtil.getWeekStartDate(-1);
		RetailCalendarDTO curWkDTO = retailCalendarDAO.getCalendarId(getConnection(), curWkStartDate, Constants.CALENDAR_WEEK);

		// LocalDate recWeekStartDate = DateUtil.toDateAsLocalDate(curWkDTO.getStartDate());
		Date recWeekStartDate = DateUtil.toDate(curWkDTO.getStartDate());
		SimpleDateFormat nf = new SimpleDateFormat("MM-dd-yyy");
		String dateInLog = nf.format(recWeekStartDate);
		String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(new java.util.Date());
		String catAndZoneNum = String.valueOf(locationId) + "_" + String.valueOf(productId);
		logPath = logPath + "/" + catAndZoneNum + "_" + dateInLog + "_" + timeStamp + ".log";

		Properties props = new Properties();
		// try {
		// InputStream configStream = getClass().getResourceAsStream("/log4j-pricing-engine.properties");
		// props.load(configStream);
		// configStream.close();
		// } catch (IOException e) {
		// System.out.println("Error: Cannot laod configuration file ");
		// }
		props.setProperty("log4j.rootLogger", logTypes);
		props.setProperty("log4j.appender.logFile", appender);
		props.setProperty("log4j.appender.logFile.File", logPath);
		props.setProperty("log4j.appender.logFile.MaxFileSize", maxFileSize);
		props.setProperty("log4j.appender.logFile.layout", patternLayout);
		props.setProperty("log4j.appender.logFile.layout.ConversionPattern", conversionPattern);

		props.setProperty("log4j.appender.console", appenderConsole);
		props.setProperty("log4j.appender.console.layout", appenderConsoleLayout);
		props.setProperty("log4j.appender.console.layout.ConversionPattern", appenderConsoleLayoutPattern);
		PropertyConfigurator.configure(props);
	}

	private void updateRegUnitPrice(List<PRItemDTO> recommendationItems) {

		for (PRItemDTO itemDTO : recommendationItems) {
			if (itemDTO.getRegMPrice() == null) {
				itemDTO.setRegMPrice(0d);
			}
			double price = PRCommonUtil.getUnitPrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice(), true);

			itemDTO.setRegPrice(price);
			itemDTO.setRegMPack((itemDTO.getRegMPack() == 0 ? 1 : itemDTO.getRegMPack()));
			itemDTO.setRegMPrice(new Double(itemDTO.getRegMPrice()));
		}
	}

	private void updateRunStatusWithException(RunStatusDTO runStatusDTO, String message) {
		runStatusDTO.statusCode = PRConstants.FAILURE;
		runStatusDTO.message = message;
	}
	
	/**
	 * To update LIG item processed flag as true if all LIG Members Processed flag is true
	 * @param itemDataMap
	 */
	private void updateLIGItemIsProcessedFlag(HashMap<ItemKey, PRItemDTO> itemDataMap){
		itemDataMap.forEach((key, value)->{
			if(value.isLir()){
				boolean isAllLirMemberProcessed = true;
				for(Map.Entry<ItemKey, PRItemDTO> entry: itemDataMap.entrySet()){
					if(!entry.getValue().isLir() && entry.getValue().getRetLirId()==value.getRetLirId() && !entry.getValue().isProcessed()){
						isAllLirMemberProcessed = false;
					}
				}
				value.setProcessed(isAllLirMemberProcessed);
			}
		});
	}

	private void updateRegPriceMap(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		for (Map.Entry<ItemKey, PRItemDTO> tempEntry : itemDataMap.entrySet()) {
			PRItemDTO itemDTO = tempEntry.getValue();
			if (!itemDTO.isLir()) {
				MultiplePrice recPrice = itemDTO.getRecommendedRegPrice();
				MultiplePrice curPrice = PRCommonUtil.getCurRegPrice(itemDTO);
				PricePointDTO recPricePointDTO = null;
				
				// Reg price prediction
				if (recPrice != null && itemDTO.getRegPricePredictionMap().get(recPrice) == null) {
					recPricePointDTO = new PricePointDTO();
					recPricePointDTO.setPredictedMovement(itemDTO.getPredictedMovement());
					recPricePointDTO.setPredictionStatus(
							itemDTO.getPredictionStatus() != null ? PredictionStatus.get(itemDTO.getPredictionStatus())
									: PredictionStatus.UNDEFINED);
					itemDTO.addRegPricePrediction(recPrice, recPricePointDTO);
					logger.debug("updateRegPriceMap,itemCode:" + itemDTO.getItemCode() + ",recPrice:" + recPrice);
				} else if (recPrice != null && itemDTO.getRegPricePredictionMap().get(recPrice) != null) {
					//update price point, so the status will be used for current price
					recPricePointDTO = itemDTO.getRegPricePredictionMap().get(recPrice);
				}
				
				// Cur price prediction
				if (curPrice != null && itemDTO.getRegPricePredictionMap().get(curPrice) == null) {
					PricePointDTO pricePointDTO = new PricePointDTO();
					pricePointDTO.setPredictedMovement(itemDTO.getCurRegPricePredictedMovement());
					//NU::9th Nov 2017, Since current pred status is not saved, use the same pred status of rec price predicted movement
					//its unlikely there will be different prediction status for same item
					if(recPricePointDTO != null) {
						pricePointDTO.setPredictionStatus(recPricePointDTO.getPredictionStatus());
					} else {
						pricePointDTO.setPredictionStatus(
								itemDTO.getCurRegPricePredictionStatus() != null ? PredictionStatus.get(itemDTO.getCurRegPricePredictionStatus())
										: PredictionStatus.UNDEFINED);	
					}					
					itemDTO.addRegPricePrediction(curPrice, pricePointDTO);
					logger.debug("updateRegPriceMap,itemCode:" + itemDTO.getItemCode() + ",curPrice:" + curPrice);
				}
			}
		}
	}

}
