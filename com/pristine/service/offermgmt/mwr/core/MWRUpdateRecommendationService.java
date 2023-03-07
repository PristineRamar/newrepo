package com.pristine.service.offermgmt.mwr.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

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
import com.pristine.dao.offermgmt.mwr.MWRSummaryDAO;
import com.pristine.dao.offermgmt.mwr.WeeklyRecDAO;
import com.pristine.dataload.offermgmt.AuditEngineWS;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dataload.offermgmt.mwr.QuarterlyDashboard;
import com.pristine.dataload.offermgmt.mwr.summary.MultiWeekRecSummary;
import com.pristine.dataload.offermgmt.mwr.summary.SummarySplitup;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ProductDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRConstraintGuardRailDetail;
import com.pristine.dto.offermgmt.PRGuidelineSize;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.SecondaryZoneRecDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.CLPDLPPredictionDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRRunHeader;
import com.pristine.dto.offermgmt.mwr.MWRSummaryDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.mwr.SummarySplitupDTO;
import com.pristine.dto.offermgmt.prediction.RunStatusDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.AuditTrailStatusLookup;
import com.pristine.lookup.AuditTrailTypeLookup;
import com.pristine.service.PredictionComponent;
import com.pristine.service.offermgmt.AuditTrailService;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.ObjectiveService;
import com.pristine.service.offermgmt.PriceGroupAdjustmentService;
import com.pristine.service.offermgmt.PriceGroupService;
import com.pristine.service.offermgmt.PricingEngineHelper;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.RecommendationErrorCode;
import com.pristine.service.offermgmt.SecondaryZoneRecService;
import com.pristine.service.offermgmt.StrategyService;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.service.offermgmt.multiWeekPrediction.CLPDLPPredictionService;
import com.pristine.service.offermgmt.mwr.basedata.BaseDataService;
import com.pristine.service.offermgmt.mwr.basedata.CompDataService;
import com.pristine.service.offermgmt.mwr.basedata.GenaralService;
import com.pristine.service.offermgmt.mwr.basedata.MovementDataService;
import com.pristine.service.offermgmt.mwr.basedata.PriceCheckListService;
import com.pristine.service.offermgmt.mwr.core.finalizeprice.MultiWeekPriceFinalizer;
import com.pristine.service.offermgmt.mwr.itemattributes.ItemAttributeService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class MWRUpdateRecommendationService {
	private static Logger logger = Logger.getLogger("Re-Recommendation");
	private Connection conn = null;
	private Context initContext;
	private Context envContext;
	private DataSource ds;
	private String chainId = null;
	private boolean isOnline = true;

	public MWRUpdateRecommendationService() {
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
		
		int noOfWeeksBehind = Integer.parseInt(PropertyManager.getProperty("PR_AVG_MOVEMENT"));
		int noOfWeeksIMSData = Integer.parseInt(PropertyManager.getProperty("PR_NO_OF_WEEKS_IMS_DATA"));
		int movHistoryWeeks = Integer.parseInt(PropertyManager.getProperty("APPLY_GUIDELINES_CONSTRAINTS_FOR_ITEMS_MOVING_IN_X_WEEKS"));
		double maxUnitPriceDiff = Double
				.parseDouble(PropertyManager.getProperty("REC_MAX_UNIT_PRICE_DIFF_FOR_MULTPLE_PRICE", "0"));
		boolean checkForPendingRetails=Boolean
				.parseBoolean(PropertyManager.getProperty("CHECK_PENDING_RETAILS", "FALSE"));
		boolean calculateGlobalZoneImpact= Boolean
				.parseBoolean(PropertyManager.getProperty("CALC_AGG_IMPACT_FOR_GLOBAL_ZONE","FALSE"));
		 boolean useCommonAPIForPrice = Boolean
				.parseBoolean(PropertyManager.getProperty("USE_COMMON_API_FOR_PRICE", "FALSE"));
		 boolean updateOnlyOverridenItems=Boolean
					.parseBoolean(PropertyManager.getProperty("UPDATE_ONLY_OVERRIDEN_ITEMS", "FALSE"));
		 boolean markItemsNotTosendToPred = Boolean
					.parseBoolean(PropertyManager.getProperty("MARK_NON_MOVING_ITEMS_NOT_TO_SEND_TO_PREDICTION", "FALSE"));
			String substituteForPrediction = (PropertyManager.getProperty("SUBSTITUE_FOR_PREDICTION", "52_WEEKS_AVG"));
			
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		MWRSummaryDAO mwrSummaryDAO = new MWRSummaryDAO();
		StrategyDAO strategyDAO = new StrategyDAO();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		WeeklyRecDAO weeklyRecDAO = new WeeklyRecDAO();
		CLPDLPPredictionService clpdlpPredictionService =  new CLPDLPPredictionService();
		SummarySplitup summarySplitup = new SummarySplitup(null, conn);
		MultiWeekRecSummary multiWeekRecSummary = new MultiWeekRecSummary();
		HashMap<Long, PRStrategyDTO> strategyFullDetails = null;
		HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = new HashMap<String, RetailCalendarDTO>();
		List<RunStatusDTO> runStatusDTOs = new ArrayList<RunStatusDTO>();
		ItemAttributeService itAttr = new ItemAttributeService();
		CommonDataHelper commonDataHelper = new CommonDataHelper();
		
		try {
			// Added property for AZ USA
			// Only items overidden will be updated and no brand relationships will be
			// updated
			if (updateOnlyOverridenItems) {
				runStatusDTOs = new MWRUpdateRecommendationServiceV2().UpdateOnlyOverridenItems(conn, runIds, userId);
				return runStatusDTOs;
			}
		
				// Get recommendation items of all runs --
					// pricingEngineDAO.getRecItemsOfRunIds()
			HashMap<Long, List<PRItemDTO>> runAndItItems = mwrSummaryDAO.getRecItemsOfRunIds(conn, runIds);

			// Update all recs as started
			for (Long runId : runAndItItems.keySet()) {
				pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 0, "Yet to start");
			}

			// Get multiweek item details
			HashMap<Long, HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>>> weeklyItemMapByRunId = weeklyRecDAO
					.getMultiWeeKItemsByRunId(conn, runIds);

			// Get List of Strategy Id from all the recommendation run id
			strategyFullDetails = getStrategyDetails(runAndItItems, strategyDAO);

			allWeekCalendarDetails = retailCalendarDAO.getAllWeeks(conn);
			List<RetailCalendarDTO> fullCalendar = retailCalendarDAO.getFullRetailCalendar(conn);
			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = pricingEngineDAO
					.getRecommendationRules(conn);
			
			Long runId = null;

			for (Map.Entry<Long, List<PRItemDTO>> entry : runAndItItems.entrySet()) {
				try {
					HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory =new HashMap<>();
					HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory =new HashMap<>();
					
					// 2. Convert current data to multiple weeks data
					HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap = weeklyItemMapByRunId
							.get(entry.getKey());
					
					List<SecondaryZoneRecDTO> secZoneRecs = mwrSummaryDAO.getSecondaryZoneRecs(conn, entry.getKey());
					
					HashMap<ItemKey, List<SecondaryZoneRecDTO>> secZoneRecsMap = (HashMap<ItemKey, List<SecondaryZoneRecDTO>>) secZoneRecs
							.stream().collect(Collectors.groupingBy(SecondaryZoneRecDTO::getItemKey));
					
					addSecondaryZoneRec(entry.getValue(), weeklyItemDataMap, secZoneRecsMap);
					// NU Bug Fix: 14th Oct 2017, in other part of program getRegPrice() is used to
					// get current price
					// when the current price is in multiple, this is not filled, so it's not
					// working in other place (e.g.
					// threshold)
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
					GenaralService genaralService = new GenaralService();

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
					RecommendationInputDTO recommendationInputDTO = mwrSummaryDAO.getRecommendationRunHeader(conn,
							runId);
					commonDataHelper.setIsGlobalZone(conn, recommendationInputDTO);

					recommendationInputDTO.setBaseWeek(recommendationInputDTO.getStartWeek());
					recommendationInputDTO.setRunType(PRConstants.RUN_TYPE_ONLINE);

					//set chain Id
					String chainId = new RetailPriceDAO().getChainId(conn);
					recommendationInputDTO.setChainId(Integer.parseInt(chainId));
					PRRecommendationRunHeader recRunHeader = new PRRecommendationRunHeader();
					recRunHeader.setLocationLevelId(recommendationInputDTO.getLocationLevelId());
					recRunHeader.setLocationId(recommendationInputDTO.getLocationId());
					recRunHeader.setProductLevelId(recommendationInputDTO.getProductLevelId());
					recRunHeader.setProductId(recommendationInputDTO.getProductId());
					recRunHeader.setCalendarId(recommendationInputDTO.getStartCalendarId());
					recRunHeader.setStartDate(recommendationInputDTO.getStartWeek());
					recRunHeader.setEndDate(recommendationInputDTO.getStartWeekEndDate());
					recRunHeader.setRunId(runId);
					MWRRunHeader mwrRunHeader = MWRRunHeader.getRunHeaderDTO(recommendationInputDTO, runId, 0);
					recommendationInputDTO.setMwrRunHeader(mwrRunHeader);
					recommendationInputDTO.setRunId(runId);
					// NU:: 7th March 2018, Update user id
					recRunHeader.setPredictedBy(userId);

					// setLog4jProperties(recRunHeader.getLocationId(),
					// recRunHeader.getProductId());
					logger.info("updaterecommendUsingOverrideRetails() - Updating recommendation for Run Id "
							+ entry.getKey() + " is started...");

					pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 0, "Started");

					// Product group properties
					List<PRProductGroupProperty> productGroupProperties = getProductGroupProperties(conn, recRunHeader,
							pricingEngineDAO);

					// 5. Fill inputDTO object (Prod Id, Prod Level id, Loc ID, Loc Level Id, Start Calendar Id)
					getInputDTODetails(recRunHeader, inputDTO, leadInputDTO, leadZoneId, leadZoneDivisionId, divisionId,
							pricingEngineDAO, pricingEngineService);

					RetailCalendarDTO resetCalDTO = getResetCalDTO(inputDTO, retailCalendarDAO);
					if (inputDTO.getLocationLevelId() == PRConstants.ZONE_LEVEL_TYPE_ID) {
						divisionId = new RetailPriceZoneDAO().getDivisionIdForZone(getConnection(),
								inputDTO.getLocationId());
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
					HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupRelation = pgDAO
							.getPriceGroupDetails(inputDTO, itemDataMap);

					// 8. Populate price group
					pricingEngineDAO.populatePriceGroupDetails(itemDataMap, priceGroupRelation);

					// To handle Object reference issue.
					// Related item details were referred to same Memory. So when relation were changing it applied all the items
					// which has same related item.
					itemDataMap.forEach((key, value) -> {
						if (value.getPgData() != null && value.getPgData().getRelationList() != null) {
							TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationList = new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>();
							relationList = value.getPgData().getRelationList();
							value.getPgData().setRelationList(
									new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>(relationList));
							// ArrayList<PRPriceGroupRelatedItemDTO> tempPGRelatedItem = new
							// ArrayList<PRPriceGroupRelatedItemDTO>();
							// tempPGRelatedItem.add(relatedItemDTO);
							// relationList.put(relatedItemKeyChar, tempPGRelatedItem);
						} else if (value.getPgData() != null && value.getPgData().getRelationList() == null) {
							value.getPgData()
									.setRelationList(new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>());
						}
					});
					boolean usePrediction = PricingEngineHelper.isUsePrediction(productGroupProperties, inputDTO.getProductLevelId(), inputDTO.getProductId(),
 							inputDTO.getLocationLevelId(), inputDTO.getLocationId());
					// 9. Set the strategyDTO to each item in the overridden item data map
					updateItemWithStrategyDTO(itemDataMap, strategyFullDetails);
					
					//Added by Karishma on 06/29 
					//Added the steps required for setting the related items same sequenceas  done in recommendations
					boolean resetPriceGroups = Boolean
							.parseBoolean(PropertyManager.getProperty("RESET_PRICEGROUP_RELATIONS", "FALSE"));
					if (resetPriceGroups) {
						itAttr.resetPriceGroups(itemDataMap);
					}
					
					priceGroupService.updatePriceRelationFromStrategy(itemDataMap);
					priceGroupService.setDefaultBrandPrecedence(itemDataMap,retLirMap);
					new PriceGroupAdjustmentService().adjustPriceGroupsByDiscontinuedItems(itemDataMap, retLirMap);
					// Set relations again after adjustement of missing items
					priceGroupService.updatePriceRelationFromStrategy(itemDataMap);
					priceGroupService.setDefaultBrandPrecedence(itemDataMap,retLirMap);

					// 10. itemDataMap only with following items
					HashMap<ItemKey, PRItemDTO> overriddenItemDataMap = getOverriddenAndRelatedItemMap(itemDataMap,
							retLirMap);
					
					/*** PROM-2223 changes **/
					// Changes done by Karishma on 11/15/2021 for AZ
					// If recc for global zone 1000 ,get the regPrices and L13 units of latest runs
					// other zones for each item and calculate impact
					// against new reatail of global zone- reg retail and L13 units of each zone
					if (recommendationInputDTO.isGlobalZone() && calculateGlobalZoneImpact) {

						genaralService.getLatestRegRetailslOfAllZones(overriddenItemDataMap, recommendationInputDTO,
								retLirMap, conn,true);
					}
					/** PROM-2223 changes end **/

					List<PRItemDTO> allItemDataList = new ArrayList<PRItemDTO>();
					allItemDataList.addAll(itemDataMap.values());

					// In a given run Id if there is no item overridden then don't process further steps
					if (overriddenItemDataMap != null && overriddenItemDataMap.size() > 0) {
						logger.debug("Size of Overridden item data Map: " + overriddenItemDataMap.size());
						logger.info("Processing overridden and related items...");
						pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 25,
								"Processing overridden and related items");
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

						
						HashMap<Integer, List<PRItemDTO>> processedLIGMap = getProcessedLIGAndMembersMap(
								overriddenItemDataMap);

						new LIGConstraint().applyLIGConstraint(processedLIGMap, overriddenItemDataMap,
								retLirConstraintMap);

						strategyMap = getStrategyMap(leadZoneId, leadZoneDivisionId, divisionId, inputDTO, leadInputDTO,
								strategyService);

						//Changes done by Bhargavi on 12/10/2020
						//update the new calculated impact only when the item is user override or system override
						HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo = new PriceCheckListService()
								.getPriceCheckLists(conn, recommendationInputDTO.getLocationLevelId(),
										recommendationInputDTO.getLocationId(),
										recommendationInputDTO.getProductLevelId(),
										recommendationInputDTO.getProductId(),
										recommendationInputDTO.isPriceTestZone(),recommendationInputDTO.getStartWeek());

						HashMap<String, ArrayList<Integer>> productListMap = genaralService
								.getProductListForProducts(conn, recommendationInputDTO);
						HashMap<Integer, Integer> productParentChildRelationMap = genaralService
								.getProductLevelRelationMap(conn, recommendationInputDTO);
						ArrayList<Integer> locationList = genaralService.getLocationListId(conn,
								recommendationInputDTO);

						new ItemAttributeService().setPriceCheckLists(recommendationInputDTO, itemDataMap, retLirMap,
								priceCheckListInfo, strategyMap, productListMap, productParentChildRelationMap,
								locationList);
						//Changes completed

						updateGuardrailZonesData(conn, strategyMap, recommendationInputDTO, itemDataMap);
						
						HashMap<Integer, LocationKey> compIdMap = locCompMapDAO.getCompetitors(conn,
								inputDTO.getLocationLevelId(), inputDTO.getLocationId(), inputDTO.getProductLevelId(),
								inputDTO.getProductId());

						// Add distinct competitor defined in strategy, so that those competition price also taken
						pricingEngineService.addDistinctCompStrId(overriddenItemDataMap, compIdMap);

						
						logger.info("setupBaseData() - Retrieving latest comp price data...");
						CompDataService compDataService = new CompDataService();
						// Step 15: Get Comp Price data
						HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> latestCompDataMap = compDataService
								.getLatestCompPriceData(conn, itemDataMap, compIdMap, recommendationInputDTO);

						logger.info("setupBaseData() - Retrieving latest comp price data size: " + latestCompDataMap.size());

						logger.info("setupBaseData() - Retrieving previous comp price data...");
						// Step 16: Get Comp Price data
						HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> previousCompDataMap = compDataService
								.getPreviousCompPriceData(conn, itemDataMap, compIdMap, recommendationInputDTO);

						logger.info("setupBaseData() - Retrieving latest comp price data size: "
								+ previousCompDataMap.size());

						pricingEngineWS.applyMultiCompRetails(conn, strategyMap, curCalDTO,
								inputDTO.getProductLevelId(), inputDTO.getProductId(), itemDataMap);

						new ItemAttributeService().setCompPriceData(recommendationInputDTO, itemDataMap,
								latestCompDataMap, previousCompDataMap, new HashMap<>(), pricingEngineDAO);
						
						// if lead zone guideline is present
						if (pricingEngineService.isLeadZoneGuidelinePresent(overriddenItemDataMap)) {
							// Get Lead Zone Item Details of Zone
							leadZoneDetails = pricingEngineService.getLeadZoneDetails(conn, overriddenItemDataMap,
									inputDTO);
						}
						LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails = retailCalendarDAO
								.getAllPreviousWeeks(conn, curCalDTO.getStartDate(), noOfWeeksIMSData);

						priceZoneStores = itemService.getPriceZoneStores(conn, inputDTO.getProductLevelId(),
								inputDTO.getProductId(), inputDTO.getLocationLevelId(), inputDTO.getLocationId());
						
						movementData = pricingEngineDAO.getMovementDataForZone(conn, inputDTO, priceZoneStores,
								curCalDTO.getStartDate(), noOfWeeksIMSData, overriddenItemDataMap);

						if (!substituteForPrediction.equalsIgnoreCase(PRConstants.LAST_YEAR_SIMILAR_WEEKS_MOVEMENT))
							pricingEngineDAO.getMovementDataForZone(movementData, overriddenItemDataMap,
									previousCalendarDetails, noOfWeeksBehind);
						else
							new MovementDataService().setPreviousYearsMovementData(conn, overriddenItemDataMap,
									recommendationInputDTO);
						
						if (markItemsNotTosendToPred)
							new ItemAttributeService().markNonMovingItemsNotTosendToPrediction(overriddenItemDataMap);

					
						pricingEngineDAO.getMovementDataForZone(movementData, overriddenItemDataMap,
								previousCalendarDetails, noOfWeeksBehind);

						pricingEngineDAO.getLastXWeeksMovForZone(conn, priceZoneStores, curCalDTO.getStartDate(),
								movHistoryWeeks, itemDataMap, retLirMap, inputDTO.getLocationId());

						// Added by Pradeep
						// Added a field to decide for what movement data has to be fetched
						int xWeeksForTotalImpact = Integer.parseInt(PropertyManager.getProperty("NO_OF_WEEKS_MOV_FOR_TOTAL_IMPACT", "52"));
						
						int prevCalID = retailCalendarDAO.getPrevCalendarId(conn,  DateUtil.getWeekStartDate(0), Constants.CALENDAR_WEEK);
						
						new MovementDataService().populateXWeeksMovementData(itemDataMap, retLirMap, movementData, previousCalendarDetails,
								xWeeksForTotalImpact, PRConstants.X_WEEKS_FOR_IMPACT, prevCalID);
						// Get Authorized items of Zone and Store. Get items authorized for a location-product combination
						// allStoreItems = itemService.getAuthorizedItemsOfZoneAndStore(conn, inputDTO, priceZoneStores);
						
						//Changes done by Bhargavi on 01/05/2021
						//update the MarkUp and MarkDown values for Rite Aid
						String impactFactor = PropertyManager.getProperty("IMPACT_CALCULATION_FACTOR", Constants.X_WEEKS_MOV);
						if (Constants.STORE_INVENTORY.equals(impactFactor))
						{
							HashMap<Integer, Integer> storeInventory = new PricingEngineDAO().getInventoryDetails(recommendationInputDTO.getBaseWeek(), recommendationInputDTO.getProductLevelId(),
									recommendationInputDTO.getProductId(), recommendationInputDTO.getLocationId());
							new ItemAttributeService().setInventoryDetails(itemDataMap, storeInventory);
						}
						//Changes- ended
						
						// Get all zone no's
						priceAndStrategyZoneNos = itemService.getPriceAndStrategyZoneNos(entry.getValue());

						//Added the property for FF as price is fetched from API and check is added to solve the null error in update reccs for FF
						if (!useCommonAPIForPrice) {

							itemPriceHistory = pricingEngineDAO.getPriceHistory(conn, Integer.parseInt(chainId),
									allWeekCalendarDetails, calDTO, resetCalDTO, overriddenItemDataMap,
									priceAndStrategyZoneNos, priceZoneStores, false);

							itemZonePriceHistory = pricingEngineService.getItemZonePriceHistory(
									Integer.parseInt(chainId), inputDTO.getLocationId(), itemPriceHistory);
						}

						pricingEngineWS.processItems(overriddenItemDataMap, recRunHeader, retLirMap,
								retLirConstraintMap, compIdMap, multiCompLatestPriceMap, finalLigMap, leadZoneDetails,
								false, curCalDTO);

						List<PRItemDTO> overriddenItemList = new ArrayList<PRItemDTO>();
						overriddenItemDataMap.forEach((itemKey, prItemDTO) -> {
							// if there is no system/user override revert the recommended price to original
							// price
							if (!prItemDTO.isSystemOverrideFlag() && prItemDTO.getUserOverrideFlag() != 1) {
								prItemDTO.setRecommendedRegPrice(prItemDTO.getRecRegPriceBeforeReRecommedation());
							}
							overriddenItemList.add(prItemDTO);
						});

						pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 50, "Predicting new price points");
						logger.info("Reg price prediction is started..");
						PredictionService predictionService = new PredictionService(movementData, itemPriceHistory,
								retLirMap);
						// Zone level prediction
						predictionComponent.predictRegPricePoints(conn, overriddenItemDataMap, overriddenItemList,
								inputDTO, productGroupProperties, recRunHeader, executionTimeLogs, isOnline, retLirMap,
								predictionService);
						logger.info("Reg price prediction is completed..");

						// NU::7th Nov 2017, predictRegPricePoints has only user override and system override items,
						// as a result of it, other items don't have cur price and rec price prediction, which is
						// needed for substitute, otherwise substitute will again call the prediction and it will
						// further increases the timing. Here predictions are set again for those items
						updateRegPriceMap(itemDataMap);

						logger.info("Apply actual objective is started..");

						// Now pick price points again based on the actual objective
						List<PRItemDTO> itemsToApplyActualObj = new ArrayList<PRItemDTO>();
						overriddenItemDataMap.forEach((key, itemDTO) -> {
							if (itemDTO.isSystemOverrideFlag()) {
								itemsToApplyActualObj.add(itemDTO);
							}
						});

						pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 75, "Recommending prices");
						new ObjectiveService().applyObjectiveAndSetRecPrice(itemsToApplyActualObj, itemZonePriceHistory,
								curCalDTO, recommendationRuleMap);
						logger.info("Apply actual objective is completed..");

						// Apply lig constraint
						HashMap<Integer, List<PRItemDTO>> overriddenLIGMap = pricingEngineService
								.formLigMap(overriddenItemDataMap);
						new LIGConstraint().applyLIGConstraint(overriddenLIGMap, overriddenItemDataMap,
								retLirConstraintMap);

						// Dinesh:: 11/08/2017, After applying actual objective, ProcessItem function applies Guidelines and
						// constraint
						// which is not correct to get final price. Instead call recommendPriceGroupRelatedItems function to apply
						// price to related items
						// based on price is which is retained by actual objective.

						logger.info("Recommending related items is Started....");
						// Recommended all related items.(Related items: Consider items only if it is not User overridden)
						List<PRItemDTO> itemListWithLIG = new ArrayList<PRItemDTO>();
						overriddenItemDataMap.forEach((key, prItemDTO) -> {
							if(prItemDTO.isLir()) {
								List<PRItemDTO> ligMembers = retLirMap.get(prItemDTO.getRetLirId());
								itemListWithLIG.add(prItemDTO);
								ligMembers.forEach(ligMember -> {
									itemListWithLIG.add(ligMember);	
								});
							} else if (prItemDTO.getRetLirId() == 0){
								itemListWithLIG.add(prItemDTO);	
							}
						});

						pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 75, "Updating system overrides");
						priceGroupService.recommendPriceGroupRelatedItems(recRunHeader, itemListWithLIG, itemDataMap,
								compIdMap, retLirConstraintMap, multiCompLatestPriceMap, curCalDTO.getStartDate(),
								leadZoneDetails, false, itemZonePriceHistory, curCalDTO, maxUnitPriceDiff,
								recommendationRuleMap);

						// TODO::SRIC Apply lead item price of same retail item code group to other item
						// TODO::Clear additional log and add log if other item price is different from lead item
						// TODO::SRIC Copy other attributes of lead to other items

						logger.info("Recommending related items is Completed....");
						// update item level attributes like current and recommended price prediction
						pricingEngineService.updateItemAttributes(overriddenItemList, recRunHeader);

						// Apply lig constraint
						HashMap<Integer, List<PRItemDTO>> ligMap = pricingEngineService
								.formLigMap(overriddenItemDataMap);

						new LIGConstraint().applyLIGConstraint(ligMap, overriddenItemDataMap, retLirConstraintMap);

						// Changes done by Karishma on 04/10/21 for getting the items approved reccs
						// from Export Queue
						if (checkForPendingRetails) {
							genaralService.getQueueRecommendations(overriddenItemDataMap, conn,
									recommendationInputDTO.getProductId(), recommendationInputDTO.getLocationId());
						}

						pricingEngineWS.calculatePriceChangeImpact(overriddenItemDataMap, retLirMap,
								recommendationInputDTO.isGlobalZone());

						new SecondaryZoneRecService().applyRecommendationForSecondaryZones(itemDataMap, retLirMap,
								true);

						new LIGConstraint().applyLIGConstraint(ligMap, overriddenItemDataMap, retLirConstraintMap);

						pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 80,
								"Updating predictions for quarter");
						MutltiWeekPredInputBuilder mutltiWeekPredInputBuilder = new MutltiWeekPredInputBuilder();
						MutliWeekPredictionComponent mutliWeekPredictionComponent = new MutliWeekPredictionComponent(
								conn);

						HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideWeeklyItemDataMap = getOverrideWeeklyItemDataMap(
								weeklyItemDataMap, overriddenItemDataMap, recommendationInputDTO.getStartWeek(),
								fullCalendar,null,usePrediction);

						// 3. Get inputs for prediction
						List<MultiWeekPredEngineItemDTO> multiWeekPredInput = mutltiWeekPredInputBuilder
								.buildInputForPrediction(overrideWeeklyItemDataMap, recommendationInputDTO);

						logger.info("recommendPrice() - Calling multi week prediction...");

						logger.info(
								"recommendPrice() - # of price points to be predicted: " + multiWeekPredInput.size());

						List<MultiWeekPredEngineItemDTO> predictionCache = new GenaralService().getPredictionCache(conn,
								recommendationInputDTO.getMwrRunHeader());

						logger.info("recommendPrice() - # of records in prediction cache: " + predictionCache.size());

						// 4. Call predicition
						List<MultiWeekPredEngineItemDTO> multiWeekPredOuput = mutliWeekPredictionComponent
								.callMultiWeekPrediction(recommendationInputDTO, multiWeekPredInput,
										allWeekCalendarDetails, predictionCache);

						logger.info("recommendPrice() - # of price points predicted: " + multiWeekPredOuput.size());

						// 5. Update prediction to weekly map
						mutliWeekPredictionComponent.updateWeeklyPredictedMovement(overrideWeeklyItemDataMap,
								multiWeekPredOuput, allWeekCalendarDetails);

						// 5. Update prediction for LIG
						mutliWeekPredictionComponent.updateLigLevelMovement(overrideWeeklyItemDataMap, retLirMap);
						
						new MultiWeekPriceFinalizer().applyRecommendationToAllWeeks(recommendationInputDTO,
								overriddenItemDataMap, overrideWeeklyItemDataMap, itemZonePriceHistory,
								recommendationRuleMap);

						aggregateUnitsSalesAndMarginAtLIGLevel(weeklyItemDataMap, retLirMap);
						
						LinkedHashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMapSorted = getSortedMap(
								weeklyItemDataMap);

						List<MWRItemDTO> quarterlyOverriddenItems = updateQuarterlySummaryItems(
								weeklyItemDataMapSorted);

						List<MWRItemDTO> periodOverriddenItems = updatePeriodSummaryItems(weeklyItemDataMapSorted);

						weeklyRecDAO.updateOverrideStatus(conn, overrideWeeklyItemDataMap);

						String noOfItemUpdateStatus = weeklyRecDAO.updateReRecommendationDetails(conn,
								overrideWeeklyItemDataMap);

						weeklyRecDAO.updateReRecommendationDetailsQuarterly(conn, quarterlyOverriddenItems,
								Constants.CALENDAR_QUARTER);
						//summary issue fix .Pass the processing run Id instead of List of run Id's
						updateWeighedRetail(quarterlyOverriddenItems, entry.getKey());
						
						///** Added for PROM-2223 by KARISHMA ***/
						// For global zone if there is a impact and new price recommended is equal to
						// curr retail
						// then mark this item as recommended as AZ wants to export the same
						if (calculateGlobalZoneImpact && recommendationInputDTO.isGlobalZone()) {
							updateIsReccFlagForGlobalzone(quarterlyOverriddenItems, entry.getKey());
						}

						weeklyRecDAO.updateReRecommendationDetailsPeriod(conn, periodOverriddenItems,
								Constants.CALENDAR_PERIOD);

						boolean isOnline = false;
						if (recommendationInputDTO.getRunMode() == PRConstants.RUN_TYPE_ONLINE) {
							isOnline = true;
						}

						HashMap<Integer, List<CLPDLPPredictionDTO>> clpDLpNumbers = new HashMap<>();
						if (Boolean.parseBoolean(PropertyManager.getProperty("CLP_DLP_MODEL_ENABLED", "FALSE"))) {

							pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 85,
									"Predicting at category level");

							clpDLpNumbers = clpdlpPredictionService.getCLPDLPPredictions(recommendationInputDTO,
									weeklyItemDataMap, allWeekCalendarDetails, isOnline);
						}

						pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 90, "Saving recommendations");
						List<SummarySplitupDTO> weeklySummary = summarySplitup.getWeeklySummary(recommendationInputDTO,
								weeklyItemDataMap, clpDLpNumbers);

						List<SummarySplitupDTO> periodSummary = summarySplitup.getPeriodSummary(recommendationInputDTO,
								weeklyItemDataMap, weeklySummary);

						mwrSummaryDAO.updateSummarySplitup(conn, weeklySummary);

						// code to calculate the period level summary after update
						HashMap<Integer, List<SummarySplitupDTO>> summaryByPeriod = (HashMap<Integer, List<SummarySplitupDTO>>) periodSummary
								.stream().collect(Collectors.groupingBy(SummarySplitupDTO::getPeriodCalendarId));

						List<SummarySplitupDTO> periodSummaryafterUpdate = new ArrayList<SummarySplitupDTO>();
						summaryByPeriod.forEach((periodCalid, periods) -> {
							SummarySplitupDTO summarySpltDto = new SummarySplitupDTO();

							double unitsPred = 0.0;
							double salesPred = 0.0;
							double marginPred = 0.0;
							double marginpctPred = 0.0;
							double unitsTotal = 0.0;
							double salesTotal = 0.0;
							double marginTotal = 0.0;
							double marginPctTotal = 0.0;
							double currUnitsTotal = 0.0;
							double currSalesTotal = 0.0;
							double currMarginTotal = 0.0;
							double currMarginPctTotal = 0.0;
							double promoUnits = 0.0;
							double promoSales = 0.0;
							double promoMargin = 0.0;
							double promomarginPCt = 0.0;
							double currunitsTotalClpDlp = 0.0;
							double currsalesTotalClpDlp = 0.0;
							double currmarginTotalClpDlp = 0.0;
							double currmarginTotalpct = 0.0;
							double unitsTotalClpDlp = 0.0;
							double salesTotalClpDlp = 0.0;
							double marginTotalClpDlp = 0.0;
							double marginTotalpct = 0.0;
							double priceChgImpact = 0.0;

							for (SummarySplitupDTO week : periods) {
								unitsPred = unitsPred + Math.round(week.getUnitsPredicted());
								salesPred = salesPred + Math.round(week.getSalesPredicted());
								marginPred = marginPred + Math.round(week.getMarginPredicted());
								marginpctPred = marginpctPred + Math.round(week.getMarginPctPredicted());
								unitsTotal = unitsTotal + Math.round(week.getUnitsTotal());
								salesTotal = salesTotal + Math.round(week.getSalesTotal());
								marginTotal = marginTotal + Math.round(week.getMarginTotal());
								marginPctTotal = marginPctTotal + Math.round(week.getCurrentUnitsTotal());
								currUnitsTotal = currUnitsTotal + Math.round(week.getCurrentSalesTotal());
								currSalesTotal = currSalesTotal + Math.round(week.getCurrentMarginTotal());
								currMarginTotal = currMarginTotal + Math.round(week.getCurrentMarginPctTotal());
								currMarginPctTotal = currMarginPctTotal + Math.round(week.getPromoUnits());
								promoUnits = promoUnits + Math.round(week.getPromoSales());
								promoSales = promoSales + Math.round(week.getPromoMargin());
								promoMargin = promoMargin + Math.round(week.getPromoMarginPct());
								promomarginPCt = promomarginPCt + Math.round(week.getCurrentUnitsTotalClpDlp());
								currunitsTotalClpDlp = currunitsTotalClpDlp
										+ Math.round(week.getCurrentSalesTotalClpDlp());
								currsalesTotalClpDlp = currsalesTotalClpDlp
										+ Math.round(week.getCurrentMarginTotalClpDlp());
								currmarginTotalClpDlp = currmarginTotalClpDlp
										+ Math.round(week.getCurrentMarginPctTotalClpDlp());
								currmarginTotalpct = currmarginTotalpct + Math.round(week.getUnitsTotalClpDlp());
								unitsTotalClpDlp = unitsTotalClpDlp + Math.round(week.getSalesTotalClpDlp());
								salesTotalClpDlp = salesTotalClpDlp + Math.round(week.getMarginTotalClpDlp());
								marginTotalClpDlp = marginTotalClpDlp + Math.round(week.getMarginTotalClpDlp());
								marginTotalpct = marginTotalpct + Math.round(week.getMarginPctTotalClpDlp());
								if(week.getPriceChangeImpact()!=0)
								priceChgImpact = Math.round(week.getPriceChangeImpact());
							}

							summarySpltDto.setUnitsPredicted(
									periods.stream().mapToDouble(SummarySplitupDTO::getUnitsPredicted).sum());
							summarySpltDto.setSalesPredicted(
									periods.stream().mapToDouble(SummarySplitupDTO::getSalesPredicted).sum());
							summarySpltDto.setMarginPredicted(
									periods.stream().mapToDouble(SummarySplitupDTO::getMarginPredicted).sum());

							summarySpltDto.setMarginPctPredicted(
									periods.stream().mapToDouble(SummarySplitupDTO::getMarginPctPredicted).sum());

							summarySpltDto.setUnitsTotal(periods.stream().mapToDouble(SummarySplitupDTO::getUnitsTotal).sum());

							summarySpltDto.setSalesTotal(periods.stream().mapToDouble(SummarySplitupDTO::getSalesTotal).sum());

							summarySpltDto.setMarginTotal(periods.stream().mapToDouble(SummarySplitupDTO::getMarginTotal).sum());

							summarySpltDto.setMarginPctTotal(
									periods.stream().mapToDouble(SummarySplitupDTO::getMarginPctTotal).sum());

							summarySpltDto.setCurrentUnitsTotal(
									periods.stream().mapToDouble(SummarySplitupDTO::getCurrentUnitsTotal).sum());

							summarySpltDto.setCurrentSalesTotal(
									periods.stream().mapToDouble(SummarySplitupDTO::getCurrentSalesTotal).sum());
							summarySpltDto.setCurrentMarginTotal(
									periods.stream().mapToDouble(SummarySplitupDTO::getCurrentMarginTotal).sum());

							summarySpltDto.setCurrentMarginPctTotal(
									periods.stream().mapToDouble(SummarySplitupDTO::getCurrentMarginPctTotal).sum());

							summarySpltDto.setPromoUnits(periods.stream().mapToDouble(SummarySplitupDTO::getPromoUnits).sum());

							summarySpltDto.setPromoSales(periods.stream().mapToDouble(SummarySplitupDTO::getPromoSales).sum());
							summarySpltDto.setPromoMargin(periods.stream().mapToDouble(SummarySplitupDTO::getPromoMargin).sum());

							summarySpltDto.setPromoMarginPct(
									periods.stream().mapToDouble(SummarySplitupDTO::getPromoMarginPct).sum());

							summarySpltDto.setCurrentUnitsTotalClpDlp(
									periods.stream().mapToDouble(SummarySplitupDTO::getCurrentUnitsTotalClpDlp).sum());

							summarySpltDto.setCurrentSalesTotalClpDlp(
									periods.stream().mapToDouble(SummarySplitupDTO::getCurrentSalesTotalClpDlp).sum());

							summarySpltDto.setCurrentMarginTotalClpDlp(
									periods.stream().mapToDouble(SummarySplitupDTO::getCurrentMarginTotalClpDlp).sum());

							summarySpltDto.setCurrentMarginPctTotalClpDlp(periods.stream()
									.mapToDouble(SummarySplitupDTO::getCurrentMarginPctTotalClpDlp).sum());

							summarySpltDto.setUnitsTotalClpDlp(
									periods.stream().mapToDouble(SummarySplitupDTO::getUnitsTotalClpDlp).sum());

							summarySpltDto.setSalesTotalClpDlp(
									periods.stream().mapToDouble(SummarySplitupDTO::getSalesTotalClpDlp).sum());
							summarySpltDto.setMarginTotalClpDlp(
									periods.stream().mapToDouble(SummarySplitupDTO::getMarginTotalClpDlp).sum());

							summarySpltDto.setMarginPctTotalClpDlp(
									periods.stream().mapToDouble(SummarySplitupDTO::getMarginPctTotalClpDlp).sum());

							summarySpltDto.setPriceChangeImpact(priceChgImpact);
							summarySpltDto.setCalendarId(periodCalid);
							summarySpltDto.setRunId(periods.get(0).getRunId());
							periodSummaryafterUpdate.add(summarySpltDto);

						});

						mwrSummaryDAO.updateSummarySplitup(conn, periodSummaryafterUpdate);

						List<MWRItemDTO> quarterItems = multiWeekRecSummary
								.getMultiWeekItemSummary(recommendationInputDTO, weeklyItemDataMap);

						MWRSummaryDTO mwrSummaryDTO = multiWeekRecSummary.getQuarterSummary(recommendationInputDTO,
								quarterItems, weeklySummary, weeklyItemDataMap);

						mwrSummaryDAO.updateMultiWeekSummary(conn, mwrSummaryDTO);

						mwrSummaryDAO.updateSecondaryZoneRecs(conn, recommendationInputDTO, quarterlyOverriddenItems);
						
						// 12. Update dashboardData
						new QuarterlyDashboard().populateDashboardData(conn,
								recommendationInputDTO.getLocationLevelId(), recommendationInputDTO.getLocationId(),
								recommendationInputDTO.getProductLevelId(), recommendationInputDTO.getProductId());

						// Update audit trail details
						AuditTrailService auditTrailService = new AuditTrailService();
						auditTrailService.auditRecommendation(conn, inputDTO.getLocationLevelId(),
								inputDTO.getLocationId(), inputDTO.getProductLevelId(), inputDTO.getProductId(),
								AuditTrailTypeLookup.RE_RECOMMENDATION.getAuditTrailTypeId(), PRConstants.REC_COMPLETED,
								recRunHeader.getRunId(), recRunHeader.getPredictedBy(),
								AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
								AuditTrailStatusLookup.AUDIT_SUB_TYPE_UPDATE_RECC.getAuditTrailTypeId(),
								AuditTrailStatusLookup.AUDIT_SUB_TYPE_UPDATE_RECC.getAuditTrailTypeId(), 0, 0, 0);

						PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
						logger.info("updaterecommendUsingOverrideRetails()-Call Audit...");
						callAudit(conn, recommendationInputDTO);

						String recStatus = "Re-Recommendation completed sucessfully " + noOfItemUpdateStatus.toString();
						pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 100,
								"Update recommendation is successful");
						pricingEngineDAO.updateOverridePredStatusInRunHeaderQR(conn, runId);
						updateRunStatus(recRunHeader.getRunId(), runStatusDTOs, PRConstants.SUCCESS, recStatus);
					} else {
						updateRunStatus(recRunHeader.getRunId(), runStatusDTOs, PRConstants.SUCCESS,
								"No price overwritten items available to recommend");
					}

				} catch (Exception | GeneralException ex) {
					pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 100,
							"Error in updating Recommendation");
					pricingEngineDAO.updateRunStatusQR(conn, runId, PRConstants.RUN_STATUS_ERROR);
					updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE, "Error in updating Recommendation");

					PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");

					logger.error("Error in RerecommendUsingOverrideRetails() " + ex.toString(), ex);

				}

				logger.info("updaterecommendUsingOverrideRetails() - Updating recommendation for Run Id "
						+ entry.getKey() + " is ended.");
			}
			 
			
		} catch (OfferManagementException oe) {
			logger.error("Error in updaterecommendUsingOverrideRetails() for runIds: "
					+ PRCommonUtil.getCommaSeperatedStringFromLongArray(runIds), oe);
			String errorMessage = "Error in updating Recommendation";
			if (oe.getRecommendationErrorCode() == RecommendationErrorCode.STRATEGY_DELETED) {
				errorMessage = oe.getMessage();
			}
			for (long runId : runIds) {
				pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 100, errorMessage);
				pricingEngineDAO.updateRunStatusQR(conn, runId, PRConstants.RUN_STATUS_ERROR);
			}
		} catch (GeneralException | Exception oe) {
			logger.error("Error in updaterecommendUsingOverrideRetails() for runIds: "
					+ PRCommonUtil.getCommaSeperatedStringFromLongArray(runIds), oe);
			for (long runId : runIds) {
				pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 100, "Error in updating Recommendation");
				pricingEngineDAO.updateRunStatusQR(conn, runId, PRConstants.RUN_STATUS_ERROR);
			}
		}
		return runStatusDTOs;
	}

	/**
	 * 
	 * @param cal
	 * @param startDate
	 * @return period cal id
	 */
	private int getPeriodCalendarId(List<RetailCalendarDTO> cal, String startDate) {
		int calId = 0;

		LocalDate date = LocalDate.parse(startDate, PRCommonUtil.getDateFormatter());
		for (RetailCalendarDTO calendar : cal) {
			if (calendar.getRowType().equals(Constants.CALENDAR_PERIOD)) {
				if ((calendar.getStartDateAsDate().isEqual(date) || calendar.getStartDateAsDate().isBefore(date))
						&& (calendar.getEndDateAsDate().isEqual(date) || calendar.getEndDateAsDate().isAfter(date))) {
					calId = calendar.getCalendarId();
					break;
				}
			}
		}

		return calId;
	}

	public void resetIsProcessedFlagForSysOverriddenItems(HashMap<ItemKey, PRItemDTO> overriddenItemDataMap) {
		overriddenItemDataMap.forEach((itemkey, prItemDTO) -> {
			if (prItemDTO.isSystemOverrideFlag()) {
				prItemDTO.setProcessed(false);
			}
		});
	}

	private HashMap<StrategyKey, List<PRStrategyDTO>> getStrategyMap(int leadZoneId, int leadZoneDivisionId,
			int divisionId, PRStrategyDTO inputDTO, PRStrategyDTO leadInputDTO, StrategyService strategyService)
			throws OfferManagementException {
		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();
		if (leadZoneId > 0) {
			HashMap<StrategyKey, List<PRStrategyDTO>> leadStrategyMap = strategyService
					.getAllActiveStrategies(getConnection(), leadInputDTO, leadZoneDivisionId);
			HashMap<StrategyKey, List<PRStrategyDTO>> dependentStrategyMap = strategyService
					.getAllActiveStrategies(getConnection(), inputDTO, divisionId);
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

		// Check LIG and its member items were processed flag is true. If so then add
		// those items in the final List
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

	private  HashMap<Long, PRStrategyDTO> getStrategyDetails(HashMap<Long, List<PRItemDTO>> runAndItItems,
			StrategyDAO strategyDAO) throws OfferManagementException {
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

	public boolean isLIGItemHasSameRelatedItem(PRItemDTO ligItemDTO, ItemKey relatedItemKey,
			Character relatedItemChar) {
		boolean ligItemHasSameRelation = false;
		if (ligItemDTO.getPgData() != null && ligItemDTO.getPgData().getRelationList() != null) {
			NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = ligItemDTO.getPgData()
					.getRelationList();
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

	public void adjustSizeAndBrandRelation(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {

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
					NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = prItemDTO.getPgData()
							.getRelationList();
					for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
						for (PRPriceGroupRelatedItemDTO relatedItemDTO : entry.getValue()) {

							// Check related item is LIG or LIG Member or non Lig Item.
							// If related item is LIG, then apply relationship at LIG level else apply at
							// item level

							// If related item Is LIG, then check Overridden item has LIG item and then LIG
							// item has same related item.
							// If LIG Item doesn't have related item, then Apply relation at LIG member
							// level itself
							ItemKey relatedItemKey = PRCommonUtil.getRelatedItemKey(relatedItemDTO);
							PRItemDTO relatedItemDetail = null;
							if (relatedItemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR
									&& prItemDTO.getRetLirId() > 0) {
								if (itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)) != null
										&& isLIGItemHasSameRelatedItem(
												itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)),
												relatedItemKey, entry.getKey())) {

									// Check
									relatedItemDetail = itemDataMap
											.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true));
								}
							}
							// If Related item is Non LIG or LIG Member.
							if (relatedItemDetail == null && itemDataMap.get(relatedItemKey) != null
									&& prItemDTO.getRetLirId() > 0) {
								if (itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)) != null
										&& isLIGItemHasSameRelatedItem(
												itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)),
												relatedItemKey, entry.getKey())) {
									relatedItemDetail = itemDataMap
											.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true));
								}
							}
							if (relatedItemDetail == null) {
								relatedItemDetail = prItemDTO;
							}

							if (relatedItemDTO.getRelatedItemCode() > 0
									&& relatedItemDTO.getRelatedItemCode() != prItemDTO.getItemCode()
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
								prPriceGroupRelatedItemDTO
										.setRelatedItemSize(relatedItemDetail.getPgData().getItemSize());
								prPriceGroupRelatedItemDTO
										.setRelatedItemPrice(relatedItemDetail.getRecommendedRegPrice());
								prPriceGroupRelatedItemDTO.setOverriddenRelatedItem(true);
								if (relatedItemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR
										&& retLirMap.get(relatedItemKey.getItemCodeOrRetLirId()) != null) {
									List<PRItemDTO> parentItemDTOList = retLirMap
											.get(relatedItemKey.getItemCodeOrRetLirId());
									// Adjust relation for LIG item. LIG item is a lead item for some other items.
									// Relation needs to be changed for LIG items
									if (itemDataMap.get(relatedItemKey) != null) {
										PRItemDTO parentItemDTO = itemDataMap.get(relatedItemKey);
//										logger.info("related item adjusting size and brand for item: " + parentItemDTO.getItemCode()
//												+ " related item:" + prItemDTO.getItemCode());
										if (parentItemDTO.getUserOverrideFlag() == 0
												&& parentItemDTO.getItemCode() != prItemDTO.getItemCode()
												&& parentItemDTO.getRetLirId() != prItemDTO.getItemCode()) {
											parentItemDTO.setRelationOverridden(true);
											PRPriceGroupRelatedItemDTO prPriceGroupRelatedItem = null;
											try {
												prPriceGroupRelatedItem = (PRPriceGroupRelatedItemDTO) prPriceGroupRelatedItemDTO
														.clone();
											} catch (CloneNotSupportedException e) {
												e.printStackTrace();
											}
											changeSizeBrandBasedOnOverrideItems(parentItemDTO, prPriceGroupRelatedItem,
													entry.getKey());
											parentItemDTO.getStrategyDTO().getGuidelines().getExecOrderMap();
										}
									}
									for (PRItemDTO parentItemDTO : parentItemDTOList) {
										if (parentItemDTO.getUserOverrideFlag() == 0
												&& parentItemDTO.getItemCode() != prItemDTO.getItemCode()
												&& parentItemDTO.getRetLirId() != prItemDTO.getItemCode()) {
											parentItemDTO.setRelationOverridden(true);
											PRPriceGroupRelatedItemDTO prPriceGroupRelatedItem = null;
											try {
												prPriceGroupRelatedItem = (PRPriceGroupRelatedItemDTO) prPriceGroupRelatedItemDTO
														.clone();
											} catch (Exception e) {
												e.printStackTrace();
											}
											changeSizeBrandBasedOnOverrideItems(parentItemDTO, prPriceGroupRelatedItem,
													entry.getKey());
											parentItemDTO.getStrategyDTO().getGuidelines().getExecOrderMap();
										}
									}
								} else if (itemDataMap.get(relatedItemKey) != null
										&& itemDataMap.get(relatedItemKey).getUserOverrideFlag() == 0
										&& itemDataMap.get(relatedItemKey).getItemCode() != prPriceGroupRelatedItemDTO
												.getRelatedItemCode()) {
									PRItemDTO parentItemDTO = itemDataMap.get(relatedItemKey);
									parentItemDTO.setRelationOverridden(true);
									PRPriceGroupRelatedItemDTO prPriceGroupRelatedItem = null;
									try {
										prPriceGroupRelatedItem = (PRPriceGroupRelatedItemDTO) prPriceGroupRelatedItemDTO
												.clone();
									} catch (Exception e) {
										e.printStackTrace();
									}
									changeSizeBrandBasedOnOverrideItems(parentItemDTO, prPriceGroupRelatedItem,
											entry.getKey());
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

		for (Map.Entry<ItemKey, PRItemDTO> entry : itemDataMap.entrySet()) {
			PRItemDTO prItemDTO = entry.getValue();

			if (prItemDTO.getPgData() != null && prItemDTO.getPgData().getRelationList() != null
					&& prItemDTO.isRelationOverridden()) {
				NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = prItemDTO.getPgData()
						.getRelationList();
				navigableMap.forEach((relatedItemKeyChar, relatedItemList) -> {
					if (relatedItemKeyChar.equals(PRConstants.BRAND_RELATION)) {
						for (PRPriceGroupRelatedItemDTO prPriceGroupRelatedItemDTO : relatedItemList) {
							String operatorText = "";
							if (prPriceGroupRelatedItemDTO.isOverriddenRelatedItem()
									&& !prPriceGroupRelatedItemDTO.isRelationOperatorChanged()) {
								if (prPriceGroupRelatedItemDTO.getPriceRelation() != null) {
									logger.debug("Processing item: " + prItemDTO.getItemCode());
									logger.debug("Related item: " + prPriceGroupRelatedItemDTO.getRelatedItemCode()
											+ ", Operator text: "
											+ prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText());
									if (prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText() != null) {
										if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(
												prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
											operatorText = PRConstants.PRICE_GROUP_EXPR_BELOW;
										} else if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(
												prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
											operatorText = PRConstants.PRICE_GROUP_EXPR_ABOVE;
										} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(
												prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
											operatorText = PRConstants.PRICE_GROUP_EXPR_LESSER_SYM;
										} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(
												prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
											operatorText = PRConstants.PRICE_GROUP_EXPR_GREATER_SYM;
										} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(
												prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
											operatorText = PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM;
										} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(
												prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
											operatorText = PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN;
										} else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(
												prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
											operatorText = PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN;
										} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(
												prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
											operatorText = PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL;
										} else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(
												prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText())) {
											operatorText = PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL;
										}
										prPriceGroupRelatedItemDTO.getPriceRelation().setOperatorText(operatorText);
										prPriceGroupRelatedItemDTO.setRelationOperatorChanged(true);
										logger.debug("Related item: " + prPriceGroupRelatedItemDTO.getRelatedItemCode()
												+ ", Operator text: "
												+ prPriceGroupRelatedItemDTO.getPriceRelation().getOperatorText());

									}
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
			if (prItemDTO.getPgData() != null && prItemDTO.getPgData().getRelationList() != null
					&& prItemDTO.isRelationOverridden()) {
				TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> overridedRelationItemMap = new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>();
				NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = prItemDTO.getPgData()
						.getRelationList();
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

	public void updateSystemOverrideFlag(HashMap<ItemKey, PRItemDTO> itemDataMap,
			PricingEngineService pricingEngineService, HashMap<Integer, List<PRItemDTO>> retLirMap) {
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
				for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : parentItemDTO.getPgData()
						.getRelationList().entrySet()) {
					for (PRPriceGroupRelatedItemDTO relItemDTO : entry.getValue()) {
						if (relItemDTO.getRelatedItemCode() == relatedItemDTO.getRelatedItemCode()
								|| relItemDTO.isOverriddenRelatedItem()) {
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
			PRPriceGroupDTO pgData = new PRPriceGroupDTO();
			parentItemDTO.setPgData(pgData);
			parentItemDTO.getPgData().setRelationList(relationList);
		}
	}

	private boolean checkPriceRangeUsingRelatedItem(PRItemDTO itemInfo, HashMap<ItemKey, PRItemDTO> itemDataMap,
			PricingEngineService pricingEngineService) {
		boolean processsFurther = true;
		PRStrategyDTO strategyDTO = itemInfo.getStrategyDTO();
		if (itemInfo.getPgData() != null && itemInfo.getPgData().getRelationList() != null) {
			NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = itemInfo.getPgData()
					.getRelationList();
			for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
				// List of Related items(Parent Item for current processing item)
				for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
					if (relatedItem.getPriceRelation() != null) {
						ItemKey relatedItemKey = PRCommonUtil.getRelatedItemKey(relatedItem);
						// If parent is overridden and processed set as true check the
						// price range
						logger.debug("Processing item :" + itemInfo.getItemCode() + " Related item: "
								+ relatedItem.getRelatedItemCode());
						if (itemDataMap.get(relatedItemKey) != null
								&& (itemDataMap.get(relatedItemKey).getUserOverrideFlag() == 1
										|| itemDataMap.get(relatedItemKey).isSystemOverrideFlag())) {
							PRItemDTO parentItemDTO = itemDataMap.get(relatedItemKey);
							if (parentItemDTO.isSystemOverrideFlag()) {
								logger.debug("System Override for an item: " + itemInfo.getItemCode()
										+ " Since it's parent item is System overridden");
								processsFurther = false;
								itemInfo.setSystemOverrideFlag(true);
							} else {
								relatedItem.setRelatedItemPrice(parentItemDTO.getRecommendedRegPrice());
								double sizeShelfPCT = Constants.DEFAULT_NA;
								List<PRGuidelineSize> sizeGuidelines = null;
								if (strategyDTO.getGuidelines() != null
										&& strategyDTO.getGuidelines().getSizeGuideline() != null) {
									sizeGuidelines = strategyDTO.getGuidelines().getSizeGuideline();
									if (sizeGuidelines.size() > 0) {
										sizeShelfPCT = sizeGuidelines.get(0).getShelfValue();
									}
								}
								PRRange priceRange = getPriceRangeBasedOnRelatedItem(relatedItem, itemInfo.getPgData(),
										entry.getKey(), sizeShelfPCT);
								boolean isPriceWithInRange = isRecommendedPriceWithInRange(priceRange, itemInfo);
								if (!isPriceWithInRange) {
									logger.debug("System Overridden for an Item: " + itemInfo.getItemCode()
											+ " Rec reg Price: " + itemInfo.getRecommendedRegPrice().price
											+ " With price range start val: " + priceRange.getStartVal()
											+ " and End Val: " + priceRange.getEndVal() + ", operator text: "
											+ relatedItem.getPriceRelation().getOperatorText());
									processsFurther = false;
									itemInfo.setSystemOverrideFlag(true);

									// Update System overridden for LIG item as well Since dependent item may have
									// LIG item as
									// related item
									if (itemInfo.getRetLirId() > 0 && itemDataMap
											.get(PRCommonUtil.getItemKey(itemInfo.getRetLirId(), true)) != null) {
										itemDataMap.get(PRCommonUtil.getItemKey(itemInfo.getRetLirId(), true))
												.setSystemOverrideFlag(true);
									}
								} else {
									logger.debug("System Overridden not done for an Item: " + itemInfo.getItemCode()
											+ " Rec reg Price: " + itemInfo.getRecommendedRegPrice().price
											+ " With price range start val: " + priceRange.getStartVal()
											+ " and End Val: " + priceRange.getEndVal());
									processsFurther = false;
									itemInfo.setProcessed(true);
								}
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

	private PRRange getPriceRangeBasedOnRelatedItem(PRPriceGroupRelatedItemDTO parentItemDTO, PRPriceGroupDTO pgItemDTO,
			char relationType, double sizeShelfPCT) {
		return parentItemDTO.getPriceRelation().getPriceRange(parentItemDTO.getRelatedItemPrice(),
				parentItemDTO.getRelatedItemSize(), pgItemDTO.getItemSize(), relationType, sizeShelfPCT);
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
	public  void updateItemWithStrategyDTO(HashMap<ItemKey, PRItemDTO> overriddenItemDataMap,
			HashMap<Long, PRStrategyDTO> strategyFullDetails) {
		overriddenItemDataMap.forEach((key, value) -> {
			if (value.getStrategyId() > 0 && strategyFullDetails.containsKey(value.getStrategyId())) {
				value.setStrategyDTO(strategyFullDetails.get(value.getStrategyId()));
			} else if (value.getStrategyId() > 0) {
				logger.error("StrategyDTO not found for the strategy id:" + value.getStrategyId());
			} else {
				logger.error("Strategy id not found for the item code:" + key.getItemCodeOrRetLirId() + " is Lir id:"
						+ key.getLirIndicator());
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
								if (overriddenItemMap.get(PRCommonUtil.getItemKey(ligItem))
										.getOverriddenRegularPrice() == null
										|| overriddenItemMap.get(PRCommonUtil.getItemKey(ligItem))
												.getOverriddenRegularPrice().price == 0) {
									overriddenItemMap.get(PRCommonUtil.getItemKey(ligItem)).setUserOverrideFlag(0);
									overriddenItemMap.get(PRCommonUtil.getItemKey(ligItem)).setOverrideRemoved(0);
								}
							});
						} else {
							overriddenItemMap.put(PRCommonUtil.getItemKey(prItemDTO), prItemDTO);
							if (overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO))
									.getOverriddenRegularPrice() == null
									|| overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO))
											.getOverriddenRegularPrice().price == 0) {
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
							overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO))
									.setOverrideRemoved(overrideRemoved);
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
									overriddenItemMap.get(PRCommonUtil.getItemKey(ligItem))
											.setOverrideRemoved(overrideRemoved);
								}
							});
						} else if (!overriddenItemMap.containsKey(PRCommonUtil.getItemKey(prItemDTO))) {
							overriddenItemMap.put(PRCommonUtil.getItemKey(prItemDTO), prItemDTO);
							overriddenItemMap.get(PRCommonUtil.getItemKey(prItemDTO))
									.setOverrideRemoved(overrideRemoved);
						}
					}
				}
			}
		});
		return overriddenItemMap;

	}

	public HashMap<ItemKey, PRItemDTO> getItemDetails(List<PRItemDTO> itemDetails) {
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
	public List<PRProductGroupProperty> getProductGroupProperties(Connection conn,
			PRRecommendationRunHeader recRunHeader, PricingEngineDAO pricingEngineDAO) throws OfferManagementException {
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
	public void getInputDTODetails(PRRecommendationRunHeader recRunHeader, PRStrategyDTO inputDTO,
			PRStrategyDTO leadInputDTO, int leadZoneId, int leadZoneDivisionId, int divisionId,
			PricingEngineDAO pricingEngineDAO, PricingEngineService pricingEngineService) throws GeneralException {

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

	public RetailCalendarDTO getResetCalDTO(PRStrategyDTO inputDTO, RetailCalendarDAO retailCalendarDAO)
			throws GeneralException {
		DashboardDAO dashboardDAO = new DashboardDAO(conn);
		String resetDate = null;
		RetailCalendarDTO resetCalDTO = null;
		if (inputDTO.getProductLevelId() == Constants.CATEGORYLEVELID) {
			// Max of reset date in dashboard or last approval date
			// in recommendation
			resetDate = dashboardDAO.getDashboardResetDate(inputDTO.getLocationLevelId(), inputDTO.getLocationId(),
					inputDTO.getProductLevelId(), inputDTO.getProductId());
			if (resetDate != null) {
				logger.info("Reset Date - " + resetDate);
				resetCalDTO = retailCalendarDAO.getCalendarId(getConnection(), resetDate, Constants.CALENDAR_WEEK);
			}
		}
		return resetCalDTO;
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

	public void triggerUpdateRecommendation(Connection conn, List<Long> runIds, String userId) {
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

	void updateRunStatus(long runId, List<RunStatusDTO> runStatusDTOs, int statusCode, String message) {

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
			if (prItemDTO.isSystemOverrideFlag()
					&& itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)) != null) {
				itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)).setSystemOverrideFlag(true);
				itemDataMap.get(PRCommonUtil.getItemKey(prItemDTO.getRetLirId(), true)).setOverrideRemoved(0);
				;
			}
		});
	}

	
	private void updateRegUnitPrice(List<PRItemDTO> recommendationItems) {

		for (PRItemDTO itemDTO : recommendationItems) {
			if (itemDTO.getRegMPrice() == null) {
				itemDTO.setRegMPrice(0d);
			}
			double price = PRCommonUtil.getUnitPrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
					itemDTO.getRegMPrice(), true);

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
	 * To update LIG item processed flag as true if all LIG Members Processed flag
	 * is true
	 * 
	 * @param itemDataMap
	 */
	private void updateLIGItemIsProcessedFlag(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		itemDataMap.forEach((key, value) -> {
			if (value.isLir()) {
				boolean isAllLirMemberProcessed = true;
				for (Map.Entry<ItemKey, PRItemDTO> entry : itemDataMap.entrySet()) {
					if (!entry.getValue().isLir() && entry.getValue().getRetLirId() == value.getRetLirId()
							&& !entry.getValue().isProcessed()) {
						isAllLirMemberProcessed = false;
					}
				}
				value.setProcessed(isAllLirMemberProcessed);
			}
		});
	}

	public  void updateRegPriceMap(HashMap<ItemKey, PRItemDTO> itemDataMap) {
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
				} else if (recPrice != null && itemDTO.getRegPricePredictionMap().get(recPrice) != null) {
					// update price point, so the status will be used for current price
					recPricePointDTO = itemDTO.getRegPricePredictionMap().get(recPrice);
				}

				// Cur price prediction
				if (curPrice != null && itemDTO.getRegPricePredictionMap().get(curPrice) == null) {
					PricePointDTO pricePointDTO = new PricePointDTO();
					pricePointDTO.setPredictedMovement(itemDTO.getCurRegPricePredictedMovement());
					// NU::9th Nov 2017, Since current pred status is not saved, use the same pred
					// status of rec price predicted movement
					// its unlikely there will be different prediction status for same item
					if (recPricePointDTO != null) {
						pricePointDTO.setPredictionStatus(recPricePointDTO.getPredictionStatus());
					} else {
						pricePointDTO.setPredictionStatus(itemDTO.getCurRegPricePredictionStatus() != null
								? PredictionStatus.get(itemDTO.getCurRegPricePredictionStatus())
								: PredictionStatus.UNDEFINED);
					}
					itemDTO.addRegPricePrediction(curPrice, pricePointDTO);
				}
			}
		}
	}

	/**
	 * 
	 * @param weeklyItemDataMap
	 * @param overrideItemDataMap
	 * @param recWeekStartDateStr
	 * @param updatedItemsMap 
	 * @return mutliweekoverride items
	 */
	public HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> getOverrideWeeklyItemDataMap(
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			HashMap<ItemKey, PRItemDTO> overrideItemDataMap, String recWeekStartDateStr,
			List<RetailCalendarDTO> fullCalendar, HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> updatedItemsMap, boolean usePrediction) {
		HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideWeeklyItemDataMap = new HashMap<>();

		LocalDate recWeekStartDate = LocalDate.parse(recWeekStartDateStr, PRCommonUtil.getDateFormatter());

		weeklyItemDataMap.forEach((recWeekKey, itemMap) -> {
			LocalDate weekStartDate = LocalDate.parse(recWeekKey.weekStartDate, PRCommonUtil.getDateFormatter());
			int periodCalId = getPeriodCalendarId(fullCalendar, recWeekKey.weekStartDate);
			if (weekStartDate.isEqual(recWeekStartDate) || weekStartDate.isAfter(recWeekStartDate)) {
				HashMap<ItemKey, MWRItemDTO> overrideItems = new HashMap<>();
				itemMap.forEach((itemKey, mwrItemDTO) -> {
					if (overrideItemDataMap.containsKey(itemKey)) {
						PRItemDTO prItemDTO = overrideItemDataMap.get(itemKey);
						mwrItemDTO.setPeriodCalendarId(periodCalId);
						if (mwrItemDTO.getOverrideRegPrice() == null && prItemDTO.isSystemOverrideFlag()) {
							mwrItemDTO.setOverrideRegPrice(prItemDTO.getRecommendedRegPrice());
							mwrItemDTO.setSystemOverrideFlag(prItemDTO.isSystemOverrideFlag());
							mwrItemDTO.setExplainLog(prItemDTO.getExplainLog());
						}
						//set pending retails if present
						
						mwrItemDTO.setPendingRetail(prItemDTO.getPendingRetail());
						mwrItemDTO
								.setRecommendedRegPriceBeforeUpdateRec(prItemDTO.getRecRegPriceBeforeReRecommedation());
						
						// Added by Karishma on 08/24/2022 to make the systemoverride flag as
						// 0 when the override is removed and set the original impact

						if (prItemDTO.getOverrideRemoved() == 1) {
							prItemDTO.setUserOverrideFlag(0);
							mwrItemDTO.setUserOverrideFlag(false);
							mwrItemDTO.setOverrideRemoved(1);
							if (mwrItemDTO.getPendingRetail() == null)
								mwrItemDTO.setPriceChangeImpact(prItemDTO.getPriceChangeImpact());
							else
								mwrItemDTO.setPriceChangeImpact(prItemDTO.getApprovedImpact());

						}

						// Changes done by Bhargavi on 12/10/2020
						// update the new calculated impact only when the item is user override or
						// system override
						if (mwrItemDTO.isUserOverrideFlag() || mwrItemDTO.isSystemOverrideFlag()) {
							mwrItemDTO.setPriceChangeImpact(prItemDTO.getPriceChangeImpact());
						}
						
						if(!usePrediction) {
							mwrItemDTO.setFinalPricePredictedMovement(prItemDTO.getAvgMovement());
							mwrItemDTO.setFinalPricePredictionStatus(PredictionStatus.SUCCESS.getStatusCode());
							mwrItemDTO.setCurrentRegUnits(prItemDTO.getAvgMovement());
							logger.info("Inside for item"+ mwrItemDTO.getItemCode()  +"final predicted movement "+ mwrItemDTO.getFinalPricePredictedMovement());
						}
						
					//Changes done by Karishma on 05/25
					//Added to populate the map only with items having override or override removed 
						if ((mwrItemDTO.isUserOverrideFlag() || mwrItemDTO.isSystemOverrideFlag()
								|| mwrItemDTO.getOverrideRemoved() == 1) && updatedItemsMap != null) {
							HashMap<ItemKey, MWRItemDTO> updatedItems = new HashMap<>();
							if (updatedItemsMap.containsKey(recWeekKey)) {
								updatedItems = updatedItemsMap.get(recWeekKey);
							}
							updatedItems.put(itemKey, mwrItemDTO);
							updatedItemsMap.put(recWeekKey, updatedItems);
						}
						overrideItems.put(itemKey, mwrItemDTO);
					}
				});

				overrideWeeklyItemDataMap.put(recWeekKey, overrideItems);
			}else
			{
				//added else condition for completed weeks to update the impact as its updated for normal weeks
				//This is added because the period summary after override was not getting correct impact
				itemMap.forEach((itemKey, mwrItemDTO) -> {
					HashMap<ItemKey, MWRItemDTO> overrideItems = new HashMap<>();
					if (overrideItemDataMap.containsKey(itemKey)) {
						PRItemDTO prItemDTO = overrideItemDataMap.get(itemKey);
					
						if (mwrItemDTO.isUserOverrideFlag() || mwrItemDTO.isSystemOverrideFlag()) {
							mwrItemDTO.setPriceChangeImpact(prItemDTO.getPriceChangeImpact());
						}//Added condition for checking if pending retail is not present
						if (mwrItemDTO.isUserOverrideFlag() == false && mwrItemDTO.getPendingRetail() == null) {
							if (mwrItemDTO.getPriceChangeImpact() != prItemDTO.getPriceChangeImpact()) {
								mwrItemDTO.setPriceChangeImpact(prItemDTO.getPriceChangeImpact());
							}
						}
						if (mwrItemDTO.isUserOverrideFlag() == false && mwrItemDTO.getPendingRetail() != null) {
							mwrItemDTO.setPriceChangeImpact(prItemDTO.getPriceChangeImpact());
						}
						overrideItems.put(itemKey, mwrItemDTO);
						overrideWeeklyItemDataMap.put(recWeekKey, overrideItems);
					
					}
				});
				
			}

		});
		return overrideWeeklyItemDataMap;
	}

	/**
	 *
	 * @param weeklyItemDataMap
	 * @param overrideItemDataMap
	 * @param recWeekStartDateStr
	 * @return mutliweekoverride items
	 */
	List<MWRItemDTO> updateQuarterlySummaryItems(
			LinkedHashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideWeeklyItemDataMap) {
		List<MWRItemDTO> quarterItemOverrideSummary = new ArrayList<>();
		HashMap<ItemKey, MWRItemDTO> itemSummary = new HashMap<>();

		overrideWeeklyItemDataMap.forEach((recWeekKey, itemMap) -> {
			itemMap.forEach((itemKey, mwrItemDTOOrig) -> {
				MWRItemDTO mwrItemDTO;
				try {
					mwrItemDTO = (MWRItemDTO) mwrItemDTOOrig.clone();
					//condition added  to not use prediction values when override is removed 
					if(mwrItemDTO.getOverrideRemoved()==0) {
					// Set predicted movement for recommended price point
					if (mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {
						MultiplePrice multipleSalePrice = new MultiplePrice(mwrItemDTO.getSaleMultiple(),
								mwrItemDTO.getSalePrice());
						PricePointDTO pricePointDTO = mwrItemDTO.getSalePricePredictionMap().get(multipleSalePrice);

						if (pricePointDTO != null) {
							// set the predicted movement only if there is no error from Prediction else set
							// it as 0
							// LIG aggregation uses -ve value which results in incorrect summary
							if (pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS
									.getStatusCode()) {
								mwrItemDTO.setFinalPricePredictedMovement(pricePointDTO.getPredictedMovement());

							} else {
								mwrItemDTO.setFinalPricePredictedMovement(0D);
							}
							mwrItemDTO
									.setFinalPricePredictionStatus(pricePointDTO.getPredictionStatus().getStatusCode());
							mwrItemDTO.setFinalPriceMargin(PRCommonUtil.getMarginDollar(multipleSalePrice,
									mwrItemDTO.getFinalCost(), pricePointDTO.getPredictedMovement()));
							mwrItemDTO.setFinalPriceRevenue(PRCommonUtil.getSalesDollar(multipleSalePrice,
									pricePointDTO.getPredictedMovement()));
						}

					} else {
						PricePointDTO pricePointDTO = mwrItemDTO.getRegPricePredictionMap()
								.get(mwrItemDTO.getRecommendedRegPrice());

						if (pricePointDTO != null) {
							// set the predicted movement only if there is no error from Prediction else set
							// it as 0
							// LIG aggregation uses -ve value  which results in incorrect summary
							if (pricePointDTO.getPredictionStatus().getStatusCode() == PredictionStatus.SUCCESS
									.getStatusCode()) {
								mwrItemDTO.setFinalPricePredictedMovement(pricePointDTO.getPredictedMovement());

							} else {
								mwrItemDTO.setFinalPricePredictedMovement(0D);
							}
							mwrItemDTO
									.setFinalPricePredictionStatus(pricePointDTO.getPredictionStatus().getStatusCode());
							mwrItemDTO.setFinalPriceRevenue(PRCommonUtil.getSalesDollar(
									mwrItemDTO.getRecommendedRegPrice(), pricePointDTO.getPredictedMovement()));
							mwrItemDTO.setFinalPriceMargin(
									PRCommonUtil.getMarginDollar(mwrItemDTO.getRecommendedRegPrice(),
											mwrItemDTO.getCost(), pricePointDTO.getPredictedMovement()));
						}
					}
					}

					Double finalPricePredictedMovement = null;
					if (mwrItemDTO.getFinalPricePredictedMovement() != null) {
						finalPricePredictedMovement = mwrItemDTO.getFinalPricePredictedMovement();
					}

					if (itemSummary.containsKey(itemKey)) {
						MWRItemDTO mwrItemDTOBase = itemSummary.get(itemKey);
						mwrItemDTOBase.setRegUnits(
								mwrItemDTOBase.getRegUnits() + checkNullAndReturnZero(finalPricePredictedMovement));
						mwrItemDTOBase.setRegRevenue(mwrItemDTOBase.getRegRevenue()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						mwrItemDTOBase.setRegMargin(mwrItemDTOBase.getRegMargin()
								+ checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));
					} else {
						mwrItemDTO.setRegUnits(checkNullAndReturnZero(finalPricePredictedMovement));
						mwrItemDTO.setRegRevenue(checkNullAndReturnZero(mwrItemDTO.getFinalPriceRevenue()));
						mwrItemDTO.setRegMargin(checkNullAndReturnZero(mwrItemDTO.getFinalPriceMargin()));
						itemSummary.put(itemKey, mwrItemDTO);
					}
				} catch (CloneNotSupportedException e) {
					e.printStackTrace();
				}
			});
		});

		itemSummary.forEach((key, item) -> {
			quarterItemOverrideSummary.add(item);
		});

		return quarterItemOverrideSummary;
	}

	/**
	 *
	 * @param weeklyItemDataMap
	 * @param overrideItemDataMap
	 * @param recWeekStartDateStr
	 * @return mutliweekoverride items
	 */
	List<MWRItemDTO> updatePeriodSummaryItems(
			LinkedHashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideWeeklyItemDataMap) throws Exception {
		List<MWRItemDTO> periodOverrideItems = new ArrayList<>();
		HashMap<Integer, HashMap<ItemKey, List<MWRItemDTO>>> periodSummary = new HashMap<>();
		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> multiWeekEntry : overrideWeeklyItemDataMap
				.entrySet()) {
			for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : multiWeekEntry.getValue().entrySet()) {
				MWRItemDTO mwrItemDTO = (MWRItemDTO) itemEntry.getValue().clone();
				mwrItemDTO.setRecWeekKey(multiWeekEntry.getKey());
				if (periodSummary.containsKey(mwrItemDTO.getPeriodCalendarId())) {
					HashMap<ItemKey, List<MWRItemDTO>> itemSummary = periodSummary
							.get(mwrItemDTO.getPeriodCalendarId());
					List<MWRItemDTO> itemList = new ArrayList<>();
					if (itemSummary.containsKey(itemEntry.getKey())) {
						itemList = itemSummary.get(itemEntry.getKey());
					}
					itemList.add(mwrItemDTO);
					itemSummary.put(itemEntry.getKey(), itemList);
				} else {
					HashMap<ItemKey, List<MWRItemDTO>> itemSummary = new HashMap<>();
					List<MWRItemDTO> itemList = new ArrayList<>();
					itemList.add(mwrItemDTO);
					itemSummary.put(itemEntry.getKey(), itemList);
					periodSummary.put(mwrItemDTO.getPeriodCalendarId(), itemSummary);
				}
			}
		}

		for (Map.Entry<Integer, HashMap<ItemKey, List<MWRItemDTO>>> multiWeekEntry : periodSummary.entrySet()) {
			for (Map.Entry<ItemKey, List<MWRItemDTO>> itemEntry : multiWeekEntry.getValue().entrySet()) {
				try {
					MWRItemDTO mwrItemDTOBase = (MWRItemDTO) itemEntry.getValue().get(0).clone();
					mwrItemDTOBase.setRegUnits(0D);
					mwrItemDTOBase.setRegMargin(0D);
					mwrItemDTOBase.setRegRevenue(0D);
					mwrItemDTOBase.setPeriodCalendarId(multiWeekEntry.getKey());
					for (MWRItemDTO mwrItemDTO : itemEntry.getValue()) {
						mwrItemDTOBase.setRegUnits(
								mwrItemDTOBase.getRegUnits() + checkNullAndReturnZero(mwrItemDTO.getRegUnits()));
						mwrItemDTOBase.setRegMargin(
								mwrItemDTOBase.getRegMargin() + checkNullAndReturnZero(mwrItemDTO.getRegMargin()));
						mwrItemDTOBase.setRegRevenue(
								mwrItemDTOBase.getRegRevenue() + checkNullAndReturnZero(mwrItemDTO.getRegRevenue()));
					}

					periodOverrideItems.add(mwrItemDTOBase);
				} catch (CloneNotSupportedException e) {
					e.printStackTrace();
				}
			}
		}

		return periodOverrideItems;
	}

	/**
	 *
	 * @param overrideWeeklyItemDataMap
	 * @return reveresed map by week
	 */
	LinkedHashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> getSortedMap(
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideWeeklyItemDataMap) {
		LinkedHashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> sortedDataMap = new LinkedHashMap<>();

		Set<RecWeekKey> keySet = overrideWeeklyItemDataMap.keySet();
		List<RecWeekKey> keyList = new ArrayList<>(keySet);
		Collections.sort(keyList, Comparator.comparing(RecWeekKey::getStartDateAsDate).reversed());

		keyList.forEach(recWeekKey -> {
			sortedDataMap.put(recWeekKey, overrideWeeklyItemDataMap.get(recWeekKey));
		});

		return sortedDataMap;
	}

	/**
	 *
	 * @param value
	 * @return 0 or value
	 */
	private double checkNullAndReturnZero(Double value) {
		if (value == null) {
			return 0;
		} else {
			return value;
		}
	}

	void callAudit(Connection conn, RecommendationInputDTO recommendationRunHeader) throws GeneralException {
		// Calling Audit process after pricing recommendation is successful
		AuditEngineWS auditEngine = new AuditEngineWS();
		auditEngine.isIntegratedProcess = true;
		logger.info("callAudit()-calling audit");
		long reportId = auditEngine.getAuditHeader(conn, recommendationRunHeader.getLocationLevelId(),
				recommendationRunHeader.getLocationId(), recommendationRunHeader.getProductLevelId(),
				recommendationRunHeader.getProductId(), PRConstants.BATCH_USER, recommendationRunHeader.getStartWeek());
		logger.info("callAudit()-Audit is completed for report id - " + reportId);
	}

	/**
	 *
	 * @param items
	 * @param weeklyItemDataMap
	 * @param secZoneRecMap
	 */
	private void addSecondaryZoneRec(List<PRItemDTO> items,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			HashMap<ItemKey, List<SecondaryZoneRecDTO>> secZoneRecMap) {

		items.forEach(item -> {
			ItemKey itemKey = PRCommonUtil.getItemKey(item);
			if (secZoneRecMap.containsKey(itemKey)) {
				item.setSecondaryZones(secZoneRecMap.get(itemKey));
				item.setSecondaryZoneRecPresent(true);
			}
		});

		weeklyItemDataMap.forEach((weekKey, itemMap) -> {
			itemMap.forEach((itemKey, item) -> {
				if (secZoneRecMap.containsKey(itemKey)) {
					item.setSecondaryZones(secZoneRecMap.get(itemKey));
					item.setSecondaryZoneRecPresent(true);
				}
			});
		});
	}

	public void aggregateUnitsSalesAndMarginAtLIGLevel(
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weekEntry : weeklyItemDataMap.entrySet()) {
			HashMap<ItemKey, MWRItemDTO> itemMapWeekly = weekEntry.getValue();
			for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : itemMapWeekly.entrySet()) {
				MWRItemDTO mwrItemDTO = itemEntry.getValue();
				if (mwrItemDTO.isLir()) {
					mwrItemDTO.setFinalPricePredictedMovement(0D);
					mwrItemDTO.setFinalPriceMargin(0D);
					mwrItemDTO.setFinalPriceRevenue(0D);
					mwrItemDTO.setCurrentRegUnits(0D);
					mwrItemDTO.setCurrentRegMargin(0D);
					mwrItemDTO.setCurrentRegRevenue(0D);
					if (retLirMap.containsKey(mwrItemDTO.getRetLirId())) {
						List<PRItemDTO> ligMembers = retLirMap.get(mwrItemDTO.getRetLirId());
						for (PRItemDTO prItemDTO : ligMembers) {
							ItemKey itemKey = PRCommonUtil.getItemKey(prItemDTO);
							if (itemMapWeekly.containsKey(itemKey)) {
								MWRItemDTO ligMember = itemMapWeekly.get(itemKey);
								mwrItemDTO.setFinalPricePredictedMovement(mwrItemDTO.getFinalPricePredictedMovement()
										+ checkNullAndReturnZero(ligMember.getFinalPricePredictedMovement()));
								mwrItemDTO.setFinalPriceRevenue(mwrItemDTO.getFinalPriceRevenue()
										+ checkNullAndReturnZero(ligMember.getFinalPriceRevenue()));
								mwrItemDTO.setFinalPriceMargin(mwrItemDTO.getFinalPriceMargin()
										+ checkNullAndReturnZero(ligMember.getFinalPriceMargin()));
								mwrItemDTO.setCurrentRegUnits(mwrItemDTO.getCurrentRegUnits()
										+ checkNullAndReturnZero(ligMember.getCurrentRegUnits()));
								mwrItemDTO.setCurrentRegMargin(mwrItemDTO.getCurrentRegMargin()
										+ checkNullAndReturnZero(ligMember.getCurrentRegMargin()));
								mwrItemDTO.setCurrentRegRevenue(mwrItemDTO.getCurrentRegRevenue()
										+ checkNullAndReturnZero(ligMember.getCurrentRegRevenue()));
							}
						}
					}
				}
			}
		}
	}

	private  void updateWeighedRetail(List<MWRItemDTO> quarterlyOverriddenItems, Long runId) throws GeneralException {
		List<MWRItemDTO> overiddenMap = new ArrayList<MWRItemDTO>();
		WeeklyRecDAO weeklyRecDAO = new WeeklyRecDAO();

		quarterlyOverriddenItems.forEach(item -> {

			if (item.isSystemOverrideFlag() || item.isUserOverrideFlag() || item.getOverrideRemoved() == 1) {
				overiddenMap.add(item);
			}

		});

		logger.info("updateWeighedRetail()- for runid : - " + runId + " OverrideMap size :- " + overiddenMap.size());
		weeklyRecDAO.updateWeightedPriceDetailsQuarterly(conn, overiddenMap, runId, Constants.CALENDAR_QUARTER);

	}

	/**
	 * 
	 * @param conn
	 * @param strategyMap
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @throws GeneralException
	 */
	private void updateGuardrailZonesData(Connection conn, HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap,
			RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap)
			throws GeneralException {
		// Added for GE to check if the guardrail cosntarint is defined for ZoneId
		GenaralService genaralService = new GenaralService();
		Set<Integer> uniqueZoneIDs = new HashSet<Integer>();

		for (Entry<StrategyKey, List<PRStrategyDTO>> stEntry : strategyMap.entrySet()) {

			List<PRStrategyDTO> strDetails = stEntry.getValue();
			for (PRStrategyDTO strategyDTO : strDetails) {
				if (strategyDTO.getConstriants().getGuardrailConstraint() != null) {
					List<PRConstraintGuardRailDetail> guardRaildetails = strategyDTO.getConstriants()
							.getGuardrailConstraint().getCompetitorDetails();

					if (guardRaildetails != null) {
						Set<Integer> uniqueZones = guardRaildetails.stream()
								.map(PRConstraintGuardRailDetail::getPriceZoneID).collect(Collectors.toSet());
						if (uniqueZones.size() > 0) {
							for (Integer zoneId : uniqueZones) {
								uniqueZoneIDs.add(zoneId);
							}
						}
					}

				}
			}

		}
		logger.info("setupBaseData() - # of guardrailConstraint for zoneID : " + uniqueZoneIDs.size());
		if (uniqueZoneIDs.size() > 0) {
			for (Integer zone : uniqueZoneIDs) {
				logger.info("setupBaseData() - Unique  zoneID : " + zone);
			}
		}
		// if uniqueZoneIDs size is populated then get the latest reccs for these
		// zoneIDs;
		if (uniqueZoneIDs.size() > 0) {
			new BaseDataService().setLatestRecsForZones(itemDataMap, uniqueZoneIDs, genaralService,
					recommendationInputDTO, conn);
		}
	}
	/** Added for PROM-2223 by KARISHMA ***/
	public void updateIsReccFlagForGlobalzone(List<MWRItemDTO> quarterlyOverriddenItems, Long runId) throws GeneralException {
		List<MWRItemDTO> recommendedItemMap = new ArrayList<MWRItemDTO>();
		
		WeeklyRecDAO weeklyRecDAO = new WeeklyRecDAO();

		quarterlyOverriddenItems.forEach(item -> {

			if (item.getPriceChangeImpact() != 0) {
				recommendedItemMap.add(item);
			} 
		});

		logger.info("updateIsReccFlagForGlobalzone()- for runid : - " + runId + " reccMap size :- " + recommendedItemMap.size() );
		weeklyRecDAO.updateRecommededFlagAtQtrLevel(conn,recommendedItemMap,runId);
	}

}
