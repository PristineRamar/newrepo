package com.pristine.service.offermgmt.mwr.core;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.StrategyDAO;
import com.pristine.dao.offermgmt.mwr.MWRSummaryDAO;
import com.pristine.dao.offermgmt.mwr.WeeklyRecDAO;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dataload.offermgmt.mwr.QuarterlyDashboard;
import com.pristine.dataload.offermgmt.mwr.summary.MultiWeekRecSummary;
import com.pristine.dataload.offermgmt.mwr.summary.SummarySplitup;
import com.pristine.dto.LigFlagsDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainAdditionalDetail;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.CLPDLPPredictionDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRRunHeader;
import com.pristine.dto.offermgmt.mwr.MWRSummaryDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.mwr.SummarySplitupDTO;
import com.pristine.dto.offermgmt.prediction.RunStatusDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.AuditTrailStatusLookup;
import com.pristine.lookup.AuditTrailTypeLookup;
import com.pristine.service.PredictionComponent;
import com.pristine.service.offermgmt.AuditTrailService;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.PricingEngineHelper;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.multiWeekPrediction.CLPDLPPredictionService;
import com.pristine.service.offermgmt.mwr.basedata.GenaralService;
import com.pristine.service.offermgmt.mwr.basedata.MovementDataService;
import com.pristine.service.offermgmt.mwr.core.finalizeprice.MultiWeekPriceFinalizer;
import com.pristine.service.offermgmt.mwr.itemattributes.ItemAttributeService;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class MWRUpdateRecommendationServiceV2 {

	private boolean isOnline = true;

	private static Logger logger = Logger.getLogger("MWRUpdateRecommendationServiceV2");

	boolean calculateGlobalZoneImpact = Boolean
			.parseBoolean(PropertyManager.getProperty("CALC_AGG_IMPACT_FOR_GLOBAL_ZONE", "FALSE"));
	int noOfWeeksBehind = Integer.parseInt(PropertyManager.getProperty("PR_AVG_MOVEMENT"));
	int noOfWeeksIMSData = Integer.parseInt(PropertyManager.getProperty("PR_NO_OF_WEEKS_IMS_DATA"));
	boolean useCommonAPIForPrice = Boolean
			.parseBoolean(PropertyManager.getProperty("USE_COMMON_API_FOR_PRICE", "FALSE"));
	boolean checkForPendingRetails = Boolean
			.parseBoolean(PropertyManager.getProperty("CHECK_PENDING_RETAILS", "FALSE"));
	boolean markItemsNotTosendToPred = Boolean
			.parseBoolean(PropertyManager.getProperty("MARK_NON_MOVING_ITEMS_NOT_TO_SEND_TO_PREDICTION", "FALSE"));
	String substituteForPrediction = (PropertyManager.getProperty("SUBSTITUE_FOR_PREDICTION", "52_WEEKS_AVG"));
	
	public List<RunStatusDTO> UpdateOnlyOverridenItems(Connection conn, List<Long> runIds, String userId)
			throws GeneralException, OfferManagementException {

		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		ItemService itemService = new ItemService(executionTimeLogs);
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		MWRSummaryDAO mwrSummaryDAO = new MWRSummaryDAO();
		WeeklyRecDAO weeklyRecDAO = new WeeklyRecDAO();
		CommonDataHelper commonDataHelper = new CommonDataHelper();
		List<RunStatusDTO> runStatusDTOs = new ArrayList<RunStatusDTO>();
		GenaralService genaralService = new GenaralService();
		PricingEngineService pricingEngineService = new PricingEngineService();
		PredictionComponent predictionComponent = new PredictionComponent();
		HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData = null;
		List<Integer> priceZoneStores = new ArrayList<Integer>();
		PricingEngineWS pricingEngineWS = new PricingEngineWS();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = new HashMap<String, RetailCalendarDTO>();
		List<String> priceAndStrategyZoneNos = new ArrayList<String>();
		CLPDLPPredictionService clpdlpPredictionService = new CLPDLPPredictionService();
		SummarySplitup summarySplitup = new SummarySplitup(null, conn);
		MultiWeekRecSummary multiWeekRecSummary = new MultiWeekRecSummary();
		HashMap<Long, PRStrategyDTO> strategyFullDetails = null;
		StrategyDAO strategyDAO = new StrategyDAO();

		try {

			String runType = PropertyManager.getProperty("PR_RUN_TYPE", "O");
			logger.info("Run Type - " + runType);
			if (runType != null && PRConstants.RUN_TYPE_ONLINE == runType.charAt(0)) {
				isOnline = true;
			} else
				isOnline = false;
			logger.info("Fetching Quarterly Data ...");
			HashMap<Long, List<PRItemDTO>> runAndItItems = mwrSummaryDAO.getRecItemsOfRunIds(conn, runIds);

			logger.info("runAndItItems # run Id's: " + runAndItItems.size());

			// Update all recs as started
			for (Long runId : runAndItItems.keySet()) {
				pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 0, "Yet to start");
			}

			// Get multiweek item details
			logger.info("Fetching weekly data started ...");
			HashMap<Long, HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>>> weeklyItemMapByRunId = weeklyRecDAO
					.getMultiWeeKItemsByRunId(conn, runIds);
			logger.info("Fetching weekly data Completed ...");

			strategyFullDetails = getStrategyDetails(conn, runAndItItems, strategyDAO);

			List<RetailCalendarDTO> fullCalendar = retailCalendarDAO.getFullRetailCalendar(conn);
			allWeekCalendarDetails = retailCalendarDAO.getAllWeeks(conn);

			HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap = pricingEngineDAO
					.getRecommendationRules(conn);

			Long runId = null;
			for (Map.Entry<Long, List<PRItemDTO>> entry : runAndItItems.entrySet()) {
				try {
					runId = entry.getKey();
					logger.info(" Update started for run id: " + runId);

					// 2. Convert current data to multiple weeks data
					HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap = weeklyItemMapByRunId
							.get(entry.getKey());

					HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory = new HashMap<>();

					HashMap<Integer, HashMap<String, RetailPriceDTO>> itemZonePriceHistory = new HashMap<>();

					RecommendationInputDTO recommendationInputDTO = mwrSummaryDAO.getRecommendationRunHeader(conn,
							runId);
					commonDataHelper.setIsGlobalZone(conn, recommendationInputDTO);

					recommendationInputDTO.setBaseWeek(recommendationInputDTO.getStartWeek());
					recommendationInputDTO.setRunType(PRConstants.RUN_TYPE_ONLINE);

					// set chain Id
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
					recRunHeader.setPredictedBy(userId);
					recRunHeader.setRunId(runId);
					MWRRunHeader mwrRunHeader = MWRRunHeader.getRunHeaderDTO(recommendationInputDTO, runId, 0);
					recommendationInputDTO.setMwrRunHeader(mwrRunHeader);
					recommendationInputDTO.setRunId(runId);

					RetailCalendarDTO curCalDTO = null;
					curCalDTO = pricingEngineWS.getCurCalDTO(conn, retailCalendarDAO);
					priceAndStrategyZoneNos = itemService.getPriceAndStrategyZoneNos(entry.getValue());

					PRStrategyDTO inputDTO = new PRStrategyDTO();
					PRStrategyDTO leadInputDTO = new PRStrategyDTO();
					int leadZoneId = 0, leadZoneDivisionId = 0, divisionId = 0;
					pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 0, "Started");

					List<PRProductGroupProperty> productGroupProperties = new MWRUpdateRecommendationService()
							.getProductGroupProperties(conn, recRunHeader, pricingEngineDAO);

					getInputDTODetails(conn, recRunHeader, inputDTO, leadInputDTO, leadZoneId, leadZoneDivisionId,
							divisionId, pricingEngineDAO, pricingEngineService);

					logger.info("UpdateOnlyOverridenItems() - Updating recommendation for Run Id "
							+ entry.getKey() + " is started...");

					RetailCalendarDTO calDTO = pricingEngineWS.getLatestWeek(conn, inputDTO, retailCalendarDAO);

					RetailCalendarDTO resetCalDTO = new MWRUpdateRecommendationService().getResetCalDTO(inputDTO,
							retailCalendarDAO);
					
					LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails = retailCalendarDAO
							.getAllPreviousWeeks(conn, curCalDTO.getStartDate(), noOfWeeksIMSData);

					HashMap<ItemKey, PRItemDTO> itemDataMap = new MWRUpdateRecommendationService()
							.getItemDetails(entry.getValue());
					//applicable only for AZ
					if (checkForPendingRetails) {
						genaralService.getItemsWithPendingRetailsFromQueue(conn, recommendationInputDTO, itemDataMap);
					}
					HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);

					logger.info("# of items in itemDataMap: - " + itemDataMap.size());

					logger.info("# of items in retLirMap: - " + retLirMap.size());

					// mark overriden items
					HashMap<ItemKey, PRItemDTO> overriddenItemDataMap = getOverriddenItemMap(itemDataMap, retLirMap);
					logger.info("# of items in overriddenItemDataMap: - " + overriddenItemDataMap.size());
					
 					new MWRUpdateRecommendationService().updateItemWithStrategyDTO(overriddenItemDataMap, strategyFullDetails);
 					
 					boolean usePrediction = PricingEngineHelper.isUsePrediction(productGroupProperties, inputDTO.getProductLevelId(), inputDTO.getProductId(),
 							inputDTO.getLocationLevelId(), inputDTO.getLocationId());

					logger.info(" run id: " + runId + " Use prediction flag: " + usePrediction);
					
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

					//do not need to call this for customers where  price is fetched  from API
					//Added for FF
					if (!useCommonAPIForPrice) {

					itemPriceHistory = pricingEngineDAO.getPriceHistory(conn, Integer.parseInt(chainId),
							allWeekCalendarDetails, calDTO, resetCalDTO, overriddenItemDataMap, priceAndStrategyZoneNos,
							priceZoneStores, false);

					itemZonePriceHistory = pricingEngineService.getItemZonePriceHistory(Integer.parseInt(chainId),
							inputDTO.getLocationId(), itemPriceHistory);
					}

					pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 25, "Processing overridden items");

					updateIsprocessedForOverriddenItem(overriddenItemDataMap);

					List<PRItemDTO> overriddenItemList = new ArrayList<PRItemDTO>();

					overriddenItemDataMap.forEach((itemKey, prItemDTO) -> {
						// if there is no system/user override or if override is removed revert the
						// recommended price to original
						// price

						if (!prItemDTO.isSystemOverrideFlag()
								&& (prItemDTO.getUserOverrideFlag() != 1 || prItemDTO.getOverrideRemoved() == 1)) {
							prItemDTO.setRecommendedRegPrice(prItemDTO.getRecRegPriceBeforeReRecommedation());
						}
						overriddenItemList.add(prItemDTO);
					});

					logger.info("# of items in overriddenItemList  :" + overriddenItemList.size());

					pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 50, "Predicting new price points");

					PredictionService predictionService = new PredictionService(movementData, itemPriceHistory,
							retLirMap);

					// Zone level prediction
					predictionComponent.predictRegPricePoints(conn, overriddenItemDataMap, overriddenItemList, inputDTO,
							productGroupProperties, recRunHeader, executionTimeLogs, isOnline, retLirMap,
							predictionService);

					/*** PROM-2223 changes **/
					// Changes done by Karishma on 11/15/2021 for AZ
					// If recc for global zone 1000 ,get the regPrices and L13 units of latest runs
					// other zones for each item and calculate impact
					// against new reatail of global zone- reg retail and L13 units of each zone
					if (recommendationInputDTO.isGlobalZone() && calculateGlobalZoneImpact) {
						// Added new parameter to identify that its called from update reccs and get
						// only
						// the curr retails for Z4 and z16 and not the L13Units because its already set
						genaralService.getLatestRegRetailslOfAllZones(overriddenItemDataMap, recommendationInputDTO,
								retLirMap, conn, true);
					}
					/** PROM-2223 changes end **/
					logger.info("# of items in map for passing to impact :" + overriddenItemDataMap.size());
					pricingEngineWS.calculatePriceChangeImpact(overriddenItemDataMap, retLirMap,
							recommendationInputDTO.isGlobalZone());

					pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 75,
							"Updating predictions for quarter");

					MutltiWeekPredInputBuilder mutltiWeekPredInputBuilder = new MutltiWeekPredInputBuilder();
					MutliWeekPredictionComponent mutliWeekPredictionComponent = new MutliWeekPredictionComponent(conn);

					HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> updatedItemsMap = new HashMap<>();
					HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideWeeklyItemDataMap = new MWRUpdateRecommendationService()
							.getOverrideWeeklyItemDataMap(weeklyItemDataMap, overriddenItemDataMap,
									recommendationInputDTO.getStartWeek(), fullCalendar, updatedItemsMap,usePrediction);

					if (usePrediction) {
						pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 75,
								"Updating predictions for quarter");
						// 3. Get inputs for prediction
						List<MultiWeekPredEngineItemDTO> multiWeekPredInput = mutltiWeekPredInputBuilder
								.buildInputForPrediction(overrideWeeklyItemDataMap, recommendationInputDTO);

						logger.info("UpdateOnlyOverridenItems() - Calling multi week prediction...");

						logger.info("UpdateOnlyOverridenItems() - # of price points to be predicted: "
								+ multiWeekPredInput.size());

						List<MultiWeekPredEngineItemDTO> predictionCache = new GenaralService().getPredictionCache(conn,
								recommendationInputDTO.getMwrRunHeader());

					logger.info(
							"UpdateOnlyOverridenItems() - # of records in prediction cache: " + predictionCache.size());

						// 4. Call predicition
						List<MultiWeekPredEngineItemDTO> multiWeekPredOuput = mutliWeekPredictionComponent
							.callMultiWeekPrediction(recommendationInputDTO, multiWeekPredInput, allWeekCalendarDetails,
									predictionCache);

					logger.info(
							"UpdateOnlyOverridenItems() - # of price points predicted: " + multiWeekPredOuput.size());

						mutliWeekPredictionComponent.updateWeeklyPredictedMovement(overrideWeeklyItemDataMap,
								multiWeekPredOuput, allWeekCalendarDetails);
					} else
						pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 75,
								"Updating movement for Quarter");

					// 5. Update prediction for LIG
					mutliWeekPredictionComponent.updateLigLevelMovement(overrideWeeklyItemDataMap, retLirMap);
					
					new MultiWeekPriceFinalizer().applyRecommendationToAllWeeks(recommendationInputDTO,
							overriddenItemDataMap, overrideWeeklyItemDataMap, itemZonePriceHistory,
							recommendationRuleMap);

					new MWRUpdateRecommendationService().aggregateUnitsSalesAndMarginAtLIGLevel(weeklyItemDataMap,
							retLirMap);

					LinkedHashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMapSorted = new MWRUpdateRecommendationService()
							.getSortedMap(weeklyItemDataMap);

					List<MWRItemDTO> quarterlyOverriddenItems = new MWRUpdateRecommendationService()
							.updateQuarterlySummaryItems(weeklyItemDataMapSorted);

					List<MWRItemDTO> periodOverriddenItems = new MWRUpdateRecommendationService()
							.updatePeriodSummaryItems(weeklyItemDataMapSorted);

					weeklyRecDAO.updateOverrideStatus(conn, updatedItemsMap);
					
					// Adding config based change because its applicable for only AZ
					if (checkForPendingRetails) {
						UpdateImpactforItemsWithPendingRetail(overrideWeeklyItemDataMap, updatedItemsMap);

						// Added to update the data for only items where there is override/user override
						// is removed.
						// update prediction details, check for pending retail and mark the necessary
						// fields and set the explain log
						// set the flag of impact included in the summary Calculation.
						updateAdditionalDetails(overrideWeeklyItemDataMap, updatedItemsMap);
					}

					String noOfItemUpdateStatus = weeklyRecDAO.updateWeeklyRecommendationDetails(conn, updatedItemsMap);
						// Added to update the lig row flag if the impact is included or not at
					// WeekLevel based on the updated lig items ands also set the is recommended flag
					Map<RecWeekKey, HashMap<Integer, LigFlagsDTO>> LIGMapAtWeekLevel = updateFlagsForLigWeekly(
							weeklyItemDataMap, updatedItemsMap, retLirMap);
					if (LIGMapAtWeekLevel.size() > 0) {
						weeklyRecDAO.updateStatusFlagForLIGRow(conn, LIGMapAtWeekLevel, runId);
					}
					
					List<MWRItemDTO> updatedItems = getUpdatedItemsAtQtrLevel(quarterlyOverriddenItems,
							recommendationInputDTO.isGlobalZone());

					if (updatedItems.size() > 0) {
						weeklyRecDAO.updateReRecommendationDetailsQuarterly(conn, updatedItems,
								Constants.CALENDAR_QUARTER);
						// Added to update the lig row flag if the impact is included or not at
						// QTRLevel based on the updated lig items
						HashMap<Integer, LigFlagsDTO> LIGMapAtQtrLevel = updateImpactIncludedFlagForLIGAtQtrLevel(
								updatedItems, retLirMap, itemDataMap, quarterlyOverriddenItems);
						if (LIGMapAtQtrLevel.size() > 0) {
							weeklyRecDAO.updateStatusFlagForLIGRowForQtr(conn, LIGMapAtQtrLevel, runId);
						}
						
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
							currunitsTotalClpDlp = currunitsTotalClpDlp + Math.round(week.getCurrentSalesTotalClpDlp());
							currsalesTotalClpDlp = currsalesTotalClpDlp
									+ Math.round(week.getCurrentMarginTotalClpDlp());
							currmarginTotalClpDlp = currmarginTotalClpDlp
									+ Math.round(week.getCurrentMarginPctTotalClpDlp());
							currmarginTotalpct = currmarginTotalpct + Math.round(week.getUnitsTotalClpDlp());
							unitsTotalClpDlp = unitsTotalClpDlp + Math.round(week.getSalesTotalClpDlp());
							salesTotalClpDlp = salesTotalClpDlp + Math.round(week.getMarginTotalClpDlp());
							marginTotalClpDlp = marginTotalClpDlp + Math.round(week.getMarginTotalClpDlp());
							marginTotalpct = marginTotalpct + Math.round(week.getMarginPctTotalClpDlp());
							if (week.getPriceChangeImpact() != 0)
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

						summarySpltDto
								.setUnitsTotal(periods.stream().mapToDouble(SummarySplitupDTO::getUnitsTotal).sum());

						summarySpltDto
								.setSalesTotal(periods.stream().mapToDouble(SummarySplitupDTO::getSalesTotal).sum());

						summarySpltDto
								.setMarginTotal(periods.stream().mapToDouble(SummarySplitupDTO::getMarginTotal).sum());

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

						summarySpltDto
								.setPromoUnits(periods.stream().mapToDouble(SummarySplitupDTO::getPromoUnits).sum());

						summarySpltDto
								.setPromoSales(periods.stream().mapToDouble(SummarySplitupDTO::getPromoSales).sum());
						summarySpltDto
								.setPromoMargin(periods.stream().mapToDouble(SummarySplitupDTO::getPromoMargin).sum());

						summarySpltDto.setPromoMarginPct(
								periods.stream().mapToDouble(SummarySplitupDTO::getPromoMarginPct).sum());

						summarySpltDto.setCurrentUnitsTotalClpDlp(
								periods.stream().mapToDouble(SummarySplitupDTO::getCurrentUnitsTotalClpDlp).sum());

						summarySpltDto.setCurrentSalesTotalClpDlp(
								periods.stream().mapToDouble(SummarySplitupDTO::getCurrentSalesTotalClpDlp).sum());

						summarySpltDto.setCurrentMarginTotalClpDlp(
								periods.stream().mapToDouble(SummarySplitupDTO::getCurrentMarginTotalClpDlp).sum());

						summarySpltDto.setCurrentMarginPctTotalClpDlp(
								periods.stream().mapToDouble(SummarySplitupDTO::getCurrentMarginPctTotalClpDlp).sum());

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

					List<MWRItemDTO> quarterItems = multiWeekRecSummary.getMultiWeekItemSummary(recommendationInputDTO,
							weeklyItemDataMap);

					MWRSummaryDTO mwrSummaryDTO = multiWeekRecSummary.getQuarterSummary(recommendationInputDTO,
							quarterItems, weeklySummary, weeklyItemDataMap);

					mwrSummaryDAO.updateMultiWeekSummary(conn, mwrSummaryDTO);

					mwrSummaryDAO.updateSecondaryZoneRecs(conn, recommendationInputDTO, quarterlyOverriddenItems);

					// 12. Update dashboardData
					new QuarterlyDashboard().populateDashboardData(conn, recommendationInputDTO.getLocationLevelId(),
							recommendationInputDTO.getLocationId(), recommendationInputDTO.getProductLevelId(),
							recommendationInputDTO.getProductId());

					// Update audit trail details
					AuditTrailService auditTrailService = new AuditTrailService();
					auditTrailService.auditRecommendation(conn, inputDTO.getLocationLevelId(), inputDTO.getLocationId(),
							inputDTO.getProductLevelId(), inputDTO.getProductId(),
							AuditTrailTypeLookup.RE_RECOMMENDATION.getAuditTrailTypeId(), PRConstants.REC_COMPLETED,
							recRunHeader.getRunId(), recRunHeader.getPredictedBy(),
							AuditTrailStatusLookup.AUDIT_TYPE.getAuditTrailTypeId(),
							AuditTrailStatusLookup.AUDIT_SUB_TYPE_UPDATE_RECC.getAuditTrailTypeId(),
							AuditTrailStatusLookup.AUDIT_SUB_TYPE_UPDATE_RECC.getAuditTrailTypeId(), 0, 0, 0);

					PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
					logger.info("updaterecommendUsingOverrideRetails()-Call Audit...");
					new MWRUpdateRecommendationService().callAudit(conn, recommendationInputDTO);

					String recStatus = "Re-Recommendation completed sucessfully " + noOfItemUpdateStatus.toString();
					pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 100,
							"Update recommendation is successful");
					pricingEngineDAO.updateOverridePredStatusInRunHeaderQR(conn, runId);
					new MWRUpdateRecommendationService().updateRunStatus(recRunHeader.getRunId(), runStatusDTOs,
							PRConstants.SUCCESS, recStatus);
					// }
					/*
					 * else { updateRunStatus(recRunHeader.getRunId(), runStatusDTOs,
					 * PRConstants.SUCCESS, "No price overwritten items available to recommend"); }
					 */

				} catch (Exception ex) {
					pricingEngineDAO.updateReRecommendationStatusQR(conn, runId, 100,
							"Error in updating Recommendation");
					pricingEngineDAO.updateRunStatusQR(conn, runId, PRConstants.RUN_STATUS_ERROR);
					// updateRunStatus(runId, runStatusDTOs, PRConstants.FAILURE, "Error in updating
					// Recommendation");

					PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");

					logger.error("Error in RerecommendUsingOverrideRetails() " + ex.toString(), ex);

				}
			}

		} catch (Exception oe) {
				throw new GeneralException("Error in RerecommendUsingOverrideRetails() ");
		}
		return runStatusDTOs;

	}

	private void updateIsprocessedForOverriddenItem(HashMap<ItemKey, PRItemDTO> overriddenItemDataMap) {
		overriddenItemDataMap.forEach((key, value) -> {
			if (value.getUserOverrideFlag() == 1) {
				value.setPriceRange(new Double[] { value.getOverriddenRegularPrice().price });
				value.setRecommendedRegPrice(value.getOverriddenRegularPrice());
				value.setRegMPack(value.getOverriddenRegularPrice().multiple);
				if (value.getRegMPack() == 1) {
					value.setRegMPrice(value.getOverriddenRegularPrice().price);
				} else {
					value.setRegPrice(value.getOverriddenRegularPrice().price);
				}
				value.setProcessed(true);
			} else {
				value.setProcessed(true);
			}
		});
	}

	private void getInputDTODetails(Connection conn, PRRecommendationRunHeader recRunHeader, PRStrategyDTO inputDTO,
			PRStrategyDTO leadInputDTO, int leadZoneId, int leadZoneDivisionId, int divisionId,
			PricingEngineDAO pricingEngineDAO, PricingEngineService pricingEngineService) throws GeneralException {

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

	private HashMap<Long, PRStrategyDTO> getStrategyDetails(Connection conn,
			HashMap<Long, List<PRItemDTO>> runAndItItems, StrategyDAO strategyDAO) throws OfferManagementException {
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

	private void UpdateImpactforItemsWithPendingRetail(
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideWeeklyItemDataMap,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> updatedItemsMap) {

		HashMap<Integer, List<MWRItemDTO>> retLirMap = null;

		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideEntry : overrideWeeklyItemDataMap.entrySet()) {

			retLirMap = new HashMap<Integer, List<MWRItemDTO>>();

			Set<Integer> ligWithpendingRetail = new HashSet<>();

			for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : overrideEntry.getValue().entrySet()) {
				if (!itemEntry.getValue().isLir() && itemEntry.getValue().getPendingRetail() != null
						&& itemEntry.getValue().getOverrideRegPrice() != null && itemEntry.getValue().getOverrideRegPrice().getUnitPrice()>0
						&& itemEntry.getValue().getRetLirId() > 0) {
					// update the approved impact field with the new impact calculated so that it
					// can be used for aggregating the impact at LIG level
					itemEntry.getValue().setApprovedImpact(itemEntry.getValue().getPriceChangeImpact());
					ligWithpendingRetail.add(itemEntry.getValue().getRetLirId());
				}

				if (!itemEntry.getValue().isLir() && itemEntry.getValue().getPendingRetail() != null) {
					List<MWRItemDTO> temp = new ArrayList<>();
					if (retLirMap.containsKey(itemEntry.getValue().getRetLirId())) {
						temp = retLirMap.get(itemEntry.getValue().getRetLirId());
					}
					temp.add(itemEntry.getValue());
					retLirMap.put(itemEntry.getValue().getRetLirId(), temp);
				}

			}

			// only if any pending retails are present
			if (retLirMap != null && retLirMap.size() > 0 && ligWithpendingRetail.size() > 0) {
				for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : overrideEntry.getValue().entrySet()) {

					if (itemEntry.getValue().isLir()
							&& ligWithpendingRetail.contains(itemEntry.getValue().getRetLirId())) {
						if (retLirMap.containsKey(itemEntry.getValue().getRetLirId())) {
							List<MWRItemDTO> ligMembers = retLirMap.get(itemEntry.getValue().getRetLirId());
							double priceChangeImpact = ligMembers.stream().mapToDouble(MWRItemDTO::getApprovedImpact)
									.sum();
							itemEntry.getValue().setApprovedImpact(priceChangeImpact);
							updatedItemsMap.get(overrideEntry.getKey()).put(itemEntry.getKey(), itemEntry.getValue());
						}
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param overrideWeeklyItemDataMap
	 * @param updatedItemsMap
	 */
	private void updateAdditionalDetails(HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideWeeklyItemDataMap,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> updatedItemsMap) {
		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideEntry : updatedItemsMap.entrySet()) {
			HashMap<ItemKey, MWRItemDTO> weeklyMapData = overrideWeeklyItemDataMap.get(overrideEntry.getKey());
			if (weeklyMapData != null && weeklyMapData.size() > 0)

				for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : overrideEntry.getValue().entrySet()) {

					MWRItemDTO mwrItemDTO = itemEntry.getValue();
					// if override is done for items on pending retail update the flag and clear the
					// additional log
					if (mwrItemDTO.getPendingRetail() != null && mwrItemDTO.getPendingRetail().getUnitPrice() > 0
							&& mwrItemDTO.getOverrideRegPrice() != null
							&& mwrItemDTO.getOverrideRegPrice().getUnitPrice() > 0 && mwrItemDTO.getOverrideRegPrice()
									.getUnitPrice() != mwrItemDTO.getPendingRetail().getUnitPrice()) {
						// update the flag to 0
						mwrItemDTO.setIsPendingRetailRecommended(0);

						if (mwrItemDTO.getExplainLog() != null) {
							List<PRExplainAdditionalDetail> explainAdditionalDetail = new ArrayList<PRExplainAdditionalDetail>();
							mwrItemDTO.getExplainLog().setExplainAdditionalDetail(explainAdditionalDetail);
						}

					} // if override is removed
					else if (mwrItemDTO.getPendingRetail() != null && mwrItemDTO.getPendingRetail().getUnitPrice() > 0
							&& mwrItemDTO.getRecommendedRegPrice() != null
							&& mwrItemDTO.getRecommendedRegPrice().getUnitPrice() > 0
							&& mwrItemDTO.getRecommendedRegPrice().getUnitPrice() == mwrItemDTO.getPendingRetail()
									.getUnitPrice()) {

						mwrItemDTO.setIsPendingRetailRecommended(1);

						if (mwrItemDTO.getExplainLog() != null) {
							List<PRExplainAdditionalDetail> explainAdditionalDetail = new ArrayList<PRExplainAdditionalDetail>();
							mwrItemDTO.getExplainLog().setExplainAdditionalDetail(
									new PricingEngineService().setPendingRetailsLog(explainAdditionalDetail));
						}

					}

					if (mwrItemDTO.getExplainLog() != null
							&& mwrItemDTO.getExplainLog().getGuidelineAndConstraintLogs() != null
							&& mwrItemDTO.getRecommendedRegPrice() != null) {
						mwrItemDTO.setIsConflict(
								UpdateConflicts(mwrItemDTO.getExplainLog().getGuidelineAndConstraintLogs(),
										mwrItemDTO.getRecommendedRegPrice().getUnitPrice(), mwrItemDTO.getItemCode()));
					}
					// Mark flag if the impact is included in SummaryCalculation
					CoreRecommendationService.setImpactField(mwrItemDTO);
				}
		}
	}

	private List<MWRItemDTO> getUpdatedItemsAtQtrLevel(List<MWRItemDTO> quarterlyOverriddenItems,
			boolean isglobalZone) {

		List<MWRItemDTO> updatedItems = new ArrayList<>();

		for (MWRItemDTO item : quarterlyOverriddenItems) {

			if (item.getOverrideRemoved() == 1 || item.isSystemOverrideFlag()
					|| (item.getOverrideRegPrice() != null && item.getOverrideRegPrice().getUnitPrice() > 0)) {
				if (calculateGlobalZoneImpact && isglobalZone) {
					if (item.getPriceChangeImpact() != 0 && (item.getPendingRetail() == null
							|| (item.getPendingRetail() != null && item.getOverrideRegPrice() != null))) {
						item.setNewPriceRecommended(true);
					} else
						item.setNewPriceRecommended(false);
				} else {
					if (item.getOverrideRegPrice() != null && item.getOverrideRegPrice().getUnitPrice() > 0
							&& item.getCurrentPrice() != null) {
						if (item.getOverrideRegPrice().getUnitPrice() != item.getCurrentPrice().getUnitPrice()) {
							item.setNewPriceRecommended(true);

						} else {
							item.setNewPriceRecommended(false);

						}

					} else if (item.getRecommendedRegPrice() != null && item.getRecommendedRegPrice().getUnitPrice() > 0
							&& item.getCurrentPrice() != null && item.getCurrentPrice().getUnitPrice() > 0
							&& item.getRecommendedRegPrice().getUnitPrice() != item.getCurrentPrice().getUnitPrice()
							&& item.getPendingRetail() == null) {
						item.setNewPriceRecommended(true);

					} else {
						item.setNewPriceRecommended(false);
					}
				}

				if (item.getPendingRetail() != null && item.getPendingRetail().getUnitPrice() > 0
						&& item.getOverrideRegPrice() != null && item.getOverrideRegPrice().getUnitPrice() > 0
						&& item.getOverrideRegPrice().getUnitPrice() != item.getPendingRetail().getUnitPrice()) {
					// update the flag to 0
					item.setIsPendingRetailRecommended(0);
					if (item.getExplainLog() != null) {
						List<PRExplainAdditionalDetail> explainAdditionalDetail = new ArrayList<PRExplainAdditionalDetail>();
						item.getExplainLog().setExplainAdditionalDetail(explainAdditionalDetail);
					}

				} // if override is removed
				else if (item.getPendingRetail() != null && item.getPendingRetail().getUnitPrice() > 0
						&& item.getRecommendedRegPrice() != null && item.getRecommendedRegPrice().getUnitPrice() > 0
						&& item.getRecommendedRegPrice().getUnitPrice() == item.getPendingRetail().getUnitPrice()) {

					item.setIsPendingRetailRecommended(1);
					if (item.getExplainLog() != null) {
						List<PRExplainAdditionalDetail> explainAdditionalDetail = new ArrayList<PRExplainAdditionalDetail>();
						item.getExplainLog().setExplainAdditionalDetail(
								new PricingEngineService().setPendingRetailsLog(explainAdditionalDetail));
					}
				}
				// Update the conflicts flag based on overriden retail
				if (item.getExplainLog() != null && item.getExplainLog().getGuidelineAndConstraintLogs() != null
						&& item.getRecommendedRegPrice() != null) {

					item.setIsConflict(UpdateConflicts(item.getExplainLog().getGuidelineAndConstraintLogs(),
							item.getRecommendedRegPrice().getUnitPrice(), item.getItemCode()));
				}
				CoreRecommendationService.setImpactField(item);
				updatedItems.add(item);
			}

		}
		logger.info("# of items updated :" + updatedItems.size());
		return updatedItems;

	}

	
	private int UpdateConflicts(List<PRGuidelineAndConstraintLog> getGuidelineAndConstraintLogs, double updatedPrice,
			int itemCode) {

		int conflictinguidelineConstraintFound = 0;
		for (PRGuidelineAndConstraintLog getGuidelineAndConstraintLog : getGuidelineAndConstraintLogs) {

			if (getGuidelineAndConstraintLog.getConstraintTypeId() == ConstraintTypeLookup.ROUNDING
					.getConstraintTypeId()) {
				List<Double> roundingDigits = getGuidelineAndConstraintLog.getRoundingDigits();
				if (roundingDigits != null && roundingDigits.size() > 0) {
					boolean isRecPricePartOfRounding = false;
					for (Double roundingDigit : roundingDigits) {

						if (roundingDigit.equals(updatedPrice)) {
							isRecPricePartOfRounding = true;
							break;
						}
					}

					if (!isRecPricePartOfRounding) {
						getGuidelineAndConstraintLog.setIsConflict(true);
						conflictinguidelineConstraintFound = 1;
					} else
						getGuidelineAndConstraintLog.setIsConflict(false);
				}
			} else {
				PRRange outputPriceRange = getGuidelineAndConstraintLog.getGuidelineOrConstraintPriceRange1();
				if (outputPriceRange != null) {
					if (outputPriceRange.getStartVal() != Constants.DEFAULT_NA && outputPriceRange.getStartVal() > 0
							&& outputPriceRange.getEndVal() != Constants.DEFAULT_NA
							&& outputPriceRange.getEndVal() > 0) {

						if (updatedPrice >= outputPriceRange.getStartVal()
								&& updatedPrice <= outputPriceRange.getEndVal())
							getGuidelineAndConstraintLog.setIsConflict(false);
						else {
							getGuidelineAndConstraintLog.setIsConflict(true);
							conflictinguidelineConstraintFound = 1;
						}

					} else if (outputPriceRange.getStartVal() != Constants.DEFAULT_NA
							& outputPriceRange.getStartVal() > 0) {
						if (updatedPrice >= outputPriceRange.getStartVal())
							getGuidelineAndConstraintLog.setIsConflict(false);
						else {
							getGuidelineAndConstraintLog.setIsConflict(true);
							conflictinguidelineConstraintFound = 1;
						}

					} else if (outputPriceRange.getEndVal() != Constants.DEFAULT_NA
							&& outputPriceRange.getEndVal() > 0) {
						if (updatedPrice <= outputPriceRange.getEndVal())
							getGuidelineAndConstraintLog.setIsConflict(false);
						else {
							getGuidelineAndConstraintLog.setIsConflict(true);
							conflictinguidelineConstraintFound = 1;
						}
					}

				}

			}
		}
		return conflictinguidelineConstraintFound;
	}

	/**
	 * 
	 * @param updatedItemsMap
	 * @param updatedItemsMap2 
	 * @param retLirMap
	 * @return
	 */
	private Map<RecWeekKey, HashMap<Integer, LigFlagsDTO>> updateFlagsForLigWeekly(
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyDatMap,
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> updatedItemsMap, HashMap<Integer, List<PRItemDTO>> retLirMap) {
		Map<RecWeekKey, HashMap<Integer, LigFlagsDTO>> finalLIGMap = new HashMap<>();
		for (Map.Entry<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> overrideEntry : updatedItemsMap.entrySet()) {
			List<MWRItemDTO> ligMembersMap = new ArrayList<>();
			
				HashMap<ItemKey, MWRItemDTO> weeklyDataMap = weeklyDatMap.get(overrideEntry.getKey());
				//get lig row  from the weeklytable and update the explain log if the rep item is overriden
				HashMap<Integer, MWRItemDTO> repItemMap=new HashMap<>();

			for (Map.Entry<ItemKey, MWRItemDTO> itemEntry : overrideEntry.getValue().entrySet()) {
				if (itemEntry.getValue().getRetLirId() > 0) {
					ligMembersMap.add(itemEntry.getValue());
					ItemKey repItem = PRCommonUtil.getItemKey(itemEntry.getValue().getRetLirId(), true);
					if (weeklyDataMap != null && weeklyDataMap.containsKey(repItem)) {
						repItemMap.put(itemEntry.getValue().getRetLirId(), weeklyDataMap.get(repItem));
					}
				}
			}
			HashMap<Integer, LigFlagsDTO> ligRowMap = new HashMap<>();
			if (ligMembersMap.size() > 0) {
				
				Map<Integer, List<MWRItemDTO>> lirItemCodeMap = ligMembersMap.stream()
						.collect(Collectors.groupingBy(MWRItemDTO::getRetLirId));
				
				setImpactCalFieldForLIG(ligRowMap, lirItemCodeMap, retLirMap,repItemMap);
				setIsRecommendedForLIG(ligRowMap, lirItemCodeMap, retLirMap);
				setWeeklyMovementForLIG(ligRowMap, lirItemCodeMap,weeklyDataMap);
				finalLIGMap.put(overrideEntry.getKey(), ligRowMap);
			}

		}
		return finalLIGMap;
	}

	/**
	 * 
	 * @param updatedItems
	 * @param retLirMap
	 * @param quarterlyOverriddenItems 
	 * @return
	 */
	private HashMap<Integer, LigFlagsDTO> updateImpactIncludedFlagForLIGAtQtrLevel(List<MWRItemDTO> updatedItems,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<ItemKey, PRItemDTO> itemDataMap, List<MWRItemDTO> quarterlyOverriddenItems) {

		List<MWRItemDTO> ligMembersMap = new ArrayList<>();
		HashMap<Integer, LigFlagsDTO> ligRowMap = new HashMap<>();
		
		HashMap<Integer, MWRItemDTO> repItemMap=new HashMap<>();

		for (MWRItemDTO itemEntry : updatedItems) {
			if (itemEntry.getRetLirId() > 0) {
				ItemKey repItem = PRCommonUtil.getItemKey(itemEntry.getRetLirId(), true);
				if (itemDataMap.containsKey(repItem)) {
					MWRItemDTO mwrItem = new MWRItemDTO();
					mwrItem.setExplainLog(itemDataMap.get(repItem).getExplainLog());
					mwrItem.setLigRepItemCode(itemDataMap.get(repItem).getLigRepItemCode());
					repItemMap.put(itemEntry.getRetLirId(), mwrItem);
				}
				ligMembersMap.add(itemEntry);
			}
		}
		if (ligMembersMap.size() > 0) {
			Map<Integer, List<MWRItemDTO>> lirItemCodeMap = ligMembersMap.stream()
					.collect(Collectors.groupingBy(MWRItemDTO::getRetLirId));
			setImpactCalFieldForLIG(ligRowMap, lirItemCodeMap, retLirMap, repItemMap);
			setIsRecommendedForLIG(ligRowMap, lirItemCodeMap, retLirMap);
			//This function will aggregate the new sales,units and margin at lig row
			setQtrMovementdataForLIG(ligRowMap,lirItemCodeMap,quarterlyOverriddenItems);
		}
		return ligRowMap;
	}


	private void setIsRecommendedForLIG(HashMap<Integer, LigFlagsDTO> ligRowMap, Map<Integer, List<MWRItemDTO>> lirItemCodeMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		for (Map.Entry<Integer, List<MWRItemDTO>> member : lirItemCodeMap.entrySet()) {

			Map<Integer, List<MWRItemDTO>> membersMap = member.getValue().stream()
					.collect(Collectors.groupingBy(MWRItemDTO::getItemCode));
			
			if (retLirMap.containsKey(member.getKey())) {
				List<PRItemDTO> ligMembers = retLirMap.get(member.getKey());
				int flag = 0;
				for (PRItemDTO prItemDTO : ligMembers) {
					if (membersMap.containsKey(prItemDTO.getItemCode())) {
						boolean status = membersMap.get(prItemDTO.getItemCode()).get(0).isNewPriceRecommended();
						if (status) {
							flag = 1;
							break;
						}
					} else {
						int status = prItemDTO.getIsNewPriceRecommended();
						if (status == 1) {
							flag = 1;
							break;
						}
					}
				}
				
				LigFlagsDTO LigFlagsDTO = new LigFlagsDTO(); 
				if (flag == 1) {
					if(ligRowMap.containsKey(member.getKey()))
					LigFlagsDTO=ligRowMap.get(member.getKey());
					LigFlagsDTO.setIsnewPriceRecommended(1);
					
				} else {
					if(ligRowMap.containsKey(member.getKey()))
					LigFlagsDTO=ligRowMap.get(member.getKey());
					LigFlagsDTO.setIsnewPriceRecommended(0);
				}
				ligRowMap.put(member.getKey(), LigFlagsDTO);
			}

		}
	}
	
	
	private HashMap<ItemKey, PRItemDTO> getOverriddenItemMap(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {

		HashMap<ItemKey, PRItemDTO> overriddenItemMap = new HashMap<ItemKey, PRItemDTO>();
		itemDataMap.forEach((itemKey, value) -> {
			// only add lig members/items which have override and not entire lig items
			if (value != null && value.getUserOverrideFlag() == 1 && value.getOverriddenRegularPrice() != null
					&& value.getOverriddenRegularPrice().price > 0) {
				overriddenItemMap.put(itemKey, value);
				overriddenItemMap.get(itemKey).setOverrideRemoved(0);

			}
			// If user override is removed
			else if (value.getUserOverrideFlag() == 1
					&& (value.getOverriddenRegularPrice() != null && value.getOverriddenRegularPrice().price == 0)) {
				int overrideRemoved = 1;
				overriddenItemMap.put(PRCommonUtil.getItemKey(value), value);
				overriddenItemMap.get(PRCommonUtil.getItemKey(value)).setOverrideRemoved(overrideRemoved);
			}
		});
		return overriddenItemMap;

	}

	
	private void setImpactCalFieldForLIG(HashMap<Integer, LigFlagsDTO> ligRowMap, Map<Integer, List<MWRItemDTO>> lirItemCodeMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<Integer, MWRItemDTO> repItemMap) {
		
		for (Map.Entry<Integer, List<MWRItemDTO>> member : lirItemCodeMap.entrySet()) {

			Map<Integer, List<MWRItemDTO>> membersMap = member.getValue().stream()
					.collect(Collectors.groupingBy(MWRItemDTO::getItemCode));

			if (retLirMap.containsKey(member.getKey())) {
				List<PRItemDTO> ligMembers = retLirMap.get(member.getKey());
				
				int flag = 0;
				for (PRItemDTO prItemDTO : ligMembers) {
						if (membersMap.containsKey(prItemDTO.getItemCode())) {
						char status = membersMap.get(prItemDTO.getItemCode()).get(0).getIsImpactIncludedInSummaryCalculation();
						if (status=='Y') {
							flag = 1;
							break;
						}
					} else {
						char status = prItemDTO.getIsImpactIncludedInSummaryCalculation();

						if (status=='Y') {
							flag = 1;
							break;
						}
					}
				}
				
				LigFlagsDTO LigFlagsDTO = new LigFlagsDTO(); 
				if (flag == 1) {
					if(ligRowMap.containsKey(member.getKey()))
					LigFlagsDTO=ligRowMap.get(member.getKey());
					LigFlagsDTO.setIsImpactIncluded("Y");
				} else {
					if(ligRowMap.containsKey(member.getKey()))
					LigFlagsDTO=ligRowMap.get(member.getKey());
					LigFlagsDTO.setIsImpactIncluded("N");
				}
				PRExplainLog explainLog =null;
				if (repItemMap.get(member.getKey()) != null) {
					MWRItemDTO mwrItemDTO = repItemMap.get(member.getKey());
					if (membersMap.containsKey(mwrItemDTO.getLigRepItemCode()))
					{
						explainLog = membersMap.get(mwrItemDTO.getLigRepItemCode()).get(0).getExplainLog();
						LigFlagsDTO.setExplainLog(explainLog);
						LigFlagsDTO.setUserOverrideFlag(membersMap.get(mwrItemDTO.getLigRepItemCode()).get(0).isUserOverrideFlag());
						LigFlagsDTO.setOverrideRemoved(membersMap.get(mwrItemDTO.getLigRepItemCode()).get(0).getOverrideRemoved());
						LigFlagsDTO.setOverriddenRegularPrice(membersMap.get(mwrItemDTO.getLigRepItemCode()).get(0).getOverrideRegPrice());
						LigFlagsDTO.setIspendingRetailRecommended(membersMap.get(mwrItemDTO.getLigRepItemCode()).get(0).getIsPendingRetailRecommended());
					}
					else {
						explainLog = mwrItemDTO.getExplainLog();
						LigFlagsDTO.setExplainLog(explainLog);
						LigFlagsDTO.setOverriddenRegularPrice(new MultiplePrice(0,0.0));
					}
				}
				
				ligRowMap.put(member.getKey(), LigFlagsDTO);
			}

		}
	}
	
	/**
	 * Function will aggreagte the new sales,units and margin for LIG row at week level
	 * @param ligRowMap
	 * @param lirItemCodeMap
	 * @param itemMapWeekly
	 */
		private void setWeeklyMovementForLIG(HashMap<Integer, LigFlagsDTO> ligRowMap,
				Map<Integer, List<MWRItemDTO>> lirItemCodeMap, HashMap<ItemKey, MWRItemDTO> itemMapWeekly) {

			for (Map.Entry<Integer, List<MWRItemDTO>> member : lirItemCodeMap.entrySet()) {
				ItemKey itemKey = PRCommonUtil.getItemKey(member.getKey(), true);
				double overridenSales = 0.0, overridenMargin = 0.0, overridenPredMov = 0.0;
				if (itemMapWeekly.containsKey(itemKey)) {
					MWRItemDTO ligMember = itemMapWeekly.get(itemKey);
					overridenPredMov = overridenPredMov
							+ checkNullAndReturnZero(ligMember.getFinalPricePredictedMovement());
					overridenSales = overridenSales + checkNullAndReturnZero(ligMember.getFinalPriceRevenue());
					overridenMargin = overridenSales + checkNullAndReturnZero(ligMember.getFinalPriceMargin());

				}
				LigFlagsDTO LigFlagsDTO = new LigFlagsDTO();
				if (ligRowMap.containsKey(member.getKey()))
					LigFlagsDTO = ligRowMap.get(member.getKey());
				LigFlagsDTO.setFinalPriceMargin(overridenMargin);
				LigFlagsDTO.setFinalpredictedMovement(overridenPredMov);
				LigFlagsDTO.setFinalPriceRevenue(overridenSales);
				ligRowMap.put(member.getKey(), LigFlagsDTO);

			}
		}
		
		/**
		 * Function will aggreagte the new sales,units and margin for LIG row at Qtr level
		 * @param ligRowMap
		 * @param lirItemCodeMap
		 * @param quarterlyOverriddenItems
		 */
	private void setQtrMovementdataForLIG(HashMap<Integer, LigFlagsDTO> ligRowMap,
			Map<Integer, List<MWRItemDTO>> lirItemCodeMap, List<MWRItemDTO> quarterlyOverriddenItems) {

		Map<Integer, List<MWRItemDTO>> membersMap = quarterlyOverriddenItems.stream()
				.collect(Collectors.groupingBy(MWRItemDTO::getRetLirId));
		for (Map.Entry<Integer, List<MWRItemDTO>> member : lirItemCodeMap.entrySet()) {
			double overridenSales = 0.0, overridenMargin = 0.0, overridenPredMov = 0.0;
			if (membersMap.containsKey(member.getKey())) {
				List<MWRItemDTO> ligMembers = membersMap.get(member.getKey());
				for (MWRItemDTO ligMember : ligMembers) {
					if (!ligMember.isLir()) {
						overridenPredMov = overridenPredMov + checkNullAndReturnZero(ligMember.getRegUnits());
						logger.info("lig "+ligMember.getRetLirId() + "item" +   ligMember.getItemCode() + "overridenPredMov  after " + overridenPredMov +"units "+ ligMember.getRegUnits());
						overridenSales = overridenSales + checkNullAndReturnZero(ligMember.getRegRevenue());
						overridenMargin = overridenMargin + checkNullAndReturnZero(ligMember.getRegMargin());
					}
				}
				LigFlagsDTO LigFlagsDTO = new LigFlagsDTO();
				if (ligRowMap.containsKey(member.getKey()))
					LigFlagsDTO = ligRowMap.get(member.getKey());
				LigFlagsDTO.setRegMargin(overridenMargin);
				logger.info("lig "+ member.getKey()+ "overridenPredMov  after " + overridenPredMov );
				LigFlagsDTO.setRegUnits(overridenPredMov);
				LigFlagsDTO.setRegRevenue(overridenSales);
			}
		}

	}
		private double checkNullAndReturnZero(Double value) {
			if (value == null) {
				return 0;
			} else {
				return value;
			}
		}

}
