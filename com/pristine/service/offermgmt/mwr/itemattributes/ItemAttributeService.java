package com.pristine.service.offermgmt.mwr.itemattributes;

import java.sql.Connection;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.PricingEngineWS;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.PRConstraintLIG;
import com.pristine.dto.offermgmt.PRConstraintLocPrice;
import com.pristine.dto.offermgmt.PRConstraintsDTO;
import com.pristine.dto.offermgmt.PRGuidelineBrand;
import com.pristine.dto.offermgmt.PRGuidelinesDTO;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PRZoneStoreReccommendationFlag;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.RetailCostServiceOptimized;
import com.pristine.service.offermgmt.CheckListService;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemRecErrorService;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.PriceGroupAdjustmentService;
import com.pristine.service.offermgmt.PriceGroupService;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.StrategyService;
import com.pristine.service.offermgmt.mwr.basedata.BaseData;
import com.pristine.service.offermgmt.mwr.basedata.CompDataService;
import com.pristine.service.offermgmt.mwr.basedata.GenaralService;
import com.pristine.service.offermgmt.mwr.basedata.MovementDataService;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;
import com.pristine.service.offermgmt.data.AdDataSetupCommonAPI;
import com.pristine.service.offermgmt.data.CostDataSetupCommonAPI;
import com.pristine.service.offermgmt.data.DisplayDataSetupCommonAPI;
import com.pristine.service.offermgmt.data.PriceDataSetupCommonAPI;
import com.pristine.service.offermgmt.data.PromotionDataSetupCommonAPI;

public class ItemAttributeService {
	private static Logger logger = Logger.getLogger("ItemAttributeService");

	/** Added for AZ mexico and FF***/

	private boolean useCommonAPIForPrice = Boolean
			.parseBoolean(PropertyManager.getProperty("USE_COMMON_API_FOR_PRICE", "FALSE"));
	
	private boolean useCommonAPIForCost = Boolean
			.parseBoolean(PropertyManager.getProperty("USE_COMMON_API_FOR_COST", "FALSE"));
	
	private boolean useCommonAPIForPrmotion = Boolean
			.parseBoolean(PropertyManager.getProperty("USE_COMMON_API_FOR_PROMOTION", "FALSE"));
	
	boolean markItemsNotTosendToPred = Boolean
			.parseBoolean(PropertyManager.getProperty("MARK_NON_MOVING_ITEMS_NOT_TO_SEND_TO_PREDICTION", "FALSE"));
	
	//config for AZ to check for pending retails
	boolean checkForPendingRetail=Boolean.parseBoolean(PropertyManager.getProperty("CHECK_PENDING_RETAILS", "FALSE"));

	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param conn 
	 * @throws OfferManagementException
	 * @throws GeneralException
	 * @throws Exception
	 */
	public void setupItemAttributes(BaseData baseData, CommonDataHelper commonDataHelper,
			RecommendationInputDTO recommendationInputDTO, Connection conn)
			throws OfferManagementException, GeneralException, Exception {
		
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		PricingEngineWS pricingEngineWS  = new PricingEngineWS();
		GenaralService genaralService = new GenaralService();
		
		// 1. Get lir id map
		HashMap<Integer, List<PRItemDTO>> retLirMap = getRetLirIdMap(baseData.getItemDataMap());
		boolean calculateGlobalZoneImpact= Boolean
				.parseBoolean(PropertyManager.getProperty("CALC_AGG_IMPACT_FOR_GLOBAL_ZONE","FALSE"));
		
		// Changes done by Karishma on 01/10/2022
		// set the max MAP retail for the LIG memebrs
		/**PRES-213**/
		if (PropertyManager.getProperty("SET_MAP_RETAIL", "FALSE").equalsIgnoreCase("TRUE")) {
			setMaxMapRetailForLIG(baseData.getItemDataMap(), retLirMap);
		}
		
		/** PROM-2223 changes Started **/
		// Changes done by Karishma on 09/29/2021 for AZ
		// If recc for gloabl zone 1000 ,get the aggregated impact for the latest
		// recommendations for each item for all zones except Zone 5

		if (recommendationInputDTO.isGlobalZone() && calculateGlobalZoneImpact) {
			//Added on 05/20 to solve issue of L13 units getting aggregated again after update reccs 
			//passing last parameter for the service to identify that its not called from update recommendations
			genaralService.getLatestRegRetailslOfAllZones(baseData.getItemDataMap(), recommendationInputDTO, retLirMap,
					conn,false);
		}

		/** PROM-2223 changes End **/
		
		// 2. Set price check lists
		setPriceCheckLists(recommendationInputDTO, baseData.getItemDataMap(), retLirMap,
				baseData.getPriceCheckListInfo(), baseData.getStrategyMap(), baseData.getProductListMap(),
				baseData.getProductParentChildRelationMap(), baseData.getLocationList());
		
		/**PRES-213**/
		//Added for setting the min max retail for LIg member if its not present 
		if (PropertyManager.getProperty("SET_MAP_RETAIL", "FALSE").equalsIgnoreCase("TRUE")) {
			setMinMaxForLIG(baseData.getItemDataMap(), retLirMap);
		}

		// 4. Set price group details
		setPriceGroupDetails(baseData.getItemDataMap(), baseData.getPriceGroupDetails());

		if (!useCommonAPIForPrice) {
			// 5. Set Price data
			setPriceData(recommendationInputDTO, baseData.getItemDataMap(), baseData.getPriceDataMap());
		} else {
			setupDatafromAPI(conn, recommendationInputDTO, baseData.getItemDataMap(),baseData);
		}

		if (!useCommonAPIForCost) {
			// 6. Set cost data
			setCostData(recommendationInputDTO, baseData.getItemDataMap(), baseData.getCostDataMap(),
					commonDataHelper.getAllWeekCalendarDetails(),baseData.getFutureCostDataMap());
		}
		// 7. Set comp price data
		setCompPriceData(recommendationInputDTO, baseData.getItemDataMap(), baseData.getLatestCompDataMap(),
				baseData.getPreviousCompDataMap(), baseData.getCompSettings(), pricingEngineDAO);

		// 8. Set movement data
		/** PROM-2223 changes  **/
		//Added condition for checking global zone for AZ and not set L13  mov here as its set 
		//in BaseDataservice by taking the aggregated mov of Z4 and Z16
		setMovementData(baseData.getMovementDataMap(), baseData.getItemDataMap(),
				commonDataHelper.getPreviousCalendarDetails(), retLirMap,commonDataHelper.getPreviousCalID(),recommendationInputDTO.isGlobalZone());
		/** PROM-2223 changes  end  **/
		
		// 9. Set last X weeks movement data
		setLastXWeeksMovement(baseData.getItemDataMap(), retLirMap, baseData.getLastXWeeksMovement(), pricingEngineDAO);

		new ItemCriteriaService().setCriteriaIdForItems(baseData.getItemDataMap(),
				baseData.getCriteriaDetailsFromStrategy(), baseData.getStrategyMap());

		if (PropertyManager.getProperty("USE_WEIGHTED_AVG_COST", "FALSE").equalsIgnoreCase("TRUE")) {
			calculateAvgWeightedCost(baseData.getItemDataMap(), retLirMap);
		}
		// 10. Set strategies
		setStrategies(recommendationInputDTO, baseData.getItemDataMap(), retLirMap, baseData.getStrategyMap(),
				baseData.getProductParentChildRelationMap(), baseData.getProductListMap(), baseData.getLocationList());

		//set the LIG constraint for LIG row
		setLIGConstraintInStrategy(retLirMap, baseData.getItemDataMap());

		// Added by Karishma
		// resetMAP to original value for LIG members, if the lIG constraint is not
		// selected from stratgey
		// This function is added because Max Map is used for all memebers which forces
		// to recommend  a higher value/same value for all memebrs which is not required if no lig
		// constrain is selected

		if (PropertyManager.getProperty("RESET_MAP", "FALSE").equalsIgnoreCase("TRUE")) {
			resetMapForNoLIGConstraint(baseData.getItemDataMap(), retLirMap);
		}

		// Added for Az to set the leads for pricegropus based on Strategy
		boolean resetPriceGroups = Boolean
				.parseBoolean(PropertyManager.getProperty("RESET_PRICEGROUP_RELATIONS", "FALSE"));
		if (resetPriceGroups) {
			resetPriceGroups(baseData.getItemDataMap());
		}

		if (!useCommonAPIForPrmotion) {
			// 11. set ad, promo, display information
			setPromoDetails(recommendationInputDTO, baseData.getItemDataMap(), baseData.getSaleDetails(),
					baseData.getAdDetails(), baseData.getDisplayDetails());
		}

		else {
			setupPromoDatafromAPI(recommendationInputDTO, baseData.getItemDataMap(), baseData);
		}
		
		// 12. Set Error codes for items
		setErrorCodes(baseData.getItemDataMap());

		// Added for ignoring non-moving Items
		// Added on 12/28/2018 by Pradeep 
		// Added based on Giant Eagle's feed back
		
		// Mark non moving items and LIG
		pricingEngineWS.markNonMovingItems(baseData.getItemDataMap(), retLirMap);
		
		// Added code to additionally check if item does not have 52 weeks movement and
		// mark it as not to send to prediction for AZ US
		if(markItemsNotTosendToPred)
		markNonMovingItemsNotTosendToPrediction(baseData.getItemDataMap());

		PriceGroupService priceGroupService = new PriceGroupService();
		priceGroupService.updatePriceRelationFromStrategy(baseData.getItemDataMap());
		priceGroupService.setDefaultBrandPrecedence(baseData.getItemDataMap(),retLirMap);
		
		// Added for adjusting price groups based on non moving items/inactive items/unauthorized items
		// Added on 06/24/2019 by Pradeep
		// This code is to handle missing relationships issue due to non-moving items
		// This will also take care of AutoZone's missing tier requirement
		new PriceGroupAdjustmentService().adjustPriceGroupsByDiscontinuedItems(baseData.getItemDataMap(), retLirMap);

		
		// Set relations again after adjustement of missing items
		priceGroupService.updatePriceRelationFromStrategy(baseData.getItemDataMap());
		priceGroupService.setDefaultBrandPrecedence(baseData.getItemDataMap(),retLirMap);
		
		HashMap<ItemKey, PRItemDTO> itemDataMap = pricingEngineWS
				.filterByActiveAuthAndMovingItems(baseData.getItemDataMap(), retLirMap);

		// Changes done by Bhargavi on 01/05/2021
		// update the MarkUp and MarkDown values for Rite Aid
		setInventoryDetails(baseData.getItemDataMap(), baseData.getStoreInventoryMap());
		
		//set the pending retails if present for items
		if(checkForPendingRetail)
		genaralService.getItemsWithPendingRetailsFromQueue(conn,recommendationInputDTO,itemDataMap);
		
		baseData.setItemDataMap(itemDataMap);
	}
	
/**
 * 
 * @param itemDataMap
 */
	public void markNonMovingItemsNotTosendToPrediction(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		int counter=0;
		for(Map.Entry<ItemKey, PRItemDTO> itemEntry: itemDataMap.entrySet()) {
			PRItemDTO prItemDTO = itemEntry.getValue();
			if(!prItemDTO.isLir()) {
				if(prItemDTO.getXweekMov() == 0) {
					counter++;
					prItemDTO.setSendToPrediction(false);
				}
			}
		}
		logger.info("markNonMovingItemsNotTosendToPrediction -# of non moving items: " + counter);
	}

	/**
	 * 
	 * @param itemDataMap
	 * @return lir id map
	 * @throws OfferManagementException
	 */
	public HashMap<Integer, List<PRItemDTO>> getRetLirIdMap(HashMap<ItemKey, PRItemDTO> itemDataMap)
			throws OfferManagementException {
		ItemService itemService = new ItemService(null);
		HashMap<Integer, List<PRItemDTO>> retLirMap = itemService.populateRetLirDetailsInMap(itemDataMap);

		return retLirMap;
	}

	/**
	 * Sets strategy for each item
	 * 
	 * @param itemDataMap
	 * @param strategyMap
	 * @throws OfferManagementException
	 */
	public void setStrategies(RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap,
			HashMap<Integer, Integer> productParentChildRelationMap, HashMap<String, ArrayList<Integer>> productListMap,
			ArrayList<Integer> locationList) throws OfferManagementException {

		StrategyService strategyService = new StrategyService(null);

		PRStrategyDTO dependentZoneInputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);
		PRStrategyDTO leadInputDTO = CommonDataHelper.convertRecInputToStrategyInputForLeadZone(recommendationInputDTO);

		boolean isZoneItem = true;

		PRZoneStoreReccommendationFlag zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();

		strategyService.getStrategies(dependentZoneInputDTO, strategyMap, itemDataMap, productParentChildRelationMap,
				retLirMap, productListMap, String.valueOf(recommendationInputDTO.getChainId()),
				recommendationInputDTO.getDivisionId(), isZoneItem, zoneStoreRecFlag, leadInputDTO,
				recommendationInputDTO.getLeadZoneDivisionId(), recommendationInputDTO.getLeadZoneId(), locationList,
				new ArrayList<>(), recommendationInputDTO.getLocationId());
	}

	/**
	 * Sets price check lists
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @param priceCheckListInfo
	 * @param strategyMap
	 * @param retLirMap
	 * @param productListMap
	 * @param productParentChildRelationMap
	 * @throws GeneralException
	 * @throws OfferManagementException
	 */
	public void setPriceCheckLists(RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap, HashMap<String, ArrayList<Integer>> productListMap,
			HashMap<Integer, Integer> productParentChildRelationMap, ArrayList<Integer> locationList)
					throws GeneralException, OfferManagementException {

		CheckListService checkListService = new CheckListService();

		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);
		PRStrategyDTO leadInputDTO = CommonDataHelper.convertRecInputToStrategyInputForLeadZone(recommendationInputDTO);

		PRZoneStoreReccommendationFlag zoneStoreRecFlag = new PRZoneStoreReccommendationFlag();

		checkListService.populatePriceCheckListDetails(null, recommendationInputDTO.getChainId(),
				recommendationInputDTO.getDivisionId(), recommendationInputDTO.getLocationId(),
				recommendationInputDTO.getLocationLevelId(), recommendationInputDTO.getLocationId(),
				recommendationInputDTO.getProductLevelId(), recommendationInputDTO.getProductId(), itemDataMap,
				priceCheckListInfo, recommendationInputDTO.getLeadZoneId(), strategyMap, retLirMap, productListMap,
				zoneStoreRecFlag, productParentChildRelationMap, new PricingEngineDAO(), new StrategyService(null),
				leadInputDTO, inputDTO, recommendationInputDTO.getLeadZoneDivisionId(), locationList, new ArrayList<>(),
				recommendationInputDTO.getLocationId());
	}

	/**
	 * Sets price group relation
	 * 
	 * @param itemDataMap
	 * @param priceGroupDetails
	 */
	public void setPriceGroupDetails(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupDetails) {

		new PricingEngineDAO().populatePriceGroupDetails(itemDataMap, priceGroupDetails);

	}

	/**
	 * Sets promotion information
	 * 
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @param saleDetails
	 * @param adDetails
	 * @param displayDetails
	 * @throws CloneNotSupportedException
	 * @throws ParseException
	 * @throws GeneralException
	 */
	public void setPromoDetails(RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, HashMap<Integer, List<PRItemAdInfoDTO>> adDetails,
			HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails)
			throws CloneNotSupportedException, ParseException, GeneralException {

		PricingEngineService pricingEngineService = new PricingEngineService();

		int noOfsaleAdDisplayWeeks = MultiWeekRecConfigSettings.getMwrNoOfSaleAdDisplayWeeks();
		// Dinesh:: 21-FEB-18, Code changes to handle future promotions and on-going
		// promotions
		pricingEngineService.fillPromoDetails(itemDataMap, adDetails, displayDetails, saleDetails,
				recommendationInputDTO.getBaseWeek(), recommendationInputDTO.getStartWeek(), noOfsaleAdDisplayWeeks);
	}

	/**
	 * Sets current and prev price information in item data
	 * 
	 * @param itemDataMap
	 * @param priceDataMap
	 */
	public void setPriceData(RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap,
			LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> priceDataMap) {

		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);

		new PricingEngineDAO().setCurrentPriceForItems(priceDataMap, itemDataMap, recommendationInputDTO.getChainId(),
				inputDTO);

	}

	/**
	 * Sets cost data in item data
	 * 
	 * @param itemDataMap
	 * @param futureCostMap 
	 * @param priceDataMap
	 * @throws GeneralException
	 */
	public void setCostData(RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, HashMap<String, List<RetailCostDTO>>> costDataMap,
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, HashMap<Integer, HashMap<String, List<RetailCostDTO>>> futureCostMap) throws GeneralException {

		RetailCostServiceOptimized retailCostServiceOptimized = new RetailCostServiceOptimized(null);

		double costChangeThreshold = MultiWeekRecConfigSettings.getMwrCostChangeThreshold();
		int costHistory = MultiWeekRecConfigSettings.getMwrCostHistory();
		//Config added to decide noOfWeeks for which Future cost has to be fetched
				int weeksTofetchFutureCost = MultiWeekRecConfigSettings.getFutureCostWeeksToFetch();

		Set<Integer> nonCachedItemCodeSet = new HashSet<Integer>();
		for (PRItemDTO item : itemDataMap.values()) {
			if (!item.isLir()) {
				nonCachedItemCodeSet.add(item.getItemCode());
			}
		}

		RetailCalendarDTO calDTO = new RetailCalendarDTO();
		calDTO.setStartDate(recommendationInputDTO.getBaseWeek());

		int locationId = 0;

		if (recommendationInputDTO.isPriceTestZone())
			locationId = recommendationInputDTO.getTempLocationID();
		else
			locationId = recommendationInputDTO.getLocationId();

		// Find latest cost of the item for zone and store
		retailCostServiceOptimized.getLatestCostOfZoneItems(costDataMap, nonCachedItemCodeSet, itemDataMap,
				recommendationInputDTO.getChainId(),locationId, calDTO, costHistory,
				allWeekCalendarDetails);

		// Find future cost of the item for zone and store
				if(futureCostMap.size()>0)
				retailCostServiceOptimized.getFutureCostOfZoneItems(futureCostMap, nonCachedItemCodeSet, itemDataMap,
						recommendationInputDTO.getChainId(),locationId, calDTO, weeksTofetchFutureCost,
						allWeekCalendarDetails);

		// Find if there is cost change for zone
		retailCostServiceOptimized.getPreviousCostOfZoneItems(costDataMap, nonCachedItemCodeSet, itemDataMap,
				recommendationInputDTO.getChainId(), locationId, calDTO, costHistory,
				allWeekCalendarDetails, costChangeThreshold);

	}

	/**
	 * Sets movement data
	 * 
	 * @param movementData
	 * @param itemDataMap
	 * @param previousCalendarDetails
	 * @param isGlobalZone 
	 * @param field 
	 * @throws GeneralException
	 */
	public void setMovementData(HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData,
			HashMap<ItemKey, PRItemDTO> itemDataMap, LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails,
			HashMap<Integer, List<PRItemDTO>> retLirMap, int prevCalID, boolean isGlobalZone) throws GeneralException {

		MovementDataService movementDataService = new MovementDataService();

		int recentXWeeks = Integer.parseInt(PropertyManager.getProperty("PRED_NO_RECENT_MOV_WEEKS", "13"));
		//This property is added for AZ on 11/22/2021 to indicate to use the aggregated impact of Zone4 and Zone16)
		boolean calAggImpact= Boolean
				.parseBoolean(PropertyManager.getProperty("CALC_AGG_IMPACT_FOR_GLOBAL_ZONE", "FALSE"));
	
			
			// Added by Karishma
			// Added a field to decide for what movement data has to be fetched
			movementDataService.populateXWeeksMovementData(itemDataMap, retLirMap, movementData,
					previousCalendarDetails, recentXWeeks, PRConstants.RECENT_X_WEEKS, prevCalID);

			// Separted code to mark items with no rec week movement from
			// populateXWeeksMovementData
			movementDataService.markItemswithNorecWeeksMov(itemDataMap, retLirMap);

			// Added by Karishma
			// Added a field to decide for what movement data has to be fetched

			int xWeeksForTotalImpact = Integer
					.parseInt(PropertyManager.getProperty("NO_OF_WEEKS_MOV_FOR_TOTAL_IMPACT", "52"));
			movementDataService.populateXWeeksMovementData(itemDataMap, retLirMap, movementData,
					previousCalendarDetails, xWeeksForTotalImpact, PRConstants.X_WEEKS_FOR_IMPACT, prevCalID);
			
			// Added by Karishma
			// Added to set field for calculating 52 weeks movement in additional criteria
			int xWeeksForaddcriteria = Integer
					.parseInt(PropertyManager.getProperty("NO_OF_WEEKS_MOV_FOR_ADDITIONAL_CRITERIA", "52"));
			movementDataService.populateXWeeksMovementData(itemDataMap, retLirMap, movementData,
					previousCalendarDetails, xWeeksForaddcriteria, PRConstants.X_WEEKS_FOR_ADD_CRITERIA, prevCalID);

			// Added to aggregate 52 movement for items belonging to same family
			movementDataService.setMovementForFamilyItems(itemDataMap);

			// Added to get the 52 weeks revenue for items further to be used to exclude in
			// Prediction
			int xWeeksForpredExc = Integer
					.parseInt(PropertyManager.getProperty("NO_OF_WEEKS_MOV_FOR_EXLCUDE_PRED", "52"));
			movementDataService.populateXWeeksMovementData(itemDataMap, retLirMap, movementData,
					previousCalendarDetails, xWeeksForpredExc, PRConstants.X_WEEKS_FOR_PRED_EXCLUDE, prevCalID);

			// Calculate per store revenue for each item.This is just for logging purpose
			movementDataService.calculateTotalRev(itemDataMap);

			// Added to calculate weighted averageCost for LIG members AZ
			if (PropertyManager.getProperty("USE_WEIGHTED_AVG_COST", "FALSE").equalsIgnoreCase("TRUE")) {
				int xWeeksforWAC = Integer.parseInt(PropertyManager.getProperty("X_WEEKS_FOR_WAC", "52"));
				movementDataService.populateXWeeksMovementData(itemDataMap, retLirMap, movementData,
						previousCalendarDetails, xWeeksforWAC, PRConstants.X_WEEKS_FOR_WAC, prevCalID);
			}
			
		/** PROM-2223 changes **/
		// This condition is set for AZ , L13 units for normal zones will be calculated
		// but not for Z1000 as its already set in BaseDataService using aggregated L13 units of Z$ and Z16.
		// Since this property will be FALSE by default for other customres, it wil work
		// as usual for them

		if (!calAggImpact) {
			int xWeeksMovement = Integer.parseInt(PropertyManager.getProperty("X_WEEKS_MOV", "52"));

			movementDataService.populateXWeeksMovementData(itemDataMap, retLirMap, movementData,
					previousCalendarDetails, xWeeksMovement, PRConstants.X_WEEKS_MOV, prevCalID);

			//Added by Karishma on 02/14/2022 for setting the variable with movement for X weeks
			// to be used for selecting the LIG rep Item for max mover constraint
			int xWeeksForLIGRepItem = Integer.parseInt(PropertyManager.getProperty("X_WEEKS_MOV_LIG_REP_ITEM", "52"));
			movementDataService.populateXWeeksMovementData(itemDataMap, retLirMap, movementData,
					previousCalendarDetails, xWeeksForLIGRepItem, PRConstants.X_WEEKS_MOV_LIG_REP_ITEM, prevCalID);
		} else if (!isGlobalZone) {
			int xWeeksMovement = Integer.parseInt(PropertyManager.getProperty("X_WEEKS_MOV", "52"));

			movementDataService.populateXWeeksMovementData(itemDataMap, retLirMap, movementData,
					previousCalendarDetails, xWeeksMovement, PRConstants.X_WEEKS_MOV, prevCalID);

			// Added by Karishma on 02/14/2022 for setting the variable with movement for X weeks
			// to be used for selecting the LIG rep Item for max mover constraint
			int xWeeksForLIGRepItem = Integer.parseInt(PropertyManager.getProperty("X_WEEKS_MOV_LIG_REP_ITEM", "52"));
			movementDataService.populateXWeeksMovementData(itemDataMap, retLirMap, movementData,
					previousCalendarDetails, xWeeksForLIGRepItem, PRConstants.X_WEEKS_MOV_LIG_REP_ITEM, prevCalID);
		}
		/** PROM-2223 changes end **/
	}

	/**
	 * 
	 * @param itemDataMap
	 * @param retLirMap
	 * @param lastXWeeksMovement
	 */
	public void setLastXWeeksMovement(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, HashMap<ProductKey, Long> lastXWeeksMovement,
			PricingEngineDAO pricingEngineDAO) {

		pricingEngineDAO.setLastXWeeksMov(itemDataMap, retLirMap, lastXWeeksMovement);

	}

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @param latestCompPriceData
	 * @param previousCompPriceData
	 * @throws GeneralException
	 * @throws Exception
	 */
	public void setCompPriceData(RecommendationInputDTO recommendationInputDTO, HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> latestCompPriceData,
			HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> previousCompPriceData,
			HashMap<Integer, Integer> compSettings,
			PricingEngineDAO pricingEngineDAO)
					throws GeneralException, Exception {

		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);

		RetailCalendarDTO calDTO = new RetailCalendarDTO();
		calDTO.setStartDate(recommendationInputDTO.getBaseWeek());

		int compHistory = MultiWeekRecConfigSettings.getMwrCompHistory();

		for (Map.Entry<LocationKey, HashMap<Integer, CompetitiveDataDTO>> latestCompPriceEntry : latestCompPriceData
				.entrySet()) {

			HashMap<Integer, CompetitiveDataDTO> latestCompPriceMap = latestCompPriceEntry.getValue();

			HashMap<Integer, CompetitiveDataDTO> previousCompPriceMap = new HashMap<>();

			if (previousCompPriceData.containsKey(latestCompPriceEntry.getKey())) {
				previousCompPriceMap = previousCompPriceData.get(latestCompPriceEntry.getKey());
			}

			PRStrategyDTO tempDTO = new PRStrategyDTO();
			tempDTO.copy(inputDTO);
			tempDTO.setLocationLevelId(latestCompPriceEntry.getKey().getLocationLevelId());
			tempDTO.setLocationId(latestCompPriceEntry.getKey().getLocationId());
			tempDTO.setChainId(latestCompPriceEntry.getKey().getChainId());
			pricingEngineDAO.getCompPriceData(tempDTO, calDTO, compHistory, itemDataMap, latestCompPriceMap,
					previousCompPriceMap);
		}
		
		new CompDataService().setupMultiCompRetails(itemDataMap, compSettings);
	}
	
	/**
	 * 
	 * @param itemDataMap
	 */
	private void setErrorCodes(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		HashMap<LocationKey, HashMap<ItemKey, PRRecommendation>> leadZoneDetails = new HashMap<>();
		ItemRecErrorService itemRecErrService = new ItemRecErrorService();
		itemRecErrService.setErrorCodeForZoneItems(itemDataMap, leadZoneDetails);
	}

	/**
	 * 
	 * @param itemDataMap
	 * @param retLirMap   Calculates WAC for LIG's
	 */

	double totalCost = 0;

	private void calculateAvgWeightedCost(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {

		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getRetLirId());
				totalCost = 0;
				for (PRItemDTO ligMember : ligMembers) {

					if (ligMember.getListCost() != null && ligMember.getListCost() > 0) {
						ligMember.setOriginalListCost(ligMember.getListCost());
						// if movement is 0 or -ve , then set is as 1
						if (ligMember.getxWeeksMovForWAC() <= 0) {
							ligMember.setxWeeksMovForWAC(1);
						}
						totalCost += ligMember.getxWeeksMovForWAC() * ligMember.getListCost();
					} else {
						if (ligMember.getxWeeksMovForWAC() <= 0) {
							ligMember.setxWeeksMovForWAC(1);
						}
					}
				}
				double totMovement = ligMembers.stream().mapToDouble(PRItemDTO::getxWeeksMovForWAC).sum();
				double weightedAvgCost = totalCost / totMovement;
				ligMembers
						.forEach((u) -> u.setListCost(PRFormatHelper.roundToTwoDecimalDigitAsDouble(weightedAvgCost)));
			} else if (itemDTO.getRetLirId() == 0 && !itemDTO.isLir()) {
				if (itemDTO.getListCost() != null && itemDTO.getListCost() > 0) {
					itemDTO.setOriginalListCost(itemDTO.getListCost());
				}

			}
		});

	}

	/**
	 * This is added for AZ for handling the nonsequential tier gapping It is going
	 * to set the lead item for each priceGroup based on strategy
	 * @param itemDataMap
	 */
	public void resetPriceGroups(HashMap<ItemKey, PRItemDTO> itemDataMap) {

		HashMap<Integer, List<PRItemDTO>> priceGroupSortedMap = new HashMap<Integer, List<PRItemDTO>>();
		// grouping items by pricegroups
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (!itemDTO.isLir()) {
				PRPriceGroupDTO pgData = itemDTO.getPgData();

				//logger.debug("Itemkey : " + itemKey + "retLirid" + itemDTO.getRetLirId());
				if (pgData != null) {
					List<PRItemDTO> tempList = new ArrayList<PRItemDTO>();
					logger.debug("item:"+ itemDTO.getItemCode()+"BrandTierId: "+ pgData.getBrandTierId());
					itemDTO.setBrandTierId(pgData.getBrandTierId());
					itemDTO.setItemKey(itemKey);
					if (priceGroupSortedMap.containsKey(pgData.getPriceGroupId())) {
						tempList = priceGroupSortedMap.get(pgData.getPriceGroupId());
					}
					tempList.add(itemDTO);
					itemDTO.getPgData()
							.setRelationList(new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>());
					priceGroupSortedMap.put(pgData.getPriceGroupId(), tempList);
				}

			}

		});

		for (Map.Entry<Integer, List<PRItemDTO>> entry : priceGroupSortedMap.entrySet()) {
			logger.debug("resetPriceGroups()- Resetting for PriceGroup: " + entry.getKey() + " PgName:"
					+ entry.getValue().get(0).getPgData().getPriceGroupName());
			
			List<PRItemDTO> ItemList = entry.getValue();

			for (PRItemDTO item : ItemList) {
				if (!item.isLir()) {
					PRPriceGroupDTO pgData = item.getPgData();
				// logger.info("item"+ item.getItemCode() +"item.getBrandTierId():"+
				// item.getBrandTierId());7
				if (pgData != null) {
					PRGuidelinesDTO guideLines = item.getStrategyDTO().getGuidelines();
					ArrayList<PRGuidelineBrand> brandGuideLineMap = guideLines.getBrandGuideline();

					if (brandGuideLineMap != null) {
						for (PRGuidelineBrand pr : brandGuideLineMap) {

							if (pr.getBrandTierId1() != 0) {
								if (item.getBrandTierId() == pr.getBrandTierId1()) {
									item.setLeadTierID(pr.getBrandTierId2());
									item.setValueType(pr.getValueType());
									break;
								}
							}
						}
					}
					if (item.getLeadTierID() != 0) {
						//logger.debug( item.getItemCode()   + "leadtier :"+ item.getLeadTierID() );
					
						int leadFound = setLeadItem(ItemList, item.getLeadTierID(), item, itemDataMap);
						// find the lead of missing tier
						if (leadFound == 0) {
							int leadofLeadTier = 0;
							char valueTypeOfLead = 'N';
							if (brandGuideLineMap != null) {
								for (PRGuidelineBrand pr : brandGuideLineMap) {
									if (pr.getBrandTierId1() != 0) {
										if (pr.getBrandTierId1() == item.getLeadTierID()) {
											leadofLeadTier = pr.getBrandTierId2();
											valueTypeOfLead = pr.getValueType();
											break;
										}
									}
								}
								if (valueTypeOfLead == item.getValueType() && leadofLeadTier != 0)
									setLeadItem(ItemList, leadofLeadTier, item, itemDataMap);
							}

						}

					}

				}
			}
			}
		}

	}

	/**
	 * 
	 * @param itemList
	 * @param leadTierID
	 * @param item
	 * @param itemDataMap
	 * @return
	 */
	private int setLeadItem(List<PRItemDTO> itemList, int leadTierID, PRItemDTO item,
			HashMap<ItemKey, PRItemDTO> itemDataMap) {
		int leaderFound = 0;
		for (PRItemDTO itemdata : itemList) {

			if (itemdata.getBrandTierId() == leadTierID) {

				TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationList = new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>();
				ArrayList<PRPriceGroupRelatedItemDTO> relatedList = new ArrayList<PRPriceGroupRelatedItemDTO>();
				PRPriceGroupRelatedItemDTO relatedItem = new PRPriceGroupRelatedItemDTO();
				relatedItem.setRelatedItemCode(itemdata.getItemCode());
				relatedItem.setRelatedItemBrandTierId(itemdata.getPgData().getBrandTierId());
				relatedItem.setRelatedItemBrandTier(itemdata.getBrandName());
				
				/*
				 * if(String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)){
				 * pgData.setIsLig(true); }
				 */
				/*
				 * if (itemdata.getRetLirId() > 0) { relatedItem.setIsLig(true); } else {
				 * relatedItem.setIsLig(false); }
				 */
				relatedItem.setIsLig(false);
				relatedItem.setRelatedItemBrandId(itemdata.getBrandId());
				relatedItem.setRelatedItemSize(itemdata.getPgData().getItemSize());
				relatedItem.setRelatedUOMName(itemdata.getPgData().getUomName());
				relatedList.add(relatedItem);

				/*
				 * logger.debug("setLeadItem()-Related item for:" + item.getItemKey() + " is: "
				 * + itemdata.getItemCode() + "BrandId of related item: "+
				 * relatedItem.getRelatedItemBrandTierId() + "isLig :" +
				 * relatedItem.getIsLig());
				 */
				relationList.put(PRConstants.BRAND_RELATION, relatedList);
				item.getPgData().setRelationList(relationList);

				if (itemDataMap.containsKey(item.getItemKey())) {
					itemDataMap.put(item.getItemKey(), item);
				}
				leaderFound++;
				break;
			}

		}
		return leaderFound;
	}
	
	//Changes done by Bhargavi on 01/05/2021
	//update the MarkUp and MarkDown values for Rite Aid
	public void setInventoryDetails(HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, Integer> storeInventory)
	{
		storeInventory.forEach((itemCode, inventory) -> {
			ItemKey itemKey = new ItemKey(itemCode, PRConstants.NON_LIG_ITEM_INDICATOR);
			if(itemDataMap.containsKey( itemKey))
			{
				PRItemDTO item = itemDataMap.get(itemKey);
				item.setInventory(inventory);
			}
			
		});
	}
	//Changes-ended

	
	/**
	 * fetch and set up Price,Cost,Promotion,AD,Display,Movement from API
	 * @param conn 
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @throws Exception 
	 * @throws GeneralException 
	 */
	private void setupDatafromAPI(Connection conn, RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap,BaseData baseData) throws Exception, GeneralException {
		logger.info("Fetching Price from api...");
		PriceDataSetupCommonAPI priceSetupApi = new PriceDataSetupCommonAPI();
		priceSetupApi.setupData(conn, recommendationInputDTO, itemDataMap, baseData);

		if (useCommonAPIForCost) {
			logger.info("Fetching cost from Api...");
			CostDataSetupCommonAPI costSetupAPI = new CostDataSetupCommonAPI();
			costSetupAPI.setupData(conn, recommendationInputDTO, itemDataMap, baseData);
		}

		
	}
	
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param itemDataMap
	 * @throws Exception
	 * @throws GeneralException
	 */
	private void setupPromoDatafromAPI(RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap, BaseData baseData) throws Exception, GeneralException {
		
		logger.info("setupPromoDatafromAPI()-getting promotions from api...");
		PromotionDataSetupCommonAPI promoAPI = new PromotionDataSetupCommonAPI();
		promoAPI.setupData(recommendationInputDTO, itemDataMap, baseData);
		
		boolean checkAdandDisplay = Boolean.parseBoolean(PropertyManager.getProperty("SET_AD_AND_DISPLAY", "FALSE"));
		
		if (checkAdandDisplay) {
			logger.info("setupPromoDatafromAPI()-getting AD Data from api...");

			AdDataSetupCommonAPI adAPI = new AdDataSetupCommonAPI();
			adAPI.setupData(recommendationInputDTO, itemDataMap, baseData);

			logger.info("setupPromoDatafromAPI()-getting Display Data from api...");

			DisplayDataSetupCommonAPI displayAPI = new DisplayDataSetupCommonAPI();
			displayAPI.setupData(recommendationInputDTO, itemDataMap, baseData);
		}
		
	}
	
	
	/**
	 * 
	 * @param itemDataMap
	 * @param retLirMap
	 */

	/** PRES-213 changes for AZ USA by Karishma **/
	// Use the max MAP retail in an LIG for all the members

	private void setMaxMapRetailForLIG(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {

		for (Map.Entry<ItemKey, PRItemDTO> itemDTO : itemDataMap.entrySet()) {
			double maxMapRetail = 0;
			if (itemDTO.getValue().isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getValue().getRetLirId());
				for (PRItemDTO ligMember : ligMembers) {
					String mapPrice = ligMember.getUserAttr5();
					if (mapPrice != null && !mapPrice.isEmpty()) {
						if (Double.parseDouble(mapPrice) != 0 && Double.parseDouble(mapPrice) > maxMapRetail) {
							maxMapRetail = Double.parseDouble(mapPrice);
						}
					}
				}

				for (PRItemDTO ligMember : ligMembers) {
					ligMember.setMapRetail(maxMapRetail);
				}
				//set the max map price to LIG 
				itemDTO.getValue().setMapRetail(maxMapRetail);

			} else if (itemDTO.getValue().getRetLirId() == 0 && !itemDTO.getValue().isLir()) {
				String mapPrice = itemDTO.getValue().getUserAttr5();
				if (mapPrice != null && !mapPrice.isEmpty()) {
					if (Double.parseDouble(mapPrice) != 0) {
						itemDTO.getValue().setMapRetail(Double.parseDouble(mapPrice));
					}
				}
			}
		}
		

	}
	
	/**
	 * 
	 * @param itemDataMap
	 * @param retLirMap
	 */
	private void setMinMaxForLIG(HashMap<ItemKey, PRItemDTO> itemDataMap, HashMap<Integer, List<PRItemDTO>> retLirMap) {
	
		for (Map.Entry<ItemKey, PRItemDTO> itemDTO : itemDataMap.entrySet()) {
			double minMaxRetail = 0;
			if (itemDTO.getValue().isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getValue().getRetLirId());
				for (PRItemDTO ligMember : ligMembers) {
					if (ligMember.getMinRetail() > 0 && ligMember.getMinRetail() > minMaxRetail) {
						minMaxRetail = ligMember.getMinRetail();
					}
				}
				for (PRItemDTO ligMember : ligMembers) {
					ligMember.setMinRetail(minMaxRetail);
				}
				// set the max map price to LIG
				itemDTO.getValue().setMapRetail(minMaxRetail);
			}
		}
		
		
	}

	/**
	 * 
	 * @param itemDataMap
	 * @param retLirMap
	 */
	private void resetMapForNoLIGConstraint(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {

		for (Map.Entry<ItemKey, PRItemDTO> itemDTO : itemDataMap.entrySet()) {

			if (itemDTO.getValue().isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getValue().getRetLirId());
				for (PRItemDTO ligMember : ligMembers) {

					PRStrategyDTO strategyDTO = ligMember.getStrategyDTO();
					PRConstraintsDTO contraintsDTO = strategyDTO.getConstriants();
					//clear the MAX map if the items do not have LIG constraint or item is on locked strategy
					if (contraintsDTO != null && contraintsDTO.getLigConstraint() == null || 
							(contraintsDTO != null &&  contraintsDTO.getLocPriceConstraint()!= null)) {

						if (ligMember.getMapRetail() > 0) {

							if (ligMember.getUserAttr5() != null && !ligMember.getUserAttr5().isEmpty()
									&& Double.parseDouble(ligMember.getUserAttr5()) > 0) {
								ligMember.setMapRetail(Double.parseDouble(ligMember.getUserAttr5()));
								// ligMember.setLockedRetail(Double.parseDouble(ligMember.getUserAttr5()));
								ligMember.setMinRetail(Double.parseDouble(ligMember.getUserAttr5()));

								/*
								 * logger.debug("resetMapForNoLIGConstraint()- item " + ligMember.getItemCode()
								 * + "locked price set :" + ligMember.getLockedRetail() + " LIG: " +
								 * ligMember.getRetLirID());
								 */
							} else {
								ligMember.setMapRetail(0);
								// ligMember.setLockedRetail(0);
								ligMember.setMinRetail(0);
								/*
								 * logger.debug("resetMapForNoLIGConstraint- item " + ligMember.getItemCode() +
								 * "locked price :" + ligMember.getLockedRetail() + "min retail : " +
								 * ligMember.getMinRetail() + " LIG: " + ligMember.getRetLirID());
								 */
							}
						}

					}
				}

			}
		}
	}

	/**
	 * Added By Karishma for LIG Representative row set the LIG constraint with the
	 * most common LIG Constraint across the members.If members are on lock ignore
	 * the same
	 * 
	 * @param ligMembers
	 * @param itemDataMap
	 * @return
	 */
	private void setLIGConstraintInStrategy(HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<ItemKey, PRItemDTO> itemDataMap) {

		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getRetLirId());
				HashMap<Long, Integer> strategyMapCount = new HashMap<>();
				HashMap<Long, Double> strategyAndMaxMoverMap = new HashMap<>();
				Map<Long, List<PRStrategyDTO>> strategyIdMemberMap = new HashMap<>();
				PRConstraintLIG ligConstraintForRep = null;

				for (PRItemDTO ligMember : ligMembers) {
					ItemKey itemkey= new ItemKey(ligMember.getItemCode(),PRConstants.NON_LIG_ITEM_INDICATOR);
					if(itemDataMap.containsKey(itemkey))
					{
						 List<PRStrategyDTO> temp= new ArrayList<>();
						
					PRStrategyDTO strategyDTO = itemDataMap.get(itemkey).getStrategyDTO();
					int counter = 0;
					double maxMovement=0.0;
					if (strategyDTO.getConstriants() != null) {
					//only check if lig memebrs is not having locked startegy else use the memebrs existing startegy to find the max lig startegy
						PRConstraintLocPrice locPriceConstraint = strategyDTO.getConstriants().getLocPriceConstraint();
							if (locPriceConstraint == null) {
							
							long strategyId = strategyDTO.getStrategyId();
								if (strategyIdMemberMap.containsKey(strategyId)) {
									temp = strategyIdMemberMap.get((strategyId));
								}
								temp.add(strategyDTO);
								strategyIdMemberMap.put(strategyId, temp);

							
							if (strategyMapCount.containsKey(strategyId)) {
								counter = strategyMapCount.get(strategyId);
							
							}
							counter = counter + 1;
							strategyMapCount.put(strategyId, counter);
								if (strategyAndMaxMoverMap.containsKey(strategyId)) {
									maxMovement = strategyAndMaxMoverMap.get(strategyId);
									if (ligMember.getXweekMovForLIGRepItem() >= maxMovement) {
										strategyAndMaxMoverMap.put(strategyId, ligMember.getXweekMovForLIGRepItem());
									}
								} else {
									strategyAndMaxMoverMap.put(strategyId, ligMember.getXweekMovForLIGRepItem());
								}
							
						}

					}
				}

				}

				if (strategyMapCount.size() > 0) {

					int maxCount = 0;
					HashMap<Integer, List<Long>> strategyIdCountMap = new HashMap<>();

					for (Map.Entry<Long, Integer> StrategyCount : strategyMapCount.entrySet()) {
						if (StrategyCount.getValue() >= maxCount) {
							maxCount = StrategyCount.getValue();
							List<Long> temp = new ArrayList<>();
							if (strategyIdCountMap.containsKey(maxCount)) {
								temp = strategyIdCountMap.get(maxCount);
							}
							temp.add(StrategyCount.getKey());
							strategyIdCountMap.put(maxCount, temp);
						}

					}

				

					int maxStrategyIdCount = strategyIdCountMap.keySet().stream()
							.max(Comparator.comparing(Integer::valueOf)).get();
					
					List<Long> strategyIdList = strategyIdCountMap.get(maxStrategyIdCount);
					if(strategyIdList.size()>1)
					{
						long StrategyId = strategyAndMaxMoverMap.keySet().stream().findFirst().get();
						ligConstraintForRep = strategyIdMemberMap.get(StrategyId).get(0).getConstriants()
								.getLigConstraint();
					}else
					{
						ligConstraintForRep = strategyIdMemberMap.get(strategyIdList.get(0)).get(0).getConstriants()
								.getLigConstraint();
					}
					

					if (itemDTO.getStrategyDTO() == null) {
						itemDTO.setStrategyDTO(new PRStrategyDTO());
					}
					itemDTO.getStrategyDTO().getConstriants().setLigConstraint(ligConstraintForRep);
				}

			}
		});

	}
}