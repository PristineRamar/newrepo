package com.pristine.service.offermgmt.mwr.basedata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.Cost;
import com.pristine.dto.Price;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.CriteriaDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiCompetitorKey;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.CLPDLPPredictionDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.service.offermgmt.ItemKey;

/**
 * 
 * @author Pradeepkumar
 * @version 1.1 BaseData class contains all the data required for quarter recommendation
 *
 */

public class BaseData {
	// List of stores for given product and location
	private List<Integer> storeList;
	
	// Authorized items
	private List<PRItemDTO> authorizedItems;

	// price check list info by item and LIG
	private HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo;

	// Item map for entire recommendation
	private HashMap<ItemKey, PRItemDTO> itemDataMap;

	// strategy Map
	private HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap;

	// product group information
	private HashMap<String, ArrayList<Integer>> productListMap;

	// product group relation
	private HashMap<Integer, Integer> productParentChildRelationMap;

	// Price group details
	private HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupDetails;

	// Competitor map
	private HashMap<Integer, LocationKey> competitorMap;
	
	// Latest Comp data map
	private HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> latestCompDataMap;
	
	// Latest Comp data map
	private HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> previousCompDataMap;

	// price and strategy zones from authorization items
	private List<String> priceAndStrategyZoneNos;

	// price data
	private HashMap<Integer, HashMap<String, RetailPriceDTO>> priceHistory;
	
	// Latest price data
	private LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> priceDataMap;

	// Cost data
	private HashMap<Integer, HashMap<String, List<RetailCostDTO>>> costDataMap;

	// Movement data
	private HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementDataMap;
	
	// Last X Weeks mov
	private HashMap<ProductKey, Long> lastXWeeksMovement;

	// Sale details
	private HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails;

	// Ad details
	private HashMap<Integer, List<PRItemAdInfoDTO>> adDetails;

	// Display details
	private HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails;
	
	// Location list
	private ArrayList<Integer> locationList;
	
	// Multi comp latest price map
	private HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap;
	
	// Recommendation rules map
	private HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	
	// Weekly Recommendation Map
	private HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap;
	
	// Parent rec details
	private HashMap<Integer, HashMap<ItemKey, MWRItemDTO>> parentRecInfo;
	
	// Prediction cache
	private List<MultiWeekPredEngineItemDTO> predictionCache;

	// Product group properties
	private List<PRProductGroupProperty> productGroupProperties;
	private HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> priceHistoryAll;
	
	// clp dlp prediction data
	private HashMap<Integer, List<CLPDLPPredictionDTO>> clpDlpPredictions;
	
	// criteria lookup
	private HashMap<Integer, List<CriteriaDTO>> criteriaDetailsFromStrategy;
	
	// Multi comp settings
	private HashMap<Integer, Integer> compSettings;
	
	//Store Inventory changes
	//Changes done by Bhargavi on 01/05/2021
	//update the MarkUp and MarkDown values for Rite Aid
	private HashMap<Integer, Integer> storeInventoryMap;
	
	//for storing futureCostFor FF
	private Map<Integer, List<Cost>>futureCost;
	//for storing future Price For FF
	private Map<Integer, List<Price>>futurePrice;
	
	private HashMap<Integer, HashMap<String, List<RetailCostDTO>>> futureCostDataMap;
	
	public HashMap<Integer, Integer> getStoreInventoryMap() {
		return storeInventoryMap;
	}

	public void setStoreInventoryMap(HashMap<Integer, Integer> storeInventoryMap) {
		this.storeInventoryMap = storeInventoryMap;
	}
	//Changes-ended

	public List<Integer> getStoreList() {
		return storeList;
	}

	public void setStoreList(List<Integer> storeList) {
		this.storeList = storeList;
	}

	public List<PRItemDTO> getAuthorizedItems() {
		return authorizedItems;
	}

	public void setAuthorizedItems(List<PRItemDTO> authorizedItems) {
		this.authorizedItems = authorizedItems;
	}

	public HashMap<ItemKey, List<PriceCheckListDTO>> getPriceCheckListInfo() {
		return priceCheckListInfo;
	}

	public void setPriceCheckListInfo(HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo) {
		this.priceCheckListInfo = priceCheckListInfo;
	}

	public HashMap<ItemKey, PRItemDTO> getItemDataMap() {
		return itemDataMap;
	}

	public void setItemDataMap(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		this.itemDataMap = itemDataMap;
	}

	public HashMap<StrategyKey, List<PRStrategyDTO>> getStrategyMap() {
		return strategyMap;
	}

	public void setStrategyMap(HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap) {
		this.strategyMap = strategyMap;
	}

	public HashMap<String, ArrayList<Integer>> getProductListMap() {
		return productListMap;
	}

	public void setProductListMap(HashMap<String, ArrayList<Integer>> productListMap) {
		this.productListMap = productListMap;
	}

	public HashMap<Integer, Integer> getProductParentChildRelationMap() {
		return productParentChildRelationMap;
	}

	public void setProductParentChildRelationMap(HashMap<Integer, Integer> productParentChildRelationMap) {
		this.productParentChildRelationMap = productParentChildRelationMap;
	}

	public HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> getPriceGroupDetails() {
		return priceGroupDetails;
	}

	public void setPriceGroupDetails(HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupDetails) {
		this.priceGroupDetails = priceGroupDetails;
	}

	public HashMap<Integer, LocationKey> getCompetitorMap() {
		return competitorMap;
	}

	public void setCompetitorMap(HashMap<Integer, LocationKey> competitorMap) {
		this.competitorMap = competitorMap;
	}

	public List<String> getPriceAndStrategyZoneNos() {
		return priceAndStrategyZoneNos;
	}

	public void setPriceAndStrategyZoneNos(List<String> priceAndStrategyZoneNos) {
		this.priceAndStrategyZoneNos = priceAndStrategyZoneNos;
	}

	public HashMap<Integer, HashMap<String, RetailPriceDTO>> getPriceHistory() {
		return priceHistory;
	}

	public void setPriceHistory(HashMap<Integer, HashMap<String, RetailPriceDTO>> priceHistory) {
		this.priceHistory = priceHistory;
	}

	public HashMap<Integer, HashMap<String, List<RetailCostDTO>>> getCostDataMap() {
		return costDataMap;
	}

	public void setCostDataMap(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> costDataMap) {
		this.costDataMap = costDataMap;
	}

	public HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> getMovementDataMap() {
		return movementDataMap;
	}

	public void setMovementDataMap(HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementDataMap) {
		this.movementDataMap = movementDataMap;
	}

	public HashMap<Integer, List<PRItemSaleInfoDTO>> getSaleDetails() {
		return saleDetails;
	}

	public void setSaleDetails(HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails) {
		this.saleDetails = saleDetails;
	}

	public HashMap<Integer, List<PRItemAdInfoDTO>> getAdDetails() {
		return adDetails;
	}

	public void setAdDetails(HashMap<Integer, List<PRItemAdInfoDTO>> adDetails) {
		this.adDetails = adDetails;
	}

	public HashMap<Integer, List<PRItemDisplayInfoDTO>> getDisplayDetails() {
		return displayDetails;
	}

	public void setDisplayDetails(HashMap<Integer, List<PRItemDisplayInfoDTO>> displayDetails) {
		this.displayDetails = displayDetails;
	}

	public ArrayList<Integer> getLocationList() {
		return locationList;
	}

	public void setLocationList(ArrayList<Integer> locationList) {
		this.locationList = locationList;
	}

	public HashMap<ProductKey, Long> getLastXWeeksMovement() {
		return lastXWeeksMovement;
	}

	public void setLastXWeeksMovement(HashMap<ProductKey, Long> lastXWeeksMovement) {
		this.lastXWeeksMovement = lastXWeeksMovement;
	}

	public HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> getLatestCompDataMap() {
		return latestCompDataMap;
	}

	public void setLatestCompDataMap(HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> compDataMap) {
		this.latestCompDataMap = compDataMap;
	}

	public HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> getPreviousCompDataMap() {
		return previousCompDataMap;
	}

	public void setPreviousCompDataMap(HashMap<LocationKey, HashMap<Integer, CompetitiveDataDTO>> previousCompDataMap) {
		this.previousCompDataMap = previousCompDataMap;
	}

	public LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> getPriceDataMap() {
		return priceDataMap;
	}

	public void setPriceDataMap(LinkedHashMap<Integer, HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>> priceDataMap) {
		this.priceDataMap = priceDataMap;
	}

	public HashMap<MultiCompetitorKey, CompetitiveDataDTO> getMultiCompLatestPriceMap() {
		return multiCompLatestPriceMap;
	}

	public void setMultiCompLatestPriceMap(HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap) {
		this.multiCompLatestPriceMap = multiCompLatestPriceMap;
	}

	public HashMap<String, List<RecommendationRuleMapDTO>> getRecommendationRuleMap() {
		return recommendationRuleMap;
	}

	public void setRecommendationRuleMap(HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		this.recommendationRuleMap = recommendationRuleMap;
	}

	public HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> getWeeklyItemDataMap() {
		return weeklyItemDataMap;
	}

	public void setWeeklyItemDataMap(HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap) {
		this.weeklyItemDataMap = weeklyItemDataMap;
	}

	public HashMap<Integer, HashMap<ItemKey, MWRItemDTO>> getParentRecInfo() {
		return parentRecInfo;
	}

	public void setParentRecInfo(HashMap<Integer, HashMap<ItemKey, MWRItemDTO>> parentRecInfo) {
		this.parentRecInfo = parentRecInfo;
	}

	
	public List<PRProductGroupProperty> getProductGroupProperties() {
		return productGroupProperties;
	}

	public void setProductGroupProperties(List<PRProductGroupProperty> productGroupProperties) {
		this.productGroupProperties = productGroupProperties;
	}

	public HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> getPriceHistoryAll() {
		return priceHistoryAll;
	}

	public void setPriceHistoryAll(HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> priceHistoryAll) {
		this.priceHistoryAll = priceHistoryAll;
	}

	public HashMap<Integer, List<CLPDLPPredictionDTO>> getClpDlpPredictions() {
		return clpDlpPredictions;
	}

	public void setClpDlpPredictions(HashMap<Integer, List<CLPDLPPredictionDTO>> clpDlpPredictions) {
		this.clpDlpPredictions = clpDlpPredictions;
	}

	public List<MultiWeekPredEngineItemDTO> getPredictionCache() {
		return predictionCache;
	}

	public void setPredictionCache(List<MultiWeekPredEngineItemDTO> predictionCache) {
		this.predictionCache = predictionCache;
	}

	public HashMap<Integer, List<CriteriaDTO>> getCriteriaDetailsFromStrategy() {
		return criteriaDetailsFromStrategy;
	}

	public void setCriteriaDetailsFromStrategy(HashMap<Integer, List<CriteriaDTO>> criteriaDetailsFromStrategy) {
		this.criteriaDetailsFromStrategy = criteriaDetailsFromStrategy;
	}

	public HashMap<Integer, Integer> getCompSettings() {
		return compSettings;
	}

	public void setCompSettings(HashMap<Integer, Integer> compSettings) {
		this.compSettings = compSettings;
	}
	
	/**
	 * Cleans up used objects
	 */
	public void cleanupCacheSet1() {
		this.authorizedItems = null;
		this.priceCheckListInfo = null;
		this.strategyMap = null;
		this.priceGroupDetails = null;
		this.latestCompDataMap = null;
		this.previousCompDataMap = null;
		this.costDataMap = null;
		this.lastXWeeksMovement = null;
		this.criteriaDetailsFromStrategy = null;
		this.compSettings = null;
	}
	
	/**
	 * Cleans up used objects
	 */
	public void cleanupCacheSet2() {
		this.productGroupProperties = null;
		this.multiCompLatestPriceMap = null;
		this.movementDataMap = null;
		this.productListMap = null;
		this.productParentChildRelationMap = null;
		this.priceAndStrategyZoneNos = null;
		this.competitorMap = null;
		this.saleDetails = null;
		this.adDetails = null;
		this.displayDetails = null;
		this.storeList = null;
		this.parentRecInfo = null;
		this.predictionCache = null;
		this.locationList = null;
	}

	public Map<Integer, List<Cost>> getFutureCost() {
		return futureCost;
	}

	public void setFutureCost(Map<Integer, List<Cost>> futureCost) {
		this.futureCost = futureCost;
	}

	
	/**
	 * @return the futurePrice
	 */
	public Map<Integer, List<Price>> getFuturePrice() {
		return futurePrice;
	}

	
	/**
	 * @param futurePrice the futurePrice to set
	 */
	public void setFuturePrice(Map<Integer, List<Price>> futurePrice) {
		this.futurePrice = futurePrice;
	}

	public HashMap<Integer, HashMap<String, List<RetailCostDTO>>> getFutureCostDataMap() {
		return futureCostDataMap;
	}

	public void setFutureCostDataMap(HashMap<Integer, HashMap<String, List<RetailCostDTO>>> futureCostDataMap) {
		this.futureCostDataMap = futureCostDataMap;
	}
	
}
