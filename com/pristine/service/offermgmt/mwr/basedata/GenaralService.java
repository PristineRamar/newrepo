package com.pristine.service.offermgmt.mwr.basedata;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

//import org.apache.log4j.Logger;

import com.pristine.dao.PriceTestDAO;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.mwr.WeeklyRecDAO;
import com.pristine.dao.offermgmt.mwr.prediction.MultiWeekPredictionDAO;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dto.ProductDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRRunHeader;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.ProductService;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class GenaralService {

//	private static Logger logger = Logger.getLogger("GenaralService");
	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param ispriceTestStore 
	 * @return List of stores for given product and location
	 * @throws GeneralException
	 */
	public List<Integer> getStoresForProductAndLoction(Connection conn, int locationLevelId, int locationId,
			int productLevelId, int productId, boolean isGlobalZone, boolean isPriceTestStore) throws GeneralException {

		List<Integer> priceZoneStores = null;
		boolean useZoneTableForStores = Boolean.parseBoolean(PropertyManager.getProperty("USE_STORES_FROM_ZONE_TABLE", "FALSE"));
		
		// Get stores for given product and location
		if (isPriceTestStore) {
			priceZoneStores = new PriceTestDAO().getStoreIdsforStoreList(conn, locationLevelId, locationId,productId,productLevelId);
		} else if (isGlobalZone) {
			priceZoneStores = new RetailPriceZoneDAO().getStoreIdsInGlobalZone(conn, locationId);
		} else if (useZoneTableForStores) {
			priceZoneStores = new RetailPriceZoneDAO().getStoreIdsInZone(conn, locationId);
		} else {
			priceZoneStores = new ItemService(null).getPriceZoneStores(conn, productLevelId, productId, locationLevelId,
					locationId);
		}
		return priceZoneStores;
	}

	/**
	 * 
	 * @param conn
	 * @param runId
	 * @param inputDTO
	 * @param allStoreItems
	 * @return map of LIG/Non LIG items
	 * @throws OfferManagementException
	 */
	public HashMap<ItemKey, PRItemDTO> getItemDataMap(Connection conn, long runId,
			RecommendationInputDTO recommendationInputDTO, List<PRItemDTO> allStoreItems)
					throws OfferManagementException {

		// Input DTO... Many methods rely on Strategy DTO to get the inputs
		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);
		// Transform zone authorized item's to object
		HashMap<ItemKey, PRItemDTO> itemDataMap = new ItemService(null).populateAuthorizedItemsOfZone(conn, runId,
				inputDTO, allStoreItems);

		return itemDataMap;
	}

	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @return product lists
	 */
	public HashMap<String, ArrayList<Integer>> getProductListForProducts(Connection conn,
			RecommendationInputDTO recommendationInputDTO) {
		return new ProductGroupDAO().getProductListForProducts(conn, recommendationInputDTO.getProductLevelId(),
				recommendationInputDTO.getProductId());
	}

	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @return product lists
	 * @throws GeneralException
	 */
	public HashMap<Integer, Integer> getProductLevelRelationMap(Connection conn,
			RecommendationInputDTO recommendationInputDTO) throws GeneralException {
		return new ProductService().getProductLevelRelationMapRec(conn, recommendationInputDTO.getProductLevelId());
	}

	/**
	 * 
	 * @param authorizedItems
	 * @return list of price and strategy zones
	 */
	public List<String> getPriceAndStrategyZoneNos(List<PRItemDTO> authorizedItems) {
		return new ItemService(null).getPriceAndStrategyZoneNos(authorizedItems);
	}

	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @return location list
	 */
	public ArrayList<Integer> getLocationListId(Connection conn, RecommendationInputDTO recommendationInputDTO) {
		ArrayList<Integer> locationList = new ArrayList<Integer>();

		if (recommendationInputDTO.isPriceTestZone()) {
			locationList = new PricingEngineDAO().getLocationListId(conn, recommendationInputDTO.getLocationLevelId(),
					recommendationInputDTO.getTempLocationID());
		} else
			locationList = new PricingEngineDAO().getLocationListId(conn, recommendationInputDTO.getLocationLevelId(),
					recommendationInputDTO.getLocationId());

		return locationList;
	}
	
	/**
	 * 
	 * @param conn
	 * @return recommendation rules
	 */
	public HashMap<String, List<RecommendationRuleMapDTO>> getRecommendationRules(Connection conn){
		return new PricingEngineDAO().getRecommendationRules(conn);
	}
	
	/**
	 * 
	 * @param conn
	 * @param parentRunId
	 * @return parent rec info
	 * @throws GeneralException
	 */
	public HashMap<Integer, HashMap<ItemKey, MWRItemDTO>> getParentRecommendation(Connection conn, long parentRunId)
			throws GeneralException {

		HashMap<Integer, HashMap<ItemKey, MWRItemDTO>> weeklyItemMap = new HashMap<>();

		List<MWRItemDTO> parentRecDetails = new WeeklyRecDAO().getParentRecommendationDetails(conn, parentRunId);

		for (MWRItemDTO mwrItemDTO : parentRecDetails) {
			ItemKey itemKey = null;
			int calendarId = mwrItemDTO.getWeekCalendarId();
			if (mwrItemDTO.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
//				itemKey = new ItemKey(PRConstants.LIG_ITEM_INDICATOR,mwrItemDTO.getProductId());
				itemKey = new ItemKey(mwrItemDTO.getProductId(), PRConstants.LIG_ITEM_INDICATOR);
			} else if (mwrItemDTO.getProductLevelId() == Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID) {
//				itemKey = new ItemKey(PRConstants.NON_LIG_ITEM_INDICATOR,mwrItemDTO.getProductId());
				itemKey = new ItemKey(mwrItemDTO.getProductId(),PRConstants.NON_LIG_ITEM_INDICATOR);
			}
			HashMap<ItemKey, MWRItemDTO> itemMap = new HashMap<>();
			if (weeklyItemMap.containsKey(calendarId)) {
				itemMap = weeklyItemMap.get(calendarId);
			}
			itemMap.put(itemKey, mwrItemDTO);
			weeklyItemMap.put(calendarId, itemMap);
		}
		
		return weeklyItemMap;
	}
	
	
	public List<MultiWeekPredEngineItemDTO> getPredictionCache(Connection conn,
			MWRRunHeader mwrRunHeader) throws GeneralException {

		return new MultiWeekPredictionDAO().getPredictionCache(conn, mwrRunHeader);

	}
	
	public List<PRProductGroupProperty> getProductProperties(Connection conn,
			RecommendationInputDTO recommendationInputDTO) throws OfferManagementException {
		List<ProductDTO> productList = new ArrayList<ProductDTO>();
		ProductDTO productDTO = new ProductDTO();
		productDTO.setProductLevelId(recommendationInputDTO.getProductLevelId());
		productDTO.setProductId(recommendationInputDTO.getProductId());
		productList.add(productDTO);
		return new PricingEngineDAO().getProductGroupProperties(conn, productList);
	}

	/**
	 * 
	 * @param itemDataMap
	 * @param uniqueZoneIDs
	 * @param recommendationInputDTO
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<ItemKey, HashMap<Integer, Double>> setlatestRecforZones(HashMap<ItemKey, PRItemDTO> itemDataMap,
			Set<Integer> uniqueZoneIDs, RecommendationInputDTO recommendationInputDTO, Connection conn)
			throws GeneralException {
		HashMap<Integer, Long> latestRunIds = new PricingEngineDAO().getLatestRunIdsForZones(conn, uniqueZoneIDs,
				recommendationInputDTO.getProductId(), recommendationInputDTO.getProductLevelId());
		HashMap<ItemKey, HashMap<Integer, Double>>priceMap = new PricingEngineDAO().setLatestRecPricesforZones(conn,
				itemDataMap, latestRunIds, recommendationInputDTO.getProductId(),
				recommendationInputDTO.getProductLevelId());
		return priceMap;
	}
	/** PROM-2223 changes Started **/
	/**
	 * Added By Karishma on 09/29/21 for AZ for setting aggregated impact for Z1000
	 * @param itemDataMap
	 * @param recommendationInputDTO
	 * @param retLirMap 
	 * @param conn
	 * @param isUpdateRecc 
	 * @return
	 * @throws GeneralException
	 * @throws Exception
	 */
	public void getLatestRegRetailslOfAllZones(HashMap<ItemKey, PRItemDTO> itemDataMap,
			RecommendationInputDTO recommendationInputDTO, HashMap<Integer, List<PRItemDTO>> retLirMap, Connection conn, boolean isUpdateRecc) throws GeneralException, Exception {

		HashMap<Integer, Map<Double, List<Double>>> itemMap = new PricingEngineDAO()
				.getLatestRegRetailslOfZones(recommendationInputDTO.getProductId(), conn);

		itemDataMap.forEach((itemKey, itemDTO) -> {

			if (!itemDTO.isLir() && itemMap.containsKey(itemDTO.getItemCode())) {
				itemDTO.setCurrRetailslOfAllZones(itemMap.get(itemDTO.getItemCode()));
				//populate the L13 units only if its not called from Update Recommendation as it is already picked from the database table when its update Recommendation
				if (!isUpdateRecc) {
					// L13 units of Z1000 should be aggregated movement of Z4 and Z16
					Map<Double, List<Double>> priceMovementMap = itemMap.get(itemDTO.getItemCode());

					priceMovementMap.forEach((price, movement) -> {
						for (Double mov : movement) {

							itemDTO.setXweekMov(itemDTO.getXweekMov() + mov);
							itemDTO.setXweekMovForLIGRepItem(itemDTO.getXweekMovForLIGRepItem() + mov);
						}
					});

			}
			}

		});
		// For LIG aggregate the L13 units from the members
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getRetLirId());

				double aggregatedL13Units = ligMembers.stream().mapToDouble(a -> a.getXweekMov()).sum();
				itemDTO.setXweekMov(aggregatedL13Units);

			}
		});
		
	

	}
	
	/** PROM-2223 changes End **/
	
	/**
	 * 
	 * @param itemDataMap
	 * @param recommendationInputDTO
	 * @param conn
	 * @throws GeneralException
	 * @throws Exception
	 */
	public void getQueueRecommendations(HashMap<ItemKey, PRItemDTO> itemDataMap,
			 Connection conn, int productId,int locationId ) throws GeneralException, Exception {

		HashMap<Integer, PRItemDTO> itemMap = new PricingEngineDAO()
				.getQueueRecommendations(conn,productId,locationId);

		itemDataMap.forEach((itemKey, itemDTO) -> {

			if (itemMap.containsKey(itemDTO.getItemCode())) {
				itemDTO.setApprovedImpact(itemMap.get(itemDTO.getItemCode()).getImpact());
				itemDTO.setApprovedRetail(itemMap.get(itemDTO.getItemCode()).getRecommendedRegPrice().getUnitPrice());
				//logger.info("item : "+ itemDTO.getItemCode() +"and approved impact set:" + itemDTO.getApprovedImpact() +" approved retail "+itemDTO.getApprovedRetail()  );

			}
		});

	}
	


	public void getItemsWithPendingRetailsFromQueue(Connection conn, RecommendationInputDTO recommendationInputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap) throws GeneralException {
		HashMap<Long, List<Integer>> runIdAndItemMap =new PricingEngineDAO().getItemsAndRunIdWithPendingRetails(conn, recommendationInputDTO);

		if (runIdAndItemMap != null && runIdAndItemMap.size() > 0) {

			int limitcount = 0;
			List<Integer> itemsList = new ArrayList<Integer>();
			for (Map.Entry<Long, List<Integer>> entrySet : runIdAndItemMap.entrySet()) {
				
				for (Integer item : entrySet.getValue()) {
					itemsList.add(item);
					limitcount++;
					if (limitcount > 0 && (limitcount % Constants.LIMIT_COUNT == 0)) {
						Object[] itemValues = itemsList.toArray();
						new PricingEngineDAO().setPendingRetails(conn, entrySet.getKey(), itemValues, itemDataMap);
						itemsList.clear();
						limitcount = 0;
					}
				}

				if (itemsList.size() > 0) {
					Object[] itemValues = itemsList.toArray();
					new PricingEngineDAO().setPendingRetails(conn, entrySet.getKey(), itemValues, itemDataMap);
					itemsList.clear();
				}

			}

		}
		
	}
}
