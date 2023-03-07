package com.pristine.service.offermgmt.mwr.basedata;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class MovementDataService {

	private static Logger logger = Logger.getLogger("MovementDataService");
	BigDecimal totalRev = new BigDecimal(0);
	BigDecimal recentXWeeksRev =new BigDecimal(0);
	
	String substituteForPrediction = (PropertyManager.getProperty("SUBSTITUE_FOR_PREDICTION", "52_WEEKS_AVG"));
	/**
	 * 
	 * @param conn
	 * @param itemDataMap
	 * @param storeList
	 * @param recommendationInputDTO
	 * @return movement data
	 * @throws GeneralException
	 * @throws Exception 
	 */
	public HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> getMovementData(Connection conn,
			HashMap<ItemKey, PRItemDTO> itemDataMap, List<Integer> storeList,
			LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails,
			RecommendationInputDTO recommendationInputDTO) throws GeneralException, Exception {

		int noOfWeeksBehind = MultiWeekRecConfigSettings.getMwrNumberOfWeeksBehind();
		int noOfWeeksIMSData = MultiWeekRecConfigSettings.getMwrNumberOfWeeksIMS();

		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);

		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();

		HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData = pricingEngineDAO
				.getMovementDataForZone(conn, inputDTO, storeList, recommendationInputDTO.getBaseWeek(),
						noOfWeeksIMSData, itemDataMap);

		pricingEngineDAO.getMovementDataForZone(movementData, itemDataMap, previousCalendarDetails, noOfWeeksBehind);

		// Added on 05/14/2022 as a temporary change for AZ to use the last years
		// movement for the time persiod of current recommendation weeks to
		// calculate the average movement
		// If use Prediction is false then this average movement will be used
		if (substituteForPrediction.equalsIgnoreCase(PRConstants.LAST_YEAR_SIMILAR_WEEKS_MOVEMENT)) {
			logger.info("using Last weeks similar movement for items that should not be passed to prediction" );
			setPreviousYearsMovementData(conn, itemDataMap, recommendationInputDTO);
		}

		return movementData;

	}

	/**
	 * 
	 * @param conn
	 * @param itemDataMap
	 * @param storeList
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 * 
	 */
	public HashMap<ProductKey, Long> getLastXWeeksMovement(Connection conn, HashMap<ItemKey, PRItemDTO> itemDataMap,
			List<Integer> storeList, RecommendationInputDTO recommendationInputDTO) throws GeneralException {

		HashMap<ProductKey, Long> lastXWeeksMov = new HashMap<ProductKey, Long>();
		int movHistoryWeeks = MultiWeekRecConfigSettings.getHistoryWeeksForMovingItems();
		//Added by Karishma on 17/04/2020 for applying guideLines/constrainsts to non moving items as well for AZ
		if (movHistoryWeeks > 0) {
			// 16th Jul 2016, retain current price when an item didn't move in last 2 years
			lastXWeeksMov = new PricingEngineDAO().getLastXWeeksMov(conn, storeList,
					recommendationInputDTO.getBaseWeek(), movHistoryWeeks, itemDataMap,
					recommendationInputDTO.getLocationId());
		}

		return lastXWeeksMov;
	}

	/**
	 * 
	 * @param itemDataMap
	 * @param retLirMap
	 * @param movementData
	 * @param previousCalendarDetails
	 * @param field
	 * @param prevCalID
	 * @throws GeneralException
	 */
	public void populateXWeeksMovementData(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData,
			LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails, int noOfRecentWeeksMov, String field,
			int prevCalID) throws GeneralException {
		
		for (Map.Entry<Integer, HashMap<Integer, ProductMetricsDataDTO>> entry : movementData.entrySet()) {
			double xWeeksMov = 0;
		
			BigDecimal xWeekTotalRev = new BigDecimal(0);
			
			int noOfWeeksCount = 1;
			ItemKey itemKey = new ItemKey(entry.getKey(), PRConstants.NON_LIG_ITEM_INDICATOR);
			HashMap<Integer, ProductMetricsDataDTO> movementHistoryData = entry.getValue();
			for (RetailCalendarDTO retailCalendarDTO : previousCalendarDetails.values()) {
				if (noOfWeeksCount > noOfRecentWeeksMov) {
					break;
				} else {
					ProductMetricsDataDTO productMetricsDataDTO = movementHistoryData
							.get(retailCalendarDTO.getCalendarId());
					if (productMetricsDataDTO != null) {
						xWeeksMov = xWeeksMov + productMetricsDataDTO.getTotalMovement();
						xWeekTotalRev = xWeekTotalRev.add(BigDecimal.valueOf(productMetricsDataDTO.getTotalRevenue()));
						
					}
				}
				// Added by Karishma on 31/03/2020 to check if we have movement for last week
				// if movement is not present for last week then we don't update counter
				// This is to get 52 weeks movement while calculating impact for AZ
				if (retailCalendarDTO.getCalendarId() == prevCalID) {

					ProductMetricsDataDTO productMetricsDataDTO = movementHistoryData
							.get(retailCalendarDTO.getCalendarId());
					if (productMetricsDataDTO != null) {
						noOfWeeksCount++;
					}

				} else
					noOfWeeksCount++;

			}
			
//			 logger.debug("noOfWeeksCount:" + noOfWeeksCount + "item" + entry.getKey());
			if (itemDataMap.get(itemKey) != null) {
				PRItemDTO tDTO = itemDataMap.get(itemKey);

				if (field.equalsIgnoreCase(PRConstants.RECENT_X_WEEKS)) {
					tDTO.setRecentXWeeksMov(xWeeksMov);
				} else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_FOR_IMPACT)) {
					tDTO.setxWeeksMovForTotimpact(xWeeksMov);
					// Added to get additionalCriteria movement for 52 weeks
				} else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_FOR_ADD_CRITERIA)) {
					tDTO.setxWeeksMovForAddlCriteria(xWeeksMov);
				} else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_FOR_PRED_EXCLUDE)) {
					tDTO.setRecentXWeeksMov(xWeeksMov);
					tDTO.setTotalRevenue(xWeekTotalRev);
				} else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_FOR_WAC)) {
					tDTO.setxWeeksMovForWAC(xWeeksMov);
				}else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_MOV)) {
					tDTO.setXweekMov(xWeeksMov);
				} //Added by Karishma on 02/14/2022 for setting the variable with movement 
					//to be used for selecting the LIG rep Item for max mover constraint
				else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_MOV_LIG_REP_ITEM)) {
					tDTO.setXweekMovForLIGRepItem(xWeeksMov);
				}
			}
		}
		// Added condition to set the 52 weeks movement for additional criteria
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getRetLirId());

				if (field.equalsIgnoreCase(PRConstants.RECENT_X_WEEKS)) {
					double recentXWeeksMovAtLig = ligMembers.stream().mapToDouble(PRItemDTO::getRecentXWeeksMov).sum();
					itemDTO.setRecentXWeeksMov(recentXWeeksMovAtLig);
				} else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_FOR_IMPACT)) {
					double recentXWeeksMovAtLig = ligMembers.stream().mapToDouble(PRItemDTO::getxWeeksMovForTotimpact)
							.sum();
					itemDTO.setxWeeksMovForTotimpact(recentXWeeksMovAtLig);
				} else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_FOR_ADD_CRITERIA)) {
					double xWeeksMovForAddlCriteria = ligMembers.stream()
							.mapToDouble(PRItemDTO::getxWeeksMovForAddlCriteria).sum();
					itemDTO.setxWeeksMovForAddlCriteriaAtLIGLevel(xWeeksMovForAddlCriteria);
					ligMembers.forEach(ligMember -> {
						ligMember.setxWeeksMovForAddlCriteriaAtLIGLevel(xWeeksMovForAddlCriteria);
					});
				} else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_FOR_PRED_EXCLUDE)) {
					double recentXWeeksMovAtLig = ligMembers.stream().mapToDouble(PRItemDTO::getRecentXWeeksMov).sum();
					itemDTO.setRecentXWeeksMov(recentXWeeksMovAtLig);
					recentXWeeksRev =new BigDecimal(0);
					ligMembers.forEach(member->{
						if(member.getTotalRevenue()!=null)
						{
						 recentXWeeksRev =recentXWeeksRev.add(member.getTotalRevenue());
						}
					});
					//double recentXWeeksRev = ligMembers.stream().mapToDouble(PRItemDTO::getTotalRevenue).sum();
					itemDTO.setTotalRevenue(recentXWeeksRev);
				}else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_FOR_WAC)) {
					double xweeksforWAC = ligMembers.stream()
							.mapToDouble(PRItemDTO::getxWeeksMovForWAC).sum();
					itemDTO.setxWeeksMovForWAC(xweeksforWAC);
				}else if (field.equalsIgnoreCase(PRConstants.X_WEEKS_MOV)) {
					double xweeksMovsum = ligMembers.stream()
							.mapToDouble(PRItemDTO::getXweekMov).sum();
					itemDTO.setXweekMov(xweeksMovsum);
				}
			}
		});
		
	
	}

	/**
	 * 
	 * @param itemDataMap
	 * @param retLirMap
	 * @param movementData
	 * @param previousCalendarDetails
	 */
	public void setExcludeWeeksMovAndRev(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap,
			HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData,
			LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails, int weeksToExclude) {
		for (Map.Entry<Integer, HashMap<Integer, ProductMetricsDataDTO>> entry : movementData.entrySet()) {
			double excludeWeeksMov = 0;
			double excludeWeeksRev = 0;
			int noOfWeeksCount = 1;
			ItemKey itemKey = new ItemKey(entry.getKey(), PRConstants.NON_LIG_ITEM_INDICATOR);
			HashMap<Integer, ProductMetricsDataDTO> movementHistoryData = entry.getValue();
			for(RetailCalendarDTO retailCalendarDTO : previousCalendarDetails.values()) {
				if(noOfWeeksCount > weeksToExclude) {
					break;
				} else {
					ProductMetricsDataDTO productMetricsDataDTO = movementHistoryData.get(retailCalendarDTO.getCalendarId());
					if(productMetricsDataDTO != null) {
						excludeWeeksMov = excludeWeeksMov + productMetricsDataDTO.getTotalMovement();
						excludeWeeksRev = excludeWeeksRev + productMetricsDataDTO.getTotalRevenue();
					}
				}
				noOfWeeksCount++;
			}
			if (itemDataMap.get(itemKey) != null) {
				PRItemDTO tDTO = itemDataMap.get(itemKey);
				tDTO.setRecentXWeeksMov(excludeWeeksMov);
			}
		}
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getRetLirId());
				double recentXWeeksMovAtLig = ligMembers.stream().mapToDouble(PRItemDTO::getRecentXWeeksMov).sum();
				itemDTO.setRecentXWeeksMov(recentXWeeksMovAtLig);
				if(recentXWeeksMovAtLig == 0) {
					itemDTO.setNoRecentWeeksMov(true);
					ligMembers.forEach(ligMember -> {
						ligMember.setNoRecentWeeksMov(true);
					});
				}
			} else if (itemDTO.getRetLirId() == 0) {
				if(itemDTO.getRecentXWeeksMov() == 0) {
					itemDTO.setNoRecentWeeksMov(true);
				}
			}
		});
	}

	/**
	 * Added By Karishma ,Separated this part out from populateXWeeksMovementData()
	 * function and will be called from ItemAttributeService once X weeks movement
	 * is populated
	 * @param itemDataMap
	 * @param retLirMap
	 */
	public void markItemswithNorecWeeksMov(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (itemDTO.isLir()) {
				List<PRItemDTO> ligMembers = retLirMap.get(itemDTO.getRetLirId());
				double recentXWeeksMovAtLig = ligMembers.stream().mapToDouble(PRItemDTO::getRecentXWeeksMov).sum();
				itemDTO.setRecentXWeeksMov(recentXWeeksMovAtLig);
				if (recentXWeeksMovAtLig == 0) {
//					logger.debug("markItemswithNorecWeeksMov()- LIR-item with no movement;" + itemDTO.getItemCode()
//							+ ";LIR ID;" + itemDTO.getRetLirId() + ";tot members:;" + ligMembers.size());
					// logger.debug("markItemswithNorecWeeksMov()- LIR-item with no movement" +
					// itemDTO.getItemCode() + "LIR ID" +itemDTO.getRetLirId()) ;
					itemDTO.setNoRecentWeeksMov(true);
					ligMembers.forEach(ligMember -> {
						ligMember.setNoRecentWeeksMov(true);
					});
				}
			} else if (itemDTO.getRetLirId() == 0) {
				if (itemDTO.getRecentXWeeksMov() == 0) {
//					logger.debug("markItemswithNorecWeeksMov()- item with no movement;" + itemDTO.getItemCode());
					itemDTO.setNoRecentWeeksMov(true);
				}
			}
		});
	}

	/**
	 * Added by Karishma to get items belonging to same family and have movement
	 * aggregated at family level This change is done for AZ to get 52 weeks
	 * movement for items belong to same family and then applying additional
	 * criteria for items having movement <100
	 * 
	 * @param itemDataMap
	 */
	public void setMovementForFamilyItems(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		// populate ItemfamilyMap using familyKey
		HashMap<String, List<PRItemDTO>> itemFamilyMap = new HashMap<String, List<PRItemDTO>>();
		itemDataMap.forEach((itemkey, itemList) -> {
			List<PRItemDTO> temp = new ArrayList<>();
			if (itemFamilyMap.containsKey(itemList.getFamilyName())) {
				temp = itemFamilyMap.get(itemList.getFamilyName());
			}
			temp.add(itemList);
			itemFamilyMap.put(itemList.getFamilyName(), temp);
		});

		itemFamilyMap.forEach((familyKey, itemList) -> {
			double familyLevelMovement = itemList.stream().filter(p -> !p.isLir())
					.mapToDouble(PRItemDTO::getxWeeksMovForAddlCriteria).sum();
			itemList.forEach(item -> {
				ItemKey itemKey = PRCommonUtil.getItemKey(item);
				PRItemDTO itemInMap = itemDataMap.get(itemKey);
				itemInMap.setFamilyXWeeksMov(familyLevelMovement);
			});

		});

	}

	/**
	 * Added by Karishma to calculate perStoreRevenue for an item
	 * 
	 * @param itemDataMap
	 */
	int count = 0;

	public void calculateTotalRev(HashMap<ItemKey, PRItemDTO> itemDataMap) {

		itemDataMap.forEach((itemkey, item) -> {
			count++;
			if (item.getTotalRevenue() != null) {
//				logger.debug("calculateTotalRev()-Revenue by item; " + item.getItemCode() + ";" + item.getRetLirId()
//						+ "; " + item.getTotalRevenue());
//				logger.debug("calculateTotalRev()-Itemstatus; " + item.isSendToPrediction());
				totalRev = totalRev.add(item.getTotalRevenue());
			}

		});
		logger.info("calculateTotalRev()-  #items:" + count + " Total   Revenue:" + totalRev);
	}
	
	/**
	 * 
	 * @param conn
	 * @param itemDataMap
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 * @throws Exception
	 */
	public void setPreviousYearsMovementData(Connection conn,
			HashMap<ItemKey, PRItemDTO> itemDataMap, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException, Exception {

		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		pricingEngineDAO.getPreviousYearsMovementDataForZone(conn, inputDTO, recommendationInputDTO.getStartWeek(),
				recommendationInputDTO.getEndWeek(), itemDataMap);

	}
}
