package com.pristine.service.offermgmt.oos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;


import com.pristine.dto.offermgmt.DayPartLookupDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.oos.OOSExpectationDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.dto.offermgmt.oos.WeeklyAvgMovement;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

/**
 * This class contains all conditions/criteria to find out of stock item
 * 
 * @author dunagaraj
 *
 */
public class OOSCriteria {
	private static Logger logger = Logger.getLogger("OOSCriteria");
	
	private double OOS_LIG_UNIT_SALE_PRICE;
	private int OOS_ITEM_PREV_DAY_TRANSACTION;
	private int OOS_LIG_PREV_DAY_TRANSACTION;	
//	private int OOS_ITEM_NO_OF_TIMES_MOVED_IN_X_WEEKS;
	private int OOS_ITEM_LOWER_CONFIDENCE;
//	private int OOS_ITEM_DAY_PART_EXPECTED_MOV;
//	private int OOS_LIG_NO_OF_TIMES_MOVED_IN_X_WEEKS;
	private int OOS_LIG_LOWER_CONFIDENCE;
//	private int OOS_LIG_DAY_PART_EXPECTED_MOV;
	private int OOS_ITEM_TRANS_BASED_EXP_MOV;
	private int OOS_LIG_TRANS_BASED_EXP_MOV;
//	private int OOS_COND5_NO_OF_UNITS;
//	private int OOS_COND5_NO_OF_TIME_SLOT;
	private int OOS_TOP_ITEMS;
	private int OOS_REPORT_LAST_X_WEEKS;
//	private int OOS_ITEM_ACT_MOV_PREV_TIME_SLOT;
//	private int OOS_LIG_ACT_MOV_PREV_TIME_SLOT;
	private int OOS_SHELF_CAPACITY_PCT;
	private int OOS_MIN_SHELF_CAPACITY_PCT_ITEM_MOV;
	
	private int OOS_COND2_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV;
	private int OOS_COND2_ITEM_LOWER_CONFIDENCE;
	private int OOS_COND2_LIG_NO_OF_UNITS;
	private int OOS_COND2_LIG_LOWER_CONFIDENCE;
	private int OOS_COND2_LIG_TRX_BASED_EXP_OF_TIME_SLOTS;
	private int OOS_COND2_LIG_NO_OF_UNITS_X_LOCATIONS;
	private int OOS_COND2_LIG_LOWER_CONFIDENCE_X_LOCATIONS;
	private int OOS_COND2_LIG_TRX_BASED_EXP_OF_TIME_SLOTS_X_LOCATIONS;
	
	private int OOS_COND3_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV;
	private int OOS_COND3_ITEM_COMBINED_FORECAST;
	private int OOS_COND3_LIG_NO_OF_UNITS;
	private int OOS_COND3_LIG_COMBINED_FORECAST;	
	private int OOS_COND3_LIG_TRX_BASED_EXP_OF_TIME_SLOTS;
	private int OOS_COND3_LIG_NO_OF_UNITS_X_LOCATIONS;
	private int OOS_COND3_LIG_COMBINED_FORECAST_X_LOCATIONS;
	private int OOS_COND3_LIG_TRX_BASED_EXP_OF_TIME_SLOTS_X_LOCATIONS;
	
	private int OOS_COND2_COND3_ITEM_TRX_BASED_EXP_OF_TIME_SLOTS;
	
	private int OOS_COND4_ITEM_MIN_EXP;
	private int OOS_COND4_ITEM_TIME_SLOT_MIN_EXP;
	private int OOS_COND4_LIG_ACT_MOV;
	private int OOS_COND4_LIG_MIN_EXP;
	private int OOS_COND4_LIG_TIME_SLOT_ACT_MOV;
	private int OOS_COND4_LIG_TIME_SLOT_MIN_EXP;
	private int OOS_COND4_NO_OF_TIME_SLOT;
	
//	private int locationId = 0, locationLevelId = 0;
	private List<Integer> preDayPartId = new ArrayList<Integer>();
//	private HashMap<OOSAlertItemKey, OOSItemDTO> prevTimeSlotItems;
//	private HashMap<OOSAlertItemKey, OOSItemDTO> prevDaySameTimeSlotItems;
	private HashMap<Integer, HashMap<ProductKey, OOSExpectationDTO>> itemAndLigTransactionDetail;
	private int processingCalendarId = 0, processingDayId = 0, processingDayPartId = 0, prevDayCalendarId =0, prevDayId = 0;
	List<DayPartLookupDTO> dayPartLookupDTO = null;
	DayPartLookupDTO processingDayPartLookup = null;
	HashSet<Integer> persisablesDepartments = null;
	HashMap<ProductKey, List<OOSItemDTO>> itemAndItsMovements = null;
	HashMap<ProductKey, Double> storeLevelAvgMovMap = null;
	HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartLevelAvgMovMap = null;
	HashMap<OOSDayPartDetailKey, OOSDayPartDetail> prevAndProcDayPartDetail = null;
	
	public OOSCriteria(int dayCalendarId, int dayId, int dayPartId, int prevDayCalendarId, int prevDayId, DayPartLookupDTO processingDayPartLookup,
			List<DayPartLookupDTO> dayPartLookupDTO, HashSet<Integer> persisablesDepartments, HashMap<ProductKey, Double> storeLevelAvgMovMap,
			HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartLevelAvgMovMap,
			HashMap<OOSDayPartDetailKey, OOSDayPartDetail> prevAndProcDayPartDetail,
			HashMap<Integer, HashMap<ProductKey, OOSExpectationDTO>> itemAndLigTransactionDetail,
			HashMap<ProductKey, List<OOSItemDTO>> itemAndItsMovements) {

		this.OOS_LIG_UNIT_SALE_PRICE = Double.parseDouble(PropertyManager.getProperty("OOS_LIG_UNIT_SALE_PRICE"));
		this.OOS_ITEM_PREV_DAY_TRANSACTION = Integer.parseInt(PropertyManager.getProperty("OOS_ITEM_PREV_DAY_TRANSACTION"));
		this.OOS_LIG_PREV_DAY_TRANSACTION = Integer.parseInt(PropertyManager.getProperty("OOS_LIG_PREV_DAY_TRANSACTION"));
//		this.OOS_ITEM_NO_OF_TIMES_MOVED_IN_X_WEEKS = Integer.parseInt(PropertyManager.getProperty("OOS_ITEM_NO_OF_TIMES_MOVED_IN_X_WEEKS"));
		this.OOS_ITEM_LOWER_CONFIDENCE = Integer.parseInt(PropertyManager.getProperty("OOS_ITEM_LOWER_CONFIDENCE"));
//		this.OOS_ITEM_DAY_PART_EXPECTED_MOV = Integer.parseInt(PropertyManager.getProperty("OOS_ITEM_DAY_PART_EXPECTED_MOV"));
//		this.OOS_LIG_NO_OF_TIMES_MOVED_IN_X_WEEKS = Integer.parseInt(PropertyManager.getProperty("OOS_LIG_NO_OF_TIMES_MOVED_IN_X_WEEKS"));
		this.OOS_LIG_LOWER_CONFIDENCE = Integer.parseInt(PropertyManager.getProperty("OOS_LIG_LOWER_CONFIDENCE"));
//		this.OOS_LIG_DAY_PART_EXPECTED_MOV = Integer.parseInt(PropertyManager.getProperty("OOS_LIG_DAY_PART_EXPECTED_MOV"));
		this.OOS_ITEM_TRANS_BASED_EXP_MOV = Integer.parseInt(PropertyManager.getProperty("OOS_ITEM_TRANS_BASED_EXP_MOV")); // 3
		this.OOS_LIG_TRANS_BASED_EXP_MOV = Integer.parseInt(PropertyManager.getProperty("OOS_LIG_TRANS_BASED_EXP_MOV")); // 7
		this.OOS_COND2_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV = Integer
				.parseInt(PropertyManager.getProperty("OOS_COND2_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV")); // 2
		this.OOS_COND2_ITEM_LOWER_CONFIDENCE = Integer.parseInt(PropertyManager.getProperty("OOS_COND2_LOWER_CONFIDENCE")); // 2
		this.OOS_COND3_ITEM_COMBINED_FORECAST = Integer.parseInt(PropertyManager.getProperty("OOS_COND3_COMBINED_FORECAST"));
		this.OOS_COND3_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV = Integer
				.parseInt(PropertyManager.getProperty("OOS_COND3_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV")); // 3
//		this.OOS_COND5_NO_OF_UNITS = Integer.parseInt(PropertyManager.getProperty("OOS_COND5_MIN_MOV")); // 2;
//		this.OOS_COND5_NO_OF_TIME_SLOT = Integer.parseInt(PropertyManager.getProperty("OOS_COND5_NO_OF_PREVIOUS_TIME_SLOTS")); // 2;
		this.OOS_TOP_ITEMS = Integer.parseInt(PropertyManager.getProperty("OOS_TOP_ITEMS"));
		this.OOS_REPORT_LAST_X_WEEKS = Integer.parseInt(PropertyManager.getProperty("OOS_REPORT_LAST_X_WEEKS"));
//		this.OOS_ITEM_ACT_MOV_PREV_TIME_SLOT = Integer.parseInt(PropertyManager.getProperty("OOS_ITEM_ACT_MOV_PREV_TIME_SLOT"));
//		this.OOS_LIG_ACT_MOV_PREV_TIME_SLOT = Integer.parseInt(PropertyManager.getProperty("OOS_LIG_ACT_MOV_PREV_TIME_SLOT"));
		this.OOS_COND2_COND3_ITEM_TRX_BASED_EXP_OF_TIME_SLOTS = Integer.parseInt(PropertyManager.getProperty("OOS_ITEM_TRX_BASED_EXP_OF_TIME_SLOTS"));
		this.OOS_SHELF_CAPACITY_PCT = Integer.parseInt(PropertyManager.getProperty("OOS_SHELF_CAPACITY_PCT"));
		this.OOS_MIN_SHELF_CAPACITY_PCT_ITEM_MOV = Integer.parseInt(PropertyManager.getProperty("OOS_MIN_SHELF_CAPACITY_PCT_ITEM_MOV"));
		this.OOS_COND2_LIG_NO_OF_UNITS = Integer.parseInt(PropertyManager.getProperty("OOS_COND2_LIG_NO_OF_UNITS"));
		this.OOS_COND2_LIG_LOWER_CONFIDENCE = Integer.parseInt(PropertyManager.getProperty("OOS_COND2_LIG_LOWER_CONFIDENCE"));
		this.OOS_COND2_LIG_TRX_BASED_EXP_OF_TIME_SLOTS = Integer.parseInt(PropertyManager.getProperty("OOS_COND2_LIG_TRX_BASED_EXP_OF_TIME_SLOTS"));
		this.OOS_COND2_LIG_NO_OF_UNITS_X_LOCATIONS = Integer.parseInt(PropertyManager.getProperty("OOS_COND2_LIG_NO_OF_UNITS_X_LOCATIONS"));//32
		this.OOS_COND2_LIG_LOWER_CONFIDENCE_X_LOCATIONS = Integer.parseInt(PropertyManager.getProperty("OOS_COND2_LIG_LOWER_CONFIDENCE_X_LOCATIONS"));//33
		this.OOS_COND2_LIG_TRX_BASED_EXP_OF_TIME_SLOTS_X_LOCATIONS = Integer
				.parseInt(PropertyManager.getProperty("OOS_COND2_LIG_TRX_BASED_EXP_OF_TIME_SLOTS_X_LOCATIONS")); //34
		this.OOS_COND3_LIG_NO_OF_UNITS = Integer.parseInt(PropertyManager.getProperty("OOS_COND3_LIG_NO_OF_UNITS"));
		this.OOS_COND3_LIG_COMBINED_FORECAST = Integer.parseInt(PropertyManager.getProperty("OOS_COND3_LIG_COMBINED_FORECAST"));
		this.OOS_COND3_LIG_TRX_BASED_EXP_OF_TIME_SLOTS = Integer.parseInt(PropertyManager.getProperty("OOS_COND3_LIG_TRX_BASED_EXP_OF_TIME_SLOTS"));
		this.OOS_COND3_LIG_NO_OF_UNITS_X_LOCATIONS = Integer.parseInt(PropertyManager.getProperty("OOS_COND3_LIG_NO_OF_UNITS_X_LOCATIONS"));//35
		this.OOS_COND3_LIG_COMBINED_FORECAST_X_LOCATIONS = Integer
				.parseInt(PropertyManager.getProperty("OOS_COND3_LIG_COMBINED_FORECAST_X_LOCATIONS"));//36
		this.OOS_COND3_LIG_TRX_BASED_EXP_OF_TIME_SLOTS_X_LOCATIONS = Integer
				.parseInt(PropertyManager.getProperty("OOS_COND3_LIG_TRX_BASED_EXP_OF_TIME_SLOTS_X_LOCATIONS"));//37
		this.OOS_COND4_ITEM_MIN_EXP = Integer.parseInt(PropertyManager.getProperty("OOS_COND4_ITEM_MIN_EXP")); //38
		this.OOS_COND4_ITEM_TIME_SLOT_MIN_EXP = Integer.parseInt(PropertyManager.getProperty("OOS_COND4_ITEM_TIME_SLOT_MIN_EXP"));//40
		this.OOS_COND4_LIG_ACT_MOV = Integer.parseInt(PropertyManager.getProperty("OOS_COND4_LIG_ACT_MOV")); //41
		this.OOS_COND4_LIG_MIN_EXP = Integer.parseInt(PropertyManager.getProperty("OOS_COND4_LIG_MIN_EXP")); //42
		this.OOS_COND4_LIG_TIME_SLOT_ACT_MOV = Integer.parseInt(PropertyManager.getProperty("OOS_COND4_LIG_TIME_SLOT_ACT_MOV")); //43
		this.OOS_COND4_LIG_TIME_SLOT_MIN_EXP = Integer.parseInt(PropertyManager.getProperty("OOS_COND4_LIG_TIME_SLOT_MIN_EXP")); //44
		this.OOS_COND4_NO_OF_TIME_SLOT = Integer.parseInt(PropertyManager.getProperty("OOS_COND4_NO_OF_TIME_SLOT"));//39
		
//		this.locationLevelId = locationLevelId;
//		this.locationId = locationId;
		this.processingCalendarId = dayCalendarId;
		this.processingDayId = dayId;
		this.processingDayPartId = dayPartId;		
		this.prevDayCalendarId = prevDayCalendarId;
		this.prevDayId = prevDayId;
		this.persisablesDepartments = persisablesDepartments;
		this.storeLevelAvgMovMap = storeLevelAvgMovMap;
		this.dayPartLevelAvgMovMap = dayPartLevelAvgMovMap;
		this.prevAndProcDayPartDetail = prevAndProcDayPartDetail;
		this.itemAndLigTransactionDetail = itemAndLigTransactionDetail;
		this.itemAndItsMovements = itemAndItsMovements;
		this.dayPartLookupDTO = dayPartLookupDTO;
		this.processingDayPartLookup = processingDayPartLookup;
		// Get all previous time slots
		for (DayPartLookupDTO dpl : dayPartLookupDTO) {
			if (dpl.getOrder() < processingDayPartLookup.getOrder()) {
				preDayPartId.add(dpl.getDayPartId());
			}
		}
		Collections.sort(preDayPartId, Collections.reverseOrder());
		logger.debug("processingCalendarId:" + this.processingCalendarId + ",:" + ",processingDayId:" + this.processingDayId + ",processingDayPartId:"
				+ this.processingDayPartId + ",prevDayCalendarId:" + this.prevDayCalendarId + ",persisablesDepartments.size():"
				+ this.persisablesDepartments.size() + ",prevAndProcDayPartDetail.size(): " + this.prevAndProcDayPartDetail.size()
				+ ",storeLevelAvgMovMap.size(): " + this.storeLevelAvgMovMap.size() + ",dayPartLevelAvgMovMap.size(): "
				+ this.dayPartLevelAvgMovMap.size() + ",itemAndLigTransactionDetail.size(): " + this.itemAndLigTransactionDetail.size()
				+ ",itemAndItsMovements.size(): " + this.itemAndItsMovements.size());
	}
	
	public void findOutOfStockItems(List<OOSItemDTO> allCandidateItems) {
		HashSet<Integer> distinctRetLirId = new HashSet<Integer>();
		List<OOSItemDTO> ligItems = new ArrayList<OOSItemDTO>();
		
		//TODO:: put below lines in other program
		//Needs lot of thinking as including the lig in authorized item list
		//will have impact everywhere
		//Add lig's to list
		for (OOSItemDTO oosItemDTO : allCandidateItems) {
			boolean isLirNotAlreadyAdded = false;
			if (oosItemDTO.getRetLirId() > 0)
				isLirNotAlreadyAdded = distinctRetLirId.add(oosItemDTO.getRetLirId());

			// only for first time
			if (isLirNotAlreadyAdded) {
				// Create lig level data
				OOSItemDTO lig = new OOSItemDTO();
				copyToLig(lig, oosItemDTO);
				ligItems.add(lig);
			}
		}
		
		
		//All non-lig items send for oos analysis
		for (OOSItemDTO oosItemDTO : allCandidateItems) {
			if (oosItemDTO.getRetLirId() == 0)
				oosItemDTO.setFindOOS(true);
		}
		//Find which lig need to be analysis for oos
		//If the item is not part of LIG or (it is part of LIG and its unit sale retail >= $2.01)
		for (OOSItemDTO ligItem : ligItems) {
			double unitSalePrice = PRCommonUtil.getUnitPrice(ligItem.getSalePrice(), true);
			if (unitSalePrice >= OOS_LIG_UNIT_SALE_PRICE) {
				// Do at lig member level
				ligItem.setFindOOS(false);
			} else {
				ligItem.setFindOOS(true);
			}
			
			//If oos to be find at lig, then do at lig members level
			//if oos to be find at lig member, then do at lig level
			for (OOSItemDTO oosItemDTO : allCandidateItems) {
				if(oosItemDTO.getRetLirId() == ligItem.getRetLirId()) {
					if(ligItem.isFindOOS()){
						oosItemDTO.setFindOOS(false);
					} else {
						oosItemDTO.setFindOOS(true);
					}
				}
			}
		}
		//add only lig's for which oos is going to be found at lig level
		for (OOSItemDTO ligItem : ligItems) {
			if (ligItem.isFindOOS())
				allCandidateItems.add(ligItem);
		}
		
		
		logger.debug("Total No of OOS Candidate Items: " + allCandidateItems.size());
		int totalOOSItems = 0;
		
		// Fill all supportive data of all non-lig & lig members
		for (OOSItemDTO oosItemDTO : allCandidateItems) {
			if (oosItemDTO.getProductLevelId() == Constants.ITEMLEVELID)
				getAllSupportiveData(allCandidateItems, oosItemDTO);
		}
		
		// Fill all supportive data of lig
		for (OOSItemDTO oosItemDTO : allCandidateItems) {
			if (oosItemDTO.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG)
				getAllSupportiveData(allCandidateItems, oosItemDTO);
		}
		
		// Loop each item
		for (OOSItemDTO oosItemDTO : allCandidateItems) {
			logger.debug("****************");
			boolean isItemOutOfStock = false;
			// Write log
			writeOOSSupportiveData(oosItemDTO);
			//Not all the lig/ lig members need to be analysis
			if (oosItemDTO.isFindOOS()) {
				isItemOutOfStock = isItemOutOfStock(allCandidateItems, oosItemDTO);
				// Add to out of stock list
				if (isItemOutOfStock) {
					oosItemDTO.setIsOutOfStockItem(true);
					totalOOSItems = totalOOSItems + 1;
				}
			}
			logger.debug("****************");
		}
		findTopXItems(totalOOSItems, allCandidateItems);
	}
	
	private void copyToLig(OOSItemDTO ligItem, OOSItemDTO item){
		ligItem.setProductLevelId(Constants.PRODUCT_LEVEL_ID_LIG);
		ligItem.setProductId(item.getRetLirId());
		ligItem.setLocationLevelId(item.getLocationLevelId());
		ligItem.setLocationId(item.getLocationId());
		ligItem.setCalendarId(item.getCalendarId());
		ligItem.setDayPartId(item.getDayPartId());
		ligItem.setRegPrice(item.getRegPrice());
		ligItem.setSalePrice(item.getSalePrice());
		ligItem.setRetLirId(item.getRetLirId());
		ligItem.setAdPageNo(item.getAdPageNo());
		ligItem.setDisplayTypeId(item.getDisplayTypeId());
		ligItem.setBlockNo(item.getBlockNo());
	}
	
	private void findTopXItems(int totalOOSItems, List<OOSItemDTO> allCandidateItems) {
		// Sort by expected movement in descending order
		Collections.sort(allCandidateItems, new Comparator<OOSItemDTO>() {
			public int compare(OOSItemDTO a, OOSItemDTO b) {
				if (a.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig() < b.getOOSCriteriaData()
						.getForecastMovOfProcessingTimeSlotOfItemOrLig())
					return 1;
				if (a.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig() > b.getOOSCriteriaData()
						.getForecastMovOfProcessingTimeSlotOfItemOrLig())
					return -1;
				return 0;
			}
		});

		// get only top 25 items
		if (totalOOSItems > OOS_TOP_ITEMS) {
			int topItemCount = 1;
			for (OOSItemDTO oosItemDTO : allCandidateItems) {
				if (oosItemDTO.getIsOutOfStockItem()) {
					// Don't count criteria 6 - Potential OOS (always include
					// that)
					if (topItemCount <= OOS_TOP_ITEMS || oosItemDTO.getOOSCriteriaId() == OOSCriteriaLookup.CRITERIA_6.getOOSCriteriaId()) {
						oosItemDTO.setIsSendToClient(true);
						if (oosItemDTO.getOOSCriteriaId() != OOSCriteriaLookup.CRITERIA_6.getOOSCriteriaId())
							topItemCount = topItemCount + 1;
					}
				}
			}
		} else {
			for (OOSItemDTO oosItemDTO : allCandidateItems) {
				if (oosItemDTO.getIsOutOfStockItem())
					oosItemDTO.setIsSendToClient(true);
			}
		}
	}

	private boolean isItemOutOfStock(List<OOSItemDTO> allCandidateItems, OOSItemDTO oosCandidateItem) {
		boolean isItemOutOfStock = false;
		//Check if primary condition is meet
		int minPrevDaysTrxCnt = 0;
		if (oosCandidateItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
			minPrevDaysTrxCnt = (processingDayId - 1) * OOS_LIG_PREV_DAY_TRANSACTION; 
		} else {
			minPrevDaysTrxCnt = (processingDayId - 1) * OOS_ITEM_PREV_DAY_TRANSACTION;
		}
		logger.debug("PrimaryCondition(ActualVsExp)--" + "1.TrxCntOfPrevDaysOfItemOrLig: "
				+ oosCandidateItem.getOOSCriteriaData().getTrxCntOfPrevDaysOfItemOrLig() + ">" + minPrevDaysTrxCnt);
		
		if (oosCandidateItem.getOOSCriteriaData().getTrxCntOfPrevDaysOfItemOrLig() > minPrevDaysTrxCnt) {
			// Criteria's
			if (isSatisfiesCriteria1(oosCandidateItem)) {
				isItemOutOfStock = true;
			} else if (isSatisfiesCriteria2(oosCandidateItem)) {
				isItemOutOfStock = true;
			} else if (isSatisfiesCriteria3(oosCandidateItem)) {
				isItemOutOfStock = true;
			} /*else if (isSatisfiesCriteria4(oosCandidateItem)) {
				isItemOutOfStock = true;
			} else if (isSatisfiesCriteria5(oosCandidateItem)) {
				isItemOutOfStock = true;
			} */
		} else {
			logger.debug("Not enough transaction on previous days");
		}
		
		if (isSatisfiesCriteria4(allCandidateItems, oosCandidateItem) && isItemOutOfStock == false) {
			isItemOutOfStock = true;
		} else if (isPotentialOOS(oosCandidateItem) && isItemOutOfStock == false) {
			// Check for potential oos
			isItemOutOfStock = true;
		}
		
		return isItemOutOfStock;
	}
	
	//TODO:: this function needs re-factoring between non-lig and lig items
	private void getAllSupportiveData(List<OOSItemDTO> allCandidateItems, OOSItemDTO oosCandidateItem) {
		OOSDayPartDetailKey oosDayPartDetailKey;
		OOSService oosService = new OOSService();
		//Note:: don't change the order of lines
		
		//Set shelf capacity
		oosCandidateItem.getOOSCriteriaData().setShelfCapacityOfItemOrLig(oosCandidateItem.getShelfCapacity());
		oosCandidateItem.getOOSCriteriaData().setNoOfShelfLocationsOfItemOrLig(oosCandidateItem.getNoOfShelfLocations());
		
		//Set Actual Movement of processing time slot
		oosDayPartDetailKey = new OOSDayPartDetailKey(processingCalendarId, processingDayPartId, oosCandidateItem.getProductLevelId(),
				oosCandidateItem.getProductId());
		if (prevAndProcDayPartDetail.get(oosDayPartDetailKey) != null) {
			oosCandidateItem.getOOSCriteriaData()
					.setActualMovOfProcessingTimeSlotOfItemOrLig(prevAndProcDayPartDetail.get(oosDayPartDetailKey).getActualMovement());
		}
		 
		// Set Store transaction count of processing time slot
		oosDayPartDetailKey = new OOSDayPartDetailKey(processingCalendarId, processingDayPartId, 0, 0);
		if (prevAndProcDayPartDetail.get(oosDayPartDetailKey) != null)
			oosCandidateItem.getOOSCriteriaData()
					.setTrxCntOfProcessingTimeSlotOfStore(prevAndProcDayPartDetail.get(oosDayPartDetailKey).getTransactionCount());
		
		//Set perishable flag
		if (oosCandidateItem.getDistFlag() == Constants.DSD || persisablesDepartments.contains(oosCandidateItem.getDeptProductId()))
			oosCandidateItem.setPersishableOrDSD(true);
		else
			oosCandidateItem.setPersishableOrDSD(false);
		
		//Set store and day part average
		ProductKey productKey = new ProductKey(oosCandidateItem.getProductLevelId(), oosCandidateItem.getProductId());
		OOSDayPartItemAvgMovKey oosDayPartItemAvgMovKey = new OOSDayPartItemAvgMovKey(processingDayId, processingDayPartId,
				oosCandidateItem.getProductLevelId(), oosCandidateItem.getProductId());
		if (storeLevelAvgMovMap.get(productKey) != null)
			oosCandidateItem.setStoreLevelAvgMov(storeLevelAvgMovMap.get(productKey));
		if (dayPartLevelAvgMovMap.get(oosDayPartItemAvgMovKey) != null)
			oosCandidateItem.setDayPartLevelAvgMov(dayPartLevelAvgMovMap.get(oosDayPartItemAvgMovKey).getAverageMovement());
		
		//Find day part average movement
		oosService.findDayPartPrediction(oosCandidateItem, storeLevelAvgMovMap, dayPartLevelAvgMovMap, oosCandidateItem.isPersishableOrDSD(),
				dayPartLookupDTO, processingDayId, processingDayPartId);
		
		//Fill Previous days Transaction Detail
		setPrevDaysTrxDetailOfItemOrLig(oosCandidateItem);
		
		// Fill noOfConsecutiveTimeSlotDidNotMove
		setNoOfConsecutiveTimeSlotDidNotMove(oosCandidateItem);
		
		setTransBasedExpectationOfItemOrLig(oosCandidateItem);
		
		// Get combined day part forecast for the previous 3 time slots
		// including current time slot
		findCombinedTimeSlotData(oosCandidateItem, OOS_COND2_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV, "CRITERIA_2");
		findCombinedTimeSlotData(oosCandidateItem, OOS_COND3_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV, "CRITERIA_3");
		
		// Actual and day part forecast of immediate and all previous time slot
		setPreviousTimeSlotsDataOfItemOrLig(allCandidateItems, oosCandidateItem);
		// isItemMovedMoreThan2UnitsInEach2PrevTimeSlot 
//		setIfItemMovedMoreThanXUnitsInEachXPrevTimeSlot(oosCandidateItem, OOS_COND5_NO_OF_UNITS, OOS_COND5_NO_OF_TIME_SLOT);
		// Find unit sale price
		double unitSalePrice = PRCommonUtil.getUnitPrice(oosCandidateItem.getSalePrice(), true);
		oosCandidateItem.getOOSCriteriaData().setUnitSalePrice(unitSalePrice);
		
		//Set actual, forecast, lower confidence of lig
		setLIGData(allCandidateItems, oosCandidateItem);
		// Set additional info like # of zero mov occur, min mov, max mov, avg mov 	
		setAdditionalLigInfo(allCandidateItems, oosCandidateItem);
//		setPrevDaySameTimeSlotDataOfItemOrLig(allCandidateItems, oosCandidateItem);
		
		oosCandidateItem.getOOSCriteriaData().setShelfCapacityXPCTOfItemOrLig(getShelfCapacity(oosCandidateItem));
		// Actual mov of the day
		oosCandidateItem.getOOSCriteriaData()
				.setActualMovOfProcessingDayItemOrLig(oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig()
						+ oosCandidateItem.getOOSCriteriaData().getActualMovOfAllPrevTimeSlotsOfItemOrLig());
	}
	
	private void writeOOSSupportiveData(OOSItemDTO oosCandidateItem) {
		//if (oosCandidateItem.isFindOOS()) {
		logger.debug("ProductLevelId: " + oosCandidateItem.getProductLevelId() + ",ProductId: " + oosCandidateItem.getProductId() + ",RetLirId: "
				+ oosCandidateItem.getRetLirId() + ",UnitSalePrice: " + oosCandidateItem.getOOSCriteriaData().getUnitSalePrice() + ",isFindOOS: "
				+ oosCandidateItem.isFindOOS() + ",isPersishableOrDSD:" + oosCandidateItem.isPersishableOrDSD() + ",StoreLevelAvgMov:"
				+ oosCandidateItem.getStoreLevelAvgMov() + ",DayPartLevelAvgMov:" + oosCandidateItem.getDayPartLevelAvgMov() + ",DayPartMovPercent:"
				+ oosCandidateItem.getDayPartMovPercent() + ",WeeklyPredictedMovement:"
				+ (oosCandidateItem.isPersishableOrDSD() ? oosCandidateItem.getClientWeeklyPredictedMovement()
						: oosCandidateItem.getWeeklyPredictedMovement())
				+ ",WeeklyConfidenceLevelLower :"
				+ (oosCandidateItem.isPersishableOrDSD() ? oosCandidateItem.getClientWeeklyPredictedMovement()
						: oosCandidateItem.getWeeklyConfidenceLevelLower())
				+ " ,ForecastMovOfProcessingTimeSlotOfLig: " + oosCandidateItem.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig()
				+ " ,LowerConfidenceOfProcessingTimeSlotOfItemOrLig: "
				+ oosCandidateItem.getOOSCriteriaData().getLowerConfidenceOfProcessingTimeSlotOfItemOrLig() + ",ShelfCapacityOfItemOrLig : "
				+ oosCandidateItem.getOOSCriteriaData().getShelfCapacityOfItemOrLig() + ",X % of ShelfCapacityOfItemOrLig : "
				+ oosCandidateItem.getOOSCriteriaData().getShelfCapacityXPCTOfItemOrLig() + " NoOfShelfLocationsOfItemOrLig:"
				+ oosCandidateItem.getOOSCriteriaData().getNoOfShelfLocationsOfItemOrLig() + " ,ActualMovOfProcessingTimeSlotOfItemOrLig: "
				+ oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig()
				+ ",TrxBasedPredOfProcessingTimeSlotOfItemOrLig: "
				+ oosCandidateItem.getOOSCriteriaData().getTrxBasedPredOfProcessingTimeSlotOfItemOrLig() + ",NoOfConsecutiveTimeSlotDidNotMove:"
				+ (oosCandidateItem.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() != null
						? oosCandidateItem.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() : "")
				+ ",CombinedXTimeSlotLowerConf:" + oosCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotLowerConfOfItemOrLig()
				+ ",CombinedTimeSlotActualMovCriteria2:" + oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotActualMovCriteria2OfItemOrLig()
				+ ",CombinedTimeSlotTrxBasedExpCriteria2:"
				+ oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig() + ",CombinedXTimeSlotForecast:"
				+ oosCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotForecastOfItemOrLig() + ",CombinedTimeSlotActualMovCriteria3:"
				+ oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotActualMovCriteria3OfItemOrLig() + ",CombinedTimeSlotTrxBasedExpCriteria3:"
				+ oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig()
				+ ",ActualMovOfAllPrevTimeSlotsOfItemOrLig:" + oosCandidateItem.getOOSCriteriaData().getActualMovOfAllPrevTimeSlotsOfItemOrLig()
				+ ",ActualMovOfProcessingDayItemOrLig:" + oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingDayItemOrLig()
				+ ",ForecastMovOfRestOfTheDayOfItemOrLig:" + oosCandidateItem.getOOSCriteriaData().getForecastMovOfRestOfTheDayOfItemOrLig());
	}
	
	private boolean isSatisfiesCriteria1(OOSItemDTO oosCandidateItem) {
		boolean satisfiesCriteria = false;
		int transBasedExp, confidenceLevelLower;
		
		if (oosCandidateItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
			confidenceLevelLower = OOS_LIG_LOWER_CONFIDENCE;
			transBasedExp = OOS_LIG_TRANS_BASED_EXP_MOV;
		} else {
			confidenceLevelLower = OOS_ITEM_LOWER_CONFIDENCE;
			transBasedExp = OOS_ITEM_TRANS_BASED_EXP_MOV;
		}
		if (oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingDayItemOrLig() >= oosCandidateItem.getOOSCriteriaData()
				.getShelfCapacityXPCTOfItemOrLig()
				&& oosCandidateItem.getOOSCriteriaData().getLowerConfidenceOfProcessingTimeSlotOfItemOrLig() >= confidenceLevelLower
				&& oosCandidateItem.getOOSCriteriaData().getTrxBasedPredOfProcessingTimeSlotOfItemOrLig() >= transBasedExp
				&& criteria1LigOrNonLigCondition(oosCandidateItem)) {
			oosCandidateItem.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_1.getOOSCriteriaId());
			satisfiesCriteria = true;
		}

		logger.debug("Criteria 1(ExpVsActual)-- " + " 1.ActualMovOfProcessingDayItemOrLig() >= "
				+ oosCandidateItem.getOOSCriteriaData().getShelfCapacityXPCTOfItemOrLig() + ":"
				+ oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingDayItemOrLig() + ">=" 
				+ oosCandidateItem.getOOSCriteriaData().getShelfCapacityXPCTOfItemOrLig()
				+ ",2.LowerConfidenceOfProcessingTimeSlotOfItemOrLig >= " + confidenceLevelLower + ":"
				+ oosCandidateItem.getOOSCriteriaData().getLowerConfidenceOfProcessingTimeSlotOfItemOrLig() + ">=" + confidenceLevelLower
				+ ",3.TrxBasedPredOfProcessingTimeSlotOfItemOrLig >= " + transBasedExp + ":"
				+ oosCandidateItem.getOOSCriteriaData().getTrxBasedPredOfProcessingTimeSlotOfItemOrLig() + ">=" + transBasedExp);

		return satisfiesCriteria;
	}

	/**
	 * @param oosCandidateItem
	 * @return
	 */
	private boolean isSatisfiesCriteria2(OOSItemDTO oosCandidateItem) {
		boolean satisfiesCriteria = false;
		String debugLog = "Criteria 2(ExpVsActual)--";
		int oosCond2LigNoOfUnits = 0, oosCond2LigLowerConfidence = 0, oosCond2LigTrxBasedExpOfTimeSlots = 0;
		if (preDayPartId.size() >= OOS_COND2_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV - 1) {
			if (oosCandidateItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
				if (oosCandidateItem.getOOSCriteriaData().getNoOfShelfLocationsOfItemOrLig() > 1) {
					// If the LIG displayed at multiple locations
					oosCond2LigNoOfUnits = OOS_COND2_LIG_NO_OF_UNITS_X_LOCATIONS;
					oosCond2LigLowerConfidence = OOS_COND2_LIG_LOWER_CONFIDENCE_X_LOCATIONS;
					oosCond2LigTrxBasedExpOfTimeSlots = OOS_COND2_LIG_TRX_BASED_EXP_OF_TIME_SLOTS_X_LOCATIONS;
				} else {
					// If the LIG displayed at one location
					oosCond2LigNoOfUnits = OOS_COND2_LIG_NO_OF_UNITS;
					oosCond2LigLowerConfidence = OOS_COND2_LIG_LOWER_CONFIDENCE;
					oosCond2LigTrxBasedExpOfTimeSlots = OOS_COND2_LIG_TRX_BASED_EXP_OF_TIME_SLOTS;
				}
				if (oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotActualMovCriteria2OfItemOrLig() < oosCond2LigNoOfUnits
						&& oosCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotLowerConfOfItemOrLig() >= oosCond2LigLowerConfidence
						&& oosCandidateItem.getOOSCriteriaData()
								.getCombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig() >= oosCond2LigTrxBasedExpOfTimeSlots) {
					logger.debug("Criteria 2 is satisfied");
					oosCandidateItem.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_2.getOOSCriteriaId());
					satisfiesCriteria = true;
				}
				debugLog = debugLog + "1.CombinedTimeSlotActualMovCriteria2OfItemOrLig < " + oosCond2LigNoOfUnits + ":";
				debugLog = debugLog + oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotActualMovCriteria2OfItemOrLig() + "<"
						+ oosCond2LigNoOfUnits;
				debugLog = debugLog + ",2.CombinedXTimeSlotLowerConfOfItemOrLig >= " + oosCond2LigNoOfUnits + ":";
				debugLog = debugLog + oosCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotLowerConfOfItemOrLig() + ">="
						+ oosCond2LigLowerConfidence;
				debugLog = debugLog + ",3.CombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig >= " + oosCond2LigTrxBasedExpOfTimeSlots + ":";
				debugLog = debugLog + oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig() + ">="
						+ oosCond2LigTrxBasedExpOfTimeSlots;
			} else {
				if (oosCandidateItem.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() != null
						&& oosCandidateItem.getOOSCriteriaData()
								.getNoOfConsecutiveTimeSlotItemDidNotMove() >= OOS_COND2_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV
						&& oosCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotLowerConfOfItemOrLig() >= OOS_COND2_ITEM_LOWER_CONFIDENCE
						&& oosCandidateItem.getOOSCriteriaData()
								.getCombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig() >= OOS_COND2_COND3_ITEM_TRX_BASED_EXP_OF_TIME_SLOTS) {
					logger.debug("Criteria 2 is satisfied");
					oosCandidateItem.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_2.getOOSCriteriaId());
					satisfiesCriteria = true;
				}

				debugLog = debugLog + "1.NoOfConsecutiveTimeSlotDidNotMove >= " + OOS_COND2_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV + ":"
						+ (oosCandidateItem.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() != null
								? oosCandidateItem.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() : "null")
						+ ">=" + OOS_COND2_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV;
				debugLog = debugLog + ",2.CombinedXTimeSlotLowerConfOfItemOrLig >= " + OOS_COND2_ITEM_LOWER_CONFIDENCE + ":";
				debugLog = debugLog + oosCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotLowerConfOfItemOrLig() + ">="
						+ OOS_COND2_ITEM_LOWER_CONFIDENCE;
				debugLog = debugLog + ",3.CombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig >= " + OOS_COND2_COND3_ITEM_TRX_BASED_EXP_OF_TIME_SLOTS
						+ ":";
				debugLog = debugLog + oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig() + ">="
						+ OOS_COND2_COND3_ITEM_TRX_BASED_EXP_OF_TIME_SLOTS;
			}
		}
		logger.debug(debugLog);
		return satisfiesCriteria;
	}
	
	/**
	 * @param oosCandidateItem
	 * @return
	 */
	private boolean isSatisfiesCriteria3(OOSItemDTO oosCandidateItem) {
		boolean satisfiesCriteria = false;
		String debugLog = "Criteria 3(ExpVsActual)--";
		//Criteria 3 is applied only from third time slot

		if (preDayPartId.size() >= OOS_COND3_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV - 1) {
			if (oosCandidateItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
				int oosCond3LigNoOfUnits = 0, oosCond3LigCombinedForecast = 0, oosCond3LigTrxBasedExpOfTimeSlots = 0;
				if (oosCandidateItem.getOOSCriteriaData().getNoOfShelfLocationsOfItemOrLig() > 1) {
					// If the LIG displayed at multiple locations
					oosCond3LigNoOfUnits = OOS_COND3_LIG_NO_OF_UNITS_X_LOCATIONS;
					oosCond3LigCombinedForecast = OOS_COND3_LIG_COMBINED_FORECAST_X_LOCATIONS;
					oosCond3LigTrxBasedExpOfTimeSlots = OOS_COND3_LIG_TRX_BASED_EXP_OF_TIME_SLOTS_X_LOCATIONS;
				} else {
					// If the LIG displayed at one location
					oosCond3LigNoOfUnits = OOS_COND3_LIG_NO_OF_UNITS;
					oosCond3LigCombinedForecast = OOS_COND3_LIG_COMBINED_FORECAST;
					oosCond3LigTrxBasedExpOfTimeSlots = OOS_COND3_LIG_TRX_BASED_EXP_OF_TIME_SLOTS;
				}

				if (oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotActualMovCriteria3OfItemOrLig() < oosCond3LigNoOfUnits
						&& oosCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotForecastOfItemOrLig() > oosCond3LigCombinedForecast
						&& oosCandidateItem.getOOSCriteriaData()
								.getCombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig() >= oosCond3LigTrxBasedExpOfTimeSlots) {
					logger.debug("Criteria 3 is satisfied");
					oosCandidateItem.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_3.getOOSCriteriaId());
					satisfiesCriteria = true;
				}
				debugLog = debugLog + "1.CombinedTimeSlotActualMovCriteria3OfItemOrLig < " + oosCond3LigNoOfUnits + ":";
				debugLog = debugLog + oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotActualMovCriteria3OfItemOrLig() + "<"
						+ oosCond3LigNoOfUnits;
				debugLog = debugLog + ",2.CombinedXTimeSlotForecastOfItemOrLig > " + oosCond3LigCombinedForecast + ":";
				debugLog = debugLog + oosCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotForecastOfItemOrLig() + ">"
						+ oosCond3LigCombinedForecast;
				debugLog = debugLog + ",3.CombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig >= " + oosCond3LigTrxBasedExpOfTimeSlots + ":";
				debugLog = debugLog + oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig() + ">="
						+ oosCond3LigTrxBasedExpOfTimeSlots;
			} else {
				if (oosCandidateItem.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() != null
						&& oosCandidateItem.getOOSCriteriaData()
								.getNoOfConsecutiveTimeSlotItemDidNotMove() >= OOS_COND3_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV
						&& oosCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotForecastOfItemOrLig() > OOS_COND3_ITEM_COMBINED_FORECAST
						&& oosCandidateItem.getOOSCriteriaData()
								.getCombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig() >= OOS_COND2_COND3_ITEM_TRX_BASED_EXP_OF_TIME_SLOTS) {
					logger.debug("Criteria 3 is satisfied");
					oosCandidateItem.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_3.getOOSCriteriaId());
					satisfiesCriteria = true;
				}
				debugLog = debugLog + "1.NoOfConsecutiveTimeSlotDidNotMove >= " + OOS_COND3_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV + ":"
						+ (oosCandidateItem.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() != null
								? oosCandidateItem.getOOSCriteriaData().getNoOfConsecutiveTimeSlotItemDidNotMove() : "null")
						+ ">=" + OOS_COND3_NO_OF_CONSECUTIVE_TIME_SLOT_NO_MOV;
				debugLog = debugLog + ",2.CombinedXTimeSlotForecastOfItemOrLig >= " + OOS_COND3_ITEM_COMBINED_FORECAST + ":";
				debugLog = debugLog + oosCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotForecastOfItemOrLig() + ">="
						+ OOS_COND3_ITEM_COMBINED_FORECAST;
				debugLog = debugLog + ",3.CombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig >= " + OOS_COND2_COND3_ITEM_TRX_BASED_EXP_OF_TIME_SLOTS
						+ ":";
				debugLog = debugLog + oosCandidateItem.getOOSCriteriaData().getCombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig() + ">="
						+ OOS_COND2_COND3_ITEM_TRX_BASED_EXP_OF_TIME_SLOTS;
			}
		}
		logger.debug(debugLog);
		return satisfiesCriteria;
	}
	
	private boolean isSatisfiesCriteria4(List<OOSItemDTO> allCandidateItems, OOSItemDTO oosCandidateItem) {
		boolean satisfiesCriteria = false;
		long actualMovOfPrevDayLastTimeSlot = 0, lowerConfOfPrevDayLastTimeSlot = 0, trxBasedExpOfPrevDayLastTimeSlot = 0;
		long actualMovOfProcDayFirstTimeSlot = 0, lowerConfOfProcDayFirstTimeSlot = 0, trxBasedExpOfProcDayFirstTimeSlot = 0;
		long actualMovFromPrevDay = 0;
		long actualMovOfLast2TimeSlots = 0, lowerConfOfLast2TimeSlots = 0;
		String debugLog = "Criteria 4(ExpVsActual)--";
		int lastDayPartId = 0, firstDayPartId = 0;
		OOSService oosService = new OOSService();

		// Proceed only if there is previous day within the same week
		if (prevDayCalendarId > 0) {
			// Get first day part id
			for (DayPartLookupDTO dpl : dayPartLookupDTO) {
				if (dpl.getOrder() == 1) {
					firstDayPartId = dpl.getDayPartId();
					break;
				}
			}

			int order = 1;
			int maxOrder = 0;
			// Get last day part id
			for (DayPartLookupDTO dpl : dayPartLookupDTO) {
				if (order <= dpl.getOrder()) {
					lastDayPartId = dpl.getDayPartId();
					maxOrder = dpl.getOrder();
				}
				order = dpl.getOrder();
			}
			//TODO:: put it in configuration
			lastDayPartId = 4;

			if (oosCandidateItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
				// Get trx based expectation of previous day last time slot
				trxBasedExpOfPrevDayLastTimeSlot =  findTransBasedExpectationOfItemOrLig(
						oosCandidateItem.getProductLevelId(), oosCandidateItem.getProductId(), prevDayCalendarId, lastDayPartId);

				// Get trx based expectation of processing day first time slot
				trxBasedExpOfProcDayFirstTimeSlot = findTransBasedExpectationOfItemOrLig(
						oosCandidateItem.getProductLevelId(), oosCandidateItem.getProductId(), processingCalendarId, firstDayPartId);
				
				for (OOSItemDTO allCandidateItem : allCandidateItems) {
					if (allCandidateItem.getProductLevelId() == Constants.ITEMLEVELID
							&& allCandidateItem.getRetLirId() == oosCandidateItem.getRetLirId()) {

						// Get actual movement of previous day last time slot
						actualMovOfPrevDayLastTimeSlot = actualMovOfPrevDayLastTimeSlot
								+ getActualMovement(prevDayCalendarId, lastDayPartId, allCandidateItem);

						// Get actual movement of processing day first time slot
						actualMovOfProcDayFirstTimeSlot = actualMovOfProcDayFirstTimeSlot
								+ getActualMovement(processingCalendarId, firstDayPartId, allCandidateItem);

						// Get lower confidence of previous day last time slot
						lowerConfOfPrevDayLastTimeSlot = lowerConfOfPrevDayLastTimeSlot + oosService.findLowerConfidence(
								allCandidateItem.getProductLevelId(), allCandidateItem.getProductId(), storeLevelAvgMovMap, dayPartLevelAvgMovMap,
								allCandidateItem.getClientWeeklyPredictedMovement(), allCandidateItem.getWeeklyConfidenceLevelLower(),
								allCandidateItem.isPersishableOrDSD(), prevDayId, lastDayPartId);

						// Get lower confidence of processing day first time slot
						lowerConfOfProcDayFirstTimeSlot = lowerConfOfProcDayFirstTimeSlot + oosService.findLowerConfidence(
								allCandidateItem.getProductLevelId(), allCandidateItem.getProductId(), storeLevelAvgMovMap, dayPartLevelAvgMovMap,
								allCandidateItem.getClientWeeklyPredictedMovement(), allCandidateItem.getWeeklyConfidenceLevelLower(),
								allCandidateItem.isPersishableOrDSD(), processingDayId, firstDayPartId);

						

						// Get actual movement from previous day to processing day part
						actualMovFromPrevDay = actualMovFromPrevDay + getActualMovFromPrevDayToProcTimeSlot(allCandidateItem);

						// Get actual movement of last 2 consecutive time slot
						actualMovOfLast2TimeSlots = actualMovOfLast2TimeSlots + getActualMovOfLastXTimeSlot(allCandidateItem, maxOrder);

						// Get lower confidence of last 2 consecutive time slot
						lowerConfOfLast2TimeSlots = lowerConfOfLast2TimeSlots + getLowerConfOfLastXTimeSlot(allCandidateItem, maxOrder);

					}
				}
			} else {
				// Get actual movement of previous day last time slot
				actualMovOfPrevDayLastTimeSlot = getActualMovement(prevDayCalendarId, lastDayPartId, oosCandidateItem);

				// Get actual movement of processing day first time slot
				actualMovOfProcDayFirstTimeSlot = getActualMovement(processingCalendarId, firstDayPartId, oosCandidateItem);

				// Get lower confidence of previous day last time slot
				lowerConfOfPrevDayLastTimeSlot = oosService.findLowerConfidence(oosCandidateItem.getProductLevelId(), oosCandidateItem.getProductId(),
						storeLevelAvgMovMap, dayPartLevelAvgMovMap, oosCandidateItem.getClientWeeklyPredictedMovement(),
						oosCandidateItem.getWeeklyConfidenceLevelLower(), oosCandidateItem.isPersishableOrDSD(), prevDayId, lastDayPartId);

				// Get lower confidence of processing day first time slot
				lowerConfOfProcDayFirstTimeSlot = oosService.findLowerConfidence(oosCandidateItem.getProductLevelId(), oosCandidateItem.getProductId(),
						storeLevelAvgMovMap, dayPartLevelAvgMovMap, oosCandidateItem.getClientWeeklyPredictedMovement(),
						oosCandidateItem.getWeeklyConfidenceLevelLower(), oosCandidateItem.isPersishableOrDSD(), processingDayId,
						firstDayPartId);
				
				
				// Get trx based expectation of previous day last time slot
				trxBasedExpOfPrevDayLastTimeSlot = findTransBasedExpectationOfItemOrLig(oosCandidateItem.getProductLevelId(),
						oosCandidateItem.getProductId(), prevDayCalendarId, lastDayPartId);

				// Get trx based expectation of processing day first time slot
				trxBasedExpOfProcDayFirstTimeSlot = findTransBasedExpectationOfItemOrLig(oosCandidateItem.getProductLevelId(),
						oosCandidateItem.getProductId(), processingCalendarId, firstDayPartId);

				// Get actual movement from previous day to processing day part
				actualMovFromPrevDay = getActualMovFromPrevDayToProcTimeSlot(oosCandidateItem);

				// Get actual movement of last 2 consecutive time slot
				actualMovOfLast2TimeSlots = getActualMovOfLastXTimeSlot(oosCandidateItem, maxOrder);

				// Get lower confidence of last 2 consecutive time slot
				lowerConfOfLast2TimeSlots = getLowerConfOfLastXTimeSlot(oosCandidateItem, maxOrder);
			}

			if (oosCandidateItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
				debugLog = debugLog + "1.actualMovOfPrevDayLastTimeSlot < " + OOS_COND4_LIG_ACT_MOV + ":";
				debugLog = debugLog + actualMovOfPrevDayLastTimeSlot + " < " + OOS_COND4_LIG_ACT_MOV;
				debugLog = debugLog + ",2.actualMovOfProcDayFirstTimeSlot < " + OOS_COND4_LIG_ACT_MOV + ":";
				debugLog = debugLog + actualMovOfProcDayFirstTimeSlot + " < " + OOS_COND4_LIG_ACT_MOV;
				debugLog = debugLog + ",3.lowerConfOfPrevDayLastTimeSlot > " + OOS_COND4_LIG_MIN_EXP + ":";
				debugLog = debugLog + lowerConfOfPrevDayLastTimeSlot + " > " + OOS_COND4_LIG_MIN_EXP;
				debugLog = debugLog + ",4.lowerConfOfProcDayFirstTimeSlot > " + OOS_COND4_LIG_MIN_EXP + ":";
				debugLog = debugLog + lowerConfOfProcDayFirstTimeSlot + " > " + OOS_COND4_LIG_MIN_EXP;
				debugLog = debugLog + ",5.trxBasedExpOfPrevDayLastTimeSlot > " + OOS_COND4_LIG_MIN_EXP + ":";
				debugLog = debugLog + trxBasedExpOfPrevDayLastTimeSlot + " > " + OOS_COND4_LIG_MIN_EXP;
				debugLog = debugLog + ",6.trxBasedExpOfProcDayFirstTimeSlot > " + OOS_COND4_LIG_MIN_EXP + ":";
				debugLog = debugLog + trxBasedExpOfProcDayFirstTimeSlot + " > " + OOS_COND4_LIG_MIN_EXP;
				debugLog = debugLog + ",7.actualMovFromPrevDay >=  shelfCapacityXPCTOfItemOrLig:";
				debugLog = debugLog + actualMovFromPrevDay + " >= " + oosCandidateItem.getOOSCriteriaData().getShelfCapacityXPCTOfItemOrLig();
				debugLog = debugLog + ",8.actualMovOfLast2TimeSlots < " + OOS_COND4_LIG_TIME_SLOT_ACT_MOV + ":";
				debugLog = debugLog + actualMovOfLast2TimeSlots + " < " + OOS_COND4_LIG_TIME_SLOT_ACT_MOV;
				debugLog = debugLog + ",9.lowerConfOfLast2TimeSlots >= " + OOS_COND4_LIG_TIME_SLOT_MIN_EXP + ":";
				debugLog = debugLog + lowerConfOfLast2TimeSlots + " >= " + OOS_COND4_LIG_TIME_SLOT_MIN_EXP;
				
				if (actualMovOfPrevDayLastTimeSlot < OOS_COND4_LIG_ACT_MOV && actualMovOfProcDayFirstTimeSlot < OOS_COND4_LIG_ACT_MOV
						&& lowerConfOfPrevDayLastTimeSlot > OOS_COND4_LIG_MIN_EXP && lowerConfOfProcDayFirstTimeSlot > OOS_COND4_LIG_MIN_EXP
						&& trxBasedExpOfPrevDayLastTimeSlot > OOS_COND4_LIG_MIN_EXP && trxBasedExpOfProcDayFirstTimeSlot > OOS_COND4_LIG_MIN_EXP
						&& actualMovFromPrevDay >= oosCandidateItem.getOOSCriteriaData().getShelfCapacityXPCTOfItemOrLig()
						&& actualMovOfLast2TimeSlots < OOS_COND4_LIG_TIME_SLOT_ACT_MOV
						&& lowerConfOfLast2TimeSlots >= OOS_COND4_LIG_TIME_SLOT_MIN_EXP) {
					satisfiesCriteria = true;
				}
			} else {
				debugLog = debugLog + "1.actualMovOfPrevDayLastTimeSlot == 0:";
				debugLog = debugLog + actualMovOfPrevDayLastTimeSlot;
				debugLog = debugLog + ",2.actualMovOfProcDayFirstTimeSlot == 0:";
				debugLog = debugLog + actualMovOfProcDayFirstTimeSlot;
				debugLog = debugLog + ",3.lowerConfOfPrevDayLastTimeSlot >= " + OOS_COND4_ITEM_MIN_EXP + ":";
				debugLog = debugLog + lowerConfOfPrevDayLastTimeSlot + " >= " + OOS_COND4_ITEM_MIN_EXP;
				debugLog = debugLog + ",4.lowerConfOfProcDayFirstTimeSlot >= " + OOS_COND4_ITEM_MIN_EXP + ":";
				debugLog = debugLog + lowerConfOfProcDayFirstTimeSlot + " >= " + OOS_COND4_ITEM_MIN_EXP;
				debugLog = debugLog + ",5.trxBasedExpOfPrevDayLastTimeSlot >= " + OOS_COND4_ITEM_MIN_EXP + ":";
				debugLog = debugLog + trxBasedExpOfPrevDayLastTimeSlot + " >= " + OOS_COND4_ITEM_MIN_EXP;
				debugLog = debugLog + ",6.trxBasedExpOfProcDayFirstTimeSlot >= " + OOS_COND4_ITEM_MIN_EXP + ":";
				debugLog = debugLog + trxBasedExpOfProcDayFirstTimeSlot + " >= " + OOS_COND4_ITEM_MIN_EXP;
				debugLog = debugLog + ",7.actualMovFromPrevDay >=  shelfCapacityXPCTOfItemOrLig:";
				debugLog = debugLog + actualMovFromPrevDay + " >= " + oosCandidateItem.getOOSCriteriaData().getShelfCapacityXPCTOfItemOrLig();
				debugLog = debugLog + ",8.actualMovOfLast2TimeSlots == 0:";
				debugLog = debugLog + actualMovOfLast2TimeSlots;
				debugLog = debugLog + ",9.lowerConfOfLast2TimeSlots >= " + OOS_COND4_ITEM_TIME_SLOT_MIN_EXP + ":";
				debugLog = debugLog + lowerConfOfLast2TimeSlots + " >= " + OOS_COND4_ITEM_TIME_SLOT_MIN_EXP;
				if (actualMovOfPrevDayLastTimeSlot == 0 && actualMovOfProcDayFirstTimeSlot == 0
						&& lowerConfOfPrevDayLastTimeSlot >= OOS_COND4_ITEM_MIN_EXP && lowerConfOfProcDayFirstTimeSlot >= OOS_COND4_ITEM_MIN_EXP
						&& trxBasedExpOfPrevDayLastTimeSlot >= OOS_COND4_ITEM_MIN_EXP && trxBasedExpOfProcDayFirstTimeSlot >= OOS_COND4_ITEM_MIN_EXP
						&& actualMovFromPrevDay >= oosCandidateItem.getOOSCriteriaData().getShelfCapacityXPCTOfItemOrLig()
						&& actualMovOfLast2TimeSlots == 0 && lowerConfOfLast2TimeSlots >= OOS_COND4_ITEM_TIME_SLOT_MIN_EXP) {
					satisfiesCriteria = true;
				}
			}
		}
		if (satisfiesCriteria)
			oosCandidateItem.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_4.getOOSCriteriaId());
		logger.debug(debugLog);
		return satisfiesCriteria;
	}
	
	private long findTransBasedExpectationOfItemOrLig(int productLevelId, int productId, int calendarId, int dayPartId) {
		long transBasedExpectation = 0;
		// Find expectation based on transaction count
		// Item Mov on Previous Days / Store Transaction Count on Previous Days
		// * Item Transaction on processing time slot

		ProductKey productKey = new ProductKey(productLevelId, productId);
		HashMap<ProductKey, OOSExpectationDTO> transactionDetails = itemAndLigTransactionDetail.get(calendarId);
		int transactionCount = 0;

		OOSDayPartDetailKey oosDayPartDetailKey = new OOSDayPartDetailKey(calendarId, dayPartId, 0, 0);
		if (prevAndProcDayPartDetail.get(oosDayPartDetailKey) != null)
			transactionCount = prevAndProcDayPartDetail.get(oosDayPartDetailKey).getTransactionCount();

		if (transactionDetails.get(productKey) != null) {
			OOSExpectationDTO oosExpectation = transactionDetails.get(productKey);
			if (oosExpectation.getStoreLevelTrxCount() > 0) {
				transBasedExpectation = Math
						.round((Double.valueOf(oosExpectation.getItemLevelUnitsCount()) / Double.valueOf(oosExpectation.getStoreLevelTrxCount()))
								* transactionCount);
			}
		}
		return transBasedExpectation;
	}
	
	private long getActualMovement(int calendarId, int dayPartId, OOSItemDTO oosCandidateItem) {
		long actualMovement = 0;
		OOSDayPartDetailKey oosDayPartDetailKey = new OOSDayPartDetailKey(calendarId, dayPartId, oosCandidateItem.getProductLevelId(),
				oosCandidateItem.getProductId());
		if (prevAndProcDayPartDetail.get(oosDayPartDetailKey) != null)
			actualMovement = prevAndProcDayPartDetail.get(oosDayPartDetailKey).getActualMovement();
		return actualMovement;
	}
	
	private long getActualMovFromPrevDayToProcTimeSlot(OOSItemDTO oosCandidateItem) {
		OOSDayPartDetailKey oosDayPartDetailKey;
		long actualMovFromPrevDay = 0;
		oosDayPartDetailKey = new OOSDayPartDetailKey(processingCalendarId, processingDayPartId, oosCandidateItem.getProductLevelId(),
				oosCandidateItem.getProductId());
		if (prevAndProcDayPartDetail.get(oosDayPartDetailKey) != null)
			actualMovFromPrevDay = actualMovFromPrevDay + prevAndProcDayPartDetail.get(oosDayPartDetailKey).getActualMovement();
		for (DayPartLookupDTO dpl : dayPartLookupDTO) {
			oosDayPartDetailKey = new OOSDayPartDetailKey(prevDayCalendarId, dpl.getDayPartId(), oosCandidateItem.getProductLevelId(),
					oosCandidateItem.getProductId());
			if (prevAndProcDayPartDetail.get(oosDayPartDetailKey) != null)
				actualMovFromPrevDay = actualMovFromPrevDay + prevAndProcDayPartDetail.get(oosDayPartDetailKey).getActualMovement();
		}
		for (Integer preDayPart : preDayPartId) {
			oosDayPartDetailKey = new OOSDayPartDetailKey(processingCalendarId, preDayPart, oosCandidateItem.getProductLevelId(),
					oosCandidateItem.getProductId());
			if (prevAndProcDayPartDetail.get(oosDayPartDetailKey) != null)
				actualMovFromPrevDay = actualMovFromPrevDay + prevAndProcDayPartDetail.get(oosDayPartDetailKey).getActualMovement();
		}
		return actualMovFromPrevDay;
	}
	
	private long getActualMovOfLastXTimeSlot(OOSItemDTO oosCandidateItem, int maxOrder) {
		OOSDayPartDetailKey oosDayPartDetailKey;
		long actualMovOfLast2TimeSlots = 0;

		actualMovOfLast2TimeSlots = oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig();
		int tempOrder = processingDayPartLookup.getOrder();
		int tempDayPartId = 0, tempCalendarId = processingCalendarId;
		for (int i = 0; i < OOS_COND4_NO_OF_TIME_SLOT - 1; i++) {
			if (tempOrder == 1) {
				tempOrder = maxOrder;
				tempCalendarId = prevDayCalendarId;
			} else {
				tempOrder = tempOrder - 1;
			}

			for (DayPartLookupDTO dpl : dayPartLookupDTO) {
				if (dpl.getOrder() == tempOrder) {
					tempDayPartId = dpl.getDayPartId();
					break;
				}
			}
			oosDayPartDetailKey = new OOSDayPartDetailKey(tempCalendarId, tempDayPartId, oosCandidateItem.getProductLevelId(),
					oosCandidateItem.getProductId());
			if (prevAndProcDayPartDetail.get(oosDayPartDetailKey) != null)
				actualMovOfLast2TimeSlots = actualMovOfLast2TimeSlots + prevAndProcDayPartDetail.get(oosDayPartDetailKey).getActualMovement();
		}
		return actualMovOfLast2TimeSlots;
	}
	
	private long getLowerConfOfLastXTimeSlot(OOSItemDTO oosCandidateItem, int maxOrder) {
		OOSService oosService = new OOSService();
		long lowerConfOfLast2TimeSlots = 0;

		lowerConfOfLast2TimeSlots = oosCandidateItem.getOOSCriteriaData().getLowerConfidenceOfProcessingTimeSlotOfItemOrLig();
		int tempOrder = processingDayPartLookup.getOrder();
		int tempDayPartId = 0, tempDayId = processingDayId;
		for (int i = 0; i < OOS_COND4_NO_OF_TIME_SLOT - 1; i++) {
			if (tempOrder == 1) {
				tempOrder = maxOrder;
				tempDayId = prevDayId;
			} else {
				tempOrder = tempOrder - 1;
			}

			for (DayPartLookupDTO dpl : dayPartLookupDTO) {
				if (dpl.getOrder() == tempOrder) {
					tempDayPartId = dpl.getDayPartId();
					break;
				}
			}

			lowerConfOfLast2TimeSlots = lowerConfOfLast2TimeSlots + oosService.findLowerConfidence(oosCandidateItem.getProductLevelId(),
					oosCandidateItem.getProductId(), storeLevelAvgMovMap, dayPartLevelAvgMovMap, oosCandidateItem.getClientWeeklyPredictedMovement(),
					oosCandidateItem.getWeeklyConfidenceLevelLower(), oosCandidateItem.isPersishableOrDSD(), tempDayId, tempDayPartId);
		}
		return lowerConfOfLast2TimeSlots;
	}
	
	private boolean criteria1LigOrNonLigCondition(OOSItemDTO oosCandidateItem) {
		boolean isCriteriaPassed = false;
		if (oosCandidateItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
			if ((oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig() < oosCandidateItem.getOOSCriteriaData()
					.getLowerConfidenceOfProcessingTimeSlotOfItemOrLig())) {
				isCriteriaPassed = true;
			} else
				isCriteriaPassed = false;
		} else {
			if (oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig() == 0) {
				isCriteriaPassed = true;
			} else {
				isCriteriaPassed = false;
			}
		}
		if (oosCandidateItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
			logger.debug("Criteria 1(ExpVsActual)-- 3.ActualMovOfProcessingTimeSlotOfItemOrLig < LowerConfidenceOfProcessingTimeSlotOfItemOrLig: "
					+ oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig() + "<"
					+ oosCandidateItem.getOOSCriteriaData().getLowerConfidenceOfProcessingTimeSlotOfItemOrLig());
		} else {
			logger.debug("Criteria 1(ExpVsActual)-- 3.ActualMovOfProcessingTimeSlotOfItemOrLig == 0: "
					+ oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig());
		}
		return isCriteriaPassed;
	}
	
	private boolean isPotentialOOS(OOSItemDTO oosCandidateItem) {
		boolean isPotentialOOS = false;
		double shelfCapacity = oosCandidateItem.getOOSCriteriaData().getShelfCapacityXPCTOfItemOrLig();
		double minShelfCapacityMov = Double.valueOf(oosCandidateItem.getOOSCriteriaData().getShelfCapacityOfItemOrLig())
				* Double.valueOf((OOS_MIN_SHELF_CAPACITY_PCT_ITEM_MOV / 100d));
		
//		logger.debug("Potential OOS(ExpVsActual)--" + "1. ((" + OOS_SHELF_CAPACITY_PCT + "% of Shelf Capacity - ActualMovOfProcessingDayItemOrLig <"
//				+ " ForecastMovOfRestOfTheDayOfItemOrLig) && (ActualMovOfProcessingDayItemOrLig < " + OOS_SHELF_CAPACITY_PCT
//				+ "% of Shelf Capacity)): " + "(" + shelfCapacity + " - "
//				+ oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingDayItemOrLig() + " < "
//				+ oosCandidateItem.getOOSCriteriaData().getForecastMovOfRestOfTheDayOfItemOrLig() + ") && ("
//				+ oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingDayItemOrLig() + "<" + shelfCapacity + ")");
		
		logger.debug("Potential OOS(ExpVsActual)--"  
				+ " 1. ActualMovOfProcessingDayItemOrLig > " + OOS_MIN_SHELF_CAPACITY_PCT_ITEM_MOV + "% of Shelf Capacity: "
				+ oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingDayItemOrLig() + ">" + minShelfCapacityMov
				+ " 2. ((" + OOS_SHELF_CAPACITY_PCT + "% of Shelf Capacity - ActualMovOfProcessingDayItemOrLig <"
				+ " ForecastMovOfRestOfTheDayOfItemOrLig): " + "(" + shelfCapacity + " - "
				+ oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingDayItemOrLig() + " < "
				+ oosCandidateItem.getOOSCriteriaData().getForecastMovOfRestOfTheDayOfItemOrLig() + ")");
		
		// find only if there is shelf capacity
		if (shelfCapacity > 0) {
			if (((oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingDayItemOrLig() >  minShelfCapacityMov) && 
					(shelfCapacity - oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingDayItemOrLig()) < oosCandidateItem
					.getOOSCriteriaData().getForecastMovOfRestOfTheDayOfItemOrLig())) {
				logger.debug("Potential OOS is satisfied");
				isPotentialOOS = true;
				oosCandidateItem.setOOSCriteriaId(OOSCriteriaLookup.CRITERIA_6.getOOSCriteriaId());
			}
		}
		
		return isPotentialOOS;
	}
	
	private void setPrevDaysTrxDetailOfItemOrLig(OOSItemDTO oosCandidateItem) {
		ProductKey productKey = new ProductKey(oosCandidateItem.getProductLevelId(), oosCandidateItem.getProductId());
		HashMap<ProductKey, OOSExpectationDTO> transactionDetails = itemAndLigTransactionDetail.get(processingCalendarId);
		if (transactionDetails.get(productKey) != null) {
			OOSExpectationDTO oosExpectation = transactionDetails.get(productKey);
			oosCandidateItem.getOOSCriteriaData().setTrxCntOfPrevDaysOfItemOrLig(oosExpectation.getItemLevelTrxCount());
			oosCandidateItem.getOOSCriteriaData().setTrxCntOfPrevDaysOfStore(oosExpectation.getStoreLevelTrxCount());
			oosCandidateItem.getOOSCriteriaData().setActualMovOfPrevDaysOfItemOrLig(oosExpectation.getItemLevelUnitsCount());
		}
	}
	
 
	private void setNoOfConsecutiveTimeSlotDidNotMove(OOSItemDTO oosCandidateItem) {
		Integer noOfConsecutiveTimeSlotDidNotMove = 0;
		if (oosCandidateItem.getProductLevelId() == Constants.ITEMLEVELID
				&& oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig() == 0)
			noOfConsecutiveTimeSlotDidNotMove = 1;

		List<OOSItemDTO> preDayPartItems = getPreviousTimeSlotItems(oosCandidateItem);

		// Find no of consecutive time slot item didn't move
		// Check if processing time slot also didn't move
		if (noOfConsecutiveTimeSlotDidNotMove > 0) {
			for (OOSItemDTO preItem : preDayPartItems) {
				if (preItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig() == 0) {
					noOfConsecutiveTimeSlotDidNotMove = noOfConsecutiveTimeSlotDidNotMove + 1;
				} else {
					break;
				}
			}
		}
		oosCandidateItem.getOOSCriteriaData().setNoOfConsecutiveTimeSlotItemDidNotMove(noOfConsecutiveTimeSlotDidNotMove);
	}
	
	/***
	 * Fill immediate previous time slot information
	 * @param oosCandidateItem
	 */
	private void setPreviousTimeSlotsDataOfItemOrLig(List<OOSItemDTO> allCandidateItems, OOSItemDTO oosCandidateItem) {
		long actualMov = 0, forecastMov = 0, actualMovAllTimeSlot = 0, forecastMovAllTimeSlot = 0;
		int firstCount = 0;
		if (oosCandidateItem.getProductLevelId() == Constants.ITEMLEVELID) {
			List<OOSItemDTO> preDayPartItems = getPreviousTimeSlotItems(oosCandidateItem);
			for (OOSItemDTO preItem : preDayPartItems) {
				if (firstCount == 0) {
					actualMov = preItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig();
					forecastMov = preItem.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig();
				}
				actualMovAllTimeSlot = actualMovAllTimeSlot + preItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig();
				forecastMovAllTimeSlot = forecastMovAllTimeSlot + preItem.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig();
				firstCount = firstCount + 1;
			}
		} else {
			for (OOSItemDTO allCandidateItem : allCandidateItems) {
				firstCount = 0;
				if (allCandidateItem.getProductLevelId() == Constants.ITEMLEVELID
						&& allCandidateItem.getRetLirId() == oosCandidateItem.getRetLirId()) {
					List<OOSItemDTO> preDayPartItems = getPreviousTimeSlotItems(allCandidateItem);
					for (OOSItemDTO preItem : preDayPartItems) {
						if (firstCount == 0) {
							actualMov = actualMov + preItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig();
							forecastMov = forecastMov + preItem.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig();
						}
						actualMovAllTimeSlot = actualMovAllTimeSlot + preItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig();
						forecastMovAllTimeSlot = forecastMovAllTimeSlot
								+ preItem.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig();
						firstCount = firstCount + 1;
					}
				}
			}
		}
		oosCandidateItem.getOOSCriteriaData().setActualMovOfPrevTimeSlotOfItemOrLig(actualMov);
		oosCandidateItem.getOOSCriteriaData().setForecastMovOfPrevTimeSlotOfItemOrLig(forecastMov);
		oosCandidateItem.getOOSCriteriaData().setActualMovOfAllPrevTimeSlotsOfItemOrLig(actualMovAllTimeSlot);
		oosCandidateItem.getOOSCriteriaData().setForecastMovOfAllPrevTimeSlotsOfItemOrLig(forecastMovAllTimeSlot);
	}
	
//	private void setPrevDaySameTimeSlotDataOfItemOrLig(List<OOSItemDTO> allCandidateItems, OOSItemDTO oosCandidateItem) {
//		OOSItemDTO preDayItem = null;
//		long actualMovOfPrevDaySameTimeSlotOfItemOrLig = 0;
//		if (oosCandidateItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
//			for (OOSItemDTO allCandidateItem : allCandidateItems) {
//				if (allCandidateItem.getProductLevelId() == Constants.ITEMLEVELID
//						&& allCandidateItem.getRetLirId() == oosCandidateItem.getRetLirId()) {
//					preDayItem = getPreviousDaySameTimeSlotItems(prevDaySameTimeSlotItems, allCandidateItem);
//					if (preDayItem != null) {
//						actualMovOfPrevDaySameTimeSlotOfItemOrLig = actualMovOfPrevDaySameTimeSlotOfItemOrLig
//								+ preDayItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig();
//					}
//				}
//			}
//		} else {
//			preDayItem = getPreviousDaySameTimeSlotItems(prevDaySameTimeSlotItems, oosCandidateItem);
//			if (preDayItem != null)
//				actualMovOfPrevDaySameTimeSlotOfItemOrLig = preDayItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig();
//		}
//		oosCandidateItem.getOOSCriteriaData().setActualMovOfPrevDaySameTimeSlotOfItemOrLig(actualMovOfPrevDaySameTimeSlotOfItemOrLig);
//	}
	/**
	 * Find previous time slot (including current time slot) data
	 * @param oosCandidateItem
	 * @param noOfTimeSlot
	 */
	private void findCombinedTimeSlotData(OOSItemDTO oosCandidateItem, int noOfTimeSlot, String criteriaType) {
		long sumOfForecast = 0, sumOfLowerConfidence = 0, sumOfActualMov = 0, sumOfTransBasedExp = 0;
		List<OOSItemDTO> preDayPartItems = getPreviousTimeSlotItems(oosCandidateItem);

		// Only if all pre time slot data is there
		//if (preDayPartItems.size() >= noOfTimeSlot - 1) {
			sumOfForecast = sumOfForecast + oosCandidateItem.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig();
			sumOfLowerConfidence = sumOfLowerConfidence + oosCandidateItem.getOOSCriteriaData().getLowerConfidenceOfProcessingTimeSlotOfItemOrLig();
			sumOfActualMov = sumOfActualMov + oosCandidateItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig();
			sumOfTransBasedExp = sumOfTransBasedExp + oosCandidateItem.getOOSCriteriaData().getTrxBasedPredOfProcessingTimeSlotOfItemOrLig();
			int timeSlotCount = 0;
			for (OOSItemDTO preItem : preDayPartItems) {
				if (timeSlotCount == noOfTimeSlot - 1)
					break;
				sumOfForecast = sumOfForecast + preItem.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig();
				sumOfLowerConfidence = sumOfLowerConfidence + preItem.getOOSCriteriaData().getLowerConfidenceOfProcessingTimeSlotOfItemOrLig();
				sumOfActualMov = sumOfActualMov + preItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig();
				sumOfTransBasedExp = sumOfTransBasedExp + preItem.getOOSCriteriaData().getTrxBasedPredOfProcessingTimeSlotOfItemOrLig();
				timeSlotCount = timeSlotCount + 1;
			}
		//}
		
		if (criteriaType == "CRITERIA_2") {
			oosCandidateItem.getOOSCriteriaData().setCombinedTimeSlotActualMovCriteria2OfItemOrLig(sumOfActualMov);
			oosCandidateItem.getOOSCriteriaData().setCombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig(sumOfTransBasedExp);
			oosCandidateItem.getOOSCriteriaData().setCombinedXTimeSlotLowerConfOfItemOrLig(sumOfLowerConfidence);
		} else {
			oosCandidateItem.getOOSCriteriaData().setCombinedTimeSlotActualMovCriteria3OfItemOrLig(sumOfActualMov);
			oosCandidateItem.getOOSCriteriaData().setCombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig(sumOfTransBasedExp);
			oosCandidateItem.getOOSCriteriaData().setCombinedXTimeSlotForecastOfItemOrLig(sumOfForecast);
		}
	}
	
//	private void setIfItemMovedMoreThanXUnitsInEachXPrevTimeSlot(OOSItemDTO oosCandidateItem, int noOfUnits, int noOfTimeSlot) {
//		Boolean isItemMovedMoreThanXUnitsInEachXPrevTimeSlot = false;
//
//		List<OOSItemDTO> preDayPartItems = getPreviousTimeSlotItems(oosCandidateItem);
//
//		if (preDayPartItems.size() == noOfTimeSlot) {
//			int timeSlotCount = 0;
//			for (OOSItemDTO preItem : preDayPartItems) {
//				if (timeSlotCount == noOfTimeSlot)
//					break;
//
//				// Even if one time slot not meeting then break from the loop
//				if (preItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig() > noOfUnits) {
//					isItemMovedMoreThanXUnitsInEachXPrevTimeSlot = true;
//				} else {
//					isItemMovedMoreThanXUnitsInEachXPrevTimeSlot = false;
//					break;
//				}
//				timeSlotCount = timeSlotCount + 1;
//			}
//		}
//
//		oosCandidateItem.getOOSCriteriaData().setItemMovedMoreThanXUnitsInEachXPrevTimeSlot(isItemMovedMoreThanXUnitsInEachXPrevTimeSlot);
//	}
	
	
	private void setLIGData(List<OOSItemDTO> allCandidateItems, OOSItemDTO ligItem) {
		// Sum of Actual movement of all lig members
		// Sum of confidence lower limit of all lig members
		// Sum of forecast movement of all lig members
		if (ligItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
			long actualMov = 0, lowerConfidence = 0, forecastMov = 0, upperConfidence = 0, weeklyPredictedMovement = 0;
			long forecastMovOfRestOfTheDayOfItemOrLig = 0;
			int noOfTimeMovedInLastXWeeks = 0, trxCntOfProcessingTimeSlotOfStore = 0;
			int shelfCapacityOfItemOfLig = 0, noOfShelfLocation = 0;;
			long combinedXTimeSlotLowerConf = 0, combinedXTimeSlotForecast = 0;
			long combinedTimeSlotActualMovCriteria2 = 0, combinedTimeSlotActualMovCriteria3 = 0;
			long combinedTimeSlotTrxBasedExpCriteria2 = 0, combinedTimeSlotTrxBasedExpCriteria3 = 0;
			for (OOSItemDTO allCandidateItem : allCandidateItems) {
				if (allCandidateItem.getProductLevelId() == Constants.ITEMLEVELID && allCandidateItem.getRetLirId() == ligItem.getRetLirId()) {
					actualMov = actualMov + allCandidateItem.getOOSCriteriaData().getActualMovOfProcessingTimeSlotOfItemOrLig();
					lowerConfidence = lowerConfidence + allCandidateItem.getOOSCriteriaData().getLowerConfidenceOfProcessingTimeSlotOfItemOrLig();
					upperConfidence = upperConfidence + allCandidateItem.getOOSCriteriaData().getUpperConfidenceOfProcessingTimeSlotOfItemOrLig();
					forecastMov = forecastMov + allCandidateItem.getOOSCriteriaData().getForecastMovOfProcessingTimeSlotOfItemOrLig();
					shelfCapacityOfItemOfLig = shelfCapacityOfItemOfLig + allCandidateItem.getOOSCriteriaData().getShelfCapacityOfItemOrLig();
					if (allCandidateItem.getOOSCriteriaData().getNoOfShelfLocationsOfItemOrLig() > noOfShelfLocation) {
						noOfShelfLocation = allCandidateItem.getOOSCriteriaData().getNoOfShelfLocationsOfItemOrLig();
					}
					weeklyPredictedMovement = weeklyPredictedMovement + allCandidateItem.getWeeklyPredictedMovement();
					if (allCandidateItem.getNoOfTimeMovedInLastXWeeksOfItemOrLig() > noOfTimeMovedInLastXWeeks) {
						noOfTimeMovedInLastXWeeks = allCandidateItem.getNoOfTimeMovedInLastXWeeksOfItemOrLig();
					}
					trxCntOfProcessingTimeSlotOfStore = trxCntOfProcessingTimeSlotOfStore
							+ allCandidateItem.getOOSCriteriaData().getTrxCntOfProcessingTimeSlotOfStore();
					forecastMovOfRestOfTheDayOfItemOrLig = forecastMovOfRestOfTheDayOfItemOrLig
							+ allCandidateItem.getOOSCriteriaData().getForecastMovOfRestOfTheDayOfItemOrLig();
					combinedXTimeSlotLowerConf = combinedXTimeSlotLowerConf
							+ allCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotLowerConfOfItemOrLig();
					combinedXTimeSlotForecast = combinedXTimeSlotForecast
							+ allCandidateItem.getOOSCriteriaData().getCombinedXTimeSlotForecastOfItemOrLig();
					
					combinedTimeSlotActualMovCriteria2 = combinedTimeSlotActualMovCriteria2
							+ allCandidateItem.getOOSCriteriaData().getCombinedTimeSlotActualMovCriteria2OfItemOrLig();
					combinedTimeSlotActualMovCriteria3 = combinedTimeSlotActualMovCriteria3
							+ allCandidateItem.getOOSCriteriaData().getCombinedTimeSlotActualMovCriteria3OfItemOrLig();
					
					combinedTimeSlotTrxBasedExpCriteria2 = combinedTimeSlotTrxBasedExpCriteria2
							+ allCandidateItem.getOOSCriteriaData().getCombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig();
					combinedTimeSlotTrxBasedExpCriteria3 = combinedTimeSlotTrxBasedExpCriteria3
							+ allCandidateItem.getOOSCriteriaData().getCombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig();
				}
			}
			ligItem.getOOSCriteriaData().setActualMovOfProcessingTimeSlotOfItemOrLig(actualMov);
			ligItem.getOOSCriteriaData().setLowerConfidenceOfProcessingTimeSlotOfItemOrLig(lowerConfidence);
			ligItem.getOOSCriteriaData().setUpperConfidenceOfProcessingTimeSlotOfItemOrLig(upperConfidence);
			ligItem.getOOSCriteriaData().setForecastMovOfProcessingTimeSlotOfItemOrLig(forecastMov);
			ligItem.setNoOfTimeMovedInLastXWeeksOfItemOrLig(noOfTimeMovedInLastXWeeks);
			ligItem.setWeeklyPredictedMovement(weeklyPredictedMovement);
			ligItem.getOOSCriteriaData().setShelfCapacityOfItemOrLig(shelfCapacityOfItemOfLig);
			ligItem.getOOSCriteriaData().setNoOfShelfLocationsOfItemOrLig(noOfShelfLocation);
			// same across all items for the time slot
			ligItem.getOOSCriteriaData().setTrxCntOfProcessingTimeSlotOfStore(trxCntOfProcessingTimeSlotOfStore);
			ligItem.getOOSCriteriaData().setForecastMovOfRestOfTheDayOfItemOrLig(forecastMovOfRestOfTheDayOfItemOrLig);
			ligItem.getOOSCriteriaData().setCombinedXTimeSlotLowerConfOfItemOrLig(combinedXTimeSlotLowerConf);
			ligItem.getOOSCriteriaData().setCombinedXTimeSlotForecastOfItemOrLig(combinedXTimeSlotForecast);
			ligItem.getOOSCriteriaData().setCombinedTimeSlotActualMovCriteria2OfItemOrLig(combinedTimeSlotActualMovCriteria2);
			ligItem.getOOSCriteriaData().setCombinedTimeSlotActualMovCriteria3OfItemOrLig(combinedTimeSlotActualMovCriteria3);
			ligItem.getOOSCriteriaData().setCombinedTimeSlotTrxBasedExpCriteria2OfItemOrLig(combinedTimeSlotTrxBasedExpCriteria2);
			ligItem.getOOSCriteriaData().setCombinedTimeSlotTrxBasedExpCriteria3OfItemOrLig(combinedTimeSlotTrxBasedExpCriteria3);
		}
	}
	
 
	private void setAdditionalLigInfo(List<OOSItemDTO> allCandidateItems, OOSItemDTO oosCandidateItem) {
		OOSService oosService = new OOSService();
		if(oosCandidateItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG){
			List<OOSItemDTO> memeberItems = oosService.getLigMembers(allCandidateItems, oosCandidateItem);
			HashMap<Integer, HashMap<ProductKey, OOSItemDTO>> calendarMap = new HashMap<Integer, HashMap<ProductKey, OOSItemDTO>>();
			List<Double> movements = new ArrayList<Double>();
			getCalendarMapOfMovedItems(calendarMap);
			for (Map.Entry<Integer, HashMap<ProductKey, OOSItemDTO>> entry : calendarMap.entrySet()) {
				double movement = 0;
				HashMap<ProductKey, OOSItemDTO> movedItems = entry.getValue();
				for (OOSItemDTO memberItem : memeberItems) {
					int productLevelId = memberItem.getProductLevelId();
					int prodcutId = memberItem.getProductId();
					ProductKey productKey = new ProductKey(productLevelId, prodcutId);
					if (movedItems.get(productKey) != null) {
						OOSItemDTO movementDTO = movedItems.get(productKey);
						movement = movement + movementDTO.getActualWeeklyMovOfDayPart();
					}
				}
				if (movement > 0) {
					movements.add(movement);
				}
			}
			oosService.setMovementInfo(movements, oosCandidateItem, this.OOS_REPORT_LAST_X_WEEKS);
		}
	}

	/**
	 * Splits x weeks data and fills calendarMap
	 * @param calendarMap
	 */
	private void getCalendarMapOfMovedItems(HashMap<Integer, HashMap<ProductKey, OOSItemDTO>> calendarMap) {
		if (this.itemAndItsMovements != null) {
			for (Map.Entry<ProductKey, List<OOSItemDTO>> entry : this.itemAndItsMovements.entrySet()) {
				for (OOSItemDTO movement : entry.getValue()) {
					int calendarId = movement.getCalendarId();
					if (calendarMap.get(calendarId) == null) {
						HashMap<ProductKey, OOSItemDTO> tempMap = new HashMap<ProductKey, OOSItemDTO>();
						ProductKey productKey = new ProductKey(movement.getProductLevelId(), movement.getProductId());
						tempMap.put(productKey, movement);
						calendarMap.put(calendarId, tempMap);
					} else {
						HashMap<ProductKey, OOSItemDTO> tempMap = calendarMap.get(calendarId);
						ProductKey productKey = new ProductKey(movement.getProductLevelId(), movement.getProductId());
						if (tempMap.get(productKey) == null) {
							tempMap.put(productKey, movement);
						}
						calendarMap.put(calendarId, tempMap);
					}
				}
			}
		}
	}
	
	private void setTransBasedExpectationOfItemOrLig(OOSItemDTO oosCandidateItem) {
		// Find expectation based on transaction count
		// Item Mov on Previous Days / Store Transaction Count on Previous Days
		// * Item Transaction on processing time slot
		if (oosCandidateItem.getOOSCriteriaData().getTrxCntOfPrevDaysOfStore() > 0) {
			long transBasedExpectation = Math.round((Double.valueOf(oosCandidateItem.getOOSCriteriaData().getActualMovOfPrevDaysOfItemOrLig())
					/ Double.valueOf(oosCandidateItem.getOOSCriteriaData().getTrxCntOfPrevDaysOfStore()))
					* oosCandidateItem.getOOSCriteriaData().getTrxCntOfProcessingTimeSlotOfStore());
			oosCandidateItem.getOOSCriteriaData().setTrxBasedPredOfProcessingTimeSlotOfItemOrLig(transBasedExpectation);
		}
	}

	/**
	 * Get all previous time slot data of an item
	 * 
	 * @param prevTimeSlotItems
	 * @param oosCandidateItem
	 * @return
	 */
//	private List<OOSItemDTO> getPreviousTimeSlotItems(HashMap<OOSAlertItemKey, OOSItemDTO> prevTimeSlotItems, OOSItemDTO oosCandidateItem) {
//		List<OOSItemDTO> preDayPartItems = new ArrayList<OOSItemDTO>();
//		for (Integer dayPartId : preDayPartId) {
//			OOSAlertItemKey oosAlertItemKey = new OOSAlertItemKey(locationLevelId, locationId, oosCandidateItem.getProductLevelId(),
//					oosCandidateItem.getProductId(), oosCandidateItem.getCalendarId(), dayPartId, oosCandidateItem.getRegPrice(),
//					oosCandidateItem.getSalePrice(), oosCandidateItem.getAdPageNo(), oosCandidateItem.getDisplayTypeId());
//			if (prevTimeSlotItems.get(oosAlertItemKey) != null) {
//				preDayPartItems.add(prevTimeSlotItems.get(oosAlertItemKey));
//			}
//		}
//
//		// Make sure all previous time slot's are there
//		if (preDayPartItems.size() != preDayPartId.size() && preDayPartId.size() == 0)
//			preDayPartItems = new ArrayList<OOSItemDTO>();
//
//		return preDayPartItems;
//	}
	
	private List<OOSItemDTO> getPreviousTimeSlotItems(OOSItemDTO oosCandidateItem) {
		List<OOSItemDTO> preDayPartItems = new ArrayList<OOSItemDTO>();
		OOSItemDTO oosItemDTO;
		OOSService oosService = new OOSService();
		for (Integer dayPartId : preDayPartId) {
			OOSDayPartDetailKey oosDayPartDetailKey = new OOSDayPartDetailKey(processingCalendarId, dayPartId, oosCandidateItem.getProductLevelId(),
					oosCandidateItem.getProductId());
			oosItemDTO = new OOSItemDTO();
			if (prevAndProcDayPartDetail.get(oosDayPartDetailKey) != null) {
				oosItemDTO.getOOSCriteriaData()
						.setActualMovOfProcessingTimeSlotOfItemOrLig(prevAndProcDayPartDetail.get(oosDayPartDetailKey).getActualMovement());
			} else {
				oosItemDTO.getOOSCriteriaData().setActualMovOfProcessingTimeSlotOfItemOrLig(0);
			}
			
			// set lower confidence, forecast
			long lowConf = oosService.findLowerConfidence(oosCandidateItem.getProductLevelId(), oosCandidateItem.getProductId(), storeLevelAvgMovMap,
					dayPartLevelAvgMovMap, oosCandidateItem.getClientWeeklyPredictedMovement(), oosCandidateItem.getWeeklyConfidenceLevelLower(),
					oosCandidateItem.isPersishableOrDSD(), processingDayId, dayPartId);
			oosItemDTO.getOOSCriteriaData().setLowerConfidenceOfProcessingTimeSlotOfItemOrLig(lowConf);

			long forecast = oosService.findForecast(oosCandidateItem.getProductLevelId(), oosCandidateItem.getProductId(), storeLevelAvgMovMap,
					dayPartLevelAvgMovMap, oosCandidateItem.getClientWeeklyPredictedMovement(), oosCandidateItem.getWeeklyPredictedMovement(),
					oosCandidateItem.isPersishableOrDSD(), processingDayId, dayPartId);
			oosItemDTO.getOOSCriteriaData().setForecastMovOfProcessingTimeSlotOfItemOrLig(forecast);

			long trxBasedExp = findTransBasedExpectationOfItemOrLig(oosCandidateItem.getProductLevelId(), oosCandidateItem.getProductId(),
					processingCalendarId, dayPartId);
			oosItemDTO.getOOSCriteriaData().setTrxBasedPredOfProcessingTimeSlotOfItemOrLig(trxBasedExp);

//			logger.debug("productId: " + oosCandidateItem.getProductId() + ",lowConf:" + lowConf + ",forecast:" + forecast + ",trxBasedExp:"
//					+ trxBasedExp);
			
			preDayPartItems.add(oosItemDTO);
		}

		return preDayPartItems;
	}
	
//	private OOSItemDTO getPreviousDaySameTimeSlotItems(HashMap<OOSAlertItemKey, OOSItemDTO> prevDaySameTimeSlotItems, OOSItemDTO oosCandidateItem) {
//		OOSItemDTO prevDayItem = null;
//		OOSAlertItemKey oosAlertItemKey = new OOSAlertItemKey(locationLevelId, locationId, oosCandidateItem.getProductLevelId(),
//				oosCandidateItem.getProductId(), prevDayCalendarId, processingDayPartId, oosCandidateItem.getRegPrice(),
//				oosCandidateItem.getSalePrice(), oosCandidateItem.getAdPageNo(), oosCandidateItem.getDisplayTypeId());
//		if (prevDaySameTimeSlotItems.get(oosAlertItemKey) != null) {
//			prevDayItem = prevDaySameTimeSlotItems.get(oosAlertItemKey);
//		}
//		return prevDayItem;
//	}

	private double getShelfCapacity(OOSItemDTO oosCandidateItem) {
		double shelfCapacity = Double.valueOf(oosCandidateItem.getOOSCriteriaData().getShelfCapacityOfItemOrLig())
				* Double.valueOf((OOS_SHELF_CAPACITY_PCT / 100d));
		return shelfCapacity;
	}
}
