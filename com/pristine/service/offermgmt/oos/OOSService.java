package com.pristine.service.offermgmt.oos;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
//import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.oos.OOSAnalysisDAO;
import com.pristine.dto.AdKey;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.DayPartLookupDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.dto.offermgmt.oos.WeeklyAvgMovement;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.RetailPriceServiceOptimized;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRFormatHelper;

public class OOSService {
	private static Logger logger = Logger.getLogger("OOSService");
	
	public DayPartLookupDTO getPreviousDayPart(List<DayPartLookupDTO> dayPartLookup, Date currentDate) throws ParseException, GeneralException{
		//1. get current time.
		SimpleDateFormat hourAndMin = new SimpleDateFormat("HH:mm");
		SimpleDateFormat hourOnly = new SimpleDateFormat("HH");
		String processTimeStr = hourAndMin.format(currentDate);
		Date actualProcessTime = hourAndMin.parse(processTimeStr);
		Date actualProcessTimeHr = hourOnly.parse(processTimeStr);
		DayPartLookupDTO currentDayPart = null;
		DayPartLookupDTO previousDayPart = null;
		boolean isTimePrevDay = false;
		HashMap<Integer, DayPartLookupDTO> dayPartLookupMap = new HashMap<Integer, DayPartLookupDTO>();
		
		for(DayPartLookupDTO oosDayPartLookupDTO: dayPartLookup){
			dayPartLookupMap.put(oosDayPartLookupDTO.getOrder(), oosDayPartLookupDTO);
		}
		
		for(DayPartLookupDTO oosDayPartLookupDTO: dayPartLookup){
			String startTimeStr = oosDayPartLookupDTO.getStartTime();
			String endTimeStr  = oosDayPartLookupDTO.getEndTime();
			Date startTime = hourAndMin.parse(startTimeStr);
			Date endTime = hourAndMin.parse(endTimeStr);
			
			if((actualProcessTime.after(startTime)|| actualProcessTime.equals(startTime))
					&& actualProcessTime.before(endTime)){
				currentDayPart = oosDayPartLookupDTO;
				break;
			}
			else if(oosDayPartLookupDTO.isSlotSpanDays()){
				endTime = DateUtil.incrementDate(endTime, 1);
				if((actualProcessTime.after(actualProcessTimeHr)|| actualProcessTime.equals(actualProcessTimeHr))
						&& actualProcessTime.before(endTime)){
					currentDayPart = oosDayPartLookupDTO;
				isTimePrevDay = true;
				break;
				}
			}
		}
		//2. find out current day part.
		if(currentDayPart == null){
			throw new GeneralException("Unable to find current Day part for given date - " + currentDate);
		}
		else{
			//3. find out previous day part from order column.
			int prevDayPartOrder = currentDayPart.getOrder() - 1;
			if(prevDayPartOrder == 0){
				Set<Integer> orderSet = dayPartLookupMap.keySet();
				prevDayPartOrder = Collections.max(orderSet);
			}
			if(dayPartLookupMap.get(prevDayPartOrder) == null){
				throw new GeneralException("Unable to find previous Day part from day part order - " + prevDayPartOrder);
			}
			else{
				previousDayPart = dayPartLookupMap.get(prevDayPartOrder);
				if(isTimePrevDay)
					previousDayPart.setDayPartSpanPrevDay(true);
			}
		}
	 return previousDayPart;
	}
	
	public Set<Integer> getItemCodesFromObject(List<PRItemDTO> prItemList){
		Set<Integer> itemCodeList = new HashSet<Integer>();
		for(PRItemDTO prItemDTO: prItemList){
			itemCodeList.add(prItemDTO.getItemCode());
		}
		return itemCodeList;
	}
	
	public void applyPriceInfoForItems(HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceMap,
			List<PRItemDTO> prItemList, int chainId, int locationId, int zoneId) throws GeneralException {
		RetailPriceCostKey chainKey = new RetailPriceCostKey(Constants.CHAIN_LEVEL_TYPE_ID, chainId);
		RetailPriceCostKey zoneKey = new RetailPriceCostKey(Constants.ZONE_LEVEL_TYPE_ID, zoneId);
		RetailPriceCostKey storeKey = new RetailPriceCostKey(Constants.STORE_LEVEL_TYPE_ID, locationId);
		for (PRItemDTO prItemDTO : prItemList) {
			if (priceMap.get(prItemDTO.getItemCode()) == null) {
				//throw new GeneralException("Price info not found for item code - " + prItemDTO.getItemCode());
				logger.warn("Price info not found for item code - " + prItemDTO.getItemCode());
			} else {
				HashMap<RetailPriceCostKey, RetailPriceDTO> priceInfo = priceMap.get(prItemDTO.getItemCode());
				if (priceInfo.get(storeKey) != null) {
					logger.debug("Store level price found");
					RetailPriceDTO retailPriceInfo = priceInfo.get(storeKey);
					setupRetails(prItemDTO, retailPriceInfo);
				} else if (priceInfo.get(zoneKey) != null) {
					logger.debug("Zone level price found");
					RetailPriceDTO retailPriceInfo = priceInfo.get(zoneKey);
					setupRetails(prItemDTO, retailPriceInfo);
				} else if (priceInfo.get(chainKey) != null) {
					logger.debug("Chain level price found");
					RetailPriceDTO retailPriceInfo = priceInfo.get(chainKey);
					setupRetails(prItemDTO, retailPriceInfo);
				}
			}
		}
	}
	
	private void setupRetails(PRItemDTO prItemDTO, RetailPriceDTO retailPriceInfo){
		populateRegularPrice(prItemDTO, retailPriceInfo);
		populateSalePrice(prItemDTO, retailPriceInfo);
		
	}
	
	private void populateRegularPrice(PRItemDTO item, RetailPriceDTO priceDTO) {
		double price = priceDTO.getRegQty() > 1 ? (priceDTO.getRegMPrice()/priceDTO.getRegQty()) : priceDTO.getRegPrice();
		item.setRegPrice(price);
		item.setRegMPack((priceDTO.getRegQty() == 0 ? 1 : priceDTO.getRegQty()));
		item.setRegMPrice(new Double(priceDTO.getRegMPrice()));
		if ((item.getRegMPack() == 1 && item.getRegPrice() == 0)  || (item.getRegMPack() > 1 && item.getRegMPrice() == 0)) {
			item.setRegMPack(0);
		}
	}
	
	private void populateSalePrice(PRItemDTO item, RetailPriceDTO priceDTO) {
		double price = priceDTO.getSaleQty() > 1 ? (priceDTO.getSaleMPrice()/priceDTO.getSaleQty()) : priceDTO.getSalePrice();
		item.setSalePrice(price);
		item.setSaleMPack((priceDTO.getSaleQty() == 0 ? 1 : priceDTO.getSaleQty()));
		item.setSaleMPrice(new Double(priceDTO.getSaleMPrice()));
		if ((item.getSaleMPack() == 1 && item.getSalePrice() == 0)  || (item.getSaleMPack() > 1 && item.getSaleMPrice() == 0)) {
			item.setSaleMPack(0);
		}
	}
	
	/***
	 * From Day Part Case based on Day Part Lookup from the table
	 * @param dayPartLookup
	 * @return
	 */
	public String fillDayPartCase(List<DayPartLookupDTO> dayPartLookup) {
		String dayPartCase = "";
		dayPartCase = " CASE ";
		for (DayPartLookupDTO dayPart : dayPartLookup) {
			if (dayPart.isSlotSpanDays()) {
				// If a time slot spans 2 days, then consider the time spans on
				// next day as same time slot
				// eg. 21:00 -- 07:00, where 00:00 to 07:00 is considered as
				// same time slot as 21:00 -- 07:00, though it falls
				// in next day
				dayPartCase = dayPartCase + " WHEN (TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) >= '";
				dayPartCase = dayPartCase + dayPart.getStartTime() + ":00" + "' ";
				dayPartCase = dayPartCase + " AND TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) <= '23:59:59') THEN ";
				dayPartCase = dayPartCase + dayPart.getDayPartId();
				dayPartCase = dayPartCase + " WHEN (TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) >= '00:00:00' ";
				dayPartCase = dayPartCase + " AND TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) < '";
				dayPartCase = dayPartCase + dayPart.getEndTime() + ":00') ";
				dayPartCase = dayPartCase + " THEN " + dayPart.getDayPartId();
			} else {
				dayPartCase = dayPartCase + " WHEN (TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) >= '";
				dayPartCase = dayPartCase + dayPart.getStartTime() + ":00" + "' ";
				dayPartCase = dayPartCase + " AND TO_CHAR(TRX_TIME, 'HH24:MI:SS' ) < '";
				dayPartCase = dayPartCase + dayPart.getEndTime() + ":00') ";
				dayPartCase = dayPartCase + " THEN " + dayPart.getDayPartId();
			}
		}
		dayPartCase = dayPartCase + " END AS DAY_PART_ID ";

		return dayPartCase;
	}
	
	/***
	 * From Day Case based on Day Part Lookup from the table
	 * @param dayPartLookup
	 * @return
	 */
	public String fillDayCase(List<DayPartLookupDTO> dayPartLookup){
		String dayCase = "";
		boolean isThereTimeSlotSpanDays = false;
		String endTimeOfSlotSpanDays = "";
		//Check if there is time slot spans across day
		for (DayPartLookupDTO dayPart : dayPartLookup) {
			if (dayPart.isSlotSpanDays()) {
				isThereTimeSlotSpanDays = true;
				endTimeOfSlotSpanDays = dayPart.getEndTime();
				break;
			}
		}
		
		//If a time spans across days, then consider the other days as day where the start time started
		//e.g. if the time slot is 21:00 -- 07:00 and assume transaction happened on Monday 21:25 and another on 
		//Tuesday 01:35, then the transaction happened on 01:35 will go under the day Monday
		//While doing this shifting to previous day must be handled properly when the shift happens on Monday
		//it should go to Sunday
		if(isThereTimeSlotSpanDays){
			dayCase = dayCase + " CASE ";
			dayCase = dayCase + " WHEN (TO_CHAR(TRX_TIME, 'HH24:MI:SS') >= '00:00:00' ";
			dayCase = dayCase + " AND TO_CHAR(TRX_TIME, 'HH24:MI:SS') < '" + endTimeOfSlotSpanDays + ":00') THEN ";
			dayCase = dayCase + " TO_CHAR(TRX_TIME - 1, 'dd-MON-yy') ELSE TO_CHAR(TRX_TIME, 'dd-MON-yy') END  AS NEW_TRX_DATE";
		} else {
			dayCase = dayCase + " TO_CHAR(TRX_TIME, 'dd-MON-yy') AS NEW_TRX_DATE";
		}
		return dayCase;			
	}
	
	
	/**
	 * Gets weekly predicted movement for given items
	 * @param oosAnalysisItems
	 * @throws GeneralException
	 */
	public void getWeeklyPredictedMovement(Connection conn, int locationLevelId, int locationId, int weekCalendarId,
			List<OOSItemDTO> oosAnalysisItems) throws GeneralException {
		PredictionService predictionService = new PredictionService();
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictionResults = new HashMap<PredictionDetailKey, PredictionDetailDTO>();
		List<PRItemDTO> itemsToPredict = new ArrayList<PRItemDTO>();

		// Prepare items
		for (OOSItemDTO oosItemDTO : oosAnalysisItems) {
			PRItemDTO itemDTO = new PRItemDTO();
			itemDTO.setItemCode(oosItemDTO.getProductId());
			itemsToPredict.add(itemDTO);
		}

		// Call the prediction engine
		predictionResults = predictionService.getAlreadyPredictedMovementOfItems(conn, locationLevelId, locationId,
				weekCalendarId, weekCalendarId, itemsToPredict, true);

		// Fill prediction result
		for (OOSItemDTO oosItemDTO : oosAnalysisItems) {
			if (oosItemDTO.getRegPrice() != null) {
				int saleQuantity = 0;
				Double salePrice = 0d;
				if (oosItemDTO.getSalePrice() != null) {
					saleQuantity = oosItemDTO.getSalePrice().multiple;
					salePrice = oosItemDTO.getSalePrice().price;
				}
				PredictionDetailKey pdk = new PredictionDetailKey(locationLevelId, locationId,
						oosItemDTO.getProductId(), oosItemDTO.getRegPrice().multiple, oosItemDTO.getRegPrice().price,
						saleQuantity, salePrice, oosItemDTO.getAdPageNo(), oosItemDTO.getBlockNo(),
						oosItemDTO.getPromoTypeId(), oosItemDTO.getDisplayTypeId());
				if (predictionResults.get(pdk) != null) {
					PredictionDetailDTO pd = predictionResults.get(pdk);
					oosItemDTO.setWeeklyPredictedMovement(Math.round(pd.getPredictedMovement()));
					oosItemDTO.setWeeklyPredictionStatus(pd.getPredictionStatus());
					oosItemDTO.setWeeklyConfidenceLevelLower(Math.round(pd.getConfidenceLevelLower()));
					oosItemDTO.setWeeklyConfidenceLevelUpper(Math.round(pd.getConfidenceLevelUpper()));
					if (pd.getPredictionStatus() == PredictionStatus.SUCCESS.getStatusCode()
							|| pd.getPredictionStatus() == PredictionStatus.ERROR_NO_MOV_DATA_SPECIFIC_UPC.getStatusCode()
							|| pd.getPredictionStatus() == PredictionStatus.ERROR_NO_PRICE_DATA_SPECIFIC_UPC.getStatusCode()
							|| pd.getPredictionStatus() == PredictionStatus.NO_RECENT_MOVEMENT.getStatusCode()
							|| pd.getPredictionStatus() == PredictionStatus.ERROR_NO_MOV_DATA_ANY_UPC.getStatusCode()
							|| pd.getPredictionStatus() == PredictionStatus.ERROR_NO_PRICE_DATA_ANY_UPC.getStatusCode()
							|| pd.getPredictionStatus() == PredictionStatus.SHIPPER_ITEM.getStatusCode()
							|| pd.getPredictionStatus() == PredictionStatus.NEW_ITEM.getStatusCode()) {
						oosItemDTO.setIsOOSAnalysis(Constants.YES);
					} else {
						oosItemDTO.setIsOOSAnalysis(Constants.NO);
					}
					
				}
			}
		}
	}
	
	

	/**
	 * Gets weekly predicted movement for given items
	 * @param oosAnalysisItems
	 * @throws GeneralException
	 */
	public void getPredictionsFromOOSForecast(Connection conn, int locationLevelId, int locationId, int weekCalendarId,
			List<OOSItemDTO> oosAnalysisItems) throws GeneralException {
		HashMap<OOSForecastItemKey, OOSItemDTO> predictionResults = new HashMap<OOSForecastItemKey, OOSItemDTO>();
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		// get predictions for current location and calendar
		predictionResults = oosAnalysisDAO.getOOSForcastMovement(conn, locationLevelId, locationId, weekCalendarId);
		
//		for(Map.Entry<OOSForecastItemKey, OOSItemDTO> entry: predictionResults.entrySet()){
//			logger.debug("Key:" + entry.getKey().toString());
//			logger.debug("Value:" + entry.getValue().toString());
//		}
		 
		// Fill prediction result
		for (OOSItemDTO oosItemDTO : oosAnalysisItems) {
			int saleQuantity = 0;
			Double salePrice = 0d;
			if (oosItemDTO.getSalePrice() != null) {
				saleQuantity = oosItemDTO.getSalePrice().multiple;
				salePrice = oosItemDTO.getSalePrice().price;
			}
			OOSForecastItemKey fik = new OOSForecastItemKey(locationLevelId, locationId, oosItemDTO.getProductId(),
					oosItemDTO.getRegPrice().multiple, oosItemDTO.getRegPrice().price, saleQuantity, salePrice,
					oosItemDTO.getAdPageNo(), oosItemDTO.getBlockNo(), oosItemDTO.getPromoTypeId(),
					oosItemDTO.getDisplayTypeId());
//			logger.debug("oosItemDTO:" + oosItemDTO.toString());
			if (predictionResults.get(fik) != null) {
				OOSItemDTO pd = predictionResults.get(fik);
				oosItemDTO.setWeeklyPredictedMovement(pd.getWeeklyPredictedMovement());
				oosItemDTO.setClientWeeklyPredictedMovement(pd.getClientWeeklyPredictedMovement());
				oosItemDTO.setWeeklyConfidenceLevelLower(pd.getWeeklyConfidenceLevelLower());
				oosItemDTO.setWeeklyConfidenceLevelUpper(pd.getWeeklyConfidenceLevelUpper());
				oosItemDTO.setWeeklyActualMovement(pd.getWeeklyActualMovement());
//				logger.debug("inside oosItemDTO:" + oosItemDTO.toString());
			} 
			else {
				logger.warn("Failed Key:" + fik.toString());
			}
		}
	}

	public HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> getRegAndSalePrice(Connection conn,
			int chainId, int locationId, int weekCalendarId, PriceZoneDTO priceZone, List<PRItemDTO> itemList)
			throws GeneralException {
		RetailCalendarDAO calendarDAO = new RetailCalendarDAO();
		RetailPriceServiceOptimized retailPriceService = new RetailPriceServiceOptimized(conn);
		List<String> priceZones = new ArrayList<String>();
		List<Integer> locations = new ArrayList<Integer>();
		int noOfWeeksHistory = Integer.parseInt(PropertyManager.getProperty("OOS_PRICE_FETCH_NO_OF_WEEKS_HISTORY"));;
		
		// Get item codes for which price and cost to be fetched
		Set<Integer> itemCodeList = getItemCodesFromObject(itemList);

		if (priceZone == null)
			throw new GeneralException("Zone not found for location - " + locationId);

		// add price zone into list to get price and cost
		priceZones.add(priceZone.getPriceZoneNum());

		// add location id to list to get price and cost
		locations.add(locationId);

		// get retail price info
		// TODO throw exception in price optimized methods
		logger.info("Getting Price for OOS Candidate Items is Started...");

		List<RetailCalendarDTO> retailCalendarList = calendarDAO
				.getCalendarList(conn, weekCalendarId, noOfWeeksHistory);
		Set<Integer> itemCodeSet = new HashSet<Integer>();
		for (Integer itemCode : itemCodeList) {
			itemCodeSet.add(itemCode);
		}
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> finalPriceMap = new HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>>();

		for (RetailCalendarDTO curCalDTO : retailCalendarList) {
			logger.debug("Running for calendar id " + curCalDTO.getCalendarId() + " for items " + itemCodeSet.size());
			if (itemCodeSet.size() > 0) {
				HashMap<Integer, HashMap<RetailPriceCostKey, RetailPriceDTO>> priceDataMap = retailPriceService
						.getRetailPrice(curCalDTO.getCalendarId(), chainId, priceZones, locations, true, itemCodeSet);
				for (Integer itemCode : priceDataMap.keySet()) {
					finalPriceMap.put(itemCode, priceDataMap.get(itemCode));
					if (itemCodeSet.contains(itemCode))
						itemCodeSet.remove(itemCode);
				}
			}
		}
		return finalPriceMap;
	}
	
	/**
	 * 
	 * @param oosCandidateItems
	 * @param ligItem
	 * @return list of member items for given lig
	 */
	public List<OOSItemDTO> getLigMembers(List<OOSItemDTO> oosCandidateItems, OOSItemDTO ligItem) {
		List<OOSItemDTO> ligMembers = new ArrayList<OOSItemDTO>();
		if (ligItem.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
			for (OOSItemDTO allCandidateItem : oosCandidateItems) {
				if (allCandidateItem.getProductLevelId() == Constants.ITEMLEVELID
						&& allCandidateItem.getRetLirId() == ligItem.getRetLirId()) {
					ligMembers.add(allCandidateItem);
				}
			}
		}

		return ligMembers;
	}
	
	/**
	 * Sets movement info # of zero mov occur, min mov, max mov, avg mov, # of consecutive time slot etc..
	 * @param movements
	 * @param oosItemDTO
	 * @param noOfWeeksHistory
	 */
	public void setMovementInfo(List<Double> movements, OOSItemDTO oosItemDTO, int noOfWeeksHistory){
		double totalMov = 0;
		long avgMov = 0l;
		if (movements != null && movements.size() > 0) {
			// list has each week row
			oosItemDTO.setNoOfTimeMovedInLastXWeeksOfItemOrLig(movements.size());
			// 13 - 2 ->11
			oosItemDTO.setNoOfZeroMovInLastXWeeks(noOfWeeksHistory - oosItemDTO.getNoOfTimeMovedInLastXWeeksOfItemOrLig());
			Collections.sort(movements);
			oosItemDTO.setMinMovementInLastXWeeks(Math.round(movements.get(0)));
			oosItemDTO.setMaxMovementInLastXWeeks(Math.round(movements.get(movements.size() - 1)));
			for (Double mov : movements) {
				totalMov = totalMov + mov;
			}
			avgMov = Math.round(totalMov / noOfWeeksHistory);
			oosItemDTO.setAvgMovementInLastXWeeks(avgMov);
		} else {
			oosItemDTO.setNoOfZeroMovInLastXWeeks(noOfWeeksHistory);
		}
	}
	
	
	/**
	 * 
	 * @param itemList
	 * @return List of LIG and Non Lig Items
	 * @throws CloneNotSupportedException
	 */
	public List<OOSItemDTO> processLIGAndNonLigData(List<OOSItemDTO> itemList) throws CloneNotSupportedException{
		List<OOSItemDTO> ligAndNonLigData = new ArrayList<>();
		HashMap<Integer, List<OOSItemDTO>> ligMap = new HashMap<>();
		HashMap<String, List<OOSItemDTO>> nonLigMap = new HashMap<>();
		for(OOSItemDTO oosItemDTO: itemList){
			
			if(oosItemDTO.getRetLirId() == 0){
				//If the item is non lig group it based on retailer item code
				if(nonLigMap.get(oosItemDTO.getRetailerItemCode()) == null){
					List<OOSItemDTO> sameItemcodeMembers = new ArrayList<>();
					sameItemcodeMembers.add(oosItemDTO);
					nonLigMap.put(oosItemDTO.getRetailerItemCode(), sameItemcodeMembers);
					
					logger.debug("**Ret Lir Id: " + oosItemDTO.getRetLirId() + ",ItemCode: " + oosItemDTO.getProductId() + ",WeeklyActualMovement: "
							+ oosItemDTO.getWeeklyActualMovement());
				}
				else{
					List<OOSItemDTO> sameItemcodeMembers = nonLigMap.get(oosItemDTO.getRetailerItemCode());
					sameItemcodeMembers.add(oosItemDTO);
					nonLigMap.put(oosItemDTO.getRetailerItemCode(), sameItemcodeMembers);
					
					logger.debug("**Ret Lir Id: " + oosItemDTO.getRetLirId() + ",ItemCode: " + oosItemDTO.getProductId() + ",WeeklyActualMovement: "
							+ oosItemDTO.getWeeklyActualMovement());
				}
				/*ligAndNonLigData.add(oosItemDTO);
				logger.debug("**Ret Lir Id: " + oosItemDTO.getRetLirId() + ",ItemCode: " + oosItemDTO.getProductId() + ",WeeklyActualMovement: "
						+ oosItemDTO.getWeeklyActualMovement());*/
			}
			else{
				//if the item is lig member, get the data at lig level.
				if(ligMap.get(oosItemDTO.getRetLirId()) == null){
					List<OOSItemDTO> ligMembers = new ArrayList<>();
					ligMembers.add(oosItemDTO);
					ligMap.put(oosItemDTO.getRetLirId(), ligMembers);
					
					logger.debug("Ret Lir Id: " + oosItemDTO.getRetLirId() + ",ItemCode: " + oosItemDTO.getProductId() + ",WeeklyActualMovement: "
							+ oosItemDTO.getWeeklyActualMovement());
				}
				else{
					List<OOSItemDTO> ligMembers = ligMap.get(oosItemDTO.getRetLirId());
					ligMembers.add(oosItemDTO);
					ligMap.put(oosItemDTO.getRetLirId(), ligMembers);
					
					logger.debug("Ret Lir Id: " + oosItemDTO.getRetLirId() + ",ItemCode: " + oosItemDTO.getProductId() + ",WeeklyActualMovement: "
							+ oosItemDTO.getWeeklyActualMovement());
				}
			}
		}
		
		addLigLevelRecord(ligMap, nonLigMap, ligAndNonLigData);
		
		return ligAndNonLigData;
	}
	
	
	/**
	 * Aggregates data at LIG level
	 * @param ligMap
	 * @param ligAndNonLigData
	 * @throws CloneNotSupportedException
	 */
	private void addLigLevelRecord(HashMap<Integer, List<OOSItemDTO>> ligMap, 
			HashMap<String, List<OOSItemDTO>> nonLigMap, List<OOSItemDTO> ligAndNonLigData)
			throws CloneNotSupportedException {
		for (Map.Entry<Integer, List<OOSItemDTO>> entry : ligMap.entrySet()) {
			processLigAndNonLigItems(entry.getValue(), ligAndNonLigData, true);
		}
	
		for (Map.Entry<String, List<OOSItemDTO>> entry : nonLigMap.entrySet()) {
			processLigAndNonLigItems(entry.getValue(), ligAndNonLigData, false);
		}
	}
	
	/**
	 * Aggregates required data at group level for Lig and Non lig items
	 * @param items
	 * @param ligAndNonLigData
	 * @throws CloneNotSupportedException
	 */
	private void processLigAndNonLigItems(List<OOSItemDTO> items, 
			List<OOSItemDTO> ligAndNonLigData, boolean isLig) throws CloneNotSupportedException{
		OOSItemDTO itemDto = null;
		for (OOSItemDTO oosItemDTO : items) {
			if (itemDto == null) {
				itemDto = (OOSItemDTO) oosItemDTO.clone();
			} else {
				// Sum member forecasts to get LIG level forecast
				long weeklyPredictedMovement = itemDto.getWeeklyPredictedMovement()
						+ oosItemDTO.getWeeklyPredictedMovement();
				long weeklyPredictedMovementTops = itemDto.getWeeklyPredictedMovementTops()
						+ oosItemDTO.getWeeklyPredictedMovementTops();
				long weeklyPredictedMovementGU = itemDto.getWeeklyPredictedMovementGU()
						+ oosItemDTO.getWeeklyPredictedMovementGU();
			/*	long clientWeeklyPredictedMovement = itemDto.getClientWeeklyPredictedMovement()
						+ oosItemDTO.getClientWeeklyPredictedMovement();*/
				long actualWeeklyMovement = itemDto.getWeeklyActualMovement()
						+ oosItemDTO.getWeeklyActualMovement();
				// If item code is less than previous item code, get its
				// info to show consistent data
				
				if(itemDto.getAdPageNo() > 0 && itemDto.getBlockNo() > 0){
					oosItemDTO.setAdPageNo(itemDto.getAdPageNo());
					oosItemDTO.setBlockNo(itemDto.getBlockNo());
				}
				
				if(itemDto.getAdPageNoGU() > 0 && itemDto.getBlockNoGU() > 0){
					oosItemDTO.setAdPageNoGU(itemDto.getAdPageNoGU());
					oosItemDTO.setBlockNoGU(itemDto.getBlockNoGU());
				}
				
				if (oosItemDTO.getProductId() < itemDto.getProductId()) {
					itemDto = (OOSItemDTO) oosItemDTO.clone();
				}
	
				if(oosItemDTO.getAdPageNo() > 0 && oosItemDTO.getBlockNo() > 0){
					itemDto.setAdPageNo(oosItemDTO.getAdPageNo());
					itemDto.setBlockNo(oosItemDTO.getBlockNo());
				}
				
				if(oosItemDTO.getAdPageNoGU() > 0 && oosItemDTO.getBlockNoGU() > 0){
					itemDto.setAdPageNoGU(oosItemDTO.getAdPageNoGU());
					itemDto.setBlockNoGU(oosItemDTO.getBlockNoGU());
				}
				
				// set aggregated data to LIG item
				itemDto.setWeeklyPredictedMovement(weeklyPredictedMovement);
				itemDto.setWeeklyPredictedMovementTops(weeklyPredictedMovementTops);
				itemDto.setWeeklyPredictedMovementGU(weeklyPredictedMovementGU);
				/*//Set aggregated client forecast only when the item is LIG memberss
				if(isLig)
					itemDto.setClientWeeklyPredictedMovement(clientWeeklyPredictedMovement);*/
				itemDto.setWeeklyActualMovement(actualWeeklyMovement);
				
				logger.debug("Ret Lir Id: " + itemDto.getRetLirId() + ",WeeklyActualMovement: " + itemDto.getWeeklyActualMovement());
			}
		}
		
		if(itemDto != null){
			if(isLig)
				itemDto.setItemName(itemDto.getLirName());
			ligAndNonLigData.add(itemDto);
		}
	
	}
	
	
	public void findDayPartPrediction(OOSItemDTO oosItemDTO, HashMap<ProductKey, Double> storeLevelAvgMovMap,
			HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartLevelAvgMovMap, boolean isPerishables, List<DayPartLookupDTO> dayPartLookupDTO,
			int dayId, int dayPartId) {
		// logger.debug("** Item Code:" + oosItemDTO.getProductId() +
		// ",WeeklyPredictedMovement:"
		// + (oosItemDTO.isPersishableOrDSD() ?
		// oosItemDTO.getClientWeeklyPredictedMovement() :
		// oosItemDTO.getWeeklyPredictedMovement()));

		// Find the day part prediction of item
		double dayPartMovPercent = findDayPartMovPercent(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId(), storeLevelAvgMovMap,
				dayPartLevelAvgMovMap, dayId, dayPartId);
		oosItemDTO.setDayPartMovPercent(dayPartMovPercent);
		// Use client's forecast if the item is perishable or dsd items
		// Set client day part predicted movement
		oosItemDTO.setClientDayPartPredictedMovement(Math.round((oosItemDTO.getClientWeeklyPredictedMovement() * dayPartMovPercent) / 100));

		oosItemDTO.getOOSCriteriaData()
				.setForecastMovOfProcessingTimeSlotOfItemOrLig(findForecast(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId(),
						storeLevelAvgMovMap, dayPartLevelAvgMovMap, oosItemDTO.getClientWeeklyPredictedMovement(),
						oosItemDTO.getWeeklyPredictedMovement(), isPerishables, dayId, dayPartId));
		
		oosItemDTO.getOOSCriteriaData()
				.setLowerConfidenceOfProcessingTimeSlotOfItemOrLig(findLowerConfidence(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId(),
						storeLevelAvgMovMap, dayPartLevelAvgMovMap, oosItemDTO.getClientWeeklyPredictedMovement(),
						oosItemDTO.getWeeklyConfidenceLevelLower(), isPerishables, dayId, dayPartId));
				
		if (oosItemDTO.isPersishableOrDSD()) {
			long dayPartPredictedMovement = Math.round((oosItemDTO.getClientWeeklyPredictedMovement() * dayPartMovPercent) / 100);
			oosItemDTO.getOOSCriteriaData().setUpperConfidenceOfProcessingTimeSlotOfItemOrLig(dayPartPredictedMovement);
		} else {
			long dayPartConfidenceLevelUpper = Double.valueOf((oosItemDTO.getWeeklyConfidenceLevelUpper() * dayPartMovPercent) / 100).longValue();
			oosItemDTO.getOOSCriteriaData().setUpperConfidenceOfProcessingTimeSlotOfItemOrLig(dayPartConfidenceLevelUpper);
		}

		// Find rest of the day prediction
		// Find day part mov percent
		long restOfTheDayPredMov = 0;
		for (DayPartLookupDTO dpl : dayPartLookupDTO) {
			// look for only future time slots
			if (dpl.getDayPartId() > dayPartId) {
				// logger.debug("Future Day Part Id:" + dpl.getDayPartId());
				dayPartMovPercent = findDayPartMovPercent(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId(), storeLevelAvgMovMap,
						dayPartLevelAvgMovMap, dayId, dpl.getDayPartId());
				// Find day part predicted movement
				if (oosItemDTO.isPersishableOrDSD()) {
					restOfTheDayPredMov = restOfTheDayPredMov + Math.round((oosItemDTO.getClientWeeklyPredictedMovement() * dayPartMovPercent) / 100);
				} else {
					restOfTheDayPredMov = restOfTheDayPredMov + Math.round((oosItemDTO.getWeeklyPredictedMovement() * dayPartMovPercent) / 100);
				}
				// logger.debug("dayPartMovPercent:" + dayPartMovPercent +
				// ",restOfTheDayPredMov:" + restOfTheDayPredMov);
			}
		}
		oosItemDTO.getOOSCriteriaData().setForecastMovOfRestOfTheDayOfItemOrLig(restOfTheDayPredMov);
	}
	
	public long findLowerConfidence(int productLevelId, int productId, HashMap<ProductKey, Double> storeLevelAvgMovMap,
			HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartLevelAvgMovMap, long clientWeeklyPredictedMov,
			long prestoWeeklyLowerConfidence, boolean isPerishables, int dayId, int dayPartId) {
		long lowerConf = 0;
		// Find the day part prediction of item
		double dayPartMovPercent = findDayPartMovPercent(productLevelId, productId, storeLevelAvgMovMap, dayPartLevelAvgMovMap, dayId, dayPartId);
		// Use client's forecast if the item is perishable or dsd items
		// Set client day part predicted movement
//		logger.debug("dayPartMovPercent:" + dayPartMovPercent + "clientWeeklyPredictedMov:" + clientWeeklyPredictedMov
//				+ ",prestoWeeklyLowerConfidence:" + prestoWeeklyLowerConfidence);
		if (isPerishables) {
			long dayPartPredictedMovement = Math.round((clientWeeklyPredictedMov * dayPartMovPercent) / 100);
			lowerConf = dayPartPredictedMovement;
		} else {
			long dayPartConfidenceLevelLower = Double.valueOf((prestoWeeklyLowerConfidence * dayPartMovPercent) / 100).longValue();
			lowerConf = dayPartConfidenceLevelLower;
		}
		return lowerConf;
	}

	public long findForecast(int productLevelId, int productId, HashMap<ProductKey, Double> storeLevelAvgMovMap,
			HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartLevelAvgMovMap, long clientWeeklyPredictedMov, long prestoWeeklyPredictedMov,
			boolean isPerishables, int dayId, int dayPartId) {
		long forecastMov = 0;

		// Find the day part prediction of item
		double dayPartMovPercent = findDayPartMovPercent(productLevelId, productId, storeLevelAvgMovMap, dayPartLevelAvgMovMap, dayId, dayPartId);
		if (isPerishables) {
			long dayPartPredictedMovement = Math.round((clientWeeklyPredictedMov * dayPartMovPercent) / 100);
			forecastMov = dayPartPredictedMovement;
		} else {
			long dayPartPredictedMovement = Math.round((prestoWeeklyPredictedMov * dayPartMovPercent) / 100);
			forecastMov = dayPartPredictedMovement;
		}

		return forecastMov;
	}

	/***
	 * Find the day part movement average percent against the weekly movement
	 * 
	 * @param locationLevelAvgMov
	 * @param dayPartLevelAvgMov
	 * @return
	 */
	private double findDayPartMovPercent(int productLevelId, int productId, HashMap<ProductKey, Double> storeLevelAvgMovMap,
			HashMap<OOSDayPartItemAvgMovKey, WeeklyAvgMovement> dayPartLevelAvgMovMap, int dayId, int dayPartId) {
		double dayPartMovPercent = 0;
		double storeLevelAvgMov = 0, dayPartLevelAvgMov = 0;

		ProductKey productKey = new ProductKey(productLevelId, productId);
		OOSDayPartItemAvgMovKey oosDayPartItemAvgMovKey = new OOSDayPartItemAvgMovKey(dayId, dayPartId, productLevelId, productId);

		
		if (storeLevelAvgMovMap.get(productKey) != null)
			storeLevelAvgMov = storeLevelAvgMovMap.get(productKey);
		if (dayPartLevelAvgMovMap.get(oosDayPartItemAvgMovKey) != null)
			dayPartLevelAvgMov = dayPartLevelAvgMovMap.get(oosDayPartItemAvgMovKey).getAverageMovement();
		if (storeLevelAvgMov > 0) {
			dayPartMovPercent = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit((dayPartLevelAvgMov / storeLevelAvgMov) * 100));
		}
//		logger.debug("storeLevelAvgMov:" + storeLevelAvgMov + ",dayPartLevelAvgMov:" + dayPartLevelAvgMov);
		return dayPartMovPercent;
	}
	
	/**
	 * Groups items based on page and block
	 * @param itemList
	 * @param pageBlockMap
	 */
	public void groupItemsByPageAndBlock_old(List<OOSItemDTO> itemList, HashMap<AdKey, List<OOSItemDTO>> pageBlockMap) {
		for (OOSItemDTO oosItemDTO : itemList) {
			AdKey adKey = new AdKey(oosItemDTO.getAdPageNo(), oosItemDTO.getBlockNo());
			if (pageBlockMap.get(adKey) == null) {
				List<OOSItemDTO> items = new ArrayList<>();
				items.add(oosItemDTO);
				pageBlockMap.put(adKey, items);
			} else {
				List<OOSItemDTO> items = pageBlockMap.get(adKey);
				// Sum Presto total chain forecast, Client total chain forecast
				// and Actual mov
				OOSItemDTO repItem = items.get(0);
				repItem.setWeeklyPredictedMovement(repItem.getWeeklyPredictedMovement() + oosItemDTO.getWeeklyPredictedMovement());
				/*
				 * repItem.setClientWeeklyPredictedMovement(repItem.
				 * getClientWeeklyPredictedMovement() +
				 * oosItemDTO.getClientWeeklyPredictedMovement());
				 */
				repItem.setWeeklyActualMovement(repItem.getWeeklyActualMovement() + oosItemDTO.getWeeklyActualMovement());
				items.add(oosItemDTO);
				pageBlockMap.put(adKey, items);
			}
		}
	}
	
	public void groupItemsByPageAndBlock(List<OOSItemDTO> itemList, HashMap<AdKey, OOSItemDTO> pageBlockMap) {
		for (OOSItemDTO oosItemDTO : itemList) {
			AdKey adKey = new AdKey(oosItemDTO.getAdPageNo(), oosItemDTO.getBlockNo());
			OOSItemDTO tempItemDTO = new OOSItemDTO();
			
			if (pageBlockMap.get(adKey) != null) {
				tempItemDTO = pageBlockMap.get(adKey);
			}
			
			tempItemDTO.setWeeklyPredictedMovement(tempItemDTO.getWeeklyPredictedMovement() + oosItemDTO.getWeeklyPredictedMovement());
			tempItemDTO.setWeeklyActualMovement(tempItemDTO.getWeeklyActualMovement() + oosItemDTO.getWeeklyActualMovement());
			tempItemDTO.setClientWeeklyPredictedMovement(oosItemDTO.getClientWeeklyPredictedMovement());
			pageBlockMap.put(adKey, tempItemDTO);
		}
	}
	
	/**
	 * Ignores categories for which if there is an item with status code 3
	 * @param itemList
	 * @return List of filtered items
	 */
	public List<OOSItemDTO> filterItemsByPredictionStatus(List<OOSItemDTO> itemList){
		HashMap<String, List<OOSItemDTO>> categoryMap = new HashMap<>();
		List<OOSItemDTO> filteredItems = new ArrayList<>();
		for(OOSItemDTO oosItemDTO: itemList){
			if(categoryMap.get(oosItemDTO.getCategoryName()) == null){
				List<OOSItemDTO> items = new ArrayList<>();
				items.add(oosItemDTO);
				categoryMap.put(oosItemDTO.getCategoryName(), items);
			}
			else{
				List<OOSItemDTO> items = categoryMap.get(oosItemDTO.getCategoryName());
				items.add(oosItemDTO);
				categoryMap.put(oosItemDTO.getCategoryName(), items);
			}
		}
		
		for(Map.Entry<String, List<OOSItemDTO>> entry: categoryMap.entrySet()){
			boolean canBeAdded = true;
			for(OOSItemDTO oosItemDTO: entry.getValue()){
				if(oosItemDTO.getWeeklyPredictionStatus() == 
						PredictionStatus.ERROR_NO_MOV_DATA_SPECIFIC_UPC.getStatusCode()
						|| oosItemDTO.getWeeklyPredictionStatus() == 
						PredictionStatus.UNDEFINED.getStatusCode()
						|| oosItemDTO.getWeeklyPredictionStatus() == 
						PredictionStatus.ERROR_CAL_CURVE_AVERAGE_1.getStatusCode()){
					canBeAdded = false;
				}
			}
			
			if(canBeAdded){
				filteredItems.addAll(entry.getValue());
			}
			else{
				logger.warn("Category " + entry.getKey() + " is ignored.");
			}
		}
		
		return filteredItems;
	}
}
