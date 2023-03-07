package com.pristine.customer;

import java.sql.Connection;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.customer.HouseHoldDetailsDAO;
import com.pristine.dto.customer.HouseHoldDetailsDTO;
import com.pristine.dto.customer.HouseHoldMonthAndItemKey;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class HouseHoldDetails {
	private static Logger logger = Logger.getLogger("HouseHoldDetails");
	private Connection conn = null;
//	String storeId;
	HashMap<String, List<HouseHoldDetailsDTO>> householdInfo;
	List<HouseHoldDetailsDTO> activeItemsPerMonth;
	HashMap<String, List<HouseHoldDetailsDTO>> allItemBoughtByHH;
	HashMap<String, List<HouseHoldDetailsDTO>> noOfVisitsBasedOnHH;
	HouseHoldDetailsDAO houseHoldDetailsDAO = new HouseHoldDetailsDAO();
	private static String[] storeIds = null;
	private final static String STORE_ID = "STORE_ID=";
	private int processRecCount = 0;
	DecimalFormat df = new DecimalFormat("####0.00");
	
	public HouseHoldDetails() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error("Error when creating connection - " + gex);
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-houseHold-item-details.properties");
		PropertyManager.initialize("analysis.properties");
		HouseHoldDetails houseHoldDetails = new HouseHoldDetails();
		for (String arg : args) {
			if (arg.startsWith(STORE_ID)) {
				String storeId = arg.substring(STORE_ID.length());
				storeIds = storeId.split(",");
			}
		}
		try {
			
			houseHoldDetails.processHouseHoldInfo();
		} catch (GeneralException e) {
			e.printStackTrace();
		}
	}

	private void processHouseHoldInfo() throws GeneralException {
		
		logger.info("Getting Active items details based each month");
		activeItemsPerMonth = houseHoldDetailsDAO.getActiveItemsPerMonth(conn);
		

		for (String storeId : storeIds) {
			allItemBoughtByHH = new HashMap<String, List<HouseHoldDetailsDTO>>();
			noOfVisitsBasedOnHH = new HashMap<String, List<HouseHoldDetailsDTO>>();
			logger.info("Getting no of visits for each household based on month");
			noOfVisitsBasedOnHH = houseHoldDetailsDAO.getNoOfVisitsBasedOnHH(storeId, conn);
			logger.info("getting customer id for the Store: "+storeId);
				processHouseHoldDetails(storeId);
		}
	}

	private void processHouseHoldDetails(String storeId) throws GeneralException {
		int recCount =0;
		//Cache all basic details
		getHouseholdDetails(storeId);
		// Group based on month and Item using Item metric summary weekly table
		List<HouseHoldDetailsDTO> listOfHouseholdDetails = new ArrayList<HouseHoldDetailsDTO>();
		HashMap<HouseHoldMonthAndItemKey, HouseHoldDetailsDTO> monthAndItemDetailsUsignIMS = groupBasedOnMonthAndItem(activeItemsPerMonth);
		// To get Price index based on items for Each household
		// By looping household, create HashMap with key as Month and item
		for (Map.Entry<String, List<HouseHoldDetailsDTO>> householdDetailsEntry : householdInfo.entrySet()) {
			// Get previous month price for each items based on month by looping each Household
			HashMap<String,Long> visitCountBasedOnMonth = new HashMap<String,Long>();
			List<HouseHoldDetailsDTO> noOfVisitsBasedOnMonth = null;
			if(noOfVisitsBasedOnHH.containsKey(householdDetailsEntry.getKey())){
				noOfVisitsBasedOnMonth = noOfVisitsBasedOnHH.get(householdDetailsEntry.getKey());
				logger.debug("Visit count found for the HouseHold: "+householdDetailsEntry.getKey());
			}else{
				logger.error("No of visit is not found for HH: "+householdDetailsEntry.getKey());
			}
			//Group visits based on each month
			visitCountBasedOnMonth = groupVisitsBasedOnMonth(noOfVisitsBasedOnMonth);
			logger.debug("Getting previous month item price");
			HashMap<String, List<HouseHoldDetailsDTO>> estItemPriceBasedMonth = setCurrentAndPrevMonthItemPrice(
					householdDetailsEntry.getValue(), monthAndItemDetailsUsignIMS);
			// Calculate PI and Other details in month level.
			logger.debug("Summarizing item price in Household level");
			HashMap<String, HouseHoldDetailsDTO> aggrItemDetailsBasedOnMonth = summarizeItemsInMonthLevel(
					estItemPriceBasedMonth,visitCountBasedOnMonth);
			// Assign number of items bought by Household overall in the
			// store
			logger.debug("Assigning Active items and its count");
			setActiveAndOverallItems(aggrItemDetailsBasedOnMonth, monthAndItemDetailsUsignIMS,
					householdDetailsEntry.getKey());
			
			for (Map.Entry<String, HouseHoldDetailsDTO> entry : aggrItemDetailsBasedOnMonth.entrySet()) {
				HouseHoldDetailsDTO houseHoldDetailsDTO = entry.getValue();
				listOfHouseholdDetails.add(houseHoldDetailsDTO);
				recCount++;
//				processRecCount = processRecCount + listOfHouseholdDetails.size();
				if (recCount % 100000 == 0) {
					processRecCount = processRecCount + listOfHouseholdDetails.size();
					logger.info("No of records completed :" + processRecCount);
					houseHoldDetailsDAO.saveHouseHoldItemDetails(listOfHouseholdDetails, conn);
					PristineDBUtil.commitTransaction(conn, "Data Load");
					listOfHouseholdDetails.clear();
					recCount=0;
				}
			}
		}
		if (listOfHouseholdDetails.size() > 0) {
			processRecCount = processRecCount + listOfHouseholdDetails.size();
			logger.info("No of records Processed :" + processRecCount);
			houseHoldDetailsDAO.saveHouseHoldItemDetails(listOfHouseholdDetails, conn);
			PristineDBUtil.commitTransaction(conn, "Data Load");
		}
	}

	private void setActiveAndOverallItems(HashMap<String, HouseHoldDetailsDTO> aggrItemDetailsBasedOnMonth,
			HashMap<HouseHoldMonthAndItemKey, HouseHoldDetailsDTO> monthAndItemDetailsUsignIMS,
			String houseHoldNumber)throws GeneralException {
		for (Map.Entry<String, HouseHoldDetailsDTO> aggrItemDetails : aggrItemDetailsBasedOnMonth.entrySet()) {
			String currentMonth = aggrItemDetails.getKey();
			HouseHoldDetailsDTO houseHoldDetailsDTO = aggrItemDetails.getValue();
			houseHoldDetailsDTO.setHouseholdNumber(houseHoldNumber);
			List<HouseHoldDetailsDTO> listOfItems = allItemBoughtByHH.get(houseHoldNumber);
			// Set count of Over all items bought by each household
			houseHoldDetailsDTO.setTotalItemsByHH(listOfItems.size());
			int activeItems = 0;
			for (HouseHoldDetailsDTO houseHoldDetailsDTO1 : listOfItems) {
				HouseHoldMonthAndItemKey key = new HouseHoldMonthAndItemKey(currentMonth,
						houseHoldDetailsDTO1.getItemId());
				if (monthAndItemDetailsUsignIMS.containsKey(key)) {
					activeItems++;
				}
			}
			// Set active items
			houseHoldDetailsDTO.setActiveItemsPerMonth(activeItems);
		}
	}

	private HashMap<String, HouseHoldDetailsDTO> summarizeItemsInMonthLevel(
			HashMap<String, List<HouseHoldDetailsDTO>> itemDetailsBasedOnMonth,HashMap<String,Long> visitCountBasedOnMonth) throws GeneralException {
		HashMap<String, HouseHoldDetailsDTO> aggrDetailsbasedOnMonth = new HashMap<String, HouseHoldDetailsDTO>();
		for (Map.Entry<String, List<HouseHoldDetailsDTO>> itemLevelDetails : itemDetailsBasedOnMonth.entrySet()) {
			HouseHoldDetailsDTO houseHoldDetailsDTO = new HouseHoldDetailsDTO();
			// Set Month info using Key
			houseHoldDetailsDTO.setMonthInfo(itemLevelDetails.getKey());
			List<HouseHoldDetailsDTO> houseHoldDetailsDTOs = itemLevelDetails.getValue();
			Double prevMonthItemPrice = 0d;
			HashSet<String> listOfDistinctItems = new HashSet<String>();
			for (HouseHoldDetailsDTO houseHoldDetailsDTO2 : houseHoldDetailsDTOs) {
				// First transaction
				houseHoldDetailsDTO.setHouseHoldFirstTrans(houseHoldDetailsDTO2.getHouseHoldFirstTrans());
				// Store id
				houseHoldDetailsDTO.setStoreId(houseHoldDetailsDTO2.getStoreId());
				// To set Total amount spend by each house hold
				houseHoldDetailsDTO.setAmountSpendByHH(((houseHoldDetailsDTO.getAmountSpendByHH() != null)
						? houseHoldDetailsDTO.getAmountSpendByHH() : 0) + houseHoldDetailsDTO2.getAmountSpendByHH());
				if(isNumeric(String.valueOf(houseHoldDetailsDTO.getAmountSpendByHH())) && isParseable(houseHoldDetailsDTO.getAmountSpendByHH())){
					houseHoldDetailsDTO.setAmountSpendByHH(Double.parseDouble(df.format(houseHoldDetailsDTO.getAmountSpendByHH())));
				}else{
					logger.error("Actual amt is not available for the HH :"+houseHoldDetailsDTO.getHouseholdNumber()+
							" And Store Id: "+houseHoldDetailsDTO.getStoreId()+" And Current Amt spend by USD: "
							+houseHoldDetailsDTO.getAmountSpendByHH());
					houseHoldDetailsDTO.setAmountSpendByHH(0.0);
				}
				// Total quantity
				houseHoldDetailsDTO.setQuantity(houseHoldDetailsDTO.getQuantity() + houseHoldDetailsDTO2.getQuantity());
				// Total Weight
				houseHoldDetailsDTO.setWeight(((houseHoldDetailsDTO.getWeight() != null)
						? houseHoldDetailsDTO.getWeight() : 0)+houseHoldDetailsDTO2.getWeight());
//				houseHoldDetailsDTO
//						.setNoOfVisits(houseHoldDetailsDTO.getNoOfVisits() + houseHoldDetailsDTO2.getNoOfVisits());
				// Private Label item count
				houseHoldDetailsDTO.setPlItems(houseHoldDetailsDTO.getPlItems() + houseHoldDetailsDTO2.getPlItems());
			
				listOfDistinctItems.add(houseHoldDetailsDTO2.getItemId());
				prevMonthItemPrice += houseHoldDetailsDTO2.getEstimatedItemPrice();
			}
			// To calculate Price Index Amt spend in Current month divided by
			// estimated amount spend for the qty previous month and multiply by
			// 100
			houseHoldDetailsDTO.setNoOfVisits(visitCountBasedOnMonth.get(itemLevelDetails.getKey()));
			houseHoldDetailsDTO.setItemsPI(((houseHoldDetailsDTO.getAmountSpendByHH() - prevMonthItemPrice) / prevMonthItemPrice) * 100);
			Double error = houseHoldDetailsDTO.getItemsPI();
			if(isNumeric(String.valueOf(houseHoldDetailsDTO.getItemsPI())) && isParseable(houseHoldDetailsDTO.getItemsPI())){
				houseHoldDetailsDTO.setItemsPI(Double.parseDouble(df.format(houseHoldDetailsDTO.getItemsPI())));
			}else{
				logger.debug("Price index is not available for the HH :"+houseHoldDetailsDTO.getHouseholdNumber()+
						" And Previous month amt: "+prevMonthItemPrice+" And Current Amt spend by USD: "
						+houseHoldDetailsDTO.getAmountSpendByHH()+" Error value is  "+error);
				houseHoldDetailsDTO.setItemsPI(0.00);
			}
			if(isNumeric(String.valueOf(prevMonthItemPrice)) && isParseable(prevMonthItemPrice)){
				houseHoldDetailsDTO.setEstimatedItemPrice(Double.parseDouble(df.format(prevMonthItemPrice)));
			}else{
				logger.debug("Est amt is not available for the HH :"+houseHoldDetailsDTO.getHouseholdNumber()+
						" And Store Id: "+houseHoldDetailsDTO.getStoreId()+" And Current Amt spend by USD: "
						+houseHoldDetailsDTO.getAmountSpendByHH());
				houseHoldDetailsDTO.setEstimatedItemPrice(houseHoldDetailsDTO.getAmountSpendByHH());
			}
			if(isNumeric(String.valueOf(houseHoldDetailsDTO.getWeight())) && isParseable(houseHoldDetailsDTO.getWeight())){
				houseHoldDetailsDTO.setWeight(Double.parseDouble(df.format(houseHoldDetailsDTO.getWeight())));
			}
			houseHoldDetailsDTO.setNoOfItemsInMonth(listOfDistinctItems.size());
			aggrDetailsbasedOnMonth.put(houseHoldDetailsDTO.getMonthInfo(), houseHoldDetailsDTO);
		}
			return aggrDetailsbasedOnMonth;

	}

	private HashMap<String, List<HouseHoldDetailsDTO>> setCurrentAndPrevMonthItemPrice(List<HouseHoldDetailsDTO> itemDetailsList,
			HashMap<HouseHoldMonthAndItemKey, HouseHoldDetailsDTO> monthAndItemDetailsUsignIMS)
			throws GeneralException {
		// create HashMap with key as Month and item and value with item details
		HashMap<HouseHoldMonthAndItemKey, HouseHoldDetailsDTO> monthAndItemBasedDetails = groupBasedOnMonthAndItem(itemDetailsList);
		// group by each month
		HashMap<String, List<HouseHoldDetailsDTO>> itemsGroupedBasedOnMonth = new HashMap<String, List<HouseHoldDetailsDTO>>();
		try{
		for (HouseHoldDetailsDTO houseHoldDetailsDTO : itemDetailsList) {
			List<HouseHoldDetailsDTO> houseHoldDetailsDTOs = new ArrayList<HouseHoldDetailsDTO>();
			if (itemsGroupedBasedOnMonth.containsKey(houseHoldDetailsDTO.getMonthInfo())) {
				houseHoldDetailsDTOs.addAll(itemsGroupedBasedOnMonth.get(houseHoldDetailsDTO.getMonthInfo()));
			}
			houseHoldDetailsDTOs.add(houseHoldDetailsDTO);
			itemsGroupedBasedOnMonth.put(houseHoldDetailsDTO.getMonthInfo(), houseHoldDetailsDTOs);
		}

		// Calculate Previous Month Item price by looping each month
		for (Map.Entry<String, List<HouseHoldDetailsDTO>> itemDetailBasedOnMonth : itemsGroupedBasedOnMonth
				.entrySet()) {
			String processingMonth = itemDetailBasedOnMonth.getKey();
			String previousMonth = getPreviousMonth(processingMonth);
			for (HouseHoldDetailsDTO houseHoldDetailsDTO : itemDetailBasedOnMonth.getValue()) {
				String itemId = houseHoldDetailsDTO.getItemId();
				HouseHoldMonthAndItemKey currentMonth = new HouseHoldMonthAndItemKey(processingMonth, itemId);
				HouseHoldMonthAndItemKey prevMonthkey = new HouseHoldMonthAndItemKey(previousMonth, itemId);
				//Set current month Amount Spend using Unit price and qty or weight
				if (monthAndItemDetailsUsignIMS.containsKey(currentMonth)) {
					HouseHoldDetailsDTO houseHoldDetailsDTO2 = monthAndItemDetailsUsignIMS.get(currentMonth);
					Double currentMonthAmtSpend;
					if(houseHoldDetailsDTO.getWeight() != null && houseHoldDetailsDTO.getWeight()>0){
						currentMonthAmtSpend = (houseHoldDetailsDTO2.getFinalPrice()* houseHoldDetailsDTO.getWeight());
					}else{
						currentMonthAmtSpend = (houseHoldDetailsDTO2.getFinalPrice()* houseHoldDetailsDTO.getQuantity());
					}
					houseHoldDetailsDTO.setAmountSpendByHH(currentMonthAmtSpend);
				}
				// If item is available then calculate price using qty..
				if (monthAndItemDetailsUsignIMS.containsKey(prevMonthkey)) {
					// If item available in Previous month then use
					HouseHoldDetailsDTO houseHoldDetailsDTO3 = monthAndItemDetailsUsignIMS.get(prevMonthkey);
					//If the Weight is available then consider weight else qty
					Double estimatedAmtPreviousMonth;
					if(houseHoldDetailsDTO.getWeight() != null && houseHoldDetailsDTO.getWeight()>0){
						estimatedAmtPreviousMonth = (houseHoldDetailsDTO3.getFinalPrice()* houseHoldDetailsDTO.getWeight());
					}else{
						estimatedAmtPreviousMonth = (houseHoldDetailsDTO3.getFinalPrice()* houseHoldDetailsDTO.getQuantity());
					}
					houseHoldDetailsDTO.setEstimatedItemPrice(estimatedAmtPreviousMonth);
					logger.debug("Previous month price: " + estimatedAmtPreviousMonth);
				}
				if(houseHoldDetailsDTO.getEstimatedItemPrice() == null || houseHoldDetailsDTO.getEstimatedItemPrice() == 0) {
					logger.debug("Previous month is not available for household: "+houseHoldDetailsDTO.getHouseholdNumber()+" Item Id: "+itemId+
							" Previous month: "+previousMonth);
					houseHoldDetailsDTO.setEstimatedItemPrice(houseHoldDetailsDTO.getAmountSpendByHH());
				}
				//To set the est price if difference between the current and Previous month is lesser than or greater than 50% and -50%.
				Double itemPriceIndex = (((houseHoldDetailsDTO.getAmountSpendByHH()-houseHoldDetailsDTO.getEstimatedItemPrice())/
						houseHoldDetailsDTO.getEstimatedItemPrice())*100);
				if(itemPriceIndex < -50 || itemPriceIndex >50){
					houseHoldDetailsDTO.setEstimatedItemPrice(houseHoldDetailsDTO.getAmountSpendByHH());
				}

			}
		}
		return itemsGroupedBasedOnMonth;
		}catch (GeneralException e){
			logger.error("Error while processing setCurrentAndPrevMonthItemPrice - " + e);
			throw new GeneralException("Error while processing setCurrentAndPrevMonthItemPrice", e);
		}
	}

	private String getPreviousMonth(String monthInfo) throws GeneralException {
		String previousMonth = null;
		String currentMonth = monthInfo.replace("-", "/");
		currentMonth = "01/" + currentMonth;
		logger.debug("Given date: " + currentMonth);
		Date date = DateUtil.toDate(currentMonth, "dd/MM/yy");
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.MONTH, -1);
		Date date1 = cal.getTime();
		SimpleDateFormat format = new SimpleDateFormat("MM/YY");
		previousMonth = format.format(date1).replace("/", "-");
		logger.debug("After change date: " + previousMonth);

		return previousMonth;
	}

	private HashMap<HouseHoldMonthAndItemKey, HouseHoldDetailsDTO> groupBasedOnMonthAndItem(
			List<HouseHoldDetailsDTO> itemDetailsList) {
		HashMap<HouseHoldMonthAndItemKey, HouseHoldDetailsDTO> groupedByMonthAndItems = new HashMap<HouseHoldMonthAndItemKey, HouseHoldDetailsDTO>();
		for (HouseHoldDetailsDTO houseHoldDetailsDTO : itemDetailsList) {
			HouseHoldMonthAndItemKey key = new HouseHoldMonthAndItemKey(houseHoldDetailsDTO.getMonthInfo(),
					houseHoldDetailsDTO.getItemId());
			groupedByMonthAndItems.put(key, houseHoldDetailsDTO);
		}
		return groupedByMonthAndItems;

	}
	
	private HashMap<String, Long> groupVisitsBasedOnMonth(
			List<HouseHoldDetailsDTO> itemDetailsList) {
		HashMap<String,Long> groupedVisitsBasedOnMonth = new HashMap<String, Long>();
		for (HouseHoldDetailsDTO houseHoldDetailsDTO : itemDetailsList) {
			groupedVisitsBasedOnMonth.put(houseHoldDetailsDTO.getMonthInfo(), houseHoldDetailsDTO.getNoOfVisits());
		}
		return groupedVisitsBasedOnMonth;

	}
	
	private void getHouseholdDetails(String storeId) throws GeneralException {
		// get basic details
		logger.info("Getting household item details");
		householdInfo = houseHoldDetailsDAO.getHoulseholdItemDetails(storeId, conn);
		// get active items for each month with their final price
		
		// GET TOTAL NUMBER OF ITEMS COUNT PURCHASED BY EACH HOUSE HOLD
		logger.info("Getting overall items bought by Household");
		allItemBoughtByHH = houseHoldDetailsDAO.getAllItemsBoughtByHH(storeId, conn);
		logger.info("Household item details cache completed");
	}
	/**
	 * To check string is Number or not
	 * @param str
	 * @return
	 */
	public static boolean isNumeric(String str){  
	  try  
	  {  
	    double d = Double.parseDouble(str);  
	  }  
	  catch(NumberFormatException nfe)  
	  {  
	    return false;  
	  }  
	  return true;  
	}
	
	private boolean isParseable(Double value){
		try{
			double d = Double.parseDouble(df.format(value));
		}
		catch(Exception nfe)  
		  {  
		    return false;  
		  }  
		return true;
	}
}


