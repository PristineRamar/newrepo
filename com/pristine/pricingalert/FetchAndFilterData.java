package com.pristine.pricingalert;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.pricingalert.AlertTypesDAO;
import com.pristine.dao.pricingalert.GoalSettingsDAO;
import com.pristine.dao.pricingalert.LocationCompetitorMapDAO;
import com.pristine.dao.pricingalert.PricingAlertDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.pricingalert.AlertTypesDto;
import com.pristine.dto.pricingalert.GoalSettingsDTO;
import com.pristine.dto.pricingalert.LocationCompetitorMapDTO;
import com.pristine.dto.pricingalert.PAItemInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class FetchAndFilterData {

	private static Logger logger = Logger.getLogger("PAFetchAndFilterData");
	
	private Connection connection = null; // DB connection
	
	private LocationCompetitorMapDTO inputData = null;
	private LocationCompetitorMapDAO locCompMapDAO = null;
	private AlertTypesDAO alertTypesDAO = null;
	private int avgRevenueDays = 0;
	private int weekCalendarId = 0;
	private double priceChangeThreshold = 0;
	private double costChangeThreshold = 0;
	private String weekEndDate = null;
	private ArrayList<String> ligAlertCodes = null;
	
	public FetchAndFilterData() {
		PropertyManager.initialize("analysis.properties");
		locCompMapDAO = new LocationCompetitorMapDAO();
		alertTypesDAO = new AlertTypesDAO();
		priceChangeThreshold = Double.parseDouble(PropertyManager.getProperty("PA_FETCHANDFILTER.PRICE_CHANGE_THRESHOLD"));
		costChangeThreshold = Double.parseDouble(PropertyManager.getProperty("PA_FETCHANDFILTER.COST_CHANGE_THRESHOLD"));
		try {
			connection = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			logger.info("Pricing Alert - FetchAndFilterData Ends unsucessfully");
			System.exit(1);
		}
		
		setLIGAlertCodes();
	}

	public FetchAndFilterData(LocationCompetitorMapDTO locCompMapDto, String weekEndDate) {
		this();
		this.inputData = locCompMapDto;
		this.weekEndDate = weekEndDate;
	}
	
	/**
	 * Main method
	 * If program has input parameters, BASE_LOCATION_LEVEL_ID is mandatory
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-pa-fetchandfilterdata.properties");
		
		FetchAndFilterData dataLoad = null;
		if(args.length > 0){
			LocationCompetitorMapDTO inputData = new LocationCompetitorMapDTO();
			int baseLocationLevelId = 0;
			int baseLocationId = 0;
			int compLocationLevelId = 0;
			int compLocationId = 0;
			int productLevelId = 0;
			int productId = 0;
			String weekEndDate = null;
			
			logCommand(args);
			
			for(String arg : args){
				try{
					if(arg.startsWith("BASE_LOCATION_LEVEL_ID")){
						baseLocationLevelId = Integer.parseInt(arg.substring("BASE_LOCATION_LEVEL_ID=".length()));
						inputData.setBaseLocationLevelId(baseLocationLevelId);
					}
					if(arg.startsWith("BASE_LOCATION_ID")){
						baseLocationId = Integer.parseInt(arg.substring("BASE_LOCATION_ID=".length()));
						inputData.setBaseLocationId(baseLocationId);
					}
					if(arg.startsWith("COMP_LOCATION_LEVEL_ID")){
						compLocationLevelId = Integer.parseInt(arg.substring("COMP_LOCATION_LEVEL_ID=".length()));
						inputData.setCompLocationLevelId(compLocationLevelId);
					}
					if(arg.startsWith("COMP_LOCATION_ID")){
						compLocationId = Integer.parseInt(arg.substring("COMP_LOCATION_ID=".length()));
						inputData.setCompLocationId(compLocationId);
					}
					if(arg.startsWith("PRODUCT_LEVEL_ID")){
						productLevelId = Integer.parseInt(arg.substring("PRODUCT_LEVEL_ID=".length()));
						inputData.setProductLevelId(productLevelId);
					}
					if(arg.startsWith("PRODUCT_ID")){
						productId = Integer.parseInt(arg.substring("PRODUCT_ID=".length()));
						inputData.setProductId(productId);
					}
					if(arg.startsWith("WEEK_END_DATE")){
						weekEndDate = arg.substring("WEEK_END_DATE=".length());
					}
				}catch(NumberFormatException exception){
					logger.error("Invalid numeric argument passed as input");
					System.exit(-1);
				}
			}
			
			// TODO : Check if either BaseLocationLevelId or WeekEndDate can be mandatory
			if(baseLocationLevelId == 0 && weekEndDate == null){
				logger.error("BASE_LOCATION_LEVEL_ID is mandatory");
				System.exit(-1);
			}
			
			dataLoad = new FetchAndFilterData(inputData, weekEndDate);
		}else{
			dataLoad = new FetchAndFilterData();
		}
		 
		dataLoad.process();
	}
	
	public void process(){
		ArrayList<LocationCompetitorMapDTO> processList = getDataFetchCriteria();
		RetailCalendarDAO retailCalDAO = new RetailCalendarDAO();
		// Get all alerts
		ArrayList<AlertTypesDto> alertList = getAlertTypes();
		
		int baseMaxRange = getMaximumBaseDataLookupRange(alertList);
		int compMaxRange = getMaximumCompDataLookupRange(alertList);
		
		// Get last week date if not specified
		String weekStartDate = null;
		Date weekStartDt = null;
		try{
			if(weekEndDate == null){
				weekStartDate = DateUtil.getWeekStartDate(1);
			}else{
				weekStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(weekEndDate), 0);
			}
			weekStartDt = DateUtil.toDate(weekStartDate);
			RetailCalendarDTO calDTO = retailCalDAO.getCalendarId(connection, weekStartDate, Constants.CALENDAR_WEEK);
			weekCalendarId = calDTO.getCalendarId();
		}catch(GeneralException exception){
			logger.error("Error when parsing date" + exception);
		}
		
		ArrayList<String> processedList = new ArrayList<String>(); // Holds the combinations that were processed already
		
		// For each location and each product
		for(LocationCompetitorMapDTO locationCompetitorMap : processList){
			PricingAlertDAO pricingAlertDAO = new PricingAlertDAO();
			// TODO : Check THis
			/*if(locationCompetitorMap.getProductLevelId() == 0){
				int defaultProductLevelId = Integer.parseInt(PropertyManager.getProperty("PA_FETCHANDFILTER.DEFAULT_PRODUCT_LEVEL_ID"));
				locationCompetitorMap.setProductLevelId(defaultProductLevelId);
			}*/
			
			// Check if this combination is processed already
			String checkString = locationCompetitorMap.getBaseLocationLevelId() + "-" + locationCompetitorMap.getBaseLocationId() + "-" + locationCompetitorMap.getCompLocationLevelId() + "-" + locationCompetitorMap.getCompLocationId() + "-" + locationCompetitorMap.getProductLevelId();
			if(processedList.contains(checkString)){
				continue;
			}else{
				if(locationCompetitorMap.getProductId() > 0){
					checkString = checkString + "-" + locationCompetitorMap.getProductId();
					if(processedList.contains(checkString)){
						continue;
					}
				}
			}
			
			// TODO : Can there be a record with null price check list id
			ArrayList<Integer> kviItemList = null;
			try{
				kviItemList = pricingAlertDAO.getKVIItems(connection, locationCompetitorMap.getPriceCheckListId());
			}catch(GeneralException exception){
				logger.error("Unable to retrieve KVI Items for location level id " + locationCompetitorMap.getBaseLocationLevelId() + 
						", location id " + locationCompetitorMap.getBaseLocationId() + " - " + exception);
			}
			
			try{
				logger.info("Location Id - " + locationCompetitorMap.getBaseLocationId());
				logger.info("Product Level Id - " + locationCompetitorMap.getProductLevelId());
				logger.info("Product Id - " + locationCompetitorMap.getProductId());
				
				HashMap<Integer, Integer> paMasterDataIdMap = pricingAlertDAO.getPAMasterIdInfo(connection, weekCalendarId, locationCompetitorMap.getLocationCompetitorMapId());
				
				// Get base store price
				HashMap<Integer, ArrayList<CompetitiveDataDTO>> baseStorePriceData = pricingAlertDAO.getPriceData(connection, locationCompetitorMap, weekStartDate, baseMaxRange, false);
				logger.info("Base store price data fetch complete");
				
				// Get base store cost
				HashMap<Integer, ArrayList<MovementWeeklyDTO>> baseStoreCostData = pricingAlertDAO.getCostData(connection, locationCompetitorMap, weekStartDate, baseMaxRange);
				logger.info("Base store cost data fetch complete");
				
				// Get comp store price
				HashMap<Integer, ArrayList<CompetitiveDataDTO>> compStorePriceData = pricingAlertDAO.getPriceData(connection, locationCompetitorMap, weekStartDate, compMaxRange, true);
				logger.info("Comp store price data fetch complete");
				
				LocationCompetitorMapDTO otherCompetitor = locCompMapDAO.getOtherCompetitorInfo(connection, locationCompetitorMap);
				otherCompetitor.setProductLevelId(locationCompetitorMap.getProductLevelId());
				otherCompetitor.setProductId(locationCompetitorMap.getProductId());
				logger.debug("Other Competitor " + otherCompetitor.getCompLocationLevelId() + "\t" + otherCompetitor.getCompLocationId());
				HashMap<Integer, ArrayList<CompetitiveDataDTO>> compStore2PriceData = pricingAlertDAO.getPriceData(connection, otherCompetitor, weekStartDate, Constants._13WEEK , true);
				logger.info("Comp store2 price data fetch complete");
				
				// Build ret lir map
				HashMap<Integer, ArrayList<Integer>> retLirMap = new HashMap<Integer, ArrayList<Integer>>();
				for(ArrayList<CompetitiveDataDTO> compDataList : baseStorePriceData.values()){
					for(CompetitiveDataDTO compData : compDataList){
						if(compData.lirId > 0){
							if(retLirMap.get(compData.lirId) != null){
								ArrayList<Integer> tList = retLirMap.get(compData.lirId);
								tList.add(compData.itemcode);
								retLirMap.put(compData.lirId, tList);
							}else{
								ArrayList<Integer> tList = new ArrayList<Integer>();
								tList.add(compData.itemcode);
								retLirMap.put(compData.lirId, tList);
							}
						}
					}
				}
				/*for(ArrayList<CompetitiveDataDTO> compDataList : compStorePriceData.values()){
					for(CompetitiveDataDTO compData : compDataList){
						if(compData.lirId > 0){
							if(retLirPriceMap.get(compData.lirId) != null){
								ArrayList<CompetitiveDataDTO> tList = retLirPriceMap.get(compData.lirId);
								tList.add(compData);
								retLirPriceMap.put(compData.lirId, tList);
							}else{
								ArrayList<CompetitiveDataDTO> tList = new ArrayList<CompetitiveDataDTO>();
								tList.add(compData);
								retLirPriceMap.put(compData.lirId, tList);
							}
						}
					}
				}*/
				
				// Get items available in zone
				ArrayList<Integer> itemsInZone = pricingAlertDAO.getAllItemsAtProductLevel(connection, locationCompetitorMap.getBaseLocationLevelId(), locationCompetitorMap.getBaseLocationId(), baseStorePriceData.keySet());
				
				//TODO : Future Price needs to be retrieved
				
				// Holds Not Processed alerts
				HashMap<String, AlertTypesDto> unprocessedAlerts = new HashMap<String, AlertTypesDto>();
				
				ArrayList<PAItemInfoDTO> insertList = new ArrayList<PAItemInfoDTO>();
				
				// Loop through every item
				HashMap<Integer, Double> avgRevenueMap = new HashMap<Integer, Double>();
				
				GoalSettingsDAO goalDAO = new GoalSettingsDAO();
				GoalSettingsDTO goalSettingsDTO = goalDAO.getGoalDetails(connection, locationCompetitorMap, itemsInZone);
				ArrayList<Integer> primaryList = null;
				ArrayList<Integer> secondaryList = null;
				int primaryCheckListId = -1;
				int secondaryCheckListId = -1;
				
				for(Integer itemCode : baseStorePriceData.keySet()){
					if(!itemsInZone.contains(itemCode)){
						continue;
					}
					
					// Added for TOPS (To support KVI and K2 display)
					try{
						if(PropertyManager.getProperty("PA_EXPORTTOEXCEL.PRIMARY_LIST") != null){
							primaryCheckListId = pricingAlertDAO.getPriceCheckListId(connection, PropertyManager.getProperty("PA_EXPORTTOEXCEL.PRIMARY_LIST"));
							primaryList = pricingAlertDAO.getKVIItems(connection, primaryCheckListId);
						}
						if(PropertyManager.getProperty("PA_EXPORTTOEXCEL.SECONDARY_LIST") != null){
							secondaryCheckListId = pricingAlertDAO.getPriceCheckListId(connection, PropertyManager.getProperty("PA_EXPORTTOEXCEL.SECONDARY_LIST"));
							secondaryList = pricingAlertDAO.getKVIItems(connection, secondaryCheckListId);
						}
					}catch(GeneralException ge){
						logger.error("Error when retrieving items for price check list - " + ge.toString());
					}
					// Added for TOPS (To support KVI and K2 display) - Ends
										
					/* HashMaps that maintain price/cost based on data lookup range so that price/cost need not be determined again
					 * if they are computed already
					 */
					HashMap<Integer, PAItemInfoDTO> baseCurPrice = new HashMap<Integer, PAItemInfoDTO>();
					HashMap<Integer, PAItemInfoDTO> basePrePrice = new HashMap<Integer, PAItemInfoDTO>();
					//HashMap<Integer, PAItemInfoDTO> baseFutPrice = new HashMap<Integer, PAItemInfoDTO>();
					
					HashMap<Integer, PAItemInfoDTO> baseCurCost = new HashMap<Integer, PAItemInfoDTO>();
					HashMap<Integer, PAItemInfoDTO> basePreCost = new HashMap<Integer, PAItemInfoDTO>();
					//HashMap<Integer, PAItemInfoDTO> baseFutCost = new HashMap<Integer, PAItemInfoDTO>();
					
					HashMap<Integer, PAItemInfoDTO> compCurPrice = new HashMap<Integer, PAItemInfoDTO>();
					HashMap<Integer, PAItemInfoDTO> compPrePrice = new HashMap<Integer, PAItemInfoDTO>();
					
					HashMap<Integer, PAItemInfoDTO> comp2CurPrice = new HashMap<Integer, PAItemInfoDTO>();
					HashMap<Integer, PAItemInfoDTO> comp2PrePrice = new HashMap<Integer, PAItemInfoDTO>();
					
					ArrayList<CompetitiveDataDTO> retLirCompPriceData = new ArrayList<CompetitiveDataDTO>();
					ArrayList<CompetitiveDataDTO> retLirComp2PriceData = new ArrayList<CompetitiveDataDTO>();
					int retLirId = baseStorePriceData.get(itemCode).get(0).lirId;
					if(retLirId > 0){
						ArrayList<Integer> retLirItemList = retLirMap.get(retLirId);
						for(Integer itemCodeTemp : retLirItemList){
							if(compStorePriceData.get(itemCodeTemp) != null)
								retLirCompPriceData.addAll(compStorePriceData.get(itemCodeTemp));
							
							if(compStore2PriceData.get(itemCodeTemp) != null)
								retLirComp2PriceData.addAll(compStore2PriceData.get(itemCodeTemp));
						}
					}
					
					double compCurPriceInBaseRange = Constants.DEFAULT_NA; // Used in KVI003
					boolean compCurPriceInBaseRangeComputed = false;
					
					// Loop through every alert type
					for(AlertTypesDto alert : alertList){
						if(ligAlertCodes.contains(alert.getTechnicalCode())){
							unprocessedAlerts.put(alert.getTechnicalCode(), alert);
							continue;
						}
						PAItemInfoDTO paItemInfoDTO = new PAItemInfoDTO();
						paItemInfoDTO.setItemCode(itemCode);
						
						// Set KVI flag
						if(kviItemList.contains(itemCode))
							paItemInfoDTO.setKVIItem(true);
						
						// Set Average Revenue
						if(avgRevenueMap.get(itemCode) != null){
							paItemInfoDTO.setAvgRevenue(avgRevenueMap.get(itemCode));
						}else{
							double avgRevenue = getAverageRevenue(baseStoreCostData.get(itemCode), weekStartDt);
							paItemInfoDTO.setAvgRevenue(avgRevenue);
							avgRevenueMap.put(itemCode, avgRevenue);
						}
						
						// Set master data info
						paItemInfoDTO.setCalendarId(weekCalendarId);
						paItemInfoDTO.setLocationCompetitorMapId(locationCompetitorMap.getLocationCompetitorMapId());
						paItemInfoDTO.setAlertTypesId(alert.getAlertTypeId());
						
						// Get Current Price Data - Base Store
						if(baseCurPrice.get(alert.getBaseCurDataRangeInDays()) != null){
							PAItemInfoDTO tempDTO = baseCurPrice.get(alert.getBaseCurDataRangeInDays());
							paItemInfoDTO.setBaseCurRegPrice(tempDTO.getBaseCurRegPrice());
							paItemInfoDTO.setBaseCurRegPriceEffDate(tempDTO.getBaseCurRegPriceEffDate());
						}else{
							PAItemInfoDTO tempDTO = getCurrentPrice(baseStorePriceData.get(itemCode), weekStartDt, alert.getBaseCurDataRangeInDays(), false);
							paItemInfoDTO.setBaseCurRegPrice(tempDTO.getBaseCurRegPrice());
							paItemInfoDTO.setBaseCurRegPriceEffDate(tempDTO.getBaseCurRegPriceEffDate());
							baseCurPrice.put(alert.getBaseCurDataRangeInDays(), tempDTO);
						}
						
						// Get Previous Price - Base Store
						if(basePrePrice.get(alert.getBasePreDataRangeInDays()) != null){
							PAItemInfoDTO tempDTO = basePrePrice.get(alert.getBasePreDataRangeInDays());
							paItemInfoDTO.setBasePreRegPrice(tempDTO.getBasePreRegPrice());
						}else{
							PAItemInfoDTO tempDTO = getPreviousPrice(baseStorePriceData.get(itemCode), paItemInfoDTO.getBaseCurRegPrice(), weekStartDt, alert.getBasePreDataRangeInDays(), false);
							paItemInfoDTO.setBasePreRegPrice(tempDTO.getBasePreRegPrice());
							if(paItemInfoDTO.getBaseCurRegPriceEffDate() == null){
								paItemInfoDTO.setBaseCurRegPriceEffDate(tempDTO.getBaseCurRegPriceEffDate());
							}
							basePrePrice.put(alert.getBasePreDataRangeInDays(), tempDTO);
							
							tempDTO = getPreviousPrice(baseStorePriceData.get(itemCode), paItemInfoDTO.getBaseCurRegPrice(), weekStartDt, Constants._13WEEK, false);
							paItemInfoDTO.setBasePreRegPrice13w(tempDTO.getBasePreRegPrice());
						}
						
						
						// Get Current Cost Data - Base Store
						if(baseCurCost.get(alert.getBaseCurDataRangeInDays()) != null){
							PAItemInfoDTO tempDTO = baseCurCost.get(alert.getBaseCurDataRangeInDays());
							paItemInfoDTO.setBaseCurListCost(tempDTO.getBaseCurListCost());
							paItemInfoDTO.setBaseCurListCostEffDate(tempDTO.getBaseCurListCostEffDate());
						}else{
							PAItemInfoDTO tempDTO = getCurrentCost(baseStoreCostData.get(itemCode), weekStartDt, alert.getBaseCurDataRangeInDays());
							paItemInfoDTO.setBaseCurListCost(tempDTO.getBaseCurListCost());
							paItemInfoDTO.setBaseCurListCostEffDate(tempDTO.getBaseCurListCostEffDate());
							baseCurCost.put(alert.getBaseCurDataRangeInDays(), tempDTO);
						}
						
						// Get Previous Cost - Base Store
						if(basePreCost.get(alert.getBasePreDataRangeInDays()) != null){
							PAItemInfoDTO tempDTO = basePreCost.get(alert.getBasePreDataRangeInDays());
							paItemInfoDTO.setBasePreListCost(tempDTO.getBasePreListCost());
						}else{
							PAItemInfoDTO tempDTO = getPreviousCost(baseStoreCostData.get(itemCode), paItemInfoDTO.getBaseCurListCost(), weekStartDt, alert.getBasePreDataRangeInDays());
							paItemInfoDTO.setBasePreListCost(tempDTO.getBasePreListCost());
							if(paItemInfoDTO.getBaseCurListCostEffDate() == null)
								paItemInfoDTO.setBaseCurListCostEffDate(tempDTO.getBaseCurListCostEffDate());
							basePrePrice.put(alert.getBasePreDataRangeInDays(), tempDTO);
						}
						
						// Get Current Price Data - Comp Store
						if(compCurPrice.get(alert.getCompCurDataRangeInDays()) != null){
							PAItemInfoDTO tempDTO = compCurPrice.get(alert.getCompCurDataRangeInDays());
							paItemInfoDTO.setCompCurRegPrice(tempDTO.getCompCurRegPrice());
							paItemInfoDTO.setCompCurRegPriceEffDate(tempDTO.getCompCurRegPriceEffDate());
							paItemInfoDTO.setCompCurRegPriceLastObsDate(tempDTO.getCompCurRegPriceLastObsDate());
						}else{
							PAItemInfoDTO tempDTO = null;
							if(retLirId > 0)
								tempDTO = getCurrentCompPrice(retLirCompPriceData, weekStartDt, alert.getCompCurDataRangeInDays());
							else
								tempDTO = getCurrentPrice(compStorePriceData.get(itemCode), weekStartDt, alert.getCompCurDataRangeInDays(), true);
							paItemInfoDTO.setCompCurRegPrice(tempDTO.getCompCurRegPrice());
							paItemInfoDTO.setCompCurRegPriceEffDate(tempDTO.getCompCurRegPriceEffDate());
							paItemInfoDTO.setCompCurRegPriceLastObsDate(tempDTO.getCompCurRegPriceLastObsDate());
							compCurPrice.put(alert.getCompCurDataRangeInDays(), tempDTO);
						}
						
						// Get Previous Price - Comp Store
						if(compPrePrice.get(alert.getCompPreDataRangeInDays()) != null){
							PAItemInfoDTO tempDTO = compPrePrice.get(alert.getCompPreDataRangeInDays());
							paItemInfoDTO.setCompPreRegPrice(tempDTO.getCompPreRegPrice());
						}else{
							PAItemInfoDTO tempDTO = null;
							if(retLirId > 0)
								tempDTO = getPreviousCompPrice(retLirCompPriceData, paItemInfoDTO.getCompCurRegPrice(), paItemInfoDTO.getCompLirItemCode(), weekStartDt, alert.getCompPreDataRangeInDays());
							else
								tempDTO = getPreviousPrice(compStorePriceData.get(itemCode), paItemInfoDTO.getCompCurRegPrice(), weekStartDt, alert.getCompPreDataRangeInDays(), true);
							paItemInfoDTO.setCompPreRegPrice(tempDTO.getCompPreRegPrice());
							if(paItemInfoDTO.getCompCurRegPriceEffDate() == null){
								paItemInfoDTO.setCompCurRegPriceEffDate(tempDTO.getCompCurRegPriceEffDate());
							}
							compPrePrice.put(alert.getCompPreDataRangeInDays(), tempDTO);
						}
						
						// Get Current Price Data - Other Comp Store
						if(comp2CurPrice.get(alert.getCompCurDataRangeInDays()) != null){
							PAItemInfoDTO tempDTO = comp2CurPrice.get(alert.getCompCurDataRangeInDays());
							paItemInfoDTO.setComp2CurRegPrice(tempDTO.getCompCurRegPrice());
						}else{
							PAItemInfoDTO tempDTO = null;
							if(retLirId > 0)
								tempDTO = getCurrentCompPrice(retLirComp2PriceData, weekStartDt, alert.getCompCurDataRangeInDays());
							else
								tempDTO = getCurrentPrice(compStore2PriceData.get(itemCode), weekStartDt, Constants._13WEEK, true);
							paItemInfoDTO.setComp2CurRegPrice(tempDTO.getCompCurRegPrice());
							comp2CurPrice.put(Constants._13WEEK, tempDTO);
						}
						
						// Get Previous Price - Other Comp Store
						if(comp2PrePrice.get(alert.getCompPreDataRangeInDays()) != null){
							PAItemInfoDTO tempDTO = comp2PrePrice.get(alert.getCompPreDataRangeInDays());
							paItemInfoDTO.setComp2PreRegPrice(tempDTO.getCompPreRegPrice());
						}else{
							PAItemInfoDTO tempDTO = null;
							if(retLirId > 0)
								tempDTO = getPreviousCompPrice(retLirComp2PriceData, paItemInfoDTO.getComp2CurRegPrice(), paItemInfoDTO.getCompLirItemCode(), weekStartDt, alert.getCompPreDataRangeInDays());
							else
								tempDTO = getPreviousPrice(compStore2PriceData.get(itemCode), paItemInfoDTO.getComp2CurRegPrice(), weekStartDt, Constants._13WEEK, true);
							paItemInfoDTO.setComp2PreRegPrice(tempDTO.getCompPreRegPrice());
							comp2PrePrice.put(Constants._13WEEK, tempDTO);
						}
						
						// Get Current Price Data - Comp Store in Base Cur Reg Price Range
						if(!compCurPriceInBaseRangeComputed){
							PAItemInfoDTO tempDTO = null;
							if(retLirId > 0)
								tempDTO = getCurrentCompPrice(retLirCompPriceData, weekStartDt, alert.getCompCurDataRangeInDays());
							else
								tempDTO = getCurrentPrice(compStorePriceData.get(itemCode), weekStartDt, alert.getBaseCurDataRangeInDays(), true);
							compCurPriceInBaseRange = tempDTO.getCompCurRegPrice();
							compCurPriceInBaseRangeComputed = true;
							paItemInfoDTO.setCompCurRegPriceInBaseRange(compCurPriceInBaseRange);
						}else{
							paItemInfoDTO.setCompCurRegPriceInBaseRange(compCurPriceInBaseRange);
						}
						
						// Set OutsideMarginRange and OutsidePIRange flags
						if(alert.getAlertTypeId() == 1){
							GoalSettingsDTO goals = null;
							
							boolean isGoalSet = false;
							// Get Current Item's Goal
							if(goalSettingsDTO.getItemLevelGoal() != null && goalSettingsDTO.getItemLevelGoal().get(paItemInfoDTO.getItemCode()) != null){
								goals = goalSettingsDTO.getItemLevelGoal().get(paItemInfoDTO.getItemCode());
								isGoalSet = true;
							}
							
							if(!isGoalSet && goalSettingsDTO.getPriceCheckLevelGoal() != null){
								if(paItemInfoDTO.isKVIItem() && primaryList == null){
									//goals = goalSettingsDTO.getPriceCheckLevelGoal();
									//isGoalSet = true;
								}else if(paItemInfoDTO.isKVIItem() && primaryList != null){
									if(primaryList.contains(paItemInfoDTO.getItemCode())){
										if(goalSettingsDTO.getPriceCheckLevelGoal().get(primaryCheckListId) != null){
											goals = goalSettingsDTO.getPriceCheckLevelGoal().get(primaryCheckListId);
											isGoalSet = true;
										}
									}
								}else{
									if(secondaryList != null){
										if(secondaryList.contains(paItemInfoDTO.getItemCode())){
											if(goalSettingsDTO.getPriceCheckLevelGoal().get(secondaryCheckListId) != null){
												goals = goalSettingsDTO.getPriceCheckLevelGoal().get(secondaryCheckListId);
												isGoalSet = true;
											}
										}
									}
								}
							}
		
							if(!isGoalSet && goalSettingsDTO.getBrandLevelGoal() != null){
								for(Map.Entry<Integer, GoalSettingsDTO> entry : goalSettingsDTO.getBrandLevelGoal().entrySet()){
									if(entry.getKey() == paItemInfoDTO.getBrandId()){
										goals = entry.getValue();
										isGoalSet = true;
										break;
									}
								}
							}
							
							if(!isGoalSet && goalSettingsDTO.getProductLevelGoal() != null){
								goals = goalSettingsDTO.getProductLevelGoal();
								isGoalSet = true;
							}
	
							if(!isGoalSet && goalSettingsDTO.getLocationLevelGoal() != null){
								goals = goalSettingsDTO.getLocationLevelGoal();
								isGoalSet = true;
							}
	
							if(!isGoalSet && goalSettingsDTO.getChainLevelGoal() != null){
								isGoalSet = true;
								goals = goalSettingsDTO.getChainLevelGoal();
							}
							
							if(goals != null && !((paItemInfoDTO.getCompCurRegPrice() == 0 || paItemInfoDTO.getBaseCurRegPrice() == 0 || paItemInfoDTO.getCompCurRegPrice() == Constants.DEFAULT_NA || paItemInfoDTO.getBaseCurRegPrice() == Constants.DEFAULT_NA))){
								double curPriceIndex = paItemInfoDTO.getCompCurRegPrice()/paItemInfoDTO.getBaseCurRegPrice()*100;
								if(curPriceIndex < goals.getCurRegMinPriceIndex()){
									paItemInfoDTO.setOutsidePIRange(true);
								}else if(curPriceIndex >= goals.getCurRegMinPriceIndex() && curPriceIndex <= goals.getCurRegMaxPriceIndex()){
									paItemInfoDTO.setOutsidePIRange(false);
								}else{
									paItemInfoDTO.setOutsidePIRange(true);
								}
							}
							
							if(goals != null && !((paItemInfoDTO.getBaseCurRegPrice() == 0 || paItemInfoDTO.getBaseCurListCost() == 0 || paItemInfoDTO.getBaseCurRegPrice() == Constants.DEFAULT_NA || paItemInfoDTO.getBaseCurListCost() == Constants.DEFAULT_NA))){
								double curMargin = (paItemInfoDTO.getBaseCurRegPrice() - paItemInfoDTO.getBaseCurListCost())/paItemInfoDTO.getBaseCurRegPrice()*100;
								if(curMargin < goals.getCurRegMinMargin()){
									paItemInfoDTO.setOutsideMarginRange(true);
								}else if(curMargin >= goals.getCurRegMinMargin() && curMargin <= goals.getCurRegMaxMargin()){
									paItemInfoDTO.setOutsideMarginRange(false);
								}else{
									paItemInfoDTO.setOutsideMarginRange(true);
								}
							}
						}
						// Set OutsideMarginRange and OutsidePIRange flag - Ends
						
						// Call alert logic
						ArrayList<PAItemInfoDTO> processedAlertList = processAlert(alert, paItemInfoDTO);
						
						insertList.addAll(processedAlertList);
					}
					retLirCompPriceData = null;
					retLirComp2PriceData = null;
				}
				
				// Insert into PA_MASTER_DATA, PA_ITEM_INFO
				ArrayList<PAItemInfoDTO> masterDataList = populatePAMasterInfo(insertList, paMasterDataIdMap);
				pricingAlertDAO.insertPAMasterInfo(connection, masterDataList);
				PristineDBUtil.commitTransaction(connection, "PA Master Data Commit");
				pricingAlertDAO.insertPAItemInfo(connection, insertList);
				PristineDBUtil.commitTransaction(connection, "PA Item Info Commit");
				
				if(unprocessedAlerts.size() > 0){
					logger.info("Processing LIG Alerts starts");
					// LIG alerts have to be handled differently. Process LIG Alerts
					for(String ligAlerts : ligAlertCodes){
						insertList = processLIGAlert(unprocessedAlerts.get(ligAlerts), baseStorePriceData, baseStoreCostData, weekStartDt);
						
						// Set master data info
						for(PAItemInfoDTO paItemInfoDTO : insertList){
							paItemInfoDTO.setCalendarId(weekCalendarId);
							paItemInfoDTO.setLocationCompetitorMapId(locationCompetitorMap.getLocationCompetitorMapId());
							paItemInfoDTO.setAlertTypesId(unprocessedAlerts.get(ligAlerts).getAlertTypeId());
							if(avgRevenueMap.get(paItemInfoDTO.getItemCode()) != null)
								paItemInfoDTO.setAvgRevenue(avgRevenueMap.get(paItemInfoDTO.getItemCode()));
						}
						
						// Insert into PA_MASTER_DATA, PA_ITEM_INFO
						masterDataList = populatePAMasterInfo(insertList, paMasterDataIdMap);
						pricingAlertDAO.insertPAMasterInfo(connection, masterDataList);
						PristineDBUtil.commitTransaction(connection, "PA Master Data Commit");
						pricingAlertDAO.insertPAItemInfo(connection, insertList);
						PristineDBUtil.commitTransaction(connection, "PA Item Info Commit");
					}
				}
				baseStorePriceData  = null;
				baseStoreCostData = null;
				compStorePriceData  = null;
				retLirMap = null;
				itemsInZone = null;
			}catch(GeneralException exception){
				logger.error("Error when retrieving price/cost data for location level id - " + locationCompetitorMap.getBaseLocationId() + ", product level id - " 
								+ locationCompetitorMap.getProductLevelId() + " - " + exception.toString());
			}
			
			// Add the combination to the processed list
			if(locationCompetitorMap.getProductId() > 0)
				processedList.add(locationCompetitorMap.getBaseLocationLevelId() + "-" + locationCompetitorMap.getBaseLocationId() + "-" + locationCompetitorMap.getProductLevelId() + "-" + locationCompetitorMap.getProductId());
			else
				processedList.add(locationCompetitorMap.getBaseLocationLevelId() + "-" + locationCompetitorMap.getBaseLocationId() + "-" + locationCompetitorMap.getProductLevelId());
		}
	}

	/**
	 * Call appropriate functions to check if the item passes all alerts and add the DTO to the final list if passed
	 * @param alert		Alert Type
	 * @param inputDTO	PA Item Info
	 * @return
	 */
	public ArrayList<PAItemInfoDTO> processAlert(AlertTypesDto alert, PAItemInfoDTO inputDTO){
		ArrayList<PAItemInfoDTO> passedAlerts = new ArrayList<PAItemInfoDTO>();
		if(alert.getTechnicalCode().equals("N001")){
			PAItemInfoDTO tempDTO = checkN001(inputDTO);
			if(tempDTO != null)
				passedAlerts.add(tempDTO);
		}
		if(alert.getTechnicalCode().equals("KVI001")){
			PAItemInfoDTO tempDTO = checkKVI001(inputDTO);
			if(tempDTO != null)
				passedAlerts.add(tempDTO);
		}
		if(alert.getTechnicalCode().equals("KVI002")){
			PAItemInfoDTO tempDTO = checkKVI002(inputDTO);
			if(tempDTO != null)
				passedAlerts.add(tempDTO);
		}
		if(alert.getTechnicalCode().equals("KVI003")){
			PAItemInfoDTO tempDTO = checkKVI003(inputDTO);
			if(tempDTO != null)
				passedAlerts.add(tempDTO);
		}
		return passedAlerts;
	}
	
	/**
	 * LIG records to be sent as alert
	 * @param alert
	 * @param baseStorePriceData
	 * @param baseStoreCostData
	 * @param weekStartDate
	 * @throws GeneralException
	 */
	public ArrayList<PAItemInfoDTO> processLIGAlert(AlertTypesDto alert, HashMap<Integer, ArrayList<CompetitiveDataDTO>> baseStorePriceData, HashMap<Integer, 
			ArrayList<MovementWeeklyDTO>> baseStoreCostData, Date weekStartDate) throws GeneralException{
		ArrayList<PAItemInfoDTO> insertList = new ArrayList<PAItemInfoDTO>();
		if(alert.getTechnicalCode().equals("LIG001")){
			ArrayList<PAItemInfoDTO> passedItems = checkLIG001(alert, baseStorePriceData, baseStoreCostData, weekStartDate);
			insertList.addAll(passedItems);
		}
		
		if(alert.getTechnicalCode().equals("LIG002")){
			ArrayList<PAItemInfoDTO> passedItems = checkLIG002(alert, baseStorePriceData, baseStoreCostData, weekStartDate);
			insertList.addAll(passedItems);
		}
		return insertList;
	}
	
	/**
	 * Items will fall under this alert type, if any of the two item properties (BaseRegPriceVariation, BaseListCostVariation, CompRegPriceVariation, 
	 * BaseFutRegPriceVariation, and BaseFutListCostVariation) goes in opposite direction. 
	 * For e.g. BaseRegPriceVariation is Reduced, CompRegPriceVariation is increased
	 * @param inputDTO
	 * @return
	 */
	public PAItemInfoDTO checkN001(PAItemInfoDTO inputDTO){
		PAItemInfoDTO paItemInfoDTO = null;
		int same = 0;
		int increased = 0;
		int reduced = 0;

		//logger.info(inputDTO.getItemCode() + " " + inputDTO.getBaseCurRegPrice() + " " + inputDTO.getBasePreRegPrice() + " " + inputDTO.getBaseCurListCost() + " " + inputDTO.getBasePreListCost() + " " + inputDTO.getCompCurRegPrice() + " " + inputDTO.getCompPreRegPrice());
		//Base Reg Price Variation
		if(inputDTO.getBaseCurRegPrice() != Constants.DEFAULT_NA && inputDTO.getBasePreRegPrice() != Constants.DEFAULT_NA){
			if(inputDTO.getBaseCurRegPrice() == inputDTO.getBasePreRegPrice())
				same++;
			else if(inputDTO.getBaseCurRegPrice() > inputDTO.getBasePreRegPrice())
				increased++;
			else if(inputDTO.getBaseCurRegPrice() < inputDTO.getBasePreRegPrice())
				reduced++;
		}
		
		//Base List Cost Variation
		if(inputDTO.getBaseCurListCost() != Constants.DEFAULT_NA && inputDTO.getBasePreListCost() != Constants.DEFAULT_NA){
			if(inputDTO.getBaseCurListCost() == inputDTO.getBasePreListCost())
				same++;
			else if(inputDTO.getBaseCurListCost() > inputDTO.getBasePreListCost())
				increased++;
			else if(inputDTO.getBaseCurListCost() < inputDTO.getBasePreListCost())
				reduced++;
		}
		
		//Comp Reg Price Variation
		if(inputDTO.getCompCurRegPrice() != Constants.DEFAULT_NA && inputDTO.getCompPreRegPrice() != Constants.DEFAULT_NA){
			if(inputDTO.getCompCurRegPrice() == inputDTO.getCompPreRegPrice())
				same++;
			else if(inputDTO.getCompCurRegPrice() > inputDTO.getCompPreRegPrice())
				increased++;
			else if(inputDTO.getCompCurRegPrice() < inputDTO.getCompPreRegPrice())
				reduced++;
		}
		
		//Base Future Reg Price Variation
		if(inputDTO.getBaseFutRegPrice() != Constants.DEFAULT_NA && inputDTO.getBaseCurRegPrice() != Constants.DEFAULT_NA){
			if(inputDTO.getBaseFutRegPrice() == inputDTO.getBaseCurRegPrice())
				same++;
			else if(inputDTO.getBaseFutRegPrice() > inputDTO.getBaseCurRegPrice())
				increased++;
			else if(inputDTO.getBaseFutRegPrice() < inputDTO.getBaseCurRegPrice())
				reduced++;
		}
		
		//Base Future List Cost Variation
		if(inputDTO.getBaseFutListCost() != Constants.DEFAULT_NA && inputDTO.getBaseCurListCost() != Constants.DEFAULT_NA){
			if(inputDTO.getBaseFutListCost() == inputDTO.getBaseCurListCost())
				same++;
			else if(inputDTO.getBaseFutListCost() > inputDTO.getBaseCurListCost())
				increased++;
			else if(inputDTO.getBaseFutListCost() < inputDTO.getBaseCurListCost())
				reduced++;
		}
		
		/*  Report this as noteworthy only if have positive confirmation for both Primary and secondary – in the last 13 weeks, 
		 * both Primary and Secondary has gone up (or down) in price but we have not changed
		 */
		boolean isPriceChgNoteworthy = false;
		double compPriceChg = 0;
		double comp2PriceChg = 0;
		if(inputDTO.getCompCurRegPrice() != Constants.DEFAULT_NA && inputDTO.getCompPreRegPrice() != Constants.DEFAULT_NA){
			compPriceChg = inputDTO.getCompCurRegPrice() - inputDTO.getCompPreRegPrice();
		}
		if(inputDTO.getComp2CurRegPrice() != Constants.DEFAULT_NA && inputDTO.getComp2PreRegPrice() != Constants.DEFAULT_NA){
			comp2PriceChg = inputDTO.getComp2CurRegPrice() - inputDTO.getComp2PreRegPrice();
		}
		
		if(compPriceChg != 0 && comp2PriceChg != 0){
			if(inputDTO.getBaseCurRegPrice() != Constants.DEFAULT_NA && !(inputDTO.getBasePreRegPrice13w() != Constants.DEFAULT_NA))
				if(compPriceChg > 0 && comp2PriceChg > 0)
					isPriceChgNoteworthy = true;
				else if(compPriceChg < 0 && comp2PriceChg < 0)
					isPriceChgNoteworthy = true;
		}
		
		/*
		 * Report it as noteworthy if both Primary and Secondary are below or above
		 */
		boolean isPriceNoteworthy = false;
		if(inputDTO.getBaseCurRegPrice() != Constants.DEFAULT_NA && inputDTO.getCompCurRegPrice() != Constants.DEFAULT_NA && inputDTO.getComp2CurRegPrice() != Constants.DEFAULT_NA){
			if((inputDTO.getBaseCurRegPrice() - inputDTO.getCompCurRegPrice() > 0) && (inputDTO.getBaseCurRegPrice() - inputDTO.getComp2CurRegPrice() > 0))
				isPriceNoteworthy = true;
			else if((inputDTO.getBaseCurRegPrice() - inputDTO.getCompCurRegPrice() > 0) && (inputDTO.getBaseCurRegPrice() - inputDTO.getComp2CurRegPrice() > 0))
				isPriceNoteworthy = true;
		}
		
		if((increased > 0 && reduced > 0) || inputDTO.isOutsideMarginRange() || inputDTO.isOutsidePIRange() || isPriceChgNoteworthy || isPriceNoteworthy){
			// Pass
			paItemInfoDTO = new PAItemInfoDTO();
			paItemInfoDTO.setCalendarId(inputDTO.getCalendarId());
			paItemInfoDTO.setLocationCompetitorMapId(inputDTO.getLocationCompetitorMapId());
			paItemInfoDTO.setAlertTypesId(inputDTO.getAlertTypesId());
			paItemInfoDTO.setItemCode(inputDTO.getItemCode());
			paItemInfoDTO.setBaseCurRegPrice(inputDTO.getBaseCurRegPrice());
			paItemInfoDTO.setBaseCurRegPriceEffDate(inputDTO.getBaseCurRegPriceEffDate());
			paItemInfoDTO.setBasePreRegPrice(inputDTO.getBasePreRegPrice());
			paItemInfoDTO.setBaseFutRegPrice(inputDTO.getBaseFutRegPrice());
			paItemInfoDTO.setBaseFutRegPriceEffDate(inputDTO.getBaseFutRegPriceEffDate());
			paItemInfoDTO.setBaseCurListCost(inputDTO.getBaseCurListCost());
			paItemInfoDTO.setBaseCurListCostEffDate(inputDTO.getBaseCurListCostEffDate());
			paItemInfoDTO.setBasePreListCost(inputDTO.getBasePreListCost());
			paItemInfoDTO.setBaseFutListCost(inputDTO.getBaseFutListCost());
			paItemInfoDTO.setBaseFutListCostEffDate(inputDTO.getBaseFutListCostEffDate());
			paItemInfoDTO.setCompCurRegPrice(inputDTO.getCompCurRegPrice());
			paItemInfoDTO.setCompCurRegPriceEffDate(inputDTO.getCompCurRegPriceEffDate());
			paItemInfoDTO.setCompPreRegPrice(inputDTO.getCompPreRegPrice());
			paItemInfoDTO.setComp2CurRegPrice(inputDTO.getComp2CurRegPrice());
			paItemInfoDTO.setComp2PreRegPrice(inputDTO.getComp2PreRegPrice());
			paItemInfoDTO.setKVIItem(inputDTO.isKVIItem());
			paItemInfoDTO.setAvgRevenue(inputDTO.getAvgRevenue());
		}
		
		return paItemInfoDTO;
	}
	
	/**
	 * Item will fall under this alert, if the item is a KVI item, whose Reg price has not changed in the past X weeks
	 * (as defined in column BASE_CUR_DATA_LOOKUP_RANGE in PA_ALERT_TYPES) and whose cost has undergone change in the past X weeks 
	 * (as defined in column BASE_CUR_DATA_LOOKUP_RANGE in PA_ALERT_TYPES)
	 * @param inputDTO
	 * @return
	 */
	public PAItemInfoDTO checkKVI001(PAItemInfoDTO inputDTO){
		PAItemInfoDTO paItemInfoDTO = null;
		//KVI Item
		if(inputDTO.isKVIItem()){
			//No Price Change
			if(inputDTO.getBaseCurRegPrice() != Constants.DEFAULT_NA && inputDTO.getBasePreRegPrice() == Constants.DEFAULT_NA){
				if(inputDTO.getBaseCurListCost() != Constants.DEFAULT_NA && inputDTO.getBasePreListCost() != Constants.DEFAULT_NA){
					if(inputDTO.getBaseCurListCost() != inputDTO.getBasePreListCost()){
						// Pass
						paItemInfoDTO = new PAItemInfoDTO();
						paItemInfoDTO.setKVIItem(inputDTO.isKVIItem());
						paItemInfoDTO.setCalendarId(inputDTO.getCalendarId());
						paItemInfoDTO.setLocationCompetitorMapId(inputDTO.getLocationCompetitorMapId());
						paItemInfoDTO.setAlertTypesId(inputDTO.getAlertTypesId());
						paItemInfoDTO.setItemCode(inputDTO.getItemCode());
						paItemInfoDTO.setBaseCurRegPrice(inputDTO.getBaseCurRegPrice());
						paItemInfoDTO.setBaseCurListCost(inputDTO.getBaseCurListCost());
						paItemInfoDTO.setBaseCurListCostEffDate(inputDTO.getBaseCurListCostEffDate());
						paItemInfoDTO.setBasePreListCost(inputDTO.getBasePreListCost());
						paItemInfoDTO.setCompCurRegPrice(inputDTO.getCompCurRegPrice());
						paItemInfoDTO.setCompCurRegPriceLastObsDate(inputDTO.getCompCurRegPriceLastObsDate());
						paItemInfoDTO.setAvgRevenue(inputDTO.getAvgRevenue());	
					}
				}
			}
		}
		return paItemInfoDTO;
	}
	
	/**
	 * Item will fall under this alert, if the item is a KVI item, whose Reg price has not changed in the past X months 
	 * (as defined in column BASE_CUR_DATA_LOOKUP_RANGE in PA_ALERT_TYPES) 
	 * @param inputDTO
	 * @return
	 */
	public PAItemInfoDTO checkKVI002(PAItemInfoDTO inputDTO){
		PAItemInfoDTO paItemInfoDTO = null;
		//KVI Item
		if(inputDTO.isKVIItem()){
			//No Price Change
			if(inputDTO.getBaseCurRegPrice() != Constants.DEFAULT_NA && inputDTO.getBasePreRegPrice() == Constants.DEFAULT_NA){
				// Pass
				paItemInfoDTO = new PAItemInfoDTO();
				paItemInfoDTO.setKVIItem(inputDTO.isKVIItem());
				paItemInfoDTO.setCalendarId(inputDTO.getCalendarId());
				paItemInfoDTO.setLocationCompetitorMapId(inputDTO.getLocationCompetitorMapId());
				paItemInfoDTO.setAlertTypesId(inputDTO.getAlertTypesId());
				paItemInfoDTO.setItemCode(inputDTO.getItemCode());
				paItemInfoDTO.setBaseCurRegPrice(inputDTO.getBaseCurRegPrice());
				paItemInfoDTO.setBaseCurListCost(inputDTO.getBaseCurListCost());
				paItemInfoDTO.setCompCurRegPrice(inputDTO.getCompCurRegPrice());
				paItemInfoDTO.setCompCurRegPriceLastObsDate(inputDTO.getCompCurRegPriceLastObsDate());
				paItemInfoDTO.setAvgRevenue(inputDTO.getAvgRevenue());	
			}
		}
		return paItemInfoDTO;
	}
	
	/**
	 * Item will fall under this alert, if the item is a KVI item, for which there is no competition Reg price is available in the past X weeks 
	 * (as defined in column BASE_CUR_DATA_LOOKUP_RANGE in PA_ALERT_TYPES).
	 * @param inputDTO
	 * @return
	 */
	public PAItemInfoDTO checkKVI003(PAItemInfoDTO inputDTO){
		PAItemInfoDTO paItemInfoDTO = null;
		//KVI Item
		if(inputDTO.isKVIItem()){
			// No competition reg price observation
			if(inputDTO.getCompCurRegPriceInBaseRange() == Constants.DEFAULT_NA){
				// Pass
				paItemInfoDTO = new PAItemInfoDTO();
				paItemInfoDTO.setKVIItem(inputDTO.isKVIItem());
				paItemInfoDTO.setCalendarId(inputDTO.getCalendarId());
				paItemInfoDTO.setLocationCompetitorMapId(inputDTO.getLocationCompetitorMapId());
				paItemInfoDTO.setAlertTypesId(inputDTO.getAlertTypesId());
				paItemInfoDTO.setItemCode(inputDTO.getItemCode());
				paItemInfoDTO.setBaseCurRegPrice(inputDTO.getBaseCurRegPrice());
				paItemInfoDTO.setCompCurRegPrice(inputDTO.getCompCurRegPrice());
				paItemInfoDTO.setCompCurRegPriceLastObsDate(inputDTO.getCompCurRegPriceLastObsDate());
				paItemInfoDTO.setAvgRevenue(inputDTO.getAvgRevenue());
			}
		}
		return paItemInfoDTO;
	}
	
	/**
	 * LIG Items whose members price are not identical
	 * @param alert
	 * @param baseStorePriceData
	 * @param weekStartDate
	 * @return
	 * @throws GeneralException
	 */
	public ArrayList<PAItemInfoDTO> checkLIG001(AlertTypesDto alert, HashMap<Integer, ArrayList<CompetitiveDataDTO>> baseStorePriceData, HashMap<Integer, ArrayList<MovementWeeklyDTO>> baseStoreCostData, Date weekStartDate) throws GeneralException{
		ArrayList<PAItemInfoDTO> passedItems = new ArrayList<PAItemInfoDTO>();
		
		HashMap<Integer, ArrayList<CompetitiveDataDTO>> ligItems = new HashMap<Integer, ArrayList<CompetitiveDataDTO>>();
		for(ArrayList<CompetitiveDataDTO> compDataList : baseStorePriceData.values()){
			for(CompetitiveDataDTO compData : compDataList){
				if(compData.lirId <= 0)
					continue;
				Date date = DateUtil.toDate(compData.weekStartDate);
				if(PrestoUtil.compareTo(date, weekStartDate) == 0){
					if(ligItems.get(compData.lirId) != null){
						ArrayList<CompetitiveDataDTO> tempDTOList = ligItems.get(compData.lirId);
						tempDTOList.add(compData);
						ligItems.put(compData.lirId, tempDTOList);
					}else{
						ArrayList<CompetitiveDataDTO> tempDTOList = new ArrayList<CompetitiveDataDTO>();
						tempDTOList.add(compData);
						ligItems.put(compData.lirId, tempDTOList);
					}
				}
			}
		}
		
		
		for(Map.Entry<Integer, ArrayList<CompetitiveDataDTO>> entry : ligItems.entrySet()){
			boolean hasPriceChange = false;
			double price = Constants.DEFAULT_NA;
			for(CompetitiveDataDTO compDataDTO : entry.getValue()){
				if(price == Constants.DEFAULT_NA){
					price = compDataDTO.regPrice;
				}else{
					if(price != compDataDTO.regPrice){
						hasPriceChange = true;
						break;
					}
				}
			}
			
			if(hasPriceChange){
				for(CompetitiveDataDTO compDataDTO : entry.getValue()){
					PAItemInfoDTO paItemInfoDTO = new PAItemInfoDTO();
					paItemInfoDTO.setItemCode(compDataDTO.itemcode);
					paItemInfoDTO.setBaseCurRegPrice(compDataDTO.regPrice);
					
					// Set current week's list cost for that item
					ArrayList<MovementWeeklyDTO> movDataList = baseStoreCostData.get(compDataDTO.itemcode);
					if(movDataList != null){
						for(MovementWeeklyDTO movData : movDataList){
							Date date = DateUtil.toDate(movData.getWeekStartDate());
							if(PrestoUtil.compareTo(date, weekStartDate) == 0){
								paItemInfoDTO.setBaseCurListCost(movData.getListCost());
								break;
							}
						}
					}
					
					passedItems.add(paItemInfoDTO);
				}
			}
		}
		
		return passedItems;
	}
	
	/**
	 * LIG Items whose members cost are not identical
	 * @param alert
	 * @param baseStorePriceData
	 * @param weekStartDate
	 * @throws GeneralException
	 */
	public ArrayList<PAItemInfoDTO> checkLIG002(AlertTypesDto alert, HashMap<Integer, ArrayList<CompetitiveDataDTO>> baseStorePriceData, HashMap<Integer, ArrayList<MovementWeeklyDTO>> baseStoreCostData, Date weekStartDate) throws GeneralException{
		ArrayList<PAItemInfoDTO> passedItems = new ArrayList<PAItemInfoDTO>();
		
		HashMap<Integer, ArrayList<MovementWeeklyDTO>> ligItems = new HashMap<Integer, ArrayList<MovementWeeklyDTO>>();
		for(ArrayList<MovementWeeklyDTO> movDTOList : baseStoreCostData.values()){
			for(MovementWeeklyDTO movDTO : movDTOList){
				if(movDTO.getLirId() <= 0)
					continue;
				Date date = DateUtil.toDate(movDTO.getWeekStartDate());
				if(PrestoUtil.compareTo(date, weekStartDate) == 0){
					if(ligItems.get(movDTO.getLirId()) != null){
						ArrayList<MovementWeeklyDTO> tempDTOList = ligItems.get(movDTO.getLirId());
						tempDTOList.add(movDTO);
						ligItems.put(movDTO.getLirId(), tempDTOList);
					}else{
						ArrayList<MovementWeeklyDTO> tempDTOList = new ArrayList<MovementWeeklyDTO>();
						tempDTOList.add(movDTO);
						ligItems.put(movDTO.getLirId(), tempDTOList);
					}
				}
			}
		}
		
		
		for(Map.Entry<Integer, ArrayList<MovementWeeklyDTO>> entry : ligItems.entrySet()){
			boolean hasCostChange = false;
			double cost = Constants.DEFAULT_NA;
			for(MovementWeeklyDTO movDTO : entry.getValue()){
				if(cost == Constants.DEFAULT_NA){
					cost = movDTO.getListCost();
				}else{
					if(cost != movDTO.getListCost()){
						hasCostChange = true;
						break;
					}
				}
			}
			
			if(hasCostChange){
				for(MovementWeeklyDTO movDTO : entry.getValue()){
					PAItemInfoDTO paItemInfoDTO = new PAItemInfoDTO();
					paItemInfoDTO.setItemCode(movDTO.getItemCode());
					paItemInfoDTO.setBaseCurListCost(movDTO.getListCost());
					
					// Set current week's list cost for that item
					ArrayList<CompetitiveDataDTO> compDataList = baseStorePriceData.get(movDTO.getItemCode());
					if(compDataList != null){
						for(CompetitiveDataDTO compData : compDataList){
							Date date = DateUtil.toDate(compData.weekStartDate);
							if(PrestoUtil.compareTo(date, weekStartDate) == 0){
								paItemInfoDTO.setBaseCurRegPrice(compData.regPrice);
								break;
							}
						}
					}
					passedItems.add(paItemInfoDTO);
				}
			}
		}
		
		return passedItems;
	}
	
	/**
	 * Determines current price and current price effective date of an item considering data within dataLookupRange
	 * current price - latest price
	 * current price effective date - start date of the immediate week last price change was observed within the given range
	 * @param priceHistory		Price History
	 * @param maxDate			Max Date in the dataLookupRange
	 * @param dataLookupRange	Range within which current price has to be determined
	 * @param isCompetitor		Price values will be set in competitor related fields if it is true
	 * @return	PAItemInfoDTO with current price and current price effective date
	 * @throws GeneralException
	 */
	public PAItemInfoDTO getCurrentPrice(ArrayList<CompetitiveDataDTO> priceHistory, Date maxDate, Integer dataLookupRange, boolean isCompetitor) throws GeneralException{
		Date minDate = DateUtil.incrementDate(maxDate, -(dataLookupRange));
		PAItemInfoDTO paItemInfo = new PAItemInfoDTO();
		if(priceHistory == null || priceHistory.size() <= 0){
			return paItemInfo;
		}
		float currentPrice = Constants.DEFAULT_NA;
		String currentPriceChangeDate = null;
		String lastObsDate = null;
		boolean isPriceSet = false;
		for(CompetitiveDataDTO compDataDTO : priceHistory){
			Date weekStartDate = DateUtil.toDate(compDataDTO.weekStartDate);
			if(PrestoUtil.compareTo(weekStartDate, minDate) >= 0 && PrestoUtil.compareTo(weekStartDate, maxDate) <= 0){
				if(currentPrice != Constants.DEFAULT_NA){
					if(currentPrice != compDataDTO.regPrice && (((currentPrice - compDataDTO.regPrice)/currentPrice * 100) > priceChangeThreshold) || (((currentPrice - compDataDTO.regPrice)/currentPrice * 100) < -(priceChangeThreshold))){
						currentPriceChangeDate = DateUtil.dateToString(DateUtil.incrementDate(DateUtil.toDate(compDataDTO.weekStartDate), 7), Constants.APP_DATE_FORMAT);
						if(!isCompetitor){
							paItemInfo.setBaseCurRegPrice(currentPrice);
							paItemInfo.setBaseCurRegPriceEffDate(currentPriceChangeDate);
							paItemInfo.setBrandId(compDataDTO.brandId);
						}else{
							paItemInfo.setCompCurRegPrice(currentPrice);
							paItemInfo.setCompCurRegPriceEffDate(currentPriceChangeDate);
							paItemInfo.setCompCurRegPriceLastObsDate(lastObsDate);
						}
						return paItemInfo;
					}
				}else{
					if(!isPriceSet){
						currentPrice = compDataDTO.regPrice;
						lastObsDate = compDataDTO.weekStartDate;
						isPriceSet = true;
					}
				}
			}
		}
		
		if(!isCompetitor){
			paItemInfo.setBaseCurRegPrice(currentPrice);
			paItemInfo.setBaseCurRegPriceEffDate(currentPriceChangeDate);
		}else{
			paItemInfo.setCompCurRegPrice(currentPrice);
			paItemInfo.setCompCurRegPriceEffDate(currentPriceChangeDate);
			paItemInfo.setCompCurRegPriceLastObsDate(lastObsDate);
		}
		return paItemInfo;
	}
	
	public PAItemInfoDTO getCurrentCompPrice(ArrayList<CompetitiveDataDTO> priceHistory, Date maxDate, Integer dataLookupRange) throws GeneralException{
		Date minDate = DateUtil.incrementDate(maxDate, -(dataLookupRange));
		PAItemInfoDTO paItemInfo = new PAItemInfoDTO();
		if(priceHistory == null || priceHistory.size() <= 0){
			return paItemInfo;
		}
		float currentPrice = Constants.DEFAULT_NA;
		String currentPriceChangeDate = null;
		String lastObsDate = null;
		int compItemCode = -1;
		for(CompetitiveDataDTO compDataDTO : priceHistory){
			Date weekStartDate = DateUtil.toDate(compDataDTO.weekStartDate);
			if(PrestoUtil.compareTo(weekStartDate, minDate) >= 0 && PrestoUtil.compareTo(weekStartDate, maxDate) <= 0){
				if(currentPrice != Constants.DEFAULT_NA){
					if(currentPrice != compDataDTO.regPrice && (((currentPrice - compDataDTO.regPrice)/currentPrice * 100) > priceChangeThreshold) || (((currentPrice - compDataDTO.regPrice)/currentPrice * 100) < -(priceChangeThreshold))){
						currentPriceChangeDate = DateUtil.dateToString(DateUtil.incrementDate(DateUtil.toDate(compDataDTO.weekStartDate), 7), Constants.APP_DATE_FORMAT);
						/*paItemInfo.setCompCurRegPrice(currentPrice);
						paItemInfo.setCompCurRegPriceEffDate(currentPriceChangeDate);
						paItemInfo.setCompCurRegPriceLastObsDate(lastObsDate);
						return paItemInfo;*/
					}
				}else{
					if(lastObsDate == null){
						currentPrice = compDataDTO.regPrice;
						lastObsDate = compDataDTO.weekStartDate;
					}else{
						if(PrestoUtil.compareTo(DateUtil.toDate(lastObsDate), DateUtil.toDate(compDataDTO.weekStartDate)) >= 0){
							currentPrice = compDataDTO.regPrice;
							lastObsDate = compDataDTO.weekStartDate;
							compItemCode = compDataDTO.itemcode;
						}
					}
				}
			}
		}
		
		paItemInfo.setCompCurRegPrice(currentPrice);
		paItemInfo.setCompCurRegPriceEffDate(currentPriceChangeDate);
		paItemInfo.setCompCurRegPriceLastObsDate(lastObsDate);
		paItemInfo.setCompLirItemCode(compItemCode);
		return paItemInfo;
	}
	
	/**
	 * Determines previous price of an item considering data within dataLookupRange
	 * previous price - last price observed prior to the latest price
	 * @param priceHistory		Price History
	 * @param maxDate			Max Date in the dataLookupRange
	 * @param dataLookupRange	Range within which previous price has to be determined
	 * @param isCompetitor		Price values will be set in competitor related fields if it is true
	 * @return	PAItemInfoDTO with previous price
	 * @throws GeneralException
	 */
	public PAItemInfoDTO getPreviousPrice(ArrayList<CompetitiveDataDTO> priceHistory, double currentPrice, Date maxDate, Integer dataLookupRange, boolean isCompetitor) throws GeneralException{
		Date minDate = DateUtil.incrementDate(maxDate, -(dataLookupRange));
		PAItemInfoDTO paItemInfo = new PAItemInfoDTO();
		if(priceHistory == null || priceHistory.size() <= 0){
			return paItemInfo;
		}
		float previousPrice = Constants.DEFAULT_NA;
		String currentPriceChangeDate = null;
		for(CompetitiveDataDTO compDataDTO : priceHistory){
			Date weekStartDate = DateUtil.toDate(compDataDTO.weekStartDate);
			if(PrestoUtil.compareTo(weekStartDate, minDate) >= 0 && PrestoUtil.compareTo(weekStartDate, maxDate) <= 0){
				if(currentPrice != compDataDTO.regPrice && (((currentPrice - compDataDTO.regPrice)/currentPrice * 100) > priceChangeThreshold) || (((currentPrice - compDataDTO.regPrice)/currentPrice * 100) < -(priceChangeThreshold))){
					currentPriceChangeDate = DateUtil.dateToString(DateUtil.incrementDate(DateUtil.toDate(compDataDTO.weekStartDate), 7), Constants.APP_DATE_FORMAT);
					previousPrice = compDataDTO.regPrice;
					if(!isCompetitor){
						paItemInfo.setBasePreRegPrice(previousPrice);
						paItemInfo.setBaseCurRegPriceEffDate(currentPriceChangeDate);
					}else{
						paItemInfo.setCompPreRegPrice(previousPrice);
						paItemInfo.setCompCurRegPriceEffDate(currentPriceChangeDate);
					}
					return paItemInfo;
				}
			}
		}
		if(!isCompetitor)
			paItemInfo.setBasePreRegPrice(previousPrice);
		else
			paItemInfo.setCompPreRegPrice(previousPrice);
		return paItemInfo;
	}
	
	public PAItemInfoDTO getPreviousCompPrice(ArrayList<CompetitiveDataDTO> priceHistory, double currentPrice, int lirItemCode, Date maxDate, Integer dataLookupRange) throws GeneralException{
		Date minDate = DateUtil.incrementDate(maxDate, -(dataLookupRange));
		PAItemInfoDTO paItemInfo = new PAItemInfoDTO();
		if(priceHistory == null || priceHistory.size() <= 0){
			return paItemInfo;
		}
		float previousPrice = Constants.DEFAULT_NA;
		String currentPriceChangeDate = null;
		for(CompetitiveDataDTO compDataDTO : priceHistory){
			Date weekStartDate = DateUtil.toDate(compDataDTO.weekStartDate);
			if(PrestoUtil.compareTo(weekStartDate, minDate) >= 0 && PrestoUtil.compareTo(weekStartDate, maxDate) <= 0){
				if(compDataDTO.itemcode == lirItemCode && currentPrice != compDataDTO.regPrice && (((currentPrice - compDataDTO.regPrice)/currentPrice * 100) > priceChangeThreshold) || (((currentPrice - compDataDTO.regPrice)/currentPrice * 100) < -(priceChangeThreshold))){
					currentPriceChangeDate = DateUtil.dateToString(DateUtil.incrementDate(DateUtil.toDate(compDataDTO.weekStartDate), 7), Constants.APP_DATE_FORMAT);
					previousPrice = compDataDTO.regPrice;
					paItemInfo.setCompPreRegPrice(previousPrice);
					paItemInfo.setCompCurRegPriceEffDate(currentPriceChangeDate);
					return paItemInfo;
				}
			}
		}
		
		paItemInfo.setCompPreRegPrice(previousPrice);
		return paItemInfo;
	}
	
	/**
	 * Determines current price and current price effective date of an item considering data within dataLookupRange
	 * current price - latest price
	 * current price effective date - start date of the immediate week last price change was observed within the given range
	 * @param priceHistory		Price History
	 * @param maxDate			Max Date in the dataLookupRange
	 * @param dataLookupRange	Range within which current price has to be determined
	 * @return	PAItemInfoDTO with current price and current price effective date
	 * @throws GeneralException
	 */
	public PAItemInfoDTO getCurrentCost(ArrayList<MovementWeeklyDTO> costHistory, Date maxDate, Integer dataLookupRange) throws GeneralException{
		Date minDate = DateUtil.incrementDate(maxDate, -(dataLookupRange));
		PAItemInfoDTO paItemInfo = new PAItemInfoDTO();
		if(costHistory == null || costHistory.size() <= 0){
			return paItemInfo;
		}
		double currentCost = Constants.DEFAULT_NA;
		String currentCostChangeDate = null;
		boolean isCostSet = false;
		for(MovementWeeklyDTO movDTO : costHistory){
			Date weekStartDate = DateUtil.toDate(movDTO.getWeekStartDate());
			if(PrestoUtil.compareTo(weekStartDate, minDate) >= 0 && PrestoUtil.compareTo(weekStartDate, maxDate) <= 0){
				if(currentCost != Constants.DEFAULT_NA){
					if(currentCost != movDTO.getListCost() && (((currentCost - movDTO.getListCost())/currentCost * 100) > costChangeThreshold) || (((currentCost - movDTO.getListCost())/currentCost * 100) < -(costChangeThreshold))){
						currentCostChangeDate = DateUtil.dateToString(DateUtil.incrementDate(DateUtil.toDate(movDTO.getWeekStartDate()), 7), Constants.APP_DATE_FORMAT);
						paItemInfo.setBaseCurListCost(currentCost);
						paItemInfo.setBaseCurListCostEffDate(currentCostChangeDate);
						return paItemInfo;
					}
				}else{
					if(!isCostSet){
						currentCost = movDTO.getListCost();
						isCostSet = true;
					}
				}
			}
		}
		paItemInfo.setBaseCurListCost(currentCost);
		paItemInfo.setBaseCurListCostEffDate(currentCostChangeDate);
		return paItemInfo;
	}
	
	/**
	 * Determines previous cost of an item considering data within dataLookupRange
	 * previous cost - last cost observed prior to the latest cost
	 * @param costHistory		Cost History
	 * @param maxDate			Max Date in the dataLookupRange
	 * @param dataLookupRange	Range within which previous cost has to be determined
	 * @return	PAItemInfoDTO with previous cost
	 * @throws GeneralException
	 */
	public PAItemInfoDTO getPreviousCost(ArrayList<MovementWeeklyDTO> costHistory, double currentCost, Date maxDate, Integer dataLookupRange) throws GeneralException{
		Date minDate = DateUtil.incrementDate(maxDate, -(dataLookupRange));
		PAItemInfoDTO paItemInfo = new PAItemInfoDTO();
		if(costHistory == null || costHistory.size() <= 0){
			return paItemInfo;
		}
		String currentCostChangeDate = null;
		double previousCost = Constants.DEFAULT_NA;
		for(MovementWeeklyDTO movDTO : costHistory){
			Date weekStartDate = DateUtil.toDate(movDTO.getWeekStartDate());
			if(PrestoUtil.compareTo(weekStartDate, minDate) >= 0 && PrestoUtil.compareTo(weekStartDate, maxDate) <= 0){
				if(currentCost != movDTO.getListCost() && (((currentCost - movDTO.getListCost())/currentCost*100) > costChangeThreshold) || (((currentCost - movDTO.getListCost())/currentCost * 100) < -(costChangeThreshold))){
					previousCost = movDTO.getListCost();
					paItemInfo.setBasePreListCost(previousCost);
					currentCostChangeDate = DateUtil.dateToString(DateUtil.incrementDate(DateUtil.toDate(movDTO.getWeekStartDate()), 7), Constants.APP_DATE_FORMAT);
					paItemInfo.setBaseCurListCostEffDate(currentCostChangeDate);
					return paItemInfo;
				}
			}
		}
		paItemInfo.setBasePreListCost(previousCost);
		return paItemInfo;
	}
	
	/**
	 * Returns average revenue for the past X weeks
	 * X - configured in the property file
	 * @param costHistory		Movement Weekly History
	 * @param maxDate			Process Date
	 * @return	Average revenue
	 * @throws GeneralException
	 */
	public double getAverageRevenue(ArrayList<MovementWeeklyDTO> costHistory, Date maxDate) throws GeneralException{
		double avgRevenue = 0;
		double totRevenue = 0;
		int noOfWeeks = 0;
		int dataRange = getAverageRevenueDays();
		if(costHistory == null || costHistory.size() <= 0)
			return 0;
		
		Date minDate = DateUtil.incrementDate(maxDate, -(dataRange));
		for(MovementWeeklyDTO movDTO : costHistory){
			Date weekStartDate = DateUtil.toDate(movDTO.getWeekStartDate());
			if(PrestoUtil.compareTo(weekStartDate, minDate) >= 0 && PrestoUtil.compareTo(weekStartDate, maxDate) <= 0){
				totRevenue = totRevenue + movDTO.getregGrossRevenue();
				noOfWeeks = noOfWeeks + 1;
			}
		}
		
		if(noOfWeeks > 0){
			avgRevenue = totRevenue / noOfWeeks;
		}
		
		return avgRevenue;
	}
	/**
	 * Returns maximum BASE_PRE_DATA_LOOKUP_RANGE between all available alerts
	 * @param alertList	List of alerts
	 * @return	maximum previous lookup range for base
	 */
	private int getMaximumBaseDataLookupRange(ArrayList<AlertTypesDto> alertList) {
		int maxRange = 0;
		for(AlertTypesDto dto : alertList){
			int basePrevDataLookupRange = dto.getBasePreDataRangeInDays();
			if(maxRange == 0 || basePrevDataLookupRange > maxRange){
				maxRange = basePrevDataLookupRange;
			}
		}
		return maxRange;
	}

	/**
	 * Returns maximum COMP_PRE_DATA_LOOKUP_RANGE between all available alerts
	 * @param alertList	List of alerts
	 * @return	maximum previous lookup range for base
	 */
	private int getMaximumCompDataLookupRange(ArrayList<AlertTypesDto> alertList) {
		int maxRange = 0;
		for(AlertTypesDto dto : alertList){
			int compPrevDataLookupRange = dto.getCompPreDataRangeInDays();
			if(maxRange == 0 || compPrevDataLookupRange > maxRange){
				maxRange = compPrevDataLookupRange;
			}
		}
		return maxRange;
	}
	
	/**
	 * Return user input if provided. If there is no user input, retrieve from PA_USER_LOCATION_MAP. 
	 * If there are no records in PA_USER_LOCATION_MAP, retrieve from LOCATION_COMPETITOR_MAP.
	 * @param conn	Connection
	 * @return		List of LocationCompetitorMapDto
	 */
	public ArrayList<LocationCompetitorMapDTO> getDataFetchCriteria(){
		ArrayList<LocationCompetitorMapDTO> processList = new ArrayList<LocationCompetitorMapDTO>();
		try{
			// If there were no inputs to the program, retrieve from database the parameters to load data for
			if(inputData == null || (inputData != null && inputData.getBaseLocationLevelId() == 0)){
				processList = locCompMapDAO.getPAUserDetails(connection);
				if(processList.size() <= 0){
					processList = locCompMapDAO.getLocationCompetitorDetails(connection);
				}
			}else{
				processList.add(inputData);
			}
		}catch(GeneralException exception){
			logger.info("Unable to retrieve data fetch criteria - " + exception.toString());
			System.exit(-1);
		}
		return processList;
	}
	
	/**
	 * Returns list of all available alerts
	 * @return	List of AlertTypesDTO
	 */
	public ArrayList<AlertTypesDto> getAlertTypes(){
		ArrayList<AlertTypesDto> alertTypes = null;
		try{
			alertTypes = alertTypesDAO.getAlertTypes(connection);
		}catch(GeneralException exception){
			logger.info("Unable to retrieve data alert list - " + exception.toString());
			System.exit(-1);
		}
		return alertTypes;
	}

	/**
	 * Returns number of weeks for which average revenue has to be computed
	 * @return
	 */
	public int getAverageRevenueDays(){
		if(avgRevenueDays == 0){
			avgRevenueDays = Integer.parseInt(PropertyManager.getProperty("PA_FETCHANDFILTER.AVG_REVENUE_WEEKS", "8")) * 7;
		}
		return avgRevenueDays;
	}

	/**
	 * Sets LIG alert codes in the list
	 */
	private void setLIGAlertCodes() {
		String ligAlertCodes = Constants.LIG_ALERT_CODES;
		String[] ligAlertCodesArr = ligAlertCodes.split(",");
		this.ligAlertCodes = new ArrayList<String>();
		for(String alertCode : ligAlertCodesArr){
			this.ligAlertCodes.add(alertCode);
		}
	}
	
	/**
	 * Static method to log the command line arguments
	 */	
    private static void logCommand (String[] args)  {
		StringBuffer sb = new StringBuffer("Command: FetchAndFilterData ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }
    
    private ArrayList<PAItemInfoDTO> populatePAMasterInfo(ArrayList<PAItemInfoDTO> insertList, HashMap<Integer, Integer> paMasterDataIdMap) {
    	ArrayList<String> processedList = new ArrayList<String>();
    	ArrayList<PAItemInfoDTO> paMasterDataList = new ArrayList<PAItemInfoDTO>();
		for(PAItemInfoDTO paItemInfo : insertList){
			String key = paItemInfo.getCalendarId() + "-" + paItemInfo.getLocationCompetitorMapId() + "-" + paItemInfo.getAlertTypesId();
			if(!processedList.contains(key)){
				PAItemInfoDTO paItem = new PAItemInfoDTO();
				paItem.setAlertTypesId(paItemInfo.getAlertTypesId());
				paItem.setCalendarId(paItemInfo.getCalendarId());
				paItem.setLocationCompetitorMapId(paItemInfo.getLocationCompetitorMapId());
				if(paMasterDataIdMap.get(paItemInfo.getAlertTypesId()) == null){
					logger.info(paItem.getCalendarId() + "\t" + paItem.getLocationCompetitorMapId() + "\t" + paItem.getAlertTypesId());
					paMasterDataList.add(paItem);
				}
				processedList.add(key);
			}
		}
		return paMasterDataList;
	}
}
