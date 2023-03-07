/*
 * Title: Load Competitive Data for Price Index
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/12/2012	Janani			Initial Version 
 * Version 0.2	04/26/2012	Janani			Changes for Unrolling price using  
 * 											store to item mapping
 * Version 0.3	06/20/2012	Janani			Change in place where commit is 
 * 											invoked
 *******************************************************************************
 */
package com.pristine.dataload.prestoload;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAOV2;
import com.pristine.dao.DBManager;
import com.pristine.dao.MovementWeeklyDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dataload.CompDataLIGRollup;
import com.pristine.dataload.service.PriceAndCostService;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.fileformatter.AholdMovementFile;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
 
public class PriceAndCostLoader extends PristineFileParser<AholdMovementFile> {

	private static Logger logger = Logger.getLogger("PriceAndCostLoader");
	
	private int calendarId = -1;
	private List<Integer> historyCalendarList = null;
	private String weekStartDate = null;
	private String weekEndDate = null;
	private String pastWeekStartDate = null;
	
	private static String storeNum = "";
	
	private static String ARG_STORE = "STORE=";
	private static String ARG_MOV_RELATIVE_PATH = "MOV_RELATIVE_PATH=";
	
	private static int NO_OF_PAST_WEEKS_TO_PROCESS = -1;
	private static int stopCount;
	private static String movFrequency = Constants.DAILY;
	private static String movRelativePath = "";
		
	// Key - UPC, Value - Movement Weekly Data
	HashMap<String,AholdMovementFile>  weeklyMovementMap = null;
	
	// Key - Item Code, Value - UPC
	private HashMap<Integer,String> itemCodeAndUPCMap = null;
	
	// Keeps distinct UPC's from the weekly movement Key- UPC, Value - ""
	private HashMap<String,String> distinctUPCMap = null;
	
	// Price Index Support By Price Zone
	private static String ARG_ZONE = "ZONE=";
	private static String ARG_LOCATION_ID = "LOCATION_ID=";
	private static String ARG_LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static String zoneNum = "";
	private static int locationId = -1;
	private static int locationLevelId = -1;
	HashMap<String, HashMap<String, List<String>>> itemStoreMapping = new HashMap<>();
	boolean processAuthItems;
	private static boolean costPresent ;
	/**
	 * Class Constructor
	 */
	public PriceAndCostLoader() {
		PropertyManager.initialize("analysis.properties");
		stopCount = Integer.parseInt(PropertyManager.getProperty(
				"DATALOAD.STOP_AFTER", "-1"));
		NO_OF_PAST_WEEKS_TO_PROCESS = Integer.parseInt(PropertyManager.getProperty(
				"MOVEMENT_WEEKS", "-1"));
		if(NO_OF_PAST_WEEKS_TO_PROCESS!= -1)
			NO_OF_PAST_WEEKS_TO_PROCESS = NO_OF_PAST_WEEKS_TO_PROCESS - 2;
		processAuthItems = Boolean.parseBoolean(PropertyManager.getProperty(
				"PRICE_INDEX.PROCESS_AUTH_ITEMS", "false"));
		costPresent=Boolean.parseBoolean(PropertyManager.getProperty(
				"COST_PRESENT", "TRUE"));
	}
	
	/**
	 * Main method of Price and Cost Loader Batch
	 * @param args[0]	STORE=[storenum]
	 * @param args[1]	MOV_RELATIVE_PATH=[RELATIVE_PATH/empty string]
	 * @param args[2]	lastweek/specificweek/currentweek/nextweek
	 * @param args[3]	Will be present only if args[1] is specificweek
	 * @param args[4]	TRUE/FALSE
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-price-and-cost-loader.properties");
		
		PriceAndCostLoader compDataLoad = new PriceAndCostLoader();
		
		
		logger.info("Competitive Data Load started");		 
						
		if (args.length < 3) {
			logger.info("Minimum Arguments Missing");
			logger.info("Possible Arguments: STORE=[NUMBER]/ZONE=[NUMBER] "
					+ "MOV_RELATIVE_PATH=[RELATIVE_PATH/empty string] lastweek/specificweek "
					+ "weekstartdate/currentweek/nextweek [TRUE/FALSE]");
			System.exit(-1);
		}

		if (args[0].indexOf(ARG_STORE) >= 0) {
			storeNum = args[0].substring(ARG_STORE.length());
			locationLevelId = Constants.STORE_LEVEL_ID;
			/*if (storeNum.isEmpty()) {
				logger.info("Argument Value Missing, args[0] STORE=[storenum]");
				System.exit(-1);
			}*/
		} 
		// Price Index Support By Price Zone
		else if(args[0].indexOf(ARG_ZONE) >= 0) {
			zoneNum = args[0].substring(ARG_ZONE.length());
			locationLevelId = Constants.ZONE_LEVEL_ID;
		} 
		
		// Price Index Support By Location level
		else if(args[0].indexOf(ARG_LOCATION_LEVEL_ID) >= 0) {
			locationLevelId = Integer.parseInt(args[0].substring(ARG_LOCATION_LEVEL_ID.length()));
			locationId = Integer.parseInt(args[1].substring(ARG_LOCATION_ID.length()));
		} 
		// Price Index Support By Price Zone - Ends
		else {
			logger.info("Invalid Argument, args[0] STORE=[storenum]/ZONE=[zonenum]");
			System.exit(-1);
		}

		if (args[1].indexOf(ARG_MOV_RELATIVE_PATH) >= 0 && !args[0].startsWith(ARG_LOCATION_LEVEL_ID)) {
			movRelativePath = args[1].substring(ARG_MOV_RELATIVE_PATH.length());			 
		} else if (args[2].indexOf(ARG_MOV_RELATIVE_PATH) >= 0 && args[0].startsWith(ARG_LOCATION_LEVEL_ID)) {
			movRelativePath = args[2].substring(ARG_MOV_RELATIVE_PATH.length());			 
		} else {
			logger.info("Invalid Argument, args[1] MOV_RELATIVE_PATH=[RELATIVE_PATH/empty string]");
			System.exit(-1);
		}	
		
		String weekType = null;
		if(args[0].startsWith(ARG_LOCATION_LEVEL_ID)){
			weekType = args[3];
		}else{
			weekType = args[2];
		}
		if(!(Constants.NEXT_WEEK.equalsIgnoreCase(weekType) 
				|| Constants.CURRENT_WEEK.equalsIgnoreCase(weekType) 
				|| Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType) 
				|| Constants.LAST_WEEK.equalsIgnoreCase(weekType))){
			logger.info("Invalid Argument, args[2] should be lastweek/currentweek/nextweek/specificweek");
        	System.exit(-1);
		}
		
		if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType) 
				&& (args.length < 5) && args[0].startsWith(ARG_LOCATION_LEVEL_ID)){
			logger.info("Invalid Arguments, args[2] lastweek/currentweek/nextweek/specificweek, "
					+ "args[3] data for a specific week, args[4] Include past weeks items");
        	System.exit(-1);
		}else if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType) 
				&& (args.length < 4) && !args[0].startsWith(ARG_LOCATION_LEVEL_ID)){
			logger.info("Invalid Arguments, args[2] lastweek/currentweek/nextweek/specificweek, "
					+ "args[3] data for a specific week, args[4] Include past weeks items");
        	System.exit(-1);
		}
		
		boolean includePastWeeksItems = false;
		String dateStr = null;
		Calendar c = Calendar.getInstance();
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		if(Constants.CURRENT_WEEK.equalsIgnoreCase(weekType)){
			dateStr = dateFormat.format(c.getTime());
		}else if(Constants.NEXT_WEEK.equalsIgnoreCase(weekType)){
			c.add(Calendar.DATE, 7);
			dateStr = dateFormat.format(c.getTime());
		}else if(Constants.LAST_WEEK.equalsIgnoreCase(weekType)){
			c.add(Calendar.DATE, -7);
			dateStr = dateFormat.format(c.getTime());
		}else if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType)){
			if(args[0].startsWith(ARG_LOCATION_LEVEL_ID)){
				dateStr = args[4];	
			}else{
				dateStr = args[3];
			}
			
		}
		if(args[0].startsWith(ARG_LOCATION_LEVEL_ID)){
			if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType)){
				if(args.length == 6 && args[5] != null){
					includePastWeeksItems = Boolean.parseBoolean(args[5]);
				}
			}else{
				if(args.length == 5 && args[4] != null){
					includePastWeeksItems = Boolean.parseBoolean(args[4]);
				}
			}
		}else{
			if(Constants.SPECIFIC_WEEK.equalsIgnoreCase(weekType)){
				if(args.length == 5 && args[4] != null){
					includePastWeeksItems = Boolean.parseBoolean(args[4]);
				}
			}else{
				if(args.length == 4 && args[3] != null){
					includePastWeeksItems = Boolean.parseBoolean(args[3]);
				}
			}
		}
		
		// Terminate if no of past weeks to be processed is not specified
		if (includePastWeeksItems && NO_OF_PAST_WEEKS_TO_PROCESS == -1) {
			logger.info("Program Terminated. MOVEMENT_WEEKS is not defined in Analysis.properties");
			System.exit(-1);
		} 
		
		// Terminate if there are more than one movement file or there is no movement file
		if(!movRelativePath.isEmpty()){
			//If movFrequency is WEEKLY, then the itemcode and movement data would be taken
			//from weekly movement file instead from movement_daily table
			movFrequency = Constants.WEEKLY;
			try {
				if(!compDataLoad.CheckMovementFileExistance(movRelativePath))
					System.exit(-1);
			} catch (GeneralException e) {
				logger.error("Exception in Function CheckMovementFileExistance() ");
			}			
		}
		
		long startTime = System.currentTimeMillis();
		
		if(locationLevelId == Constants.STORE_LEVEL_ID)
			compDataLoad.loadPriceAndCostData(storeNum, dateStr, includePastWeeksItems);
		else if(locationLevelId != Constants.STORE_LEVEL_ID 
				&& locationLevelId != Constants.ZONE_LEVEL_ID){
			compDataLoad.loadPriceAndCostDataForLocation(dateStr, includePastWeeksItems);
		}
		else
			compDataLoad.loadPriceAndCostDataForZone(zoneNum, dateStr, includePastWeeksItems);
		
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to load competitive data - " + (endTime - startTime) + "ms");
	}
	
	
	/**
	 * This method loads data into competitive_data and movement_weekly tables
	 * @param storeNum		Store Number
	 * @param inputDate		Date of the week
	 */	 
	public void loadPriceAndCostDataForLocation(String inputDate, boolean includePastWeeksItems){
		Connection conn = null;
		try{
			conn = getOracleConnection();
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			RetailCostDAO retailCostDAO = new RetailCostDAO();
			CompetitiveDataDAOV2 compDataDAO = new CompetitiveDataDAOV2(conn);
			RetailPriceZoneDAO priceZoneDAO = new RetailPriceZoneDAO();
			ScheduleDAO schDAO = new ScheduleDAO();
			
			RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, inputDate, Constants.CALENDAR_WEEK);
			
			weekStartDate = calendarDTO.getStartDate();
			weekEndDate = calendarDTO.getEndDate();
			calendarId = calendarDTO.getCalendarId();
			
			logger.info("Calendar Id - " + calendarDTO.getCalendarId());
			logger.info("Processing for location..." + locationLevelId + "-" + locationId);
			
			setPastWeekStartDate(weekEndDate);
			setCalendarIdHistory(conn, weekStartDate, historyCalendarList);
			
			
			// Retrieve/Insert schedule Id for Chain level
			int	scheduleId = schDAO.populateScheduleIdForLocation(conn, 
					locationLevelId, locationId, weekStartDate, weekEndDate);
			//Get Primary, seconday etc., competitors from PRICE_INDEX_LOCATION table
			PriceZoneDTO chainDTO = new PriceZoneDTO();
			chainDTO.setLocationLevelId(locationLevelId);
			chainDTO.setLocationId(locationId);
			/*priceZoneDAO.getPriceIndexLocation(conn, 
					Constants.CHAIN_LEVEL_ID, chainId);*/
			
			List<String> stores = priceZoneDAO.getStoresForLocation(conn, locationLevelId, locationId);
			if(stores != null && stores.size() > 0){
				chainDTO.setCompStrNo(stores);
			}else{
				logger.error("No Stores for given location. Price and cost not loaded");
				System.exit(-1);
			}
			HashMap<String, List<String>> authItemCodeMap = new HashMap<String, List<String>>();
			if(processAuthItems){
				authItemCodeMap = compDataDAO.getAuthItemCode(stores);
			}
			
			HashMap<String, HashMap<String, Integer>> itemCodeMap = 
					compDataDAO.getItemCodesForLocation(stores, weekStartDate, weekEndDate,authItemCodeMap);
			Set<String> itemCodeSet = itemCodeMap.keySet();
			logger.info("Number of items - " + itemCodeSet.size());
			
			// Process Competitive Data
			processCompetitiveData(conn, itemCodeSet, null, null, 
					scheduleId, compDataDAO, retailPriceDAO, 
					chainDTO, locationLevelId, itemCodeMap,null);
			
			// Process Movement Weekly Data
			processMovementWeeklyData(conn, itemCodeSet, null, null, 
					scheduleId, compDataDAO, retailCostDAO, 
					chainDTO, locationLevelId, itemCodeMap,null);
			
			// Perform LIG rollup
			performLIGRollup(conn, scheduleId);	
			
			PristineDBUtil.close(conn);
			
		}catch(GeneralException | Exception ge){
			logger.error("Error in Competitive Data Load", ge);
			PristineDBUtil.rollbackTransaction(conn, "Competitive Data Load V3");
			System.exit(1);
		}
	
	}
	
	
	/**
	 * This method loads data into competitive_data and movement_weekly tables
	 * @param storeNum		Store Number
	 * @param inputDate		Date of the week
	 */	 
	public void loadPriceAndCostData(String storeNum, String inputDate, boolean includePastWeeksItems){
		Connection conn = null;
		
		try{
			conn = DBManager.getConnection();
			
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, inputDate, Constants.CALENDAR_WEEK);
			
			weekStartDate = calendarDTO.getStartDate();
			weekEndDate = calendarDTO.getEndDate();
			calendarId = calendarDTO.getCalendarId();
			logger.info("Calendar Id - " + calendarDTO.getCalendarId());
			logger.info("Processing for Store - " + storeNum);
			setPastWeekStartDate(weekEndDate);
			setCalendarIdHistory(conn, weekStartDate, historyCalendarList);
			
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			RetailCostDAO retailCostDAO = new RetailCostDAO();
			
			// Retrieve Chain Id
			String chainId = retailPriceDAO.getChainId(conn);
			logger.debug("Chain Id - " + chainId);
			
			CompetitiveDataDAOV2 compDataDAO = new CompetitiveDataDAOV2(conn);
			
			// Retrieve Store Information
			StoreDTO storeInfo = compDataDAO.getStoreInfo(chainId, storeNum);
			if(storeInfo == null){
				logger.error("Invalid Store Number - No data found");
				System.exit(-1);
			}
			storeInfo.zoneNum = PrestoUtil.castZoneNumber(storeInfo.zoneNum);
			
		
			HashMap<String, Integer> storeIdMap = new HashMap<String, Integer>();
			storeIdMap.put(storeNum, storeInfo.strId);
			
			// Retrieve/Insert schedule Id
			HashMap<Integer, Integer> scheduleMap = compDataDAO.populateScheduleId(storeIdMap.values(), weekStartDate, weekEndDate);
			int scheduleId = scheduleMap.get(storeInfo.strId);
			List<String> storeNumList = new ArrayList<String>();
			storeNumList.add(storeNum);
			
			
			TreeSet<String> itemCodeSet;
			Set<String>	itemSet = null;
			
			//Get itemcode from file/database
			if(movFrequency.equals(Constants.WEEKLY)){
				//Retrieve distinct item code (Items from movement file and Items of primary&secondary store in the last 90days) 
				itemSet = GetDistinctItemCodes(conn, storeInfo.strNum,storeInfo.strId, 
						weekStartDate, weekEndDate, pastWeekStartDate, includePastWeeksItems);		
			}else{
				//Retrieve items to load price data
				itemSet = compDataDAO.getItemCodes(storeInfo.strId, 
						weekStartDate, weekEndDate, pastWeekStartDate, includePastWeeksItems,movFrequency);
			}
				
			//To remove unauthorized items or in active items
			//Added new logic to process only Auth and Active items to avoid displaying inactive items in Price Index By Dinesh(04/20/2017)
			if(processAuthItems){
				itemCodeSet = new TreeSet<String>();
				HashMap<String, List<String>> authItemCodeMap = compDataDAO.getAuthItemCode(storeNumList);
				List<String> authItemCodes = authItemCodeMap.get(storeNum);
				logger.info("Auth item code count2: "+authItemCodes.size());
				itemSet.stream().filter(itemCode-> authItemCodes.contains(itemCode)).forEach(itemCode-> {
					itemCodeSet.add(itemCode);
				});
				logger.info("Number of items - " + itemCodeSet.size());
			}else{
				itemCodeSet = new TreeSet<String>(itemSet);
			}
			
			logger.info("Number of items - " + itemCodeSet.size());
			
			long retStoreItemMapStartTime = System.currentTimeMillis();
			Set<String> itemsWithPrice = compDataDAO.retrieveItemsForStore(storeInfo);
			long retStoreItemMapEndTime = System.currentTimeMillis();
			logger.debug("Time taken to retrieve store item map - " + (retStoreItemMapEndTime - retStoreItemMapStartTime) + "ms");
			logger.debug("Number of Items in store item map - " + itemsWithPrice.size());
			
			if(itemsWithPrice == null || itemsWithPrice.size() <= 0){
				logger.warn("No Price Information for items in store - " + storeInfo.strNum);
				System.exit(-1);
			}
			// Process Competitive Data
			processCompetitiveData(conn, itemCodeSet, itemsWithPrice, storeInfo, 
					scheduleId, compDataDAO, retailPriceDAO, null, Constants.STORE_LEVEL_ID, null,null);
			
			// Process Movement Weekly Data
			processMovementWeeklyData(conn, itemCodeSet, itemsWithPrice, storeInfo, 
					scheduleId, compDataDAO, retailCostDAO, null, Constants.STORE_LEVEL_ID, null,null);
			
			
			
			// Perform LIG rollup
			performLIGRollup(conn, scheduleId);
			
			PristineDBUtil.close(conn);
		}catch(GeneralException | Exception ge){
			logger.error("Error in Competitive Data Load", ge);
			PristineDBUtil.rollbackTransaction(conn, "Competitive Data Load V3");
			System.exit(1);
		}
	}
	
	/**
	 * This method loads data into competitive_data and movement_weekly tables
	 * @param zoneNum					Zone Number
	 * @param inputDate					Date of the week
	 * @param includePastWeeksItems		If items from previous weeks needs to be included
	 */	 
	public void loadPriceAndCostDataForZone(String zoneNum, String inputDate, boolean includePastWeeksItems){
		Connection conn = null;
		
		try{
			conn = DBManager.getConnection();
			
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, inputDate, Constants.CALENDAR_WEEK);
			
			
			weekStartDate = calendarDTO.getStartDate();
			weekEndDate = calendarDTO.getEndDate();
			calendarId = calendarDTO.getCalendarId();
			logger.info("Calendar Id - " + calendarDTO.getCalendarId());
			logger.info("Processing for Zone - " + zoneNum);
			setPastWeekStartDate(weekEndDate);
			setCalendarIdHistory(conn, weekStartDate, historyCalendarList);
			
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			RetailCostDAO retailCostDAO = new RetailCostDAO();

			// Retrieve Chain Id
			String chainId = retailPriceDAO.getChainId(conn);
			logger.debug("Chain Id - " + chainId);
			
			CompetitiveDataDAOV2 compDataDAO = new CompetitiveDataDAOV2(conn);
			RetailPriceZoneDAO priceZoneDAO = new RetailPriceZoneDAO();
			ScheduleDAO schDAO = new ScheduleDAO();
			
			PriceZoneDTO priceZoneDTO = priceZoneDAO.getRetailPriceZone(conn, zoneNum);
			
			if(priceZoneDTO == null){
				logger.error("Invalid Zone Number - No data found");
				System.exit(-1);
			}
			
			//Changes added by Prasad 
			List<PriceZoneDTO> ListOfChildZones = new ArrayList<>();
			
			ListOfChildZones = priceZoneDAO.checkAndGetChildZonesIfPresent(conn,priceZoneDTO.getPriceZoneId(),chainId);
			
			// Do not process zones which do not have stores in them - Ends
			
			priceZoneDTO.setPriceZoneNum(PrestoUtil.castZoneNumber(zoneNum));
			
			HashMap<String, List<String>> deptZoneStoreMap = retailPriceDAO.getDeptZoneInfo(conn, chainId, priceZoneDTO.getPriceZoneId());
			priceZoneDTO.setDeptZoneStoreMap(deptZoneStoreMap);
			
			List<String> storesInZone =new ArrayList<String>();
			// Do not process zones which do not have stores in them
			logger.info("priceZoneDTO: "+ priceZoneDTO.getPriceZoneId() + " priceZoneDTO.isGlobalZone() : "+ priceZoneDTO.isGlobalZone());
			if (priceZoneDTO.isGlobalZone()) {

				storesInZone = priceZoneDAO.getStoresInGlobalZone(conn, priceZoneDTO.getPriceZoneId());
				logger.info("storesInZone size :" + storesInZone.size());
			} else {
				storesInZone = priceZoneDAO.getStoresInZone(conn, priceZoneDTO.getPriceZoneId());
			}
			
			if(storesInZone != null && storesInZone.size() > 0){
				priceZoneDTO.setCompStrNo(storesInZone);
			}else{
				logger.error("No Stores in Zone. Price and cost not loaded");
				System.exit(-1);
			}
			
			
			
			
			// Retrieve/Insert schedule Id
			int	scheduleId = schDAO.populateScheduleIdForZone(conn, priceZoneDTO.getPriceZoneId(), weekStartDate, weekEndDate);
			HashMap<String, List<String>> authItemCodeMap = new HashMap<String, List<String>>();
			if(processAuthItems){
				authItemCodeMap = compDataDAO.getAuthItemCode(priceZoneDTO.getCompStrNo());
			}
			HashMap<String, HashMap<String, Integer>> itemCodeMap = 
					compDataDAO.getItemCodesForZone(priceZoneDTO, weekStartDate, weekEndDate, authItemCodeMap);
			Set<String> itemCodeSet = itemCodeMap.keySet();
			logger.info("Number of items - " + itemCodeSet.size());
				
			// Process Competitive Data
			processCompetitiveData(conn, itemCodeSet, null, null, 
					scheduleId, compDataDAO, retailPriceDAO, priceZoneDTO, Constants.ZONE_LEVEL_ID, itemCodeMap,ListOfChildZones);
			
			// Process Movement Weekly Data
			if(costPresent)
				processMovementWeeklyData(conn, itemCodeSet, null, null, 
					scheduleId, compDataDAO, retailCostDAO, priceZoneDTO, Constants.ZONE_LEVEL_ID, itemCodeMap,ListOfChildZones);
			
			// Perform LIG rollup
			performLIGRollup(conn, scheduleId);	
			
			PristineDBUtil.close(conn);
		}catch(GeneralException | Exception ge){
			logger.error("Error in Competitive Data Load", ge);
			PristineDBUtil.rollbackTransaction(conn, "Competitive Data Load V3");
			System.exit(1);
		}
	}
	
	/**
	 * This method invokes LIG rollup function for the given schedule
	 * @param conn			Connection
	 * @param scheduleId	Schedule Id
	 * @throws GeneralException
	 */
	public void performLIGRollup(Connection conn, int scheduleId) throws GeneralException{
		CompDataLIGRollup ligRollup = new CompDataLIGRollup (); 
        ligRollup.LIGLevelRollUp(conn, scheduleId);
        PristineDBUtil.commitTransaction(conn, "Comp Data LIG");
	}
	
	/**
	 * This method processes weekly price data for given set of items and store
	 * @param conn				Connection
	 * @param itemCodeSet		Items to load price data
	 * @param itemsWithPrice	Items for which price information is present for the store
	 * @param storeInfo			Store Information
	 * @param scheduleId		Schedule Id
	 * @param compDataDAO		Competitive Data DAO
	 * @param retailCostDAO		Retail Price DAO
	 * @param priceZoneDTO		Price Zone Information
	 * @param levelTypeId		ZONE = 1, STORE = 2
	 * @param listOfChildZones 
	 * @throws GeneralException
	 * @throws CloneNotSupportedException 
	 */
	public void processCompetitiveData(Connection conn, Set<String> itemCodeSet, 
			Set<String> itemsWithPrice, StoreDTO storeInfo, int scheduleId, 
			CompetitiveDataDAOV2 compDataDAO, RetailPriceDAO retailPriceDAO, 
			PriceZoneDTO priceZoneInfo, int levelTypeId, 
			HashMap<String, HashMap<String, Integer>> itemInfoMap, List<PriceZoneDTO> listOfChildZones) throws GeneralException, CloneNotSupportedException{

		PriceAndCostService priceAndCostService = new PriceAndCostService();
		RetailCostDAO retailCostDAO = new RetailCostDAO();
		// Get Price for calendar id passed in the input
		long startTime = System.currentTimeMillis();
		HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = null;
		if(priceZoneInfo != null)
			priceRolledUpMap = retailPriceDAO.retrieveRetailPriceInfo5Wk(conn, 
					calendarId, -1, priceZoneInfo, weekStartDate, weekEndDate);
		else
			priceRolledUpMap = retailPriceDAO.retrieveRetailPriceInfo5Wk(conn, 
					calendarId, storeInfo.strId, null, weekStartDate, weekEndDate);
		
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to retrieve price data - " + (endTime - startTime));
		logger.debug("Size of Price rolled up map - " + priceRolledUpMap.size());
		
		startTime = System.currentTimeMillis();
		sortListInMap(priceRolledUpMap);
		endTime = System.currentTimeMillis();
		logger.info("Time taken to sort the list - " + (endTime - startTime));
		
		// If Price for an item is not present for the given calendar Id retrieve latest price
		Set<String> itemsNotPresent = new HashSet<String>(); 
		for(String itemCode : itemCodeSet){
			if(priceRolledUpMap.get(itemCode) == null)
				itemsNotPresent.add(itemCode);
		}
		if(itemsNotPresent.size() > 0){
			startTime = System.currentTimeMillis();
			HashMap<String, List<RetailPriceDTO>> priceRolledUpMap2 = retailPriceDAO.
					getRetailPriceInfoHistory(conn, itemsNotPresent, historyCalendarList);
			endTime = System.currentTimeMillis();
			logger.debug("Size of Price rolled up map - " + priceRolledUpMap2.size());
			logger.info("Time taken to retrieve price data from history- " + (endTime - startTime));
			priceRolledUpMap.putAll(priceRolledUpMap2);
		}
		
		//Unroll price data
		long unrollStartTime = System.currentTimeMillis();
		
		
		
		
		// Price Index Support By Price Zone
		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = null;
		if(levelTypeId == Constants.STORE_LEVEL_ID)
			unrolledPriceMap = unrollRetailPriceInfo(priceRolledUpMap, storeInfo, itemsWithPrice);
		// Price Index Support By Chain
		else if(levelTypeId != Constants.STORE_LEVEL_ID 
				&& levelTypeId != Constants.ZONE_LEVEL_ID) {
			startTime = System.currentTimeMillis();
			retailCostDAO.getStoreItemMapAtZonelevel(conn, itemCodeSet, null, itemStoreMapping);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to retrieve items from store_item_map - "
					+ (endTime - startTime) + "ms");
			logger.info("store_item_map size - " + itemStoreMapping.size());
			HashMap<String, List<RetailPriceDTO>> bannerLevelRolledUpMap = priceAndCostService
					.identifyStoresAndFindCommonPrice(priceRolledUpMap, priceZoneInfo, itemInfoMap, itemStoreMapping, calendarId);
			unrolledPriceMap = unrollRetailPriceInfoAtBannerLevel(bannerLevelRolledUpMap, priceZoneInfo, itemInfoMap);
		}
		else
			unrolledPriceMap = unrollRetailPriceInfoForZone(priceRolledUpMap, priceZoneInfo, itemInfoMap,listOfChildZones);
		// Price Index Support By Price Zone - Ends
		logger.debug("Size of Unrolled price list - " + unrolledPriceMap.size());
		long unrollEndTime = System.currentTimeMillis();
		logger.debug("Time taken for unrolling price data - " + (unrollEndTime - unrollStartTime) + "ms");
		
		// Process competitive data for each store
		for(List<RetailPriceDTO> unrolledPriceLst:unrolledPriceMap.values()){
			logger.debug("Size of Unrolled price list inside loop - " + unrolledPriceLst.size());
			long storeProcStartTime = System.currentTimeMillis();
			HashMap<String, CompetitiveDataDTO> compDataMap = new HashMap<String, CompetitiveDataDTO>();
			CompetitiveDataDTO compData = null;
			for(RetailPriceDTO retailPriceDTO:unrolledPriceLst){
				compData = new CompetitiveDataDTO();
				compData.compStrNo = retailPriceDTO.getLevelId();
				if(levelTypeId == Constants.STORE_LEVEL_ID)
					compData.compStrId = storeInfo.strId;
				compData.scheduleId = scheduleId;
				if(retailPriceDTO.getItemcode() == null){
					logger.debug("Item code is null: " + retailPriceDTO.toString());
				}
				compData.itemcode = Integer.parseInt(retailPriceDTO.getItemcode());
				compData.regPrice = retailPriceDTO.getRegPrice();
				compData.regMPrice = retailPriceDTO.getRegMPrice();
				compData.regMPack = retailPriceDTO.getRegQty();
				compData.fSalePrice = retailPriceDTO.getSalePrice();
				compData.fSaleMPrice = retailPriceDTO.getSaleMPrice();
				compData.saleMPack = retailPriceDTO.getSaleQty();
				compData.saleInd = retailPriceDTO.getPromotionFlag();
				compData.effRegRetailStartDate = retailPriceDTO.getRegEffectiveDate();
				compData.effSaleStartDate = retailPriceDTO.getSaleStartDate();
				compData.effSaleEndDate = retailPriceDTO.getSaleEndDate();
				compData.weekStartDate = weekStartDate;
				compData.weekEndDate = weekEndDate;
				compData.isZonePriceDiff = retailPriceDTO.isZonePriceDiff();
				compDataMap.put(compData.scheduleId+","+compData.itemcode,compData);
			}
			logger.debug("No of items to be compared against competitive data table - " + compDataMap.size());
			
			// Retrieve Check Data Id
			long populateChkIdStartTime = System.currentTimeMillis();
			compDataDAO.populateCheckDataIdV2(compDataMap, scheduleId);
			long populateChkIdEndTime = System.currentTimeMillis();
			logger.debug("Time taken for populating check data id - " + (populateChkIdEndTime - populateChkIdStartTime) + "ms");
			
			List<CompetitiveDataDTO> toBeInsertedList = new ArrayList<CompetitiveDataDTO>();
			List<CompetitiveDataDTO> toBeUpdatedList = new ArrayList<CompetitiveDataDTO>();
			
			for(CompetitiveDataDTO compDataDTO:compDataMap.values()){ 
				if(compDataDTO.checkItemId != 0){
					toBeUpdatedList.add(compDataDTO);
				}else{
					toBeInsertedList.add(compDataDTO);
				}
			}
			logger.info("Number of records to be inserted in competitive data- " + toBeInsertedList.size());
			logger.info("Number of records to be updated in competitive data- " + toBeUpdatedList.size());
			long insertStartTime = System.currentTimeMillis();
			compDataDAO.insertCompetitiveData(toBeInsertedList);
			long insertEndTime = System.currentTimeMillis();
			logger.debug("Time taken for inserting records - " + (insertEndTime - insertStartTime) + "ms");
			
			long updateStartTime = System.currentTimeMillis();
			compDataDAO.updateCompetitiveData(toBeUpdatedList);
			long updateEndTime = System.currentTimeMillis();
			logger.debug("Time taken for updating records - " + (updateEndTime - updateStartTime) + "ms");
			
			priceRolledUpMap = null; 
			unrolledPriceMap = null;
			toBeInsertedList = null;
			toBeUpdatedList = null;
			
			long storeProcEndTime = System.currentTimeMillis();
			logger.debug("Time taken to process a store - " + (storeProcEndTime - storeProcStartTime));
			PristineDBUtil.commitTransaction(conn, "Competitive Data Load");
			
			if(!costPresent)
			{
				// Do not populate records in movement weekly table with no data for them in competitive data table
				HashMap<Integer,MovementWeeklyDTO> movementWeeklyMap = new HashMap<Integer, MovementWeeklyDTO>();
				MovementWeeklyDTO movementWeeklyDTO = null;
				SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
				for(CompetitiveDataDTO compDataDTO:compDataMap.values()){
					if(compDataDTO.checkItemId != 0){
						movementWeeklyDTO = new MovementWeeklyDTO();
						if(levelTypeId == Constants.STORE_LEVEL_ID){
							movementWeeklyDTO.setCompStoreId(compDataDTO.compStrId);
							movementWeeklyDTO.setPriceZoneId(0);
						}
						else if(levelTypeId != Constants.STORE_LEVEL_ID 
								&& levelTypeId != Constants.ZONE_LEVEL_ID){
							movementWeeklyDTO.setLocationLevelId(locationLevelId);
							movementWeeklyDTO.setLocationId(locationId);
							movementWeeklyDTO.setPriceZoneId(0);
							movementWeeklyDTO.setCompStoreId(0);
						}
						else{
							movementWeeklyDTO.setPriceZoneId(priceZoneInfo.getPriceZoneId());
							movementWeeklyDTO.setCompStoreId(0);
						}
						movementWeeklyDTO.setItemCode(compDataDTO.itemcode);
						movementWeeklyDTO.setCheckDataId(compDataDTO.checkItemId);
						movementWeeklyDTO.setListCost(compDataDTO.regPrice);
						movementWeeklyDTO.setDealCost(compDataDTO.fSalePrice);
						try{
							if(compDataDTO.effRegRetailStartDate != null && !compDataDTO.effRegRetailStartDate.isEmpty())
								movementWeeklyDTO.setEffListCostDate(sdf.parse(compDataDTO.effRegRetailStartDate));
							if(compDataDTO.effSaleStartDate != null && !compDataDTO.effSaleStartDate.isEmpty())
								movementWeeklyDTO.setDealStartDate(sdf.parse(compDataDTO.effSaleStartDate));
							if(compDataDTO.effSaleEndDate != null && !compDataDTO.effSaleEndDate.isEmpty())
								movementWeeklyDTO.setDealEndDate(sdf.parse(compDataDTO.effSaleEndDate));
						}catch(ParseException pe){
							logger.error("Unable to parse date");
						}
						
						//update movementWeeklyDTO with weekly movement data
						if(movFrequency.equals(Constants.WEEKLY))
						{
							//Added below if condition on 21st Sep 2012. Update movement only if the item 
							//is present in the movement weekly file
							if(itemCodeAndUPCMap.get(movementWeeklyDTO.getItemCode()) != null)
								updateWeeklyMovement(movementWeeklyDTO);
						}
							
						
						movementWeeklyMap.put(movementWeeklyDTO.getCheckDataId(), movementWeeklyDTO);
					}
//					else{
//						noCompetitiveData.add(compDataDTO.scheduleId+","+compDataDTO.itemcode);
//					}
				}
				if(movFrequency.equals(Constants.WEEKLY))
				{
					//Clear hash map as it is not needed from here
					weeklyMovementMap = null;
					itemCodeAndUPCMap = null;
				}
				
				logger.debug("No of items to be compared against movement weekly data table - " + movementWeeklyMap.size());
				
				// Retrieve Check Data Id from competitive data table
				long retMovWeekStrtTime = System.currentTimeMillis();
				MovementWeeklyDAO movementWeeklyDAO = new MovementWeeklyDAO(conn);
				movementWeeklyDAO.populateCheckDataId(movementWeeklyMap);
				long retMovWeekEndTime = System.currentTimeMillis();
				logger.debug("Time taken for populating check data id from movement weekly table - " + (retMovWeekEndTime - retMovWeekStrtTime) + "ms");
				
									
				List<MovementWeeklyDTO> toBeInsertedList1 = new ArrayList<MovementWeeklyDTO>();
				List<MovementWeeklyDTO> toBeUpdatedList1 = new ArrayList<MovementWeeklyDTO>();
				
				for(MovementWeeklyDTO movementWeekly:movementWeeklyMap.values()){ 
					if(Constants.UPDATE_FLAG.equals(movementWeekly.getFlag())){
						toBeUpdatedList1.add(movementWeekly);
					}else{
						toBeInsertedList1.add(movementWeekly);
					}
				}
				
				logger.info("Number of records to be inserted in movement weekly- " + toBeInsertedList1.size());
				logger.info("Number of records to be updated in movement weekly- " + toBeUpdatedList1.size());
				long insertStartTime1 = System.currentTimeMillis();
				if (movFrequency.equals(Constants.WEEKLY)) {
					movementWeeklyDAO.insertMovementWeeklyWithWeeklyData(toBeInsertedList1);
				} else {
					movementWeeklyDAO.insertMovementWeeklyWithDailyData(toBeInsertedList1);
				}
				long insertEndTime1 = System.currentTimeMillis();
				logger.debug("Time taken for inserting records - "
						+ (insertEndTime1 - insertStartTime1) + "ms ");

				long updateStartTime1 = System.currentTimeMillis();
				if (movFrequency.equals(Constants.WEEKLY)) {
					movementWeeklyDAO.updateMovementWeeklyWithWeeklyData(toBeUpdatedList1);
				} else {
					movementWeeklyDAO.updateMovementWeeklyWithDailyData(toBeUpdatedList1);
				}
				long updateEndTime1 = System.currentTimeMillis();
				logger.debug("Time taken for updating records - " + (updateEndTime1 - updateStartTime1) + "ms");
				
				priceRolledUpMap = null;
				unrolledPriceMap = null;
				toBeInsertedList1 = null;
				toBeUpdatedList1 = null;
				
				PristineDBUtil.commitTransaction(conn, "Movement Weekly DataLoad");
			}
		}
	}

	private void sortListInMap(HashMap<String, List<RetailPriceDTO>> priceRolledUpMap) {
		for(Map.Entry<String, List<RetailPriceDTO>> entry : priceRolledUpMap.entrySet()){
			Collections.sort(entry.getValue());
		}
	}

	/**
	 * This method unrolls price data from the rolled up cost information passed as input
	 * @param priceRolledUpMap	Map containing rolled up cost information
	 * @param storeInfo			Store Information
	 * @param itemsWithPrice	Set of items for which price data is available for the store
	 * @return	Map containing unrolled cost data for items
	 * @throws GeneralException
	 */
	private HashMap<String, List<RetailPriceDTO>> unrollRetailPriceInfo(HashMap<String, List<RetailPriceDTO>>priceRolledUpMap, 
			StoreDTO storeInfo, Set<String> itemsWithPrice) throws GeneralException{
		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = new HashMap<String, List<RetailPriceDTO>>();
		
		RetailPriceDTO chainLevelData = null;
		boolean isChainLevelPresent = false;
		RetailPriceDTO zoneLevelData = null;
		
		for(Map.Entry<String, List<RetailPriceDTO>> entry:priceRolledUpMap.entrySet()){
			isChainLevelPresent = false;
			boolean isPopulated = false;
			for(RetailPriceDTO retailPriceDTO:entry.getValue()){
				if(Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
					isChainLevelPresent = true;
					chainLevelData = retailPriceDTO;
				}else if(Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
					if(!isPopulated){
						zoneLevelData = retailPriceDTO;
						// Unroll price for zone level data
						if(retailPriceDTO.getLevelId().equals(storeInfo.zoneNum) 
								|| retailPriceDTO.getLevelId().equals(storeInfo.dept1ZoneNum) 
								|| retailPriceDTO.getLevelId().equals(storeInfo.dept2ZoneNum) 
								|| retailPriceDTO.getLevelId().equals(storeInfo.dept3ZoneNum)){
							zoneLevelData.setLevelId(storeInfo.strNum);
							if(itemsWithPrice.contains(zoneLevelData.getItemcode())){
								populateMap(unrolledPriceMap, zoneLevelData);
								isPopulated = true;
							}
						}
					}
				}else{
					if(!isPopulated){
						if(storeInfo.strNum.equals(retailPriceDTO.getLevelId())){
							populateMap(unrolledPriceMap, retailPriceDTO);
							isPopulated = true;
						}
					}
				}
			}
			
			// Unroll price for chain level data
			if(isChainLevelPresent){
				if(!isPopulated){
					if(itemsWithPrice.contains(chainLevelData.getItemcode())){
						chainLevelData.setLevelId(storeInfo.strNum);
						populateMap(unrolledPriceMap, chainLevelData);
					}
				}
			}
		}
		
		return unrolledPriceMap;
	}
	
	/**
	 * This method unrolls price data from the rolled up cost information passed as input
	 * @param priceRolledUpMap	Map containing rolled up cost information
	 * @param priceZoneInfo		Price Zone Information
	 * @param listOfChildZones 
	 * @param storeDeptZoneMap 
	 * @param itemsWithPrice	Set of items for which price data is available for the zone
	 * @return	Map containing unrolled cost data for items
	 * @throws GeneralException
	 */
	private HashMap<String, List<RetailPriceDTO>> unrollRetailPriceInfoForZone(HashMap<String, List<RetailPriceDTO>>priceRolledUpMap, 
			PriceZoneDTO priceZoneInfo, HashMap<String, HashMap<String, Integer>> itemInfoMap, List<PriceZoneDTO> listOfChildZones) throws GeneralException{
		logger.debug("Inside unrollRetailPriceInfoForZone() of PriceAndCostLoader");
		
		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = new HashMap<String, List<RetailPriceDTO>>();
		
		for(Map.Entry<String, List<RetailPriceDTO>> entry:priceRolledUpMap.entrySet()){
			RetailPriceDTO chainLevelData = null;
			RetailPriceDTO zoneLevelData = null;
			RetailPriceDTO retailPriceDTOFinal = new RetailPriceDTO();
			HashMap<String, Integer> storeMovementMap = itemInfoMap.get(entry.getKey());
			
			if(storeMovementMap != null){
				int totMovementForZone = 0;
				int addedMovement = 0;
				
				Set<String> stores = storeMovementMap.keySet();
				for(Integer movement : storeMovementMap.values()){
					totMovementForZone = totMovementForZone + movement;
				}
				
				if(totMovementForZone > 0){
					boolean isPriceExists = false;
					for(RetailPriceDTO retailPriceDTO:entry.getValue()){
						if(Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
							chainLevelData = retailPriceDTO;
						}else if(Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
							if(priceZoneInfo.getPriceZoneNum().equals(retailPriceDTO.getLevelId())||checkIfTheZoneNumPresentInZoneList(retailPriceDTO.getLevelId(),listOfChildZones)){
								zoneLevelData = retailPriceDTO;
							}else{
								// Changes to handle dept zones
								if(priceZoneInfo.getDeptZoneStoreMap() != null)
									if(priceZoneInfo.getDeptZoneStoreMap().get(retailPriceDTO.getLevelId()) != null){
										List<String> compStrNoList = priceZoneInfo.getDeptZoneStoreMap().get(retailPriceDTO.getLevelId());
										for(String compStrNo : compStrNoList){
											if(stores.contains(compStrNo)){
												addQuantity(retailPriceDTOFinal, retailPriceDTO, storeMovementMap.get(compStrNo));
												addedMovement = addedMovement + storeMovementMap.get(compStrNo);
												isPriceExists = true;
											}
										}
									}
								// Changes to handle dept zones ends
							}
						}else{
							if(stores.contains(retailPriceDTO.getLevelId())){
								addQuantity(retailPriceDTOFinal, retailPriceDTO, storeMovementMap.get(retailPriceDTO.getLevelId()));
								addedMovement = addedMovement + storeMovementMap.get(retailPriceDTO.getLevelId());
								isPriceExists = true;
							}
						}
					}
					
					if(totMovementForZone != addedMovement){
						int movToBeAdded = totMovementForZone - addedMovement;
						if(zoneLevelData != null){
							addQuantity(retailPriceDTOFinal, zoneLevelData, movToBeAdded);
							isPriceExists = true;
						}else if(chainLevelData != null){
							addQuantity(retailPriceDTOFinal, chainLevelData, movToBeAdded);
							isPriceExists = true;
						}
					}
					
					if(isPriceExists){
						calculateAveragePrice(retailPriceDTOFinal, totMovementForZone);
						retailPriceDTOFinal.setLevelId(priceZoneInfo.getPriceZoneNum());
						populateMap(unrolledPriceMap, retailPriceDTOFinal);
					}
				}else{
					boolean isPriceExists = false;
					for(RetailPriceDTO retailPriceDTO:entry.getValue()){
						retailPriceDTOFinal.setItemcode(retailPriceDTO.getItemcode());
						if(Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
							retailPriceDTOFinal = retailPriceDTO;
							isPriceExists = true;
						}else if(Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
							if(priceZoneInfo.getPriceZoneNum().equals(retailPriceDTO.getLevelId())||checkIfTheZoneNumPresentInZoneList(retailPriceDTO.getLevelId(),listOfChildZones)){
								retailPriceDTOFinal = retailPriceDTO;
								isPriceExists = true;
								break;
							}
						}
					}
					if(isPriceExists){
						retailPriceDTOFinal.setLevelId(priceZoneInfo.getPriceZoneNum());
						populateMap(unrolledPriceMap, retailPriceDTOFinal);
					}
				}
			}else{
				boolean isPriceExists = false;
				for(RetailPriceDTO retailPriceDTO:entry.getValue()){
					retailPriceDTOFinal.setItemcode(retailPriceDTO.getItemcode());
					if(Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
						retailPriceDTOFinal = retailPriceDTO;
						isPriceExists = true;
					}else if(Constants.ZONE_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
						if(priceZoneInfo.getPriceZoneNum().equals(retailPriceDTO.getLevelId())||checkIfTheZoneNumPresentInZoneList(retailPriceDTO.getLevelId(),listOfChildZones)){
							retailPriceDTOFinal = retailPriceDTO;
							isPriceExists = true;
							break;
						}
					}
				}
				
				if(isPriceExists){
					retailPriceDTOFinal.setLevelId(priceZoneInfo.getPriceZoneNum());
					populateMap(unrolledPriceMap, retailPriceDTOFinal);
				}
			}
		}
		
		return unrolledPriceMap;
	}
	
	public boolean checkIfTheZoneNumPresentInZoneList(String ZoneNumInput, List<PriceZoneDTO> listOfChildZones) {
		boolean output = false;

		if(listOfChildZones!=null&&listOfChildZones.size()>0) {
			for(PriceZoneDTO zone : listOfChildZones) {
				if(zone.getPriceZoneNum().equals(ZoneNumInput)) {
					output = true;
					break;
				}
			}
		}
		
		return output;
	}
	
	
	
	
	/**
	 * This method unrolls price data from the rolled up price information passed as input
	 * @param priceRolledUpMap	Map containing rolled up price information
	 * @param priceZoneInfo		PRICE_INDEX_LOCATION Information
	 * @param storeDeptZoneMap 
	 * @param itemsWithPrice	Set of items for which price data is available for the zone
	 * @return	Map containing unrolled price data for items at chain level
	 * @throws GeneralException
	 * @throws CloneNotSupportedException 
	 */
	public HashMap<String, List<RetailPriceDTO>> unrollRetailPriceInfoAtBannerLevel
		(HashMap<String, List<RetailPriceDTO>>priceRolledUpMap, 
			PriceZoneDTO priceZoneInfo, HashMap<String, 
			HashMap<String, Integer>> itemInfoMap) throws GeneralException, CloneNotSupportedException{
		logger.debug("Inside unrollRetailPriceInfoForChain() of PriceAndCostLoader");
		
		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = new HashMap<String, List<RetailPriceDTO>>();
		
		for(Map.Entry<String, List<RetailPriceDTO>> entry:priceRolledUpMap.entrySet()){
			RetailPriceDTO chainLevelData = null;
			RetailPriceDTO retailPriceDTOFinal = new RetailPriceDTO();
			HashMap<String, Integer> storeMovementMap = itemInfoMap.get(entry.getKey());
			
			if(storeMovementMap != null){
				int totMovementForBanner = 0;
				int addedMovement = 0;
				for(Integer movement : storeMovementMap.values()){
					totMovementForBanner = totMovementForBanner + movement;
				}
				
				if(totMovementForBanner > 0){
					boolean isPriceExists = false;
					for(RetailPriceDTO retailPriceDTO:entry.getValue()){
						if(Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
							chainLevelData = retailPriceDTO;
						}
					}
					
					if(totMovementForBanner != addedMovement){
						int movToBeAdded = totMovementForBanner - addedMovement;
						if(chainLevelData != null){
							addQuantity(retailPriceDTOFinal, chainLevelData, movToBeAdded);
							isPriceExists = true;
						}
					}
					
					if(isPriceExists){
						calculateAveragePrice(retailPriceDTOFinal, totMovementForBanner);
						retailPriceDTOFinal.setLevelId(String.valueOf(priceZoneInfo.getLocationId()));
						populateMap(unrolledPriceMap, retailPriceDTOFinal);
					}
				}else{
					boolean isPriceExists = false;
					for(RetailPriceDTO retailPriceDTO:entry.getValue()){
						retailPriceDTOFinal.setItemcode(retailPriceDTO.getItemcode());
						if(Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
							retailPriceDTOFinal = retailPriceDTO;
							isPriceExists = true;
						}
					}
					if(isPriceExists){
						retailPriceDTOFinal.setLevelId(String.valueOf(priceZoneInfo.getLocationId()));
						populateMap(unrolledPriceMap, retailPriceDTOFinal);
					}
				}
			}else{
				boolean isPriceExists = false;
				for(RetailPriceDTO retailPriceDTO:entry.getValue()){
					retailPriceDTOFinal.setItemcode(retailPriceDTO.getItemcode());
					if(Constants.CHAIN_LEVEL_TYPE_ID == retailPriceDTO.getLevelTypeId()){
						retailPriceDTOFinal = retailPriceDTO;
						isPriceExists = true;
					}
				}
				
				if(isPriceExists){
					retailPriceDTOFinal.setLevelId(String.valueOf(priceZoneInfo.getLocationId()));
					populateMap(unrolledPriceMap, retailPriceDTOFinal);
				}
			}
		}
		return unrolledPriceMap;
	}
	
	
	public void addQuantity(RetailPriceDTO retailPriceDTOFinal, RetailPriceDTO retailPriceDTOCurrent,
			Integer quantity) {
		retailPriceDTOFinal.setItemcode(retailPriceDTOCurrent.getItemcode());
		retailPriceDTOFinal.setRegPrice(retailPriceDTOFinal.getRegPrice() + ((retailPriceDTOCurrent.getRegQty() > 1
				? (retailPriceDTOCurrent.getRegMPrice() / retailPriceDTOCurrent.getRegQty())
				: retailPriceDTOCurrent.getRegPrice()) * quantity));
		if (retailPriceDTOCurrent.getSaleQty() > 1) {
			retailPriceDTOFinal.setSalePrice(retailPriceDTOFinal.getSalePrice()
					+ (retailPriceDTOCurrent.getSaleMPrice() / retailPriceDTOCurrent.getSaleQty() * quantity));
		} else if (retailPriceDTOCurrent.getSaleQty() == 1) {
			retailPriceDTOFinal.setSalePrice(
					retailPriceDTOFinal.getSalePrice() + (retailPriceDTOCurrent.getSalePrice() * quantity));
		} else {
			retailPriceDTOFinal
					.setSalePrice(
							retailPriceDTOFinal.getSalePrice()
									+ (((retailPriceDTOCurrent.getSalePrice() <= 0)
											? (retailPriceDTOCurrent.getRegQty() > 1
													? (retailPriceDTOCurrent.getRegMPrice()
															/ retailPriceDTOCurrent.getRegQty())
													: retailPriceDTOCurrent.getRegPrice())
											: retailPriceDTOCurrent.getSalePrice()) * quantity));
		}

		if (retailPriceDTOCurrent.getRegQty() >= 0)
			retailPriceDTOFinal.setRegQty(1);
		if (retailPriceDTOCurrent.getSalePrice() > 0 || retailPriceDTOCurrent.getSaleMPrice() > 0)
			retailPriceDTOFinal.setSaleQty(1);

		if (retailPriceDTOCurrent.getSaleQty() > 0 || retailPriceDTOCurrent.getSalePrice() > 0
				|| retailPriceDTOCurrent.getSaleMPrice() > 0) {
			retailPriceDTOFinal.setPromotionFlag(String.valueOf(Constants.YES));
		}
		retailPriceDTOFinal.setRegEffectiveDate(retailPriceDTOCurrent.getRegEffectiveDate());
		retailPriceDTOFinal.setSaleStartDate(retailPriceDTOCurrent.getSaleStartDate());
		retailPriceDTOFinal.setSaleEndDate(retailPriceDTOCurrent.getSaleEndDate());

		// Changes to handle multiples at zone level
		if (retailPriceDTOFinal.getTotalMovement() <= 0 || retailPriceDTOFinal.getTotalMovement() < quantity) {
			retailPriceDTOFinal.setTotalMovement(quantity);
			retailPriceDTOFinal.setUnitRegPrice(retailPriceDTOCurrent.getRegQty() > 1
					? retailPriceDTOCurrent.getRegMPrice() / retailPriceDTOCurrent.getRegQty()
					: retailPriceDTOCurrent.getRegPrice());
			retailPriceDTOFinal.setRegMPack(retailPriceDTOCurrent.getRegQty());
			retailPriceDTOFinal.setUnitSalePrice(retailPriceDTOCurrent.getSaleQty() > 1
					? retailPriceDTOCurrent.getSaleMPrice() / retailPriceDTOCurrent.getSaleQty()
					: retailPriceDTOCurrent.getSalePrice());
			retailPriceDTOFinal.setSaleMPack(retailPriceDTOCurrent.getSaleQty());
		}
		// Changes to handle multiples at zone level - Ends
	}
	
	public void calculateAveragePrice(RetailPriceDTO retailPriceDTOFinal, Integer quantity) {
		// Changes to handle multiples at zone level
		if (PrestoUtil.round((retailPriceDTOFinal.getRegPrice() / quantity), 2) == PrestoUtil
				.round(retailPriceDTOFinal.getUnitRegPrice(), 2)) {
			if (retailPriceDTOFinal.getRegMPack() > 1) {
				retailPriceDTOFinal.setRegQty(retailPriceDTOFinal.getRegMPack());
				retailPriceDTOFinal
						.setRegMPrice(retailPriceDTOFinal.getRegPrice() / quantity * retailPriceDTOFinal.getRegMPack());
				retailPriceDTOFinal.setRegPrice(0);
			} else {
				retailPriceDTOFinal.setRegPrice(retailPriceDTOFinal.getRegPrice() / quantity);
				retailPriceDTOFinal.setRegQty(1);
			}
		} else {
			retailPriceDTOFinal.setRegPrice(retailPriceDTOFinal.getRegPrice() / quantity);
			retailPriceDTOFinal.setRegQty(1);
		}

		//changes for Ahold delahize to consider items with no reg price
		if (retailPriceDTOFinal.getUnitRegPrice()>0 && (!(PrestoUtil.round(retailPriceDTOFinal.getUnitSalePrice(), 2) < PrestoUtil
				.round(retailPriceDTOFinal.getUnitRegPrice(), 2)))) {
			retailPriceDTOFinal.setUnitSalePrice(0);
			retailPriceDTOFinal.setSalePrice(0);
			retailPriceDTOFinal.setSaleQty(0);
			retailPriceDTOFinal.setPromotionFlag(String.valueOf(Constants.NO));
		}

		if (retailPriceDTOFinal.getUnitSalePrice() > 0) {
			retailPriceDTOFinal.setPromotionFlag(String.valueOf(Constants.YES));
			if (PrestoUtil.round((retailPriceDTOFinal.getSalePrice() / quantity), 2) == PrestoUtil
					.round(retailPriceDTOFinal.getUnitSalePrice(), 2)) {
				if (retailPriceDTOFinal.getSaleMPack() > 1) {
					retailPriceDTOFinal.setSaleQty(retailPriceDTOFinal.getSaleMPack());
					retailPriceDTOFinal.setSaleMPrice(
							retailPriceDTOFinal.getSalePrice() / quantity * retailPriceDTOFinal.getSaleMPack());
					retailPriceDTOFinal.setSalePrice(0);
				} else {
					retailPriceDTOFinal.setSalePrice(retailPriceDTOFinal.getSalePrice() / quantity);
					retailPriceDTOFinal.setSaleQty(1);
				}
			} else {
				retailPriceDTOFinal.setSalePrice(retailPriceDTOFinal.getSalePrice() / quantity);
				retailPriceDTOFinal.setSaleQty(1);
			}
		} else {
			retailPriceDTOFinal.setSalePrice(0);
			retailPriceDTOFinal.setSaleQty(0);
			retailPriceDTOFinal.setPromotionFlag(String.valueOf(Constants.NO));
		}
		// Changes to handle multiples at zone level - Ends
	}
	
	/**
	 * This method processes weekly cost data for given set of items and store
	 * @param conn				Connection
	 * @param itemCodeSet		Items to load cost data
	 * @param itemsWithPrice	Items for which price information is present for the store
	 * @param storeInfo			Store Information
	 * @param scheduleId		Schedule Id
	 * @param compDataDAO		Competitive Data DAO
	 * @param retailCostDAO		Retail Cost DAO
	 * @param listOfChildZones 
	 * @throws GeneralException
	 * @throws CloneNotSupportedException 
	 */
	public void processMovementWeeklyData(Connection conn, Set<String> itemCodeSet, Set<String> itemsWithPrice, 
			StoreDTO storeInfo, int scheduleId, CompetitiveDataDAOV2 compDataDAO, RetailCostDAO retailCostDAO,
			PriceZoneDTO priceZoneInfo, int levelTypeId, 
			HashMap<String, HashMap<String, Integer>> itemInfoMap, List<PriceZoneDTO> listOfChildZones) throws GeneralException, CloneNotSupportedException{	

		PriceAndCostService priceAndCostService = new PriceAndCostService();
		long startTime = System.currentTimeMillis();
		HashMap<String, List<RetailCostDTO>> costRolledUpMap = retailCostDAO.getRetailCostInfo5Wk(conn, itemCodeSet, calendarId, false);
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to retrieve cost data - " + (endTime - startTime));
		logger.debug("Size of cost rolled up Map - " + costRolledUpMap.size());
		
		// If Price for an item is not present for the given calendar Id retrieve latest price
		Set<String> itemsNotPresent = new HashSet<String>(); 
		for(String itemCode : itemCodeSet){
			if(costRolledUpMap.get(itemCode) == null)
				itemsNotPresent.add(itemCode);
		}
		if(itemsNotPresent.size() > 0){
			startTime = System.currentTimeMillis();
			HashMap<String, List<RetailCostDTO>> costRolledUpMap2 = 
					retailCostDAO.getRetailCostInfoHistory(conn, itemsNotPresent, historyCalendarList);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to retrieve cost data from history - " + (endTime - startTime));
			logger.debug("Size of Price rolled up map - " + costRolledUpMap2.size());
			costRolledUpMap.putAll(costRolledUpMap2);
		}
		
		//Unroll cost data
		long unrollStartTime = System.currentTimeMillis();
		HashMap<String, List<RetailCostDTO>> unrolledCostMap = null;
		if(levelTypeId == Constants.STORE_LEVEL_ID)
			unrolledCostMap = unrollRetailCostInfo(costRolledUpMap, storeInfo, itemsWithPrice);
		else if(levelTypeId != Constants.STORE_LEVEL_ID 
				&& levelTypeId != Constants.ZONE_LEVEL_ID){
			HashMap<String, List<RetailCostDTO>> bannerLevelRolledUpMap = priceAndCostService
					.identifyStoresAndFindCommonCost(costRolledUpMap, priceZoneInfo, itemInfoMap, itemStoreMapping);
			unrolledCostMap = unrollRetailCostInfoAtBannerLevel(bannerLevelRolledUpMap, priceZoneInfo, itemInfoMap);
		}
		else
			unrolledCostMap = unrollRetailCostInfoForZone(costRolledUpMap, priceZoneInfo, itemInfoMap,listOfChildZones);
		
		logger.debug("Size of Unrolled cost list - " + unrolledCostMap.size());
		long unrollEndTime = System.currentTimeMillis();
		logger.debug("Time taken for unrolling cost data - " + (unrollEndTime - unrollStartTime) + "ms");
		
		Set<String> noCompetitiveData = new HashSet<String>();
		// Process competitive data for each store
		for(List<RetailCostDTO> unrolledCostLst:unrolledCostMap.values()){
			HashMap<String, CompetitiveDataDTO> compDataMap = new HashMap<String, CompetitiveDataDTO>();
			CompetitiveDataDTO compData = null;
			for(RetailCostDTO retailCostDTO:unrolledCostLst){
				compData = new CompetitiveDataDTO();
				compData.compStrNo = retailCostDTO.getLevelId();
				if(levelTypeId == Constants.STORE_LEVEL_ID)
					compData.compStrId = storeInfo.strId;
				compData.scheduleId = scheduleId;
				compData.itemcode = Integer.parseInt(retailCostDTO.getItemcode());
				compData.regPrice = retailCostDTO.getListCost();
				compData.fSalePrice = retailCostDTO.getDealCost();
				compData.saleInd = retailCostDTO.getPromotionFlag();
				compData.effRegRetailStartDate = retailCostDTO.getEffListCostDate();
				compData.effSaleStartDate = retailCostDTO.getDealStartDate();
				compData.effSaleEndDate = retailCostDTO.getDealEndDate();
				compData.weekStartDate = weekStartDate;
				compData.weekEndDate = weekEndDate;
				compDataMap.put(compData.scheduleId+","+compData.itemcode,compData);
			}
			logger.debug("No of items to be compared against competitive data table - " + compDataMap.size());
			
			// Retrieve Check Data Id from competitive data table
			long populateChkIdStartTime = System.currentTimeMillis();
			compDataDAO.populateCheckDataIdV2(compDataMap, scheduleId);
			long populateChkIdEndTime = System.currentTimeMillis();
			logger.debug("Time taken for populating check data id - " + (populateChkIdEndTime - populateChkIdStartTime) + "ms");
			
			
			// Do not populate records in movement weekly table with no data for them in competitive data table
			HashMap<Integer,MovementWeeklyDTO> movementWeeklyMap = new HashMap<Integer, MovementWeeklyDTO>();
			MovementWeeklyDTO movementWeeklyDTO = null;
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
			for(CompetitiveDataDTO compDataDTO:compDataMap.values()){
				if(compDataDTO.checkItemId != 0){
					movementWeeklyDTO = new MovementWeeklyDTO();
					if(levelTypeId == Constants.STORE_LEVEL_ID){
						movementWeeklyDTO.setCompStoreId(compDataDTO.compStrId);
						movementWeeklyDTO.setPriceZoneId(0);
					}
					else if(levelTypeId != Constants.STORE_LEVEL_ID 
							&& levelTypeId != Constants.ZONE_LEVEL_ID){
						movementWeeklyDTO.setLocationLevelId(locationLevelId);
						movementWeeklyDTO.setLocationId(locationId);
						movementWeeklyDTO.setPriceZoneId(0);
						movementWeeklyDTO.setCompStoreId(0);
					}
					else{
						movementWeeklyDTO.setPriceZoneId(priceZoneInfo.getPriceZoneId());
						movementWeeklyDTO.setCompStoreId(0);
					}
					movementWeeklyDTO.setItemCode(compDataDTO.itemcode);
					movementWeeklyDTO.setCheckDataId(compDataDTO.checkItemId);
					movementWeeklyDTO.setListCost(compDataDTO.regPrice);
					movementWeeklyDTO.setDealCost(compDataDTO.fSalePrice);
					try{
						if(compDataDTO.effRegRetailStartDate != null && !compDataDTO.effRegRetailStartDate.isEmpty())
							movementWeeklyDTO.setEffListCostDate(sdf.parse(compDataDTO.effRegRetailStartDate));
						if(compDataDTO.effSaleStartDate != null && !compDataDTO.effSaleStartDate.isEmpty())
							movementWeeklyDTO.setDealStartDate(sdf.parse(compDataDTO.effSaleStartDate));
						if(compDataDTO.effSaleEndDate != null && !compDataDTO.effSaleEndDate.isEmpty())
							movementWeeklyDTO.setDealEndDate(sdf.parse(compDataDTO.effSaleEndDate));
					}catch(ParseException pe){
						logger.error("Unable to parse date");
					}
					
					//update movementWeeklyDTO with weekly movement data
					if(movFrequency.equals(Constants.WEEKLY))
					{
						//Added below if condition on 21st Sep 2012. Update movement only if the item 
						//is present in the movement weekly file
						if(itemCodeAndUPCMap.get(movementWeeklyDTO.getItemCode()) != null)
							updateWeeklyMovement(movementWeeklyDTO);
					}
						
					
					movementWeeklyMap.put(movementWeeklyDTO.getCheckDataId(), movementWeeklyDTO);
				}else{
					noCompetitiveData.add(compDataDTO.scheduleId+","+compDataDTO.itemcode);
				}
			}
			if(movFrequency.equals(Constants.WEEKLY))
			{
				//Clear hash map as it is not needed from here
				weeklyMovementMap = null;
				itemCodeAndUPCMap = null;
			}
			
			logger.debug("No of items to be compared against movement weekly data table - " + movementWeeklyMap.size());
			
			// Retrieve Check Data Id from competitive data table
			long retMovWeekStrtTime = System.currentTimeMillis();
			MovementWeeklyDAO movementWeeklyDAO = new MovementWeeklyDAO(conn);
			movementWeeklyDAO.populateCheckDataId(movementWeeklyMap);
			long retMovWeekEndTime = System.currentTimeMillis();
			logger.debug("Time taken for populating check data id from movement weekly table - " + (retMovWeekEndTime - retMovWeekStrtTime) + "ms");
			
								
			List<MovementWeeklyDTO> toBeInsertedList = new ArrayList<MovementWeeklyDTO>();
			List<MovementWeeklyDTO> toBeUpdatedList = new ArrayList<MovementWeeklyDTO>();
			
			for(MovementWeeklyDTO movementWeekly:movementWeeklyMap.values()){ 
				if(Constants.UPDATE_FLAG.equals(movementWeekly.getFlag())){
					toBeUpdatedList.add(movementWeekly);
				}else{
					toBeInsertedList.add(movementWeekly);
				}
			}
			
			logger.info("Number of records to be inserted in movement weekly- " + toBeInsertedList.size());
			logger.info("Number of records to be updated in movement weekly- " + toBeUpdatedList.size());
			long insertStartTime = System.currentTimeMillis();
			if (movFrequency.equals(Constants.WEEKLY)) {
				movementWeeklyDAO.insertMovementWeeklyWithWeeklyData(toBeInsertedList);
			} else {
				movementWeeklyDAO.insertMovementWeeklyWithDailyData(toBeInsertedList);
			}
			long insertEndTime = System.currentTimeMillis();
			logger.debug("Time taken for inserting records - "
					+ (insertEndTime - insertStartTime) + "ms ");

			long updateStartTime = System.currentTimeMillis();
			if (movFrequency.equals(Constants.WEEKLY)) {
				movementWeeklyDAO.updateMovementWeeklyWithWeeklyData(toBeUpdatedList);
			} else {
				movementWeeklyDAO.updateMovementWeeklyWithDailyData(toBeUpdatedList);
			}
			long updateEndTime = System.currentTimeMillis();
			logger.debug("Time taken for updating records - " + (updateEndTime - updateStartTime) + "ms");
			
			costRolledUpMap = null;
			unrolledCostMap = null;
			toBeInsertedList = null;
			toBeUpdatedList = null;
			
			PristineDBUtil.commitTransaction(conn, "Movement Weekly DataLoad");
		}
		
		StringBuffer noCompDataStr = new StringBuffer();
		for(String noCompData:noCompetitiveData){
			noCompDataStr.append(noCompData);
			noCompDataStr.append("\t");
		}
		logger.info("Items with no competitive data - " + noCompDataStr.toString());
	}
	
	/**
	 * This method unrolls cost data from the rolled up cost information passed as input
	 * @param costRolledUpMap	Map containing rolled up cost information
	 * @param storeInfo			Store Information
	 * @param itemsWithPrice	Set of items for which price data is available for the store
	 * @return	Map containing unrolled cost data for items
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailCostDTO>> unrollRetailCostInfo(HashMap<String, List<RetailCostDTO>>costRolledUpMap, 
			StoreDTO storeInfo, Set<String> itemsWithPrice) throws GeneralException{
		HashMap<String, List<RetailCostDTO>> unrolledCostMap = new HashMap<String, List<RetailCostDTO>>();
		
		RetailCostDTO chainLevelData = null;
		boolean isChainLevelPresent = false;
		RetailCostDTO zoneLevelData = null;
		
		for(Map.Entry<String, List<RetailCostDTO>> entry:costRolledUpMap.entrySet()){
			isChainLevelPresent = false;
			boolean isPopulated = false;
			for(RetailCostDTO retailCostDTO:entry.getValue()){
				if(Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()){
					isChainLevelPresent = true;
					chainLevelData = retailCostDTO;
				}else if(Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()){
					if(!isPopulated){
						zoneLevelData = retailCostDTO;
						// Unroll cost for zone level data
						if(retailCostDTO.getLevelId().equals(storeInfo.zoneNum) 
								|| retailCostDTO.getLevelId().equals(storeInfo.dept1ZoneNum)
								|| retailCostDTO.getLevelId().equals(storeInfo.dept2ZoneNum) 
								|| retailCostDTO.getLevelId().equals(storeInfo.dept3ZoneNum)){
							zoneLevelData.setLevelId(storeInfo.strNum);
							if(itemsWithPrice.contains(zoneLevelData.getItemcode())){
								populateMap(unrolledCostMap, zoneLevelData);
								isPopulated = true;
							}
						}
					}
				}else{
					if(!isPopulated){
						if(storeInfo.strNum.equals(retailCostDTO.getLevelId())){
							populateMap(unrolledCostMap, retailCostDTO);
							isPopulated = true;
						}
					}
				}
			}
			
			// Unroll cost for chain level data
			if(isChainLevelPresent){
				if(!isPopulated){
					if(itemsWithPrice.contains(chainLevelData.getItemcode())){
						chainLevelData.setLevelId(storeInfo.strNum);
						populateMap(unrolledCostMap, chainLevelData);
					}
				}
			}
		}
		
		return unrolledCostMap;
	}

	
	/**
	 * This method unrolls cost data from the rolled up cost information passed as input
	 * @param costRolledUpMap	Map containing rolled up cost information
	 * @param priceZoneInfo		Price Zone Information
	 * @param listOfChildZones 
	 * @param itemsWithPrice	Set of items for which price data is available for the zone
	 * @return	Map containing unrolled cost data for items
	 * @throws GeneralException
	 */
	private HashMap<String, List<RetailCostDTO>> unrollRetailCostInfoForZone(HashMap<String, List<RetailCostDTO>>costRolledUpMap, 
			PriceZoneDTO priceZoneInfo, HashMap<String, HashMap<String, Integer>> itemInfoMap, List<PriceZoneDTO> listOfChildZones) throws GeneralException{
		logger.debug("Inside unrollRetailCostInfoForZone() of PriceAndCostLoader");
		
		HashMap<String, List<RetailCostDTO>> unrolledCostMap = new HashMap<String, List<RetailCostDTO>>();
		
		for(Map.Entry<String, List<RetailCostDTO>> entry:costRolledUpMap.entrySet()){
			RetailCostDTO chainLevelData = null;
			RetailCostDTO zoneLevelData = null;
			RetailCostDTO retailCostDTOFinal = new RetailCostDTO();
			HashMap<String, Integer> storeMovementMap = itemInfoMap.get(entry.getKey());
			
			
			if(storeMovementMap != null){
				Set<String> stores = storeMovementMap.keySet();
				
				int totMovementForZone = 0;
				int addedMovement = 0;
				for(Integer movement : storeMovementMap.values()){
					totMovementForZone = totMovementForZone + movement;
				}
			
				if(totMovementForZone > 0){
					boolean isCostExists = false;
					for(RetailCostDTO retailCostDTO:entry.getValue()){
						if(Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()){
							chainLevelData = retailCostDTO;
						}else if(Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()){
							if(priceZoneInfo.getPriceZoneNum().equals(retailCostDTO.getLevelId())||checkIfTheZoneNumPresentInZoneList(retailCostDTO.getLevelId(),listOfChildZones)){
								zoneLevelData = retailCostDTO;
							}else{
								// Changes to handle dept zones
								if(priceZoneInfo.getDeptZoneStoreMap() != null)
									if(priceZoneInfo.getDeptZoneStoreMap().get(retailCostDTO.getLevelId()) != null){
										List<String> compStrNoList = priceZoneInfo.getDeptZoneStoreMap().get(retailCostDTO.getLevelId());
										for(String compStrNo : compStrNoList){
											if(stores.contains(compStrNo)){
												addQuantity(retailCostDTOFinal, retailCostDTO, storeMovementMap.get(compStrNo));
												addedMovement = addedMovement + storeMovementMap.get(compStrNo);
												isCostExists = true;
											}
										}
									}
								// Changes to handle dept zones ends
							}
						}else{
							if(stores.contains(retailCostDTO.getLevelId())){
								addQuantity(retailCostDTOFinal, retailCostDTO, storeMovementMap.get(retailCostDTO.getLevelId()));
								addedMovement = addedMovement + storeMovementMap.get(retailCostDTO.getLevelId());
								isCostExists = true;
							}
						}
					}
					
					if(totMovementForZone != addedMovement){
						int movToBeAdded = totMovementForZone - addedMovement;
						if(zoneLevelData != null){
							addQuantity(retailCostDTOFinal, zoneLevelData, movToBeAdded);
							isCostExists = true;
						}else if(chainLevelData != null){
							addQuantity(retailCostDTOFinal, chainLevelData, movToBeAdded);
							isCostExists = true;
						}
					}
					
					if(isCostExists){
						calculateAverageCost(retailCostDTOFinal, totMovementForZone);
						retailCostDTOFinal.setLevelId(priceZoneInfo.getPriceZoneNum());
						populateMap(unrolledCostMap, retailCostDTOFinal);
					}
				}
			}else{
				boolean isCostExists = false;
				for(RetailCostDTO retailCostDTO:entry.getValue()){
					retailCostDTOFinal.setItemcode(retailCostDTO.getItemcode());
					if(Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()){
						retailCostDTOFinal = retailCostDTO;
						isCostExists = true;
					}else if(Constants.ZONE_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()){
						if(priceZoneInfo.getPriceZoneNum().equals(retailCostDTO.getLevelId())||checkIfTheZoneNumPresentInZoneList(retailCostDTO.getLevelId(),listOfChildZones)){
							retailCostDTOFinal = retailCostDTO;
							isCostExists = true;
							break;
						}
					}
				}
				if(isCostExists){
					retailCostDTOFinal.setLevelId(priceZoneInfo.getPriceZoneNum());
					populateMap(unrolledCostMap, retailCostDTOFinal);
				}
			}
		}
		
		return unrolledCostMap;
	}
	
	
	/**
	 * This method unrolls cost data from the rolled up cost information passed as input
	 * @param costRolledUpMap	Map containing rolled up cost information
	 * @param priceZoneInfo		Price Zone Information
	 * @param itemsWithPrice	Set of items for which price data is available for the zone
	 * @return	Map containing unrolled cost data for items
	 * @throws GeneralException
	 */
	public HashMap<String, List<RetailCostDTO>> unrollRetailCostInfoAtBannerLevel
		(HashMap<String, List<RetailCostDTO>>costRolledUpMap, 
				PriceZoneDTO priceZoneInfo, 
				HashMap<String, HashMap<String, Integer>> itemInfoMap) throws GeneralException{
		logger.debug("Inside unrollRetailCostInfoForZone() of PriceAndCostLoader");
		
		HashMap<String, List<RetailCostDTO>> unrolledCostMap = new HashMap<String, List<RetailCostDTO>>();
		
		for(Map.Entry<String, List<RetailCostDTO>> entry:costRolledUpMap.entrySet()){
			RetailCostDTO chainLevelData = null;
			RetailCostDTO retailCostDTOFinal = new RetailCostDTO();
			HashMap<String, Integer> storeMovementMap = itemInfoMap.get(entry.getKey());
			if(storeMovementMap != null){
				int totMovementForZone = 0;
				int addedMovement = 0;
				for(Integer movement : storeMovementMap.values()){
					totMovementForZone = totMovementForZone + movement;
				}
			
				if(totMovementForZone > 0){
					boolean isCostExists = false;
					for(RetailCostDTO retailCostDTO:entry.getValue()){
						if(Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()){
							chainLevelData = retailCostDTO;
						}
					}
					
					if(totMovementForZone != addedMovement){
						int movToBeAdded = totMovementForZone - addedMovement;
						if(chainLevelData != null){
							addQuantity(retailCostDTOFinal, chainLevelData, movToBeAdded);
							isCostExists = true;
						}
					}
					
					if(isCostExists){
						calculateAverageCost(retailCostDTOFinal, totMovementForZone);
						retailCostDTOFinal.setLevelId(String.valueOf(priceZoneInfo.getLocationId()));
						populateMap(unrolledCostMap, retailCostDTOFinal);
					}
				}
			}else{
				boolean isCostExists = false;
				for(RetailCostDTO retailCostDTO:entry.getValue()){
					retailCostDTOFinal.setItemcode(retailCostDTO.getItemcode());
					if(Constants.CHAIN_LEVEL_TYPE_ID == retailCostDTO.getLevelTypeId()){
						retailCostDTOFinal = retailCostDTO;
						isCostExists = true;
					}
				}
				if(isCostExists){
					retailCostDTOFinal.setLevelId(String.valueOf(priceZoneInfo.getLocationId()));
					populateMap(unrolledCostMap, retailCostDTOFinal);
				}
			}
		}
		
		return unrolledCostMap;
	}
	
	public void addQuantity(RetailCostDTO retailCostDTOFinal, RetailCostDTO retailCostDTOCurrent, Integer quantity){
		retailCostDTOFinal.setItemcode(retailCostDTOCurrent.getItemcode());
		retailCostDTOFinal.setListCost(retailCostDTOFinal.getListCost() + (retailCostDTOCurrent.getListCost() * quantity));
		retailCostDTOFinal.setDealCost(retailCostDTOFinal.getDealCost() + (retailCostDTOCurrent.getDealCost() * quantity));
		if(retailCostDTOFinal.getDealCost() > 0){
			retailCostDTOFinal.setPromotionFlag(String.valueOf(Constants.YES));
		}
		retailCostDTOFinal.setEffListCostDate(retailCostDTOCurrent.getEffListCostDate());
		retailCostDTOFinal.setDealStartDate(retailCostDTOCurrent.getDealStartDate());
		retailCostDTOFinal.setDealEndDate(retailCostDTOCurrent.getDealEndDate());
	}
	
	public void calculateAverageCost(RetailCostDTO retailCostDTOFinal, Integer quantity){
		retailCostDTOFinal.setListCost(retailCostDTOFinal.getListCost()/quantity);
		retailCostDTOFinal.setDealCost(retailCostDTOFinal.getDealCost()/quantity);
	}
	
	/**
	 * This method populates a HashMap with store number as key and a list of its corresponding retailPriceDTO as its value
	 * @param unrolledPriceMap		Map that needs to be populated with with store number as key and a 
	 * list of its corresponding retailPriceDTO as its value
	 * @param retailPriceDTO		Retail Price DTO that needs to be added to the Map
	 */
	public void populateMap(HashMap<String, List<RetailPriceDTO>> unrolledPriceMap, RetailPriceDTO retailPriceDTO){
		if(unrolledPriceMap.get(retailPriceDTO.getLevelId()) != null){
			List<RetailPriceDTO> tempList = unrolledPriceMap.get(retailPriceDTO.getLevelId());
			tempList.add(retailPriceDTO);
			unrolledPriceMap.put(retailPriceDTO.getLevelId(),tempList);
		}else{
			List<RetailPriceDTO> tempList = new ArrayList<RetailPriceDTO>();
			tempList.add(retailPriceDTO);
			unrolledPriceMap.put(retailPriceDTO.getLevelId(),tempList);
		}
	}
	
	/**
	 * This method populates a HashMap with store number as key and a list of its corresponding retailCostDTO as its value
	 * @param unrolledCostMap		Map that needs to be populated with with store number as key and a list of its 
	 * corresponding retailCostDTO as its value
	 * @param retailCostDTO			Retail Cost DTO that needs to be added to the Map
	 */
	public void populateMap(HashMap<String, List<RetailCostDTO>> unrolledCostMap, RetailCostDTO retailCostDTO){
		if(unrolledCostMap.get(retailCostDTO.getLevelId()) != null){
			List<RetailCostDTO> tempList = unrolledCostMap.get(retailCostDTO.getLevelId());
			tempList.add(retailCostDTO);
			unrolledCostMap.put(retailCostDTO.getLevelId(),tempList);
		}else{
			List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
			tempList.add(retailCostDTO);
			unrolledCostMap.put(retailCostDTO.getLevelId(),tempList);
		}
	}
	
	/**
	 * This method sets the Past Week start date for the specified week end date
	 * @param week1EndDate		Week End Date
	 */
	private void setPastWeekStartDate(String week1EndDate) {
		String dateFormat = "MM/dd/yyyy";
		try {
			Date startDate = DateUtil.toDate(DateUtil.getWeekStartDate(DateUtil.toDate(week1EndDate, dateFormat), NO_OF_PAST_WEEKS_TO_PROCESS));
			pastWeekStartDate = DateUtil.dateToString(startDate, dateFormat);
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
    }

	/**
	 * Returns false if there are more than one movement or no movement input
	 * file is found in the movement folder otherwise returns true
	 * @param movRelativePath
	 * @return
	 * @throws GeneralException
	 */
	private boolean CheckMovementFileExistance(String movRelativePath)
			throws GeneralException {
		ArrayList<String> fileList = getFiles(movRelativePath);
		if (fileList.size() > 1 || fileList.size() == 0) {
			logger.info("Program Terminated. More than one movement/no movement input file found.");
			return false;
		}
		return true;
	}
	
	/**
	 * Get the itemcodes from the weekly movement file and database
	 * @param conn
	 * @param movRelativePath			Relative Path of the Weekly Movement File
	 * @param strNo						Store Number
	 * @param weekStartDate				Week start date
	 * @param weekEndDate				Week end date
	 * @param pastWeekStartDate   		Past week start date
	 * @param pastWeekItems				If past week items needs to be included or not
	 * @return							Item codes as Set<String>
	 * @throws GeneralException
	 */
	private Set<String> GetDistinctItemCodes(Connection conn, String strNo, int strId, 
			String weekStartDate, String weekEndDate, String pastWeekStartDate, boolean pastWeekItems) throws GeneralException
	{
		logger.info("Fetching of itemcodes from Movement File Started");
		CompetitiveDataDAOV2 compDataDAO = new CompetitiveDataDAOV2(conn);
		Set<String> itemCodeSet = new HashSet<String>();		 
		Set<String> itemCodeSetFromDB = new HashSet<String>();
		HashMap<Integer, String> allColumns = new HashMap<Integer, String>();
		RetailPriceDAO retailPriceDao = new RetailPriceDAO();
		// Key - UPC, Value - Item Code
		HashMap<String,String> itemCodes = new HashMap<String,String>();
		itemCodeAndUPCMap = new HashMap<Integer,String>();
		weeklyMovementMap = new HashMap<String,AholdMovementFile>();
		
		fillAllColumns(allColumns);
		
		distinctUPCMap = new HashMap<String, String>();
		String fieldNames[] = new String[allColumns.size()];
		ArrayList<String> fileList = getFiles(movRelativePath);

		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}
		// Clear hashmap as it is not required from here
		allColumns = null;

		//Parse the file
		for (int j = 0; j < fileList.size(); j++) {
			parseDelimitedFile(AholdMovementFile.class, fileList.get(j), '|',
					fieldNames, stopCount);
		}
		
		//Get the itemcodes
		itemCodes = retailPriceDao.getItemCode(conn, distinctUPCMap.keySet());
					
		//Clear hash map
		distinctUPCMap = null;

		//Populate the itemcode from hashmap to set
		for (Map.Entry<String, String> itemCode : itemCodes.entrySet()) {
			itemCodeSet.add(itemCode.getValue());
			//Keep itemcode as key and upc as value in another hashmap.
			//This will be used to fill the movement weekly data
			itemCodeAndUPCMap.put(Integer.parseInt(itemCode.getValue()), itemCode.getKey());
		}
		logger.info("Fetching of itemcodes from Movement File Completed");
		
		//Clear hash map
		itemCodes = null;
				
		itemCodeSetFromDB = compDataDAO.getItemCodes(strId, weekStartDate, weekEndDate, pastWeekStartDate, pastWeekItems,movFrequency);
		
		//Add the item code of the db
		for(String itemCode:itemCodeSetFromDB){
			if(!itemCodeSet.contains(itemCode))
				itemCodeSet.add(itemCode);
		}
		
		//Clear the set
		itemCodeSetFromDB = null;
			
		return itemCodeSet;
	}

	@Override
	public void processRecords(List<AholdMovementFile> listObject) throws GeneralException {
		List<AholdMovementFile> aholdMovFile = (List<AholdMovementFile>) listObject;
		// Read all the stores data
		for (int j = 0; j < aholdMovFile.size(); j++) {
			AholdMovementFile aholdMovementFile = aholdMovFile.get(j);

			try {
				// Save only the current store's current week data
				if (aholdMovementFile.getOPCO_LOC_CD().equals(storeNum)
						&& aholdMovementFile.getWeekEndDate().equals(
								weekEndDate) && !aholdMovementFile.isIgnoreItemForMovementDaily()) {					
					weeklyMovementMap.put(aholdMovementFile.getUPC_CD(), aholdMovementFile);
					distinctUPCMap.put(aholdMovementFile.getUPC_CD(), "");			
				}
			} catch (ParseException e) {
				logger.error("Exception in processRecords :" + e);
			}
		}
	}
	
	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns(HashMap<Integer, String> allColumns) {
		allColumns.put(1, "CUSTOM_ATTR2_DESC");
		allColumns.put(2, "UPC_CD");
		allColumns.put(3, "OPCO_LOC_CD");
		allColumns.put(4, "WK_KEY");
		allColumns.put(5, "WK_DESC");
		allColumns.put(6, "SCANNEDUNITS");
		allColumns.put(7, "GROSSDOLLARS");
		allColumns.put(8, "NETDOLLARS");
		allColumns.put(9, "RETAILWEIGHT");
		allColumns.put(10, "POUNDORUNITS");
		allColumns.put(11, "EXTLEVEL3ELEMENTCOST");
		allColumns.put(12, "EXTLEVEL4ELEMENTCOST");
	} 

	
	/**
	 * Updates the MovementWeeklyDto with weekly movement, revenue,
	 * quantity and margin
	 * @param movementWeeklyDto
	 * @throws GeneralException
	 */
	private void updateWeeklyMovement(MovementWeeklyDTO movementWeeklyDto) throws GeneralException {
		
		//Get the UPC of the item code from itemCodeAndUPCMap hash map
		String upc = itemCodeAndUPCMap.get(movementWeeklyDto.getItemCode());
		
		//logger.info("updateWeeklyMovement" + "-" + upc);
		
		//Get the weekly movement data of a UPC from weeklyMovementMap hash map
		AholdMovementFile aholdMovementFile = weeklyMovementMap.get(upc);
		//logger.info("updateWeeklyMovement" + "-" + aholdMovementFile);
		
		aholdMovementFile.initializeVariables();
		
		//Update with weekly movement, revenue and quantity
		double unit = Double.valueOf(aholdMovementFile.getPOUNDORUNITS().trim()).doubleValue();
		double netDollar = Double.valueOf(aholdMovementFile.getNETDOLLARS().trim()).doubleValue();
		double grossDollar = Double.valueOf(aholdMovementFile.getGROSSDOLLARS().trim()).doubleValue();
		
		if(aholdMovementFile.isItemOnSale())
		{
			//Sale
			movementWeeklyDto.setQuantitySale(unit);
			movementWeeklyDto.setRevenueSale(netDollar);
			movementWeeklyDto.setQuantityRegular(0);
			movementWeeklyDto.setRevenueRegular(0);
		}
		else
		{
			//Regular
			movementWeeklyDto.setQuantityRegular(unit);
			movementWeeklyDto.setRevenueRegular(grossDollar);
			movementWeeklyDto.setQuantitySale(0);
			movementWeeklyDto.setRevenueSale(0);
		}		
		 
		//movementWeeklyDto.setDealCost(aholdMovementFile.getDealCostOfUnit());
		double cost=0;
		if(aholdMovementFile.dealCost() != "")
			cost = Double.valueOf(aholdMovementFile.dealCost().trim()).doubleValue();
		else
			cost = Double.valueOf(aholdMovementFile.listCost().trim()).doubleValue();
		
		// Margin
		double finalCost = (movementWeeklyDto.getQuantityRegular() + movementWeeklyDto.getQuantitySale()) * cost;
		movementWeeklyDto.setFinalCost(finalCost);
		double regularRevenue = movementWeeklyDto.getRevenueRegular();
		double saleRevenue = movementWeeklyDto.getRevenueSale();
		movementWeeklyDto.setMargin(regularRevenue + saleRevenue - finalCost);
	 
	}
	
	/**
	 * This method set calendar id history in the input list
	 * @param weekStartDate
	 * @param historyCalendarList
	 */
	private void setCalendarIdHistory(Connection conn, String weekStartDate, List<Integer> historyCalendarList) throws GeneralException{
		List<Integer> calendarList = new ArrayList<Integer>();
		String historyStartDateStr = DateUtil.getWeekStartDate(DateUtil.toDate(weekStartDate), 4);
		Date historyStartDate = DateUtil.toDate(historyStartDateStr);
		
		//String historyEndDateStr = DateUtil.getWeekEndDate(DateUtil.toDate(weekStartDate));
		Calendar cal = Calendar.getInstance();
		cal.setTime(DateUtil.toDate(weekStartDate));
		cal.add(Calendar.DATE, 6);
		Date historyEndDate = cal.getTime();
		
		RetailCalendarDAO calDAO = new RetailCalendarDAO();
		List<RetailCalendarDTO> retailCalDTOList = calDAO.RowTypeBasedCalendarList(conn, historyStartDate, historyEndDate, Constants.CALENDAR_WEEK);
		for(RetailCalendarDTO calendarDTO : retailCalDTOList){
			calendarList.add(calendarDTO.getCalendarId());
		}
		this.historyCalendarList = calendarList;
	}
}

