package com.pristine.dataload;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("unused")
public class MovementWeeklyCompute
{
	// MovementWeeklyCompute  [<start date> | CURRENT_WEEK] [<end date>] [STORE=<store no>] [STORE_SUMMARY_ONLY]
	// 		- start date is Sunday (Sunday of previous week)
	// 		- end date is Saturday (Saturday of week if ignored)
	// MovementWeeklyCompute CURRENT_WEEK
	// MovementWeeklyCompute 2/27/2011
	// MovementWeeklyCompute 2/27/2011 3/5/2011 (same as above)
	
	static Logger 		logger = Logger.getLogger("MovementWeeklyCompute");
	private Date  		_WeekStartDate;
	private Date  		_WeekEndDate;
    private Connection	_Conn = null;
	private int			_Count = 0;
	private int			_CountFailed = 0;
	private List<MovementWeeklyDTO> _SkippedList = new ArrayList<MovementWeeklyDTO>();
	private List<MovementWeeklyDTO> _NoItemList = new ArrayList<MovementWeeklyDTO>();
	private List<MovementWeeklyDTO> _UnAuthorizedItemList = new ArrayList<MovementWeeklyDTO>(); // Changes to handle multiple presto ITEM_CODE with same UPC
	private static boolean checkRetailerItemCode = false; // Changes to handle multiple presto ITEM_CODE with same UPC
	private int			_StopAfter = -1;

	private CompetitiveDataDAO _CompDataDAO;
	private CompetitiveDataDTO _CompDataDTO;
	
	private static boolean noVisitCount =false;
	private static boolean ignoreTransactionNumber = false; //Included on 18th October 2012 for ahold to handle where there is no transaction number
	
	public MovementWeeklyCompute() {
		PropertyManager.initialize("analysis.properties");
		try {
			_Conn = DBManager.getConnection();
			_Conn.setAutoCommit(true);
		} catch (GeneralException gex) {
	        logger.error("Error initializing", gex);
	    }
		catch (Exception ex) {
	        logger.error("Error initializing", ex);
	    }
       
	}

	public static void main(String[] args)
	{
		PropertyConfigurator.configure("log4j-movement-weekly.properties");
		logCommand (args);
		
		boolean ligOnly = false, weeklyOnly = false;
		boolean noVisit13Week = false;
		boolean currentWeek = false, storeSummaryOnly = false;
		String storeNum = null;
		// Price Index Support By Price Zone
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		MovementWeeklyCompute weeklyMovement = new MovementWeeklyCompute();
		String zoneNum = null;
		String locationLevelId = null;
		String locationId = null;
        try
        {
        	Date startDate = null, endDate = null;
        	String startDateStr = null, endDateStr = null;
    		DateFormat df = new SimpleDateFormat("MM/dd/yyyy"); 
    		
    		for ( int ii = 0; ii < args.length; ii++ )
    		{
        		String arg = args[ii];
        		try
        		{
	        		if ( startDate == null ) {
	        			startDateStr = arg;
	        			startDate = df.parse(arg);
	        		}
            		else {
    	        		endDateStr = arg;
    	        		endDate = df.parse(endDateStr);
            		}
        		}
        		catch (Exception ex)
        		{
            		if ( arg.compareToIgnoreCase(ARG_LIG_ONLY) == 0 ) {
            			ligOnly = true;
            		}
            		else if ( arg.compareToIgnoreCase(ARG_WEEKLY_ONLY) == 0 ) {
            			weeklyOnly = true;
            		}
            		else if ( arg.compareToIgnoreCase(ARG_NO_VISIT_13WEEK) == 0 ) {
            			noVisit13Week = true;
            		}
            		else if ( arg.startsWith(ARG_STORE) ) {
            			storeNum = arg.substring(ARG_STORE.length());
            		}
            		// Price Index Support By Price Zone
            		else if( arg.startsWith(ARG_ZONE) ) {
            			zoneNum = arg.substring(ARG_ZONE.length());
            		}
            		// Price Index Support By Price Zone - Ends
            		else if ( arg.compareToIgnoreCase(ARG_CURRENT_WEEK) == 0 ) {
            			currentWeek = true;
            		}
            		//Price Index Support by Chain
            		else if(arg.startsWith(ARG_LOCATION_LEVEL_ID)){
            			locationLevelId = arg.substring(ARG_LOCATION_LEVEL_ID.length());
            		}
            		else if(arg.startsWith(ARG_LOCATION_ID)){
            			locationId = arg.substring(ARG_LOCATION_ID.length());
            		}
            		else if ( arg.compareToIgnoreCase(ARG_STORE_SUMMARY_ONLY) == 0 ) {
            			storeSummaryOnly = true;
            			weeklyOnly = true;
            		}
            		else if ( arg.compareToIgnoreCase(NO_VISIT_COUNT) == 0 ) {
            			noVisitCount = true;
            		}
            		else if ( arg.compareToIgnoreCase(IGNORE_TRANSACTION_NUMBER) == 0 ) {
            			ignoreTransactionNumber = true;
            		}
            		else if ( arg.compareToIgnoreCase(ARG_HISTORY_CORRECTION) == 0) {
            			checkRetailerItemCode = true;
            		}else {
    	        		endDateStr = arg;
    	        		endDate = df.parse(endDateStr);
            		}
        		}
    		}
    		if ( currentWeek ) {
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(new Date());
        		//cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        		startDate = cal.getTime();
        		startDateStr = df.format(startDate);
        		try {
					RetailCalendarDTO calendarDTO = retailCalendarDAO.
							getCalendarId(weeklyMovement._Conn, startDateStr, Constants.CALENDAR_WEEK);
					startDate = df.parse(calendarDTO.getStartDate());
					endDate = df.parse(calendarDTO.getEndDate());
				} catch (GeneralException e) {
					logger.error("Error", e);
				}
    		}
        	if ( startDate == null ) {
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(new Date());
        		//cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        		cal.add(Calendar.DATE, -7);
        		startDate = cal.getTime();
        		startDateStr = df.format(startDate);
        		try {
					RetailCalendarDTO calendarDTO = retailCalendarDAO.
							getCalendarId(weeklyMovement._Conn, startDateStr, Constants.CALENDAR_WEEK);
					startDate = df.parse(calendarDTO.getStartDate());
					endDate = df.parse(calendarDTO.getEndDate());
				} catch (GeneralException e) {
					logger.error("Error", e);
				}
        	}
        	if ( endDate == null ) {
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(startDate);
        		cal.add(Calendar.DATE, 6);
        		endDate = cal.getTime();
        		endDateStr = df.format(endDate);
        	}
        	
        	//MovementWeeklyCompute weeklyMovement = new MovementWeeklyCompute(startDate, endDate);
        	weeklyMovement.setupObjects(startDate, endDate);
        	weeklyMovement._DoStoreSummaryOnly = storeSummaryOnly;
        	weeklyMovement._DoCurrentWeek = currentWeek;
        	weeklyMovement._DoWeekly = !ligOnly;
        	weeklyMovement._DoLIG	 = !weeklyOnly;
        	weeklyMovement._DoVisit13Week = !noVisit13Week;
        	weeklyMovement._StoreNum = storeNum;
        	// Price Index Support By Price Zone
        	weeklyMovement._priceZoneNum = zoneNum;
        	weeklyMovement._locationLevelId = locationLevelId;
        	weeklyMovement._locationId = locationId;
        	
        	
        	weeklyMovement.setupDates();
        	
        	weeklyMovement.setupPriceZoneInfo();
        	
        	if ( !ligOnly )
        	{
				String msg = "Weekly Movement: from " + startDate.toString() + " to " + endDate.toString();
				logger.info(msg);
				weeklyMovement.computeWeeklyMovement();
				logger.info("Weekly Movement successfully completed");
			}

        	if ( !weeklyOnly )
        	{
				String msg = "Weekly LIG Movement: from " + startDate.toString() + " to " + endDate.toString();
				logger.info(msg);
				weeklyMovement.computeWeeklyLIGMovement();
				logger.info("Weekly LIG Movement successfully completed");
			}
        	
    		// Close the connection
    		PristineDBUtil.close(weeklyMovement._Conn);
		}
        catch (Exception ex) {
            logger.error("Error", ex);
        }
    }
	
	private void setupObjects(Date weekStart, Date weekEnd){
		_WeekStartDate = weekStart;
		_WeekEndDate = weekEnd;
		
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy"); 
    	String startDateStr = df.format(weekStart);
    	String endDateStr = df.format(weekEnd);
		try
		{
			try {
				_StopAfter = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
			}
			catch (Exception ex) {
				_StopAfter = -1;
			}
	        _CompDataDAO = new CompetitiveDataDAO(_Conn);
	        String checkUser = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_USER_ID", Constants.EXTERNAL_PRICECHECK_USER);
	        String checkList = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_LIST_ID", Constants.EXTERNAL_PRICECHECK_LIST);
	        _CompDataDAO.setParameters(checkUser, checkList);
	        
	        _CompDataDTO = new CompetitiveDataDTO();
	        _CompDataDTO.weekStartDate = startDateStr;
	        _CompDataDTO.weekEndDate   = endDateStr;
	    }
		catch (GeneralException gex) {
	        logger.error("Error initializing", gex);
	    }
		catch (Exception ex) {
	        logger.error("Error initializing", ex);
	    }
	}
	
	private void setupPriceZoneInfo() {
		RetailPriceZoneDAO priceZoneDAO = new RetailPriceZoneDAO();
		try{
			if(_priceZoneNum != null) {
				priceZoneDTO = priceZoneDAO.getRetailPriceZone(_Conn, _priceZoneNum);
				isGloablZone=priceZoneDTO.isGlobalZone();	
			}else {
				priceZoneDTO = null;
			}
			
		}catch(GeneralException exception){
			logger.error("Zone information not found for - " + _priceZoneNum);
			System.exit(1);
		}
	}
	
	
	private static void logCommand (String[] args)
	{
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: MovementWeeklyCompute ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	}
	
	private void setupDates ()
	{
		Calendar cal = Calendar.getInstance();
		
		cal.setTime(_WeekStartDate);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		_WeekStartDate = cal.getTime();
		
		cal.setTime(_WeekEndDate);
		cal.set(Calendar.HOUR_OF_DAY, 23);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		cal.set(Calendar.MILLISECOND, 900);
		_WeekEndDate = cal.getTime();
	}
		
	private void computeWeeklyMovement ()
	{
		MovementDAO dao = new MovementDAO();
		try
		{
			ArrayList<String> stores = new ArrayList<String>();
			if ( _StoreNum != null ) {
				stores.add (_StoreNum);
			}
			else {
				// Price Index Support By Price Zone - If condition added
				if(_priceZoneNum == null && _locationLevelId == null)
					// Retrieve TOPS stores that had movement
					stores = dao.getTopsMovementStores (_Conn, _WeekStartDate, _WeekEndDate);
			}
				
			for ( int ii = 0; ii < stores.size(); ii++ ) {
				computeWeeklyMovement (dao, stores.get(ii), Constants.STORE_LEVEL_ID);
			}
			
			// Price Index Support By Price Zone
			if(_priceZoneNum != null){
				computeWeeklyMovement (dao, _priceZoneNum, Constants.ZONE_LEVEL_ID);
			}
			// Price Index Support By Price Zone - Ends
			
			if(_locationLevelId != null){
				computeWeeklyMovement (dao, _locationId, Integer.parseInt(_locationLevelId));
			}
			
		}
		catch (GeneralException ex) {
			logger.error("computeWeeklyMovement error: ", ex);
		}
	}
	
	// Price Index Support By Price Zone - Added parameter processLevelTypeId
	private void computeWeeklyMovement (MovementDAO dao, String locationId, int locationLevelId)
	{
		if(locationLevelId == Constants.STORE_LEVEL_ID)
			logger.info("computeWeeklyMovement begin: storeNum=" + locationId);
		else if(locationLevelId != Constants.ZONE_LEVEL_ID
				&& locationLevelId != Constants.STORE_LEVEL_ID)
			logger.info("computeWeeklyMovement begin: chain=" + locationId);
		else
			logger.info("computeWeeklyMovement begin: zoneNum=" + locationId);
		
		_Count = 0;
		_CountFailed = 0;
		
		List<MovementWeeklyDTO> list = null;
		_HashList = new Hashtable<Integer, MovementWeeklyDTO>();
		_NoCheckIdHashList = new Hashtable<Integer, MovementWeeklyDTO>();
		_NoItemHashList = new Hashtable<Integer, MovementWeeklyDTO>();
		int storeId = -1;
		HashMap<Integer, MovementWeeklyDTO> costMap = null; // Changes for performance improvement
		// Changes to handle multiple presto ITEM_CODE with same UPC
		ItemDAO itemDAO = new ItemDAO(); 
		List<Integer> authorizedItemList = new ArrayList<Integer>();
		// Changes to handle multiple presto ITEM_CODE with same UPC - Ends
		try
		{
			// Janani - 6/6/12 - Set subscriber to include chain_id condition in getStoreIdList()
			_CompDataDAO.setSubscriber(_Conn, true);
			if(locationLevelId == Constants.STORE_LEVEL_ID){
				ArrayList<String> storeIdList = _CompDataDAO.getStoreIdList(_Conn, locationId);
				if ( storeIdList.size() > 0 ) {
					storeId = Integer.parseInt(storeIdList.get(0));
					_CompDataDTO.compStrId = storeId;
					_CompDataDTO.checkDate = DateUtil.getDateFromCurrentDate(0);
					_CompDataDTO.scheduleId = _CompDataDAO.getScheduleID(_Conn, _CompDataDTO);
					
					// Changes to handle multiple presto ITEM_CODE with same UPC
					if(checkRetailerItemCode){
						List<Integer> storeList = new ArrayList<Integer>();
						storeList.add(storeId);
						authorizedItemList = itemDAO.getAuthorizedItemsOfZoneAndStore(_Conn, storeList);
					}
					// Changes to handle multiple presto ITEM_CODE with same UPC - Ends
				}
			}
			//Price Index Chain Level
			else if(locationLevelId != Constants.ZONE_LEVEL_ID
					&& locationLevelId != Constants.STORE_LEVEL_ID){
				ScheduleDAO schDAO = new ScheduleDAO();
				_CompDataDTO.scheduleId = schDAO.populateScheduleIdForLocation(_Conn, 
						Integer.parseInt(_locationLevelId), Integer.parseInt(_locationId), _CompDataDTO.weekStartDate, _CompDataDTO.weekEndDate);
			}
			//Price Index Chain Level ENds
			
			// Price Index Support By Price Zone
			else{
				ScheduleDAO schDAO = new ScheduleDAO();
				_CompDataDTO.scheduleId = schDAO.populateScheduleIdForZone(_Conn, priceZoneDTO.getPriceZoneId(), _CompDataDTO.weekStartDate, _CompDataDTO.weekEndDate);
				// Changes to handle multiple presto ITEM_CODE with same UPC
				if (checkRetailerItemCode) {
					List<Integer> storeList = new ArrayList<Integer>();
					if (isGloablZone) {
						storeList = new RetailPriceZoneDAO().getStoreIdsInGlobalZone(_Conn,
								priceZoneDTO.getPriceZoneId());
					} else {
						storeList = new RetailPriceZoneDAO().getStoreIdsInZone(_Conn, priceZoneDTO.getPriceZoneId());
					}
					authorizedItemList = itemDAO.getAuthorizedItemsOfZoneAndStore(_Conn, storeList);
				}
				// Changes to handle multiple presto ITEM_CODE with same UPC - Ends
			}
			// Price Index Support By Price Zone - Ends
			
			// Retrieve visit count
			logger.info("getVisitWeekly begin");
			//Modified below line on 18th Oct 2012, to fetch record irrespective of the transaction number.
			//This change is made for Ahold, where there is no transaction number
			list = dao.getVisitWeekly (_Conn, locationId, _WeekStartDate, _WeekEndDate,ignoreTransactionNumber, locationLevelId,isGloablZone);
			logger.info("getVisitWeekly end: count=" + list.size());
			
			costMap = dao.getCostData(_Conn, _CompDataDTO.scheduleId); // Changes for performance improvement
			
			logger.info("getItemCode begin");
			MovementWeeklyDTO dto = null;
			for ( int ii = 0; ii < list.size(); ii++ )
			{
				_Count++;
				dto = list.get(ii);

				_CompDataDTO.upc = dto.getItemUPC();
				_CompDataDTO.itemcode = dto.getItemCode();
				
				// Changes to handle multiple presto ITEM_CODE with same UPC
				if(checkRetailerItemCode && !authorizedItemList.contains(dto.getItemCode())
						&& (locationLevelId != Constants.STORE_LEVEL_ID 
						&& locationLevelId != Constants.ZONE_LEVEL_ID)){
					logger.debug("Unauthorized Item " + dto.getItemCode());
					_UnAuthorizedItemList.add(dto);
				}else{
					// Price Index By Price Zone
					if(locationLevelId == Constants.STORE_LEVEL_ID)
						dto.setCompStoreId(_CompDataDTO.compStrId);
					else if(locationLevelId != Constants.ZONE_LEVEL_ID
							&& locationLevelId != Constants.STORE_LEVEL_ID){
						dto.setLocationLevelId(Integer.parseInt(_locationLevelId));
						dto.setLocationId(Integer.parseInt(_locationId));
					}
					else
						dto.setPriceZoneId(priceZoneDTO.getPriceZoneId());
						
					if ( _CompDataDTO.itemcode > 0 )
					{
						dto.setItemCode(_CompDataDTO.itemcode);
						
						// Changes for performance improvement
						//dto.setDeptId(_CompDataDTO.deptId);
						
						if(costMap != null && costMap.get(_CompDataDTO.itemcode) != null){
							MovementWeeklyDTO movementWeeklyDTO = costMap.get(_CompDataDTO.itemcode);
							_CompDataDTO.checkItemId = movementWeeklyDTO.getCheckDataId();
						}else{
							_CompDataDTO.checkItemId = -1;
						}
						// Changes for performance improvement - Ends
							
						if ( _CompDataDTO.checkItemId != -1 )
						{
							dto.setCheckDataId(_CompDataDTO.checkItemId);
							// Add to hash
							_HashList.put (dto.getItemCode(), dto);
						}
						else
						{
							// Add to no check id hash
							_NoCheckIdHashList.put (dto.getItemCode(), dto);
							_SkippedList.add(dto);		// skipped from movement weekly processing
						}
					}
					else {
						// Add to no item hash
						_NoItemHashList.put (dto.getItemCode(), dto);
						_NoItemList.add(dto);
					}
				}
			    if ( (_Count % 10000) == 0 ) {
					logger.info("Processed " + String.valueOf(_Count) + " records");
			    }
			}
			logger.info("Processed " + String.valueOf(_Count) + " records");
			logger.info("getItemCode end: hash count=" + _HashList.size());
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		catch (SQLException sex) {
    		logger.error("Error", sex);
		}

		logList(_SkippedList, false, "CheckId not found");
		logList(_NoItemList, false, "Item not found");
		if(checkRetailerItemCode)
			logList(_UnAuthorizedItemList, true, "Unauthorized Items");

		_SkippedList = new ArrayList<MovementWeeklyDTO>();
		_NoItemList = new ArrayList<MovementWeeklyDTO>();
		
		logger.info("getQuantityMovementsForPeriod sale false begin");
		try {
			list = dao.getQuantityMovementsForPeriod (_Conn, locationId, _WeekStartDate, _WeekEndDate, false, locationLevelId,isGloablZone);
			updateMovementHash (dao, list, false, true);
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		logger.info("getQuantityMovementsForPeriod sale false end: count=" + list.size());
		//logSkippedList(false);
		
		logger.info("getQuantityMovementsForPeriod sale true begin");
		try {
			list = dao.getQuantityMovementsForPeriod (_Conn, locationId, _WeekStartDate, _WeekEndDate, true, locationLevelId,isGloablZone);
			updateMovementHash (dao, list, true, true);
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		logger.info("getQuantityMovementsForPeriod sale true end: count=" + list.size());
		//logSkippedList(false);
		
		logger.info("getWeightMovementsForPeriod sale false begin");
		try {
			list = dao.getWeightMovementsForPeriod (_Conn, locationId, _WeekStartDate, _WeekEndDate, false, locationLevelId,isGloablZone);
			updateMovementHash (dao, list, false, true);
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		logger.info("getWeightMovementsForPeriod sale false end: count=" + list.size());
		//logSkippedList(false);
		
		logger.info("getWeightMovementsForPeriod sale true begin");
		try {
			list = dao.getWeightMovementsForPeriod (_Conn, locationId, _WeekStartDate, _WeekEndDate, true, locationLevelId,isGloablZone);
			updateMovementHash (dao, list, true, true);
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		logger.info("getWeightMovementsForPeriod sale true end: count=" + list.size());
		//logSkippedList(false);
		
		// Update Movement_Weekly table
		Object[] dtoArray = _HashList.values().toArray();
		logger.info("updateWeeklyMovement begin: count=" + dtoArray.length);
		_Count = 0;
		_CountFailed = 0;
		
		List<MovementWeeklyDTO> insertList = new ArrayList<MovementWeeklyDTO>();
		List<MovementWeeklyDTO> updateList = new ArrayList<MovementWeeklyDTO>();
		for ( int ii = 0; ii < dtoArray.length; ii++ )
		{
			MovementWeeklyDTO dto = (MovementWeeklyDTO)dtoArray[ii];
			
			// Changes for performance improvement
			// Retrieve cost
			// dao.getCostWeekly(_Conn, dto, processLevelTypeId);
			if(costMap != null){
			MovementWeeklyDTO costDTO = costMap.get(dto.getItemCode());
				if(costDTO != null){
					dto.setListCost(costDTO.getListCost());
					if(costDTO.getDealCost() <= 0){
						dto.setDealCost(costDTO.getListCost());
					}else{
						dto.setDealCost(costDTO.getDealCost());
					}
					
					dto.setRegUnitPrice(costDTO.getRegUnitPrice());
					dto.setSaleUnitPrice(costDTO.getSaleUnitPrice());
				}
			}
			// Changes for performance improvement - Ends
			if(locationLevelId != Constants.STORE_LEVEL_ID
					&& locationLevelId != Constants.ZONE_LEVEL_ID){
				if(dto.getSaleUnitPrice() > 0){
					dto.setRevenueRegular(0);
					dto.setRevenueSale((dto.getQuantityRegular() + dto.getQuantitySale()) * dto.getSaleUnitPrice());
				}else{
					dto.setRevenueRegular((dto.getQuantityRegular() + dto.getQuantitySale()) * dto.getRegUnitPrice());
					dto.setRevenueSale(0);
				}
				//(Total Revenue - Unit * Final Cost) *100/Total Rev
				double totalRevenue = dto.getRevenueRegular() + dto.getRevenueSale();
				double units = dto.getQuantityRegular() + dto.getQuantitySale();
				double margin = totalRevenue - (dto.getDealCost() * units);
				dto.setMargin(margin);
			}else{
				double finalCost = (dto.getQuantityRegular() + dto.getQuantitySale()) * dto.getDealCost();
				dto.setFinalCost(finalCost);
				double regularRevenue = dto.getRevenueRegular();
				double saleRevenue = dto.getRevenueSale();
				dto.setMargin(regularRevenue + saleRevenue - finalCost);
			}
			
			// Changes for performance improvement
			if(dto.getCheckDataId() > 0){
				updateList.add(dto);
			}else{
				insertList.add(dto);
			}
			// Changes for performance improvement - Ends
			
			/*boolean result = true;
			if ( !_DoStoreSummaryOnly )
			{
				try {
					result = dao.updateWeeklyMovement(_Conn, dto, true);
				}
				catch (GeneralException gex) {
		    		logger.error("Error", gex);
					result = false;
				}
			}
			if ( result ) {
				_Count++;
			}
			else {
				_CountFailed++;
			}
			
		    if ( (_Count % 2500) == 0 ) {
				logger.info("Updated " + String.valueOf(_Count) + " records");
		    }*/
			_Count++;
	        if ( _StopAfter > 0 && ii >= _StopAfter ) {
	           	break;
	        }
		}
		
		// Changes for performance improvement
		if ( !_DoStoreSummaryOnly )
		{
			try {
				dao.updateWeeklyMovement(_Conn, updateList);
				dao.insertWeeklyMovement(_Conn, insertList);
			}catch (GeneralException gex) {
	    		logger.error("Error", gex);
			}
		}
		// Changes for performance improvement - Ends
			
			// Compute summary weekly
			/* List<Hashtable<String, MovementWeeklyDTO>> hashList = new ArrayList<Hashtable<String, MovementWeeklyDTO>>();
			hashList.add (_HashList);
			hashList.add (_NoCheckIdHashList);
			hashList.add (_NoItemHashList);
			
			SummaryWeekly summaryWeekly = new SummaryWeekly(_Conn, _WeekStartDate, _WeekEndDate);
			summaryWeekly.setStoreNum(storeNum);
			try {
				//Modified below line on 18th Oct 2012, to fetch record irrespective of the transaction number.
				//This change is made for Ahold, where there is no transaction number
				summaryWeekly.computeStoreSummaryWeekly (_CompDataDTO.compStrId, hashList,ignoreTransactionNumber);
			}
			catch (Exception ex) {
				logger.error("Error", ex);
			}*/
	
		/*logger.info("Updated " + String.valueOf(_Count) + ", failed " + String.valueOf(_CountFailed) + " records");
		if ( _CountFailed > 0 )
			logger.info("Failed to update " + String.valueOf(_CountFailed) + " records");*/
		logger.info("updateWeeklyMovement end");
		
		logger.info("computeWeeklyMovement end: storeNum=" + locationId);
	}

	private void updateMovementHash (MovementDAO dao, List<MovementWeeklyDTO> list, boolean saleFlag, boolean update)
	{
		MovementWeeklyDTO dto = null;
		for ( int ii = 0; ii < list.size(); ii++ )
		{
			dto = list.get(ii);
			/*MovementWeeklyDTO dtoHash = _HashList.get(dto.getItemUPC());
			if ( dtoHash == null ) {
				dtoHash = _NoCheckIdHashList.get(dto.getItemUPC());
			}*/
			// Changes to handle multiple presto ITEM_CODE with same UPC
			MovementWeeklyDTO dtoHash = _HashList.get(dto.getItemCode());
			if ( dtoHash == null ) {
				dtoHash = _NoCheckIdHashList.get(dto.getItemCode());
			}
			// Changes to handle multiple presto ITEM_CODE with same UPC - Ends
			if ( dtoHash != null )
			{
				if ( saleFlag ) {
					dtoHash.setRevenueSale(dto.getTotalPrice());
					dtoHash.setQuantitySale(dto.getExtnQty());
				}
				else {
					dtoHash.setRevenueRegular(dto.getTotalPrice());
					dtoHash.setQuantityRegular(dto.getExtnQty());
				}
			}
		}
	}
	
	//
	// LIG movement
	//
	private void computeWeeklyLIGMovement ()
	{
		// Retrieve TOPS stores
		MovementDAO dao = new MovementDAO();
		try
		{
			ArrayList<String> stores = new ArrayList<String>();
			if ( _StoreNum != null ) {
				stores.add (_StoreNum);
			}
			else {
				// Price Index Support By Price Zone - If condition added
				if(_priceZoneNum == null)
					stores = dao.getTopsStores(_Conn, _WeekStartDate, _WeekEndDate);
			}
				
			for ( int ii = 0; ii < stores.size(); ii++ )
			{
				_StoreNum = stores.get(ii);
				ArrayList<String> storeIdList = _CompDataDAO.getStoreIdList(_Conn, _StoreNum);
				int storeId = Integer.parseInt(storeIdList.get(0));
				_CompDataDTO.scheduleId = _CompDataDAO.getScheduleID(_Conn, _CompDataDTO);
				computeWeeklyLIGMovement (dao, storeId, Constants.STORE_LEVEL_ID);
			}
			
			// Price Index Support By Price Zone
			if(_locationLevelId != null){
				ScheduleDAO schDAO = new ScheduleDAO();
				_CompDataDTO.scheduleId = schDAO.populateScheduleIdForLocation(_Conn, 
						Integer.parseInt(_locationLevelId), Integer.parseInt(_locationId), _CompDataDTO.weekStartDate, _CompDataDTO.weekEndDate);
				computeWeeklyLIGMovement (dao, Integer.parseInt(_locationId), Integer.parseInt(_locationLevelId));
			}
			// Price Index Support By Price Zone - Ends
			
			// Price Index Support By Price Zone
			if(_priceZoneNum != null){
				ScheduleDAO schDAO = new ScheduleDAO();
				_CompDataDTO.scheduleId = schDAO.populateScheduleIdForZone(_Conn, priceZoneDTO.getPriceZoneId(), _CompDataDTO.weekStartDate, _CompDataDTO.weekEndDate);
				computeWeeklyLIGMovement (dao, priceZoneDTO.getPriceZoneId(), Constants.ZONE_LEVEL_ID);
			}
			// Price Index Support By Price Zone - Ends
		}
		catch (GeneralException ex) {
			logger.error("computeWeeklyLIGMovement error: ", ex);
		}
		catch (SQLException ex) {
			logger.error("computeWeeklyLIGMovement error: ", ex);
		}
	}
	
	private void computeWeeklyLIGMovement (MovementDAO dao, int storeId, int processLevelTypeId)
	{
		if(processLevelTypeId == Constants.STORE_LEVEL_ID)
			logger.info("computeWeeklyLIGMovement: store=" + Integer.toString(storeId));
		if(processLevelTypeId != Constants.STORE_LEVEL_ID
				&& processLevelTypeId != Constants.ZONE_LEVEL_ID)
			logger.info("computeWeeklyLIGMovement: chain=" + Integer.toString(storeId));
		else
			logger.info("computeWeeklyLIGMovement: zone=" + Integer.toString(storeId));
		
		_Count = 0;
		_CountFailed = 0;
		_SkippedList = new ArrayList<MovementWeeklyDTO>();
		
		List<MovementWeeklyDTO> list;
		try
		{
			logger.info("getWeeklyMovements begin");
			list = dao.getWeeklyMovements (_Conn, storeId, _CompDataDTO.scheduleId);
			logger.info("getWeeklyMovements end: count=" + list.size());
			
			// Changes for performance improvement
			HashMap<Integer, Integer> compLigCheckDataIdMap = dao.getCompDataLIGForSch(_Conn, _CompDataDTO.scheduleId);
			List<Integer> movementLIGCheckDataIdList = dao.getMovementLIGCheckDataId(_Conn, _CompDataDTO.scheduleId);
			// Changes for performance improvement - Ends
			
			logger.info("LIG processing begin");
			List<MovementWeeklyDTO> listToInsert = new ArrayList<MovementWeeklyDTO>();
			MovementWeeklyDTO dtoToInsert = null;
			
			Map<Integer, Map<Double, Integer>> LigCostMap = new HashMap<>();
			
			for ( int curLirId = -1, ii = 0; ii < list.size(); ii++ )
			{
				MovementWeeklyDTO dto = list.get(ii);
				int lirId = dto.getLirId();
				if ( curLirId != lirId )
				{
					// Different LIG, copy current one to insert list
					if ( dtoToInsert != null )
					{
						int lirItemCode = dtoToInsert.getItemCode();
						int lirCheckDataId = -1;
						if(compLigCheckDataIdMap != null && compLigCheckDataIdMap.get(lirItemCode) != null)
							lirCheckDataId = compLigCheckDataIdMap.get(lirItemCode);
	
						if (lirCheckDataId != -1) {
							dtoToInsert.setCheckDataId(lirCheckDataId);
							//set the most common cost for LIG.
							if (LigCostMap.containsKey(lirItemCode)) {
								double cost = getMostCommonCostForLIG(LigCostMap.get(lirItemCode));
								dtoToInsert.setListCost(cost);
							}
							listToInsert.add (dtoToInsert);
						}
						else {
							String msg = "computeWeeklyLIGMovement: No check data id for lir id -- item code=" + lirId + "--" + lirItemCode;
				    		logger.error(msg);
						}
					}
					
					// Create new dto
					dtoToInsert = new MovementWeeklyDTO();
					dtoToInsert.setLirId(lirId);
				}
				
				// Aggregate same LIG data
				dtoToInsert.setRevenueRegular(dtoToInsert.getRevenueRegular() + dto.getRevenueRegular()); 
				dtoToInsert.setQuantityRegular(dtoToInsert.getQuantityRegular() + dto.getQuantityRegular()); 
				dtoToInsert.setRevenueSale(dtoToInsert.getRevenueSale() + dto.getRevenueSale()); 
				dtoToInsert.setQuantitySale(dtoToInsert.getQuantitySale() + dto.getQuantitySale());
				double margin = dto.getMargin();
				double itemCost = dto.getTotalRevenue() - margin;
				dtoToInsert.setFinalCost(dtoToInsert.getFinalCost() + itemCost);
				if(processLevelTypeId != Constants.ZONE_LEVEL_ID
						&& processLevelTypeId != Constants.STORE_LEVEL_ID)
					dtoToInsert.setMargin(dtoToInsert.getMargin() + margin);
					dtoToInsert.setCheckDataId(dto.getCheckDataId());
					dtoToInsert.setCompStoreId(dto.getCompStoreId());
					dtoToInsert.setItemCode(dto.getLirItemCode());
					dtoToInsert.setListCost(dto.getListCost());
					dtoToInsert.setEffListCostDate(dto.getEffListCostDate());
					dtoToInsert.setDealCost(dto.getDealCost());
					dtoToInsert.setDealStartDate(dto.getDealStartDate());
					dtoToInsert.setDealEndDate(dto.getDealEndDate());
					//Populate map with ligId as key and map of different costs amonst the members 
					//and count for each .This will be used to fetch the most common cost for LIG
				populateLIGCostMap(LigCostMap, dtoToInsert);

				// Save LIR id
				curLirId = lirId;
			}
			
			// Copy last one to insert list
			if ( dtoToInsert != null )
			{
				//Populate map with ligId as key and map of different costs amonst the members 
				//and count for each .This will be used to fetch the most common cost for LIG
				populateLIGCostMap(LigCostMap, dtoToInsert);
				
				int lirItemCode = dtoToInsert.getItemCode();
				int lirCheckDataId = -1;
				if(compLigCheckDataIdMap.get(lirItemCode) != null)
					lirCheckDataId = compLigCheckDataIdMap.get(lirItemCode);
				
				if (lirCheckDataId != -1) {
					dtoToInsert.setCheckDataId(lirCheckDataId);
					if (LigCostMap.containsKey(lirItemCode)) {
						//set the most common cost for LIG
						double cost = getMostCommonCostForLIG(LigCostMap.get(lirItemCode));
						dtoToInsert.setListCost(cost);
					}
					listToInsert.add(dtoToInsert);
				}
				else {
					String msg = "computeWeeklyLIGMovement: No check data id for item code=" + lirItemCode;
		    		logger.error(msg);
				}
			}
			
			Hashtable<Integer, MovementWeeklyDTO> hashList = new Hashtable<Integer, MovementWeeklyDTO>();
			_Count = 0;
			for ( int ii = 0; ii < listToInsert.size(); ii++ )
			{
				MovementWeeklyDTO dto = listToInsert.get(ii);
				// Compute margin, 13 week data
				//double cost = PrestoUtil.getCost(dto.getListCost(), dto.getDealCost());
				//double margin = dto.getTotalRevenue() - ((dto.getQuantityRegular() * dto.getListCost() + dto.getQuantitySale() * cost));
				if(processLevelTypeId != Constants.ZONE_LEVEL_ID
						&& processLevelTypeId != Constants.STORE_LEVEL_ID){
					double margin = dto.getTotalRevenue() - dto.getFinalCost();
					dto.setMargin(margin);
				}
					
				// Moved to Movement13Week.java
				//dao.get13WeekLIGData(_Conn, _WeekStartDate, dto);
				
				// Add item into the hashmap
				Integer itemCode = new Integer(dto.getItemCode());
				hashList.put (itemCode, dto);
				
				_Count++;
		        if ( (_Count % 2500) == 0 ) {
					logger.info("Processed " + String.valueOf(_Count) + " records");
		        }
			}
			logger.info("Processed " + String.valueOf(_Count) + " records");
			logger.info("getCost13WeekData end");
			
			// Retrieve visit count
			logger.info("getLIGVisitWeekly begin");
			//Modified below line on 18th Oct 2012, to fetch record irrespective of the transaction number.
			//This change is made for Ahold, where there is no transaction number
			if(processLevelTypeId == Constants.ZONE_LEVEL_ID)
				list = dao.getLIGVisitWeekly (_Conn, _priceZoneNum, _WeekStartDate, _WeekEndDate,ignoreTransactionNumber, processLevelTypeId,isGloablZone);
			else if(processLevelTypeId != Constants.ZONE_LEVEL_ID
					&& processLevelTypeId != Constants.STORE_LEVEL_ID)
				list = dao.getLIGVisitWeekly (_Conn, _locationId, _WeekStartDate, _WeekEndDate,ignoreTransactionNumber, processLevelTypeId,isGloablZone);
			else
				list = dao.getLIGVisitWeekly (_Conn, _StoreNum, _WeekStartDate, _WeekEndDate,ignoreTransactionNumber, processLevelTypeId,isGloablZone);
			logger.info("getLIGVisitWeekly end: count=" + list.size());
			
			logger.info("insertWeeklyLIGMovement begin");
			_Count = 0;			
			
			List<MovementWeeklyDTO> insertList = new ArrayList<MovementWeeklyDTO>();
			List<MovementWeeklyDTO> updateList = new ArrayList<MovementWeeklyDTO>();
			
			if(noVisitCount)
			{
				//Some clients(Ahold) won't send any tlog, so there won't be any records in  
				//movement_daily, so the above function(getLIGVisitWeekly) will never return anything
				//It inserts all the data from hashList and doesn't consider the visit count
				for (Map.Entry<Integer, MovementWeeklyDTO> hashLst : hashList
						.entrySet()) {
					MovementWeeklyDTO dtoHashList = hashLst.getValue();
					dtoHashList.setVisitCount(0);
				}
			}
			else
			{
				//Update visit count when novisitcount is not false
				for ( int ii = 0; ii < list.size(); ii++ )
				{
					MovementWeeklyDTO dto = list.get(ii);
					MovementWeeklyDTO dtoHashList = hashList.get(new Integer(dto.getItemCode()));
					if ( dtoHashList != null )
					{
						dtoHashList.setVisitCount(dto.getVisitCount());
					}
				}
			}
			
			//Form insert and update lists
			for (Map.Entry<Integer, MovementWeeklyDTO> hashLst : hashList
					.entrySet()) {
					MovementWeeklyDTO dtoHashList = hashLst.getValue();
					if(processLevelTypeId == Constants.ZONE_LEVEL_ID){
						dtoHashList.setPriceZoneId(storeId);
						dtoHashList.setCompStoreId(0);
					}else if(processLevelTypeId != Constants.ZONE_LEVEL_ID
							&& processLevelTypeId != Constants.STORE_LEVEL_ID){
						dtoHashList.setPriceZoneId(0);
						dtoHashList.setCompStoreId(0);
						dtoHashList.setLocationLevelId(Integer.parseInt(_locationLevelId));
						dtoHashList.setLocationId(Integer.parseInt(_locationId));
					}else{
						dtoHashList.setPriceZoneId(0);
						dtoHashList.setCompStoreId(storeId);
					}
					// Changes for performance improvement
					if(movementLIGCheckDataIdList != null && 
							movementLIGCheckDataIdList.contains(dtoHashList.getCheckDataId())){
						updateList.add(dtoHashList);
					}else{
						insertList.add(dtoHashList);
					}
				}
			
			// Changes for performance improvement
			dao.updateWeeklyLIGMovement(_Conn, updateList);
			dao.insertWeeklyLIGMovement(_Conn, insertList);
			// Changes for performance improvement - Ends
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		//logger.info("Inserted " + String.valueOf(_Count) + " records");
		logger.info("insertWeeklyLIGMovement end");
	}
	
	private void logList (List<MovementWeeklyDTO> list, boolean logItemCode, String text)
	{
		if ( list.size() > 0 )
		{
			logger.info("Skipped " + String.valueOf(list.size()) + " records: " + text);
			StringBuffer sb = new StringBuffer();
			for ( int ii = 0; ii < list.size(); ii++ ) {
				if ( ii > 0 ) sb.append(',');
				if ( logItemCode )
					sb.append(list.get(ii).getItemCode());
				else
					sb.append("'").append(list.get(ii).getItemUPC()).append("'");
			}
			logger.debug(sb.toString());
		}
	}

	private boolean _DoStoreSummaryOnly	= false;
	private boolean _DoCurrentWeek	= false;
	private boolean _DoWeekly		= true;
	private boolean _DoLIG			= true;
	private boolean _DoVisit13Week	= true;
	private String 	_StoreNum		= null;
	// Changes to handle multiple presto ITEM_CODE with same UPC - Changing from String to Integer to store ITEM_CODE instead of UPC 
	/*private Hashtable<String, MovementWeeklyDTO> _HashList = null;
	private Hashtable<String, MovementWeeklyDTO> _NoCheckIdHashList = null;
	private Hashtable<String, MovementWeeklyDTO> _NoItemHashList = null;*/
	private Hashtable<Integer, MovementWeeklyDTO> _HashList = null;
	private Hashtable<Integer, MovementWeeklyDTO> _NoCheckIdHashList = null;
	private Hashtable<Integer, MovementWeeklyDTO> _NoItemHashList = null;

	private static String ARG_WEEKLY_ONLY	= "WEEKLY_ONLY";
	private static String ARG_LIG_ONLY		= "LIG_ONLY";
	private static String ARG_NO_VISIT_13WEEK	= "NO_VISIT_13WEEK";
	private static String ARG_STORE			= "STORE=";
	private static String ARG_LOCATION_ID			= "LOCATION_ID=";
	private static String ARG_LOCATION_LEVEL_ID			= "LOCATION_LEVEL_ID=";
	private static String ARG_CURRENT_WEEK	= "CURRENT_WEEK";
	private static String ARG_STORE_SUMMARY_ONLY = "STORE_SUMMARY_ONLY";
	private static String ARG_HISTORY_CORRECTION = "HISTORY_CORRECTION";
	//Applicable for clients who don't send any tlog and there
	//is no data in movement_daily table, also the movement is updated
	//from other mode(from movement file etc...)
	private static String NO_VISIT_COUNT = "NO_VISIT_COUNT";
	//Applicable for clients who doesn't send the transaction number, 
	//transaction number is used to calculate the visit count 
	private static String IGNORE_TRANSACTION_NUMBER = "IGNORE_TRANSACTION_NUMBER";
	
	// Price Index Support By Price Zone
	private static String ARG_ZONE = "ZONE=";
	private String _priceZoneNum = null;
	private String _locationLevelId = null;
	private String _locationId = null;
	private PriceZoneDTO priceZoneDTO = null;
	private boolean isGloablZone=false;
	
	/**
	 * Populate map for lig with cost as key and occurance of that cost as count further to get the most common cost
	 * @param LigCostMap
	 * @param dto
	 */
	public void populateLIGCostMap(Map<Integer, Map<Double, Integer>> LigCostMap, MovementWeeklyDTO dto) {
		int count = 0;
		Map<Double, Integer> temp = new HashMap<Double, Integer>();

		if (LigCostMap.containsKey(dto.getItemCode())) {
			temp = LigCostMap.get(dto.getItemCode());
			if (temp.containsKey(dto.getListCost())) {
				count = temp.get(dto.getListCost());
			}
			temp.put(dto.getListCost(), count + 1);
			LigCostMap.put(dto.getItemCode(), temp);

		} else {
			temp.put(dto.getListCost(), count + 1);
			LigCostMap.put(dto.getItemCode(), temp);
		}
	}

	//get the most common cost for LIG
	private double getMostCommonCostForLIG(Map<Double, Integer> costAndCountMap) {

		int max = 0;
		double mostCommonCost = 0;
		for (Entry<Double, Integer> data : costAndCountMap.entrySet()) {
			if (data.getValue() > max) {
				max = data.getValue();
				mostCommonCost = data.getKey();
			}
		}
		return mostCommonCost;
	}
	
}



