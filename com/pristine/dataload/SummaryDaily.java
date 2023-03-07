package com.pristine.dataload;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.SummaryDailyDAO;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dto.SummaryDailyDTO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SummaryDaily
{
	// SummaryDaily [<date>] [STORE=<store no>]
	// 		- date for summary (default yesterday's date)
	// 		- all stores if not specified
	// SummaryDaily 3/5/2011
	
	static Logger 		logger = Logger.getLogger("SummaryDaily");
	private Date  		_TheDate;
    private Connection	_Conn = null;
	private int			_Count = 0;
	private int			_CountFailed = 0;
	private int			_StopAfter = -1;
	private boolean		_DeleteTemp = true;
	private String		_StoreNum = null;

	private List<MovementDailyAggregateDTO> _SkippedList = new ArrayList<MovementDailyAggregateDTO>();
	private List<MovementDailyAggregateDTO> _NoItemList = new ArrayList<MovementDailyAggregateDTO>();
	
	private Hashtable<String, MovementDailyAggregateDTO> _HashList = null;
	private Hashtable<String, MovementDailyAggregateDTO> _NoCheckIdHashList = null;
	private Hashtable<String, MovementDailyAggregateDTO> _NoItemHashList = null;

	private List<Integer> _ExcludeDeptList = new ArrayList<Integer>();
	private List<String> _IncludeItemList = new ArrayList<String>();
	
	private CompetitiveDataDAO _CompDataDAO;
	private CompetitiveDataDTO _CompDataDTO;
	
	private static String ARG_STORE = "STORE=";
	
	public SummaryDaily (Date weekStart, Date weekEnd, Date theDate)
	{
		_TheDate = theDate;
		
		DateFormat df = new SimpleDateFormat("MM/dd/yyyy"); 
    	String startDateStr = df.format(weekStart);
    	String endDateStr = df.format(weekEnd);
		try
		{
	        PropertyManager.initialize("analysis.properties");
			try {
				_StopAfter = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
			}
			catch (Exception ex) {
				_StopAfter = -1;
			}
	        _Conn = DBManager.getConnection();
	        _Conn.setAutoCommit(true);
	        
	        _DeleteTemp = Boolean.parseBoolean(PropertyManager.getProperty("DATALOAD.DELETE_TEMP", "true"));

	        _CompDataDAO = new CompetitiveDataDAO(_Conn);
	        String checkUser = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_USER_ID", Constants.EXTERNAL_PRICECHECK_USER);
	        String checkList = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_LIST_ID", Constants.EXTERNAL_PRICECHECK_LIST);
	        _CompDataDAO.setParameters(checkUser, checkList);
	        
	        _CompDataDTO = new CompetitiveDataDTO();
	        _CompDataDTO.weekStartDate = startDateStr;
	        _CompDataDTO.weekEndDate   = endDateStr;
	        
	        loadCustomerProperties();
	    }
		catch (GeneralException gex) {
	        logger.error("Error initializing", gex);
	    }
		catch (Exception ex) {
	        logger.error("Error initializing", ex);
	    }
	}

	public static void main(String[] args)
	{
		PropertyConfigurator.configure("log4j-summary-daily.properties");
		logCommand (args);
		
        try
        {
        	Date startDate = null, endDate = null, theDate = null;
    		DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
    		String storeNum = null;
    		
    		for ( int ii = 0; ii < args.length; ii++ )
    		{
        		String arg = args[ii];
        		try
        		{
	        		if ( startDate == null ) {
	        			theDate = df.parse(arg);
	        		}
        		}
        		catch (Exception ex)
        		{
            		if ( arg.startsWith(ARG_STORE) ) {
            			storeNum = arg.substring(ARG_STORE.length());
            		}
        		}
    		}
    		
        	if ( theDate == null ) {
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(new Date());
        		cal.add(Calendar.DATE, -1);
        		theDate = cal.getTime();
        	}
        	if ( startDate == null ) {
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(theDate);
        		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        		startDate = cal.getTime();
        	}
    		Calendar cal = Calendar.getInstance();
    		cal.setTime(startDate);
    		cal.add(Calendar.DATE, 6);
    		endDate = cal.getTime();

        	String msg = "Daily Summary started for " + theDate.toString(); 
    		logger.info(msg);
    		
    		SummaryDaily dailySummary = new SummaryDaily(startDate, endDate, theDate);
    		dailySummary._StoreNum = storeNum;
    		dailySummary.computeDailySummary();
    		
			logger.info("Daily Summary successfully completed");
		}
        catch (Exception ex) {
            logger.error("Error", ex);
        }
    }
	
	private void loadCustomerProperties ()
	{
		// Load customer properties
        String excludeDepts = PropertyManager.getProperty("PI_REVENUE_EXCLUDE_DEPARTMENTS", null);
        String includeItems = PropertyManager.getProperty("PI_REVENUE_INCLUDE_ITEMS", null);
		logger.info("Excluding departments: " + excludeDepts);
		logger.info("Including items: " + includeItems);
        
        if ( excludeDepts != null )
        {
        	String[] depts = excludeDepts.split(",");
        	for ( int ii = 0; ii < depts.length; ii++ ) {
        		int deptId = Integer.parseInt(depts[ii]);
        		_ExcludeDeptList.add (deptId);
        	}
        }
        
        if ( includeItems != null )
        {
        	String[] items = includeItems.split(",");
        	for ( int ii = 0; ii < items.length; ii++ ) {
        		_IncludeItemList.add (items[ii]);
        	}
        }
	}
	
	private static void logCommand (String[] args)
	{
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: SummaryDaily ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	}
	
	private void computeDailySummary ()
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(_TheDate);
		int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
		
		doPeriodSummary(dayOfWeek,  4,  0, 24);		// Full day
		doPeriodSummary(dayOfWeek,  5,  0,  7);		// Midnight - 7 AM
		doPeriodSummary(dayOfWeek,  6,  7,  9);		// 7 AM - 9 AM
		doPeriodSummary(dayOfWeek,  7,  9, 11);		// 9 AM - 11 AM
		doPeriodSummary(dayOfWeek,  8, 11, 14);		// 11 AM - 2 PM
		doPeriodSummary(dayOfWeek,  9, 14, 16);		// 2 PM - 4 PM
		doPeriodSummary(dayOfWeek, 10, 16, 19);		// 4 PM - 7 PM
		doPeriodSummary(dayOfWeek, 11, 19, 21);		// 7 PM - 9 PM
		doPeriodSummary(dayOfWeek, 12, 21, 24);		// 9 PM - 12 PM
		
		// Delete temp data
		//deleteTempData();
		
		PristineDBUtil.close(_Conn);
	}

	private void doPeriodSummary (int dayOfWeekId, int timeOfDayId, int fromHour, int toHour)
	{
		Calendar cal = Calendar.getInstance();
		
		cal.setTime(_TheDate);
		cal.set(Calendar.HOUR_OF_DAY, fromHour);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date fromTime = cal.getTime();
		
		cal.set(Calendar.HOUR_OF_DAY, toHour - 1);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		cal.set(Calendar.MILLISECOND, 900);
		Date toTime = cal.getTime();
		
		//computePeriodMovement (dayOfWeekId, timeOfDayId, fromTime, toTime);
		computePeriodSummary  (dayOfWeekId, timeOfDayId, fromTime, toTime);
	}

	private void computePeriodSummary (int dayOfWeekId, int timeOfDayId, Date fromTime, Date toTime)
	{
		String msg = "computePeriodSummary: from=" + fromTime.toString() + " to=" + toTime.toString();
		if ( _StoreNum != null ) {
			msg = msg + ", StoreNum=" + _StoreNum;
		}
		logger.info(msg);
		
		_Count = 0;
		_CountFailed = 0;
		
		SummaryDailyDAO dao = new SummaryDailyDAO();
		List<SummaryDailyDTO> dtoList;
		Hashtable<String, SummaryDailyDTO> dtoHashList = new Hashtable<String, SummaryDailyDTO>();
		try
		{
			//
			// Retrieve visit summaries
			//
			logger.info("getVisitSummary Daily begin");
			dtoList = dao.getVisitSummary (_Conn, _StoreNum, fromTime, toTime, _ExcludeDeptList, _IncludeItemList,false);
			int size = dtoList.size();
			logger.info("getVisitSummary Daily end: count=" + size);
			
			if ( size > 0 )
			{
				int[] schedules = new int[size];
				int[] stores = new int[size];
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryDailyDTO dto = dtoList.get(ii);
					
					ArrayList<String> storeIdList = _CompDataDAO.getStoreIdList(_Conn, dto.getCompStoreNum());
					if ( storeIdList.size() > 0 )
					{
						int storeId = Integer.parseInt(storeIdList.get(0));
						_CompDataDTO.compStrId = storeId;
						int schId = _CompDataDAO.getScheduleID(_Conn, _CompDataDTO);
						_CompDataDTO.scheduleId = schId;
						dto.setCompStoreId(storeId);
						dto.setScheduleId(schId);
						schedules[ii] = schId;
						stores[ii] = storeId;
					}
					else {
						// error
					}
					
					dtoHashList.put(Integer.toString(dto.getCompStoreId()), dto);
				}
				logger.info("hash List count=" + dtoHashList.size());
					
				// Delete summary record

				computeStoreSummary (dtoHashList, dayOfWeekId, timeOfDayId, fromTime, toTime);
				
/*
				// Retrieve store revenue and cost summaries
				logger.info("getStoreRevenueCostSummary Daily begin");
				dtoList = dao.getStoreRevenueCostSummary (_Conn, schedules, stores);
				logger.info("getStoreRevenueCostSummary Daily end: count=" + dtoList.size());
				
				// Insert store into SUMMARY_DAILY table
				logger.info("insertStoreSummary Daily begin");
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryDailyDTO dtoRevenueCost = dtoList.get(ii);
					int storeId = dtoRevenueCost.getCompStoreId();
					SummaryDailyDTO dto = dtoHashList.get(Integer.toString(storeId));
					dtoRevenueCost.setScheduleId(dto.getScheduleId());
					dtoRevenueCost.setVisitCount(dto.getVisitCount());
					dtoRevenueCost.setVisitCostAverage(dto.getVisitCostAverage());
					
					setRevenueAndMargin (dtoRevenueCost);
					
					try {
						dtoRevenueCost.setDayOfWeekId(dayOfWeekId);
						dtoRevenueCost.setTimeOfDayId(timeOfDayId);
						dao.insertStoreSummaryDaily (_Conn, dtoRevenueCost); 
						_Count++;
					}
					catch (GeneralException gex) {
						_CountFailed++;
			    		//logger.error("Error", gex);
					}
				}
				logger.info("Inserted " + String.valueOf(_Count) + " store records");
				logger.info("insertStoreSummary Daily end");
*/
				
				//
				// Retrieve department revenue and cost summaries
				//
				logger.info("getDepartmentRevenueCostSummary Daily begin");
				dtoList = dao.getDepartmentRevenueCostSummary (_Conn, schedules, stores);
				logger.info("getDepartmentRevenueCostSummary Daily end: count=" + dtoList.size());
				
				// Insert department into SUMMARY_WEEKLY table
				logger.info("insertDepartmentSummaryDaily begin");
				_Count = 0;
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryDailyDTO dtoRevenueCost = dtoList.get(ii);
					setupDTO(dtoRevenueCost, dayOfWeekId, timeOfDayId);
					try {
						dao.insertDepartmentSummaryDaily (_Conn, dtoRevenueCost);
						_Count++;
					}
					catch (GeneralException gex) {
						_CountFailed++;
			    		//logger.error("Error", gex);
					}
				}
				logger.info("Inserted " + String.valueOf(_Count) + " department records");
				logger.info("insertDepartmentSummaryDaily end");
				
				//
				// Retrieve category revenue and cost summaries
				//
				logger.info("getCategoryRevenueCostSummary Daily begin");
				dtoList = dao.getCategoryRevenueCostSummary (_Conn, schedules, stores);
				logger.info("getCategoryRevenueCostSummary Daily end: count=" + dtoList.size());
				
				// Insert category into SUMMARY_WEEKLY table
				logger.info("insertCategorySummaryDaily begin");
				_Count = 0;
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryDailyDTO dtoRevenueCost = dtoList.get(ii);
					setupDTO(dtoRevenueCost, dayOfWeekId, timeOfDayId);
					try {
						dao.insertCategorySummaryDaily (_Conn, dtoRevenueCost);
						_Count++;
					}
					catch (GeneralException gex) {
						_CountFailed++;
			    		//logger.error("Error", gex);
					}
				}
				logger.info("Inserted " + String.valueOf(_Count) + " category records");
				logger.info("insertCategorySummaryDaily end");
				
/*
				//
				// Retrieve sub-category revenue and cost summaries
				//
				logger.info("getSubCategoryRevenueCostSummary Daily begin");
				dtoList = dao.getSubCategoryRevenueCostSummary (_Conn, schedules, stores);
				logger.info("getSubCategoryRevenueCostSummary Daily end: count=" + dtoList.size());
				
				// Insert sub-category into SUMMARY_WEEKLY table
				logger.info("insertSubCategorySummaryDaily begin");
				_Count = 0;
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryDailyDTO dtoRevenueCost = dtoList.get(ii);
					setupDTO(dtoRevenueCost, dayOfWeekId, timeOfDayId);
					try {
						dao.insertSubCategorySummaryDaily (_Conn, dtoRevenueCost);
						_Count++;
					}
					catch (GeneralException gex) {
						_CountFailed++;
			    		//logger.error("Error", gex);
					}
				}
				logger.info("Inserted " + String.valueOf(_Count) + " sub-category records");
				logger.info("insertSubCategorySummaryDaily end");
				
				//
				// Retrieve segment revenue and cost summaries
				//
				logger.info("getSegmentRevenueCostSummary Daily begin");
				dtoList = dao.getSegmentRevenueCostSummary (_Conn, schedules, stores);
				logger.info("getSegmentRevenueCostSummary Daily end: count=" + dtoList.size());
				
				// Insert segment into SUMMARY_WEEKLY table
				logger.info("insertSegmentSummaryDaily begin");
				_Count = 0;
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryDailyDTO dtoRevenueCost = dtoList.get(ii);
					setupDTO(dtoRevenueCost, dayOfWeekId, timeOfDayId);
					try {
						dao.insertSegmentSummaryDaily (_Conn, dtoRevenueCost);
						_Count++;
					}
					catch (GeneralException gex) {
						_CountFailed++;
			    		//logger.error("Error", gex);
					}
				}
				logger.info("Inserted " + String.valueOf(_Count) + " segment records");
				logger.info("insertSegmentSummaryDaily end");
*/
			}
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		catch (Exception ex) {
    		logger.error("Error", ex);
		}
		
		// Delete temp data
		//deleteTempData();

		logger.info("Inserted " + String.valueOf(_Count) + " total records");
		if ( _CountFailed > 0 )
			logger.info("Failed to insert " + String.valueOf(_CountFailed) + " records");
	}

	private void setupDTO (SummaryDailyDTO dto, int dayOfWeekId, int timeOfDayId)
	{
		_CompDataDTO.compStrId = dto.getCompStoreId();
		try {
			int schId = _CompDataDAO.getScheduleID(_Conn, _CompDataDTO);
			dto.setScheduleId(schId);
		}
		catch (GeneralException e) {
			logger.error("setupDTO: Error getting schedule id");
		}
		setRevenueAndMargin (dto);
		dto.setDayOfWeekId(dayOfWeekId);
		dto.setTimeOfDayId(timeOfDayId);
	}

		/*
	private void deleteTempData ()
	{
		// Delete temp data
		SummaryDailyDAO dao = new SummaryDailyDAO();
		try {
			if ( _DeleteTemp ) {
				logger.info("Deleting temporary movement data");
				dao.deletePeriodMovement(_Conn);
			}
		}
		catch (GeneralException ex) {
    		logger.error("Error", ex);
		}
		catch (Exception ex) {
    		logger.error("Error", ex);
		}
	}
*/
	
	private void setRevenueAndMargin (SummaryDailyDTO dto)
	{
		double regularRevenue = dto.getRegularRevenue();
		double saleRevenue = dto.getSaleRevenue();
		double finalCost = dto.getFinalCost();
		dto.setMargin (regularRevenue + saleRevenue - finalCost);
	}
	
	//
	// Compute store daily summary
	//
	private void computeStoreSummary (Hashtable<String, SummaryDailyDTO> dtoVisitList,
			int dayOfWeekId, int timeOfDayId, Date fromTime, Date toTime)
	{
		Enumeration<SummaryDailyDTO> dtoList = dtoVisitList.elements();
		while ( dtoList.hasMoreElements() ) {
			computeStoreSummary (dtoList.nextElement(), dayOfWeekId, timeOfDayId, fromTime, toTime); 
		}
	}
	
	private void computeStoreSummary (SummaryDailyDTO summaryDTO, int dayOfWeekId, int timeOfDayId, Date fromTime, Date toTime)
	{
		String storeNum = summaryDTO.getCompStoreNum(); 
		String msg = "computeStoreSummary: store=" + storeNum + " from=" + fromTime.toString() + " to=" + toTime.toString();
		logger.info(msg);
		
		SummaryDailyDAO dao = new SummaryDailyDAO();
		List<MovementDailyAggregateDTO> list = new ArrayList<MovementDailyAggregateDTO>();
		
		_HashList = new Hashtable<String, MovementDailyAggregateDTO>();
		_NoCheckIdHashList = new Hashtable<String, MovementDailyAggregateDTO>();
		_NoItemHashList = new Hashtable<String, MovementDailyAggregateDTO>();

		_Count = 0;
		_CountFailed = 0;

		logger.info("getQuantityMovementsForPeriod sale false begin");
		try {
			list = dao.getQuantityMovementsForPeriod (_Conn, storeNum, fromTime, toTime, false);
			insertPeriodMovements (dao, list, dayOfWeekId, timeOfDayId, false, true);
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		logger.info("getQuantityMovementsForPeriod sale false end: count=" + list.size());
		
		logger.info("getQuantityMovementsForPeriod sale true begin");
		try {
			list = dao.getQuantityMovementsForPeriod (_Conn, storeNum, fromTime, toTime, true);
			insertPeriodMovements (dao, list, dayOfWeekId, timeOfDayId, true, true);
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		logger.info("getQuantityMovementsForPeriod sale true end: count=" + list.size());
		
		logger.info("getWeightMovementsForPeriod sale false begin");
		try {
			list = dao.getWeightMovementsForPeriod (_Conn, storeNum, fromTime, toTime, false);
			insertPeriodMovements (dao, list, dayOfWeekId, timeOfDayId, false, true);
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		logger.info("getWeightMovementsForPeriod sale true end: count=" + list.size());
		
		logger.info("getWeightMovementsForPeriod sale true begin");
		try {
			list = dao.getWeightMovementsForPeriod (_Conn, storeNum, fromTime, toTime, true);
			insertPeriodMovements (dao, list, dayOfWeekId, timeOfDayId, true, true);
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		logger.info("getWeightMovementsForPeriod sale true end: count=" + list.size());

		try
		{
			addListData(summaryDTO, _HashList); 
			addListData(summaryDTO, _NoCheckIdHashList); 
			addListData(summaryDTO, _NoItemHashList); 
			
			summaryDTO.setDayOfWeekId(dayOfWeekId);
			summaryDTO.setTimeOfDayId(timeOfDayId);

			setRevenueAndMargin (summaryDTO);

			summaryDTO.setVisitCostAverage(summaryDTO.getTotalRevenue() / summaryDTO.getVisitCount());
			
			dao.insertStoreSummaryDaily (_Conn, summaryDTO); 
			_Count++;
		}
		catch (GeneralException gex) {
			_CountFailed++;
    		//logger.error("Error", gex);
		}
		
		logger.info("Inserted " + String.valueOf(_Count) + " records");
		if ( _CountFailed > 0 )
			logger.info("Failed to insert " + String.valueOf(_CountFailed) + " records");

		if ( _SkippedList.size() > 0 )
		{
			logger.info("Skipped " + String.valueOf(_SkippedList.size()) + " records");
			/*
			StringBuffer sb = new StringBuffer();
			for ( int ii = 0; ii < _SkippedList.size(); ii++ ) {
				if ( ii > 0 ) sb.append(',');
				sb.append(_SkippedList.get(ii).getItemUPC());
			}
			logger.info(sb.toString());
			*/
		}
	}
	
	private void addListData (SummaryDailyDTO summaryDTO, Hashtable<String, MovementDailyAggregateDTO> list)
	throws GeneralException
	{
		logger.info("addListData begin: count=" + list.size());
		
		Object[] dtoArray = list.values().toArray();
		for ( int ii = 0; ii < dtoArray.length; ii++ )
		{
			MovementWeeklyDTO dto = (MovementWeeklyDTO)dtoArray[ii];
			
			// Is dept in the exclude list?
			int deptId = dto.getDeptId();
			if ( deptId != -1 && canAddDepartment(deptId) )
			{
				//summaryDTO.setDeptId(deptId);
				summaryDTO.setRegularRevenue(summaryDTO.getRegularRevenue() + dto.getRevenueRegular());
				summaryDTO.setRegularQuantity(summaryDTO.getRegularQuantity() + dto.getQuantityRegular());
				summaryDTO.setSaleRevenue(summaryDTO.getSaleRevenue() + dto.getRevenueSale());
				summaryDTO.setSaleQuantity(summaryDTO.getSaleQuantity() + dto.getQuantitySale());
				summaryDTO.setFinalCost(summaryDTO.getFinalCost() + dto.getFinalCost());
			}
			else
			{
				if ( canAddUPC(deptId, dto.getItemUPC()) )
				{
					logger.debug("Adding: upc=" + dto.getItemUPC());
					summaryDTO.setRegularRevenue(summaryDTO.getRegularRevenue() + dto.getRevenueRegular());
					summaryDTO.setRegularQuantity(summaryDTO.getRegularQuantity() + dto.getQuantityRegular());
				}
			}
		}
		
		logger.debug("addListData end: reg rev=" + summaryDTO.getRegularRevenue() + ", reg qty=" + summaryDTO.getRegularQuantity()
					 + ", sale rev=" + summaryDTO.getSaleRevenue() + ", sale qty=" + summaryDTO.getSaleQuantity()
					 + ", cost=" + summaryDTO.getFinalCost());
	}
	
	//
	// Customer specific code
	//
	protected boolean canAddDepartment (int deptId)
	{
		boolean found = false;
		for ( int ii = 0; ii < _ExcludeDeptList.size(); ii++ )
		{
			found = (deptId == _ExcludeDeptList.get(ii));
			if ( found ) break;
		}
		return !found;
	}
	
	protected boolean canAddUPC (int deptId, String itemUPC)
	{
		boolean found = false;
		//if ( deptId == 34 )
		{
			for ( int ii = 0; ii < _IncludeItemList.size(); ii++ )
			{
				String upc = _IncludeItemList.get(ii);
				found = (itemUPC.compareTo(upc) == 0);
				if ( found ) break;
			}
		}
		return found;
	}
	
	private void insertPeriodMovements (SummaryDailyDAO dao, List<MovementDailyAggregateDTO> list,
						int dayOfWeekId, int timeOfDayId, boolean saleFlag, boolean update)
	{
		logger.info("insertPeriodMovements: " + String.valueOf(list.size()) + " to insert");
		
		MovementDailyAggregateDTO dto = null;
		for ( int ii = 0; ii < list.size(); ii++ )
		{
			_Count++;
			dto = list.get(ii);
			dto.setDayOfWeekId(dayOfWeekId);
			dto.setTimeOfDayId(timeOfDayId);
			if ( saleFlag ) {
				dto.setRevenueSale(dto.getTotalPrice());
				dto.setQuantitySale(dto.getExtnQty());
			}
			else {
				dto.setRevenueRegular(dto.getTotalPrice());
				dto.setQuantityRegular(dto.getExtnQty());
			}
			
			try
			{
				ArrayList<String> storeIdList = _CompDataDAO.getStoreIdList(_Conn, dto.getItemStore());
				if ( storeIdList.size() > 0 ) {
					_CompDataDTO.compStrId = Integer.parseInt(storeIdList.get(0));
					_CompDataDTO.scheduleId = _CompDataDAO.getScheduleID(_Conn, _CompDataDTO);
				}
				else {
					// error
				}
				
				_CompDataDTO.upc = dto.getItemUPC();
				dto.setCompStoreId(_CompDataDTO.compStrId);
				
				if ( _CompDataDAO.getItemCode(_Conn, _CompDataDTO) != -1 )
				{
					dto.setItemCode(_CompDataDTO.itemcode);
					dto.setDeptId(_CompDataDTO.deptId);

					if ( _CompDataDTO.checkItemId != -1 )
					{
						dto.setCheckDataId(_CompDataDTO.checkItemId);
						// Add to hash
						_HashList.put (dto.getItemUPC(), dto);

						// Retrieve cost info only if full day
						if ( timeOfDayId == 4 )		// 4 => full day
						{
							dao.getCostInfo(_Conn, dto);
							
							double finalCost = (dto.getQuantityRegular() + dto.getQuantitySale()) * dto.getDealCost();
							dto.setFinalCost(finalCost);
							double regularRevenue = dto.getRevenueRegular();
							double saleRevenue = dto.getRevenueSale();
							dto.setMargin(regularRevenue + saleRevenue - finalCost);
						}
					}
					else {
						// Add to no check id hash
						_NoCheckIdHashList.put (dto.getItemUPC(), dto);
						_SkippedList.add(dto);		// skipped from movement weekly processing
					}
				}
				else {
					// Add to no item hash
					_NoItemHashList.put (dto.getItemUPC(), dto);
					_NoItemList.add(dto);
				}
			}
			catch (GeneralException gex)
			{
        		String msg = String.format ("Error line %d: store=%s, upc=%s price=%s",
						_Count, dto.getItemStore(), dto.getItemUPC(), dto.getItemDateTime());
        		logger.error(msg, gex);
				_CountFailed++;
			}
			catch (Exception ex) {
			}
			
	        if ( (_Count % 10000) == 0 ) {
				logger.info("Inserted " + String.valueOf(_Count) + " records");
	        }
            if ( _StopAfter > 0 && ii >= _StopAfter ) {
            	break;
            }
		}
	}
}
