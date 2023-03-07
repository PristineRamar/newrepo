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

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.SummaryWeeklyDAO;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dto.SummaryWeeklyDTO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class SummaryWeekly
{
	// SummaryWeekly [<start date> [<end date>]] [STORE=<store num>]
	// 		- start date is Sunday
	// 		- end date is Saturday (Saturday of week if ignored)
	// SummaryWeekly 2/27/2011 STORE=0227
	// SummaryWeekly 2/27/2011 3/5/2011 (same as above)
	
	static Logger 		logger = Logger.getLogger("SummaryWeekly");
	private static Hashtable<String, SummaryWeeklyDTO>	_VisitHashList = null;
	
	private Date  		_WeekStartDate;
	private Date  		_WeekEndDate;
    private Connection	_Conn = null;
	private int			_Count = 0;
	private int			_CountFailed = 0;
	private int			_StopAfter = -1;
	private int[] 		_Schedules = new int[0];
	private int[]		_Stores = new int[0];

	private List<Integer> _ExcludeDeptList = new ArrayList<Integer>();
	private List<String> _IncludeItemList = new ArrayList<String>();
	
	private CompetitiveDataDAO _CompDataDAO;
	private CompetitiveDataDTO _CompDataDTO;
	
	public void setNoStoreDelete (boolean v) { _NoStoreDelete = v; }
	private boolean _NoStoreDelete = false;
	
	public void setStoreNum (String v) { _StoreNum = v; }
	private String _StoreNum = null;
	
	private boolean _DoStoreSummary = false;

	private static String ARG_STORE_SUMMARY	= "STORE_SUMMARY";
	private static String ARG_STORE			= "STORE=";
	
	public SummaryWeekly (Date weekStart, Date weekEnd)
	{
		this (null, weekStart, weekEnd);
	}
	
	public SummaryWeekly (Connection conn, Date weekStart, Date weekEnd)
	{
		_WeekStartDate = weekStart;
		_WeekEndDate = weekEnd;
		
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
			
			if ( conn != null ) {
				_Conn = conn;
			}
			else {
		        _Conn = DBManager.getConnection();
		        _Conn.setAutoCommit(true);
			}
	        
	        
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
		PropertyConfigurator.configure("log4j-summary-weekly.properties");
		logCommand (args);
		
        try
        {
        	Date startDate = null, endDate = null;
        	String startDateStr = null, endDateStr = null;
    		DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
    		boolean storeSummary = false;
    		String storeNum = null;

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
            		if ( arg.compareToIgnoreCase(ARG_STORE_SUMMARY) == 0 ) {
            			storeSummary = true;
            		}
            		else if ( arg.startsWith(ARG_STORE) ) {
            			storeNum = arg.substring(ARG_STORE.length());
            		}
            		else {
    	        		endDateStr = arg;
    	        		endDate = df.parse(endDateStr);
            		}
        		}
    		}

        	if ( startDate == null ) {
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(new Date());
        		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        		cal.add(Calendar.DATE, -7);
        		startDate = cal.getTime();
        		startDateStr = df.format(startDate);
        	}
        	if ( endDate == null ) {
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(startDate);
        		cal.add(Calendar.DATE, 6);
        		endDate = cal.getTime();
        		endDateStr = df.format(endDate);
        	}
        	
        	String msg = "Weekly Summary: from " + startDate.toString() + " to " + endDate.toString(); 
    		logger.info(msg);
    		
    		SummaryWeekly weeklySummary = new SummaryWeekly(startDate, endDate);
    		weeklySummary._DoStoreSummary = storeSummary;
    		weeklySummary._NoStoreDelete = !storeSummary;
    		weeklySummary._StoreNum = storeNum;
    		weeklySummary.computeWeeklySummary();
        	
			logger.info("Weekly Summary Computation successfully completed");
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
		StringBuffer sb = new StringBuffer("Command: SummaryWeekly ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	}
	
	private void computeWeeklySummary ()
	{
		Calendar cal = Calendar.getInstance();
		
		cal.setTime(_WeekStartDate);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.MILLISECOND, 1 * 1000);
		Date startDate = cal.getTime();
		
		cal.setTime(_WeekEndDate);
		cal.set(Calendar.HOUR_OF_DAY, 23);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.MILLISECOND, 59 * 1000);
		Date endDate = cal.getTime();
		
		// Retrieve TOPS stores
		MovementDAO dao = new MovementDAO();
		try
		{
			ArrayList<String> stores = new ArrayList<String>();
			if ( _StoreNum != null ) {
				stores.add (_StoreNum);
			}
			else {
				 stores = dao.getTopsMovementStores (_Conn, _WeekStartDate, _WeekEndDate);
			}
				
			int size = stores.size();
			if ( size > 0 )
			{
				_Schedules = new int[size];
				_Stores = new int[size];
				for ( int ii = 0; ii < size; ii++ )
				{
					_StoreNum = stores.get(ii);
					ArrayList<String> storeIdList = _CompDataDAO.getStoreIdList(_Conn, _StoreNum);
					int storeId = Integer.parseInt(storeIdList.get(0));
					_CompDataDTO.compStrId = storeId;
					int schId = _CompDataDAO.getScheduleID(_Conn, _CompDataDTO);
					_CompDataDTO.scheduleId = schId;
					_Schedules[ii] = schId;
					_Stores[ii] = storeId;
				}

				int storeId = _Stores.length > 1 ? -1 : _Stores[0]; 
				computeWeeklySummary (storeId, startDate, endDate);
			}
		}
		catch (GeneralException ex) {
			logger.error("computeWeeklyLIGMovement error: ", ex);
		}
		catch (SQLException sex) {
			logger.error("computeWeeklyLIGMovement error: ", sex);
		}

		// Close database connection
		PristineDBUtil.close(_Conn);
	}
		
	private void computeWeeklySummary (int storeId, Date startDate, Date endDate)
	{
		_Count = 0;
		_CountFailed = 0;
		
		SummaryWeeklyDAO dao = new SummaryWeeklyDAO();
		List<SummaryWeeklyDTO> dtoList;
		try
		{
			// Delete summary record
			dao.deleteSummaryWeekly (_Conn, startDate, endDate, storeId, _NoStoreDelete, false);
			
			// Retrieve visit summaries
			//Hashtable<String, SummaryWeeklyDTO> dtoHashList = getVisitSummaryWeekly (null, startDate, endDate);
			//if ( dtoHashList.size() > 0 )
			{
				if ( _DoStoreSummary )
				{
/*
					// Retrieve store revenue and cost summaries
					logger.info("getStoreRevenueCostSummaryWeekly begin");
					dtoList = dao.getStoreRevenueCostSummaryWeekly (_Conn, _Schedules, _Stores);
					logger.info("getStoreRevenueCostSummaryWeekly end");
					
					// Insert store into SUMMARY_WEEKLY table
					logger.info("insertStoreSummaryWeekly begin");
					for ( int ii = 0; ii < dtoList.size(); ii++ )
					{
						SummaryWeeklyDTO dtoRevenueCost = dtoList.get(ii);
						int storeId = dtoRevenueCost.getCompStoreId();
						SummaryWeeklyDTO dto = dtoHashList.get(Integer.toString(storeId));
						dtoRevenueCost.setScheduleId(dto.getScheduleId());
						dtoRevenueCost.setVisitCount(dto.getVisitCount());
						dtoRevenueCost.setVisitCostAverage(dto.getVisitCostAverage());
						
						setRevenueAndMargin (dtoRevenueCost);
						
						try {
							dao.insertStoreSummaryWeekly (_Conn, dtoRevenueCost); 
							_Count++;
						}
						catch (GeneralException gex) {
							_CountFailed++;
				    		//logger.error("Error", gex);
						}
					}
					logger.info("insertStoreSummaryWeekly end");
					logger.info("Inserted " + String.valueOf(_Count) + " store records");
*/
				}
				
				//
				// Retrieve department revenue and cost summaries
				//
				logger.info("getDepartmentRevenueCostSummaryWeekly begin");
				dtoList = dao.getDepartmentRevenueCostSummaryWeekly (_Conn, _Schedules, _Stores);
				logger.info("getDepartmentRevenueCostSummaryWeekly end: size = " + dtoList.size());

				// Insert department into SUMMARY_WEEKLY table
				logger.info("insertDepartmentSummaryWeekly begin");
				_Count = 0;
				_CountFailed = 0;
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryWeeklyDTO dtoRevenueCost = dtoList.get(ii);
					setupDTO(dtoRevenueCost);
					try {
						dao.insertDepartmentSummaryWeekly (_Conn, dtoRevenueCost);
						_Count++;
					}
					catch (GeneralException gex) {
						_CountFailed++;
			    		//logger.error("Error", gex);
					}
				}
				logger.info("Inserted " + String.valueOf(_Count) + " department records");
				logger.info("insertDepartmentSummaryWeekly end");
				
				//
				// Retrieve category revenue and cost summaries
				//
				logger.info("getCategoryRevenueCostSummaryWeekly begin");
				dtoList = dao.getCategoryRevenueCostSummaryWeekly (_Conn, _Schedules, _Stores);
				logger.info("getCategoryRevenueCostSummaryWeekly end: size = " + dtoList.size());
				
				// Insert category into SUMMARY_WEEKLY table
				logger.info("insertCategorySummaryWeekly begin");
				_Count = 0;
				_CountFailed = 0;
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryWeeklyDTO dtoRevenueCost = dtoList.get(ii);
					setupDTO(dtoRevenueCost);
					try {
						dao.insertCategorySummaryWeekly (_Conn, dtoRevenueCost);
						_Count++;
					}
					catch (GeneralException gex) {
						_CountFailed++;
			    		//logger.error("Error", gex);
					}
				}
				logger.info("Inserted " + String.valueOf(_Count) + " category records");
				logger.info("insertCategorySummaryWeekly end");
				
				//
				// Retrieve sub-category revenue and cost summaries
				//
				logger.info("getSubCategoryRevenueCostSummaryWeekly begin");
				dtoList = dao.getSubCategoryRevenueCostSummaryWeekly (_Conn, _Schedules, _Stores);
				logger.info("getSubCategoryRevenueCostSummaryWeekly end: size = " + dtoList.size());
				
				// Insert sub-category into SUMMARY_WEEKLY table
				logger.info("insertSubCategorySummaryWeekly begin");
				_Count = 0;
				_CountFailed = 0;
				int badIdCont = 0;
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryWeeklyDTO dtoRevenueCost = dtoList.get(ii);
					setupDTO(dtoRevenueCost);
					try {
						if ( dtoRevenueCost.getSubCatId() == -1 ) {
							badIdCont++;
						}
						dao.insertSubCategorySummaryWeekly (_Conn, dtoRevenueCost);
						_Count++;
					}
					catch (GeneralException gex) {
						_CountFailed++;
			    		//logger.error("Error", gex);
					}
				}
				logger.info("Inserted " + String.valueOf(_Count) + " sub-category records");
				if ( badIdCont > 0 ) {
					logger.info("\t" + String.valueOf(badIdCont) + " records with -1 sub-category id");
				}
				logger.info("insertSubCategorySummaryWeekly end");
				
				//
				// Retrieve segment revenue and cost summaries
				//
				logger.info("getSegmentRevenueCostSummaryWeekly begin");
				dtoList = dao.getSegmentRevenueCostSummaryWeekly (_Conn, _Schedules, _Stores);
				logger.info("getSegmentRevenueCostSummaryWeekly end: size = " + dtoList.size());
				
				// Insert segment into SUMMARY_WEEKLY table
				logger.info("insertSegmentSummaryWeekly begin");
				_Count = 0;
				_CountFailed = 0;
				badIdCont = 0;
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryWeeklyDTO dtoRevenueCost = dtoList.get(ii);
					setupDTO(dtoRevenueCost);
					try {
						if ( dtoRevenueCost.getSegmentId() == -1 ) {
							badIdCont++;
						}
						dao.insertSegmentSummaryWeekly (_Conn, dtoRevenueCost);
						_Count++;
					}
					catch (GeneralException gex) {
						_CountFailed++;
			    		//logger.error("Error", gex);
					}
				}
				logger.info("Inserted " + String.valueOf(_Count) + " segment records");
				if ( badIdCont > 0 ) {
					logger.info("\t" + String.valueOf(badIdCont) + " records with -1 segment id");
				}
				logger.info("insertSegmentSummaryWeekly end");
			}
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		catch (Exception ex) {
    		logger.error("Error", ex);
		}

		logger.info("Inserted " + String.valueOf(_Count) + " total records");
		if ( _CountFailed > 0 )
			logger.info("Failed to insert " + String.valueOf(_CountFailed) + " records");
	}
	
	public void computeStoreSummaryWeekly (int storeId, List<Hashtable<String, MovementWeeklyDTO>> list, boolean ignoreTransactionNumber)
	throws GeneralException
	{
		
		
		logger.info("computeStoreSummaryWeekly begin: store id=" + storeId);
		
		SummaryWeeklyDAO dao = new SummaryWeeklyDAO();
		
		// Delete summary record
		dao.deleteSummaryWeekly (_Conn, _WeekStartDate, _WeekEndDate, storeId, false, true);
		
		SummaryWeeklyDTO summaryDTO = new SummaryWeeklyDTO();
		int scheduleId = dao.getScheduleId(_Conn, storeId, _WeekStartDate, _WeekEndDate);
		summaryDTO.setScheduleId(scheduleId);
		summaryDTO.setCompStoreId(storeId);
		for ( int ii = 0; ii < list.size(); ii++ ) {
			addListData(summaryDTO, list.get(ii));
		}
		
		// Set visit counts
		//Added one more parameter on 18th October 2012 to fetch records irrespective of the transaction no count
		Hashtable<String, SummaryWeeklyDTO> visitHash = getVisitSummaryWeekly(_StoreNum, _WeekStartDate, _WeekEndDate,ignoreTransactionNumber);
		SummaryWeeklyDTO visitDTO = visitHash.get(Integer.toString(storeId));
		summaryDTO.setScheduleId(visitDTO.getScheduleId());
		summaryDTO.setVisitCount(visitDTO.getVisitCount());
		
		try
		{
			// Invoke client specific customizer
			summaryDTO.setCompStoreNum(visitDTO.getCompStoreNum());
			customizeStoreRevenue (summaryDTO);
			setRevenueAndMargin (summaryDTO);
			//Added on 18th Oct 2012, to hanle 0 visit counts for ahold
			if(summaryDTO.getVisitCount()!= 0)
				summaryDTO.setVisitCostAverage(summaryDTO.getTotalRevenue() / summaryDTO.getVisitCount());
			else
				summaryDTO.setVisitCostAverage(0);
			logger.debug("Tot_Rev=" + summaryDTO.getTotalRevenue() + ", visits=" + summaryDTO.getVisitCount()
						 + ", avg=" + summaryDTO.getVisitCostAverage());
			
			dao.insertStoreSummaryWeekly (_Conn, summaryDTO); 
		}
		//catch (GeneralException gex) {
		catch (Exception gex) {
			_CountFailed++;
    		//logger.error("Error", gex);
		}
		
		logger.info("doStoreSumarryWeekly end: store id=" + storeId);
	}
	
	private void addListData (SummaryWeeklyDTO summaryDTO, Hashtable<String, MovementWeeklyDTO> list)
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
	
	private Hashtable<String, SummaryWeeklyDTO> getVisitSummaryWeekly (String storeNum, Date startDate, Date endDate,boolean ignoreTransactionNumber)
	{
		SummaryWeeklyDAO dao = new SummaryWeeklyDAO();
		List<SummaryWeeklyDTO> dtoList;
		_VisitHashList = new Hashtable<String, SummaryWeeklyDTO>();
		try
		{
			//
			// Retrieve visit summaries
			//
			logger.info("getVisitSummaryWeekly begin");
			//Added one more parameter on 18th October 2012 to fetch records irrespective of the transaction no count
			dtoList = dao.getVisitSummaryWeekly (_Conn, storeNum, startDate, endDate, ignoreTransactionNumber);
			int size = dtoList.size();
			if ( size > 0 )
			{
				_Schedules = new int[size];
				_Stores = new int[size];
				for ( int ii = 0; ii < dtoList.size(); ii++ )
				{
					SummaryWeeklyDTO dto = dtoList.get(ii);
					// Janani - 6/6/12 - Set subscriber to include chain_id condition in getStoreIdList()
					_CompDataDAO.setSubscriber(_Conn, true);
					
					ArrayList<String> storeIdList = _CompDataDAO.getStoreIdList(_Conn, dto.getCompStoreNum());
					if ( storeIdList.size() > 0 )
					{
						int storeId = Integer.parseInt(storeIdList.get(0));
						_CompDataDTO.compStrId = storeId;
						int schId = _CompDataDAO.getScheduleID(_Conn, _CompDataDTO);
						_CompDataDTO.scheduleId = schId;
						dto.setCompStoreId(storeId);
						dto.setScheduleId(schId);
						_Schedules[ii] = schId;
						_Stores[ii] = storeId;
					}
					else {
						// error
					}
					
					_VisitHashList.put(Integer.toString(dto.getCompStoreId()), dto);
				}
			}
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		catch (Exception ex) {
    		logger.error("Error", ex);
		}
		
		logger.info("getVisitSummaryWeekly end: count=" + _VisitHashList.size());
		return _VisitHashList;
	}

	private void setupDTO (SummaryWeeklyDTO dto)
	throws GeneralException
	{
		_CompDataDTO.compStrId = dto.getCompStoreId();
		int schId = _CompDataDAO.getScheduleID(_Conn, _CompDataDTO);
		dto.setScheduleId(schId);
		setRevenueAndMargin (dto);
	}
	
	private void setRevenueAndMargin(SummaryWeeklyDTO dto)
	{
		double regularRevenue = dto.getRegularRevenue();
		double saleRevenue = dto.getSaleRevenue();
		double finalCost = dto.getFinalCost();
		dto.setMargin(regularRevenue + saleRevenue - finalCost);
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
	
	protected void customizeStoreRevenue (SummaryWeeklyDTO storeDTO)
	{
	}
}
