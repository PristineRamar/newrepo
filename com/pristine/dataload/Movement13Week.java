package com.pristine.dataload;

import java.sql.Connection;
import java.text.DateFormat;
import java.util.*;
import java.text.SimpleDateFormat;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dao.MovementDAO;

public class Movement13Week
{
	// Movement13Week [<start date>] [<end date>] [STORE=<store no>]
	// 		- start date is Sunday (Sunday of previous week)
	// 		- end date is Saturday (Saturday of week if ignored)
	// Movement13Week 2/27/2011
	// Movement13Week 2/27/2011 3/5/2011 (same as above)
	
	static Logger 		logger = Logger.getLogger("Movement13Week");
	private Date  		_WeekStartDate;
	private Date  		_WeekEndDate;
    private Connection	_Conn = null;
	private int			_Count = 0;
	private int			_CountFailed = 0;
	private int			_StopAfter = -1;
	private String		_StoreNum		= null;
	private String[] 	_StoreList = null;

	private CompetitiveDataDAO _CompDataDAO;
	private CompetitiveDataDTO _CompDataDTO;
	
	public Movement13Week (Date weekStart, Date weekEnd)
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
	        _Conn = DBManager.getConnection();
	        _Conn.setAutoCommit(true);
	        
	        _CompDataDAO = new CompetitiveDataDAO(_Conn);
	        String checkUser = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_USER_ID", Constants.EXTERNAL_PRICECHECK_USER);
	        String checkList = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_LIST_ID", Constants.EXTERNAL_PRICECHECK_LIST);
	        String storeList = PropertyManager.getProperty("DATALOAD.STORE_LIST", null);
	        if ( storeList != null && storeList.length() > 0 ) {
	        	_StoreList = storeList.split(",");
	        }
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
	
	public static void main(String[] args)
	{
		PropertyConfigurator.configure("log4j-movement-13week.properties");
		logCommand (args);
		
		String storeNum = null;
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
            		if ( arg.startsWith(ARG_STORE) ) {
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
        	
        	Movement13Week weeklyMovement = new Movement13Week(startDate, endDate);
        	weeklyMovement._StoreNum = storeNum;
        	
        	weeklyMovement.setupDates();
        	
        	String msg = "13 Weekly Movement: from " + startDate.toString() + " to " + endDate.toString(); 
    		logger.info(msg);
        	weeklyMovement.compute13WeekMovement();
			logger.info("13 Weekly Movement successfully completed");
			
    		// Close the connection
    		PristineDBUtil.close(weeklyMovement._Conn);
		}
        catch (Exception ex) {
            logger.error("Error", ex);
        }
    }
	
	private static void logCommand (String[] args)
	{
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: Movement13Week ");
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
		
	private void compute13WeekMovement ()
	{
		MovementDAO dao = new MovementDAO();
		try
		{
			ArrayList<String> stores = new ArrayList<String>();
			if ( _StoreNum != null ) {
				stores.add (_StoreNum);
			}
			else if ( _StoreList != null )
			{
				// Use stores from store list 
				for ( int ii = 0; ii < _StoreList.length; ii++ ) {
					stores.add (_StoreList[ii]);
				}
			}
			else {
				// Retrieve TOPS stores that had movement
				 stores = dao.getTopsMovementStores (_Conn, _WeekStartDate, _WeekEndDate);
			}
				
			for ( int ii = 0; ii < stores.size(); ii++ )
			{
				String storeNum = stores.get(ii);
				ArrayList<String> storeIdList = _CompDataDAO.getStoreIdList(_Conn, storeNum);
				int storeId = Integer.parseInt(storeIdList.get(0));
				compute13WeekMovement (dao, storeId);
				compute13WeekLIGMovement (dao, storeId);
			}
		}
		catch (GeneralException ex) {
			logger.error("compute13WeeklyMovement error: ", ex);
		}
		catch (SQLException sex) {
			logger.error("compute13WeeklyMovement error: ", sex);
		}
	}
	
	private void compute13WeekMovement (MovementDAO dao, int storeId)
	{
		logger.info("compute13WeeklyMovement: storeId=" + storeId);
		
		_Count = 0;
		_CountFailed = 0;
		
		List<MovementWeeklyDTO> list;
		try
		{
			logger.info("getWeeklyMovementsFor13Week begin");
			list = dao.getWeeklyMovementsFor13Week(_Conn, storeId, _WeekStartDate, _WeekEndDate);
			logger.info("getWeeklyMovementsFor13Week end: count=" + list.size());
			
			// Retrieve schedule id's to process for 13 week data
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy");
			Date firstStartDate = get13WeekFirstStartDate(_WeekStartDate);
			String startStr = formatter.format(firstStartDate);
			Date lastWeekStartDate = getPreviousWeekStartDate(_WeekStartDate);
			String endStr = formatter.format(lastWeekStartDate);

			ScheduleDAO schDao = new ScheduleDAO();
			ArrayList <ScheduleInfoDTO> schIdList = schDao.getSchedulesForStore(_Conn, storeId, -1, startStr, endStr);
			String schCSV = schIdList.size() > 0 ? ScheduleDAO.getScheduleIdCSV (schIdList) :  null;
			logger.info("Schedule IDs=" + schCSV);
			
			for ( int ii = 0; ii < list.size(); ii++ )
			{
				MovementWeeklyDTO dto = list.get(ii);
				
				// Retrieve 13 week counts
				dao.get13WeekData(_Conn, _WeekStartDate, dto);
				//dao.get13WeekData(_Conn, dto, schCSV);
				
				boolean result = dao.updateWeeklyMovement13WeekData (_Conn, dto);
				if ( result ) {
					_Count++;
				}
				else {
					_CountFailed++;
				}
				
		        if ( (_Count % 10000) == 0 ) {
					logger.info("Updated " + String.valueOf(_Count) + " records");
		        }
	            if ( _StopAfter > 0 && ii >= _StopAfter ) {
	            	break;
	            }
			}
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		logger.info("Updated " + String.valueOf(_Count) + " records");
		if ( _CountFailed > 0 )
			logger.info("Failed to update " + String.valueOf(_CountFailed) + " records");
		logger.info("compute13WeeklyMovement end");
	}
	
	private void compute13WeekLIGMovement (MovementDAO dao, int storeId)
	{
		logger.info("compute13WeekLIGMovement: store=" + Integer.toString(storeId));
		
		_Count = 0;
		_CountFailed = 0;
		//_SkippedList = new ArrayList<MovementWeeklyDTO>();
		
		List<MovementWeeklyDTO> list;
		try
		{
			logger.info("getWeeklyMovementsFor13Week begin");
			list = dao.getWeeklyLIGMovementsFor13Week(_Conn, storeId, _WeekStartDate, _WeekEndDate);
			logger.info("getWeeklyMovementsFor13Week end: count=" + list.size());
			
			// Retrieve schedule id's to process for 13 week data
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy");
			Date firstStartDate = get13WeekFirstStartDate(_WeekStartDate);
			String startStr = formatter.format(firstStartDate);
			Date lastWeekStartDate = getPreviousWeekStartDate(_WeekStartDate);
			String endStr = formatter.format(lastWeekStartDate);

/*
			ScheduleDAO schDao = new ScheduleDAO();
			ArrayList <ScheduleInfoDTO> schIdList = schDao.getSchedulesForStore(_Conn, storeId, -1, startStr, endStr);
			String schCSV = schIdList.size() > 0 ? ScheduleDAO.getScheduleIdCSV (schIdList) :  null;
			logger.info("Schedule IDs=" + schCSV);
*/
			
			for ( int ii = 0; ii < list.size(); ii++ )
			{
				MovementWeeklyDTO dto = list.get(ii);
				
				// Retrieve 13 week counts
				dao.get13WeekLIGData(_Conn, _WeekStartDate, dto);
				
				boolean result = dao.updateWeeklyLIGMovement13WeekData (_Conn, dto);
				if ( result ) {
					_Count++;
				}
				else {
					_CountFailed++;
				}
				
		        if ( (_Count % 2000) == 0 ) {
					logger.info("Updated " + String.valueOf(_Count) + " records");
		        }
	            if ( _StopAfter > 0 && ii >= _StopAfter ) {
	            	break;
	            }
			}
		}
		catch (GeneralException gex) {
    		logger.error("Error", gex);
		}
		logger.info("Updated " + String.valueOf(_Count) + " records");
		if ( _CountFailed > 0 )
			logger.info("Failed to update " + String.valueOf(_CountFailed) + " records");
		logger.info("compute13WeekLIGMovement end");
	}
	
	private static Date get13WeekFirstStartDate (Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, - 7 * 12);		// 13 weeks
		Date firstStartDate = cal.getTime();
		return firstStartDate;
	}

	private static Date getPreviousWeekStartDate (Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, - 7);		// previous week
		Date firstStartDate = cal.getTime();
		return firstStartDate;
	}

	private static String ARG_STORE			= "STORE=";
}

/*
item codes: 41898,35963,49690,35969,35980,50446,55418,55422,42000

SELECT schedule_id, comp_chain_id, TO_CHAR(start_date, 'MM/DD/YYYY') START_DATE,  TO_CHAR(end_date, 'MM/DD/YYYY') END_DATE, 
TO_CHAR(STATUS_CHG_DATE, 'MM/DD/YYYY') STATUS_CHG_DATE,  A.comp_str_id, B.name, comp_str_no  From schedule A, competitor_store B
WHERE A.comp_str_id = 5704 and A.comp_str_id = B.comp_str_id  and start_date >= to_date('04/17/11','MM/DD/YY')
and end_date <= to_date('07/03/11','MM/DD/YY') order by start_date desc

Schedule IDs=534,512,492,468,451,426,413,385,359,343,279

select sum(m.QUANTITY_REGULAR) as QUANTITY_REGULAR, sum(m.QUANTITY_SALE) as QUANTITY_SALE from MOVEMENT_WEEKLY m, COMPETITIVE_DATA d where d.SCHEDULE_ID in (534,512,492,468,451,426,413,385,359,343,279) and m.ITEM_CODE = 55422 and m.CHECK_DATA_ID = d.CHECK_DATA_ID

update MOVEMENT_WEEKLY set QTY_REGULAR_13WK = 0.0, QTY_SALE_13WK = 0.0 where CHECK_DATA_ID = 26199593
*/
