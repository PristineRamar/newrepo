package com.pristine.dataload.gianteagle;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.MonthlyPriceIndexDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dataload.CompDataLIGRollup;
import com.pristine.dataload.CompDataPISetup;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class MonthlyPriceIndexLoader{
	static Logger logger = Logger.getLogger("MonthlyPriceIndexLoader");
	static Connection conn = null;
	static String dateStr = null;
	static String mdSpecificMonth = null;
	String prevWkStartDate, startDate, endDate;
	public MonthlyPriceIndexLoader() {
		
	}
	public static void main(String[] args){
		PropertyConfigurator.configure("log4j-monthly-PI.properties");
		PropertyManager.initialize("analysis.properties");
		int locationLevelId = 0,srcLocationId = 0, destLocationId = 0, srcCompStrId = 0, destCompStrID = 0;
		Calendar c = Calendar.getInstance();
		int dayIndex = 0;
		if(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null){
			dayIndex = Integer.parseInt(PropertyManager.
					getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if(dayIndex > 0)
			c.set(Calendar.DAY_OF_WEEK, dayIndex+1);
		else
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ex) {
			logger.error("Error while connecting Database"+ex);
		}
		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];
			if (arg.startsWith("PROCESS_WEEK")) {
				String processWeek = arg.substring("PROCESS_WEEK=".length());
				if (Constants.CURRENT_WEEK.equalsIgnoreCase(processWeek)) {
					dateStr = dateFormat.format(c.getTime());
				} else if (Constants.NEXT_WEEK.equalsIgnoreCase(processWeek)) {
					c.add(Calendar.DATE, 7);
					dateStr = dateFormat.format(c.getTime());
				} else if (Constants.LAST_WEEK.equalsIgnoreCase(processWeek)) {
					c.add(Calendar.DATE, -7);
					dateStr = dateFormat.format(c.getTime());
				} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(processWeek)) {
					try {
						String weekStart_date = null;
						for(int j = 0; j < args.length; j++){
							String arg1 = args[j];
							if(arg1.startsWith("WEEK_START_DATE")){
								weekStart_date = arg1.substring("WEEK_START_DATE=".length());
							}
						}
						dateStr = DateUtil.getWeekStartDate(DateUtil.toDate(weekStart_date), 0);
					} catch (GeneralException exception) {
						logger.error("Error when parsing date - " + exception.toString());
						System.exit(-1);
					}
				}
			}
			else if(arg.startsWith("SRC_COMP_ID")){
				srcCompStrId = Integer.parseInt(arg.substring("SRC_COMP_ID=".length()));
			}
			else if(arg.startsWith("DEST_COMP_ID")){
				destCompStrID = Integer.parseInt(arg.substring("DEST_COMP_ID=".length()));
			}
			else if(arg.startsWith("SRC_LOCATION_ID")){
				srcLocationId = Integer.parseInt(arg.substring("SRC_LOCATION_ID=".length()));
			}
			else if(arg.startsWith("DEST_LOCATION_ID")){
				destLocationId = Integer.parseInt(arg.substring("DEST_LOCATION_ID=".length()));
			}
			else if(arg.startsWith("LOCATION_LEVEL_ID")){
				locationLevelId = Integer.parseInt(arg.substring("LOCATION_LEVEL_ID=".length()));
			}
			//To use market data by giving specific month start Date
			else if(arg.startsWith("MD_SPECIFIC_MONTH")){
				try {
					String weekStart_date = null;
					for(int j = 0; j < args.length; j++){
						String arg1 = args[j];
						if(arg1.startsWith("MD_SPECIFIC_MONTH")){
							weekStart_date = arg1.substring("MD_SPECIFIC_MONTH=".length());
						}
					}
					mdSpecificMonth = DateUtil.dateToString(DateUtil.toDate(weekStart_date), "MM/dd/yy");
					logger.info("Given MD specific date: "+mdSpecificMonth);
				} catch (GeneralException exception) {
					logger.error("Error when parsing date - " + exception.toString());
					System.exit(-1);
				}
			}
		}
		try {
			new MonthlyPriceIndexLoader().processMonthlyPI(srcCompStrId, destCompStrID, srcLocationId, 
					destLocationId, locationLevelId);
			PristineDBUtil.commitTransaction(conn, "Monthly Price Index Loader commit");
		} catch (GeneralException e) {
			e.printStackTrace();
			PristineDBUtil.rollbackTransaction(conn, "Monthly Price Index Loader roll back");
			logger.error("Error while processing processMonthlyPI()", e);
		}finally{
			PristineDBUtil.close(conn);
	    }
	}
	
	
	private void processMonthlyPI(int srcCompStrId, int destCompStrID,int srcLocationId, int destLocationId,int locationLevelId) 
			throws GeneralException{
		MonthlyPriceIndexDAO monthlyPIDAO = new MonthlyPriceIndexDAO();
		populateCalendarId(dateStr);
		int scheduleId = getScheduleId(destCompStrID,destLocationId,locationLevelId);
		logger.debug("Schedule Id: "+scheduleId);
		if(scheduleId >0){
			monthlyPIDAO.deleteExistingCompData(conn, scheduleId);
			//TO Process base store based on Source and dest Location Id
			if(srcLocationId != 0 && destLocationId !=0 && locationLevelId !=0){
				logger.info("Processing Competitive data");
				monthlyPIDAO.populateBaseStrMonthlyCompData(conn, scheduleId,prevWkStartDate, startDate,endDate,srcLocationId,locationLevelId);
				CompDataLIGRollup compDataLIG = new CompDataLIGRollup();
				logger.info("Processing Competitive data LIG rollup");
				compDataLIG.LIGLevelRollUp(conn, scheduleId);
				logger.info("Processing Movement weekly data");
				monthlyPIDAO.populateMonthlyMovementData(conn, scheduleId,prevWkStartDate, startDate,endDate,srcLocationId,destLocationId,
						locationLevelId,"WEEKLY");
				logger.info("Processing Movement weekly data Lig");
				monthlyPIDAO.populateMonthlyMovementData(conn, scheduleId,prevWkStartDate, startDate,endDate,srcLocationId,destLocationId,
						locationLevelId,"LIG");
				logger.info("Populating market data in Movement weekly");
				//Code changes done to use MD if specific month given as input else it will consider most recent data available in MD
				if(mdSpecificMonth == null){
					mdSpecificMonth = monthlyPIDAO.getMostRecentDateFromMD(conn);
					logger.info("Market Data will be considered with Date: "+mdSpecificMonth);
				}
				monthlyPIDAO.populateMarketMovInMovementWeekly(conn, scheduleId, mdSpecificMonth);
				logger.info("Populating market data in Movement weekly Lig");
				monthlyPIDAO.populateMarketMovInMovementWeeklyLig(conn, scheduleId, mdSpecificMonth);
				logger.info("Populating market data in Movement weekly Lig is completed");
			}
			//To process comp store based on src and dest Comp store Id
			else if(srcCompStrId  !=0 && destCompStrID !=0){
				monthlyPIDAO.populateCompStrMonthlyCompData(conn, scheduleId,prevWkStartDate, startDate,endDate,srcCompStrId);
				CompDataLIGRollup compDataLIG = new CompDataLIGRollup();
				logger.info("Processing Competitive data LIG rollup");
				compDataLIG.LIGLevelRollUp(conn, scheduleId);
			}
			//Process Competitive Data PI setup to populate value in Competitive data PI table
			logger.info("Processing Comp data PI Setup");
			CompDataPISetup compDataPI = new CompDataPISetup();
			compDataPI.setupSchedule(conn, scheduleId);
		}
	}
	
	/**
	 * Sets input week's calendar id and its previous week's calendar id
	 * 
	 * @param weekStartDate
	 *            Input Date
	 * @throws GeneralException
	 */
	private void populateCalendarId(String weekStartDate) throws GeneralException {

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK);
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		startDate = calendarDTO.getStartDate();
		endDate = calendarDTO.getEndDate();
		String prevWeekStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(weekStartDate), 3);
		calendarDTO = retailCalendarDAO.getCalendarId(conn, prevWeekStartDate, Constants.CALENDAR_WEEK);
		prevWkStartDate = calendarDTO.getStartDate();
		logger.debug("Previous Week Start date - " + calendarDTO.getStartDate());
	}
	
	/**
	 * TO get Schedule Id based on either COMP STR ID or Location Id 
	 * @param destCompStrID
	 * @param destLocationId
	 * @param locationLevelId
	 * @return
	 * @throws GeneralException
	 */
	private int getScheduleId(int destCompStrID,int destLocationId,int locationLevelId) throws GeneralException{
		int scheduleId = 0;
		ScheduleDAO schDAO = new ScheduleDAO();
		if(destLocationId !=0 && locationLevelId !=0){
			scheduleId = schDAO.populateScheduleIdForLocation(conn, 
					locationLevelId, destLocationId, startDate, endDate);
		}else if(destCompStrID!=0){
			CompetitiveDataDTO compData = new CompetitiveDataDTO();
			CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(conn); 
			compData.compStrId = destCompStrID;
			compData.weekStartDate = startDate;
			compData.weekEndDate = endDate;
			compData.checkDate = startDate;
			scheduleId = compDataDAO.getScheduleID(conn, compData);
		}else{
			logger.error("Invalid comp store Id or Location Id");
		}
		return scheduleId;
	}
	
}
