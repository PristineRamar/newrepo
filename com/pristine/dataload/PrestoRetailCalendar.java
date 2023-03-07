/*
 * Title: TOPS RETAIL CALENDAR 
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	14/02/2012	Dinesh Kumar	Initial Version 
 *******************************************************************************
 */

package com.pristine.dataload;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

import com.pristine.dto.CalendarYearlyDTO;
import com.pristine.dao.CalendarYearlyDAO;

public class PrestoRetailCalendar {
	
	static Logger logger = Logger.getLogger("PrestoRetailCalendar");
	private Connection conn = null; // DB connection
	private static String CALENDAR_TYPE = "F";
	
	/*
	 * ****************************************************************
	 * Class constructor
	 * 
	 * @throws Exception,GeneralException
	 * ****************************************************************
	 */
	public PrestoRetailCalendar(String strStartDate, String strEndDate, String strCalYear) {
		
		try {
			
			DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
			
			Date startDate = dateFormat.parse(strStartDate);
			Date endDate = dateFormat.parse(strEndDate);			
			int startYear = 1900  + startDate.getYear();
			int endYear = 1900  + endDate.getYear();
			int calYear = Integer.parseInt(strCalYear);
			
			logger.debug("Start Date....." + startDate);
			logger.debug("Start Year....." + startYear);			
			logger.debug("End Date......." + endDate);
			logger.debug("End Year......." + endYear);
			logger.debug("Process Year..." + calYear);
			logger.debug("Date Diff......" + DateUtil.getDateDiff(endDate, startDate));

			if (startYear != calYear && endYear != calYear)
			{
				logger.error("Invalid Year");
				System.exit(1);
			}
			
			if (DateUtil.getDateDiff(endDate, startDate) < 363)
			{
				logger.error("Invalid Date range");
				System.exit(1);
			}
		} catch (ParseException e) {
			logger.error("Invalid input" + e.getMessage());
			System.exit(1);
		}

		PropertyManager.initialize("analysis.properties");

		try {
			conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			logger.info("Retail Calendar Process Ends unsucessfully");
			System.exit(1);
		}
		
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-retail-calendar.properties");
		logger.info("*** Retail Calendar process Begins ***");
		logCommand(args);
		
		if(args.length < 3)
		{
			 logger.error("Required parameters missing, cannot proceed further");
			 System.exit(1);
		}
		
		String startDate = null;
		String endDate = null;
		String calYear = null;
		
		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];

			// Get the calendar start date
			if (arg.startsWith("STARTDATE")) {
				startDate = arg.substring("STARTDATE=".length());
			}

			// Get the calendar end date
			if (arg.startsWith("ENDDATE")) {
				endDate = arg.substring("ENDDATE=".length());
			}
		
			// Get the calendar year
			if (arg.startsWith("YEAR")) {
				calYear = arg.substring("YEAR=".length());
			}
			
			if(arg.startsWith("CAL_TYPE")){
				CALENDAR_TYPE = arg.substring("CAL_TYPE=".length());
			}
		}
		
		PrestoRetailCalendar retCalObj = new PrestoRetailCalendar(startDate, endDate, calYear);
		retCalObj.processCalendarPopulation(startDate, endDate, calYear);

	}
	
	private void processCalendarPopulation(String startDate, String endDate, 
					String calYear)
	{
		try
		{
			CalendarYearlyDTO calendarDto=new CalendarYearlyDTO();
			CalendarYearlyDAO calendarDao=new CalendarYearlyDAO();
	
			calendarDto.setStartDate(startDate);
			calendarDto.setEndDate(endDate);
			calendarDto.setYear(calYear);
			
			logger.info("Processing daily calendar Data");	
			calendarDao.RowtypeD(calendarDto,conn,calendarDto.getStartDate(), CALENDAR_TYPE);
			
			logger.info("Processing weekly calendar Data");	
			
			calendarDao.RowtypeW(calendarDto,conn,calendarDto.getStartDate(), CALENDAR_TYPE);
			
			logger.info("Row Type W(Week) Insertd Sucussfully");
			
			logger.info("Processing period calendar Data");	
			setWeekCountForPeriod(calendarDto);
			calendarDao.RowTypeP(calendarDto,conn,calendarDto.getStartDate(), CALENDAR_TYPE);
			
			logger.info("Processing Quarter calendar Data");	
			setPerodCountForQuarter(calendarDto);
			setWeekCountForQuarter(calendarDto);
			calendarDao.RowTypeQ(calendarDto,conn,calendarDto.getStartDate(), CALENDAR_TYPE);
	
			logger.info("Processing Year calendar Data");			
			calendarDao.RowTypeY(calendarDto,conn, CALENDAR_TYPE);
			
			logger.debug("Commit the transaction...");
			PristineDBUtil.commitTransaction(conn, "Commit");
		
		} catch (ParseException e) {
			logger.error("Error while processing " + e.getMessage());
			PristineDBUtil.rollbackTransaction(conn, "Calendar Yearly Process Rollbacked");
		} catch (GeneralException e) {
			logger.error("Error while processing " + e.getMessage());
			PristineDBUtil.rollbackTransaction(conn, "Calendar Yearly Process Rollbacked");			
		}
	}
	
	private static void setPerodCountForQuarter(CalendarYearlyDTO calendarDto) {
		calendarDto.setFirstQuarter(PropertyManager.getProperty("CALENDAR.Q1_PERIOD_COUNT","4"));
		calendarDto.setSecondQuarter(PropertyManager.getProperty("CALENDAR.Q2_PERIOD_COUNT","3"));
		calendarDto.setThirdQuarter(PropertyManager.getProperty("CALENDAR.Q3_PERIOD_COUNT","3"));
		calendarDto.setFourthQuarter(PropertyManager.getProperty("CALENDAR.Q4_PERIOD_COUNT","3"));
	 	logger.debug("-----"+calendarDto.getFirstQuarter());
 		logger.debug("-----"+calendarDto.getSecondQuarter());
		logger.debug("-----"+calendarDto.getThirdQuarter());
		logger.debug("-----"+calendarDto.getFourthQuarter());
	}
	
	
	private static void setWeekCountForPeriod(CalendarYearlyDTO calendarDto) {
		calendarDto.setPeriod1(PropertyManager.getProperty("CALENDAR.P1_WEEK_COUNT","4"));
		calendarDto.setPeriod2(PropertyManager.getProperty("CALENDAR.P2_WEEK_COUNT","4"));
		calendarDto.setPeriod3(PropertyManager.getProperty("CALENDAR.P3_WEEK_COUNT","4"));
		calendarDto.setPeriod4(PropertyManager.getProperty("CALENDAR.P4_WEEK_COUNT","4"));
		calendarDto.setPeriod5(PropertyManager.getProperty("CALENDAR.P5_WEEK_COUNT","4"));
		calendarDto.setPeriod6(PropertyManager.getProperty("CALENDAR.P6_WEEK_COUNT","4"));
		calendarDto.setPeriod7(PropertyManager.getProperty("CALENDAR.P7_WEEK_COUNT","4"));
		calendarDto.setPeriod8(PropertyManager.getProperty("CALENDAR.P8_WEEK_COUNT","4"));
		calendarDto.setPeriod9(PropertyManager.getProperty("CALENDAR.P9_WEEK_COUNT","4"));
		calendarDto.setPeriod10(PropertyManager.getProperty("CALENDAR.P10_WEEK_COUNT","4"));
		calendarDto.setPeriod11(PropertyManager.getProperty("CALENDAR.P11_WEEK_COUNT","4"));
		calendarDto.setPeriod12(PropertyManager.getProperty("CALENDAR.P12_WEEK_COUNT","4"));
		calendarDto.setPeriod13(PropertyManager.getProperty("CALENDAR.P13_WEEK_COUNT","4"));
	}
	
	private void setWeekCountForQuarter(CalendarYearlyDTO calendarDto){
		calendarDto.setQuater1Weeks(Integer.parseInt(PropertyManager.getProperty("CALENDAR.Q1_WEEK_COUNT","16")));
		calendarDto.setQuater2Weeks(Integer.parseInt(PropertyManager.getProperty("CALENDAR.Q2_WEEK_COUNT","12")));
		calendarDto.setQuater3Weeks(Integer.parseInt(PropertyManager.getProperty("CALENDAR.Q3_WEEK_COUNT","12")));
		calendarDto.setQuater4Weeks(Integer.parseInt(PropertyManager.getProperty("CALENDAR.Q4_WEEK_COUNT","12")));
	}
	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: PrestoRetailCalendar");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

}
