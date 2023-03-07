/*
 * Title: TOPS RETAIL CALENDAR 
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	14/02/2012	Dinesh Kumar	Initial Version 
 *******************************************************************************
 */

package com.pristine.dataload.salesanalysis;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.CalendarYearlyDAO;
import com.pristine.dao.DBManager;
import com.pristine.dto.CalendarYearlyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;


/**
 * @Date of creation 14-02-2012
 *
 */
public class CalendarYearly {
	
	
	private static Logger logger = Logger.getLogger("CalendarYearly");
	private static String CALENDAR_TYPE = "F";

	
	/*
	 *****************************************************************
	 * Main method of Batch
	 * Argument 1: STARTDATE
	 * Argument 2: ENDDATE
	 * ****************************************************************
	 */	
	
	public static void main(String[] args) throws GeneralException, ParseException
	{
		logger.info(" Retail Calendar Batch Process Starts ");
		
		logger.debug( " Aruguments Length "+ args.length );
		
		// argument length not equals 2 means process will exit
		
		if(args.length!=2)
		{
			 logger.debug("StartDate and EndDate Needed to Process the Request");
			 System.exit(1);
		}
		// start date invalid means process will exit 
		
		else if(DateUtil.DataValidation(args[0]).equalsIgnoreCase("Invalid"))
		{
			logger.debug("The given Start Date Not Valid");
			System.exit(1);
		}
		
		// end date invalid means process will exit
		
		else if(DateUtil.DataValidation(args[1]).equalsIgnoreCase("Invalid"))
		{
			logger.debug("The given End Date not valid");
			System.exit(1);
		}
		
		// Start date greater than end date  means process will exit
		
		else if(DateUtil.DateDifference(args[0],args[1]).equalsIgnoreCase("Invalid")){
			logger.debug("The given start date and end date Invalid/mismatched ");
			System.exit(1);
		}
		
		// date difference less than or greater than 364 means process will exit
		
		else if(DateUtil.YearDifference(args[0],args[1]).equalsIgnoreCase("Invalid"))
		{
			logger.debug("Dates Invalid/Total No of Days less/greater than 364");
			System.exit(1);
		}
		
		for(String arg: args){
			if(arg.startsWith("CAL_TYPE")){
				CALENDAR_TYPE = arg.substring("CAL_TYPE=".length());
			}
		}
		
		Connection conn = null;
		try
		{
		logger.debug(" Enter Into try class  ");
		
		// Intialize the calendar log4j
		
		PropertyConfigurator.configure("log4j-calendar-yearly.properties");
		
		// Intialize the analysis properties
		
		PropertyManager.initialize("analysis.properties");
		
		// get the db connection
		
		conn = DBManager.getConnection();
		
		CalendarYearlyDTO calendarDto=new CalendarYearlyDTO();
		CalendarYearlyDAO calendarDao=new CalendarYearlyDAO();
		calendarDto.setStartDate(args[0]);
		calendarDto.setEndDate(args[1]);
		
		logger.info(" Start Date " + args[0] + " End Date " + args[1] );
		
		
		
		calendarDto.setYear(DateUtil.FindYear(args[0],args[1]));
		
		logger.info("Given Year is"+calendarDto.getYear());
		
		// Call the date insert method and insert the all dates
		
		calendarDao.RowtypeD(calendarDto,conn,calendarDto.getStartDate(), CALENDAR_TYPE);
		
		logger.info("Row Type D(Date) Insertd Sucussfully");
		
		// call the week insert method and insert the all weeks
		
		calendarDao.RowtypeW(calendarDto,conn,calendarDto.getStartDate(), CALENDAR_TYPE);
		
		logger.info("Row Type W(Week) Insertd Sucussfully");
		
		// call the period insert method and insert the all periods
		
	    
		calendarDao.RowTypeP(calendarDto,conn,calendarDto.getStartDate(), CALENDAR_TYPE);
		
		logger.info("Row Type P(Period) Insertd Sucussfully");
		
		
		 FindQuarterForPeriods(calendarDto);
		 	logger.info("---"+calendarDto.getFirstQuarter());
		 		logger.info("----"+calendarDto.getSecondQuarter());
		 			logger.info("-----"+calendarDto.getThirdQuarter());
		 				logger.info("-----"+calendarDto.getFourthQuarter());
		  
		calendarDao.RowTypeQ(calendarDto,conn,calendarDto.getStartDate(),CALENDAR_TYPE);
			logger.info("Row Type Q(Quarter) Insertd Sucussfully");
		
		calendarDao.RowTypeY(calendarDto,conn, CALENDAR_TYPE);
		
		logger.info("Row Type Y(Year) Insertd Sucussfully");
		
		PristineDBUtil.commitTransaction(conn, "Commit");
		
		logger.info("Create Retail Calendar Exit ");
		
		logger.info("Batch Process Exit");
		}
		catch(Exception exe)
		{
		PristineDBUtil.rollbackTransaction(conn, "Calendar Yearly Process Rollbacked");
		 logger.error(exe);
		 logger.info("------------Error In Processing");
		}
				  				     		          
	 finally
	 {
		 try {
			conn.close();
		} catch (SQLException e) {
			logger.error(e);
		}
	 }
				     
		
	
	}

	private static void FindQuarterForPeriods(CalendarYearlyDTO calendarDto) {
		calendarDto.setFirstQuarter(PropertyManager.getProperty("Calendar.First.Quarter."+calendarDto.getYear(),"4"));
		calendarDto.setSecondQuarter(PropertyManager.getProperty("Calendar.Second.Quarter."+calendarDto.getYear(),"3"));
		calendarDto.setThirdQuarter(PropertyManager.getProperty("Calendar.Third.Quarter."+calendarDto.getYear(),"3"));
		calendarDto.setFourthQuarter(PropertyManager.getProperty("Calendar.Fourth.Quarter."+calendarDto.getYear(),"3"));
		
	}
	

}
