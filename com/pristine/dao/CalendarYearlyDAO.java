package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import org.apache.log4j.Logger;
import com.pristine.dto.CalendarYearlyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;


/**
 * @author dinesh
 * 
 * @Date of creation 13-02-2012
 */
public class CalendarYearlyDAO {

	static Logger logger = Logger.getLogger("CalendarYearly");
	int i = 1;
	int periodCount = 1;
	// Date gendrate

	/*
	 * ****************************************************************
	 * Find the inbetween dates and insert into the "Retail_calendar" table
	 * Argument 1: Connection Argument 2: CalendarYearlyDTO Argument 3:
	 * Processdate
	 * 
	 * @throws GeneralException,ParseException
	 * ****************************************************************
	 */

	public boolean RowtypeD(CalendarYearlyDTO calendarDto, Connection conn,
			String processdate, String calType) throws GeneralException, ParseException {

		boolean Insertflag = false;
		try {
			// Get the nextdate for a given date

			String NextDate = DateUtil.GetNextDay(processdate,
					calendarDto.getEndDate());

			// if next date is invalid means process will exit
			if (!NextDate.equalsIgnoreCase("Invalid")) {
				if (i != 1)
					processdate = NextDate;

				// call the insert method
				int calendarId = isDayAlreadyExists(conn, processdate, Constants.CALENDAR_DAY);
				if(calendarId > 0){
					this.RowTypeUpdate(conn, calendarDto, processdate, null, "D", 
							"Row Type D Update", i, calType, calendarId);
				}else{
					this.RowTypeInsert(conn, calendarDto, processdate, null, "D",
							"Row Type D Insert", i, calType);
				}
				i++;

				// using recursion

				RowtypeD(calendarDto, conn, processdate, calType);
				Insertflag = true;

			}
			i = 1;
		} catch (Exception exe) {
			throw new GeneralException("Date Insertion Error", exe);
		}
		return Insertflag;
	}

	/*
	 * ****************************************************************
	 * Find the Inbetween weeks and insert into the "Retail_calendar" table
	 * Argument 1: Connection Argument 2: CalendarYearlyDTO Argument 3:
	 * Processdate
	 * 
	 * @throws GeneralException,ParseException
	 * ****************************************************************
	 */

	public boolean RowtypeW(CalendarYearlyDTO calendarDto, Connection conn,
			String processdate, String calType) throws GeneralException, ParseException {

		boolean InsertFlag = false;

		try {
			// get the next week

			String NextWeek = DateUtil.getNextWeek(processdate,
					calendarDto.getEndDate());

			// if nextweek is invalid means process will exit
			if (!NextWeek.equalsIgnoreCase("Invalid")) {
				// call the insert method
				this.RowTypeInsert(conn, calendarDto, processdate, NextWeek,
						"W", "Row Type W Insert", i, calType);
				i++;
				// using recursion
				RowtypeW(calendarDto, conn,
						DateUtil.GetNextDay(NextWeek, calendarDto.getEndDate()), calType);
				InsertFlag = true;
			}
			i = 1;
		} catch (Exception exe) {
			throw new GeneralException(" Week Insert Error ", exe);
		}
		return InsertFlag;
	}

	/*
	 * ****************************************************************
	 * Find the Inbetween periods and insert into the "Retail_calendar" table
	 * Argument 1: Connection Argument 2: CalendarYearlyDTO Argument 3:
	 * Processdate
	 * 
	 * @throws GeneralException,ParseException
	 * ****************************************************************
	 */
	public boolean RowTypeP(CalendarYearlyDTO calendarDto, Connection conn,
			String startDate, String calType) throws GeneralException, ParseException {

		boolean InsertFlag = false;
		
		String NextPeriod = DateUtil.getNextPeriodFromConfig(startDate,
				calendarDto.getEndDate(), calendarDto, periodCount);
		if (!NextPeriod.equalsIgnoreCase("Invalid")) {
			this.RowTypeInsert(conn, calendarDto, startDate, NextPeriod, "P",
					"Row Type P Insert", i, calType);
			i++;
			periodCount++;
			RowTypeP(calendarDto, conn,
					DateUtil.GetNextDay(NextPeriod, calendarDto.getEndDate()), calType);
			InsertFlag = true;
		}

		return InsertFlag;
	}

	// quarter gendrate
	public boolean RowTypeQ(CalendarYearlyDTO calendarDto, Connection conn,
			String startDate, String calType) throws GeneralException, ParseException {

		boolean InsertFlag = false;

		String Firstquarter = DateUtil.getQuarterForFiveWeeks(startDate,
				calendarDto.getFirstQuarter(), calendarDto.getEndDate(), calendarDto, 1);
		if (Firstquarter != null && !Firstquarter.equalsIgnoreCase("Invalid")) {
			this.RowTypeInsert(conn, calendarDto, startDate, Firstquarter, "Q",
					"Row Type Q Insert", 1, calType);
			startDate = DateUtil.GetNextDay(Firstquarter,
					calendarDto.getEndDate());
			String Secondquarter = DateUtil.getQuarterForFiveWeeks(startDate,
					calendarDto.getSecondQuarter(), calendarDto.getEndDate(), calendarDto, 2);
			if (Secondquarter != null
					&& !Secondquarter.equalsIgnoreCase("Invalid")) {
				this.RowTypeInsert(conn, calendarDto, startDate, Secondquarter,
						"Q", "Row Type Q Insert", 2, calType);
				startDate = DateUtil.GetNextDay(Secondquarter,
						calendarDto.getEndDate());
				String ThirdQuarter = DateUtil
						.getQuarterForFiveWeeks(startDate, calendarDto.getThirdQuarter(),
								calendarDto.getEndDate(), calendarDto, 3);
				if (ThirdQuarter != null
						&& !ThirdQuarter.equalsIgnoreCase("Invalid")) {
					this.RowTypeInsert(conn, calendarDto, startDate,
							ThirdQuarter, "Q", "Row Type Q Insert", 3, calType);
					startDate = DateUtil.GetNextDay(ThirdQuarter,
							calendarDto.getEndDate());
					String FourthQuarter = DateUtil.getQuarterForFiveWeeks(startDate,
							calendarDto.getFourthQuarter(),
							calendarDto.getEndDate(), calendarDto, 4);
					if (FourthQuarter != null
							&& !FourthQuarter.equalsIgnoreCase("Invalid")) {
						this.RowTypeInsert(conn, calendarDto, startDate,
								FourthQuarter, "Q", "Row Type Q Insert", 4, calType);
						InsertFlag = true;
					}
				}
			}
		}

		return InsertFlag;
	}

	/*
	 * ****************************************************************
	 * Insert the date,week,period,quarter, year into "Retail-calendar" table
	 * Argument 1: Connection Argument 2: CalendarYearlyDTO Argument 3:
	 * startdate Argument 4 : enddate Argument 5 : Rowtype (D,W,P,Q,Y) Argument
	 * 6 : Method Name (RowtypeD,RowtypeW,RowtypeP,RowtypeQ,RowtypeY) Argument 7
	 * : Rownumebr
	 * 
	 * @throws GeneralException
	 * ****************************************************************
	 */

	private void RowTypeInsert(Connection conn, CalendarYearlyDTO calendarDto,
			String startDate, String endDate, String rowtype, String Method,
			int rowno, String calType) throws GeneralException {

		try {
			StringBuffer query = new StringBuffer();
			query.append("  INSERT INTO RETAIL_CALENDAR_BASE (ROW_TYPE, START_DATE, ");
			if(calType.equals(Constants.CAL_TYPE_MARKETING)){
				query.append(" MKT_CAL_YEAR, MKT_ACTUAL_NO ");
			}else if(calType.equals(Constants.CAL_TYPE_BUSINESS)){
				query.append(" FIN_CAL_YEAR, FIN_ACTUAL_NO ");
			}else if(calType.equals(Constants.CAL_TYPE_BOTH)){
				query.append(" MKT_CAL_YEAR, MKT_ACTUAL_NO, FIN_CAL_YEAR, FIN_ACTUAL_NO ");
			}
			if (endDate != null)
				query.append(" ,END_DATE ");
			query.append(" ,CALENDAR_ID, CAL_TYPE) values  ");
			if(calType.equals(Constants.CAL_TYPE_BOTH)){
				query.append(" ('" + rowtype 
						+ "', to_date('" + startDate
						+ "','dd-mm-yyyy'), '" + calendarDto.getYear() + "','" 
						+ rowno + "' , '" + calendarDto.getYear() + "','" + rowno + "' ");
			}else{
				query.append(" ('" + rowtype 
						+ "', to_date('" + startDate
						+ "','dd-mm-yyyy'), '" + calendarDto.getYear() + "','" + rowno + "' ");
			}
			if (endDate != null)
				query.append(" ,to_date('" + endDate + "','dd-mm-yyyy') ");
			if(rowtype.equals(Constants.CALENDAR_DAY)){
				query.append(" ,RETAIL_CALENDAR_SEQ.nextval, '" + Constants.CAL_TYPE_BOTH + "') ");
			}else{
				query.append(" ,RETAIL_CALENDAR_SEQ.nextval, '" + calType + "') ");
			}
			logger.debug("Insert Query For" + rowtype + "--" + query.toString());
			PristineDBUtil.execute(conn, query, Method);
		} catch (Exception exe) {
			throw new GeneralException(" Insert Error ", exe);

		}

	}
	
	
	private void RowTypeUpdate(Connection conn, CalendarYearlyDTO calendarDto,
			String startDate, String endDate, String rowtype, String Method,
			int rowno, String calType, int calendarId) throws GeneralException {

		try {
			StringBuffer query = new StringBuffer();
			query.append("  UPDATE RETAIL_CALENDAR_BASE SET ");
			if(calType.equals(Constants.CAL_TYPE_MARKETING)){
				query.append(" MKT_CAL_YEAR = '" + calendarDto.getYear() + "', MKT_ACTUAL_NO = '" + rowno + "' ");
			}else if(calType.equals(Constants.CAL_TYPE_BUSINESS)){
				query.append(" FIN_CAL_YEAR = '" + calendarDto.getYear() + "', FIN_ACTUAL_NO = '" + rowno + "' ");
			}
			
			query.append(" WHERE CALENDAR_ID = " + calendarId);
			logger.debug("Update Query For" + rowtype + "--" + query.toString());
			PristineDBUtil.execute(conn, query, Method);
		} catch (Exception exe) {
			throw new GeneralException(" Insert Error ", exe);

		}

	}

	// year gendrate
	public boolean RowTypeY(CalendarYearlyDTO calendarDto, Connection conn, String calType)
			throws GeneralException {
		boolean InsertFlag = false;

		this.RowTypeInsert(conn, calendarDto, calendarDto.getStartDate(),
				calendarDto.getEndDate(), "Y", "Row Type Y Insert", 1, calType);
		InsertFlag = true;
		logger.info("Retail calendar Exist");
		return InsertFlag;
	}

	
	public int isDayAlreadyExists(Connection conn, String processdate, String rowType) throws GeneralException{
		int calendarId = -1;
		PreparedStatement statement = null;
		ResultSet rs = null;
		try{
			String qry = "SELECT CALENDAR_ID FROM RETAIL_CALENDAR_BASE "
					+ "WHERE START_DATE = TO_DATE('" + processdate + "', "
					+ "'DD-MM-YYYY') AND ROW_TYPE = '" + rowType + "'";
			logger.debug(qry);
			statement = conn.prepareStatement(qry);
			//statement.setString(1, processdate);
			rs = statement.executeQuery();
			if(rs.next()){
				calendarId = rs.getInt("CALENDAR_ID");
			}
		}catch(SQLException sqlE){
			throw new GeneralException("Error -- isDayAlreadyExists()", sqlE);
		}finally {
			PristineDBUtil.close(statement);
			PristineDBUtil.close(rs);
		}
		return calendarId;
	}
}
