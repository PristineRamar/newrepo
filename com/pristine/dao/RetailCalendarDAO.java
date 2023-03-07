package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import javax.sql.rowset.CachedRowSet;

/*import org.apache.commons.httpclient.util.DateUtil;*/
import org.apache.log4j.Logger;

import com.pristine.dto.NelsonMarketDataDTO;
/*import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;*/
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.PeriodCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.DateUtil;

public class RetailCalendarDAO {
	static Logger logger = Logger.getLogger("RetailCalendarDAO");
	SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");	
	
	public static final String GET_CALENDAR_INFO = " SELECT LST.CALENDAR_ID LST_CALENDAR_ID, RC.CALENDAR_ID,TO_CHAR(RC.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS START_DATE, " +
			" TO_CHAR(RC.END_DATE,'"+Constants.DB_DATE_FORMAT+"') AS END_DATE, RC.ROW_TYPE FROM RETAIL_CALENDAR RC " +
			" LEFT JOIN RETAIL_CALENDAR LST ON (RC.CAL_YEAR-1)= LST.CAL_YEAR AND RC.ACTUAL_NO = LST.ACTUAL_NO AND RC.ROW_TYPE = LST.ROW_TYPE WHERE " + 
			" RC.START_DATE <= TO_DATE(?,'"+Constants.DB_DATE_FORMAT+"') AND RC.END_DATE >= TO_DATE(?,'"+Constants.DB_DATE_FORMAT+"') AND RC.ROW_TYPE = ? ";
	
	public static final String GET_PR_CALENDAR_INFO = " SELECT LST.CALENDAR_ID LST_CALENDAR_ID, RC.CALENDAR_ID,TO_CHAR(RC.START_DATE,'"
			+ Constants.DB_DATE_FORMAT + "') AS START_DATE, " + " TO_CHAR(RC.END_DATE,'" + Constants.DB_DATE_FORMAT
			+ "') AS END_DATE, RC.ROW_TYPE FROM RETAIL_CALENDAR_PROMO RC "
			+ " LEFT JOIN RETAIL_CALENDAR_PROMO LST ON (RC.CAL_YEAR-1)= LST.CAL_YEAR AND RC.ACTUAL_NO = LST.ACTUAL_NO AND RC.ROW_TYPE = LST.ROW_TYPE WHERE "
			+ " RC.START_DATE <= TO_DATE(?,'" + Constants.DB_DATE_FORMAT + "') AND RC.END_DATE >= TO_DATE(?,'"
			+ Constants.DB_DATE_FORMAT + "') AND RC.ROW_TYPE = ? ";
	
	public static final String GET_CALENDAR_INFO_FOR_DAY = " SELECT LST.CALENDAR_ID LST_CALENDAR_ID, RC.CALENDAR_ID,TO_CHAR(RC.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS START_DATE, " +
			" TO_CHAR(RC.END_DATE,'"+Constants.DB_DATE_FORMAT+"') AS END_DATE, RC.ROW_TYPE FROM RETAIL_CALENDAR RC " +
			" LEFT JOIN RETAIL_CALENDAR LST ON (RC.CAL_YEAR-1)= LST.CAL_YEAR AND RC.ACTUAL_NO = LST.ACTUAL_NO AND RC.ROW_TYPE = LST.ROW_TYPE WHERE " + 
			" RC.START_DATE = TO_DATE(?,'"+Constants.DB_DATE_FORMAT+"') AND RC.END_DATE IS NULL AND RC.ROW_TYPE = ? ";
	
	public static final String GET_PR_CALENDAR_INFO_FOR_DAY = " SELECT LST.CALENDAR_ID LST_CALENDAR_ID, RC.CALENDAR_ID,TO_CHAR(RC.START_DATE,'"
			+ Constants.DB_DATE_FORMAT + "') AS START_DATE, " + " TO_CHAR(RC.END_DATE,'" + Constants.DB_DATE_FORMAT
			+ "') AS END_DATE, RC.ROW_TYPE FROM RETAIL_CALENDAR_PROMO RC "
			+ " LEFT JOIN RETAIL_CALENDAR_PROMO LST ON (RC.CAL_YEAR-1)= LST.CAL_YEAR AND RC.ACTUAL_NO = LST.ACTUAL_NO AND RC.ROW_TYPE = LST.ROW_TYPE WHERE "
			+ " RC.START_DATE = TO_DATE(?,'" + Constants.DB_DATE_FORMAT
			+ "') AND RC.END_DATE IS NULL AND RC.ROW_TYPE = ? ";

	public static final String GET_CALENDARID_FOR_WEEK = "SELECT CALENDAR_ID, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE, TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE FROM RETAIL_CALENDAR WHERE START_DATE <= (SELECT START_DATE FROM RETAIL_CALENDAR WHERE CALENDAR_ID= ?) " +
			  											 "AND END_DATE >= (SELECT START_DATE FROM RETAIL_CALENDAR WHERE CALENDAR_ID= ? ) " +
			  											 "AND ROW_TYPE  = 'W' ";
	
	public static final String GET_WEEKNO = "SELECT ACTUAL_NO, CAL_YEAR FROM RETAIL_CALENDAR WHERE END_DATE = TO_DATE(?,'MM/dd/yy') AND ROW_TYPE = 'W'";
	
	public static final String GET_CALENDAR_DETAILS = "SELECT CALENDAR_ID, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE, TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE " +
													  "FROM RETAIL_CALENDAR WHERE CALENDAR_ID = ?";
	
	public static final String GET_CALENDAR_LIST_FOR_WEEKS = "SELECT CALENDAR_ID, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE, TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE, END_DATE END_DT FROM RETAIL_CALENDAR WHERE START_DATE >= (SELECT START_DATE - (? * 7) FROM RETAIL_CALENDAR WHERE " +
															 "CALENDAR_ID = ?) AND START_DATE <= (SELECT START_DATE FROM RETAIL_CALENDAR WHERE CALENDAR_ID = ?) AND ROW_TYPE = 'W' ORDER BY END_DT DESC";
	
	public static final String GET_PREV_CALENDAR = "SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE = ( " +
			"SELECT MAX(START_DATE) FROM RETAIL_CALENDAR WHERE START_DATE< TO_DATE(?,'MM/dd/yyyy') AND ROW_TYPE=? ) AND ROW_TYPE=? ";
	
	private static final String GET_PERIOD = " SELECT CALENDAR_ID, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE, TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE  FROM RETAIL_CALENDAR WHERE ROW_TYPE = 'P' AND START_DATE <=  TO_DATE(?,'MM/dd/yy') "
			+ " AND END_DATE >= TO_DATE(?,'MM/dd/yy') ";
	
	private static final String GET_WEEK_CALENDAR_FROM_DATE = "SELECT  CALENDAR_ID, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE, " +
			" TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE FROM RETAIL_CALENDAR RC " +
			" WHERE RC.START_DATE <= TO_DATE(?, 'MM/DD/YYYY') AND RC.END_DATE >= TO_DATE(?, 'MM/DD/YYYY') " +
			" AND RC.ROW_TYPE = 'W'" ;
	
	
	private static final String GET_ALL_PREVIOUS_WEEKS = "SELECT CALENDAR_ID, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE,"
			+ " TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE FROM RETAIL_CALENDAR RC WHERE  ROW_TYPE='W' AND RC.START_DATE < TO_DATE(?,'MM/DD/YYYY') " +
			" AND RC.START_DATE > TO_DATE(?,'MM/DD/YYYY') - (? + 1)*7 ORDER BY RC.START_DATE DESC ";
	
	private static final String GET_ALL_FUTURE_WEEKS = "SELECT CALENDAR_ID, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE,"
			+ " TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE FROM RETAIL_CALENDAR RC WHERE  ROW_TYPE='W' AND RC.START_DATE > TO_DATE(?,'MM/DD/YYYY') " +
			" AND RC.START_DATE < TO_DATE(?,'MM/DD/YYYY') + (? + 1)*7 ORDER BY RC.START_DATE ASC ";
	
	private static final String GET_ALL_WEEKS = "SELECT CALENDAR_ID, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE,"
			+ " TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE FROM RETAIL_CALENDAR RC WHERE  ROW_TYPE='W' ";
	
	private static final String GET_FULL_RETAIL_CALENDAR = "SELECT CAL_YEAR,ROW_TYPE,ACTUAL_NO, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE,"
			+ "TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE,CALENDAR_ID,SPECIAL_DAY_ID,WEEK_NO "
			+ "FROM RETAIL_CALENDAR RC";
	
	private static final String GET_FULL_RETAIL_CALENDAR_PROMO = "SELECT CAL_YEAR,ROW_TYPE,ACTUAL_NO, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE,"
			+ "TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE,CALENDAR_ID,SPECIAL_DAY_ID,WEEK_NO "
			+ "FROM RETAIL_CALENDAR_PROMO RC";
	
	private static final String GET_MONTH_START_DATE_BASED_ON_GIVEN_DATE ="SELECT TO_CHAR(trunc(to_date(?,'MM/dd/yy')) "
			+ "- (to_number(to_char(to_date(?,'MM/dd/yy'),'DD')) - 1),'MM/dd/yyyy') START_DATE FROM dual ";
	
	private static final String GET_QUARTER_DETAILS = "SELECT CAL_YEAR,ROW_TYPE,ACTUAL_NO, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE,"
			+ "TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE,CALENDAR_ID,SPECIAL_DAY_ID,WEEK_NO "
			+ "FROM RETAIL_CALENDAR RC WHERE RC.START_DATE <= TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "') " + "AND RC.END_DATE >= TO_DATE(?, '"
			+ Constants.DB_DATE_FORMAT + "') AND RC.ROW_TYPE = '" + Constants.CALENDAR_QUARTER + "'";
	private static final String GET_EVERYDAY_DATE_AND_RESPECTIVE_WEEK_CALENDAR_ID = "SELECT TO_CHAR(RC1.START_DATE,'MM/dd/YYYY') AS START_DATE, "
			+ "RC.CALENDAR_ID FROM ((SELECT START_DATE, END_DATE, CALENDAR_ID FROM RETAIL_CALENDAR WHERE ROW_TYPE=?) RC JOIN "
			+ "(SELECT START_DATE, CALENDAR_ID FROM RETAIL_CALENDAR WHERE ROW_TYPE=?) RC1 ON RC.START_DATE <= RC1.START_DATE "
			+ "AND RC.END_DATE >= RC1.START_DATE)";
	
	public static final String GET_PROMO_CALENDAR_INFO_FOR_DAY = " SELECT LST.CALENDAR_ID LST_CALENDAR_ID, "
			+ " RC.CALENDAR_ID,TO_CHAR(RC.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS START_DATE, "
			+ " TO_CHAR(RC.END_DATE,'"+Constants.DB_DATE_FORMAT+"') AS END_DATE, RC.ROW_TYPE FROM RETAIL_CALENDAR_PROMO RC "
			+ " LEFT JOIN RETAIL_CALENDAR_PROMO LST ON (RC.CAL_YEAR-1)= LST.CAL_YEAR AND RC.ACTUAL_NO = LST.ACTUAL_NO "
			+ " AND RC.ROW_TYPE = LST.ROW_TYPE WHERE "
			+ " RC.START_DATE = TO_DATE(?,'"+Constants.DB_DATE_FORMAT+"') AND RC.END_DATE IS NULL AND RC.ROW_TYPE = ? ";
	
	public static final String GET_PROMO_CALENDAR_INFO = " SELECT LST.CALENDAR_ID LST_CALENDAR_ID, RC.CALENDAR_ID,TO_CHAR(RC.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS START_DATE, " +
			" TO_CHAR(RC.END_DATE,'"+Constants.DB_DATE_FORMAT+"') AS END_DATE, RC.ROW_TYPE FROM RETAIL_CALENDAR_PROMO RC " +
			" LEFT JOIN RETAIL_CALENDAR_PROMO LST ON (RC.CAL_YEAR-1)= LST.CAL_YEAR AND RC.ACTUAL_NO = LST.ACTUAL_NO AND RC.ROW_TYPE = LST.ROW_TYPE WHERE " + 
			" RC.START_DATE <= TO_DATE(?,'"+Constants.DB_DATE_FORMAT+"') AND RC.END_DATE >= TO_DATE(?,'"+Constants.DB_DATE_FORMAT+"') AND RC.ROW_TYPE = ? ";
	 
	public static final String GET_FUTURE_CALENDAR_LIST_FOR_WEEKS = "SELECT CALENDAR_ID, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE, "
			+ " TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE, END_DATE END_DT FROM RETAIL_CALENDAR WHERE START_DATE <= (SELECT START_DATE + (? * 7) "
			+ " FROM RETAIL_CALENDAR WHERE CALENDAR_ID = ?) AND START_DATE > (SELECT START_DATE FROM RETAIL_CALENDAR WHERE CALENDAR_ID = ?) "
			+ " AND ROW_TYPE = 'W' ORDER BY END_DT DESC";
	
	
	private static final String GET_RETAIL_CALENDAR = "SELECT CAL_YEAR,ROW_TYPE,ACTUAL_NO, TO_CHAR(START_DATE,'MM/dd/yyyy') START_DATE,"
			+ "TO_CHAR(END_DATE,'MM/dd/yyyy') END_DATE,CALENDAR_ID,SPECIAL_DAY_ID,WEEK_NO "
			+ "FROM RETAIL_CALENDAR RC  WHERE   RC.START_DATE = TO_DATE(?, 'MM/DD/YYYY') AND RC.END_DATE = TO_DATE(?, 'MM/DD/YYYY')";
	
	private static final String GET_X_WEEKS_CALENDAR_ID = " SELECT START_DATE + (? * 7) AS END_DATE FROM RETAIL_CALENDAR "
			+ " WHERE CALENDAR_ID = ? AND START_DATE <= (SELECT START_DATE FROM RETAIL_CALENDAR WHERE CALENDAR_ID = ?) AND ROW_TYPE = 'W' ";

	/*
	 * ****************************************************************
	 * Method used to get the Day Calendar Id 
	 * Argument 1: Connection 
	 * Argument 2: Start Date
	 * Argument 3: End Date
	 * Argument 4: Row Type 
	 * return : List of Calendar Id
	 * @throws GeneralException,SQLException
	 * ****************************************************************
	 */

	public List<RetailCalendarDTO> dayCalendarList(Connection _Conn,
			Date Startdate, Date Enddate, String rowType) throws GeneralException {

		List<RetailCalendarDTO> dateList = new ArrayList<RetailCalendarDTO>();
		StringBuffer sql = new StringBuffer();
		
		sql.append("SELECT RCC.START_DATE, RCC.CALENDAR_ID,");
		sql.append(" RCL.CALENDAR_ID AS LAST_CALENDAR_ID, RCC.END_DATE,");
		sql.append(" RCC.ROW_TYPE FROM RETAIL_CALENDAR RCC");
		sql.append(" JOIN RETAIL_CALENDAR RCL");
		sql.append(" ON RCL.ROW_TYPE          = RCC.ROW_TYPE");
		sql.append(" AND RCC.START_DATE - 364 = RCL.START_DATE");
		sql.append(" WHERE RCC.START_DATE >= to_date('").append(formatter.format(Startdate)).append("','dd-MM-yyyy')");
		sql.append(" AND RCC.START_DATE <= to_date('").append(formatter.format(Enddate)).append("','dd-MM-yyyy')");
		sql.append(" AND RCC.ROW_TYPE ='").append(rowType).append("' ORDER BY RCC.CALENDAR_ID");

		logger.debug("DayCalendarList SQL:" + sql.toString());
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,
					"DayCalendarList");

			if (result.size() > 0){
				logger.debug("Total Calendad Id count:" + result.size() );
				while (result.next()) {
					RetailCalendarDTO dto = new RetailCalendarDTO();
					dto.setCalendarId(result.getInt("CALENDAR_ID"));
					dto.setStartDate(result.getString("START_DATE"));
					dto.setEndDate(result.getString("END_DATE"));
					dto.setcalendarMode(result.getString("ROW_TYPE"));
					dto.setlstCalendarId(result.getInt("LAST_CALENDAR_ID"));
					dateList.add(dto);
				}
			}
			else{
				logger.error("No calendar data found");
			}
		} catch (GeneralException ge) {
			logger.error("Error while fetching Calendar Ids:" 
														+ ge.getMessage());
			throw new GeneralException("dayCalendarList", ge);
		}
		catch (SQLException se) {
			logger.error("Error while fetching Calendar Ids:" + se.getMessage());
			throw new GeneralException("dayCalendarList", se);
		}
		return dateList;
	}

	/*
	 * ****************************************************************
	 * Method used to get the Week/Period/Quarter/Year Calendar Id
	 * Argument 1: Connection 
	 * Argument 2: Process Date
	 * Argument 3: End Date
	 * Argument 4: Row Type 
	 * return : List of Calendar Id
	 * @throws GeneralException,SQLException
	 * ****************************************************************
	 */

	public RetailCalendarDTO week_period_quarter_yearCalendarList(Connection _conn,
			Date _processDate, String rowType) throws GeneralException {

		RetailCalendarDTO objCalendarDto = new RetailCalendarDTO();
		
		StringBuffer sql = new StringBuffer();

		if (rowType == "W"){
			sql.append(" SELECT RC.START_DATE, ");
			sql.append(" RC.END_DATE, ");
			sql.append(" RC.CALENDAR_ID  AS CALENDAR_ID, ");
			sql.append(" LRC.CALENDAR_ID AS LAST_CALENDAR_ID,");
			sql.append(" RC.ROW_TYPE");
			sql.append(" From Retail_Calendar RC");
			sql.append(" JOIN Retail_Calendar LRC ON RC.ROW_TYPE = LRC.ROW_TYPE AND RC.START_DATE - 364 = LRC.START_DATE");
			sql.append(" Where RC.Start_Date <= To_Date('"+formatter.format(_processDate)+"', 'dd-MM-yyyy')");
			sql.append(" AND RC.end_date     >= to_date('"+formatter.format(_processDate)+"', 'dd-MM-yyyy')");
			sql.append(" AND RC.row_type ='" + rowType + "'");			
		}
		else
		{
			sql.append(" select RC.START_DATE,RC.END_DATE, RC.CALENDAR_ID AS CALENDAR_ID,");
			sql.append(" LRC.CALENDAR_ID as LAST_CALENDAR_ID,RC.ROW_TYPE  ");
			sql.append(" from  ( select CALENDAR_ID, ACTUAL_NO ");
			sql.append(" from RETAIL_CALENDAR where  ROW_TYPE ='"+rowType+"'");
			sql.append(" and CAL_YEAR=(select CAL_YEAR - 1  from retail_calendar where start_date <= to_date('"+formatter.format(_processDate)+"','dd-MM-yyyy') and end_date >= to_date('"+formatter.format(_processDate)+"','dd-MM-yyyy') and row_type='" + rowType + "' )) LRC,");
			sql.append(" RETAIL_CALENDAR RC ");
			sql.append(" where  to_date('"+formatter.format(_processDate)+"','dd-MM-yyyy')");
			sql.append(" between RC.START_DATE and RC.END_DATE and RC.ROW_TYPE ='"+rowType+"'");
			sql.append(" and LRC.ACTUAL_NO =RC.ACTUAL_NO and RC.CALENDAR_ID!=LRC.CALENDAR_ID");
			sql.append(" order by RC.CALENDAR_ID");
		}
		
		logger.debug(" Sql :  "+ sql.toString());
		try {

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"week_period_quarter_yearCalendarList");

			if (result.next()) {
				objCalendarDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objCalendarDto.setStartDate(result.getString("START_DATE"));
				objCalendarDto.setEndDate(result.getString("END_DATE"));
				objCalendarDto.setcalendarMode(rowType);
				objCalendarDto.setlstCalendarId(result.getInt("LAST_CALENDAR_ID"));
			}

		} catch (Exception gex) {
			logger.error(gex);
			throw new GeneralException("Retail Calendar Error");
		} 
		return objCalendarDto;
	}
	

	
	
	
	/*
	 * ****************************************************************
	 * Method used to get the Week/Period/Quarter/Year Calendar Id
	 * Argument 1: Connection 
	 * Argument 2: Process Date
	 * Argument 3: End Date
	 * Argument 4: Row Type 
	 * return : List of Calendar Id
	 * @throws GeneralException,SQLException
	 * ****************************************************************
	 */

	public List<RetailCalendarDTO> getProcessingPeriodList(Connection _conn,
			Date startDate, Date endDate, String rowType) throws GeneralException {

		List<RetailCalendarDTO> calendarList = new ArrayList<RetailCalendarDTO>();

		
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT TO_CHAR(RC.START_DATE, 'dd/MM/yyyy') AS START_DATE,");
		sql.append(" TO_CHAR(RC.END_DATE, 'dd/MM/yyyy') AS END_DATE,");
		sql.append(" RC.CALENDAR_ID  AS CALENDAR_ID, ");
		sql.append(" RC.ROW_TYPE");
		sql.append(" FROM RETAIL_CALENDAR RC");
		sql.append(" WHERE RC.START_DATE >= To_Date('"+formatter.format(startDate)+"', 'dd-MM-yyyy')");
		sql.append(" AND RC.END_DATE <= to_date('"+formatter.format(endDate)+"', 'dd-MM-yyyy')");
		sql.append(" AND RC.ROW_TYPE ='" + rowType + "'");
		sql.append(" ORDER BY RC.CALENDAR_ID");
		
		logger.debug(" Sql :  "+ sql.toString());
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
													"getProcessingPeriodList");

			if (result.size() > 0){
				logger.debug("Total Calendad Id count:" + result.size() );
				while (result.next()) {
					RetailCalendarDTO calendarDto = new RetailCalendarDTO();
					calendarDto.setCalendarId(result.getInt("CALENDAR_ID"));
					calendarDto.setCalendarId(result.getInt("CALENDAR_ID"));
					calendarDto.setStartDate(result.getString("START_DATE"));
					calendarDto.setEndDate(result.getString("END_DATE"));
					calendarDto.setcalendarMode(rowType);
					calendarList.add(calendarDto);
				}
			}

		} catch (Exception gex) {
			logger.error(gex);
			throw new GeneralException("Retail Calendar Error");
		} 
		return calendarList;
	}
		
	
	
	
	
	
	
	
	
	public RetailCalendarDTO week_period_quarter_yearCalendarList_DB_DATE(Connection _conn,
			Date _processDate, String rowType) throws GeneralException {

		RetailCalendarDTO objCalendarDto = new RetailCalendarDTO();
				
		StringBuffer sql = new StringBuffer();
		sql.append(" select TO_CHAR(RC.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS START_DATE," +
					" TO_CHAR(RC.END_DATE,'"+Constants.DB_DATE_FORMAT+"') AS END_DATE, RC.CALENDAR_ID AS CALENDAR_ID,");
		sql.append(" LRC.CALENDAR_ID as LAST_CALENDAR_ID,RC.ROW_TYPE  ");
		sql.append(" from  ( select CALENDAR_ID, ACTUAL_NO ");
		sql.append(" from RETAIL_CALENDAR where  ROW_TYPE ='"+rowType+"'");
		// sql.append(" and CAL_YEAR=to_number(substr('"+formatter.format(_processDate)+"',7) ,'9999')-1  ) LRC,");
		sql.append(" and CAL_YEAR=(select CAL_YEAR - 1  from retail_calendar where start_date <= to_date('"+formatter.format(_processDate)+"','dd-MM-yyyy') and end_date >= to_date('"+formatter.format(_processDate)+"','dd-MM-yyyy') and row_type='" + rowType + "' )) LRC,");
		sql.append(" RETAIL_CALENDAR RC ");
		sql.append(" where  to_date('"+formatter.format(_processDate)+"','dd-MM-yyyy')");
		sql.append(" between RC.START_DATE and RC.END_DATE and RC.ROW_TYPE ='"+rowType+"'");
		sql.append(" and LRC.ACTUAL_NO =RC.ACTUAL_NO and RC.CALENDAR_ID!=LRC.CALENDAR_ID");
		sql.append(" order by RC.CALENDAR_ID");
	
		logger.debug(" Sql :  "+ sql.toString());
		try {

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"week_period_quarter_yearCalendarList");

			if (result.next()) {
				objCalendarDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objCalendarDto.setStartDate(result.getString("START_DATE"));
				objCalendarDto.setEndDate(result.getString("END_DATE"));
				objCalendarDto.setcalendarMode(rowType);
				objCalendarDto.setlstCalendarId(result.getInt("LAST_CALENDAR_ID"));
			}

		} catch (Exception gex) {
			logger.error(gex);
			throw new GeneralException("Retail Calendar Error");
		} 
		return objCalendarDto;
	}

	public List<PeriodCalendarDTO> getPeriodForDate(Connection _conn,
			Date _processDate, String calType) throws GeneralException {

		List<PeriodCalendarDTO> objCalendarList = new ArrayList<PeriodCalendarDTO>();
				
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT RCD.CALENDAR_ID AS DAY_CALENDAR_ID," +
			" TO_CHAR(RCD.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS PROCESSING_DAY," +
			" RCW.CALENDAR_ID AS WPQY_CALENDAR_ID," +
			" RCW.ACTUAL_NO AS WPQY_NO," +
			" TO_CHAR(RCW.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS WEEK_START_DATE," +
			" TO_CHAR(RCW.END_DATE,'"+Constants.DB_DATE_FORMAT+"') AS WEEK_END_DATE," +
			" RCWL.CALENDAR_ID AS LAST_WPQY_CALENDAR_ID" +
			" FROM RETAIL_CALENDAR RCD");
		
		if (calType.equalsIgnoreCase(Constants.CALENDAR_WEEK))
			sql.append(" JOIN RETAIL_CALENDAR RCW ON RCW.ROW_TYPE = 'W'");
		else if (calType.equalsIgnoreCase(Constants.CALENDAR_PERIOD))
			sql.append(" JOIN RETAIL_CALENDAR RCW ON RCW.ROW_TYPE = 'P'");
		else if (calType.equalsIgnoreCase(Constants.CALENDAR_QUARTER))
			sql.append(" JOIN RETAIL_CALENDAR RCW ON RCW.ROW_TYPE = 'Q'");
		else if (calType.equalsIgnoreCase(Constants.CALENDAR_YEAR))
			sql.append(" JOIN RETAIL_CALENDAR RCW ON RCW.ROW_TYPE = 'Y'");
		
		 if (calType.equalsIgnoreCase(Constants.CALENDAR_WEEK)){
			 sql.append("JOIN RETAIL_CALENDAR RCWL");
			 sql.append(" ON RCW.ROW_TYPE = RCWL.ROW_TYPE");
			 sql.append(" AND RCW.START_DATE - 364 = RCWL.START_DATE ");
			 sql.append(" AND RCW.END_DATE - 364 = RCWL.END_DATE");
		 }
		 else{
			sql.append(" JOIN RETAIL_CALENDAR RCWL ON" + 
				" RCW.ROW_TYPE = RCWL.ROW_TYPE" + 
				" AND RCWL.CAL_YEAR = (RCW.CAL_YEAR - 1)" + 
				" AND RCW.ACTUAL_NO = RCWL.ACTUAL_NO");
		 }		
		
		sql.append(" AND RCW.START_DATE <= RCD.START_DATE" +
			" AND RCW.END_DATE   >= RCD.START_DATE" +
			" WHERE RCD.START_DATE = TO_DATE('"+formatter.format(_processDate)+"','dd-MM-yyyy')");
	
		logger.debug("getPeriodForDate - Sql :  "+ sql.toString());
		try {

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"week_period_quarter_yearCalendarList");

			if (result.next()) {
				PeriodCalendarDTO objCalendarDto = new PeriodCalendarDTO(); 

				 objCalendarDto.setPeriodCalendarId(result.getInt("WPQY_CALENDAR_ID"));
				 objCalendarDto.setPeriodStartDate(result.getString("WEEK_START_DATE"));
				 objCalendarDto.setPeriodEndDate(result.getString("WEEK_END_DATE"));
				 objCalendarDto.setActualNo(result.getInt("WPQY_NO"));
				 objCalendarDto.setDayCalendarId(result.getInt("DAY_CALENDAR_ID"));
				 objCalendarDto.setProcessingDate(result.getString("PROCESSING_DAY"));
				 objCalendarDto.setLastPeriodCalendarId(result.getInt("LAST_WPQY_CALENDAR_ID"));
				 objCalendarDto.setCalendarMode(calType);
				 objCalendarList.add(objCalendarDto);
			}

		} catch (Exception gex) {
			logger.error(gex);
			throw new GeneralException("Retail Calendar Error");
		} 
		return objCalendarList;
	}

	
	
	/**
	 * This method retrieves details from retail_calendar table for specified date and calendar mode
	 * @param conn				Connection 
	 * @param startDate			Date of the week
	 * @param calendarMode		Calendar Mode
	 * @return RetailCalendarDTO
	 */
	public RetailCalendarDTO getCalendarId(Connection conn, String startDate,
			String calendarMode) throws GeneralException {
		
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    RetailCalendarDTO calendar = new RetailCalendarDTO();
		
		try{
			if(Constants.CALENDAR_DAY.equals(calendarMode)){
				statement = conn.prepareStatement(GET_CALENDAR_INFO_FOR_DAY);
		        statement.setString(1, startDate);
		        statement.setString(2, calendarMode);
			}else{
		
				statement = conn.prepareStatement(GET_CALENDAR_INFO);
		        statement.setString(1, startDate);
		        statement.setString(2, startDate);
		        statement.setString(3, calendarMode);
			}
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	calendar.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	calendar.setStartDate(resultSet.getString("START_DATE"));
	        	calendar.setEndDate(resultSet.getString("END_DATE"));
	        	calendar.setlstCalendarId(resultSet.getInt("LST_CALENDAR_ID"));
	        	calendar.setcalendarMode(calendarMode);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_CALENDAR_ID");
			throw new GeneralException("Error while executing GET_CALENDAR_ID", e);
		}finally{
			try{
				if(resultSet != null){
					resultSet.close();
				}
				if(statement != null){
					statement.close();
				}
			}catch(SQLException e){
				logger.error("Error closing statement");
				throw new GeneralException("Error closing statement", e);
			}
		}
		return calendar;
	}
	
	/**
	 * This method retrieves previous calendar_id from retail_calendar table for specified date and calendar mode
	 * @param conn				Connection 
	 * @param startDate			Date of the week
	 * @param calendarMode		Calendar Type (D,W,P,Q,Y)
	 * @return calendarId		Previous Year Calendar ID
	 */
	public int getPrevCalendarId(Connection conn, String startDate, String calendarMode) throws GeneralException {
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    
	    int calendarId = 0;
		try{
			statement = conn.prepareStatement(GET_PREV_CALENDAR);
	        statement.setString(1, startDate);
	        statement.setString(2, calendarMode);
	        statement.setString(3, calendarMode);
			
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	calendarId = resultSet.getInt("CALENDAR_ID");
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getPrevCalendarId");
			throw new GeneralException("Error while executing getPrevCalendarId", e);
		}finally{
			try{
				if(resultSet != null){
					resultSet.close();
				}
				if(statement != null){
					statement.close();
				}
			}catch(SQLException e){
				logger.error("Error closing statement");
				throw new GeneralException("Error closing statement", e);
			}
		}
		return calendarId;
	}
	
	
	/**
	 * This method retrieves calendar ids for which the necessary data loading is required
	 * @param conn				Connection 
	 * @param tableName			Name of the table for which data loading is required
	 * @param calendarMode		Calendar Type (D,W,P,Q,Y)
	 */
	public List<RetailCalendarDTO> getCalendarIdsForDataLoading(Connection conn, int lastCalendarId, String calendarMode) throws GeneralException {
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    
	    List<RetailCalendarDTO> calendarList = new ArrayList<RetailCalendarDTO>();
	    int calendarId = 0;
	    StringBuffer sql = new StringBuffer();
		try{
			//1. Retrive the current date
			String currDate = DateUtil.getDateFromCurrentDate(0);
			
			//2. Get the calendar Ids for which the data loading is required.
			sql.setLength(0);
			sql.append("SELECT LST.CALENDAR_ID LST_CALENDAR_ID, RC.CALENDAR_ID,TO_CHAR(RC.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS START_DATE," +
					" TO_CHAR(RC.END_DATE,'"+Constants.DB_DATE_FORMAT+"') AS END_DATE FROM RETAIL_CALENDAR RC " +
					" LEFT JOIN RETAIL_CALENDAR LST ON (RC.CAL_YEAR-1)= LST.CAL_YEAR AND RC.ACTUAL_NO = LST.ACTUAL_NO AND RC.ROW_TYPE = LST.ROW_TYPE" +
					" WHERE RC.START_DATE > ( SELECT START_DATE FROM RETAIL_CALENDAR WHERE CALENDAR_ID = " + lastCalendarId + ") AND "); 
			
			String condition = " RC.START_DATE < to_date('" + currDate + "','"+ Constants.DB_DATE_FORMAT +"') AND RC.ROW_TYPE='" + calendarMode + "'";
			
			//if(calendarMode.equals("D"))
			//	condition = " RC.START_DATE < to_date('" + currDate + "','"+ Constants.DB_DATE_FORMAT +"') AND RC.ROW_TYPE='D'"; 
			
			condition += " ORDER BY RC.CALENDAR_ID";
			sql.append( condition );
			
			logger.debug(sql.toString());
			
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getCalendarIdsForDataLoading");
			
			while (result.next()) {
				RetailCalendarDTO dto = new RetailCalendarDTO();
				dto.setCalendarId(result.getInt("CALENDAR_ID"));
				dto.setStartDate(result.getString("START_DATE"));
				dto.setEndDate(result.getString("END_DATE"));
				dto.setcalendarMode(calendarMode);
				dto.setlstCalendarId(result.getInt("LST_CALENDAR_ID"));
				calendarList.add(dto);
			}
			
			//3. If the calendarlist is 0 then return current calendar details(lastCalendarId) only for reload.
			if(calendarList.size() == 0){
				sql.setLength(0);
				sql.append("SELECT LST.CALENDAR_ID LST_CALENDAR_ID, RC.CALENDAR_ID,TO_CHAR(RC.START_DATE,'"+Constants.DB_DATE_FORMAT+"') AS START_DATE," +
						" TO_CHAR(RC.END_DATE,'"+Constants.DB_DATE_FORMAT+"') AS END_DATE FROM RETAIL_CALENDAR RC " +
						" LEFT JOIN RETAIL_CALENDAR LST ON (RC.CAL_YEAR-1)= LST.CAL_YEAR AND RC.ACTUAL_NO = LST.ACTUAL_NO AND RC.ROW_TYPE = LST.ROW_TYPE" +
						" WHERE RC.CALENDAR_ID = " + lastCalendarId );
				
				logger.debug(sql.toString());
				
				result = PristineDBUtil.executeQuery(conn, sql, "getCalendarIdsForDataLoading");
				
				if (result.next()) {
					RetailCalendarDTO dto = new RetailCalendarDTO();
					dto.setCalendarId(result.getInt("CALENDAR_ID"));
					dto.setStartDate(result.getString("START_DATE"));
					dto.setEndDate(result.getString("END_DATE"));
					dto.setcalendarMode(calendarMode);
					dto.setlstCalendarId(result.getInt("LST_CALENDAR_ID"));
					calendarList.add(dto);
				}
			}
			
			result.close();
		} catch (SQLException e) {
			logger.error(e);
			logger.error("TYhis is test message");
			logger.error(e.getNextException());
			throw new GeneralException("getCalendarIdsForDataLoading List Method Error" + e);
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new GeneralException(
					"getCalendarIdsForDataLoading List Method Error" + e);
		}
		
		return calendarList;
	}
	
	/*
	 * Method Used to get the daily calendar id based on excel file
	 * Argument 1 : Connection
	 * Argument 2 : ProcessDate
	 * Argument 3 : RowType
	 * Returns Daily Calendar List 
	 * @catch Exception
	 * @throws General , SqlException
	 */

	public HashMap<String, Integer> excelBasedCalendarList(Connection conn,
			Date processDate, String rowType, int processYear, Date processYearDate)
			throws GeneralException {

		// hold the calendar list
		HashMap<String, Integer> calendarList = new HashMap<String, Integer>();
		StringBuffer sql = new StringBuffer();
		int i = 1; // for aviod the duplicate dbcall
		try {
			sql.append(" select CALENDAR_ID from RETAIL_CALENDAR");
			sql.append(" where ROW_TYPE='" + rowType + "'");

			// for budget process
			if (processYear != 0) {
				sql.append("  and CAL_YEAR=" + processYear + "");
			}

			// for forecast process
			else {
				sql.append(" and START_DATE > to_date('"
						+ formatter.format(processDate) + "','dd-mm-yyyy')-7");
				sql.append(" and START_DATE <=to_date('"
						+ formatter.format(processDate) + "','dd-mm-yyyy')");
				//sql.append(" and CAL_YEAR=to_number(substr('"
				//		+ formatter.format(processYearDate) + "',7) ,'9999')");
			}
			sql.append(" order by CALENDAR_ID");

		logger.debug("excelBasedCalendarList Sql : " + sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"GetForeCastDayCalendar List");
			while (result.next()) {

				// for budget process
				if (processYear != 0) {
					calendarList
							.put("PERIOD" + i, result.getInt("CALENDAR_ID"));
				}

				// for forecast process
				else {
					calendarList.put(processDate + "_" + i,
							result.getInt("CALENDAR_ID"));
				}
				i++;
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new GeneralException(
					"GetForeCastDayCalendar List Method Error" + e);
		}
		// logger.debug("Calendar List Size :" + calendarList.size());
		return calendarList;
	}
      
	/*
	 *  Method used to get the week/Period/Quarter/Year CalendarLsit (For full Year)
	 * Argument 1 : Connection
	 * Argument 2 : RowType 
	 * Argument 3 : ProcessingYear
	 * Return Calendar List
	 * @catch Exception
	 * @Throws GeneralException
	 * 
	 */
	
	public List<RetailCalendarDTO> forecastCalendarList(Connection conn,
			String rowType, Object processYear , String methodName) throws GeneralException {
	
		List<RetailCalendarDTO> calendarList = new ArrayList<RetailCalendarDTO>();
		StringBuffer sql = new StringBuffer();
		
		sql.append(" select CALENDAR_ID , START_DATE");
		sql.append(" ,END_DATE");
		sql.append(" from RETAIL_CALENDAR where  ROW_TYPE ='"+rowType+"' ");
		if( methodName.equalsIgnoreCase("BUDGET"))
			sql.append(" and CAL_YEAR=").append(processYear).append("");
		else if( methodName.equalsIgnoreCase("FORECAST")){
			sql.append(" and CAL_YEAR = (Select Cal_Year From Retail_Calendar where ROW_TYPE='");
			sql.append(Constants.CALENDAR_DAY).append("' AND");
			sql.append(" start_date = to_date('" + formatter.format(processYear) + "', 'dd-mm-yy'))");
		}
		
		sql.append(" order by CALENDAR_ID");

		logger.debug("forecastCalendarList Sql" + sql.toString());
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"GetCalendar Id");

			while (result.next()) {
				RetailCalendarDTO dto = new RetailCalendarDTO();
				dto.setCalendarId(result.getInt("CALENDAR_ID"));
				dto.setStartDate(result.getString("START_DATE"));
				dto.setEndDate(result.getString("END_DATE"));
				calendarList.add(dto);
			}
		} catch (Exception gex) {
			logger.error(gex);
			throw new GeneralException("Retail Calendar Error");
		} 
		return calendarList;
	
		 
	}

	/*
	 * Spring method : Method used to get the calendar Id 
	 * Argument 1 : JdbcTemplate
	 * Argument 2 : startDate
	 * Argument 3 : endDate
	 * Argument 4 : calendarMode
	 * @catch SqlException
	 * @throws GendralException
	 */
	
	public List<RetailCalendarDTO> RowTypeBasedCalendarList(Connection _conn,
			Date startDate, Date endDate, String calendarMode) throws GeneralException {
		
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT RC.START_DATE,  RC.CALENDAR_ID AS CALENDAR_ID");
		sql.append(" ,RC.END_DATE  FROM RETAIL_CALENDAR RC");
		sql.append(" WHERE RC.START_DATE >= to_date('"+ formatter.format(startDate) + "','dd-MM-yyyy')");
		if (calendarMode.equalsIgnoreCase("D")) {
			
			sql.append(" and RC.START_DATE <= to_date('"+formatter.format(endDate) + "','dd-MM-yyyy')");
		} else {
			sql.append(" AND RC.END_DATE   <= to_date('"+formatter.format(endDate) + "','dd-MM-yyyy')");
		}
		sql.append(" AND RC.ROW_TYPE ='" + calendarMode + "'");
		
		sql.append(" ORDER BY RC.CALENDAR_ID");
		
		//logger.debug(" Get Calendar List Query ...." + sql.toString());
		
		//  List running method
		List<RetailCalendarDTO> calendarList = new ArrayList<RetailCalendarDTO>();

		try {

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getCalendarList");

			while (result.next()) {

				RetailCalendarDTO dto = new RetailCalendarDTO();
				dto.setCalendarId(result.getInt("CALENDAR_ID"));
				dto.setStartDate(result.getString("START_DATE"));
				dto.setEndDate(result.getString("END_DATE"));

				calendarList.add(dto);
			}

		} catch (Exception exe) {
			throw new GeneralException("Error........." + exe);
		}

		logger.debug("Calendar List Size................." + calendarList.size());

		return calendarList;
	}

	
	
	
	/*
	 * Method used to get the calendarid based on the "ACTUAL NO"
	 * Argument 1 : Connection
	 * Argument 2 : Process Year
	 * Argument 3 : Actual No
	 * Argument 4 : Row Type
	 * @catch Exception
	 * @throws Gendral Exception
	 * 
	 */
	
	public int calendarIdBasedActualNumber(Connection conn, int processYear,
			int actualNo, String rowType, Object higherCalendarId) throws GeneralException {
	 
		int calendarId = 0 ;
		try{
			StringBuffer sql  = new StringBuffer();
			if( actualNo ==0){
				sql.append(" select CALENDAR_ID from RETAIL_CALENDAR");
				sql.append(" where CAL_YEAR=").append(processYear);
				sql.append(" and START_DATE in(");
				sql.append(" select END_DATE from RETAIL_CALENDAR");
				sql.append(" where CALENDAR_ID=").append(higherCalendarId);
				sql.append(") and ROW_TYPE='D'");
				 		
			}
			else{
			sql.append(" select CALENDAR_ID from RETAIL_CALENDAR");
			sql.append(" where CAL_YEAR=").append(processYear);
			sql.append( " and ROW_TYPE='").append(rowType).append("'");
			sql.append(" and ACTUAL_NO=").append(actualNo);
			}
			logger.debug("calendarIdBasedActualNumber" + sql);
			String singleColumn = PristineDBUtil.getSingleColumnVal(conn, sql, "calendarIdBasedActualNumber");
			
			if( singleColumn != null){
				calendarId = Integer.parseInt(singleColumn);
			}
		
		}catch(Exception exe){
			logger.error(" Common Error .......Method Name :calendarIdBasedActualNumber " ,exe);
			throw new GeneralException(" Common Error .......Method Name :calendarIdBasedActualNumber " ,exe);
		}
		
		return calendarId;
	}
	
	
	/*
	 * ****************************************************************
	 * Method used to get the Day Calendar Id 
	 * Argument 1: Connection 
	 * Argument 2: Start Date
	 * Argument 3: End Date
	 * Argument 4: Row Type 
	 * return : List of Calendar Id
	 * @throws GeneralException,SQLException
	 * ****************************************************************
	 */

	public HashMap<Integer, Integer> dayCalendarMap(Connection _Conn,
			Date Startdate, Date Enddate, String rowType) throws GeneralException {

		HashMap<Integer, Integer>  returnMap = new  HashMap<Integer, Integer>();
		StringBuffer sql = new StringBuffer();
		
		sql.append(" select RC.START_DATE,RC.CALENDAR_ID AS CALENDAR_ID");
		sql.append(" ,LRC.CALENDAR_ID as LAST_CALENDAR_ID,RC.END_DATE");
		sql.append(" ,RC.ROW_TYPE FROM  ( select CALENDAR_ID, ACTUAL_NO");
		sql.append(" from RETAIL_CALENDAR where  ROW_TYPE ='"+rowType+"' and");
		sql.append(" CAL_YEAR=to_number(substr('"+formatter.format(Startdate)+"',7) ,'9999')-1) LRC,  RETAIL_CALENDAR RC");
		sql.append(" where RC.START_DATE >= to_date('"
				+ formatter.format(Startdate) + "','dd-MM-yyyy')");
		sql.append(" and RC.START_DATE   <= to_date('"
				+ formatter.format(Enddate) + "','dd-MM-yyyy')");
		sql.append(" and RC.ROW_TYPE      ='" + rowType
				+ "' and LRC.ACTUAL_NO =RC.ACTUAL_NO");
		sql.append(" and RC.CALENDAR_ID!=LRC.CALENDAR_ID");
		sql.append(" order by RC.CALENDAR_ID");

		logger.debug(" Sql" + sql.toString());
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql,
					"GetCalendar Id");

			while (result.next()) {
				
				returnMap.put(result.getInt("CALENDAR_ID"),result.getInt("CALENDAR_ID"));
				
				
			}
		} catch (Exception gex) {
			logger.error(gex.getMessage());
			throw new GeneralException("RetailCalendar Error");
		}
		return returnMap;
	}

	/**
	 * Returns calendar details for the week to which input calendar id belongs to
	 * @param connection		Connection
	 * @param dayCalendarId		Calendar Id for day in a week
	 * @return
	 * @throws GeneralException
	 */
	public RetailCalendarDTO getCalendarIdForWeek(Connection connection, int dayCalendarId) throws GeneralException{
		logger.debug("Inside getCalendarIdForWeek() of RetailCalendarDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    RetailCalendarDTO calendar = new RetailCalendarDTO();
		try{
			statement = connection.prepareStatement(GET_CALENDARID_FOR_WEEK);
	        statement.setInt(1, dayCalendarId);
	        statement.setInt(2, dayCalendarId);
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	calendar.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	calendar.setStartDate(resultSet.getString("START_DATE"));
	        	calendar.setEndDate(resultSet.getString("END_DATE"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_CALENDARID_FOR_WEEK");
			throw new GeneralException("Error while executing GET_CALENDARID_FOR_WEEK", e);
		}finally{
			PristineDBUtil.close(resultSet, statement);
		}
		return calendar;
	}

	/*
	 * Check the calendar id availability in Temp table for repair process
	 * if available means get the min calendar id and max calendar id				 
	 * Argument 1 : Connection
	 * Argument 2 : locationId
	 * Argument 3 : tableName
	 * @throws GeneralException
	 */
	
	public RetailCalendarDTO getRepairCalendarDates(Connection _conn,
			int locationId, int locationLevelId ,String tableName) throws GeneralException {

		logger.debug(" Get the min and max calendar id from temp table for repair process..");

		// return list
		RetailCalendarDTO returnObj = null;

		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" SELECT START_DATE,END_DATE from");

			// get min date
			sql.append(" (select START_DATE as START_DATE from RETAIL_CALENDAR");
			sql.append(" where CALENDAR_ID in (select min(CALENDAR_ID) from ")
					.append(tableName);
			sql.append(" where LOCATION_ID=").append(locationId);
			sql.append(" and LOCATION_LEVEL_ID=").append(locationLevelId);
			sql.append(" )) START_DATE,");

			// get max date
			sql.append(" (select START_DATE as END_DATE from RETAIL_CALENDAR");
			sql.append(" where CALENDAR_ID in (select max(CALENDAR_ID) from ")
					.append(tableName);
			sql.append(" where LOCATION_ID=").append(locationId);
			sql.append(" and LOCATION_LEVEL_ID=").append(locationLevelId);
			sql.append(" )) END_DATE");
			logger.debug(" getRepairCalendarList SQL:" + sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getRepairCalendarList");

			while (result.next()) {
				returnObj = new RetailCalendarDTO();
				returnObj.setStartDate(result.getString("START_DATE"));
				returnObj.setEndDate(result.getString("END_DATE"));
			}
		} catch (SQLException e) {
			logger.error(" Error while fetching the RepairCalendarDates....", e);
			throw new GeneralException(
					" Error while fetching the RepairCalendarDates....");
		}

		return returnObj;
	}
	
	/*
	 *  get the Week/Period/Quarter/Year Calendar id for given date range
	 *  Argument 1 : Connection
	 *  Argument 2 : repairCalendarDates
	 *  Argument 3 : calendarConstant
	 * @throws GeneralException
	 * 
	 */
	
	public List<RetailCalendarDTO> getRepairCalendarList(Connection _conn,
			String repairStartDae ,Date endDate , String calendarConstant) throws GeneralException, ParseException {
		
		logger.debug(" Get the Repair Calendar Id for given date range:");
		List<RetailCalendarDTO> returnList =  new ArrayList<RetailCalendarDTO>();
		
		DateFormat sformatter = new SimpleDateFormat("yyyy-MM-dd");
		Date sDate = (Date) sformatter.parse(repairStartDae);
	 	 
		try {
			StringBuffer sql = new StringBuffer();
					
			sql.append(" select RC.CALENDAR_ID as WEEKCALENDARID,LRC.CALENDAR_ID as LASTCALENDARID");
			sql.append(" from (select CALENDAR_ID,ACTUAL_NO  from RETAIL_CALENDAR");
			sql.append(" where ROW_TYPE ='").append(calendarConstant).append("'");
			sql.append(" and CAL_YEAR = (").append(1900+sDate.getYear() - 1).append(")")
								.append(" ) LRC, RETAIL_CALENDAR RC" );
			sql.append(" where RC.CALENDAR_ID in (select CALENDAR_ID");
			sql.append(" from RETAIL_CALENDAR");
			sql.append(" where (to_date('").append(sformatter.format(sDate))
										  .append("','yyyy-mm-dd')");
		 	sql.append(" between START_DATE and END_DATE");
		 	sql.append(" or to_date('").append(formatter.format(endDate))
		 							  .append("','dd-mm-yyyy')");
		 	sql.append(" between START_DATE AND END_DATE)");
		 	sql.append(" and ROW_TYPE='").append(calendarConstant).append("')");
			sql.append(" and RC.ACTUAL_NO =LRC.ACTUAL_NO and RC.CALENDAR_ID!=LRC.CALENDAR_ID");
			sql.append(" order by RC.CALENDAR_ID");

			logger.debug(" getRepairCalendarId SQL:" + sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getRepairCalendarId");

			while (result.next()) {
				RetailCalendarDTO objCalendarDto = new RetailCalendarDTO();
				objCalendarDto.setCalendarId(result.getInt("WEEKCALENDARID"));
				objCalendarDto.setlstCalendarId(result.getInt("LASTCALENDARID"));
				returnList.add(objCalendarDto);

			}
		} catch (SQLException e) {
			 logger.error(" Error while fetching the getRepairCalendarId ..." , e);
			 throw  new GeneralException(" Error while fetching the getRepairCalendarId ...");
		}
				  
  	return returnList;
	}

	/*
	 * Method used to get the repair day calendar list based on input conidtions
	 * Argument 1 : Connection
	 * Argument 2 : weekLoopSize
	 * Argument 3 : rowNumber
	 * Argument 4 : repairCalendarDates
	 * Argument 5 : processEndDate
	 * Argument 6 : WeekCalendarId
	 * @throw  GeneralException
	 */

	public List<RetailCalendarDTO> getRepairDayCalendarList(Connection _conn,
			int weekLoopSize, int rowNumber, String repairStartDae,
			Date processEndDate, int weekCalendarId) throws GeneralException,
			ParseException {

		List<RetailCalendarDTO> returnList = new ArrayList<RetailCalendarDTO>();

		DateFormat sformatter = new SimpleDateFormat("yyyy-MM-dd");
		Date sDate = (Date) sformatter.parse(repairStartDae);

		try {
			StringBuffer sql = new StringBuffer();
			sql.append(" select CALENDAR_ID,START_DATE,END_DATE from RETAIL_CALENDAR");
			sql.append(" where ROW_TYPE='").append(Constants.CALENDAR_DAY)
					.append("'");
			
			// if repair process need to done for two weeks means 
			// get the start date and end date using following process
			// only thing repair process allow only two weeks / two periods / two quarter /two year
			if (weekLoopSize == 2 && rowNumber == 1) {

				sql.append(
						" and START_DATE >= (select START_DATE from RETAIL_CALENDAR where CALENDAR_ID=")
						.append(weekCalendarId);
				sql.append(" )");

			} else {

				sql.append(" and START_DATE >= to_date('")
						.append(sformatter.format(sDate))
						.append("','yyyy-mm-dd')");
			}

			if (weekLoopSize == 2 && rowNumber == 0) {
				sql.append(
						" and START_DATE <= (select END_DATE from RETAIL_CALENDAR where CALENDAR_ID=")
						.append(weekCalendarId);
				sql.append(" )");
			} else {
				sql.append(" and START_DATE <= to_date('")
						.append(formatter.format(processEndDate))
						.append("','dd-mm-yyyy')");
			}

			logger.debug(" getRepairDayCalendarList SQ:" + sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getRepairDayCalendarList");

			while (result.next()) {
				RetailCalendarDTO objcalDto = new RetailCalendarDTO();
				objcalDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objcalDto.setStartDate(result.getString("START_DATE"));
				objcalDto.setEndDate(result.getString("END_DATE"));
				returnList.add(objcalDto);
			}
		} catch (SQLException e) {
			logger.error(
					" Error while fetching the getRepairDayCalendarList ...", e);
			throw new GeneralException(
					" Error while fetching the getRepairDayCalendarList ...");
		}
		return returnList;
	}
	
	public boolean checkWeekStartDate(Connection _conn, String startDate,
			String calendarConstant) throws ParseException, GeneralException {
		 
		logger.debug(" Check The given date is start date of week/period/quarter/year...");
		boolean returnFlag = false;
		
		DateFormat sformatter = new SimpleDateFormat("yyyy-MM-dd");
		Date sDate = (Date) sformatter.parse(startDate);

		try {
			StringBuffer sql = new StringBuffer();
			
			sql.append(" select CALENDAR_ID from RETAIL_CALENDAR");
			sql.append(" where START_DATE = to_date('")
											.append(sformatter.format(sDate))
											.append("','yyyy-mm-dd')");
			sql.append(" and ROW_TYPE = '").append(calendarConstant).append("'");
			
			logger.debug(" Sql......" + sql.toString());
			
			String result = PristineDBUtil.getSingleColumnVal(_conn, sql, "checkWeekStartDate");
			
			if( result == null || result == ""){
				returnFlag = true;
			}
			else {
				returnFlag  = false;
			}
		} catch (GeneralException e) {
			logger.error(" Error while process checkWeekStartDate method" , e);
			throw new GeneralException(" Error while process checkWeekStartDate method" , e);
		}
			
		return returnFlag;
	}

	public int getLastYearCalendarId(Connection _conn, int calendarId) throws GeneralException, ParseException {
		int lstCalendarId = 0 ;
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE");
		sql.append(" ROW_TYPE='").append(Constants.CALENDAR_DAY).append("'"); 
		sql.append(" AND START_DATE = (SELECT START_DATE-364");
		sql.append(" FROM RETAIL_CALENDAR WHERE CALENDAR_ID =").append(calendarId).append(")") ;
				
		logger.debug("GetLastYearCalendarId SQL: " + sql.toString());
	
		try {
			String singleValue = PristineDBUtil.getSingleColumnVal(_conn, sql, "getLastYearCalendarId");
		
			if( singleValue != null && singleValue != ""){
				lstCalendarId = Integer.parseInt(singleValue);
				logger.debug("Last year calendar Id: "+ lstCalendarId);
			}
			else
				logger.warn("No last year calendar Id");
		} catch (GeneralException e) {
			logger.error("Error while fetching Last year calendar Id" + e.getMessage());
			throw new GeneralException("getLastYearCalendarId" , e);
		}
	
		return lstCalendarId;
	}

	/**
	 * This method retrieves details from retail_calendar table for specified date and calendar mode
	 * @param conn				Connection 
	 * @param startDate			Date of the week
	 * @param calendarMode		Calendar Mode
	 * @return RetailCalendarDTO
	 */
	public RetailCalendarDTO getWeekNo(Connection conn, String weekEndDate) throws GeneralException {
		logger.debug("Inside getWeekNo() of RetailCalendarDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    RetailCalendarDTO calendar = new RetailCalendarDTO();
		try{
			statement = conn.prepareStatement(GET_WEEKNO);
		    statement.setString(1, weekEndDate);
			
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	calendar.setActualNo(resultSet.getInt("ACTUAL_NO"));
	        	calendar.setCalYear(resultSet.getInt("CAL_YEAR"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_WEEKNO");
			throw new GeneralException("Error while executing GET_WEEKNO", e);
		}finally{
			try{
				if(resultSet != null){
					resultSet.close();
				}
				if(statement != null){
					statement.close();
				}
			}catch(SQLException e){
				logger.error("Error closing statement");
				throw new GeneralException("Error closing statement", e);
			}
		}
		return calendar;
	}
	
	/**
	 * Returns Calendar Start and End Date for input calendarId
	 * @param conn
	 * @param calendarId
	 * @return
	 * @throws GeneralException
	 */
	public RetailCalendarDTO getCalendarDetail(Connection conn, int calendarId) throws GeneralException {
		logger.debug("Inside getCalendarDetail() of RetailCalendarDAO");
		PreparedStatement stmt = null;
		ResultSet rs = null;
		RetailCalendarDTO calendar = null;
		try{
			stmt = conn.prepareStatement(GET_CALENDAR_DETAILS);
			stmt.setInt(1, calendarId);
			rs = stmt.executeQuery();
			if(rs.next()){
				calendar = new RetailCalendarDTO();
				calendar.setCalendarId(rs.getInt("CALENDAR_ID"));
				calendar.setStartDate(rs.getString("START_DATE"));
				if(rs.getString("END_DATE") != null)
					calendar.setEndDate(rs.getString("END_DATE"));
				else
					calendar.setEndDate(rs.getString("START_DATE"));
			}
		}catch(SQLException e){
			logger.error("Error when retrieving calendar details - " + e);
			throw new GeneralException("Error when retrieving calendar details - " + e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return calendar;
	}
	
	/**
	 * Returns list of calendar Ids for noOfWeeksBehind the input calendarId
	 * @param conn
	 * @param calendarId
	 * @param noOfWeeksBehind
	 * @return
	 * @throws GeneralException
	 */
	public List<RetailCalendarDTO> getCalendarList(Connection conn, int calendarId, int noOfWeeksBehind) throws GeneralException {
		logger.debug("Inside getCalendarDetail() of RetailCalendarDAO");
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ArrayList<RetailCalendarDTO> calendarList = new ArrayList<RetailCalendarDTO>();
		try{
			stmt = conn.prepareStatement(GET_CALENDAR_LIST_FOR_WEEKS);
			stmt.setInt(1, noOfWeeksBehind);
			stmt.setInt(2, calendarId);
			stmt.setInt(3, calendarId);
			rs = stmt.executeQuery();
			while(rs.next()){
				RetailCalendarDTO rcDto = new RetailCalendarDTO();
				rcDto.setCalendarId(rs.getInt("CALENDAR_ID"));
				rcDto.setStartDate(rs.getString("START_DATE"));
				rcDto.setEndDate(rs.getString("END_DATE"));
				calendarList.add(rcDto);
			}
		}catch(SQLException e){
			logger.error("Error when retrieving calendar details - " + e);
			throw new GeneralException("Error when retrieving calendar details - " + e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return calendarList;
	}
	
	
	/**
	 * Returns list of calendar Ids for noOfWeeksBehind the input calendarId
	 * @param conn
	 * @param calendarId
	 * @param noOfWeeksInFuture
	 * @return
	 * @throws GeneralException
	 */
	public List<RetailCalendarDTO> getFutureCalendarList(Connection conn, int calendarId, int noOfWeeksInFuture) throws GeneralException {
		logger.debug("Inside getCalendarDetail() of RetailCalendarDAO");
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ArrayList<RetailCalendarDTO> calendarList = new ArrayList<RetailCalendarDTO>();
		try{
			stmt = conn.prepareStatement(GET_FUTURE_CALENDAR_LIST_FOR_WEEKS);
			stmt.setInt(1, noOfWeeksInFuture);
			stmt.setInt(2, calendarId);
			stmt.setInt(3, calendarId);
			rs = stmt.executeQuery();
			while(rs.next()){
				RetailCalendarDTO rcDto = new RetailCalendarDTO();
				rcDto.setCalendarId(rs.getInt("CALENDAR_ID"));
				rcDto.setStartDate(rs.getString("START_DATE"));
				rcDto.setEndDate(rs.getString("END_DATE"));
				calendarList.add(rcDto);
			}
		}catch(SQLException e){
			logger.error("Error when retrieving calendar details - " + e);
			throw new GeneralException("Error when retrieving calendar details - " + e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return calendarList;
	}
	public RetailCalendarDTO getPeriodDetail(Connection conn, String date) throws GeneralException {
		logger.debug("Inside RetailCalendarDTO() of RetailCalendarDAO");
		PreparedStatement stmt = null;
		ResultSet rs = null;
		RetailCalendarDTO calendar = null;
		try{
			stmt = conn.prepareStatement(GET_PERIOD);
			stmt.setString(1, date);
			stmt.setString(2, date);
			rs = stmt.executeQuery();
			if(rs.next()){
				calendar = new RetailCalendarDTO();
				calendar.setCalendarId(rs.getInt("CALENDAR_ID"));
				calendar.setStartDate(rs.getString("START_DATE"));
				if(rs.getString("END_DATE") != null)
					calendar.setEndDate(rs.getString("END_DATE"));
				else
					calendar.setEndDate(rs.getString("START_DATE"));
			}
		}catch(SQLException e){
			logger.error("Error when retrieving calendar details - " + e);
			throw new GeneralException("Error when retrieving calendar details - " + e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return calendar;
	}
	
	/***
	 * Get the week in the which the day falls
	 * @param conn
	 * @param inputDate
	 * @return
	 * @throws GeneralException
	 */
	public RetailCalendarDTO getWeekCalendarFromDate(Connection conn, String date) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		RetailCalendarDTO calendar = null;
		try{
			stmt = conn.prepareStatement(GET_WEEK_CALENDAR_FROM_DATE);
			stmt.setString(1, date);
			stmt.setString(2, date);
			rs = stmt.executeQuery();
			if(rs.next()){
				calendar = new RetailCalendarDTO();
				calendar.setCalendarId(rs.getInt("CALENDAR_ID"));
				calendar.setStartDate(rs.getString("START_DATE"));
				if(rs.getString("END_DATE") != null)
					calendar.setEndDate(rs.getString("END_DATE"));
				else
					calendar.setEndDate(rs.getString("START_DATE"));
			}
		}catch(SQLException e){
			logger.error("Error in getWeekCalendarFromDate() - " + e);
			throw new GeneralException("Error when retrieving calendar details - " + e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return calendar;
	}
	
	public LinkedHashMap<Integer, RetailCalendarDTO> getAllPreviousWeeks(Connection conn, String date, int noOfWeeks) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		RetailCalendarDTO calendar = null;
		LinkedHashMap<Integer, RetailCalendarDTO> previuosCalendarDetails = new LinkedHashMap<Integer, RetailCalendarDTO>();
		try {
			stmt = conn.prepareStatement(GET_ALL_PREVIOUS_WEEKS);
			stmt.setString(1, date);
			stmt.setString(2, date);
			stmt.setInt(3, noOfWeeks);
			rs = stmt.executeQuery();
			while (rs.next()) {
				calendar = new RetailCalendarDTO();
				calendar.setCalendarId(rs.getInt("CALENDAR_ID"));
				calendar.setStartDate(rs.getString("START_DATE"));
				calendar.setEndDate(rs.getString("END_DATE"));
				previuosCalendarDetails.put(calendar.getCalendarId(), calendar);
			}
		} catch (SQLException e) {
			logger.error("Error in getAllPreviousWeeks() - " + e);
			throw new GeneralException("Error in getAllPreviousWeeks() - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return previuosCalendarDetails;
	}
	
	public LinkedHashMap<Integer, RetailCalendarDTO> getAllFutureWeeks(Connection conn, String date, int noOfWeeks) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		RetailCalendarDTO calendar = null;
		LinkedHashMap<Integer, RetailCalendarDTO> previuosCalendarDetails = new LinkedHashMap<Integer, RetailCalendarDTO>();
		try {
			stmt = conn.prepareStatement(GET_ALL_FUTURE_WEEKS);
			stmt.setString(1, date);
			stmt.setString(2, date);
			stmt.setInt(3, noOfWeeks);
			rs = stmt.executeQuery();
			while (rs.next()) {
				calendar = new RetailCalendarDTO();
				calendar.setCalendarId(rs.getInt("CALENDAR_ID"));
				calendar.setStartDate(rs.getString("START_DATE"));
				calendar.setEndDate(rs.getString("END_DATE"));
				previuosCalendarDetails.put(calendar.getCalendarId(), calendar);
			}
		} catch (SQLException e) {
			logger.error("Error in getAllFutureWeeks() - " + e);
			throw new GeneralException("Error in getAllFutureWeeks() - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return previuosCalendarDetails;
	}
	
	public HashMap<String, RetailCalendarDTO> getAllWeeks(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		RetailCalendarDTO calendar = null;
		HashMap<String, RetailCalendarDTO> previuosCalendarDetails = new HashMap<String, RetailCalendarDTO>();
		try {
			stmt = conn.prepareStatement(GET_ALL_WEEKS);
			rs = stmt.executeQuery();
			while (rs.next()) {
				calendar = new RetailCalendarDTO();
				calendar.setCalendarId(rs.getInt("CALENDAR_ID"));
				calendar.setStartDate(rs.getString("START_DATE"));
				calendar.setEndDate(rs.getString("END_DATE"));
				previuosCalendarDetails.put(calendar.getStartDate(), calendar);
				previuosCalendarDetails.put(calendar.getEndDate(), calendar);
			}
		} catch (SQLException e) {
			logger.error("Error in getAllWeeks() - " + e);
			throw new GeneralException("Error in getAllWeeks() - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return previuosCalendarDetails;
	}
	
	public List<RetailCalendarDTO> getFullRetailCalendar(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		RetailCalendarDTO calendar = null;
		List<RetailCalendarDTO> calendarDetails = new ArrayList<RetailCalendarDTO>();
		try {
			String calType = PropertyManager.getProperty("RETAIL_CALENDAR_TYPE", Constants.RETAIL_CALENDAR_BUSINESS);
			if (calType.equals(Constants.RETAIL_CALENDAR_PROMO)) {
				stmt = conn.prepareStatement(GET_FULL_RETAIL_CALENDAR_PROMO);
			} else {
				stmt = conn.prepareStatement(GET_FULL_RETAIL_CALENDAR);
			}

			stmt.setFetchSize(100000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				calendar = new RetailCalendarDTO();
				calendar.setCalendarId(rs.getInt("CALENDAR_ID"));
				calendar.setStartDate(rs.getString("START_DATE"));
				calendar.setEndDate(rs.getString("END_DATE"));
				calendar.setWeekNo(rs.getInt("WEEK_NO"));
				calendar.setSpecialDayId(rs.getInt("SPECIAL_DAY_ID"));
				calendar.setRowType(rs.getString("ROW_TYPE"));
				calendar.setCalYear(rs.getInt("CAL_YEAR"));
				calendar.setActualNo(rs.getInt("ACTUAL_NO"));
				calendarDetails.add(calendar);
			}
		} catch (SQLException e) {
			logger.error("Error in getFullRetailCalendar() - " + e);
			throw new GeneralException("Error in getFullRetailCalendar() - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return calendarDetails;
	}
	
	public String getMonthStartDate(Connection conn, String processingDate) throws GeneralException{
		String date = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_MONTH_START_DATE_BASED_ON_GIVEN_DATE);
			stmt.setFetchSize(100000);
			stmt.setString(1, processingDate);
			stmt.setString(2, processingDate);
			rs = stmt.executeQuery();
			while (rs.next()) {
				date =rs.getString("START_DATE");
			}
		} catch (SQLException e) {
			logger.error("Error in getMonthStartDate() - " + e);
			throw new GeneralException("Error in getMonthStartDate() - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return date;
	}
	
	/**
	 * 
	 * @param conn
	 * @param date
	 * @return quarter details
	 * @throws GeneralException
	 */
	public RetailCalendarDTO getQuarterForDate(Connection conn, 
			String date) 
			throws GeneralException{
		RetailCalendarDTO retailCalendarDTO = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_QUARTER_DETAILS);
			stmt.setFetchSize(100000);
			stmt.setString(1, date);
			stmt.setString(2, date);
			rs = stmt.executeQuery();
			if (rs.next()) {
				retailCalendarDTO = new RetailCalendarDTO();
				retailCalendarDTO.setCalendarId(rs.getInt("CALENDAR_ID"));
				retailCalendarDTO.setStartDate(rs.getString("START_DATE"));
				retailCalendarDTO.setEndDate(rs.getString("END_DATE"));
				retailCalendarDTO.setWeekNo(rs.getInt("WEEK_NO"));
				retailCalendarDTO.setSpecialDayId(rs.getInt("SPECIAL_DAY_ID"));
				retailCalendarDTO.setRowType(rs.getString("ROW_TYPE"));
				retailCalendarDTO.setCalYear(rs.getInt("CAL_YEAR"));
				retailCalendarDTO.setActualNo(rs.getInt("ACTUAL_NO")); 
			}
		} catch (SQLException e) {
			logger.error("Error in getQuarterForDate() - " + e);
			throw new GeneralException("Error in getQuarterForDate() - " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		return retailCalendarDTO;
		
	}
	
	public  HashMap<String, RetailCalendarDTO> getEverydayAndItsWeekCalendarId(Connection conn) throws GeneralException {
		logger.debug("Inside getCalendarId() of RetailCalendarDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    HashMap<String, RetailCalendarDTO> everydayAndRespWeekCalId = new HashMap<>();
		try{
				statement = conn.prepareStatement(GET_EVERYDAY_DATE_AND_RESPECTIVE_WEEK_CALENDAR_ID);
		        statement.setString(1, Constants.CALENDAR_WEEK);
		        statement.setString(2, Constants.CALENDAR_DAY);
	        resultSet = statement.executeQuery();
	        while (resultSet.next()) {
	        	RetailCalendarDTO calendar = new RetailCalendarDTO();
	        	calendar.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	calendar.setStartDate(resultSet.getString("START_DATE"));
	        	everydayAndRespWeekCalId.put(calendar.getStartDate(), calendar);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getEverydayAndItsWeekCalendarId");
			throw new GeneralException("Error while executing getEverydayAndItsWeekCalendarId", e);
		}finally{
			try{
				if(resultSet != null){
					resultSet.close();
				}
				if(statement != null){
					statement.close();
				}
			}catch(SQLException e){
				logger.error("Error closing statement");
				throw new GeneralException("Error closing statement", e);
			}
		}
		return everydayAndRespWeekCalId;
	}
	
	/**
	 * This method retrieves details from retail_calendar_promo table for specified date and calendar mode
	 * @param conn				Connection 
	 * @param startDate			Date of the week
	 * @param calendarMode		Calendar Mode
	 * @return RetailCalendarDTO
	 */
	public RetailCalendarDTO getPromoCalendarId(Connection conn, String startDate,
			String calendarMode) throws GeneralException {
		logger.debug("Inside getCalendarId() of RetailCalendarDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    RetailCalendarDTO calendar = new RetailCalendarDTO();
		try{
			if(Constants.CALENDAR_DAY.equals(calendarMode)){
				statement = conn.prepareStatement(GET_PROMO_CALENDAR_INFO_FOR_DAY);
		        statement.setString(1, startDate);
		        statement.setString(2, calendarMode);
			}else{
				statement = conn.prepareStatement(GET_PROMO_CALENDAR_INFO);
		        statement.setString(1, startDate);
		        statement.setString(2, startDate);
		        statement.setString(3, calendarMode);
			}
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	calendar.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	calendar.setStartDate(resultSet.getString("START_DATE"));
	        	calendar.setEndDate(resultSet.getString("END_DATE"));
	        	calendar.setlstCalendarId(resultSet.getInt("LST_CALENDAR_ID"));
	        	calendar.setcalendarMode(calendarMode);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getPromoCalendarId");
			throw new GeneralException("Error while executing getPromoCalendarId", e);
		}finally{
			try{
				if(resultSet != null){
					resultSet.close();
				}
				if(statement != null){
					statement.close();
				}
			}catch(SQLException e){
				logger.error("Error closing statement");
				throw new GeneralException("Error closing statement", e);
			}
		}
		return calendar;
	}
	
	
	
	/**
	 * This method retrieves details from retail_calendar table for specified date and calendar mode
	 * @param conn				Connection 
	 * @param startDate			Date of the week
	 * @param calendarMode		Calendar Mode
	 * @return RetailCalendarDTO
	 */
	public RetailCalendarDTO getPromocalId(Connection conn, String startDate,
			String calendarMode) throws GeneralException {
		logger.debug("Inside getPromocalId() of RetailCalendarDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    RetailCalendarDTO calendar = new RetailCalendarDTO();
		
		try{
			if(Constants.CALENDAR_DAY.equals(calendarMode)){
				statement = conn.prepareStatement(GET_PR_CALENDAR_INFO_FOR_DAY);
		        statement.setString(1, startDate);
		        statement.setString(2, calendarMode);
			}else{
			
				statement = conn.prepareStatement(GET_PR_CALENDAR_INFO);
		        statement.setString(1, startDate);
		        statement.setString(2, startDate);
		        statement.setString(3, calendarMode);
			}
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	calendar.setCalendarId(resultSet.getInt("CALENDAR_ID"));
	        	calendar.setStartDate(resultSet.getString("START_DATE"));
	        	calendar.setEndDate(resultSet.getString("END_DATE"));
	        	calendar.setlstCalendarId(resultSet.getInt("LST_CALENDAR_ID"));
	        	calendar.setcalendarMode(calendarMode);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_CALENDAR_ID");
			throw new GeneralException("Error while executing GET_CALENDAR_ID", e);
		}finally{
			try{
				if(resultSet != null){
					resultSet.close();
				}
				if(statement != null){
					statement.close();
				}
			}catch(SQLException e){
				logger.error("Error closing statement");
				throw new GeneralException("Error closing statement", e);
			}
		}
		return calendar;
	}

	public String getCalendarType(Connection conn, NelsonMarketDataDTO nelsonMarketDataDTO)
			throws GeneralException {
		{
			String rowType="";
			int calId=0;
			PreparedStatement stmt = null;
			ResultSet rs = null;
			try {
				
				stmt = conn.prepareStatement(GET_RETAIL_CALENDAR);
				stmt.setString(1, nelsonMarketDataDTO.getStartDate());
				stmt.setString(2, nelsonMarketDataDTO.getEndDate());
				

				rs = stmt.executeQuery();
				if (rs.next()) {

					rowType=((rs.getString("ROW_TYPE")));
					calId=rs.getInt("CALENDAR_ID");
				} 

			} catch (SQLException e) {
				logger.error("Error when inserting marketData - " + e);
				throw new GeneralException("Error in insertmarketData", e);
			} finally {
				PristineDBUtil.close(stmt);
			}

			return rowType + ";" + calId ;
		}

	}
	public String  getXweekCalId(Connection conn, int noOfweeks,
			int  calId) throws GeneralException {
		
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    String date ="";
	    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");  
		try {
				statement = conn.prepareStatement(GET_X_WEEKS_CALENDAR_ID);
		        statement.setInt(1, noOfweeks);
		        statement.setInt(2, calId);
		        statement.setInt(3, calId);
			
		        
	        resultSet = statement.executeQuery();
	        if(resultSet.next()){
	        	Date dt=resultSet.getDate("END_DATE");
	        	date= dateFormat.format(dt);
	        }
	      
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getXweekCalId");
			throw new GeneralException("Error while executing getXweekCalId", e);
		}finally{
			try{
				if(resultSet != null){
					resultSet.close();
				}
				if(statement != null){
					statement.close();
				}
			}catch(SQLException e){
				logger.error("Error closing statement");
				throw new GeneralException("Error closing statement", e);
			}
		}
		
		
		return date;
	}
	
	
	
}
