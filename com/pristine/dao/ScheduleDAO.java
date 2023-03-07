
package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.dto.VariationAnalysisDto;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ScheduleDAO implements IDAO
{

	static Logger	logger	= Logger.getLogger("com.pristine.dao.scheduleDAO");

	// Price Index Support By Price Zone
	private static final String GET_SCHEDULE_ID_FOR_ZONE = "SELECT SCHEDULE_ID FROM SCHEDULE WHERE " +
			  "PRICE_ZONE_ID = ? AND START_DATE = TO_DATE (?, 'MM/dd/yyyy') " +
			  "AND END_DATE = TO_DATE (?, 'MM/dd/yyyy') AND PRICE_CHECK_LIST_ID = ?";
	
	// Price Index Support By Price CHAIN
	private static final String GET_SCHEDULE_ID_FOR_CHAIN = "SELECT SCHEDULE_ID FROM SCHEDULE WHERE " +
			  "LOCATION_LEVEL_ID = ? AND LOCATION_ID = ? AND START_DATE = TO_DATE (?, 'MM/dd/yyyy') " +
			  "AND END_DATE = TO_DATE (?, 'MM/dd/yyyy') AND PRICE_CHECK_LIST_ID = ?";
	
	private static final String INSERT_SCHEDULE_ID_FOR_ZONE = "INSERT INTO SCHEDULE( SCHEDULE_ID, PRICE_CHECK_LIST_ID, PRICE_CHECKER_ID, " +
			     "CREATE_USER_ID, CREATE_DATETIME, PRICE_ZONE_ID, START_DATE, END_DATE, " +
			     "CURRENT_STATUS, STATUS_CHG_DATE, NO_OF_ITEMS, CHECK_DATE ) VALUES " +
			     "(SCHEDULE_SEQ.NEXTVAL, (SELECT ID FROM PRICE_CHECK_LIST WHERE NAME = ?), " +
			     "?, ?, SYSDATE, ?, TO_DATE(?, 'MM/dd/yyyy'), TO_DATE(?, 'MM/dd/yyyy'), " +
			     "2, SYSDATE, -1, SYSDATE)";
	
	private static final String INSERT_SCHEDULE_ID_FOR_CHAIN = "INSERT INTO SCHEDULE( SCHEDULE_ID, PRICE_CHECK_LIST_ID, PRICE_CHECKER_ID, " +
		     "CREATE_USER_ID, CREATE_DATETIME, LOCATION_LEVEL_ID, LOCATION_ID, START_DATE, END_DATE, " +
		     "CURRENT_STATUS, STATUS_CHG_DATE, NO_OF_ITEMS, CHECK_DATE ) VALUES " +
		     "(SCHEDULE_SEQ.NEXTVAL, (SELECT ID FROM PRICE_CHECK_LIST WHERE NAME = ?), " +
		     "?, ?, SYSDATE, ?, ?, TO_DATE(?, 'MM/dd/yyyy'), TO_DATE(?, 'MM/dd/yyyy'), " +
		     "2, SYSDATE, -1, SYSDATE)";
	
	// Price Index Support By Price Zone - Ends

	private static final String GET_SCHEDULES_FOR_NON_SUBSCRIBER = "SELECT SCHEDULE_ID FROM SCHEDULE WHERE CHECK_DATE >= TO_DATE(?, 'MM/DD/YYYY') AND CHECK_DATE <= TO_DATE(?, 'MM/DD/YYYY') " +
							" AND COMP_STR_ID NOT IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y'))";
	
	private static final String GET_SCHEDULES_FOR_THE_DAY = "SELECT S.SCHEDULE_ID, CS.NAME COMP_NAME FROM SCHEDULE S, COMPETITOR_STORE CS "
			+ " WHERE CS.COMP_STR_ID = S.COMP_STR_ID AND S.START_DATE <= TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "') "
					+ " AND S.END_DATE >= TO_DATE(?, '" + Constants.DB_DATE_FORMAT + "') "; 
	
	
	
	
	public ScheduleInfoDTO getScheduleInfo (Connection connection, int scheduleID) throws GeneralException
	{
		ScheduleInfoDTO schInfo = null;

		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();

		// Fill in the query
		sb.append(" SELECT schedule_id, TO_CHAR(start_date, 'MM/DD/YYYY') START_DATE, ");
		sb.append(" TO_CHAR(end_date, 'MM/DD/YYYY') END_DATE, TO_CHAR(STATUS_CHG_DATE, 'MM/DD/YYYY') STATUS_CHG_DATE, " );
		sb.append("  comp_str_id ");
		sb.append(" From schedule WHERE schedule_id = ");
		sb.append(scheduleID);
		logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(connection, sb, "scheduleDAO - getScheduleDetails");
		try
		{
			if (crs.next())
			{
				schInfo = new ScheduleInfoDTO();
				schInfo.setScheduleId(scheduleID);
				schInfo.setStoreId(crs.getInt("comp_str_id"));
				schInfo.setStartDate(crs.getString("START_DATE"));
				schInfo.setEndDate(crs.getString("END_DATE"));
				schInfo.setStatusChangeDate(crs.getString("STATUS_CHG_DATE"));
			}
		}
		catch (SQLException sqle)
		{
			throw new GeneralException(" Exception in reading cached row set ", sqle);
		}
		return schInfo;
	}

	
	public CachedRowSet getScheduleForLocation(Connection connection, int locationId, int daysToRetain, int processLevelTypeId ) throws GeneralException
	{
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		
		sb.append("SELECT schedule_id from schedule  where");
		if(processLevelTypeId == Constants.ZONE_LEVEL_TYPE_ID)
			sb.append(" price_zone_id =").append(locationId);
		else if(processLevelTypeId == Constants.CHAIN_LEVEL_TYPE_ID){
			sb.append(" LOCATION_LEVEL_ID = ").append(Constants.CHAIN_LEVEL_ID);
			sb.append(" AND LOCATION_ID = ").append(locationId);
		}
		else
			sb.append(" comp_str_id =").append(locationId);
		
		// Changes to include FULL/PARTIAL scenario in Competitive Data PI Setup - Starts
		if(daysToRetain > 0){
			sb.append(" and end_date < sysdate - interval ");
			sb.append("'" + daysToRetain + "' DAY");
		}
		// Changes to include FULL/PARTIAL scenario in Competitive Data PI Setup - Ends
		logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(connection, sb, "schedule DAO- getSchedulesForStore");
		return crs;
	}
	
	@Deprecated public ScheduleInfoDTO getScheduleDetails(Connection connection, int scheduleID) throws GeneralException
	{
		ScheduleInfoDTO schInfo = null;

		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();

		// Fill in the query
		sb.append(" SELECT schedule_id, comp_str_id, TO_CHAR(start_date, 'MM/DD/YYYY') START_DATE, ");
		sb.append(" TO_CHAR(end_date, 'MM/DD/YYYY') END_DATE, TO_CHAR(STATUS_CHG_DATE, 'MM/DD/YYYY') STATUS_CHG_DATE, nvl( WEEK_THRESHOLD, -1) WEEK_THRESHOLD, comp_str_id, comp_chain_name ");
		sb.append(" From performance_stat_view  WHERE schedule_id = ");
		sb.append(scheduleID);
		logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(connection, sb, "scheduleDAO - getScheduleDetails");
		try
		{
			if (crs.next())
			{
				schInfo = new ScheduleInfoDTO();
				schInfo.setScheduleId(scheduleID);
				schInfo.setChainId(crs.getInt("comp_chain_id"));
				schInfo.setStoreId(crs.getInt("comp_str_id"));
				schInfo.setWeekThreshold(crs.getInt("WEEK_THRESHOLD"));
				schInfo.setStartDate(crs.getString("START_DATE"));
				schInfo.setEndDate(crs.getString("END_DATE"));
				schInfo.setStatusChangeDate(crs.getString("STATUS_CHG_DATE"));
				schInfo.setChainName(crs.getString("COMP_CHAIN_NAME"));

				if (schInfo.getWeekThreshold() < 0)
					schInfo.setWeekThreshold(Integer.parseInt(PropertyManager.getProperty("DEFAULT_WEEK_THRESHOLD", "0")));

			}
		}
		catch (SQLException sqle)
		{
			throw new GeneralException(" Exception in reading cached row set ", sqle);
		}
		return schInfo;
	}

	public CachedRowSet getSchedulesForChain(Connection connection, int noOfDaysThreshold, int chainId, 
											 int excludeStoreId, String fromDate, 
											 boolean forVariationAnalysis) throws GeneralException
	{
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();

		sb.append("SELECT schedule_id, TO_CHAR( status_chg_date, 'MM/DD/YYYY' ) CHECK_DATE from performance_stat_view where");
		sb.append(" comp_str_id in (");
		sb.append(" SELECT comp_str_id from COMPETITOR_STORE WHERE COMP_CHAIN_ID = ");
		sb.append(chainId);
		sb.append(" AND COMP_STR_ID <> ").append(excludeStoreId).append(")");
		sb.append(" and current_status in (");
		sb.append(Constants.COMPLETED).append(',');
		sb.append(Constants.PARTIALLY_COMPLETED).append(')');
		sb.append(" and (GPS_VIOLATION is NULL or GPS_VIOLATION = 'N')");
		sb.append(" and status_chg_date >= TO_DATE(' ");
		sb.append(fromDate).append("', 'MM/DD/YYYY') - ");
		sb.append(noOfDaysThreshold);
		sb.append(" and status_chg_date <= TO_DATE(' ");
		sb.append(fromDate).append("', 'MM/DD/YYYY')");
		if ( forVariationAnalysis ){
			sb.append(" AND ENABLE_VAR_ANALYSIS = 'Y'" );
		}
		logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(connection, sb, "schedule DAO- getSchedulesForStore");
		return crs;
	}

	// getScheduleDetails passing a scheduleId - include StatusDate, StoreId,
	// ChainId, StartDate, EndDate, Status
	// getChainWeeksThreshold - return 1, Alter table

	public CachedRowSet getSchedulesForChain(Connection connection, int chainId, int excludeStoreId, String analysisStartDate, String analysisEndDate) throws GeneralException
	{
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();

		sb.append("SELECT schedule_id from SCHEDULE where");
		sb.append(" comp_str_id in (");
		sb.append(" SELECT comp_str_id from COMPETITOR_STORE WHERE COMP_CHAIN_ID = ");
		sb.append(chainId);
		sb.append(" AND COMP_STR_ID <> ").append(excludeStoreId).append(")");
		sb.append(" and current_status in (");
		sb.append(Constants.COMPLETED).append(',');
		sb.append(Constants.PARTIALLY_COMPLETED).append(')');
		sb.append(" and start_date = TO_DATE( '");
		sb.append(analysisStartDate);
		sb.append("', 'MM/DD/YYYY') ");
		sb.append(" and end_date = TO_DATE( '");
		sb.append(analysisEndDate);
		sb.append("', 'MM/DD/YYYY') ");

		logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(connection, sb, "schedule DAO - getSchedulesForChain v2 ");
		return crs;
	}

	public ArrayList<VariationAnalysisDto> getSchedulesForPriceSuggestion(String storeId) throws GeneralException
	{

		StringBuffer sb = new StringBuffer();
		ArrayList<VariationAnalysisDto> scheduleList = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		Connection con = null;

		try
		{
			scheduleList = new ArrayList<VariationAnalysisDto>();

			con = DBManager.getOracleConnection();

			sb.append(" SELECT sc.schedule_id,sc.schedule_id ||' - ' ||");
			sb.append(" u.first_name ||' '||u.last_name  ||' - ' || ");
			sb.append(" To_char(sc.status_chg_date,'MM/dd/yyyy') AS SCHEDULE_NAME");
			sb.append(" FROM schedule sc , user_details u WHERE comp_str_id=" + storeId);
			sb.append(" AND sc.price_checker_id=u.user_id ");
			sb.append(" AND status_chg_date > SYSDATE -60");
			sb.append(" ORDER BY schedule_id");

			preparedStatement = con.prepareStatement(sb.toString());

			rs = preparedStatement.executeQuery(sb.toString());

			while (rs.next())
			{
				VariationAnalysisDto schedule = new VariationAnalysisDto();
				schedule.setScheduleId(rs.getString("schedule_id"));
				schedule.setScheduleName(rs.getString("SCHEDULE_NAME"));
				scheduleList.add(schedule);
			}

			logger.info(sb);
			// execute the statement

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}finally
		{
			PristineDBUtil.close(rs, preparedStatement, con);
		}

		return scheduleList;
	}

	public String getLastCheckDate(Connection conn, int scheduleId) throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append("select max(To_CHAR(STATUS_CHG_DATE, 'MM/DD/YYYY')) as LAST_CHECK_DATE from SCHEDULE where SCHEDULE_ID < ");
		sb.append(scheduleId);
		sb.append("and comp_str_id = (Select comp_str_id from SCHEDULE where schedule_id = ");
		sb.append(scheduleId).append(')');
		return PristineDBUtil.getSingleColumnVal(conn,   sb, "ScheduleDAO - getLastCheckDate");
		
	}
	
	public ArrayList <ScheduleInfoDTO> getSchedulesForStore(Connection connection, int storeId, int checkListId, 
			String startDate, String endDate)throws GeneralException {
		
		ScheduleInfoDTO schInfo = null;
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();

		sb.append(" SELECT schedule_id, comp_chain_id, TO_CHAR(start_date, 'MM/DD/YYYY') START_DATE, ");
		sb.append(" TO_CHAR(end_date, 'MM/DD/YYYY') END_DATE, TO_CHAR(STATUS_CHG_DATE, 'MM/DD/YYYY') STATUS_CHG_DATE, ");
		sb.append(" A.comp_str_id, B.name, comp_str_no ");
		sb.append(" From schedule A,  competitor_store B WHERE A.comp_str_id = ");
		sb.append(storeId);
		sb.append(" and A.comp_str_id = B.comp_str_id ");
		if( checkListId > 0)
			sb.append(" and PRICE_CHECK_LIST_ID = ").append(checkListId);
		sb.append(" and start_date >= ").append("to_date('").append(startDate).append("','MM/DD/YY')");
		sb.append(" and end_date <= ").append("to_date('").append(endDate).append("','MM/DD/YY')");
		sb.append(" order by start_date desc");
		//logger.debug(sb.toString());
		crs = PristineDBUtil.executeQuery(connection, sb, "getSchedulesForStore");
		ArrayList <ScheduleInfoDTO> schList = new ArrayList <ScheduleInfoDTO> ();
		try {
			while (crs.next()){
				schInfo = new ScheduleInfoDTO();
				schInfo.setScheduleId(crs.getInt("schedule_id"));
				schInfo.setChainId(crs.getInt("comp_chain_id"));
				schInfo.setStoreId(crs.getInt("comp_str_id"));
				schInfo.setStartDate(crs.getString("START_DATE"));
				schInfo.setEndDate(crs.getString("END_DATE"));
				schInfo.setStatusChangeDate(crs.getString("STATUS_CHG_DATE"));
				//schInfo.setChainName(crs.getString("COMP_CHAIN_NAME"));
				schInfo.setStoreName(crs.getString("NAME"));
				schInfo.setStoreNum(crs.getString("comp_str_no"));
				schList.add(schInfo);
			}
		}catch (SQLException sqlce){
			throw new GeneralException( "Accessing cached Rowset exception", sqlce);
		}
		
		return schList;

	}
	
	public ArrayList <ScheduleInfoDTO> getSchedulesForZone(Connection connection, int zoneId, int checkListId, 
			String startDate, String endDate)throws GeneralException {
		
		ScheduleInfoDTO schInfo = null;
		StringBuffer sb = new StringBuffer();

		sb.append(" SELECT schedule_id ");
		sb.append(" From schedule where price_zone_id = ?");
		if( checkListId > 0)
			sb.append(" and PRICE_CHECK_LIST_ID = ").append(checkListId);
		sb.append(" and start_date >= to_date(?,'MM/DD/YY')");
		sb.append(" and end_date <= to_date(?,'MM/DD/YY')");
		sb.append(" order by start_date desc");

		ArrayList <ScheduleInfoDTO> schList = new ArrayList <ScheduleInfoDTO> ();
		PreparedStatement statement = null;
		ResultSet rs = null;
		try {
			statement = connection.prepareStatement(sb.toString());
			statement.setInt(1, zoneId);
			statement.setString(2, startDate);
			statement.setString(3, endDate);
			rs = statement.executeQuery();
			while (rs.next()){
				schInfo = new ScheduleInfoDTO();
				schInfo.setScheduleId(rs.getInt("schedule_id"));
				schList.add(schInfo);
			}
		}catch (SQLException sqlce){
			throw new GeneralException( "Accessing cached Rowset exception", sqlce);
		}finally{
			PristineDBUtil.close(statement);
		}
		
		return schList;

	}
	
	
	
	public ArrayList <ScheduleInfoDTO> getSchedulesForChainV2(Connection connection, int locationLevelId, int chainId, int checkListId, 
			String startDate, String endDate)throws GeneralException {
		
		ScheduleInfoDTO schInfo = null;
		StringBuffer sb = new StringBuffer();

		sb.append(" SELECT schedule_id ");
		sb.append(" From schedule where location_id = ? AND location_level_id = " + locationLevelId);
		if( checkListId > 0)
			sb.append(" and PRICE_CHECK_LIST_ID = ").append(checkListId);
		sb.append(" and start_date >= to_date(?,'MM/DD/YY')");
		sb.append(" and end_date <= to_date(?,'MM/DD/YY')");
		sb.append(" order by start_date desc");

		ArrayList <ScheduleInfoDTO> schList = new ArrayList <ScheduleInfoDTO> ();
		PreparedStatement statement = null;
		ResultSet rs = null;
		try {
			statement = connection.prepareStatement(sb.toString());
			statement.setInt(1, chainId);
			statement.setString(2, startDate);
			statement.setString(3, endDate);
			rs = statement.executeQuery();
			while (rs.next()){
				schInfo = new ScheduleInfoDTO();
				schInfo.setScheduleId(rs.getInt("schedule_id"));
				schList.add(schInfo);
			}
		}catch (SQLException sqlce){
			throw new GeneralException( "Accessing cached Rowset exception", sqlce);
		}finally{
			PristineDBUtil.close(statement);
		}
		
		return schList;

	}
	/*
	select schedule_id, A.comp_str_id, B.Name, B.addr_line1, B.City, B.State from schedule A, competitor_store B where start_date >= To_date('09/06/09', 'MM/DD/YY') and END_DATE <=To_date('09/06/09', 'MM/DD/YY')+7 and
 price_check_list_id=73 and 
A.comp_str_id = B.comp_str_id and 
b.comp_str_no = '110332'*/
	public ScheduleInfoDTO getSchedulesForStore( Connection connection, String storenum, int checkListId, 
			String startDate)throws GeneralException {
		
		ScheduleInfoDTO schInfo = null;
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append("select schedule_id, A.comp_str_id, B.comp_chain_id, B.Name, B.addr_line1, B.City, B.State, ");
		sb.append(" start_date, end_date,  STATUS_CHG_DATE, B.comp_str_no");
		sb.append(" from schedule A, competitor_store B where ");
		sb.append("start_date >= To_date('").append(startDate).append("', 'MM/DD/YY') ");
		sb.append(" and end_date <= To_date('").append(startDate).append("', 'MM/DD/YY') + 7 ");
		sb.append(" and price_check_list_id=").append(checkListId);
		sb.append(" and A.comp_str_id = B.comp_str_id ");
		sb.append(" and b.comp_str_no = '").append(storenum).append("'");
		logger.debug(sb.toString());
		crs = PristineDBUtil.executeQuery(connection, sb, "getSchedulesForStore");
		try {
			if (crs.next()){
				schInfo = new ScheduleInfoDTO();
				schInfo.setScheduleId(crs.getInt("schedule_id"));
				schInfo.setChainId(crs.getInt("comp_chain_id"));
				schInfo.setStoreId(crs.getInt("comp_str_id"));
				schInfo.setStartDate(crs.getString("START_DATE"));
				schInfo.setEndDate(crs.getString("END_DATE"));
				schInfo.setStatusChangeDate(crs.getString("STATUS_CHG_DATE"));
				//schInfo.setChainName(crs.getString("COMP_CHAIN_NAME"));
				schInfo.setStoreName(crs.getString("NAME"));
				schInfo.setStoreNum(crs.getString("comp_str_no"));
				schInfo.setAddress(crs.getString("addr_line1"));
				schInfo.setCity(crs.getString("city"));
				schInfo.setState(crs.getString("state")); 
			}
		}catch (SQLException sqlce){
			throw new GeneralException( "Accessing cached Rowset exception", sqlce);
		}
		return schInfo;
	}
	
	public ArrayList <ScheduleInfoDTO> getSchedulesByChain(Connection connection, int chainId, int checkListId, 
			String startDate, String endDate)throws GeneralException {
		
		ScheduleInfoDTO schInfo = null;
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();

		sb.append(" SELECT A.schedule_id, B.comp_chain_id, TO_CHAR(A.start_date, 'MM/DD/YYYY') START_DATE, ");
		sb.append(" TO_CHAR(A.end_date, 'MM/DD/YYYY') END_DATE, TO_CHAR(A.STATUS_CHG_DATE, 'MM/DD/YYYY') STATUS_CHG_DATE, ");
		sb.append(" A.comp_str_id, B.comp_str_no, B.state, B.city, ");
		sb.append(" B.NAME store_name ");
		sb.append(" From schedule A, competitor_Store B WHERE B.comp_chain_id = ");
		sb.append(chainId);
		if( checkListId != -1)
			sb.append(" and A.PRICE_CHECK_LIST_ID = ").append(checkListId);
		sb.append(" and A.comp_str_id =  B.comp_str_id ");
		sb.append(" and start_date >= ").append("to_date('").append(startDate).append("','MM/DD/YY')");
		sb.append(" and end_date <= ").append("to_date('").append(endDate).append("','MM/DD/YY')");
		sb.append(" order by START_DATE, state,city");
		logger.debug(sb.toString());
		crs = PristineDBUtil.executeQuery(connection, sb, "getSchedulesForStore");
		ArrayList <ScheduleInfoDTO> schList = new ArrayList <ScheduleInfoDTO> ();
		try {
			while (crs.next()){
				schInfo = new ScheduleInfoDTO();
				schInfo.setScheduleId(crs.getInt("schedule_id"));
				schInfo.setChainId(crs.getInt("comp_chain_id"));
				schInfo.setStoreId(crs.getInt("comp_str_id"));
				schInfo.setStartDate(crs.getString("START_DATE"));
				schInfo.setEndDate(crs.getString("END_DATE"));
				schInfo.setStatusChangeDate(crs.getString("STATUS_CHG_DATE"));
				//schInfo.setChainName(crs.getString("COMP_CHAIN_NAME"));
				schInfo.setStoreNum(crs.getString("comp_str_no"));
				schInfo.setState(crs.getString("state"));
				schInfo.setCity(crs.getString("city"));
				schInfo.setStoreName(crs.getString("store_name"));
				schList.add(schInfo);
			}
		}catch (SQLException sqlce){
			throw new GeneralException( "Accessing cached Rowset exception", sqlce);
		}
		
		return schList;

	}
	
	public CachedRowSet getDuplicateStoreChecks(Connection connection, int priceCheckListId, String startDate, String endDate) throws GeneralException
	{
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		
		sb.append("select count(comp_str_id), comp_str_id, TO_char( start_date - 1 , 'MM/DD/YY') start_date, ");
		sb.append("TO_char( End_date + 1, 'MM/DD/YY') End_date from schedule where ");
		sb.append("price_Check_list_id =");
		sb.append(priceCheckListId);
		sb.append(" and start_date >= TO_DATE(' ");
		sb.append(startDate).append("', 'MM/DD/YY') ");
		sb.append(" and End_date <= TO_DATE(' ");
		sb.append(endDate).append("', 'MM/DD/YY') ");
		sb.append(" group by comp_str_id, start_date, End_date");
		sb.append(" having count(comp_str_id) > 1");

		logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(connection, sb, "schedule DAO - getSchedulesForChain v2 ");
		return crs;
	}
	
	public void mergeSchedule ( Connection conn, int primaryScheduleId, int removeScheduleId) throws 
					GeneralException {
		logger.debug(" Merging schedule = " + primaryScheduleId  + " with " + removeScheduleId);
		StringBuffer sb = new StringBuffer();
	
		sb.append( " UPDATE COMPETITIVE_DATA SET ");
		sb.append( " SCHEDULE_ID = " );
		sb.append( primaryScheduleId);
		sb.append( " WHERE SCHEDULE_ID = " );
		sb.append( removeScheduleId);	
		sb.append( " AND ITEM_CODE NOT IN " );
		sb.append( " ( SELECT ITEM_CODE FROM COMPETITIVE_DATA WHERE " );
		sb.append( " SCHEDULE_ID = " );
		sb.append( primaryScheduleId);	
		sb.append( ")");	
		int updateCount = PristineDBUtil.executeUpdate(conn, sb, "Competitive DATA - Update");
		logger.error("Schedule Update Comp Data Successful, record count = " + updateCount);
		
		sb = new StringBuffer();
		sb.append( " UPDATE COMPETITIVE_DATA SET ");
		sb.append( " COMMENTS = 'DUPLICATE CHECK'" );
		sb.append( " WHERE SCHEDULE_ID = " );
		sb.append( primaryScheduleId);	
		sb.append( " AND ITEM_CODE IN " );
		sb.append( " ( SELECT ITEM_CODE FROM COMPETITIVE_DATA WHERE " );
		sb.append( " SCHEDULE_ID = " );
		sb.append( removeScheduleId);	
		sb.append( ")");	
		updateCount = PristineDBUtil.executeUpdate(conn, sb, "Competitive DATA - Update");
		logger.info( "Duplicate Update Comp Data Successful, record count = " + updateCount);
		
		
		sb = new StringBuffer();
		sb.append( " DELETE FROM COMPETITIVE_DATA WHERE ");
		sb.append( " SCHEDULE_ID = " );
		sb.append( removeScheduleId);
		PristineDBUtil.executeUpdate(conn, sb, "Competitive DATA - DELETE");
		
		sb = new StringBuffer();
		sb.append( " DELETE FROM SCHEDULE WHERE ");
		sb.append( " SCHEDULE_ID = " );
		sb.append( removeScheduleId);
		PristineDBUtil.executeUpdate(conn, sb, "SCHEDULE - DELETE");
		
	}
	
	public ArrayList<Integer> getPreviousSchedules(Connection conn, int scheduleId) throws 
		GeneralException {
		
		ArrayList<Integer> prevSchList = new ArrayList<Integer>();
		
		StringBuffer selectSql = new StringBuffer();
        selectSql.append("SELECT SCHEDULE_ID, TO_CHAR( START_DATE, 'MM/DD/YYYY') START_DATE FROM SCHEDULE WHERE COMP_STR_ID");
        selectSql.append(" IN (SELECT COMP_STR_ID FROM SCHEDULE WHERE SCHEDULE_ID = ").append(scheduleId).append(")");
        selectSql.append(" AND START_DATE < (SELECT START_DATE FROM SCHEDULE WHERE SCHEDULE_ID = ").append(scheduleId).append(")");
        selectSql.append(" AND START_DATE >= (SELECT ADD_MONTHS(START_DATE, -6) FROM SCHEDULE WHERE SCHEDULE_ID = ").append(scheduleId).append(")");
        selectSql.append(" ORDER BY START_DATE DESC");

        
        String strCurWeekStartDate = DateUtil.getWeekStartDate(0);
        Date curWeekStartDate = DateUtil.toDate(strCurWeekStartDate);
        
        
        
        CachedRowSet crs = PristineDBUtil.executeQuery(conn, selectSql, "schedule DAO - getSchedulesForChain v2 ");
		
        int count = 0;
        try{
	        while ( crs.next()){
	        	prevSchList.add(crs.getInt("SCHEDULE_ID"));
	        	count++;
	        	 crs.getString("START_DATE");
	        	Date startDate = DateUtil.toDate(strCurWeekStartDate);
	        	
	        	int noOfDays = (int)DateUtil.getDateDiff(curWeekStartDate, startDate);
	        	
	        	
	        	
	        	if( count >= 12 && noOfDays > 90)
	        		break;
	        }
        }catch( SQLException sqlce){
        	throw new GeneralException( "Accessing cached Rowset exception", sqlce);        	
        }
        return prevSchList;
	}
	
	public static String getScheduleIdCSV (ArrayList<ScheduleInfoDTO> dtoList)
	{
		StringBuffer sb = new StringBuffer();
		for ( int ii = 0; ii < dtoList.size(); ii++ )
		{
			if ( ii > 0 ) sb.append(",");
			sb.append(String.valueOf(dtoList.get(ii).getScheduleId()));
		}
		return sb.toString();
	}


	public ArrayList<Integer> getCurrentDaySchedules(Connection conn,
			int excludeChainId) throws GeneralException {
		// SELECT SCHEDULE_ID FROM SCHEDULE WHERE CURRENT_STATUS in (2,4) and status_change_time > SYSDATE - 1;
		StringBuffer sb = new StringBuffer();
		 ArrayList<Integer>  currentDaySchList = new ArrayList<Integer> (); 
		sb.append("SELECT SCHEDULE_ID FROM SCHEDULE WHERE CURRENT_STATUS in (2,4) and STATUS_CHG_DATE > SYSDATE - 1 ");
		if ( excludeChainId > 0){
			sb.append(" AND COMP_STR_ID NOT in ( SELECT COMP_STR_ID WHERE COMP_CHAIN_ID = ");
			sb.append(excludeChainId).append(" )");
		}
		 CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "schedule DAO - getCurrenDaySchedule ");
	     try{
		        while ( crs.next()){
		        	currentDaySchList.add(crs.getInt("SCHEDULE_ID"));
		        }
	     }catch( SQLException sqlce){
	    	 throw new GeneralException( "Accessing cached Rowset exception", sqlce);        	
	     }
		 
		return currentDaySchList;
	}
	
	/*
	select schedule_id, A.comp_str_id, B.Name, B.addr_line1, B.City, B.State from schedule A, competitor_store B where start_date >= To_date('09/06/09', 'MM/DD/YY') and END_DATE <=To_date('09/06/09', 'MM/DD/YY')+7 and
 price_check_list_id=73 and 
A.comp_str_id = B.comp_str_id and 
b.comp_str_no = '110332'*/
	
	public ArrayList<ScheduleInfoDTO> getScheduleListForStore( Connection connection, String storenum, int checkListId, 
			String startDate, int noOfDays)throws GeneralException {
		
		ArrayList<ScheduleInfoDTO> schInfoList = new ArrayList<ScheduleInfoDTO>();
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		sb.append("select schedule_id, A.comp_str_id, B.comp_chain_id, B.Name, B.addr_line1, B.City, B.State, ");
		sb.append(" start_date, end_date,  STATUS_CHG_DATE, B.comp_str_no");
		sb.append(" from schedule A, competitor_store B where ");
		sb.append("start_date >= To_date('").append(startDate).append("', 'MM/DD/YYYY') ");
		sb.append(" and end_date <= To_date('").append(startDate).append("', 'MM/DD/YYYY') + " + noOfDays);
		if( checkListId > 0)
			sb.append(" and price_check_list_id=").append(checkListId);
		sb.append(" and A.comp_str_id = B.comp_str_id ");
		sb.append(" and b.comp_str_no = '").append(storenum).append("'");
		sb.append(" ORDER BY START_DATE DESC");
		logger.debug(sb.toString());
		crs = PristineDBUtil.executeQuery(connection, sb, "getSchedulesForStore");
		try {
			 while ( crs.next()){
				 ScheduleInfoDTO schInfo = new ScheduleInfoDTO();
				schInfo.setScheduleId(crs.getInt("schedule_id"));
				schInfo.setChainId(crs.getInt("comp_chain_id"));
				schInfo.setStoreId(crs.getInt("comp_str_id"));
				schInfo.setStartDate(crs.getString("START_DATE"));
				schInfo.setEndDate(crs.getString("END_DATE"));
				schInfo.setStatusChangeDate(crs.getString("STATUS_CHG_DATE"));
				//schInfo.setChainName(crs.getString("COMP_CHAIN_NAME"));
				schInfo.setStoreName(crs.getString("NAME"));
				schInfo.setStoreNum(crs.getString("comp_str_no"));
				schInfo.setAddress(crs.getString("addr_line1"));
				schInfo.setCity(crs.getString("city"));
				schInfo.setState(crs.getString("state"));
				schInfoList.add(schInfo);
			}
		}catch (SQLException sqlce){
			throw new GeneralException( "Accessing cached Rowset exception", sqlce);
		}
		return schInfoList;
	}
	
	
	/**
	 * This method populates schedule Id for given Price Zone
	 * @param conn				Connection
	 * @param priceZoneId		Price Zone Id
	 * @param weekStartDate		Week Start Date
	 * @param weekEndDate		Week End Date
	 * @return
	 * @throws GeneralException
	 */
	@SuppressWarnings("resource")
	public int populateScheduleIdForZone(Connection connection, int priceZoneId, String weekStartDate, String weekEndDate) throws GeneralException{
		logger.debug("Inside populateScheduleIdForZone() of ScheduleDAO");
		
		int scheduleId = -1;
	    PreparedStatement statement =  null;
	    PreparedStatement insertStatement = null;
	    ResultSet resultSet = null;
	    CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(connection);
	    try{
	    	String checkList = PropertyManager.getProperty("DATALOAD.CHECK_LIST_ID", Constants.EXTERNAL_PRICECHECK_LIST);
	    	String checkUser = PropertyManager.getProperty("DATALOAD.CHECK_USER_ID", Constants.EXTERNAL_PRICECHECK_USER);
	    	int priceCheckListId = compDataDAO.getPriceCheckListID(checkList);
		    statement = connection.prepareStatement(GET_SCHEDULE_ID_FOR_ZONE);
		    insertStatement = connection.prepareStatement(INSERT_SCHEDULE_ID_FOR_ZONE);
			scheduleId = -1;
			statement.setInt(1, priceZoneId);
			statement.setString(2, weekStartDate);
			statement.setString(3, weekEndDate);
			statement.setInt(4, priceCheckListId);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				scheduleId = resultSet.getInt("SCHEDULE_ID");
			}
			if(scheduleId == -1){
				insertStatement.setString(1, checkList);
				insertStatement.setString(2, checkUser);
				insertStatement.setString(3, checkUser);
				insertStatement.setInt(4, priceZoneId);
				insertStatement.setString(5, weekStartDate);
				insertStatement.setString(6, weekEndDate);
				insertStatement.executeUpdate();
				
				resultSet = statement.executeQuery();
				if(resultSet.next())
					scheduleId = resultSet.getInt("SCHEDULE_ID");
			}
	    }catch (SQLException e)
		{
			logger.error("Error while executing GET_SCHEDULE_ID");
			throw new GeneralException("Error while executing GET_SCHEDULE_ID", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return scheduleId;
	}
	
	/**
	 * This method populates schedule Id for given Price Zone
	 * @param conn				Connection
	 * @param locationId		Price Zone Id
	 * @param weekStartDate		Week Start Date
	 * @param weekEndDate		Week End Date
	 * @return
	 * @throws GeneralException
	 */
	@SuppressWarnings("resource")
	public int populateScheduleIdForLocation(Connection connection, int locationLevelId, int locationId, String weekStartDate, String weekEndDate) throws GeneralException{
		logger.debug("Inside populateScheduleIdForZone() of ScheduleDAO");
		
		int scheduleId = -1;
	    PreparedStatement statement =  null;
	    PreparedStatement insertStatement = null;
	    ResultSet resultSet = null;
	    CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(connection);
	    try{
	    	String checkList = PropertyManager.getProperty("DATALOAD.CHECK_LIST_ID", Constants.EXTERNAL_PRICECHECK_LIST);
	    	String checkUser = PropertyManager.getProperty("DATALOAD.CHECK_USER_ID", Constants.EXTERNAL_PRICECHECK_USER);
	    	int priceCheckListId = compDataDAO.getPriceCheckListID(checkList);
		    statement = connection.prepareStatement(GET_SCHEDULE_ID_FOR_CHAIN);
		    insertStatement = connection.prepareStatement(INSERT_SCHEDULE_ID_FOR_CHAIN);
			scheduleId = -1;
			statement.setInt(1, locationLevelId);
			statement.setInt(2, locationId);
			statement.setString(3, weekStartDate);
			statement.setString(4, weekEndDate);
			statement.setInt(5, priceCheckListId);
			resultSet = statement.executeQuery();
			if(resultSet.next()){
				scheduleId = resultSet.getInt("SCHEDULE_ID");
			}
			if(scheduleId == -1){
				insertStatement.setString(1, checkList);
				insertStatement.setString(2, checkUser);
				insertStatement.setString(3, checkUser);
				insertStatement.setInt(4, locationLevelId);
				insertStatement.setInt(5, locationId);
				insertStatement.setString(6, weekStartDate);
				insertStatement.setString(7, weekEndDate);
				insertStatement.executeUpdate();
				
				resultSet = statement.executeQuery();
				if(resultSet.next())
					scheduleId = resultSet.getInt("SCHEDULE_ID");
			}
	    }catch (SQLException e)
		{
			logger.error("Error while executing GET_SCHEDULE_ID");
			throw new GeneralException("Error while executing GET_SCHEDULE_ID", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return scheduleId;
	}
	
	/**
	 * Returns list of competitor schedules for checkdates between input start and end dates
	 * @param connection
	 * @param startDate
	 * @param endDate
	 * @return
	 * @throws GeneralException
	 */
	public List<Integer> getSchedulesForNonSubscriber(Connection connection, String startDate, String endDate) throws GeneralException{
		PreparedStatement stmt =  null;
		ResultSet rs = null;
		List<Integer> scheduleList = new ArrayList<Integer>();
		try{
			stmt = connection.prepareStatement(GET_SCHEDULES_FOR_NON_SUBSCRIBER);
			stmt.setString(1, startDate);
			stmt.setString(2, endDate);
			rs = stmt.executeQuery();
			while(rs.next()){
				scheduleList.add(rs.getInt("SCHEDULE_ID"));
			}
		}catch (SQLException e){
			logger.error("Error while executing GET_SCHEDULES_FOR_NON_SUBSCRIBER");
			throw new GeneralException("Error while executing GET_SCHEDULES_FOR_NON_SUBSCRIBER", e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return scheduleList;
	}
	
	
	/**
	 * 
	 * @param conn
	 * @return list of schedules created for the day
	 * @throws GeneralException
	 */
	public List<ScheduleInfoDTO> getSchedulesForTheDay(Connection conn, String date) throws GeneralException{
		List<ScheduleInfoDTO> scheduleList = new ArrayList<>();
		PreparedStatement stmt =  null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_SCHEDULES_FOR_THE_DAY);
			stmt.setString(1, date);
			stmt.setString(2, date);
			rs = stmt.executeQuery();
			while(rs.next()){
				ScheduleInfoDTO scheduleInfoDTO = new ScheduleInfoDTO();
				scheduleInfoDTO.setScheduleId(rs.getInt("SCHEDULE_ID"));
				scheduleInfoDTO.setStoreName(rs.getString("COMP_NAME"));
				scheduleList.add(scheduleInfoDTO);
			}
		}
		catch(SQLException sqlE){
			logger.error("Error -- getSchedulesForTheDay() ", sqlE);
			throw new GeneralException("Error -- getSchedulesForTheDay()", sqlE);
		}
		finally{
			PristineDBUtil.close(stmt);
			PristineDBUtil.close(rs);
		}
		
		return scheduleList;
	}
}
