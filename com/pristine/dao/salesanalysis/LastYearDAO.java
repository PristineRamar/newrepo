package com.pristine.dao.salesanalysis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;

import com.pristine.dao.CompStoreDAO;
import com.pristine.dto.ForecastDto;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

/**
 * @author denish
 *
 */
public class LastYearDAO {

	static Logger logger = Logger.getLogger(LastYearDAO.class);
	
	// Date Formatter
	SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");


    /*
	 * Method used to get the daily forecast Details 
	 * Argument 1 : Connection
	 * Argument 2 : List<RetailCalendarDTO> 
	 * Returns List<ForecastDto>
	 * @catch Exception
	 * @throws GeneralException
	 */

	
	public List<ForecastDto> getDailySalesForecastDetails(Connection conn,
			List<RetailCalendarDTO> dailyCalendarList, int locationLevelId,
			String selectTableName) throws GeneralException {

		List<ForecastDto> dailyForecastList = new ArrayList<ForecastDto>();

		StringBuffer sql = new StringBuffer();

		sql.append(" select LOCATION_ID,LOCATION_LEVEL_ID,CALENDAR_ID");
		sql.append(" ,TOT_REVENUE,SUMMARY_CTD_ID");
		
		// Check for Identical Details
		if( selectTableName.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP"))
		sql.append(" ,ID_TOT_REVENUE");
		
		sql.append(" from " + selectTableName + " where CALENDAR_ID in( ");
		for (int ii = 0; ii < dailyCalendarList.size(); ii++) {
			RetailCalendarDTO calendarList = dailyCalendarList.get(ii);
			if (ii > 0)
				sql.append(",");
			sql.append(calendarList.getCalendarId());
		}
		sql.append(" )");
		sql.append(" and LOCATION_LEVEL_ID=" + locationLevelId + " ");
		sql.append(" and PRODUCT_LEVEL_ID IS NULL");
		sql.append(" order by LOCATION_ID,CALENDAR_ID,LOCATION_LEVEL_ID");

		//logger.debug("Sql : " + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"getDailySalesForecastDetails");
			while (result.next()) {
				ForecastDto objForecast = new ForecastDto();
				objForecast.settotRevenue(result.getDouble("TOT_REVENUE"));
				objForecast.setForecastCtdId(result.getInt("SUMMARY_CTD_ID"));
				objForecast.setCalendarId(result.getInt("CALENDAR_ID"));
				objForecast.setLocationId(result.getInt("LOCATION_ID"));
				objForecast.setLocationLevelId(result.getInt("LOCATION_LEVEL_ID"));
				
				if( selectTableName.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP"))
				objForecast.setIdTotRevenue(result.getDouble("ID_TOT_REVENUE"));
								
				dailyForecastList.add(objForecast);

			}

		} catch (Exception exe) {
			throw new GeneralException(" Error while processing last year data",exe);
		}

		return dailyForecastList;
	}

	/*
	 * Method used to get the lastyear forecast details in district levels.
	 * Argument 1 : Connection 
	 * Argument 2 : Process Year 
	 * Argument 3 : Row type
	 * Argument 4 : Location Level Id
	 * @catch Exception
	 * @throws GeneralException
	 */

	public List<ForecastDto> GetDistrictForecast(Connection conn,
			Date processYear, String rowType, int locationLevelId)
			throws GeneralException {

		List<ForecastDto> returnList = new ArrayList<ForecastDto>(); 
		
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(TOT_REVENUE) as TOT_REVENUE,A.CALENDAR_ID, DISTRICT_ID");
		sql.append(" from SALES_AGGR_DAILY A , COMPETITOR_STORE b , RETAIL_CALENDAR c ");
		sql.append(" where B.DISTRICT_ID in (select ID from RETAIL_DISTRICT)");
		sql.append(" and A.LOCATION_ID  = B.COMP_STR_ID AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR=substr('" + formatter.format(processYear)
				+ "',7) AND C.ROW_TYPE='" + rowType + "'");
		sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");
		sql.append(" and PRODUCT_LEVEL_ID IS NULL");
		sql.append(" group by  A.LOCATION_LEVEL_ID,A.CALENDAR_ID,DISTRICT_ID");
		sql.append(" order by A.CALENDAR_ID,DISTRICT_ID");

		// logger.debug("Sql :" + sql.toString());

		try {

			// execute the query.
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"GetDistrictForecast");
			while (result.next()) {
				ForecastDto objForcastDto = new ForecastDto();
				objForcastDto.setLocationId(result.getInt("DISTRICT_ID"));
				objForcastDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objForcastDto.settotRevenue(result.getDouble("TOT_REVENUE"));
				returnList.add(objForcastDto);
			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("District Forecast Error", exe);
		}

		return returnList;
	}

	/*
	 * Method used to get the last year records in region levels 
	 * Argument 1 : Connection 
	 * Argument 2 : Process Year 
	 * Argument 3 : Calendar Row type
	 * Argument 4 : Location Level Id
	 * @catch Exception
	 * 
	 * @throws GeneralException
	 */

	public List<ForecastDto> getRegionForeCast(Connection conn,
			Date processYear, String rowType, int locationLevelId)
			throws GeneralException {

		List<ForecastDto> regionList = new ArrayList<ForecastDto>();
		
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(TOT_REVENUE) as TOT_REVENUE, A.CALENDAR_ID, REGION_ID");
		
		// code for identical
		sql.append(" , sum(ID_TOT_REVENUE) as ID_TOTREVENUE");
		sql.append(" from SALES_AGGR_DAILY_ROLLUP A , RETAIL_DISTRICT b, RETAIL_CALENDAR c  ");
		sql.append("  where B.ID = A.LOCATION_ID AND LOCATION_LEVEL_ID="
				+ locationLevelId + " ");
		sql.append("  AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR=substr('" + formatter.format(processYear)
				+ "',7) AND C.ROW_TYPE='" + rowType + "'");
		sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");
		sql.append(" and PRODUCT_LEVEL_ID IS NULL");
		sql.append(" group by  A.LOCATION_LEVEL_ID,A.CALENDAR_ID,REGION_ID");
		sql.append(" order by A.CALENDAR_ID,REGION_ID");
		// logger.debug("Sql " + sql.toString());
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"getregionForecaste");
			while (result.next()) {
				ForecastDto objForcastDto = new ForecastDto();
				objForcastDto.setLocationId(result.getInt("REGION_ID"));
				objForcastDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objForcastDto.settotRevenue(result.getDouble("TOT_REVENUE"));
				objForcastDto.setIdTotRevenue(result.getDouble("ID_TOTREVENUE"));
				regionList.add(objForcastDto);
			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Region Forecast Error", exe);
		}

		return regionList;
	}

	/*
	 * Method used to get the last year records in division levels 
	 * Argument 1 : Connection 
	 * Argument 2 : Process Year 
	 * Argument 3 : Calendar Row type
	 * Argument 4 : Location Level Id
	 * @catch Exception
	 * @throws GeneralException
	 */

	public List<ForecastDto> getDivisionForeCast(Connection conn,
			Date processYear, String rowType, int locationLevelId)
			throws GeneralException {

		List<ForecastDto> regionList = new ArrayList<ForecastDto>();
		
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(TOT_REVENUE) as TOT_REVENUE,A.CALENDAR_ID, DIVISION_ID");
		sql.append(" ,sum(ID_TOT_REVENUE) as ID_TOTREVENUE");
		sql.append(" from SALES_AGGR_DAILY_ROLLUP A , RETAIL_REGION b, RETAIL_CALENDAR c  ");
		sql.append("  where B.ID = A.LOCATION_ID AND LOCATION_LEVEL_ID="
				+ locationLevelId + " ");
		sql.append(" AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR=substr('" + formatter.format(processYear)
				+ "',7) AND C.ROW_TYPE='" + rowType + "'");
		sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");
		sql.append(" and PRODUCT_LEVEL_ID IS NULL");
		sql.append(" group by  A.LOCATION_LEVEL_ID,A.CALENDAR_ID,DIVISION_ID");
		sql.append(" order by A.CALENDAR_ID,DIVISION_ID");
		// logger.debug("Sql " + sql.toString());
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"GetDistrictForecast");
			while (result.next()) {
				ForecastDto objForcastDto = new ForecastDto();
				objForcastDto.setLocationId(result.getInt("DIVISION_ID"));
				objForcastDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objForcastDto.settotRevenue(result.getDouble("TOT_REVENUE"));
				objForcastDto.setIdTotRevenue(result.getDouble("ID_TOTREVENUE"));
				regionList.add(objForcastDto);
			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("District Forecast Error", exe);
		}
		return regionList;
	}
	
	

	/**
	 * @param conn
	 * @param districtForeCast
	 * @param locationLevelId
	 * @param methodName
	 * @param identicalDistrictMap 
	 * @throws GeneralException
	 */
	public void lastYearRollup(Connection conn,
			List<ForecastDto> districtForeCast, int locationLevelId, String methodName, HashMap<String, Double> identicalDistrictMap)
			throws GeneralException {

		try {
			PreparedStatement psmt = conn
					.prepareStatement(forecastRollupInsert("SALES_AGGR_DAILY_ROLLUP"));

			for (int ii = 0; ii < districtForeCast.size(); ii++) {

				ForecastDto objForecast = districtForeCast.get(ii);
				
				// check for identical
				if( locationLevelId == Constants.DISTRICT_LEVEL_ID){
					
					// check the district contain identical revenue or not
					if( identicalDistrictMap.containsKey(objForecast.getCalendarId()+"_"+objForecast.getLocationId())){
						
						double IdenticalLastYear =  identicalDistrictMap.get(objForecast.getCalendarId()+"_"+objForecast.getLocationId());
						
						objForecast.setIdTotRevenue(IdenticalLastYear);
						
					}
					
				}
					

				objForecast.setLocationLevelId(locationLevelId);

				addSqlBatch(psmt, objForecast, "Rollup" , "SALES_AGGR_DAILY_ROLLUP" , "");
			}

			// call the execute batch method to insert the daily records.
			excuteBatch(psmt, methodName);
			PristineDBUtil.commitTransaction(conn,
					"Commit the forecast Process");
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("forecastRollup", exe);
		}

	}

	
	
	/**
	 * @param conn
	 * @param calendarList
	 * @param locationLevelId
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, String> getPreviousForecast(Connection conn,
			HashMap<String, Integer> calendarList, int locationLevelId)
			throws GeneralException {

		StringBuffer sql = new StringBuffer();
		HashMap<String, String> forecastList = new HashMap<String, String>();
		sql.append(" select LOCATION_ID,CALENDAR_ID from SALES_AGGR_DAILY");
		sql.append(" where CALENDAR_ID in(");
		Object[] innerLoop = calendarList.values().toArray();
		for (int ii = 0; ii < innerLoop.length; ii++) {
			if (ii > 0)
				sql.append(",");
			sql.append(innerLoop[ii]);

		}
		sql.append(" )and LOCATION_LEVEL_ID=" + locationLevelId + "");
		sql.append(" and PRODUCT_ID is null");

		/* logger.debug("Sql :" + sql.toString()); */

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"getPreviousForecast");
			while (result.next()) {
				forecastList.put(
						result.getString("location_id") + "_"
								+ result.getString("calendar_id"),
						result.getString("location_id") + "_"
								+ result.getString("calendar_id"));
			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("getPreviousForecast", exe);

		}
		// logger.debug("Previous Forecast List Size : " + forecastList.size());
		return forecastList;
	}
	
	

	/**
	 * @param insertPsmt
	 * @param updatePsmt
	 * @param previousForecast
	 * @param calendarList
	 * @param objForCastDto
	 * @param storeStatus 
	 * @throws SQLException
	 * @throws GeneralException
	 */
	public void addDayBatch(PreparedStatement insertPsmt,
			PreparedStatement updatePsmt,
			HashMap<String, String> previousForecast,
			HashMap<String, Integer> calendarList, ForecastDto objForCastDto, String storeStatus)
			throws SQLException, GeneralException {
		try {

			// for sunday
			// check the previous last year data (Insert / update )
			if (previousForecast
					.containsKey(objForCastDto.getLocationId()
							+ "_"
							+ calendarList.get(objForCastDto.getProcessDate()
									+ "_" + 1))) {

				// if map contains the searching key means update mode
				updateForecast(updatePsmt, objForCastDto.getSundayWog(),
						objForCastDto.getSundayWg(), objForCastDto,
						calendarList, 1 ,storeStatus);

			} else {

				// if key not avilable means insert mode
				insertForecast(insertPsmt, objForCastDto.getSundayWog(),
						objForCastDto.getSundayWg(), objForCastDto,
						calendarList, 1 , storeStatus);
			}

			// for monday
			if (previousForecast
					.containsKey(objForCastDto.getLocationId()
							+ "_"
							+ calendarList.get(objForCastDto.getProcessDate()
									+ "_" + 2))) {
				updateForecast(updatePsmt, objForCastDto.getMondayWog(),
						objForCastDto.getMondayWg(), objForCastDto,
						calendarList, 2 , storeStatus);
			} else {
				insertForecast(insertPsmt, objForCastDto.getMondayWog(),
						objForCastDto.getMondayWg(), objForCastDto,
						calendarList, 2 , storeStatus);
			}

			// for Tuesday

			if (previousForecast
					.containsKey(objForCastDto.getLocationId()
							+ "_"
							+ calendarList.get(objForCastDto.getProcessDate()
									+ "_" + 3))) {
				updateForecast(updatePsmt, objForCastDto.getTuesdayWog(),
						objForCastDto.getTuesdayWg(), objForCastDto,
						calendarList, 3 , storeStatus);

			} else {
				insertForecast(insertPsmt, objForCastDto.getTuesdayWog(),
						objForCastDto.getTuesdayWg(), objForCastDto,
						calendarList, 3 , storeStatus);
			}

			// for Wednesday
			if (previousForecast
					.containsKey(objForCastDto.getLocationId()
							+ "_"
							+ calendarList.get(objForCastDto.getProcessDate()
									+ "_" + 4))) {
				updateForecast(updatePsmt, objForCastDto.getWednesdayWog(),
						objForCastDto.getWednesdayWg(), objForCastDto,
						calendarList, 4 , storeStatus);

			} else {
				insertForecast(insertPsmt, objForCastDto.getWednesdayWog(),
						objForCastDto.getWednesdayWg(), objForCastDto,
						calendarList, 4 , storeStatus);
			}

			// for thursday
			if (previousForecast
					.containsKey(objForCastDto.getLocationId()
							+ "_"
							+ calendarList.get(objForCastDto.getProcessDate()
									+ "_" + 5))) {
				updateForecast(updatePsmt, objForCastDto.getThursdayWog(),
						objForCastDto.getThursdayWg(), objForCastDto,
						calendarList, 5 , storeStatus);

			} else {
				insertForecast(insertPsmt, objForCastDto.getThursdayWog(),
						objForCastDto.getThursdayWg(), objForCastDto,
						calendarList, 5 , storeStatus);
			}

			// for friday
			if (previousForecast
					.containsKey(objForCastDto.getLocationId()
							+ "_"
							+ calendarList.get(objForCastDto.getProcessDate()
									+ "_" + 6))) {
				updateForecast(updatePsmt, objForCastDto.getFridayWog(),
						objForCastDto.getFridayWg(), objForCastDto,
						calendarList, 6 , storeStatus);
			} else {
				insertForecast(insertPsmt, objForCastDto.getFridayWog(),
						objForCastDto.getFridayWg(), objForCastDto,
						calendarList, 6 , storeStatus);
			}

			// for saterday
			if (previousForecast
					.containsKey(objForCastDto.getLocationId()
							+ "_"
							+ calendarList.get(objForCastDto.getProcessDate()
									+ "_" + 7))) {
				updateForecast(updatePsmt, objForCastDto.getSaterdayWog(),
						objForCastDto.getSaterdayWg(), objForCastDto,
						calendarList, 7 , storeStatus);
			} else {
				insertForecast(insertPsmt, objForCastDto.getSaterdayWog(),
						objForCastDto.getSaterdayWg(), objForCastDto,
						calendarList, 7 , storeStatus);
			}
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Add batch Error", exe);
		}
	}

	private void insertForecast(PreparedStatement psmt, Object wog, Object wg,
			ForecastDto objForCastDto, HashMap<String, Integer> calendarList,
			int cid, String storeStatus) throws SQLException {
				
		psmt.setObject(1,
				calendarList.get(objForCastDto.getProcessDate() + "_" + cid));
		psmt.setObject(2, Constants.STORE_LEVEL_ID);
		psmt.setObject(3, objForCastDto.getLocationId());
		psmt.setObject(4, wog);
		psmt.setString(5, storeStatus);
		psmt.addBatch();
		
	}

	private void updateForecast(PreparedStatement psmt, Object wog, Object wg,
			ForecastDto objForCastDto, HashMap<String, Integer> calendarList,
			int cid, String storeStatus) throws SQLException {

		psmt.setObject(1, wog);
		psmt.setString(2, storeStatus);	
		psmt.setObject(3,calendarList.get(objForCastDto.getProcessDate() + "_" + cid));
		psmt.setObject(4, objForCastDto.getLocationId());
		psmt.setObject(5, Constants.STORE_LEVEL_ID);

		psmt.addBatch();

	}

	/*
	 * Method used to get the Forecast Update query
	 */
	public String forecastUpdate() {

		StringBuffer sql = new StringBuffer();
		sql.append(" update SALES_AGGR_DAILY set ");
		sql.append(" TOT_REVENUE=? , STORE_TYPE=?");
		sql.append(" where CALENDAR_ID=? and LOCATION_ID=? and LOCATION_LEVEL_ID=?");

		return sql.toString();
	}
	
	/*
	 * Method used to delete the Daily forecast records in rollup level
	 * Argument 1 : Connection
	 * Argument 2 : Process Year
	 * @catch Exception
	 * @throws Gendral Exception
	 */

	public void deleteDailyForecast(Connection conn, Date processYear)
			throws GeneralException {
		StringBuffer sql = new StringBuffer();
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
		
		
		try {
			sql.append(" delete from SALES_AGGR_DAILY_ROLLUP");
			sql.append(" where CALENDAR_ID in");
			sql.append(" (select CALENDAR_ID from RETAIL_CALENDAR");
			sql.append(" where CAL_YEAR=substr('" + formatter.format(processYear)+ "',7)");
			sql.append(" and ROW_TYPE='D')");
			//logger.debug("Daily Rollup Level  : " + sql.toString());
			PristineDBUtil.execute(conn, sql, "Delete ForeCast");
		} catch (Exception e) {
			 logger.error(e);
			 throw new GeneralException("deleteDailyForecast Method Error" , e);
		}
	}

	/**
	 * @param conn
	 * @param processYear
	 * @throws GeneralException
	 */
	public void deleteWeeklyLastYear(Connection conn, Date processYear)
			throws GeneralException {
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
		StringBuffer sql = new StringBuffer();

		try {
			sql.append(" delete from SALES_AGGR_WEEKLY");
			sql.append(" where CALENDAR_ID in");
			sql.append(" (select CALENDAR_ID from RETAIL_CALENDAR");
			sql.append(" where CAL_YEAR=substr('"
					+ formatter.format(processYear) + "',7) and ROW_TYPE='W')");
			// logger.debug("Sql :" + sql.toString());
			PristineDBUtil.execute(conn, sql, "Delete ForeCast");
		} catch (Exception e) {
			logger.error(e);
			throw new GeneralException("Delete SALES_AGGR_WEEKLY Error " + e);
		}

		/*sql = new StringBuffer();
		try {
			sql.append(" delete from SALES_AGGR_CTD where SUMMARY_CTD_ID in");
			sql.append(" (select SUMMARY_CTD_ID  from  SALES_AGGR_DAILY");
			sql.append(" where CALENDAR_ID in(select CALENDAR_ID from RETAIL_CALENDAR");
			sql.append(" where CAL_YEAR=substr('"
					+ formatter.format(processYear) + "',7)))");
			// logger.debug("Sql :" + sql.toString());
			PristineDBUtil.execute(conn, sql, "Delete ForeCast");
		} catch (Exception e) {
			logger.error(e);
			throw new GeneralException("Delete SALES_AGGR_CTD table " + e);
		}*/

		sql = new StringBuffer();
		try {
			sql.append(" delete from SALES_AGGR_WEEKLY_ROLLUP");
			sql.append(" where CALENDAR_ID in");
			sql.append(" (select CALENDAR_ID from RETAIL_CALENDAR");
			sql.append(" where CAL_YEAR=substr('"
					+ formatter.format(processYear) + "',7) and ROW_TYPE='W')");
			// logger.debug("Sql :" + sql.toString());
			PristineDBUtil.execute(conn, sql, "Delete ForeCast");
		} catch (Exception e) {
			logger.error(e);
			throw new GeneralException("Delete SALES_AGGR_WEEKLY_ROLLUP " + e);
		}

		sql = new StringBuffer();
		try {
			sql.append(" delete from SALES_AGGR");
			sql.append(" where CALENDAR_ID in");
			sql.append(" (select CALENDAR_ID from RETAIL_CALENDAR");
			sql.append(" where CAL_YEAR=substr('"
					+ formatter.format(processYear) + "',7))");
			// logger.debug("Sql :" + sql.toString());
			PristineDBUtil.execute(conn, sql, "Delete ForeCast");
		} catch (Exception e) {
			logger.error(e);
			throw new GeneralException("Delete SALES_AGGR" + e);
		}

		sql = new StringBuffer();
		try {
			sql.append(" delete from SALES_AGGR_ROLLUP");
			sql.append(" where CALENDAR_ID in");
			sql.append(" (select CALENDAR_ID from RETAIL_CALENDAR");
			sql.append(" where CAL_YEAR=substr('"
					+ formatter.format(processYear) + "',7))");
			// logger.debug("Sql :" + sql.toString());
			PristineDBUtil.execute(conn, sql, "Delete ForeCast");
		} catch (Exception e) {
			logger.error(e);
			throw new GeneralException("Delete SALES_AGGR_ROLLUP" + e);
		}

	}
	
	
	public String forecastInsert(String tableName) {

		StringBuffer sql = new StringBuffer();
		sql.append(" insert into " + tableName
				+ "(SUMMARY_DAILY_ID,CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID");
		sql.append(" ,TOT_REVENUE,SUMMARY_CTD_ID,STORE_TYPE)");
		//sql.append(" values(SALES_AGGR_DAILY_SEQ.NEXTVAL,?,?,?,?,SALES_AGGR_CTD_SEQ.NEXTVAL,?)");
		sql.append(" values(SALES_AGGR_DAILY_SEQ.NEXTVAL,?,?,?,?,NULL,?)");
		// logger.debug("Sql :" + sql.toString());
		return sql.toString();
	}
	
	public String forecastRollupInsert(String tableName) {

		StringBuffer sql = new StringBuffer();
		sql.append(" insert into " + tableName + "(" + tableName
				+ "_ID,CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID");
		sql.append(" ,TOT_REVENUE,SUMMARY_CTD_ID,ID_TOT_REVENUE)");
		//sql.append(" values(" + tableName + "_SEQ.NEXTVAL,?,?,?,?,SALES_AGGR_CTD_SEQ.NEXTVAL,?)");
		sql.append(" values(" + tableName + "_SEQ.NEXTVAL,?,?,?,?,NULL,?)");
		// logger.debug("Sql :" + sql.toString());
		return sql.toString();
	}

	
	public String forecastCtdInsert() {

		StringBuffer sql = new StringBuffer();
		sql.append(" insert into SALES_AGGR_CTD(SUMMARY_CTD_ID");
		sql.append(" ,CTD_TYPE,TOT_REVENUE,ID_TOT_REVENUE)");
		sql.append(" values(?,?,?,?)");
		return sql.toString();
	}
	
	public String forecastInsertWeek(String insertTableName, int locationLevelId) {

		StringBuffer sql = new StringBuffer();
		sql.append(" insert into " + insertTableName + "(" + insertTableName
				+ "_ID,CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID");
		sql.append(" ,TOT_REVENUE,LAST_AGGR_CALENDARID");

		if (locationLevelId == Constants.STORE_LEVEL_ID)
			sql.append(" ,STORE_TYPE");
		else if (insertTableName.equalsIgnoreCase("SALES_AGGR_WEEKLY_ROLLUP")
				|| insertTableName.equalsIgnoreCase("SALES_AGGR_ROLLUP"))
			sql.append(" ,ID_TOT_REVENUE");

		sql.append(" )");
		sql.append(" values(" + insertTableName + "_SEQ.NEXTVAL,?,?,?,?,?");
		if (locationLevelId == Constants.STORE_LEVEL_ID)
			sql.append(" ,?");
		else if (insertTableName.equalsIgnoreCase("SALES_AGGR_WEEKLY_ROLLUP")
				|| insertTableName.equalsIgnoreCase("SALES_AGGR_ROLLUP"))
			sql.append(" ,?");
		sql.append(" )");
		// logger.debug("Sql : " + sql.toString());
		return sql.toString();
	}

	/**
	 * @param psmt
	 * @param methodName
	 * @return
	 * @throws GeneralException
	 */
	public int excuteBatch(PreparedStatement psmt, String methodName)
			throws GeneralException {
		int insertFlag = 0;
		try {
			int[] exe = psmt.executeBatch();
			insertFlag = 1;
			logger.info(methodName + " Count" + exe.length);

		} catch (Exception exe) {

			logger.error(exe);
			throw new GeneralException("Error In excute Batch..... " + exe);
		}
		return insertFlag;
	}
	
	/**
	 * @param psmt
	 * @param objForCastDto
	 * @param batchMode
	 * @param insertTableName 
	 * @param storeStatus 
	 * @throws SQLException
	 */
	public void addSqlBatch(PreparedStatement psmt, ForecastDto objForCastDto,
			String batchMode, String insertTableName, String storeStatus) throws SQLException {

		try {

			if (batchMode.equalsIgnoreCase("Rollup")) {
				psmt.setObject(1, objForCastDto.getCalendarId());
				psmt.setObject(2, objForCastDto.getLocationLevelId());
				psmt.setObject(3, objForCastDto.getLocationId());
				psmt.setObject(4, objForCastDto.gettotRevenue());
				psmt.setObject(5, objForCastDto.getIdTotRevenue());
			} else if (batchMode.endsWith("Ctd")) {
				psmt.setObject(1, objForCastDto.getForecastCtdId());
				psmt.setObject(2, objForCastDto.getCtdType());
				psmt.setObject(3, objForCastDto.gettotRevenue());
				psmt.setObject(4, objForCastDto.getIdTotRevenue());
			}

			else if (batchMode.endsWith("Week")) {
				psmt.setObject(1, objForCastDto.getCalendarId());
				psmt.setObject(2, objForCastDto.getLocationLevelId());
				psmt.setObject(3, objForCastDto.getLocationId());
				psmt.setObject(4, objForCastDto.gettotRevenue());
				psmt.setObject(5, objForCastDto.getLastCalendarId());
				
				if (objForCastDto.getLocationLevelId() == Constants.STORE_LEVEL_ID)
					psmt.setString(6, storeStatus);
				
				else if (insertTableName
						.equalsIgnoreCase("SALES_AGGR_WEEKLY_ROLLUP")
						|| insertTableName
								.equalsIgnoreCase("SALES_AGGR_ROLLUP"))
					psmt.setObject(6, objForCastDto.getIdTotRevenue());
			  }

			psmt.addBatch();
		} catch (Exception exe) {
			logger.error(exe);
			throw new SQLException(exe);
		}

	}
	
	
	/**
	 * Method used to aggregate the division level records for chain process
	 * @param _compChainId 
	 * @param _conn
	 * @param _processYear
	 * @param calendarDay
	 * @param divisionLevelId
	 * @return List<ForecastDto>
	 * @throws GeneralException 
	 */
	public List<ForecastDto> getChainForecast(Connection conn,
			Date processYear, String rowType, int locationLevelId,
			int _compChainId) throws GeneralException {

		// resturn list
		List<ForecastDto> returnList = new ArrayList<ForecastDto>();

		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" select sum(TOT_REVENUE) as TOT_REVENUE,A.CALENDAR_ID");
			sql.append(" ,sum(ID_TOT_REVENUE) as ID_TOTREVENUE"); 
			sql.append(" from SALES_AGGR_DAILY_ROLLUP A ,RETAIL_CALENDAR B  ");
			sql.append("  where A.CALENDAR_ID = B.CALENDAR_ID ");
			sql.append(" AND B.CAL_YEAR=substr('"
					+ formatter.format(processYear) + "',7)");
			sql.append(" and B.ROW_TYPE='").append(rowType).append("'");
			sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");
			sql.append(" and PRODUCT_LEVEL_ID IS NULL");
			sql.append(" group by A.CALENDAR_ID");
			sql.append(" order by A.CALENDAR_ID");

			logger.debug(" Chian Forecast Aggregation Query....."
					+ sql.toString());
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"getChainForecast");

			while (result.next()) {
				ForecastDto objForcastDto = new ForecastDto();
				objForcastDto.setLocationId(_compChainId);
				objForcastDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objForcastDto.settotRevenue(result.getDouble("TOT_REVENUE"));
				objForcastDto.setIdTotRevenue(result.getDouble("ID_TOTREVENUE"));
				returnList.add(objForcastDto);

			}
		} catch (SQLException e) {
			logger.error(
					" Error while fetching chain level aggregation data....", e);
			throw new GeneralException(
					" Error while fetching chain level aggregation data....", e);
		} catch (GeneralException e) {
			logger.error(
					" Error while fetching chain level aggregation data....", e);
			throw new GeneralException(
					" Error while fetching chain level aggregation data....", e);
		}

		return returnList;

	}

	public HashMap<String, Double> getIdenticalDistrict(Connection _conn,
			Date processDate, String rowType, int locationLevelId)
			throws GeneralException {
	 
		HashMap<String, Double> returnMap = new HashMap<String, Double>();
		
		StringBuffer sql = new StringBuffer();
			 
		sql.append(" select sum(TOT_REVENUE) as ID_TOT_REVENUE,A.CALENDAR_ID, DISTRICT_ID");
		sql.append(" from SALES_AGGR_DAILY A , COMPETITOR_STORE b , RETAIL_CALENDAR c ");
		sql.append(" where B.DISTRICT_ID in (select ID from RETAIL_DISTRICT)");
		sql.append(" and A.LOCATION_ID  = B.COMP_STR_ID AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR=substr('" + formatter.format(processDate)+ "',7) AND C.ROW_TYPE='" + rowType + "'");
		sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");
		sql.append(" and PRODUCT_LEVEL_ID IS NULL");
		sql.append(" and B.OPEN_DATE < C.START_DATE-364");
		sql.append(" group by  A.LOCATION_LEVEL_ID,A.CALENDAR_ID,DISTRICT_ID");
		sql.append(" order by A.CALENDAR_ID,DISTRICT_ID");
		
		logger.error(" Identical District Sql ... " + sql.toString());
		
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql, "getIdenticalDistrict");
			
			while( result.next()){
				returnMap.put(result.getString("CALENDAR_ID")+"_"+result.getString("DISTRICT_ID"), result.getDouble("ID_TOT_REVENUE"));
				
			}
		} catch (SQLException e) {
		    throw new GeneralException("Error in Processing Identical District Process.... " , e);
		} catch (GeneralException e) {
			  throw new GeneralException("Error in Processing Identical District Process.... " , e);
		}
			 
				
		
		return returnMap;
	}

	public String getStoreStatus(Connection _conn, Date processDate, String storeNo, String calendarRowType) throws GeneralException {

				// Find given store is Identical or New
				String retVal = "";
				
				// format the the given date string
				SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");	
												
				try {
					// query for getting Identical
					StringBuffer sql = new StringBuffer();
					sql.append(" select");
					sql.append(" case");
					
								
					// for Identical
					sql.append(" when A.START_DATE  > B.OPEN_DATE ").append(" then 'I'");
					// for new
					sql.append(" when A.START_DATE  < B.OPEN_DATE ").append(" then 'N'");
					sql.append(" END as status");
					sql.append(" from (");
					sql.append(" select START_DATE - 364 AS START_DATE from  RETAIL_CALENDAR");
					sql.append(" where to_date('").append(formatter.format(processDate))
												   .append("' ,'dd-mm-yyyy')");			
					sql.append(" between START_DATE and END_DATE and ROW_TYPE='").append(calendarRowType)
																				 .append("') A");
					sql.append(" , ( select OPEN_DATE FROM COMPETITOR_STORE WHERE COMP_STR_NO='").append(storeNo).append("') B");
					
					 
					logger.debug(" Get store status..... " +sql.toString());
					  
					String singleColumnVal = PristineDBUtil.getSingleColumnVal(_conn, sql, "updateIdenticalDetails");
					
					
					if( singleColumnVal !=null){
						retVal =  singleColumnVal;
					}
				} catch (GeneralException e) {
					logger.error(" Error while processing the updateIdenticalDetails .... " ,e);
					throw new GeneralException(" Error while processing the updateIdenticalDetails .... " ,e);
					
				}

				return retVal;
		
		
		 
	}

	public HashMap<String, String> getStoreStatus(Connection _conn, Date _startdate, Date _endDate, String calendarWeek,
			String calendarConstant) throws GeneralException {

		// Find given store is Identical or New
		HashMap<String, String> returnMap = new HashMap<String, String>();		 
		
		// format the the given date string
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
		
		CompStoreDAO objCOmpStoreDao = new CompStoreDAO();
										
		try {
			
			// get the store No List
			List<String> storeList = objCOmpStoreDao.getKeyLocationList(_conn);
			
			for( int ii=0 ;ii< storeList.size(); ii++){
					
				// query for getting Identical
				StringBuffer sql = new StringBuffer();
				sql.append(" select");
				sql.append(" case");
			
				if (calendarConstant.equalsIgnoreCase("P") || calendarConstant.equalsIgnoreCase("Q") || calendarConstant.equalsIgnoreCase("Y")) {
					sql.append(" WHEN B.OPEN_DATE > to_date('"+ formatter.format(_startdate) + "','dd-mm-yyyy')"); 
					sql.append(" AND  B.OPEN_DATE < to_date('"+ formatter.format(_endDate)+ "' ,'dd-mm-yyyy')  then 'M'");
				}
			
				// 	for Identical
				sql.append(" when A.START_DATE  > B.OPEN_DATE ").append(" then 'I'");
				// for new
				sql.append(" when A.START_DATE  < B.OPEN_DATE ").append(" then 'N'");
				sql.append(" END as status");
				sql.append(" from (");
				sql.append(" select START_DATE - 364 AS START_DATE from  RETAIL_CALENDAR");
				sql.append(" where to_date('").append(formatter.format(_startdate))
										   .append("' ,'dd-mm-yyyy')");			
				sql.append(" between START_DATE and END_DATE and ROW_TYPE='").append(Constants.CALENDAR_WEEK)
																		 .append("') A");
				sql.append(" , ( select OPEN_DATE FROM COMPETITOR_STORE WHERE COMP_STR_ID='").append(storeList.get(ii)).append("') B");
			
			 	logger.debug(" Get store status..... " +sql.toString());
			  
			 	String singleColumnVal = PristineDBUtil.getSingleColumnVal(_conn, sql, "updateIdenticalDetails");
			
			
			 	if( singleColumnVal !=null){
					returnMap.put(storeList.get(ii), singleColumnVal);
			  	}
			}
		} catch (GeneralException e) {
			logger.error(" Error while processing the updateIdenticalDetails .... " ,e);
			throw new GeneralException(" Error while processing the updateIdenticalDetails .... " ,e);
			
		}

		return returnMap;


 
}
	
	
}
