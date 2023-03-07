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
import com.pristine.dto.ForecastDto;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;


public class SalesForecastDAO {

	static Logger logger = Logger.getLogger(SalesForecastDAO.class);
	
	SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
	
	/*
	 * Insert Query for Forcast "SALES_AGGR_FORECAST"
	 */

	public String forecastInsert(String methodName) {

		StringBuffer sql = new StringBuffer();
		sql.append(" insert into SALES_AGGR_FORECAST(CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID");
		sql.append(" ,FORECAST_WG,FORECAST_WOG,FORECAST_CTD_ID");
		if (methodName.equalsIgnoreCase("DISTRICT"))
			sql.append(" ,ID_FORECAST_WG,ID_FORECAST_WOG");
		sql.append(" )");
		//sql.append(" values(?,?,?,?,?,SALES_AGGR_FORECAST_CTD_SEQ.NEXTVAL");
		sql.append(" values(?,?,?,?,?,NULL");
		if (methodName.equalsIgnoreCase("DISTRICT"))
			sql.append(" ,?,?");
		sql.append(" )");

		return sql.toString();
	}

	public String forecastCtdInsert() {

		StringBuffer sql = new StringBuffer();
		sql.append(" insert into SALES_AGGR_FORECAST_CTD(FORECAST_CTD_ID");
		sql.append(" ,CTD_TYPE,FORECAST_WG,FORECAST_WOG,ID_FORECAST_WG,ID_FORECAST_WOG)");
		sql.append(" values(?,?,?,?,?,?)");
		return sql.toString();
	}

	public String forecastInsertWeek() {

		StringBuffer sql = new StringBuffer();
		sql.append(" insert into SALES_AGGR_FORECAST(CALENDAR_ID,LOCATION_LEVEL_ID,LOCATION_ID");
		sql.append(" ,FORECAST_WG,FORECAST_WOG,FORECAST_CTD_ID,ID_FORECAST_WG,ID_FORECAST_WOG)");
		sql.append(" values(?,?,?,?,?,?,?,?)");
		return sql.toString();
	}

	/**
	 * @param psmt
	 * @param objForCastDto
	 * @param calendarList
	 * @param batchMode
	 * @param previousForecast
	 * @throws SQLException
	 */
	public void addSqlBatch(PreparedStatement psmt, ForecastDto objForCastDto,
			HashMap<String, Integer> calendarList, String batchMode,
			HashMap<String, String> previousForecast) throws SQLException {

		try {
			// for sunday records

			if (batchMode.equalsIgnoreCase("Rollup")) {
				psmt.setObject(1, objForCastDto.getCalendarId());
				psmt.setObject(2, objForCastDto.getLocationLevelId());
				psmt.setObject(3, objForCastDto.getLocationId());
				psmt.setObject(4, objForCastDto.getForecastWg());
				psmt.setObject(5, objForCastDto.getForeCastWog());
				psmt.setObject(6, objForCastDto.getIdForecastWg());
				psmt.setObject(7, objForCastDto.getIdForecastWog());
			} else if (batchMode.endsWith("Ctd")) {
				psmt.setObject(1, objForCastDto.getForecastCtdId());
				psmt.setObject(2, objForCastDto.getCtdType());
				psmt.setObject(3, objForCastDto.getForecastWg());
				psmt.setObject(4, objForCastDto.getForeCastWog());
				psmt.setObject(5, objForCastDto.getIdForecastWg());
				psmt.setObject(6, objForCastDto.getIdForecastWog());
			}

			else if (batchMode.endsWith("Week")) {
				psmt.setObject(1, objForCastDto.getCalendarId());
				psmt.setObject(2, objForCastDto.getLocationLevelId());
				psmt.setObject(3, objForCastDto.getLocationId());
				psmt.setObject(4, objForCastDto.getForecastWg());
				psmt.setObject(5, objForCastDto.getForeCastWog());
				psmt.setObject(6, objForCastDto.getForecastCtdId());
				psmt.setObject(7, objForCastDto.getIdForecastWg());
				psmt.setObject(8, objForCastDto.getIdForecastWog());

			}

			psmt.addBatch();
		} catch (Exception exe) {
			logger.error(exe);
			throw new SQLException(exe);
		}

	}

	public void excuteBatch(PreparedStatement psmt, String methodName)
			throws GeneralException {

		try {
			int[] count = psmt.executeBatch();
			logger.info(methodName + " Count " + count.length);
		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException(" Execute batch method.......", exe);
		}

	}


	/**
	 * @param conn
	 * @param dailyCalendarList
	 * @param locationLevelId
	 * @return
	 * @throws GeneralException
	 */
	public List<ForecastDto> getDailySalesForecastDetails(Connection conn,
			List<RetailCalendarDTO> dailyCalendarList, int locationLevelId)
			throws GeneralException {

		List<ForecastDto> dailyForecastList = new ArrayList<ForecastDto>();

		StringBuffer sql = new StringBuffer();

		sql.append(" select LOCATION_ID,LOCATION_LEVEL_ID,CALENDAR_ID");
		sql.append(" ,FORECAST_WG,FORECAST_WOG,FORECAST_CTD_ID");
		sql.append(" ,ID_FORECAST_WG,ID_FORECAST_WOG");
		sql.append(" from SALES_AGGR_FORECAST where CALENDAR_ID in( ");
		for (int ii = 0; ii < dailyCalendarList.size(); ii++) {
			RetailCalendarDTO calendarList = dailyCalendarList.get(ii);
			if (ii > 0)
				sql.append(",");
			sql.append(calendarList.getCalendarId());
		}
		sql.append(" )");

		sql.append(" and LOCATION_LEVEL_ID=" + locationLevelId + " ");
		sql.append(" order by LOCATION_ID,CALENDAR_ID,LOCATION_LEVEL_ID");

		logger.debug("Sql : " + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"getDailySalesForecastDetails");
			while (result.next()) {
				ForecastDto objForecast = new ForecastDto();
				objForecast.setForecastWg(result.getDouble("FORECAST_WG"));
				objForecast.setForeCastWog(result.getDouble("FORECAST_WOG"));
				objForecast.setForecastCtdId(result.getInt("FORECAST_CTD_ID"));
				objForecast.setCalendarId(result.getInt("CALENDAR_ID"));
				objForecast.setLocationId(result.getInt("LOCATION_ID"));
				objForecast.setLocationLevelId(result
						.getInt("LOCATION_LEVEL_ID"));
				objForecast.setIdForecastWg(result.getDouble("ID_FORECAST_WG"));
				objForecast.setIdForecastWog(result
						.getDouble("ID_FORECAST_WOG"));
				dailyForecastList.add(objForecast);

			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("getDailySalesForecastDetails Error",
					exe);
		}

		return dailyForecastList;
	}

	
	/**
	 * @param conn
	 * @param processYear
	 * @param rowType
	 * @param locationLevelId
	 * @return
	 * @throws GeneralException
	 */
	public List<ForecastDto> GetDistrictForecast(Connection conn,
			Date processYear, String rowType, int locationLevelId)
			throws GeneralException {

		List<ForecastDto> districtList = new ArrayList<ForecastDto>();
		
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(FORECAST_WG) as FORECAST_WG,sum(FORECAST_WOG) as FORECAST_WOG,A.CALENDAR_ID, DISTRICT_ID");
		sql.append(" from SALES_AGGR_FORECAST A , COMPETITOR_STORE b , RETAIL_CALENDAR c ");
		sql.append(" where B.DISTRICT_ID in (select ID from RETAIL_DISTRICT)");
		sql.append(" and A.LOCATION_ID  = B.COMP_STR_ID AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR = (Select Cal_Year From Retail_Calendar where ROW_TYPE='");
		sql.append(Constants.CALENDAR_DAY).append("' AND");
		sql.append(" start_date = to_date('" + formatter.format(processYear) + "', 'dd-mm-yy'))");		
		sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");
		sql.append(" group by  A.LOCATION_LEVEL_ID,A.CALENDAR_ID,DISTRICT_ID");
		sql.append(" order by A.CALENDAR_ID,DISTRICT_ID");

		logger.info(" Process District Query..... " + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"GetDistrictForecast");
			while (result.next()) {
				ForecastDto objForcastDto = new ForecastDto();
				objForcastDto.setLocationId(result.getInt("DISTRICT_ID"));
				objForcastDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objForcastDto.setForecastWg(result.getDouble("FORECAST_WG"));
				objForcastDto.setForeCastWog(result.getDouble("FORECAST_WOG"));
				districtList.add(objForcastDto);
			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("District Forecast Error", exe);
		}

		return districtList;
	}

	/**
	 * @param conn
	 * @param processYear
	 * @param rowType
	 * @param locationLevelId
	 * @return
	 * @throws GeneralException
	 */
	public List<ForecastDto> getRegionForeCast(Connection conn,
			Date processYear, String rowType, int locationLevelId)
			throws GeneralException {

		List<ForecastDto> regionList = new ArrayList<ForecastDto>();
	
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(FORECAST_WG) as FORECAST_WG,sum(FORECAST_WOG) as FORECAST_WOG,A.CALENDAR_ID, REGION_ID");
		sql.append(" ,sum(ID_FORECAST_WG) as IDFORECAST_WG,sum(ID_FORECAST_WOG) as IDFORECAST_WOG");
		sql.append(" from SALES_AGGR_FORECAST A , RETAIL_DISTRICT b, RETAIL_CALENDAR c  ");
		sql.append("  where B.ID = A.LOCATION_ID AND LOCATION_LEVEL_ID="
				+ locationLevelId + " ");
		sql.append(" AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR = (Select Cal_Year From Retail_Calendar where ROW_TYPE='");
		sql.append(Constants.CALENDAR_DAY).append("' AND");
		sql.append(" start_date = to_date('" + formatter.format(processYear) + "', 'dd-mm-yy'))");		
		sql.append(" AND C.ROW_TYPE='" + rowType + "'");
		sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");
		sql.append(" group by  A.LOCATION_LEVEL_ID,A.CALENDAR_ID,REGION_ID");
		sql.append(" order by A.CALENDAR_ID,REGION_ID");

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"getregionForecaste");
			while (result.next()) {
				ForecastDto objForcastDto = new ForecastDto();
				objForcastDto.setLocationId(result.getInt("REGION_ID"));
				objForcastDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objForcastDto.setForecastWg(result.getDouble("FORECAST_WG"));
				objForcastDto.setForeCastWog(result.getDouble("FORECAST_WOG"));
				objForcastDto
						.setIdForecastWg(result.getDouble("IDFORECAST_WG"));
				objForcastDto.setIdForecastWog(result
						.getDouble("IDFORECAST_WOG"));
				regionList.add(objForcastDto);
			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("Region Forecast Error", exe);
		}

		return regionList;
	}

	
	
	/**
	 * @param conn
	 * @param processYear
	 * @param rowType
	 * @param locationLevelId
	 * @return
	 * @throws GeneralException
	 */
	public List<ForecastDto> getDivisionForeCast(Connection conn,
			Date processYear, String rowType, int locationLevelId)
			throws GeneralException {

		List<ForecastDto> regionList = new ArrayList<ForecastDto>();
		
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(FORECAST_WG) as FORECAST_WG,sum(FORECAST_WOG) as FORECAST_WOG,A.CALENDAR_ID, DIVISION_ID");
		sql.append(" ,sum(ID_FORECAST_WG) as IDFORECAST_WG,sum(ID_FORECAST_WOG) as IDFORECAST_WOG");
		sql.append(" from SALES_AGGR_FORECAST A , RETAIL_REGION b, RETAIL_CALENDAR c  ");
		sql.append("  where B.ID = A.LOCATION_ID AND LOCATION_LEVEL_ID=").append(locationLevelId);
		sql.append(" AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR = (Select Cal_Year From Retail_Calendar where ROW_TYPE='");
		sql.append(Constants.CALENDAR_DAY).append("' AND");
		sql.append(" start_date = to_date('" + formatter.format(processYear) + "', 'dd-mm-yy'))");		
		sql.append(" AND C.ROW_TYPE='" + rowType + "'");
		sql.append(" and A.LOCATION_LEVEL_ID=").append(locationLevelId);
		sql.append(" group by  A.LOCATION_LEVEL_ID,A.CALENDAR_ID,DIVISION_ID");
		sql.append(" order by A.CALENDAR_ID,DIVISION_ID");

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"GetDistrictForecast");
			while (result.next()) {
				ForecastDto objForcastDto = new ForecastDto();
				objForcastDto.setLocationId(result.getInt("DIVISION_ID"));
				objForcastDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objForcastDto.setForecastWg(result.getDouble("FORECAST_WG"));
				objForcastDto.setForeCastWog(result.getDouble("FORECAST_WOG"));
				objForcastDto
						.setIdForecastWg(result.getDouble("IDFORECAST_WG"));
				objForcastDto.setIdForecastWog(result
						.getDouble("IDFORECAST_WOG"));
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
	 * @param objSalesForecastDao
	 * @param districtForeCast
	 * @param identicalDistrictForecast
	 * @param locationLevelId
	 * @param methodName
	 * @throws GeneralException
	 */
	public void forecastRollup(Connection conn,
			SalesForecastDAO objSalesForecastDao,
			List<ForecastDto> districtForeCast,
			HashMap<String, ForecastDto> identicalDistrictForecast,
			int locationLevelId, String methodName) throws GeneralException {

		try {
			PreparedStatement psmt = conn
					.prepareStatement(forecastInsert("DISTRICT"));

			for (int ii = 0; ii < districtForeCast.size(); ii++) {

				ForecastDto objForecast = districtForeCast.get(ii);

				objForecast.setLocationLevelId(locationLevelId);

				if (locationLevelId == 4) {

					if (identicalDistrictForecast.containsKey(objForecast
							.getLocationId()
							+ "_"
							+ objForecast.getCalendarId())) {

						ForecastDto identicalForecastDto = identicalDistrictForecast
								.get(objForecast.getLocationId() + "_"
										+ objForecast.getCalendarId());

						objForecast.setIdForecastWg(identicalForecastDto
								.getForecastWg());

						objForecast.setIdForecastWog(identicalForecastDto
								.getForeCastWog());

					}
				}

				addSqlBatch(psmt, objForecast, null, "Rollup", null);
			}

			// call the execute batch method to insert the daily records.
			objSalesForecastDao.excuteBatch(psmt, methodName);

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
		sql.append(" select location_id,calendar_id from sales_aggr_forecast");
		sql.append(" where calendar_id in(");
		Object[] calendarLoop = calendarList.values().toArray();
		for (int ii = 0; ii < calendarLoop.length; ii++) {
			if (ii > 0)
				sql.append(",");
			sql.append(calendarLoop[ii]);

		}
		sql.append(" )and location_level_id=" + locationLevelId + "");
		logger.debug("getPreviousForecast Sql :" + sql.toString());

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
		logger.debug("Previous Forecast List Size : " + forecastList.size());
		return forecastList;
	}

	/**
	 * @param insertPsmt
	 * @param updatePsmt
	 * @param previousForecast
	 * @param calendarList
	 * @param objForCastDto
	 * @param fileType
	 * @throws SQLException
	 */
	public void addDayBatch(PreparedStatement insertPsmt,
			PreparedStatement updatePsmt,
			HashMap<String, String> previousForecast,
			HashMap<String, Integer> calendarList, ForecastDto objForCastDto,
			String fileType) throws SQLException {

		// check the previous Sales Forecast records
		if (previousForecast.containsKey(objForCastDto.getLocationId() + "_"
				+ calendarList.get(objForCastDto.getProcessDate() + "_" + 1))) {

			updateForecast(fileType, updatePsmt, objForCastDto.getSundayWog(),
					objForCastDto.getSundayWg(), objForCastDto, calendarList, 1);

		} else {
			insertForecast(insertPsmt, objForCastDto.getSundayWog(),
					objForCastDto.getSundayWg(), objForCastDto, calendarList, 1);
		}

		if (previousForecast.containsKey(objForCastDto.getLocationId() + "_"
				+ calendarList.get(objForCastDto.getProcessDate() + "_" + 2))) {
			updateForecast(fileType, updatePsmt, objForCastDto.getMondayWog(),
					objForCastDto.getMondayWg(), objForCastDto, calendarList, 2);
		} else {
			insertForecast(insertPsmt, objForCastDto.getMondayWog(),
					objForCastDto.getMondayWg(), objForCastDto, calendarList, 2);
		}

		if (previousForecast.containsKey(objForCastDto.getLocationId() + "_"
				+ calendarList.get(objForCastDto.getProcessDate() + "_" + 3))) {
			updateForecast(fileType, updatePsmt, objForCastDto.getTuesdayWog(),
					objForCastDto.getTuesdayWg(), objForCastDto, calendarList,
					3);

		} else {
			insertForecast(insertPsmt, objForCastDto.getTuesdayWog(),
					objForCastDto.getTuesdayWg(), objForCastDto, calendarList,
					3);
		}

		if (previousForecast.containsKey(objForCastDto.getLocationId() + "_"
				+ calendarList.get(objForCastDto.getProcessDate() + "_" + 4))) {
			updateForecast(fileType, updatePsmt,
					objForCastDto.getWednesdayWog(),
					objForCastDto.getWednesdayWg(), objForCastDto,
					calendarList, 4);

		} else {
			insertForecast(insertPsmt, objForCastDto.getWednesdayWog(),
					objForCastDto.getWednesdayWg(), objForCastDto,
					calendarList, 4);
		}

		if (previousForecast.containsKey(objForCastDto.getLocationId() + "_"
				+ calendarList.get(objForCastDto.getProcessDate() + "_" + 5))) {
			updateForecast(fileType, updatePsmt,
					objForCastDto.getThursdayWog(),
					objForCastDto.getThursdayWg(), objForCastDto, calendarList,
					5);

		} else {
			insertForecast(insertPsmt, objForCastDto.getThursdayWog(),
					objForCastDto.getThursdayWg(), objForCastDto, calendarList,
					5);
		}

		if (previousForecast.containsKey(objForCastDto.getLocationId() + "_"
				+ calendarList.get(objForCastDto.getProcessDate() + "_" + 6))) {
			updateForecast(fileType, updatePsmt, objForCastDto.getFridayWog(),
					objForCastDto.getFridayWg(), objForCastDto, calendarList, 6);
		} else {
			insertForecast(insertPsmt, objForCastDto.getFridayWog(),
					objForCastDto.getFridayWg(), objForCastDto, calendarList, 6);
		}
		if (previousForecast.containsKey(objForCastDto.getLocationId() + "_"
				+ calendarList.get(objForCastDto.getProcessDate() + "_" + 7))) {
			updateForecast(fileType, updatePsmt,
					objForCastDto.getSaterdayWog(),
					objForCastDto.getSaterdayWg(), objForCastDto, calendarList,
					7);
		} else {
			insertForecast(insertPsmt, objForCastDto.getSaterdayWog(),
					objForCastDto.getSaterdayWg(), objForCastDto, calendarList,
					7);
		}

	}

	/**
	 * @param psmt
	 * @param wog
	 * @param wg
	 * @param objForCastDto
	 * @param calendarList
	 * @param cid
	 * @throws SQLException
	 */
	private void insertForecast(PreparedStatement psmt, Object wog, Object wg,
			ForecastDto objForCastDto, HashMap<String, Integer> calendarList,
			int cid) throws SQLException {
		psmt.setObject(1,
				calendarList.get(objForCastDto.getProcessDate() + "_" + cid));
		psmt.setObject(2, Constants.STORE_LEVEL_ID);
		psmt.setObject(3, objForCastDto.getLocationId());
		psmt.setObject(4, wg == null ? "" : wg);
		psmt.setObject(5, wog == null ? "" : wog);
		psmt.addBatch();
	}

	/**
	 * @param fileType
	 * @param psmt
	 * @param wog
	 * @param wg
	 * @param objForCastDto
	 * @param calendarList
	 * @param cid
	 * @throws SQLException
	 */
	private void updateForecast(String fileType, PreparedStatement psmt,
			Object wog, Object wg, ForecastDto objForCastDto,
			HashMap<String, Integer> calendarList, int cid) throws SQLException {

		if (fileType.equalsIgnoreCase("WG")) {
			psmt.setObject(1, wg);
			psmt.setObject(2, calendarList.get(objForCastDto.getProcessDate()
					+ "_" + cid));
			psmt.setObject(3, Constants.STORE_LEVEL_ID);
			psmt.setObject(4, objForCastDto.getLocationId());
		} else if (fileType.equalsIgnoreCase("WOG")) {
			psmt.setObject(1, wog);
			psmt.setObject(2, calendarList.get(objForCastDto.getProcessDate()
					+ "_" + cid));
			psmt.setObject(3, Constants.STORE_LEVEL_ID);
			psmt.setObject(4, objForCastDto.getLocationId());
		} else {
			psmt.setObject(1, wg);
			psmt.setObject(2, wog);
			psmt.setObject(3, calendarList.get(objForCastDto.getProcessDate()
					+ "_" + cid));
			psmt.setObject(4, objForCastDto.getLocationId());
			psmt.setObject(5, Constants.STORE_LEVEL_ID);
		}

		psmt.addBatch();

	}

	
	
	/**
	 * @param fileType
	 * @return
	 */
	public String forecastUpdate(String fileType) {

		StringBuffer sql = new StringBuffer();
		sql.append(" update SALES_AGGR_FORECAST set ");
		if (fileType.equalsIgnoreCase("WG")) {
			sql.append(" FORECAST_WG =?");
		} else if (fileType.equalsIgnoreCase("WOG")) {
			sql.append(" FORECAST_WOG=?");
		} else {
			sql.append(" FORECAST_WG =?,FORECAST_WOG=?");
		}
		sql.append(" where CALENDAR_ID=? and LOCATION_ID=? and LOCATION_LEVEL_ID=?");
		logger.debug("Update Query " + sql.toString());
		return sql.toString();
	}

	

	/**
	 * @param conn
	 * @param processYear
	 * @throws GeneralException
	 */
	public void deleteForecast(Connection conn, Date processYear)
			throws GeneralException {

		logger.debug("Delete Forecast Details");

		StringBuffer sql = new StringBuffer();
		
		sql.append(" Delete from SALES_AGGR_FORECAST");
		sql.append(" where CALENDAR_ID in");
		sql.append(" (select CALENDAR_ID from RETAIL_CALENDAR");
		
		sql.append(" where CAL_YEAR = (Select Cal_Year From Retail_Calendar where ROW_TYPE='");
		sql.append(Constants.CALENDAR_DAY).append("' AND");
		sql.append(" start_date = to_date('" + formatter.format(processYear) + "', 'dd-mm-yy'))");
		
		sql.append("and ROW_TYPE='").append(Constants.CALENDAR_DAY).append("')");
		sql.append(" and LOCATION_LEVEL_ID in(").append(Constants.DISTRICT_LEVEL_ID);
		sql.append(" ,").append(Constants.REGION_LEVEL_ID).append(",");
		sql.append(Constants.DIVISION_LEVEL_ID).append(",").append(Constants.CHAIN_LEVEL_ID).append(")");
		
		logger.debug("Sql :" + sql.toString());
		PristineDBUtil.execute(conn, sql, "Delete ForeCast");

		sql = new StringBuffer();
		sql.append(" delete from SALES_AGGR_FORECAST");
		sql.append(" where CALENDAR_ID in  (select CALENDAR_ID");
		sql.append(" from RETAIL_CALENDAR");
		
		sql.append(" where CAL_YEAR = (Select Cal_Year From Retail_Calendar where ROW_TYPE='");
		sql.append(Constants.CALENDAR_DAY).append("' AND");
		sql.append(" start_date = to_date('" + formatter.format(processYear) + "', 'dd-mm-yy'))");
		
		sql.append(" and ROW_TYPE in('" + Constants.CALENDAR_WEEK + "'");
		sql.append(" ,'" + Constants.CALENDAR_PERIOD + "','"
				+ Constants.CALENDAR_QUARTER + "','" + Constants.CALENDAR_YEAR
				+ "'))");
		logger.debug("Sql :" + sql.toString());
		PristineDBUtil.execute(conn, sql, "Delete ForeCast");
	}

	
	public HashMap<String, ForecastDto> GetidenticalDistrictForecast(
			Connection conn, Date processDate, String rowType,
			int locationLevelId) throws GeneralException {

		HashMap<String, ForecastDto> returnMap = new HashMap<String, ForecastDto>();
		
		StringBuffer sql = new StringBuffer();
		sql.append(" select sum(FORECAST_WG) as FORECAST_WG,sum(FORECAST_WOG) as FORECAST_WOG,A.CALENDAR_ID, DISTRICT_ID");
		sql.append(" from SALES_AGGR_FORECAST A , COMPETITOR_STORE b , RETAIL_CALENDAR c ");
		sql.append(" where B.DISTRICT_ID in (select ID from RETAIL_DISTRICT)");
		sql.append(" and A.LOCATION_ID  = B.COMP_STR_ID AND A.CALENDAR_ID = C.CALENDAR_ID ");
		sql.append(" AND C.CAL_YEAR = (Select Cal_Year From Retail_Calendar where ROW_TYPE='");
		sql.append(Constants.CALENDAR_DAY).append("' AND");
		sql.append(" start_date = to_date('" + formatter.format(processDate) + "', 'dd-mm-yy'))");
		sql.append(" AND C.ROW_TYPE='" + rowType + "'");
		sql.append(" and A.LOCATION_LEVEL_ID=" + locationLevelId + "");
		sql.append(" and b.OPEN_DATE < c.start_date-364");
		sql.append(" group by  A.LOCATION_LEVEL_ID,A.CALENDAR_ID,DISTRICT_ID");
		sql.append(" order by A.CALENDAR_ID,DISTRICT_ID");

		logger.info(" Process District Query..... " + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"GetDistrictForecast");
			while (result.next()) {
				ForecastDto objForcastDto = new ForecastDto();
				objForcastDto.setLocationId(result.getInt("DISTRICT_ID"));
				objForcastDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objForcastDto.setForecastWg(result.getDouble("FORECAST_WG"));
				objForcastDto.setForeCastWog(result.getDouble("FORECAST_WOG"));

				returnMap.put(objForcastDto.getLocationId() + "_"
						+ objForcastDto.getCalendarId(), objForcastDto);
			}

		} catch (Exception exe) {
			logger.error(exe);
			throw new GeneralException("District Forecast Error", exe);
		}

		return returnMap;

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
			Date processYear, String rowType, int locationLevelId, int _compChainId)
			throws GeneralException {

		// resturn list
		List<ForecastDto> returnList = new ArrayList<ForecastDto>();
			
		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" select sum(FORECAST_WG) as FORECAST_WG,sum(FORECAST_WOG) as FORECAST_WOG,A.CALENDAR_ID");
			sql.append(" ,sum(ID_FORECAST_WG) as IDFORECAST_WG,sum(ID_FORECAST_WOG) as IDFORECAST_WOG");
			sql.append(" from SALES_AGGR_FORECAST A ,RETAIL_CALENDAR B");
			sql.append("  where A.LOCATION_LEVEL_ID=").append(locationLevelId);
			sql.append(" and A.CALENDAR_ID = B.CALENDAR_ID ");
			sql.append(" AND B.CAL_YEAR = (Select Cal_Year From Retail_Calendar where ROW_TYPE='");
			sql.append(Constants.CALENDAR_DAY).append("' AND");
			sql.append(" start_date = to_date('" + formatter.format(processYear) + "', 'dd-mm-yy'))");		
			sql.append(" and B.ROW_TYPE='").append(rowType).append("'");
			sql.append(" group by  A.CALENDAR_ID");
			sql.append(" order by A.CALENDAR_ID");

			logger.debug(" Chian Forecast Aggregation Query....."
					+ sql.toString());
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
					"getChainForecast");

			while (result.next()) {

				ForecastDto objForcastDto = new ForecastDto();
				objForcastDto.setLocationId(_compChainId);
				objForcastDto.setCalendarId(result.getInt("CALENDAR_ID"));
				objForcastDto.setForecastWg(result.getDouble("FORECAST_WG"));
				objForcastDto.setForeCastWog(result.getDouble("FORECAST_WOG"));
				objForcastDto
						.setIdForecastWg(result.getDouble("IDFORECAST_WG"));
				objForcastDto.setIdForecastWog(result
						.getDouble("IDFORECAST_WOG"));
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

}
