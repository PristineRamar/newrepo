package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class MonthlyPriceIndexDAO {
	private static Logger logger = Logger.getLogger("MonthlyPriceIndexDAO");
	
	private static final String DELETE_COMPETITIVE_DATA = "DELETE FROM COMPETITIVE_DATA WHERE SCHEDULE_ID = ?";
	private static final String DELETE_COMPETITIVE_DATA_LIG = "DELETE FROM COMPETITIVE_DATA_LIG WHERE SCHEDULE_ID = ?";
	private static final String DELETE_COMPETITIVE_DATA_PI = "DELETE FROM COMPETITIVE_DATA_PI WHERE SCHEDULE_ID = ?";
	private static final String DELETE_MOVEMENT_WEEKLY = "DELETE FROM MOVEMENT_WEEKLY WHERE CHECK_DATA_ID IN "
			+ "(SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA WHERE SCHEDULE_ID = ?)";
	private static final String DELETE_MOVEMENT_WEEKLY_LIG = "DELETE FROM MOVEMENT_WEEKLY_LIG WHERE CHECK_DATA_ID IN "
			+ "(SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA_LIG WHERE SCHEDULE_ID = ?)";
	private static final String BASE_STORE_MONTHLY_COMPETITIVE_DATA ="INSERT INTO COMPETITIVE_DATA( CHECK_DATA_ID, SCHEDULE_ID, ITEM_CODE, "
			+ "REG_PRICE, REG_M_PACK, REG_M_PRICE, SALE_PRICE, SALE_M_PACK, SALE_M_PRICE, "
			+ "ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, OUTSIDE_RANGE_IND, CHECK_DATETIME, CREATE_DATETIME ) "
			+ "Select  COMP_CHECK_ID_SEQ.NEXTVAL,?,item_code, reg_price,0,0,ROUND(sale_price,2),0,0,'N','N',CASE WHEN sale_price > 0 THEN 'Y' ELSE 'N' "
			+ "END,'N', TO_DATE(?,'MM/DD/YYYY'),SYSDATE from ( SELECT ITEM_CODE, ROUND(REG_PRICE,2) AS REG_PRICE, "
			+ "DECODE(REG_PRICE - SALE_PRICE,0,0,SALE_PRICE) AS SALE_PRICE FROM ( SELECT ITEM_CODE,AVG(unitprice(REG_PRICE,REG_M_PRICE,REG_M_PACK)) "
			+ "AS REG_PRICE, AVG(FIND_FINAL_PRICE(REG_PRICE,REG_M_PRICE,REG_M_PACK,SALE_PRICE,SALE_M_PRICE,SALE_M_PACK)) AS "
			+ "SALE_PRICE FROM COMPETITIVE_DATA WHERE SCHEDULE_ID IN (SELECT SCHEDULE_ID FROM SCHEDULE WHERE START_DATE "
			+ "BETWEEN TO_DATE(?, 'MM/dd/yyyy') AND TO_DATE(?, 'MM/dd/yyyy') AND LOCATION_LEVEL_ID=? AND LOCATION_ID=?) GROUP BY ITEM_CODE))";
	private static final String COMP_STORE_MONTHLY_COMPETITIVE_DATA ="INSERT INTO COMPETITIVE_DATA( CHECK_DATA_ID, SCHEDULE_ID, ITEM_CODE, "
			+ "REG_PRICE, REG_M_PACK, REG_M_PRICE, SALE_PRICE, SALE_M_PACK, SALE_M_PRICE, "
			+ "ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, OUTSIDE_RANGE_IND, CHECK_DATETIME, CREATE_DATETIME ) "
			+ "Select  COMP_CHECK_ID_SEQ.NEXTVAL,?,item_code, reg_price,0,0,ROUND(sale_price,2),0,0,'N','N',CASE WHEN sale_price > 0 THEN 'Y' ELSE 'N' "
			+ "END,'N', TO_DATE(?,'MM/DD/YYYY'),SYSDATE from ( SELECT ITEM_CODE, ROUND(REG_PRICE,2) AS REG_PRICE, "
			+ "DECODE(REG_PRICE - SALE_PRICE,0,0,SALE_PRICE) AS SALE_PRICE FROM ( SELECT ITEM_CODE,AVG(unitprice(REG_PRICE,REG_M_PRICE,REG_M_PACK)) "
			+ "AS REG_PRICE, AVG(FIND_FINAL_PRICE(REG_PRICE,REG_M_PRICE,REG_M_PACK,SALE_PRICE,SALE_M_PRICE,SALE_M_PACK)) AS "
			+ "SALE_PRICE FROM COMPETITIVE_DATA WHERE SCHEDULE_ID IN (SELECT SCHEDULE_ID FROM SCHEDULE WHERE START_DATE "
			+ "BETWEEN TO_DATE(?, 'MM/dd/yyyy') AND TO_DATE(?, 'MM/dd/yyyy') AND COMP_STR_ID = ?) GROUP BY ITEM_CODE))";
	private static final String INSERT_MONTHLY_MOVEMENT_WEEKLY ="INSERT INTO MOVEMENT_WEEKLY (ITEM_CODE, CHECK_DATA_ID, REVENUE_REGULAR, "
			+ "QUANTITY_REGULAR, REVENUE_SALE,QUANTITY_SALE, LIST_COST,DEAL_COST, MARGIN_PCT, TOTAL_MARGIN, "
			+ "VISIT_COUNT, LOCATION_LEVEL_ID, LOCATION_ID) SELECT CD.ITEM_CODE,CD.CHECK_DATA_ID,SUM(WI.REVENUE_REGULAR), "
			+ "SUM(WI.QUANTITY_REGULAR),SUM(WI.REVENUE_SALE),SUM(WI.QUANTITY_SALE), ROUND(AVG(WI.LIST_COST),2), "
			+ "case when sum(tot_mov) >0 then ROUND((SUM(WI.WEIGHTED_COST) / sum(tot_mov)),2) else 0 End AS DEAL_COST, "
			+ "CASE WHEN SUM(WI.REVENUE_SALE + WI.REVENUE_REGULAR) >0 THEN ROUND((sum(WI.total_margin)/SUM(WI.REVENUE_SALE + WI.REVENUE_REGULAR)),2) "
			+ "ELSE 0 END AS MARGIN_PCT, sum(WI.total_margin),SUM(WI.VISIT_COUNT), ?,? from ("
			+ "SELECT item_code,revenue_regular,quantity_regular,revenue_sale,quantity_sale,list_cost,deal_cost, "
			+ "CASE WHEN DEAL_COST>0 THEN (DEAL_COST * (QUANTITY_REGULAR+QUANTITY_SALE)) ELSE (LIST_COST * (QUANTITY_REGULAR+QUANTITY_SALE)) END "
			+ "AS WEIGHTED_COST,(QUANTITY_REGULAR+QUANTITY_SALE)as tot_mov, MARGIN_PCT,TOTAL_MARGIN, VISIT_COUNT "
			+ "FROM MOVEMENT_WEEKLY WHERE CHECK_DATA_ID IN (SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA WHERE SCHEDULE_ID IN ( "
			+ "SELECT SCHEDULE_ID FROM SCHEDULE WHERE START_DATE BETWEEN TO_DATE(?, 'MM/dd/yyyy') AND TO_DATE(?, 'MM/dd/yyyy') AND LOCATION_LEVEL_ID=? AND LOCATION_ID=?))) WI "
			+ "INNER JOIN COMPETITIVE_DATA CD ON WI.ITEM_CODE = CD.ITEM_CODE WHERE  CD.SCHEDULE_ID IN ( "
			+ "SELECT SCHEDULE_ID FROM SCHEDULE WHERE START_DATE =TO_DATE(?, 'MM/dd/yyyy') AND LOCATION_LEVEL_ID=? AND LOCATION_ID=?) "
			+ "group by CD.item_code,CD.CHECK_DATA_ID";
	private static final String INSERT_MONTHLY_MOVEMENT_WEEKLY_LIG ="INSERT INTO MOVEMENT_WEEKLY_LIG (ITEM_CODE, CHECK_DATA_ID, REVENUE_REGULAR, "
			+ "QUANTITY_REGULAR, REVENUE_SALE,QUANTITY_SALE, LIST_COST,DEAL_COST, MARGIN_PCT, TOTAL_MARGIN, "
			+ "VISIT_COUNT, LOCATION_LEVEL_ID, LOCATION_ID) SELECT CD.ITEM_CODE,CD.CHECK_DATA_ID,SUM(WI.REVENUE_REGULAR), "
			+ "SUM(WI.QUANTITY_REGULAR),SUM(WI.REVENUE_SALE),SUM(WI.QUANTITY_SALE), ROUND(AVG(WI.LIST_COST),2), "
			+ "case when sum(tot_mov) >0 then ROUND((SUM(WI.WEIGHTED_COST) / sum(tot_mov)),2) else 0 End AS DEAL_COST, "
			+ "CASE WHEN SUM(WI.REVENUE_SALE + WI.REVENUE_REGULAR) >0 THEN ROUND((sum(WI.total_margin)/SUM(WI.REVENUE_SALE + WI.REVENUE_REGULAR)),2) "
			+ "ELSE 0 END AS MARGIN_PCT, sum(WI.total_margin),SUM(WI.VISIT_COUNT), ?,? from ("
			+ "SELECT item_code,revenue_regular,quantity_regular,revenue_sale,quantity_sale,list_cost,deal_cost, "
			+ "CASE WHEN DEAL_COST>0 THEN (DEAL_COST * (QUANTITY_REGULAR+QUANTITY_SALE)) ELSE (LIST_COST * (QUANTITY_REGULAR+QUANTITY_SALE)) END "
			+ "AS WEIGHTED_COST,(QUANTITY_REGULAR+QUANTITY_SALE)as tot_mov, MARGIN_PCT,TOTAL_MARGIN, VISIT_COUNT "
			+ "FROM MOVEMENT_WEEKLY_LIG WHERE CHECK_DATA_ID IN (SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA_LIG WHERE SCHEDULE_ID IN ( "
			+ "SELECT SCHEDULE_ID FROM SCHEDULE WHERE START_DATE BETWEEN TO_DATE(?, 'MM/dd/yyyy') AND TO_DATE(?, 'MM/dd/yyyy') AND LOCATION_LEVEL_ID=? AND LOCATION_ID=?))) WI "
			+ "INNER JOIN COMPETITIVE_DATA_LIG CD ON WI.ITEM_CODE = CD.ITEM_CODE WHERE  CD.SCHEDULE_ID IN ( "
			+ "SELECT SCHEDULE_ID FROM SCHEDULE WHERE START_DATE =TO_DATE(?, 'MM/dd/yyyy') AND LOCATION_LEVEL_ID=? AND LOCATION_ID=?) "
			+ "group by CD.item_code,CD.CHECK_DATA_ID";
	private static final String POPULATE_MARKET_DATA_IN_MOVEMENT_WEEKLY ="UPDATE (SELECT CM.ITEM_CODE,MW.ITEM_CODE, CM.TOT_MOVEMENT, mw.qty_regular_13wk FROM "
			+ "(select item_code, tot_movement from (select MD.ITEM_CODE as item_code, sum(md.market_movement) as tot_movement "
			+ " from (select ITEM_CODE, (NVL(OUR_TA_UNITS, 0) + NVL(TOT_REM_TA_UNITS, 0)) as market_movement "
			+ " from market_data_v2 where item_code is not null "
			+ " AND DATE_RECEIVED= TO_DATE(?, 'MM/dd/yy')) MD, item_lookup il where "
			+ "il.item_code = md.ITEM_CODE "
			+ "and il.item_code IN (SELECT ITEM_CODE FROM item_lookup WHERE LIR_IND='N') group by md.ITEM_CODE)) CM, MOVEMENT_WEEKLY MW "
			+ "WHERE MW.ITEM_CODE = CM.ITEM_CODE AND MW.CHECK_DATA_ID IN (SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA WHERE SCHEDULE_ID = ?)) "
			+ "SET QTY_REGULAR_13WK = TOT_MOVEMENT";
	private static final String POPULATE_MARKET_DATA_IN_MOVEMENT_WEEKLY_LIG ="UPDATE (SELECT CM.ITEM_CODE,MWL.ITEM_CODE, CM.TOT_MOVEMENT, "
			+ "MWL.qty_regular_13wk FROM (select item_code, tot_movement from (select il.ret_lir_id, "
			+ " sum(md.market_movement) as tot_movement from (select ITEM_CODE, (NVL(OUR_TA_UNITS, 0) + NVL(TOT_REM_TA_UNITS, 0)) as market_movement "
			+ " from market_data_v2 where item_code is not null "
			+ " AND DATE_RECEIVED= TO_DATE(?, 'MM/dd/yy')) md, item_lookup il "
			+ " where il.item_code = md.ITEM_CODE and il.ret_lir_id is not null and md.item_code in (select item_code from "
			+ "movement_weekly where CHECK_DATA_ID IN (SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA WHERE SCHEDULE_ID = ?)) group by il.ret_lir_id)CM "
			+ "INNER join ITEM_LOOKUP IL ON il.ret_lir_id=CM.RET_LIR_ID WHERE IL.LIR_IND='Y') CM, movement_weekly_lig MWL "
			+ "WHERE MWL.ITEM_CODE = CM.ITEM_CODE AND MWL.CHECK_DATA_ID IN (SELECT CHECK_DATA_ID FROM COMPETITIVE_DATA_LIG WHERE SCHEDULE_ID = ? "
			+ ")) SET QTY_REGULAR_13WK = TOT_MOVEMENT";
	private static final String GET_MOST_RECENT_DATE_FROM_MARKET_DATA ="SELECT TO_CHAR(MAX(DATE_RECEIVED),'MM/dd/yy') AS RECENT_DATE FROM MARKET_DATA_V2 ";
	public void deleteExistingCompData(Connection conn, int scheduleId) throws GeneralException{
		logger.info("Delete existing data based on Schedule Id");
		deleteCompetitiveDataPI(conn, scheduleId);
		deleteMovementWeeklyLig(conn, scheduleId);
		deleteMovementWeekly(conn, scheduleId);
		deleteCompetitiveDataLig(conn, scheduleId);
		deleteCompetitiveData(conn, scheduleId);
	}
	
	public void deleteCompetitiveData(Connection conn, int scheduleId) throws GeneralException{
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(DELETE_COMPETITIVE_DATA);
			statement.setInt(1, scheduleId);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing DELETE_COMPETITIVE_DATA", e);
		}
	}
	
	public void deleteCompetitiveDataLig(Connection conn, int scheduleId) throws GeneralException{
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(DELETE_COMPETITIVE_DATA_LIG);
			statement.setInt(1, scheduleId);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing DELETE_COMPETITIVE_DATA_LIG", e);
		}
	}
	
	public void deleteMovementWeekly(Connection conn, int scheduleId) throws GeneralException{
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(DELETE_MOVEMENT_WEEKLY);
			statement.setInt(1, scheduleId);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing DELETE_MOVEMENT_WEEKLY", e);
		}
	}
	
	public void deleteMovementWeeklyLig(Connection conn, int scheduleId) throws GeneralException{
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(DELETE_MOVEMENT_WEEKLY_LIG);
			statement.setInt(1, scheduleId);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing DELETE_MOVEMENT_WEEKLY_LIG", e);
		}
	}
	public void deleteCompetitiveDataPI(Connection conn, int scheduleId) throws GeneralException{
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(DELETE_COMPETITIVE_DATA_PI);
			statement.setInt(1, scheduleId);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing DELETE_COMPETITIVE_DATA_PI", e);
		}
	}
	
	public void populateBaseStrMonthlyCompData(Connection conn, int scheduleId, String prevWeekStartDate, String startDate, String endDate,
			int locationId, int locationLevelId) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(BASE_STORE_MONTHLY_COMPETITIVE_DATA);
			int counter = 0;
			stmt.setInt(++counter, scheduleId);
			stmt.setString(++counter, startDate);
			stmt.setString(++counter, prevWeekStartDate);
			stmt.setString(++counter, startDate);
			stmt.setInt(++counter, locationLevelId);
			stmt.setInt(++counter, locationId);
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing populateBaseStrMonthlyCompData()", e);
		}
	}
	
	public void populateMonthlyMovementData(Connection conn, int scheduleId, String prevWeekStartDate, String startDate, String endDate,
			int srcLocationId, int descLocationId, int locationLevelId, String processType) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			if(processType.equals("WEEKLY")){
				stmt = conn.prepareStatement(INSERT_MONTHLY_MOVEMENT_WEEKLY);
			}else if(processType.equals("LIG")){
				stmt = conn.prepareStatement(INSERT_MONTHLY_MOVEMENT_WEEKLY_LIG);
			}
			int counter = 0;
			stmt.setInt(++counter, locationLevelId);
			stmt.setInt(++counter, descLocationId);
			stmt.setString(++counter, prevWeekStartDate);
			stmt.setString(++counter, startDate);
			stmt.setInt(++counter, locationLevelId);
			stmt.setInt(++counter, srcLocationId);
			stmt.setString(++counter, startDate);
			stmt.setInt(++counter, locationLevelId);
			stmt.setInt(++counter, descLocationId);
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing populateMonthlyMovementData()", e);
		}
	}
	
	public void populateMarketMovInMovementWeekly(Connection conn, int scheduleId, String mdProcessingDate) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(POPULATE_MARKET_DATA_IN_MOVEMENT_WEEKLY);
			int counter = 0;
			stmt.setString(++counter, mdProcessingDate);
			stmt.setInt(++counter, scheduleId);
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing populate Market Movement in MovementWeekly()", e);
		}
	}
	
	public void populateMarketMovInMovementWeeklyLig(Connection conn, int scheduleId, String mdProcessingDate) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			logger.debug("Movement Data for Lig query: "+POPULATE_MARKET_DATA_IN_MOVEMENT_WEEKLY_LIG);
			logger.debug("Schedule Id:"+scheduleId);
			stmt = conn.prepareStatement(POPULATE_MARKET_DATA_IN_MOVEMENT_WEEKLY_LIG);
			int counter = 0;
			stmt.setString(++counter, mdProcessingDate);
			stmt.setInt(++counter, scheduleId);
			stmt.setInt(++counter, scheduleId);
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing populate Market Movement in MovementWeeklyLig()", e);
		}
	}
	
	public void populateCompStrMonthlyCompData(Connection conn, int scheduleId, String prevWeekStartDate, String startDate, String endDate,
			int compStrId) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(COMP_STORE_MONTHLY_COMPETITIVE_DATA);
			int counter = 0;
			stmt.setInt(++counter, scheduleId);
			stmt.setString(++counter, startDate);
			stmt.setString(++counter, prevWeekStartDate);
			stmt.setString(++counter, startDate);
			stmt.setInt(++counter, compStrId);
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing populateCompStrMonthlyCompData()", e);
		}
	}
	
	public String getMostRecentDateFromMD(Connection conn) throws GeneralException {
		String recentDate = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_MOST_RECENT_DATE_FROM_MARKET_DATA);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				recentDate = resultSet.getString("RECENT_DATE");
			}
		} catch (SQLException e) {
			logger.error("Error while executing getMostRecentDateFromMD() " + e);
			throw new GeneralException("Error while executing getMostRecentDateFromMD() " + e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return recentDate;

	}
}
