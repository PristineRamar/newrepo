package com.pristine.dao.offermgmt.oos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.DayPartLookupDTO;
import com.pristine.dto.offermgmt.oos.OOSExpectationDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.oos.OOSDayPartDetail;
import com.pristine.service.offermgmt.oos.OOSDayPartDetailKey;
import com.pristine.service.offermgmt.oos.OOSService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class OOSExpectationDAO {
	private static Logger logger = Logger.getLogger("OOSExpectationDAO");
	
	private static final String GET_DAY_PART_TOTAL_STORE_TRANS = 
			" SELECT COUNT(DISTINCT CALENDAR_ID || TRX_NO) AS TRX_COUNT "
			+ " FROM "
			+ " (SELECT TL.CALENDAR_ID, TL.TRX_NO, "
			+ " CASE WHEN (TL.TRX_TIME >= ? AND TL.TRX_TIME < ?) THEN 1 ELSE 0 END AS FILTER_ID "
			+ " FROM TRANSACTION_LOG TL "
			//TODO:: For debugging, comment below line and uncomment above line
			//+ " FROM TRANSACTION_LOG_T1 TL "
			+ " LEFT JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = TL.CALENDAR_ID "
			+ " WHERE TL.STORE_ID = ? AND (RC.START_DATE = TO_CHAR(?, 'dd-MON-yy') OR "
			+ " RC.START_DATE = TO_CHAR(?, 'dd-MON-yy')) AND POS_DEPARTMENT_ID < 37 ) WHERE FILTER_ID = 1";
	
	private static final String GET_DAY_PARTS_TOTAL_STORE_TRANS = 
			" SELECT CALENDAR_ID, DAY_PART_ID, COUNT(DISTINCT CALENDAR_ID || TRX_NO) AS TRX_COUNT "
			+ " FROM ("
			+ " SELECT  RC1.CALENDAR_ID, TRX_NO, NEW_TRX_DATE, DAY_PART_ID FROM "
			+ " (SELECT TL.TRX_NO, "
			// Day Case used to group by day
			+ " %DAY_CASE% ,"
			+ " %DAY_PART_CASE% "
			+ " FROM TRANSACTION_LOG TL "
			//TODO:: For debugging, comment below line and uncomment above line
			//+ " FROM TRANSACTION_LOG_T1 TL "
			+ " LEFT JOIN RETAIL_CALENDAR RC ON RC.CALENDAR_ID = TL.CALENDAR_ID "
			+ " WHERE TL.STORE_ID = ? AND (RC.START_DATE = TO_CHAR(?, 'dd-MON-yy') OR "
			+ " RC.START_DATE = TO_CHAR(?, 'dd-MON-yy')) AND POS_DEPARTMENT_ID < 37 ) "
			+ " LEFT JOIN RETAIL_CALENDAR RC1 ON NEW_TRX_DATE = RC1.START_DATE WHERE ROW_TYPE  ='D') "
			+ "WHERE NEW_TRX_DATE >= TO_CHAR(?, 'dd-MON-yy') AND NEW_TRX_DATE   <= TO_CHAR(?, 'dd-MON-yy') "
			+ " GROUP BY CALENDAR_ID, DAY_PART_ID";
	
	public void clearOOSExpectation( Connection conn, int storeId, int calendarID) throws GeneralException {
		
		StringBuffer sql = new StringBuffer();
		sql.append(" DELETE FROM OOS_TRX_BASED_EXPECTATION where STORE_ID = "); 
		sql.append(storeId);
		if( calendarID > 0){
			sql.append(" AND CALENDAR_ID = ");
			sql.append(calendarID);
		}
		PristineDBUtil.execute(conn, sql, "clearOOSExpectation");
		
	}


	public List<OOSItemDTO> getHighMoverItems(Connection conn, int storeId,
			int weekStartDayCalendarId, int highMoverCutOff) throws GeneralException {
		// TODO Auto-generated method stub
		
		List<OOSItemDTO>  highMoverList = new ArrayList<OOSItemDTO> ();
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT A.item_id, B.ret_lir_id, SUM(QUANTITY) quantity from Transaction_log A, item_lookup B "); 
		sql.append(" where A.store_id = ").append(storeId);
		sql.append(" and A.calendar_id in (").append(weekStartDayCalendarId).append(")");
		sql.append(" and B.ITEM_CODE = A.ITEM_ID");
		sql.append(" and A.POS_DEPARTMENT_ID <37");
		sql.append(" group by A.item_id, B.ret_lir_id");
		sql.append(" having SUM(QUANTITY) >= ").append(highMoverCutOff);
		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sql, "GetHighMoverItems");
		try{
			while( crs.next()){
				OOSItemDTO itemDto = new OOSItemDTO();
				itemDto.setProductId(crs.getInt("ITEM_ID"));
				itemDto.setRetLirId(crs.getInt("RET_LIR_ID"));
				highMoverList.add(itemDto);
			}
		}catch (SQLException sqle)
		{
			throw new GeneralException(" Exception in reading cached row set ", sqle);
		}
		
		return highMoverList;
	}


	public int getStoreLevelTrxCount(Connection conn, int storeId, String prevCalendarIds) throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append(" select count(distinct A.calendar_id || A.TRX_NO) from transaction_log A ");
		sb.append(" where A.store_id = ").append(storeId);
		sb.append(" and A.calendar_id in  (").append(prevCalendarIds).append (")");
		sb.append(" and A.POS_DEPARTMENT_ID <37");
		String retVal = PristineDBUtil.getSingleColumnVal(conn, sb, "getStoreLevelTrxCount");
		return Integer.parseInt(retVal);

	}


	public ArrayList<OOSExpectationDTO> getItemLevelTrxStats(Connection conn,
			int storeId, String prevCalendarIds, int storeLevelTrxCount, String itemList) throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append("  select A.item_id, count(distinct A.calendar_id || A.TRX_NO) TRX_COUNT, sum(A.quantity) QUANTITY");
		sb.append("  from transaction_log A where A.store_id =  ").append(storeId);
		sb.append(" and A.calendar_id in  (").append(prevCalendarIds).append (")");
		sb.append(" and A.POS_DEPARTMENT_ID <37");
		sb.append(" and A.item_id in  (").append(itemList).append (")");
		sb.append("  group by A.item_id ");
		
		ArrayList<OOSExpectationDTO>  itemTrxList = new ArrayList<OOSExpectationDTO> ();
		
		try{
			CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getItemLevelTrxCount");
			while( crs.next()){
				OOSExpectationDTO itemTrxInfo = new OOSExpectationDTO();
				
				itemTrxInfo.setStoreId(storeId);
				itemTrxInfo.setStoreLevelTrxCount(storeLevelTrxCount);
				itemTrxInfo.setProductLevelId(Constants.ITEMLEVELID);
				itemTrxInfo.setProductId(crs.getInt("ITEM_ID"));
				itemTrxInfo.setItemLevelTrxCount(crs.getInt("TRX_COUNT"));
				itemTrxInfo.setItemLevelUnitsCount(crs.getInt("QUANTITY"));
				itemTrxList.add(itemTrxInfo);
			}
		}catch (SQLException sqle)
		{
			throw new GeneralException(" Exception in reading cached row set ", sqle);
		}
		
		return itemTrxList;
	}

	private static final String INSERT_OOS_EXPECTATION = " INSERT INTO OOS_TRX_BASED_EXPECTATION ( " 
			+ " calendar_id, STORE_ID, product_LEVEL_ID,product_id, STORE_level_trx_cnt, item_level_trx_cnt, ITEM_LEVEL_units ) "
			+ " VALUES ( ?, ?, ?, ?, ?, ?, ?) ";
	
	public void insertExpectationInfo(Connection conn, int calendarId,
			List<OOSExpectationDTO> itemLevelTrxList) throws GeneralException{
		
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(INSERT_OOS_EXPECTATION);
			int insertCount = 0;
			for (OOSExpectationDTO expectationInfo: itemLevelTrxList) {
				int counter = 0;

				stmt.setInt(++counter, calendarId);
				stmt.setInt(++counter, expectationInfo.getStoreId());
				stmt.setInt(++counter, expectationInfo.getProductLevelId());
				stmt.setInt(++counter, expectationInfo.getProductId());
				stmt.setInt(++counter, expectationInfo.getStoreLevelTrxCount());
				stmt.setInt(++counter, expectationInfo.getItemLevelTrxCount());
				stmt.setInt(++counter, expectationInfo.getItemLevelUnitsCount());

				stmt.addBatch();
				insertCount++;

			}
			if (insertCount > 0) {
				stmt.executeBatch();
				stmt.clearBatch();
			}
		}catch (SQLException ex) {
			logger.error("Error in insertExpectationInfo() -- " + ex.toString(), ex);
			throw new GeneralException("insertExpectationInfo", ex);
		} finally {
			PristineDBUtil.close(stmt);
		}	
	}


	public ArrayList<OOSExpectationDTO> getLirLevelTrxStats(Connection conn,
			int storeId, String prevCalendarIds, int storeLevelTrxCount,
			String lirListStr, String itemListStr) throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append("  select b.ret_lir_id, count(distinct A.calendar_id || A.TRX_NO) TRX_COUNT, sum(A.quantity) QUANTITY");
		sb.append("  from transaction_log A, item_lookup B where A.store_id =  ").append(storeId);
		sb.append(" and A.calendar_id in  (").append(prevCalendarIds).append (")");
		sb.append(" and A.POS_DEPARTMENT_ID <37");
		sb.append(" and A.item_id in  (").append(itemListStr).append (")");
		sb.append(" and B.item_code = A.item_id ");
		sb.append(" and B.ret_lir_id in  (").append(lirListStr).append (")");
		sb.append("  group by B.ret_lir_id ");
		
		logger.debug("** LIR Count Query is " + sb.toString());
		
		ArrayList<OOSExpectationDTO>  lirTrxStatsList = new ArrayList<OOSExpectationDTO> ();
		
		try{
			CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getItemLevelTrxCount");
			while( crs.next()){
				OOSExpectationDTO lirTrxInfo = new OOSExpectationDTO();
				
				lirTrxInfo.setStoreId(storeId);
				lirTrxInfo.setStoreLevelTrxCount(storeLevelTrxCount);
				lirTrxInfo.setProductLevelId(11);
				lirTrxInfo.setProductId(crs.getInt("RET_LIR_ID"));
				lirTrxInfo.setItemLevelTrxCount(crs.getInt("TRX_COUNT"));
				lirTrxInfo.setItemLevelUnitsCount(crs.getInt("QUANTITY"));
				if( lirTrxInfo.getItemLevelUnitsCount() >99999){
					logger.debug("*** High Qty Count ****" + lirTrxInfo.getItemLevelUnitsCount());
					logger.debug("*** High Qty Count ****" + lirTrxInfo.getProductId());
				}
				lirTrxStatsList.add(lirTrxInfo);
			}
		}catch (SQLException sqle)
		{
			throw new GeneralException(" Exception in reading cached row set ", sqle);
		}
		
		return lirTrxStatsList;
	}

	public int getDayPartTotalTransOfStore(Connection conn, int storeId, Timestamp startTime, Timestamp endTime) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		int totalTransactions = 0;
		try {
			String query = new String(GET_DAY_PART_TOTAL_STORE_TRANS);
			statement = conn.prepareStatement(query);
			statement.setTimestamp(1, startTime);
			statement.setTimestamp(2, endTime);
			statement.setInt(3, storeId);
			statement.setTimestamp(4, startTime);
			statement.setTimestamp(5, endTime);
			statement.setFetchSize(200000);
			resultSet = statement.executeQuery();

			if (resultSet.next()) {
				totalTransactions = resultSet.getInt("TRX_COUNT");
			}
		} catch (SQLException sqlE) {
			logger.error("Error in getDayPartTotalTransOfStore()", sqlE);
			throw new GeneralException("Error in getDayPartTotalTransOfStore()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return totalTransactions;
	}
	
	public void getDayPartTotalTransOfStore(Connection conn, List<DayPartLookupDTO> dayPartLookup,
			HashMap<OOSDayPartDetailKey, OOSDayPartDetail> prevAndProcDayPartDetail, int storeId, Timestamp startTime, Timestamp endTime)
					throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		OOSService oosService = new OOSService();
		OOSDayPartDetailKey oosDayPartDetailKey;
		OOSDayPartDetail oosDayPartDetail;
		try {
			String query = new String(GET_DAY_PARTS_TOTAL_STORE_TRANS);
			// Fill Day Case
			query = query.replaceAll("%DAY_CASE%", oosService.fillDayCase(dayPartLookup));
						
			query = query.replaceAll("%DAY_PART_CASE%", oosService.fillDayPartCase(dayPartLookup));

			statement = conn.prepareStatement(query);
			statement.setInt(1, storeId);
			statement.setTimestamp(2, startTime);
			statement.setTimestamp(3, endTime);
			statement.setTimestamp(4, startTime);
			statement.setTimestamp(5, endTime);
			statement.setFetchSize(200000);
			resultSet = statement.executeQuery();
			logger.debug("GET_DAY_PARTS_TOTAL_STORE_TRANS:" + query);
			while (resultSet.next()) {
				int calendarId = resultSet.getInt("CALENDAR_ID");
				int dayPartId = resultSet.getInt("DAY_PART_ID");
				int trxCnt = resultSet.getInt("TRX_COUNT");

				oosDayPartDetailKey = new OOSDayPartDetailKey(calendarId, dayPartId, 0, 0);
				oosDayPartDetail = new OOSDayPartDetail();
				oosDayPartDetail.setTransactionCount(trxCnt);
				
				prevAndProcDayPartDetail.put(oosDayPartDetailKey, oosDayPartDetail);
				
			}
		} catch (SQLException sqlE) {
			logger.error("Error in getDayPartTotalTransOfStore()", sqlE);
			throw new GeneralException("Error in getDayPartTotalTransOfStore()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}
}

