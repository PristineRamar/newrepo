package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class CompDataPIDAO implements IDAO {
	private static Logger	logger	= Logger.getLogger("CompDataDAOPI");
	
	public void deleteDataForSchedule( Connection conn, int schId, int itemCode) throws GeneralException{
		
		StringBuffer sb = new StringBuffer();
		sb.append(" delete from COMPETITIVE_DATA_PI WHERE SCHEDULE_ID = ").append(schId);
		if (itemCode > 0 )
			sb.append(" AND ITEM_CODE = ").append(itemCode);
		
		PristineDBUtil.execute(conn, sb, "deleteLIGDataForSchedule");
	}
	
	public void insertLIGLevelData ( Connection conn, int scheduleId) throws GeneralException {
		
		StringBuffer sb = BuildQueryForCompDataPILIG();
		sb.append(" FROM COMPETITIVE_DATA_LIG A, MOVEMENT_WEEKLY_LIG B ");
		sb.append(" WHERE A.SCHEDULE_ID = ").append(scheduleId);
		sb.append(" AND B.CHECK_DATA_ID (+)= A.CHECK_DATA_ID ");
		logger.debug("insertLIGLevelData "+ sb.toString() +" schedule id "+ scheduleId);
		PristineDBUtil.execute(conn, sb, "PI - LIG level data setup");
		
	}

	private StringBuffer BuildQueryForCompDataPILIG() {
		StringBuffer sb = new StringBuffer();
		sb.append(" INSERT INTO COMPETITIVE_DATA_PI ( ");
		sb.append(" CHECK_DATA_ID, SCHEDULE_ID, ITEM_CODE, REG_PRICE, SALE_PRICE, REG_M_PACK, REG_M_PRICE, ");
		sb.append(" SALE_M_PACK, SALE_M_PRICE, ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, ");
		sb.append(" EFF_SALE_END_DATE, CHECK_DATETIME, CREATE_DATETIME, CHANGE_DIRECTION, CHANGE_DIRECTION_SALE, ");
		sb.append(" EFF_SALE_START_DATE, EFF_REG_START_DATE, REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, ");
		sb.append(" QUANTITY_SALE, LIST_COST, EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE,");
		sb.append(" COST_CHG_DIRECTION, MARGIN_CHG_DIRECTION, MARGIN_PCT,  TOTAL_MARGIN, VISIT_COUNT, ");
		sb.append(" QTY_REGULAR_13WK, QTY_SALE_13WK, LIG_PRICE_VARIATION, IS_ZONE_PRICE_DIFF ) ");
		sb.append(" SELECT ");
		sb.append(" A.CHECK_DATA_ID, A.SCHEDULE_ID, A.ITEM_CODE, A.REG_PRICE, A.SALE_PRICE, A.REG_M_PACK, A.REG_M_PRICE, ");
		sb.append(" A.SALE_M_PACK, A.SALE_M_PRICE, A.ITEM_NOT_FOUND_FLG, A.PRICE_NOT_FOUND_FLG, A.PROMOTION_FLG, ");
		sb.append(" A.EFF_SALE_END_DATE, A.CHECK_DATETIME, A.CREATE_DATETIME, A.CHANGE_DIRECTION, A.CHANGE_DIRECTION_SALE, ");
		sb.append(" A.EFF_SALE_START_DATE, A.EFF_REG_START_DATE, " );
		sb.append(" B.REVENUE_REGULAR, B.QUANTITY_REGULAR, B.REVENUE_SALE, ");
		sb.append(" B.QUANTITY_SALE, B.LIST_COST, B.EFF_LIST_COST_DATE, B.DEAL_COST, B.DEAL_START_DATE, B.DEAL_END_DATE,");
		sb.append("  B.COST_CHG_DIRECTION, B.MARGIN_CHG_DIRECTION, B.MARGIN_PCT, B.TOTAL_MARGIN, B.VISIT_COUNT, B.QTY_REGULAR_13WK, B.QTY_SALE_13WK, A.LIG_PRICE_VARIATION, A.IS_ZONE_PRICE_DIFF  ");
		return sb;
	}
	
	private StringBuffer BuildQueryForCompDataPI() {
		StringBuffer sb = new StringBuffer();
		sb.append(" INSERT INTO COMPETITIVE_DATA_PI ( ");
		sb.append(" CHECK_DATA_ID, SCHEDULE_ID, ITEM_CODE, REG_PRICE, SALE_PRICE, REG_M_PACK, REG_M_PRICE, ");
		sb.append(" SALE_M_PACK, SALE_M_PRICE, ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, ");
		sb.append(" EFF_SALE_END_DATE, CHECK_DATETIME, CREATE_DATETIME, CHANGE_DIRECTION, CHANGE_DIRECTION_SALE, ");
		sb.append(" EFF_SALE_START_DATE, EFF_REG_START_DATE, REVENUE_REGULAR, QUANTITY_REGULAR, REVENUE_SALE, ");
		sb.append(" QUANTITY_SALE, LIST_COST, EFF_LIST_COST_DATE, DEAL_COST, DEAL_START_DATE, DEAL_END_DATE,");
		sb.append(" COST_CHG_DIRECTION, MARGIN_CHG_DIRECTION, MARGIN_PCT,  TOTAL_MARGIN, VISIT_COUNT, QTY_REGULAR_13WK, QTY_SALE_13WK, IS_ZONE_PRICE_DIFF ) ");
		sb.append(" SELECT ");
		sb.append(" A.CHECK_DATA_ID, A.SCHEDULE_ID, A.ITEM_CODE, A.REG_PRICE, A.SALE_PRICE, A.REG_M_PACK, A.REG_M_PRICE, ");
		sb.append(" A.SALE_M_PACK, A.SALE_M_PRICE, A.ITEM_NOT_FOUND_FLG, A.PRICE_NOT_FOUND_FLG, A.PROMOTION_FLG, ");
		sb.append(" A.EFF_SALE_END_DATE, A.CHECK_DATETIME, A.CREATE_DATETIME, A.CHANGE_DIRECTION, A.CHANGE_DIRECTION_SALE, ");
		sb.append(" A.EFF_SALE_START_DATE, A.EFF_REG_START_DATE, " );
		sb.append(" B.REVENUE_REGULAR, B.QUANTITY_REGULAR, B.REVENUE_SALE, ");
		sb.append(" B.QUANTITY_SALE, B.LIST_COST, B.EFF_LIST_COST_DATE, B.DEAL_COST, B.DEAL_START_DATE, B.DEAL_END_DATE,");
		sb.append("  B.COST_CHG_DIRECTION, B.MARGIN_CHG_DIRECTION, B.MARGIN_PCT, B.TOTAL_MARGIN, B.VISIT_COUNT, B.QTY_REGULAR_13WK, B.QTY_SALE_13WK, A.IS_ZONE_PRICE_DIFF  ");
		return sb;
	}
	
	public boolean insertNonLIGItem ( Connection conn, CompetitiveDataDTO compData) throws GeneralException {
		
		boolean retVal = true;
		StringBuffer sb = BuildQueryForCompDataPI();
		sb.append(" FROM COMPETITIVE_DATA A, MOVEMENT_WEEKLY B ");
		sb.append(" WHERE A.CHECK_DATA_ID = ").append(compData.checkItemId);
		sb.append(" AND B.CHECK_DATA_ID (+)= A.CHECK_DATA_ID ");
		//logger.debug(sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "PI - Non LIG level data setup");
		}catch(GeneralException ge){
			retVal = false;
		}
		return retVal;
	}
	
	/**
	 * This method inserts records into competitive data pi table
	 * @param conn						Connection
	 * @param compDataListForInsert		List of CompetitiveDataDTO to be inserted
	 * @return	integer array containing the result of batch insert
	 * @throws GeneralException
	 */
	public int[] insertNonLIGItem (Connection conn, List<CompetitiveDataDTO> compDataListForInsert) throws GeneralException {
		logger.debug("Inside insertNonLIGItem() of CompDataPIDAO");
		int[] count = null;
		PreparedStatement statement = null;
		StringBuffer insertString = new StringBuffer(BuildQueryForCompDataPI());
		insertString.append(" FROM COMPETITIVE_DATA A, MOVEMENT_WEEKLY B ");
		insertString.append(" WHERE A.CHECK_DATA_ID = ? ");
		insertString.append(" AND B.CHECK_DATA_ID (+)= A.CHECK_DATA_ID ");
	    try{
			statement = conn.prepareStatement(insertString.toString());
	        
			int itemNoInBatch = 0;
	        for(CompetitiveDataDTO compData:compDataListForInsert){
	        	int counter = 0;
	        	statement.setInt(++counter, compData.checkItemId);
	        	statement.addBatch();
	        	itemNoInBatch++;
	        	
	        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		count = statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
	        }
	        if(itemNoInBatch > 0){
	        	count = statement.executeBatch();
        		statement.clearBatch();
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while inserting non lig item");
			throw new GeneralException("Error while inserting non lig item", e);
		}finally{
			PristineDBUtil.close(statement);
		}
		return count;
	}
}
