package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.CompositeDTO;
import com.pristine.dto.CostDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;

public class CompDataDAOLIG implements IDAO {
	private static Logger	logger	= Logger.getLogger("CompDataDAOLIG");
	
	public void deleteDataForSchedule( Connection conn, int schId, int itemCode) throws GeneralException{
		
		StringBuffer sb = new StringBuffer();
		sb.append(" delete from COMPETITIVE_DATA_LIG WHERE SCHEDULE_ID = ").append(schId);
		if (itemCode > 0 )
			sb.append(" AND ITEM_CODE = ").append(itemCode);
		
		PristineDBUtil.execute(conn, sb, "deleteLIGDataForSchedule");
	}
	
	public void setupLIGData(Connection conn, CompetitiveDataDTO compData, int lirItemCode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		
		//Get the check data id for the item
		int checkDataItemId  = getCheckDataId ( conn, compData.scheduleId, lirItemCode);
		if( checkDataItemId > 0)
			compData.checkItemId = checkDataItemId;  

		
		sb.append(" INSERT INTO COMPETITIVE_DATA_LIG ( ");
		sb.append(" CHECK_DATA_ID, SCHEDULE_ID, ITEM_CODE, " );
		sb.append(" REG_PRICE, SALE_PRICE, REG_M_PACK, REG_M_PRICE, SALE_M_PACK, SALE_M_PRICE, ");
		sb.append(" ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, EFF_SALE_END_DATE, ");
		sb.append(" CHECK_DATETIME, OUTSIDE_RANGE_IND, CREATE_DATETIME, " );
		sb.append(" CHANGE_DIRECTION, CHANGE_DIRECTION_SALE, SUSPECT, ");
		sb.append(" EFF_SALE_START_DATE, EFF_REG_START_DATE " );
		
		sb.append(" ) SELECT ");
		
		sb.append(compData.checkItemId ).append(", SCHEDULE_ID, " ).append(lirItemCode).append(",");
		sb.append(" REG_PRICE, SALE_PRICE, REG_M_PACK, REG_M_PRICE, SALE_M_PACK, SALE_M_PRICE, ");
		sb.append(" ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, EFF_SALE_END_DATE, ");
		sb.append(" CHECK_DATETIME, OUTSIDE_RANGE_IND, CREATE_DATETIME, " );
		sb.append(" CHANGE_DIRECTION, CHANGE_DIRECTION_SALE, SUSPECT, ");
		sb.append(" EFF_SALE_START_DATE, EFF_REG_START_DATE " );
		
		sb.append(" FROM COMPETITIVE_DATA WHERE ");
		sb.append(" CHECK_DATA_ID = ").append(compData.checkItemId);

		PristineDBUtil.execute(conn, sb, "LIG Level Rollup - insert");
		
		
	}
	
	/**
	 * This method sets up price of lir items in competitive_data_lig table
	 * @param conn
	 * @param ligDataMap
	 */
	public void setupLIGData(Connection conn, HashMap<Integer, CompetitiveDataDTO> ligDataMap) {
		StringBuffer sb = new StringBuffer();
		
		sb.append(" INSERT INTO COMPETITIVE_DATA_LIG ( ");
		sb.append(" CHECK_DATA_ID, SCHEDULE_ID, ITEM_CODE, " );
		sb.append(" REG_PRICE, SALE_PRICE, REG_M_PACK, REG_M_PRICE, SALE_M_PACK, SALE_M_PRICE, ");
		sb.append(" ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, EFF_SALE_END_DATE, ");
		sb.append(" CHECK_DATETIME, OUTSIDE_RANGE_IND, CREATE_DATETIME, " );
		sb.append(" CHANGE_DIRECTION, CHANGE_DIRECTION_SALE, SUSPECT, ");
		sb.append(" EFF_SALE_START_DATE, EFF_REG_START_DATE, IS_ZONE_PRICE_DIFF " );
		
		sb.append(" ) SELECT ");
		
		sb.append(" ?, SCHEDULE_ID, ?,");
		sb.append(" REG_PRICE, SALE_PRICE, REG_M_PACK, REG_M_PRICE, SALE_M_PACK, SALE_M_PRICE, ");
		sb.append(" ITEM_NOT_FOUND_FLG, PRICE_NOT_FOUND_FLG, PROMOTION_FLG, EFF_SALE_END_DATE, ");
		sb.append(" CHECK_DATETIME, OUTSIDE_RANGE_IND, CREATE_DATETIME, " );
		sb.append(" CHANGE_DIRECTION, CHANGE_DIRECTION_SALE, SUSPECT, ");
		sb.append(" EFF_SALE_START_DATE, EFF_REG_START_DATE, ? " );
		
		sb.append(" FROM COMPETITIVE_DATA WHERE ");
		sb.append(" CHECK_DATA_ID = ?");

		PreparedStatement statement = null;
		
		try{
			statement = conn.prepareStatement(sb.toString());
			int count = 0;
			for(Map.Entry<Integer, CompetitiveDataDTO> entry : ligDataMap.entrySet()){
				CompetitiveDataDTO compData = entry.getValue();
				statement.setInt(1, compData.checkItemId);
				statement.setInt(2, entry.getKey());
				if(compData.isZonePriceDiff)
					statement.setString(3, "Y");
				else
					statement.setString(3, "N");
				statement.setInt(4, compData.checkItemId);
				statement.addBatch();
				count++;
				if(count % Constants.BATCH_UPDATE_COUNT == 0){
	        		statement.executeBatch();
	        		statement.clearBatch();
	        	}
			}
			
			statement.executeBatch();
			logger.info("No of records processed : " + count);
		}catch (SQLException e){
			logger.error("Error when inserting price rollup " + e);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	public int getCheckDataId(Connection conn, int scheduleId, int lirItemCode) throws GeneralException {
		// TODO Auto-generated method stub
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT A.check_data_id FROM competitive_data A ");
		sb.append(" WHERE A.SCHEDULE_ID = ").append(scheduleId);
		sb.append(" AND A.ITEM_CODE = ").append(lirItemCode);
		String val = PristineDBUtil.getSingleColumnVal(conn, sb, "getCheckDataId");
		if( val != null)
			return Integer.parseInt(val);
		
		return 0;
	}

	public CachedRowSet getLigItemsMovement( Connection conn, int scheduleId)
			throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT A.check_data_id, A.item_code from competitive_data_lig A, movement_weekly_lig B ");
		sb.append(" WHERE A.SCHEDULE_ID = ").append(scheduleId);
		sb.append(" AND B.check_data_id = A.check_data_id ");
		//sb.append(" AND (B.QUANTITY_REGULAR > 0 OR B.QUANTITY_SALE > 0) " );
		//sb.append(" AND ROWNUM < 10");
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getLigItemsWithMovement");
		return result;
	}

	public CompositeDTO getLigMaxItemMovement (Connection conn,
			int scheduleId, int ligItemCode) throws GeneralException, SQLException {

		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT A.CHECK_DATA_ID, A.ITEM_CODE, A.REG_PRICE, A.SALE_PRICE, ");
		sb.append(" A.REG_M_PACK, A.REG_M_PRICE, A.SALE_M_PACK, A.SALE_M_PRICE, " );
		sb.append(" A.ITEM_NOT_FOUND_FLG, A.PRICE_NOT_FOUND_FLG, A.PROMOTION_FLG, A.CHANGE_DIRECTION,   ");
		sb.append("  TO_CHAR(A.EFF_SALE_END_DATE,'MM/DD/YYYY') EFF_SALE_END_DATE, ");
		sb.append("  TO_CHAR(A.EFF_SALE_START_DATE,'MM/DD/YYYY') EFF_SALE_START_DATE,"); 
		sb.append("  TO_CHAR(A.EFF_REG_START_DATE, 'MM/DD/YYYY') EFF_REG_START_DATE,");
		sb.append(" UNITPRICE(A.REG_PRICE, A.REG_M_PRICE, A.REG_M_PACK) UNIT_REG_PRICE, ");
		sb.append(" UNITPRICE(A.SALE_PRICE, A.SALE_M_PRICE, A.SALE_M_PACK) UNIT_SALE_PRICE, ");
		sb.append(" NVL ( B.QUANTITY_REGULAR, 0) + NVL( B.QUANTITY_SALE, 0 ) AS QUANTITY, ");   
		sb.append(" B.LIST_COST, B.DEAL_COST, B.COST_CHG_DIRECTION, " );
		sb.append(" TO_CHAR(B.EFF_LIST_COST_DATE, 'MM/DD/YYYY') EFF_LIST_COST_DATE," );
		sb.append(" TO_CHAR(B.DEAL_START_DATE, 'MM/DD/YYYY') DEAL_START_DATE," );
		sb.append(" TO_CHAR(B.DEAL_END_DATE, 'MM/DD/YYYY')  DEAL_END_DATE" );
		sb.append(" FROM COMPETITIVE_DATA  A, MOVEMENT_WEEKLY B" );
		sb.append(" WHERE A.SCHEDULE_ID = ? ");
		sb.append(" AND A.ITEM_CODE IN ( SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE RET_LIR_ID = " );
		sb.append(" ( SELECT RET_LIR_ID FROM ITEM_LOOKUP WHERE ITEM_CODE = ? ))");
		sb.append(" AND B.CHECK_DATA_ID = A.CHECK_DATA_ID ");
		sb.append(" ");
		
		// Changes for Performance Improvement
		PreparedStatement statement = null;
		ResultSet rs = null;
		CachedRowSet result = null;
		try {
			statement = conn.prepareStatement(sb.toString());
			statement.setInt(1, scheduleId);
			statement.setInt(2, ligItemCode);
			rs = statement.executeQuery();
			result = PristineDBUtil.getCachedRowSet(rs);
		} catch (SQLException ex) {
			logger.error("Error in getLigMaxItemMovementSQL. SQL: " + sb.toString(), ex);
		}finally{
			PristineDBUtil.close(statement);
		}
		// Changes for Performance Improvement - Ends
		
		CompositeDTO compositeData = new CompositeDTO();
		compositeData.compData.checkItemId =-1;
		compositeData.compData.quantitySold = 0.0f;
		float previousSalePrice = 0.0f;
		float previousRegPrice = 0.0f;
		
		

		
		while ( result.next()){
			int itemCode = result.getInt("ITEM_CODE");
			float currentQtySold = result.getFloat("QUANTITY");  
			if ( compositeData.compData.checkItemId == -1 && itemCode == ligItemCode ){
				populateCompositeData(result, compositeData);
			}else if (currentQtySold > compositeData.compData.quantitySold){
				populateCompositeData(result, compositeData);
			}
		
			float currentRegPrice = result.getFloat("UNIT_REG_PRICE");  
			float currentSalePrice = result.getFloat("UNIT_SALE_PRICE");
			
			if( previousRegPrice > 0.0f){
				if( (previousSalePrice != currentSalePrice) || (previousRegPrice != currentRegPrice))
					compositeData.costData.hasLIGPriceVariations = true;
				
				// Notification for Price Variation in LIG
				if(currentQtySold > 0.0f && previousRegPrice != currentRegPrice){
					compositeData.compData.hasLIGRegPriceVariations = "Y";
				}
				// Notification for Price Variation in LIG - Ends
			}
			
			if(currentQtySold > 0.0f){
				previousSalePrice = currentSalePrice;
				previousRegPrice = currentRegPrice;
			}
		}
		if( compositeData.compData.checkItemId == -1 )
			return null;
		else
			return compositeData;
	}

	private void populateCompositeData(CachedRowSet result, CompositeDTO compositeData) throws SQLException {
	
		
		compositeData.compData.checkItemId = result.getInt("CHECK_DATA_ID");
		compositeData.compData.itemcode = result.getInt("ITEM_CODE");
		
		compositeData.compData.regPrice = result.getFloat("REG_PRICE");
		compositeData.compData.regMPrice = result.getFloat("REG_M_PRICE");
		compositeData.compData.regMPack = result.getInt("REG_M_PACK");
		compositeData.compData.fSalePrice = result.getFloat("SALE_PRICE");
		compositeData.compData.fSaleMPrice = result.getFloat("SALE_M_PRICE");
		compositeData.compData.saleMPack = result.getInt("SALE_M_PACK");
		
		compositeData.compData.itemNotFound = result.getString("ITEM_NOT_FOUND_FLG");
		compositeData.compData.priceNotFound = result.getString("PRICE_NOT_FOUND_FLG");
		compositeData.compData.saleInd = result.getString("PROMOTION_FLG");
		String chgDirection = result.getString("CHANGE_DIRECTION");
		if( chgDirection != null )
			compositeData.compData.chgDirection = result.getInt("CHANGE_DIRECTION");
		else
			compositeData.compData.chgDirection = Constants.PRICE_NA;
		
		compositeData.compData.effRegRetailStartDate = result.getString("EFF_REG_START_DATE");
		compositeData.compData.effSaleStartDate = result.getString("EFF_SALE_START_DATE");
		compositeData.compData.effSaleEndDate =  result.getString("EFF_SALE_END_DATE");
		compositeData.compData.quantitySold = result.getFloat("QUANTITY");
		
		
		compositeData.costData.listCost = result.getFloat("LIST_COST");
		compositeData.costData.dealCost = result.getFloat("DEAL_COST");
		
		String costChgDirection = result.getString("COST_CHG_DIRECTION");
		if( costChgDirection != null )
			compositeData.costData.costChgDirection =result.getInt("COST_CHG_DIRECTION");
		else
			compositeData.costData.costChgDirection = Constants.PRICE_NA;
		compositeData.costData.listCostEffDate = result.getString("EFF_LIST_COST_DATE");
		compositeData.costData.promoCostStartDate = result.getString("DEAL_START_DATE");
		compositeData.costData.promoCostEndDate = result.getString("DEAL_END_DATE");
	}

	public void updateLIGPriceInfo(Connection conn, CompetitiveDataDTO compData) throws GeneralException  {
		
		StringBuffer sb = new StringBuffer();
		sb.append( " UPDATE COMPETITIVE_DATA_LIG SET ");
		sb.append( " REG_PRICE = " );
		sb.append(((compData.regPrice < 0.01)? "0.0": PrestoUtil.round(compData.regPrice,2))).append(",  ");
		sb.append( " REG_M_PACK = " );
		sb.append(((compData.regMPack < 0.01)? "0": compData.regMPack)).append(",  ");
		sb.append( " REG_M_PRICE = " );
		sb.append(((compData.regMPrice < 0.01)? "0.0": PrestoUtil.round(compData.regMPrice,2))).append(",  ");
		sb.append( " SALE_PRICE = " );
		sb.append(((compData.fSalePrice < 0.01)? "0.0": PrestoUtil.round(compData.fSalePrice,2))).append(",  ");
		sb.append( " SALE_M_PACK = " );
		sb.append(((compData.saleMPack < 0.01)? "0": compData.saleMPack)).append(",  ");
		sb.append( " SALE_M_PRICE = " );
		sb.append(((compData.fSaleMPrice < 0.01)? "0.0": PrestoUtil.round(compData.fSaleMPrice,2))).append(",  ");
		sb.append(" ITEM_NOT_FOUND_FLG ='").append( compData.itemNotFound).append("', ");
		sb.append(" PRICE_NOT_FOUND_FLG ='").append( compData.priceNotFound).append("', ");
		sb.append(" PROMOTION_FLG ='").append( compData.saleInd).append("', ");
		sb.append(" CHANGE_DIRECTION = ").append( compData.chgDirection).append(", ");
		
		
		sb.append(" EFF_REG_START_DATE  = ");
		if( compData.effRegRetailStartDate == null || compData.effRegRetailStartDate.equals("") )
			sb.append(" NULL, ");
		else
			sb.append(" TO_DATE( '").append(compData.effRegRetailStartDate).append("','MM/DD/YYYY'), " );
		
		sb.append(" EFF_SALE_START_DATE = ");
		if( compData.effSaleStartDate == null || compData.effSaleStartDate.equals("") )
			sb.append(" NULL, ");
		else
			sb.append(" TO_DATE( '").append(compData.effSaleStartDate).append("','MM/DD/YYYY'), " );
		
		
		sb.append(" EFF_SALE_END_DATE = ");
		if( compData.effSaleEndDate == null || compData.effSaleEndDate.equals("") )
			sb.append(" NULL ");
		else
			sb.append(" TO_DATE( '").append(compData.effSaleEndDate).append("','MM/DD/YYYY') " );
	
		sb.append( " WHERE CHECK_DATA_ID = ");
		sb.append( compData.checkItemId );
		int updateCount = PristineDBUtil.executeUpdate(conn, sb, "Competitive DATA LIG - Update");
		if ( updateCount != 1){
			logger.error("Update Comp Data LIG Unsuccessful, item code  = " + compData.itemcode);
		}
		return;
		
	}

	public void updateLIGCostInfo(Connection conn, CostDTO costDTO) throws GeneralException {

		StringBuffer sb = new StringBuffer(); 
		sb.append( " UPDATE MOVEMENT_WEEKLY_LIG SET ");
		sb.append( " CHECK_DATA_ID = " );
		sb.append( costDTO.checkItemId).append(", ");
		sb.append( " LIST_COST = " );
		sb.append( costDTO.listCost).append(", ");
		sb.append( " DEAL_COST = " );
		sb.append( costDTO.dealCost).append(", ");
		sb.append( " EFF_LIST_COST_DATE = " );
		if( costDTO.listCostEffDate == null || costDTO.listCostEffDate.equals("") )
			sb.append(" NULL, ");
		else
			sb.append(" TO_DATE( '").append(costDTO.listCostEffDate).append("','MM/DD/YYYY'), " );
		
		sb.append( " DEAL_START_DATE = " );
		if( costDTO.promoCostStartDate== null || costDTO.promoCostStartDate.equals("") )
			sb.append(" NULL, ");
		else
			sb.append(" TO_DATE( '").append(costDTO.promoCostStartDate).append("','MM/DD/YYYY'), " );

		sb.append( " DEAL_END_DATE = " );
		if( costDTO.promoCostEndDate== null || costDTO.promoCostEndDate.equals("") )
			sb.append(" NULL, ");
		else
			sb.append(" TO_DATE( '").append(costDTO.promoCostEndDate).append("','MM/DD/YYYY'), " );

		if (costDTO.hasLIGPriceVariations) 
			sb.append( " HAS_LIG_PRICE_VARIATIONS = 'Y' ," );
		else
			sb.append( " HAS_LIG_PRICE_VARIATIONS = 'N' ," );

		sb.append( " COST_CHG_DIRECTION = " );
		sb.append(costDTO.costChgDirection);
		sb.append(" WHERE CHECK_DATA_ID = ");
		sb.append(costDTO.checkItemId);
		//logger.debug( "Update Query" + sb.toString());
		int updateCount = PristineDBUtil.executeUpdate(conn, sb, "Cost DATA - Update");
		if ( updateCount != 1){
			logger.error("Update Cost Data LIG Unsuccessful, item code = " + costDTO.itemCode);
		}
	}
	
	// Overloaded for performance improvement
	public void updateLIGPriceInfo(Connection conn, List<CompositeDTO> compositeDTOList) {
		
		StringBuffer sb = new StringBuffer();
		sb.append( " UPDATE COMPETITIVE_DATA_LIG SET REG_PRICE = ?, REG_M_PACK = ?, REG_M_PRICE = ?, SALE_PRICE = ?, SALE_M_PACK = ?, SALE_M_PRICE = ?, ");
		sb.append("ITEM_NOT_FOUND_FLG = ?, PRICE_NOT_FOUND_FLG = ?, PROMOTION_FLG = ?, CHANGE_DIRECTION = ?, EFF_REG_START_DATE = TO_DATE(?, 'MM/DD/YYYY')," );
		sb.append("EFF_SALE_START_DATE = TO_DATE(?, 'MM/DD/YYYY'), EFF_SALE_END_DATE = TO_DATE(?, 'MM/DD/YYYY'), LIG_PRICE_VARIATION = ? WHERE CHECK_DATA_ID = ?");
				
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		try{
			statement = conn.prepareStatement(sb.toString());
			for(CompositeDTO compositeDTO : compositeDTOList){
				int counter = 0;
				CompetitiveDataDTO compData = compositeDTO.compData;
				if(compData.regPrice < 0.01)
					statement.setDouble(++counter, 0.0);
				else
					statement.setDouble(++counter, PrestoUtil.round(compData.regPrice,2));
				
				if(compData.regMPack < 0.01)
					statement.setInt(++counter, 0);
				else
					statement.setInt(++counter, compData.regMPack);
				
				if(compData.regMPrice < 0.01)
					statement.setDouble(++counter, 0.0);
				else
					statement.setDouble(++counter, PrestoUtil.round(compData.regMPrice,2));
				
				if(compData.fSalePrice < 0.01)
					statement.setDouble(++counter, 0.0);
				else
					statement.setDouble(++counter, PrestoUtil.round(compData.fSalePrice,2));
				
				if(compData.saleMPack < 0.01)
					statement.setInt(++counter, 0);
				else
					statement.setInt(++counter, compData.saleMPack);
				
				if(compData.fSaleMPrice < 0.01)
					statement.setDouble(++counter, 0.0);
				else
					statement.setDouble(++counter, PrestoUtil.round(compData.fSaleMPrice,2));
				
				statement.setString(++counter, compData.itemNotFound);
				statement.setString(++counter, compData.priceNotFound);
				statement.setString(++counter, compData.saleInd);
				statement.setInt(++counter, compData.chgDirection);
				statement.setString(++counter, compData.effRegRetailStartDate);
				statement.setString(++counter, compData.effSaleStartDate);
				statement.setString(++counter, compData.effSaleEndDate);
				statement.setString(++counter, compData.hasLIGRegPriceVariations);
				statement.setInt(++counter, compData.checkItemId);
				
				statement.addBatch();
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
			}
			if(itemNoInBatch > 0){
	        	 statement.executeBatch();
        		statement.clearBatch();
	        }
		}catch(SQLException sqlException){
			logger.error("Error in updateLIGPriceInfo - " + sqlException.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
		
		return;
	}

	// Overloaded for performance improvement
	public void updateLIGCostInfo(Connection conn, List<CompositeDTO> compositeDTOList) {

		StringBuffer sb = new StringBuffer(); 
		sb.append(" UPDATE MOVEMENT_WEEKLY_LIG SET LIST_COST = ?, DEAL_COST = ?, EFF_LIST_COST_DATE = TO_DATE(?, 'MM/DD/YYYY'), ");
		sb.append("DEAL_START_DATE = TO_DATE(?, 'MM/dd/yyyy'), DEAL_END_DATE = TO_DATE(?, 'MM/dd/yyyy'), HAS_LIG_PRICE_VARIATIONS = ?, ");
		sb.append("COST_CHG_DIRECTION = ? WHERE CHECK_DATA_ID = ? ");
		
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		try{
			statement = conn.prepareStatement(sb.toString());
			for(CompositeDTO compositeDTO : compositeDTOList){
				int counter = 0;
				CostDTO costDTO = compositeDTO.costData;
				
				statement.setDouble(++counter, costDTO.listCost);
				statement.setDouble(++counter, costDTO.dealCost);
				statement.setString(++counter, costDTO.listCostEffDate);
				statement.setString(++counter, costDTO.promoCostStartDate);
				statement.setString(++counter, costDTO.promoCostEndDate);
				if (costDTO.hasLIGPriceVariations)
					statement.setString(++counter, "Y");
				else
					statement.setString(++counter, "N");
				statement.setInt(++counter, costDTO.costChgDirection);
				statement.setInt(++counter, costDTO.checkItemId);
				
				statement.addBatch();
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
	        		 statement.executeBatch();
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        	}
			}
			if(itemNoInBatch > 0){
	        	statement.executeBatch();
        		statement.clearBatch();
	        }
		}catch(SQLException sqlException){
			logger.error("Error in updateLIGCostInfo - " + sqlException.getMessage());
		}finally{
			PristineDBUtil.close(statement);
		}
		
		return;
	}
}
