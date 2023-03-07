package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.PlanogramDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class PlanogramDAO implements IDAO {

	private static Logger logger = Logger.getLogger("PlanogramDAO");
	
	public void deletePlanogramInfo( Connection conn, int storeId) throws GeneralException {
		
		StringBuffer sql = new StringBuffer();
		sql.append(" DELETE FROM planogram_info where STORE_ID = "); 
		sql.append(storeId);

		PristineDBUtil.execute(conn, sql, "clearPlanogramData");
		
	}

	public int insertPlanogramData(Connection conn,
			List<PlanogramDTO> planogramList) throws GeneralException{
		
	
		int skippedCount = 0;
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(INSERT_PLANOGRAM_DATA);
			int insertCount = 0;
			for (PlanogramDTO planogramData : planogramList) {
				
				if(planogramData.getStoreId() <0 || planogramData.getItemCode()<0){
					skippedCount++;
					continue;
				}
				int counter = 0;
				
				stmt.setInt(++counter, planogramData.getStoreId());
				stmt.setInt(++counter, planogramData.getItemCode());
				stmt.setString(++counter, planogramData.getAisleFix());
				stmt.setInt(++counter, planogramData.getAislePos());
				stmt.setString(++counter, planogramData.getShelf());
				stmt.setInt(++counter, planogramData.getShelfPos());
				stmt.setInt(++counter, planogramData.getShelfProdPos());
				stmt.setString(++counter, planogramData.getPlanogramNo());
				stmt.setInt(++counter, Integer.parseInt(planogramData.getCapacity().trim()));
				
				stmt.addBatch();
				insertCount++;
				//Add batch logic
				
				if( insertCount%Constants.BATCH_UPDATE_COUNT == 0){
					stmt.executeBatch();
					stmt.clearBatch();					
				}
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
		return skippedCount;
	}
	
	private static final String INSERT_PLANOGRAM_DATA = " INSERT INTO planogram_info ( " 
			+ "  STORE_ID, item_code, aisle_fix, AISLE_POS, SHELF, SHELF_POS, SHELF_PROD_POS, PLANOGRAM_NBR, CAPACITY, "
			+ "  FACINGS, update_timestamp) "
			+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, sysdate) ";
	
	
	}
