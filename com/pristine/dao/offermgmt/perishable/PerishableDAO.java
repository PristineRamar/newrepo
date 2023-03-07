package com.pristine.dao.offermgmt.perishable;

import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.pristine.dto.offermgmt.perishable.PerishableOrderDTO;
import com.pristine.dto.offermgmt.perishable.PerishableProductDTO;
import com.pristine.dto.offermgmt.perishable.PerishableRelationshipDTO;
import com.pristine.dto.offermgmt.perishable.PerishableSellDTO;
import com.pristine.dto.offermgmt.perishable.TargetLocationDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class PerishableDAO {
	static Logger logger = Logger.getLogger("PerishableCostExportDAO");
	private Connection conn = null;
	
	public PerishableDAO(Connection conn){
		this.conn = conn;
	}
	
	
	
	
	public ArrayList<PerishableProductDTO> getPerishableProducts(
		int relationshipType) throws GeneralException{
	
		ArrayList<PerishableProductDTO> productList = new ArrayList<PerishableProductDTO>();	
		PreparedStatement stmt = null;
		ResultSet rs = null;		

		try{
			StringBuilder sb = new StringBuilder();
		
	        sb.append("SELECT PRODUCT_LEVEL_ID, PRODUCT_ID");
	        sb.append(" FROM PR_PRODUCT_GROUP_PROPERTY");
	        sb.append(" WHERE IS_PERISHABLE = 'Y'");
	        sb.append(" AND IS_ORDER_SELL_CODE = 'Y'");
	        sb.append(" AND RELATIONSHIP_TYPE = " + relationshipType);
        
	        if (relationshipType == Constants.PERISHABLE_S_ORDER_M_SELL)
	            sb.append(" AND USE_YIELD = 'Y'");        
	        else if (relationshipType == Constants.PERISHABLE_M_ORDER_S_SELL)
	        	sb.append("  AND IS_RECIPE = 'Y'");        
		
			logger.debug("getPerishableProducts SQL:- " + sb.toString());
		
			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();

			logger.debug("Orgaize the product Data into collection...");
		
			PerishableProductDTO productObj = new PerishableProductDTO();
			while(rs.next()){
				productObj = new PerishableProductDTO();
			
				productObj.setProductLevelId(rs.getInt("PRODUCT_LEVEL_ID"));
				productObj.setProductId(rs.getInt("PRODUCT_ID"));
			
				productList.add(productObj);
			}
		}
		catch(SQLException ex){
			logger.error("com.pristine.dao.offermgmt.perishable.PerishableDAO.getPerishableProducts() - Error when getting recommendat data - " + ex.getMessage());
			throw new GeneralException("Error when getting perishable product data - " + ex.getMessage());
		}finally{
		}
	
		return productList;
	}	
	
	public ArrayList<PerishableRelationshipDTO> getPerishableRelationshipData(
			int productLevelId, int productId, int relationshipType) 
												throws GeneralException{
		
			ArrayList<PerishableRelationshipDTO> relationshipList = new ArrayList<PerishableRelationshipDTO>(); 
			
			PreparedStatement stmt = null;
			ResultSet rs = null;				

			try{
				StringBuffer sb = new StringBuffer();

                sb.append("SELECT");
                sb.append(" FH.FRESH_RELATION_PRODUCT_ID,");
                sb.append(" FH.FRESH_RELATION_HEADER_ID,");

                sb.append("FH.RELATION_START_CALENDAR_ID,");
                sb.append("FH.RELATION_END_CALENDAR_ID,");
                
                sb.append(" FP.LOCATION_LEVEL_ID, FP.LOCATION_ID,");

                sb.append(" 'WK ' || RS.ACTUAL_NO || ' (' || TO_CHAR(RS.START_DATE, 'Mon dd') || ' - ' || TO_CHAR(RS.END_DATE, 'Mon dd') || ')' AS RELATION_START,");
                sb.append(" 'WK ' || RE.ACTUAL_NO || ' (' || TO_CHAR(RE.START_DATE, 'Mon dd') || ' - ' || TO_CHAR(RE.END_DATE, 'Mon dd') || ')' AS RELATION_END,");

                sb.append(" FH.SHRINK AS ORDER_SHRINK,");
                sb.append(" FH.LABOR_COST, FH.ADDITIONAL_COST,");

                sb.append(" FO.FRESH_RELATION_ORDER_ID,");
                sb.append(" FO.ORDER_UOM AS ORDER_UOM_MAJOR, FO.ORDER_UOM_MINOR,");
                sb.append(" FO.ORDER_SIZE, FO.ORDER_AVG_COUNT,");

                sb.append(" OIL.ITEM_CODE AS ORDER_ITEM_CODE,");
                sb.append(" OIL.ITEM_NAME AS ORDER_CODE_DESC,");
                sb.append(" OIL.RETAILER_ITEM_CODE AS ORDER_CODE,");
                
                sb.append(" OIL.UOM_ID AS ORDER_UOM,");
                sb.append(" OIL.ITEM_SIZE AS ORDER_ITEM_SIZE,");
                sb.append(" OIL.UPC AS ORDER_UPC,");

                sb.append(" ULO.NAME AS ORDER_UOM_NAME,");

                sb.append(" FS.FRESH_RELATION_SELL_ID,");
                sb.append(" FS.TYPICAL_PRICE, FS.YIELD,");
                sb.append(" FS.SELL_UPC, FS.SELL_AVG_COUNT,");

                sb.append(" SIL.ITEM_CODE AS SELL_ITEM_CODE,");
                sb.append(" SIL.ITEM_NAME AS SELL_CODE_DESC,");
                sb.append(" SIL.RETAILER_ITEM_CODE AS SELL_CODE,");
                sb.append(" SIL.UOM_ID AS SELL_UOM,");
                sb.append(" SIL.ITEM_SIZE AS SELL_ITEM_SIZE,");

                sb.append(" ULS.NAME AS SELL_UOM_NAME,");

                sb.append(" PROD.CATEGORY_ID, PROD.CATEGORY_NAME");

                sb.append(" FROM PR_FRESH_RELATION_PRODUCT FP");

                sb.append(" JOIN PR_FRESH_RELATION_HEADER FH");
        		sb.append(" ON FH.FRESH_RELATION_PRODUCT_ID = FP.FRESH_RELATION_PRODUCT_ID");
                
        		sb.append(" JOIN PR_FRESH_RELATION_ORDER FO");
        		sb.append(" ON FO.FRESH_RELATION_HEADER_ID = FH.FRESH_RELATION_HEADER_ID");
        		
        		sb.append(" JOIN PR_FRESH_RELATION_SELL FS");
        		sb.append(" ON FS.FRESH_RELATION_HEADER_ID = FO.FRESH_RELATION_HEADER_ID");
        		
        		sb.append(" LEFT JOIN RETAIL_CALENDAR RS");
        		sb.append(" ON RS.CALENDAR_ID = FH.RELATION_START_CALENDAR_ID");
        		
        		sb.append(" LEFT JOIN RETAIL_CALENDAR RE");
        		sb.append(" ON RE.CALENDAR_ID = FH.RELATION_END_CALENDAR_ID");
        		
        		sb.append(" JOIN ITEM_LOOKUP OIL");
        		sb.append(" ON OIL.ACTIVE_INDICATOR = 'Y'");
        		sb.append(" AND OIL.ITEM_CODE = FO.ORDER_ITEM_CODE");
        		
        		sb.append(" JOIN ITEM_LOOKUP SIL");
        		sb.append(" ON SIL.ACTIVE_INDICATOR = 'Y'");
        		sb.append(" AND SIL.ITEM_CODE = FS.SELL_ITEM_CODE");
        		sb.append(" AND SIL.UPC = FS.SELL_UPC");
                
        		sb.append(" JOIN UOM_LOOKUP ULO ON ULO.ID = FO.ORDER_UOM");
        		
        		sb.append(" JOIN UOM_LOOKUP ULS ON ULS.ID = SIL.UOM_ID");
                
        		sb.append(" JOIN");
        		sb.append(" (SELECT START_WITH_CHILD_ID AS ITEM_CODE,");
        		sb.append(" MIN(CASE WHEN PH.PRODUCT_LEVEL_ID = ").append(Constants.CATEGORYLEVELID).append(" THEN PH.PRODUCT_ID END) AS CATEGORY_ID,");
        		sb.append(" MIN(CASE WHEN PH.PRODUCT_LEVEL_ID = ").append(Constants.CATEGORYLEVELID).append(" THEN PG.NAME END) AS CATEGORY_NAME");
        		sb.append(" FROM");
        		sb.append(" (SELECT PRODUCT_ID, PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID,");
        		sb.append(" CONNECT_BY_ROOT CHILD_PRODUCT_ID AS START_WITH_CHILD_id");
        		sb.append(" FROM PRODUCT_GROUP_RELATION PGR");
        		sb.append(" START WITH CHILD_PRODUCT_ID IN");
        		sb.append(" (SELECT CHILD_PRODUCT_ID");
        		sb.append(" FROM");
        		sb.append(" (SELECT CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID");
        		sb.append(" FROM PRODUCT_GROUP_RELATION PGR");
        		sb.append(" START WITH PRODUCT_ID = ").append(productId).append(" AND PRODUCT_LEVEL_ID = ").append(productLevelId) ;
        		sb.append(" CONNECT BY PRIOR CHILD_PRODUCT_ID = PRODUCT_ID");
        		sb.append(" AND PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID)");
        		sb.append(" WHERE CHILD_PRODUCT_LEVEL_ID = ").append(Constants.ITEMLEVELID);
        		sb.append(" ) AND CHILD_PRODUCT_LEVEL_ID = ").append(Constants.ITEMLEVELID);;
        		sb.append(" CONNECT BY PRIOR PRODUCT_ID = CHILD_PRODUCT_ID");
        		sb.append(" AND PRIOR PRODUCT_LEVEL_ID = CHILD_PRODUCT_LEVEL_ID");
        		sb.append(" ) PH");
        		sb.append(" LEFT JOIN PRODUCT_GROUP PG");
        		sb.append(" ON PH.PRODUCT_LEVEL_ID = PG.PRODUCT_LEVEL_ID");
        		sb.append(" AND PH.PRODUCT_ID      = PG.PRODUCT_ID");
        		sb.append(" GROUP BY START_WITH_CHILD_id");
        		sb.append(" ) PROD ON SIL.ITEM_CODE  = PROD.ITEM_CODE");
        		sb.append(" WHERE FP.PRODUCT_LEVEL_ID = ").append(productLevelId) ;
        		sb.append(" AND FP.PRODUCT_ID = ").append(productId) ;;
        		sb.append(" ORDER BY FH.FRESH_RELATION_HEADER_ID");

				logger.debug("getPerishableRelationshipData SQL:- " + sb.toString());
			
				stmt = conn.prepareStatement(sb.toString());
				rs = stmt.executeQuery();

				logger.debug("Orgaize the product Data into collection...");
				relationshipList = formatPerishableRelationshipData(
												rs, relationshipType);
			}
			catch(SQLException ex){
				logger.error("com.pristine.dao.offermgmt.perishable.PerishableDAO.getPerishableRelationShipData() - Error when getting recommendat data - " + ex.getMessage());
				throw new GeneralException("Error when getting Perishable Relationship Data - " + ex.getMessage());
			}finally{
			}
		
			return relationshipList;
		}
	
	public ArrayList<PerishableRelationshipDTO> formatPerishableRelationshipData(
			ResultSet rs, int relationshipType) throws SQLException{
		
		ArrayList<PerishableRelationshipDTO> relationshipList = new ArrayList<PerishableRelationshipDTO>(); 
		PerishableRelationshipDTO relaionshipObj = new PerishableRelationshipDTO();
		List<PerishableOrderDTO> orderDataList = new ArrayList<PerishableOrderDTO>();
		List<PerishableSellDTO> sellDataList = new ArrayList<PerishableSellDTO>();
		
		PerishableOrderDTO orderDataObj = new PerishableOrderDTO();
		PerishableSellDTO sellDataObj = new PerishableSellDTO();

		int lastRelationHeaderId = 0;
		
		while (rs.next()) {
			int curRelationHeaderId = rs.getInt("FRESH_RELATION_HEADER_ID");

			if (lastRelationHeaderId > 0 && curRelationHeaderId != lastRelationHeaderId){
				relaionshipObj.setOrderData(orderDataList);
				relaionshipObj.setSellData(sellDataList);
				relationshipList.add(relaionshipObj);
				
				relaionshipObj = new PerishableRelationshipDTO();
				orderDataList = new ArrayList<PerishableOrderDTO>();
				sellDataList = new ArrayList<PerishableSellDTO>();					
			}
			
			orderDataObj = new PerishableOrderDTO();
			sellDataObj = new PerishableSellDTO();

			if (lastRelationHeaderId == 0 || curRelationHeaderId != lastRelationHeaderId){
                relaionshipObj.setFreshProductHeaderId(rs.getInt("FRESH_RELATION_PRODUCT_ID"));
				relaionshipObj.setFreshRelationRefId(curRelationHeaderId);

				if (rs.getObject("LOCATION_LEVEL_ID") != null )
					relaionshipObj.setLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				
				if (rs.getObject("LOCATION_ID") != null )
					relaionshipObj.setLocationId(rs.getInt("LOCATION_ID"));
				
				if (rs.getObject("RELATION_START_CALENDAR_ID") != null )
					relaionshipObj.setRelationStartCalId(rs.getInt("RELATION_START_CALENDAR_ID"));
				
				if (rs.getObject("RELATION_END_CALENDAR_ID") != null )
					relaionshipObj.setRelationEndCalId(rs.getInt("RELATION_END_CALENDAR_ID"));
				
				if (rs.getObject("RELATION_START") != null )
					relaionshipObj.setRelationStartDate(rs.getString("RELATION_START"));
				
				if (rs.getObject("RELATION_END") != null )
					relaionshipObj.setRelationEndDate(rs.getString("RELATION_END"));
				
				if (rs.getObject("ORDER_SHRINK") != null )
					relaionshipObj.setShrink(rs.getDouble("ORDER_SHRINK"));
				
				if (rs.getObject("ADDITIONAL_COST") != null )
					relaionshipObj.setAdditionalCost(rs.getDouble("ADDITIONAL_COST"));
				
				if (rs.getObject("LABOR_COST") != null )
					relaionshipObj.setLaborCost(rs.getDouble("LABOR_COST"));
				
				// Get order data once
				if (relationshipType == Constants.PERISHABLE_S_ORDER_M_SELL){
					orderDataObj.setRefId(rs.getInt("FRESH_RELATION_ORDER_ID"));
					
					orderDataObj.setItemCode(rs.getInt("ORDER_ITEM_CODE"));
					orderDataObj.setOrderCode(rs.getString("ORDER_CODE"));
					orderDataObj.setItemDesc(rs.getString("ORDER_CODE_DESC"));
					orderDataObj.setItemUOM(rs.getInt("ORDER_UOM"));
					orderDataObj.setItemSize(rs.getDouble("ORDER_ITEM_SIZE"));
					
					orderDataObj.setIngUOMMajor(rs.getInt("ORDER_UOM_MAJOR"));
					orderDataObj.setIngUOMMinor(rs.getInt("ORDER_UOM_MINOR"));
					orderDataObj.setIngSize(rs.getDouble("ORDER_SIZE"));
					
					orderDataObj.setAverageCount(rs.getInt("ORDER_AVG_COUNT"));
					orderDataList.add(orderDataObj);
				}
				// Get customer data once
				else if (relationshipType == Constants.PERISHABLE_M_ORDER_S_SELL){
				
					sellDataObj.setRefId(rs.getInt("FRESH_RELATION_SELL_ID"));
                    sellDataObj.setCategoryId(rs.getInt("CATEGORY_ID"));
                    sellDataObj.setItemCode(rs.getInt("SELL_ITEM_CODE"));
                    sellDataObj.setSellCode(rs.getString("SELL_CODE"));
                    sellDataObj.setItemDesc(rs.getString("SELL_CODE_DESC"));
					sellDataObj.setItemUPC(rs.getString("SELL_UPC"));
					sellDataObj.setTypicalPrice(rs.getDouble("TYPICAL_PRICE"));
					sellDataObj.setItemYield(rs.getDouble("YIELD"));
					sellDataObj.setItemSize(rs.getDouble("SELL_ITEM_SIZE"));
					sellDataObj.setItemUOM(rs.getInt("SELL_UOM"));
					sellDataObj.setAverageCount(rs.getInt("SELL_AVG_COUNT"));
					sellDataList.add(sellDataObj);
				}
			}

			// Get all order item data
			if (relationshipType == Constants.PERISHABLE_M_ORDER_S_SELL){
				orderDataObj.setRefId(rs.getInt("FRESH_RELATION_ORDER_ID"));
				
				orderDataObj.setItemCode(rs.getInt("ORDER_ITEM_CODE"));
				orderDataObj.setOrderCode(rs.getString("ORDER_CODE"));
				orderDataObj.setItemDesc(rs.getString("ORDER_CODE_DESC"));
				orderDataObj.setItemUOM(rs.getInt("ORDER_UOM"));
				orderDataObj.setItemSize(rs.getDouble("ORDER_ITEM_SIZE"));
				
				orderDataObj.setIngUOMMajor(rs.getInt("ORDER_UOM_MAJOR"));
				orderDataObj.setIngUOMMinor(rs.getInt("ORDER_UOM_MINOR"));
				orderDataObj.setIngSize(rs.getDouble("ORDER_SIZE"));
				
				orderDataObj.setAverageCount(rs.getInt("ORDER_AVG_COUNT"));
				orderDataList.add(orderDataObj);
			}
			// Get all customer item data
			else if (relationshipType == Constants.PERISHABLE_S_ORDER_M_SELL){
			
				sellDataObj.setRefId(rs.getInt("FRESH_RELATION_SELL_ID"));
                sellDataObj.setCategoryId(rs.getInt("CATEGORY_ID"));
                sellDataObj.setItemCode(rs.getInt("SELL_ITEM_CODE"));
                sellDataObj.setSellCode(rs.getString("SELL_CODE"));
                sellDataObj.setItemDesc(rs.getString("SELL_CODE_DESC"));
				sellDataObj.setItemUPC(rs.getString("SELL_UPC"));
				sellDataObj.setTypicalPrice(rs.getDouble("TYPICAL_PRICE"));
				sellDataObj.setItemYield(rs.getDouble("YIELD"));
				sellDataObj.setItemSize(rs.getDouble("SELL_ITEM_SIZE"));
				sellDataObj.setItemUOM(rs.getInt("SELL_UOM"));
				sellDataObj.setAverageCount(rs.getInt("SELL_AVG_COUNT"));
				sellDataList.add(sellDataObj);
			}
			
			lastRelationHeaderId = curRelationHeaderId;
		}
		
		if (lastRelationHeaderId > 0){
			relaionshipObj.setOrderData(orderDataList);
			relaionshipObj.setSellData(sellDataList);
			relationshipList.add(relaionshipObj);
		}
		
		logger.debug("Total relationships: " + relationshipList.size());

		return relationshipList; 
		
	}
	
	public void getTargetLocation(HashMap<Integer, HashMap<Integer, TargetLocationDTO>> locationMap,
			int productLeveId, int productId, List<Integer> itemList) throws GeneralException {
		
		PreparedStatement stmt = null;
		ResultSet resultSet = null;		
		
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT");
			sb.append(" PLM.LOCATION_LEVEL_ID, PLM.LOCATION_ID, PLS.STORE_ID,"); 
			sb.append(" SIM.ITEM_CODE, RPZ.ZONE_NUM, CS.COMP_STR_NO");
			sb.append(" FROM PR_PRODUCT_LOCATION_MAPPING PLM");
					
			sb.append(" JOIN PR_PRODUCT_LOCATION_STORE PLS");
			sb.append(" ON PLM.PRODUCT_LOCATION_MAPPING_ID = PLS.PRODUCT_LOCATION_MAPPING_ID");

			sb.append(" JOIN STORE_ITEM_MAP SIM ON SIM.LEVEL_ID = PLS.STORE_ID");
			sb.append(" AND SIM.IS_AUTHORIZED = 'Y' AND SIM.ITEM_CODE IN (" + ListToString(itemList) + ")");

			sb.append(" JOIN RETAIL_PRICE_ZONE RPZ ON RPZ.PRICE_ZONE_ID = PLM.LOCATION_ID");
			sb.append(" JOIN COMPETITOR_STORE CS ON CS.COMP_STR_ID = PLS.STORE_ID");
			sb.append(" WHERE PLM.PRODUCT_LEVEL_ID = ").append(productLeveId);
			sb.append(" AND PLM.PRODUCT_ID = ").append(productId);
			sb.append(" ORDER BY SIM.ITEM_CODE, PLM.LOCATION_ID, PLS.STORE_ID"); 

			logger.debug("getTargetLocation SQL:" + sb.toString());


			stmt = conn.prepareStatement(sb.toString());
			stmt.setFetchSize(2000);
			resultSet = stmt.executeQuery();

			// HashMap<Integer, HashMap<Integer, TargetLocationDTO>> locationMap
			
			HashMap<Integer, TargetLocationDTO> storeMap = new HashMap<Integer, TargetLocationDTO>();
			TargetLocationDTO storeObj = new TargetLocationDTO();
			
			if (resultSet != null){
			
				while (resultSet.next()) {
					int itemCode = resultSet.getInt("ITEM_CODE");
					int storeId = resultSet.getInt("STORE_ID");
					int zoneId = resultSet.getInt("LOCATION_ID");
					
					storeMap = new HashMap<Integer, TargetLocationDTO>();
					storeObj = new TargetLocationDTO();
					
					if (locationMap.containsKey(itemCode)){
						storeMap = locationMap.get(itemCode);
						locationMap.remove(itemCode);
					}
	
					if (!storeMap.containsKey(storeId)){
						storeObj.setStoreId(storeId);
						storeObj.setStoreNo(resultSet.getString("COMP_STR_NO"));
						storeObj.setZoneId(zoneId);
						storeObj.setZoneNum(resultSet.getString("ZONE_NUM"));
						storeMap.put(storeId, storeObj);
					}
					
					locationMap.put(itemCode, storeMap);
				}
			}
			
		} catch (SQLException e) {
			logger.error("Error while executing getTargetLocation " + e);
			throw new GeneralException( "Exception in getTargetLocation()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(stmt);
		}
	}
	
	private String ListToString(List<Integer> intList){
		
		StringBuilder sb = new StringBuilder();
		
		for(int i=0; i < intList.size(); i++){
			
			if (sb.length() > 0)
				sb.append(", ");
			
			sb.append(intList.get(i));
		}
		
		return sb.toString();
	}
}
