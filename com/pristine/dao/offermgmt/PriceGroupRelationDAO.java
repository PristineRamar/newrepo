package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelnDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PriceGroupRelationDAO {
	private static Logger logger = Logger.getLogger("PriceGroupRelationDAO");
	
	private Connection conn = null;
	
	/*private static final String GET_PRICE_GROUP_DETAILS = "SELECT * FROM " + 
			" ( " + 
			" SELECT LOCATION_LEVEL_ID, LOCATION_ID, PRICE_GROUP_ID, PRICE_GROUP_NAME FROM " + 
			"   (SELECT LOCATION_LEVEL_ID, LOCATION_ID, PRICE_GROUP_ID, PRICE_GROUP_NAME, " + 
			"     RANK() OVER (PARTITION BY PRICE_GROUP_NAME ORDER BY LOCATION_LEVEL_ID DESC) RANK " + 
			"   FROM " + 
			"     (SELECT LOCATION_LEVEL_ID, LOCATION_ID, " + 
			"       PRICE_GROUP_ID, NAME AS PRICE_GROUP_NAME, " + 
			"       RCS.START_DATE, (CASE WHEN RCE.END_DATE IS NULL THEN RCE.START_DATE ELSE RCE.END_DATE END) AS END_DATE " + 
			"     FROM PR_PRICE_GROUP PG " + 
			"     LEFT JOIN RETAIL_CALENDAR RCS " + 
			"     ON PG.START_CALENDAR_ID = RCS.CALENDAR_ID " + 
			"     LEFT JOIN RETAIL_CALENDAR RCE " + 
			"     ON PG.END_CALENDAR_ID     = RCE.CALENDAR_ID " + 
			"     WHERE ((LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID +
			"     AND LOCATION_ID           = ?) " + 
			"     OR (LOCATION_LEVEL_ID     = " + Constants.ZONE_LIST_LEVEL_ID +
			"     AND LOCATION_ID          IN " + 
			"       (SELECT LOCATION_ID " + 
			"       FROM " + 
			"         (SELECT CHILD_LOCATION_ID, CHILD_LOCATION_LEVEL_ID, LOCATION_LEVEL_ID, LOCATION_ID " + 
			"         FROM LOCATION_GROUP_RELATION PGR WHERE LOCATION_LEVEL_ID = " + Constants.ZONE_LIST_LEVEL_ID + ") " + 
			"       WHERE CHILD_LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + " AND CHILD_LOCATION_ID = ? " + 
			"       )) " + 
			"     OR (LOCATION_LEVEL_ID = " + Constants.DIVISION_LEVEL_ID + " AND LOCATION_ID = ?) " + 
			"     OR (LOCATION_LEVEL_ID = " + Constants.CHAIN_LEVEL_ID + " AND LOCATION_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y'))) " + 
			"     ) " + 
			"   WHERE (START_DATE <= TO_DATE(?, 'MM/DD/YYYY') AND END_DATE >= TO_DATE(?, 'MM/DD/YYYY')) " + 
			"   OR (START_DATE    <= TO_DATE(?, 'MM/DD/YYYY') AND END_DATE IS NULL) " + 
			"   ) WHERE RANK = 1) PRICE_GROUP, " + 
			" ( " + 
			" SELECT " +
			//" IL.BRAND_ID, " +
			//" IL.ITEM_SIZE AS IL_ITEM_SIZE, " +
			//" UL1.ID AS IL_UOM_ID, " +
			//" UL1.NAME AS IL_UOM_NAME, " +		
			" UL2.NAME AS PGD_UOM_NAME, " +
			" UL2.ID AS PGD_UOM_ID, " +
			" PRICE_GROUP_DETAIL.* FROM ( " + 
			" SELECT PGD.PRICE_GROUP_ID, PGD.BRAND_TIER_ID, PBC.BRAND_CLASS_ID, PGD.SIZE_FAMILY_ID, PGD.IS_PRICE_GROUP_LEAD, PGD.IS_SIZE_FAMILY_LEAD, " +
			" PGD.SIZE_RELN_TEXT, PGD.ITEM_SIZE AS PGD_ITEM_SIZE, PGD.UOM_ID, PGD.BRAND_SIZE_PRECEDENCE," + 
			
			" PGD.LIR_ID_OR_ITEM_CODE AS ITEM_CODE, PGD.LIR_IND AS LIR_IND, " + 
			" PGBR.LIR_ID_OR_ITEM_CODE AS BRAND_RELATED_ITEM_CODE, PGBR.LIR_IND AS BRAND_RELATED_LIR_IND, " +
			
			" PGBR.PRICE_RELN_ID AS BRAND_RELN_ID, PRECEDENCE AS BRAND_PRECEDENCE, " + 
			" PGSR.PRICE_RELN_ID AS SIZE_RELN_ID, PGSR.HIGHER_TO_LOWER_FLAG AS HTOL " + 
			" FROM PR_PRICE_GROUP_DETAIL PGD " + 
			" LEFT JOIN PR_PRICE_GROUP_BRAND_RELATION PGBR ON PGD.PRICE_GROUP_DETAIL_ID = PGBR.PRICE_GROUP_DETAIL_ID   " + 
			" LEFT JOIN PR_PRICE_GROUP_SIZE_RELATION PGSR ON PGD.PRICE_GROUP_DETAIL_ID = PGSR.PRICE_GROUP_DETAIL_ID " + 
			" LEFT JOIN PR_BRAND_TIER_LOOKUP PBC ON PGD.BRAND_TIER_ID = PBC.BRAND_TIER_ID " + 
			" )PRICE_GROUP_DETAIL LEFT JOIN ITEM_LOOKUP IL ON PRICE_GROUP_DETAIL.ITEM_CODE = IL.ITEM_CODE " + 
			" LEFT JOIN UOM_LOOKUP UL1 " + 
			" ON IL.UOM_ID = UL1.ID " + 
			" LEFT JOIN UOM_LOOKUP UL2 " + 
			" ON PRICE_GROUP_DETAIL.UOM_ID = UL2.ID " + 
			" WHERE (PRICE_GROUP_DETAIL.ITEM_CODE IN (SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR %QUERY_CONDITION%)    " + 
			" WHERE CHILD_PRODUCT_LEVEL_ID = 1)  AND PRICE_GROUP_DETAIL.LIR_IND = 'N') " + 
			" OR (PRICE_GROUP_DETAIL.ITEM_CODE IN " +    
			" (SELECT RET_LIR_ID FROM ITEM_LOOKUP WHERE ITEM_CODE IN( SELECT CHILD_PRODUCT_ID FROM " + 
			" (SELECT * FROM PRODUCT_GROUP_RELATION PGR %QUERY_CONDITION%)    " + 
			" WHERE CHILD_PRODUCT_LEVEL_ID = 1))  AND PRICE_GROUP_DETAIL.LIR_IND = 'Y') " + 
			
			" ) PRICE_GROUP_DETAIL WHERE " + 
			" PRICE_GROUP.PRICE_GROUP_ID = PRICE_GROUP_DETAIL.PRICE_GROUP_ID " +
			//TODO:: comment below line in production, only for debugging
			//" AND PRICE_GROUP.PRICE_GROUP_ID IN (1525) " +
			" ORDER BY PRICE_GROUP_NAME";*/
	
	
	private static final String GET_PRICE_GROUP_DETAILS = "SELECT * FROM " + 
			" ( " + 
			" SELECT LOCATION_LEVEL_ID, LOCATION_ID, PRICE_GROUP_ID, PRICE_GROUP_NAME FROM " + 
			"   (SELECT LOCATION_LEVEL_ID, LOCATION_ID, PRICE_GROUP_ID, PRICE_GROUP_NAME, " + 
			"     RANK() OVER (PARTITION BY PRICE_GROUP_NAME ORDER BY LOCATION_LEVEL_ID DESC) RANK " + 
			"   FROM " + 
			"     (SELECT LOCATION_LEVEL_ID, LOCATION_ID, " + 
			"       PRICE_GROUP_ID, NAME AS PRICE_GROUP_NAME, " + 
			"       RCS.START_DATE, (CASE WHEN RCE.END_DATE IS NULL THEN RCE.START_DATE ELSE RCE.END_DATE END) AS END_DATE " + 
			"     FROM PR_PRICE_GROUP PG " + 
			"     LEFT JOIN RETAIL_CALENDAR RCS " + 
			"     ON PG.START_CALENDAR_ID = RCS.CALENDAR_ID " + 
			"     LEFT JOIN RETAIL_CALENDAR RCE " + 
			"     ON PG.END_CALENDAR_ID     = RCE.CALENDAR_ID " + 
			"     WHERE ((LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID +
			"     AND LOCATION_ID           = ?) " + 
			"     OR (LOCATION_LEVEL_ID     = " + Constants.ZONE_LIST_LEVEL_ID +
			"     AND LOCATION_ID          IN " + 
			"       (SELECT LOCATION_ID " + 
			"       FROM " + 
			"         (SELECT CHILD_LOCATION_ID, CHILD_LOCATION_LEVEL_ID, LOCATION_LEVEL_ID, LOCATION_ID " + 
			"         FROM LOCATION_GROUP_RELATION PGR WHERE LOCATION_LEVEL_ID = " + Constants.ZONE_LIST_LEVEL_ID + ") " + 
			"       WHERE CHILD_LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + " AND CHILD_LOCATION_ID = ? " + 
			"       )) " + 
			"     OR (LOCATION_LEVEL_ID = " + Constants.DIVISION_LEVEL_ID + " AND LOCATION_ID = ?) " + 
			"     OR (LOCATION_LEVEL_ID = " + Constants.CHAIN_LEVEL_ID + " AND LOCATION_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y'))) " + 
			"     ) " + 
			"   WHERE (START_DATE <= TO_DATE(?, 'MM/DD/YYYY') AND END_DATE >= TO_DATE(?, 'MM/DD/YYYY')) " + 
			"   OR (START_DATE    <= TO_DATE(?, 'MM/DD/YYYY') AND END_DATE IS NULL) " + 
			"   ) WHERE RANK = 1) PRICE_GROUP, " + 
			" ( " + 
			
			" SELECT UL2.NAME AS PGD_UOM_NAME, UL2.ID AS PGD_UOM_ID, PGD.PRICE_GROUP_ID, PGD.BRAND_TIER_ID, " +
			" PBC.BRAND_CLASS_ID, PBC.BRAND_TIER_NAME, PGD.SIZE_FAMILY_ID, PGD.IS_PRICE_GROUP_LEAD, PGD.IS_SIZE_FAMILY_LEAD, " +
			" PGD.SIZE_RELN_TEXT, PGD.ITEM_SIZE AS PGD_ITEM_SIZE, PGD.UOM_ID, PGD.BRAND_SIZE_PRECEDENCE, " +
			" PGD.LIR_ID_OR_ITEM_CODE AS ITEM_CODE, PGD.LIR_IND AS LIR_IND, " +
			" PGBR.LIR_ID_OR_ITEM_CODE AS BRAND_RELATED_ITEM_CODE, PGBR.LIR_IND AS BRAND_RELATED_LIR_IND, " +
			" PGBR.PRICE_RELN_ID AS BRAND_RELN_ID, PRECEDENCE AS BRAND_PRECEDENCE, " +
			" PGSR.PRICE_RELN_ID AS SIZE_RELN_ID, PGSR.HIGHER_TO_LOWER_FLAG AS HTOL " +
			" FROM ( " +
			" (SELECT * FROM PR_PRICE_GROUP_DETAIL PGD WHERE PGD.LIR_ID_OR_ITEM_CODE IN " +
			" (SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID " +
			" FROM PRODUCT_GROUP_RELATION_REC PGR START WITH product_level_id = ? AND product_id = ? " +
			" CONNECT BY prior child_product_id = product_id " +
			" AND prior child_product_level_id    = product_level_id " +
			" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1 " +
			" ) AND PGD.LIR_IND = 'N') " +
			" UNION ALL " +
			" (SELECT * FROM PR_PRICE_GROUP_DETAIL PGD WHERE PGD.LIR_ID_OR_ITEM_CODE IN " +
			" (SELECT DISTINCT(RET_LIR_ID) FROM ITEM_LOOKUP WHERE ITEM_CODE IN " +
			" (SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_LEVEL_ID, " +
			" CHILD_PRODUCT_ID FROM PRODUCT_GROUP_RELATION_REC PGR " +
			" START WITH product_level_id = ? AND product_id  = ? " +
			" CONNECT BY prior child_product_id = product_id " +
			" AND prior child_product_level_id    = product_level_id " +
			" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1 " +
			" ) AND RET_LIR_ID IS NOT NULL) AND PGD.LIR_IND = 'Y' " +
			" )) PGD " +
			" LEFT JOIN PR_PRICE_GROUP_BRAND_RELATION PGBR " +
			" ON PGD.PRICE_GROUP_DETAIL_ID = PGBR.PRICE_GROUP_DETAIL_ID " +
			" LEFT JOIN PR_PRICE_GROUP_SIZE_RELATION PGSR " +
			" ON PGD.PRICE_GROUP_DETAIL_ID = PGSR.PRICE_GROUP_DETAIL_ID " +
			" LEFT JOIN PR_BRAND_TIER_LOOKUP PBC " +
			" ON PGD.BRAND_TIER_ID = PBC.BRAND_TIER_ID " +
			" LEFT JOIN ITEM_LOOKUP IL " +
			" ON PGD.LIR_ID_OR_ITEM_CODE = IL.ITEM_CODE " +
			" LEFT JOIN UOM_LOOKUP UL1 " +
			" ON IL.UOM_ID = UL1.ID " +
			" LEFT JOIN UOM_LOOKUP UL2 " +
			" ON PGD.UOM_ID = UL2.ID " + 
			
			" ) PRICE_GROUP_DETAIL WHERE " + 
			" PRICE_GROUP.PRICE_GROUP_ID = PRICE_GROUP_DETAIL.PRICE_GROUP_ID " +
			//TODO:: comment below line in production, only for debugging
			//" AND PRICE_GROUP.PRICE_GROUP_ID IN (1525) " +
			" ORDER BY PRICE_GROUP_NAME";
	
	private static final String GET_PRICE_GROUP_RELATION = "SELECT VALUE_TYPE, MIN_VALUE, MAX_VALUE, OPERATOR_TEXT, "
			+ " RETAIL_TYPE FROM PR_PRICE_GROUP_RELATION PGR "
			+ " LEFT JOIN PR_RELATION_OPERATOR_LOOKUP PROL ON PGR.RELN_OPERATOR_ID = PROL.RELN_OPERATOR_ID "
			+ " WHERE PGR.PRICE_RELN_ID = ? ";
	
	public PriceGroupRelationDAO(Connection conn){
		this.conn = conn;
	}
	
	/**
	 * Retrieves Price Group relations for input product and sets related item
	 * @param inputDTO
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> getPriceGroupDetails(PRStrategyDTO inputDTO,
			HashMap<ItemKey, PRItemDTO> itemDataMap) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    //Keep price group name and its detail
	    HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupData = new HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>>();
	    //Keep related items with relation id as key and corresponding item as value
	    HashMap<Integer, PRPriceGroupRelnDTO> priceRelnMap = new HashMap<Integer, PRPriceGroupRelnDTO>(); 
	    //Keep size lead item with family id as key and item as value
	    HashMap<Integer, PRPriceGroupDTO> sizeLeadMap = new HashMap<Integer, PRPriceGroupDTO>();
	    String sql = GET_PRICE_GROUP_DETAILS;
	    //23rd Nov 15, commented as query is changed for performance improvement
	    //sql = sql.replaceAll("%QUERY_CONDITION%", getQueryCondition(inputDTO.getProductLevelId(), inputDTO.getProductId()));
	    logger.debug("Price Group data retrieval query - " + sql);
	    
	    int priceGroupId = -1;
	    String priceGroupName = null;
	    boolean isIgnorePriceGroup = false;
	    try{
	    	RetailPriceZoneDAO rpzDao = new RetailPriceZoneDAO();
			int divisionId = 0;
			if (inputDTO.isPriceTestZone())
				divisionId = rpzDao.getDivisionIdForZone(conn, inputDTO.getTempLocationID());
			else
				divisionId = rpzDao.getDivisionIdForZone(conn, inputDTO.getLocationId());
			
		    int counter = 0;
			statement = conn.prepareStatement(sql);
			if (inputDTO.isPriceTestZone()) {
				statement.setInt(++counter, inputDTO.getTempLocationID());
				statement.setInt(++counter, inputDTO.getTempLocationID());
			} else {
				statement.setInt(++counter, inputDTO.getLocationId());
				statement.setInt(++counter, inputDTO.getLocationId());
			}
			statement.setInt(++counter, divisionId);
			statement.setString(++counter, inputDTO.getStartDate());
			statement.setString(++counter, inputDTO.getStartDate());
			statement.setString(++counter, inputDTO.getStartDate());
			
			statement.setInt(++counter, inputDTO.getProductLevelId());
			statement.setLong(++counter, inputDTO.getProductId());
			statement.setInt(++counter, inputDTO.getProductLevelId());
			statement.setLong(++counter, inputDTO.getProductId());
		    //23rd Nov 15, commented below lines and added above 4 lines
			
//			if(inputDTO.getProductLevelId() > 1){
//				statement.setInt(++counter, inputDTO.getProductLevelId());
//				if(inputDTO.getProductId() > 0){
//					statement.setLong(++counter, inputDTO.getProductId());
//				}	
//			}else if(inputDTO.getProductLevelId() == 1){
//				statement.setLong(++counter, inputDTO.getProductLevelId());
//			}
//			if(inputDTO.getProductLevelId() > 1){
//				statement.setInt(++counter, inputDTO.getProductLevelId());
//				if(inputDTO.getProductId() > 0){
//					statement.setLong(++counter, inputDTO.getProductId());
//				}	
//			}else if(inputDTO.getProductLevelId() == 1){
//				statement.setLong(++counter, inputDTO.getProductLevelId());
//			}
			statement.setFetchSize(50000);
	        resultSet = statement.executeQuery();
	        while(resultSet.next()){
	        	int tPriceGroupId = resultSet.getInt("PRICE_GROUP_ID");
	        	String tPriceGroupName = resultSet.getString("PRICE_GROUP_NAME");
	        	//First time or same price group
	        	if(priceGroupId < 0 || priceGroupId != tPriceGroupId){
	        		if(priceGroupId > 0){
	        			//logger.debug("1. Setting related item brand/size data for - " + priceGroupName);
		        		setRelatedItemBrandData(priceGroupData.get(priceGroupName));
		    	        setRelatedItemForSizeFamily(priceGroupData.get(priceGroupName), sizeLeadMap);
		    	        sizeLeadMap = new HashMap<Integer, PRPriceGroupDTO>();
	        		}
	        		
	        		priceGroupId = tPriceGroupId;
	        		priceGroupName = tPriceGroupName;
	        		if(priceGroupData.get(priceGroupName) != null)
	        			isIgnorePriceGroup = true;
	        		else{ 
	        			isIgnorePriceGroup = false;
	        			HashMap<ItemKey, PRPriceGroupDTO> priceGroupItemData = new HashMap<ItemKey, PRPriceGroupDTO>();
	        			priceGroupData.put(priceGroupName, priceGroupItemData);
	        		}
	        	}
	        	
	        	if(!isIgnorePriceGroup){
	        		HashMap<ItemKey, PRPriceGroupDTO> priceGroupItemData = priceGroupData.get(priceGroupName);
	        		PRPriceGroupDTO pgData = new PRPriceGroupDTO();
	        		pgData.setPriceGroupId(priceGroupId);
	        		pgData.setPriceGroupName(priceGroupName);
	        		String brandSizePrecedence = resultSet.getString("BRAND_SIZE_PRECEDENCE");
	        		if(brandSizePrecedence != null && brandSizePrecedence.length() > 0){
	        			pgData.setBrandSizePrecedence(brandSizePrecedence.charAt(0));
	        		}else{
	        			pgData.setBrandSizePrecedence(PRConstants.BRAND_SIZE_PRECECENDENCE_NA);
	        		}
	        		String lirInd = resultSet.getString("LIR_IND");
	        		//pgData.setBrandId(resultSet.getInt("BRAND_ID"));
	        		 
	        		
	        		pgData.setBrandTierId(resultSet.getInt("BRAND_TIER_ID"));
	        		pgData.setBrandTierName(resultSet.getString("BRAND_TIER_NAME"));
	        		pgData.setBrandClassId(resultSet.getInt("BRAND_CLASS_ID"));
	        		int itemCode = resultSet.getInt("ITEM_CODE");
	        		//int itemCode = resultSet.getInt("LIR_ID_OR_ITEM_CODE");
	        		pgData.setItemCode(itemCode);
	        		
	        		if(String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)){
	        			pgData.setIsLig(true);
					}
	        		
					if (resultSet.getObject("PGD_ITEM_SIZE") != null) {
						pgData.setItemSize(resultSet.getDouble("PGD_ITEM_SIZE"));
						if (resultSet.getObject("PGD_UOM_NAME") != null)
							pgData.setUomName(resultSet.getString("PGD_UOM_NAME"));
						else
							pgData.setUomName("");
					} /*else if (resultSet.getObject("IL_ITEM_SIZE") != null) {
						pgData.setItemSize(resultSet.getDouble("IL_ITEM_SIZE"));
						if (resultSet.getObject("IL_UOM_NAME") != null)
							pgData.setUomName(resultSet.getString("IL_UOM_NAME"));
						else
							pgData.setUomName("");
					}*/
	        		
	        		pgData.setSizeFamilyId(resultSet.getInt("SIZE_FAMILY_ID"));
	        		
	        		if(resultSet.getObject("SIZE_RELN_TEXT") != null)
	        			pgData.setSizeRelationText(resultSet.getString("SIZE_RELN_TEXT"));
	        			        		
	        		//13th Aug 2015 when there is multiple relation to a size lead item, Size lead item can appear more than once, 
	        		//with only one occurrence as lead item. If any one occurrence is marked as size lead, then consider
	        		//there is size lead item for the price group
	        		if(pgData.getIsSizeFamilyLead() == '\u0000' || pgData.getIsSizeFamilyLead() == Constants.NO)
	        			pgData.setIsSizeFamilyLead(resultSet.getString("IS_SIZE_FAMILY_LEAD").charAt(0));
	        		//if(Constants.YES == pgData.getIsSizeFamilyLead()){
	        		if(Constants.YES == resultSet.getString("IS_SIZE_FAMILY_LEAD").charAt(0)){	        		
	        			if(pgData.getSizeFamilyId() > 0)
	        				sizeLeadMap.put(pgData.getSizeFamilyId(), pgData);
	        			else
	        				logger.warn("No Size Family assigned. Item marked as Size family lead");
	        		}
	        		//Add size related items
	        		PRPriceGroupRelatedItemDTO sizeRelatedItem = new PRPriceGroupRelatedItemDTO();
	        		int priceRelnIdForSize = resultSet.getInt("SIZE_RELN_ID");
	        		//If item is part of size family
	        		if(pgData.getSizeFamilyId() > 0){
	        			if(resultSet.getObject("HTOL") != null){
	        				sizeRelatedItem.setHtol(resultSet.getString("HTOL").charAt(0));
	        			}else{
	        				sizeRelatedItem.setHtol(PropertyManager.getProperty("DEFAULT_HIGHER_TO_LOWER_FLAG", "Y").charAt(0));
	        			}
	        			
	        			if(priceRelnMap.get(priceRelnIdForSize) != null){
			        		sizeRelatedItem.setPriceRelation(priceRelnMap.get(priceRelnIdForSize));
				       	}else{
				       		PRPriceGroupRelnDTO pgReln = getPriceGroupRelation(priceRelnIdForSize);
				       		// Set Retail Type as Unit when no Retail Type is specified for Size Relation
				       		if(pgReln != null){
					       		if(pgReln.getRetailType() != PRConstants.RETAIL_TYPE_SHELF && pgReln.getRetailType() != PRConstants.RETAIL_TYPE_UNIT)
				        			pgReln.setRetailType(PRConstants.RETAIL_TYPE_UNIT);
				        		sizeRelatedItem.setPriceRelation(pgReln);
				       		}
			        		priceRelnMap.put(priceRelnIdForSize, pgReln);
			        	}
	        		}
	        		//Add brand related items
	        		PRPriceGroupRelatedItemDTO brandRelatedItem = new PRPriceGroupRelatedItemDTO();
	        		brandRelatedItem.setRelatedItemCode(resultSet.getInt("BRAND_RELATED_ITEM_CODE"));
	        		//brandRelatedItem.setRelatedItemCode(resultSet.getInt("BRAND_LIR_ID_OR_ITEM_CODE"));
	        		lirInd = resultSet.getString("BRAND_RELATED_LIR_IND");
	        		if(String.valueOf(Constants.YES).equalsIgnoreCase(lirInd)){
	        			brandRelatedItem.setIsLig(true);
					}
	        		brandRelatedItem.setRelationType(PRConstants.BRAND_RELATION);
	        		brandRelatedItem.setBrandPrecedence(resultSet.getInt("BRAND_PRECEDENCE"));
	        		if(brandRelatedItem.getRelatedItemCode() > 0){
		        		int priceRelnIdForBrand = resultSet.getInt("BRAND_RELN_ID");
		        		if(priceRelnMap.get(priceRelnIdForBrand) != null){
			        		brandRelatedItem.setPriceRelation(priceRelnMap.get(priceRelnIdForBrand));
				        }else{
				        	PRPriceGroupRelnDTO pgReln = getPriceGroupRelation(priceRelnIdForBrand);
				        	// Set Retail Type as Shelf when no Retail Type is specified for Brand Relation
				        	if(pgReln != null){
					        	if(pgReln.getRetailType() != PRConstants.RETAIL_TYPE_SHELF && pgReln.getRetailType() != PRConstants.RETAIL_TYPE_UNIT)
					        		pgReln.setRetailType(PRConstants.RETAIL_TYPE_SHELF);
					        	brandRelatedItem.setPriceRelation(pgReln);
				        	}
				        	priceRelnMap.put(priceRelnIdForBrand, pgReln);
				       	}
	        		}
	        		//Set brand id, item size and uom from item map
	        		ItemKey itemKey = PRCommonUtil.getItemKey(itemCode, pgData.getIsLig());
					if (itemDataMap.get(itemKey) != null) {
						pgData.setBrandId(itemDataMap.get(itemKey).getBrandId());
						// If not updated already
						if (pgData.getItemSize() == 0) 
							pgData.setItemSize(itemDataMap.get(itemKey).getItemSize());
						if (pgData.getUomName() == null || pgData.getUomName() == "") 
							pgData.setUomName(itemDataMap.get(itemKey).getUOMName());
					}
	        	
					//Add size and brand related item
	        		if(priceGroupItemData.get(itemKey) != null){
	        			PRPriceGroupDTO pgDTO = priceGroupItemData.get(itemKey);
	        			if(brandRelatedItem.getRelatedItemCode() > 0)
	        				pgDTO.addBrandRelation(brandRelatedItem);
	        			if(pgData.getSizeFamilyId() > 0)
	        				pgDTO.addSizeRelation(sizeRelatedItem);
	        		}else{
	        			if(brandRelatedItem.getRelatedItemCode() > 0)
	        				pgData.addBrandRelation(brandRelatedItem);
    					if(pgData.getSizeFamilyId() > 0)
    						pgData.addSizeRelation(sizeRelatedItem);
    					priceGroupItemData.put(itemKey, pgData);
	        		}
	        	}
	        }
	        
	       // logger.debug("2. Setting related item brand/size data for - " + priceGroupName);
	        setRelatedItemBrandData(priceGroupData.get(priceGroupName));
	        setRelatedItemForSizeFamily(priceGroupData.get(priceGroupName), sizeLeadMap);
	    }catch (SQLException e){
			logger.error("Error while executing GET_PRICE_GROUP_DETAILS " + e);
			throw new GeneralException("Error while executing GET_PRICE_GROUP_DETAILS", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	    
	    return priceGroupData;
	}
	
	/**
	 * Method that sets brandTierId of brand related items
	 * @param priceGroupData
	 */
	private void setRelatedItemBrandData(HashMap<ItemKey, PRPriceGroupDTO> itemsInPriceGroup){
		if(itemsInPriceGroup != null){
			for(PRPriceGroupDTO pgData : itemsInPriceGroup.values()){
				TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationMap = pgData.getRelationList();
				if(relationMap != null){
					ArrayList<PRPriceGroupRelatedItemDTO> brandRelatedItems = relationMap.get(PRConstants.BRAND_RELATION);
					if(brandRelatedItems != null)
						for(PRPriceGroupRelatedItemDTO relatedItem : brandRelatedItems){
							//int relatedItemCode = relatedItem.getRelatedItemCode();
							ItemKey itemKey = PRCommonUtil.getRelatedItemKey(relatedItem);
							if(itemsInPriceGroup.get(itemKey) != null){
								relatedItem.setRelatedItemBrandTierId(itemsInPriceGroup.get(itemKey).getBrandTierId());
								relatedItem.setRelatedItemBrandTier(itemsInPriceGroup.get(itemKey).getBrandTierName());
								relatedItem.setIsLig(itemsInPriceGroup.get(itemKey).getIsLig());
								relatedItem.setRelatedItemBrandId(itemsInPriceGroup.get(itemKey).getBrandId());
								relatedItem.setRelatedItemSize(itemsInPriceGroup.get(itemKey).getItemSize());
								relatedItem.setRelatedUOMName(itemsInPriceGroup.get(itemKey).getUomName());
							}else{
								logger.warn("Related Item Code " + itemKey.getItemCodeOrRetLirId() + " brand info not present in price group");
							}
						}
				}
			}
		}else{
			logger.warn("No items in price group");
		}
	}
	
	/**
	 * 
	 * @param sizeFamilyMap
	 * @param sizeLeadMap
	 */
	private void addMissingSizeFamilyLead(HashMap<Integer, TreeMap<Double, ArrayList<PRPriceGroupDTO>>> sizeFamilyMap,
			HashMap<Integer, PRPriceGroupDTO> sizeLeadMap) {
		for(Map.Entry<Integer, PRPriceGroupDTO> leadEntry: sizeLeadMap.entrySet()) {
			if(sizeFamilyMap.containsKey(leadEntry.getKey())) {
				PRPriceGroupDTO leadItemDTO = leadEntry.getValue();
				ItemKey leadItemKey = new ItemKey(leadItemDTO.getItemCode(),
						leadItemDTO.getIsLig() ? PRConstants.LIG_ITEM_INDICATOR : PRConstants.NON_LIG_ITEM_INDICATOR);
				
				TreeMap<Double, ArrayList<PRPriceGroupDTO>> map = sizeFamilyMap.get(leadEntry.getKey());
				boolean noLeadFound = true;
				for(Map.Entry<Double, ArrayList<PRPriceGroupDTO>> familyEntry: map.entrySet()) {
					for(PRPriceGroupDTO priceGroupDTO: familyEntry.getValue()) {
						ItemKey itemKey = new ItemKey(priceGroupDTO.getItemCode(),
								priceGroupDTO.getIsLig() ? PRConstants.LIG_ITEM_INDICATOR : PRConstants.NON_LIG_ITEM_INDICATOR);
						if(itemKey.equals(leadItemKey)) {
							noLeadFound = false;
							break;
						}
					}
				}
				if(noLeadFound) {
					ArrayList<PRPriceGroupDTO> leadItems = new ArrayList<>();
					leadItems.add(leadItemDTO);
					map.put(leadItemDTO.getItemSize(), leadItems);
				}
			}
		}
	}
	
	
	/***
	 * 
	 * @param itemsInPriceGroup
	 * @param sizeLeadMap (SizeFamilyId as Key, PRPriceGroupDTO as value)
	 */
	private void setRelatedItemForSizeFamily(HashMap<ItemKey, PRPriceGroupDTO> itemsInPriceGroup, HashMap<Integer, PRPriceGroupDTO> sizeLeadMap){
		HashMap<Integer, TreeMap<Double, ArrayList<PRPriceGroupDTO>>> sizeFamilyMap = new HashMap<Integer, TreeMap<Double, ArrayList<PRPriceGroupDTO>>>();
		
		//13th Aug 2015 when there is multiple relation to a size lead item, Size lead item can appear more than once, 
		//with only one occurrence as lead item. 
		
		 //Set if an item is size family lead item 
		if (sizeLeadMap != null && sizeLeadMap.size() > 0 && itemsInPriceGroup != null) {
			// Loop each size lead item
			for (PRPriceGroupDTO sizeLeadItemPriceGroupDTO : sizeLeadMap.values()) {
				ItemKey itemKey = null;
				if (sizeLeadItemPriceGroupDTO.getIsLig())
					itemKey = new ItemKey(sizeLeadItemPriceGroupDTO.getItemCode(), PRConstants.LIG_ITEM_INDICATOR);
				else
					itemKey = new ItemKey(sizeLeadItemPriceGroupDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);

				//Set size family lead 
				if (itemsInPriceGroup.get(itemKey) != null
						&& itemsInPriceGroup.get(itemKey).getSizeFamilyId() == sizeLeadItemPriceGroupDTO
								.getSizeFamilyId()) {
					itemsInPriceGroup.get(itemKey).setIsSizeFamilyLead(sizeLeadItemPriceGroupDTO.getIsSizeFamilyLead());
				}
			}
		}
		//Keep all size related items in ascending order of size of a size family
		if(itemsInPriceGroup != null) {
			for(Map.Entry<ItemKey, PRPriceGroupDTO> entry : itemsInPriceGroup.entrySet()){
				PRPriceGroupDTO pgDTO = entry.getValue();
				int sizeFamilyId = pgDTO.getSizeFamilyId();
				
				if(sizeFamilyId > 0){
					TreeMap<Double, ArrayList<PRPriceGroupDTO>> tMap = null;
					if(sizeFamilyMap.get(sizeFamilyId) != null){
						tMap = sizeFamilyMap.get(sizeFamilyId);
						ArrayList<PRPriceGroupDTO> tList = null;
						if(tMap.get(pgDTO.getItemSize()) != null)
							tList = tMap.get(pgDTO.getItemSize());
						else
							tList = new ArrayList<PRPriceGroupDTO>();
						tList.add(pgDTO);
						tMap.put(pgDTO.getItemSize(), tList);
					}else{
						tMap = new TreeMap<Double, ArrayList<PRPriceGroupDTO>>();
						ArrayList<PRPriceGroupDTO> tList = new ArrayList<PRPriceGroupDTO>();
						tList.add(pgDTO);
						tMap.put(pgDTO.getItemSize(), tList);
					}
					sizeFamilyMap.put(sizeFamilyId, tMap);
				}
			}
		}
		
		addMissingSizeFamilyLead(sizeFamilyMap, sizeLeadMap);
		
		// Set size related item
		if(sizeFamilyMap != null)
			//Loop each Size Family
			for(Map.Entry<Integer, TreeMap<Double, ArrayList<PRPriceGroupDTO>>> outEntry : sizeFamilyMap.entrySet()){
				int sizeFamilyId = outEntry.getKey();
				//Size Family and its lead item code
				if(sizeLeadMap.get(sizeFamilyId) != null){
					//Get Size Lead Item
					PRPriceGroupDTO priceGroupDTO = sizeLeadMap.get(sizeFamilyId);
					ItemKey itemKey = PRCommonUtil.getItemKey(priceGroupDTO.getItemCode(), priceGroupDTO.getIsLig());
					//Get Size Lead Item Details
					double leadItemSize = priceGroupDTO.getItemSize();
					String leadUOMName =  priceGroupDTO.getUomName();
					boolean isLig =  priceGroupDTO.getIsLig();
					double prevSize = -1;
					PRPriceGroupDTO prevItem = null;
		    		boolean isLeadEncountered = false;
		    		//logger.debug("leadItemSize:" + leadItemSize);
		    		//loop each items of a size family
					for(Map.Entry<Double, ArrayList<PRPriceGroupDTO>> inEntry : outEntry.getValue().entrySet()){
						double curSize = inEntry.getKey();
						//logger.debug("curSize:" + curSize);
						//If size lead item is encountered
						// e.g. 6, 24, 32 - 6 as lead
						// e.g. 6, 24, 32 - 32 as lead
						if(leadItemSize == curSize){
							isLeadEncountered = true;
							//logger.debug("prevSize:" + prevSize);
							if(prevSize > 0){
								//This will get triggered when higher size item is lead item 
								//e.g. item with size 32 (with 32 as lead) i.e 24 is dependent on 32
								ArrayList<PRPriceGroupDTO> prevItemList = outEntry.getValue().get(prevSize);
								for(PRPriceGroupDTO tPrevItem : prevItemList){
									PRPriceGroupRelatedItemDTO relatedSizeItem = tPrevItem.getRelationList().get(PRConstants.SIZE_RELATION).get(0);
									relatedSizeItem.setRelatedItemCode(itemKey.getItemCodeOrRetLirId());
									relatedSizeItem.setRelatedItemSize(leadItemSize);
									relatedSizeItem.setRelatedUOMName(leadUOMName);
									relatedSizeItem.setIsLig(isLig);
								}
							}
							
							ArrayList<PRPriceGroupDTO> curItemList = outEntry.getValue().get(curSize);
							for(PRPriceGroupDTO tCurItem : curItemList){
								//logger.debug("tCurItem ItemCode :" + tCurItem.getItemCode() + ",tCurItem.getIsSizeFamilyLead():" + 
										//tCurItem.getIsSizeFamilyLead());
								if(tCurItem.getIsSizeFamilyLead() != Constants.YES){
									PRPriceGroupRelatedItemDTO relatedSizeItem = tCurItem.getRelationList().get(PRConstants.SIZE_RELATION).get(0);
									relatedSizeItem.setRelatedItemCode(itemKey.getItemCodeOrRetLirId());
									relatedSizeItem.setRelatedItemSize(leadItemSize);
									relatedSizeItem.setRelatedUOMName(leadUOMName);
									relatedSizeItem.setIsLig(isLig);
								}else{
									//when lowest size is lead item e.g. item with size 6 (with 6 as lead)
									prevItem = tCurItem;
									prevSize = curSize;
								}
							} 
						}else if(isLeadEncountered){
							//This will get triggered when lowest size item is lead item 
							//e.g. item with size 24 (with 6 as lead) i.e 24 is dependent on 6
							//prev item -- item with size 6 (with 6 as lead), cur item - item with size 24 (with 6 as lead)
							//e.g. item with size 32 (with 6 as lead) i.e 32 is dependent on 24
							//prev item -- item with size 24 (with 6 as lead), cur item - item with size 32 (with 6 as lead)
							
							ArrayList<PRPriceGroupDTO> curItemList = outEntry.getValue().get(curSize);
		    				for(PRPriceGroupDTO curItem : curItemList){
		    					PRPriceGroupRelatedItemDTO relatedSizeItem = curItem.getRelationList().get(PRConstants.SIZE_RELATION).get(0);
		    					relatedSizeItem.setRelatedItemCode(prevItem.getItemCode());
		    					relatedSizeItem.setRelatedItemSize(prevItem.getItemSize());
		    					relatedSizeItem.setRelatedUOMName(prevItem.getUomName());
		    					relatedSizeItem.setIsLig(prevItem.getIsLig());
		    				}
		    				prevItem = curItemList.get(0);
		    				prevSize = curSize;
						}else{
							//This will get triggered when highest size item is lead item 
							//e.g. item with size 6 (with 32 as lead)
		    				ArrayList<PRPriceGroupDTO> curItemList = outEntry.getValue().get(curSize);
		    				PRPriceGroupDTO curItem = curItemList.get(0);
		    				if(prevSize > 0){
		    					//e.g. item with size 24 (with 32 as lead) 6 is dependent on 24
		    					//prev item -- item with size 6 (with 32 as lead), cur item - item with size 24 (with 32 as lead)
		    					ArrayList<PRPriceGroupDTO> prevItemList = outEntry.getValue().get(prevSize);
		    					for(PRPriceGroupDTO tPrevItem : prevItemList){
		    						PRPriceGroupRelatedItemDTO relatedSizeItem = tPrevItem.getRelationList().get(PRConstants.SIZE_RELATION).get(0);
		    						relatedSizeItem.setRelatedItemCode(curItem.getItemCode());
		    						relatedSizeItem.setRelatedItemSize(curItem.getItemSize());
		    						relatedSizeItem.setRelatedUOMName(curItem.getUomName());
		    						relatedSizeItem.setIsLig(curItem.getIsLig());
		    					}
		    				}
		    				prevSize = curSize;
		    			}
					}
				}else{
					logger.warn("No Size Family Lead for Size Family I d - " + sizeFamilyId);
				}
			}
		
		// Assign correct size relation based on related item size
		//Nagaraj -- not sure this block has any effect, this blocks set the htol flag
		//which is never used any where
		if(itemsInPriceGroup != null)
			for(Map.Entry<ItemKey, PRPriceGroupDTO> entry : itemsInPriceGroup.entrySet()){
				PRPriceGroupDTO pgDTO = entry.getValue();
				if(pgDTO.getSizeFamilyId() > 0){
					ArrayList<PRPriceGroupRelatedItemDTO> sizeRelationList = pgDTO.getRelationList().get(PRConstants.SIZE_RELATION);
					PRPriceGroupRelatedItemDTO newDTO = new PRPriceGroupRelatedItemDTO();
					PRPriceGroupRelatedItemDTO origDTO = sizeRelationList.get(0);
					newDTO.setRelatedItemCode(origDTO.getRelatedItemCode());
					newDTO.setRelatedItemSize(origDTO.getRelatedItemSize());
					newDTO.setRelatedUOMName(origDTO.getRelatedUOMName());
					newDTO.setIsLig(origDTO.getIsLig());
					boolean isCorrectRelationSet = false;
					char htol = Constants.YES;
					if(pgDTO.getItemSize() < newDTO.getRelatedItemSize())
						htol = Constants.NO;
					
					// Lower size is dependent on higher size
					for(PRPriceGroupRelatedItemDTO relations : sizeRelationList){
						if(relations.getHtol() == htol){
							newDTO.setHtol(htol);
							newDTO.setPriceRelation(relations.getPriceRelation());
							isCorrectRelationSet = true;
						}
					}
						
					if(!isCorrectRelationSet){
						newDTO.setPriceRelation(origDTO.getPriceRelation());
						newDTO.setHtol(origDTO.getHtol());
					}
					
					ArrayList<PRPriceGroupRelatedItemDTO> newList = new ArrayList<PRPriceGroupRelatedItemDTO>();
					newList.add(newDTO);
					pgDTO.getRelationList().put(PRConstants.SIZE_RELATION, newList);
				}
			}
	}

	private PRPriceGroupRelnDTO getPriceGroupRelation(int priceGroupRelnId) throws GeneralException{
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    PRPriceGroupRelnDTO pgRelnDTO = null;
	    try{
	    	statement = conn.prepareStatement(GET_PRICE_GROUP_RELATION);
	    	statement.setInt(1, priceGroupRelnId);
	    	resultSet = statement.executeQuery();
	    	if(resultSet.next()){
	    		pgRelnDTO = new PRPriceGroupRelnDTO();
	    		if(resultSet.getObject("VALUE_TYPE") != null)
	    			pgRelnDTO.setValueType(resultSet.getString("VALUE_TYPE").charAt(0));
	    		if(resultSet.getObject("MIN_VALUE") != null)
	    			pgRelnDTO.setMinValue(resultSet.getDouble("MIN_VALUE"));
	    		else
	    			pgRelnDTO.setMinValue(Constants.DEFAULT_NA);
	    		if(resultSet.getObject("MAX_VALUE") != null)
	    			pgRelnDTO.setMaxValue(resultSet.getDouble("MAX_VALUE"));
	    		else
	    			pgRelnDTO.setMaxValue(Constants.DEFAULT_NA);
	    		if(resultSet.getObject("OPERATOR_TEXT") != null)
	    			pgRelnDTO.setOperatorText(resultSet.getString("OPERATOR_TEXT"));
	    		if(resultSet.getObject("RETAIL_TYPE") != null)
	    			pgRelnDTO.setRetailType(resultSet.getString("RETAIL_TYPE").charAt(0));
	    		
	    		//If there is no min & max value, and there is operator text, then value type supposed to be null
	    		//This is not happening while price group is imported from the ui, so it is handled here
	    		if(resultSet.getObject("OPERATOR_TEXT") != null && resultSet.getObject("MIN_VALUE") == null
	    				&& resultSet.getObject("MAX_VALUE") == null && resultSet.getObject("VALUE_TYPE") != null){
	    			//reset value type
	    			pgRelnDTO.setValueType('\0');    			
	    		}
	    	}
	    }catch (SQLException e){
			logger.error("Error while executing GET_PRICE_GROUP_RELATION " + e);
			throw new GeneralException("Error while executing GET_PRICE_GROUP_RELATION", e);
		}finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return pgRelnDTO;
	}

	@SuppressWarnings("unused")
	private String getQueryCondition(int productLevelId, int productId){
		StringBuffer subQuery = new StringBuffer("");
		if(productLevelId > 1){
			subQuery = new StringBuffer("start with product_level_id = ? ");
			if(productId > 0){
				subQuery.append(" and product_id = ? ");
			}
			subQuery.append("connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id");	
		}else if(productLevelId == 1){
			subQuery = new StringBuffer("where child_product_level_id = 1 and child_product_id = ?");
		}
		
		return subQuery.toString();
	}
}
