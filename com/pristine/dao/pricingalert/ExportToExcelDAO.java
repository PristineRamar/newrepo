package com.pristine.dao.pricingalert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dto.pricingalert.AlertTypesDto;
import com.pristine.dto.pricingalert.LocationCompetitorMapDTO;
import com.pristine.dto.pricingalert.PAItemInfoDTO;
import com.pristine.dto.pricingalert.ReportTemplateDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ExportToExcelDAO implements IDAO{
	static Logger logger = Logger.getLogger("ExportToExcelDAO");
	
	private ArrayList<Integer> discontinuedItemList = null;
	
	public static final String GET_ITEM_INFO =  "SELECT RG.RET_LIR_NAME, RG.RET_LIR_CODE, RG.RET_LIR_ID, B.BRAND_NAME, RESULT.*  FROM ( " +
												"SELECT BASE_CUR_REG_PRICE, TO_CHAR(BASE_CUR_REG_PRICE_EFF_DATE, 'MM/dd/yyyy') AS BASE_CUR_REG_PRICE_EFF_DATE, BASE_PRE_REG_PRICE, BASE_FUT_REG_PRICE, TO_CHAR(BASE_FUT_REG_PRICE_EFF_DATE, 'MM/dd/yyyy') AS BASE_FUT_REG_PRICE_EFF_DATE, " +
												"BASE_CUR_LIST_COST, TO_CHAR(BASE_CUR_LIST_COST_EFF_DATE, 'MM/dd/yyyy') AS BASE_CUR_LIST_COST_EFF_DATE, BASE_PRE_LIST_COST, BASE_FUT_LIST_COST, TO_CHAR(BASE_FUT_LIST_COST_EFF_DATE, 'MM/dd/yyyy') AS BASE_FUT_LIST_COST_EFF_DATE, " +
												"COMP_CUR_REG_PRICE, TO_CHAR(COMP_CUR_REG_PRICE_EFF_DATE, 'MM/dd/yyyy') AS COMP_CUR_REG_PRICE_EFF_DATE, COMP_PRE_REG_PRICE, TO_CHAR(COMP_CUR_REG_PRICE_OBS_DATE, 'MM/dd/yyyy') AS COMP_CUR_REG_PRICE_OBS_DATE, " +
												"IS_KVI_ITEM, AVG_REVENUE, ITEM_DATA.*, ALERT_DATA.ITEM_CODE, LOCATION_NUM, COMPETITOR_NAME, COMPETITOR_NO, COMP_LOCATION_TYPE, COMP2_CUR_REG_PRICE, COMP2_PRE_REG_PRICE  FROM " + 
												"( " + 
												"(SELECT (CASE WHEN LCM.BASE_LOCATION_LEVEL_ID = 5 THEN (SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE COMP_STR_ID = BASE_LOCATION_ID) " +
												"ELSE (SELECT ZONE_NUM FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID = BASE_LOCATION_ID) END) LOCATION_NUM, CS.NAME AS COMPETITOR_NAME, CS.COMP_STR_NO AS COMPETITOR_NO, T.NAME AS COMP_LOCATION_TYPE, " +
												"I.*  FROM PA_MASTER_DATA M, PA_ITEM_INFO I, LOCATION_COMPETITOR_MAP LCM, COMPETITOR_STORE CS, COMP_LOCATION_TYPES T " +
												"WHERE M.CALENDAR_ID = ?   " + 
												"%LOCATION_COMPETITOR_MAP_CONDITION% " +
												"AND M.PA_ALERT_TYPES_ID = ? " +
												"AND M.PA_MASTER_DATA_ID = I.PA_MASTER_DATA_ID " +
												"AND M.LOCATION_COMPETITOR_MAP_ID = LCM.LOCATION_COMPETITOR_MAP_ID " +
												"AND LCM.COMP_LOCATION_ID = CS.COMP_STR_ID " + 
												"AND LCM.COMP_LOCATION_TYPES_ID = T.COMP_LOCATION_TYPES_ID " +
												"AND (M.LOCATION_COMPETITOR_MAP_ID, I.ITEM_CODE) NOT IN " +
												"((SELECT M.LOCATION_COMPETITOR_MAP_ID, I.ITEM_CODE FROM PA_MASTER_DATA M, PA_ITEM_INFO I, LOCATION_COMPETITOR_MAP LCM " + 
												"WHERE M.CALENDAR_ID IN " + 
												"(SELECT CALENDAR_ID FROM RETAIL_CALENDAR WHERE START_DATE >= (SELECT START_DATE - %DELTAWEEKS%*7 FROM RETAIL_CALENDAR WHERE CALENDAR_ID = ? AND ROW_TYPE = 'W') " +
												"AND START_DATE < (SELECT START_DATE FROM RETAIL_CALENDAR WHERE CALENDAR_ID = ? AND ROW_TYPE = 'W') AND ROW_TYPE = 'W')  " +
												"%LOCATION_COMPETITOR_MAP_CONDITION% " +
												"AND M.PA_ALERT_TYPES_ID = ? " +
												"AND M.PA_MASTER_DATA_ID = I.PA_MASTER_DATA_ID " + 
												"AND M.LOCATION_COMPETITOR_MAP_ID = LCM.LOCATION_COMPETITOR_MAP_ID)) " +
												")ALERT_DATA " + 
												"INNER JOIN " + 
												"( " + 
												"SELECT PRODUCT_HIERARCHY.*, IL.RETAILER_ITEM_CODE, IL.UPC, IL.ITEM_NAME, IL.RET_LIR_ID, (IL.ITEM_SIZE || ' ' || UL.NAME) AS ITEM_SIZE, IL.BRAND_ID FROM " + 
												"(%PRODUCT_HIERARCHY%) PRODUCT_HIERARCHY " + 
												"LEFT JOIN ITEM_LOOKUP IL " +
												"ON PRODUCT_HIERARCHY.ITEM_CODE_1 = IL.ITEM_CODE " +
												"LEFT JOIN UOM_LOOKUP UL " +
												"ON IL.UOM_ID = UL.ID " +
												//"    WHERE (  " +  
												"	%QUERY_CONDITION% " +
												//"    ) " + 
												")ITEM_DATA ON ALERT_DATA.ITEM_CODE = ITEM_DATA.ITEM_CODE_1 " + 
												")) RESULT LEFT JOIN RETAILER_LIKE_ITEM_GROUP RG ON RESULT.RET_LIR_ID = RG.RET_LIR_ID " +
												"LEFT JOIN BRAND_LOOKUP B ON RESULT.BRAND_ID = B.BRAND_ID";
	
	public String GET_LOCATION_COMPETITION_DETAILS = "SELECT (CASE WHEN LCM.BASE_LOCATION_LEVEL_ID = 5 THEN CS.NAME ELSE RPZ.NAME END) BASE_LOCATION_NAME, BASE_LOCATION_LEVEL_ID, BASE_LOCATION_ID, " +
													"CSC.NAME AS COMP_LOCATION_NAME, COMP_LOCATION_ID " +
													"FROM LOCATION_COMPETITOR_MAP LCM " +
													"LEFT JOIN COMPETITOR_STORE CS ON LCM.BASE_LOCATION_ID = CS.COMP_STR_ID  " +
													"LEFT JOIN COMPETITOR_STORE CSC ON LCM.COMP_LOCATION_ID = CSC.COMP_STR_ID  " +
													"LEFT JOIN RETAIL_PRICE_ZONE RPZ ON LCM.BASE_LOCATION_ID = RPZ.PRICE_ZONE_ID  " +
													"WHERE PRICING_ALERT_ENABLED = 'Y'";
	
	public String GET_MARGIN = "SELECT ((SUM(REVENUE_REGULAR + REVENUE_SALE) - SUM((QUANTITY_REGULAR + QUANTITY_SALE) * (CASE WHEN DEAL_COST = 0 THEN LIST_COST WHEN DEAL_COST < LIST_COST THEN DEAL_COST ELSE LIST_COST END)))/ SUM(REVENUE_REGULAR + REVENUE_SALE) * 100) AS MARGIN " +
								" FROM MOVEMENT_WEEKLY MW   " +
								" LEFT JOIN COMPETITIVE_DATA CD   " +
								" ON MW.CHECK_DATA_ID = CD.CHECK_DATA_ID   " +
								" LEFT JOIN SCHEDULE S   " +
								" ON CD.SCHEDULE_ID     = S.SCHEDULE_ID   " +
								" WHERE %LOCATION_CONDITION%   " +
								" AND S.START_DATE     = TO_DATE(?,'MM/DD/YYYY')   " +
								"   AND CD.ITEM_CODE IN (  " +
								" SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR " +
								" %QUERY_CONDITION%)   " +
								" WHERE CHILD_PRODUCT_LEVEL_ID = 1 %PRICE_CHECK_LIST_CONDN% %BRAND_CONDN%) ";
	
	public String GET_BILLING_MARGIN = "SELECT ((SUM(UNITPRICE(REG_PRICE, REG_M_PRICE, REG_M_PACK) * (QTY_REGULAR_13WK + QTY_SALE_13WK)) - SUM(LIST_COST * (QTY_REGULAR_13WK + QTY_SALE_13WK))) / SUM(UNITPRICE(REG_PRICE, REG_M_PRICE, REG_M_PACK) * (QTY_REGULAR_13WK + QTY_SALE_13WK)) * 100) AS MARGIN " +
								" FROM MOVEMENT_WEEKLY MW   " +
								" JOIN COMPETITIVE_DATA CD   " +
								" ON MW.CHECK_DATA_ID = CD.CHECK_DATA_ID   " +
								" LEFT JOIN SCHEDULE S   " +
								" ON CD.SCHEDULE_ID     = S.SCHEDULE_ID   " +
								" WHERE %LOCATION_CONDITION%   " +
								" AND S.START_DATE     = TO_DATE(?,'MM/DD/YYYY')   " +
								"   AND CD.ITEM_CODE IN (  " +
								" SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR " +
								" %QUERY_CONDITION%)   " +
								" WHERE CHILD_PRODUCT_LEVEL_ID = 1 %PRICE_CHECK_LIST_CONDN% %BRAND_CONDN%) ";
	
	public String GET_PRICEINDEX = "SELECT PI.COMP_LOCATION_ID, D.W_MOVEMENT_IX_REG FROM PI_SELECTION_CRITERIA PI, PRICE_INDEX_DATA D " +
									" WHERE PI.START_DATE = TO_DATE(?,'MM/DD/YYYY') " +
									" AND PI.BASE_LOCATION_LEVEL_ID = ? AND PI.BASE_LOCATION_ID = ? AND PI.PRICE_CHECK_LIST_ID IS NULL " +
									" AND PI.OP_MODE = 'Batch' " +
									" AND PI.ANALYSIS_ID = D.ANALYSIS_ID " +
									" AND D.PRODUCT_LEVEL_ID = ? AND D.PRODUCT_ID = ? ";
	
	public String GET_PRICEINDEX_FOR_PRICE_CHECK_LIST = "SELECT PI.COMP_LOCATION_ID, D.W_MOVEMENT_IX_REG FROM PI_SELECTION_CRITERIA PI, PRICE_INDEX_DATA D " +
									" WHERE PI.START_DATE = TO_DATE(?,'MM/DD/YYYY') " +
									" AND PI.BASE_LOCATION_LEVEL_ID = ? AND PI.BASE_LOCATION_ID = ? AND PI.PRICE_CHECK_LIST_ID = ? " +
									" AND PI.OP_MODE = 'Batch' " +
									" AND PI.ANALYSIS_ID = D.ANALYSIS_ID " +
									" AND D.PRODUCT_LEVEL_ID = ? AND D.PRODUCT_ID = ? ";
	
	public String GET_BASE_PRICE_ABOVE_COMP_COUNT = "SELECT BASE_LOCATION_NAME, COUNT(*) AS COUNT FROM ( " +
													"SELECT DISTINCT (CASE WHEN LCM.BASE_LOCATION_LEVEL_ID = 5 THEN CS.NAME ELSE RPZ.NAME END) BASE_LOCATION_NAME, I.ITEM_CODE FROM PA_MASTER_DATA M " +
													"JOIN LOCATION_COMPETITOR_MAP LCM ON M.LOCATION_COMPETITOR_MAP_ID = LCM.LOCATION_COMPETITOR_MAP_ID " +
													"LEFT JOIN COMPETITOR_STORE CS ON LCM.BASE_LOCATION_ID = CS.COMP_STR_ID   " +
													"LEFT JOIN RETAIL_PRICE_ZONE RPZ ON LCM.BASE_LOCATION_ID = RPZ.PRICE_ZONE_ID " +
													"JOIN PA_ITEM_INFO I ON M.PA_MASTER_DATA_ID = I.PA_MASTER_DATA_ID " +
													"WHERE M.CALENDAR_ID = ? " +
													"AND M.PA_ALERT_TYPES_ID = 1 " +
													"AND I.ITEM_CODE IN  " +
													"(SELECT CHILD_PRODUCT_ID " +
													"      FROM " +
													"      (SELECT CHILD_PRODUCT_ID, " +
													"        CHILD_PRODUCT_LEVEL_ID " +
													"        FROM product_group_relation pgr " +
													"        START WITH product_level_id       = ? " +
													"        AND product_id                    = ? " +
													"        CONNECT BY prior child_product_id = product_id " +
													"        AND prior child_product_level_id    = product_level_id " +
													"      ) " +
													"    WHERE child_product_level_id = 1 " +
													") " +
													"AND I.BASE_CUR_REG_PRICE IS NOT NULL " +
													"AND I.COMP_CUR_REG_PRICE IS NOT NULL " +
													"AND I.COMP2_CUR_REG_PRICE IS NOT NULL " +
													"AND I.BASE_CUR_REG_PRICE > I.COMP_CUR_REG_PRICE " +
													"AND I.BASE_CUR_REG_PRICE > I.COMP2_CUR_REG_PRICE) " +
													"GROUP BY BASE_LOCATION_NAME";

	public String GET_BASE_PRICE_BELOW_COMP_COUNT = "SELECT BASE_LOCATION_NAME, COUNT(*) AS COUNT FROM ( " +
													"SELECT DISTINCT (CASE WHEN LCM.BASE_LOCATION_LEVEL_ID = 5 THEN CS.NAME ELSE RPZ.NAME END) BASE_LOCATION_NAME, I.ITEM_CODE FROM PA_MASTER_DATA M " +
													"JOIN LOCATION_COMPETITOR_MAP LCM ON M.LOCATION_COMPETITOR_MAP_ID = LCM.LOCATION_COMPETITOR_MAP_ID " +
													"LEFT JOIN COMPETITOR_STORE CS ON LCM.BASE_LOCATION_ID = CS.COMP_STR_ID   " +
													"LEFT JOIN RETAIL_PRICE_ZONE RPZ ON LCM.BASE_LOCATION_ID = RPZ.PRICE_ZONE_ID " +
													"JOIN PA_ITEM_INFO I ON M.PA_MASTER_DATA_ID = I.PA_MASTER_DATA_ID " +
													"WHERE M.CALENDAR_ID = ? " +
													"AND M.PA_ALERT_TYPES_ID = 1 " +
													"AND I.ITEM_CODE IN  " +
													"(SELECT CHILD_PRODUCT_ID " +
													"      FROM " +
													"      (SELECT CHILD_PRODUCT_ID, " +
													"        CHILD_PRODUCT_LEVEL_ID " +
													"        FROM product_group_relation pgr " +
													"        START WITH product_level_id       = ? " +
													"        AND product_id                    = ? " +
													"        CONNECT BY prior child_product_id = product_id " +
													"        AND prior child_product_level_id    = product_level_id " +
													"      ) " +
													"    WHERE child_product_level_id = 1 " +
													") " +
													"AND I.BASE_CUR_REG_PRICE IS NOT NULL " +
													"AND I.COMP_CUR_REG_PRICE IS NOT NULL " +
													"AND I.COMP2_CUR_REG_PRICE IS NOT NULL " +
													"AND I.BASE_CUR_REG_PRICE < I.COMP_CUR_REG_PRICE " +
													"AND I.BASE_CUR_REG_PRICE < I.COMP2_CUR_REG_PRICE) " +
													"GROUP BY BASE_LOCATION_NAME";

	public void setDiscontinuedItemList(Connection connection){
		try{
		if(PropertyManager.getProperty("PA_EXPORTTOEXCEL.DISCONTINUED_ITEM_LIST") != null){
			PricingAlertDAO pricingAlertDAO = new PricingAlertDAO();
			int discontinuedItemListId = pricingAlertDAO.getPriceCheckListId(connection, PropertyManager.getProperty("PA_EXPORTTOEXCEL.DISCONTINUED_ITEM_LIST"));
			discontinuedItemList = pricingAlertDAO.getKVIItems(connection, discontinuedItemListId);
		}
		}catch(GeneralException exception){
			exception.printStackTrace();
		}
	}
	
	public ArrayList<PAItemInfoDTO> getPAItemInfo(Connection conn, PricingAlertDAO pricingAlertDAO, AlertTypesDto alert, LocationCompetitorMapDTO locCompMap, ReportTemplateDTO templateDTO, boolean allProducts){
		PreparedStatement statement = null;
		ResultSet rs = null;
		ArrayList<PAItemInfoDTO> paItemInfoDTOList = new ArrayList<PAItemInfoDTO>();
		try{
			
			if(discontinuedItemList == null)
				setDiscontinuedItemList(conn);
			
			String query = new String(GET_ITEM_INFO);
			query = query.replaceAll("%PRODUCT_HIERARCHY%", pricingAlertDAO.getProductHierarchy(conn));
			query = query.replaceAll("%DELTAWEEKS%", PropertyManager.getProperty("PA_EXPORTTOEXCEL.DELTAWEEKS", "0"));
			if(locCompMap.getLocationCompetitorMapId() > 0){
				query = query.replaceAll("%LOCATION_COMPETITOR_MAP_CONDITION%", "AND M.LOCATION_COMPETITOR_MAP_ID = ? ");
			}else{
				query = query.replaceAll("%LOCATION_COMPETITOR_MAP_CONDITION%", "");
			}
			
			if(!allProducts){
				query = query.replaceAll("%QUERY_CONDITION%", pricingAlertDAO.getQueryCondition(conn, locCompMap));
			}else{
				query = query.replaceAll("%QUERY_CONDITION%", "");
			}
			
			if(locCompMap.getPriceCheckListId() > 0){
				
			}else{
				query = query.replaceAll("%PRICE_CHECK_LIST_CONDN%", "");
			}
			
			HashMap<Integer, Integer> productLevelPointer = pricingAlertDAO.getProductLevelPointerMap();
			
			logger.debug("Query -" + query);
			statement = conn.prepareStatement(query);
			int counter = 0;
			statement.setInt(++counter, locCompMap.getCalendarId());
			if(locCompMap.getLocationCompetitorMapId() > 0)
				statement.setInt(++counter, locCompMap.getLocationCompetitorMapId());
			statement.setInt(++counter, alert.getAlertTypeId());
			statement.setInt(++counter, locCompMap.getCalendarId());
			statement.setInt(++counter, locCompMap.getCalendarId());
			if(locCompMap.getLocationCompetitorMapId() > 0)
				statement.setInt(++counter, locCompMap.getLocationCompetitorMapId());
			statement.setInt(++counter, alert.getAlertTypeId());
			if(!allProducts){
				statement.setInt(++counter, locCompMap.getProductLevelId());
				statement.setInt(++counter, locCompMap.getProductId());
			}
			
			rs = statement.executeQuery();
			while(rs.next()){
				
				PAItemInfoDTO rsDTO = new PAItemInfoDTO();
				if(discontinuedItemList != null && discontinuedItemList.size() > 0){
					if(discontinuedItemList.contains(rs.getInt("ITEM_CODE"))){
						continue;
					}
				}
				rsDTO.setLocNum(rs.getString("LOCATION_NUM"));
				rsDTO.setCompName(rs.getString("COMPETITOR_NAME"));
				rsDTO.setCompNo(rs.getString("COMPETITOR_NO"));
				rsDTO.setItemCode(rs.getInt("ITEM_CODE"));
				rsDTO.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
				rsDTO.setUpc(rs.getString("UPC"));
				rsDTO.setItemName(rs.getString("ITEM_NAME"));
				rsDTO.setItemSize(rs.getString("ITEM_SIZE"));
				rsDTO.setBrandId(rs.getInt("BRAND_ID"));
				rsDTO.setBrandName(rs.getString("BRAND_NAME"));
				rsDTO.setLirItemName(rs.getString("RET_LIR_NAME"));
				rsDTO.setLirCode(rs.getString("RET_LIR_CODE"));
				rsDTO.setRetLirId(rs.getInt("RET_LIR_ID"));
				rsDTO.setMajorCategory(rs.getString("NAME_" + productLevelPointer.get(Constants.DEPARTMENTLEVELID)));
				//rsDTO.setPortfolio(rs.getString("NAME_" + productLevelPointer.get(Constants.PORTFOLIO)));
				rsDTO.setCategory(rs.getString("NAME_" + productLevelPointer.get(Constants.CATEGORYLEVELID)));
				if(String.valueOf(Constants.YES).equals(rs.getString("IS_KVI_ITEM"))){
					rsDTO.setKVIItem(true);
				}else{
					rsDTO.setKVIItem(false);
				}
				rsDTO.setAvgRevenue(rs.getDouble("AVG_REVENUE"));
		
				Object baseCurRegPrice = rs.getObject("BASE_CUR_REG_PRICE");
				if(baseCurRegPrice == null)
					rsDTO.setBaseCurRegPrice(Constants.DEFAULT_NA);
				else
					rsDTO.setBaseCurRegPrice(rs.getDouble("BASE_CUR_REG_PRICE"));
				String baseCurRegPriceEffDate = rs.getString("BASE_CUR_REG_PRICE_EFF_DATE");
				if(baseCurRegPriceEffDate == null){
					rsDTO.setBaseCurRegPriceEffDate(Constants.DEFAULT_NA_STRING);
				}else{
					rsDTO.setBaseCurRegPriceEffDate(baseCurRegPriceEffDate);
				}
				Object basePreRegPrice = rs.getObject("BASE_PRE_REG_PRICE");
				if(basePreRegPrice == null)
					rsDTO.setBasePreRegPrice(Constants.DEFAULT_NA);
				else
					rsDTO.setBasePreRegPrice(rs.getDouble("BASE_PRE_REG_PRICE"));
				Object baseFutRegPrice = rs.getObject("BASE_FUT_REG_PRICE");
				if(baseFutRegPrice == null)
					rsDTO.setBaseFutRegPrice(Constants.DEFAULT_NA);
				else
					rsDTO.setBaseFutRegPrice(rs.getDouble("BASE_FUT_REG_PRICE"));
				String baseFutRegPriceEffDate = rs.getString("BASE_FUT_REG_PRICE_EFF_DATE");
				if(baseFutRegPriceEffDate == null){
					rsDTO.setBaseFutRegPriceEffDate(Constants.DEFAULT_NA_STRING);
				}else{
					rsDTO.setBaseFutRegPriceEffDate(baseFutRegPriceEffDate);
				}
				
				Object baseCurListCost = rs.getObject("BASE_CUR_LIST_COST");
				if(baseCurListCost == null)
					rsDTO.setBaseCurListCost(Constants.DEFAULT_NA);
				else
					rsDTO.setBaseCurListCost(rs.getDouble("BASE_CUR_LIST_COST"));
				String baseCurListCostEffDate = rs.getString("BASE_CUR_LIST_COST_EFF_DATE");
				if(baseCurListCostEffDate == null){
					rsDTO.setBaseCurListCostEffDate(Constants.DEFAULT_NA_STRING);
				}else{
					rsDTO.setBaseCurListCostEffDate(baseCurListCostEffDate);
				}
				Object basePreListCost = rs.getObject("BASE_PRE_LIST_COST");
				if(basePreListCost == null)
					rsDTO.setBasePreListCost(Constants.DEFAULT_NA);
				else
					rsDTO.setBasePreListCost(rs.getDouble("BASE_PRE_LIST_COST"));
				Object baseFutListCost = rs.getObject("BASE_FUT_LIST_COST");
				if(baseFutListCost == null)
					rsDTO.setBaseFutListCost(Constants.DEFAULT_NA);
				else
					rsDTO.setBaseFutListCost(rs.getDouble("BASE_FUT_LIST_COST"));
				String baseFutListCostEffDate = rs.getString("BASE_FUT_LIST_COST_EFF_DATE");
				if(baseFutListCostEffDate == null){
					rsDTO.setBaseFutListCostEffDate(Constants.DEFAULT_NA_STRING);
				}else{
					rsDTO.setBaseFutListCostEffDate(baseFutListCostEffDate);
				}
				
				Object compCurRegPrice = rs.getObject("COMP_CUR_REG_PRICE");
				if(compCurRegPrice == null)
					rsDTO.setCompCurRegPrice(Constants.DEFAULT_NA);
				else
					rsDTO.setCompCurRegPrice(rs.getDouble("COMP_CUR_REG_PRICE"));
				String compCurRegPriceEffDate = rs.getString("COMP_CUR_REG_PRICE_EFF_DATE");
				if(compCurRegPriceEffDate == null){
					rsDTO.setCompCurRegPriceEffDate(Constants.DEFAULT_NA_STRING);
				}else{
					rsDTO.setCompCurRegPriceEffDate(compCurRegPriceEffDate);
				}
				Object compPreRegPrice = rs.getObject("COMP_PRE_REG_PRICE");
				if(compPreRegPrice == null)
					rsDTO.setCompPreRegPrice(Constants.DEFAULT_NA);
				else
					rsDTO.setCompPreRegPrice(rs.getDouble("COMP_PRE_REG_PRICE"));
				String compCurRegPriceObsDate = rs.getString("COMP_CUR_REG_PRICE_OBS_DATE");
				if(compCurRegPriceObsDate == null){
					rsDTO.setCompCurRegPriceLastObsDate(Constants.DEFAULT_NA_STRING);
				}else{
					rsDTO.setCompCurRegPriceLastObsDate(compCurRegPriceObsDate);
				}
				
				Object comp2CurRegPrice = rs.getObject("COMP2_CUR_REG_PRICE");
				if(comp2CurRegPrice == null)
					rsDTO.setComp2CurRegPrice(Constants.DEFAULT_NA);
				else
					rsDTO.setComp2CurRegPrice(rs.getDouble("COMP2_CUR_REG_PRICE"));
				Object comp2PreRegPrice = rs.getObject("COMP2_PRE_REG_PRICE");
				if(comp2PreRegPrice == null)
					rsDTO.setComp2PreRegPrice(Constants.DEFAULT_NA);
				else
					rsDTO.setComp2PreRegPrice(rs.getDouble("COMP2_PRE_REG_PRICE"));
				rsDTO.setCompLocationType(rs.getString("COMP_LOCATION_TYPE").charAt(0));
				paItemInfoDTOList.add(rsDTO);
			}
		}catch(SQLException exception){
			exception.printStackTrace();
		}catch(GeneralException exception){
			exception.printStackTrace();
		}
		finally{
			PristineDBUtil.close(statement);
		}
		return paItemInfoDTOList;
	}
	
	public ArrayList<LocationCompetitorMapDTO> getLocationCompetitorList(Connection conn){
		PreparedStatement statement = null;
		ResultSet rs = null;
		ArrayList<LocationCompetitorMapDTO> locCompList = new ArrayList<LocationCompetitorMapDTO>();
		try{
			statement = conn.prepareStatement(GET_LOCATION_COMPETITION_DETAILS);
			rs = statement.executeQuery();
			while(rs.next()){
				LocationCompetitorMapDTO locCompMapDto = new LocationCompetitorMapDTO();
				locCompMapDto.setBaseLocationLevelId(rs.getInt("BASE_LOCATION_LEVEL_ID"));
				locCompMapDto.setBaseLocationId(rs.getInt("BASE_LOCATION_ID"));
				locCompMapDto.setLocationName(rs.getString("BASE_LOCATION_NAME"));
				locCompMapDto.setCompLocationId(rs.getInt("COMP_LOCATION_ID"));
				locCompMapDto.setCompetitorName(rs.getString("COMP_LOCATION_NAME"));
				locCompList.add(locCompMapDto);
			}
		}catch(SQLException exception){
			exception.printStackTrace();
		}
		finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return locCompList;
	}
	
	public double getMargin(Connection conn, LocationCompetitorMapDTO locCompMap, String weekStartDate, int brandId){
		PreparedStatement statement = null;
		ResultSet rs = null;
		double margin = -1;
		String sql = GET_MARGIN;
		PricingAlertDAO paDAO = new PricingAlertDAO();
		sql = sql.replaceAll("%LOCATION_CONDITION%", paDAO.getLocationCondition(locCompMap, false));
		sql = sql.replaceAll("%QUERY_CONDITION%", paDAO.getQueryConditionV2(locCompMap));
		if(locCompMap.getPriceCheckListId() > 0)
			sql = sql.replaceAll("%PRICE_CHECK_LIST_CONDN%", " AND CHILD_PRODUCT_ID IN (SELECT ITEM_CODE FROM PRICE_CHECK_LIST_ITEMS WHERE PRICE_CHECK_LIST_ID = ?)");
		else
			sql = sql.replaceAll("%PRICE_CHECK_LIST_CONDN%", "");
		
		if(brandId > 0)
			sql = sql.replaceAll("%BRAND_CONDN%", " AND CHILD_PRODUCT_ID IN (SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE BRAND_ID = ?)");
		else
			sql = sql.replaceAll("%BRAND_CONDN%", "");
		
		try{
			statement = conn.prepareStatement(sql);
			int counter = 0;
			statement.setInt(++counter, locCompMap.getBaseLocationId());
			statement.setString(++counter, weekStartDate);
			if(locCompMap.getProductLevelId() > 1)
				statement.setInt(++counter, locCompMap.getProductLevelId());
			if(locCompMap.getProductId() > 0)
				statement.setInt(++counter, locCompMap.getProductId());
			if(locCompMap.getPriceCheckListId() > 0)
				statement.setInt(++counter, locCompMap.getPriceCheckListId());
			if(brandId > 0)
				statement.setInt(++counter, brandId);
			rs = statement.executeQuery();
			if(rs.next()){
				margin = rs.getDouble("MARGIN");
			}
		}catch(SQLException exception){
			exception.printStackTrace();
		}
		finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return margin;
	}
	
	public double getBillingMargin(Connection conn, LocationCompetitorMapDTO locCompMap, String weekStartDate, int brandId){
		PreparedStatement statement = null;
		ResultSet rs = null;
		double margin = -1;
		String sql = GET_BILLING_MARGIN;
		PricingAlertDAO paDAO = new PricingAlertDAO();
		sql = sql.replaceAll("%LOCATION_CONDITION%", paDAO.getLocationCondition(locCompMap, false));
		sql = sql.replaceAll("%QUERY_CONDITION%", paDAO.getQueryConditionV2(locCompMap));
		if(locCompMap.getPriceCheckListId() > 0)
			sql = sql.replaceAll("%PRICE_CHECK_LIST_CONDN%", " AND CHILD_PRODUCT_ID IN (SELECT ITEM_CODE FROM PRICE_CHECK_LIST_ITEMS WHERE PRICE_CHECK_LIST_ID = ?)");
		else
			sql = sql.replaceAll("%PRICE_CHECK_LIST_CONDN%", "");
		
		if(brandId > 0)
			sql = sql.replaceAll("%BRAND_CONDN%", " AND CHILD_PRODUCT_ID IN (SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE BRAND_ID = ?)");
		else
			sql = sql.replaceAll("%BRAND_CONDN%", "");
		
		try{
			statement = conn.prepareStatement(sql);
			int counter = 0;
			statement.setInt(++counter, locCompMap.getBaseLocationId());
			statement.setString(++counter, weekStartDate);
			if(locCompMap.getProductLevelId() > 1)
				statement.setInt(++counter, locCompMap.getProductLevelId());
			if(locCompMap.getProductId() > 0)
				statement.setInt(++counter, locCompMap.getProductId());
			if(locCompMap.getPriceCheckListId() > 0)
				statement.setInt(++counter, locCompMap.getPriceCheckListId());
			if(brandId > 0)
				statement.setInt(++counter, brandId);
			rs = statement.executeQuery();
			if(rs.next()){
				margin = rs.getDouble("MARGIN");
			}
		}catch(SQLException exception){
			exception.printStackTrace();
		}
		finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return margin;
	}
	
	public HashMap<Integer, Double> getPriceIndex(Connection conn, LocationCompetitorMapDTO locCompMap, String weekStartDate){
		PreparedStatement statement = null;
		ResultSet rs = null;
		HashMap<Integer, Double> piMap = new HashMap<Integer, Double>();
		try{
			statement = conn.prepareStatement(GET_PRICEINDEX);
			int counter = 0;
			statement.setString(++counter, weekStartDate);
			statement.setInt(++counter, locCompMap.getBaseLocationLevelId());
			statement.setInt(++counter, locCompMap.getBaseLocationId());
			statement.setInt(++counter, locCompMap.getProductLevelId());
			statement.setInt(++counter, locCompMap.getProductId());
			rs = statement.executeQuery();
			while(rs.next()){
				piMap.put(rs.getInt("COMP_LOCATION_ID"), rs.getDouble("W_MOVEMENT_IX_REG"));
			}
		}catch(SQLException exception){
			exception.printStackTrace();
		}
		finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return piMap;
	}
	
	public HashMap<Integer, HashMap<Integer, Double>> getPriceIndexForPriceCheckList(Connection conn, LocationCompetitorMapDTO locCompMap, String weekStartDate){
		PreparedStatement statement = null;
		ResultSet rs = null;
		HashMap<Integer, HashMap<Integer, Double>> piPriceCheckListMap = new HashMap<Integer, HashMap<Integer, Double>>();
		try{
			statement = conn.prepareStatement(GET_PRICEINDEX_FOR_PRICE_CHECK_LIST);
			int counter = 0;
			statement.setString(++counter, weekStartDate);
			statement.setInt(++counter, locCompMap.getBaseLocationLevelId());
			statement.setInt(++counter, locCompMap.getBaseLocationId());
			statement.setInt(++counter, locCompMap.getPriceCheckListId());
			statement.setInt(++counter, locCompMap.getProductLevelId());
			statement.setInt(++counter, locCompMap.getProductId());
			rs = statement.executeQuery();
			while(rs.next()){
				logger.info(locCompMap.getBaseLocationId() + "\t" + rs.getInt("COMP_LOCATION_ID") + "\t" + rs.getDouble("W_MOVEMENT_IX_REG"));
				HashMap<Integer, Double> piMap = new HashMap<Integer, Double>();
				piMap.put(locCompMap.getPriceCheckListId(), rs.getDouble("W_MOVEMENT_IX_REG"));
				if(piPriceCheckListMap.get(rs.getInt("COMP_LOCATION_ID")) != null){
					HashMap<Integer, Double> tMap = piPriceCheckListMap.get(rs.getInt("COMP_LOCATION_ID"));
					tMap.putAll(piMap);
					piPriceCheckListMap.put(rs.getInt("COMP_LOCATION_ID"), tMap);
				}else
					piPriceCheckListMap.put(rs.getInt("COMP_LOCATION_ID"), piMap);
			}
		}catch(SQLException exception){
			exception.printStackTrace();
		}
		finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		
		logger.info("Base Location - " + locCompMap.getBaseLocationId());
		for(Map.Entry<Integer, HashMap<Integer, Double>> entry : piPriceCheckListMap.entrySet()){
			for(Map.Entry<Integer, Double> inEntry : entry.getValue().entrySet()){
				logger.info(entry.getKey() + "\t" + inEntry.getKey() + "\t" + inEntry.getValue());
			}
		}
		return piPriceCheckListMap;
	}
	
	public HashMap<String, Integer> getBaseCompDiffCount(Connection conn, LocationCompetitorMapDTO locCompMap, boolean aboveComp){
		HashMap<String, Integer> resultMap = new HashMap<String, Integer>();
		PreparedStatement statement = null;
		ResultSet rs = null;
		try{
			if(aboveComp)
				statement = conn.prepareStatement(GET_BASE_PRICE_ABOVE_COMP_COUNT);
			else
				statement = conn.prepareStatement(GET_BASE_PRICE_BELOW_COMP_COUNT);
			int counter = 0;
			statement.setInt(++counter, locCompMap.getCalendarId());
			statement.setInt(++counter, locCompMap.getProductLevelId());
			statement.setInt(++counter, locCompMap.getProductId());
			rs = statement.executeQuery();
			while(rs.next()){
				resultMap.put(rs.getString("BASE_LOCATION_NAME"), rs.getInt("COUNT"));
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving comp Diff Count - " + exception.toString());
		}
		finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return resultMap;
	}
}
