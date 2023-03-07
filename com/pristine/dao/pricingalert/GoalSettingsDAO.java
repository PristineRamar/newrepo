package com.pristine.dao.pricingalert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dto.pricingalert.GoalSettingsDTO;
import com.pristine.dto.pricingalert.LocationCompetitorMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;


public class GoalSettingsDAO implements IDAO{
	static Logger logger = Logger.getLogger("GoalSettingsDAO");
	
	private static final String GET_GOAL_DETAILS = "SELECT ITEM_CODE, PRICE_CHECK_LIST_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, LOCATION_LEVEL_ID, LOCATION_ID, BRAND_ID, " +
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Current/Reg/Min/text()').GETSTRINGVAL() PI_CUR_REG_MIN, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Current/Reg/Max/text()').GETSTRINGVAL() PI_CUR_REG_MAX, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Current/Sale/Min/text()').GETSTRINGVAL() PI_CUR_SALE_MIN, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Current/Sale/Max/text()').GETSTRINGVAL() PI_CUR_SALE_MAX, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Future/Reg/Min/text()').GETSTRINGVAL() PI_FUT_REG_MIN, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Future/Reg/Max/text()').GETSTRINGVAL() PI_FUT_REG_MAX, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Future/Sale/Min/text()').GETSTRINGVAL() PI_FUT_SALE_MIN, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Future/Sale/Max/text()').GETSTRINGVAL() PI_FUT_SALE_MAX, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Current/Reg/Min/text()').GETSTRINGVAL() MARGIN_CUR_REG_MIN, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Current/Reg/Max/text()').GETSTRINGVAL() MARGIN_CUR_REG_MAX, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Current/Sale/Min/text()').GETSTRINGVAL() MARGIN_CUR_SALE_MIN, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Current/Sale/Max/text()').GETSTRINGVAL() MARGIN_CUR_SALE_MAX, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Future/Reg/Min/text()').GETSTRINGVAL() MARGIN_FUT_REG_MIN, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Future/Reg/Max/text()').GETSTRINGVAL() MARGIN_FUT_REG_MAX, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Future/Sale/Min/text()').GETSTRINGVAL() MARGIN_FUT_SALE_MIN, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Future/Sale/Max/text()').GETSTRINGVAL() MARGIN_FUT_SALE_MAX " +
												    "FROM GOAL_SETTINGS WHERE CHAIN_LOCATION_LEVEL_ID = ? " +
												    "AND CHAIN_LOCATION_ID = ? AND (ITEM_CODE IS NULL OR ITEM_CODE IN (%s))";
	
	private static final String GET_GOAL_DETAILS_FOR_PRODUCT = "SELECT IL.ITEM_NAME, B.BRAND_NAME, P.NAME AS PRICE_CHECK_LIST_NAME, CC.COMP_CHAIN_NAME, GS.LOCATION_LEVEL_ID, GS.LOCATION_ID, GS.PRICE_CHECK_LIST_ID, GS.BRAND_ID, GS.OVERALL_GOAL, " +
													"(CASE WHEN GS.LOCATION_LEVEL_ID = 5 THEN CS.NAME ELSE RPZ.NAME END) LOCATION_NAME, " +
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Current/Reg/Min/text()').GETSTRINGVAL() PI_CUR_REG_MIN, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Current/Reg/Max/text()').GETSTRINGVAL() PI_CUR_REG_MAX, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Current/Sale/Min/text()').GETSTRINGVAL() PI_CUR_SALE_MIN, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Current/Sale/Max/text()').GETSTRINGVAL() PI_CUR_SALE_MAX, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Future/Reg/Min/text()').GETSTRINGVAL() PI_FUT_REG_MIN, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Future/Reg/Max/text()').GETSTRINGVAL() PI_FUT_REG_MAX, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Future/Sale/Min/text()').GETSTRINGVAL() PI_FUT_SALE_MIN, " + 
													"XMLTYPE(PI_GOAL_VALUES).EXTRACT('//Future/Sale/Max/text()').GETSTRINGVAL() PI_FUT_SALE_MAX, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Current/Reg/Min/text()').GETSTRINGVAL() MARGIN_CUR_REG_MIN, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Current/Reg/Max/text()').GETSTRINGVAL() MARGIN_CUR_REG_MAX, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Current/Sale/Min/text()').GETSTRINGVAL() MARGIN_CUR_SALE_MIN, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Current/Sale/Max/text()').GETSTRINGVAL() MARGIN_CUR_SALE_MAX, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Future/Reg/Min/text()').GETSTRINGVAL() MARGIN_FUT_REG_MIN, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Future/Reg/Max/text()').GETSTRINGVAL() MARGIN_FUT_REG_MAX, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Future/Sale/Min/text()').GETSTRINGVAL() MARGIN_FUT_SALE_MIN, " + 
													"XMLTYPE(MARGIN_GOAL_VALUES).EXTRACT('//Future/Sale/Max/text()').GETSTRINGVAL() MARGIN_FUT_SALE_MAX " +
												    "FROM GOAL_SETTINGS GS " +
													"LEFT JOIN ITEM_LOOKUP IL ON GS.ITEM_CODE = IL.ITEM_CODE " +
												    "LEFT JOIN PRICE_CHECK_LIST P ON GS.PRICE_CHECK_LIST_ID = P.ID " +
													"LEFT JOIN BRAND_LOOKUP B ON GS.BRAND_ID = B.BRAND_ID " +
												    "LEFT JOIN COMPETITOR_CHAIN CC ON GS.CHAIN_LOCATION_ID = CC.COMP_CHAIN_ID " +
												    "LEFT JOIN COMPETITOR_STORE CS ON GS.LOCATION_ID = CS.COMP_STR_ID " +
												    "LEFT JOIN RETAIL_PRICE_ZONE RPZ ON GS.LOCATION_ID = RPZ.PRICE_ZONE_ID " +
												    "WHERE (CHAIN_LOCATION_LEVEL_ID = ? AND CHAIN_LOCATION_ID = ?) " +
												    "AND ((PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ?) OR " +
												    "(GS.ITEM_CODE IN (SELECT CHILD_PRODUCT_ID FROM (SELECT * FROM PRODUCT_GROUP_RELATION PGR " + 
												    "START WITH PRODUCT_LEVEL_ID = ? AND PRODUCT_ID = ? " +  
												    "CONNECT BY  PRIOR CHILD_PRODUCT_ID = PRODUCT_ID  AND  PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID) " +
												    "WHERE CHILD_PRODUCT_LEVEL_ID = 1)))";

	
	public GoalSettingsDTO getGoalDetails(Connection conn, LocationCompetitorMapDTO locCompDTO, ArrayList<Integer> itemCodes){
		GoalSettingsDTO goalSettings = new GoalSettingsDTO();
		logger.info(locCompDTO.getProductLevelId() + "\t" + locCompDTO.getProductId());
		int limitcount=0;

		List<Integer> itemCodeList = new ArrayList<Integer>();
		for(Integer itemCode:itemCodes){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%Constants.LIMIT_COUNT == 0)){
				Object[] values = itemCodeList.toArray();
				retrieveGoalDetails(conn, locCompDTO, goalSettings, values);
            	itemCodeList.clear();
            }
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			retrieveGoalDetails(conn, locCompDTO, goalSettings, values);
        	itemCodeList.clear();
		}
		
		return goalSettings;
	}
	
	/*public void retrieveGoalDetails(Connection conn, LocationCompetitorMapDTO locCompDTO, GoalSettingsDTO goalSettings, Object... values){
		String sql = new String(GET_GOAL_DETAILS);
		PreparedStatement statement = null;
		ResultSet rs = null;
		
		try{
			Integer chainId = Integer.parseInt(new RetailPriceDAO().getChainId(conn));
			statement = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
			int counter = 0;
			statement.setInt(++counter, Constants.CHAIN_LEVEL_ID);
			statement.setInt(++counter, chainId);
			PristineDBUtil.setValues(statement, 3, values);
			rs = statement.executeQuery();
			while(rs.next()){
				Integer itemCode = rs.getInt("ITEM_CODE");
				Integer priceCheckListId = rs.getInt("PRICE_CHECK_LIST_ID");
				Integer productLevelId = rs.getInt("PRODUCT_LEVEL_ID");
				Integer productId = rs.getInt("PRODUCT_ID");
				Integer locationLevelId = rs.getInt("LOCATION_LEVEL_ID");
				Integer locationId = rs.getInt("LOCATION_ID");
				if(itemCode != null && itemCode > 0){
					HashMap<Integer, GoalSettingsDTO> itemLevelGoal = goalSettings.getItemLevelGoal();
					if(itemLevelGoal == null){
						new HashMap<Integer, GoalSettingsDTO>();
					}
					itemLevelGoal.put(itemCode, populateGoalData(rs));
					goalSettings.setItemLevelGoal(itemLevelGoal);
				}else if(priceCheckListId != null && priceCheckListId > 0){
					if(locCompDTO.getPriceCheckListId() == priceCheckListId)
						goalSettings.setPriceCheckLevelGoal(populateGoalData(rs));
				}else if(locationLevelId != null && locationLevelId > 0 && productLevelId != null && productLevelId > 0){
					if(locationId != null && locationId > 0 && productId != null && productId > 0){
						if(productLevelId == locCompDTO.getProductLevelId() && productId == locCompDTO.getProductId()
								&& locationLevelId == locCompDTO.getBaseLocationLevelId() && locationId == locCompDTO.getBaseLocationId()){
							goalSettings.setProductLevelGoal(populateGoalData(rs));
						}
					}
				}else if(productLevelId != null && productLevelId > 0){
					if(productId != null && productId > 0){
						if(productLevelId == locCompDTO.getProductLevelId() && productId == locCompDTO.getProductId())
							goalSettings.setProductLevelGoal(populateGoalData(rs));
					}else{
						if(productLevelId == locCompDTO.getProductLevelId())
							goalSettings.setProductLevelGoal(populateGoalData(rs));
					}
				}
				
				if(locationLevelId != null && locationLevelId > 0){
					if(locationId != null && locationId > 0){
						if(locationLevelId.intValue() == locCompDTO.getBaseLocationLevelId() && locationId.intValue() == locCompDTO.getBaseLocationId())
							goalSettings.setLocationLevelGoal(populateGoalData(rs));
					}else{
						if(locationLevelId == locCompDTO.getBaseLocationLevelId())
							goalSettings.setLocationLevelGoal(populateGoalData(rs));
					}
				}else{
					goalSettings.setChainLevelGoal(populateGoalData(rs));
				}
			}
		}catch(SQLException e){
			logger.error("Error when retrieving goal settings - " + e.toString());
		}catch(GeneralException e){
			logger.error("Error when retrieving goal settings - " + e.toString());
		}finally{
			PristineDBUtil.close(statement);
		}
	}*/
	
	public void retrieveGoalDetails(Connection conn, LocationCompetitorMapDTO locCompDTO, GoalSettingsDTO goalSettings, Object... values){
		String sql = new String(GET_GOAL_DETAILS);
		PreparedStatement statement = null;
		ResultSet rs = null;
		
		try{
			Integer chainId = Integer.parseInt(new RetailPriceDAO().getChainId(conn));
			statement = conn.prepareStatement(String.format(sql, PristineDBUtil.preparePlaceHolders(values.length)));
			int counter = 0;
			statement.setInt(++counter, Constants.CHAIN_LEVEL_ID);
			statement.setInt(++counter, chainId);
			PristineDBUtil.setValues(statement, 3, values);
			rs = statement.executeQuery();
			while(rs.next()){
				Integer itemCode = rs.getInt("ITEM_CODE");
				Integer priceCheckListId = rs.getInt("PRICE_CHECK_LIST_ID");
				Integer productLevelId = rs.getInt("PRODUCT_LEVEL_ID");
				Integer productId = rs.getInt("PRODUCT_ID");
				Integer locationLevelId = rs.getInt("LOCATION_LEVEL_ID");
				Integer locationId = rs.getInt("LOCATION_ID");
				Integer brandId = rs.getInt("BRAND_ID");
				if(itemCode != null && itemCode > 0){
					HashMap<Integer, GoalSettingsDTO> itemLevelGoal = goalSettings.getItemLevelGoal();
					if(itemLevelGoal == null)
						itemLevelGoal = new HashMap<Integer, GoalSettingsDTO>();
					itemLevelGoal.put(itemCode, populateGoalData(rs));
					goalSettings.setItemLevelGoal(itemLevelGoal);
				}else if(priceCheckListId != null && priceCheckListId > 0){
					HashMap<Integer, GoalSettingsDTO> priceCheckListLevelGoal = goalSettings.getPriceCheckLevelGoal();
					if(priceCheckListLevelGoal == null)
						priceCheckListLevelGoal = new HashMap<Integer, GoalSettingsDTO>();
					priceCheckListLevelGoal.put(priceCheckListId, populateGoalData(rs));
					goalSettings.setPriceCheckLevelGoal(priceCheckListLevelGoal);
				}else if(locationLevelId != null && locationLevelId > 0 && productLevelId != null && productLevelId > 0){
					if(locationId != null && locationId > 0 && productId != null && productId > 0){
						if(productLevelId == locCompDTO.getProductLevelId() && productId == locCompDTO.getProductId()
								&& locationLevelId == locCompDTO.getBaseLocationLevelId() && locationId == locCompDTO.getBaseLocationId()){
							if(brandId != null && brandId > 0){
								HashMap<Integer, GoalSettingsDTO> brandLevelGoal = goalSettings.getBrandLevelGoal();
								if(brandLevelGoal == null)
									brandLevelGoal = new HashMap<Integer, GoalSettingsDTO>();
								brandLevelGoal.put(brandId, populateGoalData(rs));
								goalSettings.setBrandLevelGoal(brandLevelGoal);
							}else{
								goalSettings.setProductLevelGoal(populateGoalData(rs));
							}
						}
					}
				}else if(productLevelId != null && productLevelId > 0){
					if(productId != null && productId > 0){
						if(productLevelId == locCompDTO.getProductLevelId() && productId == locCompDTO.getProductId()){
							if(brandId != null && brandId > 0){
								HashMap<Integer, GoalSettingsDTO> brandLevelGoal = goalSettings.getBrandLevelGoal();
								if(brandLevelGoal == null)
									brandLevelGoal = new HashMap<Integer, GoalSettingsDTO>();
								brandLevelGoal.put(brandId, populateGoalData(rs));
								goalSettings.setBrandLevelGoal(brandLevelGoal);
							}else{
								goalSettings.setProductLevelGoal(populateGoalData(rs));
							}
						}
					}else{
						if(productLevelId == locCompDTO.getProductLevelId()){
							if(brandId != null && brandId > 0){
								HashMap<Integer, GoalSettingsDTO> brandLevelGoal = goalSettings.getBrandLevelGoal();
								if(brandLevelGoal == null)
									brandLevelGoal = new HashMap<Integer, GoalSettingsDTO>();
								brandLevelGoal.put(brandId, populateGoalData(rs));
								goalSettings.setBrandLevelGoal(brandLevelGoal);
							}else{
								goalSettings.setProductLevelGoal(populateGoalData(rs));
							}
						}
					}
				}
				
				if(locationLevelId != null && locationLevelId > 0){
					if(locationId != null && locationId > 0){
						if(locationLevelId.intValue() == locCompDTO.getBaseLocationLevelId() && locationId.intValue() == locCompDTO.getBaseLocationId())
							goalSettings.setLocationLevelGoal(populateGoalData(rs));
					}else{
						if(locationLevelId == locCompDTO.getBaseLocationLevelId())
							goalSettings.setLocationLevelGoal(populateGoalData(rs));
					}
				}else{
					goalSettings.setChainLevelGoal(populateGoalData(rs));
				}
			}
		}catch(SQLException e){
			logger.error("Error when retrieving goal settings - " + e.toString());
		}catch(GeneralException e){
			logger.error("Error when retrieving goal settings - " + e.toString());
		}finally{
			PristineDBUtil.close(statement);
		}
	}

	public ArrayList<GoalSettingsDTO> getGoalDetailsForProduct(Connection conn, LocationCompetitorMapDTO locCompDTO){
		PreparedStatement statement = null;
		ResultSet rs = null;
		ArrayList<GoalSettingsDTO> goalList = new ArrayList<GoalSettingsDTO>();
		try{
			Integer chainId = Integer.parseInt(new RetailPriceDAO().getChainId(conn));
			statement = conn.prepareStatement(GET_GOAL_DETAILS_FOR_PRODUCT);
			int counter = 0;
			statement.setInt(++counter, Constants.CHAIN_LEVEL_ID);
			statement.setInt(++counter, chainId);
			statement.setInt(++counter, locCompDTO.getProductLevelId());
			statement.setInt(++counter, locCompDTO.getProductId());
			statement.setInt(++counter, locCompDTO.getProductLevelId());
			statement.setInt(++counter, locCompDTO.getProductId());
			rs = statement.executeQuery();
			while(rs.next()){
				GoalSettingsDTO goalSettings = new GoalSettingsDTO();
				goalSettings.setItemName(rs.getString("ITEM_NAME"));
				goalSettings.setBrandName(rs.getString("BRAND_NAME"));
				goalSettings.setBrandId(rs.getInt("BRAND_ID"));
				goalSettings.setPriceCheckListName(rs.getString("PRICE_CHECK_LIST_NAME"));
				goalSettings.setPriceCheckListId(rs.getInt("PRICE_CHECK_LIST_ID"));
				goalSettings.setChainName(rs.getString("COMP_CHAIN_NAME"));
				goalSettings.setLocationName(rs.getString("LOCATION_NAME"));
				goalSettings.setLocationLevelId(rs.getInt("LOCATION_LEVEL_ID"));
				goalSettings.setLocationId(rs.getInt("LOCATION_ID"));
				goalSettings.setOverallGoal(rs.getDouble("OVERALL_GOAL"));
				populateGoalData(rs, goalSettings);
				goalList.add(goalSettings);
			}
		}catch(SQLException e){
			logger.error("Error when retrieving goal settings - " + e.toString());
		}catch(GeneralException e){
			logger.error("Error when retrieving goal settings - " + e.toString());
		}finally{
			PristineDBUtil.close(statement);
		}
		return goalList;
	}

	public GoalSettingsDTO populateGoalData(ResultSet rs) throws SQLException{
		GoalSettingsDTO goalsDTO = new GoalSettingsDTO();
		goalsDTO.setCurRegMinPriceIndex(rs.getInt("PI_CUR_REG_MIN"));
		goalsDTO.setCurRegMaxPriceIndex(rs.getInt("PI_CUR_REG_MAX"));
		goalsDTO.setCurSaleMinPriceIndex(rs.getInt("PI_CUR_SALE_MIN"));
		goalsDTO.setCurSaleMaxPriceIndex(rs.getInt("PI_CUR_SALE_MAX"));
		goalsDTO.setFutRegMinPriceIndex(rs.getInt("PI_FUT_REG_MIN"));
		goalsDTO.setFutRegMaxPriceIndex(rs.getInt("PI_FUT_REG_MAX"));
		goalsDTO.setFutSaleMinPriceIndex(rs.getInt("PI_FUT_SALE_MIN"));
		goalsDTO.setFutSaleMaxPriceIndex(rs.getInt("PI_FUT_SALE_MAX"));
		
		goalsDTO.setCurRegMinMargin(rs.getInt("MARGIN_CUR_REG_MIN"));
		goalsDTO.setCurRegMaxMargin(rs.getInt("MARGIN_CUR_REG_MAX"));
		goalsDTO.setCurSaleMinMargin(rs.getInt("MARGIN_CUR_SALE_MIN"));
		goalsDTO.setCurSaleMaxMargin(rs.getInt("MARGIN_CUR_SALE_MAX"));
		goalsDTO.setFutRegMinMargin(rs.getInt("MARGIN_FUT_REG_MIN"));
		goalsDTO.setFutRegMaxMargin(rs.getInt("MARGIN_FUT_REG_MAX"));
		goalsDTO.setFutSaleMinMargin(rs.getInt("MARGIN_FUT_SALE_MIN"));
		goalsDTO.setFutSaleMaxMargin(rs.getInt("MARGIN_FUT_SALE_MAX"));
		return goalsDTO;
	}
	
	public void populateGoalData(ResultSet rs, GoalSettingsDTO goalsDTO) throws SQLException{
		goalsDTO.setCurRegMinPriceIndex(rs.getInt("PI_CUR_REG_MIN"));
		goalsDTO.setCurRegMaxPriceIndex(rs.getInt("PI_CUR_REG_MAX"));
		goalsDTO.setCurSaleMinPriceIndex(rs.getInt("PI_CUR_SALE_MIN"));
		goalsDTO.setCurSaleMaxPriceIndex(rs.getInt("PI_CUR_SALE_MAX"));
		goalsDTO.setFutRegMinPriceIndex(rs.getInt("PI_FUT_REG_MIN"));
		goalsDTO.setFutRegMaxPriceIndex(rs.getInt("PI_FUT_REG_MAX"));
		goalsDTO.setFutSaleMinPriceIndex(rs.getInt("PI_FUT_SALE_MIN"));
		goalsDTO.setFutSaleMaxPriceIndex(rs.getInt("PI_FUT_SALE_MAX"));
		
		goalsDTO.setCurRegMinMargin(rs.getInt("MARGIN_CUR_REG_MIN"));
		goalsDTO.setCurRegMaxMargin(rs.getInt("MARGIN_CUR_REG_MAX"));
		goalsDTO.setCurSaleMinMargin(rs.getInt("MARGIN_CUR_SALE_MIN"));
		goalsDTO.setCurSaleMaxMargin(rs.getInt("MARGIN_CUR_SALE_MAX"));
		goalsDTO.setFutRegMinMargin(rs.getInt("MARGIN_FUT_REG_MIN"));
		goalsDTO.setFutRegMaxMargin(rs.getInt("MARGIN_FUT_REG_MAX"));
		goalsDTO.setFutSaleMinMargin(rs.getInt("MARGIN_FUT_SALE_MIN"));
		goalsDTO.setFutSaleMaxMargin(rs.getInt("MARGIN_FUT_SALE_MAX"));
	}
}
