package com.pristine.dao.pricingalert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dao.IDAO;
import com.pristine.dao.salesanalysis.SalesAggregationProductGroupDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dto.salesanalysis.ProductGroupDTO;
import com.pristine.dto.pricingalert.LocationCompetitorMapDTO;
import com.pristine.dto.pricingalert.PAItemInfoDTO;
import com.pristine.dto.pricingalert.ReportTemplateDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class PricingAlertDAO implements IDAO{

	static Logger logger = Logger.getLogger("PricingAlertDAO");
	
	private HashMap<Integer, Integer> productLevelPointer = new HashMap<Integer, Integer>();
	
	public ArrayList<ProductGroupDTO> getChildHierarchy(Connection connection) {
		ArrayList<ProductGroupDTO> productHierarchy = new ArrayList<ProductGroupDTO>();
		try{
			SalesAggregationProductGroupDAO salesAggrDAO = new SalesAggregationProductGroupDAO();
			productHierarchy = salesAggrDAO.getChildHierarchy(connection, Constants.DEPARTMENTLEVELID);
			int counter = 0;
			for (int i = productHierarchy.size(); i > 1; i--){
				productLevelPointer.put(productHierarchy.get(counter).getProductLevelId(), i-1);
				counter++;
			}
			
			return productHierarchy;
		}catch(SQLException exception){
			logger.error("Error when retrieving child hierarchy - " + exception.toString());
		}catch(GeneralException exception){
			logger.error("Error when retrieving child hierarchy - " + exception.toString());
		}
		return productHierarchy;
	}

	public String getProductHierarchy(Connection connection) throws SQLException, GeneralException
    {
        StringBuffer sb = new StringBuffer();
        
        ArrayList<ProductGroupDTO> productHierarchy = getChildHierarchy(connection);
        int productHierarchyCount = productHierarchy.size();

        sb.append("SELECT PRODUCT_HIERARCHY.* FROM ( ");
        sb.append("SELECT ITEM_CODE as ITEM_CODE_1,");
        for (int i = 1; i < productHierarchyCount; i++)
        {
            sb.append("P_L_ID_" + i + ", P_ID_" + i + ", P" + i + ".NAME NAME_" + i + ",");
        }
        sb.delete((sb.length() - 1), sb.length());
        sb.append(" FROM ");

        for (int s = 1; s < productHierarchyCount; s++)
        {
            sb.append("( ");
        }

        sb.append(" SELECT ");
        for (int j = 1; j < productHierarchyCount; j++)
        {
            if (j == 1)
            {
                sb.append(" A" + j + ".PRODUCT_LEVEL_ID P_L_ID_" + j + ", A" + j + ".PRODUCT_ID P_ID_" + j + ", A" + j + ".CHILD_PRODUCT_ID ITEM_CODE ,");
            }
            else
            {
                sb.append(" A" + j + ".PRODUCT_LEVEL_ID P_L_ID_" + j + ", A" + j + ".PRODUCT_ID P_ID_" + j + ", A" + j + ".CHILD_PRODUCT_ID CHILD_P_ID_" + j + ",");
            }
        }
        sb.delete((sb.length() - 1), sb.length());

        sb.append(" FROM ");
        for (int x = 1; x < productHierarchyCount; x++)
        {
            int loop = productHierarchyCount - 1;
            if (x > loop)
            {
                break;
            }
            sb.append("( ");
        }


        for (int k = 1; k <= productHierarchyCount; k++)
        {
            if (k == 1)
            {
                sb.append(" PRODUCT_GROUP_RELATION A" + k + " ");
            }
            else if (k == 2)
            {
                sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION A" + k + " ");
                break;
            }
        }

        String firstLevel = "";
        String nextLevel = "";
        for (int a = 1; a < productHierarchyCount + 1; a++)
        {
            if (a == 1)
            {
                firstLevel = "A" + a;
            }
            else if (a == 2)
            {
                nextLevel = "A" + a;
                break;
            }
        }
        sb.append("ON " + firstLevel + ".PRODUCT_ID = " + nextLevel + ".CHILD_PRODUCT_ID AND " + firstLevel + ".PRODUCT_LEVEL_ID = " + nextLevel + ".CHILD_PRODUCT_LEVEL_ID) ");

        for (int b = 1; b < productHierarchyCount + 1; b++)
        {
            int loopCheck = b;
            if (b >= 3)
            {
                sb.append("LEFT JOIN PRODUCT_GROUP_RELATION A" + b + " ");
                sb.append(" ON A" + (loopCheck - 1) + ".PRODUCT_ID = A" + b + ".CHILD_PRODUCT_ID AND A" + (loopCheck - 1) + ".PRODUCT_LEVEL_ID = A" + b + ".CHILD_PRODUCT_LEVEL_ID ) ");
            }
        }

        for (int c = 1; c < productHierarchyCount + 1; c++)
        {
            if (c == 1)
            {
                sb.append("WHERE A" + c + ".CHILD_PRODUCT_LEVEL_ID = 1 ");
                break;
            }
        }
        sb.append(") ");

        for (int d = 1; d < productHierarchyCount; d++)
        {
            sb.append("LEFT JOIN PRODUCT_GROUP P" + d + " ON P_ID_" + d + " = P" + d + ".PRODUCT_ID AND P_L_ID_" + d + " = P" + d + ".PRODUCT_LEVEL_ID ) ");
        }
        sb.append(" PRODUCT_HIERARCHY left join ITEM_LOOKUP on PRODUCT_HIERARCHY.ITEM_CODE_1 = ITEM_LOOKUP.ITEM_CODE  ");
        sb.append("ORDER BY ");
        for (int e = 1; e < productHierarchyCount; e++)
        {
            sb.append(" NAME_" + e + ",");
        }
        sb.delete((sb.length() - 1), sb.length());
        
        return sb.toString();
    }
	
	public HashMap<Integer, Integer> getProductLevelPointerMap(){
		return productLevelPointer;
	}
	
	public ArrayList<Integer> getKVIItems(Connection connection, int priceCheckListId) throws GeneralException{
		ArrayList<Integer> itemCodeList = new ArrayList<Integer>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    
		try{
			statement = connection.prepareStatement(GET_KVI_ITEMS);
			statement.setInt(1, priceCheckListId);
			statement.setFetchSize(10000);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				itemCodeList.add(resultSet.getInt("ITEM_CODE"));
			}
		}
		catch (SQLException e){
			logger.error("Error while executing GET_KVI_ITEMS");
			throw new GeneralException("Error while executing GET_KVI_ITEMS" + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return itemCodeList;
	}
	
	public HashMap<Integer, Integer> getPAMasterIdInfo(Connection connection, int calendarId, int locationCompetitorMapId) throws GeneralException{
		PreparedStatement statement = null;
		ResultSet rs = null;
		HashMap<Integer, Integer> paMasterDataIdMap = new HashMap<Integer, Integer>();
		try{
			statement = connection.prepareStatement(SELECT_PA_MASTER_DATA);
			statement.setInt(1, calendarId);
			statement.setInt(2, locationCompetitorMapId);
			rs = statement.executeQuery();
			
			while(rs.next()){
				paMasterDataIdMap.put(rs.getInt("PA_ALERT_TYPES_ID"), rs.getInt("PA_MASTER_DATA_ID"));
			}
		}catch(SQLException exception){
			logger.error("Error when executing SELECT_PA_MASTER_DATA - " + exception);
			throw new GeneralException("Error when executing SELECT_PA_MASTER_DATA - " + exception);
		}
		return paMasterDataIdMap;
	}
	
	public void insertPAMasterInfo(Connection connection, ArrayList<PAItemInfoDTO> paItemInfoDTOList) throws GeneralException{
		PreparedStatement statement = null;
		try{
			statement = connection.prepareStatement(INSERT_PA_MASTER_DATA);
			for(PAItemInfoDTO paItemInfo : paItemInfoDTOList){
				int counter = 0;
				statement.setInt(++counter, paItemInfo.getCalendarId());
				statement.setInt(++counter, paItemInfo.getLocationCompetitorMapId());
				statement.setInt(++counter, paItemInfo.getAlertTypesId());
				logger.info(paItemInfo.getCalendarId() + "\t" + paItemInfo.getLocationCompetitorMapId() + "\t" + paItemInfo.getAlertTypesId());
				statement.executeUpdate();
			}
		}catch(SQLException exception){
			logger.error("Error when executing INSERT_PA_MASTER_DATA - " + exception);
			throw new GeneralException("Error when executing INSERT_PA_MASTER_DATA - " + exception);
		}finally{
			PristineDBUtil.close(statement);
		}
	}
	
	public void insertPAItemInfo(Connection connection, ArrayList<PAItemInfoDTO> paItemInfoDTOList) throws GeneralException{
		PreparedStatement statement = null;
		try{
			statement = connection.prepareStatement(INSERT_PA_ITEM_INFO);
			int recCnt = 0;
			for(PAItemInfoDTO paItemInfo : paItemInfoDTOList){
				int counter = 0;
				statement.setInt(++counter, paItemInfo.getCalendarId());
				statement.setInt(++counter, paItemInfo.getLocationCompetitorMapId());
				statement.setInt(++counter, paItemInfo.getAlertTypesId());
				statement.setInt(++counter, paItemInfo.getItemCode());
				
				if(paItemInfo.getBaseCurRegPrice() == Constants.DEFAULT_NA)
					statement.setNull(++counter, Types.DOUBLE);
				else
					statement.setDouble(++counter, paItemInfo.getBaseCurRegPrice());
				statement.setString(++counter, paItemInfo.getBaseCurRegPriceEffDate());
				
				if(paItemInfo.getBasePreRegPrice() == Constants.DEFAULT_NA)
					statement.setNull(++counter, Types.DOUBLE);
				else
					statement.setDouble(++counter, paItemInfo.getBasePreRegPrice());
				
				if(paItemInfo.getBaseFutRegPrice() == Constants.DEFAULT_NA)
					statement.setNull(++counter, Types.DOUBLE);
				else
					statement.setDouble(++counter, paItemInfo.getBaseFutRegPrice());
				statement.setString(++counter, paItemInfo.getBaseFutRegPriceEffDate());
				
				if(paItemInfo.getBaseCurListCost() == Constants.DEFAULT_NA)
					statement.setNull(++counter, Types.DOUBLE);
				else
					statement.setDouble(++counter, paItemInfo.getBaseCurListCost());
				statement.setString(++counter, paItemInfo.getBaseCurListCostEffDate());
				
				if(paItemInfo.getBasePreListCost() == Constants.DEFAULT_NA)
					statement.setNull(++counter, Types.DOUBLE);
				else
					statement.setDouble(++counter, paItemInfo.getBasePreListCost());
				
				if(paItemInfo.getBaseFutListCost() == Constants.DEFAULT_NA)
					statement.setNull(++counter, Types.DOUBLE);
				else
					statement.setDouble(++counter, paItemInfo.getBaseFutListCost());
				statement.setString(++counter, paItemInfo.getBaseFutListCostEffDate());
				
				if(paItemInfo.getCompCurRegPrice() == Constants.DEFAULT_NA)
					statement.setNull(++counter, Types.DOUBLE);
				else
					statement.setDouble(++counter, paItemInfo.getCompCurRegPrice());
				statement.setString(++counter, paItemInfo.getCompCurRegPriceEffDate());
				
				if(paItemInfo.getCompPreRegPrice() == Constants.DEFAULT_NA)
					statement.setNull(++counter, Types.DOUBLE);
				else
					statement.setDouble(++counter, paItemInfo.getCompPreRegPrice());
				statement.setString(++counter, paItemInfo.getCompCurRegPriceLastObsDate());
				if(paItemInfo.isKVIItem())
					statement.setString(++counter, "Y");
				else
					statement.setString(++counter, "N");
				statement.setDouble(++counter, paItemInfo.getAvgRevenue());
				
				statement.addBatch();
				if(recCnt % Constants.BATCH_UPDATE_COUNT == 0){
					statement.executeBatch();
					statement.clearBatch();
					recCnt = 0;
				}
			}
			if(recCnt > 0){
				statement.executeBatch();
				statement.clearBatch();
			}
		}catch(SQLException exception){
			logger.error("Error when executing INSERT_PA_ITEM_INFO - " + exception);
			throw new GeneralException("Error when executing INSERT_PA_ITEM_INFO - " + exception);
		}
	}
	
	public HashMap<Integer, ArrayList<CompetitiveDataDTO>> getPriceData(Connection connection, LocationCompetitorMapDTO locationCompetitorMap, String weekStartDate, int maxDataLookupRange, boolean isCompetitor) throws GeneralException{
		long startTime = System.currentTimeMillis();
		HashMap<Integer, ArrayList<CompetitiveDataDTO>> priceData = new HashMap<Integer, ArrayList<CompetitiveDataDTO>>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    
		try{
			String query = new String(GET_PRICE_DATA);
			//query = query.replaceAll("%PRODUCT_HIERARCHY%", PRODUCT_HIERARCHY_QUERY);
			//query = query.replaceAll("%PRODUCT_HIERARCHY%", getProductHierarchy(connection));
			
			String subQuery = getQueryConditionV2(connection, locationCompetitorMap);
			query = query.replaceAll("%QUERY_CONDITION%", subQuery);
			
			subQuery = getLocationCondition(locationCompetitorMap, isCompetitor);
			query = query.replaceAll("%LOCATION_CONDITION%", subQuery);
			
			logger.debug("Price Query " + query);
			logger.debug("Counters: 1- " + locationCompetitorMap.getBaseLocationId() + " or "
					+ locationCompetitorMap.getCompLocationId() + " 2- " + weekStartDate + " 3- " + weekStartDate			
					+ " 4- " + maxDataLookupRange + " 5- " +  locationCompetitorMap.getProductLevelId() + " 6- " +			
					locationCompetitorMap.getProductId());
			
			statement = connection.prepareStatement(query);
			int counter = 0;
			if(!isCompetitor){
				statement.setInt(++counter, locationCompetitorMap.getBaseLocationId());
			}else
				statement.setInt(++counter, locationCompetitorMap.getCompLocationId());
			statement.setString(++counter,  weekStartDate);
			statement.setString(++counter,  weekStartDate);
			statement.setInt(++counter,  maxDataLookupRange);
			if(locationCompetitorMap.getProductLevelId() > 1)
				statement.setInt(++counter, locationCompetitorMap.getProductLevelId());
			if(locationCompetitorMap.getProductId() > 0)
				statement.setInt(++counter, locationCompetitorMap.getProductId());
			
			
			statement.setFetchSize(100000);
			resultSet = statement.executeQuery();
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken to retrieve price data " + (endTime - startTime));
			logger.debug("Price Query executed");
			
			while(resultSet.next()){
				CompetitiveDataDTO compDataDTO = new CompetitiveDataDTO();
				compDataDTO.itemcode = resultSet.getInt("ITEM_CODE");
				if(compDataDTO.itemcode == 0)
					continue;
				compDataDTO.weekStartDate = resultSet.getString("START_DATE");
				compDataDTO.regPrice = resultSet.getFloat("REG_UNIT_PRICE");
				compDataDTO.regMPack = resultSet.getInt("REG_M_PACK");
				compDataDTO.regMPrice = resultSet.getInt("REG_M_PRICE");
				compDataDTO.effRegRetailStartDate = resultSet.getString("EFF_REG_START_DATE");
				compDataDTO.lirId = resultSet.getInt("RET_LIR_ID");
				compDataDTO.checkDate = resultSet.getString("CHECK_DATETIME");
				if(priceData.get(compDataDTO.itemcode) != null){
					ArrayList<CompetitiveDataDTO> tempList = priceData.get(compDataDTO.itemcode);
					tempList.add(compDataDTO);
					priceData.put(compDataDTO.itemcode, tempList);
				}else{
					ArrayList<CompetitiveDataDTO> tempList = new ArrayList<CompetitiveDataDTO>();
					tempList.add(compDataDTO);
					priceData.put(compDataDTO.itemcode, tempList);
				}
			}
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_PRICE_DATA");
			throw new GeneralException("Error while executing GET_PRICE_DATA" + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		long endTime = System.currentTimeMillis();
		logger.debug("Time taken to retrieve price data " + (endTime - startTime));
		
		return priceData;
	}
	
	public HashMap<Integer, ArrayList<MovementWeeklyDTO>> getCostData(Connection connection, LocationCompetitorMapDTO locationCompetitorMap, String weekStartDate, int maxDataLookupRange) throws GeneralException{
		long startTime = System.currentTimeMillis();
		HashMap<Integer, ArrayList<MovementWeeklyDTO>> costData = new HashMap<Integer, ArrayList<MovementWeeklyDTO>>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    
		try{
			String query = new String(GET_COST_DATA);
			
			String subQuery = getQueryConditionV2(connection, locationCompetitorMap);
			query = query.replaceAll("%QUERY_CONDITION%", subQuery);
			
			subQuery = getLocationCondition(locationCompetitorMap, false);
			query = query.replaceAll("%LOCATION_CONDITION%", subQuery);
			
			logger.debug("Cost Query " + query);
			logger.debug("Counter 1: " + locationCompetitorMap.getBaseLocationId() + 
					",Counter 2 & 3: " + weekStartDate + ",Counter 4: " + maxDataLookupRange);
			
			statement = connection.prepareStatement(query);
			
			int counter = 0;
			statement.setInt(++counter, locationCompetitorMap.getBaseLocationId());
			statement.setString(++counter,  weekStartDate);
			statement.setString(++counter,  weekStartDate);
			statement.setInt(++counter,  maxDataLookupRange);
			if(locationCompetitorMap.getProductLevelId() > 1)
				statement.setInt(++counter, locationCompetitorMap.getProductLevelId());
			if(locationCompetitorMap.getProductId() > 0)
				statement.setInt(++counter, locationCompetitorMap.getProductId());
			statement.setFetchSize(100000);
			resultSet = statement.executeQuery();
			long endTimeTemp = System.currentTimeMillis();
			logger.debug("Time taken to retrieve movement data " + (endTimeTemp - startTime));
			logger.debug("Cost Query executed");
			
			while(resultSet.next()){
				MovementWeeklyDTO movementWeeklyDTO = new MovementWeeklyDTO();
				movementWeeklyDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				if(movementWeeklyDTO.getItemCode() == 0)
					continue;
				movementWeeklyDTO.setLirId(resultSet.getInt("RET_LIR_ID"));
				movementWeeklyDTO.setWeekStartDate(resultSet.getString("START_DATE"));
				movementWeeklyDTO.setListCost(resultSet.getFloat("LIST_COST"));
				movementWeeklyDTO.setEffListCostDate(resultSet.getString("EFF_LIST_COST_DATE"));
				movementWeeklyDTO.setregGrossRevenue(resultSet.getDouble("TOTAL_REVENUE"));
				movementWeeklyDTO.setQuantityRegular(resultSet.getDouble("TOTAL_QUANTITY"));
				if(costData.get(movementWeeklyDTO.getItemCode()) != null){
					ArrayList<MovementWeeklyDTO> tempList = costData.get(movementWeeklyDTO.getItemCode());
					tempList.add(movementWeeklyDTO);
					costData.put(movementWeeklyDTO.getItemCode(), tempList);
				}else{
					ArrayList<MovementWeeklyDTO> tempList = new ArrayList<MovementWeeklyDTO>();
					tempList.add(movementWeeklyDTO);
					costData.put(movementWeeklyDTO.getItemCode(), tempList);
				}
			}
		}
		catch (SQLException e)
		{
			logger.error("Error while executing GET_COST_DATA");
			throw new GeneralException("Error while executing GET_COST_DATA" + e);
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		long endTime = System.currentTimeMillis();
		logger.debug("Time taken to retrieve movement data " + (endTime - startTime));
		return costData;
	}
	
	public HashMap<String, ArrayList<ReportTemplateDTO>> getExcelTemplatesForUser(Connection connection, String userId) throws GeneralException{
		HashMap<String, ArrayList<ReportTemplateDTO>> excelTemplates = new HashMap<String, ArrayList<ReportTemplateDTO>>();
		PreparedStatement statement = null;
		ResultSet rs = null;
		try{
			statement = connection.prepareStatement(GET_EXCEL_TEMPLATE_NAME_FOR_USER);
			statement.setString(1, userId);
			rs = statement.executeQuery();
			while(rs.next()){
				ReportTemplateDTO reportTemplateDTO = new ReportTemplateDTO();
				reportTemplateDTO.setExcelTemplateName(rs.getString("PA_EXCEL_TEMPLATE_NAME"));
				reportTemplateDTO.setAlertIdForUser(rs.getString("PA_ALERT_TYPES_ID"));
				reportTemplateDTO.setColumnKeys(rs.getString("COLUMN_KEYS"));
				
				if(excelTemplates.get(rs.getString("PA_EXCEL_TEMPLATE_NAME")) != null){
					ArrayList<ReportTemplateDTO> reportTemplateDTOList = excelTemplates.get(rs.getString("PA_EXCEL_TEMPLATE_NAME"));
					reportTemplateDTOList.add(reportTemplateDTO);
					excelTemplates.put(reportTemplateDTO.getExcelTemplateName(), reportTemplateDTOList);
				}else{
					ArrayList<ReportTemplateDTO> reportTemplateDTOList = new ArrayList<ReportTemplateDTO>();
					reportTemplateDTOList.add(reportTemplateDTO);
					excelTemplates.put(reportTemplateDTO.getExcelTemplateName(), reportTemplateDTOList);
				}
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving excel template for user - " + exception);
			throw new GeneralException("Error when retrieving excel template for user - " + exception);
		}finally{
			PristineDBUtil.close(statement);
		}
		return excelTemplates;
	}
	
	public String getQueryCondition(Connection connection, LocationCompetitorMapDTO locationCompetitorMap){
		StringBuffer subQuery = new StringBuffer("");
		
		if(productLevelPointer.size() == 0){
			getChildHierarchy(connection);
		}
		if(locationCompetitorMap.getProductLevelId() > 1){
			if(productLevelPointer.get(locationCompetitorMap.getProductLevelId()) != null){
				int pointer = productLevelPointer.get(locationCompetitorMap.getProductLevelId());
				subQuery = new StringBuffer("WHERE (P_L_ID_" + pointer + " = ? ");
				if(locationCompetitorMap.getProductId() > 0){
					subQuery.append(" AND P_ID_" + pointer + " = ?)");
				}
			}
		}else if(locationCompetitorMap.getProductLevelId() == 1){
			subQuery = new StringBuffer("WHERE ITEM_CODE_1 = ? ");
		}
		
		return subQuery.toString();
	}
	
	public String getQueryConditionV2(Connection connection, LocationCompetitorMapDTO locationCompetitorMap){
		StringBuffer subQuery = new StringBuffer("");
		if(locationCompetitorMap.getProductLevelId() > 1){
			subQuery = new StringBuffer("start with product_level_id = ? ");
			if(locationCompetitorMap.getProductId() > 0){
				subQuery.append(" and product_id = ? ");
			}
			subQuery.append("connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id");	
		}else if(locationCompetitorMap.getProductLevelId() == 1){
			subQuery = new StringBuffer("where child_product_level_id = 1 and child_product_id = ?");
		}
		
		return subQuery.toString();
	}
	
	public String getLocationCondition(LocationCompetitorMapDTO locationCompetitorMap, boolean isCompetitor){
		StringBuffer subQuery = new StringBuffer("");
		if(!isCompetitor){
			if(locationCompetitorMap.getBaseLocationLevelId() == Constants.STORE_LEVEL_ID){
				subQuery.append(" S.COMP_STR_ID = ? ");
			}else{
				subQuery.append(" S.PRICE_ZONE_ID = ? ");
			}
		}else{
			if(locationCompetitorMap.getCompLocationLevelId() == Constants.STORE_LEVEL_ID){
				subQuery.append(" S.COMP_STR_ID = ? ");
			}else{
				subQuery.append(" S.PRICE_ZONE_ID = ? ");
			}
		}
		return subQuery.toString();
	}
	
	public String getQueryConditionV2(LocationCompetitorMapDTO locationCompetitorMap){
		StringBuffer subQuery = new StringBuffer("");
		if(locationCompetitorMap.getProductLevelId() > 1){
			subQuery = new StringBuffer("start with product_level_id = ? ");
			if(locationCompetitorMap.getProductId() > 0){
				subQuery.append(" and product_id = ? ");
			}
			subQuery.append("connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id");	
		}else if(locationCompetitorMap.getProductLevelId() == 1){
			subQuery = new StringBuffer("where child_product_level_id = 1 and child_product_id = ?");
		}
		
		return subQuery.toString();
	}


	public Integer getPriceCheckListId(Connection connection, String priceCheckListName) {
		Integer priceCheckListId = -1;
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    
		try{
			statement = connection.prepareStatement(GET_PRICE_CHECK_LIST_ID);
			statement.setString(1, priceCheckListName);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				priceCheckListId = resultSet.getInt("ID");
			}
		}
		catch (SQLException e){
			logger.error("Error while executing GET_PRICE_CHECK_LIST_ID");
		}
		finally{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return priceCheckListId;
	}

	/**
	 * Gets all items mapped to the input store or zone
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @return
	 * @throws GeneralException
	 */
	public ArrayList<Integer> getAllItemsAtProductLevel(Connection conn, int locationLevelId, int locationId, Set<Integer> itemCodeSet) throws GeneralException{
		ArrayList<Integer> itemsInZone = new ArrayList<Integer>();
		
		List<Integer> itemCodeList = new ArrayList<Integer>();
		int limitcount = 0;
	    int commitCount = 1000;
		for(Integer itemCode:itemCodeSet){
			itemCodeList.add(itemCode);
			limitcount++;
			if( limitcount > 0 && (limitcount%commitCount == 0)){
				Object[] values = itemCodeList.toArray();
				retrieveAllItemsAtLocationLevel(conn, locationLevelId, locationId, itemsInZone, values);
				itemCodeList.clear();
			}
		}
		if(itemCodeList.size() > 0){
			Object[] values = itemCodeList.toArray();
			retrieveAllItemsAtLocationLevel(conn, locationLevelId, locationId, itemsInZone, values);
		}
		return itemsInZone;
	}
	
	private void retrieveAllItemsAtLocationLevel(Connection conn, int locationLevelId, int locationId, ArrayList<Integer> itemsInZone, Object... values){
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			if(locationLevelId == Constants.STORE_LEVEL_ID){
				stmt = conn.prepareStatement(String.format(GET_ITEMS_IN_STORE_P1, PristineDBUtil.preparePlaceHolders(values.length)));
			}else{
				stmt = conn.prepareStatement(String.format(GET_ITEMS_IN_ZONE_P1, PristineDBUtil.preparePlaceHolders(values.length)));
			}
			stmt.setInt(1, locationId);
			PristineDBUtil.setValues(stmt, 2, values);
			stmt.setFetchSize(100000);
			long startTime = System.currentTimeMillis();
			rs = stmt.executeQuery();
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken to execute query - " + (endTime - startTime));
			startTime = System.currentTimeMillis();
			while(rs.next()){
				int itemCode = rs.getInt("ITEM_CODE");
				if(!itemsInZone.contains(itemCode)){
					itemsInZone.add(itemCode);
				}
			}
			endTime = System.currentTimeMillis();
			logger.debug("Time taken to retrieve records - " + (endTime - startTime));
			
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
			
			if(locationLevelId == Constants.STORE_LEVEL_ID){
				stmt = conn.prepareStatement(String.format(GET_ITEMS_IN_STORE_P2, PristineDBUtil.preparePlaceHolders(values.length)));
			}else{
				stmt = conn.prepareStatement(String.format(GET_ITEMS_IN_ZONE_P2, PristineDBUtil.preparePlaceHolders(values.length)));
			}
			stmt.setInt(1, locationId);
			PristineDBUtil.setValues(stmt, 2, values);
			stmt.setFetchSize(100000);
			startTime = System.currentTimeMillis();
			rs = stmt.executeQuery();
			endTime = System.currentTimeMillis();
			logger.debug("Time taken to execute query - " + (endTime - startTime));
			startTime = System.currentTimeMillis();
			while(rs.next()){
				int itemCode = rs.getInt("ITEM_CODE");
				if(!itemsInZone.contains(itemCode)){
					itemsInZone.add(itemCode);
				}
			}
			endTime = System.currentTimeMillis();
			logger.debug("Time taken to retrieve records - " + (endTime - startTime));
		}catch(SQLException exception){
			logger.info("Error in getAllItemsAtProductLevel - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
	}
	
	//private static final String GET_KVI_ITEMS = "SELECT ITEM_CODE FROM PRICE_CHECK_LIST_ITEMS WHERE PRICE_CHECK_LIST_ID = ?";
	private static final String GET_KVI_ITEMS = "SELECT DISTINCT (CASE WHEN ITEM_CODE IS NOT NULL THEN ITEM_CODE ELSE LIST_ITEM_CODE END) ITEM_CODE FROM ( " + 
												" SELECT PC.ITEM_CODE AS LIST_ITEM_CODE, I1.RET_LIR_ID, I2.ITEM_CODE FROM PRICE_CHECK_LIST_ITEMS PC " + 
												" JOIN ITEM_LOOKUP I1 ON PC.ITEM_CODE = I1.ITEM_CODE " + 
												" LEFT JOIN ITEM_LOOKUP I2 ON I1.RET_LIR_ID = I2.RET_LIR_ID " + 
												" WHERE PC.PRICE_CHECK_LIST_ID = ?)";
	
	private static final String SELECT_PA_MASTER_DATA = "SELECT PA_MASTER_DATA_ID, PA_ALERT_TYPES_ID FROM PA_MASTER_DATA WHERE CALENDAR_ID = ? AND LOCATION_COMPETITOR_MAP_ID = ?";
	
	private static final String INSERT_PA_MASTER_DATA = "INSERT INTO PA_MASTER_DATA (PA_MASTER_DATA_ID, CALENDAR_ID, LOCATION_COMPETITOR_MAP_ID, PA_ALERT_TYPES_ID) " +
														"VALUES (PA_MASTER_DATA_SEQ.NEXTVAL, ?, ?, ?)";
	
	private static final String INSERT_PA_ITEM_INFO = "INSERT INTO PA_ITEM_INFO (PA_MASTER_DATA_ID, ITEM_CODE, BASE_CUR_REG_PRICE, BASE_CUR_REG_PRICE_EFF_DATE, BASE_PRE_REG_PRICE, BASE_FUT_REG_PRICE, BASE_FUT_REG_PRICE_EFF_DATE, " +
													  "BASE_CUR_LIST_COST, BASE_CUR_LIST_COST_EFF_DATE, BASE_PRE_LIST_COST, BASE_FUT_LIST_COST, BASE_FUT_LIST_COST_EFF_DATE, " +
													  "COMP_CUR_REG_PRICE, COMP_CUR_REG_PRICE_EFF_DATE, COMP_PRE_REG_PRICE, COMP_CUR_REG_PRICE_OBS_DATE, IS_KVI_ITEM, AVG_REVENUE) VALUES " +
													  "((SELECT PA_MASTER_DATA_ID FROM PA_MASTER_DATA WHERE CALENDAR_ID = ? AND LOCATION_COMPETITOR_MAP_ID = ? AND PA_ALERT_TYPES_ID = ?) , " +
													  "?, ?, TO_DATE(?, 'MM/dd/yyyy'), ?, ?, TO_DATE(?, 'MM/dd/yyyy'), ?, TO_DATE(?, 'MM/dd/yyyy'), ?, ?, TO_DATE(?, 'MM/dd/yyyy'), ?, TO_DATE(?, 'MM/dd/yyyy'), ?, TO_DATE(?, 'MM/dd/yyyy'), ?, ?) ";
	
	/*private static final String GET_PRICE_DATA = "SELECT RETAILER_ITEM_CODE, " + 
													"  UPC, " + 
													"  ITEM_CODE, " + 
													"  RET_LIR_ID, " +
													"  TO_CHAR(START_DATE,'MM/DD/YYYY') AS START_DATE, " + 
													"  REG_UNIT_PRICE, " +
													"  REG_M_PACK, REG_M_PRICE, " +
													"  TO_CHAR(EFF_REG_START_DATE,'MM/DD/YYYY') AS EFF_REG_START_DATE, " + 
													"  TO_CHAR(CHECK_DATETIME, 'MM/DD/YYYY') AS CHECK_DATETIME " +
													"FROM " + 
													"  (SELECT * " + 
													"  FROM " + 
													"    (SELECT PRODUCT_HIERARCHY.*, " + 
													"      IL.RETAILER_ITEM_CODE, " + 
													"      IL.UPC, " + 
													"      IL.RET_LIR_ID " +
													"    FROM " + 
													"      ( " + 
													"      %PRODUCT_HIERARCHY% " + 
													"      )PRODUCT_HIERARCHY " + 
													"    LEFT JOIN ITEM_LOOKUP IL " + 
													"    ON PRODUCT_HIERARCHY.ITEM_CODE_1 = IL.ITEM_CODE " + 
													//"    WHERE (  " +  
													"	%QUERY_CONDITION% " + 
													//"    ) " + 
													"    ) ITEM_HIERARCHY " + 
													"  LEFT JOIN " + 
													"    (SELECT UNITPRICE(CD.REG_PRICE, CD.REG_M_PRICE, CD.REG_M_PACK) AS REG_UNIT_PRICE, S.START_DATE, S.END_DATE, CD.* " + 
													"    FROM COMPETITIVE_DATA CD " + 
													"    LEFT JOIN SCHEDULE S " + 
													"    ON CD.SCHEDULE_ID                     = S.SCHEDULE_ID " + 
													"    WHERE " +
													"	 %LOCATION_CONDITION% " +
													//"		COMP_STR_ID                     = ? " + 
													//"    AND PRICE_ZONE_ID                    IS NULL " + 
													"    AND START_DATE                       <= TO_DATE(?,'MM/DD/YYYY') " + 
													"    AND START_DATE                       >= TO_DATE(?,'MM/DD/YYYY') - ? " + 
													"    )ITEM_PRICES ON ITEM_PRICES.ITEM_CODE = ITEM_HIERARCHY.ITEM_CODE_1 " + 
													"  )  " + 
													"ORDER BY ITEM_CODE, " + 
													"  END_DATE DESC ";*/
	
	/*private static final String GET_PRICE_DATA = "SELECT RETAILER_ITEM_CODE, " + 
													"   UPC, " + 
													"   ITEM_PRICES.ITEM_CODE, " + 
													"   RET_LIR_ID, " + 
													"   TO_CHAR(START_DATE,'MM/DD/YYYY') AS START_DATE, " + 
													"   REG_UNIT_PRICE, " + 
													"   REG_M_PACK, " + 
													"   REG_M_PRICE, " + 
													"   TO_CHAR(EFF_REG_START_DATE,'MM/DD/YYYY') AS EFF_REG_START_DATE, " + 
													"   TO_CHAR(CHECK_DATETIME, 'MM/DD/YYYY')    AS CHECK_DATETIME " + 
													" FROM " + 
													"   (SELECT UNITPRICE(CD.REG_PRICE, CD.REG_M_PRICE, CD.REG_M_PACK) AS REG_UNIT_PRICE, " + 
													"     S.START_DATE, " + 
													"     S.END_DATE, " + 
													"     CD.* " + 
													"   FROM COMPETITIVE_DATA CD " + 
													"   LEFT JOIN SCHEDULE S " + 
													"   ON CD.SCHEDULE_ID   = S.SCHEDULE_ID " + 
													"   WHERE %LOCATION_CONDITION% " + 
													"   AND START_DATE     <= TO_DATE(?,'MM/DD/YYYY') " + 
													"   AND START_DATE     >= TO_DATE(?,'MM/DD/YYYY') - ? " + 
													"   )ITEM_PRICES " + 
													" LEFT JOIN " + 
													"   (SELECT * " + 
													"   FROM " + 
													"     (SELECT * " + 
													"     FROM product_group_relation pgr " + 
													"      %QUERY_CONDITION% " + 
													"     ) " + 
													"   WHERE child_product_level_id = 1 " + 
													"   ) ITEM_LIST " + 
													" ON ITEM_PRICES.ITEM_CODE = ITEM_LIST.child_product_id " + 
													" LEFT JOIN ITEM_LOOKUP IL " + 
													" ON ITEM_PRICES.ITEM_CODE = IL.ITEM_CODE " + 
													" ORDER BY ITEM_CODE ASC, " + 
													"   END_DATE DESC";*/
		private static final String GET_PRICE_DATA = "SELECT RETAILER_ITEM_CODE, " + 
											"   UPC, " + 
											"   ITEM_PRICES.ITEM_CODE, " + 
											"   RET_LIR_ID, " + 
											"   TO_CHAR(START_DATE,'MM/DD/YYYY') AS START_DATE, " + 
											"   REG_UNIT_PRICE, " + 
											"   REG_M_PACK, " + 
											"   REG_M_PRICE, " + 
											"   TO_CHAR(EFF_REG_START_DATE,'MM/DD/YYYY') AS EFF_REG_START_DATE, " + 
											"   TO_CHAR(CHECK_DATETIME, 'MM/DD/YYYY')    AS CHECK_DATETIME " + 
											" FROM " + 
											"   (SELECT CD.ITEM_CODE, UNITPRICE(CD.REG_PRICE, CD.REG_M_PRICE, CD.REG_M_PACK) AS REG_UNIT_PRICE, " + 
											"     S.START_DATE, " + 
											"     S.END_DATE, " + 
											"     CD.REG_M_PACK, CD.REG_M_PRICE, CD.EFF_REG_START_DATE, CD.CHECK_DATETIME " + 
											//"   FROM SYNONYM_COMPETITIVE_DATA CD " + 
											"   FROM SYN_COMPETITIVE_DATA CD " +
											"   LEFT JOIN SCHEDULE S " + 
											"   ON CD.SCHEDULE_ID   = S.SCHEDULE_ID " + 
											"   WHERE %LOCATION_CONDITION% " + 
											"   AND START_DATE     <= TO_DATE(?,'MM/DD/YYYY') " + 
											"   AND START_DATE     >= TO_DATE(?,'MM/DD/YYYY') - ? " + 
											"   AND CD.ITEM_CODE IN ( " +
											"   SELECT CHILD_PRODUCT_ID " + 
											"   FROM " + 
											"     (SELECT CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID " + 
											"     FROM product_group_relation pgr " + 
											"      %QUERY_CONDITION% " +
											"     ) " + 
											"   WHERE child_product_level_id = 1 " + 
											"   ) " +
											"   )ITEM_PRICES " +
											" LEFT JOIN ITEM_LOOKUP IL " + 
											" ON ITEM_PRICES.ITEM_CODE = IL.ITEM_CODE " + 
											" ORDER BY ITEM_CODE ASC, " + 
											"   END_DATE DESC";
	
	/*private static final String GET_COST_DATA = "SELECT RETAILER_ITEM_CODE, " + 
													"  UPC, " + 
													"  ITEM_CODE, " + 
													"  RET_LIR_ID, " +
													"  TO_CHAR(START_DATE,'MM/DD/YYYY') AS START_DATE, " + 
													"  LIST_COST,   " + 
													"  TO_CHAR(EFF_LIST_COST_DATE,'MM/DD/YYYY') AS EFF_LIST_COST_DATE, " + 
													"  REVENUE_REGULAR + REVENUE_SALE AS TOTAL_REVENUE,  " +  
													"  QUANTITY_REGULAR + QUANTITY_SALE AS TOTAL_QUANTITY " + 
													"FROM " + 
													"  (SELECT * " + 
													"  FROM " +
													"    (SELECT MW.ITEM_CODE, " + 
													"      MW.LIST_COST, " + 
													"      MW.EFF_LIST_COST_DATE, " + 
													"      MW.QUANTITY_REGULAR, " + 
													"      MW.QUANTITY_SALE, " + 
													"      MW.REVENUE_REGULAR, " + 
													"      MW.REVENUE_SALE , " + 
													"      S.START_DATE " + 
													"    FROM MOVEMENT_WEEKLY MW " + 
													"    LEFT JOIN COMPETITIVE_DATA CD " + 
													"    ON MW.CHECK_DATA_ID = CD.CHECK_DATA_ID " + 
													"    LEFT JOIN SCHEDULE S " + 
													"    ON CD.SCHEDULE_ID    = S.SCHEDULE_ID " + 
													"    WHERE " +
													" %LOCATION_CONDITION% " +
													//"		S.COMP_STR_ID  = ? " + 
													//"    AND S.PRICE_ZONE_ID IS NULL " + 
													"    AND S.START_DATE    <= TO_DATE(?,'MM/DD/YYYY') " + 
													"    AND S.START_DATE    >= TO_DATE(?,'MM/DD/YYYY') - ? " + 
													"    )ITEM_PRICES " +
													"  LEFT JOIN " +
													"    (SELECT PRODUCT_HIERARCHY.*, " + 
													"      IL.RETAILER_ITEM_CODE, " + 
													"      IL.UPC, " + 
													"      IL.RET_LIR_ID " +
													"    FROM " + 
													"      ( " + 
													"      %PRODUCT_HIERARCHY% " + 
													"      )PRODUCT_HIERARCHY " + 
													"    LEFT JOIN ITEM_LOOKUP IL " + 
													"    ON PRODUCT_HIERARCHY.ITEM_CODE_1 = IL.ITEM_CODE " + 
													//"    WHERE (  " +  
													"	%QUERY_CONDITION% " + 
													//"    ) " + 
													"    ) ITEM_HIERARCHY " + 
													"	ON ITEM_PRICES.ITEM_CODE = ITEM_HIERARCHY.ITEM_CODE_1 " + 
													"  )  " + 
													"ORDER BY ITEM_CODE, " + 
													"  START_DATE DESC ";	*/
	
	/*private static final String GET_COST_DATA = "SELECT  " + 
												"   IL.RETAILER_ITEM_CODE, " + 
												"   IL.UPC, " + 
												"   IL.ITEM_CODE, " + 
												"   IL.RET_LIR_ID, " + 
												"   TO_CHAR(START_DATE,'MM/DD/YYYY') AS START_DATE, " + 
												"   LIST_COST, " + 
												"   TO_CHAR(EFF_LIST_COST_DATE,'MM/DD/YYYY') AS EFF_LIST_COST_DATE, " + 
												"   REVENUE_REGULAR  + REVENUE_SALE           AS TOTAL_REVENUE, " + 
												"   QUANTITY_REGULAR + QUANTITY_SALE          AS TOTAL_QUANTITY " + 
												" FROM " + 
												" ( " + 
												"     SELECT MW.ITEM_CODE, " + 
												"       MW.LIST_COST, " + 
												"       MW.EFF_LIST_COST_DATE, " + 
												"       MW.QUANTITY_REGULAR, " + 
												"       MW.QUANTITY_SALE, " + 
												"       MW.REVENUE_REGULAR, " + 
												"       MW.REVENUE_SALE , " + 
												"       S.START_DATE, " +
												"		S.END_DATE " +
												"     FROM MOVEMENT_WEEKLY MW " + 
												"     LEFT JOIN COMPETITIVE_DATA CD " + 
												"     ON MW.CHECK_DATA_ID = CD.CHECK_DATA_ID " + 
												"     LEFT JOIN SCHEDULE S " + 
												"     ON CD.SCHEDULE_ID     = S.SCHEDULE_ID " + 
												"     WHERE %LOCATION_CONDITION% " + 
												"     AND S.START_DATE     <= TO_DATE(?,'MM/DD/YYYY') " + 
												"     AND S.START_DATE     >= TO_DATE(?,'MM/DD/YYYY') - ? " + 
												" ) ITEM_PRICES " + 
												" LEFT JOIN " + 
												" (	 " + 
												"     select * from (select * from product_group_relation pgr  " + 
												"     %QUERY_CONDITION%) " + 
												"     where child_product_level_id = 1 " + 
												" ) ITEM_LIST  " + 
												" ON ITEM_PRICES.ITEM_CODE = ITEM_LIST.child_product_id  " + 
												" LEFT JOIN ITEM_LOOKUP IL ON ITEM_PRICES.ITEM_CODE = IL.ITEM_CODE  " + 
												" ORDER BY ITEM_CODE, " + 
												"   END_DATE DESC  ";*/
	
	private static final String GET_COST_DATA = "SELECT  " + 
												"   IL.RETAILER_ITEM_CODE, " + 
												"   IL.UPC, " + 
												"   IL.ITEM_CODE, " + 
												"   IL.RET_LIR_ID, " + 
												"   TO_CHAR(START_DATE,'MM/DD/YYYY') AS START_DATE, " + 
												"   LIST_COST, " + 
												"   TO_CHAR(EFF_LIST_COST_DATE,'MM/DD/YYYY') AS EFF_LIST_COST_DATE, " + 
												"   REVENUE_REGULAR  + REVENUE_SALE           AS TOTAL_REVENUE, " + 
												"   QUANTITY_REGULAR + QUANTITY_SALE          AS TOTAL_QUANTITY " + 
												" FROM " + 
												" ( " + 
												"     SELECT MW.ITEM_CODE, " + 
												"       MW.LIST_COST, " + 
												"       MW.EFF_LIST_COST_DATE, " + 
												"       MW.QUANTITY_REGULAR, " + 
												"       MW.QUANTITY_SALE, " + 
												"       MW.REVENUE_REGULAR, " + 
												"       MW.REVENUE_SALE , " + 
												"       S.START_DATE, " +
												"		S.END_DATE " +
												"     FROM MOVEMENT_WEEKLY MW " + 
												//"     LEFT JOIN SYNONYM_COMPETITIVE_DATA CD " + 
												"     LEFT JOIN SYN_COMPETITIVE_DATA CD " +
												"     ON MW.CHECK_DATA_ID = CD.CHECK_DATA_ID " + 
												"     LEFT JOIN SCHEDULE S " + 
												"     ON CD.SCHEDULE_ID     = S.SCHEDULE_ID " + 
												"     WHERE %LOCATION_CONDITION% " + 
												"     AND S.START_DATE     <= TO_DATE(?,'MM/DD/YYYY') " + 
												"     AND S.START_DATE     >= TO_DATE(?,'MM/DD/YYYY') - ? " + 
												"	  AND CD.ITEM_CODE IN ( " +
												"     select CHILD_PRODUCT_ID from (select child_product_id, child_product_level_id from product_group_relation pgr " +
												"     %QUERY_CONDITION%) " + 
												"     where child_product_level_id = 1 " +
												"	  ) " +	
												" )ITEM_PRICES " + 
												" LEFT JOIN ITEM_LOOKUP IL ON ITEM_PRICES.ITEM_CODE = IL.ITEM_CODE  " + 
												" ORDER BY ITEM_CODE, " + 
												"   END_DATE DESC  ";
	
	private static final String GET_EXCEL_TEMPLATE_NAME_FOR_USER = "SELECT UT.PA_EXCEL_TEMPLATE_NAME, PT.PA_ALERT_TYPES_ID, PT.COLUMN_KEYS " + 
																   "FROM PA_USER_TEMPLATE_MAP UT, PA_REPORT_TEMPLATES PT " +
																   "WHERE UT.USER_ID = ? " +
																   "AND UT.PA_EXCEL_TEMPLATE_NAME = PT.EXCEL_TEMPLATE_NAME";
	
	private static final String GET_ITEMS_IN_ZONE_P1 = "SELECT ITEM_CODE FROM STORE_ITEM_MAP WHERE " +
			"LEVEL_TYPE_ID = 2 AND LEVEL_ID IN (SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID = ?) AND ITEM_CODE IN(%s)";

	private static final String GET_ITEMS_IN_ZONE_P2 = "SELECT ITEM_CODE FROM STORE_ITEM_MAP WHERE LEVEL_TYPE_ID = 1 AND LEVEL_ID = (SELECT '00'||ZONE_NUM FROM RETAIL_PRICE_ZONE WHERE PRICE_ZONE_ID = ?) " +
				"AND ITEM_CODE IN(%s)";
	
	private static final String GET_ITEMS_IN_STORE_P1 = "SELECT ITEM_CODE FROM STORE_ITEM_MAP WHERE " +
				"LEVEL_TYPE_ID = 1 AND LEVEL_ID IN (SELECT '00'||ZONE_NUM FROM RETAIL_PRICE_ZONE WHERE COMP_STR_ID = ?) AND ITEM_CODE IN(%s)";
	
	private static final String GET_ITEMS_IN_STORE_P2 = "SELECT ITEM_CODE FROM STORE_ITEM_MAP WHERE LEVEL_TYPE_ID = 2 AND LEVEL_ID = (SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE COMP_STR_ID = ?) " +
				"AND ITEM_CODE IN(%s)";
	
	private static final String GET_PRICE_CHECK_LIST_ID = "SELECT ID FROM PRICE_CHECK_LIST WHERE NAME = ?";

}
