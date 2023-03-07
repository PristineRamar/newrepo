package com.pristine.dao;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class PriceHistoryDAO implements IDAO {
	private static Logger logger = Logger.getLogger("PriceHistoryDAO ");
	
	public String getCompStrName(Connection conn, String compStrNo) throws GeneralException{
		
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT NAME FROM COMPETITOR_STORE where COMP_STR_NO = '"); 
		sql.append(compStrNo);
		sql.append("'");
		
		return PristineDBUtil.getSingleColumnVal(conn, sql, "getCompStrName");
		
	}
	
	public String getZoneNum(Connection conn, int zoneId) throws GeneralException{
		
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT ZONE_NUM FROM retail_price_zone where PRICE_ZONE_ID = "); 
		sql.append(zoneId);
		
		return PristineDBUtil.getSingleColumnVal(conn, sql, "getZoneName");
		
	}
	
	public String getProductGroupName(Connection conn, int productLevelId, int productId) throws GeneralException{
		
		StringBuffer sql = new StringBuffer();
		
		sql.append(" SELECT NAME FROM PRODUCT_GROUP where PRODUCT_LEVEL_ID = "); 
		sql.append(productLevelId);
		sql.append(" AND PRODUCT_ID = ");
		sql.append(productId);
		
		return PristineDBUtil.getSingleColumnVal(conn, sql, "getProductGroupName");
	}

	
	public List<ProductMetricsDataDTO> getIMSHistory(Connection conn, int productLevelId, int productId, int locationLevelId, 
			int locationId, String startDate, String endDate, String IMS_TABLE ) throws GeneralException{
		
		List<ProductMetricsDataDTO> imsHistoryList = new ArrayList<ProductMetricsDataDTO>(); 
		StringBuffer sb = new StringBuffer();
		
		sb.append("SELECT IMS.CALENDAR_ID, IMS.LOCATION_LEVEL_ID, IMS.LOCATION_ID, IMS.PRODUCT_LEVEL_ID, IMS.PRODUCT_ID,");
		sb.append(" IMS.SALE_FLAG, IMS.PROMOTION_ID,");
		sb.append(" UNITPRICE(IMS.REG_PRICE, IMS.REG_M_PRICE, IMS.REG_M_PACK) AS REG_PRICE, ");
		sb.append(" UNITPRICE(IMS.SALE_PRICE, IMS.SALE_M_PRICE, IMS.SALE_M_PACK) AS SALE_PRICE, ");
		sb.append(" IMS.FINAL_PRICE, IMS.LIST_COST, IMS.DEAL_COST, IMS.FINAL_COST, ");
		sb.append(" IMS.TOT_REVENUE, IMS.TOT_MOVEMENT, IL.RET_LIR_ID, TO_CHAR(RC.START_DATE, 'MM/DD/YYYY') AS START_DATE, ");
		sb.append(" TO_CHAR(RC.END_DATE, 'MM/DD/YYYY') AS END_DATE ");
		sb.append(" FROM ").append(IMS_TABLE).append(" IMS");
		sb.append(" JOIN ITEM_LOOKUP IL ON IMS.PRODUCT_ID = IL.ITEM_CODE");
		sb.append(" JOIN RETAIL_CALENDAR RC on IMS.CALENDAR_ID = RC.CALENDAR_ID ");
	    sb.append(" WHERE IMS.PRODUCT_ID IN ( ");
	    
	    sb.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR ");
	    sb.append(" start with product_level_id = ").append(productLevelId);
	    sb.append(" and product_id = ").append(productId);
	    sb.append(" connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id ");
	    sb.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1 ");
	    sb.append(") AND IL.ACTIVE_INDICATOR ='Y' ");
	    sb.append(" AND IMS.LOCATION_LEVEL_ID =  ").append(locationLevelId);
	    sb.append(" AND IMS.LOCATION_ID =  ").append(locationId);
	    sb.append(" AND RC.START_DATE BETWEEN TO_DATE('").append(startDate).append("', 'MM/DD/YYYY')");
	    sb.append(" AND TO_DATE('").append(endDate).append("', 'MM/DD/YYYY')");
	    sb.append(" ORDER BY IL.RET_LIR_ID, IMS.PRODUCT_ID, RC.START_DATE ASC ");
	    //Add the Date conditions
	    
	    //Add the code to get the info to DTO
	    logger.debug("Query -- " + sb.toString());
	    CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getIMSHistory");
	    try {
            while(result.next()){
            	ProductMetricsDataDTO imsdto = new ProductMetricsDataDTO();
            	imsdto.setProductId(result.getInt("PRODUCT_ID"));
            	imsdto.setCalendarId( result.getInt("CALENDAR_ID"));
            	imsdto.setSaleFlag(result.getString("SALE_FLAG"));
            	imsdto.setRegularPrice(result.getDouble("REG_PRICE"));
            	imsdto.setSalePrice(result.getDouble("SALE_PRICE"));
            	imsdto.setListPrice(result.getDouble("LIST_COST"));
            	imsdto.setDealPrice(result.getDouble("DEAL_COST"));
            	imsdto.setTotalRevenue(result.getDouble("TOT_REVENUE"));
            	imsdto.setTotalMovement(result.getDouble("TOT_MOVEMENT"));
            	imsdto.setStartDate(result.getString("START_DATE"));
            	imsdto.setEndDate(result.getString("END_DATE"));
            	imsHistoryList.add(imsdto);
            }
	    }catch(Exception e){
            throw new GeneralException ( "Cached Rowset Access Exception", e);
	    }

	    return (imsHistoryList);
	}

	public int getCompStrId(Connection conn, String compStrNo) throws GeneralException {
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT COMP_STR_ID FROM COMPETITOR_STORE where COMP_STR_NO = '"); 
		sql.append(compStrNo);
		sql.append("'");
		
		return Integer.parseInt(PristineDBUtil.getSingleColumnVal(conn, sql, "getCompStrId"));
	}
	
	public HashMap<String,Double> getCompetitorRetails( Connection conn, int compStrId, int productLevelId, int productId, 
			int NO_OF_DAYS_BACK) throws GeneralException {
		HashMap<String,Double> compPriceMap = new HashMap<String,Double>();

		StringBuffer sb = new StringBuffer();  
		sb.append("select * FROM (");
		sb.append(" select check_data_id, CD.ITEM_CODE, 'LIR' || IL.RET_LIR_ID  as KEY_ITEM_CODE, ");
		sb.append(" UNITPRICE(REG_PRICE,REG_M_PRICE,REG_M_PACK) AS REG_PRICE FROM COMPETITIVE_DATA_LIG CD ");
		sb.append(" JOIN ITEM_LOOKUP IL on CD.ITEM_CODE = IL.ITEM_CODE ");
		sb.append(" where Schedule_id in " );
		sb.append(" ( select schedule_id from schedule" );
		sb.append(" where start_date between sysdate - ").append(NO_OF_DAYS_BACK);
		sb.append(" and sysdate ");
		sb.append(" and comp_Str_id = ").append(compStrId).append(")");;
		sb.append(" and IL.RET_LIR_ID is not null ");
		sb.append(" UNION ");
		sb.append(" select check_data_id, CD.ITEM_CODE, 'ITM' || IL.ITEM_CODE  as KEY_ITEM_CODE, "); 
		sb.append(" UNITPRICE(REG_PRICE,REG_M_PRICE,REG_M_PACK) AS REG_PRICE FROM COMPETITIVE_DATA CD ");
		sb.append(" JOIN ITEM_LOOKUP IL on CD.ITEM_CODE = IL.ITEM_CODE ");
		sb.append(" where Schedule_id in " );
		sb.append(" ( select schedule_id from schedule" );
		sb.append(" where start_date between sysdate - ").append(NO_OF_DAYS_BACK);
		sb.append(" and sysdate ");
		sb.append(" and comp_Str_id = ").append(compStrId).append(")");
		sb.append(" and IL.RET_LIR_ID is null ").append(")");
		sb.append(" where item_code in (  ");
	    sb.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR ");
	    sb.append(" start with product_level_id = ").append(productLevelId);
	    sb.append(" and product_id = ").append(productId);
	    sb.append(" connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id ");
	    sb.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1 ) and REG_PRICE> 0 order by check_data_id desc");
	    CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getCompHistory");

	    try {
            while(result.next()){
            	String keyValue = result.getString("KEY_ITEM_CODE");
            	if( !compPriceMap.containsKey(keyValue)){
            		compPriceMap.put(keyValue, result.getDouble("REG_PRICE"));
            	}
            }
    
         }catch(Exception e){
                throw new GeneralException ( "Cached Rowset Access Exception", e);
    	    }

		return compPriceMap;
	}

}
