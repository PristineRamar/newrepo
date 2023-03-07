package com.pristine.dao.priceChangePerformance;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;


import com.pristine.dto.priceChangePerformance.PriceDataDTO;
import com.pristine.util.PristineDBUtil;

public class PriceDataDAO {
	
	private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	private static final String Get_Price_Data = "SELECT "
			+ "PRODUCT_ID,"
			+ "REG_PRICE,"
			+ "REG_M_PRICE,"
			+ "REG_M_PACK,"
			//+ "FINAL_PRICE,"
			//+ "SALE_FLAG,"
			//+ "CALENDAR_ID "
			+ "LIST_COST "
			//+ "SALE_PRICE,"
			//+ "SALE_M_PRICE,"
			//+ "SALE_M_PACK "
			+ "FROM IMS_WEEKLY_ZONE "
			+ "WHERE PRODUCT_ID IN (%ITEM_CODES_LIST%) "
			+ "AND CALENDAR_ID IN "
			+ "( SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC WHERE "
			+ "(RC.START_DATE >= (TO_DATE( '%START_DATE%','yyyy-MM-dd')) "
			+ "AND RC.START_DATE <= (TO_DATE( '%END_DATE%','yyyy-MM-dd')) AND ROW_TYPE = 'W')) "
			+ "AND LOCATION_ID = ? ";
	
	private static Logger logger = Logger.getLogger("PriceDataDAO");
	private Connection conn = null;
	
	public Map<Integer, PriceDataDTO> GetPriceData(Connection Con, String ItemCodeList, String StartDate ,String EndDate, String LocId ){
		conn =Con;
		
		//List<PriceDataDTO> ResultObj = new ArrayList<PriceDataDTO>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Map<Integer, PriceDataDTO> ResultObjMap = new HashMap();
		 BigDecimal bd=null;
		
		try {
		String sql =String.valueOf(Get_Price_Data);
		
		sql = sql.replace("%ITEM_CODES_LIST%", ItemCodeList);
		sql = sql.replace("%START_DATE%", StartDate);
		sql = sql.replace("%END_DATE%", EndDate);
	
		stmt = conn.prepareStatement(sql);
		
		
		//stmt.setString(1, ItemCodeList);
		//stmt.setString(1, StartDate);
	    //stmt.setString(2, EndDate);
		stmt.setInt(1, Integer.parseInt(LocId));
		//logger.info("Price Data Fetch Query:" + stmt);
		//logger.debug("Fetching Price data For " + ItemCodeList);
		System.out.println("Fetching Price DATA ");
		rs = stmt.executeQuery();
		
		while(rs.next()){
			PriceDataDTO Pricedata = new PriceDataDTO();
			Pricedata.setProductId(rs.getInt("PRODUCT_ID"));
			Pricedata.setLY_RegularPrice(rs.getFloat("REG_PRICE"));
			/*if(rs.wasNull()) {
				Pricedata.setRegularPriceNull(true);
			}else {
				Pricedata.setRegularPriceNull(false);
			}*/
			Pricedata.setLY_ListCost(rs.getFloat("LIST_COST"));
		//	if(rs.wasNull()||Pricedata.getLY_ListCost()==0) {
			//	Pricedata.setListCostNull(true);
			//}else {
				//Pricedata.setListCostNull(false);
			//}
			if(rs.getInt("REG_M_PACK")>1) {
				Pricedata.setLY_RegularPrice((rs.getFloat("REG_M_PRICE")/rs.getInt("REG_M_PACK")));
			}
		//	if(Pricedata.getLY_RegularPrice()==0) {
			//	Pricedata.setRegularPriceNull(true);
			//}else {
			//	Pricedata.setRegularPriceNull(false);
			//}
			 bd = new BigDecimal(Pricedata.getLY_RegularPrice()).setScale(2, RoundingMode.HALF_UP);
			Pricedata.setLY_RegularPrice(bd.floatValue());
			
			bd = new BigDecimal(Pricedata.getLY_ListCost()).setScale(2,RoundingMode.HALF_UP);
			Pricedata.setLY_ListCost(bd.floatValue());
			if(Pricedata.getLY_RegularPrice()!=0) {
			if(!ResultObjMap.containsKey(Pricedata.getProductId())) {
				ResultObjMap.put(Pricedata.getProductId(),Pricedata );
			}
			}
			
			
			//ResultObj.add(Pricedata);
			
			
		}
		
		logger.info("Fetching Price data Complete " );
		System.out.println("Fetching Price DATA Complete");
		
		}
		catch(Exception Ex)
		{
			logger.error("PriceDataDAO getPriceData: " + Ex.getMessage());
			
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
	
	
	return ResultObjMap;
		
	}

}
