package com.pristine.dao.priceChangePerformance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dto.priceChangePerformance.ItemInfoDTO;
import com.pristine.util.PristineDBUtil;


public class CommonTaskDAO {
	
	private static Logger logger = Logger.getLogger("CommonTaskDAO");
	private Connection conn = null;
	
	private static final String Get_Week_Cal_Id ="SELECT RC.CALENDAR_ID,TO_CHAR(RC.START_DATE,'YYYY-MM-DD') AS START_DATE FROM RETAIL_CALENDAR RC"
		+" WHERE RC.START_DATE >= (TO_DATE( '%START_DATE%','yyyy-MM-dd')) AND" 
			+" RC.START_DATE <= (TO_DATE( '%END_DATE%','yyyy-MM-dd')) AND RC.ROW_TYPE = 'W'";
	
	private static final String Get_Item_Info = "SELECT "
			+ "ITEM_CODE,"
			+ "RETAILER_ITEM_CODE,"
			+ "ITEM_NAME,"
			+ "UPC,"
			+ "RET_LIR_ID,"
			+ "RET_LIR_NAME "
			+ "FROM "
			+ "ITEM_DETAILS_VIEW WHERE CATEGORY_ID IN (?) AND ACTIVE_INDICATOR = 'Y' AND LIR_IND = 'N'";
	private static final String Get_Lig_Info_2 ="SELECT UNIQUE(rl.ret_lir_id) AS RET_LIR_ID, rl.ret_lir_code AS RETAILER_ITEM_CODE, rl.ret_lir_name AS ret_lir_name FROM  RETAILER_LIKE_ITEM_GROUP RL right JOIN (SELECT * FROM item_details_view WHERE ret_lir_id is not null and category_id = ? and active_indicator='Y')IDW ON RL.ret_lir_id=IDW.ret_lir_id  ";
	
	private static final String Get_Lig_Info = "SELECT "
			+ "ITEM_CODE,"
			+ "RETAILER_ITEM_CODE,"
			+ "ITEM_NAME,"
			+ "UPC,"
			+ "RET_LIR_ID,"	
			+ "RET_LIR_NAME "
			+ "FROM "
			+ "ITEM_DETAILS_VIEW WHERE CATEGORY_ID IN (?) AND ACTIVE_INDICATOR = 'Y' AND LIR_IND = 'Y' AND RET_LIR_ID IS NOT NULL";
	
	private static final String Get_Store_ids ="SELECT comp_str_id STORE_ID" + 
			"  FROM competitor_store" + 
			"  WHERE comp_str_id IN" + 
			"    ( SELECT DISTINCT STORE_ID" + 
			"    FROM PR_PRODUCT_LOCATION_STORE" + 
			"    WHERE PRODUCT_LOCATION_MAPPING_ID IN" + 
			"      (SELECT PRODUCT_LOCATION_MAPPING_ID" + 
			"      FROM PR_PRODUCT_LOCATION_MAPPING" + 
			"      WHERE PRODUCT_LEVEL_ID = ?" + 
			"      AND PRODUCT_ID        IN (?)" + 
			"      AND LOCATION_LEVEL_ID  = ?" + 
			"      AND LOCATION_ID       IN (?)" + 
			"      )" + 
			"    )" + 
			"  AND excl_forecast IS NULL" + 
			"  AND close_date IS Null" + 
			"  AND open_date <= to_date('%LY_START_WEEK_DATE%','yyyy-mm-dd')";
	
	public Map<Integer, ItemInfoDTO> getItemInfo(int prodId , Connection Con){
		Map<Integer, ItemInfoDTO> ItemList = new HashMap();
		conn = Con;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String sql = Get_Item_Info;
		
		try {
		stmt = conn.prepareStatement(sql);
		
		stmt.setInt(1, prodId);
		
		logger.debug("Item Info Fetch Query:" + stmt.toString());
		stmt.setFetchSize(10000);
		rs = stmt.executeQuery();
		
		while(rs.next()){
			ItemInfoDTO Item = new ItemInfoDTO();
			
			Item.setItemCode(rs.getInt("ITEM_CODE"));
			Item.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
			//Item.setItemName(rs.getString("ITEM_NAME"));
			//Item.setUPC(rs.getLong("UPC"));
			Item.setRetLIRId(rs.getLong("RET_LIR_ID"));
			Item.setRetLIRName(rs.getString("RET_LIR_NAME")==null?rs.getString("ITEM_NAME"):rs.getString("RET_LIR_NAME"));
			//Item.setCategoryName(rs.getString("CATEGORY_NAME"));
			
			if(!ItemList.containsKey(Item.getItemCode())) {
				ItemList.put(Item.getItemCode(),Item );	
			}
			
		}
		}
		catch(Exception Ex)
		{
			logger.error("CommonTaskDAO getItemInfo: " + Ex.getMessage());
			
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		
		
		return ItemList;
	}

	public Map<LocalDate, Integer> getCalendarIds(List<LocalDate> dates, Connection conn2) {
		Map<LocalDate, Integer> result = new HashMap();
		StringBuilder Querry = new StringBuilder();
		int counter[]= {0};
		 Querry.append("SELECT RC.CALENDAR_ID, TO_CHAR(RC.START_DATE,'YYYY-MM-DD') AS START_DATE FROM RETAIL_CALENDAR RC WHERE RC.START_DATE IN ( ");
		dates.forEach(k->{
			if(counter[0]!=0) {
			Querry.append(" ,TO_DATE( '"+k.toString()+"','yyyy-MM-dd')");
			}else {
				Querry.append(" TO_DATE( '"+k.toString()+"','yyyy-MM-dd') ");	
			}
			counter[0]++;
		});
		 Querry.append(") AND ROW_TYPE ='W'");
		 DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		 conn = conn2;
			PreparedStatement stmt = null;
			ResultSet rs = null;
			
			
			try {
			stmt = conn.prepareStatement(Querry.toString());
			
		//logger.info(Querry.toString());
			
			rs = stmt.executeQuery();
			
			while(rs.next()){
				
				String date = rs.getString("START_DATE");
				LocalDate WSD = LocalDate.parse(date,dtf);
				if(!result.containsKey(WSD)) {
					result.put(WSD,rs.getInt("CALENDAR_ID") );	
				}
				
			}
			}
			catch(Exception Ex)
			{
				logger.error("CommonTaskDAO getCalendarId: " + Ex.getMessage());
				
			}finally{
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		 
		return result;
	}

	public Map<LocalDate, Integer> getWeekCalId(Connection conn2, String strStartWeekDate, String strEndWeekDate) throws Exception {
		Map<LocalDate, Integer> cid = new HashMap();
		PreparedStatement stmt =null;
		ResultSet rs = null;
		String querry = String.valueOf(Get_Week_Cal_Id);
		
		querry = querry.replace("%START_DATE%", strStartWeekDate);
		 querry = querry.replace("%END_DATE%", strEndWeekDate);
		try {
		 stmt = conn2.prepareStatement(querry);
		 rs = stmt.executeQuery();
		 DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		while(rs.next()) {
		 
			LocalDate WSD = LocalDate.parse(rs.getString("START_DATE"),dtf);
			if(!cid.containsKey(WSD)) {
				cid.put(WSD,rs.getInt("CALENDAR_ID"));
			}
		}
		}catch(Exception Ex)
		{
			logger.error("CommonTaskDAO getWeekCalId: " + Ex.getMessage());
			throw new Exception(Ex.getMessage());
		}
	 
		
		return cid;
	}

	public List<Integer> getStoreIds(Connection con ,String OldWeekStartDate ,int prodId,int prodLvlId,int locId,int locLvlId) throws Exception{
		List<Integer> storeIds = new ArrayList();
		conn = con;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String sql = String.valueOf(Get_Store_ids);
		try {
			sql = sql.replace("%LY_START_WEEK_DATE%", OldWeekStartDate);
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1,prodLvlId);
			stmt.setInt(2, prodId);
			stmt.setInt(3, locLvlId);
			stmt.setInt(4, locId);
			
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				 int a = rs.getInt("STORE_ID");
				if(!storeIds.contains(a)) 
				{
					storeIds.add(a);
				 }
			}
			}catch(Exception Ex) {
			logger.error("CommonTaskDAO getStoreIds: " + Ex.getMessage());
			throw new Exception(Ex.getMessage());
		}
		
		
		return storeIds;
		
	}

	public Map<Long, ItemInfoDTO> getLigInfo(int parseInt, Connection conn2) throws Exception {
		 Map<Long, ItemInfoDTO> LIGInfo = new HashMap();
		 
		    conn = conn2;
			PreparedStatement stmt = null;
			ResultSet rs = null;
			String sql = String.valueOf(Get_Lig_Info_2);
			
		try {
			stmt = conn.prepareStatement(sql);
			
			stmt.setInt(1, parseInt);
			
			rs=stmt.executeQuery();
			
			while(rs.next()) {
				long RLI = rs.getLong("RET_LIR_ID");
				if(!LIGInfo.containsKey(RLI)) {
					ItemInfoDTO I = new ItemInfoDTO();
					I.setRetLIRId(RLI);
					I.setItemCode(rs.getInt("RET_LIR_ID"));
					I.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
					LIGInfo.put(RLI,I);
				}
			}
			
		}catch(Exception Ex ) {
			logger.error("CommonTaskDAO getLigInfo: " + Ex.getMessage());
			throw new Exception(Ex.getMessage());
		}
		
		
		return LIGInfo;
	}
}
