package com.pristine.dao.priceChangePerformance;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dto.priceChangePerformance.IMSDataDTO;
import com.pristine.dto.priceChangePerformance.ItemInfoDTO;
import com.pristine.dto.priceChangePerformance.PriceDataDTO;
import com.pristine.dto.priceChangePerformance.TlogDataDTO;
import com.pristine.util.PristineDBUtil;
import com.sun.glass.ui.Pixels.Format;

import sun.text.resources.cldr.FormatData;

public class TlogDataDAO {
	
	private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	private static final String Get_Tlog_Data = "SELECT "
			+ "TL.CALENDAR_ID, "
			//+ "TL.STORE_ID, "
			+ "TL.ITEM_ID, "
			+ "TL.UNIT_PRICE, "
			+ "TL.QUANTITY, "
			//+ "TL.WEIGHT, "
			+ "TL.NET_AMT, "
			//+ "TL.SALE_TYPE, "
			+ "TL.CUSTOMER_ID, "
			+ "TL.REGULAR_AMT, "
			+ "TO_CHAR(R.START_DATE,'YYYY-MM-DD') AS START_DATE "
			+ "FROM TRANSACTION_LOG TL LEFT JOIN RETAIL_CALENDAR R ON R.CALENDAR_ID = TL.CALENDAR_ID"
			+ " WHERE TL.ITEM_ID IN (%ITEM_CODES_LIST%) "
			+ "AND TL.CALENDAR_ID "
			+ "IN ( SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC "
			+ "WHERE  (RC.START_DATE >= TO_DATE('%START_DATE%','YYYY-MM-DD') AND RC.START_DATE <= TO_DATE('%END_DATE%','YYYY-MM-DD') AND ROW_TYPE = 'D'))"
			+ " AND TL.STORE_ID IN (select comp_str_id STORE_ID from competitor_store where comp_str_id in "
			+ "( SELECT DISTINCT STORE_ID FROM PR_PRODUCT_LOCATION_STORE WHERE PRODUCT_LOCATION_MAPPING_ID IN "
			+ "( SELECT PRODUCT_LOCATION_MAPPING_ID FROM PR_PRODUCT_LOCATION_MAPPING WHERE PRODUCT_LEVEL_ID = ? AND PRODUCT_ID in (?) AND LOCATION_LEVEL_ID = ? AND LOCATION_ID IN (%LOCATION_ID%))) "
			+ "and excl_forecast IS NULL and CLOSE_DATE IS NULL)";
	
	private static Logger logger = Logger.getLogger("TlogDataDAO");
	private Connection conn = null;
	
	public List<TlogDataDTO> GetTlogData(Connection con,String FunctionType, String ItemList ,String CatId,String ProdLvlId ,String LocId, String LocLvlId,String StartDayDate, String EndDayDate, Map<Integer, PriceDataDTO> priceData,Map<Integer, ItemInfoDTO> iteminfo, Map<Integer, Map<LocalDate, IMSDataDTO>> IMSData){
		
		conn=con;
		List<TlogDataDTO> ResultObj = new ArrayList();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		
		try {
			String sql =String.valueOf(Get_Tlog_Data);
			
			sql = sql.replace("%ITEM_CODES_LIST%", ItemList);
			sql = sql.replace("%START_DATE%", StartDayDate);
			sql = sql.replace("%END_DATE%", EndDayDate);
			sql = sql.replace("%LOCATION_ID%", LocId);
		//	logger.info("Fetching Price data For " + sql);	
		stmt = conn.prepareStatement(sql);
		
		//stmt.setString(1, ItemList);
		//stmt.setString(2, StartDayDate);
		//stmt.setString(3, EndDayDate);
		stmt.setInt(1, Integer.parseInt(ProdLvlId));
		stmt.setInt(2, Integer.parseInt(CatId));
		stmt.setInt(3, Integer.parseInt(LocLvlId));
	//	logger.debug("Tlog Info Fetch Query:" + sql);
	//	logger.debug("Fetching TLOG data For " + ItemList);
		System.out.println("Fetching Tlog DATA ");
		stmt.setFetchSize(200000);
		rs = stmt.executeQuery();
		
		//Index
		//Map<Integer,Map<LocalDate,Integer>> IMSIndex
		
		
		
		
		while(rs.next()){
			if((rs.getFloat("UNIT_PRICE")>0)) {
				TlogDataDTO tlog = new TlogDataDTO();
				
				tlog.setDayCalendarId(rs.getInt("CALENDAR_ID"));
				tlog.setCustomerId(rs.getLong("CUSTOMER_ID"));
			
			//	tlog.setStoreId(rs.getInt("STORE_ID"));
				tlog.setProductId(rs.getInt("ITEM_ID"));
				tlog.setUnitPrice(rs.getFloat("UNIT_PRICE"));
				tlog.setQuantity(rs.getInt("QUANTITY"));
				//tlog.setWeight(rs.getFloat("WEIGHT"));
				tlog.setNetAmount(rs.getFloat("NET_AMT"));
				//tlog.setSaleType(rs.getString("SALE_TYPE"));
				tlog.setRegAmount(rs.getFloat("REGULAR_AMT"));
				String date = rs.getString("START_DATE");
				tlog.setDayStartDate(LocalDate.parse(date,dtf));
				if(!(tlog.getDayStartDate().getDayOfWeek()==DayOfWeek.SUNDAY)) {
					tlog.setWeekStartDate(tlog.getDayStartDate().with(DayOfWeek.MONDAY).minusDays(1));
				}else {
					tlog.setWeekStartDate(tlog.getDayStartDate());
				}
				
				
				
				tlog.setCustomerType("OTHER");
			
				tlog.setNetRegAmt(tlog.getRegAmount()/tlog.getQuantity());
				tlog.setDiscount(((tlog.getNetRegAmt()-tlog.getUnitPrice())/tlog.getNetRegAmt())*100);
					
				BigDecimal bd = new BigDecimal(tlog.getUnitPrice()).setScale(4, RoundingMode.HALF_UP);
				tlog.setUnitPrice( bd.floatValue());
				
				bd = new BigDecimal(tlog.getNetRegAmt()).setScale(4, RoundingMode.HALF_UP);
				tlog.setNetRegAmt(bd.floatValue());
				
				bd = new BigDecimal(tlog.getDiscount()).setScale(4,RoundingMode.HALF_UP);
				tlog.setDiscount(bd.floatValue());
				float d = tlog.getDiscount();
				
				if(tlog.getCustomerId()!=0) {
				if(d>=19.5 && d<=20.5) {
					tlog.setCustomerType("GOLD");
				}else if(d>=9.5 && d<=10.5) {
					tlog.setCustomerType("SILVER");
				}else if(tlog.getUnitPrice()==tlog.getNetRegAmt()) {
					tlog.setCustomerType("REGULAR");
				}
				}else {
					tlog.setCustomerType("NOCARD");
				}
			
				//Approach 3
				int Id = tlog.getProductId();
				LocalDate Dt = tlog.getWeekStartDate();
				
				
				if(iteminfo.containsKey(Id)) {
					//int ind1 = IndexItemInfo.get(Id);
					ItemInfoDTO ItI =iteminfo.get(Id);
					
					tlog.setRetailerItemCode(ItI.getRetailerItemCode());
					tlog.setRetLIRName(ItI.getRetLIRName());
					}
				
				
			
				
				
				if(FunctionType == "OLD") {
					//merging Price Data
					if(priceData.containsKey(Id)) {
						
						//int ind3 = IndexPriceData.get(Id);
						PriceDataDTO PDT = priceData.get(Id);
						tlog.setLY_RegularPrice(PDT.getLY_RegularPrice());
						tlog.setLY_ListCost(PDT.getLY_ListCost());
					//	tlog.setListCostNull(PDT.isListCostNull());
						//tlog.setRegularPriceNull(PDT.isRegularPriceNull());
					}
				}
			
				if(IMSData.containsKey(Id)) {
					if(IMSData.get(Id).containsKey(Dt)) {
					//int ind = IndexIMS.get(Id).get(Dt);
					IMSDataDTO IDT = IMSData.get(Id).get(Dt);
					tlog.setRegularPrice(IDT.getRegularPrice());
					tlog.setRegular_M_Price(IDT.getRegular_M_Price());
					tlog.setRegular_M_Pack(IDT.getRegular_M_Pack());
					//tlog.setFinalprice(IDT.getFinalprice());
					tlog.setListCost(IDT.getListCost());
					tlog.setTotalRevenue(IDT.getTotalRevenue());
					tlog.setTotalMovement(IDT.getTotalMovement());
					tlog.setNetMargin(IDT.getNetMargin());
					tlog.setTotalMovementNull(IDT.isTotalMovementNull());
					tlog.setTotalRevenueNull(IDT.isTotalRevenueNull());
					tlog.setWeekCalendarId(IDT.getCalendarId());
					//tlog.setCalendarId(IDT.getCalendarId());
					//tlog.setSalePrice(IDT.getSalePrice());
					//tlog.setSale_M_Price(IDT.getSale_M_Price());
					//tlog.setSale_M_Pack(IDT.getSale_M_Pack());
					//tlog.setWeekEndDate(IDT.getWeekEndDate());
					tlog.setProductLevelId(1);
					tlog.setLigMember(false);
					if(FunctionType == "OLD"&&tlog.getLY_RegularPrice()!=0) {
					ResultObj.add(tlog);
					}else if(FunctionType == "NEW"){
						ResultObj.add(tlog);
					}
					}
					
				}
				
				
			}
						
		}

		
		logger.info("Fetching Tlog data Complete " );
		System.out.println("Fetching Tlog DATA Complete");
		}
		catch(Exception Ex)
		{
			logger.error("TlogDataDAO getTlogData: " + Ex.getMessage() + Ex.getStackTrace().toString());
			
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		System.out.println("TlogRows found : "+ ResultObj.size());

		return ResultObj;
		
		
	}

}
