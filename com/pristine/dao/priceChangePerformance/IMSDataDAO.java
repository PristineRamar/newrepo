package com.pristine.dao.priceChangePerformance;

import java.math.BigDecimal;
import java.math.RoundingMode;
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

import com.pristine.dto.priceChangePerformance.IMSDataDTO;
import com.pristine.util.PristineDBUtil;

public class IMSDataDAO {

	private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	private static final String Create_IMS_data_for_one_week ="SELECT ITEM_CODE," + 
			" (REGULAR_MOVEMENT + SALE_MOVEMENT) AS TOTAL_MOVEMENT, " + 
			" (REG_REVENUE + SALE_REVENUE) AS TOTAL_REVENUE, " + 
			" REGULAR_MOVEMENT, SALE_MOVEMENT, " + 
			" REG_REVENUE, SALE_REVENUE FROM ( " +  
			" SELECT ITEM_CODE, (REG_QUANTITY + REG_WEIGHT) AS REGULAR_MOVEMENT, " +   
			" (SALE_QUANTITY + SALE_WEIGHT) AS SALE_MOVEMENT, " +  
			" REG_REVENUE, SALE_REVENUE FROM ( " + 
			" select count(distinct trx_no) TRX_COUNT, MV.ITEM_ID as ITEM_CODE, " + 
			" count(case when MV.SALE_TYPE = 'Y' then 1 else null end) As SALE_FLAG_COUNT, " +  
			" sum(case when MV.SALE_TYPE = 'N'  then MV.QUANTITY else 0 end) as REG_QUANTITY, " +  
			" sum(case when MV.SALE_TYPE = 'Y' then MV.QUANTITY else 0 end) as SALE_QUANTITY, " + 
			" sum(case when MV.SALE_TYPE = 'N' and  MV.WEIGHT<>0  then MV.WEIGHT else 0 end ) as REG_WEIGHT, " + 
			" sum(case when MV.SALE_TYPE = 'Y' and MV.WEIGHT<>0  then MV.WEIGHT else 0 end) as SALE_WEIGHT, " + 
			" sum( case when MV.SALE_TYPE = 'N' then MV.net_amt else 0 end) as REG_REVENUE, " + 
			" sum( case when MV.SALE_TYPE = 'Y' then MV.net_amt else 0 end) as SALE_REVENUE from " + 
			" TRANSACTION_LOG  MV " + 
			" where MV.CALENDAR_ID IN (%CALENDAR_ID%)  " + 
			" and STORE_ID IN (%STORE_ID%) " + 
			" group by MV.ITEM_ID))";
	
	private static final String Get_Day_Cal_Ids_In_Week="SELECT RC.Calendar_Id from retail_calendar RC where RC.row_type='D' and RC.start_date>=To_Date('%WEEK_START_DATE%','yyyy-MM-dd') and start_date<=To_date('%WEEK_END_DATE%','yyyy-MM-dd')";
	
	private static final String Get_IMS_Data = "SELECT "
			+ "IMS.PRODUCT_ID,"
			+ "IMS.REG_PRICE,"
			+ "IMS.REG_M_PRICE,"
			+ "IMS.REG_M_PACK,"
			//+ "IMS.FINAL_PRICE,"
			+ "IMS.LIST_COST,"
			+ "IMS.TOT_REVENUE,"
			+ "IMS.TOT_MOVEMENT,"
			+ "IMS.NET_MARGIN,"
			+ "IMS.CALENDAR_ID, IMS.SALE_FLAG, IMS.SALE_MOVEMENT,"
			//+ "IMS.SALE_PRICE,"
			//+ "IMS.SALE_M_PRICE, "
			+ "TO_CHAR(R.START_DATE,'YYYY-MM-DD') AS START_DATE, "
			+ "TO_CHAR(R.END_DATE,'YYYY-MM-DD') AS END_DATE "
			+ "FROM IMS_WEEKLY_ZONE IMS "
			+ "LEFT JOIN RETAIL_CALENDAR R "
			+ "ON IMS.CALENDAR_ID=R.CALENDAR_ID "
			+ "WHERE IMS.PRODUCT_ID IN (%ITEM_LIST%) "
			+"AND IMS.CALENDAR_ID IN ( SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC"
			+" WHERE (RC.START_DATE >= (TO_DATE( '%START_DATE%','yyyy-MM-dd')) AND"
			+ "  RC.START_DATE <= (TO_DATE( '%END_DATE%','yyyy-MM-dd')) AND ROW_TYPE = 'W')) "
			+ "AND IMS.LOCATION_ID = ? AND IMS.SALE_FLAG='N'";
	
	private static Logger logger = Logger.getLogger("IMSDataDAO");
	private Connection conn = null;
	
	public Map<Integer, Map<LocalDate, IMSDataDTO>> getIMSData(Connection Con, String ICodeString , String LocationId, String StartWeekDate , String EndWeekDate){
			conn =Con;
			
			//List<IMSDataDTO> ResultObj = new ArrayList<IMSDataDTO>();
			Map<Integer,Map<LocalDate,IMSDataDTO>> IMSResultMap = new HashMap();
			PreparedStatement stmt = null;
			ResultSet rs = null;
			BigDecimal bd=null;
			
			try {
				String sql = String.valueOf(Get_IMS_Data);
				sql=sql.replace("%ITEM_LIST%", ICodeString);
				sql = sql.replace("%START_DATE%", StartWeekDate);
				sql = sql.replace("%END_DATE%", EndWeekDate);
				
				stmt = conn.prepareStatement(sql);
			
		//	stmt.setString(1, ICodeString);
			//stmt.setString(2, StartWeekDate);
			//stmt.setString(3, EndWeekDate);
			stmt.setInt(1, Integer.parseInt(LocationId));
			//logger.info("IMS Info Fetch Query:" + sql);
			//logger.debug("Fetching IMS data For " + ICodeString);
			System.out.println("Fetching IMS DATA ");
			stmt.setFetchSize(100000);
			rs = stmt.executeQuery();
			
			while(rs.next()){
			//	double TotalMovement = rs.getDouble("TOT_MOVEMENT");
			//	double SaleMovement = rs.getDouble("SALE_MOVEMENT");
				//if((SaleMovement/TotalMovement)<0.70) {
				IMSDataDTO IMSdata = new IMSDataDTO();
				IMSdata.setProductId(rs.getInt("PRODUCT_ID"));
				IMSdata.setRegularPrice(rs.getFloat("REG_PRICE"));
				IMSdata.setRegular_M_Price(rs.getFloat("REG_M_PRICE"));
				IMSdata.setRegular_M_Pack(rs.getInt("REG_M_PACK"));
				//IMSdata.setFinalprice(rs.getFloat("FINAL_PRICE"));
				IMSdata.setListCost(rs.getFloat("LIST_COST"));
				
				IMSdata.setTotalRevenue(rs.getDouble("TOT_REVENUE"));
				if(rs.wasNull()) {
					IMSdata.setTotalRevenueNull(true);
				}else {
					IMSdata.setTotalRevenueNull(false);
				}
				IMSdata.setTotalMovement(rs.getDouble("TOT_MOVEMENT"));
				if(rs.wasNull()) {
					IMSdata.setTotalMovementNull(true);
				}else {
					IMSdata.setTotalMovementNull(false);
				}
				IMSdata.setNetMargin(rs.getDouble("NET_MARGIN"));
				IMSdata.setCalendarId(rs.getInt("CALENDAR_ID"));
				//IMSdata.setSalePrice(rs.getFloat("SALE_PRICE"));
				//IMSdata.setSale_M_Price(rs.getFloat("SALE_M_PRICE"));
				//IMSdata.setSale_M_Pack(rs.getInt("SALE_M_PACK"));
				
				String date = rs.getString("START_DATE");
				//date = date.substring(0, date.indexOf(" "));
				IMSdata.setWeekStartDate(LocalDate.parse(date,dtf));
				date=rs.getString("END_DATE");
				IMSdata.setWeekEndDate(LocalDate.parse(date,dtf));
			 
				if(IMSdata.getRegular_M_Pack()>1) {
					IMSdata.setRegularPrice((IMSdata.getRegular_M_Price()/IMSdata.getRegular_M_Pack()));
				}
				
				/*if(IMSdata.getSale_M_Pack()>1) {
					IMSdata.setSalePrice(IMSdata.getSale_M_Price()/IMSdata.getSale_M_Pack());
				}*/
				
				 bd = new BigDecimal(IMSdata.getRegularPrice()).setScale(2, RoundingMode.HALF_UP);
					IMSdata.setRegularPrice(bd.floatValue());
					
					bd = new BigDecimal(IMSdata.getListCost()).setScale(2,RoundingMode.HALF_UP);
					IMSdata.setListCost(bd.floatValue());
				
					int Id =IMSdata.getProductId();
					LocalDate WSD = IMSdata.getWeekStartDate();
					if(!IMSResultMap.containsKey(Id)) {
						Map<LocalDate,IMSDataDTO> I = new HashMap();
						I.put(WSD, IMSdata);
						IMSResultMap.put(Id, I);
							
					}else {
						if(!IMSResultMap.get(Id).containsKey(WSD)) {
							IMSResultMap.get(Id).put(WSD,IMSdata);
						}	
					//}
				}
				
				
			//	ResultObj.add(IMSdata);
				
				
			}
			
			logger.info("Fetching IMS data Complete " );
			System.out.println("Fetching IMS DATA Complete");
			
			}
			catch(Exception Ex)
			{
				logger.error("IMSDataDAO getIMSData: " + Ex.getMessage());
				
			}finally{
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		
		
		return IMSResultMap;
		
	} 
    
	
	public Map<LocalDate , Map<Integer, IMSDataDTO>> createIMSData(Connection Con, String StoreIds, LocalDate StartWeekDate, LocalDate EndWeekDate) throws Exception{
		Map<LocalDate , Map<Integer, IMSDataDTO>> IMSData = new HashMap();
		LocalDate CurrentWeekStartDate = LocalDate.parse(dtf.format(StartWeekDate),dtf);
		LocalDate CurrentWeekEndDate ;
		conn = Con;
		
		int c = 0;
		try {
		while(EndWeekDate.compareTo(CurrentWeekStartDate)==0) {
			if(c==0) {
				CurrentWeekEndDate = CurrentWeekStartDate.plusDays(6);
			}else {
				CurrentWeekStartDate = CurrentWeekStartDate.plusDays(7);
				CurrentWeekEndDate = CurrentWeekStartDate.plusDays(6);
			}
			
			String CalIdsQuerry = String.valueOf(Get_Day_Cal_Ids_In_Week);
			CalIdsQuerry = CalIdsQuerry.replace("%WEEK_START_DATE%", dtf.format(CurrentWeekStartDate));
			CalIdsQuerry = CalIdsQuerry.replace("%WEEK_END_DATE%", dtf.format(CurrentWeekEndDate));
			
			String IMS_QUERRY = String.valueOf(Create_IMS_data_for_one_week);
			IMS_QUERRY = IMS_QUERRY.replace("%CALENDAR_ID%", CalIdsQuerry);
			IMS_QUERRY = IMS_QUERRY.replace("%STORE_ID%",StoreIds);
			
			PreparedStatement stmt = conn.prepareStatement(IMS_QUERRY);
			ResultSet rs= null;
			stmt.setFetchSize(10000);
			rs = stmt.executeQuery();
			
			while(rs.next()) {
				
				IMSDataDTO I = new IMSDataDTO();
				I.setProductId(rs.getInt("ITEM_CODE"));
				
			}
			
			c++;
		}
		}catch(Exception Ex) {
			logger.error("IMSDataDAO createIMSData: " + Ex.getMessage());
			throw new Exception(Ex.getMessage());
			
		}
		logger.info("IMS DATA generated for "+c+" Weeks");
		return IMSData;
		}
	}
