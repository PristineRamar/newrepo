package com.pristine.dao.priceChangePerformance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dto.priceChangePerformance.CatContriSummaryBO;
import com.pristine.dto.priceChangePerformance.CatForecastAccBO;
import com.pristine.dto.priceChangePerformance.FinalDataDTO;
import com.pristine.dto.priceChangePerformance.ItemInfoDTO;
import com.pristine.dto.priceChangePerformance.TlogDataDTO;
import com.pristine.dto.priceChangePerformance.WeekGraphDataDTO;
import com.pristine.util.PristineDBUtil;

public class ReportDataInsertDAO {
	private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	private static final String insert_Item_Level_Trend_Data_Old="INSERT INTO PS_PERFORMANCE_DETAIL(PERFORMANCE_DETAIL_ID,RUN_ID,CAL_TYPE,DATA_TYPE,START_CAL_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,REG_PRICE_LY,LIST_COST_LY,AVG_UNITS_LY,AVG_SALES_LY,AVG_MARGIN_LY,TIER_1_VISIT_PCT_LY,TIER_1_UNITS_LY,TIER_1_SALES_LY,TIER_1_MARGIN_LY,TIER_2_VISIT_PCT_LY,TIER_2_UNITS_LY,TIER_2_SALES_LY,TIER_2_MARGIN_LY,TIER_3_VISIT_PCT_LY,TIER_3_UNITS_LY,TIER_3_SALES_LY,TIER_3_MARGIN_LY,SALE_VISIT_PCT_LY,NO_CARD_VISIT_PCT_LY) " + 
			"VALUES(PS_PERFORMANCE_DETAIL_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private static final String insert_Item_Level_Trend_Data_New="INSERT INTO PS_PERFORMANCE_DETAIL(PERFORMANCE_DETAIL_ID,RUN_ID,CAL_TYPE,DATA_TYPE,START_CAL_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,REG_PRICE,LIST_COST,AVG_UNITS,AVG_SALES,AVG_MARGIN,TIER_1_VISIT_PCT,TIER_1_UNITS,TIER_1_SALES,TIER_1_MARGIN,TIER_2_VISIT_PCT,TIER_2_UNITS,TIER_2_SALES,TIER_2_MARGIN,TIER_3_VISIT_PCT,TIER_3_UNITS,TIER_3_SALES,TIER_3_MARGIN,SALE_VISIT_PCT,PREDICTION,NO_CARD_VISIT_PCT,PREDICTED_SALES,PREDICTED_MARGIN) " + 
			"VALUES(PS_PERFORMANCE_DETAIL_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private static final String insert_Final_Report_Data="INSERT INTO PS_PERFORMANCE_DETAIL(PERFORMANCE_DETAIL_ID,RUN_ID,CAL_TYPE,DATA_TYPE,PRICE_CHANGE_TYPE,START_CAL_ID,END_CAL_ID,PRODUCT_LEVEL_ID,PRODUCT_ID,"
			+ "REG_PRICE_LY,REG_PRICE,REG_PRICE_DIFF,LIST_COST_LY,LIST_COST,"
			+ "AVG_UNITS_LY,AVG_UNITS,AVG_UNIT_DIFF,AVG_SALES_LY,AVG_SALES,AVG_SALES_DIFF,AVG_MARGIN_LY,AVG_MARGIN,AVG_MARGIN_DIFF,"
			+ "TIER_1_VISIT_PCT_LY,TIER_1_VISIT_PCT,TIER_1_UNITS,TIER_1_UNITS_LY,TIER_1_UNITS_DIFF,TIER_1_SALES,TIER_1_SALES_LY,TIER_1_SALES_DIFF,TIER_1_MARGIN,TIER_1_MARGIN_LY,TIER_1_MARGIN_DIFF,"
			+ "TIER_2_VISIT_PCT_LY,TIER_2_VISIT_PCT,TIER_2_UNITS,TIER_2_UNITS_LY,TIER_2_UNITS_DIFF,TIER_2_SALES,TIER_2_SALES_LY,TIER_2_SALES_DIFF,TIER_2_MARGIN,TIER_2_MARGIN_LY,TIER_2_MARGIN_DIFF,"
			+ "TIER_3_VISIT_PCT_LY,TIER_3_VISIT_PCT,TIER_3_UNITS,TIER_3_UNITS_LY,TIER_3_UNITS_DIFF,TIER_3_SALES,TIER_3_SALES_LY,TIER_3_SALES_DIFF,TIER_3_MARGIN,TIER_3_MARGIN_LY,TIER_3_MARGIN_DIFF,"
			+ "SALE_VISIT_PCT,SALE_VISIT_PCT_LY,"
			+ "PREDICTION,PRED_ACC_PCT,NO_CARD_VISIT_PCT,NO_CARD_VISIT_PCT_LY,PREDICTED_SALES,PRED_SALE_ACC_PCT,PREDICTED_MARGIN,PRED_MARGIN_ACC_PCT) " + 
			"VALUES(PS_PERFORMANCE_DETAIL_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private static final String insert_Week_Graph_Data ="INSERT INTO PS_PERFORMANCE_SUMMARY(PERFORMANCE_SUMMARY_ID,RUN_ID,CAL_TYPE,CAL_FORECAST_TYPE,REPORT_TYPE,START_CAL_ID,NO_OF_ITEMS,TOT_UNITS,TOT_UNITS_LY,TOT_UNITS_PRED,TOT_SALES,TOT_SALES_LY,TOT_SALES_PRED,TOT_MARGIN,TOT_MARGIN_LY,TOT_MARGIN_PRED) "
			+"VALUES (PS_PERFORMANCE_SUMMARY_SEQ.NEXTVAL,?,'W',?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private static final String Insert_Category_Contribution_Info="INSERT INTO PS_PERFORMANCE_SUMMARY(RUN_ID,CAL_TYPE,CAL_FORECAST_TYPE,REPORT_TYPE,START_CAL_ID,END_CAL_ID,AVG_UNITS,AVG_UNITS_LY,	AVG_UNITS_PRED,	AVG_SALES,AVG_SALES_LY,AVG_SALES_PRED,AVG_MARGIN,AVG_MARGIN_LY,AVG_MARGIN_PRED)	VALUES(" + 
			"?,'A','P',?,?,?,?,?,?,?,?,?,?,?,?)";

	private static final String Create_New_RunId="INSERT INTO PS_PERFORMANCE_HEADER(RUN_ID,LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID, START_CALENDAR_ID, END_CALENDAR_ID, PRICE_START_CALENDAR_ID, PRICE_END_CALENDAR_ID, ACTIVE, CREATED) "
												+"VALUES(PS_PERFORMANCE_HEADER_RUN_SEQ.NEXTVAL,?,?,?,?,(%START_CALENDAR_ID%),(%END_CALENDAR_ID%),(%PRICE_START_CALENDAR_ID%),(%PRICE_END_CALENDAR_ID%),'Y',TO_DATE('%CREATED_DATE%','yyyy-MM-dd'))";
	
	private static final String Mark_RunId_Inactive="UPDATE PS_PERFORMANCE_HEADER " + 
			"SET ACTIVE = 'N' " + 
			"WHERE RUN_ID IN (%RUN_ID%)";
			
	private static final String Insert_Category_FORECAST_Acc="INSERT INTO PS_PERFORMANCE_SUMMARY(PERFORMANCE_SUMMARY_ID,RUN_ID,CAL_TYPE,CAL_FORECAST_TYPE,REPORT_TYPE,START_CAL_ID,END_CAL_ID,TOT_UNITS,TOT_UNITS_PRED,TOT_SALES,TOT_SALES_PRED,TOT_MARGIN,TOT_MARGIN_PRED,AVG_UNITS,AVG_UNITS_LY,	AVG_UNITS_PRED,	AVG_SALES,AVG_SALES_LY,AVG_SALES_PRED,AVG_MARGIN,AVG_MARGIN_LY,AVG_MARGIN_PRED)"+
																" VALUES(PS_PERFORMANCE_SUMMARY_SEQ.NEXTVAL,?,'A','P',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String Get_ACTIVE_RunID_Data = "SELECT "
			+ "* "
			+ "FROM PS_PERFORMANCE_HEADER "
			+ "WHERE PRODUCT_ID = ? AND PRODUCT_LEVEL_ID = ? "
			+ " AND LOCATION_ID = ? AND LOCATION_LEVEL_ID = ? "
			+ "AND START_CALENDAR_ID IN "
			+ "( SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC WHERE (RC.START_DATE = (TO_DATE( '%START_DATE%','yyyy-MM-dd')) AND ROW_TYPE = 'W')) "
			+ "AND END_CALENDAR_ID IN "
			+ "(SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC WHERE (RC.START_DATE = (TO_DATE( '%END_DATE%','yyyy-MM-dd')) AND ROW_TYPE = 'W')) "
			+ "AND PRICE_START_CALENDAR_ID IN "
			+ "( SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC WHERE (RC.START_DATE = (TO_DATE( '%PRICE_START_DATE%','yyyy-MM-dd')) AND ROW_TYPE = 'W')) "
			+ "AND PRICE_END_CALENDAR_ID IN "
			+ "(SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC WHERE (RC.START_DATE = (TO_DATE( '%PRICE_END_DATE%','yyyy-MM-dd')) AND ROW_TYPE = 'W')) "
			+"AND ACTIVE = 'Y'";
	
	private static Logger logger = Logger.getLogger("ReportDataInsertDAO");
	public int createNewRunId(Connection conn, String productId, String productLevelId, String locationId,
			String locationLevelId, String strStartWeekDate, String strEndWeekDate, String strPriceStartWeekDate,
			String strPriceEndWeekDate) throws Exception {
	int RunId = 0;
	//check if any RunId is active for the current Combination
	String querry = String.valueOf(Get_ACTIVE_RunID_Data);
	
	PreparedStatement stmt = null;
	ResultSet rs = null;
	try {
		querry = querry.replace("%START_DATE%",strStartWeekDate);
		querry = querry.replace("%END_DATE%",strEndWeekDate);
		querry = querry.replace("%PRICE_START_DATE%",strPriceStartWeekDate);
		querry = querry.replace("%PRICE_END_DATE%",strPriceEndWeekDate);
		stmt = conn.prepareStatement(querry);
		
		stmt.setInt(1, Integer.parseInt(productId));
		stmt.setInt(2, Integer.parseInt(productLevelId));
		stmt.setInt(3, Integer.parseInt(locationId));
		stmt.setInt(4, Integer.parseInt(locationLevelId));
		
		System.out.println("Checking For Active Run Id ");
	
		rs = stmt.executeQuery();
		String ActiveRunId = "";
		int count = 0;
		while(rs.next()){
				
				if(count==0) {
					ActiveRunId = rs.getString("RUN_ID");
				}else {
					ActiveRunId = ActiveRunId + ", "+rs.getString("RUN_ID");
				}
				count++;
			
		}
		rs.close();
		stmt.close();
		
		if(count!=0) {
		System.out.println("Found Active Run Ids : "+ActiveRunId);
		//marking the active RunId As inactive
		String Update = String.valueOf(Mark_RunId_Inactive);
		
		Update=Update.replace("%RUN_ID%", ActiveRunId);
		
		stmt = conn.prepareStatement(Update);
		int i = stmt.executeUpdate();
		if(i>0) {
			conn.commit();
			logger.info("Successfully Marked RunIds Inactive.");
		}else {
			logger.error("Failed to mark RunIds InActive");
			conn.rollback();
			throw new Exception("Failed to mark RunIds InActive");
			
		}
		stmt.close();
		
		}
		else {
			System.out.println("No Active RunId Found ");
		}
		
		
		String InsertQuerry = String.valueOf(Create_New_RunId);
		InsertQuerry=InsertQuerry.replace("%START_CALENDAR_ID%", "SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC WHERE (RC.START_DATE = (TO_DATE( '"+strStartWeekDate+"','yyyy-MM-dd')) AND ROW_TYPE = 'W')");
		InsertQuerry=InsertQuerry.replace("%END_CALENDAR_ID%", "SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC WHERE (RC.START_DATE = (TO_DATE( '"+strEndWeekDate+"','yyyy-MM-dd')) AND ROW_TYPE = 'W')");
		InsertQuerry=InsertQuerry.replace("%PRICE_START_CALENDAR_ID%", "SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC WHERE (RC.START_DATE = (TO_DATE( '"+strPriceStartWeekDate+"','yyyy-MM-dd')) AND ROW_TYPE = 'W')");
		InsertQuerry=InsertQuerry.replace("%PRICE_END_CALENDAR_ID%", "SELECT RC.CALENDAR_ID FROM RETAIL_CALENDAR RC WHERE (RC.START_DATE = (TO_DATE( '"+strPriceEndWeekDate+"','yyyy-MM-dd')) AND ROW_TYPE = 'W')");
		String CurrDate = LocalDate.now().format(dtf);
		InsertQuerry=InsertQuerry.replace("%CREATED_DATE%", CurrDate);
		//LOCATION_LEVEL_ID, LOCATION_ID, PRODUCT_LEVEL_ID, PRODUCT_ID
		stmt=conn.prepareStatement(InsertQuerry);
		stmt.setInt(1, Integer.parseInt(locationLevelId));
		stmt.setInt(2, Integer.parseInt(locationId));
		stmt.setInt(3, Integer.parseInt(productLevelId));
		stmt.setInt(4, Integer.parseInt(productId));
		logger.info("Querry: " +InsertQuerry);
		int i = stmt.executeUpdate();
		
		if(i>0) {
			conn.commit();
			stmt.close();
			logger.info("Successfully Created RunId.");
			stmt = conn.prepareStatement(querry);
			stmt.setInt(1, Integer.parseInt(productId));
			stmt.setInt(2, Integer.parseInt(productLevelId));
			stmt.setInt(3, Integer.parseInt(locationId));
			stmt.setInt(4, Integer.parseInt(locationLevelId));
			rs = stmt.executeQuery();
			while(rs.next()) {
				RunId =rs.getInt("RUN_ID");
			}
		}else {
			conn.rollback();
			logger.error("Failed to create RunId");
			throw new Exception("Failed to create new RunId");
		}
		
		
	}catch(Exception Ex) {
		conn.rollback();
		logger.error("ReportDataInsertDAO createRunId: " + Ex.getMessage() + Ex.getStackTrace().toString());
		throw new Exception(Ex.getMessage());
	}finally{
		PristineDBUtil.close(rs);
		PristineDBUtil.close(stmt);
	}
	
	return RunId;
	}
	public void InsertCategoryContribution(Connection conn, CatContriSummaryBO allSummary,
			CatContriSummaryBO increasedSummary, CatContriSummaryBO decreasedSummary, int runId, int StartWeekId, int EndWeekId) throws Exception {
		
		String q1 = String.valueOf(Insert_Category_Contribution_Info);
		String q2 = String.valueOf(Insert_Category_Contribution_Info);
		String q3 = String.valueOf(Insert_Category_Contribution_Info);
		
		try {
			PreparedStatement stmt = conn.prepareStatement(q1);
			stmt.setInt(1, runId);
			stmt.setString(2,"C");
			stmt.setInt(3,StartWeekId);
			stmt.setInt(4, EndWeekId);
			stmt.setDouble(5, allSummary.getNewWeekAverageUnits());
			stmt.setDouble(6, allSummary.getLyWeekAverageUnits());
			stmt.setDouble(7,allSummary.getPredAverageUnits());
			stmt.setDouble(8, allSummary.getNewWeekAverageSales());
			stmt.setDouble(9, allSummary.getLyWeekAverageSales());
			stmt.setDouble(10,allSummary.getPredAverageSales());
			stmt.setDouble(11, allSummary.getNewWeekAverageMargin());
			stmt.setDouble(12, allSummary.getLyWeekAverageMargin());
			stmt.setDouble(13,allSummary.getPredAverageMargin());
			stmt.executeUpdate();
			

			stmt = conn.prepareStatement(q2);
			stmt.setInt(1, runId);
			stmt.setString(2,"I");
			stmt.setInt(3,StartWeekId);
			stmt.setInt(4, EndWeekId);
			stmt.setDouble(5, increasedSummary.getNewWeekAverageUnits());
			stmt.setDouble(6, increasedSummary.getLyWeekAverageUnits());
			stmt.setDouble(7,increasedSummary.getPredAverageUnits());
			stmt.setDouble(8, increasedSummary.getNewWeekAverageSales());
			stmt.setDouble(9, increasedSummary.getLyWeekAverageSales());
			stmt.setDouble(10,increasedSummary.getPredAverageSales());
			stmt.setDouble(11, increasedSummary.getNewWeekAverageMargin());
			stmt.setDouble(12, increasedSummary.getLyWeekAverageMargin());
			stmt.setDouble(13,increasedSummary.getPredAverageMargin());
			stmt.executeUpdate();
			
			stmt = conn.prepareStatement(q3);
			stmt.setInt(1, runId);
			stmt.setString(2,"D");
			stmt.setInt(3,StartWeekId);
			stmt.setInt(4, EndWeekId);
			stmt.setDouble(5, decreasedSummary.getNewWeekAverageUnits());
			stmt.setDouble(6, decreasedSummary.getLyWeekAverageUnits());
			stmt.setDouble(7,decreasedSummary.getPredAverageUnits());
			stmt.setDouble(8, decreasedSummary.getNewWeekAverageSales());
			stmt.setDouble(9, decreasedSummary.getLyWeekAverageSales());
			stmt.setDouble(10,decreasedSummary.getPredAverageSales());
			stmt.setDouble(11, decreasedSummary.getNewWeekAverageMargin());
			stmt.setDouble(12, decreasedSummary.getLyWeekAverageMargin());
			stmt.setDouble(13,decreasedSummary.getPredAverageMargin());
			stmt.executeUpdate();
			
			conn.commit();
		}catch(Exception Ex) {
		conn.rollback();
		logger.error("ReportDataInsertDAO InsertCategoryContribution: " + Ex.getMessage() + Ex.getStackTrace().toString());
		throw new Exception(Ex.getMessage());
	}
				
	}
	public void InsertWeekGraphData(Connection conn,String PriceChangeType, Map<Integer, WeekGraphDataDTO> d, int RunId) throws Exception {
		
		String q1 = String.valueOf(insert_Week_Graph_Data);
		try {
			int status[] = {0};
			String Message[]= {""};
			d.forEach((k,v)->{
				if(status[0]==0) {
				try {
				PreparedStatement stmt = conn.prepareStatement(q1);
				stmt.setInt(1, RunId);
				if(v.getIsFutureWeek()==0) {
					stmt.setString(2,"P");
					
					stmt.setDouble(6,v.getNewMovement());
					stmt.setDouble(7,v.getOldMovement());
					
					stmt.setDouble(9,v.getNewRevenue());
					stmt.setDouble(10,v.getOldRevenue());
					
					stmt.setDouble(12,v.getNewMargin());
					stmt.setDouble(13,v.getOldMargin());
					
					
				}else {
					stmt.setString(2, "F");
					stmt.setDouble(6,0);
					stmt.setDouble(7,0);
					
					stmt.setDouble(9,0);
					stmt.setDouble(10,0);
					
					stmt.setDouble(12,0);
					stmt.setDouble(13,0);
				}
				stmt.setString(3,PriceChangeType);
				stmt.setInt(4, v.getWeekCalendarID());
				stmt.setInt(5,v.getRetailerItemCodes().size());
				stmt.setDouble(8,v.getPrediction_Movement());
				stmt.setDouble(11,v.getPrediction_Sales());
				stmt.setDouble(14,v.getPrediction_Margin());
				stmt.executeUpdate();
			}catch(Exception Ex) {
				status[0]=-1;
				Message[0]=Ex.getMessage();
			}}
			});
			if(status[0]==-1) {
				conn.rollback();
				throw new Exception(Message[0]);
			}
			conn.commit();
		}catch(Exception Ex) {
		conn.rollback();
		logger.error("ReportDataInsertDAO InsertWeekGraphData: " + Ex.getMessage() + Ex.getStackTrace().toString());
		throw new Exception(Ex.getMessage());
	}
	}
	public void InsertCatForecastSummary(Connection conn,String PriceChangeType, CatForecastAccBO d, CatContriSummaryBO ContriSummary, int runId, int StartCalId,
			int EndCalId) throws Exception {
	
		String q1 = String.valueOf(Insert_Category_FORECAST_Acc);
		
		try {
			PreparedStatement stmt = conn.prepareStatement(q1);
			stmt.setInt(1, runId);
			stmt.setString(2,PriceChangeType);
			stmt.setInt(3,StartCalId);
			stmt.setInt(4, EndCalId);
			stmt.setDouble(5, d.getUnitActual());
			stmt.setDouble(6, d.getUnitForecast());
			stmt.setDouble(7,d.getSalesActual());
			stmt.setDouble(8, d.getSalesForecast());
			stmt.setDouble(9, d.getMarginActual());
			stmt.setDouble(10,d.getMarginForecast());
			stmt.setDouble(11, ContriSummary.getNewWeekAverageUnits());
			stmt.setDouble(12, ContriSummary.getLyWeekAverageUnits());
			stmt.setDouble(13,ContriSummary.getPredAverageUnits());
			stmt.setDouble(14, ContriSummary.getNewWeekAverageSales());
			stmt.setDouble(15, ContriSummary.getLyWeekAverageSales());
			stmt.setDouble(16,ContriSummary.getPredAverageSales());
			stmt.setDouble(17, ContriSummary.getNewWeekAverageMargin());
			stmt.setDouble(18, ContriSummary.getLyWeekAverageMargin());
			stmt.setDouble(19,ContriSummary.getPredAverageMargin());
			stmt.executeUpdate();

			
			conn.commit();
		}catch(Exception Ex) {
		conn.rollback();
		logger.error("ReportDataInsertDAO InsertCategoryForecastSummary: " + Ex.getMessage() + Ex.getStackTrace().toString());
		throw new Exception(Ex.getMessage());
	}
		
	}
	public void InsertFinalReportData(Connection conn, int runId, int StartCalID, int EndCalId,
			List<FinalDataDTO> finalData, Map<Long, ItemInfoDTO> ligInfo) throws Exception {
		

		String q1 = String.valueOf(insert_Final_Report_Data);
		
		try {
			PreparedStatement stmt = conn.prepareStatement(q1);
			for(FinalDataDTO f: finalData) {
				stmt.setInt(1, runId);
				stmt.setString(2, "A");
				if(f.isOldDataPresent()&&f.isNewDataPresent()) {
					if(f.isToConsiderRecord()) {
						stmt.setString(3, "A");
					}else {
						stmt.setString(3, "ANC");
					}
				}else {
					if(f.isNewDataPresent()) {
						stmt.setString(3, "C");
					}else if(f.isOldDataPresent()) {
						stmt.setString(3, "L");
					}else {
						stmt.setString(3, "N");
					}
				}
				
				stmt.setString(4,(f.getPriceChangeIndicator()==1?"N":(f.getPriceChangeIndicator()==2?"I":(f.getPriceChangeIndicator()==3?"D":"N"))));
				stmt.setInt(5, StartCalID);
				stmt.setInt(6, EndCalId);
				stmt.setInt(7, f.getProductLevelId());
				stmt.setInt(8, f.getProductId());				
				if(f.isOldDataPresent()) {
				stmt.setFloat(9, f.getOldData().getLY_RegularPrice());
				stmt.setFloat(12, f.getOldData().getLY_ListCost());
				stmt.setDouble(14,f.getOldData().getTotalMovement());
				stmt.setDouble(17,f.getOldData().getTotalRevenue());
				stmt.setDouble(20,f.getOldData().getNetMargin());
				stmt.setDouble(23, f.getOldData().getGold_Perc());
				stmt.setDouble(25, f.getOldData().getGold_Mov());
				stmt.setDouble(28, f.getOldData().getGold_Rev());
				stmt.setDouble(31, f.getOldData().getGold_Mar());
				stmt.setDouble(34, f.getOldData().getSil_Perc());
				stmt.setDouble(36, f.getOldData().getSil_Mov());
				stmt.setDouble(39, f.getOldData().getSil_Rev());
				stmt.setDouble(42, f.getOldData().getSil_Mar());
				stmt.setDouble(45, f.getOldData().getReg_Perc());
				stmt.setDouble(47, f.getOldData().getReg_Mov());
				stmt.setDouble(50, f.getOldData().getReg_Rev());
				stmt.setDouble(53, f.getOldData().getReg_Mar());
				stmt.setDouble(57, f.getOldData().getSale_Perc());
				stmt.setFloat(61, f.getOldData().getNo_Card_Perc());

				}else {
					stmt.setFloat(9,0);
					stmt.setFloat(12,0);
					stmt.setDouble(14,0);
					stmt.setDouble(17,0);
					stmt.setDouble(20,0);
					stmt.setDouble(23, 0);
					stmt.setDouble(25,0);
					stmt.setDouble(28, 0);
					stmt.setDouble(31,0);
					stmt.setDouble(34,0);
					stmt.setDouble(36, 0);
					stmt.setDouble(39,0);
					stmt.setDouble(42,0);
					stmt.setDouble(45,0);
					stmt.setDouble(47,0);
					stmt.setDouble(50,0);
					stmt.setDouble(53, 0);
					stmt.setDouble(57,0);
					stmt.setFloat(61, 0);
				}
				if(f.isNewDataPresent()) {
				stmt.setFloat(10, f.getNewData().getRegularPrice());
				stmt.setFloat(13, f.getNewData().getListCost());
				stmt.setDouble(15,f.getNewData().getTotalMovement());
				stmt.setDouble(18,f.getNewData().getTotalRevenue());
				stmt.setDouble(21,f.getNewData().getNetMargin());
				stmt.setDouble(24,f.getNewData().getGold_Perc());
				stmt.setDouble(26,f.getNewData().getGold_Mov());
				stmt.setDouble(29,f.getNewData().getGold_Rev());
				stmt.setDouble(32,f.getNewData().getGold_Mar());
				stmt.setDouble(35,f.getNewData().getSil_Perc());
				stmt.setDouble(37,f.getNewData().getSil_Mov());
				stmt.setDouble(40,f.getNewData().getSil_Rev());
				stmt.setDouble(43,f.getNewData().getSil_Mar());
				stmt.setDouble(46,f.getNewData().getReg_Perc());
				stmt.setDouble(48,f.getNewData().getReg_Mov());
				stmt.setDouble(51,f.getNewData().getReg_Rev());
				stmt.setDouble(54,f.getNewData().getReg_Mar());
				stmt.setDouble(56,f.getNewData().getSale_Perc());
				stmt.setDouble(58, f.getNewData().getPrediction());
				stmt.setFloat(60, f.getNewData().getNo_Card_Perc());
				stmt.setFloat(62, f.getNewData().getPredictedSale());
				stmt.setFloat(64, f.getNewData().getPredictedMargin());
				}else {
					stmt.setFloat(10, 0);
					stmt.setFloat(13, 0);
					stmt.setDouble(15,0);
					stmt.setDouble(18,0);
					stmt.setDouble(21,0);
					stmt.setDouble(24,0);
					stmt.setDouble(26,0);
					stmt.setDouble(29,0);
					stmt.setDouble(32,0);
					stmt.setDouble(35,0);
					stmt.setDouble(37,0);
					stmt.setDouble(40,0);
					stmt.setDouble(43,0);
					stmt.setDouble(46,0);
					stmt.setDouble(48,0);
					stmt.setDouble(51,0);
					stmt.setDouble(54,0);
					stmt.setDouble(56,0);
					stmt.setDouble(58, 0);
					stmt.setFloat(60, 0);
					stmt.setFloat(62, 0);
					stmt.setFloat(64, 0);
				}
				if(f.isNewDataPresent()&&f.isOldDataPresent()) {
				stmt.setDouble(11,f.getPriceDiff());
				stmt.setDouble(16,f.getMovementVariance());
				stmt.setDouble(19,f.getRevenueVariance());
				stmt.setDouble(22,f.getMarginVariance());
				stmt.setDouble(27, f.getGoldMovVariance());
				stmt.setDouble(30, f.getGoldRevVariance());
				stmt.setDouble(33, f.getGoldMarVariance());
				stmt.setDouble(38, f.getSilMovVariance());
				stmt.setDouble(41, f.getSilRevVariance());
				stmt.setDouble(44, f.getSilMarVariance());
				stmt.setDouble(49, f.getRegMovVariance());
				stmt.setDouble(52, f.getRegRevVariance());
				stmt.setDouble(55, f.getRegMarVariance());
				stmt.setDouble(59,f.getPredictionAccuracyPerc());
				stmt.setDouble(63, f.getSalePredictionAccuracyPerc());
				stmt.setDouble(65, f.getMarginPredictionAccuracyPerc());
				}else {
					stmt.setDouble(11,0);
					stmt.setDouble(16,0);
					stmt.setDouble(19,0);
					stmt.setDouble(22,0);
					stmt.setDouble(27, 0);
					stmt.setDouble(30, 0);
					stmt.setDouble(33, 0);
					stmt.setDouble(38,0);
					stmt.setDouble(41, 0);
					stmt.setDouble(44,0);
					stmt.setDouble(49, 0);
					stmt.setDouble(52,0);
					stmt.setDouble(55, 0);
					stmt.setDouble(59,0);
					stmt.setDouble(63, 0);
					stmt.setDouble(65, 0);
				}
				stmt.addBatch();
				
			}
			stmt.executeBatch();
			conn.commit();
		}catch(Exception Ex) {
		conn.rollback();
		logger.error("ReportDataInsertDAO InsertFinalReportData: " + Ex.getMessage() + Ex.getStackTrace().toString());
		throw new Exception(Ex.getMessage());
	}
	}
	public void InsertItemLevelWeeklyData(Connection conn, String DataType, int runId, List<TlogDataDTO> trendsData,
			Map<LocalDate, Integer> calendarIds) throws Exception {
		String q ;
		if(DataType=="OLD") {
			q= String.valueOf(insert_Item_Level_Trend_Data_Old);
		}else {
			q= String.valueOf(insert_Item_Level_Trend_Data_New);
		}
		
		try {
			PreparedStatement stmt = conn.prepareStatement(q);
			for(TlogDataDTO t: trendsData) {
				stmt.setInt(1, runId);
				stmt.setString(2, "W");
				stmt.setString(3,(DataType=="OLD"?"L":"C"));
				stmt.setInt(4,t.getWeekCalendarId());
				stmt.setInt(5,t.getProductLevelId());
				stmt.setInt(6, t.getProductId());
				if(DataType=="OLD") {
				stmt.setFloat(7, t.getLY_RegularPrice());
				stmt.setFloat(8, t.getLY_ListCost());
				
				}else {
					stmt.setFloat(7, t.getRegularPrice());
					stmt.setFloat(8, t.getListCost());
				}
				stmt.setDouble(9, t.getTotalMovement());
				stmt.setDouble(10, t.getTotalRevenue());
				stmt.setDouble(11, t.getNetMargin());
				//Gold
				stmt.setFloat(12, t.getGold_Perc());
				stmt.setFloat(13, t.getGold_Mov());
				stmt.setFloat(14, t.getGold_Rev());
				stmt.setFloat(15, t.getGold_Mar());
				//Sil
				stmt.setFloat(16, t.getSil_Perc());
				stmt.setFloat(17, t.getSil_Mov());
				stmt.setFloat(18, t.getSil_Rev());
				stmt.setFloat(19, t.getSil_Mar());
				
				//Reg
				stmt.setFloat(20, t.getReg_Perc());
				stmt.setFloat(21, t.getReg_Mov());
				stmt.setFloat(22, t.getReg_Rev());
				stmt.setFloat(23, t.getReg_Mar());
				
				//sale
				stmt.setFloat(24, t.getSale_Perc());
				
				if(DataType=="NEW") {
					//Pred
					stmt.setFloat(25, t.getPrediction());
					stmt.setFloat(26, t.getNo_Card_Perc());
					stmt.setFloat(27, t.getPredictedSale());
					stmt.setFloat(28, t.getPredictedMargin());
					}else {
						stmt.setFloat(25, t.getNo_Card_Perc());	
					}
			
				stmt.addBatch();
				
			}
			stmt.executeBatch();
			conn.commit();
		}catch(Exception Ex) {
		conn.rollback();
		logger.error("ReportDataInsertDAO InsertWeeklyItemTrendData: " + Ex.getMessage() + Ex.getStackTrace().toString());
		throw new Exception(Ex.getMessage());
	}
		
		
	}
	
	
	
	
}
