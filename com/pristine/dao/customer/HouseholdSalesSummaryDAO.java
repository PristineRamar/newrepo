package com.pristine.dao.customer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.customer.CustomerVisitSummaryDTO;
import com.pristine.dto.customer.HouseholdSummaryDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class HouseholdSalesSummaryDAO {
	
	static Logger logger = Logger.getLogger("HouseholdSalesSummaryDAO");
	private static String lineSeparator = System.lineSeparator();
	
	public Connection conn = null; // DB connection
	
	public static final String INSERT_CUSTOMER_SALES = "INSERT INTO " +
			"CUSTOMER_SALES_SUMMARY (CUSTOMER_SALES_SUMMARY_ID, STORE_ID, " +
			"CALENDAR_ID,CUSTOMER_ID,SALE_MOVEMENT, REG_MOVEMENT, " +
			"TOT_MOVEMENT, SALE_REVENUE, REG_REVENUE, TOT_REVENUE, " +
			"SALE_MARGIN, REG_MARGIN,TOT_MARGIN,TOT_VISITS) VALUES " +
			"(CUSTOMER_SALES_SUMMARY_SEQ.nextval,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	
	public static final String INSERT_SALES_SUMMARY_DAILY = 
			"INSERT INTO CST_SUMMARY_DAILY" + 
			" (CST_SUMMARY_DAILY_ID, CUSTOMER_ID, CALENDAR_ID, STORE_ID,"+ 
			" TOTAL_VISITS," +
			" TOT_MOVEMENT, REG_MOVEMENT, SALE_MOVEMENT," + 
			" TOT_MOVEMENT_VOL, REG_MOVEMENT_VOL, SALE_MOVEMENT_VOL," + 
			" TOT_REVENUE, REG_REVENUE, SALE_REVENUE, " + 
			" TOT_MARGIN, REG_MARGIN, SALE_MARGIN," + 
			" TOT_MARGIN_PCT, REG_MARGIN_PCT, SALE_MARGIN_PCT," + 
			" PL_TOTAL_VISITS, " + 
			" PL_TOT_MOVEMENT, PL_REG_MOVEMENT, PL_SALE_MOVEMENT," + 
			" PL_TOT_MOVEMENT_VOL, PL_REG_MOVEMENT_VOL, PL_SALE_MOVEMENT_VOL," + 
			" PL_TOT_REVENUE, PL_REG_REVENUE, PL_SALE_REVENUE," +
			" PL_TOT_MARGIN, PL_REG_MARGIN, PL_SALE_MARGIN," + 
			" PL_TOT_MARGIN_PCT, PL_REG_MARGIN_PCT, PL_SALE_MARGIN_PCT)" +
			" VALUES" +
			" (HH_SUMMARY_DAILY_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + 
			" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + 
			" ?, ?, ?, ?, ?)";

	public static final String INSERT_SALES_SUMMARY_WEEKLY = 
			"INSERT INTO CST_SUMMARY_WEEKLY" + 
			" (CST_SUMMARY_WEEKLY_ID, CUSTOMER_ID, CALENDAR_ID, STORE_ID," + 
			" TOTAL_VISITS," +
			" TOT_MOVEMENT, REG_MOVEMENT, SALE_MOVEMENT," + 
			" TOT_MOVEMENT_VOL, REG_MOVEMENT_VOL, SALE_MOVEMENT_VOL," + 
			" TOT_REVENUE, REG_REVENUE, SALE_REVENUE, " + 
			" TOT_MARGIN, REG_MARGIN, SALE_MARGIN," + 
			" TOT_MARGIN_PCT, REG_MARGIN_PCT, SALE_MARGIN_PCT," + 
			" PL_TOTAL_VISITS," + 
			" PL_TOT_MOVEMENT, PL_REG_MOVEMENT, PL_SALE_MOVEMENT," + 
			" PL_TOT_MOVEMENT_VOL, PL_REG_MOVEMENT_VOL, PL_SALE_MOVEMENT_VOL," + 
			" PL_TOT_REVENUE, PL_REG_REVENUE, PL_SALE_REVENUE," +
			" PL_TOT_MARGIN, PL_REG_MARGIN, PL_SALE_MARGIN," + 
			" PL_TOT_MARGIN_PCT, PL_REG_MARGIN_PCT, PL_SALE_MARGIN_PCT)" +
			" VALUES" +
			" (HH_SUMMARY_WEEKLY_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + 
			" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + 
			" ?, ?, ?, ?, ?)";
	
	public static final String INSERT_SALES_SUMMARY_PERIOD = 
			"INSERT INTO CST_SUMMARY_PERIOD" + 
			" (CST_SUMMARY_PERIOD_ID, CUSTOMER_ID, CALENDAR_ID, STORE_ID," + 
			" TOTAL_VISITS," +
			" TOT_MOVEMENT, REG_MOVEMENT, SALE_MOVEMENT," + 
			" TOT_MOVEMENT_VOL, REG_MOVEMENT_VOL, SALE_MOVEMENT_VOL," + 
			" TOT_REVENUE, REG_REVENUE, SALE_REVENUE, " + 
			" TOT_MARGIN, REG_MARGIN, SALE_MARGIN," + 
			" TOT_MARGIN_PCT, REG_MARGIN_PCT, SALE_MARGIN_PCT," + 
			" PL_TOTAL_VISITS," + 
			" PL_TOT_MOVEMENT, PL_REG_MOVEMENT, PL_SALE_MOVEMENT," + 
			" PL_TOT_MOVEMENT_VOL, PL_REG_MOVEMENT_VOL, PL_SALE_MOVEMENT_VOL," + 
			" PL_TOT_REVENUE, PL_REG_REVENUE, PL_SALE_REVENUE," +
			" PL_TOT_MARGIN, PL_REG_MARGIN, PL_SALE_MARGIN," + 
			" PL_TOT_MARGIN_PCT, PL_REG_MARGIN_PCT, PL_SALE_MARGIN_PCT)" +
			" VALUES" +
			" (HH_SUMMARY_PERIOD_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + 
			" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + 
			" ?, ?, ?, ?, ?)";	
	
	public static final String INSERT_ITEM_SUMMARY_DAILY = 
			"INSERT INTO CST_SUMMARY_DAILY_ITEM" + 
			" (CST_SUMMARY_DAILY_ITEM_ID, CUSTOMER_ID, CALENDAR_ID," + 
			" STORE_ID, ITEM_CODE, TOTAL_VISITS," +
			" TOT_MOVEMENT, REG_MOVEMENT, SALE_MOVEMENT," + 
			" TOT_MOVEMENT_VOL, REG_MOVEMENT_VOL, SALE_MOVEMENT_VOL," + 
			" TOT_REVENUE, REG_REVENUE, SALE_REVENUE, " + 
			" TOT_MARGIN, REG_MARGIN, SALE_MARGIN," + 
			" TOT_MARGIN_PCT, REG_MARGIN_PCT, SALE_MARGIN_PCT)" +
			" VALUES" +
			" (HH_SUMMARY_DAILY_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + 
			" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	public static final String INSERT_ITEM_SUMMARY_WEEKLY = 
			"INSERT INTO CST_SUMMARY_WEEKLY_ITEM" + 
			" (CST_SUMMARY_WEEKLY_ITEM_ID, CUSTOMER_ID, CALENDAR_ID," + 
			" STORE_ID, ITEM_CODE, TOTAL_VISITS," +
			" TOT_MOVEMENT, REG_MOVEMENT, SALE_MOVEMENT," + 
			" TOT_MOVEMENT_VOL, REG_MOVEMENT_VOL, SALE_MOVEMENT_VOL," + 
			" TOT_REVENUE, REG_REVENUE, SALE_REVENUE, " + 
			" TOT_MARGIN, REG_MARGIN, SALE_MARGIN," + 
			" TOT_MARGIN_PCT, REG_MARGIN_PCT, SALE_MARGIN_PCT)" +
			" VALUES" +
			" (HH_SUMMARY_WEEKLY_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + 
			" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	public static final String INSERT_ITEM_SUMMARY_PERIOD = 
			"INSERT INTO CST_SUMMARY_PERIOD_ITEM" + 
			" (CST_SUMMARY_PERIOD_ITEM_ID, CUSTOMER_ID, CALENDAR_ID," + 
			" STORE_ID, ITEM_CODE, TOTAL_VISITS," +
			" TOT_MOVEMENT, REG_MOVEMENT, SALE_MOVEMENT," + 
			" TOT_MOVEMENT_VOL, REG_MOVEMENT_VOL, SALE_MOVEMENT_VOL," + 
			" TOT_REVENUE, REG_REVENUE, SALE_REVENUE, " + 
			" TOT_MARGIN, REG_MARGIN, SALE_MARGIN," + 
			" TOT_MARGIN_PCT, REG_MARGIN_PCT, SALE_MARGIN_PCT)" +
			" VALUES" +
			" (HH_SUMMARY_PERIOD_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + 
			" ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	public boolean deleteHouseholdSalesSummaryDaily(Connection conn,int storeId, 
			int calendarId) throws GeneralException,SQLException {

		try {
			StringBuffer sbb = new StringBuffer("DELETE FROM CST_SUMMARY_DAILY");	    
			sbb.append(" where STORE_ID = ").append(storeId);
			sbb.append(" AND CALENDAR_ID = ").append(calendarId);	    
			
			logger.debug("deleteHouseholdSalesSummaryDaily SQL:" + sbb);
			PristineDBUtil.execute(conn, sbb, "deleteHouseholdSalesSummaryDaily - delete");		
			
			sbb = new StringBuffer("DELETE FROM CST_SUMMARY_DAILY_ITEM");	    
			sbb.append(" where STORE_ID = ").append(storeId);
			sbb.append(" AND CALENDAR_ID = ").append(calendarId);	    
			
			logger.debug("deleteHouseholdSalesSummaryDaily SQL:" + sbb);
			PristineDBUtil.execute(conn, sbb, "deleteHouseholdSalesSummaryDaily - delete");		
			
			PristineDBUtil.commitTransaction(conn, "Data Deletion");
		}
		catch(GeneralException ex){
			logger.error("Error while delete Household Sales Summary Daily data :" + ex);
			throw new GeneralException("Error while executing delete Household Sales Summary Daily", ex);
		}
		
		return true;
	}

	public boolean deleteHouseholdSalesSummaryRollup(Connection conn, 
								int calid, int storeId, String calendarMode) 
										throws GeneralException,SQLException {
		try {
			String summaryTable = "CST_SUMMARY_WEEKLY";
			String summaryItemTable = "CST_SUMMARY_WEEKLY_ITEM";
			
			if (calendarMode.equalsIgnoreCase(Constants.CALENDAR_PERIOD)){
				summaryTable = "CST_SUMMARY_PERIOD";
				summaryItemTable = "CST_SUMMARY_PERIOD_ITEM";
			}
			
			StringBuffer sbb = new StringBuffer("DELETE FROM ").append(summaryTable);	    
			sbb.append(" WHERE CALENDAR_ID = ").append(calid);	    

			sbb.append(" AND STORE_ID = ").append(storeId);

			logger.debug("deleteSummaryWeekly SQL:" + sbb);
			PristineDBUtil.execute(conn, sbb, "deleteHouseholdSalesSummaryRollup - delete");		

			sbb = new StringBuffer("DELETE FROM ").append(summaryItemTable);	    
			sbb.append(" WHERE CALENDAR_ID = ").append(calid);	    
			sbb.append(" AND STORE_ID = ").append(storeId);

			logger.debug("deleteSummaryWeekly SQL:" + sbb);
			PristineDBUtil.execute(conn, sbb, "deleteHouseholdSalesSummaryRollup - delete");
			PristineDBUtil.commitTransaction(conn, "Data Deletion");
		}
		catch(GeneralException ex){
			logger.error("Error while delete Summary Rollup data :" + ex);
			throw new GeneralException("Error while executing deleteCustomerSalesSummaryRollup", ex);
		}
		
		return true;
	}
	
	public boolean deleteCustomerSalesSummary(Connection conn,int storeId, 
							int calid) throws GeneralException,SQLException {
		
		try{	
			StringBuffer sbb = new StringBuffer("delete from CUSTOMER_SALES_SUMMARY");	    
			sbb.append(" where STORE_ID = ").append(storeId);
			sbb.append(" AND CALENDAR_ID = ").append(calid);	    
	
			PristineDBUtil.execute(conn, sbb, "CustomerSalesSummaryDAO - delete");		
			PristineDBUtil.commitTransaction(conn, "Data Deletion");
		}
		catch (Exception se)
		{
			logger.error("Error while deleting exisiting Customer Sales Summary weekly data: "+se);
			throw new GeneralException("Error while executing deleteCustomerSalesSummary", se);
		}		
		return true;
	}

	
	public boolean insertCustomerSalesSummary (Connection conn, 
							List<CustomerVisitSummaryDTO> custVisitList) 
									throws GeneralException,SQLException {	
		
		 PreparedStatement statement = null;
			try{				    	  
				statement = conn.prepareStatement(INSERT_CUSTOMER_SALES);
				int itemNoInBatch = 0;
				for(CustomerVisitSummaryDTO custVisit: custVisitList){
					int counter = 0;					
					statement.setDouble(++counter, custVisit.getComp_str_no());					
					statement.setDouble(++counter, custVisit.getCalendarid());
					
					if(custVisit.getCustomerid()!=null)
						statement.setDouble(++counter, custVisit.getCustomerid());
					else
						statement.setNull(++counter, java.sql.Types.DOUBLE);
					
					statement.setDouble(++counter, custVisit.getSalemovement());					
					statement.setDouble(++counter, custVisit.getRegmovement());					
					statement.setDouble(++counter, custVisit.getTotmovement());					
					statement.setDouble(++counter, custVisit.getSalerevenue());					
					statement.setDouble(++counter, custVisit.getRegrevenue());					
					statement.setDouble(++counter, custVisit.getTotrevenue());					
					statement.setDouble(++counter, custVisit.getSalemargin());					
					statement.setDouble(++counter, custVisit.getRegmargin());					
					statement.setDouble(++counter, custVisit.getTotmargin());					
					statement.setDouble(++counter, custVisit.getTransactionCount());					
					statement.addBatch();
		        	itemNoInBatch++;
		        	
		        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
		        		long startTime = System.currentTimeMillis();
		        		int[] count = statement.executeBatch();
		        		long endTime = System.currentTimeMillis();
		        		logger.debug("Time taken for inserting a batch:" + (endTime - startTime) + "ms");
		        		statement.clearBatch();
		        		itemNoInBatch = 0;
		        		PristineDBUtil.commitTransaction(conn, "Data Load");
		        		logger.debug("The number of records inserted:"+count.length);
		        	}
		        }
		        if(itemNoInBatch > 0){
		        	int[] count = statement.executeBatch();
		        	PristineDBUtil.commitTransaction(conn, "Data Load");
	        		statement.clearBatch();
	        		logger.debug("The number of records inserted:"+count.length);
		        }		
			}
			catch (SQLException se)
			{
				logger.error("Error while executing INSERT_CUSTOMER_SALES_FRM_CUSTOMER_VISIT"+se);
				throw new GeneralException("Error while executing INSERT_CUSTOMER_SALES_FRM_CUSTOMER_VISIT", se);
			}
			catch (GeneralException ge)
			{
				logger.error("Error while executing INSERT_CUSTOMER_SALES_FRM_CUSTOMER_VISIT"+ge);
				throw new GeneralException("Error while executing INSERT_CUSTOMER_SALES_FRM_CUSTOMER_VISIT", ge);
			}			
			
			finally{
				PristineDBUtil.close(statement);
			}
				return true;
	}
	
	
	public CachedRowSet getCustomerWeeklySummaryData(Connection conn, 
									String date, int storeid, int weekCount){
		
		CachedRowSet result = null;
		
		try{
			int datebeginNo = (7 * weekCount) - 1;
			StringBuffer sb = new StringBuffer();
			sb.append("select id as CUSTOMER_ID,");
			sb.append(" NVL(SUM(TOT_REVENUE), 0) as TOT_REVENUE,");
			sb.append(" NVL(sum(TOT_VISITS), 0) as TOT_VISITS");
			sb.append(" from  CUSTOMER c left join CUSTOMER_SALES_SUMMARY cs ");
			sb.append(" on c.ID = cs.CUSTOMER_ID where c.STORE_ID =" + storeid);
			sb.append(" and cs.STORE_ID = " + storeid + " ");
			sb.append(" and cs.CALENDAR_ID in");
			sb.append(" (select CALENDAR_ID from  RETAIL_CALENDAR R where");
			sb.append(" R.END_DATE >= (TO_DATE('"+ date +"')-").append(datebeginNo);
			sb.append(") and R.End_Date <='" + date + "' and R.row_type='W')");
			sb.append(" group by ID order by ID");
			logger.debug("GetCustomerWeeklySummaryData SQL:" + sb.toString());
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerSegment");
			
		}catch(GeneralException ge){
			logger.error("Exception in getCustomerSegment CustomerSalesSummaryDAO "+ge);
		}
		return result;
	}

	
	public CachedRowSet getTrxSummary(Connection conn, 
									String date, int storeid, int weekCount){
		
		CachedRowSet result = null;
		
		try{
			int datebeginNo = (7 * weekCount) - 1;
			StringBuffer sb = new StringBuffer();
			sb.append("select id as CUSTOMER_ID,");
			sb.append(" NVL(SUM(TOT_REVENUE), 0) as TOT_REVENUE,");
			sb.append(" NVL(sum(TOT_VISITS), 0) as TOT_VISITS");
			sb.append(" from  CUSTOMER c left join CUSTOMER_SALES_SUMMARY cs ");
			sb.append(" on c.ID = cs.CUSTOMER_ID where c.STORE_ID =" + storeid);
			sb.append(" and cs.STORE_ID = " + storeid + " ");
			sb.append(" and cs.CALENDAR_ID in");
			sb.append(" (select CALENDAR_ID from  RETAIL_CALENDAR R where");
			sb.append(" R.END_DATE >= (TO_DATE('"+ date +"')-").append(datebeginNo);
			sb.append(") and R.End_Date <='" + date + "' and R.row_type='W')");
			sb.append(" group by ID order by ID");
			logger.debug("GetCustomerWeeklySummaryData SQL:" + sb.toString());
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerSegment");
			
		}catch(GeneralException ge){
			logger.error("Exception in getCustomerSegment CustomerSalesSummaryDAO "+ge);
		}
		return result;
	}

	public List<Integer> getCustomerIds(String analysisPeriodStart, String analysisPeriodEnd) {
		CachedRowSet result = null;
		List<Integer> customerIds = new ArrayList<Integer>();
		
		try{
			StringBuffer sb = new StringBuffer();
			String tLog = PropertyManager.getProperty("SUBS_TLOG_SOURCE_TABLE");
			
			//sb.append("SELECT customer_id FROM customer_loyalty_info WHERE customer_id in (546372970, 528312156, 501750576, 539807123, 562538618, 563873847, 553672898, 525948631, 505600201, 551268910, 900420705, 541017566, 548920079, 500003248, 523097448, 501751199, 503360371, 556677014, 520737702, 503410208, 560805043, 550096276, 560555297, 558179943, 900092955, 563874635, 505316554, 505774439, 507893487, 901052600, 560545671, 524199503, 507428978, 505799784, 537837987, 503358734, 519355947, 510195045, 542626925, 555723003, 561294533, 500091488, 523247498, 563016740, 900443179, 539380809, 500017226, 502304064, 507078872, 503949770, 502300348, 541416937, 539856876, 527143678, 529067971, 506899252, 520826883, 524049967, 528298804, 503360221, 541127579, 555450305, 514116811, 504997210, 508273864, 563487052, 509212597, 901060521, 515115357, 500022045, 506010554, 500811038, 516041908, 500502330, 501777191, 511452054, 536945629, 505000341, 507256998, 500527368, 545095620, 505000295, 561179739, 503364950, 506392507, 503954707, 559353582, 500026327, 500026252, 500539019, 500026175, 503652665, 505551987, 507137753, 507581938, 560669825, 507257521, 545229947, 501778476, 535032839, 528511573, 563750885, 509798959, 522417177, 506763980, 500568683, 500027908, 501707266, 541295043, 502852088, 500027770, 500457199, 500455716, 549835140, 521971252, 559325622, 563004995, 537282997, 501450943, 560960403, 500028990, 505976693, 508269479, 553872720, 507872913, 500973295, 520917118, 539676585, 510513678, 505182426, 900008318, 553707707, 538132628, 500033655, 528599181, 509517847, 527599955, 505471412, 528875231, 502244348, 523197290, 526546987, 525853346, 505167859, 563875466, 542999512, 530018550, 502340109, 548798244, 545752078, 519221199, 512477719, 558374705, 559028072, 561665868, 501787025, 900270606, 542527983, 557069507, 527036592, 502336399, 556121098, 506017186, 900002989, 505957137, 503658852, 510717001, 503658848, 505790155, 505331411, 508784542, 551512067, 559942353, 513902074, 508922544, 512262156, 507585125, 528384299, 503371768, 527647695, 563875616, 900003151, 563514774, 516243238, 500540075, 543414353, 500039516, 511288471, 504279226, 500028987, 506906799, 501791665, 500540004, 900289522, 504785367, 541634569, 500170719, 502341472, 516834174, 506681440, 506540208, 502341339, 509798958, 516864989, 518851026, 504245289, 518275711, 506002669, 516056354, 548284089, 501778220, 550438450, 537041298, 515274080, 509675461, 563875915, 500048382, 500048324, 519245921, 562543063, 552945568, 501812173, 500540054, 537186281, 503704295, 503382187, 502360136, 502360045, 551539642, 506913719, 500057314, 500998820, 508636778, 500557303, 500085938, 526003980, 517583976, 532124816, 563876225, 547254766, 519263248, 523507984, 563876221, 502360821, 515108503, 502360805, 503671106, 538067743, 500999205, 545938538, 541378369, 551471558, 511885709, 900350949, 526239568, 521671360, 523913087, 558823643, 562185919, 518563054, 533478107, 502387925, 901065896, 506026393, 563876232, 527342996, 507735300, 504255050, 541959741, 544854424, 505012929, 542703315, 522268961, 501862612, 550737432, 520610797, 557343609, 544839077, 515912850, 529117672, 504808077, 537487991, 900440585, 507431490, 501408028, 546927293, 528805195, 524907857, 528885573, 502368915, 504713558, 555598488, 529750629, 545624314, 538133739, 504740468, 556721909, 501379341, 554663409, 510209851, 557598656, 510041877, 509020192, 561983941, 504003613, 563876560, 505805647, 536904469, 507275286, 554472489, 506034796, 507741967, 522606952, 542470973, 500577186, 526951893, 558087395, 512479726, 511294323, 515425377, 500029062, 536983359, 501422632, 513960291, 562183821, 506016207, 528743825, 545723400, 533048919, 510690711, 529972907, 528968278, 502884077, 522180855, 506557909, 531522799, 509382590, 522271124, 501752971, 517045766, 563698035, 563592704, 508814889, 513567450, 507249864, 500086947, 501023786, 523426554, 516239426, 506568969, 500457657, 549665330, 530120442, 551145572, 503990195, 538277042, 900348076, 522811905, 516242932, 558338806, 513214452, 506409924, 513333375, 500589071, 547223840, 505021396, 521490731, 545061121, 551196795, 551400193, 526174299, 532879487, 553765895, 520610624, 518851984, 502326643, 501027299, 505177347, 501850194, 527443904, 506707579, 544496978, 518052027, 501033225, 541832785, 554949171, 517961957, 554560053, 506568948, 505579019, 553864926, 561625396, 563877513, 539463449, 553769286, 504508666, 552361351, 503639657, 504233387, 513482391, 547700823, 534640193, 503994284, 547596508, 515424183, 501281195, 506772973, 534581525, 563877527, 522508653, 507749331, 508653595, 520704656, 518522472, 509241002, 900277416, 502422569, 511306402, 501862582, 503997456, 508424246, 508084958, 501708913, 556837290, 502411901, 535684956, 531295688, 556803079, 505182418, 520437163, 556349581, 510512524, 521294215, 501868130, 901049600, 556837430, 502244905, 513548642, 529297541, 529542996, 501041103, 516214643, 524325633, 501858286, 500536284, 558977352, 547767702, 501041107, 531844577, 513910475, 540745093, 545577262, 503127254, 504557145, 544893186, 502245567, 501375862, 529411014, 510833977, 563877860, 505552463, 530194742, 547451454, 501005727, 507872434, 557297656, 515532369, 547403359, 516865593, 502903852, 503411883, 509530721, 563815281, 556531197, 501048063, 901062749, 515130859, 517969946, 563899394, 505590385, 900039155, 504285096, 540850070, 561569674, 503171429, 507109827, 523396977, 500116940, 503414049, 504540449, 500457356, 509342188, 504004868, 508307280, 507559170, 524004018, 505582820, 548474078, 560905525)");
			sb.append("SELECT distinct customer_id FROM " + tLog +
						"  where calendar_id in  " +  lineSeparator +
						"  (SELECT calendar_id FROM retail_calendar WHERE start_date >= to_date('" + analysisPeriodStart + "','MM/dd/yyyy') AND start_date <= to_date('" + analysisPeriodEnd + "','MM/dd/yyyy') AND row_type = 'D') "); 
		    
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerIds");
			while ( result.next() ) {
				customerIds.add(result.getInt("customer_id"));
			}
			
			return customerIds;
		}
		catch (GeneralException e) {
			logger.error("getCustomerIds() - GeneralException=" + e.getMessage());
			e.printStackTrace();
			return customerIds;
	    }
		catch (Exception e) {
			logger.error("getCustomerIds() - Exception=" + e.getMessage());
			e.printStackTrace();
			return customerIds;
	    }
	}

	/**
	 * Analyze and Store the transaction summary based on input parameters.   
	 * 
	 * @param conn
	 * @param analysisDate: Analysis done for a period ending analysisDate.
	 * @param location_level_id: Presently accepts level Id for Store.
	 * @param locationId: Presently accepts Id of Store to be analyzed.
	 * 
	 * @return success/failure
	 * @throws 
	 */
	public boolean performTrxSummary(String analysisPeriodStart, String analysisPeriodEnd, 
			String customerIdString)
	{
		
	    try
	    {
		    StringBuffer sql = new StringBuffer();
			String tLog = PropertyManager.getProperty("SUBS_TLOG_SOURCE_TABLE");

		    
		    // Forming query of type "INSERT INTO ... SELECT ..."  type.
		    
		    sql.append("INSERT INTO cst_trx_summary " + lineSeparator +
		    " SELECT cst_trx_summary_seq.nextval, nvl(customer_id, -1), TO_DATE('" + analysisPeriodStart + "','MM/dd/yyyy') analysis_period_start, TO_DATE('" + analysisPeriodEnd + "','MM/dd/yyyy') analysis_period_end, " + 
		    "    primary_store_id, " + 
		    "    NVL(fvd_rc.start_date, TO_DATE('01/01/2000', 'MM/dd/yyyy')) first_visit_date, " + 
		    "    NVL(lvd_rc.start_date, TO_DATE('01/01/2000', 'MM/dd/yyyy')) last_visit_date, " +
		    "    visit_days_cnt, " + 
		    "    approximate_visit_cnt visit_cnt, " + 
		    "    case visit_days_cnt when 1 then null else round((lvd_rc.start_date - fvd_rc.start_date) / (visit_days_cnt - 1), 2) end visit_days_interval_avg, " + 
		    "    trx_cnt, " + 
		    "    trx_reg_cnt, trx_sale_cnt, " + 
		    "    trx_regonly_cnt, trx_saleonly_cnt, " + 
		    "    -1 trx_prom_cnt, -1 trx_promoonly_cnt, " + 
		    "    ROUND(primary_store_trx_cnt_ratio, 2) primary_store_trx_cnt_ratio, " + 
		    "    items_cnt, -1 free_items_cnt, -1 items_qty, -1 items_vol, " + 
		    "    li_cnt, suspense_item_li_cnt, " + 
		    "    li_reg_cnt, li_sale_cnt, ROUND(li_sale_cnt/ li_cnt, 2) li_sale_ratio, " + 
		    "    -1 li_promo_cnt, -1 li_promo_ratio, " + 
		    //"    -1 revenue_tot, " + 
		    "    revenue_gross, revenue_net, " + 
		    "    revenue_net_reg, revenue_net_sale, " + 
		    "    -1 revenue_promo_tot, " + 
		    "    ROUND(revenue_net / trx_cnt, 2) aos, " + 
		    "    CASE WHEN trx_cnt <= 5 THEN '-5' WHEN trx_cnt <= 10 THEN '6-10' WHEN trx_cnt <= 20 THEN '11-20' WHEN trx_cnt <= 30 THEN '21-30' " + 
		    "      WHEN trx_cnt <= 40 THEN '31-40' WHEN trx_cnt <= 50 THEN '41-50' WHEN trx_cnt <= 75 THEN '51-75' WHEN trx_cnt <= 100 THEN '76-100' " + 
		    "      WHEN trx_cnt <= 150 THEN '101-150' WHEN trx_cnt <= 250 THEN '151-250' WHEN trx_cnt <= 500 THEN '251-500' WHEN trx_cnt <= 1000 THEN '501-1000' ELSE '1001-' END trx_segment, " + 
		    "    CASE WHEN approximate_visit_cnt <= 5 THEN '-5' WHEN approximate_visit_cnt <= 10 THEN '6-10' WHEN approximate_visit_cnt <= 20 THEN '11-20' WHEN approximate_visit_cnt <= 30 THEN '21-30' " + 
		    "      WHEN approximate_visit_cnt <= 40 THEN '31-40' WHEN approximate_visit_cnt <= 50 THEN '41-50' WHEN approximate_visit_cnt <= 75 THEN '51-75' WHEN approximate_visit_cnt <= 100 THEN '76-100' " + 
		    "      WHEN approximate_visit_cnt <= 150 THEN '101-150' WHEN approximate_visit_cnt <= 250 THEN '151-250' WHEN approximate_visit_cnt <= 500 THEN '251-500' WHEN approximate_visit_cnt <= 1000 THEN '501-1000' ELSE '1001-' END visit_cnt_segment, " + 
		    "    CASE WHEN revenue_net <= 25 THEN '-25' WHEN revenue_net <= 50 THEN '25-50' WHEN revenue_net <= 100 THEN '50-100' WHEN revenue_net <= 250 THEN '100-250' " + 
		    "      WHEN revenue_net <= 500 THEN '250-500' WHEN revenue_net <= 1000 THEN '500-1000' WHEN revenue_net <= 2500 THEN '1000-2500' ELSE '2500-' END net_revenue_segment, " + 
		    "    -1 margin, sysdate " + lineSeparator + 
		    "  FROM " +  lineSeparator +
		    "    (SELECT customer_id, " + 
		    "        MAX(primary_store_id) primary_store_id, " + 
		    "        MIN(first_visit_date) first_visit_date, " + 
		    "        MAX(last_visit_date) last_visit_date, " + 
		    "        MAX(visit_days_cnt) visit_days_cnt, " + 
		    "        SUM(trx_cnt) trx_cnt, SUM(trx_reg_cnt) trx_reg_cnt, SUM(trx_sale_cnt) trx_sale_cnt, " + 
		    "        SUM(trx_regonly_cnt) trx_regonly_cnt, SUM(trx_saleonly_cnt) trx_saleonly_cnt, " + 
		    "        ROUND(MAX(primary_store_trx_cnt_ratio), 2) primary_store_trx_cnt_ratio, " + 
		    "        MAX(items_cnt) items_cnt, SUM(li_cnt) li_cnt, SUM(suspense_item_li_cnt) suspense_item_li_cnt, " + 
		    "        SUM(li_reg_cnt) li_reg_cnt, SUM(li_sale_cnt) li_sale_cnt, " + 
		    "        SUM(revenue_net_reg) revenue_net_reg, SUM(revenue_net_sale) revenue_net_sale, " + 
		    "        SUM(revenue_net) revenue_net, round(SUM(revenue_gross), 2) revenue_gross, " + 
		    "        SUM(approximate_visit_cnt) approximate_visit_cnt " +
		    "      FROM " +  lineSeparator +
		    "      (SELECT customer_id, store_id, " + 
		    "            primary_store_id, " + 
		    "            first_visit_date, " + 
		    "            last_visit_date, " + 
		    "            visit_days_cnt, " + 
		    "            trx_cnt, trx_reg_cnt, trx_sale_cnt, " + 
		    "            trx_regonly_cnt, trx_saleonly_cnt, " + 
		    "            CASE WHEN store_id = primary_store_id THEN trx_cnt/SUM(trx_cnt) OVER (PARTITION BY customer_id ) ELSE 0 END primary_store_trx_cnt_ratio, " + 
		    "            items_cnt, li_cnt, suspense_item_li_cnt, " + 
		    "            li_reg_cnt, li_sale_cnt, " + 
		    "            revenue_net_reg, revenue_net_sale, " + 
		    "            revenue_net, revenue_gross, approximate_visit_cnt " + 
		    "          FROM " +  lineSeparator +
		    "            (SELECT customer_id, store_id, " + 
		    "                CASE WHEN (row_number() OVER (PARTITION BY customer_id order BY SUM(trx_cnt) DESC NULLS LAST)) = 1 THEN store_id ELSE 0 END primary_store_id, " + 
		    "                MIN(calendar_id) first_visit_date, " + 
		    "                MAX(calendar_id) last_visit_date, " +
		    "                MAX(visit_days_cnt) visit_days_cnt, " + 
		    "                SUM(trx_cnt) trx_cnt, " + 
		    "                SUM(trx_reg_cnt) trx_reg_cnt, SUM(trx_sale_cnt) trx_sale_cnt, " + 
		    "                SUM(trx_regonly_cnt) trx_regonly_cnt, SUM(trx_saleonly_cnt) trx_saleonly_cnt, " + 
		    "                MAX(items_cnt) items_cnt, SUM(li_cnt) li_cnt, SUM(suspense_item_li_cnt) suspense_item_li_cnt, " + 
		    "                SUM(li_reg_cnt) li_reg_cnt, SUM(li_sale_cnt) li_sale_cnt, " + 
		    "                SUM(revenue_net_reg) revenue_net_reg, SUM(revenue_net_sale) revenue_net_sale, " + 
		    "                SUM(revenue_net) revenue_net, SUM(revenue_gross) revenue_gross, " + 
		    "                SUM(approximate_visit_cnt) approximate_visit_cnt " +
		    "              FROM " +  lineSeparator +
		    
		    "                (SELECT tmp.*, count(distinct calendar_id) over (partition by customer_id) visit_days_cnt from " +
		    
		    "                (SELECT customer_id, calendar_id, store_id, " + 
		    "                    count(distinct trx_no) trx_cnt, " + 
		    "                    SUM(CASE WHEN li_reg_cnt > 0 THEN 1 ELSE 0 END) trx_reg_cnt, SUM(CASE WHEN li_sale_cnt > 0 THEN 1 ELSE 0 END) trx_sale_cnt, " + 
		    "                    SUM(CASE WHEN li_sale_cnt = 0 THEN 1 ELSE 0 END) trx_regonly_cnt, SUM(CASE WHEN li_reg_cnt = 0 THEN 1 ELSE 0 END) trx_saleonly_cnt, " + 
		    "                    MAX(items_cnt) items_cnt, SUM(li_cnt) li_cnt, SUM(suspense_item_li_cnt) suspense_item_li_cnt, " + 
		    "                    SUM(li_reg_cnt) li_reg_cnt, SUM(li_sale_cnt) li_sale_cnt, " + 
		    "                    SUM(revenue_net_reg) revenue_net_reg, SUM(revenue_net_sale) revenue_net_sale, " + 
		    "                    SUM(revenue_net) revenue_net, SUM(revenue_gross) revenue_gross, " + 
		    "                    sum(case when minutes is null then 1 when minutes > 120 then 1 else 0 end) approximate_visit_cnt " +
		    "                  FROM " +  lineSeparator +
		    
		    "                    (SELECT tmp.*, ROUND((trx_time_max - ( lag(trx_time_max, 1) over (partition by customer_id, calendar_id, store_id order by customer_id, calendar_id, store_id, trx_time_max))) * 24 * 60, 2) minutes from " + lineSeparator +
		    
		    "                      (SELECT customer_id, calendar_id, store_id, trx_no, " + 
		    "                          MAX(items_cnt) items_cnt, SUM(li_cnt) li_cnt, SUM(suspense_item_li_cnt) suspense_item_li_cnt, " + 
		    "                          SUM(li_reg_cnt) li_reg_cnt, SUM(li_sale_cnt) li_sale_cnt, " + 
		    "                          SUM(revenue_net_reg) revenue_net_reg, SUM(revenue_net_sale) revenue_net_sale, " + 
		    "                          SUM(revenue_net) revenue_net, SUM(revenue_gross) revenue_gross, " +
		    "                          MAX(trx_time_max) trx_time_max " +
		    "                        FROM " +  lineSeparator +
		    
			"                          (SELECT tmp.*, count(distinct item_id) over (partition by customer_id) items_cnt from " + lineSeparator +
			
		    "                            (SELECT customer_id, calendar_id, store_id, trx_no, item_id, " + 
		    "                                count(item_id) li_cnt, SUM(CASE WHEN item_id <= 0 THEN 1 ELSE 0 END) suspense_item_li_cnt, " + 
		    "                                SUM(CASE WHEN sale_type = 'N' THEN 1 ELSE 0 END) li_reg_cnt, SUM(CASE WHEN sale_type = 'Y' THEN 1 ELSE 0 END) li_sale_cnt, " + 
		    "                                SUM(CASE WHEN sale_type = 'N' THEN net_amt ELSE 0 END) revenue_net_reg, SUM(CASE WHEN sale_type = 'Y' THEN net_amt ELSE 0 END) revenue_net_sale, " + 
		    "                                SUM(net_amt) revenue_net, SUM(regular_amt * (quantity + weight)) revenue_gross, " + 
		    "                                MAX(trx_time) trx_time_max " +
		    "                              FROM  " +  lineSeparator +
		    "                                (SELECT * " + 
		    "                                  FROM  " +  lineSeparator +
		    "                                    " + tLog + 
		    "                                  WHERE calendar_id IN  " +  lineSeparator +
		    "                                    (SELECT calendar_id FROM retail_calendar WHERE start_date >= TO_DATE('" + analysisPeriodStart + "','MM/dd/yyyy') AND start_date   <= TO_DATE('" + analysisPeriodEnd + "','MM/dd/yyyy') AND row_type = 'D') " + lineSeparator + 
		    //"                                    and customer_id IN ( " + customerIdString + " ) " + 
		    "                                ) " +  lineSeparator +
		    "                              GROUP BY customer_id, calendar_id, store_id, trx_no, item_id) tmp" + lineSeparator +
		    
		    "                          )     " + lineSeparator +
		    
		    "                        GROUP BY customer_id, calendar_id, store_id, trx_no " +  lineSeparator +
		    "                      ) tmp " +  lineSeparator +
		    
		    "                    )" +  lineSeparator +
		    
		    "                  GROUP BY customer_id, calendar_id, store_id) tmp " +  lineSeparator +
		    
		    "                  ) " +  lineSeparator +
		    
		    "              GROUP BY customer_id, store_id) cst_str_stats) " +  lineSeparator +
		    "      GROUP BY customer_id)  " +  lineSeparator +
		    "    LEFT JOIN retail_calendar fvd_rc ON first_visit_date = fvd_rc.calendar_id " + 
		    "    LEFT JOIN retail_calendar lvd_rc ON last_visit_date = lvd_rc.calendar_id");		    

	    	// Execution of large query.  Takes long time!
		    // ??? May fail when the number of transaction log records is very huge!
	    	PristineDBUtil.execute(conn, sql, "performTrxSummary() - Analyse and Save Customer Trx Summary.");
	    	conn.commit();
	    	
	    	return true;
	    }
	    catch (GeneralException e) {
	    	logger.error("performTrxSummary() - GeneralException=" + e.getMessage());
			e.printStackTrace();			
	    	return false;
	    }
		catch (SQLException e) {
			logger.error("performTrxSummary() - SQLException=" + e.getMessage());
			e.printStackTrace();
			return false;
	    }
		catch (Exception e) {
			logger.error("performTrxSummary() - Exception=" + e.getMessage());
			e.printStackTrace();
			return false;
	    }
	}
	
	
	public boolean insertHouseholdSalesSummaryDaily (Connection conn, 
			HashMap<Integer, HouseholdSummaryDTO> householdMap, int storeId, 
			int calendarId) throws GeneralException,SQLException {	

		PreparedStatement statement = null;

		try{				    	  
			
			statement = conn.prepareStatement(INSERT_SALES_SUMMARY_DAILY);
			
			int itemNoInBatch = 0;

			HouseholdSummaryDTO ObjSummaryDto = new HouseholdSummaryDTO();
			for (Integer householdKey : householdMap.keySet()) {
				ObjSummaryDto = new HouseholdSummaryDTO();
				ObjSummaryDto = householdMap.get(householdKey);
				int counter = 0;					
				// CUSTOMER_ID	NUMBER(15,0)
				statement.setDouble(++counter, ObjSummaryDto.getCustomerId());
				
				// CALENDAR_ID	NUMBER(5,0)
				statement.setDouble(++counter, calendarId);
				
				// STORE_ID	NUMBER(5,0)
				statement.setInt(++counter, storeId);
				
				// TOTAL_VISITS	NUMBER(3,0)
				statement.setDouble(++counter, ObjSummaryDto.getTotalVisit());
				
				// TOT_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalUnit());
				
				// REG_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularUnit());
				
				// SALE_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleUnit());
				
				// TOT_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalVolume());
				
				// REG_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularVolume());
				
				// SALE_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleVolume());
				
				// TOT_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalRevenue());
				
				// REG_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularRevenue());
				
				// SALE_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleRevenue());
				
				// TOT_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalMargin());
				
				// REG_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularMargin());
				
				// SALE_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleMargin());
				
				// TOT_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalMarginPct());
				
				// REG_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularMarginPct());
				
				// SALE_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleMarginPct());
				
				// PL_TOTAL_VISITS	NUMBER(3,0)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalVisit()); 				
				
				// PL_TOT_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalUnit());
				
				// PL_REG_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLRegularUnit());
				
				// PL_SALE_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLSaleUnit());
				
				// PL_TOT_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalVolume());
				
				// PL_REG_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLRegularVolume());
				
				// PL_SALE_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLSaleVolume());
				
				// PL_TOT_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalRevenue());
				
				// PL_REG_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLRegularRevenue());
				
				// PL_SALE_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLSaleRevenue());
				
				// PL_TOT_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalMargin());
				
				// PL_REG_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLRegularMargin());
				
				// PL_SALE_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLSaleMargin());
				
				// PL_TOT_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalMarginPct());
				
				// PL_REG_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLRegularMarginPct());
				
				// PL_SALE_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLSaleMarginPct());
				
				statement.addBatch();
				itemNoInBatch++;
				
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
					long startTime = System.currentTimeMillis();
					int[] count = statement.executeBatch();
					long endTime = System.currentTimeMillis();
					logger.debug("Time taken for inserting a batch:" + (endTime - startTime) + "ms");
					statement.clearBatch();
					itemNoInBatch = 0;
					PristineDBUtil.commitTransaction(conn, "Data Load");
					
					logger.debug("The number of household summary inserted:"+count.length);
				}
			}
	
			if(itemNoInBatch > 0){
				int[] count = statement.executeBatch();
				PristineDBUtil.commitTransaction(conn, "Data Load");
				statement.clearBatch();
				logger.debug("The number of records inserted:"+count.length);
			}		
		}
		catch (SQLException se)
		{
			logger.error("Error while executing insert Household Sales Summary Daily " + se);
			throw new GeneralException("Error while executing insert Household Sales Summary Daily", se);
		}
		catch (GeneralException ge)
		{
			logger.error("Error while executing insert Household Sales Summary Daily " + ge);
			throw new GeneralException("Error while executing insert Household Sales Summary Daily ", ge);
		}			

		finally{
			PristineDBUtil.close(statement);
		}

		return true;
	}

	public boolean insertHouseholdSalesSummaryRollup (Connection conn, 
			HashMap<Integer, SummaryDataDTO> householdMap, int storeId, 
			int calendarId, String calendarModde) throws GeneralException,SQLException {	

		PreparedStatement statement = null;

		try{				    	  
			
			if (calendarModde.equalsIgnoreCase(Constants.CALENDAR_WEEK))
				statement = conn.prepareStatement(INSERT_SALES_SUMMARY_WEEKLY);
			else if (calendarModde.equalsIgnoreCase(Constants.CALENDAR_PERIOD))
				statement = conn.prepareStatement(INSERT_SALES_SUMMARY_PERIOD);
			
			int itemNoInBatch = 0;

			SummaryDataDTO ObjSummaryDto = new SummaryDataDTO();
			for (Integer householdKey : householdMap.keySet()) {
				ObjSummaryDto = new SummaryDataDTO();
				ObjSummaryDto = householdMap.get(householdKey);
				int counter = 0;					
				
				// CUSTOMER_ID	NUMBER(15,0)
				statement.setDouble(++counter, householdKey);
				
				// CALENDAR_ID	NUMBER(5,0)
				statement.setDouble(++counter, calendarId);
				
				// STORE_ID	NUMBER(5,0)
				statement.setInt(++counter, storeId);
				
				// TOTAL_VISITS	NUMBER(3,0)
				statement.setDouble(++counter, ObjSummaryDto.getTotalVisitCount());
				
				// TOT_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalMovement());
				
				// REG_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularMovement());
				
				// SALE_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleMovement());
				
				// TOT_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.gettotMovementVolume());
				
				// REG_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getregMovementVolume());
				
				// SALE_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getsaleMovementVolume());
				
				// TOT_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalRevenue());
				
				// REG_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularRevenue());
				
				// SALE_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleRevenue());
				
				// TOT_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalMargin());
				
				// REG_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularMargin());
				
				// SALE_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleMargin());
				
				// TOT_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalMarginPer());
				
				// REG_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularMarginPer());
				
				// SALE_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleMarginPer());
				
				// PL_TOTAL_VISITS	NUMBER(3,0)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalVisitCount());				
				
				// PL_TOT_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalMovement());
				
				// PL_REG_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLRegularMovement());
				
				// PL_SALE_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLSaleMovement());
				
				// PL_TOT_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLtotMovementVolume());
				
				// PL_REG_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLregMovementVolume());
				
				// PL_SALE_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLsaleMovementVolume());
				
				// PL_TOT_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalRevenue());
				
				// PL_REG_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLRegularRevenue());
				
				// PL_SALE_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLSaleRevenue());
				
				// PL_TOT_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalMargin());
				
				// PL_REG_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLRegularMargin());
				
				// PL_SALE_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLSaleMargin());
				
				// PL_TOT_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLTotalMarginPer());
				
				// PL_REG_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLRegularMarginPer());
				
				// PL_SALE_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getPLSaleMarginPer());
				
				statement.addBatch();
				itemNoInBatch++;
				
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
					long startTime = System.currentTimeMillis();
					int[] count = statement.executeBatch();
					long endTime = System.currentTimeMillis();
					logger.debug("Time taken for inserting a batch:" + (endTime - startTime) + "ms");
					statement.clearBatch();
					itemNoInBatch = 0;
					PristineDBUtil.commitTransaction(conn, "Data Load");
					logger.debug("The number of household summary inserted:"+count.length);
				}
			}
	
			if(itemNoInBatch > 0){
				int[] count = statement.executeBatch();
				PristineDBUtil.commitTransaction(conn, "Data Load");
				statement.clearBatch();
				logger.debug("The number of records inserted:"+count.length);
			}		
		}
		catch (SQLException se)
		{
			logger.error("Error while executing insert Household Sales Summary Daily " + se);
			throw new GeneralException("Error while executing insert Household Sales Summary Daily", se);
		}
		catch (GeneralException ge)
		{
			logger.error("Error while executing insert Household Sales Summary Daily " + ge);
			throw new GeneralException("Error while executing insert Household Sales Summary Daily ", ge);
		}			

		finally{
			PristineDBUtil.close(statement);
		}

		return true;
	}
		
	public boolean insertItemSalesSummaryDaily (Connection conn, 
			HashMap<Integer, HashMap<Integer, HouseholdSummaryDTO>> householdMap, int storeId, 
			int calendarId) throws GeneralException,SQLException {	

		PreparedStatement statement = null;

		try{				    	  
			
			statement = conn.prepareStatement(INSERT_ITEM_SUMMARY_DAILY);
			
			int itemNoInBatch = 0;

			HouseholdSummaryDTO ObjSummaryDto = new HouseholdSummaryDTO();
			
			HashMap<Integer, HouseholdSummaryDTO> itemMap = new HashMap<Integer, HouseholdSummaryDTO>();
			
			for (Integer householdKey : householdMap.keySet()) {
				itemMap = new HashMap<Integer, HouseholdSummaryDTO>();
				itemMap = householdMap.get(householdKey);
				
				for (Integer itemKey : itemMap.keySet()) {
					ObjSummaryDto = new HouseholdSummaryDTO();
					ObjSummaryDto = itemMap.get(itemKey);
					int counter = 0;					

					// CUSTOMER_ID NUMBER(15,0)
					statement.setDouble(++counter, ObjSummaryDto.getCustomerId());
				
					// CALENDAR_ID	NUMBER(5,0)
					statement.setDouble(++counter, calendarId);
					
					// STORE_ID	NUMBER(5,0)
					statement.setInt(++counter, storeId);
					
					// ITEM_CODE
					statement.setInt(++counter, itemKey);
					

					// TOTAL_VISITS	NUMBER(3,0)
					statement.setDouble(++counter, ObjSummaryDto.getTotalVisit());
				
					// TOT_MOVEMENT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getTotalUnit());
					
					// REG_MOVEMENT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getRegularUnit());
					
					// SALE_MOVEMENT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getSaleUnit());
				
					// TOT_MOVEMENT_VOL	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getTotalVolume());
					
					// REG_MOVEMENT_VOL	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getRegularVolume());
					
					// SALE_MOVEMENT_VOL	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getSaleVolume());
				
					// TOT_REVENUE	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getTotalRevenue());
					
					// REG_REVENUE	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getRegularRevenue());
					
					// SALE_REVENUE	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getSaleRevenue());
					
					// TOT_MARGIN	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getTotalMargin());
				
					// REG_MARGIN	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getRegularMargin());
					
					// SALE_MARGIN	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getSaleMargin());
					
					// TOT_MARGIN_PCT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getTotalMarginPct());
					
					// REG_MARGIN_PCT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getRegularMarginPct());
					
					// SALE_MARGIN_PCT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getSaleMarginPct());
					
					statement.addBatch();
					itemNoInBatch++;
				
					if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
						long startTime = System.currentTimeMillis();
						int[] count = statement.executeBatch();
						long endTime = System.currentTimeMillis();
						logger.debug("Time taken for inserting a batch:" + (endTime - startTime) + "ms");
						statement.clearBatch();
						itemNoInBatch = 0;
						PristineDBUtil.commitTransaction(conn, "Data Load");
						logger.debug("The number of item records inserted:"+count.length);
					}
				}
			}

			if(itemNoInBatch > 0){
				int[] count = statement.executeBatch();
				PristineDBUtil.commitTransaction(conn, "Data Load");
				statement.clearBatch();
				logger.debug("The number of records inserted:"+count.length);
			}		
		}
		catch (SQLException se)
		{
			logger.error("Error while executing insert Household Sales Summary Daily " + se);
			throw new GeneralException("Error while executing insert Household Sales Summary Daily", se);
		}
		catch (GeneralException ge)
		{
			logger.error("Error while executing insert Household Sales Summary Daily " + ge);
			throw new GeneralException("Error while executing insert Household Sales Summary Daily ", ge);
		}			

		finally{
			PristineDBUtil.close(statement);
		}

		return true;
	}

	public boolean insertItemSalesSummaryRollup (Connection conn, 
			HashMap<Integer, HashMap<Integer, HouseholdSummaryDTO>> householdMap, int storeId, 
			int calendarId, String calendarMode) throws GeneralException,SQLException {	

		PreparedStatement statement = null;

		try{				    	  
			
			if (calendarMode.equalsIgnoreCase(Constants.CALENDAR_WEEK))
				statement = conn.prepareStatement(INSERT_ITEM_SUMMARY_WEEKLY);
			else if (calendarMode.equalsIgnoreCase(Constants.CALENDAR_PERIOD))
				statement = conn.prepareStatement(INSERT_ITEM_SUMMARY_PERIOD);
			
			int itemNoInBatch = 0;

			HouseholdSummaryDTO ObjSummaryDto = new HouseholdSummaryDTO();
			
			HashMap<Integer, HouseholdSummaryDTO> itemMap = new HashMap<Integer, HouseholdSummaryDTO>();
			
			for (Integer householdKey : householdMap.keySet()) {
				itemMap = new HashMap<Integer, HouseholdSummaryDTO>();
				itemMap = householdMap.get(householdKey);
				
				for (Integer itemKey : itemMap.keySet()) {
					ObjSummaryDto = new HouseholdSummaryDTO();
					ObjSummaryDto = itemMap.get(itemKey);
					int counter = 0;					

					// CUSTOMER_ID	NUMBER(15,0)
					statement.setDouble(++counter, householdKey);
				
					// CALENDAR_ID	NUMBER(5,0)
					statement.setDouble(++counter, calendarId);
					
					// STORE_ID	NUMBER(5,0)
					statement.setInt(++counter, storeId);
					
					// ITEM_CODE
					statement.setInt(++counter, itemKey);
					

					// TOTAL_VISITS	NUMBER(3,0)
					statement.setDouble(++counter, ObjSummaryDto.getTotalVisit());
				
					// TOT_MOVEMENT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getTotalUnit());
					
					// REG_MOVEMENT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getRegularUnit());
					
					// SALE_MOVEMENT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getSaleUnit());
				
					// TOT_MOVEMENT_VOL	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getTotalVolume());
					
					// REG_MOVEMENT_VOL	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getRegularVolume());
					
					// SALE_MOVEMENT_VOL	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getSaleVolume());
				
					// TOT_REVENUE	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getTotalRevenue());
					
					// REG_REVENUE	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getRegularRevenue());
					
					// SALE_REVENUE	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getSaleRevenue());
					
					// TOT_MARGIN	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getTotalMargin());
				
					// REG_MARGIN	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getRegularMargin());
					
					// SALE_MARGIN	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getSaleMargin());
					
					// TOT_MARGIN_PCT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getTotalMarginPct());
					
					// REG_MARGIN_PCT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getRegularMarginPct());
					
					// SALE_MARGIN_PCT	NUMBER(10,2)
					statement.setDouble(++counter, ObjSummaryDto.getSaleMarginPct());
					
					statement.addBatch();
					itemNoInBatch++;
				
					if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
						long startTime = System.currentTimeMillis();
						int[] count = statement.executeBatch();
						long endTime = System.currentTimeMillis();
						logger.debug("Time taken for inserting a batch:" + (endTime - startTime) + "ms");
						statement.clearBatch();
						itemNoInBatch = 0;
						PristineDBUtil.commitTransaction(conn, "Data Load");
						logger.debug("The number of item records inserted:"+count.length);
					}
				}
			}

			if(itemNoInBatch > 0){
				int[] count = statement.executeBatch();
				PristineDBUtil.commitTransaction(conn, "Data Load");
				statement.clearBatch();
				logger.debug("The number of records inserted:"+count.length);
			}		
		}
		catch (SQLException se)
		{
			logger.error("Error while executing insert Household Sales Summary Daily " + se);
			throw new GeneralException("Error while executing insert Household Sales Summary Daily", se);
		}
		catch (GeneralException ge)
		{
			logger.error("Error while executing insert Household Sales Summary Daily " + ge);
			throw new GeneralException("Error while executing insert Household Sales Summary Daily ", ge);
		}			

		finally{
			PristineDBUtil.close(statement);
		}

		return true;
	}

	public boolean insertItemSalesSummaryRollupExisting (Connection conn, 
			HashMap<Integer, SummaryDataDTO> householdMap, int storeId, 
			int calendarId, String calendarModde) throws GeneralException,SQLException {	

		PreparedStatement statement = null;

		try{				    	  
			
			if (calendarModde.equalsIgnoreCase(Constants.CALENDAR_WEEK))
				statement = conn.prepareStatement(INSERT_ITEM_SUMMARY_WEEKLY);
			else if (calendarModde.equalsIgnoreCase(Constants.CALENDAR_PERIOD))
				statement = conn.prepareStatement(INSERT_ITEM_SUMMARY_PERIOD);
			
			int itemNoInBatch = 0;

			SummaryDataDTO ObjSummaryDto = new SummaryDataDTO();
			for (Integer householdKey : householdMap.keySet()) {
				ObjSummaryDto = new SummaryDataDTO();
				ObjSummaryDto = householdMap.get(householdKey);
				int counter = 0;					
				int itemCode = Integer.parseInt(ObjSummaryDto.getProductId());
				
				// CUSTOMER_ID	NUMBER(15,0)
				statement.setDouble(++counter, householdKey);
				
				// CALENDAR_ID	NUMBER(5,0)
				statement.setDouble(++counter, calendarId);
				
				// STORE_ID	NUMBER(5,0)
				statement.setInt(++counter, storeId);
				
				// ITEM_CODE
				statement.setInt(++counter, itemCode);

				// TOTAL_VISITS	NUMBER(3,0)
				statement.setDouble(++counter, ObjSummaryDto.getTotalVisitCount());
				
				// TOT_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalMovement());
				
				// REG_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularMovement());
				
				// SALE_MOVEMENT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleMovement());
				
				// TOT_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.gettotMovementVolume());
				
				// REG_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getregMovementVolume());
				
				// SALE_MOVEMENT_VOL	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getsaleMovementVolume());
				
				// TOT_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalRevenue());
				
				// REG_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularRevenue());
				
				// SALE_REVENUE	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleRevenue());
				
				// TOT_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalMargin());
				
				// REG_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularMargin());
				
				// SALE_MARGIN	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleMargin());
				
				// TOT_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getTotalMarginPer());
				
				// REG_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getRegularMarginPer());
				
				// SALE_MARGIN_PCT	NUMBER(10,2)
				statement.setDouble(++counter, ObjSummaryDto.getSaleMarginPer());
				
				statement.addBatch();
				itemNoInBatch++;
				
				if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
					long startTime = System.currentTimeMillis();
					int[] count = statement.executeBatch();
					long endTime = System.currentTimeMillis();
					logger.debug("Time taken for inserting a batch:" + (endTime - startTime) + "ms");
					statement.clearBatch();
					itemNoInBatch = 0;
					PristineDBUtil.commitTransaction(conn, "Data Load");
					logger.debug("The number of household summary inserted:"+count.length);
				}
			}
	
			if(itemNoInBatch > 0){
				int[] count = statement.executeBatch();
				PristineDBUtil.commitTransaction(conn, "Data Load");
				statement.clearBatch();
				logger.debug("The number of records inserted:"+count.length);
			}		
		}
		catch (SQLException se)
		{
			logger.error("Error while executing insert Household Sales Summary Daily " + se);
			throw new GeneralException("Error while executing insert Household Sales Summary Daily", se);
		}
		catch (GeneralException ge)
		{
			logger.error("Error while executing insert Household Sales Summary Daily " + ge);
			throw new GeneralException("Error while executing insert Household Sales Summary Daily ", ge);
		}			

		finally{
			PristineDBUtil.close(statement);
		}

		return true;
	}
	
	public HashMap<Integer, SummaryDataDTO> getHouseholdSummaryRollupData(
			Connection _Conn, int storeId,  RetailCalendarDTO objCalendarDto,
								String calendarMode) throws GeneralException {
		HashMap<Integer, SummaryDataDTO> householdMap = new HashMap<Integer, SummaryDataDTO>();
		
		StringBuffer sql = new StringBuffer();
		
		sql.append(" SELECT CUSTOMER_ID,");
		sql.append(" SUM(TOTAL_VISITS) AS TOTAL_VISITS,");
		sql.append(" SUM(TOT_MOVEMENT) AS TOT_MOVEMENT,");
		sql.append(" SUM(REG_MOVEMENT) AS REG_MOVEMENT,");
		sql.append(" SUM(SALE_MOVEMENT) AS SALE_MOVEMENT,");
		sql.append(" SUM(TOT_MOVEMENT_VOL) AS TOT_MOVEMENT_VOL,");
		sql.append(" SUM(REG_MOVEMENT_VOL) AS REG_MOVEMENT_VOL,");
		sql.append(" SUM(SALE_MOVEMENT_VOL) AS SALE_MOVEMENT_VOL,");
		sql.append(" SUM(TOT_REVENUE) AS TOT_REVENUE,");
		sql.append(" SUM(REG_REVENUE) AS REG_REVENUE,");
		sql.append(" SUM(SALE_REVENUE) AS SALE_REVENUE,");
		sql.append(" SUM(TOT_MARGIN) AS TOT_MARGIN,");
		sql.append(" SUM(REG_MARGIN) AS REG_MARGIN,");
		sql.append(" SUM(SALE_MARGIN) AS SALE_MARGIN,");
		sql.append(" SUM(PL_TOT_MOVEMENT) AS PL_TOT_MOVEMENT,");
		sql.append(" SUM(PL_REG_MOVEMENT) AS PL_REG_MOVEMENT,");
		sql.append(" SUM(PL_SALE_MOVEMENT) AS PL_SALE_MOVEMENT,");
		sql.append(" SUM(PL_TOT_MOVEMENT_VOL) AS PL_TOT_MOVEMENT_VOL,");
		sql.append(" SUM(PL_REG_MOVEMENT_VOL) AS PL_REG_MOVEMENT_VOL,");
		sql.append(" SUM(PL_SALE_MOVEMENT_VOL) AS PL_SALE_MOVEMENT_VOL,");
		sql.append(" SUM(PL_TOT_REVENUE) AS PL_TOT_REVENUE,");
		sql.append(" SUM(PL_REG_REVENUE) AS PL_REG_REVENUE,");
		sql.append(" SUM(PL_SALE_REVENUE) AS PL_SALE_REVENUE,");
		sql.append(" SUM(PL_TOT_MARGIN) AS PL_TOT_MARGIN,");
		sql.append(" SUM(PL_REG_MARGIN) AS PL_REG_MARGIN,");
		sql.append(" SUM(PL_SALE_MARGIN) AS PL_SALE_MARGIN");
		sql.append(" FROM CST_SUMMARY_DAILY");
		sql.append(" WHERE CALENDAR_ID IN (");
				
		sql.append(" SELECT CALENDAR_ID FROM RETAIL_CALENDAR");
		sql.append(" WHERE START_DATE BETWEEN");
		sql.append (" TO_DATE('").append(objCalendarDto.getStartDate()).append("', 'DD/MM/YYYY')");
		sql.append(" AND TO_DATE('").append(objCalendarDto.getEndDate()).append("', 'DD/MM/YYYY')");
		sql.append(" AND ROW_TYPE = 'D')");
		
		sql.append(" AND STORE_ID = ").append(storeId);
		sql.append(" GROUP BY CUSTOMER_ID");
		
		logger.debug("getHouseholdSummaryRollupData SQL:" + sql.toString());
		
		try {
		CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
			"getHouseholdSummaryRollupData");
		
		
		if (rst.size() > 0 ) {
			while (rst.next()) {
				SummaryDataDTO objSummaryDao = new SummaryDataDTO();
				int customerId = rst.getInt("CUSTOMER_ID");

				double totalMarginPct =0;
				double regularMarginPct =0;
				double saleMarginPct =0;
				double plTotalMarginPct =0;
				double plRegularMarginPct =0;
				double plSaleMarginPct =0;

				objSummaryDao.setTotalVisitCount(rst.getDouble("TOTAL_VISITS"));

				objSummaryDao.setTotalMovement(rst.getDouble("TOT_MOVEMENT"));
				objSummaryDao.setRegularMovement(rst.getDouble("REG_MOVEMENT"));
				objSummaryDao.setSaleMovement(rst.getDouble("SALE_MOVEMENT"));				

				objSummaryDao.settotMovementVolume(rst.getDouble("TOT_MOVEMENT_VOL"));
				objSummaryDao.setregMovementVolume(rst.getDouble("REG_MOVEMENT_VOL"));
				objSummaryDao.setsaleMovementVolume(rst.getDouble("SALE_MOVEMENT_VOL"));

				objSummaryDao.setTotalRevenue(rst.getDouble("TOT_REVENUE"));
				objSummaryDao.setRegularRevenue(rst.getDouble("REG_REVENUE"));
				objSummaryDao.setSaleRevenue(rst.getDouble("SALE_REVENUE"));

				objSummaryDao.setTotalMargin(rst.getDouble("TOT_MARGIN"));
				objSummaryDao.setRegularMargin(rst.getDouble("REG_MARGIN"));
				objSummaryDao.setSaleMargin(rst.getDouble("SALE_MARGIN"));

				if (objSummaryDao.getTotalRevenue() != 0 && objSummaryDao.getTotalMargin() != 0) 
					totalMarginPct = objSummaryDao.getTotalMargin() / objSummaryDao.getTotalRevenue() * 100;

				if (objSummaryDao.getRegularRevenue() != 0 && objSummaryDao.getRegularMargin() != 0) 
					regularMarginPct = objSummaryDao.getRegularMargin() / objSummaryDao.getRegularRevenue() * 100;

				if (objSummaryDao.getSaleRevenue() != 0 && objSummaryDao.getSaleMargin() != 0) 
					saleMarginPct = objSummaryDao.getSaleMargin() / objSummaryDao.getSaleRevenue() * 100;

				objSummaryDao.setTotalMarginPer(totalMarginPct);
				objSummaryDao.setRegularMarginPer(regularMarginPct);
				objSummaryDao.setSaleMarginPer(saleMarginPct);
				
				objSummaryDao.setPLTotalMovement(rst.getDouble("PL_TOT_MOVEMENT"));
				objSummaryDao.setPLRegularMovement(rst.getDouble("PL_REG_MOVEMENT"));
				objSummaryDao.setPLSaleMovement(rst.getDouble("PL_SALE_MOVEMENT"));
				
				objSummaryDao.setPLtotMovementVolume(rst.getDouble("PL_TOT_MOVEMENT_VOL"));
				objSummaryDao.setPLregMovementVolume(rst.getDouble("PL_REG_MOVEMENT_VOL"));
				objSummaryDao.setPLsaleMovementVolume(rst.getDouble("PL_SALE_MOVEMENT_VOL"));
				
				objSummaryDao.setPLTotalRevenue(rst.getDouble("PL_TOT_REVENUE"));
				objSummaryDao.setPLRegularRevenue(rst.getDouble("PL_REG_REVENUE"));
				objSummaryDao.setPLSaleRevenue(rst.getDouble("PL_SALE_REVENUE"));
				
				objSummaryDao.setPLTotalMargin(rst.getDouble("PL_TOT_MARGIN"));
				objSummaryDao.setPLRegularMargin(rst.getDouble("PL_REG_MARGIN"));
				objSummaryDao.setPLSaleMargin(rst.getDouble("PL_SALE_MARGIN"));

				if (objSummaryDao.getPLTotalRevenue() != 0 && objSummaryDao.getPLTotalMargin() != 0) 
					plTotalMarginPct = objSummaryDao.getPLTotalMargin() / objSummaryDao.getPLTotalRevenue() * 100;

				if (objSummaryDao.getPLRegularRevenue() != 0 && objSummaryDao.getPLRegularMargin() != 0) 
					plRegularMarginPct = objSummaryDao.getPLRegularMargin() / objSummaryDao.getPLRegularRevenue() * 100;

				if (objSummaryDao.getPLSaleRevenue() != 0 && objSummaryDao.getPLSaleMargin() != 0) 
					plSaleMarginPct = objSummaryDao.getPLSaleMargin() / objSummaryDao.getPLSaleRevenue() * 100;

				objSummaryDao.setPLTotalMarginPer(plTotalMarginPct);
				objSummaryDao.setPLRegularMarginPer(plRegularMarginPct);
				objSummaryDao.setPLSaleMarginPer(plSaleMarginPct);
				
				householdMap.put(customerId, objSummaryDao);
			}
		}
		else
		{
			logger.warn("There is no movement data");
		}
		
		/*logger.info(" Daily Summary Process Count " + movementDataList.size());*/
		
		rst.close();
				
		
		} catch (GeneralException exe) {
		logger.error(" Error while fetching the results.... ",exe);
		throw  new GeneralException(" Error while fetching the results.... ",exe);
		} catch (SQLException exe) {
		logger.error(" Error while fetching the results.... ",exe);
		throw  new GeneralException(" Error while fetching the results.... ",exe);
		}
		
		return householdMap;
}

	public HashMap<Integer, HashMap<Integer, HouseholdSummaryDTO>>  
		getItemSummaryRollupData(Connection _Conn, int storeId,  
		RetailCalendarDTO objCalendarDto, String calendarMode) 
										throws GeneralException {
		
		HashMap<Integer, HashMap<Integer, HouseholdSummaryDTO>> householdMap = 
				new HashMap<Integer, HashMap<Integer, HouseholdSummaryDTO>>(); 
		
		StringBuffer sql = new StringBuffer();
		
		sql.append(" SELECT CUSTOMER_ID, ITEM_CODE,");
		sql.append(" SUM(TOTAL_VISITS) AS TOTAL_VISITS,");
		sql.append(" SUM(TOT_MOVEMENT) AS TOT_MOVEMENT,");
		sql.append(" SUM(REG_MOVEMENT) AS REG_MOVEMENT,");
		sql.append(" SUM(SALE_MOVEMENT) AS SALE_MOVEMENT,");
		sql.append(" SUM(TOT_MOVEMENT_VOL) AS TOT_MOVEMENT_VOL,");
		sql.append(" SUM(REG_MOVEMENT_VOL) AS REG_MOVEMENT_VOL,");
		sql.append(" SUM(SALE_MOVEMENT_VOL) AS SALE_MOVEMENT_VOL,");
		sql.append(" SUM(TOT_REVENUE) AS TOT_REVENUE,");
		sql.append(" SUM(REG_REVENUE) AS REG_REVENUE,");
		sql.append(" SUM(SALE_REVENUE) AS SALE_REVENUE,");
		sql.append(" SUM(TOT_MARGIN) AS TOT_MARGIN,");
		sql.append(" SUM(REG_MARGIN) AS REG_MARGIN,");
		sql.append(" SUM(SALE_MARGIN) AS SALE_MARGIN");
		sql.append(" FROM CST_SUMMARY_DAILY_ITEM");
		sql.append(" WHERE CALENDAR_ID IN (");
				
		sql.append(" SELECT CALENDAR_ID FROM RETAIL_CALENDAR");
		sql.append(" WHERE START_DATE BETWEEN");
		sql.append (" TO_DATE('").append(objCalendarDto.getStartDate()).append("', 'DD/MM/YYYY')");
		sql.append(" AND TO_DATE('").append(objCalendarDto.getEndDate()).append("', 'DD/MM/YYYY')");
		sql.append(" AND ROW_TYPE = 'D')");
		
		sql.append(" AND STORE_ID = ").append(storeId);
		sql.append(" GROUP BY CUSTOMER_ID, ITEM_CODE");
		sql.append(" ORDER BY CUSTOMER_ID");
		
		logger.debug("getItemSummaryRollupData SQL:" + sql.toString());
		
		try {
		CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
			"getItemSummaryRollupData");
		
		HashMap<Integer, HouseholdSummaryDTO> itemMap = 
				new HashMap<Integer, HouseholdSummaryDTO>();
		
		int preHouseholdNo = 0;
		
		if (rst.size() > 0 ) {
			while (rst.next()) {
				HouseholdSummaryDTO objSummaryDao = new HouseholdSummaryDTO();
				int customerId = rst.getInt("CUSTOMER_ID");
				int itemCode = rst.getInt("ITEM_CODE");

				if (preHouseholdNo > 0 && preHouseholdNo != customerId){
					householdMap.put(preHouseholdNo, itemMap);
					itemMap = new HashMap<Integer, HouseholdSummaryDTO>();
				}
				
				double totalMarginPct =0;
				double regularMarginPct =0;
				double saleMarginPct =0;

				objSummaryDao.setTotalVisit(rst.getDouble("TOTAL_VISITS"));

				objSummaryDao.setTotalUnit(rst.getDouble("TOT_MOVEMENT"));
				objSummaryDao.setRegularUnit(rst.getDouble("REG_MOVEMENT"));
				objSummaryDao.setSaleUnit(rst.getDouble("SALE_MOVEMENT"));				

				objSummaryDao.setTotalVolume(rst.getDouble("TOT_MOVEMENT_VOL"));
				objSummaryDao.setRegularVolume(rst.getDouble("REG_MOVEMENT_VOL"));
				objSummaryDao.setSaleVolume(rst.getDouble("SALE_MOVEMENT_VOL"));

				objSummaryDao.setTotalRevenue(rst.getDouble("TOT_REVENUE"));
				objSummaryDao.setRegularRevenue(rst.getDouble("REG_REVENUE"));
				objSummaryDao.setSaleRevenue(rst.getDouble("SALE_REVENUE"));

				objSummaryDao.setTotalMargin(rst.getDouble("TOT_MARGIN"));
				objSummaryDao.setRegularMargin(rst.getDouble("REG_MARGIN"));
				objSummaryDao.setSaleMargin(rst.getDouble("SALE_MARGIN"));

				if (objSummaryDao.getTotalRevenue() != 0 && objSummaryDao.getTotalMargin() != 0) 
					totalMarginPct = objSummaryDao.getTotalMargin() / objSummaryDao.getTotalRevenue() * 100;

				if (objSummaryDao.getRegularRevenue() != 0 && objSummaryDao.getRegularMargin() != 0) 
					regularMarginPct = objSummaryDao.getRegularMargin() / objSummaryDao.getRegularRevenue() * 100;

				if (objSummaryDao.getSaleRevenue() != 0 && objSummaryDao.getSaleMargin() != 0) 
					saleMarginPct = objSummaryDao.getSaleMargin() / objSummaryDao.getSaleRevenue() * 100;

				objSummaryDao.setTotalMarginPct(totalMarginPct);
				objSummaryDao.setRegularMarginPct(regularMarginPct);
				objSummaryDao.setSaleMarginPct(saleMarginPct);
				
				itemMap.put(itemCode, objSummaryDao);
				
				preHouseholdNo =customerId;
			}
			
			if (itemMap != null && itemMap.size() >0)
				householdMap.put(preHouseholdNo, itemMap);
		}
		else
		{
			logger.warn("There is no movement data");
		}
		
		/*logger.info(" Daily Summary Process Count " + movementDataList.size());*/
		
		rst.close();
				
		
		} catch (GeneralException exe) {
		logger.error(" Error while fetching the results.... ",exe);
		throw  new GeneralException(" Error while fetching the results.... ",exe);
		} catch (SQLException exe) {
		logger.error(" Error while fetching the results.... ",exe);
		throw  new GeneralException(" Error while fetching the results.... ",exe);
		}
		
		return householdMap;
}


	
	public HashMap<Integer, SummaryDataDTO> getItemSummaryRollupDataExisting(
			Connection _Conn, int storeId,  RetailCalendarDTO objCalendarDto,
								String calendarMode) throws GeneralException {
		HashMap<Integer, SummaryDataDTO> householdMap = new HashMap<Integer, SummaryDataDTO>();
		
		StringBuffer sql = new StringBuffer();
		
		sql.append(" SELECT CUSTOMER_ID, ITEM_CODE,");
		sql.append(" SUM(TOTAL_VISITS) AS TOTAL_VISITS,");
		sql.append(" SUM(TOT_MOVEMENT) AS TOT_MOVEMENT,");
		sql.append(" SUM(REG_MOVEMENT) AS REG_MOVEMENT,");
		sql.append(" SUM(SALE_MOVEMENT) AS SALE_MOVEMENT,");
		sql.append(" SUM(TOT_MOVEMENT_VOL) AS TOT_MOVEMENT_VOL,");
		sql.append(" SUM(REG_MOVEMENT_VOL) AS REG_MOVEMENT_VOL,");
		sql.append(" SUM(SALE_MOVEMENT_VOL) AS SALE_MOVEMENT_VOL,");
		sql.append(" SUM(TOT_REVENUE) AS TOT_REVENUE,");
		sql.append(" SUM(REG_REVENUE) AS REG_REVENUE,");
		sql.append(" SUM(SALE_REVENUE) AS SALE_REVENUE,");
		sql.append(" SUM(TOT_MARGIN) AS TOT_MARGIN,");
		sql.append(" SUM(REG_MARGIN) AS REG_MARGIN,");
		sql.append(" SUM(SALE_MARGIN) AS SALE_MARGIN");
		sql.append(" FROM CST_SUMMARY_DAILY_ITEM");
		sql.append(" WHERE CALENDAR_ID IN (");
				
		sql.append(" SELECT CALENDAR_ID FROM RETAIL_CALENDAR");
		sql.append(" WHERE START_DATE BETWEEN");
		sql.append (" TO_DATE('").append(objCalendarDto.getStartDate()).append("', 'DD/MM/YYYY')");
		sql.append(" AND TO_DATE('").append(objCalendarDto.getEndDate()).append("', 'DD/MM/YYYY')");
		sql.append(" AND ROW_TYPE = 'D')");
		sql.append(" AND STORE_ID = ").append(storeId);
		
		sql.append(" GROUP BY CUSTOMER_ID, ITEM_CODE");
		
		logger.debug("getItemSummaryRollupData SQL:" + sql.toString());
		
		try {
		CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
			"getItemSummaryRollupData");
		
		
		if (rst.size() > 0 ) {
			while (rst.next()) {
				SummaryDataDTO objSummaryDao = new SummaryDataDTO();
				int customerId = rst.getInt("CUSTOMER_ID");

				double totalMarginPct =0;
				double regularMarginPct =0;
				double saleMarginPct =0;

				objSummaryDao.setProductId(rst.getString("ITEM_CODE"));
				
				objSummaryDao.setTotalVisitCount(rst.getDouble("TOTAL_VISITS"));

				objSummaryDao.setTotalMovement(rst.getDouble("TOT_MOVEMENT"));
				objSummaryDao.setRegularMovement(rst.getDouble("REG_MOVEMENT"));
				objSummaryDao.setSaleMovement(rst.getDouble("SALE_MOVEMENT"));				

				objSummaryDao.settotMovementVolume(rst.getDouble("TOT_MOVEMENT_VOL"));
				objSummaryDao.setregMovementVolume(rst.getDouble("REG_MOVEMENT_VOL"));
				objSummaryDao.setsaleMovementVolume(rst.getDouble("SALE_MOVEMENT_VOL"));

				objSummaryDao.setTotalRevenue(rst.getDouble("TOT_REVENUE"));
				objSummaryDao.setRegularRevenue(rst.getDouble("REG_REVENUE"));
				objSummaryDao.setSaleRevenue(rst.getDouble("SALE_REVENUE"));

				objSummaryDao.setTotalMargin(rst.getDouble("TOT_MARGIN"));
				objSummaryDao.setRegularMargin(rst.getDouble("REG_MARGIN"));
				objSummaryDao.setSaleMargin(rst.getDouble("SALE_MARGIN"));

				if (objSummaryDao.getTotalRevenue() != 0 && objSummaryDao.getTotalMargin() != 0) 
					totalMarginPct = objSummaryDao.getTotalMargin() / objSummaryDao.getTotalRevenue() * 100;

				if (objSummaryDao.getRegularRevenue() != 0 && objSummaryDao.getRegularMargin() != 0) 
					regularMarginPct = objSummaryDao.getRegularMargin() / objSummaryDao.getRegularRevenue() * 100;

				if (objSummaryDao.getSaleRevenue() != 0 && objSummaryDao.getSaleMargin() != 0) 
					saleMarginPct = objSummaryDao.getSaleMargin() / objSummaryDao.getSaleRevenue() * 100;

				objSummaryDao.setTotalMarginPer(totalMarginPct);
				objSummaryDao.setRegularMarginPer(regularMarginPct);
				objSummaryDao.setSaleMarginPer(saleMarginPct);
				
				householdMap.put(customerId, objSummaryDao);
			}
		}
		else
		{
			logger.warn("There is no movement data");
		}
		
		/*logger.info(" Daily Summary Process Count " + movementDataList.size());*/
		
		rst.close();
				
		
		} catch (GeneralException exe) {
		logger.error(" Error while fetching the results.... ",exe);
		throw  new GeneralException(" Error while fetching the results.... ",exe);
		} catch (SQLException exe) {
		logger.error(" Error while fetching the results.... ",exe);
		throw  new GeneralException(" Error while fetching the results.... ",exe);
		}
		
		return householdMap;
}
	
	
}
