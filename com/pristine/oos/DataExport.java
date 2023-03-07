package com.pristine.oos;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.OOSCalcsDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class DataExport {
	private static Logger  logger = Logger.getLogger("OOSDataExport");

	OOSCalcsDAO ossCalcDao = new OOSCalcsDAO();
	RetailCalendarDAO calendarDao = new RetailCalendarDAO();
	Connection conn = null;
	int pastDayCount = 7;
	
	String analysisDate ="";
	String beginDateForTlog ="";
	int strId = -1;
	String storeNum = "";
	String itemListASQL = "";
	String itemListBSQL = "";
	String itemListCSQL = "";
	String itemListDSQL = "";
	String tLogExtractSQL = "";
	String adExtractSQL = "";

	private String dataType ="";
	
	public DataExport(){
		super();
		String sqlFileName = "sql/SQLDefinitions.xml";
		 Properties properties = new Properties();
		 InputStream is = this.getClass().getClassLoader().getResourceAsStream(sqlFileName);
	
		 
		 try {
			properties.loadFromXML(is);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 itemListASQL = properties.getProperty("ItemListASQL");
		 itemListBSQL = properties.getProperty("ItemListBSQL");
		 itemListCSQL = properties.getProperty("ItemListCSQL");
		 itemListDSQL = properties.getProperty("ItemListDSQL");
		 tLogExtractSQL = properties.getProperty("tLogExtractSQL");
		 adExtractSQL = properties.getProperty("adExtractSQL");

	}

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-oos-data-export.properties");
		PropertyManager.initialize("analysis.properties");
 		Date startDate ;
 		String storeNum;
		if(args.length >= 2){
			try {
				storeNum = args[0];
				startDate = DateUtil.toDate(args[1]);
				DataExport de = new DataExport();
				
				de.analysisDate = args[1];
				if(args.length>=3){
					DateUtil.toDate(args[2]); // check validity
					de.beginDateForTlog = args[2];
				}

				if(args.length==4){
					de.dataType  = args[3];
				}

				
				de.exportItemLists(storeNum,args[1]);
			} catch (GeneralException e) {
				
				logger.info("Error exporting Item List:"+e.getMessage());
				System.exit(1);
			} catch (Exception e) {
				
				logger.info("Error exporting List:"+e.getMessage());
				System.exit(1);
			}
		}
		else{ 
			logger.info("Insufficient Arguments - OOS DataExport StoreNumber StartDate(MM/DD/YYYY) ");
			System.exit(1);
		}


	}
	
	public List<String> generateItemListA() throws GeneralException, Exception{
		List<String> iL = new ArrayList<String>();
		String sql = new String(itemListASQL);
		sql = sql.replace("__GE__", " >= ");
		sql = sql.replace("__LT__", " < ");
		sql = sql.replace("__THE_STORE_ID__", " "+strId +" ");
		sql = sql.replace("__PAST_DAY_COUNT__"," " + pastDayCount+" ");
		sql = sql.replace("__ANALYSIS_DATE__", analysisDate);
		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "itemListA");
		generateCSV(crs,"listA");
		logger.info("Item List A count : " + crs.size());
		return iL;
	}

	public List<String> generateItemListBold() throws GeneralException, Exception{
		List<String> iL = new ArrayList<String>();
		String sql = new String(itemListBSQL);
		sql = sql.replace("__GE__", " >= ");
		sql = sql.replace("__LT__", " < ");
		sql = sql.replace("__THE_STORE_ID__", " "+strId +" ");
		sql = sql.replace("__PAST_DAY_COUNT__"," " + pastDayCount+" ");
		sql = sql.replace("__ANALYSIS_DATE__", analysisDate);
		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "itemListB");
		generateCSV(crs,"listB");
		logger.info("Item List A count : " + crs.size());
		return iL;
	}
	
	// Focus items:  Items which moved > 2 units per day during 25 out of the last 28 days + items in last week’s Ad, current week’s Ad and next 2 weeks Ads
	public List<String> generateItemListC() throws GeneralException, Exception{
		List<String> iL = new ArrayList<String>();
		String sql = new String(itemListCSQL);
		sql = sql.replace("__GE__", " >= ");
		sql = sql.replace("__LT__", " < ");
		sql = sql.replace("__THE_STORE_ID__", " "+strId +" ");
		sql = sql.replace("__PAST_DAY_COUNT__"," " + pastDayCount+" ");
		sql = sql.replace("__ANALYSIS_DATE__", analysisDate);
		
		Date aDate = DateUtil.toDate(analysisDate);
		Date theDate = DateUtil.incrementDate(aDate, -7);
		int calendarWeekId1 = (calendarDao.getCalendarId(conn, DateUtil.dateToString(theDate, Constants.APP_DATE_FORMAT), Constants.CALENDAR_WEEK)).getCalendarId();
		int calendarWeekId2 = (calendarDao.getCalendarId(conn, analysisDate, Constants.CALENDAR_WEEK)).getCalendarId();
		theDate = DateUtil.incrementDate(aDate, 7);
		int calendarWeekId3 = (calendarDao.getCalendarId(conn, DateUtil.dateToString(theDate, Constants.APP_DATE_FORMAT), Constants.CALENDAR_WEEK)).getCalendarId();
		theDate = DateUtil.incrementDate(theDate, 7);
		int calendarWeekId4 = (calendarDao.getCalendarId(conn, DateUtil.dateToString(theDate, Constants.APP_DATE_FORMAT), Constants.CALENDAR_WEEK)).getCalendarId();

		sql = sql.replace("__CAL_ID_1__", calendarWeekId1 +"");
		sql = sql.replace("__CAL_ID_2__", calendarWeekId2 +"");
		sql = sql.replace("__CAL_ID_3__", calendarWeekId3 +"");
		sql = sql.replace("__CAL_ID_4__", calendarWeekId4 +"");

		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "itemListC");
		generateCSV(crs,"listDayLevel");
		logger.info("Item List C count : " + crs.size());
		return iL;
	}
	
	// Focus items:  Items which moved > 2 units per day during 25 out of the last 28 days + items in Analysis Week
	public List<String> generateItemListB() throws GeneralException, Exception{
		List<String> iL = new ArrayList<String>();
		String sql = new String(itemListBSQL);
		sql = sql.replace("__GE__", " >= ");
		sql = sql.replace("__LT__", " < ");
		sql = sql.replace("__THE_STORE_ID__", " "+strId +" ");
		sql = sql.replace("__PAST_DAY_COUNT__"," " + pastDayCount+" ");
		sql = sql.replace("__ANALYSIS_DATE__", analysisDate);
		
		Date aDate = DateUtil.toDate(analysisDate);
		Date theDate = DateUtil.incrementDate(aDate, -7);
		int calendarWeekId1 = (calendarDao.getCalendarId(conn, DateUtil.dateToString(theDate, Constants.APP_DATE_FORMAT), Constants.CALENDAR_WEEK)).getCalendarId();
		int calendarWeekId2 = (calendarDao.getCalendarId(conn, analysisDate, Constants.CALENDAR_WEEK)).getCalendarId();
		theDate = DateUtil.incrementDate(aDate, 7);
		int calendarWeekId3 = (calendarDao.getCalendarId(conn, DateUtil.dateToString(theDate, Constants.APP_DATE_FORMAT), Constants.CALENDAR_WEEK)).getCalendarId();
		theDate = DateUtil.incrementDate(theDate, 7);
		int calendarWeekId4 = (calendarDao.getCalendarId(conn, DateUtil.dateToString(theDate, Constants.APP_DATE_FORMAT), Constants.CALENDAR_WEEK)).getCalendarId();

		sql = sql.replace("__CAL_ID_1__", calendarWeekId3 +""); // next week, which would be analysis week

		logger.info("Day Level SQL:"+ sql);
		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "itemListB");
		generateCSVB(crs,"listDayPartLevel");
		logger.info("Item List B count : " + crs.size());
		return iL;
	}

	
	public List<String> generateItemListD() throws GeneralException, Exception{
		List<String> iL = new ArrayList<String>();
		String sql = new String(itemListDSQL);
		sql = sql.replace("__GE__", " >= ");
		sql = sql.replace("__LT__", " < ");
		sql = sql.replace("__THE_STORE_ID__", " "+strId +" ");
		sql = sql.replace("__PAST_DAY_COUNT__"," " + pastDayCount+" ");
		sql = sql.replace("__ANALYSIS_DATE__", analysisDate);
		

		logger.info("Day Level SQL:"+ sql);
		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "itemListD");
		generateCSV(crs,"listDayPartLevelNew");
		logger.info("Item List B count : " + crs.size());
		return iL;
	}

	public List<String> generateAdData() throws GeneralException, Exception{
		List<String> iL = new ArrayList<String>();
		String sql = new String(adExtractSQL);
		sql = sql.replace("__GE__", " >= ");
		sql = sql.replace("__LT__", " < ");
		sql = sql.replace("__THE_STORE_ID__", " "+strId +" ");

		Date aDate = DateUtil.toDate(analysisDate);
		Date theDate = DateUtil.incrementDate(aDate, 7);
		int calendarWeekId3 = (calendarDao.getCalendarId(conn, DateUtil.dateToString(theDate, Constants.APP_DATE_FORMAT), Constants.CALENDAR_WEEK)).getCalendarId();

		sql = sql.replace("__WEEK_ID__"," " + calendarWeekId3+" ");
		

		logger.info("Ad SQL:"+ sql);
		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "Ad Data");
		generateAdCSV(crs,"adData");
		logger.info("Item List B count : " + crs.size());
		return iL;
	}
	
	
	public void generateTLog() throws GeneralException, Exception{
		List<String> iL = new ArrayList<String>();
		String sql = new String(tLogExtractSQL);
		sql = sql.replace("__GE__", " >= ");
		sql = sql.replace("__LT__", " < ");
		
		sql = sql.replace("__STORE_LIST__", " "+strId +" ");
		logger.info(beginDateForTlog+"    <<<>>>    "+analysisDate);

		if(beginDateForTlog.length()>0){
			sql = sql.replace("__BEGIN_DATE__", beginDateForTlog);
		}else{
			sql = sql.replace("__BEGIN_DATE__", analysisDate);
			
		}
		
		sql = sql.replace("__END_DATE__", analysisDate);
		

		logger.info("Day Level SQL:"+ sql);
		
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, new StringBuffer(sql), "tLogExtract");
		generateCSVtLog(crs,"tLogExtract");
		logger.info("T Log count : " + crs.size());
		return ;
	}

	
	private void generateCSVtLog(CachedRowSet tlog, String fileSuffix) throws Exception {
		logger.info("Begin Generate CSV");
		String filename = "/DE/tlog_"+strId+"_"+analysisDate.replace("/", "_")+fileSuffix+".csv";
		String filePath = PropertyManager.getProperty("OOS.ROOTPATH", "");
		ItemDAO itemDao = new ItemDAO();
		BufferedWriter out = new BufferedWriter(new FileWriter(filePath+"/"+filename)); 
		out.write("\"COMP_STR_NO\",\"UPC\",\"TRAN_TIME\",\"SALE_FLAG\",\"QUANTITY\",\"WEIGHT\",\"NET_PRICE\",\"TRANSACTION_NO\",\"PRESTO_ITEM_CODE\",\"EXTENDED_GROSS_PRICE\"," +
				"\"STORE_COUPON_USED\",\"MFR_COUPON_USED\",\"SEGMENT_ID\"\r\n");
		
		/* A.store_id comp_str_no, C.upc, to_char( A.trx_time, 'MM-DD-YY hh24:mi') as tran_time, A.SALE_TYPE SALE_FLAG, 
			A.QUANTITY, A.WEIGHT, A.net_amt NET_PRICE, 
			A.trx_no TRANSACTION_NO, A.ITEM_id as PRESTO_ITEM_CODE, A.regular_amt EXTENDED_GROSS_PRICE , A.STORE_COUPON_USED , A.MFR_COUPON_USED, 
			C.SEGMENT_ID
		 */
		
		while(tlog.next()){
			try{
//			String storeNum = tlog.getString(1);
			String upc = tlog.getString(2);
			String tranTime = tlog.getString(3);
			String saleFlag = tlog.getString(4);
			Integer quantity = tlog.getInt(5);
			Double weight = tlog.getDouble(6);
			Double netPrice = tlog.getDouble(7);
			String trxNo = tlog.getString(8);
			Integer itemCode = tlog.getInt(9);
			Double extGrossPrice = tlog.getDouble(10);
			String storeCouponUsed = tlog.getString(11);
			String mfrCouponUsed = tlog.getString(12);
			Integer segmentId  = tlog.getInt(13);
			
				out.write("\""+storeNum  + "\",");
				out.write("\""+upc.substring(1)  + "\",");
				out.write("\""+tranTime  + "\",");
				out.write("\""+saleFlag  + "\",");
				out.write(quantity  + ",");
				out.write(weight  + ",");
				out.write(netPrice  + ",");
				out.write(trxNo  + ",");
				out.write(itemCode  + ",");
				out.write(extGrossPrice  + ",");
				out.write("\""+storeCouponUsed  + "\",");
				out.write("\""+mfrCouponUsed  + "\",");
				out.write(segmentId+"");
				out.write("\r\n");

			} catch (Exception   e) {
				e.printStackTrace();
			}
		}
		out.flush();
		out.close();
		logger.info("End Generate CSV");
		
	}

	private void generateAdCSV(CachedRowSet adData, String fileSuffix) throws Exception {
		logger.info("Begin Generate CSV");
		String filename = "/DE/ad_data_"+"_"+analysisDate.replace("/", "_")+fileSuffix+".csv";
		String filePath = PropertyManager.getProperty("OOS.ROOTPATH", "");
		ItemDAO itemDao = new ItemDAO();
		BufferedWriter out = new BufferedWriter(new FileWriter(filePath+"/"+filename)); 
		out.write("\"WEEK_START_DATE\",\"WEEK_END_DATE\",\"PAGE\",\"BLOCK\",\"RETAILER_ITEM_CODE\",\"PRESTO_ITEM_CODE\",\"ITEM_NAME\",\"AD_LOCATION\",\"REG_PRICE\",\"ON_TPR\",\"ORG_UNIT_AD_PRICE\",\"ORG_AD_RETAIL\",\"DISPLAY_TYPE\",\"UPC\"\r\n");
		
		
		while(adData.next()){
			try{
			String week_start_date = isNull(adData.getString("week_start_date"));
			String week_end_date = isNull(adData.getString("week_end_date"));
			String page = isNull(adData.getString("page"));
			String block = isNull(adData.getString("block"));
			String ret_item_code = isNull(adData.getString("retailer_item_code"));
			Integer presto_item_code = adData.getInt("presto_item_code");
			String item_name = isNull(adData.getString("item_name"));
			String adLocation = isNull(adData.getString("ad_location"));
			Double regPrice = adData.getDouble("reg_price");
			String onTPR = isNull(adData.getString("on_tpr"));
			Double origUnitAdPrice = adData.getDouble("org_unit_ad_price");
			String origAdRetail  = isNull(adData.getString("org_ad_retail"));
			String displayType = isNull(adData.getString("display_type"));
			String upc = isNull(adData.getString("upc"));
			
				out.write("\""+week_start_date  + "\",");
				out.write("\""+week_end_date  + "\",");
				out.write("\""+page+ "\",");
				out.write("\""+block  + "\",");
				out.write("\""+ret_item_code  + "\",");
				out.write(""+presto_item_code  + ",");
				out.write("\""+item_name  + "\",");
				out.write("\""+adLocation  + "\",");
				out.write(""+regPrice  + ",");
				out.write("\""+onTPR  + "\",");
				out.write(""+origUnitAdPrice  + ",");
				out.write("\""+origAdRetail  + "\",");
				out.write("\""+displayType  + "\",");
				out.write("\""+upc  + "\"");
				out.write("\r\n");

			} catch (Exception   e) {
				e.printStackTrace();
			}
		}
		out.flush();
		out.close();
		logger.info("End Generate CSV");
		
	}

	

	private String isNull(String string) {
		if(string ==null || string.compareToIgnoreCase("null")==0){
			return "";
		}
		else return string;
	}


	public void exportItemLists(String storeNum, String strDate) throws GeneralException, Exception{
		conn = DBManager.getConnection();
		this.storeNum = storeNum;
		StoreDAO strdao = new StoreDAO();
		strId = strdao.getStoreID(conn, storeNum,null,-1);
		if( strId <= 0){
			throw new GeneralException ("Invalid Store Number, Store Number passed = " + storeNum );
		}

		if(dataType.compareToIgnoreCase("TLOG_PREVDAY")==0){
			// 
			beginDateForTlog = DateUtil.getDateFromCurrentDate(-1);
			analysisDate = DateUtil.getDateFromCurrentDate(-1);
			logger.info(beginDateForTlog+"    <<<>>>    "+analysisDate);
			generateTLog();

			return;
		}
		// all items with movement in last 7 days
//		generateItemListA();
		
		
		// all items with 2 unit movement in all of previous 7 days
		generateItemListB();
		
		
		// Focus items:  Items which moved > 2 units per day during 25 out of the last 28 days + items in last week’s Ad, current week’s Ad and next 2 weeks Ads
		generateItemListC();

		// Focus items:  Items which moved > 1 units per day during 25 out of the last 28 days 
		generateItemListD();
		
		
		
//		generateCSV(storeNum, strId, c, priceDataMap);
		generateTLog();
		
		generateAdData();

	}
	
	public void generateCSV(CachedRowSet itemList, String fileSuffix) throws Exception{
		try {
			logger.info("Begin Generate CSV");
			String filename = "/DE/item_list_"+pastDayCount+"_day_"+strId+"_"+analysisDate.replace("/", "_")+fileSuffix+".csv";
			String filePath = PropertyManager.getProperty("OOS.ROOTPATH", "");
			ItemDAO itemDao = new ItemDAO();
			BufferedWriter out = new BufferedWriter(new FileWriter(filePath+"/"+filename)); 
			out.write("\"UPC\"\r\n");
			
			while(itemList.next()){
				String upc = itemList.getString("upc");
				try {
					out.write(upc  + "\r\n");
				} catch (Exception   e) {
					e.printStackTrace();
				}
			}
			out.flush();
			out.close();
			logger.info("End Generate CSV");
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
			
		}
	}

	
	public void generateCSVB(CachedRowSet itemList, String fileSuffix) throws Exception{
		try {
			logger.info("Begin Generate CSV");
			String filename = "/DE/item_list_"+pastDayCount+"_day_"+strId+"_"+analysisDate.replace("/", "_")+fileSuffix+".csv";
			String filePath = PropertyManager.getProperty("OOS.ROOTPATH", "");
			ItemDAO itemDao = new ItemDAO();
			BufferedWriter out = new BufferedWriter(new FileWriter(filePath+"/"+filename)); 
			out.write("\"UPC\",\"Daily Avg\"\r\n");
			
			while(itemList.next()){
				String upc = itemList.getString(1);
				Double avg = itemList.getDouble(2);
				try {
					out.write(upc  +","+avg+ "\r\n");
				} catch (Exception   e) {
					e.printStackTrace();
				}
			}
			out.flush();
			out.close();
			logger.info("End Generate CSV");
		} catch (IOException e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
			
		}
	}

}
