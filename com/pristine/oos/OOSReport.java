package com.pristine.oos;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

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

public class OOSReport {

	static Logger logger = Logger.getLogger("OOSreport");
	
	private String _analysisDate = null; // Processing Date
	private String _storeNum = null; // Processing store number
	private boolean _onlyOOS=false;
	private String tag = "";

	private Connection _conn = null; // DB connection

	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-oos-report.properties");
		logCommand(args);
		
		String analysisDate = null;
		String storeNum = "";
		boolean onlyOOS=false;
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		OOSReport oosReport = new OOSReport();

		try {
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];

				
				// Get the  date  from command line
				if (arg.startsWith("ANALYSISDATE")) {
					analysisDate = arg.substring("ANALYSISDATE=".length());
					try {
						if(analysisDate.compareToIgnoreCase("PREVIOUSDATE")==0){
							Date date = DateUtil.incrementDate(new Date(), -1);
							analysisDate = DateUtil.dateToString(date, Constants.APP_DATE_FORMAT);
							
						}else if(analysisDate.compareToIgnoreCase("CURRENTDATE")==0){
							Date date = new Date();
							analysisDate = DateUtil.dateToString(date, Constants.APP_DATE_FORMAT);
							
						}else
						{
							Date aDate = dateFormat.parse(analysisDate);
						}
					} catch (ParseException par) {
						logger.error("Analysis Date Parsing Error, check Input");
					}
				}


				// Get the Store number from command line
				if (arg.startsWith("STORE")) {
					storeNum = arg.substring("STORE=".length());
				}


				if (arg.startsWith("OOSONLY")) {
					onlyOOS = true;
				}

				
				if (arg.startsWith("TAG")) {
					oosReport.tag = arg.substring("TAG=".length());
				}

			}


				// Call summary calculation process for Base Store
				oosReport.generateOOS(storeNum,analysisDate,onlyOOS);

		} catch (Exception e) {
			logger.error(e.getMessage());

		} finally {
			PristineDBUtil.close(oosReport._conn);
		}

	}

	
	public OOSReport(){
		try {
			PropertyManager.initialize("analysis.properties");
			_conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			System.exit(1);
		}

	}
	
	public void generateOOS(String storeNo, String date, boolean onlyOOS){
		_storeNum = storeNo;
		_analysisDate = date;
		_onlyOOS = onlyOOS;
		// get week id, dow_id, store_id
		// execute sql
		int weekCalendarId=-1;
		int dowId =-1;
		int storeId =-1;
		try{
		StoreDAO strdao = new StoreDAO();
		storeId  = strdao.getStoreID(_conn, storeNo,null,-1);
		if( storeId <= 0){
			throw new GeneralException ("Invalid Store Number, Store Number passed = " + storeNo );
		}

		dowId = DateUtil.getdayofWeek( date, Constants.APP_DATE_FORMAT);
		RetailCalendarDAO calendarDao = new RetailCalendarDAO();
		weekCalendarId = (calendarDao.getCalendarId(_conn, date, Constants.CALENDAR_WEEK)).getCalendarId();

		
		OOSCalcsDAO oosCalcsDao = new OOSCalcsDAO();
		//oosCalcsDao.generateOOSReport(_conn,5651, 3700,1);
		CachedRowSet oosResult = oosCalcsDao.generateOOSReport(_conn,storeId, weekCalendarId,dowId, onlyOOS);
//		generateCSV(oosResult);
		generateExcel(oosResult);
		// iterate results and write to file
		}catch(GeneralException e){
			logger.error(e.getMessage());
			
		}catch(Exception e){
			logger.error(e.getMessage());
		}
	}
	

	
	/**
R	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */

	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: SearchListDailyV2 ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

	
	public void generateCSV(CachedRowSet oosResult) throws Exception{
		try {
			logger.info("Begin Generate CSV");
			String filename = "oos_"+_storeNum+"_"+_analysisDate.replace("/", "_")+".csv";
			String filePath = PropertyManager.getProperty("OOS.ROOTPATH", "");
			ItemDAO itemDao = new ItemDAO();
			BufferedWriter out = new BufferedWriter(new FileWriter(filePath+"/OOSreports/"+filename));
			StringBuffer tb = new StringBuffer("");
			tb.append("Store Number\t");
			tb.append("Date\t");
			tb.append("Department\t");
			tb.append("Category\t");
			tb.append("Segment\t");
			tb.append("Item Code\t");
			tb.append("Description\t");
			tb.append("Time\t");
			tb.append("Sale?\t");
			tb.append("Act. Movement\t");
			tb.append("Exp   Used\t");
			tb.append("OOS method\t");
			tb.append("Exp Qty based on last 7 days\t");
			tb.append("Exp Qty based on last 28 days\t");
			tb.append("Exp Qty based on last 120 days\t");
			tb.append("Exp based on Previous Day\t");
			tb.append("Exp Qty based on last 42 days(Avg)\t");
			tb.append("last 7 day obs\t");
			tb.append("last 7 days - Days moved\t");
			tb.append("last 28 day obs\t");
			tb.append("last 28 days - days moved\t");
			tb.append("last 120 day obs\t");
			tb.append("last 120 days - days moved\t");
			tb.append("last 42 day Avg : daycount used\t");
			tb.append("OOS\t");
			tb.append("Price Used\t");
			tb.append("Unit Reg Price\t");
			tb.append("Item Visit\t");
			tb.append("Total store Visits\t");
			tb.append("Moved >2 on each of past 42 days\t");
			tb.append("OOS Classification\t");
			tb.append("Presto Item Code\t");
			tb.append("Last shipping Date\t");
			tb.append("Last shipping Qty\t");
			tb.append("Store Pack\t");
			tb.append("Cases\t");
			tb.append("MAPE\t");
			tb.append("Sigma Used\t");
			tb.append("64sq Exp\t");
			tb.append("AD: retailer Item code\t");
			tb.append("AD: Store Location\t");
			tb.append("AD: item description\t");
			tb.append("AD: Orig Unit Ad Price\t");
			tb.append("AD: Orid Ad Retail\t");
			tb.append("AD: Display Type\t");
			tb.append("Focus Item\t");
			tb.append("Exp With Ad Factor\t");
			tb.append("Sigma With Ad Factor\t");

			out.write(tb+"\r\n");
			SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy");
			SimpleDateFormat nf = new SimpleDateFormat("dd-MMM-yy");
			
			while(oosResult.next()){
				try {
					int count = 1;
					StringBuffer sb = new StringBuffer("");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getDate(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					sb.append(oosResult.getInt(count++) + "\t");
					sb.append(oosResult.getInt(count++) + "\t");
					sb.append(oosResult.getInt(count++) + "\t");
					sb.append(oosResult.getInt(count++) + "\t");
					sb.append(oosResult.getInt(count++) + "\t");
					sb.append(oosResult.getInt(count++) + "\t");
					sb.append(oosResult.getInt(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					sb.append(oosResult.getInt(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");
					sb.append(oosResult.getString(count++) + "\t");

					String oosClass = oosResult.getString(count++) ;
					sb.append((oosClass==null)?"\t":oosClass+ "\t");
					
					sb.append(oosResult.getInt(count++) + "\t");
					
					Date lastShipDate = oosResult.getDate(count++);
					sb.append((lastShipDate==null)?"\t":lastShipDate + "\t");
					
					int lastShipQty = oosResult.getInt(count++);
					sb.append((lastShipQty==0)?"\t":lastShipQty + "\t");

					int storePack = oosResult.getInt(count++);
					sb.append((storePack==0)?"\t":storePack + "\t");

					int cases = oosResult.getInt(count++);
					sb.append((cases==0)?"\t":cases + "\t");
					
					sb.append(oosResult.getDouble(count++) + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					
					double predExp = oosResult.getDouble(count++);
					sb.append((predExp==0)?"\t":predExp + "\t");
					
					String str = oosResult.getString(count++);
					sb.append((str==null)?"\t":str + "\t");

					str = oosResult.getString(count++);
					sb.append((str==null)?"\t":str + "\t");

					str = oosResult.getString(count++);
					sb.append((str==null)?"\t":str + "\t");

					str = oosResult.getString(count++);
					sb.append((str==null)?"\t":str + "\t");

					str = oosResult.getString(count++);
					sb.append((str==null)?"\t":str + "\t");

					str = oosResult.getString(count++);
					sb.append((str==null)?"\t":str + "\t");


					str = oosResult.getString(count++);
					sb.append((str==null)?"\t":str + "\t");

					double adFactorExp = oosResult.getDouble(count++);
					sb.append((adFactorExp==0)?"\t":adFactorExp + "\t");
					sb.append(oosResult.getDouble(count++) + "\t");
					

					out.write(sb+"\r\n");
					
				} catch (Exception   e) {
					e.printStackTrace();
				}
			}
			out.flush();
			out.close();
			logger.info("End Generate CSV");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception(e.getMessage());
			
		}
	}
		
		public void generateExcel(CachedRowSet oosResult) throws Exception{
			try {
				logger.info("Begin Generate Excel");
				String filename = tag+"oos_"+_storeNum+"_"+_analysisDate.replace("/", "_")+".xls";
				String filePath = PropertyManager.getProperty("OOS.ROOTPATH", "");
				
				HSSFWorkbook wb = new HSSFWorkbook();
			    FileOutputStream fileOut = new FileOutputStream(filePath+"/OOSreports/"+filename);
			    
			    HSSFSheet sheet = wb.createSheet("oos");
			    
			    HSSFCell firstCell;
			    HSSFCell lastCell;

			    HSSFRow header = sheet.createRow(0);
			    int c =0;
			    header.createCell(c++).setCellValue("Store Number"); 
			    header.createCell(c++).setCellValue("Date");
			    header.createCell(c++).setCellValue("Department");
			    header.createCell(c++).setCellValue("Category");
			    header.createCell(c++).setCellValue("Segment");
			    header.createCell(c++).setCellValue("Item Code");
			    header.createCell(c++).setCellValue("Description");
			    header.createCell(c++).setCellValue("Time");
			    header.createCell(c++).setCellValue("Sale?");
			    header.createCell(c++).setCellValue("Act. Movement");
			    header.createCell(c++).setCellValue("Exp   Used");
			    header.createCell(c++).setCellValue("OOS method");
			    header.createCell(c++).setCellValue("Exp Qty based on last 7 days");
			    header.createCell(c++).setCellValue("Exp Qty based on last 28 days");
			    header.createCell(c++).setCellValue("Exp Qty based on last 120 days");
			    header.createCell(c++).setCellValue("Exp based on Previous Day");
			    header.createCell(c++).setCellValue("Exp Qty based on last 42 days(Avg)");
			    header.createCell(c++).setCellValue("last 7 day obs");
			    header.createCell(c++).setCellValue("last 7 day obs - Used");
			    header.createCell(c++).setCellValue("last 7 days - Days moved");
			    header.createCell(c++).setCellValue("last 28 day obs");
			    header.createCell(c++).setCellValue("last 28 day obs - Used");
			    header.createCell(c++).setCellValue("last 28 days - days moved");
			    header.createCell(c++).setCellValue("last 120 day obs");
			    header.createCell(c++).setCellValue("last 120 day obs - Used");
			    header.createCell(c++).setCellValue("last 120 days - days moved");
			    header.createCell(c++).setCellValue("last 42 day Avg : daycount used");
			    header.createCell(c++).setCellValue("OOS");
			    header.createCell(c++).setCellValue("Price Used");
			    header.createCell(c++).setCellValue("Unit Reg Price");
			    header.createCell(c++).setCellValue("Item Visit");
			    header.createCell(c++).setCellValue("Total store Visits");
			    header.createCell(c++).setCellValue("Moved >2 on each of past 42 days");
			    header.createCell(c++).setCellValue("OOS Classification");
			    header.createCell(c++).setCellValue("Presto Item Code");
			    header.createCell(c++).setCellValue("Last shipping Date");
			    header.createCell(c++).setCellValue("Last shipping Qty");
			    header.createCell(c++).setCellValue("Store Pack");
			    header.createCell(c++).setCellValue("Cases");
			    header.createCell(c++).setCellValue("MAPE");
			    header.createCell(c++).setCellValue("Sigma Used");
			    header.createCell(c++).setCellValue("64sq Exp");
			    header.createCell(c++).setCellValue("AD: retailer Item code");
			    header.createCell(c++).setCellValue("AD: Store Location");
			    header.createCell(c++).setCellValue("AD: item description");
			    header.createCell(c++).setCellValue("AD: Orig Unit Ad Price");
			    header.createCell(c++).setCellValue("AD: Orid Ad Retail");
			    header.createCell(c++).setCellValue("AD: Display Type");
			    header.createCell(c++).setCellValue("Focus Item");
			    header.createCell(c++).setCellValue("Ad Factor Exp");
			    header.createCell(c++).setCellValue("Ad Factor Sigma");
			    header.createCell(c++).setCellValue("Ad Factor Method");
			    header.createCell(c++).setCellValue("Last Movement");
			    
			    
			    header.setHeightInPoints((3*sheet.getDefaultRowHeightInPoints()));
			    HSSFCellStyle cstyle = wb.createCellStyle();
			    cstyle.setWrapText(true);
			    header.setRowStyle(cstyle);

//			    BufferedWriter out = new BufferedWriter(new FileWriter(filePath+"/OOSreports/"+filename));
			    int rowCount=0;
				while(oosResult.next()){
					try {
						rowCount++;
					    HSSFRow row = sheet.createRow(rowCount);
						// excel starts at 0, cachedeowset starts at 1
						int count = 0;
						row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getDate(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));
					    row.createCell(count++).setCellValue(oosResult.getString(count));


						String oosClass = oosResult.getString(count+1) ;
					    row.createCell(count++).setCellValue((oosClass==null)?"":oosClass);
						
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
						
						Date lastShipDate = oosResult.getDate(count+1);
					    row.createCell(count++).setCellValue((lastShipDate==null)?"":lastShipDate+"");
						
//						lastShipQty
					    // store pack
					    // cases
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));
					    row.createCell(count++).setCellValue(oosResult.getInt(count));


					    
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));
						
						double predExp = oosResult.getDouble(count+1);
					    row.createCell(count++).setCellValue((predExp==0)?"":predExp + "");
						
						String str = oosResult.getString(count+1);
					    row.createCell(count++).setCellValue((str==null)?"":str + "");

						str = oosResult.getString(count+1);
					    row.createCell(count++).setCellValue((str==null)?"":str + "");

						str = oosResult.getString(count+1);
					    row.createCell(count++).setCellValue((str==null)?"":str + "");

						str = oosResult.getString(count+1);
					    row.createCell(count++).setCellValue((str==null)?"":str + "");

						str = oosResult.getString(count+1);
					    row.createCell(count++).setCellValue((str==null)?"":str + "");

						str = oosResult.getString(count+1);
					    row.createCell(count++).setCellValue((str==null)?"":str + "");


						str = oosResult.getString(count+1);
					    row.createCell(count++).setCellValue((str==null)?"":str + ""); 

						double adFactorExp = oosResult.getDouble(count+1);
					    row.createCell(count++).setCellValue((adFactorExp==0)?"":adFactorExp + "");
					    row.createCell(count++).setCellValue(oosResult.getDouble(count));

						str = oosResult.getString(count+1);
					    row.createCell(count++).setCellValue((str==null)?"":str + "");

						str = oosResult.getString(count+1);
					    row.createCell(count++).setCellValue((str==null)?"":str + "");

					} catch (Exception   e) {
						e.printStackTrace();
					}
				}

				for(int i=0;i<=8;i++){
					sheet.autoSizeColumn(i);
				}
				
				
				

				
				wb.write(fileOut);
			    fileOut.close();
				
				logger.info("End Generate Excel");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new Exception(e.getMessage());
				
			}

	}

	
}
