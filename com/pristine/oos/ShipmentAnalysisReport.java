package com.pristine.oos;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;

import com.pristine.dao.DBManager;
import com.pristine.dao.OOSCalcsDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dto.OOSPredictedMovementDTO;
import com.pristine.dto.ShippingInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ShipmentAnalysisReport {
	/*
	 * 
	 * Analyse Shipment data and produce report indicating relationship between movement, shipment , excess stock etc.
	 * 
	 * Input
	 * Store Number
	 * 
	 * Algorithm
	 * (1) Take the items that moved at least 1 unit in last 15 days
	 * (2) Assume a base start date, and opening balance zero for all items
	 * (3) Load Last shipment data (after the start date) for these items into a cache 
	 * (4) Load total shipment since start date <- could simply be the Sum of units  Hash<item_id, shipped units>
	 * (5) Load total movement since start date <-  query into cache total movement hash<item_id, total movement>
	 * (6) for each item
	 * 		(a) query and get movement since last shipment (from summary_daily_mov) 
	 * 		(b) difference in total shipment Qty (4) and movement (5) since start date 
	 * 		(c) date when balance became zero <- this is just the last movement date, when (b) is zero
	 * 		(d) derive other items in report from the above
	 * 
	 * 
	 * 
	 */
	
	
	static Logger logger = Logger.getLogger("ShipmentAnalysisReport");
	
	private String analysisStartDate = "09/01/2013"; // Processing Date
	private String storeNum = null; // Processing store number
	Date analysisDate = new Date();
	private String strAnalysisDate = DateUtil.dateToString(analysisDate, Constants.APP_DATE_FORMAT);

	private Connection _conn = null; // DB connection

	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-shipment-analysis-report.properties");
		logCommand(args);
		
		String argStoreNum = "";
		String argAnalysisDate = "";
		ShipmentAnalysisReport oosReport = new ShipmentAnalysisReport();

		try {
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];

				
				// Get the  date  from command line

				// Get the Store number from command line
				if (arg.startsWith("STORE")) {
					argStoreNum = arg.substring("STORE=".length());
				}

				if (arg.startsWith("ANALYSISDATE")) {
					argAnalysisDate = arg.substring("ANALYSISDATE=".length());
				}

			}

			oosReport.generateShipmentAnalysis(argStoreNum, argAnalysisDate);

		} catch (Exception e) {
			logger.error(e.getMessage());
		} catch (GeneralException e) {
			logger.error(e.getMessage());

		} finally {
			PristineDBUtil.close(oosReport._conn);
		}

	}

	
	public ShipmentAnalysisReport(){
		try {
			PropertyManager.initialize("analysis.properties");
			_conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("Error while connecting DB:" + exe);
			System.exit(1);
		}

	}
	
	public void generateShipmentAnalysis(String argStoreNum, String argAnalysisDate) throws GeneralException{
		storeNum = argStoreNum;
		if(argAnalysisDate!=null && argAnalysisDate.length()>0){
			strAnalysisDate = argAnalysisDate;
			analysisDate = DateUtil.toDate(argAnalysisDate);
		}
		
		OOSCalcsDAO oosCalcsDao = new OOSCalcsDAO();
		
		int storeId =-1;
		try{
		StoreDAO strdao = new StoreDAO();
		storeId  = strdao.getStoreID(_conn, storeNum,null,-1);
		if( storeId <= 0){
			throw new GeneralException ("Invalid Store Number, Store Number passed = " + storeNum );
		}


//		 * (1) Take the items that moved at least 1 unit in last 15 days
		CachedRowSet crsItemList = oosCalcsDao.getDynamicItemListExt(_conn, storeId, strAnalysisDate);
		logger.info("Item List Size:"+ crsItemList.size());
//		 * (2) Assume a base start date, and opening balance zero for all items

//		 * (3) Load Last shipment data (after the start date) for these items into a cache
		Hashtable<Integer,ShippingInfoDTO> shipmentInfo = oosCalcsDao.getShipmentInfoForDynamicItemList(_conn, storeId, strAnalysisDate);
		logger.info("Shipment Info Size:"+shipmentInfo.size());
		
//		 * (4) Load total shipment since start date <- could simply be the Sum of units  Hash<item_id, shipped units>
		Hashtable<Integer,Integer> totalShipment = oosCalcsDao.getTotalShipmentForDynamicItemList(_conn, storeId, strAnalysisDate,analysisStartDate);
		logger.info("total shipment data size:"+totalShipment.size());
//		 * (5) Load total movement since start date <-  query into cache total movement hash<item_id, total movement>
		Hashtable<Integer,Double> totalMovement = oosCalcsDao.getMovementSummarySince(_conn, storeId, strAnalysisDate,analysisStartDate);
		logger.info("total movement data size:"+totalMovement.size());
		
		HashMap<Integer,String> lastMovementCache = oosCalcsDao.loadLastMovementDateTime(_conn, storeId, strAnalysisDate);
		logger.info("last movement data size:"+lastMovementCache.size());
		
	
		// Load weeklevel prediction cache
		Date weekStart = DateUtil.getSunday(analysisDate);
		String strWeekStart = DateUtil.dateToString(weekStart, Constants.APP_DATE_FORMAT);
		Hashtable<Integer,Double> weeklyPrediction = oosCalcsDao.getWeeklyPredictionForDynItemList(_conn, storeId,strAnalysisDate,strWeekStart);
		logger.info("weekly prediction data size:"+weeklyPrediction.size());
		
		// start date of predicted week.
		
		
		logger.info("Begin Generate Excel");
		String filename = "ship_analysis_"+storeNum+"_"+ strAnalysisDate.replace("/", "_")+".xls";
		String filePath = PropertyManager.getProperty("OOS.ROOTPATH", "");
		
		HSSFWorkbook wb = new HSSFWorkbook();
	    FileOutputStream fileOut = new FileOutputStream(filePath+"/OOSreports/"+filename);
	    
	    HSSFSheet sheet = wb.createSheet("ship_analysis");
	    
	    HSSFCell firstCell;
	    HSSFCell lastCell;

	    HSSFRow header = sheet.createRow(0);
	    int c =0;
	    header.createCell(c++).setCellValue("Store Number"); 
	    header.createCell(c++).setCellValue("Category");
	    header.createCell(c++).setCellValue("Segment");
	    header.createCell(c++).setCellValue("Item Code");
	    header.createCell(c++).setCellValue("Description");
	    header.createCell(c++).setCellValue("Last Shipment Date");
	    header.createCell(c++).setCellValue("# of cases shipped");
	    header.createCell(c++).setCellValue("# of units shipped");
	    header.createCell(c++).setCellValue("Total movement since last shipment");
	    header.createCell(c++).setCellValue("Balance Units");
	    header.createCell(c++).setCellValue("Date on which Balance became Zero");
	    header.createCell(c++).setCellValue("Days since last shipment");
	    header.createCell(c++).setCellValue("Days since Balance became Zero");
	    header.createCell(c++).setCellValue("Last 42 Day Avg Movement");
	    header.createCell(c++).setCellValue("# of days’ supply left ");
	    header.createCell(c++).setCellValue("Weekly expectation ");
	    header.createCell(c++).setCellValue("Method used ");
	    header.createCell(c++).setCellValue("Week start Date ");
	    header.createCell(c++).setCellValue("Over Ordered");

	    header.setHeightInPoints((3*sheet.getDefaultRowHeightInPoints()));
	    HSSFCellStyle cstyle = wb.createCellStyle();
	    cstyle.setWrapText(true);
	    header.setRowStyle(cstyle);
	    Date prevDate = DateUtil.incrementDate(analysisDate, -1);
	    RetailCalendarDAO rcDAO = new RetailCalendarDAO();
	    String strPrevDate = DateUtil.dateToString(prevDate, Constants.APP_DATE_FORMAT);
	    int calId = (rcDAO.getCalendarId(_conn, strPrevDate, Constants.CALENDAR_WEEK)).getCalendarId();
	    double avgDailyVisitCount = oosCalcsDao.getdailyAvgStoreVisitCount(_conn, storeId, strPrevDate); 
	    //load predicted movement cache
		logger.info("Begin load predicted Movement");
		HashMap<Integer, HashSet<OOSPredictedMovementDTO>> predictedMovementCache = oosCalcsDao.loadPredictedMovementCache(_conn,storeId,calId,DateUtil.getdayofWeek(prevDate));
		logger.info("Endload predicted Movement:cache size" + predictedMovementCache.size());

	    
	    int rowCount =0;
		
//		 * (6) for each item
		while(crsItemList.next()){
			Integer item_code = crsItemList.getInt("item_code");
			String segment = crsItemList.getString("segment_name");
			String category = crsItemList.getString("cat_name");
			String description = crsItemList.getString("item_name");
			ShippingInfoDTO sInfo = shipmentInfo.get(item_code);
			Date lastShip = null;
			Integer unitsShipped = 0;
			Integer casesShipped = 0;
			if(sInfo!=null){
				lastShip = sInfo.getShipmentDate();
				unitsShipped = sInfo.getQuantity();
				casesShipped  = sInfo.getCasesShipped();
			}
			
//		 * 		(b) difference in total shipment Qty (4) and movement (5) since start date
			Integer sQty = totalShipment.get(item_code);
			Double mQty = totalMovement.get(item_code);
			Double balance = 0.0;
			if(sQty != null && mQty !=null){
				balance = sQty - mQty;
			}
//			 * 		(a) query and get movement since last shipment (from summary_daily_mov)
				Integer movementSinceLS = 0;
				if(lastShip !=null ){
					movementSinceLS = oosCalcsDao.getMovementSummaryForItemSince(_conn, storeId, DateUtil.dateToString(lastShip, Constants.APP_DATE_FORMAT),item_code);
				}else{
					if(mQty!=null){
					movementSinceLS = mQty.intValue();
					}
				}
			
//		 * 		(c) date when balance became zero <- this is just the last movement date, when (b) is zero
			String lastMovement = "";
			if(balance ==0){
				lastMovement=lastMovementCache.get(item_code);
			}
			
			if(balance < (unitsShipped - movementSinceLS)){
				balance = (double)(unitsShipped - movementSinceLS);
			}
			
//		 * 		(d) derive other items in report from the above

			// write to Excel
			rowCount++;
		    HSSFRow row = sheet.createRow(rowCount);
			// excel starts at 0, cachedeowset starts at 1
			int count = 0;
			row.createCell(count++).setCellValue(storeNum);
		    row.createCell(count++).setCellValue(category);
		    row.createCell(count++).setCellValue(segment);
		    row.createCell(count++).setCellValue(item_code);
		    row.createCell(count++).setCellValue(description);
		    
		    CellStyle cellStyle = wb.createCellStyle();
		    CreationHelper createHelper = wb.getCreationHelper();
		    cellStyle.setDataFormat(
		        createHelper.createDataFormat().getFormat("mm/dd/yyyy"));
		    
		    Long daysSinceLast =null;
		    
		    if(lastShip != null){
		    HSSFCell cell = row.createCell(count++);
		    cell.setCellValue(lastShip);
		    cell.setCellStyle(cellStyle);
		    daysSinceLast = DateUtil.getDateDiff(new Date(),lastShip);
		    
		    }else{
			    row.createCell(count++);
		    }
		    
		    
		    row.createCell(count++).setCellValue(casesShipped);
		    row.createCell(count++).setCellValue(unitsShipped);
		    
		    if(movementSinceLS !=null){
		    row.createCell(count++).setCellValue(movementSinceLS);
		    }else{
		    	row.createCell(count++);
		    }
		    
		    row.createCell(count++).setCellValue(balance);
		    
		    row.createCell(count++).setCellValue(lastMovement);
		    
		    Long daysSinceZero=null;
		    if(lastMovement !=null && balance ==0){
		    try{	
		    SimpleDateFormat sf = new SimpleDateFormat("MM/dd/yyyy hh:mm aaa");  // 09/16/2013 03:14 PM
		    Date d = sf.parse(lastMovement);
		    daysSinceZero = DateUtil.getDateDiff(new Date(),d);
		    }catch(Exception e){
		    	
		    }
		    }

		    if(daysSinceLast != null){
		    row.createCell(count++).setCellValue(daysSinceLast);
		    }else{
		    	row.createCell(count++);
		    }

		    if(daysSinceZero !=null){
			    row.createCell(count++).setCellValue(daysSinceZero);
		    }else{
		    	row.createCell(count++);
		    }
		    

		    Double fortyTwoDayAvg = null;
		    HashSet<OOSPredictedMovementDTO> predictedMovement = predictedMovementCache.get(item_code);
		    if(predictedMovement!=null){
		    	for(OOSPredictedMovementDTO pm : predictedMovement){
		    		if(pm.timeOfDayId == 4){
		    			fortyTwoDayAvg = pm.last7dayavgexp;
		    		}
		    	}
		    }
		    
		    if(fortyTwoDayAvg !=null){
			    row.createCell(count++).setCellValue(fortyTwoDayAvg*avgDailyVisitCount);
		    }else{
		    	row.createCell(count++);
		    }

		    Double weeklyExp = weeklyPrediction.get(item_code);
		    Double daysLeft =0.0;
		    if(weeklyExp!=null){
		    	daysLeft = balance/(weeklyExp/7);
		    }else   if(fortyTwoDayAvg !=null && fortyTwoDayAvg !=0 && avgDailyVisitCount!=0){
			    daysLeft = balance/(fortyTwoDayAvg*avgDailyVisitCount);
		    }
		    
		    if(daysLeft !=0){
			    row.createCell(count++).setCellValue(daysLeft);
		    }else{
		    	row.createCell(count++);
		    }

		    String wExpMtd = "";
		    if(weeklyExp == null && fortyTwoDayAvg!=null){
		    	weeklyExp = fortyTwoDayAvg*7*avgDailyVisitCount;
		    	wExpMtd = "42 day Avg";
		    }
		    if(weeklyExp!=null){
		    	if(wExpMtd.length()==0){
		    		wExpMtd ="Pred Model";
		    	}
		    	row.createCell(count++).setCellValue(weeklyExp);
		    }else{
		    	row.createCell(count++);
		    	
		    }

		    row.createCell(count++).setCellValue(wExpMtd);

	    	row.createCell(count++).setCellValue(strWeekStart);
	    	String overOrder  ="";
		    if(casesShipped != null && unitsShipped != null && weeklyExp !=null){
		    	if(casesShipped > 1 && unitsShipped > (3 * weeklyExp)){
		    		overOrder = "Y";
		    	}
		    }
		    row.createCell(count++).setCellValue(overOrder);
		    
		}
		
		
		for(int i=0;i<=8;i++){
			sheet.autoSizeColumn(i);
		}
		
		
		

		
		wb.write(fileOut);
	    fileOut.close();
		
		logger.info("End Generate Excel");
		
		
		
//		generateExcel(oosResult);
		}catch(GeneralException e){
			logger.error(e.getMessage());
			e.printStackTrace();
		}catch(Exception e){
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	

	
	/**
R	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */

	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: ShipmentAnalysis ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}

	


	
}
