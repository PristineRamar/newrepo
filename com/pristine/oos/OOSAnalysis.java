package com.pristine.oos;

import java.io.FileOutputStream;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.OOSCalcsDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.OOSSummaryDTO;
import com.pristine.dto.OOSTimeFactor;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;

public class OOSAnalysis {

	private int strId;
	private String storeNo;
	private String analysisDate;
	private int dow;
	private String dayOfWeek;
	private String curTimeRange;
	private boolean isLIGItem;
	
	private Connection conn;
	private String schIdList;
	private static Logger  logger = Logger.getLogger("OOSStats");
	private OOSCalcsDAO oosdao = new OOSCalcsDAO (); 
	private HSSFWorkbook wb;
	private int rowCount = 0;
	private HSSFSheet sheet;

	private static final float OOS_FACTOR = 0.5f;

	
	// Prepare the schedules to 
	public void initialize( Connection connection, int storeId, String lStoreNo, String lAnalysisDate, int ldow) throws GeneralException {
		conn = connection;
		strId = storeId;
		storeNo = lStoreNo;
		analysisDate = lAnalysisDate;
		String startDate = DateUtil.getWeekStartDate(26);//Look at the last 26 weeks data.
		String endDate = DateUtil.getWeekStartDate(0);
		//get schedules for Store
		ScheduleDAO schDao = new ScheduleDAO();
		ArrayList <ScheduleInfoDTO> schList = schDao.getSchedulesForStore(conn, strId, -1, startDate, endDate);
		schIdList = ScheduleDAO.getScheduleIdCSV(schList);
		dow = ldow;
		dayOfWeek = DateUtil.getDayOfWeek(dow);	
		initializeExcelSheet();
		
	}
	private void initializeExcelSheet() {
		wb = new HSSFWorkbook();
		sheet = wb.createSheet();
		HSSFRow row = sheet.createRow(rowCount);
		createHeaderRow(row);
		rowCount++;
	}
	
	public void performCloseOperation() throws GeneralException {
		try{
			FileOutputStream fileOut = new FileOutputStream("C:/Presto/OOSAnalysis.xls");
			wb.write(fileOut);
			fileOut.close();
		} catch (IOException ioe){
			throw new GeneralException("Error in Excel File write... ",ioe);
		}

	}
	
	private void createHeaderRow(HSSFRow row) {
		int colCount = 0;
		writeToCell(row, colCount, "Store");
		writeToCell(row, ++colCount, "Date");
		writeToCell(row, ++colCount, "Day");
		writeToCell(row, ++colCount, "Time");
		writeToCell(row, ++colCount, "Dept");
		writeToCell(row, ++colCount, "Category");
		writeToCell(row, ++colCount, "Item");
		writeToCell(row, ++colCount, "Presto Item #");
		writeToCell(row, ++colCount, "Line Group");
		writeToCell(row, ++colCount, "UPC");
		writeToCell(row, ++colCount, "Item Code");
		writeToCell(row, ++colCount, "Reg Pr");
		writeToCell(row, ++colCount, "Sale Pr");
		writeToCell(row, ++colCount, "On Sale");
		writeToCell(row, ++colCount, "Cur. Movement");
		writeToCell(row, ++colCount, "Cur. Visit#");
		writeToCell(row, ++colCount, "Avg Movement");
		writeToCell(row, ++colCount, "Min Movement");
		writeToCell(row, ++colCount, "Max Movement");
		writeToCell(row, ++colCount, "Mode");
		writeToCell(row, ++colCount, "Visit Count");
		writeToCell(row, ++colCount, "# of Obs");
		writeToCell(row, ++colCount, "OOS Type");
		writeToCell(row, ++colCount, "Priority");
		writeToCell(row, ++colCount, "Cur. Daily Movement");
		writeToCell(row, ++colCount, "Avg Daily Movement");
	}
	
	public void writeToCell( HSSFRow row, int colCount, Object val) {
		if( val != null){
			if( val instanceof String  ){
				HSSFCell cell   = row.createCell(colCount,HSSFCell.CELL_TYPE_STRING);
				cell.setCellValue((String)val);
			}else if ( val instanceof Integer ){
				HSSFCell cell   = row.createCell(colCount,HSSFCell.CELL_TYPE_NUMERIC);
				Integer intObj = (Integer)val;
				cell.setCellValue(intObj.doubleValue());
			}
			else if ( val instanceof Double ){
				HSSFCell cell   = row.createCell(colCount,HSSFCell.CELL_TYPE_NUMERIC);
				Double dblObj = (Double)val;
				cell.setCellValue(dblObj.doubleValue());
			}
		}
	}
	
	private void printOOSRecord(OOSSummaryDTO oosSummaryBase, OOSSummaryDTO oosInpSummary, CompetitiveDataDTO compData ) {
		
		HSSFRow row = sheet.createRow(rowCount);
		int colCount = 0;
		writeToCell(row, colCount, storeNo);
		writeToCell(row, ++colCount, analysisDate);
		writeToCell(row, ++colCount, dayOfWeek);
		writeToCell(row, ++colCount, curTimeRange);
		

		writeToCell(row, ++colCount, oosSummaryBase.deptName);
		writeToCell(row, ++colCount, oosSummaryBase.catName);
		writeToCell(row, ++colCount, oosSummaryBase.itemName);
		writeToCell(row, ++colCount, Integer.valueOf(oosSummaryBase.itemCode));
		writeToCell(row, ++colCount, isLIGItem ? "Y" : "N");
		
		writeToCell(row, ++colCount, oosSummaryBase.upc);
		writeToCell(row, ++colCount, oosSummaryBase.retailerItemCode);
		writeToCell(row, ++colCount, Double.valueOf(compData.regPrice) );
		writeToCell(row, ++colCount, Double.valueOf(compData.fSalePrice));

		
		writeToCell(row, ++colCount, oosInpSummary.onSale ? "Y":"N");
		writeToCell(row, ++colCount, Double.valueOf(oosInpSummary.onSale ? oosInpSummary.saleQty:oosInpSummary.regQty));
		writeToCell(row, ++colCount, Integer.valueOf(oosInpSummary.visitCount));
		writeToCell(row, ++colCount, Double.valueOf(oosInpSummary.onSale ? oosSummaryBase.saleQty:oosSummaryBase.regQty));
		writeToCell(row, ++colCount, Double.valueOf(oosInpSummary.onSale? oosSummaryBase.minSaleQty:oosSummaryBase.minRegQty));
		writeToCell(row, ++colCount, Double.valueOf(oosInpSummary.onSale ? oosSummaryBase.maxSaleQty:oosSummaryBase.maxRegQty));
		writeToCell(row, ++colCount, Integer.valueOf(oosSummaryBase.qtyMode));
		writeToCell(row, ++colCount, Integer.valueOf(oosSummaryBase.visitCount));
		writeToCell(row, ++colCount, Integer.valueOf(oosSummaryBase.noOfObs));

		writeToCell(row, ++colCount, oosInpSummary.ossType);
		writeToCell(row, ++colCount, oosInpSummary.priority);

		writeToCell(row, ++colCount, Double.valueOf(oosInpSummary.dailyQty));
		writeToCell(row, ++colCount, Double.valueOf(oosSummaryBase.dailyQty));

		rowCount++;
	}

		private boolean isOOS(OOSSummaryDTO oosSummaryBase,	OOSSummaryDTO oosInpSummary) {
		boolean retVal = false;
		if( oosInpSummary.onSale){
			if( oosSummaryBase.noOfObs > 3 && ( (int)oosInpSummary.saleQty != oosSummaryBase.qtyMode)){
				
				if( oosInpSummary.saleQty  <  (oosSummaryBase.saleQty * (1.0f - OOS_FACTOR))){
					// From slow movement, eliminating instances in which average movement is < 1.
					if ( oosSummaryBase.saleQty >= 1){ 
						retVal=true;
						oosInpSummary.ossType = "SlowMovement";
					}
				}else if ( oosInpSummary.saleQty  > (oosSummaryBase.saleQty * (1.0f + OOS_FACTOR))){
					//From fast movement, eliminating instances in which average movement is 0 and current movement is 1. 
					 if( oosSummaryBase.saleQty > 0 && oosInpSummary.saleQty  > 1 ){
						retVal=true;
						oosInpSummary.ossType = "FastMovement";
					 }
				}
				
				if(  oosInpSummary.saleQty < oosSummaryBase.minSaleQty){
					oosInpSummary.priority = "High"; 
				} else if ( oosInpSummary.saleQty > oosSummaryBase.maxSaleQty){
					oosInpSummary.priority = "High"; 
				}
				if( oosInpSummary.saleQty == 0){
					oosInpSummary.ossType = "ZeroMovement";
				}
			}
		}else{
			if( oosSummaryBase.noOfObs > 8 && ( (int)oosInpSummary.regQty != oosSummaryBase.qtyMode)){

				if( oosInpSummary.regQty  <  (oosSummaryBase.regQty * (1.0f - OOS_FACTOR))){
					// From slow movement, eliminating instances in which average movement is < 1.
					if ( oosSummaryBase.regQty >= 1){ 
						retVal=true;
						oosInpSummary.ossType = "SlowMovement";
					}
				}else if ( oosInpSummary.regQty  > (oosSummaryBase.regQty * (1.0f + OOS_FACTOR))){
					//From fast movement, eliminating instances in which average movement is 0 and current movement is 1. 
					 if( oosSummaryBase.regQty > 0 && oosInpSummary.regQty  > 1 ){
						retVal=true;
						oosInpSummary.ossType = "FastMovement";
					 }
					
				}
				
				if(  oosInpSummary.regQty < oosSummaryBase.minRegQty){
					oosInpSummary.priority = "High"; 
				} else if ( oosInpSummary.regQty > oosSummaryBase.maxRegQty){
					oosInpSummary.priority = "High"; 
				}
				if( oosInpSummary.regQty == 0){
					oosInpSummary.ossType = "ZeroMovement";
				}
			}

		}
		
		
		if ( retVal){
			//From slow movement, eliminating cases (may be very rare) where # of current visits => avg visits 
			//but current movement < average movement. 
			//That means people are buying but buying less for some reason - may be price is too high.
			if( (oosInpSummary.visitCount >= oosSummaryBase.visitCount) &&  oosInpSummary.ossType.equals("SlowMovement"))
				retVal = false;
		}
		return retVal;
	}
	public OOSTimeFactor[] prepareTimeSlots() {
		// TODO Auto-generated method stub
		OOSTimeFactor[] timeSlots  = new OOSTimeFactor[8];
//		timeSlots[0] = new OOSTimeFactor(5,0, "8:00 AM");
//		timeSlots[1] = new OOSTimeFactor(6,0, "11:00 AM");
//		timeSlots[2] = new OOSTimeFactor(7,0, "2:00 PM");
//		timeSlots[3] = new OOSTimeFactor(8,0, "4:00 PM");
//		timeSlots[4] = new OOSTimeFactor(9,0, "6:00 PM");
//		timeSlots[5] = new OOSTimeFactor(10,0, "9:00 PM");
//		timeSlots[6] = new OOSTimeFactor(5,1, "8:00 AM");

		// DateUtil.toDate(startPeriod, OSS_DATE_TIME_FORMAT)
		timeSlots[0] = new OOSTimeFactor(11,0, "12:00 AM");
		timeSlots[1] = new OOSTimeFactor(5,0, "8:00 AM");
		timeSlots[2] = new OOSTimeFactor(6,0, "11:00 AM");
		timeSlots[3] = new OOSTimeFactor(7,0, "2:00 PM");
		timeSlots[4] = new OOSTimeFactor(8,0, "4:00 PM");
		timeSlots[5] = new OOSTimeFactor(9,0, "6:00 PM");
		timeSlots[6] = new OOSTimeFactor(10,0, "9:00 PM");
		timeSlots[7] = new OOSTimeFactor(11,1, "12:00 AM");
		
		return timeSlots; 
	
	}
	
}
