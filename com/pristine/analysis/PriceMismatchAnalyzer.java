package com.pristine.analysis;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.MovementDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PriceMismatchAnalyzer {


	private static Logger logger = Logger.getLogger("PriceMismatchAnalyzer");
	private static Connection dbconn = null;

    private static boolean checkSale = true;
    private static boolean checkRegular = true;
    
    private int noOfMatches = 0;
    private int noOfMisMatches = 0;
    private int ignoredCount = 0;
    
    private HashMap<Integer, RetailPriceDTO> retailPriceList = new HashMap<Integer, RetailPriceDTO> ();
    // Mismatch T-Log Price
    private ArrayList <RetailPriceDTO> misMatchPriceList = new ArrayList <RetailPriceDTO> ();
    // Mismatch movement records
    private HashMap<Integer, MovementDTO> misMatchMovementList = new HashMap<Integer, MovementDTO> ();
    
    //Excel File o/p parameters
	private HSSFWorkbook wb;
	private int rowCount = 0;
	private HSSFSheet sheet;

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j.properties");
		PropertyManager.initialize("analysis.properties");

		//Arguments - Store number , Date to check and optional Regular/Sale - 
		//Passing Regular would  validate only Regular price
		//Passing Sale would  validate only Sale price
		if ( args.length < 2 || args.length > 3){
			logger.error( "In correct Arguments, Expected arguments are Store_Number Date_to_check(MM/DD/YYYY) optional - Regular/Sale ");
			System.exit(1);
		}
		
		//set flags accordingly
		
		if( args.length == 3){
			if ( args[2].equalsIgnoreCase("regular"))
				checkSale = false;
			else if ( args[2].equalsIgnoreCase("sale"))
				checkRegular = false;
			//Note - default would check both option
		}

		PriceMismatchAnalyzer priceAnalyzer = new PriceMismatchAnalyzer();
		
		try{
			priceAnalyzer.analyzePriceData( args[0], args[1]);
		}catch( GeneralException ge){
			logger.error("Exception in Price Mismatch Analysis", ge);
		}finally{
			if ( dbconn!= null)
				PristineDBUtil.close(dbconn);
		}
		
		
	}

	private void analyzePriceData(String storeNum, String dateToCheck) throws GeneralException {
		
		logger.info("Price Mismatch Analysis Started for Store - " + storeNum + " Date - " + dateToCheck );

		//Get Connection
		dbconn = DBManager.getConnection();
		
		//Get Subscriber Chain Id

		// ****Janani*** ensure Subscriber Id is in the config properly
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		String subscriberStr = retailPriceDAO.getChainId(dbconn);
		int subscriberId = Integer.parseInt(subscriberStr);
		
		
		//Get the store id
		// ***Janani*** Move this API to Store DAO class
		StoreDTO strObj =  getStoreInfo(dbconn, storeNum,  subscriberId);
		

		//Get retailer calendar
		Date inputDate = DateUtil.toDate(dateToCheck);
		String inputDateStr = DateUtil.dateToString(inputDate, Constants.APP_DATE_FORMAT);
		String dateInFile = DateUtil.dateToString(inputDate, "MMddyyyy");
		int calendarId = -1;
	
		//create object for RetailCalendarDTO and get the calendar id
		RetailCalendarDAO objCal = new RetailCalendarDAO();
		RetailCalendarDTO calendarDto = objCal.getCalendarId(dbconn, inputDateStr, Constants.CALENDAR_DAY);
		if(calendarDto != null){
			calendarId = calendarDto.getCalendarId();
		}
		
		logger.info("Calendar Id : " + calendarId);
		logger.info("Store Number : " + strObj.strNum);
		
		if( calendarId < 0){
			throw new GeneralException(" Calendard Id is invalid, cannot proceed !!!!");
		}
		
		//Identify the start and end date for the dateToCheck
		String startDate = DateUtil.getWeekStartDate(inputDate, 0);
		String endDate = DateUtil.getWeekEndDate(inputDate);
		
		//Get the prices for store ( from retailer price table)
		prepareRetailPrices( dbconn, strObj.strId, startDate, endDate);

		
		//Get the T-log data for that day by item
		ArrayList<MovementDTO> movementDataList = getMovementData(dbconn, storeNum, calendarId);
		if( movementDataList.size() == 0){
			throw new GeneralException(" No T-log data to compare, cannot proceed !!!!");
		}
		
		// Compare the prices
		for( MovementDTO movementData : movementDataList){
			RetailPriceDTO posPriceObj = new RetailPriceDTO ();
			
			posPriceObj.setItemcode( Integer.toString(movementData.getItemCode()));
			
			double divFactor = (movementData.getExtnWeight() > 0)?movementData.getExtnWeight():movementData.getExtnQty();
			if( divFactor > 0){
				float regPrice = (float) (movementData.getExtendedGrossPrice()/divFactor);
				posPriceObj.setRegPrice(regPrice);
				float salePrice = (float) (movementData.getExtendedNetPrice()/divFactor);
				if( salePrice > 0 &&  (regPrice - salePrice) > 0.01f ){ 
					posPriceObj.setSalePrice(salePrice);
					posPriceObj.setPromotionFlag("Y");
				}else{
					posPriceObj.setPromotionFlag("N");
				}
				RetailPriceDTO actPriceObj = retailPriceList.get( movementData.getItemCode());
				if( actPriceObj != null ){
					if( doesPriceMatch(actPriceObj,posPriceObj)){
						noOfMatches++;
					}else{
						noOfMisMatches++;
						logger.info(movementData.getItemCode() + "\t" + actPriceObj.getRegPrice() + "\t" + posPriceObj.getRegPrice());
						misMatchPriceList.add(posPriceObj);
						
						misMatchMovementList.put(movementData.getItemCode(), movementData);
					}
						
				}else{
					ignoredCount++;
				}
			}
			
		}
		
		for(RetailPriceDTO retailPriceDTO : misMatchPriceList){
			calendarDto = objCal.getCalendarId(dbconn, inputDateStr, Constants.CALENDAR_WEEK);
			analyzeMismatch(storeNum, calendarDto.getCalendarId(), retailPriceDTO);
		}
		
		logger.info("No of Matches = " + noOfMatches);
		logger.info("No of MisMatches = " + noOfMisMatches);
		logger.info("No of Ignored Items = " + ignoredCount);
		
		
		// Print the matches to the spread-sheet
		// Header - Store Number, Address Info
		// Date Analyzed
		// Department name, Category, Item Name, Retailer Item code, UPC, Exp Reg Price, Exp Sale Price, Act Reg Price, Act Sale Price, Matches, MisMatches, Comments 
		
		initializeExcelSheet();
		createHeaderRow(strObj, dateToCheck);
		createMisMatchReport();
		writeToFile("F:/Production/PrestoRA/MismatchReport/" + storeNum + "_" + dateInFile +  ".xls"); 
		logger.info("Price Mismatch Analysis Completed for Store - " + storeNum + " Date - " + dateToCheck );
	}

	

	private void analyzeMismatch(String storeNum, int calendarId, RetailPriceDTO retailPriceDTO) throws GeneralException{
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		logger.debug(retailPriceDTO.getItemcode() + "\t" + calendarId + "\t" + storeNum);
		HashMap<Integer, List<RetailPriceDTO>> retailPriceMap = retailPriceDAO.getItemPriceInStore(dbconn, calendarId-1, calendarId, retailPriceDTO.getItemcode(), storeNum);
		TreeMap<Integer, List<RetailPriceDTO>> priceMap = new TreeMap<Integer, List<RetailPriceDTO>>(Collections.reverseOrder());
		for(Map.Entry<Integer, List<RetailPriceDTO>> entry : retailPriceMap.entrySet()){
			priceMap.put(entry.getKey(), entry.getValue());
		}
		
		List<RetailPriceDTO> finalPriceList = new ArrayList<RetailPriceDTO>();
		
		for(Map.Entry<Integer, List<RetailPriceDTO>> entry : priceMap.entrySet()){
			finalPriceList.add(entry.getValue().get(0));
		}
		
		String effectiveDate = null;
		RetailPriceDTO actRetailPriceDTO = retailPriceList.get(Integer.parseInt(retailPriceDTO.getItemcode()));
		for(int i = 0 ; i < finalPriceList.size() ; i++){
			float price = 0;
			RetailPriceDTO tempDTO = finalPriceList.get(i);
			effectiveDate = tempDTO.getRegEffectiveDate();
			
			if(String.valueOf(Constants.YES).equalsIgnoreCase(tempDTO.getPromotionFlag())){
				if(tempDTO.getSaleQty() > 1) price = tempDTO.getSaleMPrice()/tempDTO.getSaleQty();
				else price = tempDTO.getSalePrice();
			}else{
				if(tempDTO.getRegQty() > 1) price = tempDTO.getRegMPrice()/tempDTO.getRegQty();
				else price = tempDTO.getRegPrice();
			}
			if(actRetailPriceDTO != null)
				logger.debug(actRetailPriceDTO.getRegPrice() + "\t" + actRetailPriceDTO.getSalePrice());
			logger.debug(price);
			if(retailPriceDTO.getRegPrice() == price || retailPriceDTO.getSalePrice() == price){
				logger.debug("POS Price effective date - " + tempDTO.getRegEffectiveDate());
				retailPriceDTO.setUpdateTimeStamp(tempDTO.getRegEffectiveDate());
				break;
			}
			
			if(actRetailPriceDTO != null)
				if((actRetailPriceDTO.getRegPrice() != 0 && actRetailPriceDTO.getRegPrice() == price) || (actRetailPriceDTO.getSalePrice() != 0 && actRetailPriceDTO.getSalePrice() == price)){
					logger.debug("Actual Price effective Date - " + effectiveDate);
					retailPriceDTO.setRegEffectiveDate(effectiveDate);
				}
		}
		
	}

	private boolean doesPriceMatch(RetailPriceDTO actPriceObj,
			RetailPriceDTO posPriceObj) {
		boolean retVal = true;
		if(checkRegular){
			if( Math.abs(actPriceObj.getRegPrice() - posPriceObj.getRegPrice())> 0.01 )
				retVal= false;	
		}
		if( checkSale){
			if( (actPriceObj.getSalePrice() > 0 ) || (posPriceObj.getSalePrice() > 0 )){
				if( Math.abs(actPriceObj.getSalePrice() - posPriceObj.getSalePrice())> 0.01 )
					retVal= false;	
			}
		}
		
		return retVal;
	}

	/*** Janani *** Should go to movementDataDao ****/
	private ArrayList<MovementDTO> getMovementData(Connection conn,String storeNum, int calendarId) throws GeneralException {
		ArrayList<MovementDTO> movementDataList = new ArrayList<MovementDTO>(); 
		StringBuffer sql = new StringBuffer();
		
		
		sql.append(" select COMP_STR_NO, Calendar_id, item_code, ");
		sql.append(" sum(NVL(Quantity,0)) as QUANTITY, sum(NVL(Weight,0)) as WEIGHT, ");
		sql.append(" sum(Price) as NET_PRICE, SUM(EXTENDED_GROSS_PRICE) as GROSS_PRICE ");
		sql.append(" FROM MOVEMENT_DAILY ");
		sql.append(" WHERE COMP_STR_NO = '").append(storeNum).append("'") ;
		sql.append(" and calendar_id = ").append(calendarId);
		sql.append(" and ROWNUM < 2000 ");
		sql.append(" group by COMP_STR_NO, Calendar_id, item_code");
		CachedRowSet crs =  PristineDBUtil.executeQuery(conn, sql, "getStoreInfo");
		try {
			while (crs.next()){
				MovementDTO movementObj = new MovementDTO ();
				movementObj.setItemStore(crs.getString("COMP_STR_NO"));
				movementObj.setCalendarId(calendarId);
				movementObj.setExtendedGrossPrice(crs.getDouble("GROSS_PRICE"));
				movementObj.setExtendedNetPrice(crs.getDouble("NET_PRICE"));
				movementObj.setExtnQty(crs.getDouble("QUANTITY"));
				movementObj.setExtnWeight(crs.getDouble("WEIGHT"));
				movementObj.setItemCode(crs.getInt("ITEM_CODE"));
				movementDataList.add(movementObj);
			}
		}catch(SQLException sqle){
			throw new GeneralException("Cached Row Access Exception- GetMovementData", sqle);
		}
		
		return movementDataList ;
	}

	private void prepareRetailPrices(Connection conn, int strId, String startDate, String endDate) throws GeneralException {
		
		//*** Janani *** this API needs to be changed to use RetailPrice table eventually 
		
		//Get the schedule Id
		//Get the competititive data
		//Prepare RetailPrice hashmap

		ScheduleDAO schDAO = new ScheduleDAO (); 
		ArrayList<ScheduleInfoDTO> schList = schDAO.getSchedulesForStore(conn, strId, -1, startDate, endDate);
		if( schList.size() != 1 ){
			throw new GeneralException ("Unable to find schedules !!!!");
		}
		ScheduleInfoDTO schInfo = schList.get(0);
		
		CompetitiveDataDAO compDao = new CompetitiveDataDAO(conn);
		
		ArrayList<CompetitiveDataDTO> compDataList =  compDao.getCompData(conn, schInfo.getScheduleId(), -1, -1, true);
		
		for (CompetitiveDataDTO compData : compDataList) {
			RetailPriceDTO priceObj =  new RetailPriceDTO(); 
			priceObj.setItemcode( Integer.toString(compData.itemcode));
			priceObj.setRegPrice(compData.regPrice);
			priceObj.setSalePrice(compData.fSalePrice);
			priceObj.setPromotionFlag(compData.saleInd);
			priceObj.setCategoryName(compData.categoryName);
			priceObj.setDeptName(compData.deptName);
			priceObj.setRetailerItemCode(compData.retailerItemCode);
			priceObj.setUpc(compData.upc);
			priceObj.setItemName(compData.itemName);
			retailPriceList.put(compData.itemcode, priceObj);			
			
		}
		
	}

	private StoreDTO getStoreInfo(Connection conn, String storeNum, int chainId) throws GeneralException {
		StringBuffer sql = new StringBuffer();

		StoreDTO  strDTO = null;
		sql.append(" SELECT COMP_STR_ID, COMP_STR_NO, ADDR_LINE1, CITY, ZIP, STATE ");
		sql.append(" FROM COMPETITOR_STORE ");
		if (chainId> 0){
			sql.append(" WHERE  comp_chain_id= " + chainId + " AND ");
		}else
			sql.append(" WHERE  ");
		sql.append(" COMP_STR_NO = '" + storeNum + "'");
		CachedRowSet crs =  PristineDBUtil.executeQuery(conn, sql, "getStoreInfo");
		
		try{
			if( crs.next()){
				strDTO = new StoreDTO();
				strDTO.strId = crs.getInt("COMP_STR_ID");
				strDTO.strNum = crs.getString("COMP_STR_NO");
				strDTO.addrLine1 = crs.getString("ADDR_LINE1");
				strDTO.city = crs.getString("CITY");
				strDTO.zip = crs.getString("ZIP");
				strDTO.state = crs.getString("STATE");
			}
		}catch(SQLException sqle){
			throw new GeneralException("Cached Row Access Exception - GetStoreInfo", sqle);
		}
		
		return strDTO;
	}
	


	private void initializeExcelSheet() {
		wb = new HSSFWorkbook();
		sheet = wb.createSheet();
		
	}

	
	
	public void writeToCell( HSSFRow row, int colCount, Object val) {
		if( val != null){
			if( val instanceof String  ){
				HSSFCell cell   = row.createCell(colCount,HSSFCell.CELL_TYPE_STRING);
				cell.setCellValue((String)val);
			}else if ( val instanceof Integer ){
				HSSFCell cell   = row.createCell(colCount,HSSFCell.CELL_TYPE_NUMERIC);
				Integer intObj = (Integer)val;
				cell.setCellValue(intObj.intValue());
			}
			else if ( val instanceof Double ){
				HSSFCell cell   = row.createCell(colCount,HSSFCell.CELL_TYPE_NUMERIC);
				Double dblObj = (Double)val;
				cell.setCellValue(dblObj.doubleValue());
			}
			else if ( val instanceof Float ){
				HSSFCell cell   = row.createCell(colCount,HSSFCell.CELL_TYPE_NUMERIC);
				Float flObj = (Float)val;
				cell.setCellValue(flObj.floatValue());
			}
		}
	}
	
	// Header - Store Number, Address Info
	// Date Analyzed
	// Department name, Category, Item Name, Retailer Item code, UPC, Exp Reg Price, Exp Sale Price, Act Reg Price, Act Sale Price, Qty, Comments 
	
	private void createHeaderRow(StoreDTO strDto, String analysisDate) {
		HSSFRow row = sheet.createRow(rowCount);
		
		int colCount = 0;
		writeToCell(row, colCount, "Store:");
		writeToCell(row, ++colCount, strDto.strNum + ", " + strDto.addrLine1 + ", " + strDto.city +", " +  strDto.state + " - " + strDto.zip);
		
		rowCount++;
		
		row = sheet.createRow(rowCount);
		colCount = 0;
		writeToCell(row, colCount, "Analysis Date:");
		writeToCell(row, ++colCount, analysisDate);

		rowCount++;
		rowCount++;
		row = sheet.createRow(rowCount);
		colCount = 0;
		writeToCell(row, colCount, "Major Category");
		writeToCell(row, ++colCount, "Category");
		writeToCell(row, ++colCount, "Item");
		writeToCell(row, ++colCount, "Retailer Item Code");
		writeToCell(row, ++colCount, "UPC");
		writeToCell(row, ++colCount, "Exp Reg Price");
		writeToCell(row, ++colCount, "Exp Sale Price");
		writeToCell(row, ++colCount, "Pos Reg Price");
		writeToCell(row, ++colCount, "Pos Sale Price");
		writeToCell(row, ++colCount, "Qty Sold");
		writeToCell(row, ++colCount, "Actual Price Effective Date");
		writeToCell(row, ++colCount, "Pos Price Last Effective Date");
		writeToCell(row, ++colCount, "Comments");
		
		
	}


	// Department name, Category, Item Name, Retailer Item code, UPC, Exp Reg Price, Exp Sale Price, Act Reg Price, Act Sale Price, Qty, Comments
	private void createMisMatchReport() {
		
		//Loop and print the price mismatches
		
		for(RetailPriceDTO posPrice: misMatchPriceList ){

			Integer itemCode = Integer.parseInt(posPrice.getItemcode());
			RetailPriceDTO actPriceObj = retailPriceList.get(itemCode);
			
			//***Janani ****Get the Item Details from Item_lookup_View
			
			rowCount++;
			HSSFRow row = sheet.createRow(rowCount);
		
			int colCount = 0;
			writeToCell(row, colCount, actPriceObj.getDeptName());
			writeToCell(row, ++colCount, actPriceObj.getCategoryName());
			writeToCell(row, ++colCount, actPriceObj.getItemName());
			//**** Janani Change this to Retailer Item code
			writeToCell(row, ++colCount, actPriceObj.getRetailerItemCode());  
			writeToCell(row, ++colCount, actPriceObj.getUpc());
			
			writeToCell(row, ++colCount, Double.valueOf(actPriceObj.getRegPrice()));
			writeToCell(row, ++colCount, Double.valueOf(actPriceObj.getSalePrice()));
			
			writeToCell(row, ++colCount, Double.valueOf(posPrice.getRegPrice()));
			writeToCell(row, ++colCount, Double.valueOf(posPrice.getSalePrice()));
			
			MovementDTO  movementData = misMatchMovementList.get(itemCode);
			double Qty = (movementData.getExtnWeight() > 0)?movementData.getExtnWeight():movementData.getExtnQty();

			writeToCell(row, ++colCount, Double.valueOf(Qty));
			writeToCell(row, ++colCount, posPrice.getRegEffectiveDate());
			writeToCell(row, ++colCount, posPrice.getUpdateTimeStamp());
			writeToCell(row, ++colCount, "Reg price mismatch");
		}
		
		return;
		
	}
	
	public void writeToFile(String fileName) throws GeneralException {
		try{
			FileOutputStream fileOut = new FileOutputStream(fileName);
			wb.write(fileOut);
			fileOut.close();
		} catch (IOException ioe){
			throw new GeneralException("Error in Excel File write... ",ioe);
		}

	}
	

}