/*
 * Title: Shipping Info Loader
 * Loads shipping information
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/09/2013	Ganapathy		Created
 *******************************************************************************
 */

package com.pristine.dataload;

import java.sql.Connection;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pristine.dao.AdDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.ShippingInfoDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.StorePerformanceRollupDAO;
import com.pristine.dao.SummaryGoalDAO;
import com.pristine.dto.AdDataDTO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.ShippingInfoDTO;
import com.pristine.dto.StorePerformanceRollupSummaryDTO;
import com.pristine.dto.SummaryGoalDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.Constants;

public class AdLoader {

	private static final int FIRST_ROW_TO_PROCESS = 3;
	private static final String AD_XLS_SHEET_NAME = "Ad Financials";
	private static Logger  logger = Logger.getLogger("AdLoader");
	private Connection 	_Conn = null;
	private String _xlsFilePath;
	
	private HashMap<String, Integer> _storeIdList;
	
	private int _rows_total_count = 1;
	private int _rows_inserted_count = 0;
	private int _rows_skipped_count = 0;
	
	private HashMap<String, Integer> _weekEndDates;
	
	final int CELL_PAGE = 0;
	final int CELL_BLOCK = 1;
	final int CELL_ITEM_CODE = 3;
	final int CELL_ITEM_DESC = 4;
	final int CELL_STORE_LOCATION = 5;
	final int CELL_CATEGORY = 6;
	final int CELL_AD_LOCATION = 7;
	final int CELL_CASE_PACK = 8;
	final int CELL_CASE_COST = 9;
	final int CELL_UNIT_COST = 10;
	final int CELL_REG_PRICE = 11;
	final int CELL_MARGIN_PCT = 12;
	final int CELL_TPR = 13;
	final int CELL_INVOICE = 14;
	final int CELL_BILLBACK = 15;
	final int CELL_SCAN = 16;
	final int CELL_NET_UNIT_COST = 17;
	final int CELL_ORG_UNIT_AD_PRICE = 18;
	final int CELL_ORG_AD_RETAIL = 19;
	final int CELL_DISPLAY_TYPE = 32;


//	// 10/13/2012
//	final int CELL_PAGE = 0;
//	final int CELL_BLOCK = 1;
//	final int CELL_ITEM_CODE = 3;
//	final int CELL_ITEM_DESC = 4;
////	final int CELL_STORE_LOCATION = 5;
//	final int CELL_CATEGORY = 5;
//	final int CELL_AD_LOCATION = 6;
//	final int CELL_CASE_PACK = 7;
//	final int CELL_CASE_COST = 8;
//	final int CELL_UNIT_COST = 9;
//	final int CELL_REG_PRICE = 10;
//	final int CELL_MARGIN_PCT = 11;
//	final int CELL_TPR = 12;
//	final int CELL_INVOICE = 13;
//	final int CELL_BILLBACK = 14;
//	final int CELL_SCAN = 15;
//	final int CELL_NET_UNIT_COST = 16;
//	final int CELL_ORG_UNIT_AD_PRICE = 23;
//	final int CELL_ORG_AD_RETAIL = 24;
//	final int CELL_DISPLAY_TYPE = 39;

	
	final private int INPUT_FILE_COL_COUNT = 8;
	
	final private int EXCEL_MIN_ROW = 7;
	final private double VALUE_DEFAULT_NULL = -99;
	final private int VALUE_PROCESS_LOG = 100;
	
	/*
	 *****************************************************************
	 * Class constructor
	 * Argument : null
	 * @throws Exception
	 * ****************************************************************
	 */	
	public AdLoader ()
	{
        try
		{
        	//Create DB connection
        	_Conn = DBManager.getConnection();
	        _Conn.setAutoCommit(true);
	    }
		catch (GeneralException gex) {
	        logger.error(gex);
	    } catch (SQLException se) {
	        logger.error(se);
		}
	}


	/*
	 *****************************************************************
	 * Main method of Batch
	 * Argument 1: Excel File Name
	 * Argument 2: Excel Sheet Number
	 * ****************************************************************
	 */	
	
	public static void main(String[] args) throws Exception 
	{
		PropertyConfigurator.configure("log4j-ad-loader.properties");

        PropertyManager.initialize("analysis.properties");
        
        //Get the file path to locate the Excel file
        String excelFilePath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		
        if( !excelFilePath.equals("")) {
			excelFilePath = excelFilePath +"/AdData/";
	        //logger.debug("Excel Path       " + excelFilePath);
        }
        else {
			logger.error("Invalid configuration: Excel file path missing");
			System.exit(0);        	
        }
		
		String xlsFileName = "";
		String xlsSheetName = AD_XLS_SHEET_NAME;
		
		//Get Excel file name command line argument, this is the first parameter
		if (args.length > 0) {
			
			xlsFileName = args[0];
			logger.info("Excel File Name  " + xlsFileName);

		} else {
			logger.error("Invalid input: Excel file name missing");
			System.exit(0);
		}

		/*Get Excel sheet number, this is the first parameter
		If the Sheet number is not provided than take 0 as default*/
		
		//Create object for class
		AdLoader sIL = new AdLoader();
		
		sIL._xlsFilePath = excelFilePath;
		
		//Call method to perform Summary Goal calculation
		try {
			sIL.loadAdData(xlsFileName, xlsSheetName);
		} catch (GeneralException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
			e.printStackTrace();
		}
		
		// Close the connection
		PristineDBUtil.close(sIL._Conn);
	}
	
	
    /*
	 *****************************************************************
	 * Function to get process the summary goal data from excel file
	 * Argument 1: Excel file name with extension
	 * Argument 2: Excel Sheet number 
	 * Return : void 
     * @throws GeneralException, SQLException, IOException 
	 *****************************************************************
	 */
	private boolean loadAdData(String xlsFileName, String xlsSheetName) throws Exception, GeneralException {
		
		//Object for competitive store data
		CompetitiveDataDAO 	objcompDataDAO;
		objcompDataDAO = new CompetitiveDataDAO(_Conn);

		//Variable to hold the excel file name with path
		String xlsFile = _xlsFilePath + xlsFileName ;
		//logger.debug("File Name " + xlsFile);		
		Boolean processStatus = true;
		
		String strDate = xlsFileName.substring(0, 8).replace("_", "/"); 
		SimpleDateFormat sf = new SimpleDateFormat("MM/dd/yy");
		Date d = sf.parse(strDate);
		SimpleDateFormat nf = new SimpleDateFormat("MM/dd/yyyy");
		strDate = nf.format(d);
		RetailCalendarDAO calendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calDTO = calendarDAO.getCalendarId(_Conn, strDate, "W");
		try {
			//Instance for file
			FileInputStream objXlsFile = new FileInputStream(xlsFile);
			ItemDAO itemDAO = new ItemDAO();
			
			//object for excel work book
			HSSFWorkbook objWbook = new HSSFWorkbook(objXlsFile);
			
			//Object for excel sheet
			HSSFSheet objSheet = objWbook.getSheet(xlsSheetName);


			_rows_total_count = objSheet.getPhysicalNumberOfRows();
			logger.info("Total Excel Rows to process " + String.valueOf(objSheet.getPhysicalNumberOfRows()));

				List<AdDataDTO> aDL = new ArrayList<AdDataDTO>();
				HashSet<Integer> itemL = new HashSet<Integer>();
				for (int irow=FIRST_ROW_TO_PROCESS; irow<= _rows_total_count; irow++)	{
					HSSFRow row = objSheet.getRow(irow);
					if(row==null || row.getCell(CELL_PAGE)==null){
						continue;
					}
					if(row.getCell(CELL_PAGE).getCellType() == HSSFCell.CELL_TYPE_STRING && row.getCell(CELL_PAGE).getStringCellValue().startsWith("Pulled")){
						logger.debug("Done...........!");
						break;
					}
					if(row.getCell(CELL_PAGE).getCellType() != HSSFCell.CELL_TYPE_NUMERIC || row.getCell(CELL_PAGE).getNumericCellValue() ==0){
						continue;
					}
					
					String retailerItemCode ="";
					if(row.getCell(CELL_ITEM_CODE).getCellType() == HSSFCell.CELL_TYPE_NUMERIC){
						retailerItemCode = (int)row.getCell(CELL_ITEM_CODE).getNumericCellValue()+"";
					}else{
					retailerItemCode = row.getCell(CELL_ITEM_CODE).getStringCellValue();
					}
					retailerItemCode = retailerItemCode.replaceAll("[^-0-9]", "");
//					logger.debug(row.getCell(CELL_PAGE).getNumericCellValue());
//					logger.debug(row.getCell(CELL_BLOCK).getNumericCellValue());
					
//					logger.debug(retailerItemCode);
					String itemDesc = row.getCell(CELL_ITEM_DESC).getStringCellValue();
					itemDesc = itemDesc.trim();
//					logger.debug(row.getCell(CELL_AD_LOCATION).getStringCellValue());
//					logger.debug(row.getCell(CELL_TPR).getNumericCellValue());
//					logger.debug(row.getCell(CELL_ORG_UNIT_AD_PRICE).getNumericCellValue());
//					logger.debug(row.getCell(CELL_ORG_AD_RETAIL).getNumericCellValue());
//					logger.debug(row.getCell(CELL_DISPLAY_TYPE).getStringCellValue());

					ItemDTO itemIN = new ItemDTO();
					itemIN.setUpc(retailerItemCode);
					itemIN.setRetailerItemCode(retailerItemCode);
					itemIN.setItemName(itemDesc.trim());
					ItemDTO item = itemDAO.getItemDetailsFuzzyLookup(_Conn,itemIN);
					if((item == null) ||item.getItemCode() == -1){
						logger.debug("Not matched : "+retailerItemCode+" : "+itemDesc);
					}	else if(itemL.contains(item.getItemCode())){
						logger.debug("Multiple entires : "+retailerItemCode+" : "+itemDesc);
						
					}else{
						itemL.add(item.getItemCode());
						AdDataDTO aD = new AdDataDTO();
						aD.setCalendarId(calDTO.getCalendarId());
						String page ="";
						if(row.getCell(CELL_PAGE).getCellType() ==HSSFCell.CELL_TYPE_NUMERIC){
							page = ((int)row.getCell(CELL_PAGE).getNumericCellValue()) +"";
						}
						else{
							page = row.getCell(CELL_PAGE).getStringCellValue();
						}
						page = page.trim();
						
						aD.setPage(page);
						String block = "";
						if(row.getCell(CELL_BLOCK).getCellType() ==HSSFCell.CELL_TYPE_NUMERIC){
							block = ((int) row.getCell(CELL_BLOCK).getNumericCellValue()) +"";
						}
						else{
							block = row.getCell(CELL_BLOCK).getStringCellValue();
						}
						aD.setBlock(block);
						
						if(page == null || block == null || page.length()==0 || block.length()==0){
							logger.debug("Invalid/Empty Page or Block");
							continue;
						}
						aD.setRetailerItemCode(retailerItemCode);
						aD.setPrestoItemCode(item.getItemCode());
						aD.setItemDescription(itemDesc);
						

						String adLocation  ="";
						if(row.getCell(CELL_AD_LOCATION).getCellType() ==HSSFCell.CELL_TYPE_NUMERIC){
							adLocation = row.getCell(CELL_AD_LOCATION).getNumericCellValue() +"";
						}
						else{
							adLocation = row.getCell(CELL_AD_LOCATION).getStringCellValue();
						}
						aD.setAdLocation(adLocation);

						String storeLocation  ="";
//						if(row.getCell(CELL_STORE_LOCATION).getCellType() ==HSSFCell.CELL_TYPE_NUMERIC){
//							storeLocation = row.getCell(CELL_STORE_LOCATION).getNumericCellValue() +"";
//						}
//						else{
//							storeLocation = row.getCell(CELL_STORE_LOCATION).getStringCellValue();
//						}
						aD.setStoreLocation(storeLocation);
						
						Double regPrice  = 0.0;
						if(row.getCell(CELL_REG_PRICE).getCellType() ==HSSFCell.CELL_TYPE_NUMERIC || row.getCell(CELL_REG_PRICE).getCellType() ==HSSFCell.CELL_TYPE_FORMULA){
							regPrice = row.getCell(CELL_REG_PRICE).getNumericCellValue() ;
						}
						else{
							String s = row.getCell(CELL_REG_PRICE).getStringCellValue();
							s = s.replace("$", "");
							s = s.replace(",", "");
							if(s.trim().length()>0){
							regPrice = Double.parseDouble(s);
							}
						}
						aD.setRegPrice(regPrice);
						
						String tpr  ="";
						if(row.getCell(CELL_TPR).getCellType() ==HSSFCell.CELL_TYPE_NUMERIC || row.getCell(CELL_TPR).getCellType() ==HSSFCell.CELL_TYPE_FORMULA){
							tpr = row.getCell(CELL_TPR).getNumericCellValue() +"";
						}
						else{
							tpr = row.getCell(CELL_TPR).getStringCellValue();
						}
						aD.setOnTpr(tpr);
						

						aD.setOrgUnitAdPrice(row.getCell(CELL_ORG_UNIT_AD_PRICE).getNumericCellValue());
						
						String adRetail = "";
						if(row.getCell(CELL_ORG_AD_RETAIL).getCellType() ==HSSFCell.CELL_TYPE_NUMERIC || 
								(row.getCell(CELL_ORG_AD_RETAIL).getCellType() ==HSSFCell.CELL_TYPE_FORMULA && row.getCell(CELL_ORG_AD_RETAIL).getCachedFormulaResultType() ==HSSFCell.CELL_TYPE_NUMERIC)){
							adRetail = row.getCell(CELL_ORG_AD_RETAIL).getNumericCellValue() +"";
						}
						else{
							adRetail = row.getCell(CELL_ORG_AD_RETAIL).getStringCellValue();
						}
						aD.setOrgAdRetail(adRetail);
						
						String displayType = "";
						if(row.getCell(CELL_DISPLAY_TYPE).getCellType() ==HSSFCell.CELL_TYPE_NUMERIC){
							displayType = row.getCell(CELL_DISPLAY_TYPE).getNumericCellValue() +"";
						}
						else{
							displayType = row.getCell(CELL_DISPLAY_TYPE).getStringCellValue();
						}
						aD.setDisplayType(displayType);
						aDL.add(aD);
						logger.debug(retailerItemCode +":"+itemDesc + " : "+item.getItemCode()+":"+item.getItemName());
					}
    				int proRecSize = irow;
    				if ( (proRecSize % VALUE_PROCESS_LOG) == 0 ) {
    					logger.info( "Rows processed....." + String.valueOf(proRecSize));
    				}
				
				}
				AdDataDAO aDDao = new AdDataDAO();
				aDDao.deleteAdData(_Conn, calDTO.getCalendarId());
				aDDao.insertAdData(_Conn, aDL);
				aDDao.expandAdDataToLIG(_Conn, calDTO.getCalendarId());
				logger.info("Processed Rows..." + String.valueOf(_rows_total_count));
				logger.info("Inserted Rows...." + String.valueOf(_rows_inserted_count));
				logger.info("Skipped Rows....." + String.valueOf(_rows_skipped_count));

		
			objSheet = null;
			objWbook = null;
			objXlsFile.close();
			
		} 
		catch (FileNotFoundException fe) {
			logger.error("Invalid input : Excel file missing " + xlsFileName);
			processStatus = false;
		}
		catch (IOException ie) {
			logger.error(ie.getLocalizedMessage());
			processStatus = false;
		}
		catch (IllegalArgumentException ae) {
			logger.error(ae.getLocalizedMessage());
			processStatus = false;
		}			
	return processStatus;
	}
	
	
	
	/*
	 *****************************************************************
	 * Function to move source file to destination based on status
	 * Argument 1: Store Number
	 * Return : int (Store ID) 
     * @throws GeneralException, SQLException 
	 *****************************************************************
	 */
	private void moveSourceFile(String xlsFileName) {
		/*if all the records are processed successfully, then move excel to completed folder
		  if any of the row is skipped then move the excel to bad folder */
		if (_rows_total_count == (_rows_inserted_count + 1) ) {
			//logger.debug("Move the file " + xlsFileName + " to completed folder");
			//logger.debug("File path " + (_xlsFilePath + Constants.COMPLETED_FOLDER));
			PrestoUtil.moveFile(_xlsFilePath + xlsFileName, _xlsFilePath + Constants.COMPLETED_FOLDER);					
		}
		else {
			//logger.debug("Move the file " + xlsFileName + " to bad folder");
			//logger.debug("File path " + (_xlsFilePath + Constants.BAD_FOLDER));
			PrestoUtil.moveFile(_xlsFilePath + xlsFileName, _xlsFilePath + Constants.BAD_FOLDER);
		}
	}
	
    
	/*
	 *****************************************************************
	 * Function to get keep the processed dates in a collection
	 * Argument 1: processed week end date 
	 * Return : void
     * *****************************************************************
	 */
	private void logProcessedDate(String weekEndDate) {
		
		if ((_weekEndDates.size() < 1 ) || (!_weekEndDates.containsKey(weekEndDate))) {
			_weekEndDates.put(weekEndDate, 1);
			//logger.debug("Week End Date added in collection " + weekEndDate);
		}
		/*else {
			//logger.debug("Week End Date exist already " + weekEndDate);
		}*/
	}

	
	/*
	 *****************************************************************
	 * Function to get the Store Number either from Excel cell data  
	 * Argument 1: Excel Cell
	 * Return : String (Store Number) 
	 *****************************************************************
	 */
	private String getStoreNumber(HSSFCell cellStore) {

		String strStoreNumber = null;
		
		//logger.debug("Cell Type        " + cellStore.getCellType());		
		
		if (cellStore.getCellType() == HSSFCell.CELL_TYPE_NUMERIC) {
		
			int storeNumber = (int) cellStore.getNumericCellValue();
			strStoreNumber = String.valueOf(storeNumber);
		
		}
		else if (cellStore.getCellType() == HSSFCell.CELL_TYPE_STRING) {
		
			strStoreNumber = cellStore.getStringCellValue();
		
		}
		
		strStoreNumber = strStoreNumber.substring(7, 11);
		//logger.debug("Store Number     " + strStoreNumber);
		return strStoreNumber;
		
	}
	
	
    /*
	 *****************************************************************
	 * Function to get competitor Store IDs
	 * Argument 1: Store Number
	 * Return : int (Store ID) 
     * @throws GeneralException, SQLException 
	 *****************************************************************
	 */
    private int getCompStore(String StoreNum, CompetitiveDataDAO objcomp)
    {
    	int storeID = -1;
		try {
			
			/*Check the input Store Number exist in the collection 
			  if exist then get the Store ID from the collection */
			if ((_storeIdList.size() > 0 ) && (_storeIdList.containsKey(StoreNum))) {
				
				storeID = _storeIdList.get(StoreNum);
				//logger.debug("Get Store Number form collection....");
				
			}
			else {
				
				//logger.debug("Get store Number from DB............");
				//Call the DAO to get the Store ID 
				ArrayList<String> storeIdList = objcomp.getStoreIdList(_Conn, StoreNum);
				if (storeIdList.size() >0 ) {
					//Assign the Store ID into private variable
					storeID = Integer.parseInt(storeIdList.get(0));
					//Store the Store Id in the collection with Store Number as key
					_storeIdList.put(StoreNum, storeID);
				}
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}    		
    return storeID;
    }
}