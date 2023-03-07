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
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import com.pristine.dao.DBManager;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.ShippingInfoDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.StorePerformanceRollupDAO;
import com.pristine.dao.SummaryGoalDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.ShippingInfoDTO;
import com.pristine.dto.StorePerformanceRollupSummaryDTO;
import com.pristine.dto.SummaryGoalDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.Constants;

public class ShippingInfoLoaderExcel {

	private static final int FIRST_ROW_TO_PROCESS = 6;
	private static Logger  logger = Logger.getLogger("ShippingInfoLoader");
	private Connection 	_Conn = null;
	private String _xlsFilePath;
	private HashMap<String, Integer> _storeIdList;
	
	private int _rows_total_count = 1;
	private int _rows_inserted_count = 0;
	private int _rows_skipped_count = 0;
	
	private HashMap<String, Integer> _weekEndDates;
	
	final int CELL_INVOICE_DATE = 0;
	final int CELL_SHIPMENT_DATE = 2;
	final int CELL_STORE = 1;
	final int CELL_ITEM_DESC = 3;
	final int CELL_RETAILER_CODE = 4;
	final int CELL_UPC = 5;
	final int CELL_STORE_PACK = 6;
	final int CELL_CASES = 7;
	
	
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
	public ShippingInfoLoaderExcel ()
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
		PropertyConfigurator.configure("log4j-shipping-info-loader.properties");

        PropertyManager.initialize("analysis.properties");
        
        //Get the file path to locate the Excel file
        String excelFilePath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		
        if( !excelFilePath.equals("")) {
			excelFilePath = excelFilePath +"/ShippingData/";
	        //logger.debug("Excel Path       " + excelFilePath);
        }
        else {
			logger.error("Invalid configuration: Excel file path missing");
			System.exit(0);        	
        }
		
		String xlsFileName = "";
		int xlsSheetNumber = 0;
		
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
		if (args.length > 1) {

			xlsSheetNumber = Integer.parseInt(args[1]);
			logger.info("XLS Sheet Number  " + xlsSheetNumber);
		
		} else {

			logger.warn("Invalid input: Excel sheet number missing");

			//Assign first sheet as default
			xlsSheetNumber = 0;
		}		

		//Create object for class
		ShippingInfoLoaderExcel sIL = new ShippingInfoLoaderExcel();
		
		sIL._xlsFilePath = excelFilePath;
		
		//Call method to perform Summary Goal calculation
		try {
			sIL.loadShippingInfo(xlsFileName, xlsSheetNumber);
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
	private boolean loadShippingInfo(String xlsFileName, int xlsSheetNumber) throws Exception, GeneralException {
		
		//Object for competitive store data
		CompetitiveDataDAO 	objcompDataDAO;
		objcompDataDAO = new CompetitiveDataDAO(_Conn);

		//Variable to hold the excel file name with path
		String xlsFile = _xlsFilePath + xlsFileName;
		//logger.debug("File Name " + xlsFile);		
		Boolean processStatus = true;
		
		try {
			//Instance for file
			FileInputStream objXlsFile = new FileInputStream(xlsFile);
			
			//object for excel work book
			HSSFWorkbook objWbook = new HSSFWorkbook(objXlsFile);
			
			//Object for excel sheet
			HSSFSheet objSheet = objWbook.getSheetAt(xlsSheetNumber);


			_rows_total_count = objSheet.getPhysicalNumberOfRows();
			logger.info("Total Excel Rows to process " + String.valueOf(objSheet.getPhysicalNumberOfRows()));

			if (_rows_total_count >= EXCEL_MIN_ROW) {
				
				_storeIdList = new HashMap<String, Integer>();
				_weekEndDates = new HashMap<String, Integer>();
				ItemDAO itemDAO = new ItemDAO();
				ShippingInfoDAO sIDAO = new ShippingInfoDAO();
				List<ShippingInfoDTO> sIL = new ArrayList<ShippingInfoDTO>();
				//Run loop for every row in excel sheet
				
				Hashtable<String,ShippingInfoDTO> uHash = new Hashtable<String,ShippingInfoDTO>();
				for (int irow=FIRST_ROW_TO_PROCESS; irow<= _rows_total_count; irow++)	{

					//logger.debug("Processing Row   " + irow);
					//Assign the current row in a object for processing
					HSSFRow row = objSheet.getRow(irow);
		
					//Number of columns in every row should be four
					if (row.getPhysicalNumberOfCells() >=INPUT_FILE_COL_COUNT) {

						//initialize store id
						int storeId = -1;
						
						//Get the 1st cell value as store number
						String storeNumber = getStoreNumber(row.getCell(CELL_STORE));

						//Get the store ID for the store number 
						if (storeNumber != null) {
							storeId = getCompStore(storeNumber, objcompDataDAO);								
							//logger.debug("Store ID         " + String.valueOf(storeId));
						}

						/*if store id exist, proceed further
						  otherwise skip the current row and proceed to next */
						if (storeId > 0 ) {	
							boolean rowStatus = true;
							ShippingInfoDTO sI = new ShippingInfoDTO();

							sI.setInvoiceDate(row.getCell(CELL_INVOICE_DATE).getDateCellValue());
							sI.setShipmentDate(row.getCell(CELL_SHIPMENT_DATE).getDateCellValue());
							sI.setStoreID(storeId);
							sI.setCasesShipped((int) row.getCell(CELL_CASES).getNumericCellValue());

							sI.setRetailerCode(row.getCell(CELL_RETAILER_CODE).toString());
							
							String upc = row.getCell(CELL_UPC).toString();
							if(upc!=null && upc.length()>=12 ){
								upc = upc.substring(upc.length()-12);
							}
							sI.setUPC(upc);
							sI.setStorePack((int) row.getCell(CELL_STORE_PACK).getNumericCellValue());
							sI.setItemDesc(row.getCell(CELL_ITEM_DESC).toString());
							
							ItemDTO itemIN = new ItemDTO();
							itemIN.setUpc(sI.getUPC());
							ItemDTO item = itemDAO.getItemDetails(_Conn,itemIN);
							if(item!=null && item.getItemCode() != -1){
								if(item.getDeptID() != 19){
									 Calendar cal = Calendar.getInstance();
								        cal.setTime(sI.getShipmentDate());
								        cal.add(Calendar.DATE, 1); //minus number would decrement the days
								        cal.getTime();
									sI.setShipmentDate(cal.getTime());
								}
								sI.setItemCode(item.getItemCode());
								sI.setQuantity(sI.getStorePack()*sI.getCasesShipped());
								//if not in unique hash then insert, else sum
								String uKey = sI.getItemCode()+":"+sI.getStoreID()+":"+sI.getShipmentDate();
								if(uHash.containsKey(uKey)){
									ShippingInfoDTO sI0 = uHash.get(uKey);
									sI0.setCasesShipped(sI0.getCasesShipped()+sI.getCasesShipped());
									sI0.setQuantity(sI0.getQuantity()+sI.getQuantity());
								}else{
									uHash.put(uKey, sI);
									sIL.add(sI);
								}
							}else{
								rowStatus = false;
							}
							
							if (rowStatus) {
								_rows_inserted_count = _rows_inserted_count +1;
							}
							else {
								_rows_skipped_count =_rows_skipped_count + 1;
								logger.warn("Process terminated for Row     " + String.valueOf(irow));
							}
						}
						else {
							
							logger.warn("Invalid Store Number           "  + storeNumber);
							logger.warn("Process terminated for Row     " + String.valueOf(irow));
							_rows_skipped_count =_rows_skipped_count + 1;
						}
						
					}
					else {
						logger.warn("Insufficient number of columns ");
						logger.warn("Process terminated for Row     " + String.valueOf(irow));						
						_rows_skipped_count =_rows_skipped_count + 1;
					}
				
    				int proRecSize = irow;
    				if ( (proRecSize % VALUE_PROCESS_LOG) == 0 ) {
    					logger.info( "Rows processed....." + String.valueOf(proRecSize));
    				}
				
				}
				sIDAO.insertShippingInfoWrap(_Conn, sIL);
				logger.info("Processed Rows..." + String.valueOf(_rows_total_count));
				logger.info("Inserted Rows...." + String.valueOf(_rows_inserted_count));
				logger.info("Skipped Rows....." + String.valueOf(_rows_skipped_count));

			}
			else {
				logger.error("No data found in Excel Sheet......" + objSheet.getSheetName());
			}			
		
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