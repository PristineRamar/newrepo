/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.pristine.dataload;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.TransactionLogDTO;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.customer.CustomerDAO;
import com.pristine.dao.logger.TransactionLogTrackerDAO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;

/**
 *
 * @author sakthidasan
 * Compatible for TOPS and Ahold.
 * Input for TOPS : 30 mins Tlog file.
 * Input for Ahold : Formatted input file.
 * File formatter for Ahold : com.pristine.fileformatter.ahold.MovementFileFormatter 
 * 
 * 
 *******************************************************************************************************************
 *     Date       *  Version  *	     Modified by          *                     	Comments              		   *		 		
 *******************************************************************************************************************
 *	  		      * Initial   *	     sakthidasan          *                                                        *
 ******************************************************************************************************************* 
 *	03/27/2014    *   0.1     *	     Pradeep              *   Proudct hierarchy changes   / Ahold Tlog issues      *
 *******************************************************************************************************************
 */
public class TransactionLogLoader extends PristineFileParser
{
	static Logger logger = Logger.getLogger("MovementDataLoad");
  	Connection _Conn = null;
	private int _StopAfter = -1;
	private Set<String> _UnprocessedStores = new HashSet<String>();
	private HashMap<String, Integer> _itemHashMap = new HashMap<String, Integer>(); 
	private HashMap<String, Integer> _upcMissinghMap = new HashMap<String, Integer>(); 
	private HashMap<String, String> _calendarMap = new HashMap<String, String>(); 
	private int _processCalendarId = -1;
	private int _calIdType = 1;		//Populate Calendar Id type
	private HashMap<String, Integer> _storeMap = new HashMap<String, Integer>();
	private HashMap<String, Integer> _custMap;
	private HashMap<String, Integer> _newCustomerMap = new HashMap<String, Integer>();//Card No and Store ID
	private int _storeFromConfig = 0;
	private int _unclassifiedSegment = 0;
	private int _unclassifiedSubCat = 0;
	private int _unclassifiedCat = 0;
	private int _unclassifiedDept = 0;
	private int _unclassifiedSubcatProductId = 0;
	private int _unclassifiedSegmentProductId = 0;
	private int updateDBAfter = 0;
	private int newItemCount = 0;
	private boolean preloadItemCache = false;
	private boolean preloadCustCache = false;
	// Changes to retrieve item_code using upc and retailer_item_code
	boolean checkRetailerItemCode = false;
	private HashMap<Integer, HashMap<String, Integer>> _itemHashMapStoreUpc = new HashMap<Integer, HashMap<String, Integer>>();
	private HashMap<String, Integer> compIdMap = new HashMap<String, Integer>();
	private static boolean ignoreTruncStoreNum = false;
	public TransactionLogLoader (String StoreFromConfig)
	{
		super ("analysis.properties");
		try {
			//Create DB connection
			_Conn = DBManager.getConnection();

			//Get the Chain Id
			int prestoSubscriber = Integer.parseInt(PropertyManager.getProperty("PRESTO_SUBSCRIBER"));
			
			//Load the target store from DB or Configuration
			if (StoreFromConfig.equalsIgnoreCase("Y")){
				
				logger.debug("Load Processing Stores from configuration");
				String store = PropertyManager.getProperty("MOVEMENTLOAD_STORES");
				
				String[] storeArr = store.split(",");
				_storeMap = new CompStoreDAO().getCompStoreData(_Conn, prestoSubscriber, storeArr);
				
				_storeFromConfig = 1;
			}
			else {
				logger.debug("Load Processing Stores from Database");
				_storeMap = new CompStoreDAO().getCompStoreData(_Conn, prestoSubscriber, null);
			}
			
			_custMap = new HashMap<String, Integer>();
			
			_unclassifiedSegment = Integer.parseInt(PropertyManager.getProperty("ITEM.UNCLASSIFIED_SEGMENT", "0"));
			_unclassifiedSubCat= Integer.parseInt(PropertyManager.getProperty("ITEM.UNCLASSIFIED_SUB_CATEGORY", "0"));
			_unclassifiedCat= Integer.parseInt(PropertyManager.getProperty("ITEM.UNCLASSIFIED_CATEGORY", "0"));
			_unclassifiedDept = Integer.parseInt(PropertyManager.getProperty("ITEM.UNCLASSIFIED_DEPARTMENT", "0"));
			this._unclassifiedSegmentProductId = Integer.parseInt(PropertyManager
					.getProperty("ITEM.UNCLASSIFIED_SEGEMENT_PRODUCT_ID", "0"));
			this._unclassifiedSubcatProductId = Integer.parseInt(PropertyManager
					.getProperty("ITEM.UNCLASSIFIED_SUB_CAT_PRODUCT_ID", "0"));
			
			checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
			preloadItemCache = Boolean.parseBoolean(PropertyManager.getProperty("MOVEMENTLOAD.PRELOAD_ITEM_CACHE", "TRUE"));
			preloadCustCache = Boolean.parseBoolean(PropertyManager.getProperty("MOVEMENTLOAD.PRELOAD_CUSTOMER_CACHE", "TRUE"));
			updateDBAfter = Integer.parseInt(PropertyManager.getProperty("MOVEMENTLOAD.COMMITRECOUNT", "25000"));
			compIdMap = new StoreDAO().getStoreIdMap(_Conn);
		}
		catch (GeneralException ex) {
		}
	}
	
	/*
	 * Main  Method for the batch 
	 * Argument 1 : Subfolder name
	 * Argument 2 : Updatemode
	 * Argument 3 : Calendartype (Single =1 , Multiple = 2 , Default =0)
	 *  
	 */
	public static void main(String[] args)
	{
		PropertyConfigurator.configure("log4j-transaction-log-loader.properties");
		logCommand (args);
		
    	String subFolder = null;
    	boolean updateTransactions = false;
    	int calendarType = 0;
    	Date processDate = null;
    	DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		String storeFromConfig = "N";
		String targetTableType = null;
		String targetTable = "TRANSACTION_LOG";
		
		logger.info("main() - Started");
		
		for (int ii = 0; ii < args.length; ii++) {
			
			String arg = args[ii]; 
			
			if (arg.startsWith("SUBFOLDER")) {
				subFolder = arg.substring("SUBFOLDER=".length());
			}

			if (arg.startsWith("UPDATEMODE")) {
				String input = arg.substring("UPDATEMODE=".length());
				try {
					updateTransactions = Boolean.parseBoolean(input);
				} catch (Exception exe) {
					logger.error("main() - Input Parameter : UPDATEMODE : " + exe);
					logger.info("main() - Ended");
					System.exit(1);
				}
			}

			if (arg.startsWith("CALENDARTYPE")) {
				String calTypeStr = arg.substring("CALENDARTYPE=".length());
				if (calTypeStr.equalsIgnoreCase("PARAMETER"))
					calendarType = 0;
				else if (calTypeStr.equalsIgnoreCase("SINGLE"))
					calendarType = 1;
				else if (calTypeStr.equalsIgnoreCase("MULTI"))
					calendarType = 2;
			}
			
			if (calendarType == 0) {
				if (arg.startsWith("PROCESSDATE")) {
					
					String processDateStr = arg.substring("PROCESSDATE=".length());
					try {
						processDate = dateFormat.parse(processDateStr);
					} catch (ParseException par) {
						logger.error("main() - Process Date Parsing Error, check Input");
					}
				}
			}
			
			if (arg.startsWith("STORECONFIG")) {
				storeFromConfig = arg.substring("STORECONFIG=".length());
			}
			
			if (arg.startsWith("TARGETTYPE")) {
				targetTableType = arg.substring("TARGETTYPE=".length());
				
				if (targetTableType.equalsIgnoreCase("DAILY"))
					targetTable = "MOVEMENT_DAILY";
				if (targetTableType.equalsIgnoreCase("HOURLY"))
					targetTable = "MOVEMENT_HOURLY";					
				if (targetTableType.equalsIgnoreCase("ITEM"))
					targetTable = "MOVEMENT_ITEM_SUMMARY";	
			}
			if(arg.startsWith("TARGETTABLE")){
				targetTable = arg.substring("TARGETTABLE=".length());
			}
			
			if(arg.startsWith("IGNORE_TRUNC_STORE_NUM")){
				ignoreTruncStoreNum = Boolean.parseBoolean(
						arg.substring("IGNORE_TRUNC_STORE_NUM=".length()));
			}
		}

		logger.info("main() - Target Table " + targetTable);
		
		TransactionLogLoader dataload = new TransactionLogLoader(storeFromConfig);
		dataload._UpdateTransactions = updateTransactions;
		dataload._calIdType = calendarType;
		
		dataload.process(subFolder, processDate, targetTable);
		
		logger.info("main() - Ended");
    }
	
	public void processRecords(List listobj) throws GeneralException
	{
	}
	
	private void process (String subFolder, Date processDate, String targetTable)
	{
		// Update_Transactions
		String msg = "process() - Transaction file processing started" + (_UpdateTransactions == true ? ": updating Transactions" : "");
		logger.info(msg);
		
        try
        {
		
			try {
				_StopAfter = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
			}
			catch (Exception ex) {
				_StopAfter = -1;
			}
			
			if ((_calIdType == 0) && (processDate != null)) {
				String calKey = processDate.getDate() + "_" + processDate.getMonth() + "_" + processDate.getYear();
				getRetailCalendarIDV2(_Conn, processDate,  calKey, Constants.CALENDAR_DAY);
			}
			//Preload Item cache.
			if(preloadItemCache){
				getItemCodeList();
				if(checkRetailerItemCode){
					getAuthorizedItems();
				}
				logger.info("process() - Item codes are cached. Size of Cache - c1 = " + _itemHashMap.size() + " c2 = " + _itemHashMapStoreUpc.size());
			}
			
			//getzip files
			ArrayList<String> zipFileList = getZipFiles(subFolder);
			
			logger.info("process() - Number of Zip files to be processed=" + zipFileList.size());
			
			
			//Start with -1 so that if any reqular files are present, they are processed first
			int nFilesProcessed = 0;
			int curZipFileCount = -1;
			boolean processZipFile = false;
			String zipFilePath = getRootPath() + "/" + subFolder;
			do
			{
				ArrayList<String> files = null;
				boolean commit = false;
			
				try
				{
					if (processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;

					files = getFiles(subFolder);
					
					if ( files != null && files.size() > 0 )
					{						
						if (processZipFile)
							logger.info("process() - files to be processed after unzip=" + files.size());
						else
							logger.info("process() - Non zip files uploaded!  Count=" + files.size());
						
					
						/*
						 * RUN 0 is to setup Customer Id records after reading the file
						 * RUN 1 is to setup T-LOG records after reading the file one more time
						 * */
						 
						
						for (int ii = 0; ii < files.size(); ii++) {
							String file = files.get(ii);
							nFilesProcessed++;
							int run = 0;
							do {
								if (run > 0) {
									logger.info("Run " + run + " - Tlog Loading process() - Processing file  "
											+ nFilesProcessed + "...  file_name=" + file);

									commit = readAndLoadMovements(file, '|', targetTable);
									logger.info("Skipping " + run);
								} else {
									// Preload customer cache.
									if (preloadCustCache) {
										getCustomerList();
										// logger.info("process() - Customer ids
										// are cached.");
									}

									logger.info("Run " + run + " Customer Setup process() - Processing file  "
											+ nFilesProcessed + "...  file_name=" + file);
									preProcessCustomers(file, '|');

									logger.info("setting up customers count =  " + _newCustomerMap.size());
									// Preprocessing run, then insert the
									// customers.
									CustomerDAO custdao = new CustomerDAO();
									int newCustomerCount = custdao.setupNewCustomerCards(_Conn, _newCustomerMap,
											updateDBAfter);
									logger.info("# of new customers setup =  " + newCustomerCount);
									// Reload the customer list.
									// Preload customer cache.
									if (preloadCustCache) {
										getCustomerList();
									}
								}
								run++;
							} while (run < 2);
						}
					}
					else
					{
						if( processZipFile)
						{							
							logger.error("process() - Csv/txt file NOT FOUND after unzip!");
						}
					}
				} catch (GeneralException ex) {
			        logger.error("process() - GeneralException=", ex);
			    } catch (Exception ex) {
			        logger.error("process() - Exception=", ex);
			    }
		    
				// Delete only the uncompressed files 
			    if( processZipFile){
			    	
			    	if (files != null)
			    	{
			    		if (files.size() > 0)
			    		{
			    			logger.info("process() - DELETING files..");
			    			PrestoUtil.deleteFiles(files);
			    		}
				    	files.clear();
				    	files.add(zipFileList.get(curZipFileCount));
			    	}
			    	
			    }
			    
			    // Move the file
		    	if (files != null)
		    	{
				    if ( files.size() > 0 )
				    {
				    	// ??? FLAW - Files moved based on the last file status.  
				    	// 		In case of multiple files inside zip this will not work!
						if( commit ){
							PrestoUtil.moveFiles(files, zipFilePath + "/" + Constants.COMPLETED_FOLDER);
						}
						else{
							logger.info("process() - Moving 'Failed files' to BAD folder...");
							PrestoUtil.moveFiles(files, zipFilePath + "/" + Constants.BAD_FOLDER);
						}
				    }
		    	}			    
				curZipFileCount++;
				processZipFile = true;
				
        	} while (curZipFileCount < zipFileList.size());

			if ( nFilesProcessed > 0 )
				logger.info("process() - Transaction file processing completed.");
			else 
				logger.warn("process() - No transaction file was processed!");

        	PristineDBUtil.close(_Conn);
		}
        catch (Exception ex) {
            logger.error("process() - Exception=", ex);
        }
        catch (GeneralException ex) {
            logger.error("process() - GeneralException=", ex);
        }
	}
	
	private boolean preProcessCustomers(String fileName, char delimiter)
			throws Exception, GeneralException {
		_newCustomerMap = new HashMap<String, Integer>();
		CsvReader reader = readFile(fileName, delimiter);
        String line[];
        int total_count = 0;
        int previousTranId = -1;
        int previousStoreId = -1;
        boolean ret = true;
        try
        {
        	int error_code; 
	        while (reader.readRecord())
	        {
	        	error_code = 0;
	            line = reader.getValues();
	            total_count++;
	            if ( line.length > 0 )
	            {
					String store = line[1];
					if(!ignoreTruncStoreNum)
						store = store.substring(store.length() - 4);	// keep only last 4 characters
					// Skip store if not listed in the list
					// Process all stores if list is empty
					
					int storeId = getStoreId(store);
					if ( storeId == 0) {
						logger.debug("storeId is zero for  " + store);
							continue;
					}
					
					if( previousStoreId != storeId){
						previousTranId = -1;
						previousStoreId = storeId;
					}
						
					
					int termNo = Integer.parseInt(line[2]);
					
					// 3: IDW-ITEM-TRANSNUM PIC 9(04).
					int tranNo = Integer.parseInt(line[3]);
					int transactionId = termNo * 10000 + tranNo;
					
					if( previousTranId == transactionId){
						continue;
					}
					previousTranId = transactionId;
	
					String customerCardNo = line[6] != null ? line[6] : "";
					
					 if (customerCardNo.trim().length() > 0){
						 if(!_custMap.containsKey(customerCardNo) && !_newCustomerMap.containsKey(customerCardNo)) {
							 _newCustomerMap.put(customerCardNo,storeId);
						 }
					 }
					
	            }
	        }
        }  catch (Exception ex) {
        	logger.error("readAndLoadMovements() - Exception=", ex);
        	ret = false;

        }
        catch (GeneralException ex) {
        	logger.error("readAndLoadMovements() - GeneralException=", ex);
        	ret = false;

        }
        finally {
        
        	if( reader != null){
        		reader.close();
        	}
        	logger.info(" # of records processed - " + total_count);
        }
		return ret;
	}

	private boolean readAndLoadMovements (String fileName, char delimiter, 
					String targetTable) throws Exception, GeneralException {
		
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		String processDateTime = dateFormat.format(new Date());
		SimpleDateFormat fileDateFormat = null;
		TransactionLogTrackerDAO trackDAO = new TransactionLogTrackerDAO();
		String processStatus = "Started";
		
		String fileNameStr = fileName.substring(fileName.lastIndexOf('/') + 1);
		
		int trackId = trackDAO.insertTLogTrack(_Conn, fileNameStr, 
										processDateTime, processStatus);
		if (trackId > 0) 
			_Conn.commit();
		
		logger.info("readAndLoadMovements() - trackId=" + trackId);
		//ArrayList<MovementDTO> listMoment = new ArrayList<MovementDTO>();
		ArrayList<TransactionLogDTO> transList = new ArrayList<TransactionLogDTO>();
		ArrayList<TransactionLogDTO> exceptionList = new ArrayList<TransactionLogDTO>();
		
		boolean ret;
		
        CsvReader reader = readFile(fileName, delimiter);
        String line[];
        
        MovementDAO movementDao = new MovementDAO();
        
        int saved_count = 0;
        int total_count = 0, countFailed = 0, countSkipped = 0;
        int processStat = 0;
        
        int previousTranId = -1;
        int currentTranId = -1;
        int prevCustomerId = -1;
        int prevCalendarId = -1;
        int previousStoreId = -1;
        try
        {
        	int error_code; 
	        while (reader.readRecord())
	        {
	        	error_code = 0;
	            line = reader.getValues();
	            total_count++;
	            //logger.debug("Processing record count....." + count);

				//MovementDTO dto = new MovementDTO(); //Unwanted
				TransactionLogDTO tLogdto = new TransactionLogDTO();
	            
	            if ( line.length > 0 )
	            {
	            	try {
	            		// 0: IDW-ITEM-COMPANY PIC 9(05).
						
						// 1: IDW-ITEM-STORE PIC 9(06).
						String store = line[1];
						if(!ignoreTruncStoreNum)
							store = store.substring(store.length() - 4);	// keep only last 4 characters
						// Skip store if not listed in the list
						// Process all stores if list is empty
						
						tLogdto.setCompStoreNumber(store);
						
						int storeId = getStoreId(store);
						
						if (_storeFromConfig == 0 && storeId == 0){
							countSkipped++;
							_UnprocessedStores.add(store);
							tLogdto.setErrorMessage("Invalid Store Number");
							error_code = 1;
						}

						if (_storeFromConfig == 1 && storeId == 0) {
							countSkipped++;
							_UnprocessedStores.add(store);
							tLogdto = null;
							continue;
						}

						if( previousStoreId != storeId){
							previousTranId = -1;
							previousStoreId = storeId;
						}
						//dto.setItemStore(store);
						tLogdto.setCompStoreId(storeId);
												
						// 2: IDW-ITEM-TERMINAL PIC 9(04).
						int termNo = Integer.parseInt(line[2]);
						
						// 3: IDW-ITEM-TRANSNUM PIC 9(04).
						int tranNo = Integer.parseInt(line[3]);
						
						currentTranId = termNo * 10000 + tranNo;
						tLogdto.setTransactionNo(currentTranId);
						
						// 4: IDW-ITEM-OPERATOR PIC 9(10).
						
						// 5: IDW-ITEM-CCYYMMDDHHMM PIC 9(12).
						
						
						
						String timestamp = line[5];
						/*int year = Integer.parseInt(timestamp.substring(0,4));
						int month = Integer.parseInt(timestamp.substring(4,6)) - 1;
						int date = Integer.parseInt(timestamp.substring(6,8));
						int hour = Integer.parseInt(timestamp.substring(8,10));
						int minute = 0;
						int second = 0;
						java.util.Calendar cal = java.util.Calendar.getInstance();
						//Date: 03/18/2015 Added by Pradeep.
						//Include seconds from Ahold Tlog feed.
						if(timestamp.length() == Constants.LENGTH_TRX_TIMESTAMP_WITH_SECONDS)
						{
							minute = Integer.parseInt(timestamp.substring(10, 12));
							second = Integer.parseInt(timestamp.substring(12));
							cal.set(year, month, date, hour, minute, second);
						}
						else
						{
							minute = Integer.parseInt(timestamp.substring(10));
							cal.set(year, month, date, hour, minute);
						}*/
						
						if(timestamp.length() == Constants.LENGTH_TRX_TIMESTAMP_WITH_SECONDS){
							fileDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
						}
						else{
							fileDateFormat = new SimpleDateFormat("yyyyMMddHHmm");
						}
					
						java.util.Calendar cal = java.util.Calendar.getInstance();
						Date trx_time = fileDateFormat.parse(timestamp);
						cal.setTime(trx_time);
						tLogdto.setTransationTimeStamp(trx_time);
				
						
						int calendarId = 0;
						
						if( previousTranId == currentTranId){
							calendarId = prevCalendarId;
						}else{
							
							String calKey = Integer.toString(cal.get(Calendar.YEAR)) + Integer.toString(cal.get(Calendar.MONTH)) + Integer.toString(cal.get(Calendar.DATE));

							if ((_calIdType == 1) || (_calIdType == 0)){
								calendarId = getRetailCalendarIDV2(_Conn, tLogdto.getTransationTimeStamp(),  calKey, Constants.CALENDAR_DAY);
							}
							else{
								calendarId = getRetailCalendarID(_Conn, tLogdto.getTransationTimeStamp(),  calKey, Constants.CALENDAR_DAY);
							}
							
							prevCalendarId = calendarId;
						}
						
						if (calendarId > 0) {
							tLogdto.setCalendarId(calendarId);
						}
						else {
							logger.error("readAndLoadMovements() - Calendar Id NOT found for date " + trx_time + ", Check Retail Calendar Master data!");
							tLogdto.setErrorMessage("Calendar Id not found");
							countSkipped++;
							error_code = 2;
							//continue;
						}
						
						// 6: IDW-ITEM-CUSTOMER-ID PIC X(24).
						String customer = line[6] != null ? line[6] : "";
						tLogdto.setCustomerCardNo(customer);
						if( previousTranId != currentTranId && error_code == 0){
							//Get the customer id since the prev transaction and current transaction are different
							prevCustomerId = getCustomerId(customer, storeId);
						}
						tLogdto.setCustomerId(prevCustomerId);

						
						// 7: IDW-ITEM-UPC PIC 9(14).
						String UPC = line[7];
						
						//Changes for retailer item code..
						int itemCode = 0;
						if(error_code == 0){
							if(!checkRetailerItemCode){
								itemCode = getItemCode(UPC);
							}
							else{
								if(compIdMap.get(store) != null){
								int compStrId = compIdMap.get(store);
									if(compStrId > 0){
										itemCode = getItemCode(UPC, compStrId);
										if (itemCode == -1)
											itemCode = getItemCode(UPC);
									}
									else{
										countSkipped++;
										_UnprocessedStores.add(store);
										continue;
									}
								}
								else{
									countSkipped++;
									_UnprocessedStores.add(store);
									continue;
								}
							}
						}
						
						tLogdto.setItemUPC(UPC);
						if(itemCode > 0){
							tLogdto.setItemCode(itemCode);
						}
						else if(error_code == 0){
							error_code = 49;
							tLogdto.setErrorMessage("Invalid Item");
							//logger.info("Invalid item. UPC - " + UPC);
						}

						// 8: IDW-ITEM-NET-PRICE PIC X(12).
						double netPrice = Double.parseDouble(line[8].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						tLogdto.setItemNetPrice(netPrice);
						
						if (netPrice < -99999.99 || netPrice > 999999999.99)
						{
							error_code = 3;
							tLogdto.setErrorMessage("Invlid unit price");
						}
						
						
						// 9: IDW-ITEM-GROSS-PRICE PIC X(10).
						double regularPrice = Double.parseDouble(line[9].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						tLogdto.setGrossPrice(regularPrice);
						
						// 10: IDW-ITEM-POS-DEPT PIC 9(04).
						
						// for pos department
						int posDep = Integer.parseInt(line[10].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						tLogdto.setPOSDepartmentId(posDep);	
						
						// 11: IDW-ITEM-COUP-FAMILY-CURR PIC 9(03).
						// 12: IDW-ITEM-COUP-FAMILY-PREV PIC 9(03).
						// 13: IDW-EXTN-MULT-PRICE-GRP PIC 9(02).
						// 14: IDW-EXTN-DEAL-QTY PIC 9(08).
						// 15: IDW-EXTN-PRICE-MTHD PIC 9(02).
						// 16: IDW-EXTN-SALE-QTY PIC 9(02).
						// 17: IDW-EXTN-SALE-PRICE PIC 9(10)
						
						// 18: IDW-EXTN-QTY PIC X(10).
						double scannedUnits = Double.parseDouble(line[18].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						tLogdto.setQuantity(scannedUnits);
						if (scannedUnits < -999999 || scannedUnits > 999999)
						{
							error_code = 5;
							tLogdto.setErrorMessage("Invlid Quantity");
						}
						
						// 19: IDW-EXTN-WGT PIC X(12).
						double itemWeight = Double.parseDouble(line[19].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						tLogdto.setWeight(itemWeight);
						
						if (itemWeight < -9999.9999 || itemWeight > 9999.9999)
						{
							error_code = 6;
							tLogdto.setErrorMessage("Invlid Weight");
						}
						
						// 20: IDW-WEIGHTED-FLAG PIC 9(01).
						// 21: IDW-WEIGHTED-CNT PIC X(07).
						// 22: IDW-COUPON-USED PIC 9(02).
						
						// 23: IDW-EXTENDED-NET-PRICE PIC X(12).
						double extNetPrice = Double.parseDouble(line[23].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						tLogdto.setExtendedNetPrice(PrestoUtil.round(extNetPrice, 2));
						if (extNetPrice < -999999.99 || extNetPrice > 999999999.99)
						{
							error_code = 7;
							tLogdto.setErrorMessage("Invlid Net price");
						}						
						// 24: IDW-EXTENDED-PROFIT PIC X(12).
						// 25: IDW-UNIT-COST PIC X(10).
						// 26: IDW-MVMT-TYPE PIC X(02).
						// 27: IDW-DEFAULT-COST-USED PIC X(01).
						// 28: IDW-PERCENT-USED PIC X(7).
						// 29: IDW-UNIT-COST-GROSSPIC X(11).
						
						// 30: IDW-EXTENDED-GROSS-PRICE PIC X(10) 
						double extGrossPrice = Double.parseDouble(line[30].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						tLogdto.setExtendedGrossPrice(PrestoUtil.round(extGrossPrice, 2));
						
						if (extGrossPrice < -999999.99 || extGrossPrice > 999999999.99)
						{
							error_code = 8;
							tLogdto.setErrorMessage("Invlid Gross price");
						}											
						
						// 31: IDW-COUNT-ON-DEAL PIC X(06) 
						// 32: IDW-MISCL-FUND-AMT PIC X(10)
						String otherAmount = line[32].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY);
						tLogdto.setOtherDiscountAmt(!Constants.EMPTY.equals(otherAmount) &&(otherAmount!= null)?PrestoUtil.round(Double.parseDouble(otherAmount),2):0);
						// 33: IDW-MISCL-FUND-CNT PIC X(4).
						
						// 34: IDW-STORE-CPN-USED PIC X(01).
						String strore_cpn_used = line[34];
						tLogdto.setStoreCouponUsed(strore_cpn_used);
						
						// 35: IDW-MFR-CPN-USED PIC X(01).
						String mfr_cpn_used = line[35];
						tLogdto.setMfrCouponUsed(mfr_cpn_used);
						
						//printZeroPriceRecord(line, dto);
						//set Previous tran Id to current Tran Id
						previousTranId = currentTranId;
						
					} catch (Exception e) {
						logger.error("readAndLoadMovements() - Error occurred in data " + e);
						e.printStackTrace();
						tLogdto.setErrorMessage("Unhandled exception");
						error_code = 99;
					}
	            	
					tLogdto.setProcessRow(total_count);

	            	if (error_code == 0)
	            		transList.add(tLogdto);
	            	else {
						countFailed++;
	            		exceptionList.add(tLogdto);
	            	}
	            	
					tLogdto = null;

					// Save transactions
            		if ( transList.size() % updateDBAfter == 0)	            		
	            	{
	            		logger.debug("readAndLoadMovements() - Update into DB");
	            		//call DB update
	            		trackDAO.UpdateTLogStatus(_Conn, trackId, processDateTime, "InProgress", total_count, countFailed);
	            		_Conn.commit();
	            		movementDao.insertTransactionLog(_Conn, transList, targetTable, trackId);
	            		saved_count = saved_count + transList.size();
	            		// Update in-progress status
	            		//Reset Array list
	            		transList.clear();
	            	}
	            	
            		// Log status
                    if ( total_count % 25000 == 0 ) {
                    	logger.info("readAndLoadMovements() - Record Counts: Processed=" + String.valueOf(total_count) + 
                    			", Saved=" + String.valueOf(saved_count) +
                    			", Error=" + String.valueOf(countFailed) +
                    			", Skipped=" + String.valueOf(countSkipped) );
                    }
                    if ( _StopAfter > 0 && total_count >= _StopAfter ) {
                    	break;
                    }
	            }
	        } // end while
	        
	        
	        // Save pending transactions
        	if ( transList.size() > 0)
        	{
        		MovementDAO objMovement = new MovementDAO();
        		objMovement.insertTransactionLog(_Conn, transList, targetTable, trackId);
        		saved_count = saved_count + transList.size();
        	}

			// Log skipped stores numbers
			if ( countSkipped > 0 )
			{
				Iterator<String> it = _UnprocessedStores.iterator();
				int ii = 0;
				StringBuffer sb = new StringBuffer("readAndLoadMovements() - Stores skipped: ");
				while (it.hasNext()) {
				    String store = it.next();
					if ( ii > 0 ) sb.append(',');
					sb.append(store);
					ii++;
				}
				logger.info(sb.toString());
			}

			// Save error records
        	if ( exceptionList.size() > 0)
        	{
        		MovementDAO objMovement = new MovementDAO();
        		
        		logger.info("readAndLoadMovements() - Saving error records...  Count=" + exceptionList.size() );
        		objMovement.insertErrorTLog(_Conn, exceptionList, targetTable, trackId);
        	}
        							
			ret = countFailed < total_count;
        }
        catch (Exception ex) {
        	logger.error("readAndLoadMovements() - Exception=", ex);
        	ret = false;
        	processStat = -1;
        }
        catch (GeneralException ex) {
        	logger.error("readAndLoadMovements() - GeneralException=", ex);
        	ret = false;
        	processStat = -1;
        }
        finally {

        	logger.info("readAndLoadMovements() - Record Counts: Processed=" + String.valueOf(total_count) + 
        			", Saved=" + String.valueOf(saved_count) +
        			", Error=" + String.valueOf(countFailed) +
        			", Skipped=" + String.valueOf(countSkipped) );
        	logger.info("readAndLoadMovements() - Number of new items inserted into Item master : "
					+ String.valueOf(newItemCount));
        	processDateTime = dateFormat.format(new Date());
        	
        	if (processStat == -1)
        	{
            	trackDAO.UpdateTLogStatus(_Conn, trackId, processDateTime, "Failed", total_count, countFailed);
            	_Conn.commit();
        	}
        	else        		
        	{
        		trackDAO.UpdateTLogStatus(_Conn, trackId, processDateTime, "Completed", total_count, countFailed);
        		_Conn.commit();
        	}
        	
        	if( reader != null){
        		reader.close();
        	}
        }
     
        return ret;
	}
	
	
	/* 
	 * Method to support multiple calendar ids. Currently, this method is 
	 * not used in MovementDataLoad and used in MovementDataLoad2  
	 */
	public int getRetailCalendarID(Connection _Conn, Date startDate, String calKey, String calendarMode) throws GeneralException{
		int calId = -1;
		
		if (_calendarMap.containsKey(calKey)) {
			calId = Integer.parseInt(_calendarMap.get(calKey));
		}
		else{
			//Create object for RetailCalendarDAO
			RetailCalendarDAO objCal = new RetailCalendarDAO();
		
			//create object for RetailCalendarDTO and get the calendar id
			List<RetailCalendarDTO> dateList = objCal.dayCalendarList(_Conn, startDate, startDate, calendarMode);
			 if (dateList.size() > 0) {
				 RetailCalendarDTO calDto = dateList.get(0);
				 calId = calDto.getCalendarId();
				 
				 //Update the map the calendar id for this date is first
				 _calendarMap.put(calKey,  String.valueOf(calId));
				 
				 logger.info("getRetailCalendarID() - Calendar Id for date " + startDate.toString() +" is " + calId );				 
				 if (_calendarMap.size() > 1)
				 {
					 logger.warn("getRetailCalendarID() - Transaction received for MULTIPLE DATES!  Date Count=" + _calendarMap.size() );					 
				 }
			 }
		}
			 
		//Return the calendar id to calling method
		return calId;
	}


	/* 
	 * Method to supres second calendar id. 
	 * Currently, this method is not used in MovementDataLoad  
	 */
	public int getRetailCalendarIDV2(Connection _Conn, Date startDate, String calKey, String calendarMode) throws GeneralException{
		int calId = -1;
		
		if (_calendarMap.containsKey(calKey)) { //If the date already exist in the collection
			calId = _processCalendarId;
		}
		else { //If the date not exist in the collection
			if (_processCalendarId > 0) //If calendar id exist
			{
				_calendarMap.put(calKey, "S");
				calId = _processCalendarId;
				logger.warn("Input file has data for differnt date.." + startDate.toString());
			}
			else //If calendar id not exist
			{
				//Create object for RetailCalendarDAO
				RetailCalendarDAO objCal = new RetailCalendarDAO();
		
				//create object for RetailCalendarDTO and get the calendar id
				List<RetailCalendarDTO> dateList = objCal.dayCalendarList(_Conn, startDate, startDate, calendarMode);
				if (dateList.size() > 0) {
					RetailCalendarDTO calDto = dateList.get(0);
					_processCalendarId = calDto.getCalendarId();
					calId = _processCalendarId;
					_calendarMap.put(calKey, "F");
					logger.info("Processing Date is....................." + startDate.toString());
					logger.info("Calendar Id for processing Date is....." + calId);
				}
				else {
					logger.error("Calendar Id not found for Date........" + startDate.toString());
				}
			}
		}
			 
		//Return the calendar id to calling method
		return calId;
	}

	
	
	/**returns item code from ITEM_LOOKUP table if the given UPC is available. 
	 * Otherwise it will insert a record for new item in ITEM_LOOKUP as well as in PRODUCT_GROUP_RELATION also.
	 * @param strUpc String
	 * @throws GeneralException*/
	public int getItemCode(String strUpc) {
		int itemCode = -1;
		try {
			
			String itemUpc = PrestoUtil.castUPC(strUpc, false);
			
			if (this._itemHashMap.containsKey(itemUpc)) {
				itemCode = ((Integer) this._itemHashMap.get(itemUpc))
						.intValue();
			} else {
				ItemDAO objItemDao = new ItemDAO();
				itemCode = objItemDao.getItemCodeForUPC(this._Conn, itemUpc);
				// Calculating check digit to fill STANDARD_UPC column in
				// ITEM_LOOKUP...
				
				//Check digit calculation ends.
				if(itemCode < 0){
					//check with Standard UPC also in ITEM_LOOKUP to get item code. 
					itemCode = objItemDao.getItemCodeForUPC(this._Conn, getStdUpc(itemUpc));
				}
				if (itemCode > 0) {
					this._itemHashMap.put(itemUpc, Integer.valueOf(itemCode));
				} else {
					ItemDTO item = new ItemDTO();
					setUpItemToProductMaster(item, getStdUpc(itemUpc), itemUpc);
					return item.itemCode;
				}
			}
		} catch (GeneralException e) {
			logger.error("Error while getting item code " + e.toString());
		}
		catch (Exception e) {
			logger.error("Error while getting item code " + e.toString());
		}
		return itemCode;
	}
	
	
	private String getStdUpc(String itemUpc){
		String stdUpc = "";
		if (itemUpc.length() == 12 && itemUpc.charAt(0) == '0') {
			stdUpc = itemUpc.substring(1);
			ShopRiteDataProcessor dataProcessor = new ShopRiteDataProcessor();
			int checkDigit = dataProcessor
					.findUPCCheckDigit(stdUpc);
			stdUpc = stdUpc
					+ Integer.toString(checkDigit);
		}
		else{
			stdUpc = itemUpc;
		}
		return stdUpc;
	}
	
	private void setUpItemToProductMaster(ItemDTO item, String stdUpc, String itemUpc) throws GeneralException, Exception{
		ItemDAO objItemDao = new ItemDAO();
		item.upc = itemUpc;
		item.standardUPC = stdUpc;
		item.itemName = ("Unclassified " + itemUpc);
		//item.retailerItemCode = "X-" + itemUpc.substring(6);
		item.segmentID = this._unclassifiedSegment;
		item.subCatID = this._unclassifiedSubCat;
		item.catID = this._unclassifiedCat;
		item.deptID = this._unclassifiedDept;
		//inserts new item into item master. 
		boolean isItemInserted = objItemDao.insertItem(this._Conn,
				item, false, false);
		//inserts the item into item level of product group hierarchy.
		if (isItemInserted) {
			newItemCount++;
			com.pristine.dto.salesanalysis.ProductDTO product = new com.pristine.dto.salesanalysis.ProductDTO();
			product.setChildProductId(item.itemCode);
			product.setChildProductLevelId(Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID);
			if(this._unclassifiedSegmentProductId > 0){
				product.setProductLevelId(Constants.SEGMENT_LEVEL_PRODUCT_LEVEL_ID);
				product.setProductId(this._unclassifiedSegmentProductId);
			}
			else{
				product.setProductLevelId(Constants.SUBCATEGORYLEVELID);
				product.setProductId(this._unclassifiedSubcatProductId);
			}
			ProductGroupDAO productDao = new ProductGroupDAO();
			productDao.insertProductGroupRelation(_Conn, product);
		} else {
			logger.warn("getItemCode() - The item with UPC '" + item.upc
					+ "' was not inserted into ITEM_LOOKUP!");
		}
	}
	
	private void getItemCodeList()
	{
		ItemDAO objItem = new ItemDAO();
		
		try {
			this._itemHashMap = objItem.getUPCAndItem(_Conn);
		} catch (GeneralException e) {
			logger.error(e.getCause());
		}
		
	}
	
	
	private void getAuthorizedItems(){
		ItemDAO objItem = new ItemDAO();
		try {
			logger.info("Preload Item code/UPC authorized at store level");
			_itemHashMapStoreUpc = objItem.getAuthorizedItems(_Conn);
			logger.info("Total Item code/UPC authorized at store level: " + _itemHashMapStoreUpc.size());
		
		} catch (GeneralException e) {
			logger.error(e.getCause());
		}
	}
	
	
	private void getCustomerList()
	{
		
		try {
			CustomerDAO custdao = new CustomerDAO();	
			_custMap = custdao.getAllCustomerCards(_Conn, null);
		} catch (GeneralException e) {
			logger.error(e.getCause());
		}
		
	}
	
	private static void logCommand (String[] args)
	{
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: MovementDataLoad ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	}
	
	private boolean _UpdateTransactions = false;

	private static String ARG_UPDATE_TRANSACTIONS = "UPDATE_TRANSACTIONS";
	
	public HashMap<String, Integer> getupcMissingMap() {
		return _upcMissinghMap;
	}
	
	private int getCustomerId(String customerCardNo, 
			int storeId) throws GeneralException, Exception{

		int customerId=0;

		 if (customerCardNo.trim().length() > 0){
			 if(_custMap.containsKey(customerCardNo)) {
				 customerId =  _custMap.get(customerCardNo);
			 }
			 else {
				 CustomerDAO custdao = new CustomerDAO();	
				 customerId = custdao.getCustomerId(_Conn, customerCardNo, storeId);
				 _custMap.put(customerCardNo, customerId);
			 }
		 }		
		return customerId;
	}
	
	private int getStoreId(String storeNo) throws GeneralException, Exception{

		int storeId = 0;
		
		if (_storeMap.containsKey(storeNo))
			storeId = _storeMap.get(storeNo);
		
		return storeId;
	}
	
	
	
	/**
	 * Returns item code authorized for store and input upc
	 * @param strUpc
	 * @param posDept
	 * @param storeId
	 * @return
	 * @throws GeneralException
	 */
	public int getItemCode(String strUpc, Integer storeId) throws GeneralException
	{
		int itemCode=-1;
    	String itemUpc = PrestoUtil.castUPC(strUpc, false);
    	if (_itemHashMapStoreUpc.containsKey(storeId)) {
    		HashMap<String, Integer> tMap = _itemHashMapStoreUpc.get(storeId);
    		if(tMap.containsKey(itemUpc)){
    			itemCode = tMap.get(itemUpc);
    		}else if(_itemHashMap.containsKey(itemUpc)){
        		// Code should reach this point only if items are preloaded
        		itemCode = _itemHashMap.get(itemUpc);
        	}else{
        		ItemDAO objItemDao = new ItemDAO();
    			itemCode = objItemDao.getItemCodeForStoreUPC(_Conn, itemUpc, storeId);
    			if (itemCode > 0){
    				tMap.put(itemUpc, itemCode);
    				_itemHashMapStoreUpc.put(storeId, tMap);
    			}
    			else{
    			itemCode = 	getItemCodeFromDB(itemUpc, objItemDao, storeId);
    			}
    		}
    	}else if(_itemHashMap.containsKey(itemUpc)){
    		// Code should reach this point only if items are preloaded
    		itemCode = _itemHashMap.get(itemUpc);
    	}else{
    		ItemDAO objItemDao = new ItemDAO();
    		itemCode = getItemCodeFromDB(itemUpc, objItemDao, storeId);
    	}
    	return itemCode;
    }
	
	
	
	
	private int getItemCodeFromDB(String itemUpc, ItemDAO objItemDao, int storeId){
		int itemCode = -1;
		try {
		itemCode = objItemDao.getItemCodeForUPC(this._Conn, itemUpc);
		// Calculating check digit to fill STANDARD_UPC column in
		// ITEM_LOOKUP...
		
		//Check digit calculation ends.
		if(itemCode < 0){
			//check with Standard UPC also in ITEM_LOOKUP to get item code. 
			itemCode = objItemDao.getItemCodeForUPC(this._Conn, getStdUpc(itemUpc));
		}
		if (itemCode > 0) {
			this._itemHashMap.put(itemUpc, Integer.valueOf(itemCode));
			HashMap<String, Integer> tMap = null;
			if(_itemHashMapStoreUpc.get(storeId) != null)
				tMap = _itemHashMapStoreUpc.get(storeId);
			else
				tMap = new HashMap<String, Integer>();
			tMap.put(itemUpc, itemCode);
			_itemHashMapStoreUpc.put(storeId, tMap);
		}
		
		else{ 
				ItemDTO item = new ItemDTO();
				setUpItemToProductMaster(item, getStdUpc(itemUpc), itemUpc);
				itemCode = item.itemCode;
		}
		} catch (GeneralException e) {
			logger.error("Error while getting item code for UPC - " + itemUpc + " item code -  " + itemCode + " " + e.toString());
			e.printStackTrace();
		}
		catch (Exception e) {
			logger.error("Error while getting item code for UPC - " + itemUpc, e);
			e.printStackTrace();
		}
		return itemCode;
	}
	
	
	
/*
	private void printZeroPriceRecord (String[] line, MovementDTO dto)
	{
		if ( dto.getItemNetPrice() == 0 && dto.getItemGrossPrice() == 0
			 && (dto.getExtnQty() > 0 || dto.getExtnWeight() > 0) )
		{
			long upcInt = Long.parseLong(dto.getItemUPC());
			if ( !dto.getItemUPC().contains("1114600002") && upcInt > 99999 )
			{
				String msg = "";
				for ( int ii = 0; ii < line.length; ii++ )
				{
					if ( ii > 0 ) msg = msg + "|";
					msg = msg + line[ii];
				}
				logger.info(msg);
			}
		}
	}
*/
	
}
