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
import com.pristine.dto.MovementDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

/**
 *
 * @author sakthidasan
 */
public class MovementDataLoad extends PristineFileParser
{
	static Logger logger = Logger.getLogger("MovementDataLoad");
  	Connection _Conn = null;
	private int _StopAfter = -1;
	private List<String> _StoreList = new ArrayList<String>();
	private Set<String> _UnprocessedStores = new HashSet<String>();
	private HashMap<String, Integer> _itemHashMap = new HashMap<String, Integer>();
	private HashMap<String, Integer> _upcMissinghMap = new HashMap<String, Integer>(); 
	private HashMap<String, String> _calendarMap = new HashMap<String, String>(); 
	private int _processCalendarId = -1;
	private int _calIdType = 1;		//Populate Calendar Id type
	private int unclassifiedItemCode = 0;
	private int unclassifiedHBCItemCode = 0;
	private int updateDBAfter = 5000;
	private int writelogAfter = 1000000;

	private String preloadItem = "NO";

	private static boolean ignoreTruncStoreNum = false;
	// Changes to retrieve item_code using upc and retailer_item_code
	boolean checkRetailerItemCode = false;
	private HashMap<Integer, HashMap<String, Integer>> _itemHashMapStoreUpc = new HashMap<Integer, HashMap<String, Integer>>();
	private HashMap<String, Integer> compIdMap = new HashMap<String, Integer>();
	
	public MovementDataLoad (String StoreFromConfig, boolean preloadCache)
	{
		super ("analysis.properties");
		try {
			//Create DB connection
			_Conn = DBManager.getConnection();

			
			//Load the target store from DB or Configuration
			if (StoreFromConfig.equalsIgnoreCase("Y")){
				logger.debug("Load Processing Stores from configuration");
				String store = PropertyManager.getProperty("MOVEMENTLOAD_STORES");
				String[] storeArr = store.split(",");
				for (int i = 0; i < storeArr.length; i++) {
					_StoreList.add(storeArr[i]);
					logger.debug("Store " + storeArr[i]);
				}
			}
			else {
				logger.debug("Load Processing Stores from Database");
				_StoreList = new CompStoreDAO().getKeyStoreNumberList(_Conn);
			}
			
			//Get Unclassified ItemCode
			unclassifiedItemCode = Integer.parseInt(PropertyManager.getProperty("UNCLASSIFIED_OTHERS_ITEM_CODE", "0"));
			unclassifiedHBCItemCode = Integer.parseInt(PropertyManager.getProperty("UNCLASSIFIED_HBC_ITEM_CODE", "0"));

			// Changes to retrieve item_code using upc and retailer_item_code
			compIdMap = new StoreDAO().getStoreIdMap(_Conn);
			checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
			if(preloadCache){
				getItemCodeList();
				getAuthorizedItems();
			}
		}
		catch (GeneralException ex) {
			logger.error("Error while initializing - " + ex.toString());
		}
		
		//Get item commit count from configuration
		try{
			updateDBAfter = Integer.parseInt(PropertyManager.getProperty("MOVEMENT_COMMIT_COUNT"));
		}
		catch (Exception ex) {
		}

		//Get config for pre load item data
		try{
			preloadItem = PropertyManager.getProperty("MOVEMENT_PRELOAD_ITEM").toUpperCase();
		}
		catch (Exception ex) {
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
		logCommand (args);
		
    	String subFolder = null;
    	boolean updateTransactions = false;
    	int calendarType = 0;
    	Date processDate = null;
    	DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		String storeFromConfig = "N";
		String targetTableType = null;
		String targetTable = "MOVEMENT_DAILY";
		
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
					logger.error("Input Parameter : UPDATEMODE : " + exe);
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
						logger.error("Process Date Parsing Error, check Input");
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
			
			if(arg.startsWith("IGNORE_TRUNC_STORE_NUM")){
				ignoreTruncStoreNum = Boolean.parseBoolean(
						arg.substring("IGNORE_TRUNC_STORE_NUM=".length()));
			}
		}

		logger.info("Target Table " + targetTable);
		
		MovementDataLoad dataload = new MovementDataLoad(storeFromConfig, false);
		dataload._UpdateTransactions = updateTransactions;
		dataload._calIdType = calendarType;
		dataload.process(subFolder, processDate, targetTable);
    }
	
	public void processRecords(List listobj) throws GeneralException
	{
	}
	
	private void process (String subFolder, Date processDate, String targetTable)
	{
		PropertyConfigurator.configure("log4j-movement-daily.properties");
		
		// Update_Transactions
		String msg = "Movement Data Load started" + (_UpdateTransactions == true ? ": updating Transactions" : "");
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
			
			//getzip files
			ArrayList<String> zipFileList = getZipFiles (subFolder);
			
			//Start with -1 so that if any reqular files are present, they are processed first
			int nFilesProcessed = 0;
			int curZipFileCount = -1;
			boolean processZipFile = false;
			String zipFilePath = getRootPath() + "/" + subFolder;
			do
			{
				ArrayList<String> files = null;
				boolean commit = true;
			
				try
				{
					if( processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;

					files = getFiles (subFolder);
					if ( files != null && files.size() > 0 )
					{
						//Get the UPC and its item code
						if (preloadItem.equals("YES")){
							getItemCodeList();	
							if(checkRetailerItemCode){
								getAuthorizedItems();
							}
						}
						
						for ( int ii = 0; ii < files.size(); ii++ ) {
							String file = files.get(ii);
							nFilesProcessed++;
							commit = readAndLoadMovements(file, '|', targetTable);
							
							if (commit){
								logger.info("Commit Transaction");
								_Conn.commit();
							}
							else{
								logger.info("Rollback Transaction");
								_Conn.rollback();
							}
						}
					}
				} catch (GeneralException ex) {
			        logger.error("Inner Exception - GeneralException", ex);
					logger.info("Rollback Transaction");
					_Conn.rollback();
			        commit = false;
			    } catch (Exception ex) {
					logger.info("Rollback Transaction");
			        logger.error("Inner Exception - JavaException", ex);
					_Conn.rollback();
			        commit = false;
			    }
		    
			    if( processZipFile){
			    	PrestoUtil.deleteFiles(files);
			    	files.clear();
			    	files.add(zipFileList.get(curZipFileCount));
			    }
				if( commit ){
					PrestoUtil.moveFiles(files, zipFilePath + "/" + Constants.COMPLETED_FOLDER);
				}
				else{
					PrestoUtil.moveFiles(files, zipFilePath + "/" + Constants.BAD_FOLDER);
				}
				
				curZipFileCount++;
				processZipFile = true;
				
        	} while (curZipFileCount < zipFileList.size());

			if ( nFilesProcessed > 0 )
				logger.info("Movement Data Load successfully completed");
			else 
				logger.info("No Movement Data file found");

        	PristineDBUtil.close(_Conn);
		}
        catch (Exception ex) {
            logger.error("Error", ex);
        }
        catch (GeneralException ex) {
            logger.error("Error", ex);
        }
	}
	
	private boolean readAndLoadMovements (String fileName, char delimiter, String targetTable) throws Exception, GeneralException {
		logger.info("Processing file: " + fileName);
		ArrayList<MovementDTO> listMoment = new ArrayList<MovementDTO>();
		boolean ret;
		
        CsvReader reader = readFile(fileName, delimiter);
        String line[];
        
        MovementDAO movementDao = new MovementDAO();
        int count = 0, countFailed = 0, countSkipped = 0, itemCodeMissCount = 0;
        try
        {
	        while (reader.readRecord())
	        {
	            line = reader.getValues();
	            count++;
	            //logger.debug("Processing record count....." + count);
	            if ( line.length > 0 )
	            {
	            	try {
						// 0: IDW-ITEM-COMPANY PIC 9(05).
						
						// 1: IDW-ITEM-STORE PIC 9(06).
						String store = line[1];
						//if ignore truncating is enabled, don't keep last 4 digits only, Get the full store #
						if(!ignoreTruncStoreNum)
							store = store.substring(store.length() - 4);	// keep only last 4 characters
						// Skip store if not listed in the list
						// Process all stores if list is empty
						boolean processStore = true;
						if ( _StoreList.size() > 0 ) {
							processStore = _StoreList.contains(store); 
						}

						if ( !processStore ) {
							countSkipped++;
							_UnprocessedStores.add(store);
							continue;
						}
						
						MovementDTO dto = new MovementDTO();
						dto.setItemStore(store);
						
						// 2: IDW-ITEM-TERMINAL PIC 9(04).
						int termNo = Integer.parseInt(line[2]);
						// 3: IDW-ITEM-TRANSNUM PIC 9(04).
						int tranNo = Integer.parseInt(line[3]);
						/*
						if ( tranNo > 9990 ) {
							logger.info("Transaction no = " + tranNo + " at terminal " + termNo + " of store " + store);
						}
						*/
						dto.setTransactionNo(termNo * 10000 + tranNo);
						
						// 4: IDW-ITEM-OPERATOR PIC 9(10).
						
						// 5: IDW-ITEM-CCYYMMDDHHMM PIC 9(12).
						String timestamp = line[5];
						int year = Integer.parseInt(timestamp.substring(0,4));
						int month = Integer.parseInt(timestamp.substring(4,6)) - 1;
						int date = Integer.parseInt(timestamp.substring(6,8));
						int hour = Integer.parseInt(timestamp.substring(8,10));
						int minute = Integer.parseInt(timestamp.substring(10, 12));
						java.util.Calendar cal = java.util.Calendar.getInstance();
						cal.set(year, month, date, hour, minute);
						dto.setItemDateTime(cal.getTime());
						dto.setDayOfWeek(cal.get(Calendar.DAY_OF_WEEK));
						
						String calKey = date + "_" + month + "_" + year;
						
						int calendarId = 0;
						if ((_calIdType == 1) || (_calIdType == 0)){
							calendarId = getRetailCalendarIDV2(_Conn, dto.getItemDateTime(),  calKey, Constants.CALENDAR_DAY);
						}
						else{
							calendarId = getRetailCalendarID(_Conn, dto.getItemDateTime(),  calKey, Constants.CALENDAR_DAY);
						}
						
						if (calendarId > 0) {
							dto.setCalendarId(calendarId);	   
						}
						else {
							logger.warn("Calendar Id not found for date" + cal.getTime() + ", Please check with Retail Calendar Master data");
							countSkipped++;
							continue;
						}
						
						// 6: IDW-ITEM-CUSTOMER-ID PIC X(24).
						String customer = line[6] != null ? line[6] : "";
						dto.setCustomerId(customer);
						
						// 7: IDW-ITEM-UPC PIC 9(14).
						String UPC = line[7];
						dto.setItemUPC(UPC);
						
						// 8: IDW-ITEM-NET-PRICE PIC X(12).
						double d = Double.parseDouble(line[8].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						if(d < -999999999.99 || d > 999999999.99){
							countSkipped++;
							logger.warn("Record with invalid net price found " + d);
							continue;
						}else
							dto.setItemNetPrice(d);
						
						// 9: IDW-ITEM-GROSS-PRICE PIC X(10).
						d = Double.parseDouble(line[9].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						dto.setItemGrossPrice(d);
														
						// 10: IDW-ITEM-POS-DEPT PIC 9(04).
						
						// for pos department
						int posDep = Integer.parseInt(line[10].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						dto.setPosDepartment(posDep);
						
						// 11: IDW-ITEM-COUP-FAMILY-CURR PIC 9(03).
						// 12: IDW-ITEM-COUP-FAMILY-PREV PIC 9(03).
						// 13: IDW-EXTN-MULT-PRICE-GRP PIC 9(02).
						// 14: IDW-EXTN-DEAL-QTY PIC 9(08).
						// 15: IDW-EXTN-PRICE-MTHD PIC 9(02).
						// 16: IDW-EXTN-SALE-QTY PIC 9(02).
						// 17: IDW-EXTN-SALE-PRICE PIC 9(10)
						
						// 18: IDW-EXTN-QTY PIC X(10).
						d = Double.parseDouble(line[18].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						dto.setExtnQty(d);
						
						// 19: IDW-EXTN-WGT PIC X(12).
						d = Double.parseDouble(line[19].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						dto.setExtnWeight(d);
						
						// 20: IDW-WEIGHTED-FLAG PIC 9(01).
						// 21: IDW-WEIGHTED-CNT PIC X(07).
						// 22: IDW-COUPON-USED PIC 9(02).
						
						// 23: IDW-EXTENDED-NET-PRICE PIC X(12).
						d = Double.parseDouble(line[23].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						dto.setExtendedNetPrice(d);
						
						// 24: IDW-EXTENDED-PROFIT PIC X(12).
						// 25: IDW-UNIT-COST PIC X(10).
						// 26: IDW-MVMT-TYPE PIC X(02).
						// 27: IDW-DEFAULT-COST-USED PIC X(01).
						// 28: IDW-PERCENT-USED PIC X(7).
						// 29: IDW-UNIT-COST-GROSSPIC X(11).
						
						// 30: IDW-EXTENDED-GROSS-PRICE PIC X(10) 
						d = Double.parseDouble(line[30].replaceAll(Constants.PLUS_CHARACTER, Constants.EMPTY));
						dto.setExtendedGrossPrice(d);
						
						// 31: IDW-COUNT-ON-DEAL PIC X(06) 
						// 32: IDW-MISCL-FUND-AMT PIC X(10) 
						// 33: IDW-MISCL-FUND-CNT PIC X(4).
						
						// 34: IDW-STORE-CPN-USED PIC X(01).
						String strore_cpn_used = line[34];
						dto.setStoreCpnUsed(strore_cpn_used);
						
						// 35: IDW-MFR-CPN-USED PIC X(01).
						String mfr_cpn_used = line[35];
						dto.setMfrCpnUsed(mfr_cpn_used);
						
						//printZeroPriceRecord(line, dto);
						
						//Get the itemcode and update into DTO - Added by Britto
						int itemCode = -1;
						if(!checkRetailerItemCode){
							itemCode = getItemCode(UPC, posDep);
						}
						// Changes to retrieve item_code using upc and retailer_item_code
						else{
							int compStrId = 0;
							
							if (compIdMap != null && compIdMap.size() > 0 && compIdMap.containsKey(store))
								compStrId = compIdMap.get(store);
							
							if(compStrId > 0){
								itemCode = getItemCode(UPC, posDep, compStrId);
								
								if (itemCode == -1)
									itemCode = getItemCode(UPC, posDep);
							}
							else{
								countSkipped++;
								_UnprocessedStores.add(store);
								continue;
							}
						}
						// Changes to retrieve item_code using upc and retailer_item_code - Ends
						
						if (itemCode > 0) {
							dto.setItemCode(itemCode);

							if (itemCode == unclassifiedItemCode || 
									itemCode == unclassifiedHBCItemCode)
								itemCodeMissCount++;
						}
						
						listMoment.add (dto);
						dto = null;
						
					} catch (Exception e) {
						logger.error("Row: " + count+ ", Error occured in row data " + e);
						countFailed++;
					}
	            	
//                	try {
//                		if ( _UpdateTransactions )
//                		{
//                			boolean updated = movementDao.updateDailyMovementTransactions (getOracleConnection(), dto, false);
//                			if ( !updated )
//                				countFailed++;
//                		}
//                		else {
//                			boolean inserted = movementDao.insertDailyMovement (getOracleConnection(), dto, true);
//                			if ( !inserted ) {
//                        		String msg = String.format ("Error line %d: store=%s, upc=%s price=%s time=%s",
//    									count, dto.getItemStore(), dto.getItemUPC(),
//    									Double.toString(dto.getItemNetPrice()), dto.getItemDateTime());
//                				logger.error(msg);
//                				countFailed++;
//                			}
//                		}
//                	}
//                	catch (GeneralException gex)
//                	{
//                		String msg = String.format ("Error line %d: store=%s, upc=%s price=%s time=%s",
//                									count, dto.getItemStore(), dto.getItemUPC(),
//                									Double.toString(dto.getItemNetPrice()), dto.getItemDateTime());
//        				logger.error(msg, gex);
//        				countFailed++;
//                	}

                    if ( count % writelogAfter == 0 ) {
        				logger.info("Processed " + String.valueOf(count) + " records");
                    }
	            	
	            	if ( listMoment.size() % updateDBAfter == 0)
	            	{
	            		logger.debug("Update into DB");
	            		//call DB update
	            		movementDao .insertMovementDaily(_Conn, listMoment, targetTable);
	            		//Reset Array list
	            		listMoment.clear();
	            		
	            	}
	            	
                    if ( _StopAfter > 0 && count >= _StopAfter ) {
                    	break;
                    }
	            }
	        }
        	if ( listMoment.size() > 0)
        	{
        		//logger.debug("Update into DB");
        		//call DB update
        		MovementDAO objMovement = new MovementDAO();
        		objMovement.insertMovementDaily(_Conn, listMoment, targetTable);
        	}
        	
			logger.info("Processed " + String.valueOf(count) + " records.");
			logger.info("Item code not exist for " + itemCodeMissCount + " records");
			logger.info("Error in " + String.valueOf(countFailed) + " records.");
			logger.info("UPC not found for " + _upcMissinghMap.size());
			String tmp = "Skipped " + String.valueOf(countSkipped) + " records.";
			logger.info(tmp);
			
			//Write skipped stores numbers
			if ( countSkipped > 0 )
			{
				Iterator<String> it = _UnprocessedStores.iterator();
				int ii = 0;
				StringBuffer sb = new StringBuffer("Stores skipped: ");
				while (it.hasNext()) {
				    String store = it.next();
					if ( ii > 0 ) sb.append(',');
					sb.append(store);
					ii++;
				}
				logger.info(sb.toString());
			}
			
			
			//Write skipped UPCs
			if ( _upcMissinghMap.size() > 0 )
			{
				Iterator<String> it = _upcMissinghMap.keySet().iterator();
				int ii = 0;
				StringBuffer sb = new StringBuffer("UPCs skipped: ");
				while (it.hasNext()) {
				    String store = it.next();
					if ( ii > 0 ) sb.append(',');
					sb.append(store);
					ii++;
				}
				logger.info(sb.toString());
			}
			
			ret = countFailed < count;
        }
        catch (Exception ex) {
        	logger.fatal("Fatal error", ex);
        	ret = false;
        	throw ex;
        }
        finally {
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
				logger.info("Calendar Id for date " + startDate.toString() +" is " + calId );
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

	
	
	public int getItemCode(String strUpc, int posDept) throws GeneralException
	{
    	int itemCode=-1;
    	String itemUpc = PrestoUtil.castUPC(strUpc, false);
    	//logger.debug("UPC Original...." + strUpc);
    	
    	if (_itemHashMap.containsKey(itemUpc)) {
        	//Use the existing item code
    		itemCode = _itemHashMap.get(itemUpc);
			//logger.debug("Item Code From Collection..." + itemCode);
    	}
    	else{
    		//Get Item code from DB
			ItemDAO objItemDao = new ItemDAO();
			itemCode = objItemDao.getItemCodeForUPC(_Conn, itemUpc);
			
			//If exist
			if (itemCode > 0)
				_itemHashMap.put(itemUpc, itemCode);
			else //If not exist
    		{
				if (!_upcMissinghMap.containsKey(itemUpc))
					_upcMissinghMap.put(itemUpc, itemCode);
				
				//Use Unclassified Item Code
				if (posDept == 9 || posDept == 22 || posDept == 23) //If POS for HBC
					itemCode = unclassifiedHBCItemCode;
				else if (posDept > 0)
					itemCode = unclassifiedItemCode;
				else
					itemCode = 0;
    		}
		}
		return itemCode;
	}
	
	/**
	 * Returns item code authorized for store and input upc
	 * @param strUpc
	 * @param posDept
	 * @param storeId
	 * @return
	 * @throws GeneralException
	 */
	public int getItemCode(String strUpc, int posDept, Integer storeId) throws GeneralException
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
    		}
    	}else if(_itemHashMap.containsKey(itemUpc)){
    		// Code should reach this point only if items are preloaded
    		itemCode = _itemHashMap.get(itemUpc);
    	}else{
    		//Get Item code from DB
			ItemDAO objItemDao = new ItemDAO();
			itemCode = objItemDao.getItemCodeForStoreUPC(_Conn, itemUpc, storeId);
			
			//If exist
			if (itemCode > 0){
				HashMap<String, Integer> tMap = null;
				if(_itemHashMapStoreUpc.get(storeId) != null)
					tMap = _itemHashMapStoreUpc.get(storeId);
				else
					tMap = new HashMap<String, Integer>();
				tMap.put(itemUpc, itemCode);
				_itemHashMapStoreUpc.put(storeId, tMap);
			}else //If not exist
    		{
				if (!_upcMissinghMap.containsKey(itemUpc))
					_upcMissinghMap.put(itemUpc, itemCode);
				
				//Use Unclassified Item Code
				if(unclassifiedHBCItemCode > 0 && unclassifiedItemCode > 0){
					if (posDept == 9 || posDept == 22 || posDept == 23) //If POS for HBC
						itemCode = unclassifiedHBCItemCode;
					else if (posDept > 0)
						itemCode = unclassifiedItemCode;
					else
						itemCode = 0;
				}
    		}
		}
		return itemCode;
	}
	
	private void getItemCodeList()
	{
		ItemDAO objItem = new ItemDAO();
		
		try {
			logger.info("Preload Item code and UPC for all active items");
			_itemHashMap = objItem.getUPCAndItem(_Conn);
			logger.info("Total Preloaded Item count: " + _itemHashMap.size());
		
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
