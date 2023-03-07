/*
 * Title: Populate daily movement from TLog data input
 *
 **********************************************************************************
 * Modification History
 *---------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *---------------------------------------------------------------------------------
 * Version 0.1	04/30/2012	Janani			Initial Version
 **********************************************************************************
 */
package com.pristine.dataload.prestoload;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.MovementWeeklyDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dataload.MovementDataLoad;
import com.pristine.dto.MovementDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class MovementDataLoadV2 extends PristineFileParser{

	private static Logger logger = Logger.getLogger("MovementDataLoadV2");
	private int stopAfter = -1;
	Connection _Conn = null;
	private List<String> storeList = new ArrayList<String>();
	private Set<String> unprocessedStores = new HashSet<String>();
	
	public MovementDataLoadV2(String StoreFromConfig)
	{
		super ("analysis.properties");
		try {
			if (StoreFromConfig.equalsIgnoreCase("Y")){
				logger.debug("Load Processing Stores from configuration");
				String store = PropertyManager.getProperty("MOVEMENTLOAD_STORES");
				String[] storeArr = store.split(",");
				for (int i = 0; i < storeArr.length; i++) {
					storeList.add(storeArr[i]);
					logger.debug("Store " + storeArr[i]);
				}
			}
			else {
				logger.debug("Load Processing Stores from Database");
				storeList = new CompStoreDAO().getKeyStoreNumberList(getOracleConnection());
			}
			
			//Create DB connection
			_Conn = DBManager.getConnection();
		}
		catch (GeneralException ex) {
		}
	}
	
	/**
	 * Main method of the batch that populate daily movement from TLog input data.
	 * @param args[0]	Relative path of the input file
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-movement-daily-dataload.properties");
		String storeFromConfig = "N";
		for (int ii = 0; ii < args.length; ii++) {	
			String arg = args[ii];
			if (arg.startsWith("STORECONFIG")) {
				storeFromConfig = arg.substring("STORECONFIG=".length());
			}
		}
		MovementDataLoadV2 movementDataLoad = new MovementDataLoadV2(storeFromConfig);
		
		movementDataLoad.processInputData(args[0]);
	}
	
	/**
	 * This method reads and loads daily movement data from the files
	 * under specified path.
	 * @param relativePath	Relative Path from which files needs to be read
	 */
	private void processInputData(String relativePath) {
		logger.info("Daily Movement Dataload Starts");
		try
        {
			try {
				stopAfter = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
			}
			catch (Exception ex) {
				stopAfter = -1;
			}
			
			//getzip files
			ArrayList<String> zipFileList = getZipFiles (relativePath);
			
			//Start with -1 so that if any reqular files are present, they are processed first
			int nFilesProcessed = 0;
			int curZipFileCount = -1;
			boolean processZipFile = false;
			String zipFilePath = getRootPath() + "/" + relativePath;
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			MovementWeeklyDAO movementDAO = new MovementWeeklyDAO(getOracleConnection());
			do
			{
				ArrayList<String> files = null;
				boolean commit = true;
			
				try
				{
					if( processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;

					files = getFiles (relativePath);
					if ( files != null && files.size() > 0 )
					{
						for ( int ii = 0; ii < files.size(); ii++ ) {
							String file = files.get(ii);
							nFilesProcessed++;
							
							logger.info("Processing file " + file);
							long startTime = System.currentTimeMillis();
							
							// Parse the input file and populate movement dto
							long populateStartTime = System.currentTimeMillis();
							readAndLoadMovements(file, '|');
							long populateEndTime = System.currentTimeMillis();
							logger.info("Time taken for populating the list - " + (populateEndTime - populateStartTime) + "ms");
							
							long endTime = System.currentTimeMillis();
							
							logger.info("Time taken to process " + file + " - " + (endTime - startTime) + "ms");
							PristineDBUtil.commitTransaction(getOracleConnection(), "Commiting data for file " +  file);
						}
					}
				} catch (GeneralException ex) {
			        logger.error("Inner Exception - GeneralException", ex);
			        commit = false;
			    } catch (Exception ex) {
			        logger.error("Inner Exception - JavaException", ex);
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

        	PristineDBUtil.close(getOracleConnection());
		}
        catch (Exception ex) {
            logger.error("Error", ex);
        }
        catch (GeneralException ex) {
            logger.error("Error", ex);
        }
	}

	/**
	 * This method reads from the input file and populates a list of Movement DTOs
	 * Also populates a global set of upc to retrieve item code for the same.
	 * @param fileName		Input file name
	 * @param delimiter		Delimiter with which input files needs to be parsed
	 * @return	List of movements
	 * @throws GeneralException
	 */
	private void readAndLoadMovements(String fileName, char delimiter) throws GeneralException{
		logger.info("Inside readAndLoadMovements() of MovementDataLoadV2");
		logger.info("Processing file: " + fileName);
		ArrayList<MovementDTO> listMoment = new ArrayList<MovementDTO>();
		Set<String> itemCodeSet = new HashSet<String>();
		MovementDataLoad dataLoad = new MovementDataLoad("N", false);
		CsvReader reader = readFile(fileName, delimiter);
        String line[];
        MovementDAO movementDao = new MovementDAO();
        int count = 0, countFailed = 0, countSkipped = 0, itemCodeMissCount = 0;
        String inputDate = null;
        try
        {
	        while (reader.readRecord())
	        {
	            line = reader.getValues();
	            count++;
	            if ( line.length > 0 )
	            {
	            	// 0: IDW-ITEM-RECORD TYPE.
	            	// 1: IDW-ITEM-CHAIN ID.
	            	
	            	// 2: IDW-ITEM-STORE NUMBER
	            	String store = line[2];
	            	store = PrestoUtil.castStoreNumber(store);
	            	
	            	// Skip store if not listed in the list
	            	// Process all stores if list is empty
	            	boolean processStore = true;
	            	if ( storeList.size() > 0 ) {
	            		processStore = storeList.contains(store); 
	            	}

	            	if ( !processStore ) {
	            		countSkipped++;
	            		unprocessedStores.add(store);
	            		continue;
	            	}
	            	
	            	MovementDTO dto = new MovementDTO();
	            	dto.setItemStore(store);
	            	
	            	// 3: IDW-ITEM-TERMINAL NUMBER.
	            	int termNo = 0;
	            	if(!StringUtils.isEmpty(line[3]))
	            		termNo = Integer.parseInt(line[3]);
	            	
	            	// 4: IDW-ITEM-TRANSACTION TYPE.
	            	
	            	// 5: IDW-ITEM-TRANSACTION NUMBER.
	            	int tranNo = new Double(Double.parseDouble(line[5])).intValue();;
	
	            	dto.setTransactionNo(termNo * 10000 + tranNo);
	            	
	            	// 6: IDW-ITEM-DATETIME.
	            	String timestamp = line[6];
	            	int year = Integer.parseInt(timestamp.substring(0,4));
	            	int month = Integer.parseInt(timestamp.substring(4,6)) - 1;
	            	int date = Integer.parseInt(timestamp.substring(6,8));
	            	int hour = Integer.parseInt(timestamp.substring(8,10));
	            	int minute = Integer.parseInt(timestamp.substring(10));
	            	java.util.Calendar cal = java.util.Calendar.getInstance();
	            	cal.set(year, month, date, hour, minute);
	            	dto.setItemDateTime(cal.getTime());
	            	dto.setDayOfWeek(cal.get(Calendar.DAY_OF_WEEK));
	            	
//	            	if (calendarId > 0) {
//	            		dto.setCalendarId(calendarId);
//	            	}
//	            	else {
//	            		calendarId = dataLoad.getRetailCalendarID(_Conn, dto.getItemDateTime(), Constants.CALENDAR_DAY);
//	            		dto.setCalendarId(calendarId);	   
//	            	}	 
	            	
	            	
					String calKey = date + "_" + month + "_" + year;
					int calendarId = dataLoad.getRetailCalendarID(_Conn, dto.getItemDateTime(),  calKey, Constants.CALENDAR_DAY);
					
					if (calendarId > 0) {
						dto.setCalendarId(calendarId);	   
					}
					else {
						logger.warn("Calendar Id not found for date" + cal.getTime() + ", Please check with Retail Calendar Master data");
						countSkipped++;
						continue;
					}
	            	
					if(count == 1){
		            	SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
		            	inputDate = sdf.format(dto.getItemDateTime());
	            	}
	            	
	            	// 7: IDW-ITEM-UPC.
	            	//int upcInt = new Double(Double.parseDouble(line[7])).intValue();
					String upc = "";
					if (line[7].indexOf(".") >= 0)
						upc = PrestoUtil.castUPC(line[7].substring(0, line[7].indexOf(".")), false);
					else
						upc = PrestoUtil.castUPC(line[7], false);

					dto.setItemUPC(upc);
	            	int itemCode = dataLoad.getItemCode(upc, 0);
	            	if (itemCode > 0) {
	            		dto.setItemCode(itemCode);
	            	}
	            	else{
	            		itemCodeMissCount++;
	            	}
	            	
	            	// 8: IDW-ITEM-UNIT SALE PRICE.
	            	double unitSalePrice = 0;
	            	if(!StringUtils.isEmpty(line[8]))
	            		unitSalePrice = Double.parseDouble(line[8]);
	            	
	            	// 9: IDW-ITEM-UNIT REGULAR PRICE.
	            	double unitRegPrice = 0;
	            	if(!StringUtils.isEmpty(line[9]))
	            		unitRegPrice = Double.parseDouble(line[9]);
	            	
	            	double unitPrice = (unitSalePrice < unitRegPrice)? unitSalePrice : unitRegPrice;
	            	dto.setItemNetPrice(unitPrice);
	            	
	            	// 10: IDW-ITEM-ITEM COUNT.
	            	int itemCount = 0;
	            	if(!StringUtils.isEmpty(line[10]))
	            		itemCount = Integer.parseInt(line[10]);
	            	dto.setExtnQty(itemCount);
	            	
	            	// 11: IDW-ITEM-ITEM WEIGHT.
	            	int itemWeight = 0;
	            	if(!StringUtils.isEmpty(line[11]))
	            		itemWeight = Integer.parseInt(line[11].trim());
	            	dto.setExtnWeight(itemWeight);
	            	
	            	// 12: IDW-ITEM-GROSS-PRICE.
	            	double itemGrossPrice = 0;
	            	if(!StringUtils.isEmpty(line[12]))
	            		itemGrossPrice = Double.parseDouble(line[12]);
	            	dto.setExtendedGrossPrice(itemGrossPrice);
	            	
	            	// 13: IDW-ITEM-TPR DISCOUNT.
	            	double tprDiscount = 0;
	            	if(!StringUtils.isEmpty(line[13]))
	            		tprDiscount = Double.parseDouble(line[13]);
	            	
	            	// 14: IDW-ITEM-LOYALTY CARD DISCOUNT.
	            	double loyaltyDiscount = 0;
	            	if(!StringUtils.isEmpty(line[14]))
	            		loyaltyDiscount = Double.parseDouble(line[14]);
	            	
	            	// 15: IDW-ITEM-OTHER DISCOUNT.
	            	double otherDiscount = 0;
	            	if(!StringUtils.isEmpty(line[15]))
	            		otherDiscount = Double.parseDouble(line[15]);
	            	
	            	// 16: IDW-ITEM-NET-PRICE.
	            	double itemNetPrice = 0;
	            	if(!StringUtils.isEmpty(line[16]))
	            		itemNetPrice = Double.parseDouble(line[16]);
	            	else
	            		itemNetPrice = itemGrossPrice - tprDiscount - loyaltyDiscount - otherDiscount;
	            	
	            	dto.setExtendedNetPrice(PrestoUtil.round(itemNetPrice,2));
	            	
	            	// 17: IDW-ITEM-LOYALTY CARD NUM.
	            	String loyaltyCardNum = line[17];
	            	dto.setCustomerId(loyaltyCardNum);
	            	
	            	// 18: IDW-ITEM-STORE COUPON USED.
	            	// 19: IDW-ITEM-MANUFACTURER COUPON USED.
	            	// 20: IDW-ITEM-AD EVENT.
	            	// 21: IDW-ITEM-AD PAGE.
	            	// 22: IDW-ITEM-AD BLOCK.
            		
	            	listMoment.add(dto);
	            	
	            	if(itemCode > 0){
	            		itemCodeSet.add(String.valueOf(dto.getItemCode()));
	            	}
	            	
	            	dto = null;
	            	
	            	if ( listMoment.size() % Constants.BATCH_UPDATE_COUNT == 0)
	            	{
	            		logger.debug("Update into DB");
	            		//call DB update
	            		movementDao .insertMovementDaily(_Conn, listMoment, "MOVEMENT_DAILY");
	            		//Reset Array list
	            		listMoment = new ArrayList<MovementDTO>();
	            		
	            	}
	            	
                    if ( count % 25000 == 0 ) {
        				logger.info("Processed " + String.valueOf(count) + " records");
        				//break;
                    }
                    if ( stopAfter > 0 && count >= stopAfter ) {
                    	break;
                    }
	            }
	        }
	        if(listMoment.size() > 0){
	        	logger.debug("Update into DB");
        		//call DB update
        		movementDao.insertMovementDaily(_Conn, listMoment, "MOVEMENT_DAILY");
        		//Reset Array list
        		listMoment = new ArrayList<MovementDTO>();
	        }
	        logger.info("Processed " + String.valueOf(count) + " records.");
			logger.info("Error in " + String.valueOf(countFailed) + " records.");
			logger.info("Item code not exist for " + itemCodeMissCount + " records");
			String tmp = "Skipped " + String.valueOf(countSkipped) + " records.";
			logger.info(tmp);
			if ( countSkipped > 0 )
			{
				Iterator<String> it = unprocessedStores.iterator();
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
		}
        catch (Exception ex) {
        	logger.fatal("Fatal error", ex);
        	throw new GeneralException("Exception from readAndLoadMovements " + ex.getMessage());
        }
        finally {
        	if( reader != null){
        		reader.close();
        	}
        }	
	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		// TODO Auto-generated method stub
		
	}

}
