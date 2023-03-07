package com.pristine.dataload.prestoload;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.CompetitiveDataDAOV2;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.MovementWeeklyDTO;
import com.pristine.dto.fileformatter.AholdMovementFile;
import com.pristine.exception.GeneralException;
import com.pristine.fileformatter.AholdMovFileToRetailPriceAndCost;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.RetailPriceDAO;

/**
 * Import the data from Ahold movement file to movement_weekly table
 * 
 * @author Nagarajan
 * 
 */
public class AholdMovementWeeklyDataLoad extends PristineFileParser {

	private static Logger logger = Logger
			.getLogger("AholdMovementWeeklyDataLoad");	
	private static int stopCount;
	private static String chainId;
	private static String inputFileRelativePath;
	private String rootPath;
	
	
	private List<AholdMovementFile> allRecords = new ArrayList<AholdMovementFile>();
	static HashMap<Integer, String> allColumns = new HashMap<Integer, String>();
	// Key - Comp Store Number, Value - Comp Store Id
	private HashMap<String,Integer> compStoreIds = new HashMap<String, Integer>();
	private HashMap<String,String> allUPCSofStore = null;
	// Key - UPC, Value - Item Code
	private HashMap<String,String> itemCodes = null;
	// Key - WK_DESC, Value - Schedule Id
	private HashMap<String, Integer> scheduleIds =  null;
	// Key - UPC, Value - Item Code
	private HashMap<String,Integer> storeItemCodes = null;
	// Key - ScheduleId,ItemCode Value - CompDataDto
	private HashMap<String, CompetitiveDataDTO> checkDataIds = null;	
	
	
	private MovementDAO movementDao = null;
	private CompetitiveDataDAOV2 compDataDaoV2 = null;
	private CompetitiveDataDAO compDataDao = null;
	private StoreDAO storeDao = null;
	private ItemDAO itemDao = null;
	private Connection	conn = null;
	private CompetitiveDataDTO compDataDto = null;	
	private int totalItems, validItems, updatedItems, skippedItems,updateFailedItems = 0, processedRecords=0;
	private int[] updateCount;
	private String itemNotFound ="";
	  
	/**
	 * @param args
	 * @throws GeneralException
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, GeneralException {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j.properties");
		stopCount = Integer.parseInt(PropertyManager.getProperty(
				"DATALOAD.STOP_AFTER", "-1"));

		if (args.length < 2) {
			logger.info("Invalid Arguments,  args[0] - Movement File Input Relative Path, args[1] - Comp Chain Id");
			System.exit(-1);
		}
		AholdMovementWeeklyDataLoad aholdMovWeeklyDataLoad = new AholdMovementWeeklyDataLoad();
		inputFileRelativePath = args[0];
		chainId = args[1];
		aholdMovWeeklyDataLoad.parseFile();
	}

	/**
	 * Read all the files from the input folder and process it
	 * 
	 * @param fieldNames
	 * @throws GeneralException
	 * @throws IOException
	 */
	private void parseFile() throws GeneralException, IOException {
		headerPresent = true;
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}

		//Clear hashmap as it is not required from here
		allColumns = null;
		ArrayList<String> fileList = getFiles(inputFileRelativePath);
		conn = DBManager.getConnection();
		
		for (int j = 0; j < fileList.size(); j++) {
			logger.info("Reading of file " + fileList.get(j)
					+ " is Started... ");
			try {
				parseDelimitedFile(AholdMovementFile.class, fileList.get(j),
						'|', fieldNames, stopCount);
				logger.info("Reading of file " + fileList.get(j)
						+ " is Completed... ");
				//Process remaining records
				if(allRecords.size() > 0)
					computeWeeklyStoreWise(allRecords);
				//Move the file to completed folder
				PrestoUtil.moveFile(fileList.get(j), rootPath + "/" + Constants.COMPLETED_FOLDER);
			} catch (GeneralException ge) {
				logger.error("File Skipped. Exception while reading of file "
						+ fileList.get(j));
				//Move the file to bad folder
				PrestoUtil.moveFile(fileList.get(j), rootPath + "/" + Constants.BAD_FOLDER);
			}
			
		}
		PristineDBUtil.close(conn);
	}

	public void processRecords(List listObject) throws GeneralException {			
			List<AholdMovementFile> aholdMovFile = (List<AholdMovementFile>) listObject;
			for (int j = 0; j < aholdMovFile.size(); j++) {			 
				allRecords.add(aholdMovFile.get(j));
			}
			
			//Batch process. Keeping all data in list may lead to out-of-memory exception
			if(allRecords.size()>= Constants.LIST_SIZE_LIMIT )
			{				
				computeWeeklyStoreWise(allRecords);
				processedRecords = processedRecords + allRecords.size();
				allRecords = null;
				allRecords = new ArrayList<AholdMovementFile>();				
			}			
	}
	
	private void computeWeeklyStoreWise(List<AholdMovementFile> allRecords) throws GeneralException
	{
		logger.info("Processing of Record Range " + processedRecords +  "-" + (processedRecords+allRecords.size()) + " is Started ");
		
		try {
		String storeNumber; 
		//Get Distinct stores
		getDistinctStoreIdAndUPC(allRecords);
		getItemCodesOfUPC(allUPCSofStore);
		//Loop through each stores
		for (Map.Entry<String, Integer> store : compStoreIds
				.entrySet()) {
			logger.info("Processing for Store Id " + store.getValue()
					+ " is Started... ");
			
			storeNumber = store.getKey();
			//Get all schedule id and item Code of the store
			getScheduleIdsAndItemCodeOfStore(allRecords,store.getValue(),storeNumber);
			//Get the check_data_id 
			getAllCheckDataIdofStore();
			computeMovementWeekly(allRecords,storeNumber);						
			writeRecordLog(store.getValue());
			//Reset resources for next store
			clearDatas();
			logger.info("Processing for Store Id " + store.getValue()
					+ " is Completed... ");
		}
		
		logger.info("Processing of Record Range " + processedRecords +  "-" + (processedRecords+allRecords.size()) + " is Completed ");
		
	} catch (SQLException e) {
	  
		e.printStackTrace();
	} catch (ParseException e) {
		 //TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
	
	/**
	 * Get all distinct store id from store number 
	 * and distinct item code from upc
	 * @param listObject
	 */
	private void getDistinctStoreIdAndUPC(List listObject) {		
		List<AholdMovementFile> aholdMovFile = (List<AholdMovementFile>) listObject;
		allUPCSofStore = new HashMap<String,String>();
		for (int j = 0; j < aholdMovFile.size(); j++) {
			AholdMovementFile aholdMovementFile = aholdMovFile.get(j);
			getStoreIdFromNo(aholdMovementFile.getOPCO_LOC_CD());			
			//Keep distinct upc's, same upc's will be overwritten
			allUPCSofStore.put(aholdMovementFile.getUPC_CD(), "");			 
		}	 
	}
	
	/**
	 * Get the Store Id from store number.
	 * The store id is fetched from database if it is not found in hashtable 
	 * @param storeNo
	 */
	private int getStoreIdFromNo(String storeNo) {
		int storeId = -1;
		if (compStoreIds.get(storeNo) != null) {
			storeId = compStoreIds.get(storeNo);
		} else {
			//To Do:
			storeDao = new StoreDAO();
			try{
			storeId = storeDao.getStoreID(conn, storeNo, null,Integer.valueOf(chainId));
			if(storeId != -1)
			compStoreIds.put(storeNo,storeId);
			}catch(GeneralException ge)
			{
				storeId = -1;
			}			
		}
		storeDao = null;
		return storeId;
	}
	
	private void getItemCodesOfUPC(HashMap<String,String> upcList) throws GeneralException
	{
		itemCodes = null;
		itemCodes = new HashMap<String, String>();
		RetailPriceDAO retailPriceDao = new RetailPriceDAO();
		itemCodes = retailPriceDao.getItemCode(conn, upcList.keySet());
	}
	
	/**
	 * Get all schedule id of the store
	 * @param listObject
	 * @param storeId
	 * @throws GeneralException 
	 * @throws ParseException 
	 */
	private void getScheduleIdsAndItemCodeOfStore(List listObject, int storeId, String storeNumber) throws ParseException, GeneralException {
		//Clear schedule id's for each store
		scheduleIds = new HashMap<String, Integer>();
		storeItemCodes = new HashMap<String, Integer>();
		List<AholdMovementFile> aholdMovFile = (List<AholdMovementFile>) listObject;
		for (int j = 0; j < aholdMovFile.size(); j++) {
			AholdMovementFile aholdMovementFile = aholdMovFile.get(j);
			//Filter store data
			if(aholdMovementFile.getOPCO_LOC_CD().equals(storeNumber))
			{
				storeItemCodes.put(aholdMovementFile.getUPC_CD(), getItemCodeFromUPC(aholdMovementFile.getUPC_CD()));
				getScheduleFromWeekDesc(aholdMovementFile, storeId);
			}
		}	
		
	}
	
	
	/**
	 * Get the schedule id from week description.
	 * The schedule id is fetched from database if it is not found in hashtable
	 * @param weekDesc
	 * @throws GeneralException 
	 * @throws ParseException 
	 */
	private int getScheduleFromWeekDesc(AholdMovementFile aholdMovementFile, int compStrId) throws GeneralException, ParseException {
		int scheduleId = -1;
		if (scheduleIds.get(aholdMovementFile.getWK_DESC()) != null) {
			scheduleId = scheduleIds.get(aholdMovementFile.getWK_DESC());
		} else {
			try{
			CompetitiveDataDTO compDataDto = new  CompetitiveDataDTO ();
			compDataDao = new CompetitiveDataDAO(conn);
			compDataDto.compStrId = compStrId;
			compDataDto.compStrNo = aholdMovementFile.getOPCO_LOC_CD();
			compDataDto.weekStartDate= aholdMovementFile.getWeekStartDate();
			compDataDto.weekEndDate = aholdMovementFile.getWeekEndDate();
			compDataDto.checkDate = aholdMovementFile.getWK_DESC();
			
			scheduleId = compDataDao.getScheduleID(conn, compDataDto);
			if(scheduleId != -1)
				scheduleIds.put(aholdMovementFile.getWK_DESC(),scheduleId);
			
			}catch(GeneralException ge)
			{
				scheduleId = -1;
			}
			 
		}
		compDataDao = null;
		return scheduleId;
	}
	
	

	/**
	 * Get all check_data_id of all schedules and its items of a store
	 * @throws GeneralException 
	 */
	private void getAllCheckDataIdofStore() throws GeneralException
	{
		
		checkDataIds  = new HashMap<String, CompetitiveDataDTO>();
		compDataDaoV2 = new CompetitiveDataDAOV2(conn);
		
		//Loop all schedule
		for (Map.Entry<String, Integer> schedule : scheduleIds
				.entrySet()) {
			
			//Loop all items
			for (Map.Entry<String, Integer> itemCode : storeItemCodes
					.entrySet()) {
				compDataDto = new CompetitiveDataDTO();
				if(itemCode.getValue() != -1)
				checkDataIds.put(schedule.getValue() + "," + itemCode.getValue(), compDataDto);
			}
		}
		compDataDaoV2.populateCheckDataId(checkDataIds);
		compDataDaoV2 = null;
	}
	
	/**
	 * Process store level data
	 * @param listObject
	 * @param storeNumber
	 * @throws GeneralException
	 * @throws SQLException
	 */
	private void computeMovementWeekly(List listObject, String storeNumber) throws GeneralException, SQLException {
	
		
		List<AholdMovementFile> aholdMovFile = (List<AholdMovementFile>) listObject;
		List<MovementWeeklyDTO> movWeeklyCollection = new ArrayList<MovementWeeklyDTO>();
		MovementWeeklyDTO movWeeklyDto;
		movementDao = new MovementDAO();

		for (int j = 0; j < aholdMovFile.size(); j++) {
			AholdMovementFile aholdMovementFile = aholdMovFile.get(j);
			
			//Filter store data
			if(aholdMovementFile.getOPCO_LOC_CD().equals(storeNumber))
			{
				//Initialize variables
				aholdMovementFile.initializeVariables();
				totalItems++;
				//Load data to movement weekly dto
				movWeeklyDto = fillMovementWeeklyDto(aholdMovementFile);
				if(movWeeklyDto != null)
				{
					validItems++;
					movWeeklyCollection.add(movWeeklyDto);
				}
				 
				//Batch update every 15000 records
				if(movWeeklyCollection.size() >= 15000)
				{
					//Get the update count
					updateCount = movementDao.batchUpdateToMovementWeekly(conn, movWeeklyCollection);
					if(updateCount!=null)
						findUpdatedRecords(updateCount);
					movWeeklyCollection = null;
					movWeeklyCollection = new ArrayList<MovementWeeklyDTO>();				
				}
			}						
		}
		//Update remaining records
		updateCount = movementDao.batchUpdateToMovementWeekly(conn, movWeeklyCollection);	
		if(updateCount!=null)
			findUpdatedRecords(updateCount);
		
		PristineDBUtil.commitTransaction(conn, "Movement Data Load");
		
	}
	
	private void writeRecordLog(int storeId)
	{
		String store = String.valueOf(storeId);
		logger.info("Total No of Records for Store Id " + store + " : "+ totalItems);
		logger.info("Total No of Valid Records for Store Id : " + store + " : "+  validItems);
		logger.info("Total No of Skipped Records for Store Id : " + store + " : "+  skippedItems);
		//logger.info("Skipped Items. Item/Check_data_id not found for UPC's: " + itemNotFound);
		logger.info("Total No of Successfully Updated Records for Store Id : " + store + " : "+  updatedItems);
		//logger.info("Total No of Successfully Updated Records for Store Id " + updateFailedItems);
	}
	
	private void clearDatas()
	{
		
		checkDataIds = null;
		scheduleIds = null;
		storeItemCodes = null;
		allUPCSofStore = null;
		 
		totalItems = 0;
		validItems = 0;
		updatedItems= 0;
		skippedItems = 0;
		updateFailedItems =0;
		itemNotFound = "";
	}

	
	/** 
	 * Get check_data_id from the hashmap
	 * @param scheduleId
	 * @param itemCode
	 * @return
	 */
	private int getCheckDataIdFromScheduleAndItemCode(int scheduleId, int itemCode)
	{		
		int checkDataId = -1;
		String key;
		key = String.valueOf(scheduleId) + "," + String.valueOf(itemCode);
		if (checkDataIds.get(key) != null) {
			compDataDto = new CompetitiveDataDTO();
			compDataDto = checkDataIds.get(key);
			checkDataId = compDataDto.checkItemId;
		}  
		 
		return checkDataId;
	}
	
	 
	/**
	 * Fill the data in movementweeklydto
	 * 
	 * @param aholdMovementFile
	 * @param movWeeklyDto
	 * @throws GeneralException 
	 */
	private MovementWeeklyDTO fillMovementWeeklyDto(AholdMovementFile aholdMovementFile) throws GeneralException {
		MovementWeeklyDTO movWeeklyDto = new MovementWeeklyDTO();
		 
		int scheduleId;
		try
		{
			movWeeklyDto.setCompStoreId(getStoreIdFromNo(aholdMovementFile.getOPCO_LOC_CD()));
			movWeeklyDto.setItemCode(getItemCodeFromUPC(aholdMovementFile.getUPC_CD()));
			scheduleId = getScheduleFromWeekDesc(aholdMovementFile,movWeeklyDto.getCompStoreId());
			
			movWeeklyDto.setCheckDataId(getCheckDataIdFromScheduleAndItemCode(scheduleId, movWeeklyDto.getItemCode()));
		 
			double unit = Double.valueOf(aholdMovementFile.getPOUNDORUNITS().trim()).doubleValue();
			double netDollar = Double.valueOf(aholdMovementFile.getNETDOLLARS().trim()).doubleValue();
			double grossDollar = Double.valueOf(aholdMovementFile.getGROSSDOLLARS().trim()).doubleValue();
			
			if(aholdMovementFile.isItemOnSale())
			{
				//Sale
				movWeeklyDto.setQuantitySale(unit);
				movWeeklyDto.setRevenueSale(netDollar);
				movWeeklyDto.setQuantityRegular(0);
				movWeeklyDto.setRevenueRegular(0);
			}
			else
			{
				//Regular
				movWeeklyDto.setQuantityRegular(unit);
				movWeeklyDto.setRevenueRegular(grossDollar);
				movWeeklyDto.setQuantitySale(0);
				movWeeklyDto.setRevenueSale(0);
			}		
			 
			movWeeklyDto.setDealCost(aholdMovementFile.getDealCostOfUnit());
			
			// Margin
			double finalCost = (movWeeklyDto.getQuantityRegular() + movWeeklyDto.getQuantitySale()) * movWeeklyDto.getDealCost();
			movWeeklyDto.setFinalCost(finalCost);
			double regularRevenue = movWeeklyDto.getRevenueRegular();
			double saleRevenue = movWeeklyDto.getRevenueSale();
			movWeeklyDto.setMargin(regularRevenue + saleRevenue - finalCost);
			 
		}
		catch(Exception e)
		{
			skippedItems++;			
			logger.error("Error. Item Skipped. Exception while processing item -- CompStoreNo: " + 
			aholdMovementFile.getOPCO_LOC_CD() + " UPC: " + aholdMovementFile.getUPC_CD() + " WEEK DESC: " + aholdMovementFile.getWK_DESC() );
			movWeeklyDto = null;		 
		}
		
		if(movWeeklyDto.getCompStoreId() == -1 || movWeeklyDto.getItemCode() == -1 || movWeeklyDto.getCheckDataId() == -1 )
		{
			skippedItems++;		
			//itemNotFound = itemNotFound + "," + aholdMovementFile.getUPC_CD();
			/*logger.error("Error. Item Skipped. CompStoreId(" + movWeeklyDto.getCompStoreId() + ")" +
			"/ItemCode(" + movWeeklyDto.getItemCode() + ")/CheckDataId(" + movWeeklyDto.getCheckDataId() + ")" + "is not found" +
					" for UPC: " + aholdMovementFile.getUPC_CD());*/
			movWeeklyDto = null;
		}
		
		return movWeeklyDto;
	}

	

	/**
	 * Get the Item Code from UPC
	 * The item code is fetched hashtable
	 * @param upc
	 */
	private int getItemCodeFromUPC(String upc) {
		int itemCode = -1;
		if (itemCodes.get(upc) != null) {
			itemCode = Integer.valueOf(itemCodes.get(upc));
		} 			
		 
		return itemCode;
	}

	
	

	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns() {
		allColumns.put(1, "CUSTOM_ATTR2_DESC");
		allColumns.put(2, "UPC_CD");
		allColumns.put(3, "OPCO_LOC_CD");
		allColumns.put(4, "WK_KEY");
		allColumns.put(5, "WK_DESC");
		allColumns.put(6, "SCANNEDUNITS");
		allColumns.put(7, "GROSSDOLLARS");
		allColumns.put(8, "NETDOLLARS");
		allColumns.put(9, "RETAILWEIGHT");
		allColumns.put(10, "POUNDORUNITS");
		allColumns.put(11, "EXTLEVEL3ELEMENTCOST");
		allColumns.put(12, "EXTLEVEL4ELEMENTCOST");
	}

	/**
	 * Find how many records were successfully updated
	 * @param updateCount
	 */
	private void findUpdatedRecords(int[] updateCount)
	{
		for (int i=0; i<updateCount.length; i++) {
	        if (updateCount[i] >= 0) {
	            updatedItems++;
	        }
	        else if (updateCount[i] == Statement.SUCCESS_NO_INFO) {
	        	updatedItems++;
	        }
	        else if (updateCount[i] == Statement.EXECUTE_FAILED) {
	            //updateFailedItems++;
	        }
	    }
	}
}
