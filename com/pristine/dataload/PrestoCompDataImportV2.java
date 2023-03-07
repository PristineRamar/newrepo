package com.pristine.dataload;

/*
 * This program load Competitive data provided by external agency
 * Used for TOPS
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.CompetitiveDataDAOV2;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.fileformatter.AholdMovementFile;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

/*
 * Deployment notes
 * 1) Added properties for Header row and Data Separator
 * 2) Input file is a path from Directory
 * 3) Comp Store Id.
 * 4) Create folder BadData and CompletedData
 * 
 */
public class PrestoCompDataImportV2 {

	private static Logger logger = Logger.getLogger("PrestoCompDataLoad");
	private CompetitiveDataDAOV2 compDataDAOV2 = null;
	private CompetitiveDataDAO compDataDAO = null;

	private HashMap<String, String> allUPCSofStore = null;
	private HashMap<String, Integer> rawUOMList = new HashMap<String, Integer>();
	private HashMap<String, String> uomMap;
	// Key - UPC, Value - Item Code
	private HashMap<String, String> itemCodes = null;
	private HashMap<String, HashMap<String, Integer>> itemCodeList = null; 
	// Changes to handle multiple presto ITEM_CODE with same UPC
	private HashMap<String, List<Integer>> upcAndItsItem = null;
	private HashMap<String, List<Integer>> retailerItemAndItsItem  = new HashMap<String, List<Integer>>();

	private List<CompetitiveDataDTO> insertItemList = new ArrayList<CompetitiveDataDTO>();
	private List<CompetitiveDataDTO> updateItemList = new ArrayList<CompetitiveDataDTO>();
	private List<CompetitiveDataDTO> notCarriedItemList = new ArrayList<CompetitiveDataDTO>();

	private int recordCount = 0;
	
	private static boolean castingRetItemCode = true;
	
	private static String CAST_RET_ITEM_CODE = "CAST_RET_ITEM_CODE="; 

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j-comp-data.properties");
		logger.info("Data Load started");
		ArrayList<String> fileList = new ArrayList<String>();

		PropertyManager.initialize("analysis.properties");
		String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		Connection conn = null;
		boolean isNotPathArg = false;
		try {
			conn = DBManager.getConnection();
			PrestoCompDataImportV2 topsDataloader = new PrestoCompDataImportV2();
			if (args.length > 2) {
				logger.debug("Insufficient Arguments - Root Path and file name");
				System.exit(1);
			}
			logger.debug("arg[0] = " + args[0]);
			if (args.length > 0) {
				rootPath = rootPath + "/" + args[0];
			}
			logger.debug("Root Path is " + rootPath);

			for(String arg: args){
				if(arg.startsWith(CAST_RET_ITEM_CODE)){
					castingRetItemCode = Boolean.parseBoolean(
							arg.substring(CAST_RET_ITEM_CODE.length()));
					isNotPathArg = true;
				}
			}
			
			if (args.length == 2 && !isNotPathArg) {
				String fileName = rootPath;
				if (!rootPath.equals(""))
					fileName = fileName + "/";
				fileName = fileName + args[1];
				fileList.add(fileName);
			} else {
				File dir = new File(rootPath);

				String[] children = dir.list();
				if (children != null) {
					for (int i = 0; i < children.length; i++) {
						// Get filename of file or directory
						String filename = children[i];
						// logger.debug(filename);
						if (filename.contains(".csv")
								|| filename.contains(".txt"))
							fileList.add(rootPath + "/" + filename);
					}
				}

			}

			
			if (fileList.isEmpty()) {
				logger.info("No file present to load");
				System.exit(1);
			}
			for (String file : fileList) {
				logger.info("Processing of file:" + file + " is Started ");
				try {
					topsDataloader.compDataLoad(conn, file);
					PrestoUtil.moveFile(file, rootPath + "/"
							+ Constants.COMPLETED_FOLDER);
					logger.info("Processing of file:" + file + " is Completed ");
				} catch (GeneralException e) {
					// File level catching. So that processing of next file is
					// not blocked.
					e.printStackTrace();
					logger.error("Error while processing the file: " + file);
					logger.error("Error in Comp data loading",e);
					logger.error("Further processing of file is stopped ");
					logger.error("File moved to Bad Folder");
					PrestoUtil.moveFile(file, rootPath + "/"
							+ Constants.BAD_FOLDER);
				}

			}

		} catch (GeneralException ge) {
			logger.error("Error in Load", ge);
			System.exit(1);
		} catch (Exception e) {
			logger.error("Error in Load", e);
			System.exit(1);
		} finally {
			PristineDBUtil.close(conn);
		}

	}

	public void compDataLoad(Connection conn, String fileName)
			throws GeneralException {

		CsvReader reader = null;
		boolean headerPresent = true;
		String line[] = null;
		int lineCount = 0;

		String separator = PropertyManager.getProperty("DATALOAD.SEPARATOR",
				",");
		String checkUser = PropertyManager.getProperty(
				"DATALOAD.CHECK_USER_ID", Constants.PRESTO_LOAD_USER);
		String checkList = PropertyManager.getProperty(
				"DATALOAD.CHECK_LIST_ID", Constants.PRESTO_LOAD_CHECKLIST);

		CompetitiveDataDTO compData = null;
		ItemDAO itemdao = new ItemDAO();
		HashSet<Integer> schedulesProcessed = new HashSet<Integer>();
		List<CompetitiveDataDTO> compDataList;

		compDataDAOV2 = new CompetitiveDataDAOV2(conn);
		compDataDAO = new CompetitiveDataDAO(conn);
		compDataList = new ArrayList<CompetitiveDataDTO>();
		
		HashMap<ItemDetailKey, String> retailerItemandItemMap = itemdao.getAllItemsFromItemLookup(conn);
		for(Map.Entry<ItemDetailKey, String> retailerandItsItem: retailerItemandItemMap.entrySet()){
			ItemDetailKey key = retailerandItsItem.getKey();
			List<Integer> itemcodes = new ArrayList<Integer>();
			if(retailerItemAndItsItem.containsKey(key.getRetailerItemCode())){
				itemcodes = retailerItemAndItsItem.get(key.getRetailerItemCode());
			}
			itemcodes.add(Integer.parseInt(retailerandItsItem.getValue()));
			retailerItemAndItsItem.put(key.getRetailerItemCode(), itemcodes);
		}
		 
		
		HashSet<String> invalidStores = new HashSet<String>(); 
		String invStrs = "";

		if (PropertyManager.getProperty("DATALOAD.HEADER_PRESENT", "true")
				.equalsIgnoreCase("false"))
			headerPresent = false;

		try {

			reader = new CsvReader(new FileReader(fileName));
			reader.setDelimiter(separator.charAt(0));

			compDataDAO.setParameters(checkUser, checkList);
			

			// Populate UOM Map ToDO
			uomMap = itemdao.getUOMList(conn , "PRESTO_COMP_DATA");

			// Read the CSV file
			while (reader.readRecord()) {
				line = reader.getValues();
				lineCount++;
				if (headerPresent && lineCount == 1)
					continue;

				// Parse the line and save it in CompetitiveDataDTO
				compData = new CompetitiveDataDTO();
				compData = parseCompDataRecord(line);

				if (compData != null) {
					compData.newUOM = "";

					// Get the Store Id's
					ArrayList<String> storeIdList = null;
					//Ignore if there are store, which is not
					//configured in our system
					try
					{
					  storeIdList = compDataDAO.getStoreIdList(
							conn, compData.compStrNo);
					}
					catch(GeneralException ge)
					{
						//12/28/13 - Nagaraj - To consolidate the invalid stores and display it as a single log						
						  //to avoid so much of log being written and making difficult to find  other errors
						  if(storeIdList == null){
								if( !invalidStores.contains(compData.compStrNo))
									invalidStores.add(compData.compStrNo);
						  }
					}
					
					 
					if(storeIdList!=null)
					{
					Iterator<String> strItr = storeIdList.iterator();

					// Loop each stores
					while (strItr.hasNext()) {
						String compStrIdVal = strItr.next();
						compData.compStrId = Integer.parseInt(compStrIdVal);
						// Get ScheduleId
						compData.scheduleId = compDataDAO.getScheduleID(conn,
								compData);

						// Keep the schedule's
						if (!schedulesProcessed.contains(compData.scheduleId))
							schedulesProcessed.add(compData.scheduleId);

						// Add in the arraylist
						compDataList.add(compData);
					}
					}

					// Break the operation. Keeping more items in list would
					// lead to out-of-memory exception
					if (compDataList.size() >= Constants.LIST_SIZE_LIMIT ) {
						processCompDataList(conn, compDataList);

					}

				}
			}
			
			for (String invStoreNo : invalidStores) {
				invStrs = invStrs + "," + invStoreNo;	
			}
			logger.error("*** Following Invalid Stores found in File: " + invStrs + " **** ");			
			//logger.error("*** Following Invalid Stores found in File: " + StringUtils.join(invalidStores, ',') + " **** ");

			// Process remaining records
			if (compDataList.size() > 0) {
				processCompDataList(conn, compDataList);
			}

			// Now call performance schedule
			PriceChangeStats pcs = new PriceChangeStats();
			String loadLIGValue = PropertyManager.getProperty(
					"DATALOAD.ROLLUPLIG", "FALSE");
			String priceIndexFlagUpdate = PropertyManager.getProperty(
					"DATALOAD.UPDATEPRICEINDEXFLAG", "FALSE");
			logger.debug("LIG Load Flag value is " + loadLIGValue);
			CompDataLIGRollup ligRollup = new CompDataLIGRollup();
			CompDataPISetup piDataSetup = new CompDataPISetup();

			logger.info("Number of Schedule to process: " + schedulesProcessed.size());
			
			int schProcessCount = 0;
			for (int schId : schedulesProcessed) {		
				
				if(schId % 50 == 0)
				{
					logger.info("Number of Schedule Processed: " + schProcessCount);
					logger.info("Number of Schedule to yet Process: " + (schedulesProcessed.size()- schProcessCount));
				}
				pcs.calculatePriceChangeStats(conn, schId, 0);
				if (loadLIGValue.equalsIgnoreCase("true")) {
					ligRollup.LIGLevelRollUp(conn, schId);
					piDataSetup.setupSchedule(conn, schId);
				}

				try {
					if (priceIndexFlagUpdate.equalsIgnoreCase("true")) {
						itemdao.updatePriceIndexFlag(conn, schId);
						logger.info("Price Index Flag Updated for Schedule "
								+ schId);
					}
				} catch (Exception e) {
					logger.error("Error in setting Price Index Enabled Flag ",
							e);
				}
				PristineDBUtil.commitTransaction(conn, "Comp Data LIG");
				schProcessCount++;
			}

		} catch (GeneralException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		} catch (FileNotFoundException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		} catch (NumberFormatException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		} catch (IOException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		} catch (SQLException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		} catch (CloneNotSupportedException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (Exception e) {
				throw new GeneralException(
						"Error while executing compDataLoad", e);
			}
		}
	}

	private void processCompDataList(Connection conn,
			List<CompetitiveDataDTO> compDataList)throws GeneralException, CloneNotSupportedException {
		HashMap<String, CompetitiveDataDTO> compDataHashMap = new HashMap<String, CompetitiveDataDTO>();
		boolean checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
		boolean useAllItemsWithSameUPC = Boolean.parseBoolean(PropertyManager.getProperty("COMP_LOAD.USE_ALL_ITEMS_WITH_SAME_UPC", "FALSE"));
		upcAndItsItem = new HashMap<String, List<Integer>>();
		
		try {
			//Above configuration is added to handle comp file received for TOPS.
			//TOPS comp feed doesn't have RETAILER_ITEM_CODE
			//But when the configuration ITEM_LOAD.CHECK_RETAILER_ITEM_CODE is enabled for TOPS
			//it will try to use both retailer_item_code and UPC, as TOPS doesn't have retailer_item_code in the feed
			//it doesn't update any records

			//So one more configuration is added, which will get upc and all its item code
			//Ahold -- update comp price for item code based on UPC + Retailer Item Code
			//Tops  -- update comp price for all the items which has same UPC
			
			// Get the distinct UPC's
			getDistinctUPC(compDataList);

			// Get the item codes of UPC's
			if (!checkRetailerItemCode)
				getItemCodesOfUPC(conn, allUPCSofStore);
			else {
				getAllRetailerItemCodesOfUPC(conn, allUPCSofStore);
				// get upc and all its item code
				for (Entry<String, HashMap<String, Integer>> upcAndItsRetailerItemCode : itemCodeList.entrySet()) {
					HashMap<String, Integer> retItemCodeAndItsItem = upcAndItsRetailerItemCode.getValue();
					List<Integer> upcAndItems = new ArrayList<Integer>();
					for (Integer itemCode : retItemCodeAndItsItem.values()) {
						upcAndItems.add(itemCode);
					}
					if (upcAndItems.size() > 0)
						upcAndItsItem.put(upcAndItsRetailerItemCode.getKey(), upcAndItems);
				}
			}
				
	
			// Now keep everything in hashmap
			for (int j = 0; j < compDataList.size(); j++) {
//				boolean isItemFound = false;
				CompetitiveDataDTO compData = compDataList.get(j);
				String upc= PrestoUtil.castUPC(compData.upc, false);
				if (upcAndItsItem.get(upc) != null) {
					List<Integer> itemCodes = upcAndItsItem.get(upc);
					for(Integer prestoItemCode : itemCodes){
						compData = compDataList.get(j);
						CompetitiveDataDTO clonedCompData = (CompetitiveDataDTO) compData.clone();
						clonedCompData.itemcode = prestoItemCode;
						compDataHashMap.put(clonedCompData.scheduleId + "," + clonedCompData.itemcode, clonedCompData);
//						isItemFound = true;
					}
				}
				// Code changed to get presto item code if the item were not
				// available based on UPC and checkRetailerItemCode is given as TRUE
				else if (checkRetailerItemCode && retailerItemAndItsItem.get(compData.retailerItemCode) != null) {
					List<Integer> itemCodes = retailerItemAndItsItem.get(compData.retailerItemCode);
					for (Integer prestoItemCode : itemCodes) {
						compData = compDataList.get(j);
						CompetitiveDataDTO clonedCompData = (CompetitiveDataDTO) compData.clone();
						clonedCompData.itemcode = prestoItemCode;
						compDataHashMap.put(clonedCompData.scheduleId + "," + clonedCompData.itemcode, clonedCompData);
					}
				} else {
					notCarriedItemList.add(compData);
				}
			}
		
			// Get all the check data id's
			compDataDAOV2.populateCheckDataId(compDataHashMap);

			// Loop the Comp Data List
			for (CompetitiveDataDTO compDataDTO : compDataHashMap.values()) {
				// Find insert, update and not-carried items and fill it
				if (compDataDTO.checkItemId != 0) {
					updateItemList.add(compDataDTO);
				} else {
					insertItemList.add(compDataDTO);
				}
			}

			// Insert Items
			compDataDAO.insertCompetitiveData(insertItemList); 

			// Update Items
			compDataDAO.updateCompetitiveData(updateItemList);

			// Insert Not Carried Items
			// Database operation done in sequential and not in batch
			for (int j = 0; j < notCarriedItemList.size(); j++) {
			compDataDAO.insertNonCarriedItem(conn, notCarriedItemList.get(j));
			}
			//compDataDAO.insertNotCarriedItems(notCarriedItemList);

			logger.info("Record Range: " + recordCount + " -- "
					+ (compDataList.size() + recordCount));
			logger.info("Total No of Valid Records: " + compDataHashMap.size());
			logger.info("No of Records Inserted: " + insertItemList.size());
			logger.info("No of Records Updated: " + updateItemList.size());
			logger.info("No of Not Carried Items Inserted: "
					+ notCarriedItemList.size());

			// Commit the transaction
			PristineDBUtil.commitTransaction(conn, "Comp Data Load");
			logger.info("Transaction committed");

			recordCount = recordCount + compDataList.size();

			// Clear Hash Map and ArrayList
			compDataList.clear();
			insertItemList.clear();
			updateItemList.clear();
			notCarriedItemList.clear();
			allUPCSofStore.clear();
			if(itemCodes != null)
				itemCodes.clear();
			if(itemCodeList != null)
				itemCodeList.clear();

		} catch (GeneralException e) {
			// Batch level catch. 
			logger.error("Error while processing record range: " + recordCount
					+ " -- " + compDataList.size()
					+ " .Transaction Rollbacked. ");
			PristineDBUtil.rollbackTransaction(conn, "PrestoCompDataImportV2");
			recordCount = recordCount + compDataList.size();
			throw new GeneralException(
					"Error while executing processCompDataList", e);
		}
	}

	private void getDistinctUPC(List<CompetitiveDataDTO> compDataList) {

		allUPCSofStore = new HashMap<String, String>();
		for (int j = 0; j < compDataList.size(); j++) {
			CompetitiveDataDTO compData = compDataList.get(j);
			allUPCSofStore.put(compData.upc, "");
		}
	}

	private void getItemCodesOfUPC(Connection conn,
			HashMap<String, String> upcList) throws GeneralException {
		RetailPriceDAO retailPriceDao = new RetailPriceDAO();
		itemCodes = new HashMap<String, String>();
		itemCodes = retailPriceDao.getItemCode(conn, upcList.keySet());
	}
	
	
	private void getAllRetailerItemCodesOfUPC(Connection conn, HashMap<String, String> upcList) throws GeneralException {
		ItemDAO itemDAO = new ItemDAO();
		itemCodeList = itemDAO.getItemCode(conn, upcList.keySet());
	}

	private CompetitiveDataDTO parseCompDataRecord(String line[])
			throws GeneralException {
		/*
		 * 0 - Str Num 1 - Check Date 2 - NA (Retailer Item code) 3 - Item Desc
		 * 4 - Reg Qty 5 - Reg Price 6 - Size UOM 7 - UPC 8 - Outside Ind 9 -
		 * Additional Comments 10 - Sale Qty 11 - Sale Price 12 - Sale Date
		 */

		// logger.debug("***** New Record ****");
		CompetitiveDataDTO compData = new CompetitiveDataDTO();
		compData.compStrNo = line[0];

		// Cast the UPC. Expected length of the UPC is 11, but there are
		// UPC with 10 digit. So change the UPC to 11 by prefixing 0 to it
		// If the second parameter of the function is true, then it makes the
		// UPC to 11 digit
		if (line[7].length() == 10)
			compData.upc = PrestoUtil.castUPC(line[7], true);
		else
			compData.upc = line[7];
		// logger.debug("UPC = " + compData.upc);

		// There are rows with Reg quantity and Reg price with empty values.
		// Assign 0 if reg quantity/reg price is empty or null

		if (line[4] != null && !line[4].equals(""))
			compData.regMPack = Integer.parseInt(line[4]);
		else
			compData.regMPack = 0;

		if (line[5] != null && !line[5].equals(""))
			compData.regPrice = Float.parseFloat(line[5]);
		else
			compData.regPrice = 0;

		if (compData.regMPack <= 1) {
			compData.regMPack = 0;
			compData.regMPrice = 0;
		} else {
			compData.regMPrice = compData.regPrice;
			compData.regPrice = 0;
		}
		
		if (line[9] != null && !line[9].equals(""))
			compData.comment = line[9];
		
		if (line[10] != null && !line[10].equals(""))
			compData.saleMPack = Integer.parseInt(line[10]);
		else
			compData.saleMPack = 0;

		if (line[11] != null && !line[11].equals(""))
			compData.fSalePrice = Float.parseFloat(line[11]);
		else
			compData.fSalePrice = 0;

		if (compData.saleMPack <= 1) {
			compData.fSaleMPrice = 0;
			compData.saleMPack = 0;
		} else {
			compData.fSaleMPrice = compData.fSalePrice;
			compData.fSalePrice = 0;
		}

		compData.outSideRangeInd = line[8];
		compData.saleInd = "N";
		compData.priceNotFound = "N";
		compData.itemNotFound = "N";
		if (compData.regPrice == 0 && compData.regMPrice == 0
				&& (compData.fSalePrice > 0 || compData.fSaleMPrice > 0))
			compData.priceNotFound = "Y";
		else if (compData.regPrice == 0 && compData.regMPrice == 0)
			compData.itemNotFound = "Y";

		if (compData.fSalePrice > 0 || compData.fSaleMPrice > 0)
			compData.saleInd = "Y";

		// Format the date
		compData.checkDate = line[1];
		
		if(line[2] != null && !line[2].equals("")){
			if(castingRetItemCode){
				String retailerItemCode = String.valueOf(line[2]);
				compData.retailerItemCode = Long.toString((long) (Double.valueOf(retailerItemCode).longValue()));
			}else{
				compData.retailerItemCode = line[2];
			}
		}
		
		RDSDataLoad.setupWeekStartEndDate(compData);

		splitUOMSize(compData, line[6]);

		compData.itemName = line[3].trim();

		compData.itemName = compData.itemName.replaceAll("'", "''");
		// logger.debug("Item Name = " + compData.itemName);

		// logger.debug("***** End Record ****");
		return compData;
		// return null;

	}

	private void splitUOMSize(CompetitiveDataDTO compData, String sizeUOM) {

		int i = 0;

		i = 0;
		while (i < sizeUOM.length()) {
			int c = sizeUOM.charAt(i);
			// Non character
			if ((c >= 48 && c <= 57) || (c == 46)) {
				i++;
				continue;
			} else
				break;
		}
		compData.size = sizeUOM.substring(0, i);

		try {
			if (compData.size.length() > 0) {
				float size = Float.parseFloat(compData.size);
			}
		} catch (Exception e) {
			logger.error("Size in Error = " + compData.size);
			logger.error("Exception in parsing size", e);
			compData.size = null;
		}

		compData.uom = sizeUOM.substring(i).trim();
		// logger.debug( "Size = " + compData.size + "uom = " + compData.uom );

		if (!rawUOMList.containsKey(compData.uom))
			rawUOMList.put(compData.uom, 1);
		else {
			int count = rawUOMList.get(compData.uom);
			count++;
			rawUOMList.put(compData.uom, count);
		}
		/*
		 * 1 LB 2 OZ 3 CT 4 ML 5 DZ 24 PIR 7 QT 25 FT 26 DOS 27 ROL 13 EA 14 GA
		 * 15 SF 16 PT 18 PK 20 GR 21 LT 23 FZ 28 CF
		 */

		if (compData.uom.equals("EA") || compData.uom.equals("LT")
				|| compData.uom.equals("FZ") || compData.uom.equals("QT")
				|| compData.uom.equals("SF") || compData.uom.equals("CT")
				|| compData.uom.equals("OZ") || compData.uom.equals("LB")
				|| compData.uom.equals("ML"))
			;
		else if (compData.uom.equalsIgnoreCase("count")
				|| compData.uom.equalsIgnoreCase("SH")
				|| compData.uom.equalsIgnoreCase("ct")
				|| compData.uom.equalsIgnoreCase("COUN")
				|| compData.uom.equalsIgnoreCase("CO"))
			compData.uom = "CT";
		else if (compData.uom.equalsIgnoreCase("FOZ")
				|| compData.uom.equalsIgnoreCase("FLZ")
				|| compData.uom.equalsIgnoreCase("FL OZ")
				|| compData.uom.equalsIgnoreCase("FLO")
				|| compData.uom.equalsIgnoreCase("FLOZ"))
			compData.uom = "FZ";
		else if (compData.uom.equalsIgnoreCase("ROLL")
				|| compData.uom.equalsIgnoreCase("RL"))
			compData.uom = "ROL";
		else if (compData.uom.equalsIgnoreCase("SQF")
				|| compData.uom.equalsIgnoreCase("SQ"))
			compData.uom = "SF";
		else if (compData.uom.equalsIgnoreCase("LTR"))
			compData.uom = "LT";
		else if (compData.uom.equalsIgnoreCase("GAL")
				|| compData.uom.equalsIgnoreCase("GL"))
			compData.uom = "GA";
		else if (compData.uom.equalsIgnoreCase("PINT"))
			compData.uom = "PT";
		else if (compData.uom.equalsIgnoreCase("DOZ"))
			compData.uom = "DZ";
		else if (compData.uom.equalsIgnoreCase("oz")
				|| compData.uom.equalsIgnoreCase("OUNCE")
				|| compData.uom.equalsIgnoreCase("Z")
				|| compData.uom.equalsIgnoreCase("O"))
			compData.uom = "OZ";
		else if (compData.uom.equalsIgnoreCase("PK")) {
			compData.uom = null;
			compData.itemPack = compData.size;
			compData.size = null;
		} else {
			// logger.debug(" UOM unmapped:" + compData.uom );
			compData.itemName += " " + sizeUOM.replaceAll("'", "''");
			compData.uom = null;
		}

		if (compData.uom != null && uomMap != null
				&& uomMap.containsKey(compData.uom)) {
			compData.uomId = uomMap.get(compData.uom);
		}

	}

	private void printUOMList() {
		Iterator<String> keyItr = rawUOMList.keySet().iterator();

		while (keyItr.hasNext()) {
			String key = keyItr.next();
			int count = rawUOMList.get(key);
			logger.debug(" UOM = " + key + ", No of Occurences = " + count);
		}

	}

}
