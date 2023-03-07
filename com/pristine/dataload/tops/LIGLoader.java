package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ItemDAO;
//import com.pristine.dao.RetailPriceDAO;
import com.pristine.dataload.PrestoItemLoad;
//import com.pristine.dao.StoreItemMapDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.LigDTO;
import com.pristine.exception.GeneralException;
//import com.pristine.exception.OfferManagementException;
import com.pristine.parsinginterface.PristineFileParser;
//import com.pristine.service.offermgmt.RecommendationErrorCode;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
//import com.pristine.util.PristineDBUtil;
//import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;


/**
 * @author anand
 * 
 * This program loads LIG File received from Tops (Step 2). It inserts new LIRs. It updates reference to existing LIRs in Item_lookup table. 
 * If there is change in LIR definition (current LIR id is different from Prev LIR Id) then it resets 
 * RET_LIR_ITEM_CODE (representative item code) to null for both the lir ids (current and previous)   
 * 
 * This program update's RET_LIR_ID in the ITEM_LOOKUP table for all the items (both non-lig, lig representing item) in the lig feed
 * If there is a new LIG, then it create a new record in RETAILER_LIKE_ITEM_GROUP table
 * If a item is moved from one lig to another lig, then it resets 
 * RET_LIR_ITEM_CODE (representative item code) in RETAILER_LIKE_ITEM_GROUP table to null for both the lir ids 
 * 
 * This is Step 2 in the LIG Loading Process.  
 * Overall steps in loading LIG
 * Step 1) 	a) Copy ret_lir_item_code to ret_lir_item_code_prev in the retailer_like_item_group (manual)
 * 					UPDATE RETAILER_LIKE_ITEM_GROUP SET RET_LIR_ITEM_CODE_PREV = RET_LIR_ITEM_CODE 
 * 			b) Copy ret_lir_id to prev_ret_lir_id in the item_lookup table. (manual)
 * 					UPDATE ITEM_LOOKUP SET PREV_RET_LIR_ID = RET_LIR_ID;
 * 			c) Set ret_lir_id as null in the item_lookup table. (manual)
 * 					UPDATE ITEM_LOOKUP SET RET_LIR_ID = NULL;
 * Step 2) Run this program to setup new LIRs and update existing LIRs
 * Step 3) Setup representation item by running PrestoItemLoader as below. It updates representative item code and add new if required. 
 * 			Passing false updates items with null representative item codes. It adds new representative item code if not present  
 * 		   >PrestoItemLoader LIG_ITEM_CODE_SETUP false	
 * Step 4) Correction to Price Index programs.
 */
public class LIGLoader extends PristineFileParser{
	private static Logger logger = Logger.getLogger("LIGLoader");
	
	private Connection conn = null;
	private ItemDAO itemdao = null;
	private static int columnCount = 10;
	private static int retailerItemCodeLength = 6;
	private int recordCount = 0;
	private HashMap<String, List<ItemDTO>> itemMap = null;
	//private boolean checkRetailerItemCode = false;
	Set<String> distinctIgnoredKVICode = new HashSet<String>();
	private HashMap<Integer, ItemDTO> kviItems = new HashMap<Integer, ItemDTO>();
	private int priceCheckListIdKvi = 0;
	private int priceCheckListIdK2 = 0;
	HashMap<String, Integer> priceCheckListTypeIdPriceCheckListIdMap = new HashMap<String, Integer>();
	HashMap<String, String> retItemCodeAndItsName = new HashMap<String, String>();
	//HashMap<String, String> retItemCodeAndItsKviCode = new HashMap<String, String>();
	HashMap<String, Integer> retItemCodeAndItsVersion = new HashMap<String, Integer>();
	
	public LIGLoader(){
		PropertyConfigurator.configure("log4j-lig-loader.properties");
		PropertyManager.initialize("analysis.properties");
		conn = getOracleConnection();
		itemdao = new ItemDAO();
		itemMap = new HashMap<String, List<ItemDTO>>();
		priceCheckListIdKvi = Integer.parseInt(PropertyManager.getProperty("TOPS_PRICE_CHECK_LIST_ID_KVI"));
		priceCheckListIdK2 = Integer.parseInt(PropertyManager.getProperty("TOPS_PRICE_CHECK_LIST_ID_K2"));
		//checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
	}
	
	/**
	 * Main method of Retail Price Setup Batch
	 * @param args[0]	Relative path of the input file
	 */
	public static void main(String[] args) {
		LIGLoader ligLoader = new LIGLoader();
		ligLoader.loadLIG(args[0]);
	}
	
	private void loadLIG(String relativePath) {
		try {
			logger.info("LIG Loader Starts");
			// Reset LIG's
			ArrayList<String> zipFileList = getZipFiles(relativePath);
			ArrayList<String> fileList = getFiles(relativePath);

			// Continue only if there are files
			if (zipFileList.size() > 0 || fileList.size() > 0) {
				resetLIG();
				prepareData();
				processLIGInfo(relativePath);
				// set RET_LIR_ITEM_CODE to null for items which is removed from lig
				logger.info("unlinking of unused ret_lir_id is started");
				itemdao.unlinkRetLirId(conn);
				logger.info("unlinking of unused ret_lir_id is completed");
				
				logger.info("Setting KVI List is started");
				loadKeyListItems(kviItems);
				if (!distinctIgnoredKVICode.isEmpty()) {
					logger.info("Following KVI codes "
							+ PRCommonUtil.getCommaSeperatedStringFromStrSet(distinctIgnoredKVICode)
							+ " are ignored");
				}
				logger.info("Setting KVI List  is completed");
				
				super.performCloseOperation(conn, true);
				logger.info("LIG Loading Complete");
			} else {
				logger.error("No input files present, lig is not loaded");
			}
		} catch (GeneralException ge) {
			logger.error("Error in LIG setup. Transaction is rollbacked - " + ge);
			super.performCloseOperation(conn, false);
		}

		// Call item loader to setup the lig
		String args[] = { "LIG_ITEM_CODE_SETUP", "TRUE" };
		PrestoItemLoad.main(args);
	}
	
	private void resetLIG() throws GeneralException {
		logger.info("Copying RET_LIR_ITEM_CODE to RET_LIR_ITEM_CODE_PREV in RETAILER_LIKE_ITEM_GROUP table is Started");
		itemdao.copyRetLirItemCode(conn);
		logger.info("Copying RET_LIR_ITEM_CODE to RET_LIR_ITEM_CODE_PREV in RETAILER_LIKE_ITEM_GROUP table is Completed");
		
		logger.info("Copying RET_LIR_ID to PREV_RET_LIR_ID in ITEM_LOOKUP table is Started");
		itemdao.copyRetLirId(conn);
		logger.info("Copying RET_LIR_ID to PREV_RET_LIR_ID in ITEM_LOOKUP table is Completed");
		
		logger.info("Reseting RET_LIR_ID in ITEM_LOOKUP table is Started");
		itemdao.resetRetLirId(conn);
		logger.info("Reseting RET_LIR_ID in ITEM_LOOKUP table is Completed");
	}
	
	private void prepareData() throws GeneralException {
		try {
			itemMap = itemdao.getRetItemCodeAndItem(conn);
			priceCheckListTypeIdPriceCheckListIdMap = itemdao.getKviCodeForPriceCheckListId(conn, priceCheckListIdKvi,
					priceCheckListIdK2);
		} catch (GeneralException ge) {
			logger.error("Error when retrieving item info - " + ge);
			throw new GeneralException("Error when retrieving item info");
		}
	}

	private void  processLIGInfo(String relativePath) throws GeneralException{
		try
		{		
			//getzip files
			ArrayList<String> zipFileList = getZipFiles(relativePath);
			
			//Start with -1 so that if any regular files are present, they are processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;
			
			String zipFilePath = getRootPath() + "/" + relativePath;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;
				try {
					if( processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;
	
					fileList = getFiles(relativePath);
	
					for (int i = 0; i < fileList.size(); i++) {
						
						long fileProcessStartTime = System.currentTimeMillis(); 
						
					    String files = fileList.get(i);
					    logger.info("File Name - " + fileList.get(i));
					    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
					    
					    String fieldNames[] = new String[11];
		
				    	fieldNames[0] = "lineGroupIdentifier";
				    	fieldNames[1] = "retailerItemCode";
				    	fieldNames[2] = "internalItemNo";
				    	fieldNames[3] = "lineGroupStatus";
				    	fieldNames[4] = "sizeClass";
				    	fieldNames[5] = "sizeFamily";
				    	fieldNames[6] = "brandClass";
				    	fieldNames[7] = "brandFamily";
				    	fieldNames[8] = "upc";
				    	fieldNames[9] = "kviCode";
				    	
				    	String []fileFields  = mapFileField(fieldNames, columnCount); 
				    	recordCount = 0;
				    	super.parseDelimitedFile(LigDTO.class, files, '|',fileFields, stopCount);
				    	
						logger.info("Number of records processed - " + recordCount);
					    long fileProcessEndTime = System.currentTimeMillis();
					    logger.info("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime) + "ms");
					}
				} catch (GeneralException | Exception ex) {
					logger.error("GeneralException", ex);
					commit = false;
				}
				
				if( processZipFile){
			    	PrestoUtil.deleteFiles(fileList);
			    	fileList.clear();
			    	fileList.add(zipFileList.get(curZipFileCount));
			    }
			    String archivePath = getRootPath() + "/" + relativePath + "/";
			    
				if (commit) {
					// if zip file, move the zip file
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
					throw new GeneralException("Error in processLIGInfo()");
				}
				curZipFileCount++;
				processZipFile = true;
			}while (curZipFileCount < zipFileList.size());
		}catch (GeneralException | Exception ex) {
	        logger.error("Outer Exception -  GeneralException", ex);
	        throw new GeneralException("Error in processLIGInfo()");
	    }
	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		loadLIGInfo(listobj);
	}
	
	private void loadLIGInfo(List ligListObject) throws GeneralException{
		List<LigDTO> itemList = (List<LigDTO>) ligListObject;
		List<ItemDTO> updateList = new ArrayList<ItemDTO>();
		List<ItemDTO> lirUpdateList = new ArrayList<ItemDTO>();
		
		for (int j = 0; j < itemList.size(); j++) {
			recordCount++;
			LigDTO ligDTO = itemList.get(j);
			String retItemCode = ligDTO.getRetailerItemCode().substring(0, ligDTO.getRetailerItemCode().indexOf("_"));
			retItemCode = retItemCode.substring(retItemCode.length() - retailerItemCodeLength);
			
			String strItemCodeVersion = ligDTO.getRetailerItemCode().substring(ligDTO.getRetailerItemCode().indexOf("_") + 1);
			int itemCodeVersion = Integer.valueOf(strItemCodeVersion);
			
			if( retItemCodeAndItsVersion.containsKey(retItemCode)){
				int processedItemCodeVersion = retItemCodeAndItsVersion.get(retItemCode);
				if( processedItemCodeVersion > itemCodeVersion) {
					continue;
				}
			}
				
			//let the for loop continue
			retItemCodeAndItsVersion.put(retItemCode,itemCodeVersion);
			
			List<ItemDTO> allItemsWithSameRetItemCode = itemMap.get(retItemCode);
			if (allItemsWithSameRetItemCode != null) {
				// Items with same retailer item code
				for (ItemDTO item : allItemsWithSameRetItemCode) {
					ItemDTO itemDTO = new ItemDTO();
					itemDTO.itemCode = item.getItemCode();
					itemDTO.retailerItemCode = retItemCode;
					itemDTO.likeItemGrp = ligDTO.getLineGroupIdentifier();
					itemDTO.upc = item.getUpc();
					itemDTO.kviCode = ligDTO.getKviCode();
					
					/* SG - July 8 Remove the following block of code - Start */
					//Added on 2nd July 2015, if the feed has retailer item code more than once,
					//and if any one of the retailer item code has lig name, then consider that retailer item code is part of lig, 
					//even though other occurrence of same retailer item code has empty lig name
					
					//Keep first occurrence of non empty lig name
					/*if ((itemDTO.likeItemGrp != null && !itemDTO.likeItemGrp.equals(""))) {
						//if already there is a entry with lig name, then continue with that
						if (retItemCodeAndItsName.get(itemDTO.retailerItemCode) == null)
							retItemCodeAndItsName.put(itemDTO.retailerItemCode, itemDTO.likeItemGrp);
					}
					
					//If there is no lig name
					if ((itemDTO.likeItemGrp == null || itemDTO.likeItemGrp.equals(""))) {
						//Check if there is any entry with lig name, if there pick that
						if (retItemCodeAndItsName.get(itemDTO.retailerItemCode) != null)
							itemDTO.likeItemGrp = retItemCodeAndItsName.get(itemDTO.retailerItemCode);
					}
					
					//Keep first occurrence of non empty kvi code
					if ((itemDTO.kviCode != null && !itemDTO.kviCode.equals(""))) {
						//if already there is a entry with kvi code, then continue with that
						if (retItemCodeAndItsKviCode.get(itemDTO.retailerItemCode) == null)
							retItemCodeAndItsKviCode.put(itemDTO.retailerItemCode, itemDTO.kviCode);
					}
					
					//If there is no kvi code
					if ((itemDTO.kviCode == null || itemDTO.kviCode.equals(""))) {
						//Check if there is any entry with lig name, if there pick that
						if (retItemCodeAndItsKviCode.get(itemDTO.retailerItemCode) != null)
							itemDTO.kviCode = retItemCodeAndItsKviCode.get(itemDTO.retailerItemCode);
					}
					
					*/
					/* SG - July 8 Remove the following block of code - End  */
					
					if ((itemDTO.likeItemGrp != null && !itemDTO.likeItemGrp.equals(""))) {
						itemDTO.likeItemGrp = itemDTO.likeItemGrp.replaceAll("'", "''").trim();
						itemDTO.likeItemId = itemdao.setupRetailerLikeItem(conn, itemDTO.likeItemCode,
								itemDTO.likeItemGrp);
					} else {
						itemDTO.likeItemId = 0;
					}
					
					int prvLirId = 0;
					//get pre ret_lir_id from item_lookup table
					prvLirId = itemdao.getPreRetLirId(conn, itemDTO.upc, itemDTO.retailerItemCode);
					
					//Set all lig representing items
					//Mostly there can't be more than one active lig representing item in a lig
					//But there may be cases, when lig representing item is moved from one lig to another lig
					//So upc checked along with that to update correct records
					if (itemMap.get('L' + itemDTO.retailerItemCode) != null) {
						for (ItemDTO ligRepItem : itemMap.get('L' + itemDTO.retailerItemCode)) {
							if (ligRepItem.upc.equals('L' + itemDTO.upc) && ligRepItem.lirInd) {
								ItemDTO lirItemDTO = new ItemDTO();
								lirItemDTO.upc = 'L' + itemDTO.upc;
								lirItemDTO.retailerItemCode = 'L' + itemDTO.retailerItemCode;
								lirItemDTO.likeItemGrp = itemDTO.likeItemGrp;
								lirItemDTO.likeItemId = itemDTO.likeItemId;
								lirItemDTO.itemCode = itemDTO.itemCode;
								lirItemDTO.ligRepItemCode = ligRepItem.itemCode;
								lirUpdateList.add(lirItemDTO);
								if (itemDTO.likeItemId != prvLirId) {
									if (itemDTO.likeItemId > 0)
										itemdao.updateLirItemCode(conn, itemDTO.likeItemId);
									if (prvLirId > 0)
										itemdao.updateLirItemCode(conn, prvLirId);
								}
							}
						}
					}
					if (itemDTO.kviCode.trim().equals(Constants.KVI) && priceCheckListIdKvi > 0 &&
							priceCheckListTypeIdPriceCheckListIdMap.containsKey(itemDTO.kviCode)) {
						itemDTO.priceCheckTypeId = priceCheckListIdKvi;
						kviItems.put(itemDTO.itemCode, itemDTO);
					} else if (itemDTO.kviCode.trim().equals(Constants.K2) && priceCheckListIdK2 > 0 &&
							priceCheckListTypeIdPriceCheckListIdMap.containsKey(itemDTO.kviCode)) {
						itemDTO.priceCheckTypeId = priceCheckListIdK2;
						kviItems.put(itemDTO.itemCode, itemDTO);
					} else {
						distinctIgnoredKVICode.add(itemDTO.kviCode);
					}
					
					updateList.add(itemDTO);
				}
			}
			if(recordCount % 10000 == 0)
				logger.info("Number of records processed - " + recordCount);
		}
		//Update item's (except lig representing item) with latest ret_lir_id to item_lookup
		itemdao.updateLikeItemId(conn, updateList);
		//Update lig representing item's with latest ret_lir_id to item_lookup
		itemdao.updateLirItem(conn, lirUpdateList);
	}

	/**
	 * This method returns an array of fields in input file
	 * @param fieldNames
	 * @param columnCount2
	 * @return String array
	 */
	public static String[] mapFileField(String[] fieldNames, int columnCount2) {
		int fieldLength = columnCount2 < fieldNames.length? columnCount2 :fieldNames.length;
		String [] fileFields = new String[fieldLength];
		for ( int i = 0; i < fieldLength; i++)
			fileFields[i] = fieldNames[i];
		return fileFields;
	}

	private void loadKeyListItems(HashMap<Integer, ItemDTO> kviCodeRecords) throws GeneralException {
		int deleteCount = 0, insertCount = 0;

		logger.info("Deleting from Price_Check_List_Items is Started");
		deleteCount = itemdao.deletePriceCheckListRecords(conn, priceCheckListIdKvi, priceCheckListIdK2);
		logger.info("Deleting from Price_Check_List_Items is Completed");
		logger.info("No of Records deleted from Price_Check_List_Items : " + deleteCount);

		logger.info("Inserting into Price_Check_List_Items is Started");
		if (kviCodeRecords.size() > 0)
			insertCount = itemdao.loadKviInfoInPriceCheckListItems(conn, kviCodeRecords);
		logger.info("Inserting into Price_Check_List_Items is Completed");
		logger.info("No of Records Inserted in Price_Check_List_Items : " + insertCount);
	}
}
