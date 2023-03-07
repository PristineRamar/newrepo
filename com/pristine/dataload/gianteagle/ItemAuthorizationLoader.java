/*
 * Title: Item Authorization Loader
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	01/16/2012	John Britto		Initial Version 
 **********************************************************************************
 */

package com.pristine.dataload.gianteagle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.StoreItemMapDAO;
import com.pristine.dataload.service.StoreItemMapService;
import com.pristine.dto.StoreItemMapDTO;
import com.pristine.dto.offermgmt.VendorFileDTO;

import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class ItemAuthorizationLoader extends PristineFileParser {
	static Logger logger = Logger.getLogger("ItemAuthorizationLoader");
	int updateInsertCount = 0;
	int recordCount = 0;
	int ignored_NoZone_Count = 0;
	int ignored_NoItem_Count = 0;
	int statusMismatchCount = 0;
	int updateFailedCount = 0;
	int activeItemCount = 0;
	int inactiveItemCount = 0;
	int chainId = 0;
	List<StoreItemMapDTO> notProcessedItems;
	Connection conn = null;
	StoreItemMapDAO storeItemMapDAO = new StoreItemMapDAO();
	RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
	HashMap<String, Integer> retailPriceZone = new HashMap<String, Integer>();
	
	//Constructor
	public ItemAuthorizationLoader() {
		super("analysis.properties");
		try {
			// Create DB connection
			conn = DBManager.getConnection();

			} 
		catch (GeneralException ex) {
		}
	}

	//Main
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-ItemAuthorizationLoader.properties");
		PropertyManager.initialize("analysis.properties");
		
		String subFolder = null;
		String fileMode = null;
		String listFile = "";

		logger.info("ItemAuthorizationLoader - main() - Started");
		logCommand(args);
		
		// Read Arguments
		for (int ii = 0; ii < args.length; ii++) {

			String arg = args[ii];
		
			if (arg.startsWith("SUBFOLDER")) {
				subFolder = arg.substring("SUBFOLDER=".length());
			}
			else if (arg.startsWith("MODE")) {
				fileMode = arg.substring("MODE=".length());
			}
			else if (arg.startsWith("LIST_FILE")) {
				listFile = arg.substring("LIST_FILE=".length());
			}
		}

		// Create instance
		ItemAuthorizationLoader itemAuthorizationLoader = new ItemAuthorizationLoader();
		
		// Call the loader
		itemAuthorizationLoader.processItemAuthFile(subFolder,listFile, fileMode);
	}

	/**
	 * 
	 * @param subFolder
	 * @param listFile
	 * @return list files in order
	 * @throws GeneralException
	 */
	private ArrayList<String> getFileList(String subFolder, String listFile) throws GeneralException {
		ArrayList<String> fileList = new ArrayList<String>();

		String filePath = getRootPath() + "/" + subFolder + "/"  + listFile;
		File file = new File(filePath);
		if (!file.exists()){
    		logger.error("getFileList - File doesn't exist : " + filePath);
			throw new GeneralException("File: " + filePath + " doesn't exist");
		}
		
		BufferedReader br = null;
		try {
			String line;
			FileReader textFile = new FileReader(file);
			boolean allFilesExists = true;
			br = new BufferedReader(textFile);
			while ((line = br.readLine()) != null) {
				if (!line.trim().equals(""))
				{
					String strFile = getRootPath() + "/" + subFolder + "/" + line;
					File file1 = new File(strFile);
					if (file1.exists()) {
						fileList.add(strFile);
					}
					else
					{
						allFilesExists = false;
			    		logger.error("getFileList - File doesn't exist : " + strFile);
					}
				}
			}

			if (!allFilesExists){
				throw new GeneralException("Some files are missing.");
			}
		} catch (Exception e) {
        	throw new GeneralException("Error in getFileList()",e);
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return fileList;
	}
	
	/**
	 * 
	 * @param subFolder
	 * @param listFile
	 */
	@SuppressWarnings("unchecked")
	void processItemAuthFile(String subFolder,String listFile, String fileMode) {
			boolean commit = true;
			int updateStatus = 0;
			
			/*
			 * 1.	STR_NO – Store #
			 * 2.	RITEM_NO – Item Number (Retailer item code)
			 * 3.	SPLR_NO – Supplier Number (Vendor code)
			 * 4.	PRC_GRP_CD – Price group code
			 * 5.	AUTH_CD – Authorization code
			 * 6.	BNR_CD – Banner code (GE or MI)
			 * 7.	ZN_NO – Zone number
			 * */

			String fieldName[] = new String[7];
			fieldName[0] = "compStrNo";// STR_NO
			fieldName[1] = "retItemCode";// RITEM_NO
			fieldName[2] = "vendorNo";// SPLR_NO
			fieldName[3] = "prcGrpCode";// PRC_GRP_CD
			fieldName[4] = "itemStatus";// AUTH_CD
			fieldName[5] = "bannerCode";// BNR_CD
			fieldName[6] = "zoneNum";// ZN_NO

			
			logger.info("Item authorization starts");
			
			if (fileMode != null && fileMode.equalsIgnoreCase(Constants.DATA_LOAD_FULL)){
				logger.debug("Unauthorize all store item map...");
				updateStatus = storeItemMapDAO.UnauthorizeAllData(conn);
			}
			else
				updateStatus = 1;
			
			if (updateStatus == 0){
				logger.info("Error while Unauthorize item...");
				logger.info("Item authorization Ends");
			}
			
			ArrayList<String> fileList = new ArrayList<String>(); 
			try {
				// Get zone Id mapping
				long startTime = System.currentTimeMillis();
				logger.debug("Get Retail Price Zone...");
				retailPriceZone = retailPriceDAO.getRetailPriceZone(conn);
				long endTime = System.currentTimeMillis();
				logger.info("getRetailPriceZone() - Time taken to retreive Retail Price Zone mapping - " + (endTime - startTime));
				
				if (listFile.trim().isEmpty())
					fileList = getFiles(subFolder);
				else
					fileList = getFileList(subFolder, listFile);
		
			// Get chain id 	
				logger.debug("Get Chain Id...");				
			chainId = Integer.parseInt(retailPriceDAO.getChainId(conn));
			
			// Read input file
			logger.debug("Read input files...");
			for (int j = 0; j < fileList.size(); j++) {
				notProcessedItems = new ArrayList<StoreItemMapDTO>();
				logger.info("processing - " + fileList.get(j));
				long strTime = System.currentTimeMillis();
				
				String line[] = null;
				CsvReader reader = new CsvReader(fileList.get(j));
				int stop = -1;
				reader.setDelimiter('|');
				while(reader.readRecord()){
					line = reader.getValues();
					stop++;
				}
				stop--;
				reader.close();
				parseDelimitedFile(StoreItemMapDTO.class, fileList.get(j), '|',
						fieldName, stop);
				
				endTime = System.currentTimeMillis();
				logger.info("Time taken to process file - " + (endTime - strTime) + "ms");
				logger.info("Saving ignored items...");
				storeItemMapDAO.saveNotProcessedRecords(conn, notProcessedItems, fileList.get(j), null);
				logger.info("# of Records Precessed (total): " + updateInsertCount);
				logger.info("# of Status Active records: " + activeItemCount);
				logger.info("# of Status Inactive records: " + inactiveItemCount);
				logger.info("# of Status mismatch records: " + statusMismatchCount);
				logger.info("# of Records ignored (No zone mapping): " + ignored_NoZone_Count);
				logger.info("# of Records ignored (No Item): " + ignored_NoItem_Count);
			}

			logger.info("Item authorization ends");
			PristineDBUtil.commitTransaction(conn, "batch record update");

		} catch (GeneralException ge) {
			logger.error("Error while processing item authorization files - " + ge.toString());
			commit = false;
			ge.printStackTrace();
		}

		catch (Exception e) {
			logger.error("Error while processing item authorization files - " + e.toString());
			e.printStackTrace();
			commit = false;
		}
		String archivePath = getRootPath() + "/" + subFolder + "/";
		  if( commit ){
				PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
			}
			else{
				PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
			}
		
	}

	@Override
	public void processRecords(List listObject) throws GeneralException {
		ArrayList<StoreItemMapDTO> storeItemMapList = new ArrayList<StoreItemMapDTO>();
		List<StoreItemMapDTO> finalList = new ArrayList<StoreItemMapDTO>();
		Set<String> retItemCodeSet = new HashSet<String>();
		Set<String> strNoSet = new HashSet<String>();
		List<VendorFileDTO> vendorList = new ArrayList<>();
		StoreItemMapService storeItemMapService = new StoreItemMapService();
		for (int j = 0; j < listObject.size(); j++) {

			StoreItemMapDTO storeItemMapDTO = (StoreItemMapDTO) listObject.get(j);
			String retailerItemCode = storeItemMapDTO.getRetItemCode();
			
			setZoneNum(storeItemMapDTO);
			storeItemMapDTO.setRetItemCode(retailerItemCode);
			if(!retItemCodeSet.contains(retailerItemCode))
			retItemCodeSet.add(retailerItemCode);
			String storeNumber = storeItemMapDTO.getCompStrNo();
			storeItemMapDTO.setCompStrNo(storeNumber);
			if(!strNoSet.contains(storeNumber))
			strNoSet.add(storeNumber);
			
			//If price zone id is found for given zone number, process the record. else ignore
			if(retailPriceZone.containsKey(storeItemMapDTO.getZoneNumCombined())){
				storeItemMapDTO.setPriceZoneId(retailPriceZone.get(storeItemMapDTO.getZoneNumCombined()));
			}else{
				ignored_NoZone_Count++;
				notProcessedItems.add(storeItemMapDTO);
				continue;
			}
			
			//if the prc grp code is DSD, then setup the vendor information
			if(storeItemMapDTO.getPrcGrpCode().equals(Constants.GE_PRC_GRP_CD_DSD)){
				VendorFileDTO vendorFileDTO = new VendorFileDTO();
				String vendorName = "VENDOR-" + storeItemMapDTO.getVendorNo();
				vendorFileDTO.setVendorCode(storeItemMapDTO.getVendorNo());
				vendorFileDTO.setVendorName(vendorName);
				vendorList.add(vendorFileDTO);
			}
		
			storeItemMapList.add(storeItemMapDTO);
		}

		long startTime = System.currentTimeMillis();
		HashMap<String, List<Integer>> itemCodeMap = storeItemMapDAO
				.getItemCodeListForRetItemcodes(conn, retItemCodeSet);
		long endTime = System.currentTimeMillis();
		logger.info("Time taken to retreive itemcode map - "
				+ (endTime - startTime) + "ms");
		
		startTime = System.currentTimeMillis();
		HashMap<String, Integer> compStrNoLevelIdMap = retailPriceDAO
				.getStoreId(conn, chainId, strNoSet);
		endTime = System.currentTimeMillis();
		logger.info("Time taken to retreive storeId map - "
				+ (endTime - startTime) + "ms");

		HashMap<String, Long> vendorLookup = new HashMap<String, Long>(); 

		// If there is vendor data then get vendor lookup data
		if (vendorList != null && vendorList.size() >0){
			startTime = System.currentTimeMillis();
			vendorLookup = storeItemMapService.setupVendorLookup(conn, vendorList);
			endTime = System.currentTimeMillis();
			logger.info("Time taken to setup vendor lookup - "
					+ (endTime - startTime) + "ms");
		}
		
		
		for (StoreItemMapDTO storeItemMapDTO : storeItemMapList) {
			if (itemCodeMap.containsKey(storeItemMapDTO.getRetItemCode())
					&& compStrNoLevelIdMap.containsKey(storeItemMapDTO
							.getCompStrNo())) {

				List<Integer> itemCodeList = itemCodeMap.get(storeItemMapDTO
						.getRetItemCode());
				
				if (storeItemMapDTO.getItemStatus().equals(Constants.GE_AUTH_CD_Y)) {
					activeItemCount++;
				}
				else if (storeItemMapDTO.getItemStatus().equals(Constants.GE_AUTH_CD_M) 
						|| storeItemMapDTO.getItemStatus().equals(Constants.GE_AUTH_CD_S) 
						|| storeItemMapDTO.getItemStatus().equals(Constants.GE_AUTH_CD_R)) {
					inactiveItemCount++;
				} else {
					notProcessedItems.add(storeItemMapDTO);					
					statusMismatchCount++;
				}				
				
				for (int itemCode : itemCodeList) {
					StoreItemMapDTO outStoreItemMapDTO = new StoreItemMapDTO();

					boolean canBeAdded = false;
					outStoreItemMapDTO.copy(storeItemMapDTO);
					outStoreItemMapDTO.setItemCode(itemCode); //ITEM_CODE
					outStoreItemMapDTO.setCompStrId(compStrNoLevelIdMap
							.get(storeItemMapDTO.getCompStrNo())); //LEVEL_ID

					if(vendorLookup.containsKey(storeItemMapDTO.getVendorNo())){
						outStoreItemMapDTO.setVendorId(vendorLookup.get(storeItemMapDTO.getVendorNo())); //VENDOR_ID
					}else{
						outStoreItemMapDTO.setVendorId(null);//VENDOR_ID
					}
					
					if(outStoreItemMapDTO.getPrcGrpCode().equals(Constants.GE_PRC_GRP_CD_DSD)){
						outStoreItemMapDTO.setDistFlag(String.valueOf(Constants.DSD));//DIST_FLAG
					}else{
						outStoreItemMapDTO.setDistFlag(String.valueOf(Constants.WAREHOUSE));//DIST_FLAG
					}
					
					//if AUTH_CD is Y in the feed, authorize the item. Otherwise, unauthorized
					if (outStoreItemMapDTO.getItemStatus().equals(Constants.GE_AUTH_CD_Y)) {
						//Authorize
						outStoreItemMapDTO.setIsAuthorized("Y");//IS_AUTHORIZED
						canBeAdded = true;
					}
					else if (outStoreItemMapDTO.getItemStatus().equals(Constants.GE_AUTH_CD_M) 
							|| outStoreItemMapDTO.getItemStatus().equals(Constants.GE_AUTH_CD_S) 
							|| outStoreItemMapDTO.getItemStatus().equals(Constants.GE_AUTH_CD_R)) {
						//Unauthorized
						outStoreItemMapDTO.setIsAuthorized("N");//IS_AUTHORIZED
						canBeAdded = true;
					}
					
					if (canBeAdded) {
						finalList.add(outStoreItemMapDTO);
					}

				}
			} else {
				ignored_NoItem_Count++;
				notProcessedItems.add(storeItemMapDTO);
			}
		}
		startTime = System.currentTimeMillis();
		storeItemMapDAO.setupStoreItemMap(conn, finalList);
		endTime = System.currentTimeMillis();
		updateInsertCount += listObject.size();
		
		logger.info("Time taken to setup store item map - "
				+ (endTime - startTime) + "ms");
		logger.info("processRecords() - # of records processed - "
				+ updateInsertCount);


	}
	
	/**
	 * Combines banner code, zone number, prc grp cd and splr no
	 * @param storeItemMapDTO
	 */
	private void setZoneNum(StoreItemMapDTO storeItemMapDTO){
		String zoneNum = storeItemMapDTO.getBannerCode() + "-" + storeItemMapDTO.getZoneNum() + "-" + storeItemMapDTO.getPrcGrpCode();
		
		if(storeItemMapDTO.getVendorNo() != ""){
			zoneNum = zoneNum + "-" + storeItemMapDTO.getVendorNo();
		}
		
		storeItemMapDTO.setZoneNumCombined(zoneNum);
	}
	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: ItemAuthorizationLoader ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}
}
