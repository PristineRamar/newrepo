package com.pristine.dataload.sas;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;



/**
 * 
 * @author Pradeepkumar
 *
 */

@SuppressWarnings({"rawtypes", "unchecked"})
public class ItemRetailerLoader extends PristineFileParser{
	 private static Logger logger = Logger.getLogger("ItemRetailerLoader");
	 private static String INPUT_PATH = "INPUT_PATH=";
	 private static final int LOG_RECORD_COUNT = 25000;
	 private List<ItemDTO> itemList = null;
	 private List<String> skippedList = null; 
	 Connection conn = null;
	
	 public ItemRetailerLoader() {
		 super();
		 conn = getOracleConnection();
	 }

	 
	 public static void main(String[] args) {
		ItemRetailerLoader itemRetailerLoader = new ItemRetailerLoader();
		
		PropertyConfigurator.configure("log4j-item-retailer-loader.properties");
		
		String inFolderName = null;
		for(String arg: args){
			if(arg.startsWith(INPUT_PATH)){
				inFolderName = arg.substring(INPUT_PATH.length());
			}
		}
		
		logger.info("*************************************");
		itemRetailerLoader.process(inFolderName);
		logger.info("*************************************");
	 }
	
	 
	 
	 /**
	  * begins the process (root method)
	  * @param inFolderName
	  */
	 private void process(String inFolderName){
		 
		 try{
			 init();
			 
			 processFiles(inFolderName);
			
			 insertNewRetailers();
			 
			 PristineDBUtil.commitTransaction(conn, "Retailer loading");
		 }
		 catch(GeneralException e){
			 logger.error("Error processing files", e);
			 PristineDBUtil.rollbackTransaction(conn, "Error processing files");
		 }
		 finally {
			PristineDBUtil.close(conn);
			//Clear cache
			itemList = null;
			skippedList = null;
		}
	 }
	 
	 /**
	  * Intializes objects
	  */
	 
	 private void init(){
		 itemList = new ArrayList<>();
		 skippedList = new ArrayList<>();
		 headerPresent = false;
	 }
	
	 
	 
	 /**
	  * Parses each file and caches all the rows
	  * @param inFolderName
	  * @throws GeneralException
	  */
	 
	private void processFiles(String inFolderName) throws GeneralException {
		String fieldNames[] = getFeildNames();
		ArrayList<String> fileList = getFiles(inFolderName);
		for (int j = 0; j < fileList.size(); j++) {
			logger.info("processFiles() - processing file -> " + fileList.get(j));
			
			parseDelimitedFile(ItemDTO.class, fileList.get(j), '|', fieldNames, -1);
			
			logger.info("processFiles() - " + itemList.size() + 
					" rows cached from file -> " + fileList.get(j));
		}
	}
	 
	 
	 /**
	  * Inserts new retailers into the SOURCE_RETAILER_MAP table
	  * @throws GeneralException
	  */
	private void insertNewRetailers() throws GeneralException {
		Set<String> upcSet = new HashSet<>();
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		ItemDAO itemDAO = new ItemDAO();
		HashMap<String, List<String>> newRetailerMap = new HashMap<>();
		HashMap<String, List<String>> existingRetailerMap = null;
		for (ItemDTO itemDTO : itemList) {
			upcSet.add(PrestoUtil.castUPC(itemDTO.getUpc(), false));
		}

		logger.info("insertNewRetailers() - Getting item codes for - " + upcSet.size() + " UPCs.");
		HashMap<String, String> itemcodeMap = retailPriceDAO.getItemCode(conn, upcSet);
		logger.info("insertNewRetailers() - Retrieved item codes for - " + itemcodeMap.size() + " UPCs.");

		logger.info("insertNewRetailers() - Getting retailer names for - " + itemcodeMap.values().size() + " items.");
		existingRetailerMap = itemDAO.getRetailerNamesForItems(conn, itemcodeMap.values());
		logger.info("insertNewRetailers() - Retrieved retailer names for - " + existingRetailerMap.size() + " items.");
		
		
		int recordCount = 0;
		for (ItemDTO itemDTO : itemList) {
			if(!Constants.EMPTY.equals(itemDTO.getRetailerName()) && itemDTO.getRetailerName() != null){
				recordCount++;
				String upcKey = PrestoUtil.castUPC(itemDTO.getUpc(), false);
							
				if(itemcodeMap.get(upcKey) == null){
					skippedList.add(upcKey);
				}else{
					String itemCode = itemcodeMap.get(upcKey);
					if(existingRetailerMap.get(itemCode) == null){
						updateMap(itemCode, itemDTO.getRetailerName(), newRetailerMap);
					}else{
						List<String> existingNames = existingRetailerMap.get(itemCode);
						if(!existingNames.contains(itemDTO.getRetailerName().toLowerCase())){
							updateMap(itemCode, itemDTO.getRetailerName(), newRetailerMap);
						}
					}
				}
				
				if (recordCount % LOG_RECORD_COUNT == 0) {
					logger.info("insertNewRetailers() - Processed " + recordCount + " records.");
				}
			}
		}
		
		logger.info("insertNewRetailers() - # of items to insert -> " + newRetailerMap.size());
		itemDAO.insertRetailerNames(conn, newRetailerMap);
		logger.info("insertNewRetailers() - Insert successful");
		
	}
	 
	/**
	 * Updates map through given value and key
	 * @param key
	 * @param value
	 * @param retailerMap
	 */
	private void updateMap(String key, String value, 
			HashMap<String, List<String>> retailerMap){
		if(retailerMap.get(key) == null){
			List<String> tempList = new ArrayList<>();
			tempList.add(value);
			retailerMap.put(key, tempList);
		}else{
			List<String> tempList = retailerMap.get(key);
			boolean isValueFound = false;
			for(String tempVal: tempList){
				if(tempVal.toLowerCase().equals(value.toLowerCase())){
					isValueFound = true;
				}
			}
			if(!isValueFound){
				tempList.add(value);
				retailerMap.put(key, tempList);
			}
		}	
	}
	 
	 /**
	  * Map of feilds in the file
	  * @return
	  */
	 private String[] getFeildNames(){
		 	String fieldNames[] = new String[21];
	    	int i = 0;
	    	
	    	fieldNames[i] = "majDeptName";
	    	i++;
	    	
	    	fieldNames[i] = "majDeptCode";
	    	i++;
	    	
	    	fieldNames[i] = "deptName";
	    	i++;
	    	
	    	fieldNames[i] = "deptCode";
	    	i++;
	    	
	    	fieldNames[i] = "deptShortName";
	    	i++;
	    	
	    	fieldNames[i] = "catName";
	    	i++;
	    	
	    	fieldNames[i] = "catCode";
	    	i++;
	    	
	    	fieldNames[i] = "subCatName";
	    	i++;
	    	
	    	fieldNames[i] = "subCatCode";
	    	i++;
	    	
	    	fieldNames[i] = "segmentName";
	    	i++;
	    	
	    	fieldNames[i] = "segmentCode";
	    	i++;
	    	
	    	
	    	fieldNames[i] = "retailerItemCode";
	    	i++;
	    	
	    	fieldNames[i] = "itemName";
	    	i++;
	    	
	    	fieldNames[i] = "size";
	    	i++;
	    	
	    	fieldNames[i] = "uom";
	    	i++;
	    	
	    	fieldNames[i] = "pack";
	    	i++;
	    	
	    	fieldNames[i] = "privateLabelCode";
	    	i++;
	    	fieldNames[i] = "likeItemGrp";
	    	i++;
	    	
	    	fieldNames[i] = "likeItemCode";
	    	i++;

	    	fieldNames[i] = "upc";
	    	i++;
	    	
	    	fieldNames[i] = "retailerName";
	    	i++;
	    	
	    	return fieldNames;
	 }
	 
	@Override
	public void processRecords(List listobj) throws GeneralException {
		itemList.addAll((List<ItemDTO>) listobj);
	}

}
