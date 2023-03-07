package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.ItemDetailDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PopulateBuyerCode {
	
	private static Logger logger = Logger.getLogger("PopulateBuyerCode");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private String rootPath;
	private Connection conn = null;
	private static String relativeInputPath;
	List<String>skippedItem;
	
	private PopulateBuyerCode() {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error("Error when creating connection - " + gex);
		}
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-buyer_code_loader.properties");
		PropertyManager.initialize("analysis.properties");
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
		}
		PopulateBuyerCode populateBuyerCode = new PopulateBuyerCode();
		populateBuyerCode.processFile();
	}
	private void processFile(){
		ExcelFileParser parser = new ExcelFileParser();
		String inputPath = rootPath + "/" + relativeInputPath;
		parser.setFirstRowToProcess(1); // skip header row
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		mapFieldNames(fieldNames);
		ArrayList<String> fileList = parser.getFiles(relativeInputPath);
		if(fileList.size() == 0){
			logger.error("No Buyer Code input file were found");
		}
		String fileName = null;
		try {
			for (String processigFile : fileList) {
				skippedItem = new ArrayList<String>();
				logger.info("Processing file - " + processigFile);
				fileName = processigFile;
				List<ItemDetailDTO> buyerCodeDetails = parser.parseExcelFile(ItemDetailDTO.class, fileName, 0, fieldNames);
				logger.info("Total number of Items to process: "+buyerCodeDetails.size());
				processBuyerCodeDetails(buyerCodeDetails);
				PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.COMPLETED_FOLDER);
				PristineDBUtil.commitTransaction(conn, "Populate Buyer Code");
			}
		} catch (GeneralException ge) {
			logger.error("Error when parsing or processing input file", ge);
			PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.BAD_FOLDER);
			PristineDBUtil.rollbackTransaction(conn, "Populate Buyer Code");
		} finally {
			PristineDBUtil.close(conn);
		}
	}
	
	private void processBuyerCodeDetails(List<ItemDetailDTO> buyerCodeDetails) throws GeneralException{
		HashMap<String,Integer> itemWithAttrId = new HashMap<String,Integer>();
		//Load User Attr lookup details
		ItemDAO itemDAO = new ItemDAO();
		HashMap<String,Integer> userAttrDetailsMap = itemDAO.getUserAttrDetailsMap(conn);
		//Loop each buyer details and process
		buyerCodeDetails.forEach(buyerCode ->{
			buyerCode.setBuyerCode(convertBuyerCodetoSting(buyerCode.getBuyerCode()));
			if(userAttrDetailsMap.containsKey(buyerCode.getBuyerCode())){
				itemWithAttrId.put(buyerCode.getRetailerItemCode(), userAttrDetailsMap.get(buyerCode.getBuyerCode()));
			}else{
				try {
					//Insert new Buyer code in USER ATTR LOOKUP table
					itemDAO.insertUserAttrDetails(conn, buyerCode.getBuyerCode());
					//Get Attr Id based on the Buyer code
					itemDAO.getUserAttrDetails(conn, userAttrDetailsMap, buyerCode.getBuyerCode());
					//If the Buyer code is available in Collection then get Id to update in Item lookup
					if(userAttrDetailsMap.containsKey(buyerCode.getBuyerCode())){
						itemWithAttrId.put(buyerCode.getRetailerItemCode(), userAttrDetailsMap.get(buyerCode.getBuyerCode()));
					}
				} catch (GeneralException e) {
					e.printStackTrace();
					logger.error("Error occured while processing processBuyerCodeDetails() for the new Buyer code: "+buyerCode.getBuyerCode());
				}
			}
		});
		
		itemDAO.updateUserAttrInItemLookup(conn, itemWithAttrId);
	}

	private void mapFieldNames(TreeMap<Integer, String> fieldNames) {
			fieldNames.put(0, "upc");//UPC
			fieldNames.put(1, "buyerCode");//Buyer Code
			fieldNames.put(2, "retailerItemCode");// Retailer Item code
	}
	
	private String convertBuyerCodetoSting(String buyerCode){
		try{
			buyerCode = String.valueOf((int)Double.parseDouble(buyerCode));
		}catch(Exception e){
			logger.warn("Given buyer code is not integer: "+buyerCode);
		}
		return buyerCode;
	}
	
}

