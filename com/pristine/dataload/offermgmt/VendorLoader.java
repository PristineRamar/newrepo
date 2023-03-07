package com.pristine.dataload.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.vendor.*;
import com.pristine.dto.offermgmt.VendorFileDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class VendorLoader {
	private static Logger  logger = Logger.getLogger("VendorLoader");
	private Connection 	_Conn = null;
	private String userName = "BATCH";
	
	private VendorDAO vendorDAO = null;
	
	/**
	 * Constructor
	 */
	public VendorLoader ()
	{
        try
		{
        	_Conn = DBManager.getConnection();   
	    }catch (GeneralException gex) {
	        logger.error("Error when creating connection - " + gex);
	    }
        
        vendorDAO = new VendorDAO(_Conn);
        
	}
	
	/**
	 * Main method
	 * @param args[0] inputFilePath
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		PropertyConfigurator.configure("log4j-vendor-loader.properties");
		PropertyManager.initialize("analysis.properties");
        
		String relativePath = null;
		if(args.length > 0){
			relativePath = args[0];
		}else{
			logger.error("Invalid input - Input File Path is mandatory");
		}
		
		VendorLoader vendorLoader = new VendorLoader();
		
		vendorLoader.loadVendorData(relativePath);
	}
	
	public void loadVendorData(String relativePath){
		ExcelFileParser parser = new ExcelFileParser();
		parser.setFirstRowToProcess(1); //skip header row
		ArrayList<String> fileList = parser.getFiles(relativePath);
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		mapFieldNames(fieldNames);
		
		try{
			for(String fileName : fileList){
				logger.info("Processing file - " + fileName);
				List<VendorFileDTO> rawList = parser.parseExcelFile(VendorFileDTO.class, fileName, 0, fieldNames);
				processVendorData(rawList);
				PristineDBUtil.commitTransaction(_Conn, "Vendor data loaded for file: " + fileName);
			}
		}catch(GeneralException ge){
			logger.error("Error when parsing or processing input file" , ge);
		}finally{
			PristineDBUtil.close(_Conn);
		}
	}
	
	private void mapFieldNames(TreeMap<Integer, String> fieldNames){
		fieldNames.put(0, "upc");
		fieldNames.put(1, "itemCode");
		fieldNames.put(2, "description");
		fieldNames.put(3, "size");
		fieldNames.put(4, "uom");
		fieldNames.put(5, "store");
		fieldNames.put(6, "vendorCode");
		fieldNames.put(7, "vendorName");
	}
	
	
	
	private void processVendorData(List<VendorFileDTO> rawList){
		int noOfProcessedRecords = 0;
		int noOfIgnoredRecords = 0;	 	
		for(VendorFileDTO v : rawList){
			if (processVendorRow(v))
				noOfProcessedRecords ++;
			else
				noOfIgnoredRecords ++;
		}
		logger.info("No of processed records: " + noOfProcessedRecords);
		logger.info("No of ignored records: " + noOfIgnoredRecords);
	}
	
	private boolean processVendorRow(VendorFileDTO v){
		boolean status = false;
		
		long vendorId = vendorDAO.getVendorId(v.getVendorCode());
		if (vendorId ==0) //if vendor is not present insert
		{
			vendorDAO.insertVendor(v);
			vendorId = vendorDAO.getVendorId(v.getVendorCode());
		}
		
		long compStoreId = vendorDAO.getCompStoreId(v.getStore(), 74);
		long itemCode = vendorDAO.getItemCode(v.getItemCode(), v.getUpc());
		
		if(compStoreId > 0 && itemCode > 0)
		{
			vendorDAO.updateStoreItemMap(vendorId, compStoreId, itemCode);
			status = true;
		}
		else
		{
			logger.info("Ignored: No matching record with compStoreId, itemCode: " + v.toString());
		}
		return status;
	}
	
	
}
