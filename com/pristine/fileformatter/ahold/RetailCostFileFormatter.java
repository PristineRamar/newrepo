package com.pristine.fileformatter.ahold;

/**
 * @author Pradeep
 * File formatter for cost loader.
 */

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.CostDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.vendor.VendorDAO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

public class RetailCostFileFormatter extends PristineFileParser{
	private static Logger logger = Logger.getLogger("RetailPriceFileFormatter");
	
	private String inputFilePath;
	private String outputFilePath;
	
	private static int columnCount = 8;
	private int recordCount = 0;
	private int skipRecordCount = 0;
	private String chainId;
	private Connection conn = null;
	private boolean isFirstRecordInFile = false;
	List<RetailCostDTO> skippedList = null;
	private HashMap<String, HashMap<String, RetailPriceDTO>> salePriceMap = null;
	
	private FileWriter fw = null;
	private PrintWriter pw = null;
	Set<String> distinctStoreSet = new HashSet<String>();
	private static String LOAD_FUTURE_PRICE = "LOAD_FUTURE_PRICE=";
	private boolean loadFuturePrice = true;
	private static String SETUP_STORE_ITEM_MAP = "SETUP_STORE_ITEM_MAP=";
	private boolean setupStoreItemMap = false;
	
	// Changes to debug future price eff day
	Set<String> sundaySet = new HashSet<String>();
	Set<String> mondaySet = new HashSet<String>();
	Set<String> tuesdaySet = new HashSet<String>();
	Set<String> wednesdaySet = new HashSet<String>();
	Set<String> thursdaySet = new HashSet<String>();
	Set<String> fridaySet = new HashSet<String>();
	Set<String> saturdaySet = new HashSet<String>();
	int sunday = 0;
	int monday = 0;
	int tuesday = 0;
	int wednesday = 0;
	int thursday = 0;
	int friday = 0;
	int saturday = 0;
	// Changes to debug future price eff day Ends
	
	
	public RetailCostFileFormatter(String inputPath, String outputPath, boolean loadFuturePrice, boolean setupStoreItemMap){
		this.inputFilePath = inputPath;
		this.outputFilePath = outputPath;
		this.conn = getOracleConnection();
		this.loadFuturePrice = loadFuturePrice;
		this.setupStoreItemMap = setupStoreItemMap;
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-retail-cost-file-formatter.properties");
		PropertyManager.initialize("analysis.properties");
		
		String inputFilePath = args[0];
		String outputFilePath = args[1];
		
		boolean loadFuturePrice = true;
		boolean setupStoreItemMap = false;
		RetailCostFileFormatter fileFormatter = new RetailCostFileFormatter(inputFilePath, outputFilePath, loadFuturePrice, setupStoreItemMap);
		fileFormatter.processFiles();
	}
	
	
	private void processFiles(){
		try
		{
			String separator = PropertyManager.getProperty("DATALOAD.SEPARATOR", "|");
			//getzip files
			ArrayList<String> zipFileList = getZipFiles(inputFilePath);
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			//Start with -1 so that if any regular files are present, they are processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;
			
			String zipFilePath = getRootPath() + "/" + inputFilePath;
			
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;
			
		    	
				try {
					if( processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;
	
					fileList = getFiles(inputFilePath);
	
					for (int i = 0; i < fileList.size(); i++) {
						
						long fileProcessStartTime = System.currentTimeMillis(); 
						
						String files = fileList.get(i);
					    logger.info("File Name - " + fileList.get(i));
					    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
					    
					    String fieldNames[] = new String[11];
					    /*CST-RECORD                  1     98     A				Total Record                           
						CST-PRODUCT-KEY             1     14     A				UPC	
						CST-ITEM-NUMBER             1     10     N				Item Number
						CST-FILLER-1               11      1     A				filler
						CST-IDW-VERSION            12      3     N				IDW Version number
						CST-FILLER-2               15      1     A				filler	
						CST-LOCATION-KEY           16      5     N				Store Number
						CST-FILLER-3               21      1     A				filler
						CST-EFF-STR-DATE           22     10     A  mm/dd/ccyy		Effective Start Date 	
						CST-FILLER-4               32      1     A				filler
						CST-UNIT-VIP-COST          33      9     A  9999.9999		VIP Cost
						CST-FILLER-5               42      1     A				filler
						CST-VEND-NUM               53     10     N				Vendor Number
						CST-FILLER-6               63      1     A				filler
						CST-LIST-COST              64      9     A  9999.9999		List Cost
						CST-FILLER-7               73      1     A				filler	
						CST-VEND-NAME              74     24     A				Vendor Name
						CST-FILLER-6               98      1     A				filler*/
					    fieldNames[0] = "retailerItemCode";
				    	fieldNames[1] = "levelId";
				    	fieldNames[2] = "effListCostDate";
				    	fieldNames[3] = "vipCost";
				    	fieldNames[4] = "avgCost";
				    	fieldNames[5] = "vendorNumber";
				    	fieldNames[6] = "listCost";
				    	fieldNames[7] = "vendorName";
				    	String []fileFields  = mapFileField(fieldNames, columnCount); 
				    	logger.info("Processing Retail Cost Records ...");
				    	createFileWriter(fileList.get(i));
				    	createPrintWriter(fw);
				    	isFirstRecordInFile = true;
				    	salePriceMap = new HashMap<String, HashMap<String,RetailPriceDTO>>();
				    	skippedList = new ArrayList<RetailCostDTO>();
				    	recordCount = 0;
				    	distinctStoreSet = new HashSet<String>();
					    skipRecordCount = 0;
				    	chainId = retailPriceDAO.getChainId(conn);
				    	super.parseDelimitedFile(RetailCostDTO.class, files, separator.charAt(0),fileFields, stopCount);
				    	
				    	logger.info("Processed " + recordCount + " records");
				    	logger.info("Skipped " + skippedList.size() + " records");
				    	logger.info("No. of records skipped based on vendor - " + skipRecordCount);
				    	logger.info("No. of distinct stores - " + distinctStoreSet.size());
				    	StringBuffer stores = new StringBuffer();
					    for(String storeNo : distinctStoreSet){
					    	stores.append(storeNo + ", ");
					    }
					    if(stores.length() > 0)
					    logger.info("Distinct stores : " + stores.toString());
					    close(pw, fw);
					    String line[] = null;
					    CsvReader reader = readFile(files, separator.charAt(0));
					    //reading last record from file.
					    while(reader.readRecord()){
					    	line = reader.getValues();
					    }
					    reader.close();
					    String effectiveFileDate = line[1];
					    logger.info("Saving skipped records...");
					    long savingStartTime = System.currentTimeMillis();
					    RetailCostDAO retailCostDAO = new RetailCostDAO();
					    String file[] = files.split("/");
					    String fileName = file[file.length - 1];
					    retailCostDAO.saveSkippedRecords(conn, skippedList, fileName, effectiveFileDate);
					    long savingEndTime = System.currentTimeMillis();
					    logger.info("Time taken to save skipped records - " + (savingEndTime - savingStartTime) + "ms");
					    long fileProcessEndTime = System.currentTimeMillis();
					    logger.info("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime) + "ms");
					    
					}
				}catch (GeneralException ex) {
				    logger.error("GeneralException", ex);
				    commit = false;
				} catch (Exception ex) {
				    logger.error("JavaException", ex);
				    commit = false;
				}
					
				if( processZipFile){
				    PrestoUtil.deleteFiles(fileList);
				    fileList.clear();
				    fileList.add(zipFileList.get(curZipFileCount));
				}
					
				String archivePath = getRootPath() + "/" + inputFilePath + "/";
				
				if(!setupStoreItemMap){
					if( commit ){
					   	PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
					}
					else{
						PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
					}
				}
				curZipFileCount++;
				processZipFile = true;
			}while (curZipFileCount < zipFileList.size());
		}catch (GeneralException ex) {
	        logger.error("Outer Exception -  GeneralException", ex);
	    }
	    catch (Exception ex) {
	        logger.error("Outer Exception - JavaException", ex);
	    }
		
		super.performCloseOperation(conn, true);
		return;
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

	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<RetailCostDTO> retailCostList = (List<RetailCostDTO>) listobj;
		
		ItemDAO item = new ItemDAO();
		VendorDAO vendorDAO = new VendorDAO(conn);
		String ignVendor = PropertyManager
				.getProperty("COST_IGNORE_VENDOR_ID");

		String[] vendorArr = ignVendor.split(",");
		List<String> vendorsToBeIgnored = new ArrayList<String>();
		for(String vendorId : vendorArr){
			vendorsToBeIgnored.add(vendorId);
		}
		Set<String> retItemCodes = new HashSet<String>();
		Set<String> vendorCodeSet = new HashSet<String>();
		
		HashMap<String, List<String>> retItemCodeAndUpcMap = new HashMap<String, List<String>>();
		HashMap<String, Long> vendorMap = new HashMap<String, Long>();
		logger.debug("Creating retailer item code and UPC map...");
		long startTime = System.currentTimeMillis();
		for(RetailCostDTO retailCostDTO : retailCostList){
			String levelId = retailCostDTO.getLevelId().substring(1); 
			retailCostDTO.setLevelId(levelId);
			retailCostDTO.setLevelTypeId(2);
			String retailerItemCode = Integer.toString(Integer.parseInt(retailCostDTO.getRetailerItemCode().split("_")[0]));
			retailCostDTO.setRetailerItemCode(retailerItemCode);
			if(!retItemCodes.contains(retailerItemCode))
			retItemCodes.add(retailerItemCode);
			if(!vendorCodeSet.contains(retailCostDTO.getVendorNumber()))
				vendorCodeSet.add(retailCostDTO.getVendorNumber());
			if(!distinctStoreSet.contains(levelId))
				distinctStoreSet.add(levelId);
			if(retItemCodes.size() % 1000 == 0){
				retItemCodeAndUpcMap.putAll(item.getUPCListForRetItemcodes(conn, retItemCodes));
				vendorMap.putAll(vendorDAO.getVendorIdMap(vendorCodeSet));
				vendorCodeSet = new HashSet<String>();
				retItemCodes = new HashSet<String>();
			}
			}
		if(retItemCodes.size() > 0){
			retItemCodeAndUpcMap.putAll(item.getUPCListForRetItemcodes(conn, retItemCodes));
			retItemCodes = new HashSet<String>();
		}
		if(vendorCodeSet.size() > 0){
			vendorMap.putAll(vendorDAO.getVendorIdMap(vendorCodeSet));
			vendorCodeSet = new HashSet<String>();
		}
		long endTime = System.currentTimeMillis();
		logger.debug("Time taken to map retailer item code and UPC - " + (endTime - startTime) + "ms");
		
		List<RetailCostDTO> tempList = new ArrayList<RetailCostDTO>();
		for(RetailCostDTO retailCostDTO : retailCostList){
			recordCount++;
			if(vendorMap.get(retailCostDTO.getVendorNumber()) != null){
				if(vendorsToBeIgnored.contains(Long.toString(vendorMap.get(retailCostDTO.getVendorNumber())))){	
					skipRecordCount++;
					continue;
				}
			}
			List<String> upcList = retItemCodeAndUpcMap.get(retailCostDTO.getRetailerItemCode());
			if(upcList != null && !upcList.isEmpty()){
				//Handling multiple upcs for one retailer item code.
				if(upcList.size() > 1){
					//logger.info("multiple upcs found for item number : " + retailCostDTO.getRetailerItemCode());
					for(String upc:upcList){
						RetailCostDTO tempRetailCostDTO = new RetailCostDTO();
						tempRetailCostDTO.setAvgCost(retailCostDTO.getAvgCost());
						tempRetailCostDTO.setUpc(upc);
						tempRetailCostDTO.setEffListCostDate(retailCostDTO.getEffListCostDate());
						tempRetailCostDTO.setLevelId(retailCostDTO.getLevelId());
						tempRetailCostDTO.setLevelTypeId(retailCostDTO.getLevelTypeId());
						tempRetailCostDTO.setRetailerItemCode(retailCostDTO.getRetailerItemCode());
						tempRetailCostDTO.setVendorName(retailCostDTO.getVendorName());
						tempRetailCostDTO.setVendorNumber(retailCostDTO.getVendorNumber());
						tempRetailCostDTO.setVipCost(retailCostDTO.getVipCost());
						tempRetailCostDTO.setListCost(retailCostDTO.getListCost());
						tempList.add(tempRetailCostDTO);
					}
				}
				else{
					retailCostDTO.setUpc(upcList.get(0));
				}
			}
			else{
				skippedList.add(retailCostDTO);
			}
			
			if(recordCount % 25000 == 0){
				logger.info("Processed " + recordCount + " records");
				}
			}

		retailCostList.addAll(new ArrayList<RetailCostDTO>(tempList));
		writeToFile(pw, retailCostList);
		retItemCodeAndUpcMap = new HashMap<String, List<String>>();
	}
	
	private void createFileWriter(String fileName){
		try{
			String fileNameSplit[] = fileName.split("/");
			String inputFileName = fileNameSplit[fileNameSplit.length - 1];
			String indvNameSplit[] = inputFileName.split("_");
			//String indvName = indvNameSplit[0] + "_" + indvNameSplit[1];
			String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
			String outputPath = rootPath + "/" + outputFilePath
					+ "/Cost_" + inputFileName;
			File file = new File(outputPath);
			if (!file.exists())
				fw = new FileWriter(outputPath);
			else
				fw = new FileWriter(outputPath, true);
		}catch(IOException exception){
			logger.error("Exception when creating RetailPriceFile" + exception);
		}
	}
	
	private void createPrintWriter(FileWriter fw){
		pw = new PrintWriter(fw);
	}
	
	public void setFileWriter(FileWriter fw){
		this.fw = fw;
	}
	
	public void setPrintWriter(PrintWriter pw){
		this.pw = pw;
	}
	
	public void writeToFile(PrintWriter pw, List<RetailCostDTO> retailCostList) throws GeneralException{
		/*1	upc	numeric(11)		No check digit
		2	item code	char		
		3	Level	numeric (1)	"1 - Zone Level
		2 - Store level"	Indicates whether the price is for a zone or store specific
		4	Zone#/Store#	char		
		5	list cost effective date	char (8)	MM/DD/YY	
		6	list cost	numeric (5.2)	99999.99	At Item Level (scan level)
		7	deal cost start date	char (8)	MM/DD/YY	
		8	deal cost End date	char (8)	MM/DD/YY	
		9	deal Cost	numeric (5.2)	99999.99*/	
		for(RetailCostDTO retailCostDTO : retailCostList){
			if(retailCostDTO.getUpc() != null && !retailCostDTO.getUpc().equals("")){
					pw.print(retailCostDTO.getUpc()); // UPC
					pw.print("|");
					pw.print(retailCostDTO.getRetailerItemCode()); // item code
					pw.print("|");
					pw.print(retailCostDTO.getLevelTypeId());        //Zone#/Store#
					pw.print("|");
					pw.print(retailCostDTO.getLevelId()); // Level
					pw.print("|");
					pw.print(retailCostDTO.getEffListCostDate()); // Effective cost date
					pw.print("|");
					pw.print(retailCostDTO.getListCost()); //List cost
					pw.print("|");
					pw.print("");//deal cost start date 
					pw.print("|");
					pw.print("");//deal cost end date
					pw.print("|");
					pw.print("");//deal cost
					pw.print("|");
					pw.print(retailCostDTO.getVipCost());//Vip cost
					pw.print("|");
					pw.print(retailCostDTO.getAvgCost());//Avg cost
					pw.print("|");
					pw.print(retailCostDTO.getVendorName());//Vendor Name
					pw.print("|");
					pw.print(retailCostDTO.getVendorNumber());//Avg cost
					pw.print("|");
					pw.println("");
			}
		}
		}
	
	private String printSet(Set<String> storeSet){
		int count = 0;
		StringBuffer storeString = new StringBuffer("");
		for(String store : storeSet){
			if(count == 0){
				storeString.append(store);
				count++;
			}else{
				storeString.append(",");
				storeString.append(store);
			}
		}
		return storeString.toString();
	}

	public void close(PrintWriter pw, FileWriter fw){
		try{
			pw.flush();
			pw.close();
			fw.close();
		}catch(IOException exception){
			logger.error("Error while closing writer - " + exception.toString());
		}
	}
}
