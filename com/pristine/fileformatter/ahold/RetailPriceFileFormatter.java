package com.pristine.fileformatter.ahold;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DepartmentDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

public class RetailPriceFileFormatter extends PristineFileParser{
	private static Logger logger = Logger.getLogger("RetailPriceFileFormatter");
	
	private String inputFilePath;
	private String outputFilePath;
	
	private static int columnCount = 11;
	private int recordCount = 0;
	
	private Connection conn = null;
	private boolean isDeptIdSet = false;
	
	private HashMap<String, HashMap<String, RetailPriceDTO>> salePriceMap = null;
	
	private FileWriter fw = null;
	private PrintWriter pw = null;
	
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
	
	
	public RetailPriceFileFormatter(String inputPath, String outputPath, boolean loadFuturePrice, boolean setupStoreItemMap){
		this.inputFilePath = inputPath;
		this.outputFilePath = outputPath;
		this.conn = getOracleConnection();
		this.loadFuturePrice = loadFuturePrice;
		this.setupStoreItemMap = setupStoreItemMap;
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-retail-price-file-formatter.properties");
		PropertyManager.initialize("analysis.properties");
		
		String inputFilePath = args[0];
		String outputFilePath = args[1];
		
		boolean loadFuturePrice = true;
		boolean setupStoreItemMap = false;
		
		// Changes to handle future price
		for(String arg : args){
			if(arg.indexOf(LOAD_FUTURE_PRICE) != -1){
				loadFuturePrice = Boolean.valueOf(arg.substring(LOAD_FUTURE_PRICE.length()));
			}
			
			// Changes to not move input files into completed folder when store item map setup runs
			if(arg.indexOf(SETUP_STORE_ITEM_MAP) != -1){
				setupStoreItemMap = Boolean.valueOf(arg.substring(SETUP_STORE_ITEM_MAP.length()));
			}
		}
		// Changes to handle future price ends
				
		RetailPriceFileFormatter fileFormatter = new RetailPriceFileFormatter(inputFilePath, outputFilePath, loadFuturePrice, setupStoreItemMap);
		fileFormatter.processFiles();
	}
	
	
	private void processFiles(){
		try
		{
			headerPresent = true;
			String separator = PropertyManager.getProperty("DATALOAD.SEPARATOR", "|");
			//getzip files
			ArrayList<String> zipFileList = getZipFiles(inputFilePath);
			
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
		
				    	fieldNames[0] = "upc";
				    	fieldNames[1] = "retailerItemCode";
				    	fieldNames[2] = "levelTypeId";
				    	fieldNames[3] = "levelId";
				    	fieldNames[4] = "prePrice";
				    	fieldNames[5] = "regEffectiveDate";
				    	fieldNames[6] = "regPrice";
				    	fieldNames[7] = "regQty";
				    	fieldNames[8] = "futureRegEffDate";
				    	fieldNames[9] = "futureRegPrice";
				    	fieldNames[10] = "futureRegQty";
				    	
				    			    	
				    	String []fileFields  = mapFileField(fieldNames, columnCount); 
				    	
				    	logger.info("Processing Retail Price Records ...");
				    	createFileWriter(fileList.get(i));
				    	createPrintWriter(fw);
				    	isDeptIdSet = false;
				    	salePriceMap = new HashMap<String, HashMap<String,RetailPriceDTO>>();
				    	recordCount = 0;
				    	super.parseDelimitedFile(RetailPriceDTO.class, files, separator.charAt(0),fileFields, stopCount);
				    	logger.info("Processed " + recordCount + " records");
					    close(pw, fw);
					    
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
		
		logger.debug("No of future Price eff date as Sunday - " + sunday);
		logger.debug("Stores with Price eff date as Sunday - " + printSet(sundaySet));
		logger.debug("No of future Price eff date as Monday - " + monday);
		logger.debug("Stores with Price eff date as Monday - " + printSet(mondaySet));
		logger.debug("No of future Price eff date as Tuesday - " + tuesday);
		logger.debug("Stores with Price eff date as Tuesday - " + printSet(tuesdaySet));
		logger.debug("No of future Price eff date as Wednesday - " + wednesday);
		logger.debug("Stores with Price eff date as Wednesday - " + printSet(wednesdaySet));
		logger.debug("No of future Price eff date as Thursday - " + thursday);
		logger.debug("Stores with Price eff date as Thursday - " + printSet(thursdaySet));
		logger.debug("No of future Price eff date as Friday - " + friday);
		logger.debug("Stores with Price eff date as Friday - " + printSet(fridaySet));
		logger.debug("No of future Price eff date as Saturday - " + saturday);
		logger.debug("Stores with Price eff date as Saturday - " + printSet(saturdaySet));
		logger.info("Retail Price File Formatting Complete");
		
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
		List<RetailPriceDTO> retailPriceList = (List<RetailPriceDTO>) listobj;
		
		if(!isDeptIdSet){
			Set<String> noDeptUpc = new HashSet<String>();
			for(RetailPriceDTO retailPriceDTO : retailPriceList){
				String upc = PrestoUtil.castUPC(retailPriceDTO.getUpc(), false);
				if(!noDeptUpc.contains(upc)){
					retailPriceDTO.setLevelId(PrestoUtil.castStoreNumber(retailPriceDTO.getLevelId()));	
					DepartmentDAO deptDAO = new DepartmentDAO();
					RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
					int deptProductId = deptDAO.getDepartmentProductId(conn, upc);
					logger.debug("UPC - " + upc + " Department Product Id - " + deptProductId);
					if(deptProductId > 0){
						salePriceMap = retailPriceDAO.getRetailSalePrice(conn, deptProductId);
						isDeptIdSet = true;
						break;
					}else
						noDeptUpc.add(upc);
				}
			}
		}
	
		for(RetailPriceDTO retailPriceDTO : retailPriceList){
			String upc = PrestoUtil.castUPC(retailPriceDTO.getUpc(), false);
			if(salePriceMap != null){
				HashMap<String, RetailPriceDTO> storePriceMap = salePriceMap.get(upc);
				if(storePriceMap != null){
					RetailPriceDTO salePriceDTO = storePriceMap.get(PrestoUtil.castStoreNumber(retailPriceDTO.getLevelId()));
					if(salePriceDTO != null){
						retailPriceDTO.setSalePrice(salePriceDTO.getSalePrice());
						retailPriceDTO.setSaleQty(salePriceDTO.getSaleQty());
						retailPriceDTO.setSaleStartDate(salePriceDTO.getSaleStartDate());
						retailPriceDTO.setSaleEndDate(salePriceDTO.getSaleEndDate());
					}
				}
			}
			writeToFile(pw, retailPriceDTO);
			recordCount++;
			if(recordCount % Constants.LOG_RECORD_COUNT == 0){
				logger.info("Processed " + recordCount + " records");
			}
		}
	}
	
	private void createFileWriter(String fileName){
		try{
			String fileNameSplit[] = fileName.split("/");
			String inputFileName = fileNameSplit[fileNameSplit.length - 1];
			String indvNameSplit[] = inputFileName.split("_");
			String indvName = indvNameSplit[indvNameSplit.length - 1];
			String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
			String outputPath = rootPath + "/" + outputFilePath
					+ "/Retail_Price_" + indvName;
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
	
	public void writeToFile(PrintWriter pw, RetailPriceDTO retailPriceDTO) throws GeneralException{
		try{
			if(retailPriceDTO.getRegEffectiveDate().contains(".")){
				DateUtil.toDate(retailPriceDTO.getRegEffectiveDate(), "yyyy.MM.dd");
			}else{
				DateUtil.toDate(retailPriceDTO.getRegEffectiveDate(), "MM/dd/yyyy");
			}
		}catch(GeneralException parseException){
			return;
		}
		
		pw.print(retailPriceDTO.getUpc()); // UPC
		pw.print("|");
		pw.print(retailPriceDTO.getRetailerItemCode()); // item code
		pw.print("|");
		pw.print(retailPriceDTO.getLevelTypeId());        //Zone#/Store#
		pw.print("|");
		pw.print(retailPriceDTO.getLevelId()); // Level
		pw.print("|");
		if(retailPriceDTO.getRegEffectiveDate().contains(".")){
			String regEffectiveDate = DateUtil.dateToString(DateUtil.toDate(retailPriceDTO.getRegEffectiveDate(), "yyyy.MM.dd"), "MM/dd/yyyy");
			pw.print(regEffectiveDate);
		}else{
			pw.print(retailPriceDTO.getRegEffectiveDate());      //Regular retail effective date
		}
		pw.print("|");
		pw.print(retailPriceDTO.getRegPrice());         //Regular retail
		pw.print("|");
		pw.print(retailPriceDTO.getRegQty());          //Regular retail qty
		pw.print("|");
		if(retailPriceDTO.getSaleStartDate() == null)
			pw.print("");   //Sale retail Start date
		else
			pw.print(retailPriceDTO.getSaleStartDate());   //Sale retail Start date
		pw.print("|");
		if(retailPriceDTO.getSaleEndDate() == null)
			pw.print("");     //Sale retail End date
		else
			pw.print(retailPriceDTO.getSaleEndDate());     //Sale retail End date
		pw.print("|");
		pw.print(retailPriceDTO.getSalePrice());            //Sale retail
		pw.print("|");
		pw.println(retailPriceDTO.getSaleQty());       //Sale retail qty
		
		// Changes to handle future price
		if(loadFuturePrice && retailPriceDTO.getFutureRegPrice() > 0 && retailPriceDTO.getFutureRegEffDate() != null){
			pw.print(retailPriceDTO.getUpc()); // UPC
			pw.print("|");
			pw.print(""); // item code
			pw.print("|");
			pw.print(retailPriceDTO.getLevelTypeId());        //Zone#/Store#
			pw.print("|");
			pw.print(retailPriceDTO.getLevelId()); // Level
			pw.print("|");
			String regEffectiveDate = null;
			if(retailPriceDTO.getRegEffectiveDate().contains(".")){
				regEffectiveDate = DateUtil.dateToString(DateUtil.toDate(retailPriceDTO.getFutureRegEffDate(), "yyyy.MM.dd"), "MM/dd/yyyy");
				pw.print(regEffectiveDate);
			}else{
				regEffectiveDate = retailPriceDTO.getFutureRegEffDate();
				pw.print(retailPriceDTO.getFutureRegEffDate());      //Regular retail effective date
			}
			// Changes to debug future price eff day
			int dayOfWeek = DateUtil.getdayofWeek(regEffectiveDate, "MM/dd/yyyy");
			populateDayOfWeek(dayOfWeek, retailPriceDTO.getLevelId(), retailPriceDTO.getUpc());
			// Changes to debug future price eff day Ends
			pw.print("|");
			pw.print(retailPriceDTO.getFutureRegPrice());         //Regular retail
			pw.print("|");
			pw.print(retailPriceDTO.getRegQty());          //Regular retail qty
			pw.print("|");
			pw.print("");								//Sale retail Start date
			pw.print("|");
			pw.print("");     						//Sale retail End date
			pw.print("|");
			pw.print("");            				//Sale retail
			pw.print("|");
			pw.println("");       					//Sale retail qty
		}
		// Changes to handle future price Ends
	}
	
	private void populateDayOfWeek(int dayOfWeek, String levelId, String upc) {
		switch(dayOfWeek){
			case 1:
				sunday++;
				sundaySet.add(levelId);
				break;
			case 2:
				monday++;
				mondaySet.add(levelId);
				break;
			case 3:
				tuesday++;
				tuesdaySet.add(levelId);
				break;
			case 4:
				wednesday++;
				wednesdaySet.add(levelId);
				break;
			case 5:
				thursday++;
				thursdaySet.add(levelId);
				break;
			case 6:
				friday++;
				fridaySet.add(levelId);
				break;
			case 7:
				saturday++;
				saturdaySet.add(levelId);
				break;
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
