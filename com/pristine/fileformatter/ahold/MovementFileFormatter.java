package com.pristine.fileformatter.ahold;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dto.MovementDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;
//import com.steadystate.css.parser.ParseException;

public class MovementFileFormatter extends PristineFileParser{
	
	private static Logger logger = Logger.getLogger("AholdMovementFileFormatter");
	
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	
	private int recordCount = 0;
	private int skippedCount = 0;
	
	private static String relativeInputPath, relativeOutputPath;
	private String rootPath;
	
	private FileWriter fw = null;
	private PrintWriter pw = null;
	
	private List<String> _StoreList = new ArrayList<String>();
	private List<String> _UpcList = new ArrayList<String>();
	
	public MovementFileFormatter(String storeFromConfig, String upcFromConfig){
		if (storeFromConfig.equalsIgnoreCase("Y")){
			logger.debug("Load Processing Stores from configuration");
			String store = PropertyManager.getProperty("MOVEMENTFORMAT_STORES");
			String[] storeArr = store.split(",");
			for (int i = 0; i < storeArr.length; i++) {
				_StoreList.add(storeArr[i]);
				logger.debug("Store " + storeArr[i]);
			}
		}
		if (upcFromConfig.equalsIgnoreCase("Y")){
			logger.debug("Load Processing UPCs from configuration");
			String upc = PropertyManager.getProperty("MOVEMENTFORMAT_UPCS");
			String[] upcArr = upc.split(",");
			for (int i = 0; i < upcArr.length; i++) {
				_UpcList.add(upcArr[i]);
				logger.debug("UPC " + upcArr[i]);
			}
		}
	}
	
	/**
	 * @param args
	 * @throws GeneralException
	 * @throws IOException
	 */
	public static void main(String[] args) throws GeneralException, IOException {
		// TODO Auto-generated method stub
		String fileFullPath;
		
		String storeFromConfig = "N";
		String upcFromConfig = "N";
		
		PropertyConfigurator.configure("log4j-movement-file-formatter.properties");
	
		if (args.length < 2) {
			logger.info("Invalid Arguments,  args[0] - TLog File Input Path, args[1] - TLog File Output Path");
			System.exit(-1);
		}
		
		relativeInputPath = args[0];
		relativeOutputPath = args[1];
		
		for(String arg : args){
			if (arg.startsWith("STORECONFIG")) {
				storeFromConfig = arg.substring("STORECONFIG=".length());
			}
			if (arg.startsWith("UPCCONFIG")) {
				upcFromConfig = arg.substring("UPCCONFIG=".length());
			}
		}
		MovementFileFormatter fileFormatter = new MovementFileFormatter(storeFromConfig, upcFromConfig);
		fileFormatter.processFile();
	}
	
	/**
	 * Parse file and create retail price and retail cost file
	 * @throws GeneralException
	 * @throws IOException
	 */
	private void processFile() throws GeneralException, IOException {

		super.headerPresent = false;
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}

		logger.info("Formatting Tlog file Started... ");
		parseFile(fieldNames);
		logger.info("Total Number of Items " + recordCount);
		logger.info("Formatting Tlog file Completed... ");
	}
	
	private void parseFile(String fieldNames[]) throws GeneralException, IOException {

		try {
			//getzip files
			ArrayList<String> zipFileList = getZipFiles(relativeInputPath);
			
			//Start with -1 so that if any regular files are present, they are processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;
			
			String zipFilePath = getRootPath() + "/" + relativeInputPath;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;
			
		    	
				try {
					if( processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;
	
					fileList = getFiles(relativeInputPath);
	
					for (int i = 0; i < fileList.size(); i++) {
						
						long fileProcessStartTime = System.currentTimeMillis(); 
						
						recordCount = 0;
						skippedCount = 0;
						
						String files = fileList.get(i);
					    logger.info("File Name - " + files);
					    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
				    	
					    String outputFileName[] = files.split("/");
					    logger.info("Output File name - " + outputFileName[outputFileName.length - 1]);
					    
					    String outputPath = rootPath + "/" + relativeOutputPath + "/" + outputFileName[outputFileName.length - 1];
					    
					    File file = new File(outputPath);
						if (!file.exists())
							fw = new FileWriter(outputPath);
						else
							fw = new FileWriter(outputPath, true);
				
						pw = new PrintWriter(fw);
						
				    	logger.info("Processing Retail Records ...");
				    	super.parseDelimitedFile(MovementDTO.class, fileList.get(i), '|', fieldNames, stopCount);
				    	
						logger.info("No of records processed - " + recordCount);
						logger.info("No of records skipped - " + skippedCount);
					    
						pw.flush();
						fw.flush();
						pw.close();
						fw.close();
						
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
			    String archivePath = getRootPath() + "/" + relativeInputPath + "/";
			    
			    if( commit ){
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				}
				else{
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			}while (curZipFileCount < zipFileList.size());
		
		}catch (GeneralException ex) {
	        logger.error("Outer Exception - JavaException", ex);
	        ex.printStackTrace();
	    }catch (Exception ex) {
	        logger.error("Outer Exception - JavaException", ex);
	        ex.printStackTrace();
	    }
	}

	public void processRecords(List listObject) throws GeneralException {
		List<MovementDTO> movementDTOList = (List<MovementDTO>) listObject;
		for (int j = 0; j < movementDTOList.size(); j++) {
			MovementDTO movementDTO = movementDTOList.get(j);
								 					
			recordCount++;							
			boolean processStore = true;
			if ( _StoreList.size() > 0 ) {
				processStore = _StoreList.contains(movementDTO.getItemStore().substring(movementDTO.getItemStore().length() - 4)); 
			}
			if( processStore && _UpcList.size() > 0 ) {
				processStore = _UpcList.contains(movementDTO.getItemUPC().substring(movementDTO.getItemUPC().length() - 12));
			}
			
			if(!processStore){
				skippedCount++;
				continue;
			}
			
			String date = movementDTO.getDate();
			String time = movementDTO.getTime();
			String itemDateTime = null;
			try{
				itemDateTime = DateUtil.dateToString(DateUtil.toDate(date + time, "yyyy-MM-ddHHmmss"), "yyyyMMddHHmmss");
			}catch(Exception exception){
				exception.printStackTrace();
			}
				
			pw.print("0");
			pw.print("|");
			pw.print(movementDTO.getItemStore()); // Store # 1
			pw.print("|");
			pw.print(movementDTO.getItemTerminal()); // Terminal # 2
			pw.print("|");
			pw.print(movementDTO.getItemTransum()); // Transaction # 3
			pw.print("|");
			pw.print(movementDTO.getItemOperator()); //Operator 4
			pw.print("|");
			pw.print(itemDateTime); //Item Date Time 5
			pw.print("|");
			pw.print(movementDTO.getCustomerId()); // Customer Id 6
			pw.print("|");
			pw.print(movementDTO.getItemUPC()); // Item UPC 7 
			pw.print("|");
			pw.print(movementDTO.getItemNetPrice()); // Item Net Price 8
			pw.print("|");
			pw.print(movementDTO.getItemGrossPrice()); // Item Gross Price 9
			pw.print("|");
			pw.print(movementDTO.getPosDepartment()); // POS Department 10
			pw.print("|");
			pw.print(""); // Current Coupon Family Code 11
			pw.print("|");
			pw.print(""); // Previous Coupon Family Code 12
			pw.print("|");
			pw.print(movementDTO.getExtnMultiPriceGrp()); // Multi Price Group 13
			pw.print("|");
			pw.print(movementDTO.getExtnDealQty()); // Deal Qty 14 
			pw.print("|");
			pw.print(movementDTO.getExtnPriceMethod()); // Price Method 15 
			pw.print("|");
			pw.print(movementDTO.getExtnSaleQty()); // Sale Qty 16
			pw.print("|");
			pw.print(""); // Sale Price 17
			pw.print("|");
			pw.print(movementDTO.getExtnQty()); // Extn Qty 18
			pw.print("|");
			pw.print(movementDTO.getExtnWeight()); // Extn Weight 19
			pw.print("|");
			pw.print((movementDTO.getWeightedFlag() == null) ? "" : movementDTO.getWeightedFlag()); // Weighted Flag 20
			pw.print("|");
			pw.print(movementDTO.getWeightedCount()); // Weighted Count 21
			pw.print("|");
			pw.print(""); // Coupon Used 22
			pw.print("|");
			pw.print(movementDTO.getExtendedNetPrice()); // Extn Net Price 23
			pw.print("|");
			pw.print(movementDTO.getExtendedProfit()); // Extn Profit 24
			pw.print("|");
			pw.print(movementDTO.getUnitCost()); // Unit Cost 25
			pw.print("|");
			pw.print(movementDTO.getMovementType()); // Movement Type 26
			pw.print("|");
			pw.print(movementDTO.getDefaultCostUsed()); // Default Cost Used 27 
			pw.print("|");
			pw.print(""); // Percent Used 28
			pw.print("|"); 
			pw.print(""); // Unit Cost Gross 29
			pw.print("|");
			if(movementDTO.getExtnWeight() > 0)
				pw.print((movementDTO.getExtendedGrossPrice() * movementDTO.getExtnWeight())); // Extended Gross Price 30
			else
				pw.print((movementDTO.getExtendedGrossPrice() * movementDTO.getExtnQty())); // Extended Gross Price 30
			pw.print("|");
			pw.print(""); // Count on Deal 31
			pw.print("|");
			pw.print(""); // Misc Fund Amount 32
			pw.print("|");
			pw.print(""); // Misc Fund Count 33
			pw.print("|");
			pw.print(""); // Store Coupon Used 34
			pw.print("|");
			pw.print(""); // Manufacturer Coupon Used 35
			pw.print("|");
			pw.println("       "); // spaces
			
			if(recordCount % Constants.LOG_RECORD_COUNT == 0){
				logger.info("No of records processed - " + recordCount);
			}
		}
	}

	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns() {
		allColumns.put(1, "date");
		allColumns.put(2, "time");
		allColumns.put(3, "itemStore");		
		allColumns.put(4, "customerId");
		allColumns.put(5, "itemUPC");
		allColumns.put(6, "itemNetPrice");
		allColumns.put(7, "movementType");
		allColumns.put(8, "itemTransum");
		allColumns.put(9, "extendedGrossPrice");
		allColumns.put(10, "extnQty");
		allColumns.put(11, "extnWeight");
		allColumns.put(12, "weightedCount");
		allColumns.put(13, "itemOperator");
		allColumns.put(14, "itemTerminal");
		allColumns.put(15, "posDepartment");
		allColumns.put(16, "extnMultiPriceGrp");
		allColumns.put(17, "extnDealQty");
		allColumns.put(18, "extnPriceMethod");
		allColumns.put(19, "extnSaleQty");
		allColumns.put(20, "extendedNetPrice");
		allColumns.put(21, "extendedProfit");
		allColumns.put(22, "unitCost");
		allColumns.put(23, "defaultCostUsed");
	}
}