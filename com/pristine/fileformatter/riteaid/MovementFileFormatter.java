package com.pristine.fileformatter.riteaid;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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

public class MovementFileFormatter extends PristineFileParser{
	
	private static Logger logger = Logger.getLogger("RiteAidMovementFileFormatter");
	
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	
	private int recordCount = 0;
	private int skippedCount = 0;
	
	private static String relativeInputPath, relativeOutputPath;
	private String rootPath;
	
	private List<String> _StoreList = new ArrayList<String>();
	private List<String> _UpcList = new ArrayList<String>();
	private HashMap<String, String> _UPCExclude = new HashMap<String, String>();

	private FileWriter fw = null;
	private PrintWriter pw = null;
	
	public MovementFileFormatter(String storeFromConfig){
		
		if (storeFromConfig.equalsIgnoreCase("Y")){
			logger.debug("Load Processing Stores from configuration");
			String store = PropertyManager.getProperty("MOVEMENTFORMAT_STORES");
			String[] storeArr = store.split(",");
			for (int i = 0; i < storeArr.length; i++) {
				_StoreList.add(storeArr[i]);
				logger.debug("Store " + storeArr[i]);
			}
		}
		
		//Load list of UPC which is meant for exclution from processing
		String upc = PropertyManager.getProperty("MOVEMENTLOAD.UPC_EXCLUDE");

		logger.info("Load UPCs from configuration to exclude" + upc);
		
		String[] upcArr = upc.split(",");
		
		for (int i = 0; i < upcArr.length; i++) {
			_UpcList.add(upcArr[i]);
			_UPCExclude.put(upcArr[i], upcArr[i]);
			logger.debug("UPC " + upcArr[i]);
		}
	}
	
	public static void main(String[] args) throws GeneralException, IOException {
		PropertyConfigurator.configure("log4j-movement-file-formatter.properties");
		logger.info("*** Movement File Formatter for Rite-Aid Process Begins ***");
		logCommand(args);
		String storeFromConfig = "N";
		//Get the command line input parameteres
		for (int ii = 0; ii < args.length; ii++) {
			
			String arg = args[ii]; 
			
			if (arg.startsWith("INPUTFOLDER")) {
				relativeInputPath = arg.substring("INPUTFOLDER=".length());
				logger.info("Relative input file path....." + relativeInputPath);
			}

			if (arg.startsWith("OUTPUTFOLDER")) {
				relativeOutputPath = arg.substring("OUTPUTFOLDER=".length());
				logger.info("Relative output file path...." + relativeOutputPath);	
			}
			
			if (arg.startsWith("STORECONFIG")) {
				storeFromConfig = arg.substring("STORECONFIG=".length());
			}
		}
		
		MovementFileFormatter fileFormatter = new MovementFileFormatter(storeFromConfig);
		fileFormatter.processFile();
	}
	
	private void processFile() throws GeneralException, IOException {

		super.headerPresent = true;
		
		//Get the based path for input file
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		logger.debug("Input file base path..." + rootPath);

		//Create mapping for input fields
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			//logger.debug("Key..." + i + " Value..." + fieldNames[i]);
			i++;
		}

		logger.info("Start Data formatting...");
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
					if(processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;
	
					fileList = getFiles(relativeInputPath);
					logger.info("Total files to format..." + fileList.size());
					
				    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));					
					
					for (int i = 0; i < fileList.size(); i++) {
						
						long fileProcessStartTime = System.currentTimeMillis(); 
						
						//Initialize
						recordCount = 0;
						skippedCount = 0;
						
						String files = fileList.get(i);
					    logger.info("Processing input file..." + files);
				    	
					    String outputFileName[] = files.split("/");
					    
					    String outputPath = rootPath + "/" + relativeOutputPath + "/" + outputFileName[outputFileName.length - 1];
					    
					    File file = new File(outputPath);
						
					    //Check the file existance
					    if (!file.exists())
							fw = new FileWriter(outputPath);
						else
							fw = new FileWriter(outputPath, true);
									    
						pw = new PrintWriter(fw);
						
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
		
		DecimalFormat twoDForm = new DecimalFormat("#.00");
		
		for (int j = 0; j < movementDTOList.size(); j++) {
			MovementDTO movementDTO = movementDTOList.get(j);

			recordCount++;
			
			//logger.debug("Processing record count..." + recordCount);
			
			if (_UPCExclude.containsKey(movementDTO.getItemUPC())){
				skippedCount++;
				continue;
			}
			
			if(_StoreList.size() > 0 && !_StoreList.contains(PrestoUtil.castStoreNumber(movementDTO.getItemStore()))){
				skippedCount++;
				continue;
			}
			
			String date = movementDTO.getDate();
			String time = String.format("%4s", movementDTO.getTime()).replace(' ', '0');
			
			String itemDateTime = null;
			
			try{
				Date tempDate = new SimpleDateFormat("MM/dd/yyyy").parse(date);
				itemDateTime = DateUtil.dateToString(tempDate, "yyyyMMdd") + time;
			}catch(Exception e){
				logger.error(e.getMessage());
			}
			
			pw.print("2");	//Record type, 2: detailed record
			pw.print("|");
			pw.print(""); //Chain Id
			pw.print("|");
			//pw.print(String.format("%5s", movementDTO.getItemStore()).replace(' ', '0')); // Store #
			pw.print(movementDTO.getItemStore()); // Store #
			pw.print("|");
			pw.print(""); // Terminal #
			pw.print("|");
			pw.print(movementDTO.getTransactionType()); //Transaction Type
			pw.print("|");
			pw.print(movementDTO.getItemTransum()); // Transaction #
			pw.print("|");
			pw.print(itemDateTime); //Item Date Time
			pw.print("|");
			pw.print(movementDTO.getItemUPC()); // Item UPC
			pw.print("|");
			String unitSalePrice =  "0";
			String unitRegPrice = "0";
			if (movementDTO.getExtnQty() !=0){
				unitSalePrice =  twoDForm.format(movementDTO.getExtendedNetPrice() / movementDTO.getExtnQty());
				unitRegPrice = twoDForm.format(movementDTO.getExtendedGrossPrice() / movementDTO.getExtnQty());
			}
			pw.print(unitSalePrice);
			pw.print("|");
			
			pw.print(unitRegPrice);
			pw.print("|");
			
			int quantity = 0;

			if (movementDTO.getExtnQty() != 0)
				quantity = (int) movementDTO.getExtnQty();
			
			pw.print(quantity); // Extn Qty
			pw.print("|");
			pw.print("");						//Weight
			pw.print("|");
			pw.print(movementDTO.getExtendedGrossPrice()); // Extended Gross Price
			pw.print("|");
			pw.print(movementDTO.getAdDiscount()); // Ad discount
			pw.print("|");
			pw.print(movementDTO.getCardDiscount()); // Card discount
			pw.print("|");
			pw.print(movementDTO.getOtherDiscount()); // Other discount
			pw.print("|");
			pw.print("|");
			pw.print(movementDTO.getCustomerId()); // Customer Id
			pw.print("|");
			pw.print(""); 		//Store coupon used
			pw.print("|");
			pw.print(""); 		//Manufacturer coupon used
			pw.print("|");
			pw.print(movementDTO.getAdEvent()); // POS Department
			pw.print("|");
			pw.print(""); // Previous Coupon Family Code
			pw.print("|");
			pw.print(movementDTO.getAdPageId()); // Current Coupon Family Code
			pw.println(""); // spaces
	
			if(recordCount % Constants.LOG_RECORD_COUNT == 0){
				logger.info("No of records processed - " + recordCount);
			}
		}
	}

	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns() {
		allColumns.put(1, "customerId");
		allColumns.put(2, "itemTransum");
		allColumns.put(3, "date");
		allColumns.put(4, "time");
		allColumns.put(5, "itemStore");	
		allColumns.put(6, "transactionType");
		allColumns.put(7, "itemUPC");
		allColumns.put(8, "uncode");
		allColumns.put(9, "extendedNetPrice");
		allColumns.put(10, "cardDiscount");
		allColumns.put(11, "adDiscount");
		allColumns.put(12, "otherDiscount");
		allColumns.put(13, "extendedGrossPrice");
		allColumns.put(14, "extnQty");
		allColumns.put(15, "cardNumber");
		allColumns.put(16, "isRxBasket");
		allColumns.put(17, "adEvent");
		allColumns.put(18, "adVersion");
		allColumns.put(19, "adPageId");
	}
	
	/**
	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */
	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Command: SummaryDailyV2 ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info(sb.toString());
	}
}