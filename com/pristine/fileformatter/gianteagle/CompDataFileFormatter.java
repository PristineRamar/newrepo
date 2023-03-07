package com.pristine.fileformatter.gianteagle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ItemDAO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleCompDataDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class CompDataFileFormatter extends PristineFileParser{
	
	private static Logger logger = Logger.getLogger("CompDataFileFormatterGE");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private String rootPath;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private int recordCount = 0;
	private int skippedCount = 0;
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private static String relativeInputPath, relativeOutputPath;
	private Connection conn = null;
	List<String> skippedRetailerItemcodes = null;

	public CompDataFileFormatter()
	{
		conn = getOracleConnection();
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-comp-data-formatter.properties");
		PropertyManager.initialize("analysis.properties");
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				relativeOutputPath = arg.substring(OUTPUT_FOLDER.length());
			}
		}
		CompDataFileFormatter fileFormatter = new CompDataFileFormatter();
		fileFormatter.processFile();
	}
	
	/**
	 * Fill all columns in list and processing the file.
	 */
	private void processFile() {
		skippedRetailerItemcodes = new ArrayList<>();
		super.headerPresent = false;
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}

		logger.info("Formatting Competitive Data file Started... ");
		try {
			parseFile(fieldNames);
			logSkippedRecords();
		} catch (IOException | GeneralException e) {
			logger.error("Error -- processFile()", e);
		}
		logger.info("Total Number of Items " + recordCount);
		logger.info("Formatting Competitive Data file Completed... ");
	}
	
	/**
	 * Get the Path of Input file, and delimiting.
	 * @param fieldNames
	 * @throws GeneralException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private void parseFile(String fieldNames[]) throws GeneralException, IOException {

		try {
			//getzip files
			ArrayList<String> zipFileList = getZipFiles(relativeInputPath);
			
			//Start with -1 so that if any regular files are present, they are processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;
			
			String zipFilePath = getRootPath() + "/" + relativeInputPath;
			int counter = 1;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;
				
		    	
				try {
					if( processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;
	
					fileList = getFiles(relativeInputPath);
	
					for (int i = 0; i < fileList.size(); i++) {
						counter++;
						long fileProcessStartTime = System.currentTimeMillis(); 
						
						recordCount = 0;
						skippedCount = 0;
						
						String files = fileList.get(i);
					    logger.info("File Name - " + files);
					    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
				    	
					    String outputFileName[] = files.split("/");
					    logger.info("Output File name - " + outputFileName[outputFileName.length - 1]);
					    
					    String outputPath = rootPath + "/" + relativeOutputPath + "/" + counter + outputFileName[outputFileName.length - 1];
					    
					    File file = new File(outputPath);
						if (!file.exists())
							fw = new FileWriter(outputPath);
						else
							fw = new FileWriter(outputPath, true);
				
						pw = new PrintWriter(fw);
						
				    	logger.info("Processing Records ...");
				    	super.parseDelimitedFile(GiantEagleCompDataDTO.class, fileList.get(i), '|', fieldNames, stopCount);
				    	
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
	
	
	/**
	 * Fill the values in list
	 */
	private void fillAllColumns() {
		allColumns.put(1, "CMPTR_BNR_CD");
		allColumns.put(2, "CMPTR_ID");
		allColumns.put(3, "RITEM_NO");		
		//allColumns.put(4, "LOAD_WEEK_DTE");
		allColumns.put(4, "PRC_CHK_DTE");
		allColumns.put(6, "CMPTR_CURR_PRC");
		allColumns.put(7, "CMPTR_MUNIT_CNT");
		allColumns.put(8, "SL_PRC_CHK_DTE");
		allColumns.put(9, "CMPTR_SL_PRC");
		allColumns.put(10, "CMPTR_SL_MUNIT_CNT");
		//allColumns.put(11, "ZN_NO");
		//allColumns.put(12, "PRC_GRP_CD");
/*		CMPTR_BNR_CD
		CMPTR_ID
		RITEM_NO
		PRC_CHK_DTE
		CMPTR_CURR_PRC
		CMPTR_MUNIT_CNT
		SL_PRC_CHK_DTE
		CMPTR_SL_PRC
		CMPTR_SL_MUNIT_CNT
*/
	}

	/**
	  * Logs skipped records
	  */
	 private void logSkippedRecords(){
	  if(skippedRetailerItemcodes.size() > 0){
	   StringBuilder sb = new StringBuilder();
	   for(String itemCode: skippedRetailerItemcodes){
	    sb.append(itemCode + ", ");
	   }
	   logger.warn("Skipped retailer item codes -> " + sb.toString());
	  }
	 }
	 
	@Override
	public void processRecords(List listobj) throws GeneralException {
		// TODO Auto-generated method stub
		
		ItemDAO itemDAO = new ItemDAO();
		
		List<GiantEagleCompDataDTO> CompDataList = (List<GiantEagleCompDataDTO>) listobj;
		
		Set<String> retItemcodeSet = new HashSet<>();
		  for(GiantEagleCompDataDTO giantEagleCompDataDTO: CompDataList){
		   retItemcodeSet.add(giantEagleCompDataDTO.getRITEM_NO());
		  }
		  HashMap<String, List<String>> upcMap = 
		    itemDAO.getUPCListForRetItemcodes(conn, retItemcodeSet);
		  
		  
		  for(GiantEagleCompDataDTO giantEagleCompDataDTO: CompDataList){
			   recordCount++;
			   if(upcMap.get(giantEagleCompDataDTO.getRITEM_NO()) == null){
			    skippedRetailerItemcodes.add(giantEagleCompDataDTO.getRITEM_NO());
			   }
			   else
			   {
				   List<String> upcs = upcMap.get(giantEagleCompDataDTO.getRITEM_NO());
				   for(String upc: upcs){
					    pw.print(giantEagleCompDataDTO.getCMPTR_BNR_CD() + "-" + giantEagleCompDataDTO.getCMPTR_ID()); // Store Number # 1
						pw.print("|");
						pw.print(giantEagleCompDataDTO.getPRC_CHK_DTE()); // Price Check Date # 2
						pw.print("|");
						pw.print(giantEagleCompDataDTO.getRITEM_NO()); // Item Code # 3
						pw.print("|");
						pw.print(""); // Item Desc # 4
						pw.print("|");
						pw.print(giantEagleCompDataDTO.getCMPTR_MUNIT_CNT()); // Regular Quantity # 5
						pw.print("|");
						pw.print(giantEagleCompDataDTO.getCMPTR_CURR_PRC()); // Regular Retail # 6
						pw.print("|");
						pw.print(""); // Size # 7
						pw.print("|");
						pw.print(upc); // UPC # 8
						pw.print("|");
						pw.print("N"); // Outside Indicator # 9
						pw.print("|");
						pw.print(""); // Additional Info # 10
						pw.print("|");
						pw.print(giantEagleCompDataDTO.getCMPTR_SL_MUNIT_CNT()); // Sale Quantity # 11
						pw.print("|");
						pw.print(giantEagleCompDataDTO.getCMPTR_SL_PRC()); // Sale Retail # 12
						pw.print("|");
						pw.print(giantEagleCompDataDTO.getSL_PRC_CHK_DTE()); // Sale Retail # 13
						pw.println("       "); // spaces
						
						if (recordCount % Constants.LOG_RECORD_COUNT == 0) {
							logger.info("No of records processed - " + recordCount);
						}
				   }
			   }
		  }
	}
}
