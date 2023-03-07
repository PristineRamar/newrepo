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

import com.pristine.dao.DBManager;
import com.pristine.dao.StoreDAO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleStoreDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleZoneMappingDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class StoreFileFormatter extends PristineFileParser{
	
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("StoreFileFormatterGE");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private final static String MODE = "MODE=";
	private final static String BASE_STORE = "BASE";
	private final static String COMP_STORE = "COMP";
	private String mode = "BASE";
	private String rootPath;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private int recordCount = 0;
	private int skippedCount = 0;
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private static String relativeInputPath, relativeOutputPath;
	private Set<String> uniqueStoreNumbers = null;
	private HashMap<String, GiantEagleStoreDTO> storeandCompNameMap;
	private String UNKNOWN_BANNER_CODE = "UNK";
	private String NOT_AVAILABLE = "N/A";
	private String DEFAULT_CITY = "PITTSBURGH";
	private String DEFAULT_STATE = "PA";
	
	public static void main(String[] args) {
		StoreFileFormatter fileFormatter = new StoreFileFormatter();
		PropertyConfigurator.configure("log4j_GE_fileFormatter.properties");
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				relativeOutputPath = arg.substring(OUTPUT_FOLDER.length());
			}
			if (arg.startsWith(MODE)) {
				fileFormatter.mode = arg.substring(MODE.length());
			}
		}
		fileFormatter.processFile();
	}
	
	/**
	 * Fill all columns in list and processing the file.
	 */
	@SuppressWarnings("unchecked")
	private void processFile() {

		super.headerPresent = false;
		uniqueStoreNumbers = new HashSet<>();
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillAllColumns();
		if(mode.equals(COMP_STORE)){
			intialSetup();
			try {
				StoreDAO storeDAO = new StoreDAO();
				storeandCompNameMap = storeDAO.getStoreandCompNameMap(conn);
			} catch (GeneralException e) {
				logger.error("Error in getting Store number and Comp Name...");
				e.printStackTrace();
			}finally {
				PristineDBUtil.close(getOracleConnection());
			}
		}
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}

		logger.info("Formatting Store file Started... ");
		try {
			parseFile(fieldNames);
		} catch (IOException | GeneralException e) {
			logger.error("Error -- processFile()", e);
		}
		logger.info("Total Number of Stores " + recordCount);
		logger.info("Formatting Store file Completed... ");
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
				    	
				    	
				    	writeHeader();
				    				    	
				    	super.parseDelimitedFile(GiantEagleStoreDTO.class, fileList.get(i), '|', fieldNames, stopCount);
				    					    	
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
		if(mode.equals(BASE_STORE)){
			allColumns.put(1, "STR_NO");
			allColumns.put(2, "CORP_NAME");
			allColumns.put(3, "BNR_CD");		
			allColumns.put(4, "STT1_ADR");
			allColumns.put(5, "CTY_ADR");
			allColumns.put(6, "ST_ADR");
			allColumns.put(7, "ZIP1_ADR");
			allColumns.put(8, "ZIP2_ADR");
			allColumns.put(9, "AD_ZN_REP_FG");
			allColumns.put(10, "PRC_GRP_CD");
			allColumns.put(11, "ZN_NO");
			allColumns.put(12, "SPLR_NO");
			allColumns.put(13, "DIV_CD");
			allColumns.put(14, "DIV_DSCR");
			allColumns.put(15, "AD_ZN");
			allColumns.put(16, "AD_ZN_DSCR");
		}else if(mode.equals(COMP_STORE)){
			//CORP_NME|CMPTR_ID|CMPTR_BNR_CD|CTY_ADR|STT1_ADR|STT2_ADR|ST_ADR|ZIP1_ADR|COMPOSITE_FG
			allColumns.put(1, "CORP_NAME");
			allColumns.put(2, "STR_NO");
			allColumns.put(3, "BNR_CD");		
			allColumns.put(4, "CTY_ADR");
			allColumns.put(5, "STT1_ADR");
			allColumns.put(6, "STT2_ADR");
			allColumns.put(7, "ST_ADR");
			allColumns.put(8, "ZIP1_ADR");
			allColumns.put(9, "COMPOSITE_FG");
		}
	}
	

	@SuppressWarnings("unchecked")
	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<GiantEagleStoreDTO> giantEagleStoreDTOList = (List<GiantEagleStoreDTO>) listobj;
		for (int j = 1; j < giantEagleStoreDTOList.size(); j++) {
			GiantEagleStoreDTO giantEagleStoreDTO = giantEagleStoreDTOList.get(j);
			String storeNum = giantEagleStoreDTO.getBNR_CD() 
					+ "-" + giantEagleStoreDTO.getSTR_NO();
			if(!uniqueStoreNumbers.contains(storeNum)){
				uniqueStoreNumbers.add(storeNum);
				recordCount++;
				if(mode.equals(BASE_STORE)){
					pw.print("53"); // Presto Chain ID # 1
					pw.print("|");
					pw.print("GIANT EAGLE"); // Chain Name # 2
					pw.print("|");
					pw.print(giantEagleStoreDTO.getSTR_NO()); // Store Number # 3
					pw.print("|");
					pw.print(giantEagleStoreDTO.getSTT1_ADR()); // Store Address Line1 # 4
					pw.print("|");
					pw.print(""); // Store Address Line2 # 5
					pw.print("|");
					pw.print(giantEagleStoreDTO.getCTY_ADR()); // City # 6
					pw.print("|");
					pw.print(giantEagleStoreDTO.getST_ADR()); // State # 7
					pw.print("|");
					pw.print(giantEagleStoreDTO.getZIP1_ADR()); // Zip # 8
					pw.print("|");
				}else if(mode.equals(COMP_STORE)){
					//Short name as a key returns an object then assign values else assign default values 
					if(storeandCompNameMap.get(giantEagleStoreDTO.getBNR_CD()) != null){
						GiantEagleStoreDTO giantEagleStoreDTO2 = storeandCompNameMap.get(giantEagleStoreDTO.getBNR_CD());
						pw.print(giantEagleStoreDTO2.getCHAIN_ID()); // Presto Chain ID # 1
						pw.print("|");
						pw.print(giantEagleStoreDTO2.getCORP_NAME()); // Chain Name # 2
						pw.print("|");
						pw.print(giantEagleStoreDTO2.getBNR_CD()+"-"+giantEagleStoreDTO.getSTR_NO()); // Store Number # 3
					}else{
						//Set default values 
						GiantEagleStoreDTO giantEagleStoreDTO2 = storeandCompNameMap.get(UNKNOWN_BANNER_CODE);
						pw.print(giantEagleStoreDTO2.getCHAIN_ID()); // Presto Chain ID # 1
						pw.print("|");
						pw.print(giantEagleStoreDTO2.getCORP_NAME()); // Chain Name # 2
						pw.print("|");
						pw.print(giantEagleStoreDTO2.getBNR_CD()+"-"+giantEagleStoreDTO.getSTR_NO()); // Store Number # 3
					}
					pw.print("|");
					if (giantEagleStoreDTO.getSTT1_ADR().equals(NOT_AVAILABLE)
							|| giantEagleStoreDTO.getSTT1_ADR().equals("")
							|| giantEagleStoreDTO.getSTT1_ADR() == null) {
						pw.print("101 Kappa Dr");
					}else{
						pw.print(giantEagleStoreDTO.getSTT1_ADR()); // Store Address Line1 # 4
					}
					pw.print("|");
					if (giantEagleStoreDTO.getSTT2_ADR().equals(NOT_AVAILABLE)
							|| giantEagleStoreDTO.getSTT2_ADR().equals("")
							|| giantEagleStoreDTO.getSTT2_ADR() == null) {
						pw.print("");
					}else{
						pw.print(giantEagleStoreDTO.getSTT2_ADR()); // Store Address Line2 # 5
					}
					pw.print("|");
					if (giantEagleStoreDTO.getCTY_ADR().equals(NOT_AVAILABLE)
							|| giantEagleStoreDTO.getCTY_ADR().equals("")
							|| giantEagleStoreDTO.getCTY_ADR() == null) {
						pw.print(DEFAULT_CITY); // City # 6
					}else{
						pw.print(giantEagleStoreDTO.getCTY_ADR()); // City # 6
					}
					pw.print("|");
					if(giantEagleStoreDTO.getST_ADR().equals(NOT_AVAILABLE)|| giantEagleStoreDTO.getST_ADR().equals("")
							|| giantEagleStoreDTO.getST_ADR() == null){
						pw.print(DEFAULT_STATE);
					}else{
						pw.print(giantEagleStoreDTO.getST_ADR()); // State # 7
					}
					pw.print("|");
					if(giantEagleStoreDTO.getZIP1_ADR().equals(NOT_AVAILABLE)|| giantEagleStoreDTO.getZIP1_ADR().equals("")
							|| giantEagleStoreDTO.getZIP1_ADR() == null){
						pw.print("15238");
					}else{
						pw.print(giantEagleStoreDTO.getZIP1_ADR()); // Zip # 8
					}
				pw.print("|");
				}
				
				pw.print(""); // Trade Area ID # 9
				pw.print("|");
				pw.print(""); // Zone Number # 10
				pw.print("|");
				pw.print(""); // Zone Name # 11
				pw.print("|");
				pw.print(""); // 24 HRS # 12
				pw.print("|");
				pw.print(""); // Pharmacy # 13
				pw.print("|");
				pw.print(""); // Gas # 14
				pw.print("|");
				pw.print(""); // Bank # 15
				pw.print("|");
				pw.print(""); // Fast Food # 16
				pw.print("|");
				pw.print(""); // Coffee Shop # 17
				pw.print("|");
				pw.print(""); // Store Classification # 18
				pw.print("|");
				pw.print(""); // classic/super # 19
				pw.print("|");
				pw.print(""); // Additional Type 1 # 20
				pw.print("|");
				pw.print(""); // Additional Type 2 # 21
				pw.print("|");
				pw.print(""); // Store Open Date # 22
				pw.print("|");
				pw.print(""); // ReModel Date # 23
				pw.print("|");
				pw.print(""); // Acquired Date # 24
				pw.print("|");
				pw.print(""); // Store Closed Date # 25
				pw.print("|");
				pw.print(""); // Anniversary Date # 26
				pw.print("|");
				pw.print(""); // Square Footage # 27
				pw.print("|");
				pw.print(""); // Department1 Zone Num # 28
				pw.print("|");
				pw.print(""); // Department2 Zone Num # 29
				pw.print("|");
				pw.print(""); // Department3 Zone Num # 30
				pw.print("|");
				pw.print(""); // Store Manager # 31
				pw.print("|");
				pw.print(""); // Contact Number # 32
				pw.print("|");
				pw.print(""); // Fax Number # 33
				pw.print("|");
				pw.print(giantEagleStoreDTO.getAD_ZN_DSCR()); // District # 34
				pw.print("|");
				pw.print(""); // District Manager # 35
				pw.print("|");
				pw.print(""); // District Contact . # 36
				pw.print("|");
				pw.print(""); // District Fax . # 37
				pw.print("|");
				pw.print(giantEagleStoreDTO.getDIV_DSCR()); // Region # 38
				pw.print("|");
				pw.print(""); // Regional Manager # 39
				pw.print("|");
				pw.print(""); // Regional Contact . # 40
				pw.print("|");
				pw.print(""); // Regional Fax . # 41
				pw.print("|");
				if("GE".equals(giantEagleStoreDTO.getBNR_CD())){
					pw.print("GIANT EAGLE"); // Division # 42	
				}else if("MI".equals(giantEagleStoreDTO.getBNR_CD())){
					pw.print("INDY"); // Division # 42	
				}
				pw.print("|");
				pw.print(""); // Division Manager # 43
				pw.print("|");
				pw.print(""); // Division Contact . # 44
				pw.print("|");
				pw.print(""); // Division Fax . # 45
				pw.print("|");
				pw.print(""); // Comments . # 46
				pw.print("|");
				pw.println("       "); // spaces
				
				if (recordCount % Constants.LOG_RECORD_COUNT == 0) {
					logger.info("No of records processed - " + recordCount);
				}
			}
		}
	}
	
	private void writeHeader(){
		pw.print("Presto Chain ID"); // Presto Chain ID # 1
		pw.print("|");
		pw.print("Chain Name"); // Chain Name # 2
		pw.print("|");
		pw.print("Store Number"); // Store Number # 3
		pw.print("|");
		pw.print("Store Address Line1"); // Store Address Line1 # 4
		pw.print("|");
		pw.print("Store Address Line2"); // Store Address Line2 # 5
		pw.print("|");
		pw.print("City"); // City # 6
		pw.print("|");
		pw.print("State"); // State # 7
		pw.print("|");
		pw.print("Zip"); // Zip # 8
		pw.print("|");
		pw.print("Trade Area ID"); // Trade Area ID # 9
		pw.print("|");
		pw.print("Zone Number"); // Zone Number # 10
		pw.print("|");
		pw.print("Zone Name #"); // Zone Name # 11
		pw.print("|");
		pw.print("24 HRS"); // 24 HRS # 12
		pw.print("|");
		pw.print("Pharmacy"); // Pharmacy # 13
		pw.print("|");
		pw.print("Gas"); // Gas # 14
		pw.print("|");
		pw.print("Bank"); // Bank # 15
		pw.print("|");
		pw.print("Fast Food"); // Fast Food # 16
		pw.print("|");
		pw.print("Coffee Shop"); // Coffee Shop # 17
		pw.print("|");
		pw.print("Store Classification"); // Store Classification # 18
		pw.print("|");
		pw.print("classic/super"); // classic/super # 19
		pw.print("|");
		pw.print("Additional Type 1"); // Additional Type 1 # 20
		pw.print("|");
		pw.print("Additional Type 2"); // Additional Type 2 # 21
		pw.print("|");
		pw.print("Store Open Date"); // Store Open Date # 22
		pw.print("|");
		pw.print("ReModel Date"); // ReModel Date # 23
		pw.print("|");
		pw.print("Acquired Date"); // Acquired Date # 24
		pw.print("|");
		pw.print("Store Closed Date"); // Store Closed Date # 25
		pw.print("|");
		pw.print("Anniversary Date"); // Anniversary Date # 26
		pw.print("|");
		pw.print("Square Footage"); // Square Footage # 27
		pw.print("|");
		pw.print("Department1 Zone Num"); // Department1 Zone Num # 28
		pw.print("|");
		pw.print("Department2 Zone Num"); // Department2 Zone Num # 29
		pw.print("|");
		pw.print("Department3 Zone Num"); // Department3 Zone Num # 30
		pw.print("|");
		pw.print("Store Manager"); // Store Manager # 31
		pw.print("|");
		pw.print("Contact Number"); // Contact Number # 32
		pw.print("|");
		pw.print("Fax Number"); // Fax Number # 33
		pw.print("|");
		pw.print("District"); // District # 34
		pw.print("|");
		pw.print("District Manager"); // District Manager # 35
		pw.print("|");
		pw.print("District Contact ."); // District Contact . # 36
		pw.print("|");
		pw.print("District Fax ."); // District Fax . # 37
		pw.print("|");
		pw.print("Region"); // Region # 38
		pw.print("|");
		pw.print("Regional Manager"); // Regional Manager # 39
		pw.print("|");
		pw.print("Regional Contact ."); // Regional Contact . # 40
		pw.print("|");
		pw.print("Regional Fax ."); // Regional Fax . # 41
		pw.print("|");
		pw.print("Division"); // Division # 42
		pw.print("|");
		pw.print("Division Manager"); // Division Manager # 43
		pw.print("|");
		pw.print("Division Contact ."); // Division Contact . # 44
		pw.print("|");
		pw.print("Division Fax ."); // Division Fax . # 45
		pw.print("|");
		pw.print("Comments ."); // Comments . # 46
		pw.println("       "); // spaces
	}
	public void intialSetup() {
		initialize();
	}
	protected void setConnection() {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	/**
	 * Initializes object
	 */
	protected void initialize() {
		setConnection();
	}

}
