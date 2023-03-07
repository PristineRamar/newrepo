package com.pristine.fileformatter.gianteagle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.pristine.dto.fileformatter.gianteagle.GiantEagleItemDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleStoreDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleZoneMappingDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class ZoneMappingFileFormatter extends PristineFileParser{

	private static Logger logger = Logger.getLogger("StoreFileFormatterGE");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private String rootPath;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private int recordCount = 0;
	private int skippedCount = 0;
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private static String relativeInputPath, relativeOutputPath;
		
	public static void main(String[] args) {
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				relativeOutputPath = arg.substring(OUTPUT_FOLDER.length());
			}
					}
		ZoneMappingFileFormatter fileFormatter = new ZoneMappingFileFormatter();
		fileFormatter.processFile();
	}
	
	@SuppressWarnings("unchecked")
	private void processFile() {

		super.headerPresent = false;
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillAllColumns();
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
				       	super.parseDelimitedFile(GiantEagleZoneMappingDTO.class, fileList.get(i), '|', fieldNames, stopCount);
				    					    	
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
	}
	
	private void writeHeader(){
		pw.print("Category"); // Category # 1
		pw.print("|");
		pw.print("Primary Competitor"); // Primary Competitor # 2
		pw.print("|");
		pw.print("Price Zone"); // Price Zone # 3
		pw.print("|");
		pw.print("Store"); // Store # 4
		pw.print("|");
		pw.print("Prc Grp Code"); // Price group code # 5
		pw.println("       "); // spaces
	}

	@SuppressWarnings("unchecked")
	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<GiantEagleZoneMappingDTO> giantEagleZoneMappingDTOList = (List<GiantEagleZoneMappingDTO>) listobj;
		for (int j = 1; j < giantEagleZoneMappingDTOList.size(); j++) {
			GiantEagleZoneMappingDTO giantEagleZoneMappingDTO = giantEagleZoneMappingDTOList.get(j);
			recordCount++;
			pw.print("9999"); // Category # 1
			pw.print("|");
			pw.print(""); // Primary Competitor # 2
			pw.print("|");
			if(giantEagleZoneMappingDTO.getSPLR_NO() == "")
			{
				pw.print(giantEagleZoneMappingDTO.getBNR_CD() + "-" 
						+ giantEagleZoneMappingDTO.getZN_NO() + "-" 
						+ giantEagleZoneMappingDTO.getPRC_GRP_CD()); // Price Zone # 3
			}
			else
			{
				pw.print(giantEagleZoneMappingDTO.getBNR_CD() + "-" 
						+ giantEagleZoneMappingDTO.getZN_NO() + "-" 
						+ giantEagleZoneMappingDTO.getPRC_GRP_CD() 
						+ "-" + giantEagleZoneMappingDTO.getSPLR_NO()); // Price Zone # 3
			}
			pw.print("|");
			pw.print(giantEagleZoneMappingDTO.getSTR_NO()); // Store # 4
			pw.print("|");
			pw.print(giantEagleZoneMappingDTO.getPRC_GRP_CD()); // prc grp code # 5
			
			pw.println("       "); // spaces
			if (recordCount % Constants.LOG_RECORD_COUNT == 0) {
				logger.info("No of records processed - " + recordCount);
			}
		}
	}

}
