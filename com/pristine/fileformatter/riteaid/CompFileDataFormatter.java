package com.pristine.fileformatter.riteaid;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dto.CompetitiveDataDTO;

import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

public class CompFileDataFormatter extends PristineFileParser{
	 private static Logger logger = Logger.getLogger("CompFileFormat");
	 private boolean headerPresent = false;
	 private int recordCount = 0;
	 private FileWriter fw = null;
	
	 private PrintWriter pw = null;
		
	 public static void main(String[] args) {
			PropertyConfigurator.configure("log4j-comp-file-formatter.properties");
			String inputFilePath = args[0];
			String outputFilePath = args[1];
			CompFileDataFormatter compFormatter = new CompFileDataFormatter ();
			compFormatter.processFiles(inputFilePath,outputFilePath);
	 }

	private void processFiles(String inputFilePath, String outputFilePath) {
		try
		{
			super.headerPresent = true;
			String separator = ",";
			int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
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
						    
						    String fieldNames[] = new String[8];
	
						    //IPOSD_DTE_RCV,
						    //IPOSD_CPTR_ID,
						    //IPOSD_CPTR_STR_NBR,
						    //IPOSD_UPC,
						    //IPOSD_CPTR_ITM_DES,
						    //IPOSD_CPTR_ITM_PRC,
						    //IPOSD_CPTR_UOM_QTY,
						    //IPOSD_ITM_NBR
						    
						    //Note** Change the field Names
					    	//fieldNames[0] = "comment";
					    	fieldNames[0] = "checkDate";
					    	fieldNames[1] = "comment";
					    	fieldNames[2] = "compStrNo";
					    	fieldNames[3] = "upc";
					    	fieldNames[4] = "itemName";
					    	fieldNames[5] = "regMPrice";
					    	fieldNames[6] = "regMPack";
					    	fieldNames[7] = "retailerItemCode";
					    	/*fieldNames[6] = "fSaleMPrice";
					    	fieldNames[7] = "saleMPack";
					    	fieldNames[8] = "saleInd";
					    	
					    	fieldNames[10] = "size";
							*/
					    	logger.info("Processing Retail Price Records ...");
					    	createFileWriter(fileList.get(i), outputFilePath);
					    	createPrintWriter(fw);
					    	recordCount = 0;
					    	super.parseDelimitedFile(CompetitiveDataDTO.class, files, separator.charAt(0),fieldNames, stopCount);
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
        logger.error("Outer Exception -  GeneralException", ex);
		}
		catch (Exception ex) {
			logger.error("Outer Exception - JavaException", ex);
		}
	
		logger.info("Comp File Formatting Complete");
	
	}
			
	private void createFileWriter(String fileName, String outputFilePath){
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
	public void close(PrintWriter pw, FileWriter fw){
		try{
			pw.flush();
			pw.close();
			fw.close();
		}catch(IOException exception){
			logger.error("Error while closing writer - " + exception.toString());
		}
	}

	public void processRecords(List listobj) throws GeneralException {
		List<CompetitiveDataDTO> compDataList = (List<CompetitiveDataDTO>) listobj;
		
		for(CompetitiveDataDTO compDataDTO : compDataList){
			writeToFile(pw, compDataDTO);
			recordCount++;
			if(recordCount % Constants.LOG_RECORD_COUNT == 0){
				logger.info("Processed " + recordCount + " records");
			}
		}
	}
	
	public void writeToFile(PrintWriter pw, CompetitiveDataDTO compDataDTO) throws GeneralException{
		
		String checkDate = compDataDTO.checkDate.replaceAll(" 0:00:00", "");
		
		
		compDataDTO.saleMPack = 0;
		compDataDTO.fSaleMPrice = 0;
		pw.print(compDataDTO.compStrNo);
		pw.print("|");
		pw.print(checkDate);
		pw.print("|");
		//itemCode 
		pw.print("|");
		pw.print(compDataDTO.itemName.replaceAll("|", ""));
		pw.print("|");
		pw.print(compDataDTO.regMPack);
		pw.print("|");
		pw.print(compDataDTO.regMPrice);
		pw.print("|");
		//Size 		
		pw.print("|");
		pw.print(compDataDTO.upc);
		pw.print("|");
		
		//OutSide Range Ind
		pw.print("N");
		pw.print("|");

		//additional Info
		pw.print(compDataDTO.comment);
		pw.print("|");
		
		//Sale Qty 
		pw.print(compDataDTO.saleMPack);
		pw.print("|");

		//Sale Retail 
		pw.print(compDataDTO.fSaleMPrice);
		pw.print("|");

		//Sale Date 
		pw.println(" ");

	}
	
	public String removeLeadingZero (String str){ 
		int lastLeadZeroIndex = 0;
		for (int i = 0; i < str.length(); i++) {
		  char c = str.charAt(i);
		  if (c == '0') {
		    lastLeadZeroIndex = i;
		  } else {
		    break;
		  }
		}
	
		str = str.substring(lastLeadZeroIndex+1, str.length());
		return str;
	}
}
