package com.pristine.dataload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class TopsHistoricCompDataProcessor {

	private static Logger  logger = Logger.getLogger("TopHistoricCompDataProcessor");
	static String storeNum;
	static String checkDate;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
			// TODO Auto-generated method stub
	
			if( args.length < 1  ){
				logger.info("Insufficient Arguments - Root Path and (File name)");
				System.exit(1);
			}
			ArrayList <String> fileList = new ArrayList <String> ();
			
			PropertyManager.initialize("analysis.properties");
			String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		
			logger.debug( "arg[0] = " +  args[0]);

			
			if( args.length >= 1){
				rootPath = rootPath + "/"+ args[0];
			}
			logger.debug("Root Path is " + rootPath);
			 
			try{
			if( args.length== 2){
				String fileName = rootPath;
				if( !rootPath.equals(""))
					fileName = fileName +"/";
				fileName = fileName + args[1];
				fileList.add(fileName);
			}
			else{
				File dir = new File(rootPath);
	
				String[] children = dir.list();
				if (children != null) {
				    for (int i=0; i<children.length; i++) {
				        // Get filename of file or directory
				        String filename = children[i];
				        //logger.debug(filename);
				        if( filename.contains(".xls"))
				        	fileList.add(rootPath + "/" +filename);
				    }
				}
				
			}
			
			if( fileList.isEmpty()){
				logger.info("No file present to load");
				System.exit(1);
			}
			TopsHistoricCompDataProcessor  compDataProcessor = new TopsHistoricCompDataProcessor();
			for(String file : fileList){
				logger.info("Loading file:" + file);
				ArrayList<String> newFileList = new ArrayList <String>();
				newFileList.add(file);
				if( compDataProcessor.setStoreCheckDate(file)){
					compDataProcessor.preProcess(file);
					PrestoUtil.moveFiles(newFileList , rootPath + "/" + Constants.COMPLETED_FOLDER);
				}
				else{
					logger.info("Did not process file:" + file);
					PrestoUtil.moveFiles(newFileList , rootPath + "/" + Constants.BAD_FOLDER);

				}
			}
			
			logger.info("Processing completed");
			
		}catch(Exception e){
			logger.error("Error in Load", e);
			System.exit(1);
		}
		
	}

	private boolean setStoreCheckDate(String file){
		//get the file name
		//store num and date
	
		boolean retVal = true;
		try{
			String path[]=file.split("/");
			String fileName = path[path.length-1];
			String fields[]=fileName.split("_");
			storeNum = fields[0];
			checkDate = fields[2].substring(0, 2) + "/" + fields[2].substring(2, 4) + "/20" + fields[2].substring( 4);  
			try{
				Date javaDate = DateUtil.toDate(checkDate, Constants.APP_DATE_FORMAT);
			}catch (GeneralException e){
				logger.info("Invalid Date Format, should be MM/DD/yyyy");
				retVal = false;
			}
			logger.info("Store Num:" + storeNum + " Check Date" + checkDate);
		
		}catch(Exception e){
			retVal = false;
			logger.error("Incorrect File name" + file );
			logger.error("Incorrect File name format", e );
		}
		
		return retVal;

	}
	private void preProcess(String filename) throws Exception{
		// TODO Auto-generated method stub
		List<List<String>> excelData = parseExcelFile(filename,1);
		int count = 0;
		
		String outputfileName = filename.replace(".xls",".csv");
		FileOutputStream fileOut =  new FileOutputStream(outputfileName);
		
		try {
			for(List<String> row : excelData){
				count++;
				if ( count == 1) continue; //skip header row
				int colCount = 0;
				
				/*
				if ( count == 100)
					break;
				else if (count < 10){
					logger.info("Rownum:" + count + " ColCount:" + row.size());
					continue;
				}*/
				
				
				writeToCell(colCount, storeNum);
				writeToCell(++colCount, checkDate);
				writeToCell(++colCount, "NA");
				writeToCell(++colCount, row.get(1).replaceAll(",", " "));
				
				
				//RegQty and Price
				String regQty = clean(row.get(3));
				int qty = (int) Double.parseDouble(regQty);
				String regPrice = clean(row.get(4));
				writeToCell( ++colCount, Integer.toString(qty));
				writeToCell( ++colCount, regPrice);
				
				
				writeToCell(++colCount, row.get(2)); //size UOM
				String upc =  row.get(0).replaceAll("'", "");
				upc = upc.substring(1);
				writeToCell(++colCount, upc);
				
				String additionalInfo = "";
				writeToCell(++colCount, "N");
				writeToCell( ++colCount, additionalInfo);
				
				String saleQty = clean(row.get(5));
				
				String salePrice = clean(row.get(6));
	
				if (salePrice.equals("0"))
					saleQty="0";
				
				int numericSaleQty = (int) Double.parseDouble(saleQty);
				writeToCell( ++colCount, Integer.toString(numericSaleQty));
				writeToCell(++colCount, salePrice);
				
				
				//SaleEnd Date
				writeToCell( ++colCount, "");
				writeRowToFile(fileOut);
			}
		}catch(Exception e){
			logger.info( "# of Rows Processed: " + count);
			throw e;
		}
		logger.info( "# of Rows Processed: " + count);
		fileOut.close();
		
	}
	
	private String clean(String inp){
		if( inp != null ){
			inp = inp.trim();
		}else{
			inp = "";
		}
		if( inp.equals("")){
			inp = "0";
		}
		return inp;
	}
	StringBuffer sb = new StringBuffer (); 
	private void writeToCell( int colCount, String val) {
		if( colCount == 0)
			sb = new StringBuffer ();
		else
			sb.append("|");
		sb.append(val);
		
	}
	
	private void writeRowToFile(FileOutputStream fileOut) throws Exception{

		fileOut.write(sb.toString().getBytes());
		fileOut.write('\n');
	
	}
	
	public List<List<String>> parseExcelFile(String filename, int sheetNo) throws Exception{
		List<List<String>> sheetData = new ArrayList <List<String>>(); 
		FileInputStream fis = null; 
		try { 
			fis = new FileInputStream(filename); 
        
			HSSFWorkbook workbook = new HSSFWorkbook(fis); 
		           
			HSSFSheet sheet = workbook.getSheetAt(sheetNo); 
       
			Iterator rows = sheet.rowIterator(); 
          
			while (rows.hasNext()) { 
		               
				HSSFRow row = (HSSFRow) rows.next(); 
				
				List<String> data = new ArrayList <String>(); 		             
				addToData( data, row, 2); //UPC
				addToData( data, row, 4); //Item Name
				addToData( data, row, 5); //SizeUOM
				addToData( data, row, 12); //regular qty
				addToData( data, row, 13); //regular Price
				addToData( data, row, 15); //sale qty
				addToData( data, row, 16); //sale price
				sheetData.add(data); 
      
			} 
		}  finally { 
			if (fis != null) { 
				fis.close(); 
			} 
		} 
		return sheetData;
		 
	}
	
	private void addToData( List<String> data, HSSFRow row, int cellNum){
		HSSFCell cell = row.getCell(cellNum);
		if ( cell == null)
			data.add("");
		else{
			data.add(cell.toString());
		}
			
	}

}
