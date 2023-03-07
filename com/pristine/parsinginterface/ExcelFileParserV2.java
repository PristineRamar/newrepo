package com.pristine.parsinginterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.stream.events.StartDocument;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.csvreader.CsvReader;
import com.pristine.dto.offermgmt.promotion.PromoBuyItems;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.weeklyad.RawWeeklyAd;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAd;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdBlock;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdPage;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdPromo;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class ExcelFileParserV2<T> {
	String rootPath = null;
	
	private int FIRST_ROW_TO_PROCESS = 0;
	public HashMap<String, List<Integer>> page= null;
	public HashMap<String, List<Integer>> block = null;
	private Logger logger = Logger.getLogger("DataParser");
	
	public ExcelFileParserV2(){
		page = new HashMap<String, List<Integer>>();
		block = new HashMap<String, List<Integer>>();
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	}
	
	/**
	 * Returns Excel File List
	 * @param specificPath
	 * @return
	 * @throws GeneralException
	 */
	public ArrayList<String> getFiles(String specificPath) 
	{
		String fullPath = rootPath;
	    if(specificPath!=null && specificPath.trim().length()>0)
	    {
	    	fullPath=fullPath+"/"+specificPath;
	    }
	    
	    ArrayList <String> fileList = new ArrayList <String> ();

	    File dir = new File(fullPath);
	    
	    String[] children = dir.list();
        if (children != null) {
            for (int i=0; i<children.length; i++) {
                // Get filename of file or directory
                String filename = children[i];
                if( filename.toLowerCase().contains(".xls") || filename.toLowerCase().contains(".csv"))
                        fileList.add(fullPath + "/" +filename);
            }
        }
		return fileList;
	}

	public List<T> parseExcelFile(Class returnObject, String fileName, int sheetNo, TreeMap<Integer, String> fieldNames) throws GeneralException{
		String sheetName = null;
		try{
			FileInputStream objXlsFile = new FileInputStream(fileName);
			HSSFWorkbook objWbook = new HSSFWorkbook(objXlsFile);
			sheetName = objWbook.getSheetName(sheetNo);
			objWbook.close();
			objXlsFile.close();
		}catch(IOException ioException){
			throw new GeneralException("Error when reading excel file " + ioException);
		}
		
		List<T> listObj = parseExcelFile(returnObject, fileName, sheetName, fieldNames);
		return listObj;
	}
	
	/**
	 * Parses and stores columns specified in fieldNames
	 * @param returnObject	Class
	 * @param fileName		File Name
	 * @param sheetName		Sheet Name
	 * @param fieldNames	Column Pos as Key, Field name as Value
	 * @return
	 * @throws GeneralException
	 */
	public List<T> parseExcelFile(Class returnObject, String fileName, String sheetName, TreeMap<Integer, String> fieldNames) throws GeneralException{
		List<T> listobj = new ArrayList<T>();
		try{
			FileInputStream objXlsFile = new FileInputStream(fileName);
			HSSFWorkbook objWbook = new HSSFWorkbook(objXlsFile);
			HSSFSheet objSheet = null;
			for(int i = 0; i < objWbook.getNumberOfSheets(); i++){
				if(objWbook.getSheetName(i).equalsIgnoreCase(sheetName) || objWbook.getSheetName(i).contains(sheetName)){
					objSheet = objWbook.getSheetAt(i);
					break;
				}
			}
			int recordCount = 0;
			int ignoreCount = 0;
			for (int i=FIRST_ROW_TO_PROCESS; i <= objSheet.getPhysicalNumberOfRows(); i++){
				HSSFRow row = objSheet.getRow(i);
				if(row == null) continue;
				Object objDTO = returnObject.newInstance();
				boolean status = true;
				for(Map.Entry<Integer, String> entry : fieldNames.entrySet()){
					if(status){
						HSSFCell cell = row.getCell(entry.getKey());
						if(cell != null){
							try {
		                        Field field = null;
		                        field = returnObject.getDeclaredField(entry.getValue());
		                        field.setAccessible(true);
		                        field=setFieldValue(field, objDTO, cell, null);
		                    } catch (Exception e){
		                    	logger.warn("Parse Error " + e);
		                    	status = false;
		                    } catch (GeneralException e){
		                    	logger.warn("Parse Error " + e);
		                    	status = false;
		                    }
						}
					}
				}
				if(status){
					listobj.add((T) objDTO);
					recordCount++;
				}else{
					ignoreCount++;
				}
			}
			logger.info("No of records considered - " + recordCount);
			logger.info("No of records ignored - " + ignoreCount);
			objWbook.close();
			objXlsFile.close();
		}catch(IOException ioException){
			throw new GeneralException("Error when reading excel file " + ioException);
		}catch(Exception e) {
        	throw new GeneralException("File read error ", e);
        } 
		return listobj;
	}
	
	
	/**
	 * Parses and stores columns specified in fieldNames
	 * @param returnObject	Class
	 * @param fileName		File Name
	 * @param sheetName		Sheet Name
	 * @param fieldNames	Column Pos as Key, Field name as Value
	 * @return
	 * @throws GeneralException
	 */
	public List<T> parseCSVFile(Class returnObject, String fileName, TreeMap<Integer, String> fieldNames, char delimiter) throws GeneralException{
		List<T> listobj = new ArrayList<T>();
		CsvReader csvReader = null;
		try{
			csvReader = new CsvReader(fileName);
			csvReader.setDelimiter(delimiter);
			int recordCount = 0;
			int ignoreCount = 0;
			String line[];
			while (csvReader.readRecord()) {
            	//Skip header Record.
            	if( FIRST_ROW_TO_PROCESS == 1 && recordCount == 0){ 
            		recordCount++;
            		continue;
            	}
            	
            	Object objDTO = returnObject.newInstance();
				boolean status = true;
				line = csvReader.getValues();
				for(Map.Entry<Integer, String> entry : fieldNames.entrySet()){
					if(status){
						String val = line[entry.getKey()];
						//logger.info(val);
						if(val.length() > 0 && val.charAt(val.length() - 1) == '.'){
							val = val.replace("\\.", "");
						}
							try {
		                        Field field = null;
		                        field = returnObject.getDeclaredField(entry.getValue());
		                        field.setAccessible(true);
		                        field=setFieldValue(field, objDTO, val);
		                    } catch (Exception e){
		                    	logger.warn("Parse Error ", e);
		                    	status = false;
		                    } catch (GeneralException e){
		                    	logger.warn("Parse Error ",e);
		                    	status = false;
		                    }
					}
				}
				if(status){
					listobj.add((T) objDTO);
					recordCount++;
				}else{
					ignoreCount++;
				}
        	}
			
			logger.info("No of records considered - " + recordCount);
			logger.info("No of records ignored - " + ignoreCount);
		}catch(IOException ioException){
			throw new GeneralException("Error when reading csv file " + ioException);
		}catch(Exception e) {
        	throw new GeneralException("File read error ", e);
        } finally {
        	csvReader.close();
		}
		return listobj;
	}
	
	
	protected Field setFieldValue(Field field, Object dtoObject, String val) throws GeneralException {
	    try {
	    	if (field.getType() == String.class) {
    			field.set(dtoObject, val);
			} else if (field.getType() == float.class) {
				val = val.replaceAll(",", "");
				if(val.equals(Constants.EMPTY)){
					field.setFloat(dtoObject, new Double("0").floatValue());
				}else{
					field.setFloat(dtoObject, new Double(val).floatValue());	
				}
			} else if (field.getType() == double.class) {
				val = val.replaceAll(",", "");
				if(val.equals(Constants.EMPTY)){
					field.setDouble(dtoObject, new Double("0"));
				}else{
					field.setDouble(dtoObject, new Double(val));	
				}
			} else if (field.getType() == int.class) {
				val = val.replaceAll(",", "");
				if(val.equals(Constants.EMPTY)){
					field.setInt(dtoObject, new Double("0").intValue());
				}else{
					field.setInt(dtoObject, new Double(val).intValue());	
				}
			} else if (field.getType() == boolean.class) {
				field.setBoolean(dtoObject, new Boolean(val));
			} else if (field.getType() == Date.class) {
				SimpleDateFormat format = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
				Date date = format.parse(val);
				field.set(dtoObject, date);
			} 
	    } catch (Exception e) {
	    	throw new GeneralException("File Parser - Setting Values " , e);
	    }
		return field;
	}	
	
	
	protected Field setFieldValue(Field field, Object dtoObject, HSSFCell cell, List<String> upcList) throws GeneralException {
	    try {
	    	if (field.getType() == String.class) {
	    		if(cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC)
	    			field.set(dtoObject, String.valueOf(new Double(cell.getNumericCellValue()).intValue()));
	    		else
	    			field.set(dtoObject, cell.getStringCellValue().trim());
			} else if (field.getType() == float.class) {
				field.setFloat(dtoObject, new Double(cell.getNumericCellValue()).floatValue()); 	
			} else if (field.getType() == double.class) {
			    field.setDouble(dtoObject, cell.getNumericCellValue());
			} else if (field.getType() == int.class) {
				field.setInt(dtoObject, new Double(cell.getNumericCellValue()).intValue());
			} else if (field.getType() == boolean.class) {
				field.setBoolean(dtoObject, cell.getBooleanCellValue());
			} else if (field.getType() == Date.class) {
				field.set(dtoObject, cell.getDateCellValue());
			} 
		else if(field.getType() == List.class){
				field.set(dtoObject, upcList);
			}
			
	    } catch (Exception e) {
	    	String cellValue = null;
	    	if(cell.getCellType() == HSSFCell.CELL_TYPE_STRING)
	    		cellValue = cell.getStringCellValue();
	    	else if(cell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC)
	    		cellValue = String.valueOf(cell.getNumericCellValue());
	    	throw new GeneralException("File Parser - Setting Values " + field.getName() + "\t" + cellValue + "\t" + ((RawWeeklyAd)dtoObject).getItemDesc());
	    }
		return field;
	}	
	
	public void setFirstRowToProcess(int rowNumber){
		this.FIRST_ROW_TO_PROCESS = rowNumber;
	}

	public List<String> parseParticipatingBuyItems(String fileName) throws GeneralException{
		List<String> participatingItems = new ArrayList<String>();
		try{
			FileInputStream objXlsFile = new FileInputStream(fileName);
			HSSFWorkbook objWbook = new HSSFWorkbook(objXlsFile);
			HSSFSheet objSheet = objWbook.getSheet(Constants.PROMO_SHEETNAME);
			
			for(int i = 0 ; i < objSheet.getPhysicalNumberOfRows() ; i++){
				HSSFRow row = objSheet.getRow(i);
				if(row == null) continue;
				HSSFCell cell = row.getCell(8);
				String cellValue = cell.getStringCellValue();
				if(cellValue != null && cellValue.trim().length() > 0){
					participatingItems.add(cell.getStringCellValue());
				}
			}
			objXlsFile.close();
		}catch(IOException ioException){
			throw new GeneralException("Error when parsing participating items " + ioException);
		}catch(Exception e) {
        	throw new GeneralException("File read error ", e);
        } 
		return participatingItems;
	}
	
	public List<String> parseParticipatingFreeItems(String fileName) throws GeneralException{
		List<String> participatingItems = new ArrayList<String>();
		try{
			FileInputStream objXlsFile = new FileInputStream(fileName);
			HSSFWorkbook objWbook = new HSSFWorkbook(objXlsFile);
			HSSFSheet objSheet = objWbook.getSheet(Constants.PROMO_PARTICIPATING_PRODUCTS_SHEET_NAME);
			
			for(int i = 8 ; i < objSheet.getPhysicalNumberOfRows() ; i++){
				HSSFRow row = objSheet.getRow(i);
				if(row == null) continue;
				HSSFCell cell = row.getCell(12);
				String cellValue = cell.getStringCellValue();
				if(cellValue != null && cellValue.trim().length() > 0){
					participatingItems.add(cell.getStringCellValue());
				}
			}
			objXlsFile.close();
		}catch(IOException ioException){
			throw new GeneralException("Error when parsing participating items " + ioException);
		}catch(Exception e) {
        	throw new GeneralException("File read error ", e);
        } 
		return participatingItems;
	}
}
