package com.pristine.parsinginterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;


import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;


import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.offermgmt.weeklyad.RawWeeklyAd;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

public class ExcelFileParser<T> {
	String rootPath = null;
	private int FIRST_ROW_TO_PROCESS = 0;
	FormulaEvaluator formulaEval = null;	
	private Logger logger = Logger.getLogger("DataParser");
	
	public ExcelFileParser(){
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	}
	
	public ArrayList<String> getFiles(String specificPath) 
	{
		List<String> excelTypes = new ArrayList<String>();
		excelTypes.add("xls");
		excelTypes.add("xlsx");
		excelTypes.add("xlsm");
		ArrayList <String> fileList = getExcelFiles(specificPath, excelTypes);
		return fileList;
	}
	
	public ArrayList<String> getAllExcelVariantFiles(String specificPath) 
	{
		List<String> excelTypes = new ArrayList<String>();
		excelTypes.add("xls");
		excelTypes.add("xlsx");
		excelTypes.add("xlsm");
		ArrayList <String> fileList = getExcelFiles(specificPath, excelTypes);
		return fileList;
	}
	
	/**
	 * Returns Excel File List
	 * @param specificPath
	 * @return
	 * @throws GeneralException
	 */
	private ArrayList<String> getExcelFiles(String specificPath, List<String> excelTypes) 
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
			for (int i = 0; i < children.length; i++) {
				// Get filename of file or directory
				String filename = children[i];
				for (String excelType : excelTypes) {
					if (FilenameUtils.getExtension(filename).toLowerCase().equals(excelType)) {
						fileList.add(fullPath + "/" + filename);
						break;
					}
				}
			}
		}
		return fileList;
	}

	public List<T> parseExcelFile(Class returnObject, String fileName, int sheetNo, TreeMap<Integer, String> fieldNames) throws GeneralException{
		String sheetName = null;
		try{
			FileInputStream objXlsFile = new FileInputStream(fileName);
			Workbook objWbook = WorkbookFactory.create(objXlsFile);
			sheetName = objWbook.getSheetName(sheetNo);
			objXlsFile.close();
		}catch (IOException e) {
			e.printStackTrace();
		} catch (EncryptedDocumentException e) {
			e.printStackTrace();
		} catch (InvalidFormatException e) {
			e.printStackTrace();
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
			Workbook objWbook = WorkbookFactory.create(objXlsFile);
			formulaEval = objWbook.getCreationHelper().createFormulaEvaluator();
			Sheet objSheet = null;
			for(int i = 0; i < objWbook.getNumberOfSheets(); i++){
				if(objWbook.getSheetName(i).equalsIgnoreCase(sheetName) || objWbook.getSheetName(i).contains(sheetName)){
					objSheet = objWbook.getSheetAt(i);
					break;
				}
			}
			int recordCount = 0;
			int ignoreCount = 0;
			for (int i=FIRST_ROW_TO_PROCESS; i <= objSheet.getPhysicalNumberOfRows(); i++){
				Row row = objSheet.getRow(i);
				if(row == null) continue;
				Object objDTO = returnObject.newInstance();
				boolean status = true;
				for(Map.Entry<Integer, String> entry : fieldNames.entrySet()){
					if(status){
						Cell cell = row.getCell(entry.getKey());
						if(cell != null){
							try {
		                        Field field = null;
		                        field = returnObject.getDeclaredField(entry.getValue());
		                        field.setAccessible(true);
		                        field=setFieldValue(field, objDTO, cell);
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
			objXlsFile.close();
			objWbook.close();
		}catch(IOException ioException){
			throw new GeneralException("Error when reading excel file " + ioException);
		}catch(Exception e) {
        	throw new GeneralException("File read error ", e);
        } 
		return listobj;
	}
	
	/**
	 * Parses and stores columns in rows specified in fieldNames
	 * @param returnObject		Class
	 * @param fileName			File Name
	 * @param fieldNames		Promo Type as Key and Map containing Row-Column pos as Key, Field Name as Value
	 * @return
	 * @throws GeneralException
	 */
	public T parsePromoOverview(Class returnObject, String fileName, TreeMap<Integer, TreeMap<String, String>> fieldNames) throws GeneralException{
		Object objDTO = null;
		try{
			FileInputStream objXlsFile = new FileInputStream(fileName);
			Workbook objWbook = WorkbookFactory.create(objXlsFile);
			Sheet objSheet = objWbook.getSheet(Constants.PROMO_OVERVIEW_SHEETNAME);
			Row row = objSheet.getRow(7);
			Cell cell = null;
			int promoTypeId = -1;
			String promoType = row.getCell(2).getStringCellValue();
			if(promoType.contains(Constants.PROMO_OVERVIEW_TYPE_COMBO_DEAL)){
				promoTypeId = PromoTypeLookup.BUX_X_GET_Y_SAME.getPromoTypeId();
			}else if(promoType.contains(Constants.PROMO_OVERVIEW_TYPE_MUST_BUY)){
				promoTypeId = PromoTypeLookup.MUST_BUY.getPromoTypeId();
			}else if(promoType.contains(Constants.PROMO_OVERVIEW_TYPE_CATALINA) || promoType.contains(Constants.PROMO_OVERVIEW_TYPE_INSTANT_SAVINGS)){
				promoTypeId = Constants.PROMO_TYPE_CATALINA;
			}else if(promoType.contains(Constants.PROMO_OVERVIEW_TYPE_SUPER_COUPON)){
				promoTypeId = PromoTypeLookup.SUPER_COUPON.getPromoTypeId();
			}else if(promoType.contains(Constants.PROMO_OVERVIEW_TYPE_EBONUS_COUPON)){
				promoTypeId = Constants.PROMO_TYPE_EBONUS_COUPON;
			}else if(promoType.contains(Constants.PROMO_OVERVIEW_TYPE_MEAL_DEAL)){
				promoTypeId = PromoTypeLookup.MEAL_DEAL.getPromoTypeId();
			}else if(promoType.contains(Constants.PROMO_OVERVIEW_TYPE_GAS_POINTS)){
				promoTypeId = Constants.PROMO_TYPE_GAS_POINTS;
			}
			
			TreeMap<String, String> fieldNameMap = fieldNames.get(promoTypeId);
			if(fieldNameMap == null){
				logger.error("Error in parsing promo type");
				return null;
			}
			
			objDTO = returnObject.newInstance();
			try {
            	Field field = null;
                field = returnObject.getDeclaredField("promoTypeId");
                field.setAccessible(true);
                field.setInt(objDTO, promoTypeId);
			} catch (Exception e){
            	logger.warn("Parse Error " + e);
			} 
			
			for(Map.Entry<String, String> entry : fieldNameMap.entrySet()){
				String[] rowColPos = entry.getKey().split("-");
				int rowPos = Integer.parseInt(rowColPos[0]);
				int colPos = Integer.parseInt(rowColPos[1]);
				row = objSheet.getRow(rowPos);
				cell = row.getCell(colPos);
				if(cell != null){
					try {
		            	Field field = null;
		                field = returnObject.getDeclaredField(entry.getValue());
		                field.setAccessible(true);
		                field=setFieldValue(field, objDTO, cell);
					} catch (Exception e){
		            	logger.warn("Parse Error " + e);
					} catch (GeneralException e){
		            	logger.warn("Parse Error " + e);
					}
				}
			}
			objWbook.close();
			objXlsFile.close();
		}catch(IOException ioException){
			throw new GeneralException("Error when reading excel file " + ioException);
		}catch(Exception e) {
        	throw new GeneralException("File read error ", e);
        } 
		return (T)objDTO;
	}
	
//	protected Field setFieldValue(Field field, Object dtoObject, Cell cell) throws GeneralException {
//	    try {
//	    	if(cell != null){
//		    	if (field.getType() == String.class) {
//		    		if(cell.getCellType() == Cell.CELL_TYPE_NUMERIC){
//		    			if(cell.getCellStyle().getDataFormat() == 6){
//		    				String value = String.valueOf(new Double(cell.getNumericCellValue()).intValue());
//		    				if(value.indexOf("$") == -1){
//		    					value = "$" + value;
//		    				}
//		    				field.set(dtoObject, value);
//		    			}else{
//		    				field.set(dtoObject, String.valueOf(new Double(cell.getNumericCellValue()).intValue()));
//		    			}
//		    		}else if(cell.getCellType() == Cell.CELL_TYPE_FORMULA){
//		    			String value =	formulaEval.evaluate(cell).formatAsString();
//		    			field.set(dtoObject, value);
//		    		}
//		    		else
//		    			field.set(dtoObject, cell.getStringCellValue().trim());
//				} else if (field.getType() == float.class) {
//					if(cell.getCellType() == Cell.CELL_TYPE_STRING){
//						if(isNumber(cell.getStringCellValue()))
//							field.setFloat(dtoObject, new Double(cell.getStringCellValue()).floatValue());
//						else
//							field.setFloat(dtoObject, 0);
//					}
//					else if(cell.getCellType() == Cell.CELL_TYPE_FORMULA){
//		    			String value =	formulaEval.evaluate(cell).formatAsString();
//		    			if(isNumber(value))
//		    				field.setFloat(dtoObject, Float.valueOf(value));
//		    			else
//		    				field.setFloat(dtoObject, 0);
//		    		}
//					else
//						field.setFloat(dtoObject, new Double(cell.getNumericCellValue()).floatValue());
//				} else if (field.getType() == double.class) {
//					if(cell.getCellType() == Cell.CELL_TYPE_STRING){
//						if(isNumber(cell.getStringCellValue()))
//							field.setDouble(dtoObject, new Double(cell.getStringCellValue()));
//						else
//							field.setDouble(dtoObject, 0);
//					}
//					else if(cell.getCellType() == Cell.CELL_TYPE_FORMULA){
//		    			String value =	formulaEval.evaluate(cell).formatAsString();
//		    			if(isNumber(value))
//		    				field.setDouble(dtoObject, Double.valueOf(value));
//		    			else
//		    				field.setFloat(dtoObject, 0);
//		    		}
//					else
//						field.setDouble(dtoObject, cell.getNumericCellValue());
//				} else if (field.getType() == int.class) {
//					if(cell.getCellType() == Cell.CELL_TYPE_STRING){
//						if(isNumber(cell.getStringCellValue()))
//							field.setInt(dtoObject, new Double(cell.getStringCellValue()).intValue());
//						else
//							field.setInt(dtoObject, 0);
//					}
//					else if(cell.getCellType() == Cell.CELL_TYPE_FORMULA){
//		    			String value =	formulaEval.evaluate(cell).formatAsString();
//		    			if(isNumber(value))
//		    				field.setInt(dtoObject, Integer.valueOf(value));
//		    			else
//		    				field.setInt(dtoObject, 0);
//		    		}
//					else
//						field.setInt(dtoObject, new Double(cell.getNumericCellValue()).intValue());
//				} else if (field.getType() == boolean.class) {
//					field.setBoolean(dtoObject, cell.getBooleanCellValue());
//				} else if (field.getType() == Date.class) {
//					field.set(dtoObject, cell.getDateCellValue());
//				} 
//	    	}
//	    } catch (Exception e) {
//	    	String cellValue = null;
//	    	if(cell.getCellType() == Cell.CELL_TYPE_STRING)
//	    		cellValue = cell.getStringCellValue();
//	    	else if(cell.getCellType() == Cell.CELL_TYPE_NUMERIC)
//	    		cellValue = String.valueOf(cell.getNumericCellValue());
//	    	e.printStackTrace();
//	    	throw new GeneralException("File Parser - Setting Values " + field.getName() + "\t" + cellValue + "\t" + ((RawWeeklyAd)dtoObject).getItemDesc());
//	    	
//	    }
//		return field;
//	}	
	
	public void setFirstRowToProcess(int rowNumber){
		this.FIRST_ROW_TO_PROCESS = rowNumber;
	}
	
	
	public boolean isNumber( String input ) {
	    try {
	        Integer.parseInt( input );
	        return true;
	    }
	    catch( Exception e ) {
	        return false;
	    }
	}

	public List<ItemDetailKey> parseParticipatingBuyItems(String fileName) throws GeneralException{
		List<ItemDetailKey> participatingItems = new ArrayList<ItemDetailKey>();
		try{
			FileInputStream objXlsFile = new FileInputStream(fileName);
			Workbook objWbook = WorkbookFactory.create(objXlsFile);
			Sheet objSheet = objWbook.getSheet(Constants.PROMO_PARTICIPATING_PRODUCTS_SHEET_NAME);
			
			for(int i = 8 ; i < objSheet.getPhysicalNumberOfRows() ; i++){
				Row row = objSheet.getRow(i);
				if(row == null) continue;
				String cellUPC = "";
				String cellSourceVendor = "";
				String cellItemNumber = "";
				Cell cell = row.getCell(2);
				if(cell != null)
					if(Cell.CELL_TYPE_NUMERIC ==  cell.getCellType())
						cellUPC = String.valueOf(cell.getNumericCellValue()).replace("-", "");
					else
						cellUPC = cell.getStringCellValue().replace("-", "");
				cell = row.getCell(0);
				if(cell != null)
					if(Cell.CELL_TYPE_NUMERIC ==  cell.getCellType())
						cellSourceVendor = String.valueOf((int)cell.getNumericCellValue());
					else
						cellSourceVendor = cell.getStringCellValue();
				
				cell = row.getCell(1);
				if(cell != null)
					if(Cell.CELL_TYPE_NUMERIC ==  cell.getCellType())
						cellItemNumber = String.valueOf((int)cell.getNumericCellValue());
					else
						cellItemNumber = cell.getStringCellValue();
				if((cellUPC != null && cellUPC.trim().length() > 0)
						&& (cellSourceVendor != null && cellSourceVendor.trim().length() > 0)
						&& (cellItemNumber != null && cellItemNumber.trim().length() > 0)){
					cellSourceVendor = String.valueOf((int) Double.parseDouble(cellSourceVendor));
					cellItemNumber = String.valueOf((int) Double.parseDouble(cellItemNumber));
					String zero = "0";
					if(cellItemNumber.length() < 6){
						for(int j=0; j<(7 - cellItemNumber.length()); j++){
							cellItemNumber = zero + cellItemNumber;
						}
					}
					if(cellSourceVendor.length() < 6){
						for(int j=0; j<(7 - cellSourceVendor.length()); j++){
							cellSourceVendor = zero + cellSourceVendor;
						}
					}
					String retailerItemCode = cellSourceVendor + cellItemNumber;
					
					ItemDetailKey itemDetailKey = new ItemDetailKey(PrestoUtil.castUPC(cellUPC, false), retailerItemCode);
					participatingItems.add(itemDetailKey);
				}
			}
			objXlsFile.close();
		}catch(IOException ioException){
			ioException.printStackTrace();
			throw new GeneralException("Error when parsing participating items " + ioException);
			}catch(Exception e) {
				e.printStackTrace();
				throw new GeneralException("File read error ", e);
        	
        } 
		return participatingItems;
	}
	
	public List<ItemDetailKey> parseParticipatingFreeItems(String fileName) throws GeneralException{
		List<ItemDetailKey> participatingItems = new ArrayList<ItemDetailKey>();
		try{
			FileInputStream objXlsFile = new FileInputStream(fileName);
			Workbook objWbook = WorkbookFactory.create(objXlsFile);
			Sheet objSheet = objWbook.getSheet(Constants.PROMO_PARTICIPATING_PRODUCTS_SHEET_NAME);
			
			for(int i = 8 ; i < objSheet.getPhysicalNumberOfRows() ; i++){
				Row row = objSheet.getRow(i);
				if(row == null) continue;
				String cellUPC = "";
				String cellSourceVendor = "";
				String cellItemNumber = "";
				Cell cell = row.getCell(12);
				if(cell != null)
					if(Cell.CELL_TYPE_NUMERIC ==  cell.getCellType())
						cellUPC = String.valueOf(cell.getNumericCellValue());
					else
						cellUPC = cell.getStringCellValue();
				cell = row.getCell(10);
				if(cell != null)
					if(Cell.CELL_TYPE_NUMERIC ==  cell.getCellType())
						cellSourceVendor = String.valueOf(cell.getNumericCellValue());
					else
						cellSourceVendor = cell.getStringCellValue();
				cell = row.getCell(11);
				if(cell != null)
					if(Cell.CELL_TYPE_NUMERIC ==  cell.getCellType())
						cellItemNumber = String.valueOf(cell.getNumericCellValue());
					else
						cellItemNumber = cell.getStringCellValue();
				if((cellUPC != null && cellUPC.trim().length() > 0)
						&& (cellSourceVendor != null && cellSourceVendor.trim().length() > 0)
						&& (cellItemNumber != null && cellItemNumber.trim().length() > 0)){
					
					cellItemNumber = String.valueOf((int) Double.parseDouble(cellItemNumber));
					if(cellItemNumber.length() < 6){
						String zero = "0";
						for(int j=0; j<(6 - cellItemNumber.length()); j++){
							cellItemNumber = zero + cellItemNumber;
						}
					}
					String retailerItemCode = cellSourceVendor + cellItemNumber;
					
					ItemDetailKey itemDetailKey = new ItemDetailKey(PrestoUtil.castUPC(cellUPC, false), retailerItemCode);
					participatingItems.add(itemDetailKey);
				
				/*Cell cell = row.getCell(12);
				String cellUPC = cell.getStringCellValue();
				cell = row.getCell(10);
				String cellSourceVendor = cell.getStringCellValue();
				cell = row.getCell(11);
				String cellItemNumber = cell.getStringCellValue();
				if((cellUPC != null && cellUPC.trim().length() > 0)
						&& (cellSourceVendor != null && cellSourceVendor.trim().length() > 0)
						&& (cellItemNumber != null && cellItemNumber.trim().length() > 0)){
					String retailerItemCode = cellSourceVendor + cellItemNumber;
					ItemDetailKey itemDetailKey = new ItemDetailKey(PrestoUtil.castUPC(cellUPC, false), retailerItemCode);
					participatingItems.add(itemDetailKey);*/
				}

			}
			objXlsFile.close();
		}catch(IOException ioException){
			throw new GeneralException("Error when parsing participating items " + ioException);
		}catch(Exception e) {
			e.printStackTrace();
        	throw new GeneralException("File read error ", e);
        } 
		return participatingItems;
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
	public List<T> parseExcelFileV2(Class returnObject, String fileName, String sheetName, TreeMap<Integer, String> fieldNames) throws GeneralException{
		List<T> listobj = new ArrayList<T>();
		try{
			FileInputStream objXlsFile = new FileInputStream(fileName);
			Workbook objWbook = WorkbookFactory.create(objXlsFile);
			formulaEval = objWbook.getCreationHelper().createFormulaEvaluator();
			Sheet objSheet = null;
			for(int i = 0; i < objWbook.getNumberOfSheets(); i++){
				if(objWbook.getSheetName(i).equalsIgnoreCase(sheetName) || objWbook.getSheetName(i).contains(sheetName)){
					objSheet = objWbook.getSheetAt(i);
					break;
				}
			}
			int recordCount = 0;
			int ignoreCount = 0;
			for (int i=FIRST_ROW_TO_PROCESS; i <= objSheet.getPhysicalNumberOfRows(); i++){
				Row row = objSheet.getRow(i);
				if(row == null) continue;
				Object objDTO = returnObject.newInstance();
				boolean status = true;
				for(Map.Entry<Integer, String> entry : fieldNames.entrySet()){
					if(status){
						Cell cell = row.getCell(entry.getKey());
						if(cell != null){
							try {
		                        Field field = null;
		                        field = returnObject.getDeclaredField(entry.getValue());
		                        field.setAccessible(true);
		                        field=setFieldValue(field, objDTO, cell);
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
			ioException.printStackTrace();
			throw new GeneralException("Error when reading excel file " + ioException);
		}catch(Exception e) {
			e.printStackTrace();
        	throw new GeneralException("File read error ", e);
        } 
		return listobj;
	}

	protected Field setFieldValue(Field field, Object dtoObject, Cell cell) throws GeneralException {
	    try {
	    	if(cell != null){
		    	if (field.getType() == String.class) {
		    		if(cell.getCellType() == Cell.CELL_TYPE_NUMERIC){
		    			if(cell.getCellStyle().getDataFormat() == 6){
		    				String value = String.valueOf(new Double(cell.getNumericCellValue()).doubleValue());
		    				if(value.indexOf("$") == -1){
		    					value = "$" + value;
		    				}
		    				field.set(dtoObject, value);
		    			}else{
		    				field.set(dtoObject, String.valueOf(new Double(cell.getNumericCellValue()).doubleValue()));
		    			}
		    		}else if(cell.getCellType() == Cell.CELL_TYPE_FORMULA){
		    			String value =	formulaEval.evaluate(cell).formatAsString();
		    			field.set(dtoObject, value);
		    		}
		    		else
		    			field.set(dtoObject, cell.getStringCellValue().trim());
				} else if (field.getType() == float.class) {
					if(cell.getCellType() == Cell.CELL_TYPE_STRING){
						if(isNumber(cell.getStringCellValue()))
							field.setFloat(dtoObject, new Double(cell.getStringCellValue()).floatValue());
						else if(isDouble(cell.getStringCellValue())){
							field.setFloat(dtoObject, new Double(cell.getStringCellValue()).floatValue());
						}
						else
							field.setFloat(dtoObject, 0);
					}
					else if(cell.getCellType() == Cell.CELL_TYPE_FORMULA){
		    			String value =	formulaEval.evaluate(cell).formatAsString();
		    			if(isNumber(value))
		    				field.setFloat(dtoObject, Float.valueOf(value));
		    			else
		    				field.setFloat(dtoObject, 0);
		    		}
					else
						field.setFloat(dtoObject, new Double(cell.getNumericCellValue()).floatValue());
				} else if (field.getType() == double.class) {
					if(cell.getCellType() == Cell.CELL_TYPE_STRING){
						if(isNumber(cell.getStringCellValue()))
							field.setDouble(dtoObject, new Double(cell.getStringCellValue()));
						else
							field.setDouble(dtoObject, 0);
					}
					else if(cell.getCellType() == Cell.CELL_TYPE_FORMULA){
		    			String value =	formulaEval.evaluate(cell).formatAsString();
		    			if(isNumber(value))
		    				field.setDouble(dtoObject, Double.valueOf(value));
		    			else
		    				field.setFloat(dtoObject, 0);
		    		}
					else
						field.setDouble(dtoObject, cell.getNumericCellValue());
				} else if (field.getType() == int.class) {
					if(cell.getCellType() == Cell.CELL_TYPE_STRING){
						if(isNumber(cell.getStringCellValue()))
							field.setInt(dtoObject, new Double(cell.getStringCellValue()).intValue());
						else
							field.setInt(dtoObject, 0);
					}
					else if(cell.getCellType() == Cell.CELL_TYPE_FORMULA){
		    			String value =	formulaEval.evaluate(cell).formatAsString();
		    			if(isNumber(value))
		    				field.setInt(dtoObject, Integer.valueOf(value));
		    			else
		    				field.setInt(dtoObject, 0);
		    		}
					else
						field.setInt(dtoObject, new Double(cell.getNumericCellValue()).intValue());
				} else if (field.getType() == boolean.class) {
					field.setBoolean(dtoObject, cell.getBooleanCellValue());
				} else if (field.getType() == Date.class) {
					field.set(dtoObject, cell.getDateCellValue());
				} 
	    	}
	    } catch (Exception e) {
	    	String cellValue = null;
	    	if(cell.getCellType() == Cell.CELL_TYPE_STRING)
	    		cellValue = cell.getStringCellValue();
	    	else if(cell.getCellType() == Cell.CELL_TYPE_NUMERIC)
	    		cellValue = String.valueOf(cell.getNumericCellValue());
	    	e.printStackTrace();
	    	throw new GeneralException("File Parser - Setting Values " + field.getName() + "\t" + cellValue + "\t" + ((RawWeeklyAd)dtoObject).getItemDesc());
	    	
	    }
		return field;
	}	
	public boolean isDouble(String value){
		try
		{
		  Double.parseDouble(value);
		  return true;
		}
		catch(Exception e)
		{
			return false;
		}
	}
}
