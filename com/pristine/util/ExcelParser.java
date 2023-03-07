package com.pristine.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import com.pristine.dto.ForecastDto;
import com.pristine.dto.salesanalysis.BudgetDto;
import com.pristine.exception.GeneralException;


public class ExcelParser {
	
	 private static Logger logger = Logger.getLogger(ExcelParser.class);
	 	  
	  
	 /*
	  * Method used read the excel file and add cell values into list
	  * Argument 1 : File Name
	  * Argument 2 : File Path
	  * Argument 3 : Sheet Name
	  * Argument 4 : File Type
	  * Argument 5 : Start Column
	  * Argument 6 : Start Date
	  * Argument 7 : End Date
	  * Argument 8 : Row Number 
	  * @catch Exception
	  * @throws IoException
	  */
	 
	 
	public List<ForecastDto> forecastExcelParser(String fileName,
			String sheetName,  int startColumn,
			Date startdate, Date endDate, int storeLength, int processingRow, String mode)   {
		
		List<ForecastDto> foreCastList =  new ArrayList<ForecastDto>();
			
		// read the excel file using Fileinputstream reader
		FileInputStream inputFileStream = null;
		try {
			inputFileStream = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			logger.error(e);
		 }
		
		
		HSSFWorkbook inputExcel = null;
		try {
			inputExcel = new HSSFWorkbook(inputFileStream);
		} catch (IOException e) {
			logger.error(e);	
		}
		
		HSSFSheet sheet = inputExcel.getSheetAt(Integer.parseInt(sheetName));
			
		int rows = sheet.getPhysicalNumberOfRows();
						
		try{
		
			for (int ii = 2; ii < rows; ii++) {

				// Object for foreCastDto
				ForecastDto objForeCastDto = new ForecastDto();
				HSSFRow row = sheet.getRow(ii);
				if(row != null && row.getCell(0) != null){
				    HSSFCell processdate = row.getCell(0);

				objForeCastDto.setProcessDate(processdate.getDateCellValue());

				// get the store number
				HSSFCell storeCell = row.getCell(1);
				objForeCastDto.setStoreNumber(getStoreNumber(storeLength,storeCell.getNumericCellValue()));
				
				if (endDate == null) {
					if (startdate.before(objForeCastDto.getProcessDate())) {
						//logger.info("Process Date " +objForeCastDto.getProcessDate());
						if (startColumn == 0) {
							getStartingColumn(row, objForeCastDto, processingRow);
							if(mode.equalsIgnoreCase("ForecastLoader"))
							getEndingColumn(row, objForeCastDto, processingRow);
						} else {
							getEndingColumn(row, objForeCastDto, processingRow);
						}
						foreCastList.add(objForeCastDto);
						processingRow++;
					}
				}else if (!mode.equalsIgnoreCase("ForecastLoader")  && startdate.before(objForeCastDto.getProcessDate())
						&& endDate.after(objForeCastDto.getProcessDate())) {
					//logger.info("Process Date " +objForeCastDto.getProcessDate());
					if (startColumn == 0) {
						getStartingColumn(row, objForeCastDto, processingRow);
						// if(mode.equalsIgnoreCase("ForecastLoader"))
						// getEndingColumn(row, objForeCastDto, processingRow);
					} else {
						getEndingColumn(row, objForeCastDto, processingRow);
					}
					foreCastList.add(objForeCastDto);
					processingRow++;
				}else if (mode.equalsIgnoreCase("ForecastLoader") && startdate.before(objForeCastDto.getProcessDate())
						&& endDate.compareTo(objForeCastDto.getProcessDate()) == 0) {
					//logger.info("Process Date " +objForeCastDto.getProcessDate());
					if (startColumn == 0) {
						getStartingColumn(row, objForeCastDto, processingRow);
						getEndingColumn(row, objForeCastDto, processingRow);
					} else {
						getEndingColumn(row, objForeCastDto, processingRow);
					}
					foreCastList.add(objForeCastDto);
					processingRow++;
				}

				// Added for Daily Revenue Correction - Janani
				if(mode.equals(Constants.DAILY_REVENUE_CORRECTION_MODE)){
					if (endDate.compareTo(objForeCastDto.getProcessDate()) == 0){
						getStartingColumn(row, objForeCastDto, processingRow);
						foreCastList.add(objForeCastDto);
						processingRow++;
					}
				}
			}
			}
			logger.debug(" Total Number of Rows :" + processingRow);
			
		}catch(Exception exe){
			logger.error("Error in Parse Excel " ,exe);
			 
		}
		
		return foreCastList;
	}

	/**
	 * Returns latest date in forecast file
	 * @param fileName		Forecast file name
	 * @param sheetName		Excel sheet name
	 * @return
	 */
	public Date getLatestDateInFile(String fileName, String sheetName)   {
		
		Date latestDateInFile = null;
		logger.debug("Get latest date from file : " + fileName );	
			
		// read the excel file using Fileinputstream reader
		FileInputStream inputFileStream = null;
		try {
			inputFileStream = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			logger.error("Forecast file not found: " + e);
			return null;
		 }
		
		HSSFWorkbook inputExcel = null;
		try {
			inputExcel = new HSSFWorkbook(inputFileStream);
		} catch (IOException e) {
			logger.error("Forecast file does not have the sheet mentioned: " + e);
			return null;
		}
		
		HSSFSheet sheet = inputExcel.getSheetAt(Integer.parseInt(sheetName));
			
		int rows = sheet.getPhysicalNumberOfRows();
						
		try{
		
			HSSFRow row = sheet.getRow(rows - 1);
				
			if( row.getCell(0) != null){
				HSSFCell processdate = row.getCell(0);
				latestDateInFile = processdate.getDateCellValue();
			}
		}catch(Exception exe){
			logger.error("Error in Parsing Excel " + exe);
		}
		
		return latestDateInFile;
	}
	
	private void getEndingColumn(HSSFRow row, ForecastDto objForeCastDto, int processingRow){
		
		try{
		
		// Sunday record for with gas 
		HSSFCell sunday_wg = row.getCell(13); 
		// logger.info("sunday_wg.getNumericCellValue()" +sunday_wg.getNumericCellValue());
		objForeCastDto.setSundayWg(checkCelltype(sunday_wg));
		
		// Monday record for with gas
		HSSFCell monday_wg = row.getCell(14); 
		// logger.info("monday_wg.getNumericCellValue()()"+ monday_wg.getNumericCellValue());
		objForeCastDto.setMondayWg(checkCelltype(monday_wg));
		
		 // Tuesday record for with gas
		HSSFCell tuesday_wg = row.getCell(15);
		// logger.info("tuesday_wg.getNumericCellValue()"+tuesday_wg.getNumericCellValue());
		objForeCastDto.setTuesdayWg(checkCelltype(tuesday_wg));
		
		// Wednesday record for with gas
		HSSFCell wednesday_wg = row.getCell(16); 
		// logger.info("wednesday_wg.getNumericCellValue()"+wednesday_wg.getNumericCellValue());
		objForeCastDto.setWednesdayWg(checkCelltype(wednesday_wg));
		
		// Thursday record for with gas
		HSSFCell thursday_wg = row.getCell(17); 
		objForeCastDto.setThursdayWg(checkCelltype(thursday_wg));
		
		// Friday record for with gas
		HSSFCell friday_wg = row.getCell(18); 
		objForeCastDto.setFridayWg(checkCelltype(friday_wg));
		
		// Saturday record for with gas
		HSSFCell saturday_wg = row.getCell(19); 
		objForeCastDto.setSaterdayWg(checkCelltype(saturday_wg));
		}catch(Exception exe){
			logger.error(exe);
			logger.error("Error In File Reading : Row Number "+processingRow);
		}
		
	}

	private Object checkCelltype(HSSFCell cell) {
		
		Object value = 0;
		
		if( cell !=null){
		
		switch (cell.getCellType()) {

		case HSSFCell.CELL_TYPE_NUMERIC:
			value = cell.getNumericCellValue();
			break;
			
		case HSSFCell.CELL_TYPE_STRING:
			value = 0;
			break;
			
		case HSSFCell.CELL_TYPE_BLANK:
			value = 0;
			break;
			
		case HSSFCell.CELL_TYPE_ERROR:
			value = 0;
			break;
			
		}
		}
		return value;
	}

	private String getStoreNumber(int storeLength, Object Actual) {
		
		String storeNumebr = String.valueOf(Actual).substring(0, String.valueOf(Actual).indexOf("."));
		try{
		int actualpadding =   storeLength-storeNumebr.length();
		String missingPadding = "";
		for(int i=0 ; i<actualpadding ;i++){
			missingPadding += "0";
		}
		storeNumebr =missingPadding+""+storeNumebr;
		//logger.debug("Final Store Number is " +storeNumebr  );
		}catch(Exception exe){
			logger.error(exe);
		}
		return storeNumebr;
	}

	
	private void getStartingColumn(HSSFRow row, ForecastDto objForeCastDto, int processingRow) {
		
		try{
			
		// Sunday record for without gas
		HSSFCell sunday_wog = row.getCell(2); 
		//logger.info("sunday_wog.getNumericCellValue()" +sunday_wog.getNumericCellValue());
		objForeCastDto.setSundayWog(checkCelltype(sunday_wog));
		
		// Monday record for without gas
		HSSFCell monday_wog = row.getCell(3); 
		//logger.info("monday_wog.getNumericCellValue()()"+monday_wog.getNumericCellValue());
		objForeCastDto.setMondayWog(checkCelltype(monday_wog));
		
		// Tuesday record for without gas
		HSSFCell tuesday_wog = row.getCell(4); 
		//logger.info("tuesday_wog.getNumericCellValue()"+tuesday_wog.getNumericCellValue());
		objForeCastDto.setTuesdayWog(checkCelltype(tuesday_wog));
		
		// Wednesday record for without gas
		HSSFCell wednesday_wog = row.getCell(5); 
		//logger.info("wednesday_wog.getNumericCellValue()"+wednesday_wog.getNumericCellValue());
		objForeCastDto.setWednesdayWog(checkCelltype(wednesday_wog));
		
		// Thursday record for without gas
		HSSFCell thursday_wog = row.getCell(6); 
		objForeCastDto.setThursdayWog(checkCelltype(thursday_wog));
		//logger.info("Th.getNumericCellValue()"+thursday_wog.getNumericCellValue());
		// Friday record for without gas
		HSSFCell friday_wog = row.getCell(7); 
		objForeCastDto.setFridayWog(checkCelltype(friday_wog));
		//logger.info("Fri.getNumericCellValue()"+friday_wog.getNumericCellValue());
		// Saturday record for without gas
		HSSFCell saturday_wog = row.getCell(8); 
		objForeCastDto.setSaterdayWog(checkCelltype(saturday_wog));
		//logger.info("Sta.getNumericCellValue()"+saturday_wog.getNumericCellValue());
		}catch(Exception exe){
			logger.error(exe);
			logger.error("Error In File Reading : Row Number "+processingRow);
		}
		
	}
	
	
	public ArrayList<String> getFiles(String rootPath, String specificPath)
			throws GeneralException {
		String fullPath = rootPath;
		if (specificPath != null && specificPath.trim().length() > 0) {
			fullPath = fullPath + "/" + specificPath;
		}

		ArrayList<String> fileList = new ArrayList<String>();

		File dir = new File(fullPath);

		String[] children = dir.list();
		if (children != null) {
			for (int i = 0; i < children.length; i++) {
				// Get filename of file or directory
				String filename = children[i];
				// logger.debug(filename);
				if (filename.contains(".xls"))
					fileList.add(fullPath + "/" + filename);
			}
		}
		return fileList;
	}

	/*
	 * Method used to parse the budget excel file ., and read the excel sheet in
	 * onebyone, and summing the results and insert the values into DB 
	 * Argument 1 : fileName Argument 2 : productGroupList Argument 3 : Start Period
	 * Argument 4 : End Period
	 * 
	 * @catch Exception
	 * 
	 * @throws GendralException....
	 */
	/**
	 * @param fileName
	 * @param calendarMap
	 * @param gasProductId
	 * @param methodName
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, BudgetDto> budgetExcelParser(String fileName,
			HashMap<String, Integer> calendarMap, String methodName) throws GeneralException {

		logger.info("..................... Excel Parser Method starts ");

		HashMap<String, BudgetDto> BudgetMap = new HashMap<String, BudgetDto>();
		
		Object productId = null;     // hold the excel file product id

		FileInputStream inputFileStream = null;
		try {
			inputFileStream = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			logger.error(" File Not Found ", e);
			throw new GeneralException(" File Not Found" ,e);
		}

		HSSFWorkbook inputExcel = null;
		try {
			inputExcel = new HSSFWorkbook(inputFileStream);
		} catch (IOException e) {
			logger.error(" IoException occured while reading the excel file", e);
			throw new GeneralException(" IoException occured while reading the excel file" ,e);
		}

		int no_rows = inputExcel.getNumberOfSheets();
		logger.info(" Test No of rows" + no_rows);

		int processRow = 0;
		// itreate the product group list through for loop., and get the product
		// id and product name (sheet name)

		try {

			for (int sheetIndex = 0; sheetIndex < no_rows; sheetIndex++) {

				HSSFSheet sheet = inputExcel.getSheetAt(sheetIndex);
				// logger.info(" Sheet Name " + sheet.getSheetName());

				int sheetRows = sheet.getPhysicalNumberOfRows();

				processRow = 0;

				for (int cols = 0; cols < sheetRows; cols++) {

					BudgetDto objbudgetDto = new BudgetDto();

					// used to get the row
					HSSFRow row = sheet.getRow(cols);
					
					objbudgetDto.setProductLevelId(Constants.FINANCEDEPARTMENT);

					if (processRow == 0) {
					// get the product id from excel file
						productId = row.getCell(0).getStringCellValue(); 
					}
					else 
					if (row.getCell(0) != null && row.getCell(1) != null
							&& row.getCell(2) != null) {

						// get the location id (Store id) from excel file
						objbudgetDto.setLocationId(row.getCell(0).getStringCellValue());
						objbudgetDto.setLocationLevelId(Constants.STORE_LEVEL_ID);
						objbudgetDto.setProductId(productId);
						
						getPeriodDetails(objbudgetDto, row);
						
					/*	if (methodName.equalsIgnoreCase("ACCOUNTS")) {
							getPeriodDetails(objbudgetDto, row);
						} else if (methodName.equalsIgnoreCase("BUDGET")) {
						
															
							if (Integer.parseInt((String) productId) == gasProductId) {
									getGasPeriodDetails(objbudgetDto, row);

							} else {
								getPeriodDetails(objbudgetDto, row);
							}

						}*/
						
									
						// for budget process						
														
						BudgetMap.put(sheet.getSheetName() + "_"+ objbudgetDto.getLocationId(), objbudgetDto);
					}

					processRow++;
				}

			}

		} catch (Exception exe) {
			logger.error(" Error in Excel reading ..... Line No " + processRow + ".....Error " + exe);
		}

		return BudgetMap;
	}

	/*private void getGasPeriodDetails(BudgetDto objbudgetDto, HSSFRow row) {

		// get the period 01 budget
		objbudgetDto.setGasperiod_01(checkCelltypeString(row.getCell(1)));

		// get the period 02 budget
		objbudgetDto
				.setGasperiod_02(checkCelltypeString(row.getCell(2)));

		// get the period 03 budget
		objbudgetDto
				.setGasperiod_03(checkCelltypeString(row.getCell(3)));

		// get the period 04 budget
		objbudgetDto
				.setGasperiod_04(checkCelltypeString(row.getCell(4)));

		// get the period 05 budget
		objbudgetDto
				.setGasperiod_05(checkCelltypeString(row.getCell(5)));

		// get the period 06 budget
		objbudgetDto
				.setGasperiod_06(checkCelltypeString(row.getCell(6)));

		// get the period 07 budget
		objbudgetDto
				.setGasperiod_07(checkCelltypeString(row.getCell(7)));

		// get the period 08 budget
		objbudgetDto
				.setGasperiod_08(checkCelltypeString(row.getCell(8)));

		// get the period 09 budget
		objbudgetDto
				.setGasperiod_09(checkCelltypeString(row.getCell(9)));

		// get the period 10 budget
		objbudgetDto
				.setGasperiod_10(checkCelltypeString(row.getCell(10)));

		// get the period 11 budget
		objbudgetDto
				.setGasperiod_11(checkCelltypeString(row.getCell(11)));

		// get the period 12 budget
		objbudgetDto
				.setGasperiod_12(checkCelltypeString(row.getCell(12)));

		// get the period 13 budget
		objbudgetDto.setGasperiod_13(checkCelltypeString(row.getCell(13)));
		
	}
*/

	private void getPeriodDetails(BudgetDto objbudgetDto, HSSFRow row) {
		 
		


		// get the period 01 budget
		objbudgetDto
				.setPeriod_01(checkCelltypeString(row.getCell(1)));

		// get the period 02 budget
		objbudgetDto
				.setPeriod_02(checkCelltypeString(row.getCell(2)));

		// get the period 03 budget
		objbudgetDto
				.setPeriod_03(checkCelltypeString(row.getCell(3)));

		// get the period 04 budget
		objbudgetDto
				.setPeriod_04(checkCelltypeString(row.getCell(4)));

		// get the period 05 budget
		objbudgetDto
				.setPeriod_05(checkCelltypeString(row.getCell(5)));

		// get the period 06 budget
		objbudgetDto
				.setPeriod_06(checkCelltypeString(row.getCell(6)));

		// get the period 07 budget
		objbudgetDto
				.setPeriod_07(checkCelltypeString(row.getCell(7)));

		// get the period 08 budget
		objbudgetDto
				.setPeriod_08(checkCelltypeString(row.getCell(8)));

		// get the period 09 budget
		objbudgetDto
				.setPeriod_09(checkCelltypeString(row.getCell(9)));

		// get the period 10 budget
		objbudgetDto
				.setPeriod_10(checkCelltypeString(row.getCell(10)));

		// get the period 11 budget
		objbudgetDto
				.setPeriod_11(checkCelltypeString(row.getCell(11)));

		// get the period 12 budget
		objbudgetDto
				.setPeriod_12(checkCelltypeString(row.getCell(12)));

		// get the period 13 budget
		objbudgetDto.setPeriod_13(checkCelltypeString(row.getCell(13)));
		
	}


	private double checkCelltypeString(HSSFCell cell) {
		 
		double value = 0;
		
		switch (cell.getCellType()) {
			
		case HSSFCell.CELL_TYPE_STRING:
			value = 0;
			break;
		
		case HSSFCell.CELL_TYPE_NUMERIC:
			value = cell.getNumericCellValue();
			break;
			
		}
	
		return value;
	}

	public String getDateInWeeklyReport(String fileName){
		FileInputStream inputFileStream = null;
		try {
			inputFileStream = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			logger.error(e);
		 }


		HSSFWorkbook inputExcel = null;
		try {
			inputExcel = new HSSFWorkbook(inputFileStream);
		} catch (IOException e) {
			logger.error(e);	
		}

		HSSFSheet sheet = inputExcel.getSheetAt(0);
		HSSFRow row = sheet.getRow(3);
		HSSFCell cell = row.getCell(2);
		String weekEndDate = cell.getStringCellValue();
		return weekEndDate;
	}
	
	public HashMap<String, Double[]> weeklyReportExcelParser(String fileName, int storeLength, int processingRow, String processingMode){
		
		HashMap<String, Double[]> revenueMapForFinanceDept = new HashMap<String, Double[]>();
		logger.debug("Processing File Name : " + fileName );	
			
		// read the excel file using Fileinputstream reader
		FileInputStream inputFileStream = null;
		try {
			inputFileStream = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			logger.error(e);
		 }
		
		
		HSSFWorkbook inputExcel = null;
		try {
			inputExcel = new HSSFWorkbook(inputFileStream);
		} catch (IOException e) {
			logger.error(e);	
		}

		HSSFSheet sheet = null;
		
		if(Constants.WEEKLY_REVENUE_CORRECTION_FD.equals(processingMode)){
			sheet = inputExcel.getSheetAt(inputExcel.getNumberOfSheets() - 1);
		}else if(Constants.WEEKLY_REVENUE_CORRECTION_STORE.equals(processingMode)){
			sheet = inputExcel.getSheetAt(inputExcel.getNumberOfSheets() - 2);
		}
			
		int rows = sheet.getPhysicalNumberOfRows();
		int count = 0;	
		try{
		
			for (int ii = 0; ii < rows; ii++) {

				HSSFRow row = sheet.getRow(ii);
				if(row != null){
					HSSFCell storeNumberCell = row.getCell(0);
					if(storeNumberCell != null && 
						storeNumberCell.getCellType() == HSSFCell.CELL_TYPE_NUMERIC){
						
						if(Constants.WEEKLY_REVENUE_CORRECTION_FD.equals(processingMode)){
							Double[] financeDeptRevenue = new Double[15];
							count = 0;
							
							HSSFCell groceryCell = row.getCell(3);
							financeDeptRevenue[count++] = groceryCell.getNumericCellValue();
							
							HSSFCell dairyCell = row.getCell(4);
							financeDeptRevenue[count++] = dairyCell.getNumericCellValue();
							
							HSSFCell frozenCell = row.getCell(5);
							financeDeptRevenue[count++] = frozenCell.getNumericCellValue();
							
							HSSFCell gmCell = row.getCell(7);
							financeDeptRevenue[count++] = gmCell.getNumericCellValue();
							
							HSSFCell hbcCell = row.getCell(8);
							financeDeptRevenue[count++] = hbcCell.getNumericCellValue();
							
							HSSFCell pharmacyCell = row.getCell(10);
							financeDeptRevenue[count++] = pharmacyCell.getNumericCellValue();
							
							HSSFCell produceCell = row.getCell(11);
							financeDeptRevenue[count++] = produceCell.getNumericCellValue();
							
							HSSFCell floralCell = row.getCell(12);
							financeDeptRevenue[count++] = floralCell.getNumericCellValue();
							
							HSSFCell meatCell = row.getCell(13);
							financeDeptRevenue[count++] = meatCell.getNumericCellValue();
							
							HSSFCell seafoodCell = row.getCell(14);
							financeDeptRevenue[count++] = seafoodCell.getNumericCellValue();
							
							HSSFCell deliCell = row.getCell(15);
							financeDeptRevenue[count++] = deliCell.getNumericCellValue();
							
							HSSFCell foodcourtCell = row.getCell(16);
							financeDeptRevenue[count++] = foodcourtCell.getNumericCellValue();
							
							HSSFCell bakeryCell = row.getCell(18);
							financeDeptRevenue[count++] = bakeryCell.getNumericCellValue();
							
							HSSFCell promoCell = row.getCell(19);
							financeDeptRevenue[count++] = promoCell.getNumericCellValue();
							
							HSSFCell gasCell = row.getCell(21);
							financeDeptRevenue[count++] = gasCell.getNumericCellValue();
							
							revenueMapForFinanceDept.put(getStoreNumber(storeLength,storeNumberCell.getNumericCellValue()), financeDeptRevenue);
						}else if(Constants.WEEKLY_REVENUE_CORRECTION_STORE.equals(processingMode)){
							Double[] storeRevenue = new Double[3];
							count = 0;
							
							HSSFCell revenueCell = row.getCell(16);
							storeRevenue[count++] = revenueCell.getNumericCellValue();
							
							HSSFCell visitCountCell = row.getCell(20);
							storeRevenue[count++] = visitCountCell.getNumericCellValue();
							
							HSSFCell aosCell = row.getCell(24);
							storeRevenue[count++] = aosCell.getNumericCellValue();
							
							revenueMapForFinanceDept.put(getStoreNumber(storeLength,storeNumberCell.getNumericCellValue()), storeRevenue);
						}
						processingRow++;
					}
				}
			}
			logger.debug(" Total Number of Rows :" + processingRow);
			
		}catch(Exception exe){
			logger.error("Error in Parse Excel "+exe);
			exe.printStackTrace();
		}
		return revenueMapForFinanceDept;
	}
}
