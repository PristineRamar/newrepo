package com.pristine.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import com.pristine.dto.ForecastDto;


public class ParseExcel {
	
	 private static Logger logger = Logger.getLogger(ParseExcel.class);
	 
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
	 
	 
	public List<ForecastDto> parseExcel(String fileName, String filePath,
			String sheetName,  int startColumn,
			Date startdate, Date endDate, int storeLength, int processingRow, String mode) throws IOException  {
		
		List<ForecastDto> foreCastList =  new ArrayList<ForecastDto>();
		logger.debug("Processing File Name : " + filePath+fileName );	
			
		// read the excel file using Fileinputstream reader
		FileInputStream inputFileStream = new FileInputStream(filePath+fileName);
		
		HSSFWorkbook inputExcel = new HSSFWorkbook(inputFileStream);
		
		HSSFSheet sheet = inputExcel.getSheetAt(Integer.parseInt(sheetName));
			
		int rows = sheet.getPhysicalNumberOfRows();
						
		try{
		
			for (int ii = 2; ii < rows-1; ii++) {

				// Object for foreCastDto
				ForecastDto objForeCastDto = new ForecastDto();
				HSSFRow row = sheet.getRow(ii);

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
				} else if (startdate.before(objForeCastDto.getProcessDate())
						&& endDate.after(objForeCastDto.getProcessDate())) {
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

			}
			
			logger.debug(" Total Number of Rows :" + processingRow);
			
		}catch(Exception exe){
			logger.error("Error in Parse Excel "+exe);
			
		}
		
		return foreCastList;
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
		
		Object value = null;
		
		switch (cell.getCellType()) {

		case HSSFCell.CELL_TYPE_NUMERIC:
			value = cell.getNumericCellValue();
			break;
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

}
