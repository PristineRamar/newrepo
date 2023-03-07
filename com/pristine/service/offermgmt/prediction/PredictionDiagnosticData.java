package com.pristine.service.offermgmt.prediction;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.pristine.dto.offermgmt.prediction.PredictionDiagnosticDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

public class PredictionDiagnosticData {

	private static Logger logger = Logger.getLogger("PredictionDiagnosticData");
	private Connection _Conn = null;
	private static String ITEM_CODES = "ITEM_CODES=";
	private static String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static String LOCATION_ID = "LOCATION_ID=";
	
	private String inputItemCodes = "";
	private int locationLevelId = 0;
	private int locationId;
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-prediction-diagnostic-data.properties");
		PropertyManager.initialize("recommendation.properties");
		PredictionDiagnosticData predictionDiagnosticData = new PredictionDiagnosticData();
		for (String arg : args) {
			if (arg.startsWith(ITEM_CODES)) {
				predictionDiagnosticData.inputItemCodes = arg.substring(ITEM_CODES.length());
			} else if (arg.startsWith(LOCATION_LEVEL_ID)) {
				predictionDiagnosticData.locationLevelId = Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length()));
			} else if (arg.startsWith(LOCATION_ID)) {
				predictionDiagnosticData.locationId = Integer.parseInt(arg.substring(LOCATION_ID.length()));
			}
		}
		logger.info("***********************************************");
		predictionDiagnosticData.getDiagnosticData();
		logger.info("***********************************************");
	}
	
	private void getDiagnosticData() {
		try {
			// Get movement history for all the item codes

			// Group the data
			List<PredictionDiagnosticDataDTO> predictionDiagnosticData = groupData();

			// Write it in to excel
			writeExcelFile(predictionDiagnosticData);
		} catch (GeneralException e) {
			e.printStackTrace();
			logger.error("Exception in getDiagnosticData() " + e.toString());
		}
	}
	
	private List<PredictionDiagnosticDataDTO> groupData(){
		List<PredictionDiagnosticDataDTO> predictionDiagnosticData = new ArrayList<PredictionDiagnosticDataDTO>();
		
		
		return predictionDiagnosticData;
	}

	private void writeExcelFile(List<PredictionDiagnosticDataDTO> predictionDiagnosticData) throws GeneralException {
		String outputPath = PropertyManager.getProperty("PREDICTION_DIAGNOSTIC_DATA_OUTPUT_PATH");
		String templatePath = PropertyManager.getProperty("PREDICTION_DIAGNOSTIC_DATA_TEMPLATE");
		
		String outFile = outputPath + "/" + DateUtil.now() + ".xlsx";
		
		FileInputStream inputStream;
		try {
			inputStream = new FileInputStream(templatePath);
			XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
			XSSFSheet diagnosticDataSheet = workbook.getSheet("DiagnosticData");
			
			fillDiagnosticDataSheet(predictionDiagnosticData, diagnosticDataSheet);
			
			FileOutputStream outputStream = new FileOutputStream(outFile);
			workbook.write(outputStream);
			workbook.close();
			inputStream.close();
			outputStream.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Exception in writeExcelFile() " + ex.toString());
			throw new GeneralException("Error in writeExcelFile()" + ex.toString());
		}
	}
	
	private void fillDiagnosticDataSheet(List<PredictionDiagnosticDataDTO> predictionDiagnosticData, XSSFSheet diagnosticDataSheet)
			throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		int rowCnt = 0;

		for (PredictionDiagnosticDataDTO predDiagnosticDataDTO : predictionDiagnosticData) {
			int catNameColCount = 0;
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getItemCode(), diagnosticDataSheet);
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getMinPrice(), diagnosticDataSheet);
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getPageNo(), diagnosticDataSheet);
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getBlockNo(), diagnosticDataSheet);
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getDisplay(), diagnosticDataSheet);
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getSaleFlag(), diagnosticDataSheet);
			
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getRegPrice().multiple + "/" + predDiagnosticDataDTO.getRegPrice().price,
					diagnosticDataSheet);
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getSalePrice().multiple + "/" + predDiagnosticDataDTO.getSalePrice().price,
					diagnosticDataSheet);
			
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getSpecialDay1(), diagnosticDataSheet);
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getNoOfObservations(), diagnosticDataSheet);
			setCellValue(rowCnt, ++catNameColCount, predDiagnosticDataDTO.getAvgMovement(), diagnosticDataSheet);
			rowCnt++;
		}
	}
	
	private void setCellValue(int rowNum, int cellNum, Object value, XSSFSheet sheet) {
		XSSFRow row = sheet.getRow(rowNum);
		XSSFCell cell = row.getCell(cellNum);
		if (value instanceof String) {
			cell.setCellValue(String.valueOf(value));
		} else if (value instanceof Double) {
			cell.setCellValue((Double) value);
		} else if (value instanceof Long) {
			cell.setCellValue((Long) value);
		} else if (value instanceof Integer) {
			cell.setCellValue((Integer) value);
		}
		cell.setCellStyle(cell.getCellStyle());
	}

}

class PredictionDiagnosticDataKey {
	private int locationLevelId;
	private int locationId;
	private int itemCode;
	private double minPrice;
	private int pageNo;
	private int blockNo;
	private String display;
	
	
}
