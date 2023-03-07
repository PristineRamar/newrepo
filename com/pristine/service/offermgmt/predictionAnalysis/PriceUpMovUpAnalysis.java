package com.pristine.service.offermgmt.predictionAnalysis;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.pristine.dao.DBManager;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dto.offermgmt.predictionAnalysis.MovementKeyDTO;
import com.pristine.dao.offermgmt.predictionAnalysis.PredictionAnalysisDAO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRecommendation;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.salesanalysis.ProductDTO;
import com.pristine.dto.offermgmt.predictionAnalysis.PredictionAnalysisOutputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PriceUpMovUpAnalysis {
	Connection conn = null;
	private static Logger logger = Logger.getLogger("PriceUpMovUpAnalysis");
	DateFormat formatterMMddyy = new SimpleDateFormat("MM/dd/yy");
	private static String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static String LOCATION_ID = "LOCATION_ID=";
	private static String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static String PRODUCT_ID = "PRODUCT_ID=";
	private static final String WEEK_START_DATE = "WEEK_START_DATE=";
	private int locationLevelId = 0;
	private String locationIds = "";
	private int productLevelId = 0;
	private String productIds = "";
	String weekStartDate = null, outputPath = "";
	int WEEK_78 = 78;
	int WEEK_13 = 13;
	private HashMap<ProductKey,ProductDTO> productDTOs = new HashMap<ProductKey,ProductDTO>();

	public PriceUpMovUpAnalysis() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ge) {
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-prediction-analysis.properties");
		PropertyManager.initialize("recommendation.properties");

		PriceUpMovUpAnalysis priceUpMovUpAnalysis = new PriceUpMovUpAnalysis();
		for (String arg : args) {
			if (arg.startsWith(LOCATION_ID)) {
				priceUpMovUpAnalysis.locationIds = arg.substring(LOCATION_ID.length());
			} else if (arg.startsWith(LOCATION_LEVEL_ID)) {
				priceUpMovUpAnalysis.locationLevelId = Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length()));
			} else if (arg.startsWith(WEEK_START_DATE)) {
				priceUpMovUpAnalysis.weekStartDate = arg.substring(WEEK_START_DATE.length());
			} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
				priceUpMovUpAnalysis.productLevelId = Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length()));
			} else if (arg.startsWith(PRODUCT_ID)) {
				priceUpMovUpAnalysis.productIds = arg.substring(PRODUCT_ID.length());
			}
		}
		priceUpMovUpAnalysis.analysisPriceUpMovUp();
	}

	private void analysisPriceUpMovUp() {
		PredictionAnalysisDAO predictionAnalysisDAO = new PredictionAnalysisDAO();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailPriceZoneDAO retailPriceZoneDAO = new RetailPriceZoneDAO();
		ProductGroupDAO productGroupDAO = new ProductGroupDAO();
		HashMap<ProductLocationKey, List<PredictionAnalysisOutputDTO>> analysisOutputByCategory = new HashMap<ProductLocationKey, List<PredictionAnalysisOutputDTO>>();

		outputPath = PropertyManager.getProperty("PREDICTION_ANALYSIS_OUTPUT_PATH");
		double correlationFactor = Double.valueOf(PropertyManager.getProperty("PREDICTION_ANALYSIS_CORRELATION_FACTOR"));
		
		try {
			String[] products = productIds.split(",");

			String[] locations = locationIds.split(",");

			// week calendar Id
			int weekCalendarId = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK).getCalendarId();

			// Get calendar details
			LinkedHashMap<Integer, RetailCalendarDTO> calendarDetails = predictionAnalysisDAO.getRetailCalendarDetail(conn, weekStartDate, WEEK_78);

			// Each category
			for (String productIdStr : products) {				
				int productId = Integer.valueOf(productIdStr);
				ProductKey productKey = new ProductKey(productLevelId, productId);
				// Get Category Name
				ProductDTO productDTO = productGroupDAO.getProductName(conn, productLevelId, productId);
				productDTOs.put(productKey, productDTO);
				
				// Each Zone
				for (String locationIdStr : locations) {
					int locationId = Integer.valueOf(locationIdStr);
					logger.info("********Running for product:" + productId + ",location:" + locationId + "is Started........");
					ProductLocationKey productLocationKey = new ProductLocationKey(productId, locationId);
					List<PredictionAnalysisOutputDTO> analysisOutput = new ArrayList<PredictionAnalysisOutputDTO>();

					// Get Zone
					PriceZoneDTO priceZoneDTO = retailPriceZoneDAO.getPriceZoneInfo(conn, locationId);

					// Get item details
					HashMap<Integer, PRItemDTO> itemDetails = predictionAnalysisDAO.getItemDetail(conn, locationLevelId, locationId, productLevelId,
							productId);

					// Get Run Id
					long runId = predictionAnalysisDAO.getLatestRecommendationRunId(conn, weekCalendarId, locationId, productId);

					// Get positive correlation items
					HashMap<Integer, Double> itemsWithCorrelatedValue = predictionAnalysisDAO.getCorrelationOfItems(conn, locationId, weekCalendarId,
							runId, correlationFactor);

					// Get recommendation detail
					HashMap<Integer, PRRecommendation> recDetails = predictionAnalysisDAO.getRecommendationItems(conn, runId);

					if (itemsWithCorrelatedValue.size() > 0) {
						// Get movement of last 78 weeks
						String itemCodes = "";
						for (Map.Entry<Integer, Double> entry : itemsWithCorrelatedValue.entrySet()) {
							itemCodes = itemCodes + "," + entry.getKey();
						}
						itemCodes = itemCodes != "" ? itemCodes.substring(1) : itemCodes;
						// HashMap<MovementKeyDTO, ProductMetricsDataDTO>
						// movementHistory = new HashMap<MovementKeyDTO,
						// ProductMetricsDataDTO>();
						HashMap<MovementKeyDTO, ProductMetricsDataDTO> movementHistory = predictionAnalysisDAO.getZoneMovementHistoryList(conn,
								locationLevelId, locationId, productLevelId, productId, weekStartDate, WEEK_78, itemCodes);

						// Find other details,
						analysisOutput = fillOtherData(itemDetails, productDTO, priceZoneDTO, calendarDetails, itemsWithCorrelatedValue,
								movementHistory, recDetails);

						analysisOutputByCategory.put(productLocationKey, analysisOutput);
					} else {
						analysisOutputByCategory.put(productLocationKey, analysisOutput);
						logger.info("*********** No correlate items -- " + locationId + "-" + productId + "*********");
					}
					
					logger.info("********Running for product:" + productId + ",location:" + locationId + "is Completed........");
				}
			}

			// Write to a text file
			writeToExcel(analysisOutputByCategory, outputPath);

		} catch (ParseException | GeneralException | IOException e) {
			e.printStackTrace();
			logger.error("Exception in analysisPriceUpMovUp:" + e.toString());
		} finally {
			PristineDBUtil.close(conn);
		}
	}

	private List<PredictionAnalysisOutputDTO> fillOtherData(HashMap<Integer, PRItemDTO> itemDetails, ProductDTO productDTO, PriceZoneDTO priceZoneDTO,
			LinkedHashMap<Integer, RetailCalendarDTO> calendarDetails, HashMap<Integer, Double> itemsWithCorrelatedValue,
			HashMap<MovementKeyDTO, ProductMetricsDataDTO> movementHistory, HashMap<Integer, PRRecommendation> recDetails) throws ParseException {

		List<PredictionAnalysisOutputDTO> analysisOutput = new ArrayList<PredictionAnalysisOutputDTO>();

		for (Map.Entry<Integer, Double> entry : itemsWithCorrelatedValue.entrySet()) {
			PredictionAnalysisOutputDTO predictionAnalysisOutputDTO = new PredictionAnalysisOutputDTO();
			HashSet<Double> distinctRegPrice = new HashSet<Double>();
			int noOfRegPriceChange = 0, zeroMovCnt = 0;

			int itemCode = entry.getKey();
			Double correlation = entry.getValue();
			PRItemDTO itemDTO = itemDetails.get(itemCode);
			if (itemDTO == null) {
				logger.warn("Item is not present in item_lookup:" + entry.getKey());
				continue;
			}
			double previousPriceChange = 0, actualWeekDataPresent = 0;
			int weekCounter = 0, prePriceCnt = 0;
			double regMovementSum = 0;
			// Loop last 78 weeks
			for (RetailCalendarDTO retailCalendarDTO : calendarDetails.values()) {
				MovementKeyDTO movementKey = new MovementKeyDTO(itemCode, retailCalendarDTO.getCalendarId());
				ProductMetricsDataDTO productMetricsDataDTO = movementHistory.get(movementKey);
				boolean isItemCreatedBeforeThisCalendar = true;
				if (itemDTO.getCreateTimeStamp() != null) {
					Date itemCreateDate = formatterMMddyy.parse(itemDTO.getCreateTimeStamp());
					Date calendarStartDate = formatterMMddyy.parse(retailCalendarDTO.getStartDate());
					if (!itemCreateDate.before(calendarStartDate)) {
						isItemCreatedBeforeThisCalendar = false;
					}
				}

				// Consider weeks from item introduced
				if (isItemCreatedBeforeThisCalendar) {
					if (productMetricsDataDTO != null && productMetricsDataDTO.getSalePrice() == 0) {
						// Distinct price changes
						if (productMetricsDataDTO.getRegularPrice() > 0) {
							distinctRegPrice.add(productMetricsDataDTO.getRegularPrice());
						}

						// Zero movement week
						if (productMetricsDataDTO.getRegularMovement() == 0) {
							zeroMovCnt++;
						}

						// last 13 weeks average reg mov
						if (weekCounter < WEEK_13) {
							regMovementSum = regMovementSum + productMetricsDataDTO.getRegularMovement();
						}

						// No of price changes
						if (prePriceCnt == 0) {
							previousPriceChange = productMetricsDataDTO.getRegularPrice();
						} else {
							if (productMetricsDataDTO.getRegularPrice() > 0 && previousPriceChange != productMetricsDataDTO.getRegularPrice()) {
								previousPriceChange = productMetricsDataDTO.getRegularPrice();
								noOfRegPriceChange++;
							}
						}
						prePriceCnt++;
					} else {
						// zero movement week
						zeroMovCnt++;
					}
					if (weekCounter < WEEK_13) {
						actualWeekDataPresent++;
					}
				}
				weekCounter++;
			}

			// Check if rec price is present in last 78 weeks
			boolean isRecPricePresentIn78Weeks = true;
			// get recommendation detail
			if (recDetails.get(itemCode) != null) {
				PRRecommendation prRecommendation = recDetails.get(itemCode);
				if (prRecommendation.getRecRegPrice() != null && prRecommendation.getRecRegPrice() > 0) {
					Double unitPrice = PRFormatHelper
							.roundToTwoDecimalDigitAsDouble(prRecommendation.getRecRegPrice() / prRecommendation.getRecRegMultiple());
					if(!distinctRegPrice.contains(unitPrice)) {
						isRecPricePresentIn78Weeks = false;
					} 
				}
			}
			// find unit retail
			// check if it exists

			predictionAnalysisOutputDTO.setCategoryName(productDTO.getProductName());
			predictionAnalysisOutputDTO.setZone(priceZoneDTO.getPriceZoneNum());
			predictionAnalysisOutputDTO.setPrestoItemCode(itemCode);
			predictionAnalysisOutputDTO.setItemName(itemDTO.getItemName());
			predictionAnalysisOutputDTO.setLigName(itemDTO.getRetLirName());
			predictionAnalysisOutputDTO.setRetailerItemCode(itemDTO.getRetailerItemCode());
			predictionAnalysisOutputDTO.setUPC(itemDTO.getUpc());
			predictionAnalysisOutputDTO.setItemCreatedDate(itemDTO.getCreateTimeStamp());
			predictionAnalysisOutputDTO.setCorrelation(correlation);
			predictionAnalysisOutputDTO.setNoOfDistinctRegPriceIn78Weeks(distinctRegPrice.size());
			predictionAnalysisOutputDTO.setNoOfRegPriceChangesIn78Weeks(noOfRegPriceChange);
			predictionAnalysisOutputDTO.setNoOfWeekWithZeroMovRegPriceIn78Weeks(zeroMovCnt);
			if (actualWeekDataPresent > 0) {
				predictionAnalysisOutputDTO
						.setAvgMovRegPriceLast13Weeks(PRFormatHelper.roundToTwoDecimalDigitAsDouble(regMovementSum / actualWeekDataPresent));
			}
			predictionAnalysisOutputDTO.setRecPricePresentIn78Weeks(isRecPricePresentIn78Weeks);
			analysisOutput.add(predictionAnalysisOutputDTO);
		}
		return analysisOutput;
	}

	public void writeToExcel(HashMap<ProductLocationKey, List<PredictionAnalysisOutputDTO>> analysisOutputByCategory, String path)
			throws IOException, GeneralException {
		try {
			// DateFormat dt = new SimpleDateFormat("_yyyy-MM-dd");
			// Date date = new Date();
			// String d = dt.format(date).toString();
			String tempWeekStratDate = weekStartDate.replaceAll("/", "");
			String fname = "PredictionAnalysis_" + tempWeekStratDate + "_" + DateUtil.now();
			String outputPath = path + "/" + fname + ".xlsx";

			FileInputStream inputStream = new FileInputStream(PropertyManager.getProperty("PREDICTION_ANALYSIS_TEMPLATE_PATH"));
			XSSFWorkbook workbook = new XSSFWorkbook(inputStream);

			int sheetCount = 1;
			String[] products = productIds.split(",");
			String[] locations = locationIds.split(",");

			for (String productIdStr : products) {
				int productId = Integer.valueOf(productIdStr);
				ProductKey productKey = new ProductKey(Constants.CATEGORYLEVELID, productId);
				int columnCount = -1;
				int rowCount = 0;
				// Get the sheet
				String sheetName = "Sheet" + String.valueOf(sheetCount);
				XSSFSheet sheet = workbook.getSheet(sheetName);				
				//Constraint on sheet name in excel
				String productName = productDTOs.get(productKey).getProductName();
				productName = productName.length() >= 30 ? productName.substring(0, 29) : productName;
				workbook.setSheetName(workbook.getSheetIndex(sheet), productName);
				
				for (String locationIdStr : locations) {
					int locationId = Integer.valueOf(locationIdStr);
					ProductLocationKey productLocationKey = new ProductLocationKey(productId, locationId);

					for (PredictionAnalysisOutputDTO pred : analysisOutputByCategory.get(productLocationKey)) {
						Row row = sheet.createRow(++rowCount);
						columnCount = -1;
						Cell cell9 = row.createCell(++columnCount);
						cell9.setCellValue((String) pred.getCategoryName());

						Cell cell10 = row.createCell(++columnCount);
						cell10.setCellValue((String) pred.getZone());

						Cell cell = row.createCell(++columnCount);
						cell.setCellValue((long) pred.getPrestoItemCode());

						Cell cell12 = row.createCell(++columnCount);
						cell12.setCellValue((String) pred.getItemName());

						Cell cell13 = row.createCell(++columnCount);
						cell13.setCellValue((String) pred.getLigName());

						Cell cell1 = row.createCell(++columnCount);
						cell1.setCellValue((String) pred.getRetailerItemCode());

						Cell cell2 = row.createCell(++columnCount);
						cell2.setCellValue((String) pred.getUPC());

						Cell cell8 = row.createCell(++columnCount);
						cell8.setCellValue((String) pred.getItemCreatedDate());

						Cell cell3 = row.createCell(++columnCount);
						cell3.setCellValue((double) pred.getCorrelation());

						Cell cell4 = row.createCell(++columnCount);
						cell4.setCellValue((int) pred.getNoOfDistinctRegPriceIn78Weeks());

						Cell cell5 = row.createCell(++columnCount);
						cell5.setCellValue((int) pred.getNoOfRegPriceChangesIn78Weeks());

						Cell cell6 = row.createCell(++columnCount);
						cell6.setCellValue((int) pred.getNoOfWeekWithZeroMovRegPriceIn78Weeks());

						Cell cell7 = row.createCell(++columnCount);
						cell7.setCellValue((double) pred.getAvgMovRegPriceLast13Weeks());
						
						Cell cell14 = row.createCell(++columnCount);
						cell14.setCellValue((boolean) pred.isRecPricePresentIn78Weeks());
					}
				}
				sheetCount++;
			}

			FileOutputStream out = new FileOutputStream(outputPath);
			workbook.write(out);
			out.close();
			workbook.close();
			inputStream.close();
			logger.info("file created.");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception while writing prediction analysis file", e);
		} finally {
		}
	}
}

class ProductLocationKey {
	private int productId;
	private int locationId;

	public ProductLocationKey(int productId, int locationId) {
		this.productId = productId;
		this.locationId = locationId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + locationId;
		result = prime * result + productId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProductLocationKey other = (ProductLocationKey) obj;
		if (locationId != other.locationId)
			return false;
		if (productId != other.productId)
			return false;
		return true;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
}
