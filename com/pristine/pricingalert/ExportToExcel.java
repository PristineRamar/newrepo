package com.pristine.pricingalert;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pristine.dao.DBManager;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.pricingalert.AlertTypesDAO;
import com.pristine.dao.pricingalert.ExportToExcelDAO;
import com.pristine.dao.pricingalert.GoalSettingsDAO;
import com.pristine.dao.pricingalert.LocationCompetitorMapDAO;
import com.pristine.dao.pricingalert.PricingAlertDAO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.pricingalert.AlertTypesDto;
import com.pristine.dto.pricingalert.GoalSettingsDTO;
import com.pristine.dto.pricingalert.LocationCompetitorMapDTO;
import com.pristine.dto.pricingalert.PAItemInfoDTO;
import com.pristine.dto.pricingalert.ReportTemplateDTO;
import com.pristine.dto.salesanalysis.ProductDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

public class ExportToExcel {
	
	private static Logger logger = Logger.getLogger("PAExportToExcel");
	
	private Connection connection = null; // DB connection
	
	private PricingAlertDAO pricingAlertDAO = null;
	private int weekCalendarId = 0;
	private String weekEndDate = null;
	private String allStores = null;
	private boolean isKVINoCompProcessed = false;
	private boolean isSummarySheetPresent = false;
	DecimalFormat twoDForm = new DecimalFormat("#.##");
	
	public ExportToExcel() {
		PropertyManager.initialize("analysis.properties");
		allStores = PropertyManager.getProperty("PA_EXPORTTOEXCEL.ALL_STORES");
		try {
			connection = DBManager.getConnection();
			pricingAlertDAO = new PricingAlertDAO();
	} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			logger.info("Pricing Alert - FetchAndFilterData Ends unsucessfully");
			System.exit(1);
		}
	}
	
	public ExportToExcel(boolean doInitiaze) throws GeneralException {
		if( doInitiaze ) throw new GeneralException( "Does not initiliaze, use different constructor");
	}
	
	public ExportToExcel(String weekEndDate) {
		this();
		this.weekEndDate = weekEndDate;
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-pa-exporttoexcel.properties");
		String weekEndDate = null;
		for(String arg : args){
			try{
				if(arg.startsWith("WEEK_END_DATE")){
					weekEndDate = arg.substring("WEEK_END_DATE=".length());
				}
			}catch(NumberFormatException exception){
				logger.error("Invalid numeric argument passed as input");
				System.exit(-1);
			}
		}
		ExportToExcel export = new ExportToExcel(weekEndDate);
		export.process();
	}
	
	public void process(){
		RetailCalendarDAO retailCalDAO = new RetailCalendarDAO();
		
		// Get last week date if not specified
		String weekStartDate = null;
		try{
			if(weekEndDate == null){
				weekStartDate = DateUtil.getWeekStartDate(1);
			}else{
				weekStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(weekEndDate), 0);
			}
			RetailCalendarDTO calDTO = retailCalDAO.getCalendarId(connection, weekStartDate, Constants.CALENDAR_WEEK);
			weekCalendarId = calDTO.getCalendarId();
			weekEndDate = calDTO.getEndDate();
		}catch(GeneralException exception){
			logger.error("Error when parsing date" + exception);
		}
		
		try{
			LocationCompetitorMapDAO locCompDAO = new LocationCompetitorMapDAO();
			// Get user list
			ArrayList<LocationCompetitorMapDTO> userList = null;
			if("FALSE".equalsIgnoreCase(allStores))
				userList = locCompDAO.getPAUserDetails(connection);
			else
				userList = locCompDAO.getPAUserDetailsOnly(connection);
			
			logger.info("User List Size - " + userList.size());
			logger.info("Report by all stores - " + allStores);
			for(LocationCompetitorMapDTO locCompMap : userList){
				// Get excel template
				HashMap<String, ArrayList<ReportTemplateDTO>> reportTemplateDTOList = pricingAlertDAO.getExcelTemplatesForUser(connection, locCompMap.getUserId());
				// Get Location Name
				if("FALSE".equalsIgnoreCase(allStores)){
					if(locCompMap.getBaseLocationLevelId() == Constants.STORE_LEVEL_ID){
						StoreDTO storeDTO = new StoreDAO().getStoreInfo(connection, locCompMap.getBaseLocationId());
						locCompMap.setCompStrNo(storeDTO.strNum);
					}else{
						PriceZoneDTO pzDTO = new RetailPriceZoneDAO().getPriceZoneInfo(connection, locCompMap.getBaseLocationId());
						locCompMap.setCompStrNo(pzDTO.getPriceZoneNum());
					}
					
					// Get Competitor Name
					if(locCompMap.getCompLocationLevelId() == Constants.STORE_LEVEL_ID){
						StoreDTO storeDTO = new StoreDAO().getStoreInfo(connection, locCompMap.getCompLocationId());
						locCompMap.setCompetitorName(storeDTO.strName);
					}
				}
				
				// Get Product Name
				ProductDTO productDTO = new ProductGroupDAO().getProductName(connection, locCompMap.getProductLevelId(), locCompMap.getProductId());
				if(productDTO != null && productDTO.getProductName() != null){
					locCompMap.setProductName(productDTO.getProductName());
					logger.debug("Product Name - " + productDTO.getProductName());
				}
				
				locCompMap.setWeekStartDate(weekStartDate);
				locCompMap.setWeekEndDate(weekEndDate);
				locCompMap.setCalendarId(weekCalendarId);
				
				
				writeToExcel(reportTemplateDTOList, locCompMap);
			}
		}catch(GeneralException exception){
			exception.printStackTrace();
		}
	}
	
	public void writeToExcel(HashMap<String, ArrayList<ReportTemplateDTO>> reportTemplateList, LocationCompetitorMapDTO locCompMap) throws GeneralException{
		// Get Alerts
		AlertTypesDAO alertDAO = new AlertTypesDAO();
		HashMap<Integer, AlertTypesDto> alertsMap = new HashMap<Integer, AlertTypesDto>();
		ArrayList<AlertTypesDto> alertList = alertDAO.getAlertTypes(connection);
		for(AlertTypesDto alerts : alertList){
			alertsMap.put(alerts.getAlertTypeId(), alerts);
		}
		String inputPath = PropertyManager.getProperty("PA_EXPORTTOEXCEL.DEFAULTEXCELTEMPLATE");
		String weekEndDate = "WE" + locCompMap.getWeekEndDate().replaceAll("/", "");
		String outputFileName = weekEndDate + "_" + (allStores.equalsIgnoreCase("FALSE")?locCompMap.getCompStrNo():"") + (locCompMap.getProductName() == null?"":locCompMap.getProductName().replaceAll(" ", "").replaceAll("/", "_"));
		
		for(Map.Entry<String, ArrayList<ReportTemplateDTO>> entry : reportTemplateList.entrySet()){
			boolean copyStatus = false;
			String outputPath = null;
			if(entry.getKey().equals("Alerts.xlsx")){
				outputFileName = weekEndDate + "_" + (locCompMap.getProductName() == null?"":locCompMap.getProductName().replaceAll(" ", "").replaceAll("/", "_")) + "_";
				outputPath = PropertyManager.getProperty("PA_EXPORTTOEXCEL.DEFAULTEXCELOUTPUT") + outputFileName + entry.getKey();
				copyStatus = copyExcel(inputPath+entry.getKey(), outputPath);
				logger.info("No of alerts to process - " + entry.getValue().size());
				
				// Changes to write Summary HSSFSheet
				String summarySheet = PropertyManager.getProperty("PA_EXPORTTOEXCEL.SUMMARY", "NO");
				if("YES".equalsIgnoreCase(summarySheet)){
					if(copyStatus){
						writeSummarySheet(outputPath, locCompMap);
						isSummarySheetPresent = true;
					}
				}
			}else if((entry.getKey().equals("KVINoComp.xlsx") && !isKVINoCompProcessed)){
				outputFileName = weekEndDate + "_";
				outputPath = PropertyManager.getProperty("PA_EXPORTTOEXCEL.DEFAULTEXCELOUTPUT") + outputFileName + entry.getKey();
				copyStatus = copyExcel(inputPath+entry.getKey(), outputPath);
			}else if((entry.getKey().equals("KVINoComp.xlsx") && isKVINoCompProcessed)){
				continue;
			}
			
			if(copyStatus){
				for(ReportTemplateDTO reportTemplateDTO: entry.getValue()){
					if(Integer.parseInt(reportTemplateDTO.getAlertIdForUser()) == 4 && !isKVINoCompProcessed){
						writeToExcel(alertsMap.get(Integer.parseInt(reportTemplateDTO.getAlertIdForUser())), outputPath, reportTemplateDTO, locCompMap);
						isKVINoCompProcessed = true;
					}else if(Integer.parseInt(reportTemplateDTO.getAlertIdForUser()) != 4){
						writeToExcel(alertsMap.get(Integer.parseInt(reportTemplateDTO.getAlertIdForUser())), outputPath, reportTemplateDTO, locCompMap);
					}
				}
			}else{
				logger.error("Unable to copy template");
				System.exit(-1);
			}
		}
	}
	
	public boolean copyExcel(String inputFile, String outputFile){
		
		boolean copyStatus = false;
		File source = new File(inputFile);
		File dest = new File(outputFile);
		
		try{
			FileUtils.copyFile(source, dest);
			copyStatus = true;
		}catch(IOException ie){
			ie.printStackTrace();
			copyStatus = false;
		}
		return copyStatus;
	}
	
	public void writeToExcel(AlertTypesDto alert, String outputFile, ReportTemplateDTO templateDTO, LocationCompetitorMapDTO locCompMap){
		if(alert.getTechnicalCode().equals("N001")){
			writeToExcelN001(alert, outputFile, templateDTO, locCompMap);
		}else if(alert.getTechnicalCode().equals("KVI001")){
			writeToExcelKVI001(alert, outputFile, templateDTO, locCompMap);
		}else if(alert.getTechnicalCode().equals("KVI002")){
			writeToExcelKVI002(alert, outputFile, templateDTO, locCompMap);
		}else if(alert.getTechnicalCode().equals("KVI003")){
			writeToExcelKVI003(alert, outputFile, templateDTO, locCompMap);
		}else if(alert.getTechnicalCode().equals("LIG001")){
			writeToExcelLIG001(alert, outputFile, templateDTO, locCompMap);
		}else if(alert.getTechnicalCode().equals("LIG002")){
			writeToExcelLIG002(alert, outputFile, templateDTO, locCompMap);
		}
	}
	
	public void writeSummarySheet(String outputFile, LocationCompetitorMapDTO locCompMap){
		GoalSettingsDAO goalsDAO = new GoalSettingsDAO();
		ArrayList<GoalSettingsDTO> goalList = goalsDAO.getGoalDetailsForProduct(connection, locCompMap);
		
		boolean isLocationLevelGoalPresent = false;
		for(GoalSettingsDTO goal : goalList){
			if(goal.getLocationLevelId() > 0 && goal.getLocationId() > 0){
				isLocationLevelGoalPresent = true;
			}
		}
		
		ExportToExcelDAO exportDAO = new ExportToExcelDAO();
		logger.info("Output file - " + outputFile);
		try{
			InputStream inp = new FileInputStream(outputFile);    
			    
			HSSFWorkbook wb = new HSSFWorkbook(inp);    
			HSSFSheet sheet = wb.getSheet("Summary");  
			
			HashMap<String, Integer> baseAboveComp = exportDAO.getBaseCompDiffCount(connection, locCompMap, true);
			HashMap<String, Integer> baseBelowComp = exportDAO.getBaseCompDiffCount(connection, locCompMap, false);
			HashMap<String, String> mergedMap = new HashMap<String, String>();
			
			for(Map.Entry<String, Integer> entry : baseAboveComp.entrySet()){
				mergedMap.put(entry.getKey(), entry.getValue() + "-" + baseBelowComp.get(entry.getKey()));
			}
			
			for(Map.Entry<String, Integer> entry : baseBelowComp.entrySet()){
				if(mergedMap.get(entry.getKey()) != null){
					mergedMap.put(entry.getKey(), mergedMap.get(entry.getKey()) + "-" + baseBelowComp.get(entry.getKey()));
				}else{
					mergedMap.put(entry.getKey(), "-" + baseBelowComp.get(entry.getKey()));
				}
			}
			
			int rowNo = 1;
			for(Map.Entry<String, String> entry : mergedMap.entrySet()){
				HSSFRow row = sheet.getRow(++rowNo); 
				if(row == null)
					row = sheet.createRow(rowNo);

				int cellNo = 0;
				
				HSSFCell cell = row.getCell(++cellNo);    
				if (cell == null)
				    cell = row.createCell(cellNo);
				cell.setCellValue(entry.getKey());
				
				String[] split = entry.getValue().split("-");
				cell = row.getCell(++cellNo);    
				if (cell == null)
				    cell = row.createCell(cellNo);
				if(split[0] == null || split[0].length() == 0)
					cell.setCellValue("");
				else
					cell.setCellValue(split[0]);
				
				cell = row.getCell(++cellNo);    
				if (cell == null)
				    cell = row.createCell(cellNo);
				if(split[1] == null || split[1].length() == 0)
					cell.setCellValue(split[1]);
				else
					cell.setCellValue("");
			}
			
			rowNo = 9;
			if(isLocationLevelGoalPresent){
				for(GoalSettingsDTO goal : goalList){
					HSSFRow row = sheet.getRow(++rowNo); 
					if(row == null)
						row = sheet.createRow(rowNo);
	
					int cellNo = 0;
					
					HSSFCell cell = row.getCell(++cellNo);    
					if (cell == null){    
					    cell = row.createCell(cellNo);
					    HSSFRow tempRow = sheet.getRow(rowNo - 1);
					    if(tempRow != null){
					    	HSSFCell tempCell = tempRow.getCell(cellNo);
					    	if(tempCell != null)
					    		cell.setCellStyle(tempCell.getCellStyle());
					    }
					}
					if(goal.getLocationName() != null){
						cell.setCellValue(goal.getLocationName());
					}else
						cell.setCellValue(goal.getChainName());
					
					cell = row.getCell(++cellNo);    
					if (cell == null){    
					    cell = row.createCell(cellNo);
					    HSSFRow tempRow = sheet.getRow(rowNo - 1);
					    if(tempRow != null){
					    	HSSFCell tempCell = tempRow.getCell(cellNo);
					    	if(tempCell != null)
					    		cell.setCellStyle(tempCell.getCellStyle());
					    }
					}
					cell.setCellValue(goal.getBrandName());
					
					cell = row.getCell(++cellNo);    
					if (cell == null){    
					    cell = row.createCell(cellNo);
					    HSSFRow tempRow = sheet.getRow(rowNo - 1);
					    if(tempRow != null){
					    	HSSFCell tempCell = tempRow.getCell(cellNo);
					    	if(tempCell != null)
					    		cell.setCellStyle(tempCell.getCellStyle());
					    }
					}
					cell.setCellValue(goal.getPriceCheckListName());
					
					cell = row.getCell(++cellNo);    
					if (cell == null){    
					    cell = row.createCell(cellNo);
					    HSSFRow tempRow = sheet.getRow(rowNo - 1);
					    if(tempRow != null){
					    	HSSFCell tempCell = tempRow.getCell(cellNo);
					    	if(tempCell != null)
					    		cell.setCellStyle(tempCell.getCellStyle());
					    }
					}
					cell.setCellValue(goal.getItemName());
					
					cell = row.getCell(++cellNo);    
					if (cell == null){    
					    cell = row.createCell(cellNo);
					    HSSFRow tempRow = sheet.getRow(rowNo - 1);
					    if(tempRow != null){
					    	HSSFCell tempCell = tempRow.getCell(cellNo);
					    	if(tempCell != null)
					    		cell.setCellStyle(tempCell.getCellStyle());
					    }
					}
					cell.setCellValue(goal.getCurRegMinPriceIndex() + "% to " + goal.getCurRegMaxPriceIndex() + "%");
					
					cell = row.getCell(++cellNo);    
					if (cell == null){    
					    cell = row.createCell(cellNo);
					    HSSFRow tempRow = sheet.getRow(rowNo - 1);
					    if(tempRow != null){
					    	HSSFCell tempCell = tempRow.getCell(cellNo);
					    	if(tempCell != null)
					    		cell.setCellStyle(tempCell.getCellStyle());
					    }
					}
					cell.setCellValue(goal.getCurRegMinMargin() + "% to " + goal.getCurRegMaxMargin() + "%");
				}
			}else{
				ArrayList<LocationCompetitorMapDTO> locCompList = exportDAO.getLocationCompetitorList(connection);
				HashMap<Integer, Double> marginGoal = new HashMap<Integer, Double>();
				HashMap<Integer, Double> billingMarginGoal = new HashMap<Integer, Double>();
				HashMap<Integer, HashMap<Integer, Double>> marginPriceCheckList = new HashMap<Integer, HashMap<Integer,Double>>();
				HashMap<Integer, HashMap<Integer, Double>> billingMarginPriceCheckList = new HashMap<Integer, HashMap<Integer,Double>>();
				HashMap<Integer, HashMap<Integer, Double>> marginForBrand = new HashMap<Integer, HashMap<Integer,Double>>();
				HashMap<Integer, HashMap<Integer, Double>> billingMarginForBrand = new HashMap<Integer, HashMap<Integer,Double>>();
				HashMap<Integer, HashMap<Integer, Double>> piMap = new HashMap<Integer, HashMap<Integer, Double>>();
				
				for(GoalSettingsDTO goal : goalList){
					
					for(LocationCompetitorMapDTO locComp : locCompList){
						LocationCompetitorMapDTO tempDTO = new LocationCompetitorMapDTO();
						tempDTO.setBaseLocationLevelId(locComp.getBaseLocationLevelId());
						tempDTO.setBaseLocationId(locComp.getBaseLocationId());
						tempDTO.setProductLevelId(locCompMap.getProductLevelId());
						tempDTO.setProductId(locCompMap.getProductId());
						tempDTO.setPriceCheckListId(goal.getPriceCheckListId());
						
						HSSFRow row = sheet.getRow(++rowNo); 
						if(row == null)
							row = sheet.createRow(rowNo);
		
						int cellNo = 0;
						
						HSSFCell cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						cell.setCellValue(locComp.getLocationName());
						
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						cell.setCellValue(locComp.getCompetitorName());
						
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						cell.setCellValue(locCompMap.getProductName());
						
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						cell.setCellValue(goal.getBrandName());
						
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						cell.setCellValue(goal.getPriceCheckListName());
						
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						cell.setCellValue(goal.getItemName());
						
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						cell.setCellValue(goal.getCurRegMinPriceIndex() + "% to " + goal.getCurRegMaxPriceIndex() + "%");
						
						// Actual Price Index
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						if(goal.getPriceCheckListName() != null){
								HashMap<Integer, HashMap<Integer, Double>> tMap = exportDAO.getPriceIndexForPriceCheckList(connection, tempDTO, locCompMap.getWeekStartDate());
								
								if(tMap.get(locComp.getCompLocationId()) != null){
									if(tMap.get(locComp.getCompLocationId()).get(tempDTO.getPriceCheckListId()) != null){
										cell.setCellValue(tMap.get(locComp.getCompLocationId()).get(tempDTO.getPriceCheckListId()));
									}else
										cell.setCellValue("");
								}
						}else if(goal.getBrandName() == null && goal.getPriceCheckListName() == null && goal.getItemName() == null){
							if(piMap.get(locComp.getBaseLocationId()) != null && piMap.get(locComp.getBaseLocationId()).get(locComp.getCompLocationId()) != null)
								cell.setCellValue(piMap.get(locComp.getBaseLocationId()).get(locComp.getCompLocationId()));
							else{
								HashMap<Integer, Double> tMap = exportDAO.getPriceIndex(connection, tempDTO, locCompMap.getWeekStartDate());
								piMap.put(locCompMap.getBaseLocationId(), tMap);
								if(tMap.get(locComp.getCompLocationId()) != null)
									cell.setCellValue(tMap.get(locComp.getCompLocationId()));
								else
									cell.setCellValue("");
							}
						}else
							cell.setCellValue("");
						// Actual Price Index Ends
						
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						cell.setCellValue(goal.getCurRegMinMargin() + "% to " + goal.getCurRegMaxMargin() + "%");
						
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						if(goal.getOverallGoal() > 0)
							cell.setCellValue(goal.getOverallGoal());
						else
							cell.setCellValue("");
						
						// Actual Margin
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						double margin = -1;
						if(goal.getPriceCheckListId() > 0){
							if(marginPriceCheckList.get(locComp.getBaseLocationId()) == null){
								margin = exportDAO.getMargin(connection, tempDTO, locCompMap.getWeekStartDate(), goal.getBrandId());
								HashMap<Integer, Double> marginMap = new HashMap<Integer, Double>();
								marginMap.put(tempDTO.getPriceCheckListId(), margin);
								marginPriceCheckList.put(locComp.getBaseLocationId(), marginMap);
							}else{
								if(marginPriceCheckList.get(locComp.getBaseLocationId()).get(tempDTO.getPriceCheckListId()) != null){
									margin = marginPriceCheckList.get(locComp.getBaseLocationId()).get(tempDTO.getPriceCheckListId());
								}else{
									margin = exportDAO.getMargin(connection, tempDTO, locCompMap.getWeekStartDate(), goal.getBrandId());
									HashMap<Integer, Double> marginMap = new HashMap<Integer, Double>();
									marginMap.put(tempDTO.getPriceCheckListId(), margin);
									marginPriceCheckList.put(locComp.getBaseLocationId(), marginMap);
								}
							}
							if(margin == 0)
								cell.setCellValue("");
							else
								cell.setCellValue(margin);
						}else if(goal.getBrandId() > 0){
							if(marginForBrand.get(locComp.getBaseLocationId()) == null){
								margin = exportDAO.getMargin(connection, tempDTO, locCompMap.getWeekStartDate(), goal.getBrandId());
								HashMap<Integer, Double> marginMap = new HashMap<Integer, Double>();
								marginMap.put(goal.getBrandId(), margin);
								marginForBrand.put(locComp.getBaseLocationId(), marginMap);
							}else{
								if(marginForBrand.get(locComp.getBaseLocationId()).get(tempDTO.getPriceCheckListId()) != null){
									margin = marginForBrand.get(locComp.getBaseLocationId()).get(tempDTO.getPriceCheckListId());
								}else{
									margin = exportDAO.getMargin(connection, tempDTO, locCompMap.getWeekStartDate(), goal.getBrandId());
									HashMap<Integer, Double> marginMap = new HashMap<Integer, Double>();
									marginMap.put(goal.getBrandId(), margin);
									marginForBrand.put(locComp.getBaseLocationId(), marginMap);
								}
							}
							if(margin == 0)
								cell.setCellValue("");
							else
								cell.setCellValue(margin);
						}else if(goal.getBrandName() == null && goal.getPriceCheckListName() == null && goal.getItemName() == null){
							if(marginGoal.get(locComp.getBaseLocationId()) != null){
								margin = marginGoal.get(locComp.getBaseLocationId());
							}else{
								margin = exportDAO.getMargin(connection, tempDTO, locCompMap.getWeekStartDate(), goal.getBrandId());
								marginGoal.put(locComp.getBaseLocationId(), margin);
								
							}
							if(margin == 0)
								cell.setCellValue("");
							else
								cell.setCellValue(margin);
						}else
							cell.setCellValue("");
						// Actual Margin Ends
						
						// Billing Margin
						cell = row.getCell(++cellNo);    
						if (cell == null)    
							cell = row.createCell(cellNo);
						double billingMargin = -1;
						if(goal.getPriceCheckListId() > 0){
							if(billingMarginPriceCheckList.get(locComp.getBaseLocationId()) == null){
								billingMargin = exportDAO.getBillingMargin(connection, tempDTO, locCompMap.getWeekStartDate(), goal.getBrandId());
								HashMap<Integer, Double> marginMap = new HashMap<Integer, Double>();
								marginMap.put(tempDTO.getPriceCheckListId(), billingMargin);
								billingMarginPriceCheckList.put(locComp.getBaseLocationId(), marginMap);
							}else{
								if(billingMarginPriceCheckList.get(locComp.getBaseLocationId()).get(tempDTO.getPriceCheckListId()) != null){
									billingMargin = billingMarginPriceCheckList.get(locComp.getBaseLocationId()).get(tempDTO.getPriceCheckListId());
								}else{
									billingMargin = exportDAO.getBillingMargin(connection, tempDTO, locCompMap.getWeekStartDate(), goal.getBrandId());
									HashMap<Integer, Double> marginMap = new HashMap<Integer, Double>();
									marginMap.put(tempDTO.getPriceCheckListId(), billingMargin);
									billingMarginPriceCheckList.put(locComp.getBaseLocationId(), marginMap);
								}
							}
							if(billingMargin == 0)
								cell.setCellValue("");
							else
								cell.setCellValue(billingMargin);
						}else if(goal.getBrandId() > 0){
							if(billingMarginForBrand.get(locComp.getBaseLocationId()) == null){
								billingMargin = exportDAO.getBillingMargin(connection, tempDTO, locCompMap.getWeekStartDate(), goal.getBrandId());
								HashMap<Integer, Double> marginMap = new HashMap<Integer, Double>();
								marginMap.put(goal.getBrandId(), billingMargin);
								billingMarginForBrand.put(locComp.getBaseLocationId(), marginMap);
							}else{
								if(billingMarginForBrand.get(locComp.getBaseLocationId()).get(tempDTO.getPriceCheckListId()) != null){
									billingMargin = billingMarginForBrand.get(locComp.getBaseLocationId()).get(tempDTO.getPriceCheckListId());
								}else{
									billingMargin = exportDAO.getBillingMargin(connection, tempDTO, locCompMap.getWeekStartDate(), goal.getBrandId());
									HashMap<Integer, Double> marginMap = new HashMap<Integer, Double>();
									marginMap.put(goal.getBrandId(), billingMargin);
									billingMarginForBrand.put(locComp.getBaseLocationId(), marginMap);
								}
							}
							if(billingMargin == 0)
								cell.setCellValue("");
							else
								cell.setCellValue(billingMargin);
						}else if(goal.getBrandName() == null && goal.getPriceCheckListName() == null && goal.getItemName() == null){
							if(billingMarginGoal.get(locComp.getBaseLocationId()) != null){
								billingMargin = billingMarginGoal.get(locComp.getBaseLocationId());
							}else{
								billingMargin = exportDAO.getBillingMargin(connection, tempDTO, locCompMap.getWeekStartDate(), goal.getBrandId());
								billingMarginGoal.put(locComp.getBaseLocationId(), billingMargin);
							}
							if(billingMargin == 0)
								cell.setCellValue("");
							else
								cell.setCellValue(billingMargin);
						}else
							cell.setCellValue("");
						// BillingMarginEnds
					}
				}
			}
			
			FileOutputStream fileOut = new FileOutputStream(outputFile);  
		    wb.write(fileOut);  
		    fileOut.close(); 
		}catch(Exception fe){
			fe.printStackTrace();
		}
	}
	
	public void writeToExcelN001(AlertTypesDto alert, String outputFile, ReportTemplateDTO templateDTO, LocationCompetitorMapDTO locCompMap){
		ExportToExcelDAO excelDAO = new ExportToExcelDAO();
		ArrayList<PAItemInfoDTO> itemList = excelDAO.getPAItemInfo(connection, pricingAlertDAO, alert, locCompMap, templateDTO, false);
		logger.info("No of items in alerts " + itemList.size());
		Collections.sort(itemList);
		String reportColumns[] = templateDTO.getColumnKeys().split("\\|");
		
		ArrayList<Integer> itemCodeList = new ArrayList<Integer>();
		for(PAItemInfoDTO item : itemList){
			itemCodeList.add(item.getItemCode());
		}
		GoalSettingsDAO goalDAO = new GoalSettingsDAO();
		
		double avgRevenueThreshold = Double.parseDouble(PropertyManager.getProperty("PA_EXPORTTOEXCEL.AVG_REVENUE_THRESHOLD", "1"));
		try{
			InputStream inp = new FileInputStream(outputFile);    
			    
			HSSFWorkbook wb = new HSSFWorkbook(inp);    
			HSSFSheet sheet = wb.getSheet(alert.getName());    
			HSSFCellStyle rowStyle = null;
			
			HSSFRow row = sheet.getRow(0);
			String header = null;
			
			HSSFCell cell = row.getCell(0); 
			
			header = cell.getStringCellValue();
			if(cell.getStringCellValue().equals("Store")){
				if(locCompMap.getBaseLocationLevelId() == Constants.STORE_LEVEL_ID)
					cell.setCellValue("Store");
				else
					cell.setCellValue("Zone");
			}
			
			cell = row.getCell(3);    
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(1);
			cell = row.getCell(0);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(2);
			cell = row.getCell(0);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(3);
			cell = row.getCell(0);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			int rowNo = 5;

			ArrayList<Integer> primaryList = null;
			ArrayList<Integer> secondaryList = null;
			int primaryCheckListId = -1;
			int secondaryCheckListId = -1;
			// Added for TOPS (To support KVI and K2 display)
			try{
				if(PropertyManager.getProperty("PA_EXPORTTOEXCEL.PRIMARY_LIST") != null){
					primaryCheckListId = pricingAlertDAO.getPriceCheckListId(connection, PropertyManager.getProperty("PA_EXPORTTOEXCEL.PRIMARY_LIST"));
					primaryList = pricingAlertDAO.getKVIItems(connection, primaryCheckListId);
					logger.info("Primary List Size - " + primaryList.size());
				}
				if(PropertyManager.getProperty("PA_EXPORTTOEXCEL.SECONDARY_LIST") != null){
					secondaryCheckListId = pricingAlertDAO.getPriceCheckListId(connection, PropertyManager.getProperty("PA_EXPORTTOEXCEL.SECONDARY_LIST"));
					secondaryList = pricingAlertDAO.getKVIItems(connection, secondaryCheckListId);
					logger.info("Secondary List Size - " + secondaryList.size());
				}
			}catch(GeneralException ge){
				logger.error("Error when retrieving items for price check list - " + ge.toString());
			}
			// Added for TOPS (To support KVI and K2 display) - Ends
			GoalSettingsDTO goalSettingsDTO = goalDAO.getGoalDetails(connection, locCompMap, itemCodeList);
			
			for(PAItemInfoDTO item : itemList){
				
				if(primaryList != null && primaryList.contains(item.getItemCode())){
					item.setKviDesc(PropertyManager.getProperty("PA_EXPORTTOEXCEL.PRIMARY_LIST"));
				}else if(secondaryList != null && secondaryList.contains(item.getItemCode())){
					item.setKviDesc(PropertyManager.getProperty("PA_EXPORTTOEXCEL.SECONDARY_LIST"));
				}
				
				GoalSettingsDTO goals = null;
				
				boolean isGoalSet = false;
				// Get Current Item's Goal
				if(goalSettingsDTO.getItemLevelGoal() != null && goalSettingsDTO.getItemLevelGoal().get(item.getItemCode()) != null){
					goals = goalSettingsDTO.getItemLevelGoal().get(item.getItemCode());
					isGoalSet = true;
				}
				
				if(!isGoalSet && goalSettingsDTO.getPriceCheckLevelGoal() != null){
					if(item.isKVIItem() && primaryList == null){
						//goals = goalSettingsDTO.getPriceCheckLevelGoal();
						//isGoalSet = true;
					}else if(item.isKVIItem() && primaryList != null){
						if(primaryList.contains(item.getItemCode())){
							if(goalSettingsDTO.getPriceCheckLevelGoal().get(primaryCheckListId) != null){
								goals = goalSettingsDTO.getPriceCheckLevelGoal().get(primaryCheckListId);
								isGoalSet = true;
							}
						}
					}else{
						if(secondaryList != null){
							if(secondaryList.contains(item.getItemCode())){
								if(goalSettingsDTO.getPriceCheckLevelGoal().get(secondaryCheckListId) != null){
									goals = goalSettingsDTO.getPriceCheckLevelGoal().get(secondaryCheckListId);
									isGoalSet = true;
								}
							}
						}
					}
				}
				
				// Changes to incorporate goals for brand
				if(!isGoalSet && goalSettingsDTO.getBrandLevelGoal() != null){
					for(Map.Entry<Integer, GoalSettingsDTO> entry : goalSettingsDTO.getBrandLevelGoal().entrySet()){
						if(entry.getKey() == item.getBrandId()){
							goals = entry.getValue();
							isGoalSet = true;
							break;
						}
					}
				}
				// Changes to incorporate goals for brand - Ends
				
				if(!isGoalSet && goalSettingsDTO.getProductLevelGoal() != null){
					goals = goalSettingsDTO.getProductLevelGoal();
					isGoalSet = true;
				}

				if(!isGoalSet && goalSettingsDTO.getLocationLevelGoal() != null){
					goals = goalSettingsDTO.getLocationLevelGoal();
					isGoalSet = true;
				}

				if(!isGoalSet && goalSettingsDTO.getChainLevelGoal() != null){
					isGoalSet = true;
					goals = goalSettingsDTO.getChainLevelGoal();
				}
				
				if(item.getAvgRevenue() < avgRevenueThreshold){
					continue;
				}
				row = sheet.getRow(++rowNo); 
				if(row == null){
					row = sheet.createRow(rowNo);
					row.setRowStyle(rowStyle);
				}else
					rowStyle = row.getRowStyle();
				
				int cellNo = -1;
				
				for(String columnKey : reportColumns){
					cell = row.getCell(++cellNo);    
					if (cell == null)    
					    cell = row.createCell(cellNo);
					
					if("PR".equals(columnKey)){
						if((item.getBaseCurRegPrice() == item.getBasePreRegPrice() && item.getBaseCurRegPrice() != Constants.DEFAULT_NA && item.getBasePreRegPrice() != Constants.DEFAULT_NA) ||  (item.getBaseCurRegPrice() != Constants.DEFAULT_NA && item.getBasePreRegPrice() == Constants.DEFAULT_NA))
							cell.setCellValue("\u2194");
						else if(item.getBaseCurRegPrice() != Constants.DEFAULT_NA && item.getBasePreRegPrice() != Constants.DEFAULT_NA){
							if(item.getBaseCurRegPrice() > item.getBasePreRegPrice())
								cell.setCellValue("\u2191");
							else if(item.getBaseCurRegPrice() < item.getBasePreRegPrice())
								cell.setCellValue("\u2193");
						}else
							cell.setCellValue("NA");
					}else if("CO".equals(columnKey)){
						if((item.getBaseCurListCost() == item.getBasePreListCost() && item.getBaseCurListCost() != Constants.DEFAULT_NA && item.getBasePreListCost() != Constants.DEFAULT_NA) || (item.getBaseCurListCost() != Constants.DEFAULT_NA && item.getBasePreListCost() == Constants.DEFAULT_NA))
							cell.setCellValue("\u2194");
						else if(item.getBaseCurListCost() != Constants.DEFAULT_NA && item.getBasePreListCost() != Constants.DEFAULT_NA){
							if(item.getBaseCurListCost() > item.getBasePreListCost())
								cell.setCellValue("\u2191");
							else if(item.getBaseCurListCost() < item.getBasePreListCost())
								cell.setCellValue("\u2193");
						}else
							cell.setCellValue("NA");
					}else if("COMP".equals(columnKey)){
						if((item.getCompCurRegPrice() == item.getCompPreRegPrice() && item.getCompCurRegPrice() != Constants.DEFAULT_NA && item.getCompPreRegPrice() != Constants.DEFAULT_NA) || (item.getCompCurRegPrice() != Constants.DEFAULT_NA && item.getCompPreRegPrice() == Constants.DEFAULT_NA))
							cell.setCellValue("\u2194");
						else if(item.getCompCurRegPrice() != Constants.DEFAULT_NA && item.getCompPreRegPrice() != Constants.DEFAULT_NA){
							if(item.getCompCurRegPrice() > item.getCompPreRegPrice())
								cell.setCellValue("\u2191");
							else if(item.getCompCurRegPrice() < item.getCompPreRegPrice())
								cell.setCellValue("\u2193");
						}else
							cell.setCellValue("NA");
					}else if("PI".equals(columnKey)){
						if(goals != null && !((item.getCompCurRegPrice() == 0 || item.getBaseCurRegPrice() == 0 || item.getCompCurRegPrice() == Constants.DEFAULT_NA || item.getBaseCurRegPrice() == Constants.DEFAULT_NA))){
							double curPriceIndex = item.getCompCurRegPrice()/item.getBaseCurRegPrice()*100;
							if(curPriceIndex < goals.getCurRegMinPriceIndex()){
								cell.setCellValue("Below");
							}else if(curPriceIndex >= goals.getCurRegMinPriceIndex() && curPriceIndex <= goals.getCurRegMaxPriceIndex()){
								cell.setCellValue("\u2713");
							}else{
								cell.setCellValue("Above");
							}
						}else{
							cell.setCellValue("NA");
						}
					}else if("M".equals(columnKey)){
						if(goals != null && !((item.getBaseCurRegPrice() == 0 || item.getBaseCurListCost() == 0 || item.getBaseCurRegPrice() == Constants.DEFAULT_NA || item.getBaseCurListCost() == Constants.DEFAULT_NA))){
							double curMargin = (item.getBaseCurRegPrice() - item.getBaseCurListCost())/item.getBaseCurRegPrice()*100;
							if(curMargin < goals.getCurRegMinMargin()){
								cell.setCellValue("Below");
							}else if(curMargin >= goals.getCurRegMinMargin() && curMargin <= goals.getCurRegMaxMargin()){
								cell.setCellValue("\u2713");
							}else{
								cell.setCellValue("Above");
							}
						}else{
							cell.setCellValue("NA");
						}
					}else if("R".equals(columnKey)){
						StringBuffer reason = new StringBuffer("");
						
						boolean isOutsidePIGoal = false;
						if(goals != null && !((item.getCompCurRegPrice() == 0 || item.getBaseCurRegPrice() == 0 || item.getCompCurRegPrice() == Constants.DEFAULT_NA || item.getBaseCurRegPrice() == Constants.DEFAULT_NA))){
							double curPriceIndex = item.getCompCurRegPrice()/item.getBaseCurRegPrice()*100;
							if(curPriceIndex < goals.getCurRegMinPriceIndex()){
								isOutsidePIGoal = true;
							}else if(curPriceIndex >= goals.getCurRegMinPriceIndex() && curPriceIndex <= goals.getCurRegMaxPriceIndex()){
							}else{
								isOutsidePIGoal = true;
							}
						}
						
						if(isOutsidePIGoal){
							if(goals != null && !((item.getComp2CurRegPrice() == 0 || item.getBaseCurRegPrice() == 0 || item.getComp2CurRegPrice() == Constants.DEFAULT_NA || item.getBaseCurRegPrice() == Constants.DEFAULT_NA))){
								char compLocationType;
								if(item.getCompLocationType() == Constants.COMP_LOCATION_TYPE_S)
									compLocationType = Constants.COMP_LOCATION_TYPE_P;
								else
									compLocationType = Constants.COMP_LOCATION_TYPE_S;
								double curPriceIndex = item.getComp2CurRegPrice()/item.getBaseCurRegPrice()*100;
								if(curPriceIndex < goals.getCurRegMinPriceIndex()){
									reason.append(", PI(" + compLocationType + ") Below");
								}else if(curPriceIndex >= goals.getCurRegMinPriceIndex() && curPriceIndex <= goals.getCurRegMaxPriceIndex()){
									reason.append(", PI(" + compLocationType + ") Within");
								}else{
									reason.append(", PI(" + compLocationType + ") Above");
								}
							}
						}
						
						if(item.getBaseCurRegPrice() != Constants.DEFAULT_NA && item.getCompCurRegPrice() != Constants.DEFAULT_NA && item.getComp2CurRegPrice() != Constants.DEFAULT_NA){
							if((item.getBaseCurRegPrice() - item.getCompCurRegPrice() > 0) && (item.getBaseCurRegPrice() - item.getComp2CurRegPrice() > 0))
								reason.append(", Comp(P&S) \u2193");
							else if((item.getBaseCurRegPrice() - item.getCompCurRegPrice() < 0) && (item.getBaseCurRegPrice() - item.getComp2CurRegPrice() < 0))
								reason.append(", Comp(P&S) \u2191");
						}
						
						double compPriceChg = 0;
						double comp2PriceChg = 0;
						if(item.getCompCurRegPrice() != Constants.DEFAULT_NA && item.getCompPreRegPrice() != Constants.DEFAULT_NA){
							compPriceChg = item.getCompCurRegPrice() - item.getCompPreRegPrice();
						}
						if(item.getComp2CurRegPrice() != Constants.DEFAULT_NA && item.getComp2PreRegPrice() != Constants.DEFAULT_NA){
							comp2PriceChg = item.getComp2CurRegPrice() - item.getComp2PreRegPrice();
						}
						
						if(item.getBaseCurRegPriceEffDate() != null){
							try{
								long dateDiff = DateUtil.getDateDiff(DateUtil.toDate(locCompMap.getWeekStartDate()), DateUtil.toDate(item.getBaseCurRegPriceEffDate()));
								if(dateDiff > Constants._13WEEK)
									if(compPriceChg != 0 && comp2PriceChg != 0){
										if(compPriceChg > 0 && comp2PriceChg > 0)
											reason.append(", Comp Change(P&S) \u2191");
										else if(compPriceChg < 0 && comp2PriceChg < 0)
											reason.append(", Comp Change(P&S) \u2193");
									}
							}catch(GeneralException exception){
								
							}
						}
						
						if((item.getBaseCurRegPrice() == item.getBasePreRegPrice() && item.getBaseCurRegPrice() != Constants.DEFAULT_NA && item.getBasePreRegPrice() != Constants.DEFAULT_NA) ||  (item.getBaseCurRegPrice() != Constants.DEFAULT_NA && item.getBasePreRegPrice() == Constants.DEFAULT_NA))
							reason.append("Price is \u2194");
						else if(item.getBaseCurRegPrice() != Constants.DEFAULT_NA && item.getBasePreRegPrice() != Constants.DEFAULT_NA){
							if(item.getBaseCurRegPrice() > item.getBasePreRegPrice())
								reason.append("Price \u2191");
							else if(item.getBaseCurRegPrice() < item.getBasePreRegPrice())
								reason.append("Price \u2193");
						}
						
						//Base List Cost Variation
						if((item.getBaseCurListCost() == item.getBasePreListCost() && item.getBaseCurListCost() != Constants.DEFAULT_NA && item.getBasePreListCost() != Constants.DEFAULT_NA) || (item.getBaseCurListCost() != Constants.DEFAULT_NA && item.getBasePreListCost() == Constants.DEFAULT_NA))
							if(reason.length() > 0)
								reason.append(", Cost is \u2194");
							else
								reason.append("Cost is \u2194");
						else if(item.getBaseCurListCost() != Constants.DEFAULT_NA && item.getBasePreListCost() != Constants.DEFAULT_NA){
							if(item.getBaseCurListCost() > item.getBasePreListCost())
								if(reason.length() > 0)
									reason.append(", Cost \u2191");
								else
									reason.append("Cost \u2191");
							else if(item.getBaseCurListCost() < item.getBasePreListCost())
								if(reason.length() > 0)
									reason.append(", Cost \u2193");
								else
									reason.append("Cost \u2193");
						}
						
						//Comp Reg Price Variation
						if((item.getCompCurRegPrice() == item.getCompPreRegPrice() && item.getCompCurRegPrice() != Constants.DEFAULT_NA && item.getCompPreRegPrice() != Constants.DEFAULT_NA) || (item.getCompCurRegPrice() != Constants.DEFAULT_NA && item.getCompPreRegPrice() == Constants.DEFAULT_NA))
							if(reason.length() > 0)
								reason.append(", Comp is \u2194");
							else
								reason.append("Comp is \u2194");
						else if(item.getCompCurRegPrice() != Constants.DEFAULT_NA && item.getCompPreRegPrice() != Constants.DEFAULT_NA){
							if(item.getCompCurRegPrice() > item.getCompPreRegPrice())
								if(reason.length() > 0)
									reason.append(", Comp \u2191");
								else
									reason.append("Comp \u2191");
							else if(item.getCompCurRegPrice() < item.getCompPreRegPrice())
								if(reason.length() > 0)
									reason.append(", Comp \u2193");
								else
									reason.append("Comp \u2193");
						}
						
						// Price Index Goals
						if(goals != null && !((item.getCompCurRegPrice() == 0 || item.getBaseCurRegPrice() == 0 || item.getCompCurRegPrice() == Constants.DEFAULT_NA || item.getBaseCurRegPrice() == Constants.DEFAULT_NA))){
							double curPriceIndex = item.getCompCurRegPrice()/item.getBaseCurRegPrice()*100;
							if(curPriceIndex < goals.getCurRegMinPriceIndex()){
								if(reason.length() > 0)
									reason.append(", Below Index");
								else
									reason.append("Below Index");
							}else if(curPriceIndex >= goals.getCurRegMinPriceIndex() && curPriceIndex <= goals.getCurRegMaxPriceIndex()){
								if(reason.length() > 0)
									reason.append(", Within Index");
								else
									reason.append("Within Index");
							}else{
								if(reason.length() > 0)
									reason.append(", Above Index");
								else
									reason.append("Above Index");
							}
						}
						
						// Margin Goals
						if(goals != null && !((item.getBaseCurRegPrice() == 0 || item.getBaseCurListCost() == 0 || item.getBaseCurRegPrice() == Constants.DEFAULT_NA || item.getBaseCurListCost() == Constants.DEFAULT_NA))){
							double curMargin = (item.getBaseCurRegPrice() - item.getBaseCurListCost())/item.getBaseCurRegPrice()*100;
							if(curMargin < goals.getCurRegMinMargin()){
								if(reason.length() > 0)
									reason.append(", Below Margin");
								else
									reason.append("Below Margin");
							}else if(curMargin >= goals.getCurRegMinMargin() && curMargin <= goals.getCurRegMaxMargin()){
								if(reason.length() > 0)
									reason.append(", Within Margin");
								else
									reason.append("Within Margin");
							}else{
								if(reason.length() > 0)
									reason.append(", Above Margin");
								else
									reason.append("Above Margin");
							}
						}
						
						cell.setCellValue(reason.toString());
					}else{
						setCellValue(columnKey, item, cell);
					}
				}
			}
			
			/*row = sheet.getRow(0);
			cell = row.getCell(12);    
			if (cell == null)    
			    cell = row.createCell(12);    
			cell.setCellValue(goals.getCurRegMinPriceIndex()+"/"+goals.getCurRegMaxPriceIndex());
			
			row = sheet.getRow(1);
			cell = row.getCell(12);    
			if (cell == null)    
			    cell = row.createCell(12);    
			cell.setCellValue(goals.getCurRegMinMargin()+"/"+goals.getCurRegMaxMargin());
			
			row = sheet.getRow(2);
			cell = row.getCell(12);    
			if (cell == null)    
			    cell = row.createCell(12);    
			cell.setCellValue(goals.getCurRegMinPriceIndex()+"/"+goals.getCurRegMaxPriceIndex());
			
			row = sheet.getRow(3);
			cell = row.getCell(12);    
			if (cell == null)    
			    cell = row.createCell(12);    
			cell.setCellValue(goals.getCurRegMinMargin()+"/"+goals.getCurRegMaxMargin());*/
			FileOutputStream fileOut = new FileOutputStream(outputFile);  
		    wb.write(fileOut);  
		    fileOut.close(); 
		}catch(Exception fe){
			fe.printStackTrace();
		}
	}
	
	public void writeToExcelKVI001(AlertTypesDto alert, String outputFile, ReportTemplateDTO templateDTO, LocationCompetitorMapDTO locCompMap){
		ExportToExcelDAO excelDAO = new ExportToExcelDAO();
		ArrayList<PAItemInfoDTO> itemList = excelDAO.getPAItemInfo(connection, pricingAlertDAO, alert, locCompMap, templateDTO, false);
		Collections.sort(itemList);
		String reportColumns[] = templateDTO.getColumnKeys().split("\\|");
		try{
			InputStream inp = new FileInputStream(outputFile);    
			    
			HSSFWorkbook wb = new HSSFWorkbook(inp);    
			HSSFSheet sheet = wb.getSheet(alert.getName());    
			HSSFCellStyle rowStyle = null;
			
			HSSFRow row = sheet.getRow(0);
			String header = null;
			
			HSSFCell cell = row.getCell(1);    
			
			header = cell.getStringCellValue();
			if(cell.getStringCellValue().equals("Store")){
				if(locCompMap.getBaseLocationLevelId() == Constants.STORE_LEVEL_ID)
					cell.setCellValue("Store");
				else
					cell.setCellValue("Zone");
			}
			
			cell = row.getCell(3);    
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(1);
			cell = row.getCell(1);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(2);
			cell = row.getCell(1);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			int rowNo = 4;
			
			for(PAItemInfoDTO item : itemList){
				row = sheet.getRow(++rowNo); 
				if(row == null){
					row = sheet.createRow(rowNo);
					row.setRowStyle(rowStyle);
				}else
					rowStyle = row.getRowStyle();
				int cellNo = 0;
				
				
				for(String columnKey : reportColumns){
					cell = row.getCell(++cellNo);    
					if (cell == null)    
					    cell = row.createCell(cellNo);
					setCellValue(columnKey, item, cell);
				}
			}
			
			FileOutputStream fileOut = new FileOutputStream(outputFile);  
		    wb.write(fileOut);  
		    fileOut.close(); 
		}catch(Exception fe){
			fe.printStackTrace();
		}
	}
	
	public void writeToExcelKVI002(AlertTypesDto alert, String outputFile, ReportTemplateDTO templateDTO, LocationCompetitorMapDTO locCompMap){
		ExportToExcelDAO excelDAO = new ExportToExcelDAO();
		ArrayList<PAItemInfoDTO> itemList = excelDAO.getPAItemInfo(connection, pricingAlertDAO, alert, locCompMap, templateDTO, false);
		Collections.sort(itemList);
		String reportColumns[] = templateDTO.getColumnKeys().split("\\|");
		
		try{
			InputStream inp = new FileInputStream(outputFile);    
			    
			HSSFWorkbook wb = new HSSFWorkbook(inp);    
			HSSFSheet sheet = wb.getSheet(alert.getName());    
			HSSFCellStyle rowStyle = null;
			
			HSSFRow row = sheet.getRow(0);
			String header = null;
			
			HSSFCell cell = row.getCell(1);    
			
			header = cell.getStringCellValue();
			if(cell.getStringCellValue().equals("Store")){
				if(locCompMap.getBaseLocationLevelId() == Constants.STORE_LEVEL_ID)
					cell.setCellValue("Store");
				else
					cell.setCellValue("Zone");
			}
			
			cell = row.getCell(3);    
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(1);
			cell = row.getCell(1);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(2);
			cell = row.getCell(1);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			int rowNo = 4;
			
			for(PAItemInfoDTO item : itemList){
				row = sheet.getRow(++rowNo); 
				if(row == null){
					row = sheet.createRow(rowNo);
					row.setRowStyle(rowStyle);
				}else
					rowStyle = row.getRowStyle();
				int cellNo = 0;
				
				for(String columnKey : reportColumns){
					cell = row.getCell(++cellNo);    
					if (cell == null)    
					    cell = row.createCell(cellNo);
					setCellValue(columnKey, item, cell);
				}
			}
			
			FileOutputStream fileOut = new FileOutputStream(outputFile);  
		    wb.write(fileOut);  
		    fileOut.close(); 
		}catch(Exception fe){
			fe.printStackTrace();
		}
	}
	
	public void writeToExcelKVI003(AlertTypesDto alert, String outputFile, ReportTemplateDTO templateDTO, LocationCompetitorMapDTO locCompMap){
		ExportToExcelDAO excelDAO = new ExportToExcelDAO();
		ArrayList<PAItemInfoDTO> itemList = excelDAO.getPAItemInfo(connection, pricingAlertDAO, alert, locCompMap, templateDTO, true);
		Collections.sort(itemList);
		String reportColumns[] = templateDTO.getColumnKeys().split("\\|");
		
		try{
			InputStream inp = new FileInputStream(outputFile);    
			    
			HSSFWorkbook wb = new HSSFWorkbook(inp);    
			HSSFSheet sheet = wb.getSheet(alert.getName());    
			HSSFCellStyle rowStyle = null;
			
			HSSFRow row = sheet.getRow(0);
			String header = null;
			
			HSSFCell cell = row.getCell(1);    
			
			header = cell.getStringCellValue();
			if(cell.getStringCellValue().equals("Store")){
				if(locCompMap.getBaseLocationLevelId() == Constants.STORE_LEVEL_ID)
					cell.setCellValue("Store");
				else
					cell.setCellValue("Zone");
			}
			
			cell = row.getCell(3);    
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(1);
			cell = row.getCell(1);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(2);
			cell = row.getCell(1);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			int rowNo = 4;
			
			for(PAItemInfoDTO item : itemList){
				row = sheet.getRow(++rowNo); 
				if(row == null){
					row = sheet.createRow(rowNo);
					row.setRowStyle(rowStyle);
				}else
					rowStyle = row.getRowStyle();
				int cellNo = 0;
				
				for(String columnKey : reportColumns){
					cell = row.getCell(++cellNo);    
					if (cell == null)    
					    cell = row.createCell(cellNo);
					setCellValue(columnKey, item, cell);
				}
			}
			
			FileOutputStream fileOut = new FileOutputStream(outputFile);  
		    wb.write(fileOut);  
		    fileOut.close(); 
		}catch(Exception fe){
			fe.printStackTrace();
		}
	}
	
	public void writeToExcelLIG001(AlertTypesDto alert, String outputFile, ReportTemplateDTO templateDTO, LocationCompetitorMapDTO locCompMap){
		ExportToExcelDAO excelDAO = new ExportToExcelDAO();
		ArrayList<PAItemInfoDTO> itemList = excelDAO.getPAItemInfo(connection, pricingAlertDAO, alert, locCompMap, templateDTO, false);
		String reportColumns[] = templateDTO.getColumnKeys().split("\\|");
		
		HashMap<Integer, Double> maxOccPriceMap = getMaxOccuringPrice(itemList);
		try{
			InputStream inp = new FileInputStream(outputFile);    
			    
			HSSFWorkbook wb = new HSSFWorkbook(inp);
			HSSFSheet sheet = null;
			if(isSummarySheetPresent)
				sheet = wb.getSheetAt(4);
			else
				sheet = wb.getSheetAt(3);
			
			HSSFCellStyle rowStyle = null;
			
			HSSFRow row = sheet.getRow(0);
			String header = null;
			
			HSSFCell cell = row.getCell(1);    
			
			header = cell.getStringCellValue();
			if(cell.getStringCellValue().equals("Store")){
				if(locCompMap.getBaseLocationLevelId() == Constants.STORE_LEVEL_ID)
					cell.setCellValue("Store");
				else
					cell.setCellValue("Zone");
			}
			
			cell = row.getCell(3);    
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
				
			row = sheet.getRow(1);
			cell = row.getCell(1);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(2);
			cell = row.getCell(1);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			int rowNo = 4;
            
			for(PAItemInfoDTO item : itemList){
				row = sheet.getRow(++rowNo); 
				if(row == null)
					row = sheet.createRow(rowNo);
					
				int cellNo = 0;
				
				for(String columnKey : reportColumns){
					cell = row.getCell(++cellNo);    
					if (cell == null){    
					    cell = row.createCell(cellNo);
					    HSSFRow tempRow = sheet.getRow(rowNo - 1);
					    if(tempRow != null){
					    	HSSFCell tempCell = tempRow.getCell(cellNo);
					    	if(tempCell != null)
					    		cell.setCellStyle(tempCell.getCellStyle());
					    }
					}
					if("BCRP".equals(columnKey)){
						if(item.getBaseCurRegPrice() != Constants.DEFAULT_NA){
							if(maxOccPriceMap.get(item.getRetLirId()) != null && item.getBaseCurRegPrice() != maxOccPriceMap.get(item.getRetLirId())){
								HSSFCellStyle style = cell.getCellStyle();
								HSSFCellStyle tempStyle = wb.createCellStyle();
								tempStyle.cloneStyleFrom(style);
								HSSFFont font = wb.createFont();
							    font.setColor(HSSFColor.RED.index);
							    tempStyle.setFont(font);
								cell.setCellValue(item.getBaseCurRegPrice());
								cell.setCellStyle(tempStyle);
							}else{
								cell.setCellValue(item.getBaseCurRegPrice());
							}
						}else
							cell.setCellValue(Constants.DEFAULT_NA_STRING);
					}else
						setCellValue(columnKey, item, cell);
				}	
			}
			
			FileOutputStream fileOut = new FileOutputStream(outputFile);  
		    wb.write(fileOut);  
		    fileOut.close(); 
		}catch(Exception fe){
			fe.printStackTrace();
		}
	}
	
	private HashMap<Integer, Double> getMaxOccuringPrice(ArrayList<PAItemInfoDTO> itemList) {
		HashMap<Integer, HashMap<Double, Integer>> priceCntMap = new HashMap<Integer, HashMap<Double,Integer>>();
		HashMap<Integer, Double> maxOcrPriceMap = new HashMap<Integer, Double>();
		for(PAItemInfoDTO paItem : itemList){
			if(priceCntMap.get(paItem.getRetLirId()) != null){
				HashMap<Double, Integer> tempMap = priceCntMap.get(paItem.getRetLirId());
				if(tempMap.get(paItem.getBaseCurRegPrice()) != null){
					int cnt = tempMap.get(paItem.getBaseCurRegPrice());
					cnt = cnt + 1;
					tempMap.put(paItem.getBaseCurRegPrice(), cnt);
				}else{
					tempMap.put(paItem.getBaseCurRegPrice(), 1);
				}
				priceCntMap.put(paItem.getRetLirId(), tempMap);
			}else{
				HashMap<Double, Integer> tempMap = new HashMap<Double, Integer>();
				tempMap.put(paItem.getBaseCurRegPrice(), 1);
				priceCntMap.put(paItem.getRetLirId(), tempMap);
			}
		}
		
		for(Map.Entry<Integer, HashMap<Double, Integer>> entry : priceCntMap.entrySet()){
			double maxOccPrice = 0;
			int maxCnt = 0;
			int counter = 1;
			for(Map.Entry<Double, Integer> inEntry : entry.getValue().entrySet()){
				if(counter == 1){
					maxOccPrice = inEntry.getKey();
					maxCnt = inEntry.getValue();
					counter++;
				}else{
					if(maxCnt < inEntry.getValue()){
						maxOccPrice = inEntry.getKey();
						maxCnt = inEntry.getValue();
					}
				}
			}
			
			maxOcrPriceMap.put(entry.getKey(), maxOccPrice);
		}
		return maxOcrPriceMap;
	}

	private HashMap<Integer, Double> getMaxOccuringCost(ArrayList<PAItemInfoDTO> itemList) {
		HashMap<Integer, HashMap<Double, Integer>> costCntMap = new HashMap<Integer, HashMap<Double,Integer>>();
		HashMap<Integer, Double> maxOcrCostMap = new HashMap<Integer, Double>();
		for(PAItemInfoDTO paItem : itemList){
			if(costCntMap.get(paItem.getRetLirId()) != null){
				HashMap<Double, Integer> tempMap = costCntMap.get(paItem.getRetLirId());
				if(tempMap.get(paItem.getBaseCurListCost()) != null){
					int cnt = tempMap.get(paItem.getBaseCurListCost());
					cnt = cnt + 1;
					tempMap.put(paItem.getBaseCurListCost(), cnt);
				}else{
					tempMap.put(paItem.getBaseCurListCost(), 1);
				}
				costCntMap.put(paItem.getRetLirId(), tempMap);
			}else{
				HashMap<Double, Integer> tempMap = new HashMap<Double, Integer>();
				tempMap.put(paItem.getBaseCurListCost(), 1);
				costCntMap.put(paItem.getRetLirId(), tempMap);
			}
		}
		
		for(Map.Entry<Integer, HashMap<Double, Integer>> entry : costCntMap.entrySet()){
			double maxOccPrice = 0;
			int maxCnt = 0;
			int counter = 1;
			for(Map.Entry<Double, Integer> inEntry : entry.getValue().entrySet()){
				if(counter == 1){
					maxOccPrice = inEntry.getKey();
					maxCnt = inEntry.getValue();
					counter++;
				}else{
					if(maxCnt < inEntry.getValue()){
						maxOccPrice = inEntry.getKey();
						maxCnt = inEntry.getValue();
					}
				}
			}
			
			maxOcrCostMap.put(entry.getKey(), maxOccPrice);
		}
		return maxOcrCostMap;
	}
	
	public void writeToExcelLIG002(AlertTypesDto alert, String outputFile, ReportTemplateDTO templateDTO, LocationCompetitorMapDTO locCompMap){
		ExportToExcelDAO excelDAO = new ExportToExcelDAO();
		ArrayList<PAItemInfoDTO> itemList = excelDAO.getPAItemInfo(connection, pricingAlertDAO, alert, locCompMap, templateDTO, false);
		
		HashMap<Integer, Double> maxOcrCostMap = getMaxOccuringCost(itemList);
		String reportColumns[] = templateDTO.getColumnKeys().split("\\|");
		
		try{
			InputStream inp = new FileInputStream(outputFile);    
			    
			HSSFWorkbook wb = new HSSFWorkbook(inp);
			HSSFSheet sheet = null;
			if(isSummarySheetPresent)
				sheet = wb.getSheetAt(5);
			else
				sheet = wb.getSheetAt(4);
			
			HSSFCellStyle rowStyle = null;
			
			HSSFRow row = sheet.getRow(0);
			String header = null;
			
			HSSFCell cell = row.getCell(1);    
			
			header = cell.getStringCellValue();
			if(cell.getStringCellValue().equals("Store")){
				if(locCompMap.getBaseLocationLevelId() == Constants.STORE_LEVEL_ID)
					cell.setCellValue("Store");
				else
					cell.setCellValue("Zone");
			}
			
			cell = row.getCell(3);    
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(1);
			cell = row.getCell(1);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			row = sheet.getRow(2);
			cell = row.getCell(1);
			header = cell.getStringCellValue();
			
			cell = row.getCell(3);
			if (cell == null)    
			    cell = row.createCell(3);    
			cell.setCellValue(getHeaderCellValue(header, locCompMap));
			
			
			int rowNo = 4;
		    
			for(PAItemInfoDTO item : itemList){
				
				
				row = sheet.getRow(++rowNo); 
				if(row == null){
					row = sheet.createRow(rowNo);
				}
				
				int cellNo = 0;
				
				for(String columnKey : reportColumns){
					cell = row.getCell(++cellNo);    
					if (cell == null){    
					    cell = row.createCell(cellNo);
					    HSSFRow tempRow = sheet.getRow(rowNo - 1);
					    if(tempRow != null){
					    	HSSFCell tempCell = tempRow.getCell(cellNo);
					    	if(tempCell != null)
					    		cell.setCellStyle(tempCell.getCellStyle());
					    }
					}
					if("BCLC".equals(columnKey)){
						if(item.getBaseCurListCost() != Constants.DEFAULT_NA){
							if(maxOcrCostMap.get(item.getRetLirId()) != null && item.getBaseCurListCost() != maxOcrCostMap.get(item.getRetLirId())){
								HSSFCellStyle style = cell.getCellStyle();
								HSSFCellStyle tempStyle = wb.createCellStyle();
								tempStyle.cloneStyleFrom(style);
								HSSFFont font = wb.createFont();
							    font.setColor(HSSFColor.RED.index);
							    tempStyle.setFont(font);
							    cell.setCellStyle(tempStyle);
								cell.setCellValue(item.getBaseCurListCost());
							}else{
								cell.setCellValue(item.getBaseCurListCost());
							}
						}else
							cell.setCellValue(Constants.DEFAULT_NA_STRING);
					}else
						setCellValue(columnKey, item, cell);
				}
			}
			
			FileOutputStream fileOut = new FileOutputStream(outputFile);  
		    wb.write(fileOut);  
		    fileOut.close(); 
		}catch(Exception fe){
			fe.printStackTrace();
		}
	}
	
	private String getHeaderCellValue(String header, LocationCompetitorMapDTO locCompMap) {
		if(header.equals("Store"))
			return locCompMap.getCompStrNo();
		else if(header.equals("Competitor"))
			return locCompMap.getCompetitorName();
		else if(header.equals("Base Week"))
			return locCompMap.getWeekStartDate();
		else if(header.equals("Category"))
			return locCompMap.getProductName();
		else return "";
	}

	public void setCellValue(String columnKey, PAItemInfoDTO paItemInfo, HSSFCell cell){
		if(columnKey.equals("P")){
			cell.setCellValue(paItemInfo.getPortfolio());
		}else if(columnKey.equals("MC")){
			cell.setCellValue(paItemInfo.getMajorCategory());
		}else if(columnKey.equals("C")){
			cell.setCellValue(paItemInfo.getCategory());
		}else if(columnKey.equals("LIC")){
			cell.setCellValue(paItemInfo.getLirCode());
		}else if(columnKey.equals("LIN")){
			cell.setCellValue(paItemInfo.getLirItemName());
		}else if(columnKey.equals("RIC")){
			cell.setCellValue(paItemInfo.getRetailerItemCode());
		}else if(columnKey.equals("IN")){
			cell.setCellValue(paItemInfo.getItemName());
		}else if(columnKey.equals("UPC")){
			cell.setCellValue(paItemInfo.getUpc());
		}else if(columnKey.equals("KVID")){
			cell.setCellValue(paItemInfo.getKviDesc());
		}else if(columnKey.equals("KVI")){
			if(paItemInfo.isKVIItem())
				cell.setCellValue("Yes");
			else
				cell.setCellValue("No");
		}else if(columnKey.equals("AR")){
			cell.setCellValue(paItemInfo.getAvgRevenue());
		}else if(columnKey.equals("BCRP")){
			if(paItemInfo.getBaseCurRegPrice() != Constants.DEFAULT_NA)
				cell.setCellValue(paItemInfo.getBaseCurRegPrice());
			else
				cell.setCellValue(Constants.DEFAULT_NA_STRING);
		}else if(columnKey.equals("BCRPD")){
			cell.setCellValue(paItemInfo.getBaseCurRegPriceEffDate());
		}else if(columnKey.equals("BPRP")){
			if(paItemInfo.getBasePreRegPrice() != Constants.DEFAULT_NA)
				cell.setCellValue(paItemInfo.getBasePreRegPrice());
			else
				cell.setCellValue(Constants.DEFAULT_NA_STRING);
		}else if(columnKey.equals("BFRP")){
			if(paItemInfo.getBaseFutRegPrice() != Constants.DEFAULT_NA)
				cell.setCellValue(paItemInfo.getBaseFutRegPrice());
			else
				cell.setCellValue(Constants.DEFAULT_NA_STRING);
		}else if(columnKey.equals("BFRPD")){
			cell.setCellValue(paItemInfo.getBaseFutRegPriceEffDate());
		}else if(columnKey.equals("BCLC")){
			if(paItemInfo.getBaseCurListCost() != Constants.DEFAULT_NA)
				cell.setCellValue(paItemInfo.getBaseCurListCost());
			else
				cell.setCellValue(Constants.DEFAULT_NA_STRING);
		}else if(columnKey.equals("BCLCD")){
			cell.setCellValue(paItemInfo.getBaseCurListCostEffDate());
		}else if(columnKey.equals("BPLC")){
			if(paItemInfo.getBasePreListCost() != Constants.DEFAULT_NA)
				cell.setCellValue(paItemInfo.getBasePreListCost());
			else
				cell.setCellValue(Constants.DEFAULT_NA_STRING);
		}else if(columnKey.equals("BFLC")){
			if(paItemInfo.getBaseFutListCost() != Constants.DEFAULT_NA)
				cell.setCellValue(paItemInfo.getBaseFutListCost());
			else
				cell.setCellValue(Constants.DEFAULT_NA_STRING);
		}else if(columnKey.equals("BFLCD")){
			cell.setCellValue(paItemInfo.getBaseFutListCostEffDate());
		}else if(columnKey.equals("CCRP")){
			if(paItemInfo.getCompCurRegPrice() != Constants.DEFAULT_NA)
				cell.setCellValue(paItemInfo.getCompCurRegPrice());
			else
				cell.setCellValue(Constants.DEFAULT_NA_STRING);
		}else if(columnKey.equals("CCRPD")){
			cell.setCellValue(paItemInfo.getCompCurRegPriceEffDate());
		}else if(columnKey.equals("CPRP")){
			if(paItemInfo.getCompPreRegPrice() != Constants.DEFAULT_NA)
				cell.setCellValue(paItemInfo.getCompPreRegPrice());
			else
				cell.setCellValue(Constants.DEFAULT_NA_STRING);
		}else if(columnKey.equals("CCRPOD")){
			cell.setCellValue(paItemInfo.getCompCurRegPriceLastObsDate());
		}else if(columnKey.equals("CI")){
			if(paItemInfo.getCompCurRegPrice() == 0 || paItemInfo.getBaseCurRegPrice() == 0 || paItemInfo.getCompCurRegPrice() == Constants.DEFAULT_NA || paItemInfo.getBaseCurRegPrice() == Constants.DEFAULT_NA)
				cell.setCellValue("NA");
			else
				cell.setCellValue(paItemInfo.getCompCurRegPrice()/paItemInfo.getBaseCurRegPrice()*100);
		}else if(columnKey.equals("CM")){
			if(paItemInfo.getBaseCurRegPrice() == 0 || paItemInfo.getBaseCurListCost() == 0 || paItemInfo.getBaseCurRegPrice() == Constants.DEFAULT_NA || paItemInfo.getBaseCurListCost() == Constants.DEFAULT_NA)
				cell.setCellValue("NA");
			else
				cell.setCellValue((paItemInfo.getBaseCurRegPrice() - paItemInfo.getBaseCurListCost())/paItemInfo.getBaseCurRegPrice()*100);
		}else if(columnKey.equals("PM")){
			if(paItemInfo.getBaseCurRegPrice() == 0 || paItemInfo.getBasePreListCost() == 0 || paItemInfo.getBaseCurRegPrice() == Constants.DEFAULT_NA || paItemInfo.getBasePreListCost() == Constants.DEFAULT_NA)
				cell.setCellValue("NA");
			else
				cell.setCellValue((paItemInfo.getBaseCurRegPrice() - paItemInfo.getBasePreListCost())/paItemInfo.getBaseCurRegPrice()*100);
		}else if(columnKey.equals("FI")){
			if(paItemInfo.getCompCurRegPrice() == 0 || paItemInfo.getBaseCurRegPrice() == 0 || paItemInfo.getCompCurRegPrice() == Constants.DEFAULT_NA || paItemInfo.getBaseCurRegPrice() == Constants.DEFAULT_NA)
				cell.setCellValue("NA");
			else
				cell.setCellValue(paItemInfo.getCompCurRegPrice()/paItemInfo.getBaseCurRegPrice()*100);
		}else if(columnKey.equals("FM")){
			if(paItemInfo.getBaseCurRegPrice() == 0 || paItemInfo.getBaseCurListCost() == 0 || paItemInfo.getBaseCurRegPrice() == Constants.DEFAULT_NA || paItemInfo.getBaseCurListCost() == Constants.DEFAULT_NA)
				cell.setCellValue("NA");
			else
				cell.setCellValue((paItemInfo.getBaseCurRegPrice() - paItemInfo.getBaseCurListCost())/paItemInfo.getBaseCurRegPrice()*100);
		}else if(columnKey.equals("CC")){
			double costChangePct = (paItemInfo.getBaseCurListCost() - paItemInfo.getBasePreListCost())/paItemInfo.getBasePreListCost()*100;
			if(costChangePct < 0)
				cell.setCellValue("\u2193 (" + twoDForm.format(Math.abs(costChangePct)) + ")");
			else
				cell.setCellValue("\u2191 (" + twoDForm.format(Math.abs(costChangePct)) + ")");
		}else if(columnKey.equals("LN")){
			cell.setCellValue(paItemInfo.getLocNum());
		}else if(columnKey.equals("CN")){
			cell.setCellValue(paItemInfo.getCompName());
		}else if(columnKey.equals("CNUM")){
			cell.setCellValue(paItemInfo.getCompNo());
		}else if(columnKey.equals("IS")){
			cell.setCellValue(paItemInfo.getItemSize());
		}else if(columnKey.equals("BN")){
			cell.setCellValue(paItemInfo.getBrandName());
		}
	}
	
	/**
	 * Static method to log the command line arguments
	 */	
    private static void logCommand (String[] args)  {
		StringBuffer sb = new StringBuffer("Command: FetchAndFilterData ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }
}
