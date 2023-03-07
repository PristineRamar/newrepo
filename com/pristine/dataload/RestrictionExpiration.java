package com.pristine.dataload;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.RegionUtil;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.pristine.customer.Util;
import com.pristine.dao.DBManager;
import com.pristine.dao.RestrictionExpirationDAO;
import com.pristine.dao.offermgmt.PriceExportDAO;
import com.pristine.dto.ReDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.email.EmailService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class RestrictionExpiration {

	private static Logger logger = Logger.getLogger("RestrictionExpiration");
	private Connection conn = null;

	PriceExportDAO exportDao = new PriceExportDAO();
	RestrictionExpirationDAO REDao = new RestrictionExpirationDAO();

	PrintWriter pw;
	
	String mailTo = PropertyManager.getProperty("RE_REPORT.MAILTO");
	String mailCC = PropertyManager.getProperty("RE_REPORT.MAILCC");
	String mailBCC = PropertyManager.getProperty("RE_REPORT.MAILBCC");
	
	String rootPath = PropertyManager.getProperty("EXP_FILEPATH");

	DateFormat format = new SimpleDateFormat("MMddyyyy");
	String timeStamp = format.format(new Date());
	String fileNameConfig = PropertyManager.getProperty("EXP_FILENAME");
	String fileName = fileNameConfig + "-" + timeStamp;

	File sourceFile = new File(rootPath + "/" + fileName + ".xlsx");
	String attachmentFile = rootPath + "/" + fileName + ".xlsx";
	
	List<String> attachmentFiles = new ArrayList<String>();

	public static void main(String[] args) {
		
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-restriction-expiration.properties");
		

		RestrictionExpiration restrictionExpiration = new RestrictionExpiration();

		logger.info("*******************************************************************************");
		logger.info("main() - Reporting Expiring Items On next week - Started");

		restrictionExpiration.reportItemsRestrictionExpiration();

		logger.info("main() - Reporting Expiring Items On next week - Done");
		logger.info("*******************************************************************************");

	}

	public RestrictionExpiration() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException e) {
			logger.error("Error while getting connection!", e);
		}
	}

	public void reportItemsRestrictionExpiration() {

		HashMap<Integer, String> categoryAndItemPair = new HashMap<Integer, String>();
		HashMap<Integer, String> recommendationUnitAndItemPair = new HashMap<Integer, String>();
		List<ReDTO> expiryItemList = new ArrayList<ReDTO>();
		List<ReDTO> finalListOfReport = new ArrayList<ReDTO>();

		try {
			
			//List<String> UsersMailList = REDao.getEmailsOfUsers(conn);
			

			//HashMap<Integer, String> retItemCodeMap = REDao.getRetailerItemCodeForItemCode(conn);
			
			logger.info("exportItemsRestrictionExpiration() - Getting items expire on next week");
			// get items, stores, expired date
			expiryItemList = REDao.getNextWeekExpiredItems(conn);
			logger.info("# in exp list after adding store lock list: " +expiryItemList.size());	
			REDao.getNextWeekExpiryItemsFromStoreList(conn,expiryItemList);
			logger.info("# in exp list after adding store lock list: " +expiryItemList.size());	
			REDao.getNextWeekExpiryItemsForMinMaxAndLockedRetail(conn,expiryItemList);
			logger.info("# in exp list after adding min max and locked retail : " +expiryItemList.size());			

			if(expiryItemList.size() > 0) {
				
			//updateExpiryItems(expiryItemList);
			
			HashMap<Integer, List<ReDTO>> expiredItemsOnNextWeek = (HashMap<Integer, List<ReDTO>>) expiryItemList
					.stream().collect(Collectors.groupingBy(ReDTO::getItemCode));
			logger.info("# of records in expiredItemsOnNextWeek: " + expiredItemsOnNextWeek.size());
			
			logger.info("exportItemsRestrictionExpiration() - Getting Category Name of the item");
			categoryAndItemPair = exportDao.getCategoryAndRecomUnitOfItem(conn, expiredItemsOnNextWeek, Constants.CATEGORYLEVELID);

			logger.info("exportItemsRestrictionExpiration() - Getting Recommendation Unit of the item");
			recommendationUnitAndItemPair = exportDao.getCategoryAndRecomUnitOfItem(conn, expiredItemsOnNextWeek, Constants.RECOMMENDATIONUNIT);

			setFinalList(finalListOfReport, expiredItemsOnNextWeek, categoryAndItemPair, recommendationUnitAndItemPair);
			
			logger.info("# of records in final List: " + finalListOfReport.size());

			
			try {
				logger.info("exportItemsRestrictionExpiration() - Generating Restriction Expiration Report - Started");
				//writeCSVForExpiredItemsOnNextWeek(finalListOfReport, sourceFile);
				writeExcelFileForExpiredItemsOnNextWeek(finalListOfReport, sourceFile);
				logger.info("exportItemsRestrictionExpiration() - Generating Restriction Expiration Report - Done");
				logger.info(
						"exportItemsRestrictionExpiration() - # of records in File: " + finalListOfReport.size());
			} catch (IOException e) {
				logger.error("Error while writing CSV file for Restriction Expiration Report ", e);
			}
			
			boolean mailStatus = sendMail(false);
			if(mailStatus) {
				logger.info("exportItemsRestrictionExpiration() - Mail is sent");
			}else {
				logger.error("exportItemsRestrictionExpiration() - Error while sending mail");
			}
			}
			else {
				//writeCSVForExpiredItemsOnNextWeek(finalListOfReport, sourceFile);
				writeExcelFileForExpiredItemsOnNextWeek(finalListOfReport, sourceFile);
				boolean mailStatus = sendMail(true);
				if(mailStatus) {
					logger.info("exportItemsRestrictionExpiration() - Empty file is sent in mail");
				}else {
					logger.error("exportItemsRestrictionExpiration() - Error while sending empty file mail");
				}
			}
			
			PristineDBUtil.commitTransaction(conn, "Report Restriction Expiration");


		} catch (GeneralException | Exception e) {
			logger.error("exportItemsRestrictionExpiration() - Error while reporting Restriction Expiration", e);
			PristineDBUtil.rollbackTransaction(conn, "Report Restriction Expiration");
		} finally {
			PristineDBUtil.close(conn);
		}

	}
	

	

	
	

	

	


	private void updateExpiryItems(List<ReDTO> expiryItemList) throws GeneralException {		

		List<ReDTO> expiryItemListAtStoreList = expiryItemList.stream().filter(e -> !e.isRegularItem())
				.collect(Collectors.toList());
		List<ReDTO> expiryItemListRegular = expiryItemList.stream().filter(e -> e.isRegularItem())
				.collect(Collectors.toList());

		if (expiryItemListRegular.size() > 0) {
			REDao.updateExpiryExportFlagForRegularItemList(conn, expiryItemListRegular);
		}
		if (expiryItemListAtStoreList.size() > 0) {
			REDao.updateExpiryExportFlagNotRegular(conn, expiryItemListAtStoreList);
		}
	}
	

	private boolean sendMail(boolean emptyData) throws GeneralException {
		boolean mailStatus = false;
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MMddyy");
		String formattedDate = dateFormat.format(date);
		String from = PropertyManager.getProperty("MAIL.FROMADDR");
		if (from.isEmpty())
			throw new GeneralException("Unable to send mail without MAIL.FROMADDR property in analysis.properties");
		String mailContent = "";
		String subject = "Restriction Expiration Report - " + formattedDate;
		if(emptyData) {
			mailContent = "No restrictions expiring in next 7 days.\r\n" + 
					"\r\n" + 					
					"-Presto Support team\r\n" + 
					"\r\n" + 
					"This email communication including its attachments is intended only for the use of the individuals "
					+ "to whom it has been addressed. It may contain information that is privileged and confidential to "
					+ "Pristine Infotech, Inc. If the reader of this message is not the intended recipient, you are hereby "
					+ "notified that any dissemination, distribution or copying of this communication is strictly prohibited. "
					+ "If you have read this communication in error, please notify us immediately by return e-mail.";
		}else {
			mailContent = 					
					"-Presto Support team\r\n" + 
					"\r\n" + 
					"This email communication including its attachments is intended only for the use of the individuals "
					+ "to whom it has been addressed. It may contain information that is privileged and confidential to "
					+ "Pristine Infotech, Inc. If the reader of this message is not the intended recipient, you are hereby "
					+ "notified that any dissemination, distribution or copying of this communication is strictly prohibited. "
					+ "If you have read this communication in error, please notify us immediately by return e-mail.";
		}
		
			
		attachmentFiles.add(attachmentFile);
				
		mailStatus = EmailService.sendEmail(from, getMailToList(), null,
				null, subject, mailContent, attachmentFiles);
		return mailStatus;
	}

	private List<String> getMailToList() {	
		
		List<String> mailToList = null;
		if(mailTo != null&& !Constants.EMPTY.equals(mailTo)){
			String[] mailToArr = mailTo.split(";");
			mailToList = new ArrayList<String>();
			for(String to: mailToArr){
				mailToList.add(to);
			}
		}
		return mailToList;
	}

	private void setFinalList(List<ReDTO> finalListOfReport, HashMap<Integer, List<ReDTO>> expiredItemsOnNextWeek,
			HashMap<Integer, String> categoryAndItemPair, HashMap<Integer, String> recommendationUnitAndItemPair) {
		expiredItemsOnNextWeek.forEach((itemCode, itemList) -> {
			
			itemList.forEach(itemObj -> {
				ReDTO itemDTO = new ReDTO();
				itemDTO.setRetailerItemCode(itemObj.getRetailerItemCode());
				if(itemObj.getEndDate() == null || itemObj.getEndDate().isEmpty()) {
					itemDTO.setEndDate("");	
				}
				else {
					itemDTO.setEndDate(itemObj.getEndDate());
				}
				if(itemObj.getStoreNo() == null || itemObj.getStoreNo().isEmpty()) {
					itemDTO.setStoreNo("");	
				}
				else {
					itemDTO.setStoreNo(itemObj.getStoreNo());
				}				
				itemDTO.setCategory(categoryAndItemPair.get(itemCode));
				if(itemObj.getZoneName() == null || itemObj.getZoneName().isEmpty()) {
					itemDTO.setZoneName("");
				}else {
				itemDTO.setZoneName(itemObj.getZoneName());
				}
				if(itemObj.getZoneNo() == null || itemObj.getZoneNo().isEmpty()) {
					itemDTO.setZoneNo("");
				}else {
				itemDTO.setZoneNo(itemObj.getZoneNo());
				}
				if(itemObj.getStartDate() == null || itemObj.getStartDate().isEmpty()) {
					itemDTO.setStartDate("");	
				}
				else {
					itemDTO.setStartDate(itemObj.getStartDate());
				}
				itemDTO.setRecommendationUnit(recommendationUnitAndItemPair.get(itemCode));
				if (itemObj.getPriceCheckListTypeId() == null || itemObj.getPriceCheckListTypeId().isEmpty()) {
					itemDTO.setPriceCheckListTypeId("");
				}
				else {
					
					if (itemObj.getPriceCheckListTypeId().equals(Constants.LOCKED_RETAIL)) {
						itemDTO.setPriceCheckListTypeId("LOCKED_RETAIL");
					}
					if (itemObj.getPriceCheckListTypeId().equals(Constants.STORE_LOCK)) {
						itemDTO.setPriceCheckListTypeId("STORE_LOCK");
					}
					if (itemObj.getPriceCheckListTypeId().equals(Constants.MIN_MAX)) {
						itemDTO.setPriceCheckListTypeId("MIN-MAX");
					}
					if (itemObj.getPriceCheckListTypeId().equals(Constants.CLEARANCE_LIST_TYPE)) {
						itemDTO.setPriceCheckListTypeId("CLEARANCE");
					}
				}
				if(itemObj.getItemListComments() == null || itemObj.getItemListComments().isEmpty()) {
					itemDTO.setItemListComments("");	
				}
				else {
					itemDTO.setItemListComments(itemObj.getItemListComments());
				}
				if(itemObj.getECRetail() == null || itemObj.getECRetail().isEmpty()) {
					itemDTO.setECRetail("");	
				}
				else {
					itemDTO.setECRetail(itemObj.getECRetail());
				}
				
				if(itemObj.getItemName() == null || itemObj.getItemName().isEmpty()) {
					itemDTO.setItemName("");	
				}
				else {
					itemDTO.setItemName(itemObj.getItemName());
				}
				
				if(itemObj.getListName() == null || itemObj.getListName().isEmpty()) {
					itemDTO.setListName("");	
				}
				else {
					itemDTO.setListName(itemObj.getListName());
				}
				
				if(itemObj.getUserName() == null || itemObj.getUserName().isEmpty()) {
					itemDTO.setUserName("");	
				}
				else {
					itemDTO.setUserName(itemObj.getUserName());
				}

				finalListOfReport.add(itemDTO);
				
			});
		});

	}

	private void writeCSVForExpiredItemsOnNextWeek(List<ReDTO> finalListOfReport, File sourceFile) throws IOException {

		FileWriter fw;		

		fw = new FileWriter(sourceFile);

		pw = new PrintWriter(fw);

		String separator = "|";
		
		pw.print("Store");
		pw.print(separator);
		pw.print("Zone");
		pw.print(separator);
		pw.print( "Zone Name");
		pw.print(separator);
		pw.print("Category");
		pw.print(separator);
		pw.print("Recom Unit");
		pw.print(separator);
		pw.print("Item Code");
		pw.print(separator);
		pw.print("Item Name");
		pw.print(separator);
		pw.print("Retail");
		pw.print(separator);
		pw.print("Start date");
		pw.print(separator);
		pw.print("End date");
		pw.print(separator);
		pw.print("Type");
		pw.print(separator);
		pw.print("Comments");
		pw.println();

		if (finalListOfReport.size() > 0) {

			for (ReDTO item : finalListOfReport) {

				pw.print(item.getStoreNo());
				pw.print(separator);
				pw.print(item.getZoneNo());
				pw.print(separator);
				pw.print(item.getZoneName());
				pw.print(separator);
				pw.print(item.getCategory());
				pw.print(separator);
				pw.print(item.getRecommendationUnit());
				pw.print(separator);
				pw.print(item.getRetailerItemCode());
				pw.print(separator);
				pw.print(item.getItemName());
				pw.print(separator);
				pw.print(item.getECRetail());
				pw.print(separator);
				pw.print(item.getStartDate());
				pw.print(separator);
				pw.print(item.getEndDate());
				pw.print(separator);
				pw.print(item.getPriceCheckListTypeId());
				pw.print(separator);
				pw.print(item.getItemListComments());
				pw.println();

			}
		}
		pw.flush();
		pw.close();
		fw.close();

	}
	private void writeExcelFileForExpiredItemsOnNextWeek(List<ReDTO> finalListOfReport,  File sourceFile) throws IOException {
		// Create a Workbook
        Workbook workbook = new XSSFWorkbook(); // new HSSFWorkbook() for generating `.xls` file
        
        String[] columns = {"List Name", "Type", "Store", "Zone", "Zone Name", "Category", "Recom unit", "Item code", "Item Name", "Retail", 
        		"Start Date", "End Date",  "Created By", "Comments"};

        CreationHelper createHelper = workbook.getCreationHelper();

        // Create a Sheet
        Sheet sheet = workbook.createSheet("Restriction Expiration");

        // Create a Font for styling header cells
        Font headerFont = workbook.createFont();
        headerFont.setBold(true);
        headerFont.setFontHeightInPoints((short) 14);
        headerFont.setColor(IndexedColors.ROYAL_BLUE.getIndex());

        // Create a CellStyle with the font
        CellStyle headerCellStyle = workbook.createCellStyle();
        headerCellStyle.setFont(headerFont);

        // Create a Row
        Row headerRow = sheet.createRow(0);

        // Create cells
        for(int i = 0; i < columns.length; i++) {
            Cell c = headerRow.createCell(i);
            c.setCellValue(columns[i]);
            c.setCellStyle(headerCellStyle);
        }

        // Create Cell Style for formatting Date
        CellStyle dateCellStyle = workbook.createCellStyle();
        dateCellStyle.setDataFormat(createHelper.createDataFormat().getFormat("MM-dd-yyyy"));

        // Create Other rows and cells with employees data
        int row = 1;
        for(ReDTO item: finalListOfReport) {
        	int cellCount = -1;
			Row r = sheet.createRow(row++);
			Cell c = r.createCell(++cellCount);
			c.setCellValue(item.getListName());
			
			c = r.createCell(++cellCount);
			c.setCellValue(item.getPriceCheckListTypeId());
			
			c = r.createCell(++cellCount);
			c.setCellValue(item.getStoreNo());			

			c = r.createCell(++cellCount);
			c.setCellValue(item.getZoneNo());

			c = r.createCell(++cellCount);
			c.setCellValue(item.getZoneName());
			
			c = r.createCell(++cellCount);
			c.setCellValue(item.getCategory());			

			c = r.createCell(++cellCount);
			c.setCellValue(item.getRecommendationUnit());
			
			c = r.createCell(++cellCount);
			c.setCellValue(item.getRetailerItemCode());
			
			c = r.createCell(++cellCount);
			c.setCellValue(item.getItemName());

			c = r.createCell(++cellCount);
			c.setCellValue(item.getECRetail());
			
			c = r.createCell(++cellCount);
			c.setCellValue(item.getStartDate());
			c.setCellStyle(dateCellStyle);

			c = r.createCell(++cellCount);
			c.setCellValue(item.getEndDate());
			c.setCellStyle(dateCellStyle);

			c = r.createCell(++cellCount);
			c.setCellValue(item.getUserName());

			c = r.createCell(++cellCount);
			c.setCellValue(item.getItemListComments());

        }

		// Resize all columns to fit the content size
        for(int i = 0; i < columns.length; i++) {
            sheet.autoSizeColumn(i);
        }
              
        FileOutputStream fileOut = new FileOutputStream(sourceFile, true);
        workbook.write(fileOut);
        fileOut.close();

        // Closing the workbook
        workbook.close();
    }		
}
