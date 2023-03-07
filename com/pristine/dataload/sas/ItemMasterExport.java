package com.pristine.dataload.sas;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.email.EmailService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;


/**
 * @author Pradeepkumar
 */

public class ItemMasterExport {

	private static Logger logger = Logger.getLogger("ItemMasterExport");
	private final static int LOG_RECCORD_COUNT = 25000;
	private static String relativeOutputPath = null;
	private String templatePath = null;
	private String mailTo = null;
	private String mailCC = null;
	private String mailBCC = null;
	
	Connection conn = null;
	ItemDAO itemDAO = null;
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-item-master-export.properties");
		PropertyManager.initialize("analysis.properties");
		ItemMasterExport itemMasterExport = new ItemMasterExport();
		logger.info("*******************************************");
		itemMasterExport.exportItemMaster();
		logger.info("*******************************************");
	}
	
	
	/**
	 * Export items in excel file
	 */
	private void exportItemMaster(){
		try{
			init();
			
			logger.info("exportItemMaster() - Started getting all items...");
			List<ItemDTO> itemList = itemDAO.getItemLookupForExport(conn);
			logger.info("exportItemMaster() - Started getting all items is completed.");
			
			writeExcelFile(itemList);
			
			//sendMail();
		}catch(GeneralException ge){
			logger.error("Error -- exportItemMaster()", ge);
		}
		finally {
			PristineDBUtil.close(conn);
		}
	}
	
	
	/**
	 * initilize
	 */
	private void init(){
		conn = getConnection();
		itemDAO = new ItemDAO();
		templatePath = PropertyManager.getProperty("SAS_EXPORT.TEMPLATE_FILE");
		mailTo = PropertyManager.getProperty("SAS_EXPORT.MAILTO");
		mailCC = PropertyManager.getProperty("SAS_EXPORT.MAILCC");
		mailBCC = PropertyManager.getProperty("SAS_EXPORT.MAILBCC");
		relativeOutputPath = PropertyManager.getProperty("SAS_EXPORT.OUTPUTFOLDER");
		//relativeOutputPath = rootPath + relativeOutputPath;
	}
	
	/**
	 * Writes file with given template
	 * @param itemList
	 * @return output file
	 * @throws GeneralException
	 */
	private String writeExcelFile(List<ItemDTO> itemList) throws GeneralException{
		String outFile = null;
		try {
			/*Date date = new Date();
			SimpleDateFormat dateFormat = new SimpleDateFormat("MMddyy");
			String formattedDate = dateFormat.format(date);*/
			outFile = relativeOutputPath + "/Item_master_export.xlsx";
			FileInputStream inputStream = new FileInputStream(templatePath);
			XSSFWorkbook xssfwb = new XSSFWorkbook(inputStream);

	        SXSSFWorkbook wb = new SXSSFWorkbook(xssfwb, 100);
			//SXSSFWorkbook workbook = new SXSSFWorkbook(inputStream);
			Sheet sheet =  wb.getSheet("Sheet1");
			int rowNum = 1;
			for(ItemDTO itemDTO: itemList){
				Row row = sheet.getRow(rowNum);
				if(row == null){
					row = sheet.createRow(rowNum);
				}
				rowNum++;
				if(rowNum % LOG_RECCORD_COUNT == 0){
					logger.info("writeExcelFile() - # of Rows processed -> " + rowNum);
				}
				writeToCell(itemDTO, row, sheet, null);	
			}
			FileOutputStream outputStream = new FileOutputStream(outFile);
			wb.write(outputStream);
			wb.close();
			inputStream.close();
			outputStream.close();
		} catch (Exception e) {
			throw new GeneralException("Error while writing excel file", e);
		}
		return outFile;
	}
	
	
	
	/**
	 * calls mail service and sends mail with produced excel report
	 * @param oosEmailAlertDTO
	 * @param file
	 * @return mail status
	 * @throws GeneralException
	 */
	@SuppressWarnings("unused")
	private boolean sendMail() throws GeneralException {
		boolean mailStatus = false;
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MMddyy");
		String formattedDate = dateFormat.format(date);
		String from = PropertyManager.getProperty("MAIL.FROMADDR", "");
		if (from.isEmpty())
			throw new GeneralException("Unable to send mail without MAIL.FROMADDR property in analysis.properties");
		
		String subject = "Item Master File - " + formattedDate;
		String mailContent = "Latest Item master export is ready in our FTP site. Please download."
				+ " \n - Presto Price Check Support Team.";
		mailStatus = EmailService.sendEmail(from, getMailToList(), getMailCCList(),
				getMailBCCList(), subject, mailContent, null);
		return mailStatus;
	}
	
	/**
	 * Writes each value to cell
	 * @param itemDTO
	 * @param row
	 * @param sheet
	 * @param style
	 */
	private void writeToCell(ItemDTO itemDTO, Row row, Sheet sheet, CellStyle style){
		int colCount = -1;
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getDeptShortName()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getDeptCode()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getDeptName()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getDeptCode()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getDeptShortName()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getCatName()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getCatCode()), sheet, null);
		setCellValue(row, ++colCount, Constants.EMPTY, sheet, null);
		setCellValue(row, ++colCount, Constants.EMPTY, sheet, null);
		setCellValue(row, ++colCount, Constants.EMPTY, sheet, null);
		setCellValue(row, ++colCount, Constants.EMPTY, sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getRetailerItemCode()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getItemName()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getSize()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getUom()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getPack()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getPrivateLabelCode().equals("Y") ? "1" : "0"), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getLikeItemGrp()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getLikeItemCode()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getUpc()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getRetailerName()), sheet, null);
		setCellValue(row, ++colCount, checkNullAndResturn(itemDTO.getBrandName()), sheet, null);
	}

	
	private String checkNullAndResturn(String val){
		return val == null ? Constants.EMPTY : val;
	}
	/**
	 * Sets String value to the cell
	 * @param row
	 * @param colCount
	 * @param value
	 * @param sheet
	 * @param cellStyle
	 */
	private void setCellValue(Row row, int colCount, String value, Sheet sheet, CellStyle cellStyle) {
		Cell cell;
		cell = row.getCell(colCount);
		if (cell == null)    
		    cell = row.createCell(colCount);
		cell.setCellValue(value);	
		cell.setCellStyle(sheet.getColumnStyle(colCount));
	}
	
	/**
	 * Sets double value to the cell
	 * @param row
	 * @param colCount
	 * @param value
	 * @param sheet
	 * @param cellStyle
	 */
	/*private void setCellValue(XSSFRow row, int colCount, double value,  XSSFSheet sheet, XSSFCellStyle cellStyle) {
		XSSFCell cell;
		cell = row.getCell(colCount);
		if (cell == null)    
		    cell = row.createCell(colCount);
		cell.setCellValue(value);	
		cell.setCellStyle(sheet.getColumnStyle(colCount));
	}*/
	
	/**
	 * Gives DB connection
	 * 
	 * @return Connection
	 */
	private Connection getConnection() {
		Connection conn = null;
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ge) {
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
		return conn;
	}
	
	
	public List<String> getMailToList() {
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
	
	public List<String> getMailBCCList() {
		List<String> mailBCCList = null;
		if(mailBCC != null && !Constants.EMPTY.equals(mailBCC)){
			String[] mailBCCArr = mailBCC.split(";");
			mailBCCList = new ArrayList<String>();
			for(String bcc: mailBCCArr){
				mailBCCList.add(bcc);
			}
		}
		return mailBCCList;
	}
	public List<String> getMailCCList() {
		List<String> mailCCList = null;
		if(mailCC != null && !Constants.EMPTY.equals(mailCC)){
			String[] mailCCArr = mailCC.split(";");
			mailCCList = new ArrayList<String>();
			for(String cc: mailCCArr){
				mailCCList.add(cc);
			}
		}
		return mailCCList;
	}

}
