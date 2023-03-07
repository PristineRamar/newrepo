package com.pristine.fileformatter.ahold;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.FuturePriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class FuturePriceFileFormatter {
	private static Logger logger = Logger.getLogger("FuturePriceFileFormatter");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private static String relativeInputPath, relativeOutputPath;
	
	private Connection conn = null;
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private String rootPath;
	List<FuturePriceDTO> futurePriceDataList;
	private Set<String> skippedRetailerItemcodes = null;
	public static final int LOG_RECORD_COUNT = 25000;
	
	private FuturePriceFileFormatter() {
		
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error("Error when creating connection - " + gex);
		}
	}
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-future-price-file-formatter.properties");
		PropertyManager.initialize("analysis.properties");
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				relativeOutputPath = arg.substring(OUTPUT_FOLDER.length());
			}
		}
		
		FuturePriceFileFormatter fileFormatter = new FuturePriceFileFormatter();
		try {
			fileFormatter.processFile();
		} catch (IOException | GeneralException e) {
			e.printStackTrace();
			logger.error("Error Processing File", e);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void processFile() throws GeneralException, IOException {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		ExcelFileParser parser = new ExcelFileParser();
		String inputPath = rootPath + "/" + relativeInputPath;
		parser.setFirstRowToProcess(1); // skip header row
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		mapFieldNames(fieldNames);
		ArrayList<String> fileList = parser.getFiles(relativeInputPath);
		String fileName = null;
		try {
			for (String processigFile : fileList) {
				String tempFileName = processigFile.replace(inputPath, "").toUpperCase().replace(".XLSX", "")
						.replace("/", "").trim();
				String outputPath = rootPath + "/" + relativeOutputPath + "/" + tempFileName + ".txt";
				File file = new File(outputPath);
				if (!file.exists())
					fw = new FileWriter(outputPath);
				else
					fw = new FileWriter(outputPath, true);

				pw = new PrintWriter(fw);
				logger.info("Processing file - " + processigFile);
				fileName = processigFile;
				String sheetName = "Sheet1";
				try{
					futurePriceDataList = parser.parseExcelFileV2(FuturePriceDTO.class, fileName, sheetName, fieldNames);
					processData();
					futurePriceDataList.clear();
				}catch(IllegalArgumentException ge){
					logger.error("Error when parsing or processing input file", ge);
				 		futurePriceDataList.clear();
					 }
				pw.flush();
				fw.flush();
				pw.close();
				fw.close();
				PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.COMPLETED_FOLDER);
			}
		} catch (GeneralException ge) {
			logger.error("Error when parsing or processing input file", ge);
			PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.BAD_FOLDER);
		} finally {
			PristineDBUtil.close(conn);
		}
	}
	
	private void mapFieldNames(TreeMap<Integer, String> fieldNames) {
		fieldNames.put(0, "storenum");
		fieldNames.put(1, "retailerItemCode");
		fieldNames.put(2, "effectivestartdate");
		fieldNames.put(3, "portfolio");
		fieldNames.put(4, "itemdesc");
		fieldNames.put(5, "offerqty");
		fieldNames.put(6, "offeramt");
		fieldNames.put(7, "regqty");
		fieldNames.put(8, "regprice");
		fieldNames.put(9, "item2");
		fieldNames.put(10, "item3");
		fieldNames.put(11, "strategyzone");
		fieldNames.put(12, "diff");
	}
	
	private void processData() throws GeneralException {
		skippedRetailerItemcodes = new HashSet<>();
		ItemDAO itemDAO = new ItemDAO();
		Set<String> retItemcodeSet = new HashSet<>();
		for(FuturePriceDTO futurePriceDTO: futurePriceDataList){
			
			int retItemCode = (int)Double.parseDouble(futurePriceDTO.getRetailerItemCode());
			futurePriceDTO.setRetailerItemCode(String.valueOf(retItemCode));
			retItemcodeSet.add(futurePriceDTO.getRetailerItemCode());
			
			int compStrNo = (int)Double.parseDouble(futurePriceDTO.getCompStrNo());
			futurePriceDTO.setCompStrNo(String.valueOf(compStrNo));
			
			int stDate = (int)Double.parseDouble(futurePriceDTO.getEffectivestartdate());
			Date startDate= org.apache.poi.ss.usermodel.DateUtil.getJavaDate((double) stDate);
			futurePriceDTO.setEffectivestartdate(new SimpleDateFormat(Constants.APP_DATE_FORMAT).format(startDate));
		
		}
		logger.info("processCachedList() - "
				+ " Distinct retailer item code size -> "
						+ retItemcodeSet.size());
		
		long startTimeUPCFetch = System.currentTimeMillis();
		HashMap<String, List<String>> upcMap = 
				itemDAO.getUPCListForRetItemcodes(conn, retItemcodeSet);
		long endTimeUPCFetch = System.currentTimeMillis();
		logger.info("processCachedList() - Time taken to fetch UPCs -> " 
				+ (endTimeUPCFetch - startTimeUPCFetch) + " ms");
		
		
		int recordCount = 0;
		int recordsWithPastDates = 0;
		for(FuturePriceDTO futurePriceDTO: futurePriceDataList){
			recordCount++;
			
			LocalDate effectiveDate = LocalDate.parse(futurePriceDTO.getEffectivestartdate(), PRCommonUtil.getDateFormatter());
			LocalDate today = LocalDate.now();
			
			if(effectiveDate.isBefore(today)){
				logger.info("Effective Date is not a Future Date - " + effectiveDate);
				recordsWithPastDates++;
				continue;
			}
			
			if(upcMap.get(futurePriceDTO.getRetailerItemCode()) == null){
				skippedRetailerItemcodes.add(futurePriceDTO.getRetailerItemCode());
			}
			else
			{
				List<String> upcs = upcMap.get(futurePriceDTO.getRetailerItemCode());
				for(String upc: upcs){
					pw.print(upc); // UPC #1
					pw.print("|");
					pw.print(futurePriceDTO.getRetailerItemCode()); //Retailer item code #2
					pw.print("|");
					pw.print("2");// Level Type ID # 3
					pw.print("|");
					pw.print(futurePriceDTO.getCompStrNo());// Store Number # 4
					pw.print("|");
					pw.print(futurePriceDTO.getEffectivestartdate()); // Effective Start Date # 5
					pw.print("|");
					pw.print(new DecimalFormat("#0.00").format(futurePriceDTO.getOfferAmt()));// Future Regular Retail # 6
					pw.print("|");
					pw.print(futurePriceDTO.getOfferQty());// Future Regular Multiple # 7
					pw.print("|");
					pw.print("|"); // # 8
					pw.print("|"); // # 9
					pw.print("0.0");// Sale Price # 10
					pw.print("|");
					pw.print("0");// Sale Multiple # 11
					pw.println();
				}
			}
			if(recordCount % LOG_RECORD_COUNT == 0){
				logger.info("# of Processed Records - " + recordCount);
				logger.info("# of Records with Past Dates - " + recordsWithPastDates);
			}
		}
		
	}
	
}
