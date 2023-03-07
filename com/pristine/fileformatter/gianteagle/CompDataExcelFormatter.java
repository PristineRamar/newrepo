package com.pristine.fileformatter.gianteagle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleCompDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.ExcelFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class CompDataExcelFormatter {
	private static Logger logger = Logger.getLogger("CompDataExcelFormatter");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private final static String FILE_TYPE = "FILE_TYPE=";
	private final static String STORE_NUM = "STORE_NUM=";
	private String rootPath;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private int recordCount = 0;
	private int skippedCount = 0;
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private static String relativeInputPath, relativeOutputPath;
	private Connection conn = null;
	List<String> skippedRetailerItemcodes = null;
	List<CompetitiveDataDTO> compDataList;
	CompetitiveDataDAO competitiveDataDAO;
//	HashMap<String, String> compStoreDetails;
	HashMap<String, List<String>> retailerItemBasedOnUPC;
	HashMap<String, List<String>> upcBasedOnCompSKU;
	private static String fileType, compStoreNum;
	List<String>skippedUPC;
	
	private CompDataExcelFormatter() {
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException gex) {
			logger.error("Error when creating connection - " + gex);
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-comp-data-Excelformatter.properties");
		PropertyManager.initialize("analysis.properties");
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				relativeOutputPath = arg.substring(OUTPUT_FOLDER.length());
			}
			if(arg.startsWith(FILE_TYPE)){
				fileType = arg.substring(FILE_TYPE.length());
			}
			if(arg.startsWith(STORE_NUM)){
				compStoreNum = arg.substring(STORE_NUM.length());
			}
		}
		CompDataExcelFormatter fileFormatter = new CompDataExcelFormatter();
		try {
			fileFormatter.processFile();
		} catch (IOException | GeneralException e) {
			e.printStackTrace();
		}
	}

	private void processFile() throws GeneralException, IOException {
		competitiveDataDAO =new CompetitiveDataDAO(conn);
		ExcelFileParser parser = new ExcelFileParser();
		String inputPath = rootPath + "/" + relativeInputPath;
		parser.setFirstRowToProcess(2); // skip header row
		TreeMap<Integer, String> fieldNames = new TreeMap<Integer, String>();
		mapFieldNames(fieldNames);
		ArrayList<String> fileList = parser.getFiles(relativeInputPath);

		// Get comp str no from competitor store table
//		logger.info("Cache Store number from Competitor store");
//		compStoreDetails = competitiveDataDAO.getCompStoreNO();
		logger.info("Cache Retailer item codes");
		ItemDAO itemDAO = new ItemDAO();
		retailerItemBasedOnUPC = itemDAO.getRetItemCodeBasedOnUPC(conn);
		upcBasedOnCompSKU = competitiveDataDAO.getCompUPCMapping(conn, compStoreNum);
		String fileName = null;
		try {
			for (String processigFile : fileList) {
				skippedUPC = new ArrayList<String>();
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
				// Get store name from file name
				// Get check data from File name
				String StoreName = null, checkDate = null;
				String[] tempFileNames = tempFileName.split("-");
				if (tempFileNames != null) {
//					StoreName = (tempFileNames[0] + "-" + tempFileNames[1]).toUpperCase();
					try{
						checkDate = tempFileNames[2].replace(".", "").substring(0, 8);
					}catch(Exception ge){
						checkDate = tempFileNames[2].replace(".", "").substring(0, 6);
						checkDate = DateUtil.dateToString(DateUtil.toDate(checkDate, "MMddyy"), "yyyyMMdd");
					}
					
				} else {
					logger.error("File name is not found");
				}
				int sheetNo = 0;
				try{
					compDataList = parser.parseExcelFile(CompetitiveDataDTO.class, fileName, sheetNo, fieldNames);
					processCompData(checkDate);
					compDataList.clear();
				}catch(IllegalArgumentException ge){
					logger.error("Error when parsing or processing input file", ge);
					try{
						sheetNo = 1;
						compDataList.clear();
						compDataList = parser.parseExcelFile(CompetitiveDataDTO.class, fileName, sheetNo, fieldNames);
						processCompData(checkDate);
						compDataList.clear();
					}catch(Exception e){
						logger.info("Error when parsing or processing input file", e);
						throw new GeneralException("Error when parsing or processing input file", e);
					}
					
				}
				
				
				pw.flush();
				fw.flush();
				pw.close();
				fw.close();
				logger.error("# of Retailer item code not found based on UPC:"+skippedUPC.size());
//				logger.debug("List Of UPC where retailer item code not found: "+PRCommonUtil.getCommaSeperatedStringFromStrArray(skippedUPC));
				PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.COMPLETED_FOLDER);
			}
		} catch (GeneralException ge) {
			logger.error("Error when parsing or processing input file", ge);
			PrestoUtil.moveFile(fileName, inputPath + "/" + Constants.BAD_FOLDER);
		} finally {
			PristineDBUtil.close(conn);
		}
	}

	private boolean checkUPCValueFormat(String upc){
		boolean isUPCParsed = true;
		try{
			Long.toString((long) (Double.valueOf(upc).longValue()));
		}catch(Exception e){
			isUPCParsed = false;
		}
		return isUPCParsed;
	}
	
	private void processCompData(String checkDate) throws GeneralException {
		// Get Comp store Number
//		String compStoreNum = compStoreDetails.get(storeName);
		int noOfEntryWithoutUPC =0;
		if (compStoreNum != null) {
			for (CompetitiveDataDTO competitiveDataDTO : compDataList) {
				if (competitiveDataDTO.upc != null && !competitiveDataDTO.upc.isEmpty()) {
					
					List<String> UPCs = new ArrayList<>();
					
					if(checkUPCValueFormat(competitiveDataDTO.upc)) {
						// To convert exponentials to String
						competitiveDataDTO.upc = Long.toString((long) (Double.valueOf(competitiveDataDTO.upc).longValue()));
						// Assign UPC as per Presto Standard
						String UPCWithNoCheckDigit;
						String upc = String.valueOf(competitiveDataDTO.upc);
						if (upc.length() > 11)
							UPCWithNoCheckDigit = upc.substring(0, 11);
						else
							UPCWithNoCheckDigit = upc;
						upc = PrestoUtil.castUPC(UPCWithNoCheckDigit, false);

						UPCs.add(upc);
						// Dinesh::11-Dec-2017. Changes done to consider UPC from GE given comp upc mapping
						// To load Perishable related items
						boolean processOnlyUPCBasedOnSkuItems = Boolean
								.parseBoolean(PropertyManager.getProperty("PROCESS_ONLY_ITEM_FROM_COMP_UPC_MAPPING", "FALSE"));
						if (processOnlyUPCBasedOnSkuItems) {
							UPCs.clear();
						}
					}

					if (upcBasedOnCompSKU != null && upcBasedOnCompSKU.size() > 0) {

						String key = null;
						if (fileType.equals("TYPE_2") || fileType.equals("TYPE_3")) {
							key = competitiveDataDTO.upc;
						} else if (competitiveDataDTO.compSKU != null && !competitiveDataDTO.compSKU.isEmpty()
								&& checkUPCValueFormat(competitiveDataDTO.compSKU)) {
							competitiveDataDTO.compSKU = Long.toString((long) (Double.valueOf(competitiveDataDTO.compSKU).longValue()));
							key = competitiveDataDTO.compSKU;
						}

						if (key != null && upcBasedOnCompSKU.containsKey(key)) {
							// High priority given to Comp UPC Mapping table than the comp UPC's
							UPCs.clear();

							upcBasedOnCompSKU.get(key).forEach(value -> {
								UPCs.add(value);
							});
						}
					}
					
					// If processing for Kroger, then handle perishable items price using PPU price if available
					if (fileType.equals("TYPE_3")) {
						if (competitiveDataDTO.salePricePPU != null && !competitiveDataDTO.salePricePPU.isEmpty()) {
							Matcher matcher = Pattern.compile("(\\d+[.]\\d+)").matcher(competitiveDataDTO.salePricePPU);
							if (matcher.find()) {
								float salePrice = Float.parseFloat(matcher.group(0));
								if (salePrice > 0) {
									
									// To find regular price using Sale Price PPU and each reg price
									competitiveDataDTO.regPrice = salePrice * (competitiveDataDTO.regPrice/competitiveDataDTO.fSalePrice);
									
									competitiveDataDTO.fSalePrice = salePrice;
								}
							}
						} else if (competitiveDataDTO.regPricePPU != null && !competitiveDataDTO.regPricePPU.isEmpty()) {
							Matcher matcher = Pattern.compile("(\\d+[.]\\d+)").matcher(competitiveDataDTO.regPricePPU);
							if (matcher.find()) {
								float regPrice = Float.parseFloat(matcher.group(0));
								if (regPrice > 0) {
									competitiveDataDTO.regPrice = regPrice;
								}
							}
						}
					}
					for (String compUPC : UPCs) {
						if (retailerItemBasedOnUPC.containsKey(compUPC)) {
							if (competitiveDataDTO.regPrice > 0) {
								List<String> retailerItemList = retailerItemBasedOnUPC.get(compUPC);
								for (String retailerItemCode : retailerItemList) {
									if (retailerItemCode != null) {
										pw.print(compStoreNum);// Store Number # 1
										pw.print("|");
										pw.print(DateUtil.dateToString(DateUtil.toDate(checkDate, "yyyyMMdd"), Constants.APP_DATE_FORMAT));
										pw.print("|");
										pw.print(retailerItemCode); // Item Code # 3
										pw.print("|"); // pw.print(competitiveDataDTO.itemName); // Item Desc # 4
										pw.print(""); // Item Desc # 4
										pw.print("|");
										pw.print("1");// Regular Quantity # 5
										pw.print("|");
										if(competitiveDataDTO.regMultiple > 0)
										{
											pw.print(new DecimalFormat("#0.00").format(competitiveDataDTO.regPrice/competitiveDataDTO.regMultiple));// Regular Retail # 6
										}
										else
										{
											pw.print(new DecimalFormat("#0.00").format(competitiveDataDTO.regPrice));// Regular Retail # 6
										}
										pw.print("|");
										pw.print("");// Size # 7
										pw.print("|");
										pw.print(compUPC);// UPC # 8
										pw.print("|");
										pw.print("N");// Outside Indicator # 9
										pw.print("|");
										pw.print("");// Additional Info # 10
										pw.print("|");
										if (competitiveDataDTO.fSalePrice > 0) {
											pw.print("1"); // Sale Quantity # 11
											pw.print("|");
											if(competitiveDataDTO.saleMultiple > 0)
											{
												pw.print(new DecimalFormat("##0.00").format(competitiveDataDTO.fSalePrice/competitiveDataDTO.saleMultiple)); // Sale Retail # 12
											}
											else
											{
												pw.print(new DecimalFormat("##0.00").format(competitiveDataDTO.fSalePrice)); // Sale Retail # 12
											}
											
											pw.print("|");
											pw.print(DateUtil.dateToString(DateUtil.toDate(checkDate, "yyyyMMdd"), Constants.APP_DATE_FORMAT));
										} else {
											pw.print(""); // Sale Quantity # 11
											pw.print("|");
											pw.print(""); // Sale Retail # 12
											pw.print("|");
											pw.print(""); // Sale check date # 13
										}
										pw.println("       "); // spaces
									}
								}
							}
						} else {
							skippedUPC.add(compUPC);
						}
					}
				} else {
					noOfEntryWithoutUPC++;
				}
			}
			if (noOfEntryWithoutUPC > 0) {
				logger.error("No of records got no UPC: " + noOfEntryWithoutUPC);
			}
		}else{
			logger.error("Store number is not given");
			throw new GeneralException("Store number is not given");
		}
	}

	private void mapFieldNames(TreeMap<Integer, String> fieldNames) {
		if(fileType.equals("TYPE_1")){
			fieldNames.put(1, "compSKU");
			fieldNames.put(2, "upc");
			fieldNames.put(3, "itemName");
			fieldNames.put(4, "size");
			fieldNames.put(5, "uom");
			fieldNames.put(6, "regPrice");
			fieldNames.put(7, "fSalePrice");
		}else if(fileType.equals("TYPE_2")){
			fieldNames.put(1, "upc");
			fieldNames.put(2, "itemName");
			fieldNames.put(3, "size");
			fieldNames.put(4, "uom");
			fieldNames.put(5, "deptName");
			fieldNames.put(6, "categoryName");
			fieldNames.put(7, "regPrice");
			fieldNames.put(8,"fSalePrice");
		}else if(fileType.equals("TYPE_3")){
			fieldNames.put(1, "upc");
			fieldNames.put(2, "itemName");
			fieldNames.put(3, "size");
			fieldNames.put(4, "deptName");
			fieldNames.put(5, "categoryName");
			fieldNames.put(6, "subCategory");
			fieldNames.put(7, "regMultiple");
			fieldNames.put(8, "regPrice");
			fieldNames.put(9, "saleMultiple");
			fieldNames.put(10, "fSalePrice");
			fieldNames.put(11,"effSaleEndDate");
			fieldNames.put(12, "regPricePPU");
			fieldNames.put(13, "salePricePPU");
//			fieldNames.put(11, "salePricePPU");
		}else if(fileType.equals("TYPE_4")){
			fieldNames.put(1, "compSKU");
			fieldNames.put(2, "upc");
			fieldNames.put(3, "itemName");
			fieldNames.put(4, "size");
			fieldNames.put(5, "regMPack");
			fieldNames.put(6, "regPrice");
			fieldNames.put(7, "saleMPack");
			fieldNames.put(8, "fSalePrice");
		}else if(fileType.equals("TYPE_5")){
			fieldNames.put(1, "compSKU");
			fieldNames.put(2, "upc");
			fieldNames.put(3, "itemName");
			fieldNames.put(4, "regMPack");
			fieldNames.put(5, "regPrice");
			fieldNames.put(6, "saleMPack");
			fieldNames.put(7, "fSalePrice");
		}else if(fileType.equals("TYPE_6")){
			fieldNames.put(1, "compSKU");
			fieldNames.put(2, "upc");
			fieldNames.put(3, "itemName");
			fieldNames.put(4, "size");
			fieldNames.put(5, "uom");
			fieldNames.put(6, "deptName");
			fieldNames.put(7, "categoryName");
			fieldNames.put(8, "regPrice");
			fieldNames.put(9,"fSalePrice");
		}
	}
}
