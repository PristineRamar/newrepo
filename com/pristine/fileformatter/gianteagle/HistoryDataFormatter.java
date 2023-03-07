package com.pristine.fileformatter.gianteagle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.pristine.dto.fileformatter.gianteagle.HistoryDataFormatterDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class HistoryDataFormatter extends PristineFileParser {
	private static Logger logger = Logger.getLogger("HistoryDataFormatterGE");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_COST_FOLDER = "COST_FOLDER=";
	private final static String OUTPUT_PRICE_FOLDER = "PRICE_FOLDER=";
	private final static String OUTPUT_ALLOWANCE_FOLDER = "ALLOWANCE_FOLDER=";
	private final static String OUTPUT_SCANBACK_FOLDER = "SCANBACK_FOLDER=";
	private final static String MODE = "MODE=";
	private final static String FORMAT_MODE = "FORMAT";
	private String rootPath;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private int recordCount = 0;
	private int skippedCount = 0;
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private static String relativeInputPath, relativeOutputPath, mode = FORMAT_MODE;
	private static String costFolder;
	private static String priceFolder;
	private static String allowanceFolder;
	private static String scanBackFolder;
	List<HistoryDataFormatterDTO> _itemList = null;
	String priceTypeInd = "PPR";

	public static void main(String[] args) {
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_COST_FOLDER)) {
				costFolder = arg.substring(OUTPUT_COST_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_PRICE_FOLDER)) {
				priceFolder = arg.substring(OUTPUT_PRICE_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_ALLOWANCE_FOLDER)) {
				allowanceFolder = arg.substring(OUTPUT_ALLOWANCE_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_SCANBACK_FOLDER)) {
				scanBackFolder = arg.substring(OUTPUT_SCANBACK_FOLDER.length());
			}
			if (arg.startsWith(MODE)) {
				mode = arg.substring(MODE.length());
			}
		}
		HistoryDataFormatter historyDataFormatter = new HistoryDataFormatter();
		historyDataFormatter.processFile();
	}

	/**
	 * Fill all columns in list and processing the file.
	 */
	private void processFile() {

		_itemList = new ArrayList<HistoryDataFormatterDTO>();
		super.headerPresent = true;
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}

		logger.info("Formatting History data file Started... ");
		try {
			parseFile(fieldNames);
		} catch (IOException | GeneralException e) {
			logger.error("Error -- processFile()", e);
		}
		logger.info("Total Number of Items " + recordCount);
		logger.info("Formatting History data file Completed... ");
	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<HistoryDataFormatterDTO> historyDataFormatterDTOs = (List<HistoryDataFormatterDTO>) listobj;
		for (HistoryDataFormatterDTO historyDataFormatterDTO : historyDataFormatterDTOs) {
			_itemList.add(historyDataFormatterDTO);
			recordCount++;
		}
//		logger.info("Number of Records Processed: " + recordCount);
	}

	/**
	 * Get the Path of Input file, and delimiting.
	 * 
	 * @param fieldNames
	 * @throws GeneralException
	 * @throws IOException
	 */
	private void parseFile(String fieldNames[]) throws GeneralException, IOException {

		try {
			// getzip files
			ArrayList<String> zipFileList = getZipFiles(relativeInputPath);
			headerPresent = true;
			// Start with -1 so that if any regular files are present, they are
			// processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;

			String zipFilePath = getRootPath() + "/" + relativeInputPath;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;

				try {
					if (processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

					fileList = getFiles(relativeInputPath);

					for (int i = 0; i < fileList.size(); i++) {

						long fileProcessStartTime = System.currentTimeMillis();

						recordCount = 0;
						skippedCount = 0;

						String files = fileList.get(i);
						logger.info("File Name - " + files);
						int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
						super.parseDelimitedFile(HistoryDataFormatterDTO.class, fileList.get(i), '|', fieldNames,
								stopCount);
						String outputFileName[] = files.split("/");
						logger.info("Output File name - " + outputFileName[outputFileName.length - 1]);
						if (costFolder != null) {
							relativeOutputPath = costFolder;
							String outputPath = rootPath + "/" + relativeOutputPath + "/"
									+ outputFileName[outputFileName.length - 1];

							File file = new File(outputPath);
							if (!file.exists())
								fw = new FileWriter(outputPath);
							else
								fw = new FileWriter(outputPath, true);

							pw = new PrintWriter(fw);
							logger.info("Processing Cost Records ...");
							writeRetailCostFile();
							pw.flush();
							fw.flush();
							pw.close();
							fw.close();
						}
						if (priceFolder != null) {
							relativeOutputPath = priceFolder;
							String outputPath = rootPath + "/" + relativeOutputPath + "/"
									+ outputFileName[outputFileName.length - 1];

							File file = new File(outputPath);
							if (!file.exists())
								fw = new FileWriter(outputPath);
							else
								fw = new FileWriter(outputPath, true);

							pw = new PrintWriter(fw);
							logger.info("Processing Price Records ...");
							writeRetailPriceFile();
							pw.flush();
							fw.flush();
							pw.close();
							fw.close();
						}
						if (allowanceFolder != null) {
							relativeOutputPath = allowanceFolder;
							String outputPath = rootPath + "/" + relativeOutputPath + "/"
									+ outputFileName[outputFileName.length - 1];

							File file = new File(outputPath);
							if (!file.exists())
								fw = new FileWriter(outputPath);
							else
								fw = new FileWriter(outputPath, true);

							pw = new PrintWriter(fw);
							logger.info("Processing Allowance Records ...");
							writeAllowanceFile();
							pw.flush();
							fw.flush();
							pw.close();
							fw.close();
						}
						if (scanBackFolder != null) {
							relativeOutputPath = scanBackFolder;
							String outputPath = rootPath + "/" + relativeOutputPath + "/"
									+ outputFileName[outputFileName.length - 1];

							File file = new File(outputPath);
							if (!file.exists())
								fw = new FileWriter(outputPath);
							else
								fw = new FileWriter(outputPath, true);

							pw = new PrintWriter(fw);
							logger.info("Processing ScanBack Records ...");
							writeScanBackFile();
							pw.flush();
							fw.flush();
							pw.close();
							fw.close();
						}

						long fileProcessEndTime = System.currentTimeMillis();

						logger.info("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime)
								+ "ms");
					}
				} catch (GeneralException | Exception ex) {
					logger.error("GeneralException", ex);
					commit = false;
					PristineDBUtil.rollbackTransaction(getOracleConnection(), "Error updating user attr");
				} finally {
					PristineDBUtil.close(getOracleConnection());
				}

				if (processZipFile) {
					PrestoUtil.deleteFiles(fileList);
					fileList.clear();
					fileList.add(zipFileList.get(curZipFileCount));
				}
				String archivePath = getRootPath() + "/" + relativeInputPath + "/";

				if (commit) {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());

		} catch (GeneralException ex) {
			logger.error("Outer Exception - JavaException", ex);
			ex.printStackTrace();
		} catch (Exception ex) {
			logger.error("Outer Exception - JavaException", ex);
			ex.printStackTrace();
		}
	}

	/**
	 * To write retail cost file from the actual list..
	 * @throws GeneralException 
	 */
	private void writeRetailCostFile() throws GeneralException {
		writeHeader("COST");
		Set<String> uniqueUPCNum = new HashSet<>();
		for (HistoryDataFormatterDTO historyDataFormatterDTO : _itemList) {
			
			String key = historyDataFormatterDTO.getUPC() +
					historyDataFormatterDTO.getZN_NO() +
					historyDataFormatterDTO.getSPLR_NO() +
					historyDataFormatterDTO.getPRC_GRP_CD() +
					historyDataFormatterDTO.getSTR_BNR_CD(); 
			toChangeDateFormat(historyDataFormatterDTO);
			
				if (!(historyDataFormatterDTO.getPRC_TYP_IND().equals(priceTypeInd))) {
					if(!uniqueUPCNum.contains(key)){
						uniqueUPCNum.add(key);
						pw.print(historyDataFormatterDTO.getUPC()); // UPC
						pw.print("|");
						pw.print(historyDataFormatterDTO.getWHITEM_NO()); // WHITEM_NO
						pw.print("|");
						pw.print(historyDataFormatterDTO.getSTRT_DTE()); // STRT_DTE
						pw.print("|");
						pw.print(historyDataFormatterDTO.getZN_NO()); // CST_ZONE_NO
						pw.print("|");
						String splr = historyDataFormatterDTO.getSPLR_NO().equals("0") ? "" : historyDataFormatterDTO.getSPLR_NO();
						pw.print(historyDataFormatterDTO.getSPLR_NO().equals("0") ? "" : historyDataFormatterDTO.getSPLR_NO()); // SPLR_NO
						pw.print("|");
						pw.print(historyDataFormatterDTO.getCST_STAT_CD()); // CST_STAT_CD
						pw.print("|");
						pw.print(historyDataFormatterDTO.getBS_CST()); // BS_CST_AKA_STORE_CST
						pw.print("|");
						if(!historyDataFormatterDTO.getDLVD_CST_AKA_WHSE_CST().equals("0") && 
								historyDataFormatterDTO.getDLVD_CST_AKA_WHSE_CST() !=""){
							pw.print(historyDataFormatterDTO.getDLVD_CST_AKA_WHSE_CST()); // DLVD_CST_AKA_WHSE_CST
//							logger.info("Ware house cost is not zero"+historyDataFormatterDTO.getDLVD_CST_AKA_WHSE_CST());
						}else{
							pw.print(historyDataFormatterDTO.getBS_CST()); // DLVD_CST_AKA_WHSE_CST
						}
						pw.print("|");
						pw.print(historyDataFormatterDTO.getLONG_TERM_REFLECT_FG()); // LONG_TERM_REFLECT_FG
						pw.print("|");
						pw.print(historyDataFormatterDTO.getSTR_BNR_CD()); // BNR_CD
						pw.println("");
				}
			}
		}
	}

	/**
	 * To write Retail price values
	 * @throws GeneralException 
	 */
	private void writeRetailPriceFile() throws GeneralException {
		writeHeader("PRICE");
		Set<String> uniqueRetailerItemCodes = new HashSet<>();
		for (HistoryDataFormatterDTO historyDataFormatterDTO : _itemList) {

			String key = historyDataFormatterDTO.getRITEM_NO() +
					historyDataFormatterDTO.getZN_NO() +
					historyDataFormatterDTO.getSPLR_NO() +
					historyDataFormatterDTO.getPRC_GRP_CD() +
					historyDataFormatterDTO.getSTR_BNR_CD() +
					historyDataFormatterDTO.getPRC_TYP_IND();
			
			if(!uniqueRetailerItemCodes.contains(key)){
				uniqueRetailerItemCodes.add(key);
				pw.print(historyDataFormatterDTO.getRITEM_NO()); // RITEM_NO
				pw.print("|");
				pw.print(historyDataFormatterDTO.getSTRT_DTE()); // PRC_STRT_DTE
				pw.print("|");
				pw.print(historyDataFormatterDTO.getEND_DTE()); // PRC_END_DTE
				pw.print("|");
				pw.print(historyDataFormatterDTO.getCURR_PRC()); // CURR_PRC
				pw.print("|");
				pw.print(historyDataFormatterDTO.getMUNIT_CNT()); // MUNIT_CNT
				pw.print("|");
				pw.print(historyDataFormatterDTO.getPRC_STAT_CD()); // PRC_STAT_CD
				pw.print("|");
				pw.print(historyDataFormatterDTO.getPROM_CD()); // PROM_CD
				pw.print("|");
				pw.print(historyDataFormatterDTO.getPROM_PCT()); // PROM_PCT
				pw.print("|");
				pw.print(historyDataFormatterDTO.getPROM_AMT_OFF()); // PROM_AMT_OFF
				pw.print("|");
				pw.print(historyDataFormatterDTO.getZN_NO()); // ZN_NO
				pw.print("|");
				pw.print(historyDataFormatterDTO.getPRC_GRP_CD()); // PRC_GRP_CD
				pw.print("|");
				pw.print(historyDataFormatterDTO.getSPLR_NO().equals("0") ? "" : historyDataFormatterDTO.getSPLR_NO()); // SPLR_NO
				pw.print("|");
				pw.print(historyDataFormatterDTO.getPRC_TYP_IND()); // PRC_TYP_IND
				pw.print("|");
				pw.print(historyDataFormatterDTO.getDEAL_ID()); // DEAL_ID
				pw.print("|");
				pw.print(historyDataFormatterDTO.getOFFER_ID()); // OFFER_ID
				pw.print("|");
				pw.print(historyDataFormatterDTO.getOFFER_DSCR()); // OFFER_DSCR
				pw.print("|");
				pw.print(historyDataFormatterDTO.getAD_TYP_DSCR()); // AD_TYP_DSCR
				pw.print("|");
				pw.print(historyDataFormatterDTO.getAD_LOCN_DSCR()); // AD_LOCN_DSCR
				pw.print("|");
				pw.print(historyDataFormatterDTO.getPCT_OF_PGE()); // PCT_OF_PGE
				pw.print("|");
				pw.print(historyDataFormatterDTO.getSTR_BNR_CD()); // BNR_CD
				pw.println("");
			}
		}
	}

	private void writeAllowanceFile() throws GeneralException {
		writeHeader("ALLOWANCE");
		Set<String> uniqueUPCNum = new HashSet<>();
		for (HistoryDataFormatterDTO historyDataFormatterDTO : _itemList) {
			
			String key = historyDataFormatterDTO.getUPC() +
					historyDataFormatterDTO.getZN_NO() +
					historyDataFormatterDTO.getSPLR_NO() +
					historyDataFormatterDTO.getPRC_GRP_CD() +
					historyDataFormatterDTO.getSTR_BNR_CD(); 
			
				if (!(historyDataFormatterDTO.getPRC_TYP_IND().equals(priceTypeInd))
						&& historyDataFormatterDTO.getALLW_AMT() != null && historyDataFormatterDTO.getALLW_AMT() != "") {
					if(!uniqueUPCNum.contains(key)){
						uniqueUPCNum.add(key);
						pw.print(historyDataFormatterDTO.getUPC()); // UPC
						pw.print("|");
						pw.print(historyDataFormatterDTO.getWHITEM_NO()); // WHITEM_NO
						pw.print("|");
						pw.print(historyDataFormatterDTO.getSTRT_DTE()); // STRT_DTE
						pw.print("|");
						pw.print(historyDataFormatterDTO.getEND_DTE()); // PRC_END_DTE
						pw.print("|");
						pw.print(historyDataFormatterDTO.getZN_NO()); // CST_ZONE_NO
						pw.print("|");
						pw.print(historyDataFormatterDTO.getSPLR_NO().equals("0") ? "" : historyDataFormatterDTO.getSPLR_NO()); // SPLR_NO
						pw.print("|");
						pw.print(historyDataFormatterDTO.getCST_STAT_CD()); // ALLW_STAT_CD
						pw.print("|");
						pw.print(historyDataFormatterDTO.getALLW_AMT()); // ALLW_AMT
						pw.print("|");
						pw.print(""); // DEAL_CST
						pw.print("|");
						pw.print(historyDataFormatterDTO.getLONG_TERM_REFLECT_FG()); // LONG_TERM_REFLECT_FG
						pw.print("|");
						pw.print(historyDataFormatterDTO.getDEAL_ID()); // DEAL_ID
						pw.print("|");
						pw.print(historyDataFormatterDTO.getSTR_BNR_CD()); // BNR_CD
						pw.println("");
				}
			}
		}
	}

	private void writeScanBackFile() throws GeneralException {
		writeHeader("SCANBACK");
		Set<String> uniqueUPCNum = new HashSet<>();
		for (HistoryDataFormatterDTO historyDataFormatterDTO : _itemList) {
			
			String key = historyDataFormatterDTO.getUPC() +
					historyDataFormatterDTO.getZN_NO() +
					historyDataFormatterDTO.getSPLR_NO() +
					historyDataFormatterDTO.getPRC_GRP_CD() +
					historyDataFormatterDTO.getSTR_BNR_CD(); 
			toChangeDateFormat(historyDataFormatterDTO);
				if (!(historyDataFormatterDTO.getPRC_TYP_IND().equals(priceTypeInd)) && !(historyDataFormatterDTO.getSCNBCK_AMT().equals("0"))) {
					if(!uniqueUPCNum.contains(key)){
						uniqueUPCNum.add(key);
						pw.print(historyDataFormatterDTO.getRITEM_NO()); // RITEM_NO
						pw.print("|");
						pw.print(historyDataFormatterDTO.getZN_NO()); // zoneNumber
						pw.print("|");
						pw.print(historyDataFormatterDTO.getSPLR_NO().equals("0") ? "" : historyDataFormatterDTO.getSPLR_NO()); // splrNo
						pw.print("|");
						pw.print(historyDataFormatterDTO.getSTR_BNR_CD()); // bnrCD
						pw.print("|");
						pw.print(historyDataFormatterDTO.getSCNBCK_AMT()); // scanBackAmt1
						pw.print("|");
						pw.print(historyDataFormatterDTO.getSTRT_DTE()); // scanBackStartDate1
						pw.print("|");
						pw.print(historyDataFormatterDTO.getEND_DTE()); // scanBackEndDate1
						pw.print("|");
						pw.print(""); // scanBackAmt2
						pw.print("|");
						pw.print(""); // scanBackStartDate2
						pw.print("|");
						pw.print(""); // scanBackEndDate2
						pw.print("|");
						pw.print(""); // scanBackAmt3
						pw.print("|");
						pw.print(""); // scanBackStartDate3
						pw.print("|");
						pw.print("");// scanBackEndDate3
						pw.println("");
				}
			}
		}
	}

	private void writeHeader(String FileFormat) {
		if (FileFormat.equals("COST")) {
			pw.print("UPC");
			pw.print("|");
			pw.print("WHITEM_NO");
			pw.print("|");
			pw.print("STRT_DTE");
			pw.print("|");
			pw.print("CST_ZONE_NO");
			pw.print("|");
			pw.print("SPLR_NO");
			pw.print("|");
			pw.print("CST_STAT_CD");
			pw.print("|");
			pw.print("BS_CST_AKA_STORE_CST");
			pw.print("|");
			pw.print("DLVD_CST_AKA_WHSE_CST");
			pw.print("|");
			pw.print("LONG_TERM_REFLECT_FG");
			pw.print("|");
			pw.print("BNR_CD");
			pw.println(""); // spaces
		} else if (FileFormat.equals("PRICE")) {
			pw.print("RITEM_NO");
			pw.print("|");
			pw.print("PRC_STRT_DTE");
			pw.print("|");
			pw.print("PRC_END_DTE");
			pw.print("|");
			pw.print("CURR_PRC");
			pw.print("|");
			pw.print("MUNIT_CNT");
			pw.print("|");
			pw.print("PRC_STAT_CD");
			pw.print("|");
			pw.print("PROM_CD");
			pw.print("|");
			pw.print("PROM_PCT");
			pw.print("|");
			pw.print("PROM_AMT_OFF");
			pw.print("|");
			pw.print("ZN_NO");
			pw.print("|");
			pw.print("PRC_GRP_CD");
			pw.print("|");
			pw.print("SPLR_NO");
			pw.print("|");
			pw.print("PRC_TYP_IND");
			pw.print("|");
			pw.print("DEAL_ID");
			pw.print("|");
			pw.print("OFFER_ID");
			pw.print("|");
			pw.print("OFFER_DSCR");
			pw.print("|");
			pw.print("AD_TYP_DSCR");
			pw.print("|");
			pw.print("AD_LOCN_DSCR");
			pw.print("|");
			pw.print("PCT_OF_PGE");
			pw.print("|");
			pw.print("BNR_CD");
			pw.println("");
		} else if (FileFormat.equals("ALLOWANCE")) {
			pw.print("UPC");
			pw.print("|");
			pw.print("WHITEM_NO");
			pw.print("|");
			pw.print("STRT_DTE");
			pw.print("|");
			pw.print("END_DTE");
			pw.print("|");
			pw.print("CST_ZONE_NO");
			pw.print("|");
			pw.print("SPLR_NO");
			pw.print("|");
			pw.print("ALLW_STAT_CD");
			pw.print("|");
			pw.print("ALLW_AMT");
			pw.print("|");
			pw.print("DEAL_CST");
			pw.print("|");
			pw.print("LONG_TERM_REFLECT_FG");
			pw.print("|");
			pw.print("DEAL_ID");
			pw.print("|");
			pw.print("BNR_CD");
			pw.println("");
		} else if (FileFormat.equals("SCANBACK")) {
			pw.print("RITEM_NO");
			pw.print("|");
			pw.print("ZONE_NO");
			pw.print("|");
			pw.print("SPLR_NO");
			pw.print("|");
			pw.print("BNR_CD");
			pw.print("|");
			pw.print("SCNBCK_AMT1");
			pw.print("|");
			pw.print("SCNBCK_STRT_DTE1");
			pw.print("|");
			pw.print("SCNBCK_END_DTE1");
			pw.print("|");
			pw.print("SCNBCK_AMT2");
			pw.print("|");
			pw.print("SCNBCK_STRT_DTE2");
			pw.print("|");
			pw.print("SCNBCK_END_DTE2");
			pw.print("|");
			pw.print("SCNBCK_AMT3");
			pw.print("|");
			pw.print("SCNBCK_STRT_DTE3");
			pw.print("|");
			pw.print("SCNBCK_END_DTE3");
			pw.println("");

		}
	}

	/**
	 * Fill the values in list
	 */
	private void fillAllColumns() {
		allColumns.put(1, "UPC");
		allColumns.put(2, "WHITEM_NO");
		allColumns.put(3, "RITEM_NO");
		allColumns.put(4, "STRT_DTE");
		allColumns.put(5, "END_DTE");
		allColumns.put(6, "ZN_NO");
		allColumns.put(7, "PRC_GRP_CD");
		allColumns.put(8, "SPLR_NO");
		allColumns.put(9, "PRC_STAT_CD");
		allColumns.put(10, "CURR_PRC");
		allColumns.put(11, "MUNIT_CNT");
		allColumns.put(12, "PROM_CD");
		allColumns.put(13, "PROM_PCT");
		allColumns.put(14, "PROM_AMT_OFF");
		allColumns.put(15, "PRC_TYP_IND");
		allColumns.put(16, "SCNBCK_AMT");
		allColumns.put(17, "CST_STAT_CD");
		allColumns.put(18, "BS_CST");
		allColumns.put(19, "ALLW_AMT");
		allColumns.put(20, "DLVD_CST_AKA_WHSE_CST");
		allColumns.put(21, "LONG_TERM_REFLECT_FG");
		allColumns.put(22, "STR_BNR_CD");
		allColumns.put(23, "DEAL_ID");
		allColumns.put(24, "OFFER_ID");
		allColumns.put(25, "OFFER_DSCR");
		allColumns.put(26, "AD_TYP_DSCR");
		allColumns.put(27, "AD_LOCN_DSCR");
		allColumns.put(28, "PCT_OF_PGE");
	}
	
	private void toChangeDateFormat(HistoryDataFormatterDTO historyDataFormatterDTO) throws GeneralException {
		DateFormat inputFormatter = new SimpleDateFormat("yyyyMMdd");
		DateFormat df = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		try {
			Date startDate = inputFormatter.parse(historyDataFormatterDTO.getSTRT_DTE());
			Date endDate = inputFormatter.parse(historyDataFormatterDTO.getEND_DTE());
			historyDataFormatterDTO.setSTRT_DTE(df.format(startDate));
			historyDataFormatterDTO.setEND_DTE(df.format(endDate));
		} catch (ParseException e) {
			throw new GeneralException("Date Parsing exception ", e);
		}
	}
}
