package com.pristine.fileformatter.gianteagle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleAllowanceDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleCostDTO;
import com.pristine.exception.GeneralException;
import com.pristine.feedvalidator.CostFileValidator;
import com.pristine.feedvalidator.service.FeedValidatorService;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

/**
 * 
 * Formats Cost and allowance file at a time and creates output in Pristine's standard file format.
 * 
 * @author Pradeepkumar
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class CostFileFormatterWithValidator extends PristineFileParser {
	private static Logger logger = Logger.getLogger("CostFileFormatterGE");
	private final static String INPUT_COST_FOLDER = "INPUT_COST_FOLDER=";
	private final static String INPUT_ALLOW_FOLDER = "INPUT_ALLOW_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private final static String DIST_ERROR_OUTPUT_FOLDER = "DIST_ERROR_OUTPUT_FOLDER=";
	private final static String FUL_ERROR_OUTPUT_FOLDER = "FUL_ERROR_OUTPUT_FOLDER=";
	private final static String LOAD_FUTURE_WEEKS = "LOAD_FUTURE_WEEKS=";
	private final static String SKIP_ERR_REC_AND_PROCESS ="SKIP_ERR_REC_AND_PROCESS=";
	private final static String PRINT_DIST_ERROR_REC = "PRINT_DIST_ERROR_REC=";
	Connection conn = null;
	static TreeMap<Integer, String> allColumns = null;
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private Set<String> skippedRetailerItemcodes = null;
	List<GiantEagleCostDTO> costList = null;
	List<GiantEagleAllowanceDTO> allowanceList = null;
	public static final int LOG_RECORD_COUNT = 25000;
	private String ALLOWANCE = "Allowance";
	private String COST = "Cost";
	private String CURR_COST = "C";
	private String FUTURE_COST = "F";
	private String NESTED_COST = "N";
	private String _fileType = null;
	String _inFolderForCostFile = null;
	String _inFolderForAllowFile = null;
	String _outFolder = null, errOutputFolder = null;
	int calendarId = -1;
	int prevCalendarId = -1;
	static String dateStr = null;
	String startDate = null;
	String prevWkStartDate = null;
	public static HashMap<String, RetailCalendarDTO> calendarDetails = new HashMap<>();
	HashMap<String, List<ItemDTO>> upcMap = new HashMap<>();
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
	List<String> allwItemNotFoundInCost = new ArrayList<>();
	private static boolean loadFutureWeekCost = true, ignoreErrRecAndProcess = false, printDistErrRec=true;
	HashMap<String, RetailCalendarDTO> everydayAndRespWeekCalId = new HashMap<>();
	
	
	public CostFileFormatterWithValidator() {
		super();
		super.headerPresent = true;
		conn = getOracleConnection();
	}

	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-ge-cost-formatter.properties");
		CostFileFormatterWithValidator priceFileFormatter = new CostFileFormatterWithValidator();
		for (String arg : args) {
			if (arg.startsWith(INPUT_COST_FOLDER)) {
				priceFileFormatter._inFolderForCostFile = arg.substring(INPUT_COST_FOLDER.length());
			} else if (arg.startsWith(INPUT_ALLOW_FOLDER)) {
				priceFileFormatter._inFolderForAllowFile = arg.substring(INPUT_ALLOW_FOLDER.length());
			} else if (arg.startsWith(OUTPUT_FOLDER)) {
				priceFileFormatter._outFolder = arg.substring(OUTPUT_FOLDER.length());
			} else if (arg.startsWith(LOAD_FUTURE_WEEKS)) {
				loadFutureWeekCost = Boolean.parseBoolean(arg.substring(LOAD_FUTURE_WEEKS.length()));
			} else if (arg.startsWith(SKIP_ERR_REC_AND_PROCESS)) {
				ignoreErrRecAndProcess = Boolean.parseBoolean(arg.substring(SKIP_ERR_REC_AND_PROCESS.length()));
			} else if (arg.startsWith(DIST_ERROR_OUTPUT_FOLDER)) {
				priceFileFormatter.errOutputFolder = arg.substring(DIST_ERROR_OUTPUT_FOLDER.length());
			} else if (arg.startsWith(PRINT_DIST_ERROR_REC)) {
				printDistErrRec = Boolean.parseBoolean(arg.substring(PRINT_DIST_ERROR_REC.length()));
			} else if (arg.startsWith(FUL_ERROR_OUTPUT_FOLDER)) {
				priceFileFormatter.errOutputFolder = arg.substring(FUL_ERROR_OUTPUT_FOLDER.length());
			}
		}
		// Default week type to current week if it is not specified

		Calendar c = Calendar.getInstance();
		int dayIndex = 0;
		if (PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null) {
			dayIndex = Integer.parseInt(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if (dayIndex > 0)
			c.set(Calendar.DAY_OF_WEEK, dayIndex + 1);
		else
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		if (Constants.CURRENT_WEEK.equalsIgnoreCase(args[1])) {
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.NEXT_WEEK.equalsIgnoreCase(args[1])) {
			c.add(Calendar.DATE, 7);
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.LAST_WEEK.equalsIgnoreCase(args[1])) {
			c.add(Calendar.DATE, -7);
			dateStr = dateFormat.format(c.getTime());
		} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[1])) {
			try {
				dateStr = DateUtil.getWeekStartDate(DateUtil.toDate(args[2]), 0);
			} catch (GeneralException exception) {
				logger.error("Error when parsing date - " + exception.toString());
				System.exit(-1);
			}
		}

		logger.info("********************************************");
		priceFileFormatter.processCostFile();
		logger.info("********************************************");
	}

	/**
	 * 
	 * @param inFolderForCostFile
	 */
	private void processCostFile() {
		try {

			initializeCache();
			populateCalendarId(dateStr);
			parseFiles(_inFolderForCostFile, _outFolder, COST, null);

			logSkippedRecords();

		} catch (IOException | GeneralException e) {
			logger.error("Error -- processCostFile() ", e);
		}
	}

	/**
	 * Gets all items from Item lookup and creates Map with UPC as key
	 * 
	 * @throws GeneralException
	 */
	private void initializeCache() throws GeneralException {
		skippedRetailerItemcodes = new HashSet<>();
		costList = new ArrayList<>();
		allowanceList = new ArrayList<>();
		allColumns = new TreeMap<>();
		ItemDAO itemDAO = new ItemDAO();
		logger.info("initializeCache() - Getting all items started...");
		List<ItemDTO> activeItems = itemDAO.getAllActiveItems(conn);
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		everydayAndRespWeekCalId = retailCalendarDAO.getEverydayAndItsWeekCalendarId(conn);
		logger.info("initializeCache() - Getting all items is completed...");
		for (ItemDTO itemDTO : activeItems) {
			if (upcMap.get(itemDTO.getUpc()) == null) {
				List<ItemDTO> items = new ArrayList<>();
				items.add(itemDTO);
				upcMap.put(itemDTO.getUpc(), items);
			} else {
				List<ItemDTO> items = upcMap.get(itemDTO.getUpc());
				items.add(itemDTO);
				upcMap.put(itemDTO.getUpc(), items);
			}
		}
	}

	/**
	 * Parses given file formats to Pristine standard cost file format
	 * 
	 * @param inFolder
	 * @param outFolder
	 * @throws GeneralException
	 * @throws IOException
	 */
	private void parseFiles(String inFolder, String outFolder, String fileType, String costFileName) throws GeneralException, IOException {

		try {
			fillAllColumns(fileType);
			String fieldNames[] = new String[allColumns.size()];
			int k = 0;
			for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
				fieldNames[k] = columns.getValue();
				k++;
			}

			// getzip files
			ArrayList<String> zipFileList = getZipFiles(inFolder);

			// Start with -1 so that if any regular files are present, they are
			// processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;

			String zipFilePath = getRootPath() + "/" + inFolder;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;
				try {
					if (processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

					fileList = getFiles(inFolder);
					for (int i = 0; i < fileList.size(); i++) {
						long fileProcessStartTime = System.currentTimeMillis();
						String fileNameFull = fileList.get(i);

						logger.info("parseFiles() - File Name - " + fileNameFull);
						int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
						String outputFileName[] = fileNameFull.split("/");
						String fileName = outputFileName[outputFileName.length - 1];
						logger.info("parseFiles() - Output File name - " + fileName);
						String outputPath = getRootPath() + "/" + outFolder + "/" + outputFileName[outputFileName.length - 1];

						if (fileNameFull.contains(fileType) && fileType.equals(COST)) {
							// Set _fileType so that processRecords identifies which list to be filled.
							this._fileType = COST;

							logger.info("parseFiles() - Processing Cost File ...");
							super.parseDelimitedFile(GiantEagleCostDTO.class, fileList.get(i), '|', fieldNames, stopCount);

							// Feed validating process
							
							
							
							CostFileValidator costFileValidator = new CostFileValidator(upcMap, everydayAndRespWeekCalId);
							logger.info("Checking each warehouse item has only one cost zone..");
							boolean itemsHasOnlyOneZone = costFileValidator.isWHSEItemHasOnlyOneZoneAtBnrLevel(costList);

							if(!itemsHasOnlyOneZone){
								String mailSubject ="RE: More than one cost zone found for an item in cost feed";
								String mailBody ="This is an automatic Email received. There are more than cost zone for an item in given feed. ";
								FeedValidatorService feedValidatorService = new FeedValidatorService();
								feedValidatorService.sendEMail(null, mailSubject, mailBody);
								logger.error("Cost program is stopped from proceeding further.");
								System.exit(0);
							}
							logger.info("Validating cost zone is completed.");
							
							logger.info("Feed validation started..");
							logger.info("Total number of actual records passing to validate: "+costList.size());
							List<GiantEagleCostDTO> costErrRecList = costFileValidator.getErrorRecordsAlone(costList);
							if(costErrRecList!= null && costErrRecList.size()>0){
								FeedValidatorService feedValidatorService = new FeedValidatorService();
								feedValidatorService.writeCostErrRecAndSendMail(costErrRecList, errOutputFolder, printDistErrRec, true);
								// To get only valid records
								if(ignoreErrRecAndProcess){
									List<GiantEagleCostDTO> validCostRecList = costFileValidator.getNonErrorRecords(costList);
									costList = new ArrayList<>();
									costList = validCostRecList;
								}else{
									logger.error("Cost feed has invalid data in the input feed. Please check the given file: "+fileName);
									logger.error("Program terminates to stop proceeding further..");
									System.exit(0);
								}
							}
							logger.info("Feed validation completed..");
							logger.info("parseFiles() - # of cost records to be processed -> " + costList.size());

							File file = new File(outputPath);
							if (!file.exists())
								fw = new FileWriter(outputPath);
							else
								fw = new FileWriter(outputPath, true);
							pw = new PrintWriter(fw);

							// Recurse this method to get the allowance file records as part of Cost file formatting
							parseFiles(_inFolderForAllowFile, outFolder, ALLOWANCE, fileName);

							if (allowanceList.size() == 0) {
								logger.warn("parseFiles() - Allowance file is not found.");
							}
							logger.info("parseFiles() - # of cost records to be processed -> " + allowanceList.size());

							HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = processCurrentAndFutureWeeksCost(costList,
									allowanceList, startDate);

							processCachedList(multipleWeekCostDetails);
							// Clear cache
							costList = new ArrayList<>();
							allowanceList = new ArrayList<>();

							pw.flush();
							fw.flush();
							pw.close();
							fw.close();
						} else if (fileNameFull.contains(fileType) && fileType.equals(ALLOWANCE)) {
							boolean processFile = true;

							if (costFileName.contains("_") && fileName.contains("_")) {
								String costFileDateStr = costFileName.split("_")[1];
								String allowFileDateStr = fileName.split("_")[1];
								if (!costFileDateStr.equals(allowFileDateStr)) {
									processFile = false;
								}
							}

							if (processFile) {
								this._fileType = ALLOWANCE;
								logger.info("Processing Allowance File ...");
								super.parseDelimitedFile(GiantEagleAllowanceDTO.class, fileList.get(i), '|', fieldNames, stopCount);
								
							}
						}

						long fileProcessEndTime = System.currentTimeMillis();
						logger.info("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime) + "ms");
					}
				} catch (GeneralException ex) {
					logger.error("GeneralException", ex);
					commit = false;
				} catch (Exception ex) {
					logger.error("JavaException", ex);
					commit = false;
				}

				if (processZipFile) {
					PrestoUtil.deleteFiles(fileList);
					fileList.clear();
					fileList.add(zipFileList.get(curZipFileCount));
				}
				String archivePath = getRootPath() + "/" + inFolder + "/";

				if (commit) {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());

		} catch (GeneralException | Exception ex) {
			throw new GeneralException("Outer Exception - JavaException", ex);
		}
	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		// Fill cache based on the processing file type.
		if (_fileType.equals(COST)) {
			costList.addAll((List<GiantEagleCostDTO>) listobj);
		} else if (_fileType.equals(ALLOWANCE)) {
			allowanceList.addAll((List<GiantEagleAllowanceDTO>) listobj);
		}
	}

	/**
	 * Processes all the cached rows and generates formatted file
	 * 
	 * @throws GeneralException
	 */
	private void processCachedList(HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails) throws GeneralException {
		int recordCount = 0;
		for (Map.Entry<String, HashMap<String, GiantEagleCostDTO>> multiWeekCostDetail : multipleWeekCostDetails.entrySet()) {

			if (multiWeekCostDetail.getKey().equals(startDate) || (!multiWeekCostDetail.getKey().equals(startDate) && loadFutureWeekCost)) {

				for (GiantEagleCostDTO giantEagleCostDTO : multiWeekCostDetail.getValue().values()) {
					recordCount++;
					giantEagleCostDTO.setUPC(PrestoUtil.castUPC(giantEagleCostDTO.getUPC(), false));
					// Get list of items for given upc
					if (upcMap.get(giantEagleCostDTO.getUPC()) == null) {
						skippedRetailerItemcodes.add(giantEagleCostDTO.getUPC());
					} else {
						// Get list of items for given upc and write records for each item
						List<ItemDTO> itemList = upcMap.get(giantEagleCostDTO.getUPC());
						for (ItemDTO itemDTO : itemList) {
							if (itemDTO.getRetailerItemCode() != null && !itemDTO.getRetailerItemCode().isEmpty()) {
								// Added Banner code along with the levelId to differentiate between GE and MI
								String levelId = giantEagleCostDTO.getBNR_CD() + "-" + giantEagleCostDTO.getCST_ZONE_NO() + "-"
										+ itemDTO.getPrcGrpCd();
								if ((!Constants.EMPTY.equals(giantEagleCostDTO.getSPLR_NO().trim()) && giantEagleCostDTO.getSPLR_NO() != null)
										&& (giantEagleCostDTO.getWHITEM_NO() == null || Constants.EMPTY.equals(giantEagleCostDTO.getWHITEM_NO()))) {
									levelId = levelId + "-" + giantEagleCostDTO.getSPLR_NO();
								}
								pw.print(giantEagleCostDTO.getUPC()); // UPC
								pw.print("|");
								pw.print(itemDTO.getRetailerItemCode()); // item code
								pw.print("|");
								pw.print("1"); // Zone#/Store#
								pw.print("|");
								pw.print(levelId); // Level
								pw.print("|");
								pw.print(giantEagleCostDTO.getSTRT_DTE()); // Effective cost date
								pw.print("|");
								pw.print(giantEagleCostDTO.getDLVD_CST_AKA_WHSE_CST()); // List cost
								pw.print("|");
								pw.print(giantEagleCostDTO.getDealStartDate() == null ? Constants.EMPTY : giantEagleCostDTO.getDealStartDate());// deal
																																				// cost
																																				// start
																																				// date
								pw.print("|");
								pw.print(giantEagleCostDTO.getDealEndDate() == null ? Constants.EMPTY : giantEagleCostDTO.getDealEndDate());// deal
																																			// cost
																																			// end
																																			// date
								pw.print("|");
								pw.print(giantEagleCostDTO.getDealCost() > 0 ? round(giantEagleCostDTO.getDealCost(), 4) : Constants.EMPTY);// deal
																																			// cost
								pw.print("|");
								pw.print(giantEagleCostDTO.getALLW_AMT() > 0 ? round(giantEagleCostDTO.getALLW_AMT(), 4) : Constants.EMPTY);// Allow
																																			// Amt
								pw.print("|");
								pw.print(giantEagleCostDTO.getLONG_TERM_REFLECT_FG());// Long term flag
								pw.print("|");
								pw.print(itemDTO.getPrcGrpCd());// Prc Grp code

								pw.println("");
							}
						}
						if (recordCount % LOG_RECORD_COUNT == 0) {
							logger.info("processCachedList() Processed - " + recordCount);
						}
					}
				}
			}

		}

	}

	/**
	 * Processes allowance file records and cost file records and appends the deal cost info
	 * 
	 * @return list of records with deal cost added.
	 * @throws GeneralException
	 */
	private List<GiantEagleCostDTO> getAllowanceDataAndFillDealCost() throws GeneralException {
		HashMap<String, List<GiantEagleCostDTO>> listCostMap = new HashMap<>();
		HashMap<String, List<GiantEagleAllowanceDTO>> dealCostMap = new HashMap<>();
		List<GiantEagleCostDTO> outList = new ArrayList<>();
		Set<String> costUniqueRecords = new HashSet<>();
		Set<String> allowanceUniqueRecords = new HashSet<>();
		// Create map for cost and allowance data with key as UPC, Zone number and SPLR Number (Vendor number)
		for (GiantEagleCostDTO giantEagleCostDTO : costList) {
			String key = giantEagleCostDTO.getBNR_CD() + "-" + giantEagleCostDTO.getUPC() + "-" + giantEagleCostDTO.getCST_ZONE_NO();
			if ((!Constants.EMPTY.equals(giantEagleCostDTO.getSPLR_NO().trim()) && giantEagleCostDTO.getSPLR_NO() != null)
					&& (giantEagleCostDTO.getWHITEM_NO() == null || Constants.EMPTY.equals(giantEagleCostDTO.getWHITEM_NO()))) {
				key = key + "-" + giantEagleCostDTO.getSPLR_NO();
				giantEagleCostDTO.setDLVD_CST_AKA_WHSE_CST(giantEagleCostDTO.getBS_CST_AKA_STORE_CST());
			}

			String uniqueKey = key + giantEagleCostDTO.getSTRT_DTE();
			if (!costUniqueRecords.contains(uniqueKey)) {
				costUniqueRecords.add(uniqueKey);
				if (listCostMap.get(key) == null) {
					List<GiantEagleCostDTO> items = new ArrayList<>();
					items.add(giantEagleCostDTO);
					listCostMap.put(key, items);
				} else {
					List<GiantEagleCostDTO> items = listCostMap.get(key);
					items.add(giantEagleCostDTO);
					listCostMap.put(key, items);
				}
			}
		}
		// Create map for cost and allowance data with key as UPC, Zone number and SPLR Number (Vendor number)
		for (GiantEagleAllowanceDTO giantEagleAllowanceDTO : allowanceList) {
			if (giantEagleAllowanceDTO.getALLW_AMT() > 0 || giantEagleAllowanceDTO.getDEAL_CST() > 0) {
				String key = giantEagleAllowanceDTO.getBNR_CD() + "-" + giantEagleAllowanceDTO.getUPC() + "-"
						+ giantEagleAllowanceDTO.getCST_ZONE_NO();
				if ((!Constants.EMPTY.equals(giantEagleAllowanceDTO.getSPLR_NO().trim()) && giantEagleAllowanceDTO.getSPLR_NO() != null)
						&& (giantEagleAllowanceDTO.getWHITEM_NO() == null || Constants.EMPTY.equals(giantEagleAllowanceDTO.getWHITEM_NO()))) {
					key = key + "-" + giantEagleAllowanceDTO.getSPLR_NO();
				}

				String uniqueKey = key + giantEagleAllowanceDTO.getSTRT_DTE() + giantEagleAllowanceDTO.getEND_DTE();
				if (!allowanceUniqueRecords.contains(uniqueKey)) {
					allowanceUniqueRecords.add(uniqueKey);
					if (dealCostMap.get(key) == null) {
						List<GiantEagleAllowanceDTO> items = new ArrayList<>();
						items.add(giantEagleAllowanceDTO);
						dealCostMap.put(key, items);
					} else {
						List<GiantEagleAllowanceDTO> items = dealCostMap.get(key);
						items.add(giantEagleAllowanceDTO);
						dealCostMap.put(key, items);
					}
				}
			}

		}

		// Loop cost map and identifiy allowance data.
		for (Map.Entry<String, List<GiantEagleCostDTO>> listCostEntry : listCostMap.entrySet()) {
			List<GiantEagleAllowanceDTO> dealCostList = dealCostMap.get(listCostEntry.getKey());
			for (GiantEagleCostDTO giantEagleCostDTO : listCostEntry.getValue()) {
				if (dealCostList != null) {
					// If allowance/ deal cost data is found, then calculate the deal cost and append it to list cost records
					for (GiantEagleAllowanceDTO giantEagleAllowanceDTO : dealCostList) {
						// Check if both allowance and list cost entries are flagged as CURRENT ('C'), If so calculate deal cost
						if (giantEagleAllowanceDTO.getALLW_STAT_CD().equals(CURR_COST) && giantEagleCostDTO.getCST_STAT_CD().equals(CURR_COST)) {
							calculateDealCost(giantEagleCostDTO, giantEagleAllowanceDTO);
						}
						// Check if both allowance and list cost entries are flagged as FUTURE ('F')
						else if (giantEagleAllowanceDTO.getALLW_STAT_CD().equals(FUTURE_COST)
								&& giantEagleCostDTO.getCST_STAT_CD().equals(FUTURE_COST)) {

							// If both allowance and list cost entries are flagged as FUTURE,
							// Check list effective date falls or starts or ends between or with deal start date or end date.
							// If so, calculate the deal cost and append
							Date listEffDate = DateUtil.toDate(giantEagleCostDTO.getSTRT_DTE());
							Date dealStartDate = DateUtil.toDate(giantEagleAllowanceDTO.getSTRT_DTE());
							Date dealEndDate = DateUtil.toDate(giantEagleAllowanceDTO.getEND_DTE());
							if ((listEffDate.equals(dealStartDate) || listEffDate.after(dealStartDate))
									&& (listEffDate.equals(dealEndDate) || listEffDate.before(dealEndDate))) {
								calculateDealCost(giantEagleCostDTO, giantEagleAllowanceDTO);
							} else {
								// If there is no list cost for deal cost record
								logger.warn("getAllowanceDataAndFillDealCost() - Invalid deal cost date range for list cost -> "
										+ " for record key -> " + listCostEntry.getKey());
							}
						}
						/*
						 * else { //Any other flags will be skipped.Other than F or C logger.warn(
						 * "getAllowanceDataAndFillDealCost() - Unable to check record with allowance status code -> " +
						 * giantEagleAllowanceDTO.getALLW_STAT_CD() + " for record key -> " + listCostEntry.getKey()); }
						 */
					}
				}
				outList.add(giantEagleCostDTO);
			}
		}

		return outList;
	}

	/**
	 * calculates deal cost from allowance records
	 * 
	 * @param giantEagleCostDTO
	 * @param giantEagleAllowanceDTO
	 */
	private void calculateDealCost(GiantEagleCostDTO giantEagleCostDTO, GiantEagleAllowanceDTO giantEagleAllowanceDTO) {

		double listCost = giantEagleCostDTO.getDLVD_CST_AKA_WHSE_CST();
		if (giantEagleAllowanceDTO.getDEAL_CST() > 0) {
			double allwAmount = listCost - giantEagleAllowanceDTO.getDEAL_CST();
			giantEagleCostDTO.setDealCost(giantEagleAllowanceDTO.getDEAL_CST());
			giantEagleCostDTO.setALLW_AMT(allwAmount);
		} else if (giantEagleAllowanceDTO.getALLW_AMT() > 0) {
			double dealCost = listCost - giantEagleAllowanceDTO.getALLW_AMT();
			giantEagleCostDTO.setDealCost(dealCost);
			giantEagleCostDTO.setALLW_AMT(giantEagleAllowanceDTO.getALLW_AMT());
		}
		giantEagleCostDTO.setDealStartDate(giantEagleAllowanceDTO.getSTRT_DTE());
		giantEagleCostDTO.setDealEndDate(giantEagleAllowanceDTO.getEND_DTE());
	}

	/**
	 * Logs skipped records
	 */
	private void logSkippedRecords() {
		if (skippedRetailerItemcodes.size() > 0) {
			StringBuilder sb = new StringBuilder();
			for (String itemCode : skippedRetailerItemcodes) {
				sb.append(itemCode + ", ");
			}
			logger.warn("Skipped retailer item codes -> " + sb.toString());
		}
	}

	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns(String fileType) {
		if (fileType.equals(COST)) {
			allColumns.put(1, "UPC");
			allColumns.put(2, "WHITEM_NO");
			allColumns.put(3, "STRT_DTE");
			allColumns.put(4, "CST_ZONE_NO");
			allColumns.put(5, "SPLR_NO");
			allColumns.put(6, "CST_STAT_CD");
			allColumns.put(7, "BS_CST_AKA_STORE_CST");
			allColumns.put(8, "DLVD_CST_AKA_WHSE_CST");
			allColumns.put(9, "LONG_TERM_REFLECT_FG");
			allColumns.put(10, "BNR_CD");
		} else if (fileType.equals(ALLOWANCE)) {
			allColumns.put(1, "UPC");
			allColumns.put(2, "WHITEM_NO");
			allColumns.put(3, "STRT_DTE");
			allColumns.put(4, "END_DTE");
			allColumns.put(5, "CST_ZONE_NO");
			allColumns.put(6, "SPLR_NO");
			allColumns.put(7, "ALLW_STAT_CD");
			allColumns.put(8, "ALLW_AMT");
			allColumns.put(9, "DEAL_CST");
			allColumns.put(10, "LONG_TERM_REFLECT_FG");
			allColumns.put(11, "DEAL_ID");
			allColumns.put(12, "BNR_CD");
		}
	}

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}

	/**
	 * Sets input week's calendar id and its previous week's calendar id
	 * 
	 * @param weekStartDate
	 *            Input Date
	 * @throws GeneralException
	 */
	private void populateCalendarId(String weekStartDate) throws GeneralException {
		conn = getOracleConnection();

		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK);
		logger.info("Calendar Id - " + calendarDTO.getCalendarId());
		calendarId = calendarDTO.getCalendarId();
		startDate = calendarDTO.getStartDate();

		calendarDetails.put(startDate, calendarDTO);

		String prevWeekStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(weekStartDate), 1);
		calendarDTO = retailCalendarDAO.getCalendarId(conn, prevWeekStartDate, Constants.CALENDAR_WEEK);
		prevWkStartDate = calendarDTO.getStartDate();
		logger.info("Previous Week Calendar Id - " + calendarDTO.getCalendarId());
		prevCalendarId = calendarDTO.getCalendarId();
		calendarDetails.put(prevWkStartDate, calendarDTO);
	}

	public HashMap<String, HashMap<String, GiantEagleCostDTO>> processCurrentAndFutureWeeksCost(List<GiantEagleCostDTO> costList,
			List<GiantEagleAllowanceDTO> allowanceList, String startDate) throws GeneralException {

		int noOfFutureWeeksToLoad = Integer.parseInt(PropertyManager.getProperty("NO_OF_FUTURE_WEEKS_TO_LOAD_IN_COST", "13"));
		List<String> futureWeeks = getFutureWeeks(startDate, noOfFutureWeeksToLoad);
		HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails = getCurrentCostDetails(costList, startDate);

		// Include Future Weeks based on Current week Cost details
		splitCurrentWeekIntoFutureWeeks(futureWeeks, multipleWeekCostDetails, startDate);

		populateFutureWeeks(noOfFutureWeeksToLoad, multipleWeekCostDetails, costList, startDate);

		HashMap<String, HashMap<String, GiantEagleAllowanceDTO>> multiWeeksAllowDetails = getMultiWeeksAllowByHandlingDiffTypes(allowanceList,
				startDate);

		calculateDealCostUsingAllow(multipleWeekCostDetails, multiWeeksAllowDetails);

		return multipleWeekCostDetails;
	}

	public List<String> getFutureWeeks(String weekStartDate, int noOfFutureWeeks) {
		List<String> futureWeeks = new ArrayList<String>();
		for (int i = 0; i < noOfFutureWeeks; i++) {
			String futureDate = formatter.format(LocalDate.parse(weekStartDate, formatter).plus(7 * (i + 1), ChronoUnit.DAYS));
			futureWeeks.add(futureDate);
		}
		return futureWeeks;
	}

	public RetailCalendarDTO getWeekStartDateCalDTO(String weekStartDate) {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		RetailCalendarDTO calendarDTO = null;
		if (calendarDetails.get(weekStartDate) != null) {
			calendarDTO = calendarDetails.get(weekStartDate);
		}
		if (calendarDTO == null) {
			try {
				calendarDTO = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK);
				calendarDetails.put(weekStartDate, calendarDTO);
			} catch (GeneralException e) {
				e.printStackTrace();
			}

		}
		return calendarDTO;
	}

	public List<String> getFutureWeeks(String weekStartDate, int noOfFutureWeeks, String costEffWeekStartDate) {
		List<String> futureWeeks = new ArrayList<String>();

		for (int i = 0; i < noOfFutureWeeks; i++) {
			String tempDate = formatter.format(LocalDate.parse(weekStartDate, formatter).plus(7 * (i + 1), ChronoUnit.DAYS));

			long promoDuration = ChronoUnit.DAYS.between(LocalDate.parse(costEffWeekStartDate, formatter), LocalDate.parse(tempDate, formatter));
			if (promoDuration >= 0) {
				futureWeeks.add(tempDate);
			}
		}

		return futureWeeks;
	}

	private HashMap<String, HashMap<String, GiantEagleCostDTO>> getCurrentCostDetails(List<GiantEagleCostDTO> costList, String currentWeekStartDate) {
		HashMap<String, HashMap<String, GiantEagleCostDTO>> currentWeekCostDetails = new HashMap<>();
		HashMap<String, GiantEagleCostDTO> listCostMap = new HashMap<>();
		Set<String> costUniqueRecords = new HashSet<>();
		String currentWeekEndDate = formatter.format(LocalDate.parse(currentWeekStartDate, formatter).plus(6, ChronoUnit.DAYS));

		// Consider only Current cost and populate data
		costList.stream()
				.filter(s -> s.getCST_STAT_CD().equals(CURR_COST) && s.getSTRT_DTE() != null && !s.getSTRT_DTE().isEmpty()
						&& ((s.getSPLR_NO() == null || s.getSPLR_NO().trim().isEmpty())
								|| (s.getSPLR_NO() != null && !s.getSPLR_NO().isEmpty() && !isNEISplr(s.getSPLR_NO()))))
				.forEach(giantEagleCostDTO -> {
					String key = giantEagleCostDTO.getBNR_CD() + "-" + giantEagleCostDTO.getUPC() + "-" + giantEagleCostDTO.getCST_ZONE_NO();
					if (!Constants.EMPTY.equals(giantEagleCostDTO.getSPLR_NO().trim()) && giantEagleCostDTO.getSPLR_NO() != null) {
						key = key + "-" + giantEagleCostDTO.getSPLR_NO();
						giantEagleCostDTO.setDLVD_CST_AKA_WHSE_CST(giantEagleCostDTO.getBS_CST_AKA_STORE_CST());
					}

					// To filter only records which starts on or before processing week
					long promoDuration = ChronoUnit.DAYS.between(LocalDate.parse(currentWeekEndDate, formatter),
							LocalDate.parse(giantEagleCostDTO.getSTRT_DTE(), formatter));

					String uniqueKey = key + giantEagleCostDTO.getSTRT_DTE();
					if (!costUniqueRecords.contains(uniqueKey) && promoDuration <= 0) {
						costUniqueRecords.add(uniqueKey);
						listCostMap.put(key, giantEagleCostDTO);
					}
				});

		currentWeekCostDetails.put(currentWeekStartDate, listCostMap);

		return currentWeekCostDetails;
	}

	/**
	 * If given item is NEI (Non Everyday Item), then skip those SPLR numbers
	 * 
	 * @return
	 */
	private boolean isNEISplr(String splrNo) {
		boolean isNEISplr = false;

		String neiSplrNo = PropertyManager.getProperty("SKIP_NEI_SPLR_NUMBERS", null);

		if (neiSplrNo != null && !neiSplrNo.isEmpty()) {
			String[] neiSplrNOs = neiSplrNo.split(",");

			for (String neiSplr : neiSplrNOs) {
				if (splrNo.trim().equals(neiSplr)) {
					isNEISplr = true;
					break;
				}
			}
		}

		return isNEISplr;
	}

	private void splitCurrentWeekIntoFutureWeeks(List<String> futureWeeks, HashMap<String, HashMap<String, GiantEagleCostDTO>> currentWeekCostDetails,
			String currentWeekStartDate) throws GeneralException {
		if (currentWeekCostDetails.get(currentWeekStartDate) != null) {
			HashMap<String, GiantEagleCostDTO> currentWeekCostDetail = currentWeekCostDetails.get(currentWeekStartDate);

			// Populate cost details for each week using Current week Cost details
			futureWeeks.forEach(futureWeek -> {
				HashMap<String, GiantEagleCostDTO> futureWeekCost = new HashMap<>();

				currentWeekCostDetail.forEach((key, value) -> {
					GiantEagleCostDTO giantEagleCostDTO = null;
					try {
						giantEagleCostDTO = (GiantEagleCostDTO) value.clone();
					} catch (Exception e) {
						e.printStackTrace();
						logger.error("Error in Cloning Current Week Cost details", e);
					}

					giantEagleCostDTO.setSTRT_DTE(futureWeek);
					futureWeekCost.put(key, giantEagleCostDTO);
				});
				currentWeekCostDetails.put(futureWeek, futureWeekCost);
			});
		}
	}

	private void populateFutureWeeks(int noOfFutureWeeksToLoad, HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails,
			List<GiantEagleCostDTO> costList, String currentWeekStartDate) {

		String currentWeekEndDate = formatter.format(LocalDate.parse(currentWeekStartDate, formatter).plus(6, ChronoUnit.DAYS));

		String futureWeekEndDateAfterXWeeks = formatter
				.format(LocalDate.parse(currentWeekStartDate, formatter).plus(6, ChronoUnit.DAYS).plus(noOfFutureWeeksToLoad, ChronoUnit.WEEKS));

		LocalDate curWeekStartDate = LocalDate.parse(currentWeekStartDate, formatter);
		LocalDate curWeekEndDate = LocalDate.parse(currentWeekEndDate, formatter);
		LocalDate futureWeekEndDate = LocalDate.parse(futureWeekEndDateAfterXWeeks, formatter).plus(1, ChronoUnit.DAYS);

		costList.stream()
				.filter(s -> s.getCST_STAT_CD().equals(FUTURE_COST) && s.getSTRT_DTE() != null && !s.getSTRT_DTE().isEmpty()
						&& ((s.getSPLR_NO() == null || s.getSPLR_NO().trim().isEmpty())
								|| (s.getSPLR_NO() != null && !s.getSPLR_NO().isEmpty() && !isNEISplr(s.getSPLR_NO()))))
				.forEach(giantEagleCostDTO -> {
					LocalDate processingDate = LocalDate.parse(giantEagleCostDTO.getSTRT_DTE(), formatter);

					// Check Given future Effective date is after current week and before X weeks end date to populate data
					if (processingDate.isBefore(futureWeekEndDate)) {
						String weekStartDate = null;

						// Get Week Start date based on given Effective date
						RetailCalendarDTO calDTO = null;

						// Consider Future weeks only if it is on or after current week.
						// (If given future weeks are older than current week (processing week) skip those records)
						if (!processingDate.isBefore(curWeekStartDate) && !processingDate.isAfter(curWeekEndDate)) {
							calDTO = getWeekStartDateCalDTO(currentWeekStartDate);
						} else if (processingDate.isAfter(curWeekEndDate)) {
							calDTO = getWeekStartDateCalDTO(giantEagleCostDTO.getSTRT_DTE());
						}
						if (calDTO != null) {
							weekStartDate = calDTO.getStartDate();

							List<String> futureWeeks = getFutureWeeks(currentWeekStartDate, noOfFutureWeeksToLoad, weekStartDate);
							String key = giantEagleCostDTO.getBNR_CD() + "-" + giantEagleCostDTO.getUPC() + "-" + giantEagleCostDTO.getCST_ZONE_NO();
							if ((!Constants.EMPTY.equals(giantEagleCostDTO.getSPLR_NO().trim()) && giantEagleCostDTO.getSPLR_NO() != null)) {
								key = key + "-" + giantEagleCostDTO.getSPLR_NO();
								giantEagleCostDTO.setDLVD_CST_AKA_WHSE_CST(giantEagleCostDTO.getBS_CST_AKA_STORE_CST());
							}

							if (!processingDate.isBefore(curWeekStartDate) && !processingDate.isAfter(curWeekEndDate)) {
								futureWeeks.add(currentWeekStartDate);
							}

							for (String futureWeek : futureWeeks) {

								try {
									GiantEagleCostDTO giantEagleCostDTO1 = (GiantEagleCostDTO) giantEagleCostDTO.clone();
									giantEagleCostDTO1.setSTRT_DTE(futureWeek);
									if (multipleWeekCostDetails.get(futureWeek) != null) {
										HashMap<String, GiantEagleCostDTO> costDetailBasedOnWeek = multipleWeekCostDetails.get(futureWeek);

										// Add Future record in map
										costDetailBasedOnWeek.put(key, giantEagleCostDTO1);
									}
								} catch (Exception e) {
									e.printStackTrace();
									logger.error("Error in populateFutureWeeks() while cloning GiantEagleCostDTOs....");
								}
							}
						}
					}
				});
	}

	private HashMap<String, HashMap<String, GiantEagleAllowanceDTO>> getMultiWeeksAllowByHandlingDiffTypes(List<GiantEagleAllowanceDTO> allowanceList,
			String currentWeekStartDate) {
		HashMap<String, HashMap<String, GiantEagleAllowanceDTO>> multiWeeksAllowDetails = new HashMap<>();

		// Allw_Stat_CD has 3 types N,C,F.
		// Priority will be given to C type comparing to N type and preference will be given to F type than C type

		// First process Nested Type records (Type = N)
		getMultiWeekAllowBasedOnGivenType(multiWeeksAllowDetails, NESTED_COST, allowanceList, currentWeekStartDate);

		// Process Current cost type records (Type = C)
		getMultiWeekAllowBasedOnGivenType(multiWeeksAllowDetails, CURR_COST, allowanceList, currentWeekStartDate);

		// Process future cost type records (Type = F)
		getMultiWeekAllowBasedOnGivenType(multiWeeksAllowDetails, FUTURE_COST, allowanceList, currentWeekStartDate);

		return multiWeeksAllowDetails;
	}

	private void getMultiWeekAllowBasedOnGivenType(HashMap<String, HashMap<String, GiantEagleAllowanceDTO>> multiWeeksAllowDetails, String statusCode,
			List<GiantEagleAllowanceDTO> allowanceList, String currentWeekStartDate) {
		allowanceList.stream().filter(s -> s.getALLW_STAT_CD().equals(statusCode) && (s.getALLW_AMT() > 0 || s.getDEAL_CST() > 0))
				.forEach(allowCost -> {

					String key = allowCost.getBNR_CD() + "-" + allowCost.getUPC() + "-" + allowCost.getCST_ZONE_NO();
					if ((!Constants.EMPTY.equals(allowCost.getSPLR_NO().trim()) && allowCost.getSPLR_NO() != null)) {
						key = key + "-" + allowCost.getSPLR_NO();
					}

					String allowWeekStartDate = getWeekStartDateCalDTO(allowCost.getSTRT_DTE()).getStartDate();
					String allowWeekEndDate = getWeekStartDateCalDTO(allowCost.getEND_DTE()).getEndDate();

					long diffBtwAllowStartAndCurWeek = ChronoUnit.DAYS.between(LocalDate.parse(currentWeekStartDate, formatter),
							LocalDate.parse(allowCost.getSTRT_DTE(), formatter));

					// Get no of weeks between each allowance (By default one week is given)
					List<String> futureWeeks = new ArrayList<>();
					String processFromWeekStart = null;

					if (diffBtwAllowStartAndCurWeek < 0) {
						processFromWeekStart = currentWeekStartDate;
					} else {
						processFromWeekStart = allowWeekStartDate;
					}
					long promoDuration = ChronoUnit.WEEKS.between(LocalDate.parse(processFromWeekStart, formatter),
							LocalDate.parse(allowWeekEndDate, formatter));
					futureWeeks = getFutureWeeks(processFromWeekStart, (int) promoDuration);

					// to consider current week
					futureWeeks.add(processFromWeekStart);

					for (String weekStartDate : futureWeeks) {
						HashMap<String, GiantEagleAllowanceDTO> allowanceDetailBasedOnWeek = new HashMap<>();

						if (multiWeeksAllowDetails.get(weekStartDate) != null) {
							allowanceDetailBasedOnWeek = multiWeeksAllowDetails.get(weekStartDate);
						}
						allowanceDetailBasedOnWeek.put(key, allowCost);
						multiWeeksAllowDetails.put(weekStartDate, allowanceDetailBasedOnWeek);
					}
				});
	}

	private void calculateDealCostUsingAllow(HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails,
			HashMap<String, HashMap<String, GiantEagleAllowanceDTO>> multiWeeksAllowDetails) {

		// Loop Cost details based on each week
		multipleWeekCostDetails.forEach((weekStartDate, costDetailMap) -> {

			if (multiWeeksAllowDetails.get(weekStartDate) != null) {

				HashMap<String, GiantEagleAllowanceDTO> allwDetailsBasedOnWeek = multiWeeksAllowDetails.get(weekStartDate);

				// Loop all the allowance items in a given week and apply deal cost
				allwDetailsBasedOnWeek.forEach((key, allowDetail) -> {
					if (costDetailMap.get(key) != null) {
						GiantEagleCostDTO listCostDetail = costDetailMap.get(key);
						calculateDealCost(listCostDetail, allowDetail);
					} else {
						allwItemNotFoundInCost.add(allowDetail.getUPC());
					}
				});
			}
		});

	}
}
