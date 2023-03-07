package com.pristine.dataload.riteaid;

/**
 * 
 * @Author : Pradeep
 * @Class : TransactionLogLoader
 * ---------------------------------------
 * Compatible for RiteAid..
 * ---------------------------------------
 * Input : Formatted file path
 * File formatter for RiteAid : com.pristine.fileformatter.riteaid.MovementFileFormatter
 *
 *  
 */

import com.csvreader.CsvReader;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.customer.CustomerDAO;
import com.pristine.dao.logger.TransactionLogTrackerDAO;
import com.pristine.dataload.ShopRiteDataProcessor;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.TransactionLogDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TransactionLogLoader extends PristineFileParser {
	static Logger logger = Logger.getLogger("MovementDataLoad");
	Connection _Conn = null;
	CustomerDAO _custDao = null;
	private int _StopAfter = -1;
	private Set<String> _UnprocessedStores = new HashSet<String>();
	private HashMap<String, Integer> _itemHashMap = new HashMap<String, Integer>();
	private HashMap<String, Integer> _upcMissinghMap = new HashMap<String, Integer>();
	private HashMap<String, String> _calendarMap = new HashMap();
	private ShopRiteDataProcessor dataProcessor = new ShopRiteDataProcessor();
	private int _processCalendarId = -1;
	private int _calIdType = 1;
	private HashMap<String, Integer> _storeMap = new HashMap();
	private HashMap<String, Integer> _custMap;
	private int _storeFromConfig = 0;
	private int _unclassifiedSegment = 0;
	private int _unclassifiedSubCat = 0;
	private int _unclassifiedCat = 0;
	private int _unclassifiedDept = 0;
	private int _unclassifiedSegmentProductId = 0;
	private int updateDBAfter = 5000;
	private int newItemCount = 0;
	private int newCustomerCount = 0;
	private HashMap<String, Integer> _newCustomerMap = new HashMap<String, Integer>();//Card No and Store ID
	private boolean preloadItemCache = false;
	private boolean preloadCustCache = false;
	// Changes to retrieve item_code using upc and retailer_item_code
	boolean checkRetailerItemCode = false;
	private HashMap<Integer, HashMap<String, Integer>> _itemHashMapStoreUpc = new HashMap<Integer, HashMap<String, Integer>>();
	public TransactionLogLoader(String StoreFromConfig) {
		super("analysis.properties");
		try {
			this._Conn = DBManager.getConnection();
			_custDao = new CustomerDAO();
			int prestoSubscriber = Integer.parseInt(PropertyManager
					.getProperty("PRESTO_SUBSCRIBER"));
			if (StoreFromConfig.equalsIgnoreCase("Y")) {
				logger.debug("Load Processing Stores from configuration");
				String store = PropertyManager
						.getProperty("MOVEMENTLOAD_STORES");

				String[] storeArr = store.split(",");
				this._storeMap = new CompStoreDAO().getCompStoreData(
						this._Conn, prestoSubscriber, storeArr);

				this._storeFromConfig = 1;
			} else {
				logger.debug("Load Processing Stores from Database");
				this._storeMap = new CompStoreDAO().getCompStoreData(
						this._Conn, prestoSubscriber, null);
			}
			this._custMap = new HashMap();

			this._unclassifiedSegment = Integer.parseInt(PropertyManager
					.getProperty("ITEM.UNCLASSIFIED_SEGMENT", "0"));
			this._unclassifiedSubCat = Integer.parseInt(PropertyManager
					.getProperty("ITEM.UNCLASSIFIED_SUB_CATEGORY", "0"));
			this._unclassifiedCat = Integer.parseInt(PropertyManager
					.getProperty("ITEM.UNCLASSIFIED_CATEGORY", "0"));
			this._unclassifiedDept = Integer.parseInt(PropertyManager
					.getProperty("ITEM.UNCLASSIFIED_DEPARTMENT", "0"));
			this._unclassifiedSegmentProductId = Integer.parseInt(PropertyManager
					.getProperty("ITEM.UNCLASSIFIED_SEGEMENT_PRODUCT_ID", "0"));
			checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
			preloadItemCache = Boolean.parseBoolean(PropertyManager.getProperty("MOVEMENTLOAD.PRELOAD_ITEM_CACHE", "TRUE"));
			preloadCustCache = Boolean.parseBoolean(PropertyManager.getProperty("MOVEMENTLOAD.PRELOAD_CUSTOMER_CACHE", "TRUE"));
			updateDBAfter = Integer.parseInt(PropertyManager.getProperty("MOVEMENTLOAD.COMMITRECOUNT", "25000"));
			
		} catch (GeneralException localGeneralException) {
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator
				.configure("log4j-transaction-log-loader.properties");
		logCommand(args);

		String subFolder = null;
		boolean updateTransactions = false;
		int calendarType = 0;
		Date processDate = null;
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		String storeFromConfig = "N";
		String targetTableType = null;
		String targetTable = "TRANSACTION_LOG";

		logger.info("main() - Started");
		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];
			if (arg.startsWith("SUBFOLDER")) {
				subFolder = arg.substring("SUBFOLDER=".length());
			}
			if (arg.startsWith("UPDATEMODE")) {
				String input = arg.substring("UPDATEMODE=".length());
				try {
					updateTransactions = Boolean.parseBoolean(input);
				} catch (Exception exe) {
					logger.error("main() - Input Parameter : UPDATEMODE : "
							+ exe);
					logger.info("main() - Ended");
					System.exit(1);
				}
			}
			if (arg.startsWith("CALENDARTYPE")) {
				String calTypeStr = arg.substring("CALENDARTYPE=".length());
				if (calTypeStr.equalsIgnoreCase("PARAMETER")) {
					calendarType = 0;
				} else if (calTypeStr.equalsIgnoreCase("SINGLE")) {
					calendarType = 1;
				} else if (calTypeStr.equalsIgnoreCase("MULTI")) {
					calendarType = 2;
				}
			}
			if ((calendarType == 0) && (arg.startsWith("PROCESSDATE"))) {
				String processDateStr = arg.substring("PROCESSDATE=".length());
				try {
					processDate = dateFormat.parse(processDateStr);
				} catch (ParseException par) {
					logger.error("main() - Process Date Parsing Error, check Input");
				}
			}
			if (arg.startsWith("STORECONFIG")) {
				storeFromConfig = arg.substring("STORECONFIG=".length());
			}
			if (arg.startsWith("TARGETTYPE")) {
				targetTableType = arg.substring("TARGETTYPE=".length());
				if (targetTableType.equalsIgnoreCase("DAILY")) {
					targetTable = "MOVEMENT_DAILY";
				}
				if (targetTableType.equalsIgnoreCase("HOURLY")) {
					targetTable = "MOVEMENT_HOURLY";
				}
				if (targetTableType.equalsIgnoreCase("ITEM")) {
					targetTable = "MOVEMENT_ITEM_SUMMARY";
				}
			}
		}
		logger.info("main() - Target Table " + targetTable);

		TransactionLogLoader dataload = new TransactionLogLoader(
				storeFromConfig);
		dataload._UpdateTransactions = updateTransactions;
		dataload._calIdType = calendarType;

		dataload.process(subFolder, processDate, targetTable);
		logger.info("main() - Ended");
	}

	public void processRecords(List listobj) throws GeneralException {
	}

	private void process(String subFolder, Date processDate, String targetTable) {
		String msg = "process() - Transaction file processing started"
				+ (this._UpdateTransactions ? ": updating Transactions" : "");
		logger.info(msg);
		
		try {
			try {
				this._StopAfter = Integer.parseInt(PropertyManager.getProperty(
						"DATALOAD.STOP_AFTER", "-1"));
			} catch (Exception ex) {
				this._StopAfter = -1;
			}
			if ((this._calIdType == 0) && (processDate != null)) {
				String calKey = processDate.getDate() + "_"
						+ processDate.getMonth() + "_" + processDate.getYear();
				getRetailCalendarIDV2(this._Conn, processDate, calKey, "D");
			}
			
			//Preload Item cache.
			if(preloadItemCache){
				getItemCodeList();
				if(checkRetailerItemCode){
					getAuthorizedItems();
				}
				logger.info("process() - Item codes are cached. Size of Cache - c1 = " + _itemHashMap.size() + " c2 = " + _itemHashMapStoreUpc.size());
			}
			//Preload customer cache.
		/*	if(preloadCustCache){
				getCustomerList();
				logger.info("process() - Customer ids are cached.");
			}*/
				
			
			ArrayList<String> zipFileList = getZipFiles(subFolder);

			logger.info("process() - Number of Zip files to be processed="
					+ zipFileList.size());

			int nFilesProcessed = 0;
			int curZipFileCount = -1;
			boolean processZipFile = false;
			String zipFilePath = getRootPath() + "/" + subFolder;
			do {
				ArrayList<String> files = null;
				boolean commit = false;
				try {
					if (processZipFile) {
						PrestoUtil.unzip(
								(String) zipFileList.get(curZipFileCount),
								zipFilePath);
					}
					files = getFiles(subFolder);
					if ((files != null) && (files.size() > 0)) {
						if (processZipFile) {
							logger.info("process() - files to be processed after unzip="
									+ files.size());
						} else {
							logger.info("process() - Non zip files uploaded!  Count="
									+ files.size());
						}
						//int run = 0;
						/*
						 * RUN 0 is to setup Customer Id records after reading the file
						 * RUN 1 is to setup T-LOG records after reading the file one more time
						 * */
						 
							for ( int ii = 0; ii < files.size(); ii++ ) {
								String file = files.get(ii);
								nFilesProcessed++;
								int run = 0;
								do {
									if (run > 0) {
										logger.info("Run " + run + " - Tlog Loading process() - Processing file  "
												+ nFilesProcessed + "...  file_name=" + file);

										commit = readAndLoadMovements(file, '|', targetTable);
										logger.info("Skipping " + run);
									} else {
										if(preloadCustCache){
											logger.info("Run " + run + " Customer Setup process() - Processing file  "
													+ nFilesProcessed + "...  file_name=" + file);
											preProcessCustomers(file, '|');

											logger.info("setting up customers count =  " + _newCustomerMap.size());
											//Preprocessing run, then insert the customers.
											CustomerDAO custdao = new CustomerDAO();	
											_custMap = custdao.getCustomerList(_Conn, _newCustomerMap.keySet());
											//Remove card numbers which are available already in the database.
											Iterator<Entry<String, Integer>> it = _newCustomerMap.entrySet().iterator();
											List<String> listToRemove = new ArrayList<String>();
											while (it.hasNext())
											{
											   Entry<String, Integer> item = it.next();
											   if(_custMap.get(item.getKey()) != null){
												   listToRemove.add(item.getKey());
											   }
											}
											
											for(String key : listToRemove){
												_newCustomerMap.remove(key);
											}
											int newCustCount = custdao.setupNewCustomerCardsAndUpdateCache(_Conn, _newCustomerMap, updateDBAfter);
											logger.info("process() - # of new customers inserted - " + newCustCount);
											_custMap.putAll(custdao.getCustomerList(_Conn, _newCustomerMap.keySet()));
											}
										}
									run++;
								} while (run < 2);
							}
					} else if (processZipFile) {
						logger.error("process() - Csv/txt file NOT FOUND after unzip!");
					}
				} catch (GeneralException ex) {
					logger.error("process() - GeneralException=", ex);
				} catch (Exception ex) {
					logger.error("process() - Exception=", ex);
				}
				if (processZipFile) {
					if (files.size() > 0) {
						logger.info("process() - DELETING files..");
						PrestoUtil.deleteFiles(files);
					}
					files.clear();
					files.add((String) zipFileList.get(curZipFileCount));
				}
				if (files.size() > 0) {
					if (commit) {
						PrestoUtil.moveFiles(files, zipFilePath + "/"
								+ "CompletedData");
					} else {
						logger.info("process() - Moving 'Failed files' to BAD folder...");
						PrestoUtil.moveFiles(files, zipFilePath + "/"
								+ "BadData");
					}
				}
				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());
			if (nFilesProcessed > 0) {
				logger.info("process() - Transaction file processing completed.");
			} else {
				logger.warn("process() - No transaction file was processed!");
			}
			PristineDBUtil.close(this._Conn);
			_custMap.clear();
		} catch (Exception ex) {
			logger.error("process() - Exception=", ex);
		} catch (GeneralException ex) {
			logger.error("process() - GeneralException=", ex);
		}
	}

	private boolean readAndLoadMovements(String fileName, char delimiter,
			String targetTable) throws Exception, GeneralException {
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		String processDateTime = dateFormat.format(new Date());

		TransactionLogTrackerDAO trackDAO = new TransactionLogTrackerDAO();

		String processStatus = "Started";

		String fileNameStr = fileName.substring(fileName.lastIndexOf('/') + 1);

		int trackId = trackDAO.insertTLogTrack(this._Conn, fileNameStr,
				processDateTime, processStatus);
		if (trackId > 0) {
			this._Conn.commit();
		}
		logger.info("readAndLoadMovements() - trackId=" + trackId);

		ArrayList<TransactionLogDTO> transList = new ArrayList();
		ArrayList<TransactionLogDTO> exceptionList = new ArrayList();
		ArrayList<String> errorList = new ArrayList<String>();
		this._Conn.setAutoCommit(false);
		CsvReader reader = readFile(fileName, delimiter);

		MovementDAO movementDao = new MovementDAO();

		int saved_count = 0;
		int total_count = 0;
		int countFailed = 0;
		int countSkipped = 0;
		int itemCodeMissCount = 0;
		int processStat = 0;
		boolean ret;
		try {
			reader = readFile(fileName, delimiter);
			while (reader.readRecord()) {
				int error_code = 0;
				String[] line = reader.getValues();
				total_count++;
				TransactionLogDTO tLogdto = new TransactionLogDTO();
				if (line.length > 0) {
					try {
						String store = line[2];
						store = PrestoUtil.castStoreNumber(store);
						tLogdto.setCompStoreNumber(store);
						int storeId = getStoreId(store);
						if ((this._storeFromConfig == 0) && (storeId == 0)) {
							tLogdto.setErrorMessage("Invalid Store Number");
							error_code = 1;
						}
						if ((this._storeFromConfig == 1) && (storeId == 0)) {
							countSkipped++;
							this._UnprocessedStores.add(store);
							tLogdto = null;
							continue;
						}
						tLogdto.setCompStoreId(storeId);

						int termNo = 0;
						if (!StringUtils.isEmpty(line[3])) {
							termNo = Integer.parseInt(line[3]);
						}
						int tranNo = new Double(Double.parseDouble(line[5]))
								.intValue();
						tLogdto.setTransactionNo(termNo * 10000 + tranNo);

						String timestamp = line[6];
						int year = Integer.parseInt(timestamp.substring(0, 4));
						int month = Integer.parseInt(timestamp.substring(4, 6)) - 1;
						int date = Integer.parseInt(timestamp.substring(6, 8));
						int hour = Integer.parseInt(timestamp.substring(8, 10));
						int minute = Integer.parseInt(timestamp.substring(10));
						Calendar cal = Calendar.getInstance();
						cal.set(year, month, date, hour, minute);
						tLogdto.setTransationTimeStamp(cal.getTime());

						String calKey = date + "_" + month + "_" + year;

						int calendarId = 0;
						if ((this._calIdType == 1) || (this._calIdType == 0)) {
							calendarId = getRetailCalendarIDV2(this._Conn,
									tLogdto.getTransationTimeStamp(), calKey,
									"D");
						} else {
							calendarId = getRetailCalendarID(this._Conn,
									tLogdto.getTransationTimeStamp(), calKey,
									"D");
						}
						if (calendarId > 0) {
							tLogdto.setCalendarId(calendarId);
						} else {
							logger.error("readAndLoadMovements() - Calendar Id NOT found for date "
									+ cal.getTime()
									+ ", Check Retail Calendar Master data!");
							tLogdto.setErrorMessage("Calendar Id not found");
							countSkipped++;
							error_code = 2;
						}
						String UPC = "";
						if (line[7].indexOf(".") >= 0) {
							UPC = PrestoUtil.castUPC(
									line[7].substring(0, line[7].indexOf(".")),
									false);
						} else {
							UPC = PrestoUtil.castUPC(line[7], false);
						}
						int itemCode = getItemCode(UPC);
						tLogdto.setItemUPC(UPC);
						if (itemCode > 0) {
							tLogdto.setItemCode(itemCode);
						} else {
							itemCodeMissCount++;
							error_code = 49;
						}
						double unitSalePrice = 0.0D;
						if (!StringUtils.isEmpty(line[8])) {
							unitSalePrice = Double.parseDouble(line[8]);
						}
						double unitRegPrice = 0.0D;
						if (!StringUtils.isEmpty(line[9])) {
							unitRegPrice = Double.parseDouble(line[9]);
						}
						double unitPrice = unitSalePrice < unitRegPrice ? unitSalePrice
								: unitRegPrice;
						tLogdto.setItemNetPrice(PrestoUtil.round(unitPrice, 2));
						if ((unitSalePrice < -99999.990000000005D)
								|| (unitSalePrice > 99999.990000000005D)) {
							error_code = 3;
							tLogdto.setErrorMessage("Invlid unit price");
						}
						int itemCount = 0;
						if (!StringUtils.isEmpty(line[10])) {
							itemCount = Integer.parseInt(line[10]);
						}
						tLogdto.setQuantity(itemCount);
						if ((itemCount < -999999) || (itemCount > 999999)) {
							error_code = 5;
							tLogdto.setErrorMessage("Invlid Quantity");
						}
						int itemWeight = 0;
						if (!StringUtils.isEmpty(line[11])) {
							itemWeight = Integer.parseInt(line[11].trim());
						}
						tLogdto.setWeight(itemWeight);
						if ((itemWeight < -9999.9999000000007D)
								|| (itemWeight > 9999.9999000000007D)) {
							error_code = 6;
							tLogdto.setErrorMessage("Invlid Weight");
						}
						double itemGrossPrice = 0.0D;
						if (!StringUtils.isEmpty(line[12])) {
							itemGrossPrice = Double.parseDouble(line[12]);
						}
						tLogdto.setExtendedGrossPrice(PrestoUtil.round(
								itemGrossPrice, 2));
						if ((itemGrossPrice < -999999.98999999999D)
								|| (itemGrossPrice > 999999.98999999999D)) {
							error_code = 8;
							tLogdto.setErrorMessage("Invlid Gross price");
						}
						double tprDiscount = 0.0D;
						if (!StringUtils.isEmpty(line[13])) {
							tprDiscount = Double.parseDouble(line[13]);
						}
						double loyaltyDiscount = 0.0D;
						if (!StringUtils.isEmpty(line[14])) {
							loyaltyDiscount = Double.parseDouble(line[14]);
						}
						double otherDiscount = 0.0D;
						if (!StringUtils.isEmpty(line[15])) {
							otherDiscount = Double.parseDouble(line[15]);
						}
						double itemNetPrice = 0.0D;
						if (!StringUtils.isEmpty(line[16])) {
							itemNetPrice = Double.parseDouble(line[16]);
						} else {
							itemNetPrice = itemGrossPrice - tprDiscount
									- loyaltyDiscount - otherDiscount;
						}
						tLogdto.setExtendedNetPrice(PrestoUtil.round(
								itemNetPrice, 2));
						if ((itemNetPrice < -999999.98999999999D)
								|| (itemNetPrice > 999999.98999999999D)) {
							error_code = 7;
						}

						String customer = line[17] != null ? line[17] : "";
						tLogdto.setCustomerCardNo(customer);
						if (!customer.isEmpty() && customer.trim().length() > 0)
							tLogdto.setCustomerId(getCustomerId(customer,
									storeId));
						// MFG CPN, SPONSORED CPN
						// String strore_cpn_used = line[18];
						if (!line[4].equalsIgnoreCase("SALE")
								&& !line[4].equalsIgnoreCase("MFG CPN"))
							tLogdto.setStoreCouponUsed("Y");
						else
							tLogdto.setStoreCouponUsed("N");
						if (line[4].equalsIgnoreCase("MFG CPN"))
							// String mfr_cpn_used = line[19];
							tLogdto.setMfrCouponUsed("Y");
						else
							tLogdto.setMfrCouponUsed("N");
					} catch (Exception e) {
						logger.error("readAndLoadMovements() - Error occured in data "
								+ e);
						e.printStackTrace();
						tLogdto.setErrorMessage("Unhandled exception");
						error_code = 99;
					}
					tLogdto.setProcessRow(total_count);
					if (error_code == 0) {
						transList.add(tLogdto);
					} else {
						countFailed++;
						exceptionList.add(tLogdto);
						errorList.add("Error # - " + error_code);
					}
					tLogdto = null;

					if (transList.size() % updateDBAfter == 0) {
						logger.debug("readAndLoadMovements() - Update into DB");

						movementDao.insertTransactionLog(this._Conn, transList,
								targetTable, trackId);
						saved_count += transList.size();

						transList.clear();
					}
					if (total_count % 25000 == 0) {
						logger.info("readAndLoadMovements() - Record Counts: Processed="
								+ String.valueOf(total_count)
								+ ", Saved="
								+ String.valueOf(saved_count)
								+ ", Error="
								+ String.valueOf(countFailed)
								+ ", Skipped="
								+ String.valueOf(countSkipped));
						trackDAO.UpdateTLogStatus(this._Conn, trackId,
								processDateTime, "InProgress", total_count,
								countFailed);
						this._Conn.commit();
					}
					if ((this._StopAfter > 0)
							&& (total_count >= this._StopAfter)) {
						break;
					}
				}
			}

			if (transList.size() > 0) {
				MovementDAO objMovement = new MovementDAO();
				objMovement.insertTransactionLog(this._Conn, transList,
						targetTable, trackId);
				saved_count += transList.size();
			}
			if (countSkipped > 0) {
				Iterator<String> it = this._UnprocessedStores.iterator();
				int ii = 0;
				StringBuffer sb = new StringBuffer(
						"readAndLoadMovements() - Stores skipped: ");
				while (it.hasNext()) {
					String store = (String) it.next();
					if (ii > 0) {
						sb.append(',');
					}
					sb.append(store);
					ii++;
				}
				logger.info(sb.toString());
			}
			if (exceptionList.size() > 0) {
				MovementDAO objMovement = new MovementDAO();
				logger.info("readAndLoadMovements() - Saving error records...  Count="
						+ exceptionList.size());
				objMovement.insertErrorTLog(this._Conn, exceptionList,
						targetTable, trackId);
			}
			ret = countFailed < total_count;
		} catch (Exception ex) {

			logger.error("readAndLoadMovements() - Exception=", ex);
			ret = false;
			processStat = -1;
		} catch (GeneralException ex) {
			logger.error("readAndLoadMovements() - GeneralException=", ex);
			ret = false;
			processStat = -1;
		} finally {
			logger.info("readAndLoadMovements() - Record Counts: Processed="
					+ String.valueOf(total_count) + ", Saved="
					+ String.valueOf(saved_count) + ", Error="
					+ String.valueOf(countFailed) + ", Skipped="
					+ String.valueOf(countSkipped));
			logger.info("readAndLoadMovements() - Number of new items inserted into Item master : "
					+ String.valueOf(newItemCount));
		/*	logger.info("readAndLoadMovements() - Number of new customers inserted : "
					+ String.valueOf(newCustomerCount));*/
			processDateTime = dateFormat.format(new Date());
			if (processStat == -1) {
				trackDAO.UpdateTLogStatus(this._Conn, trackId, processDateTime,
						"Failed", total_count, countFailed);
				this._Conn.commit();
			} else {
				trackDAO.UpdateTLogStatus(this._Conn, trackId, processDateTime,
						"Completed", total_count, countFailed);
				this._Conn.commit();

			}
			if (reader != null) {
				reader.close();
			}
			//_custMap.clear();
			newCustomerCount = 0;
			newItemCount = 0;
		}
		return ret;
	}

	public int getRetailCalendarID(Connection _Conn, Date startDate,
			String calKey, String calendarMode) throws GeneralException {
		int calId = -1;
		if (this._calendarMap.containsKey(calKey)) {
			calId = Integer.parseInt((String) this._calendarMap.get(calKey));
		} else {
			RetailCalendarDAO objCal = new RetailCalendarDAO();

			List<RetailCalendarDTO> dateList = objCal.dayCalendarList(_Conn,
					startDate, startDate, calendarMode);
			if (dateList.size() > 0) {
				RetailCalendarDTO calDto = (RetailCalendarDTO) dateList.get(0);
				calId = calDto.getCalendarId();

				this._calendarMap.put(calKey, String.valueOf(calId));

				logger.info("getRetailCalendarID() - Calendar Id for date "
						+ startDate.toString() + " is " + calId);
				if (this._calendarMap.size() > 1) {
					logger.warn("getRetailCalendarID() - Transasaction received for MULTIPLE DATES!  Date Count="
							+ this._calendarMap.size());
				}
			}
		}
		return calId;
	}

	public int getRetailCalendarIDV2(Connection _Conn, Date startDate,
			String calKey, String calendarMode) throws GeneralException {
		int calId = -1;
		if (this._calendarMap.containsKey(calKey)) {
			calId = this._processCalendarId;
		} else if (this._processCalendarId > 0) {
			this._calendarMap.put(calKey, "S");
			calId = this._processCalendarId;
			logger.warn("Input file has data for differnt date.."
					+ startDate.toString());
		} else {
			RetailCalendarDAO objCal = new RetailCalendarDAO();

			List<RetailCalendarDTO> dateList = objCal.dayCalendarList(_Conn,
					startDate, startDate, calendarMode);
			if (dateList.size() > 0) {
				RetailCalendarDTO calDto = (RetailCalendarDTO) dateList.get(0);

				this._processCalendarId = calDto.getCalendarId();
				calId = this._processCalendarId;
				this._calendarMap.put(calKey, "F");
				logger.info("Processing Date is....................."
						+ startDate.toString());
				logger.info("Calendar Id for processing Date is....." + calId);
			} else {
				logger.error("Calendar Id not found for Date........"
						+ startDate.toString());
			}
		}
		return calId;
	}
	/**returns item code from ITEM_LOOKUP table if the given UPC is available. 
	 * Otherwise it will insert a record for new item in ITEM_LOOKUP as well as in PRODUCT_GROUP_RELATION also.
	 * @param strUpc String
	 * @throws GeneralException*/
	public int getItemCode(String strUpc) throws GeneralException {
		int itemCode = -1;
		try {
			String itemUpc = PrestoUtil.castUPC(strUpc, false);
			String stdUpc = "";
			if (this._itemHashMap.containsKey(itemUpc)) {
				itemCode = ((Integer) this._itemHashMap.get(itemUpc))
						.intValue();
			} else {
				ItemDAO objItemDao = new ItemDAO();
				itemCode = objItemDao.getItemCodeForUPC(this._Conn, itemUpc);
				// Calculating check digit to fill STANDARD_UPC column in
				// ITEM_LOOKUP...
				if (itemUpc.length() == 12 && itemUpc.charAt(0) == '0') {
					stdUpc = itemUpc.substring(1);
					int checkDigit = dataProcessor
							.findUPCCheckDigit(stdUpc);
					stdUpc = stdUpc
							+ Integer.toString(checkDigit);
				}
				else{
					stdUpc = itemUpc;
				}
				//Check digit calculation ends.
				if(itemCode < 0){
					//check with Standard UPC also in ITEM_LOOKUP to get item code. 
					itemCode = objItemDao.getItemCodeForUPC(this._Conn, stdUpc);
				}
				if (itemCode > 0) {
					this._itemHashMap.put(itemUpc, Integer.valueOf(itemCode));
				} else {
					ItemDTO item = new ItemDTO();
					item.upc = itemUpc;
					item.standardUPC = stdUpc;
					item.itemName = ("Unclassified " + itemUpc);
					item.retailerItemCode = "X-" + itemUpc.substring(6);
					item.segmentID = this._unclassifiedSegment;
					item.subCatID = this._unclassifiedSubCat;
					item.catID = this._unclassifiedCat;
					item.deptID = this._unclassifiedDept;
					//inserts new item into item master. 
					boolean isItemInserted = objItemDao.insertItem(this._Conn,
							item, false, false);
					//inserts the item into item level of product group hierarchy.
					if (isItemInserted) {
						newItemCount++;
						com.pristine.dto.salesanalysis.ProductDTO product = new com.pristine.dto.salesanalysis.ProductDTO();
						product.setChildProductId(item.itemCode);
						product.setChildProductLevelId(Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID);
						product.setProductLevelId(Constants.SEGMENT_LEVEL_PRODUCT_LEVEL_ID);
						product.setProductId(this._unclassifiedSegmentProductId);
						ProductGroupDAO productDao = new ProductGroupDAO();
						productDao.insertProductGroupRelation(_Conn, product);
					} else {
						logger.warn("getItemCode() - The item with UPC '" + item.upc
								+ "' was not inserted into ITEM_LOOKUP!");
					}
					return item.itemCode;
				}
			}
		} catch (Exception e) {
			logger.error("Error while getting item code " + e.getMessage());
		}
		return itemCode;
	}

	private void getItemCodeList() {
		ItemDAO objItem = new ItemDAO();
		try {
			this._itemHashMap = objItem.getUPCAndItem(this._Conn);
		} catch (GeneralException e) {
			logger.error(e.getCause());
		}
	}

	private static void logCommand(String[] args) {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: MovementDataLoad ");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0) {
				sb.append(' ');
			}
			sb.append(args[ii]);
		}
		logger.info(sb.toString());
	}

	private boolean _UpdateTransactions = false;
	private static String ARG_UPDATE_TRANSACTIONS = "UPDATE_TRANSACTIONS";

	public HashMap<String, Integer> getupcMissingMap() {
		return this._upcMissinghMap;
	}

	private int getCustomerId(String customerCardNo, int storeId)
			throws GeneralException, Exception {
		int customerId = 0;
		try{
		if (customerCardNo.trim().length() > 0) {
			if (this._custMap.containsKey(customerCardNo)) {
				customerId = ((Integer) this._custMap.get(customerCardNo))
						.intValue();
			} else {
				customerId = _custDao.getCustomerId(this._Conn, customerCardNo,
						storeId);
				if (customerId != 0){
					this._custMap.put(customerCardNo,
							Integer.valueOf(customerId));
				}
			}
		}
		}
		catch(Exception e){
			logger.error("Error while getting customer id.");
		}
		return customerId;
	}

	private int getStoreId(String storeNo) throws GeneralException, Exception {
		int storeId = 0;
		if (this._storeMap.containsKey(storeNo)) {
			storeId = ((Integer) this._storeMap.get(storeNo)).intValue();
		}
		return storeId;
	}
	
	private boolean preProcessCustomers(String fileName, char delimiter)
			throws Exception, GeneralException {

		CsvReader reader = readFile(fileName, delimiter);
        String line[];
        int total_count = 0;
        int previousTranId = -1;
        int previousStoreId = -1;
        boolean ret = true;
        try
        {
        	int error_code; 
	        while (reader.readRecord())
	        {
	        	error_code = 0;
	            line = reader.getValues();
	            total_count++;
	            if ( line.length > 0 )
	            {
	            	String store = line[2];
					store = PrestoUtil.castStoreNumber(store);
					int storeId = getStoreId(store);
					if ( storeId == 0) {
							continue;
					}
					
					if( previousStoreId != storeId){
						previousTranId = -1;
						previousStoreId = storeId;
					}
						
					
					int termNo = 0;
					if (!StringUtils.isEmpty(line[3])) {
						termNo = Integer.parseInt(line[3]);
					}
					int tranNo = new Double(Double.parseDouble(line[5]))
							.intValue();
					int transactionId = termNo * 10000 + tranNo;
					
					if( previousTranId == transactionId){
						continue;
					}
					previousTranId = transactionId;
	
					String customerCardNo = line[17] != null ? line[17] : "";
					
					 if (customerCardNo.trim().length() > 0){
						 if(!_custMap.containsKey(customerCardNo) && !_newCustomerMap.containsKey(customerCardNo)) {
							 _newCustomerMap.put(customerCardNo,storeId);
						 }
					 }
					
	            }
	        }
        }  catch (Exception ex) {
        	logger.error("readAndLoadMovements() - Exception=", ex);
        	ret = false;

        }
        catch (GeneralException ex) {
        	logger.error("readAndLoadMovements() - GeneralException=", ex);
        	ret = false;

        }
        finally {
        
        	if( reader != null){
        		reader.close();
        	}
        	logger.info(" # of records processed - " + total_count);
        }
		return ret;
	}
	
	private void getAuthorizedItems(){
		ItemDAO objItem = new ItemDAO();
		try {
			logger.info("Preload Item code/UPC authorized at store level");
			_itemHashMapStoreUpc = objItem.getAuthorizedItems(_Conn);
			logger.info("Total Item code/UPC authorized at store level: " + _itemHashMapStoreUpc.size());
		
		} catch (GeneralException e) {
			logger.error(e.getCause());
		}
	}
	
	
	private void getCustomerList()
	{
		
		try {
			CustomerDAO custdao = new CustomerDAO();	
			_custMap = custdao.getAllCustomerCards(_Conn, null);
		} catch (GeneralException e) {
			logger.error(e.getCause());
		}
		
	}
	
}
