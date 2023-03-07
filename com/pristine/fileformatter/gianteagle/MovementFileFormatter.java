package com.pristine.fileformatter.gianteagle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleTlogDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MovementFileFormatter extends PristineFileParser{
	
	private static Logger logger = Logger.getLogger("GEMovementFileFormatter");
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private int recordCount = 0;
	private int noOfReturnCount = 0;
	List<TransactionLogKey> skippedReturnRecList;
	private int skippedCount = 0;
	private static String relativeInputPath, relativeOutputPath, couponOutputPath;
	private String rootPath;
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private List<String> _StoreList = new ArrayList<String>();
	private List<String> _UpcList = new ArrayList<String>();
	private List<String> couponTypeList = new ArrayList<String>();
	private String ZERO = "0";
	private static boolean ignoreCouponFile = false;
	HashMap<TransactionLogKey, List<GiantEagleTlogDTO>> couponRecordsMap;
	HashMap<TransactionLogKey, List<GiantEagleTlogDTO>> regularRecordsMap;
	List<GiantEagleTlogDTO> couponRecWithoutLinkUPC;
	HashMap<TransactionLogKey, Integer> regularItemCountMap;
	private boolean isCouponRecProcessed = false;
	GiantEagleTlogDTO aggrTlogDTO;
	boolean isRecordExits = false;
	FileWriter fw1 = null;
	PrintWriter pw1 = null;
	Connection conn = null;
	
	
	public MovementFileFormatter(String storeFromConfig, String upcFromConfig){
		if (storeFromConfig.equalsIgnoreCase("Y")){
			logger.debug("Load Processing Stores from configuration");
			String store = PropertyManager.getProperty("MOVEMENTFORMAT_STORES");
			String[] storeArr = store.split(",");
			for (int i = 0; i < storeArr.length; i++) {
				_StoreList.add(storeArr[i]);
				logger.debug("Store " + storeArr[i]);
			}
		}
		if (upcFromConfig.equalsIgnoreCase("Y")){
			logger.debug("Load Processing UPCs from configuration");
			String upc = PropertyManager.getProperty("MOVEMENTFORMAT_UPCS");
			String[] upcArr = upc.split(",");
			for (int i = 0; i < upcArr.length; i++) {
				_UpcList.add(upcArr[i]);
				logger.debug("UPC " + upcArr[i]);
			}
		}
		
		String couponTypes = PropertyManager.getProperty("COUPON_TYPES_TO_PROCESS");
		String[] coupon = couponTypes.split(",");
		for (int i = 0; i < coupon.length; i++) {
			couponTypeList.add(coupon[i].trim().toUpperCase());
			logger.debug("Coupon" + coupon[i]);
		}
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException e) {
			logger.info("Error", e);
		}
	}
	
	/**
	 * @param args
	 * @throws GeneralException
	 * @throws IOException
	 */
	public static void main(String[] args) throws GeneralException, IOException {
		String storeFromConfig = "N";
		String upcFromConfig = "N";
		
		PropertyConfigurator.configure("log4j-ge-movement-file-formatter.properties");
	
		if (args.length < 2) {
			logger.info("Invalid Arguments,  args[0] - TLog File Input Path, args[1] - TLog File Output Path");
			System.exit(-1);
		}
		
		relativeInputPath = args[0];
		relativeOutputPath = args[1];
		
		for(String arg : args){
			if (arg.startsWith("STORECONFIG")) {
				storeFromConfig = arg.substring("STORECONFIG=".length());
			}
			if (arg.startsWith("UPCCONFIG")) {
				upcFromConfig = arg.substring("UPCCONFIG=".length());
			}
			if(arg.startsWith("IGNORE_COUPON_RECORD")){
				ignoreCouponFile = Boolean.parseBoolean(arg.substring("IGNORE_COUPON_RECORD=".length()));
			}
			if(arg.startsWith("COUPON_OUTPUT_PATH")){
				couponOutputPath = arg.substring("COUPON_OUTPUT_PATH=".length());
			}
		}
		MovementFileFormatter fileFormatter = new MovementFileFormatter(storeFromConfig, upcFromConfig);
		fileFormatter.processFile();
	}
	
	/**
	 * Parse file and create retail price and retail cost file
	 * @throws GeneralException
	 * @throws IOException
	 */
	private void processFile() throws GeneralException, IOException {

		super.headerPresent = false;
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}

		logger.info("Formatting Tlog file Started... ");
		parseFile(fieldNames);
		logger.info("Total Number of Items " + recordCount);
		logger.info("Formatting Tlog file Completed... ");
	}
	
	private void parseFile(String fieldNames[]) throws GeneralException, IOException {

		try {
			//getzip files
			ArrayList<String> zipFileList = getZipFiles(relativeInputPath);
			
			//Start with -1 so that if any regular files are present, they are processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;
			
			String zipFilePath = getRootPath() + "/" + relativeInputPath;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;
				String archivePath = getRootPath() + "/" + relativeInputPath + "/";
				String files = null;
				
					if( processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;
	
					fileList = getFiles(relativeInputPath);
					
				for (int i = 0; i < fileList.size(); i++) {
					try {
						long fileProcessStartTime = System.currentTimeMillis();

						recordCount = 0;
						skippedCount = 0;

						files = fileList.get(i);
						logger.info("File Name - " + files);
						int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));

						String outputFileName[] = files.split("/");
						logger.info("Output File name - " + outputFileName[outputFileName.length - 1]);

						String outputPath = rootPath + "/" + relativeOutputPath + "/"
								+ outputFileName[outputFileName.length - 1];

						File file = new File(outputPath);
						if (!file.exists())
							fw = new FileWriter(outputPath);
						else
							fw = new FileWriter(outputPath, true);

						pw = new PrintWriter(fw);

						logger.info("Processing Retail Records ...");
						// To process Coupon records
						skippedReturnRecList = new ArrayList<TransactionLogKey>();
						couponRecordsMap = new HashMap<TransactionLogKey, List<GiantEagleTlogDTO>>();
						couponRecWithoutLinkUPC = new ArrayList<GiantEagleTlogDTO>();
						regularRecordsMap = new HashMap<TransactionLogKey, List<GiantEagleTlogDTO>>();
						regularItemCountMap  = new HashMap<TransactionLogKey, Integer>();
						isCouponRecProcessed = false;
						super.parseDelimitedFile(GiantEagleTlogDTO.class, fileList.get(i), '|', fieldNames, stopCount);
						isCouponRecProcessed = true;
						// To process regular records
						super.parseDelimitedFile(GiantEagleTlogDTO.class, fileList.get(i), '|', fieldNames, stopCount);
						logger.info("No of records processed - " + recordCount);
						logger.info("No of records skipped - " + skippedCount);
						logger.info("No of return records skipped -"+noOfReturnCount);
						pw.flush();
						fw.flush();
						pw.close();
						fw.close();
						// To generate Coupon records
						generateCouponRecFile();
						long fileProcessEndTime = System.currentTimeMillis();
						logger.debug("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime)
								+ "ms");
					} catch (GeneralException ex) {
						logger.error("GeneralException", ex);
						commit = false;
					} catch (Exception ex) {
						logger.error("JavaException", ex);
						commit = false;
					}
					if (commit) {
						PrestoUtil.moveFile(files, archivePath + Constants.COMPLETED_FOLDER);
					} else {
						PrestoUtil.moveFile(files, archivePath + Constants.BAD_FOLDER);
					}
				}
				
				if( processZipFile){
			    	PrestoUtil.deleteFiles(fileList);
			    	fileList.clear();
			    	fileList.add(zipFileList.get(curZipFileCount));
			    }
				curZipFileCount++;
				processZipFile = true;
			}while (curZipFileCount < zipFileList.size());
		
		}catch (GeneralException ex) {
	        logger.error("Outer Exception - JavaException", ex);
	        ex.printStackTrace();
	    }catch (Exception ex) {
	        logger.error("Outer Exception - JavaException", ex);
	        ex.printStackTrace();
	    }finally{
	    	PristineDBUtil.close(conn);
	    }
	}

	public void processRecords(List listObject) throws GeneralException {
		List<GiantEagleTlogDTO> movementDTOList = (List<GiantEagleTlogDTO>) listObject;
		// To get only Coupon records
		if (!isCouponRecProcessed) {
			movementDTOList.stream().filter(
					s -> !_StoreList.contains(s.getSTR_NO()) && !_UpcList.contains(PrestoUtil.castUPC(s.getUPC(), false)))
					.forEach(movementDetail -> {
						String date = movementDetail.getTX_DTE_TME();
						String itemDateTime = null;
						try {
							itemDateTime = DateUtil.dateToString(DateUtil.toDate(date, "yyyyMMdd HH:mm"),
									"yyyyMMddHHmm");
						} catch (GeneralException exception) {
							exception.printStackTrace();
						}

						if (!Constants.EMPTY.equals(movementDetail.getCPN_TYP_CD())
								&& movementDetail.getCPN_TYP_CD() != null) {
							// To Process Only link records
							if(!couponTypeList.contains(movementDetail.getCPN_TYP_CD())){
								logger.error("Coupon types were not matching. New coupon type: "+movementDetail.getCPN_TYP_CD() );
							}
							if (couponTypeList.contains(movementDetail.getCPN_TYP_CD()) && !Constants.EMPTY.equals(movementDetail.getLINK_UPC())
									&& movementDetail.getLINK_UPC() != null) {
								TransactionLogKey key = new TransactionLogKey(itemDateTime, movementDetail.getSTR_NO(),
										movementDetail.getBS_STR_NO(), movementDetail.getLOYAL_CARD_NO(),
										movementDetail.getTRML_NO(), movementDetail.getTX_NO(),
										movementDetail.getLINK_UPC());
								List<GiantEagleTlogDTO> giantEagleDTO = new ArrayList<GiantEagleTlogDTO>();
								if (couponRecordsMap.containsKey(key)) {
									giantEagleDTO = couponRecordsMap.get(key);
								}
								giantEagleDTO.add(movementDetail);
								couponRecordsMap.put(key, giantEagleDTO);

								if (!ignoreCouponFile && (Constants.MANUFACTURE_COUPON_TYPE).toUpperCase()
										.equals(movementDetail.getCPN_TYP_CD().trim().toUpperCase())) {
									couponRecWithoutLinkUPC.add(movementDetail);
								}
							}
							// If it is coupon record but link UPC were not provided and ignoreCouponFile is false(to avoid generating
							// Coupon records if not needed)
							else if (!ignoreCouponFile) {
								couponRecWithoutLinkUPC.add(movementDetail);
							}
						}
						// To find number of records available for each items in transaction level data
						else if (Constants.EMPTY.equals(movementDetail.getCPN_TYP_CD())
								|| movementDetail.getCPN_TYP_CD() == null
								|| movementDetail.getITEM_TYP_CD().equals("0")) {
							TransactionLogKey key = new TransactionLogKey(itemDateTime, movementDetail.getSTR_NO(),
									movementDetail.getBS_STR_NO(), movementDetail.getLOYAL_CARD_NO(),
									movementDetail.getTRML_NO(), movementDetail.getTX_NO(), movementDetail.getUPC());
							int itemCount = 0;
							if (regularItemCountMap.containsKey(key)) {
								itemCount = regularItemCountMap.get(key);
							}
							itemCount++;
							regularItemCountMap.put(key, itemCount);
						}

					});
		}
		if (isCouponRecProcessed) {
			 List<GiantEagleTlogDTO> regularRecordList = movementDTOList.stream().filter(movementDTO ->
			(Constants.EMPTY.equals(movementDTO.getCPN_TYP_CD()) || movementDTO.getCPN_TYP_CD() == null) || movementDTO.getITEM_TYP_CD().equals("0"))
			 .collect(Collectors.toList());
			 
			 
			 regularRecordList.stream().forEach(movementDTO -> {
						boolean processStore = true;
						if (_StoreList.size() > 0) {
							processStore = _StoreList.contains(movementDTO.getSTR_NO());
						}
						if (processStore && _UpcList.size() > 0) {
							processStore = _UpcList.contains(PrestoUtil.castUPC(movementDTO.getUPC(), false));
						}
						if (processStore) {
							String date = movementDTO.getTX_DTE_TME();
							String itemDateTime = null;
							try {
								itemDateTime = DateUtil.dateToString(DateUtil.toDate(date, "yyyyMMdd HH:mm"),
										"yyyyMMddHHmm");
							} catch (GeneralException exception) {
								exception.printStackTrace();
							}
							TransactionLogKey key = new TransactionLogKey(itemDateTime, movementDTO.getSTR_NO(),
									movementDTO.getBS_STR_NO(), movementDTO.getLOYAL_CARD_NO(),
									movementDTO.getTRML_NO(), movementDTO.getTX_NO(), movementDTO.getUPC());
							List<GiantEagleTlogDTO> giantEagleDTO = new ArrayList<GiantEagleTlogDTO>();
							if (regularRecordsMap.containsKey(key)) {
								giantEagleDTO = regularRecordsMap.get(key);
							}
							giantEagleDTO.add(movementDTO);
							regularRecordsMap.put(key, giantEagleDTO);
						} else {
							skippedCount++;
						}
					});
			try {
				HashMap<TransactionLogKey, GiantEagleTlogDTO> aggrMovementDetailsMap = aggrRegRecWithCouponRecord();
				generateTransactionDetailsFile(aggrMovementDetailsMap);
			} catch (Exception e) {
				e.printStackTrace();
				throw new GeneralException("Error while processing aggrRegWithCouponRecord()", e);
			}
		}

	}

	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns() {
		allColumns.put(1,"TX_DTE_TME");
		allColumns.put(2,"STR_NO");
		allColumns.put(3,"BS_STR_NO");
		allColumns.put(4,"LOYAL_CARD_NO");
		allColumns.put(5,"UPC");
		allColumns.put(6,"ITEM_TYP_CD");
		allColumns.put(7,"STR_DAY_DTE");
		allColumns.put(8,"TRML_NO");
		allColumns.put(9,"TX_NO");
		allColumns.put(10,"SEQ_NO");
		allColumns.put(11,"PKG_QTY");
		allColumns.put(12,"EXTD_PRC");
		allColumns.put(13,"CPN_TYP_CD");
		allColumns.put(14,"PARENT_UPC");
		allColumns.put(15,"UOM_QTY");
		allColumns.put(16,"PRC_PNT");
		allColumns.put(17,"REG_ENT_TYP_CD");
		allColumns.put(18,"POS_DPT_CD");
		allColumns.put(19,"WGH_FG");
		allColumns.put(20,"CAN_KEY_FG");
		allColumns.put(21,"STR_CPN_MTPL_FG");
		allColumns.put(22,"WGH_QTY_VOL_FG");
		allColumns.put(23,"PRC_KEYED_FG");
		allColumns.put(24,"PTS_ITEM_FG");
		allColumns.put(25,"PTS_RDM_FG");
		allColumns.put(26,"TAX_A_FG");
		allColumns.put(27,"FSTMP_FG");
		allColumns.put(28,"TAX_B_FG");
		allColumns.put(29,"TAX_C_FG");
		allColumns.put(30,"TAX_D_FG");
		allColumns.put(31,"OPRT_NO");
		allColumns.put(32,"LINK_UPC");
		allColumns.put(33,"MISC_ACT_NO");
		allColumns.put(34,"FIN_TYP_CD");
	}
	
	//TO write to Transaction log details
	private void generateTransactionDetailsFile(HashMap<TransactionLogKey,GiantEagleTlogDTO> movementDTOList) throws GeneralException, Exception{
		movementDTOList.forEach((key,movementDetail)->{
			//To Skip sale cancel records or return records
			double netPrice =0.0;
			if(movementDetail.getUOM_QTY() > 0 && "Y".equals(movementDetail.getWGH_FG())){
				netPrice = PrestoUtil.round(movementDetail.getEXTD_PRC()/ movementDetail.getUOM_QTY(), 2); // Item Net Price 8	
			}else{
				netPrice = PrestoUtil.round(movementDetail.getEXTD_PRC()/ movementDetail.getPKG_QTY(), 2); // Item Net Price 8
			}
			if(movementDetail.getPKG_QTY()> 0 || movementDetail.getUOM_QTY() >0 &&
					(netPrice > -999999999.99 && netPrice < 999999999.99)){
				recordCount++;
				
				String date = movementDetail.getTX_DTE_TME();
				String itemDateTime = null;
				try{
					itemDateTime = DateUtil.dateToString(DateUtil.toDate(date, "yyyyMMdd HH:mm"), "yyyyMMddHHmm");
				}catch(GeneralException exception){
					exception.printStackTrace();
				}
					
				pw.print("0");
				pw.print("|");
				pw.print(movementDetail.getSTR_NO()); // Store # 1
				pw.print("|");
				pw.print(movementDetail.getTRML_NO()); // Terminal # 2
				pw.print("|");
				pw.print(movementDetail.getTX_NO()); // Transaction # 3
				pw.print("|");
				pw.print(movementDetail.getOPRT_NO()); //Operator 4
				pw.print("|");
				pw.print(itemDateTime); //Item Date Time 5
				pw.print("|");
				pw.print(movementDetail.getLOYAL_CARD_NO()); // Customer Id 6
				pw.print("|");
				pw.print(movementDetail.getUPC()); // Item UPC 7 
				pw.print("|");
				if(movementDetail.getUOM_QTY() > 0 && "Y".equals(movementDetail.getWGH_FG())){
					pw.print(PrestoUtil.round(movementDetail.getEXTD_PRC()/ movementDetail.getUOM_QTY(), 2)); // Item Net Price 8	
				}else{
					pw.print(PrestoUtil.round(movementDetail.getEXTD_PRC()/ movementDetail.getPKG_QTY(), 2)); // Item Net Price 8
				}
				pw.print("|");
				pw.print(movementDetail.getPRC_PNT()); // Item Gross Price 9
				pw.print("|");
				pw.print(movementDetail.getPOS_DPT_CD()); // POS Department 10
				pw.print("|");
				pw.print(Constants.EMPTY); // Current Coupon Family Code 11
				pw.print("|");
				pw.print(Constants.EMPTY); // Previous Coupon Family Code 12
				pw.print("|");
				pw.print(Constants.EMPTY); // Multi Price Group 13
				pw.print("|");
				pw.print(ZERO); // Deal Qty 14
				pw.print("|");
				pw.print(ZERO); // Price Method 15 
				pw.print("|");
				pw.print(ZERO); // Sale Qty 16
				pw.print("|");
				pw.print(Constants.EMPTY); // Sale Price 17
				pw.print("|");
				pw.print((movementDetail.getUOM_QTY() > 0 && "Y".equals(movementDetail.getWGH_FG())) ? ZERO:movementDetail.getPKG_QTY()); // Extn Qty 18
				pw.print("|");
				pw.print(PrestoUtil.round(movementDetail.getUOM_QTY(), 2)); // Extn Weight 19
				pw.print("|");
				pw.print((movementDetail.getWGH_FG() == null) ? "" : movementDetail.getWGH_FG()); // Weighted Flag 20
				pw.print("|");
				pw.print(PrestoUtil.round(movementDetail.getPKG_QTY(), 2)); // Weighted Count 21
				pw.print("|");
				pw.print(""); // Coupon Used 22
				pw.print("|");
				pw.print(PrestoUtil.round(movementDetail.getEXTD_PRC(), 2)); // Extn Net Price 23
				pw.print("|");
				pw.print(ZERO); // Extn Profit 24
				pw.print("|");
				pw.print(ZERO); // Unit Cost 25
				pw.print("|");
				pw.print(ZERO); // Movement Type 26
				pw.print("|");
				pw.print(ZERO); // Default Cost Used 27 
				pw.print("|");
				pw.print(Constants.EMPTY); // Percent Used 28
				pw.print("|"); 
				pw.print(Constants.EMPTY); // Unit Cost Gross 29
				pw.print("|");
				pw.print((movementDetail.getREG_AMT()> 0)? PrestoUtil.round(movementDetail.getREG_AMT(), 2): PrestoUtil.round(movementDetail.getEXTD_PRC(), 2));// Extended Gross Price 30	
				pw.print("|");
				pw.print(Constants.EMPTY); // Count on Deal 31
				pw.print("|");
				pw.print((!Constants.EMPTY.equals(movementDetail.getMISC_AMT()) && movementDetail.getMISC_AMT() !=0)? PrestoUtil.round(movementDetail.getMISC_AMT(), 2): Constants.EMPTY); // Misc Fund Amount 32
				pw.print("|");
				pw.print(Constants.EMPTY); // Misc Fund Count 33
				pw.print("|");
				//IA(In Ad Display) and EL(Electronic) is the store coupon representation. 
				pw.print((movementDetail.getREG_AMT()> 0) && (movementDetail.getCPN_TYP_CD().trim().toUpperCase().equals("IA") ||
						movementDetail.getCPN_TYP_CD().trim().toUpperCase().equals("EL"))? Constants.YES : Constants.NO); // Store Coupon Used 34
				pw.print("|");
				pw.print((!Constants.EMPTY.equals(movementDetail.getMISC_AMT()) && movementDetail.getMISC_AMT() !=0)? Constants.YES : Constants.NO); // Manufacturer Coupon Used 35
				pw.print("|");
				pw.println("       "); // spaces
				
				if(recordCount % Constants.LOG_RECORD_COUNT == 0){
					logger.info("No of records processed - " + recordCount);
				}
			}else{
				noOfReturnCount++;
				skippedReturnRecList.add(key);
			}
			
		});
	}
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}
	/**
	 * To generate Key based on DATE TIME, STORE NO, BASE STORE NO, LOYALTY CARD NO, TRML NO, TX NO, UPC
	 * @author Dinesh Ramanathan
	 *
	 */
	private class TransactionLogKey{
		private String TX_DTE_TME;
		private String STR_NO;
		private String BS_STR_NO;
		private String 	LOYAL_CARD_NO;
		private String TRML_NO;
		private String TX_NO;
		private String UPC;
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((BS_STR_NO == null) ? 0 : BS_STR_NO.hashCode());
			result = prime * result + ((LOYAL_CARD_NO == null) ? 0 : LOYAL_CARD_NO.hashCode());
			result = prime * result + ((STR_NO == null) ? 0 : STR_NO.hashCode());
			result = prime * result + ((TRML_NO == null) ? 0 : TRML_NO.hashCode());
			result = prime * result + ((TX_DTE_TME == null) ? 0 : TX_DTE_TME.hashCode());
			result = prime * result + ((TX_NO == null) ? 0 : TX_NO.hashCode());
			result = prime * result + ((UPC == null) ? 0 : UPC.hashCode());
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
			TransactionLogKey other = (TransactionLogKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (BS_STR_NO == null) {
				if (other.BS_STR_NO != null)
					return false;
			} else if (!BS_STR_NO.equals(other.BS_STR_NO))
				return false;
			if (LOYAL_CARD_NO == null) {
				if (other.LOYAL_CARD_NO != null)
					return false;
			} else if (!LOYAL_CARD_NO.equals(other.LOYAL_CARD_NO))
				return false;
			if (STR_NO == null) {
				if (other.STR_NO != null)
					return false;
			} else if (!STR_NO.equals(other.STR_NO))
				return false;
			if (TRML_NO == null) {
				if (other.TRML_NO != null)
					return false;
			} else if (!TRML_NO.equals(other.TRML_NO))
				return false;
			if (TX_DTE_TME == null) {
				if (other.TX_DTE_TME != null)
					return false;
			} else if (!TX_DTE_TME.equals(other.TX_DTE_TME))
				return false;
			if (TX_NO == null) {
				if (other.TX_NO != null)
					return false;
			} else if (!TX_NO.equals(other.TX_NO))
				return false;
			if (UPC == null) {
				if (other.UPC != null)
					return false;
			} else if (!UPC.equals(other.UPC))
				return false;
			return true;
		}

		public TransactionLogKey(String tX_DTE_TME, String sTR_NO, String bS_STR_NO, String lOYAL_CARD_NO,
				String tRML_NO, String tX_NO, String uPC) {
			this.TX_DTE_TME = tX_DTE_TME;
			this.STR_NO = sTR_NO;
			this.BS_STR_NO = bS_STR_NO;
			this.LOYAL_CARD_NO = lOYAL_CARD_NO;
			this.TRML_NO = tRML_NO;
			this.TX_NO = tX_NO;
			this.UPC = uPC;
		}
		private MovementFileFormatter getOuterType() {
			return MovementFileFormatter.this;
		}
		
	}
	
	
	/**
	 * To generate Key based on DATE TIME, STORE NO, BASE STORE NO, LOYALTY CARD NO, TRML NO, TX NO, UPC
	 * @author Dinesh Ramanathan
	 *
	 */
	private class CouponRecordKey{
		private String TX_DTE_TME;
		private String STR_NO;
		private String CPN_TYP_CD;
		private String 	LOYAL_CARD_NO;
		private String TRML_NO;
		private String TX_NO;
		private String UPC;
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((CPN_TYP_CD == null) ? 0 : CPN_TYP_CD.hashCode());
			result = prime * result + ((LOYAL_CARD_NO == null) ? 0 : LOYAL_CARD_NO.hashCode());
			result = prime * result + ((STR_NO == null) ? 0 : STR_NO.hashCode());
			result = prime * result + ((TRML_NO == null) ? 0 : TRML_NO.hashCode());
			result = prime * result + ((TX_DTE_TME == null) ? 0 : TX_DTE_TME.hashCode());
			result = prime * result + ((TX_NO == null) ? 0 : TX_NO.hashCode());
			result = prime * result + ((UPC == null) ? 0 : UPC.hashCode());
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
			CouponRecordKey other = (CouponRecordKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (CPN_TYP_CD == null) {
				if (other.CPN_TYP_CD != null)
					return false;
			} else if (!CPN_TYP_CD.equals(other.CPN_TYP_CD))
				return false;
			if (LOYAL_CARD_NO == null) {
				if (other.LOYAL_CARD_NO != null)
					return false;
			} else if (!LOYAL_CARD_NO.equals(other.LOYAL_CARD_NO))
				return false;
			if (STR_NO == null) {
				if (other.STR_NO != null)
					return false;
			} else if (!STR_NO.equals(other.STR_NO))
				return false;
			if (TRML_NO == null) {
				if (other.TRML_NO != null)
					return false;
			} else if (!TRML_NO.equals(other.TRML_NO))
				return false;
			if (TX_DTE_TME == null) {
				if (other.TX_DTE_TME != null)
					return false;
			} else if (!TX_DTE_TME.equals(other.TX_DTE_TME))
				return false;
			if (TX_NO == null) {
				if (other.TX_NO != null)
					return false;
			} else if (!TX_NO.equals(other.TX_NO))
				return false;
			if (UPC == null) {
				if (other.UPC != null)
					return false;
			} else if (!UPC.equals(other.UPC))
				return false;
			return true;
		}

		public CouponRecordKey(String tX_DTE_TME, String sTR_NO, String cPN_TYP_CD, String lOYAL_CARD_NO,
				String tRML_NO, String tX_NO, String uPC) {
			this.TX_DTE_TME = tX_DTE_TME;
			this.STR_NO = sTR_NO;
			this.CPN_TYP_CD = cPN_TYP_CD;
			this.LOYAL_CARD_NO = lOYAL_CARD_NO;
			this.TRML_NO = tRML_NO;
			this.TX_NO = tX_NO;
			this.UPC = uPC;
		}
		private MovementFileFormatter getOuterType() {
			return MovementFileFormatter.this;
		}
		
	}
	private HashMap<TransactionLogKey,GiantEagleTlogDTO> aggrRegRecWithCouponRecord() throws GeneralException, Exception{
		HashMap<TransactionLogKey, GiantEagleTlogDTO> aggrMovementMap = new HashMap<TransactionLogKey, GiantEagleTlogDTO>();
		List<TransactionLogKey> processedKeyList = new ArrayList<TransactionLogKey>();
		
		//Aggregate Movement details if count of records were matching
		regularRecordsMap.forEach((key, value) ->{
			isRecordExits = false;
			if(value !=null){
				if(regularItemCountMap.get(key)== value.size()){
					processedKeyList.add(key);
					aggrTlogDTO = new GiantEagleTlogDTO();
					if(aggrMovementMap.containsKey(key)){
						aggrTlogDTO = aggrMovementMap.get(key);
						isRecordExits = true;
					}	
					value.stream().forEach(movementDTO -> {
						if(isRecordExits){
							aggrTlogDTO.setEXTD_PRC(aggrTlogDTO.getEXTD_PRC() + movementDTO.getEXTD_PRC());
							aggrTlogDTO.setPKG_QTY(aggrTlogDTO.getPKG_QTY() + movementDTO.getPKG_QTY());
							aggrTlogDTO.setUOM_QTY(aggrTlogDTO.getUOM_QTY() + movementDTO.getUOM_QTY());
							
						}else{
							aggrTlogDTO = movementDTO;
							isRecordExits = true;
						}
						
					});
					aggrMovementMap.put(key, aggrTlogDTO);
					aggrTlogDTO = new GiantEagleTlogDTO();
					
				}
			}else{
				TransactionLogKey transactionLogKey = key;
				logger.error("List of values is null for the UPC: "+transactionLogKey.UPC+" store No: "+transactionLogKey.BS_STR_NO+" transaction time: "+transactionLogKey.TX_DTE_TME
						+" Terminal No: "+transactionLogKey.TRML_NO);
			}
			
		});
		
		aggrMovementMap.forEach((key, aggrMovement)-> {
			if(couponRecordsMap.containsKey(key)){
				
				List<GiantEagleTlogDTO> couponDetailList = couponRecordsMap.get(key);
				//If coupon type is Manufacture type then assign MISC Amt using Coupon Amt
				couponDetailList.forEach(couponDetail -> {
					if((Constants.MANUFACTURE_COUPON_TYPE).toUpperCase().equals(couponDetail.getCPN_TYP_CD().trim().toUpperCase())){
						aggrMovement.setMISC_AMT(aggrMovement.getMISC_AMT()+couponDetail.getEXTD_PRC());
						aggrMovement.setCPN_TYP_CD(couponDetail.getCPN_TYP_CD().trim().toUpperCase());
					}
					//Other than MF coupon type, Apply MISC AMT and reduce the coupon amount from Extd Amt.
					else{
						// Set coupon Type
						aggrMovement.setCPN_TYP_CD(couponDetail.getCPN_TYP_CD().trim().toUpperCase());
						//Set REG amt from extended amount before deducting coupon amt
						if(aggrMovement.getREG_AMT() == 0){
							aggrMovement.setREG_AMT(aggrMovement.getEXTD_PRC());
						}
						//Subtract Discount amt from actual amt
						aggrMovement.setEXTD_PRC(aggrMovement.getEXTD_PRC()+ couponDetail.getEXTD_PRC());
					}
				});
				
			}
		});
		if(processedKeyList.size()>0){
			processedKeyList.forEach(removeKey -> {
					regularRecordsMap.remove(removeKey);
				
			});
		}
		return aggrMovementMap;
	}
	
	/**
	 * To generate Coupon detail in text file
	 * @throws IOException
	 */
	private void generateCouponRecFile() throws GeneralException,IOException {

		if (!ignoreCouponFile && couponRecWithoutLinkUPC.size() > 0) {
			try {
				HashMap<CouponRecordKey, GiantEagleTlogDTO> aggrCouponRecMap = new HashMap<CouponRecordKey, GiantEagleTlogDTO>();

				// To aggregate Coupon record based on the Key
				couponRecWithoutLinkUPC.stream().forEach(couponDetail -> {
					CouponRecordKey key = new CouponRecordKey(couponDetail.getTX_DTE_TME(), couponDetail.getSTR_NO(),
							couponDetail.getCPN_TYP_CD(), couponDetail.getLOYAL_CARD_NO(),
							couponDetail.getTRML_NO(), couponDetail.getTX_NO(), couponDetail.getPARENT_UPC());
					GiantEagleTlogDTO giantEagleTlogDTO = new GiantEagleTlogDTO();
					isRecordExits = false;
					if (aggrCouponRecMap.containsKey(key)) {
						giantEagleTlogDTO = aggrCouponRecMap.get(key);
						isRecordExits = true;
					}
					if (isRecordExits) {
						giantEagleTlogDTO.setPRC_PNT(giantEagleTlogDTO.getPRC_PNT() + couponDetail.getPRC_PNT());
						giantEagleTlogDTO.setPKG_QTY(giantEagleTlogDTO.getPKG_QTY() + couponDetail.getPKG_QTY());
						giantEagleTlogDTO.setUOM_QTY(giantEagleTlogDTO.getUOM_QTY() + couponDetail.getUOM_QTY());
					} else {
						giantEagleTlogDTO = couponDetail;
						isRecordExits = true;
					}
					aggrCouponRecMap.put(key, giantEagleTlogDTO);

				});
				MovementDAO movementDAO = new MovementDAO();
				HashMap<String, String> couponIdMap = movementDAO.getCouponId(conn);
				SimpleDateFormat df = new SimpleDateFormat("ddMMyyyy-HHmm");
				Date currentDate = new Date();
				if (couponOutputPath != null) {
					String outputPath = rootPath + "/" + couponOutputPath + "/" + "CouponRecords-"
							+ df.format(currentDate) + ".txt";
					File file = new File(outputPath);
					if (!file.exists())
						fw1 = new FileWriter(outputPath);
					else
						fw1 = new FileWriter(outputPath, true);

					pw1 = new PrintWriter(fw1);
					aggrCouponRecMap.forEach((key, couponRecords) -> {
						String date = couponRecords.getTX_DTE_TME();
						String itemDate = null;
						String itemTime = null;
						try {
							itemDate = DateUtil.dateToString(DateUtil.toDate(date, "yyyyMMdd HH:mm"), "yyyyMMdd");
							itemTime = DateUtil.dateToString(DateUtil.toDate(date, "yyyyMMdd HH:mm"), "HHmm");
						} catch (GeneralException exception) {
							exception.printStackTrace();
						}
						pw1.print("0"); // company
						pw1.print("|");
						pw1.print(couponRecords.getSTR_NO()); // Store No
						pw1.print("|");
						pw1.print(couponRecords.getTRML_NO()); // terminal
						pw1.print("|");
						pw1.print(couponRecords.getTX_NO());// transactionNo
						pw1.print("|");
						pw1.print(couponRecords.getOPRT_NO());// operator
						pw1.print("|");
						pw1.print(itemDate);// cpnDate
						pw1.print("|");
						pw1.print(itemTime);// cpnTime
						pw1.print("|");
						pw1.print(couponRecords.getLOYAL_CARD_NO()); // customerId
						pw1.print("|");
						pw1.print(Constants.EMPTY);// couponNumber
						pw1.print("|");
						pw1.print(Long.toString((long) (Double.valueOf(couponRecords.getPARENT_UPC()).longValue())));// itemUPC
						pw1.print("|");
						pw1.print(PrestoUtil.round(couponRecords.getPRC_PNT(), 2)); // Item Net Price
						pw1.print("|");
						pw1.print(couponRecords.getPOS_DPT_CD());// posDept
						pw1.print("|");
						pw1.print(Constants.EMPTY);// cpnFamilyCurr
						pw1.print("|");
						pw1.print(Constants.EMPTY);// cpnFamilyPrev
						pw1.print("|");
						pw1.print(Constants.EMPTY);// cpnMfgNbr
						pw1.print("|");
						pw1.print(couponRecords.getPKG_QTY());// strCpnQty
						pw1.print("|");
						pw1.print(PrestoUtil.round(couponRecords.getUOM_QTY(), 2));// strCpnWeight
						pw1.print("|");
						pw1.print(Constants.EMPTY);// cpnCnt
						pw1.print("|");
						pw1.print(Constants.EMPTY);// discSalePrcAmt
						pw1.print("|");
						pw1.print(Constants.EMPTY);// discTaxExempt
						pw1.print("|");
						pw1.print(Constants.EMPTY);// discSalePrice
						pw1.print("|");
						pw1.print(Constants.EMPTY);// discQty
						pw1.print("|");
						pw1.print(Constants.EMPTY);// cpnDiscGrp
						pw1.print("|");
						pw1.print(Constants.EMPTY);// cpnDiscPct
						pw1.print("|");
						pw1.print(couponIdMap.get(couponRecords.getCPN_TYP_CD().trim().toUpperCase()));// couponType id
						pw1.print("|");
						pw1.print(Constants.EMPTY);// cpnNotUsed
						pw1.print("|");
						pw1.print(Constants.EMPTY);// cpnMult
						pw1.print("|");
						pw1.println("       "); // spaces
					});

					pw1.flush();
					fw1.flush();

				} else {
					logger.error("Coupon output file path were not given");
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				pw1.close();
				fw1.close();
			}

		}
		couponRecWithoutLinkUPC.clear();
	}
}