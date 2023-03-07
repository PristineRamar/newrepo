package com.pristine.fileformatter.gianteagle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import com.pristine.dto.fileformatter.gianteagle.GiantEaglePriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;
/**
 * 
 * @author Pradeepkumar
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class PriceFileFormatter extends PristineFileParser{
	private static Logger logger = Logger.getLogger("PriceFileFormatterGE");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	Connection conn = null;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private Set<String> skippedRetailerItemcodes = null;
	List<GiantEaglePriceDTO> priceList = null;
	public static final int LOG_RECORD_COUNT = 25000;
	private String PROMO_PRICE = "PPR";
	private String FUTURE = "F";
	private String PROMO = "P";
	public PriceFileFormatter() {
		super();
		super.headerPresent = true;
		conn = getOracleConnection();
	}
	
	public static void main(String[] args) {
		String inFolder = null;
		String outFolder = null;
		PropertyConfigurator.configure("log4j-ge-price-formatter.properties");
		PriceFileFormatter priceFileFormatter = new PriceFileFormatter();
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				inFolder = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				outFolder = arg.substring(OUTPUT_FOLDER.length());
			}
		}

		logger.info("********************************************");
		priceFileFormatter.processPriceFile(inFolder, outFolder);
		logger.info("********************************************");
	}
	
	/**
	 * 
	 * @param inFolder
	 */
	private void processPriceFile(String inFolder, String outFolder) {
		try {
			parseFile(inFolder, outFolder);
			logSkippedRecords();
		} catch (IOException | GeneralException e) {
			logger.error("Error -- processPriceFile() ", e);
		}
	}
	
	
	/**
	 * Parses given file formats to Pristine standard price file format
	 * @param inFolder
	 * @param outFolder
	 * @throws GeneralException
	 * @throws IOException
	 */
	private void parseFile(String inFolder, 
 String outFolder) throws GeneralException, IOException {

		try {
			skippedRetailerItemcodes = new HashSet<>();
			priceList = new ArrayList<>();
			fillAllColumns();
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
						String files = fileList.get(i);
						logger.info("File Name - " + files);
						int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));

						String outputFileName[] = files.split("/");
						logger.info("Output File name - " + outputFileName[outputFileName.length - 1]);

						String outputPath = getRootPath() + "/" + outFolder + "/"
								+ outputFileName[outputFileName.length - 1];

						File file = new File(outputPath);
						if (!file.exists())
							fw = new FileWriter(outputPath);
						else
							fw = new FileWriter(outputPath, true);

						pw = new PrintWriter(fw);

						logger.info("Processing Retail Records ...");
						super.parseDelimitedFile(GiantEaglePriceDTO.class, fileList.get(i), '|', fieldNames, stopCount);
						processCachedList();
						priceList = new ArrayList<>();
						pw.flush();
						fw.flush();
						pw.close();
						fw.close();
						long fileProcessEndTime = System.currentTimeMillis();
						logger.info("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime)
								+ "ms");
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
		priceList.addAll((List<GiantEaglePriceDTO>) listobj);
		logger.info("# of records cached - " + priceList.size());
	}
	
	/**
	 * Processes all the cached rows and generates formatted file 
	 * @throws GeneralException
	 * @throws CloneNotSupportedException 
	 */
	private void processCachedList() throws GeneralException, CloneNotSupportedException{
		ItemDAO itemDAO = new ItemDAO();
		String separator = "|";
		Set<String> retItemcodeSet = new HashSet<>();
		for(GiantEaglePriceDTO giantEaglePriceDTO: priceList){
			retItemcodeSet.add(giantEaglePriceDTO.getRITEM_NO());
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
		
		List<GiantEaglePriceDTO> finalList = identifySaleAndRegRowsAndGroup(priceList);
		
		int recordCount = 0;
		for(GiantEaglePriceDTO giantEaglePriceDTO: finalList){
			recordCount++;
			if(upcMap.get(giantEaglePriceDTO.getRITEM_NO()) == null){
				skippedRetailerItemcodes.add(giantEaglePriceDTO.getRITEM_NO());
			}
			else{
				List<String> upcs = upcMap.get(giantEaglePriceDTO.getRITEM_NO());
				for(String upc: upcs){
					String zoneNum = giantEaglePriceDTO.getBNR_CD() + "-" + giantEaglePriceDTO.getZN_NO() 
							+ "-" + giantEaglePriceDTO.getPRC_GRP_CD();
					//Append vendor number to zone if it is not blank
					if(!Constants.EMPTY.equals(giantEaglePriceDTO.getSPLR_NO().trim()) 
							&& giantEaglePriceDTO.getSPLR_NO() != null){
						zoneNum = zoneNum + "-" + giantEaglePriceDTO.getSPLR_NO();
					}
					pw.print(upc); // UPC #1
					pw.print(separator);
					pw.print(giantEaglePriceDTO.getRITEM_NO()); //Retailer item code #2
					pw.print(separator);
					pw.print("1"); // Level type #3
					pw.print(separator);
					pw.print(zoneNum); //Zone or Store num #4
					pw.print(separator);
					pw.print(giantEaglePriceDTO.getPRC_STRT_DTE()); //Effective date #5
					pw.print(separator);
					pw.print(giantEaglePriceDTO.getCURR_PRC()); //Current reg Price #6
					pw.print(separator);
					pw.print(giantEaglePriceDTO.getMUNIT_CNT()); //Current reg multiple #7
					pw.print(separator);
					pw.print(giantEaglePriceDTO.getSaleStartDate() ==  null ? 
							Constants.EMPTY : giantEaglePriceDTO.getSaleStartDate());//Sale start date #8
					pw.print(separator);
					pw.print(giantEaglePriceDTO.getSaleEndDate() ==  null ? 
							Constants.EMPTY : giantEaglePriceDTO.getSaleEndDate());//Sale End date #9
					pw.print(separator);
					pw.print(giantEaglePriceDTO.getSalePrice() > 0 ? 
							PrestoUtil.round(giantEaglePriceDTO.getSalePrice(), 2) : Constants.EMPTY);//Sale retail #10
					pw.print(separator);
					pw.print(giantEaglePriceDTO.getSaleQty() > 0 ? 
							giantEaglePriceDTO.getSaleQty() : Constants.EMPTY);//Sale retail qty #11
					pw.print(separator);
					pw.print(giantEaglePriceDTO.getPRC_GRP_CD());//Prc Grp code #12
					pw.println("       ");
					
				}
			}
			if(recordCount % LOG_RECORD_COUNT == 0){
				logger.info("processRecords() Processed - " + recordCount);
			}
		}
	}
	
	/**
	 * Identifies sale and regular price rows item by item and appends sale price in regular rows
	 * @param priceList
	 * @return
	 * @throws GeneralException 
	 * @throws CloneNotSupportedException 
	 */
	public List<GiantEaglePriceDTO> identifySaleAndRegRowsAndGroup(List<GiantEaglePriceDTO> priceList) throws GeneralException, CloneNotSupportedException {
		List<GiantEaglePriceDTO> outList = new ArrayList<>();
		HashMap<String, List<GiantEaglePriceDTO>> itemGroups = new HashMap<>();
		// Group items by retailer item code
		for (GiantEaglePriceDTO giantEaglePriceDTO : priceList) {
			if (itemGroups.get(giantEaglePriceDTO.getRITEM_NO()) == null) {
				List<GiantEaglePriceDTO> tempList = new ArrayList<>();
				tempList.add(giantEaglePriceDTO);
				itemGroups.put(giantEaglePriceDTO.getRITEM_NO(), tempList);
			} else {
				List<GiantEaglePriceDTO> tempList = itemGroups.get(giantEaglePriceDTO.getRITEM_NO());
				tempList.add(giantEaglePriceDTO);
				itemGroups.put(giantEaglePriceDTO.getRITEM_NO(), tempList);
			}
		}

		// Identify sale and regular rows and group
		for (Map.Entry<String, List<GiantEaglePriceDTO>> entry : itemGroups.entrySet()) {
			HashMap<String, List<GiantEaglePriceDTO>> salePriceMap = new HashMap<>();
			HashMap<String, List<GiantEaglePriceDTO>> regPriceMap = new HashMap<>();

			for (GiantEaglePriceDTO giantEaglePriceDTO : entry.getValue()) {
				// Key is the zone number. Zone number + Price group code
				// becomes actual zone number..
				String key = giantEaglePriceDTO.getBNR_CD() + "-" + giantEaglePriceDTO.getZN_NO() + "-" + giantEaglePriceDTO.getPRC_GRP_CD();
				//Append vendor number to zone if it is not blank
				if(!Constants.EMPTY.equals(giantEaglePriceDTO.getSPLR_NO()) 
						&& giantEaglePriceDTO.getSPLR_NO() != null){
					key = key + "-" + giantEaglePriceDTO.getSPLR_NO();
				}
				if (giantEaglePriceDTO.getPRC_TYP_IND().equals(PROMO_PRICE)) {
					// Group all sale prices.
					groupPrices(salePriceMap, key, giantEaglePriceDTO);
				} else {
					// Group all regular prices.
					groupPrices(regPriceMap, key, giantEaglePriceDTO);
				}
			}

			for (Map.Entry<String, List<GiantEaglePriceDTO>> regEntry : regPriceMap.entrySet()) {
				List<GiantEaglePriceDTO> salePrices = salePriceMap.get(regEntry.getKey());
				// Loop regular price rows and find corresponding sale price if
				// any
				List<GiantEaglePriceDTO> regPrices = regEntry.getValue();
				Collections.sort(regPrices, new Comparator<GiantEaglePriceDTO>() {
					@Override
					public int compare(final GiantEaglePriceDTO left, final GiantEaglePriceDTO right) {
						int c = 0;
						try{
							Date leftStart = DateUtil.toDate(left.getPRC_STRT_DTE());
							Date rightStart = DateUtil.toDate(right.getPRC_STRT_DTE());
							
							c = rightStart.compareTo(leftStart);
						}catch(GeneralException e){
							logger.info("Error while parsing", e);
						}
						return c;
					}
					});
					for (GiantEaglePriceDTO regPriceDTO : regPrices) {
						if (salePrices != null) {
							for (GiantEaglePriceDTO salePriceDTO : salePrices) {
								
								 if (salePriceDTO.getAD_LOCN_DSCR() != null || !salePriceDTO.getAD_LOCN_DSCR().isEmpty()) {
									 regPriceDTO.setAD_LOCN_DSCR(salePriceDTO.getAD_LOCN_DSCR());
								 }
									// Match price using vendor no.
									// If there is amount off in regular, subtract
									// from it from regular and put it as Sale price
									//Update sale price if both reg and sale are in FUTURE
									if(salePriceDTO.getPRC_STAT_CD().equals(FUTURE)
											&& regPriceDTO.getPRC_STAT_CD().equals(FUTURE)){
										if(!salePriceDTO.isProcessed()){
											Date regEffDate = DateUtil.toDate(regPriceDTO.getPRC_STRT_DTE());
											Date saleStartDate = DateUtil.toDate(salePriceDTO.getPRC_STRT_DTE());
											Date saleEndDate = DateUtil.toDate(salePriceDTO.getPRC_END_DTE());
											//Check reg price is falling sale start and end date range,
											//If so update sale price
											if((regEffDate.equals(saleStartDate) || regEffDate.after(saleStartDate))
													&& (regEffDate.equals(saleEndDate) || regEffDate.before(saleEndDate))){
												computeAndSetSalePrice(regPriceDTO, salePriceDTO);
												salePriceDTO.setProcessed(true);	
											}
											
										}
									}else if(salePriceDTO.getPRC_STAT_CD().equals(FUTURE)){
										if(!salePriceDTO.isProcessed()){
											Date regEffDate = DateUtil.toDate(regPriceDTO.getPRC_STRT_DTE());
											Date saleStartDate = DateUtil.toDate(salePriceDTO.getPRC_STRT_DTE());
											//Date saleEndDate = DateUtil.toDate(salePriceDTO.getPRC_END_DTE());
											//Check reg price is falling sale start and end date range,
											//If so update sale price
											if(regEffDate.before(saleStartDate)){
												GiantEaglePriceDTO regPriceDTONew = (GiantEaglePriceDTO) regPriceDTO.clone();
												outList.add(regPriceDTONew);
												computeAndSetSalePrice(regPriceDTONew, salePriceDTO);
												salePriceDTO.setProcessed(true);	
											}
										}
									}else if(salePriceDTO.getPRC_TYP_IND().equals(PROMO_PRICE)
											&& salePriceDTO.getPRC_STAT_CD().equals(PROMO)
											&& !regPriceDTO.getPRC_STAT_CD().equals(FUTURE)){
										//If status code is promo, directly sale price can be applied.
										if(!salePriceDTO.isProcessed()){
											computeAndSetSalePrice(regPriceDTO, salePriceDTO);
											salePriceDTO.setProcessed(true);
										}
									}
								}
							}
						outList.add(regPriceDTO);
					}
				}
			
				for(Map.Entry<String, List<GiantEaglePriceDTO>> saleEntry: salePriceMap.entrySet()){
					if(regPriceMap.get(saleEntry.getKey()) == null){
						GiantEaglePriceDTO giantEaglePriceDTO = saleEntry.getValue().get(0);
						logger.warn("identifySaleAndRegRowsAndGroup() - "
								+ "Regular price not found for Item code -> " 
								+ giantEaglePriceDTO.getRITEM_NO() + ",  & Zone -> " + saleEntry.getKey());
					}
				}
			}
		return outList;
	}
	
	private void computeAndSetSalePrice(GiantEaglePriceDTO regPriceDTO, GiantEaglePriceDTO salePriceDTO){
		if (salePriceDTO.getPROM_AMT_OFF() > 0) {
			double regUnitPrice = regPriceDTO.getCURR_PRC() / regPriceDTO.getMUNIT_CNT();
			double salePrice = regUnitPrice - salePriceDTO.getPROM_AMT_OFF();
			regPriceDTO.setSalePrice(salePrice);
			regPriceDTO.setSaleQty(1);
			regPriceDTO.setSaleStartDate(salePriceDTO.getPRC_STRT_DTE());
			regPriceDTO.setSaleEndDate(salePriceDTO.getPRC_END_DTE());
		}
		// If there is promo % in regular, Identify the
		// sale price using given % from the regular.
		if (salePriceDTO.getPROM_PCT() > 0) {
			double regUnitPrice = regPriceDTO.getCURR_PRC() / regPriceDTO.getMUNIT_CNT();
			double salePrice = regUnitPrice * (1-(salePriceDTO.getPROM_PCT() / 100));
			regPriceDTO.setSalePrice(salePrice);
			regPriceDTO.setSaleQty(1);
			regPriceDTO.setSaleStartDate(salePriceDTO.getPRC_STRT_DTE());
			regPriceDTO.setSaleEndDate(salePriceDTO.getPRC_END_DTE());
		}
		// If Sale price is given directly, apply it in
		// regular price row
		if (salePriceDTO.getCURR_PRC() > 0) {
			regPriceDTO.setSalePrice(salePriceDTO.getCURR_PRC());
			regPriceDTO.setSaleQty(salePriceDTO.getMUNIT_CNT());
			regPriceDTO.setSaleStartDate(salePriceDTO.getPRC_STRT_DTE());
			regPriceDTO.setSaleEndDate(salePriceDTO.getPRC_END_DTE());
		}
	}
	
	/**
	 * Adds object to given map with respect to key
	 * @param priceMap
	 * @param key
	 * @param giantEaglePriceDTO
	 */
	private void groupPrices(HashMap<String, List<GiantEaglePriceDTO>> priceMap,
			String key, GiantEaglePriceDTO giantEaglePriceDTO){
		if(priceMap.get(key) == null){
			List<GiantEaglePriceDTO> tempList = new ArrayList<>();
			tempList.add(giantEaglePriceDTO);
			priceMap.put(key, tempList);
		}
		else{
			List<GiantEaglePriceDTO> tempList = priceMap.get(key);
			tempList.add(giantEaglePriceDTO);
			priceMap.put(key, tempList);
		}
	}
	
	/**
	 * Logs skipped records
	 */
	private void logSkippedRecords(){
		if(skippedRetailerItemcodes.size() > 0){
			StringBuilder sb = new StringBuilder();
			for(String itemCode: skippedRetailerItemcodes){
				sb.append(itemCode + ", ");
			}
			logger.warn("Skipped retailer item codes -> " + sb.toString());
		}
	}
	
	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns() {
		allColumns.put(1, "RITEM_NO");
		allColumns.put(2, "PRC_STRT_DTE");
		allColumns.put(3, "PRC_END_DTE");		
		allColumns.put(4, "CURR_PRC");
		allColumns.put(5, "MUNIT_CNT");
		allColumns.put(6, "PRC_STAT_CD");
		allColumns.put(7, "PROM_CD");
		allColumns.put(8, "PROM_PCT");
		allColumns.put(9, "PROM_AMT_OFF");
		allColumns.put(10, "ZN_NO");
		allColumns.put(11, "PRC_GRP_CD");
		allColumns.put(12, "SPLR_NO");
		allColumns.put(13, "PRC_TYP_IND");
		allColumns.put(14, "DEAL_ID");
		allColumns.put(15, "OFFER_ID");
		allColumns.put(16, "OFFER_DSCR");
		allColumns.put(17, "AD_TYP_DSCR");
		allColumns.put(18, "AD_LOCN_DSCR");
		allColumns.put(19, "PCT_OF_PGE");
		allColumns.put(20, "BNR_CD");
	}

}
