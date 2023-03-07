
package com.pristine.fileformatter.gianteagle;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEaglePriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class PromoFileFormatter
extends PristineFileParser {
    private static Logger logger = Logger.getLogger((String)"PromoFileFormatterGE");
    private static final String INPUT_FOLDER = "INPUT_FOLDER=";
    private static final String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private static String startDate = null;
    static TreeMap<Integer, String> allColumns = new TreeMap();
    private FileWriter fw = null;
    private PrintWriter pw = null;
    private Set<String> skippedRetailerItemcodes = null;
    List<GiantEaglePriceDTO> priceList = null;
    public static final int LOG_RECORD_COUNT = 100000;
    private String PROMO_PRICE = "PPR";
    private String REG_PRICE = "RRG";
    private String REG_PRICE1 = "REG";
    Connection conn = null;
    private List<String> regPriceNotAvailable = new ArrayList<String>();

    public PromoFileFormatter() {
        super("analysis.properties");
        headerPresent = true;
        try {
            conn = DBManager.getConnection();
        }
        catch (GeneralException var1_1) {
            // empty catch block
        }
    }

    public static void main(String[] args) {
        String inFolder = null;
        String outFolder = null;
        PropertyConfigurator.configure("log4j-ge-promo-file-formatter.properties");
        PropertyManager.initialize("analysis.properties");
        PromoFileFormatter priceFileFormatter = new PromoFileFormatter();
        String[] arrstring = args;
        int n = arrstring.length;
        int n2 = 0;
        for(String arg:args){
            if (arg.startsWith(INPUT_FOLDER)) {
                inFolder = arg.substring(INPUT_FOLDER.length());
            }
            if (arg.startsWith(OUTPUT_FOLDER)) {
                outFolder = arg.substring(OUTPUT_FOLDER.length());
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
     		DateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
     		if (Constants.CURRENT_WEEK.equalsIgnoreCase(args[2])) {
     			startDate = dateFormat.format(c.getTime());
     		} else if (Constants.NEXT_WEEK.equalsIgnoreCase(args[2])) {
     			c.add(Calendar.DATE, 7);
     			startDate = dateFormat.format(c.getTime());
     		} else if (Constants.LAST_WEEK.equalsIgnoreCase(args[2])) {
     			c.add(Calendar.DATE, -7);
     			startDate = dateFormat.format(c.getTime());
     		} else if (Constants.SPECIFIC_WEEK.equalsIgnoreCase(args[2])) {
     			try {
     				String tempStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(args[3]), 0);
     				Date date = dateFormat.parse(tempStartDate);
     				startDate = dateFormat.format(date);
     			} catch (GeneralException | ParseException exception) {
     				logger.error("Error when parsing date - ", exception);
     				System.exit(-1);
     			}
     		}
        logger.info("********************************************");
        priceFileFormatter.processPriceFile(inFolder, outFolder);
        logger.info("********************************************");
    }

    private void processPriceFile(String inFolder, String outFolder) {
        try {
            parseFile(inFolder, outFolder);
            logSkippedRecords();
        }
        catch (GeneralException | IOException e) {
            logger.error((Object)"Error -- processPriceFile() ", e);
        }
    }

    private void parseFile(String inFolder, String outFolder) throws GeneralException, IOException {
        try {
            skippedRetailerItemcodes = new HashSet<String>();
            priceList = new ArrayList<GiantEaglePriceDTO>();
            fillAllColumns();
            String[] fieldNames = new String[allColumns.size()];
            int k = 0;
            for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
                fieldNames[k] = columns.getValue();
                ++k;
            }
            ArrayList<String> zipFileList = getZipFiles(inFolder);
            int curZipFileCount = -1;
            boolean processZipFile = false;
            String zipFilePath = String.valueOf(getRootPath()) + "/" + inFolder;
            do {
                ArrayList<String> fileList = null;
                boolean commit = true;
                try {
                    if (processZipFile) {
                        PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);
                    }
                    fileList = getFiles(inFolder);
                    int i = 0;
                    while (i < fileList.size()) {
                        long fileProcessStartTime = System.currentTimeMillis();
                        String files = fileList.get(i);
                        logger.info((Object)("File Name - " + files));
                        int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
//                        String outputFileName = "PromoFormattedFile_"+startDate.replace("/", "")+".txt";
                        String outputPath = String.valueOf(getRootPath()) + "/" + outFolder + "/" + "PromoFormattedFile.txt";
                        File file = new File(outputPath);
                        fw = !file.exists() ? new FileWriter(outputPath) : new FileWriter(outputPath, true);
                        pw = new PrintWriter(fw);
                        logger.info((Object)"Processing Retail Records ...");
                        super.parseDelimitedFile(GiantEaglePriceDTO.class, fileList.get(i), '|', fieldNames, stopCount);
                        processCachedList();
                        priceList = new ArrayList<GiantEaglePriceDTO>();
                        pw.flush();
                        fw.flush();
                        pw.close();
                        fw.close();
                        long fileProcessEndTime = System.currentTimeMillis();
                        logger.info((Object)("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime) + "ms"));
                        ++i;
                    }
                }
                catch (GeneralException ex) {
                    logger.error((Object)"GeneralException", (Throwable)ex);
                    commit = false;
                }
                catch (Exception ex) {
                    logger.error((Object)"JavaException", (Throwable)ex);
                    commit = false;
                }
                if (processZipFile) {
                    PrestoUtil.deleteFiles(fileList);
                    fileList.clear();
                    fileList.add(zipFileList.get(curZipFileCount));
                }
                String archivePath = String.valueOf(getRootPath()) + "/" + inFolder + "/";
                if (commit) {
                    PrestoUtil.moveFiles(fileList, String.valueOf(archivePath) + "CompletedData");
                } else {
                    PrestoUtil.moveFiles(fileList, String.valueOf(archivePath) + "BadData");
                }
                processZipFile = true;
            } while (++curZipFileCount < zipFileList.size());
        }
        catch (GeneralException | Exception ex) {
            throw new GeneralException("Outer Exception - JavaException", ex);
        }
    }

    public void processRecords(List listobj) throws GeneralException {
        priceList.addAll(listobj);
        logger.info((Object)("# of records cached - " + priceList.size()));
    }

    private void processCachedList() throws GeneralException, CloneNotSupportedException {
        String separator = "|";
        writeHeader(separator);
//        List<GiantEaglePriceDTO> finalList = getPromoRecords(priceList);
        List<GiantEaglePriceDTO> finalList = new PriceFileFormatter().identifySaleAndRegRowsAndGroup(priceList); 
        HashMap<String, String> retailerItemCodeMap = getRetailerItemCodeMap();
        int recordCount = 0;
        for (GiantEaglePriceDTO giantEaglePriceDTO : finalList) {
        	if(giantEaglePriceDTO.getSaleQty() > 0 && giantEaglePriceDTO.getSalePrice()>0) {
        		++recordCount;
                String zoneNum = String.valueOf(giantEaglePriceDTO.getBNR_CD()) + "-" + giantEaglePriceDTO.getZN_NO() + "-" + giantEaglePriceDTO.getPRC_GRP_CD();
                if (!Constants.EMPTY.equals(giantEaglePriceDTO.getSPLR_NO().trim()) && giantEaglePriceDTO.getSPLR_NO() != null) {
                    zoneNum = String.valueOf(zoneNum) + "-" + giantEaglePriceDTO.getSPLR_NO();
                }
                pw.print(Constants.EMPTY);
                pw.print(separator);
                if (giantEaglePriceDTO.getAD_LOCN_DSCR() != null || !giantEaglePriceDTO.getAD_LOCN_DSCR().isEmpty()) {
                    pw.print(giantEaglePriceDTO.getSaleEndDate());
                } else {
                    pw.print("00/00/00");
                }
                pw.print(separator);
                pw.print("00/00/00");
                pw.print(separator);
                pw.print("00/00/00");
                pw.print(separator);
                pw.print("00/00/00");
                pw.print(separator);
                pw.print(giantEaglePriceDTO.getSaleStartDate());
                pw.print(separator);
                pw.print(giantEaglePriceDTO.getSaleEndDate());
                pw.print(separator);
                pw.print(Constants.EMPTY);
                pw.print(separator);
                pw.print(giantEaglePriceDTO.getRITEM_NO());
                pw.print(separator);
                if (retailerItemCodeMap.containsKey(giantEaglePriceDTO.getRITEM_NO())) {
                	 pw.print(retailerItemCodeMap.get(giantEaglePriceDTO.getRITEM_NO()));
                }else {
                	pw.print(Constants.EMPTY);
                }
                pw.print(separator);
                pw.print(zoneNum);
                pw.print(separator);
                pw.print("000");
                pw.print(separator);
                pw.print("000");
                pw.print(separator);
                pw.print(giantEaglePriceDTO.getMUNIT_CNT());
                pw.print(separator);
                pw.print(giantEaglePriceDTO.getCURR_PRC());
                pw.print(separator);
                pw.print(giantEaglePriceDTO.getSaleQty());
                pw.print(separator);
                pw.print(giantEaglePriceDTO.getSalePrice());
                pw.print(separator);
                pw.print(Constants.EMPTY);
                pw.print(separator);
                pw.print(Constants.EMPTY);
                pw.print(separator);
                pw.print(Constants.EMPTY);
                pw.print(separator);
                pw.print(Constants.EMPTY);
                pw.print(separator);
                pw.print(Constants.EMPTY);
                pw.print(separator);
                pw.println("       ");
                if (recordCount % 100000 != 0) continue;
                logger.info((Object)("processRecords() Processed - " + recordCount));
        	}
        }
    }

    private void writeHeader(String separator) {
        pw.print("BUYER");
        pw.print(separator);
        pw.print("ADV 1");
        pw.print(separator);
        pw.print("ADV 2");
        pw.print(separator);
        pw.print("ADV 3");
        pw.print(separator);
        pw.print("ADV 4");
        pw.print(separator);
        pw.print("EFF DATE");
        pw.print(separator);
        pw.print("END DATE");
        pw.print(separator);
        pw.print("VENDOR");
        pw.print(separator);
        pw.print("SRC ITEM");
        pw.print(separator);
        pw.print("DESCRIPTION");
        pw.print(separator);
        pw.print("ZONE");
        pw.print(separator);
        pw.print("STORE");
        pw.print(separator);
        pw.print("STR/ZONE");
        pw.print(separator);
        pw.print("BEG 4");
        pw.print(separator);
        pw.print("BEG RET");
        pw.print(separator);
        pw.print("FIN 4");
        pw.print(separator);
        pw.print("FIN RET");
        pw.print(separator);
        pw.print("SAV AMT");
        pw.print(separator);
        pw.print("PAGE");
        pw.print(separator);
        pw.print("BLOCK");
        pw.print(separator);
        pw.print("DT PROMO");
        pw.print(separator);
        pw.print("PROMO");
        pw.print(separator);
        pw.println("       ");
    }

    /*private List<GiantEaglePriceDTO> getPromoRecords(List<GiantEaglePriceDTO> priceList) throws GeneralException, CloneNotSupportedException {
        ArrayList<GiantEaglePriceDTO> outList = new ArrayList<GiantEaglePriceDTO>();
        HashMap<String, GiantEaglePriceDTO> regPriceList = new HashMap<String, GiantEaglePriceDTO>();
        for (GiantEaglePriceDTO giantEaglePriceDTOs : priceList) {
            Object zoneNum = String.valueOf(giantEaglePriceDTOs.getBNR_CD()) + "-" + giantEaglePriceDTOs.getZN_NO() + "-" + giantEaglePriceDTOs.getPRC_GRP_CD();
            if (!Constants.EMPTY.equals(giantEaglePriceDTOs.getSPLR_NO().trim()) && giantEaglePriceDTOs.getSPLR_NO() != null) {
                zoneNum = String.valueOf(zoneNum) + "-" + giantEaglePriceDTOs.getSPLR_NO();
            }
            String Key = String.valueOf(giantEaglePriceDTOs.getRITEM_NO()) + "-" + (String)zoneNum;
            if (giantEaglePriceDTOs.getPRC_TYP_IND().equals(REG_PRICE) ||giantEaglePriceDTOs.getPRC_TYP_IND().equals(REG_PRICE1)){
            	GiantEaglePriceDTO giantEaglePriceDTO = new GiantEaglePriceDTO();
            	giantEaglePriceDTO.setCURR_PRC(giantEaglePriceDTOs.getCURR_PRC());
            	giantEaglePriceDTO.setREG_MUNIT_CNT(giantEaglePriceDTOs.getMUNIT_CNT());
            	regPriceList.put(Key, giantEaglePriceDTO);
            }
        }
        HashMap<String, String> retailerItemCodeMap = getRetailerItemCodeMap();
        for (GiantEaglePriceDTO giantEaglePriceDTO : priceList) {
            String Key;
            if (!giantEaglePriceDTO.getPRC_TYP_IND().equals(PROMO_PRICE)) continue;
            String zoneNum = String.valueOf(giantEaglePriceDTO.getBNR_CD()) + "-" + giantEaglePriceDTO.getZN_NO() + "-" + giantEaglePriceDTO.getPRC_GRP_CD();
            if (!Constants.EMPTY.equals(giantEaglePriceDTO.getSPLR_NO().trim()) && giantEaglePriceDTO.getSPLR_NO() != null) {
                zoneNum = String.valueOf(zoneNum) + "-" + giantEaglePriceDTO.getSPLR_NO();
            }
            Key = String.valueOf(giantEaglePriceDTO.getRITEM_NO()) + "-" + zoneNum;
			if (regPriceList.containsKey(Key)) {
				GiantEaglePriceDTO regPriceDTO = regPriceList.get(Key);
				if (giantEaglePriceDTO.getPROM_AMT_OFF() > 0) {
					double regUnitPrice = regPriceDTO.getCURR_PRC() / regPriceDTO.getMUNIT_CNT();
					double salePrice = regUnitPrice - giantEaglePriceDTO.getPROM_AMT_OFF();
					giantEaglePriceDTO.setRegPrice(salePrice);
					giantEaglePriceDTO.setREG_MUNIT_CNT(1);
				} 
				if (giantEaglePriceDTO.getPROM_PCT() > 0) {
					double regUnitPrice = regPriceDTO.getCURR_PRC() / regPriceDTO.getMUNIT_CNT();
					double salePrice = regUnitPrice * (1 - (giantEaglePriceDTO.getPROM_PCT() / 100));
					giantEaglePriceDTO.setRegPrice(salePrice);
					giantEaglePriceDTO.setREG_MUNIT_CNT(1);
				} 
				if(giantEaglePriceDTO.getCURR_PRC()>0){
					regPriceDTO.setRegPrice(giantEaglePriceDTO.getCURR_PRC());
					regPriceDTO.setREG_MUNIT_CNT(giantEaglePriceDTO.getMUNIT_CNT());
				}

			} else {
                regPriceNotAvailable.add(giantEaglePriceDTO.getRITEM_NO());
            }
            if (retailerItemCodeMap.containsKey(giantEaglePriceDTO.getRITEM_NO())) {
                giantEaglePriceDTO.setItemDesc(retailerItemCodeMap.get(giantEaglePriceDTO.getRITEM_NO()));
            }
            outList.add(giantEaglePriceDTO);
        }
        return outList;
    }*/

    public HashMap<String, String> getRetailerItemCodeMap() throws GeneralException {
        ItemDAO itemDAO = new ItemDAO();
        HashMap<String, String> retailerItemCodeMap = new HashMap<String, String>();
        List<ItemDTO> activeItems = itemDAO.getAllActiveItems(conn);
        for (ItemDTO itemDTO : activeItems) {
            retailerItemCodeMap.put(itemDTO.getRetailerItemCode(), itemDTO.getItemName());
        }
        return retailerItemCodeMap;
    }

    private void logSkippedRecords() {
        if (skippedRetailerItemcodes.size() > 0) {
            StringBuilder sb = new StringBuilder();
            for (String itemCode : skippedRetailerItemcodes) {
                sb.append(String.valueOf(itemCode) + ", ");
            }
            logger.warn((Object)("Skipped retailer item codes -> " + sb.toString()));
        }
    }

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

