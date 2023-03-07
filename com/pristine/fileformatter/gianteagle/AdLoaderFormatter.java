package com.pristine.fileformatter.gianteagle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.pristine.dto.AdStartAndEndDateKey;
import com.pristine.dto.fileformatter.gianteagle.GiantEaglePriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

/**
 * 
 * @author Dinesh Ramanathan
 *
 */
@SuppressWarnings("rawtypes")
public class AdLoaderFormatter extends PristineFileParser {
	private static Logger logger = Logger.getLogger("AdFeedFormatterGE");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	List<GiantEaglePriceDTO> priceList = null;
	public static final int LOG_RECORD_COUNT = 100000;
	private String GE_BNR = "GE";
	private String MI_BNR = "MI";
	private static String startDate = null;
	String outputDir = null;
	List<String> filesListInDir = new ArrayList<String>();
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	@SuppressWarnings("unchecked")
	public AdLoaderFormatter() {
		super();
		super.headerPresent = true;
	}

	public static void main(String[] args) throws ParseException {
		String inFolder = null;
		String outFolder = null;
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-ge-Adfeed-formatter.properties");
		AdLoaderFormatter adLoaderFormatter = new AdLoaderFormatter();
		for (String arg : args) {
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
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yy");
		if(args.length>2){
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
				logger.info("Date:"+startDate);
			} catch (GeneralException exception) {
				logger.error("Error when parsing date - " + exception.toString());
				System.exit(-1);
			}
		}
		}

		logger.info("********************************************");
		adLoaderFormatter.processPriceFile(inFolder, outFolder);
		logger.info("********************************************");
	}

	/**
	 * 
	 * @param inFolder
	 */
	private void processPriceFile(String inFolder, String outFolder) {
		try {
			parseFile(inFolder, outFolder);
		} catch (IOException | GeneralException e) {
			logger.error("Error -- processPriceFile() ", e);
		}
	}

	/**
	 * Parses given file formats to Pristine standard price file format
	 * 
	 * @param inFolder
	 * @param outFolder
	 * @throws GeneralException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private void parseFile(String inFolder, String outFolder) throws GeneralException, IOException {

		try {
			priceList = new ArrayList<>();
			List<GiantEaglePriceDTO> geAdFeedList = new ArrayList<GiantEaglePriceDTO>();
			List<GiantEaglePriceDTO> miAdFeedList = new ArrayList<GiantEaglePriceDTO>();
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
						String files = fileList.get(i);
						logger.info("File Name - " + files);
						int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
						super.parseDelimitedFile(GiantEaglePriceDTO.class, fileList.get(i), '|', fieldNames, stopCount);
						// Group based on Start and End Date
						HashMap<AdStartAndEndDateKey, List<GiantEaglePriceDTO>> priceListBasedOnStartAndEndDate = groupByStartAndEndDate(
								priceList);
						// Separate ad data based on Start date
						HashMap<String, List<GiantEaglePriceDTO>> adDataBasedOnStartDate = priceListBasedOnStartDate(
								priceListBasedOnStartAndEndDate);
						
						//By looping each Start date process the Ad data.
						for (Map.Entry<String, List<GiantEaglePriceDTO>> finalEntry : adDataBasedOnStartDate
								.entrySet()) {
							geAdFeedList = new ArrayList<GiantEaglePriceDTO>();
							miAdFeedList = new ArrayList<GiantEaglePriceDTO>();
							// Based on Banner code get a separate List for GE and MI
							for (GiantEaglePriceDTO giantEaglePriceDTO : finalEntry.getValue()) {
								// To process GE banner code
								if (giantEaglePriceDTO.getBNR_CD().equals(GE_BNR)
										&& giantEaglePriceDTO.getAD_LOCN_DSCR() != null
										&& !giantEaglePriceDTO.getAD_LOCN_DSCR().isEmpty()) {
									geAdFeedList.add(giantEaglePriceDTO);
								}
								// TO process MI banner code
								else if (giantEaglePriceDTO.getBNR_CD().equals(MI_BNR)
										&& giantEaglePriceDTO.getAD_LOCN_DSCR() != null
										&& !giantEaglePriceDTO.getAD_LOCN_DSCR().isEmpty()) {
									miAdFeedList.add(giantEaglePriceDTO);
								}
							}
							priceList = new ArrayList<>();
							// TO process GE list of Ad feed
							generateGEAdFeedFile(outFolder, geAdFeedList, finalEntry.getKey());
							// To process MI list of Ad feed
							generateMIAdFeedFile(outFolder, miAdFeedList, finalEntry.getKey());
							// Zip both GE and MI Ad feed
							zip(finalEntry.getKey());
						}
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

	/**
	 * To generate Giant Eagle ad feed file
	 * 
	 * @param outFolder
	 * @param geAdFeedList
	 * @throws GeneralException
	 */
	private void generateGEAdFeedFile(String outFolder, List<GiantEaglePriceDTO> geAdFeedList, String date) throws GeneralException {
		logger.info("Processing GE Records ...");
		// String date = startDate.replace("/", "").substring(0, 4);
		date= DateUtil.dateToString(DateUtil.toDate(date), "MM/dd/YY");
		date = date.replace("/", "_");
		String outputFileName = date + "_GE AdFeed.xlsx";
		logger.info("Output File name - " + outputFileName);

		String outputPath = getRootPath() + "/" + outFolder + "/" + outputFileName;

		XSSFWorkbook workbook = new XSSFWorkbook();
		XSSFSheet geAdFeedSheet = workbook.createSheet("sheet1");

		try {
			processCachedList(workbook, geAdFeedSheet, geAdFeedList);
			geAdFeedList = new ArrayList<>();
			FileOutputStream outputStream = new FileOutputStream(outputPath);
			workbook.write(outputStream);
			workbook.close();
			outputStream.close();
		} catch (CloneNotSupportedException | IOException e) {
			e.printStackTrace();
			throw new GeneralException("Error in generateGEAdFeedFile()", e);
		}

	}

	/**
	 * To generate MI ad feed file
	 * 
	 * @param outFolder
	 * @param geAdFeedList
	 * @throws GeneralException
	 */
	private void generateMIAdFeedFile(String outFolder, List<GiantEaglePriceDTO> miAdFeedList, String date) throws GeneralException {
		logger.info("Processing MI Records ...");
		 
		date= DateUtil.dateToString(DateUtil.toDate(date), "MM/dd/YY");
		date = date.replace("/", "_");
		String outputFileName = date + "_MI AdFeed.xlsx";
		logger.info("Output File name - " + outputFileName);
		outputDir = getRootPath() + "/" + outFolder;
		String outputPath = getRootPath() + "/" + outFolder + "/" + outputFileName;

		XSSFWorkbook workbook = new XSSFWorkbook();
		XSSFSheet miAdFeedSheet = workbook.createSheet("sheet1");

		try {
			processCachedList(workbook, miAdFeedSheet, miAdFeedList);
			miAdFeedList = new ArrayList<>();
			FileOutputStream outputStream = new FileOutputStream(outputPath);
			workbook.write(outputStream);
			workbook.close();
			outputStream.close();
		} catch (CloneNotSupportedException | IOException e) {
			e.printStackTrace();
			throw new GeneralException("Error in generateMIAdFeedFile()", e);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public void processRecords(List listobj) throws GeneralException {
		priceList.addAll((List<GiantEaglePriceDTO>) listobj);
		logger.info("# of records cached - " + priceList.size());
	}

	/**
	 * Processes given list and generates formatted file
	 * 
	 * @throws GeneralException
	 * @throws CloneNotSupportedException
	 */
	private void processCachedList(XSSFWorkbook workbook, XSSFSheet geAdFeedSheet, List<GiantEaglePriceDTO> adFeedList)
			throws GeneralException, CloneNotSupportedException {
		HashMap<String, String> duplicatesList = new HashMap<String, String>();
		int currentRow = 0;
		writeHeader(workbook, geAdFeedSheet, currentRow);
		currentRow++;
		for (GiantEaglePriceDTO giantEaglePriceDTO : adFeedList) {

			String key = giantEaglePriceDTO.getAD_LOCN_DSCR().substring(0, 3).trim() + "-"
					+ giantEaglePriceDTO.getRITEM_NO();
			if (!duplicatesList.containsKey(key) && IsInt_ByException(giantEaglePriceDTO.getAD_LOCN_DSCR().substring(0, 3).trim())){
				int columnIndex = 0;
				XSSFRow row = geAdFeedSheet.createRow(currentRow);
				row.createCell(columnIndex++)
						.setCellValue(Integer.parseInt(giantEaglePriceDTO.getAD_LOCN_DSCR().substring(0, 3).trim()));
				row.createCell(columnIndex++)
						.setCellValue(Integer.parseInt(giantEaglePriceDTO.getAD_LOCN_DSCR().substring(0, 3).trim()));
				row.createCell(columnIndex++).setCellValue(Integer.parseInt("1"));
				row.createCell(columnIndex++).setCellValue(Integer.parseInt("0"));
				row.createCell(columnIndex++).setCellValue(giantEaglePriceDTO.getRITEM_NO().toString());
				row.createCell(columnIndex++);
				row.createCell(columnIndex++);
				row.createCell(columnIndex++);
				row.createCell(columnIndex++);
				row.createCell(columnIndex++);
				row.createCell(columnIndex++);
				row.createCell(columnIndex++);
				row.createCell(columnIndex++);
				row.createCell(columnIndex++);
				currentRow++;
				duplicatesList.put(key, giantEaglePriceDTO.getRITEM_NO());
			}
		}

	}
	
	private boolean IsInt_ByException(String str)
	 {
	     try
	     {
	         Integer.parseInt(str);
	         return true;
	     }
	     catch(NumberFormatException nfe)
	     {
	         return false;
	     }
	 }
	
	private void writeHeader(XSSFWorkbook workbook, XSSFSheet sheet, int currentRow) {
		int columnIndex = 0;
		XSSFRow row = sheet.createRow(currentRow);
		row.createCell(columnIndex++).setCellValue("Page.No.");
		row.createCell(columnIndex++).setCellValue("Page.No.");
		row.createCell(columnIndex++).setCellValue("Page.Box");
		row.createCell(columnIndex++).setCellValue("Item.Code.1.Division.No.");
		row.createCell(columnIndex++).setCellValue("Item.Code_ITM");
		row.createCell(columnIndex++).setCellValue("Item.Code.CopyGroup.SN.Net");
		row.createCell(columnIndex++).setCellValue("Copy.SubHeadline");
		row.createCell(columnIndex++).setCellValue("Copy.Headline");
		row.createCell(columnIndex++).setCellValue("Price.Special.For.Qty");
		row.createCell(columnIndex++).setCellValue("Price.Special");
		row.createCell(columnIndex++).setCellValue("ObjectCodeImage1");
		row.createCell(columnIndex++).setCellValue("SignsPublishedSN");
		row.createCell(columnIndex++).setCellValue("length");
		row.createCell(columnIndex++).setCellValue("COMMENT");

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

	private void zip(String date) throws GeneralException {
		String zipFile = null;
		String fileExtension = "xlsx";
		try {

			ArrayList<String> listOfFileName = PrestoUtil.getAllFilesInADirectory(outputDir, fileExtension);
			String moveFileDir = outputDir + "/" + date.replace("/", "") + " AdFeed";
			File newFileDir = new File(moveFileDir);
			if (!newFileDir.exists()) {
				newFileDir.mkdir();
			}
			String subFolder = moveFileDir + "/" + date.replace("/", "") + " AdFeed";
			File subFolderDir = new File(subFolder);
			if (!subFolderDir.exists()) {
				subFolderDir.mkdir();
			}
			moveFiles(listOfFileName, subFolder);
			zipFile = moveFileDir + ".zip";
			File dir = new File(moveFileDir);
			PrestoUtil.zipDirectory(dir, zipFile);
			delete(newFileDir);

			logger.info("zip() - zip completed.  Filename=" + zipFile);
		} catch (Exception e) {
			// e.printStackTrace();
			logger.error("zip() ExceptionMessage=" + e.getMessage());
			logger.info("zip() - zip FAILED.  Filename=" + zipFile);

			throw new GeneralException("zip() - Problem with zipping!  Exception has occurred.  ", e);
		}
	}

	private void moveFiles(ArrayList<String> fileList, String destinationDirectory) {
		for (String sourceFile : fileList) {
			File file = new File(sourceFile);
			// Destination directory
			File dir = new File(destinationDirectory + "/" + file.getName());

			boolean success = file.renameTo(dir);

			if (!success)
				logger.error("moveFile() - File moving FAILED!  Filename=" + sourceFile + ", destinationDirectory="
						+ destinationDirectory);
		}
	}

	private void delete(File file) throws IOException {
		if (file.isDirectory()) {
			// directory is empty, then delete it
			if (file.list().length == 0) {
				file.delete();
				System.out.println("Directory is deleted : " + file.getAbsolutePath());
			} else {
				// list all the directory contents
				String files[] = file.list();
				for (String temp : files) {
					File fileDelete = new File(file, temp);
					delete(fileDelete);
				}
				// check the directory again, if empty then delete it
				if (file.list().length == 0) {
					file.delete();
					System.out.println("Directory is deleted : " + file.getAbsolutePath());
				}
			}

		} else {
			// if file, then delete it
			file.delete();
			System.out.println("File is deleted : " + file.getAbsolutePath());
		}
	}
	
	private HashMap<AdStartAndEndDateKey, List<GiantEaglePriceDTO>>groupByStartAndEndDate(List<GiantEaglePriceDTO> priceList){
		HashMap<AdStartAndEndDateKey, List<GiantEaglePriceDTO>> priceListBasedOnStartAndEndDate = new HashMap<AdStartAndEndDateKey, List<GiantEaglePriceDTO>>();
		for(GiantEaglePriceDTO giantEaglePriceDTO: priceList){
			if (giantEaglePriceDTO.getAD_LOCN_DSCR() != null
					&& !giantEaglePriceDTO.getAD_LOCN_DSCR().isEmpty() && giantEaglePriceDTO.getPRC_TYP_IND().equals("PPR")){
			AdStartAndEndDateKey key = new AdStartAndEndDateKey(giantEaglePriceDTO.getPRC_STRT_DTE(),giantEaglePriceDTO.getPRC_END_DTE());
//			String key = giantEaglePriceDTO.getPRC_STRT_DTE()+"_"+giantEaglePriceDTO.getPRC_END_DTE();
			List<GiantEaglePriceDTO> giantEaglePriceDTO2 = new ArrayList<GiantEaglePriceDTO>();
			if(priceListBasedOnStartAndEndDate.containsKey(key)){
				giantEaglePriceDTO2.addAll(priceListBasedOnStartAndEndDate.get(key));
			}
			giantEaglePriceDTO2.add(giantEaglePriceDTO);
			priceListBasedOnStartAndEndDate.put(key, giantEaglePriceDTO2);
			}
		}
		return priceListBasedOnStartAndEndDate;
		
	}
	
	private HashMap<String, List<GiantEaglePriceDTO>>priceListBasedOnStartDate(HashMap<AdStartAndEndDateKey, List<GiantEaglePriceDTO>> groupedByStartAndEndDate) throws ParseException, GeneralException{
		HashMap<String, List<GiantEaglePriceDTO>> priceBasedOnStartDate = new HashMap<String, List<GiantEaglePriceDTO>>();
		for(Map.Entry<AdStartAndEndDateKey, List<GiantEaglePriceDTO>> entry: groupedByStartAndEndDate.entrySet()){
			AdStartAndEndDateKey key = entry.getKey();
			String startDate = key.getPRC_STRT_DTE();
			//To get Number of weeks to be 
			long noOfDays = updateNoOfDaysInPromoDuration(entry.getKey());
			int noOfWeeksToProcess = (int) Math.ceil((double) noOfDays / 7);
			for(int i = 1; i<=noOfWeeksToProcess;i++){
				if(i != 1){
					startDate = DateUtil.dateToString(getNextWeekDate(DateUtil.toDate(startDate)), Constants.APP_DATE_FORMAT);
				}
				for(GiantEaglePriceDTO giantEaglePriceDTO: entry.getValue()){
					List<GiantEaglePriceDTO> giantEaglePriceDTOs = new ArrayList<GiantEaglePriceDTO>();
					if(priceBasedOnStartDate.containsKey(startDate)){
						giantEaglePriceDTOs.addAll(priceBasedOnStartDate.get(startDate));
					}
					giantEaglePriceDTOs.add(giantEaglePriceDTO);
					priceBasedOnStartDate.put(startDate, giantEaglePriceDTOs);
				}
			}
		}
		return priceBasedOnStartDate;
	}
	
	private long updateNoOfDaysInPromoDuration(AdStartAndEndDateKey startAndEndDate) throws ParseException, GeneralException {
		
		Date endDateOfPromo = getLastDateOfWeek(DateUtil.toDate(startAndEndDate.getPRC_END_DTE()));
		Date startDateTemp = getFirstDateOfWeek(DateUtil.toDate(startAndEndDate.getPRC_STRT_DTE()));

		long diff = endDateOfPromo.getTime() - startDateTemp.getTime();
		// System.out.println ("Days: " + TimeUnit.DAYS.convert(diff,TimeUnit.MILLISECONDS));
		return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}
//	private Date getFirstDateOfWeek(Date inputDate) throws ParseException {
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(inputDate);
//		int startDay = (cal.get(Calendar.DAY_OF_WEEK) - cal.getFirstDayOfWeek());
//		Date outputDate = DateUtil.incrementDate(inputDate, -startDay);
//		return outputDate;
//	}
//	
//	private Date getLastDateOfWeek(Date inputDate) throws ParseException {
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(inputDate);
//		Date outputDate = DateUtil.incrementDate(inputDate, 7 - cal.get(Calendar.DAY_OF_WEEK));
//		return outputDate;
//	}

	private Date getNextWeekDate(Date inputDate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);
		Date outputDate = DateUtil.incrementDate(inputDate, 7);
		return outputDate;
	}

	private Date getCurrentWeekEndDate(Date inputDate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(inputDate);
		Date outputDate = DateUtil.incrementDate(inputDate, 5 + cal.get(Calendar.DAY_OF_WEEK));
		return outputDate;
	}
	private Date getFirstDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date outputDate = appDateFormatter.parse(strDate);
		return outputDate;
	}

	private Date getLastDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date endDate = appDateFormatter.parse(strDate);
		Date outputDate = DateUtil.incrementDate(endDate, 6);
		return outputDate;
	}
}
