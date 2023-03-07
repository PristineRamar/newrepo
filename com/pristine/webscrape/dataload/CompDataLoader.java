package com.pristine.webscrape.dataload;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.DateUtil;


public class CompDataLoader extends PristineFileParser {
	private static Logger logger = Logger.getLogger("AdFeedFormatterGE");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private final static String STORE_NAME = "STORE_NAME=";
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private static String storeName;
	public static final int LOG_RECORD_COUNT = 100000;
	List<String> filesListInDir = new ArrayList<String>();
	List<CompetitiveDataDTO> compDataList;
	DateFormat appDateFormatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
	Connection conn = null;

	public CompDataLoader(){
		super("analysis.properties");
		super.headerPresent = true;
		 try {
	            conn = DBManager.getConnection();
	        }
	        catch (GeneralException var1_1) {
	            // empty catch block
	        }
	}
	public static void main(String[] args) throws ParseException {
		String inFolder = null;
		String outFolder = null;
		PropertyManager.initialize("analysis.properties");
		PropertyConfigurator.configure("log4j-Raw_CompData_loader.properties");
		CompDataLoader compDataLoader = new CompDataLoader();
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				inFolder = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				outFolder = arg.substring(OUTPUT_FOLDER.length());
			}
			if (arg.startsWith(STORE_NAME)) {
				storeName = arg.substring(STORE_NAME.length());
			}
		}
		

		logger.info("********************************************");
		try {
			compDataLoader.processCompDataFile(inFolder);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		logger.info("********************************************");
	}
	
	
	private void processCompDataFile(String inFolder) throws SQLException, ParseException{
		try {
			parseFile(inFolder);
			//Set Week Start for all the entries 
			assignWeekStartDate();
			//Save comp data in the Database
			CompetitiveDataDAO cometitiveDataDAO = new CompetitiveDataDAO(conn);
			cometitiveDataDAO.insertRawCompData(compDataList);
		} catch (IOException | GeneralException e) {
			logger.error("Error -- processPriceFile() ", e);
		}finally{
			conn.close();
		}
	}
	
	private void parseFile(String inFolder) throws GeneralException, IOException {

		try {
			compDataList = new ArrayList<>();
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
				String files = null;
				try {
					if (processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

					fileList = getFiles(inFolder);

					for (int i = 0; i < fileList.size(); i++) {
						files = fileList.get(i);
						logger.info("File Name - " + files);
						int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
						super.parseDelimitedFile(CompetitiveDataDTO.class, fileList.get(i), ',', fieldNames, stopCount);
						//PrestoUtil.moveFile(files, zipFilePath + "/" + Constants.COMPLETED_FOLDER);
						processRecords(filesListInDir);
					}
					
				} catch (GeneralException ex) {
					logger.error("GeneralException", ex);
					//PrestoUtil.moveFile(files, zipFilePath + "/" + Constants.BAD_FOLDER);
				} 
				curZipFileCount++;
			}while (curZipFileCount < zipFileList.size());
		}catch (GeneralException | Exception ex) {
			throw new GeneralException("Outer Exception - JavaException", ex);
		}

	}
	
	private void fillAllColumns() {
		
			allColumns.put(1, "compStrNo");
			allColumns.put(2, "checkDate");
			allColumns.put(3, "itemcode");
			allColumns.put(4, "itemName");
			allColumns.put(5, "regMPack");
			allColumns.put(6, "regPrice");
			allColumns.put(7, "size");
			allColumns.put(8, "upc");
			allColumns.put(9, "outSideRangeInd");
			allColumns.put(10, "comment");
			allColumns.put(11, "saleMPack");
			allColumns.put(12, "fSalePrice");
			allColumns.put(13, "effSaleStartDate");
			if(storeName.equals("HANNAFORD")){
				allColumns.put(14, "itemName");
			}else{
				allColumns.put(14, "suggestedStrId");
			}
			allColumns.put(15, "plFlag");
	}
	@Override
	public void processRecords(List listobj) throws GeneralException {
		for(CompetitiveDataDTO competitiveDataDTO: (List<CompetitiveDataDTO>) listobj){
//			competitiveDataDTO.upc =PrestoUtil.castUPC(competitiveDataDTO.upc, false);
			String UPCWithNoCheckDigit;
			   if (competitiveDataDTO.upc.length() > 11)
			    UPCWithNoCheckDigit = competitiveDataDTO.upc.substring(0, 11);
			   else
			    UPCWithNoCheckDigit = competitiveDataDTO.upc;
			   competitiveDataDTO.upc =PrestoUtil.castUPC(UPCWithNoCheckDigit, false);
			compDataList.add(competitiveDataDTO);
		}
		
	}
	
	private void assignWeekStartDate() throws GeneralException, ParseException{
		HashMap<String, String> weekStartDate = new HashMap<String, String>();
		for(CompetitiveDataDTO competitiveDataDTO: compDataList){
			if(weekStartDate.containsKey(competitiveDataDTO.checkDate)){
				competitiveDataDTO.weekStartDate = weekStartDate.get(competitiveDataDTO.checkDate);
			}else{
				Date startDate = getFirstDateOfWeek(DateUtil.toDate(competitiveDataDTO.checkDate));
				competitiveDataDTO.weekStartDate = DateUtil.dateToString(startDate, Constants.APP_DATE_FORMAT);
				weekStartDate.put(competitiveDataDTO.checkDate, DateUtil.dateToString(startDate, Constants.APP_DATE_FORMAT));
			}
		}
	}
	
	private Date getFirstDateOfWeek(Date inputDate) throws ParseException {
		String strDate = DateUtil.getWeekStartDate(inputDate, 0);
		Date outputDate = appDateFormatter.parse(strDate);
		return outputDate;
	}
}
