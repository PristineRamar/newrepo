package com.pristine.dataload.ahold;

import java.sql.Connection;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.MovementDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.fileformatter.AholdMovementFile;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

/**
 * Loads movement data for Ahold
 * @author Janani
 */
public class MovementDataLoad extends PristineFileParser{
	private static Logger logger = Logger.getLogger("MovementDataLoad");
	private int stopCount = -1;
		
	private com.pristine.dataload.MovementDataLoad movementLoad = null;
	private List<AholdMovementFile> allRecords = new ArrayList<AholdMovementFile>();
	static HashMap<Integer, String> allColumns = new HashMap<Integer, String>();
	private MovementDAO objMovement = null;
	
	private String rootPath;
	private int recordCount = 0;
	private int ignoredCount = 0;
	private int calendarId = -1;
	private int dayOfWeek = -1;
	
	Connection conn = null;
	
	/**
	 * Initializes database connection and store list
	 */
	public MovementDataLoad()
	{
		PropertyManager.initialize("analysis.properties");
		try {	
			//Create DB connection
			conn = DBManager.getConnection();
			stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
		}
		catch (GeneralException ex) {
		}
		catch (Exception ex) {
			stopCount = -1;
		}
		movementLoad = new com.pristine.dataload.MovementDataLoad("N", false);
		objMovement = new MovementDAO();
	}
	
	/**
	 * Main method to process movement data
	 * @param args[0]	Relative path of movement files
	 */
	public static void main(String[] args) throws GeneralException{
		PropertyConfigurator.configure("log4j-movement-daily-dataload.properties");
		if (args.length < 1) {
			logger.info("Invalid Arguments,  args[0] - Movement File Input Relative Path");
			System.exit(-1);
		}
		
		MovementDataLoad movementDataLoad = new MovementDataLoad();
		movementDataLoad.parseFile(args[0]);
	}
	
	private void parseFile(String relativePath) throws GeneralException{
		super.headerPresent = true;
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}

		//Clear hashmap as it is not required from here
		allColumns = null;
		
		//getzip files
		ArrayList<String> zipFileList = getZipFiles(relativePath);
		
		//Start with -1 so that if any regular files are present, they are processed first
		int curZipFileCount = -1;
		boolean processZipFile = false;
		
		String zipFilePath = getRootPath() + "/" + relativePath;
		do {
			ArrayList<String> fileList = null;
			boolean commit = true;
			
			if( processZipFile)
				PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;
			
			fileList = getFiles(relativePath);
			
			for (int j = 0; j < fileList.size(); j++) {
				logger.info("Reading of file " + fileList.get(j)
						+ " is Started... ");
				try {
					parseDelimitedFile(AholdMovementFile.class, fileList.get(j), '|', fieldNames, stopCount);
					logger.info("Reading of file " + fileList.get(j) + " is Complete... ");
					if(allRecords.size() > 0){
						logger.info("Processing last batch " + allRecords.size());
						processInputData(allRecords);
					}
					
					logger.info("No of records Processed - " + recordCount);
					logger.info("No of records Ignored - " + ignoredCount);
					logger.info("No of UPCs with no item code - " + movementLoad.getupcMissingMap().size());
				} catch (GeneralException ge) {
					logger.error("File Skipped. Exception while reading of file "
							+ fileList.get(j));
					commit = false;
				}
				
			}
			
			if( processZipFile){
		    	PrestoUtil.deleteFiles(fileList);
		    	fileList.clear();
		    	fileList.add(zipFileList.get(curZipFileCount));
		    }
		    String archivePath = getRootPath() + "/" + relativePath + "/";
		    
		    if( commit ){
				PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
			}
			else{
				PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
			}
			curZipFileCount++;
			processZipFile = true;
		}while (curZipFileCount < zipFileList.size());
		
		super.performCloseOperation(conn, true);
	}
	
	/**
	 * This method reads and loads daily movement data from the files
	 * under specified path.
	 * @param relativePath	Relative Path from which files needs to be read
	 */
	private void processInputData(List<AholdMovementFile> allRecords) {
		try{	
			// Populate Movement List
			ArrayList<MovementDTO> movementDtoList = new ArrayList<MovementDTO>();
			for(AholdMovementFile aholdMovementData : allRecords){
				aholdMovementData.initializeVariables();
				// Ignore items when loading into movement_daily table
				if(aholdMovementData.isIgnoreItemForMovementDaily()){
					logger.debug("UPC :" + aholdMovementData.getUPC_CD() + " Comp Str No :" + aholdMovementData.getOPCO_LOC_CD());
					ignoredCount++;
					continue;
				}
				MovementDTO movementData = new MovementDTO();
				populateMovementData(aholdMovementData, movementData);
				movementDtoList.add(movementData);
				recordCount++;
				
				if ( recordCount % Constants.LOG_RECORD_COUNT == 0 ) {
					logger.info("Processed " + String.valueOf(recordCount) + " records");
					//break;
	            }
			}
			
       		objMovement.insertMovementDaily(conn, movementDtoList, "MOVEMENT_DAILY");
			
		}catch (Exception ex) {
            logger.error("Error " + ex);
        }
        catch (GeneralException ex) {
            logger.error("Error " + ex);
        }
		
	}
	
	/**
	 * Populate MovementDTO using AholdMovementFile
	 * @param aholdMovementData
	 * @param movementData
	 */
	private void populateMovementData(AholdMovementFile aholdMovementData, MovementDTO movementData){
		
		movementData.setItemStore(aholdMovementData.getOPCO_LOC_CD());
		movementData.setItemUPC(PrestoUtil.castUPC(aholdMovementData.getUPC_CD(), false));
		try{
			String date = aholdMovementData.getWeekEndDate();
			movementData.setItemDateTime(DateUtil.toDate(date));
			if(calendarId == -1){
				Date calDate = DateUtil.toDate(date);
				RetailCalendarDAO objCal = new RetailCalendarDAO();
				List<RetailCalendarDTO> dateList = objCal.dayCalendarList(conn, calDate, calDate, Constants.CALENDAR_DAY);
				calendarId = dateList.get(0).getCalendarId();
				java.util.Calendar cal = java.util.Calendar.getInstance();
				cal.setTime(calDate);
				dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
			}
			movementData.setDayOfWeek(dayOfWeek);
			movementData.setCalendarId(calendarId);
		}catch(ParseException exception){
			logger.error("Error when parsing date - " + exception);
		}catch(GeneralException exception){
			logger.error("Error when parsing date - " + exception);
		}
		movementData.setSaleFlag(aholdMovementData.isItemOnSale());
		movementData.setExtendedGrossPrice(PrestoUtil.round(Double.parseDouble(aholdMovementData.getGROSSDOLLARS()), 2));
		movementData.setExtendedNetPrice(PrestoUtil.round(Double.parseDouble(aholdMovementData.getNETDOLLARS()), 2));
		movementData.setExtnQty(PrestoUtil.round(aholdMovementData.getUnitCount(),0));
		movementData.setExtnWeight(PrestoUtil.round(Double.parseDouble(aholdMovementData.getRETAILWEIGHT()), 4));
		int posDept = Integer.parseInt(aholdMovementData.getLIST_MEMBER());
		if(posDept <= 50)
			movementData.setPosDepartment(Integer.parseInt(aholdMovementData.getLIST_MEMBER()));
		
		try{
			if(aholdMovementData.isItemOnSale())
				movementData.setItemNetPrice(PrestoUtil.round(Double.parseDouble(aholdMovementData.saleRetail()), 2));
			else
				movementData.setItemNetPrice(PrestoUtil.round(Double.parseDouble(aholdMovementData.regularRetail()), 2));
			movementData.setItemCode(movementLoad.getItemCode(movementData.getItemUPC(), movementData.getPosDepartment()));
		}catch(GeneralException ge){
			logger.error("Exception in setting movement data - " + ge);
		}catch(Exception e){
			logger.error("Exception in setting movement data - " + e);
		}
	}
	
	public void processRecords(List listObject) throws GeneralException {			
		List<AholdMovementFile> aholdMovFile = (List<AholdMovementFile>) listObject;
		allRecords.addAll(aholdMovFile);
		//Batch process. Keeping all data in list may lead to out-of-memory exception
		if(allRecords.size()>= Constants.LIST_SIZE_LIMIT )
		{				
			processInputData(allRecords);
			allRecords.clear();				
		}				
	}
	
	/***
	 * Fill all possible columns of the csv file with key
	 */
	private void fillAllColumns() {
		allColumns.put(1, "CUSTOM_ATTR2_DESC");
		allColumns.put(2, "UPC_CD");
		// Added for Sales Analysis
		allColumns.put(3, "LIST_MEMBER");
		allColumns.put(4, "OPCO_LOC_CD");
		allColumns.put(5, "WK_KEY");
		allColumns.put(6, "WK_DESC");
		allColumns.put(7, "SCANNEDUNITS");
		allColumns.put(8, "GROSSDOLLARS");
		allColumns.put(9, "NETDOLLARS");
		allColumns.put(10, "RETAILWEIGHT");
		allColumns.put(11, "POUNDORUNITS");
		allColumns.put(12, "EXTLEVEL3ELEMENTCOST");
		allColumns.put(13, "EXTLEVEL4ELEMENTCOST");
	}
}
