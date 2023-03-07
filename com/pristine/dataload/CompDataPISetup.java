package com.pristine.dataload;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.CompDataPIDAO;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class CompDataPISetup {
	private static Logger logger = Logger.getLogger("CompDataLIG");
	private static String LOCATION = "LOCATION_ID=";
	private static String LOCATION_LEVEL = "LOCATION_LEVEL_ID=";
	public static void main(String[] args) {
		if (args.length < 2) {
			logger.error("Insufficient Arguments - CompDataPISetup S/C scheduleId/CompStrId");
			System.exit(1);
		}
		PropertyConfigurator.configure("log4j-lig.properties");

		
		PropertyManager.initialize("analysis.properties");

		Connection conn = null;
		try {
			conn = DBManager.getConnection();

			CompDataPISetup compDataPI = new CompDataPISetup();
			int locationId = 0;
			if (!args[0].startsWith(LOCATION_LEVEL))
				locationId = Integer.parseInt(args[1]); 
			if( args[0].equals("C" ) || args[0].equals("Z" ) || args[0].startsWith(LOCATION_LEVEL)){
				
				// Changes to include FULL/PARTIAL scenario in Competitive Data PI Setup - Starts
				// Number of weeks to retain data for needs to be specified if the setup is PARTIAL
				String typeOfInput = null;
				String weeksToKeep = null;
				int locationLevelId = -1;
				if(args.length > 3 && args[0].startsWith(LOCATION_LEVEL) 
						&& (Constants.PI_SETUP_FULL.equalsIgnoreCase(args[3]) 
								|| Constants.PI_SETUP_PARTIAL.equalsIgnoreCase(args[3]))){
					typeOfInput = args[3];
					weeksToKeep = PropertyManager.getProperty("PI_SETUP_WEEKS_TO_KEEP");
				}else if(args.length > 2 && 
						(Constants.PI_SETUP_FULL.equalsIgnoreCase(args[2]) 
								|| Constants.PI_SETUP_PARTIAL.equalsIgnoreCase(args[2]))){
					typeOfInput = args[2];
					weeksToKeep = PropertyManager.getProperty("PI_SETUP_WEEKS_TO_KEEP");
				}else{
					logger.error("Insufficient Arguments - CompDataPISetup S/C scheduleId/CompStrId PI_SETUP_FULL/PI_SETUP_PARTIAL");
					System.exit(1);
				}
				// Changes to include FULL/PARTIAL scenario in Competitive Data PI Setup - Ends
				
				if(args[0].equals("C" )) locationLevelId = Constants.STORE_LEVEL_ID;
				else if(args[0].equals("Z")) locationLevelId = Constants.ZONE_LEVEL_ID;
				else if (args[0].startsWith(LOCATION_LEVEL)){
					if (args[1].startsWith(LOCATION)) {
						locationLevelId = Integer.parseInt(args[0].substring(LOCATION_LEVEL.length()));
						locationId = Integer.parseInt(args[1].substring(LOCATION.length()));
					}
				}
				compDataPI.setupLocationLevel(conn, locationId, typeOfInput, weeksToKeep, locationLevelId);
			}else if ( args[0].equals("S" ))
					compDataPI.setupSchedule(conn, locationId);
			
		} catch (GeneralException ge) {
			logger.error("Error in Comp Data Price Index Setup ", ge);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}

		try {
			if (PropertyManager.getProperty("DATALOAD.COMMIT", "")
					.equalsIgnoreCase("TRUE")) {
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Comp Data LIG");
			} else {
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "Comp Data LIG");
			}
		} catch (GeneralException ge) {
			logger.error("Error in commit", ge);
			System.exit(1);
		} finally {
			PristineDBUtil.close(conn);
		}
	}

	public void setupSchedule(Connection conn, int scheduleId) throws GeneralException {
		// TODO Auto-generated method stub
		logger.info("Doing Comp Data Price setup for  " + scheduleId);
		CompDataPIDAO compDataPriceIndexDao = new CompDataPIDAO (); 
		//delete data at schedule
		compDataPriceIndexDao.deleteDataForSchedule(conn, scheduleId, Constants.NULLID);
		//insert data from LIR table
		compDataPriceIndexDao.insertLIGLevelData ( conn, scheduleId);
		//insert data from competitive data where active and Lir is nullv
		CompetitiveDataDAO compDataDao = new CompetitiveDataDAO(conn);
		// Get items in the current in the current schedule
		ArrayList<CompetitiveDataDTO> compDataList = compDataDao.getCompData(
				conn, scheduleId, -1, -1, true);
		int count = 0;
		int procRecCount = 0;
		boolean commonCheckDataIdOrItemCodeExist = false;
		String commonCheckDataId = "", commonItemCodes = "";
		HashSet<Integer> checkDataIdOfLigItems = new HashSet <Integer>(); 
		HashSet<Integer> itemCodeOfLigItems = new HashSet <Integer>(); 
		
		List<CompetitiveDataDTO> compDataListForInsert = new ArrayList<CompetitiveDataDTO>(); // Added for batch insert
		
		// Get LIG level data from competitive_data_pi table
		List<CompetitiveDataDTO> compDataListLIG = compDataDao.getCompDataPI(
				conn, scheduleId);
		logger.info("LIG record count: " + compDataListLIG.size());
		
		//Keep in HashSet 5th May 2014 to keep check data id and item code in hashset
		for (CompetitiveDataDTO compDataLIG : compDataListLIG) {
			if( !checkDataIdOfLigItems.contains(compDataLIG.checkItemId))
				checkDataIdOfLigItems.add(compDataLIG.checkItemId);
			
			if( !itemCodeOfLigItems.contains(compDataLIG.itemcode))
				itemCodeOfLigItems.add(compDataLIG.itemcode);
		}
		
				
		for (CompetitiveDataDTO compData : compDataList) {

			// Now insert non Lir items
			if (compData.lirId < 0) {
				commonCheckDataIdOrItemCodeExist = false;
				boolean insert = true;
				
				//Get the Discontinued Flag and PI Analyze flag and then determine
				if (compData.discontFlag != null && compData.discontFlag.equals("Y") )
					insert = false;
				
				if (compData.piAnalyzeFlag != null && compData.piAnalyzeFlag.equals("N") )
					insert = false;
				
				if(insert){
					// Changes for batch insert - Starts
					/* if( compDataPriceIndexDao.insertNonLIGItem(conn, compData))
						insertCount++;
					else{
						errorCount++;
						logger.debug("Check Data Id in error = " + compData.checkItemId);
					}*/
					
					// Added on 30th Apr 2014, to avoid lig and non-lig with same check data id being inserted					 
					// Check if same Check Data Id is exists with any LIG
					// item
					if (checkDataIdOfLigItems.contains(compData.checkItemId)) {
						commonCheckDataId = commonCheckDataId + ","
								+ compData.checkItemId;
						commonCheckDataIdOrItemCodeExist = true;
					}
					
					if (itemCodeOfLigItems.contains(compData.itemcode)) {
						commonItemCodes = commonItemCodes + ","
								+ compData.itemcode;
						commonCheckDataIdOrItemCodeExist = true;
					}
					
					if (!commonCheckDataIdOrItemCodeExist)
						compDataListForInsert.add(compData);
					// Changes for batch insert - Ends
				}
				
			}
			count++;
			if ( count > 0 && count%10000 == 0){
				// Changes for batch insert - Starts
				compDataPriceIndexDao.insertNonLIGItem(conn, compDataListForInsert);
				procRecCount = procRecCount + count;
				logger.info("Processed # of records: " + procRecCount);
				count = 0;
				compDataListForInsert.clear();
				// Changes for batch insert - Ends
			}
		}
		
		logger.info("Common Check Data Id's in LIG and NON-LIG: "
				+ commonCheckDataId);
		
		logger.info("Common Item Code's in LIG and NON-LIG: "
				+ commonItemCodes);
		
		// Changes for batch insert - Starts
		if(count > 0){
			compDataPriceIndexDao.insertNonLIGItem(conn, compDataListForInsert);
			procRecCount = procRecCount + count;
			logger.info("Processed # of records: " + procRecCount);
		}
		// Changes for batch insert - Ends
		
		// logger.info( errorCount + " item in error " );
		// logger.info( insertCount + " item inserted out of " + procRecCount);
		logger.info("Completed Comp Data Price setup for  " + scheduleId);
	}

	public void setupLocationLevel(Connection conn, int locationId, 
			String typeOfSetup, String weeksToKeep, int locationLevelId) throws GeneralException {
		
		//TODO delete data at store level
		try {
			logger.info("Doing Comp Data Price setup for  Store " + locationId + "*****");
			CompDataPIDAO compDataPriceIndexDao = new CompDataPIDAO ();
			
			// Changes to include FULL/PARTIAL scenario in Competitive Data PI Setup - Starts
			int weeks[] = null;
			String[] weeksStr = null;
			String weeksForSetup = null;
			int daysToRetain = 0;
			if(Constants.PI_SETUP_FULL.equalsIgnoreCase(typeOfSetup)){
				weeksForSetup = PropertyManager.getProperty("PI_SETUP_FULL_WEEKS");
			}else if(Constants.PI_SETUP_PARTIAL.equalsIgnoreCase(typeOfSetup)){
				weeksForSetup = PropertyManager.getProperty("PI_SETUP_PARTIAL_WEEKS");
				int weeksToRetain = Integer.parseInt(weeksToKeep);
				daysToRetain = weeksToRetain * 7;
			}
			weeksStr = weeksForSetup.split(",");
			weeks = new int[weeksStr.length];
			for(int i = 0; i < weeksStr.length; i++){
				weeks[i] = Integer.parseInt(weeksStr[i]);
			}
			// Changes to include FULL/PARTIAL scenario in Competitive Data PI Setup - Ends
			
			ScheduleDAO schDao = new ScheduleDAO();
			CachedRowSet crs = schDao.getScheduleForLocation(conn, locationId, daysToRetain, locationLevelId);
			while (crs.next()){
				int schId = crs.getInt("Schedule_Id");
				compDataPriceIndexDao.deleteDataForSchedule(conn, schId, Constants.NULLID);
			}
			
			// int weeks[] = {1,2,3,4,5,6,14,27,53};
			//int weeks[] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,53};
			CompetitiveDataDAO compDataDao = new CompetitiveDataDAO(conn);
			//get schedule for last week
			for ( int i = 0; i < weeks.length; i++){
				String startDate = DateUtil.getWeekStartDate(weeks[i]);
				Date endDate = DateUtil.incrementDate(DateUtil.toDate(startDate), 6);
				String endDateStr = DateUtil.dateToString(endDate, Constants.APP_DATE_FORMAT);
				
				logger.info( "loading data for " + startDate + " - " + endDateStr);
				
				CompetitiveDataDTO compDto = new CompetitiveDataDTO();
				compDto.weekStartDate = startDate;
				compDto.weekEndDate = endDateStr;
				compDto.compStrId = locationId;
				//get the scheduleId
				int schId = -1;
				
				if(locationLevelId == Constants.ZONE_LEVEL_ID){
					schId = schDao.populateScheduleIdForZone(conn, locationId, startDate, endDateStr);
				} else if(locationLevelId != Constants.ZONE_LEVEL_ID
						&& locationLevelId != Constants.STORE_LEVEL_ID){
					schId = schDao.populateScheduleIdForLocation(conn, locationLevelId, locationId, startDate, endDateStr);
				}else
					schId = compDataDao.getScheduleID(conn, compDto, false);
				//Call the 

				if( schId > 0){
					setupSchedule(conn, schId);
				}
			}
			logger.info("Completed Comp Data Price setup for  Store " + locationId + "*****");
			
		}catch(SQLException e){
			throw new GeneralException ( "Error in Cached Row access", e);
		}

	}

}
