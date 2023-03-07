package com.pristine.service.offermgmt.oos;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.offermgmt.oos.AvgMovementCalculationDAO;
import com.pristine.dao.offermgmt.oos.OOSAnalysisDAO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.DayPartLookupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

/**
 * Find the weekly average movement of store and each time slot of a day
 * @author NAGARAJ
 *
 */
public class AverageMovementCalculation {
	private static Logger logger = Logger.getLogger("AverageMovementCalculator");
	private Connection conn = null;
	private int locationLevelId = 0;
	private int locationId = 0;
	
	public AverageMovementCalculation() {
		PropertyConfigurator.configure("log4j-avg-mov-cal.properties");
		PropertyManager.initialize("recommendation.properties");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			System.exit(1);
		}
	}

	public static void main(String[] args) throws Exception {
		AverageMovementCalculation amc = new AverageMovementCalculation();
		
		//Get the location_level_id and location_id from argument
		if(args.length > 0){
			for(String arg : args){
				if(arg.startsWith("LOCATION_LEVEL_ID")){
					amc.setLocationLevelId(Integer.parseInt(arg.substring("LOCATION_LEVEL_ID=".length())));
				}else if(arg.startsWith("LOCATION_ID")){
					amc.setLocationId(Integer.parseInt(arg.substring("LOCATION_ID=".length())));
				}
			}			
		}
		logger.info("**********************************************");
		amc.calculateAverageMovement();
		logger.info("**********************************************");
	}
	
	/***
	 * Calculate weekly average movement for store and each time slot of the day
	 */
	private void calculateAverageMovement(){
		try{
			List<LocationKey> locationsToProcess = null;
			AvgMovementCalculationDAO arcDAO = new AvgMovementCalculationDAO();
			OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
			int noOfWeeksHistory = Integer.parseInt(PropertyManager.getProperty("WEEKLY_AVG_MOV_HISTORY"));
			List<DayPartLookupDTO> dayPartLookup = new ArrayList<DayPartLookupDTO>();
			
			locationsToProcess = setLocations();

			//Get Day Part Lookup
			dayPartLookup = oosAnalysisDAO.getDayPartLookup(conn);
			
			//Delete location level average movement
			logger.info("Deletion of Store Level Average Movement is Started...");
			arcDAO.deleteWeeklyAverageMovement(conn, locationsToProcess);
			logger.info("Deletion of Store Level Average Movement is Completed...");
			
			//Delete day part level average movement
			logger.info("Deletion of Day Part Level Average Movement is Started...");
			arcDAO.deleteDayPartAverageMovement(conn, locationsToProcess);
			logger.info("Deletion of Day Part Level Average Movement is Completed...");
			
			//Run store by store
			for(LocationKey locationKey: locationsToProcess){
				logger.info("Processing of Location: " + locationKey.toString() + " is Started... ");
				
				logger.info("Finding Store Average Movement");
				//Get and insert store average movement
				arcDAO.getAndInsertItemLevelStoreAvgMovement(conn, noOfWeeksHistory, locationKey.getLocationId());
				
				logger.info("Finding Day Part Average Movement");
				//Get and insert each day's time slot average movement
				arcDAO.getAndInsertItemLevelDayPartAvgMovement(conn, dayPartLookup, noOfWeeksHistory, locationKey.getLocationId());
				
				logger.info("Processing of Location: " + locationKey.toString() + " is Completed... ");	
			}
			
			PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
		} catch(Exception | GeneralException e){
			e.printStackTrace();
			PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
			logger.error("Exception in calculateAverageMovement and transaction is rollbacked " + e + e.getMessage());			
		}finally{
			PristineDBUtil.close(conn);
		}
	}
	
	
	/***
	 * Set all the locations to be processed
	 * @return
	 */
	private List<LocationKey> setLocations() {
		List<LocationKey> locationsToProcess = new ArrayList<LocationKey>();
		// If location is specified in the argument, process only for that store
		if (locationId > 0 && locationLevelId > 0) {
			LocationKey locationKey = new LocationKey(locationLevelId, locationId);
			locationsToProcess.add(locationKey);
		} else {
			// TODO:: Get all the active stores
		}
		return locationsToProcess;
	}
	
	private void setLocationLevelId(int locationLevelId){
		this.locationLevelId = locationLevelId;
	}
	
	private void setLocationId(int locationId){
		this.locationId = locationId;
	}
}
