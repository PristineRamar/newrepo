package com.pristine.consolidatedreco;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ConsolidatedRecommendation {
	
	Connection conn = null;
	private static Logger logger = Logger.getLogger("ConsolidatedRecommendation");
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String WEEK_START_DATES = "WEEK_START_DATES=";
	public int inputLocationLevelId = 0, inputLocationId = 0, inputProductId = 0, inputProductLevelId = 0;
	public String[] inputStartDates = null; 
	private static final String DELIMITER = ",";

	//Construction with Database connection
	/**
	 * Constructor to initialize DB connection at startup
	 */
	public ConsolidatedRecommendation() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ge) {
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-consolidated-recommendation.properties");
		PropertyManager.initialize("recommendation.properties");
		ConsolidatedRecommendation consolidatedRecommendation = new ConsolidatedRecommendation();

		for (String arg : args) {
			if (arg.startsWith(PRODUCT_ID)) {
				consolidatedRecommendation.inputProductId = Integer.parseInt(arg.substring(PRODUCT_ID.length()));
			} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
				consolidatedRecommendation.inputProductLevelId = Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length()));
			} else if (arg.startsWith(LOCATION_ID)) {
				consolidatedRecommendation.inputLocationId = Integer.parseInt(arg.substring(LOCATION_ID.length()));
			} else if (arg.startsWith(LOCATION_LEVEL_ID)) {
				consolidatedRecommendation.inputLocationLevelId = Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length()));
			} else if (arg.startsWith(WEEK_START_DATES)) {
				consolidatedRecommendation.inputStartDates = arg.substring(WEEK_START_DATES.length()).split(DELIMITER);
			}
		}

		logger.info("*****************************************************************************");
		logger.info("Starting Recommendation Consolidation Process");
		logger.info("*****************************************************************************");
		
		consolidatedRecommendation.consolidateRecommendation();
		
		logger.info("*****************************************************************************");
		logger.info("End of Recommendation Consolidation Process");
		logger.info("*****************************************************************************");

	}
	

	/**
	 * Consolidation main process which invokes respective method is Utility class
	 */
	private void consolidateRecommendation(){
		
		logger.debug("consolidateRecommendation() : Start of the method.");
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		ConsolidatedRecoUtil consolidatedRecoUtil = new ConsolidatedRecoUtil();
		ArrayList<RetailCalendarDTO> calendarList = new ArrayList<RetailCalendarDTO>();
		List<PRRecomDTO> prRecomDTOList = null;		
		HashMap<Integer, Long> runIdsList = null;
		TreeMap<Date,String> sortedStartDates = null;
		int calendarIdPRHeader = 0;
		RetailCalendarDTO retailCalendarDTO = null;
		
		logger.info("Input arguments are :  " );
		logger.info("PRODUCT_ID : " + inputProductId ); 
		logger.info("PRODUCT_LEVEL_ID : " + inputProductLevelId );
		logger.info("LOCATION_ID : " + inputLocationId );
		logger.info("LOCATION_LEVEL_ID : " + inputLocationLevelId );
		logger.info("WEEK_START_DATES : " + inputStartDates );
		
		try {
			
			logger.debug("consolidateRecommendation() :  sorting the list of Start Dates in input.");
			// sorting the list of Start Dates in input
			sortedStartDates = consolidatedRecoUtil.getSortedDate(inputStartDates);
			
			logger.debug("consolidateRecommendation() :  Retrieve next week for PR Header");
			//Get next week for PR Header
			//calendarIdPRHeader = consolidatedRecoUtil.
					//getNextCalendarId(conn, sortedStartDates.get(sortedStartDates.lastKey()));
			//Get quarter calendar id in which the first week falls
			
			//retailCalendarDTO = retailCalendarDAO.getQuarterDetail(conn, sortedStartDates.get(sortedStartDates.firstKey()));

			retailCalendarDTO = new RetailCalendarDTO();
			calendarIdPRHeader = retailCalendarDTO.getCalendarId();
			
			logger.debug("consolidateRecommendation() : Retrieving week details for which consolidation is required ");
			// Get week details for which consolidation is required 
			for(String startDate : sortedStartDates.values()){
				RetailCalendarDTO curCalDTO = retailCalendarDAO.getWeekCalendarFromDate(conn, startDate );
				calendarList.add(curCalDTO);
				//calendarIdPRHeader = curCalDTO.getCalendarId();
			}
			
			logger.debug("consolidateRecommendation() : Retrieving latest run id list for given weeks ");
			// Get latest run id list for given weeks
			runIdsList = consolidatedRecoUtil.getLatestRecommendationRunIdforCalendar(conn, calendarList, 
					inputLocationLevelId, inputLocationId, inputProductId);
			
			logger.debug("consolidateRecommendation() : Retrieving the prRecommendation Records List for given Run Ids ");
			//Get the prRecommendation Records List for given Run Ids		
			prRecomDTOList = consolidatedRecoUtil.getRecommendationItemsForConsolidation(conn, runIdsList);
			
			logger.debug("consolidateRecommendation() : Inititating Consolidation process for collected recommendation data for given weeks");
			//Inititate Consolidation process for collected recommendation data for given weeks		
			List<PRRecomDTO> listOutPut = consolidatedRecoUtil.processConsolidation(prRecomDTOList, 
					sortedStartDates, calendarList, runIdsList);
			
			logger.debug("consolidateRecommendation() : Saving consolidated Recommendation Data in Database");
			//Saving consolidated Recommendation Data in Database
			consolidatedRecoUtil.saveConsolidatedData(conn, calendarIdPRHeader, inputLocationLevelId ,
					inputLocationId , inputProductId , inputProductLevelId, listOutPut );
			
			logger.debug("consolidateRecommendation() : Consolidation successful.");
			logger.debug("consolidateRecommendation() : End of the method.");
			
		} catch (Exception | GeneralException ex) {
			ex.printStackTrace();
			PristineDBUtil.rollbackTransaction(conn, "Recommendation Consolidation changes are rolled back.");
			logger.error("Exception in recommendation consolidation process " + ex.toString() + ex + ex.getMessage());
		} finally {
			PristineDBUtil.close(conn);
		}
	}	
}
