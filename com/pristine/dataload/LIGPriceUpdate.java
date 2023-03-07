package com.pristine.dataload;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.sql.rowset.CachedRowSet;



import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompDataDAOLIG;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.CompositeDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class LIGPriceUpdate {

	
	private static Logger logger = Logger.getLogger("LIGPriceUpdate");
	private CompDataDAOLIG compDaoLIG = new CompDataDAOLIG();
	private static String LOCATION = "LOCATION_ID=";
	private static String LOCATION_LEVEL = "LOCATION_LEVEL_ID=";
	
	public static void main(String[] args) {
		if (args.length > 2) {
			logger.debug("Incorrect Arguments - LIGPriceUpdate [scheduleId] or [ C Comp Store Id] or [ Z Price Zone Id]");
			System.exit(1);
		}
		PropertyConfigurator.configure("log4j-lig.properties");
		

		PropertyManager.initialize("analysis.properties");

		Connection conn = null;
		try {
			
			LIGPriceUpdate ligUpdater = new LIGPriceUpdate ();
		    conn = DBManager.getConnection();
			int scheduleId;
			if( args.length > 1){
				String startDate = DateUtil.getWeekStartDate(1);
				Date endDate = DateUtil.incrementDate(DateUtil.toDate(startDate), 6);
				String endDateStr = DateUtil.dateToString(endDate, Constants.APP_DATE_FORMAT);
				
				logger.info( "Updating Prices for " + startDate + " - " + endDateStr);
				
				// Changes for Price Index By Price Zone
				CompetitiveDataDAO compDataDao = new CompetitiveDataDAO(conn);
				int schId = -1;
				CompetitiveDataDTO compDto = new CompetitiveDataDTO();
				compDto.weekStartDate = startDate;
				compDto.weekEndDate = endDateStr;
				if("Z".equalsIgnoreCase(args[0])){
					ScheduleDAO schDAO = new ScheduleDAO();
					schId = schDAO.populateScheduleIdForZone(conn, Integer.parseInt(args[1]), startDate, endDateStr);
					logger.info("Processing for schedule : " + schId);
				}
				else if(args[0].startsWith(LOCATION_LEVEL)){
					int locationLevelId = 0;
					int locationId = 0;
					if (args[1].startsWith(LOCATION)) {
						locationLevelId = Integer.parseInt(args[0].substring(LOCATION_LEVEL.length()));
						locationId = Integer.parseInt(args[1].substring(LOCATION.length()));
					}
					ScheduleDAO schDAO = new ScheduleDAO();
					schId = schDAO.populateScheduleIdForLocation(conn, locationLevelId, locationId, startDate, endDateStr);
					logger.info("Processing for schedule : " + schId);
				}else{
					compDto.compStrId = Integer.parseInt(args[1]);
					schId = compDataDao.getScheduleID(conn, compDto, false);
				}
				//get the scheduleId
				
				//Call the 

				if( schId > 0){
					ligUpdater.doLIGUpdate(schId, conn);
				}else{
					logger.error("LIG Price Update did not happen for Store - " + args[1]);
				}
		
			}
			else if (args.length == 1){ 
				scheduleId = Integer.parseInt(args[0]);
				ligUpdater.doLIGUpdate(scheduleId, conn);
			} 
			
			else {
				ArrayList <Integer> scheduleIdList =  ligUpdater.findScheduleList(conn);
				logger.info("Doing LIG Price Update for " +  scheduleIdList.size() + " Stores");
				Iterator <Integer> itr = scheduleIdList.iterator();
			    while (itr.hasNext()){
			      scheduleId = itr.next();
			      ligUpdater.doLIGUpdate(scheduleId, conn);
			    }

			}


		} catch (GeneralException ge) {
			logger.error("LIG Price Update", ge);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}catch (Exception e) {
			logger.error("LIG Price Update", e);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}finally{
			PristineDBUtil.close(conn);
		}

		
}
	private  void doLIGUpdate(int scheduleId, Connection conn) throws GeneralException {

		
		logger.info("Starting LIG Price Update for " + scheduleId);
	//select items from lig where movement > 0
		CachedRowSet crsLigItems = compDaoLIG.getLigItemsMovement(conn, scheduleId);
		List<CompositeDTO> compositeDtoList = new ArrayList<CompositeDTO>(); // Changes for performance improvement
		try {
			while ( crsLigItems.next()){
				int checkData = crsLigItems.getInt("check_data_id");
				int ligItemCode=crsLigItems.getInt("item_code");
				CompositeDTO compositeData = compDaoLIG.getLigMaxItemMovement(conn, scheduleId, ligItemCode);
				if( compositeData != null ){
					//Update Price Price
					compositeData.compData.checkItemId = checkData;
					compositeData.compData.itemcode = ligItemCode;
					compositeData.costData.checkItemId = checkData;
					// compDaoLIG.updateLIGPriceInfo(conn, compositeData.compData);
					// compDaoLIG.updateLIGCostInfo(conn, compositeData.costData);
					compositeDtoList.add(compositeData); // Changes for performance improvement
					// Update the cost
				}

			}
			
			// Changes for performance improvement
			compDaoLIG.updateLIGPriceInfo(conn, compositeDtoList);
			compDaoLIG.updateLIGCostInfo(conn, compositeDtoList);
			// Changes for performance improvement - Ends
		}catch(SQLException  sqle){
			throw new GeneralException ("CachedRowSet Exception", sqle);
		}
		
		PristineDBUtil.commitTransaction(conn,"LIGPriceUpdate");
		logger.info("LIG Price Update for " + scheduleId + " Done.");

		
	}
	private  ArrayList<Integer> findScheduleList(Connection conn) throws GeneralException, SQLException {
		// TODO Auto-generated method stub

		//Get the schedule
		//Call Lig Price Update
		ArrayList<Integer> schList = new ArrayList<Integer> ();
		//Get Start and End Date
		String startDate = DateUtil.getWeekStartDate(1);
		String endDate = DateUtil.getWeekEndDate(DateUtil.toDate(startDate));

		//Get the list of stores
		CompStoreDAO storeDAO = new CompStoreDAO ();
		CompetitiveDataDAO compDataDAO  = new CompetitiveDataDAO(conn);
	
		CachedRowSet storeList = storeDAO.getKeyStoreList(conn);
		while (storeList.next()){
			CompetitiveDataDTO compData  = new CompetitiveDataDTO();
			compData.weekStartDate = startDate;
			compData.weekEndDate = endDate;
			compData.compStrId = storeList.getInt("COMP_STR_ID");
			int schId = compDataDAO.getScheduleID(conn, compData, false);
			if(schId > 0 )
				schList.add(schId);
			
		}
		

		return schList;
		
	}
}