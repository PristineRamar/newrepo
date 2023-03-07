package com.pristine.dataload;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dao.DBManager;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class MergeSchedule {

	static Logger	logger	= Logger.getLogger("com.pristine.analysis.MergeSchedule");


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertyManager.initialize(MergeSchedule.class.getClassLoader().getResourceAsStream(Constants.ANALYSIS_CONFIG_FILE));
		PropertyManager.initialize(MergeSchedule.class.getClassLoader().getResourceAsStream(Constants.CONST_DB_FILENAME));
		
		if ( args.length != 3){
			logger.info("Insufficient parameters - List Id, Start date (mm/dd/yy) and End Date (mm/dd/yy)");
		}
		int checkListId = Integer.parseInt(args[0]);
		try {
			MergeSchedule ms  = new MergeSchedule();
			ms.performScheduleMerging(checkListId, args[1], args[2]);
		} catch (GeneralException ge){
			ge.logException(logger);
		}
	
	}
	
	public void performScheduleMerging( int checkListId, String startDate, String endDate) throws GeneralException {
		
		//get the stores for the start and end dates
		
		// get the schedules
		
		//for each pair, call mergeSchedule
		
		Connection conn = null;
		try {
			conn = DBManager.getConnection();
			ScheduleDAO schDao = new ScheduleDAO();
			CachedRowSet crs = schDao.getDuplicateStoreChecks(conn, checkListId, startDate, endDate);
			while ( crs.next()){
			
				int compStoreId = crs.getInt("comp_str_id");
				String schStartDate = crs.getString("start_date");
				String schEndDate = crs.getString("end_date");
				
				int primarySch = -1;
				
				ArrayList <ScheduleInfoDTO> schList = 
					schDao.getSchedulesForStore(conn, compStoreId, checkListId, schStartDate, schEndDate);
			
				Iterator <ScheduleInfoDTO> schItr =  schList.iterator();
				while (schItr.hasNext() ){
					ScheduleInfoDTO schInfo = schItr.next();
					if ( primarySch == -1)
						primarySch = schInfo.getScheduleId();
					else
						schDao.mergeSchedule(conn, primarySch, schInfo.getScheduleId());
						
				}
			}
			
		}catch ( SQLException e){
			throw new GeneralException( "Cached Rowset exception likely...",e);
		}
		
		finally {
			
			if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE")){
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Data Load");
			}
			else{
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "Data Load");
			}
			logger.info("Data Load successfully completed");
			if( conn != null )
				PristineDBUtil.close(conn);
		}
		
	}
	

}
