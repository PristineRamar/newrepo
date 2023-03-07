package com.pristine.dao.logger;

import java.sql.Connection;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import org.apache.log4j.Logger;

public class TransactionLogTrackerDAO {

	static Logger logger = Logger.getLogger("TransactionLogTrackerDAO");
	
	public int insertTLogTrack (Connection conn, String fileName, 
								String startDateTime, String processStatus)
			throws GeneralException
		{
		
		int trackId = 0;

		try {
			StringBuffer sb = new StringBuffer("INSERT INTO TLOG_TRACKER");

			//Insert Track Id			
			sb.append(" (TRACK_LOG_ID, FILENAME, LOAD_START_TIME, LOAD_STATUS");
			sb.append(") VALUES (TLOG_TRACKER_SEQ.nextval, '");
			sb.append(fileName).append("', To_DATE(").append(startDateTime);
			sb.append(", 'YYYYMMDDHH24MISS'), '").append(processStatus).append("')");
			logger.debug("insertTLogTrack Insert SQL:" + sb.toString());
			PristineDBUtil.execute(conn, sb, "insertTLogTrack - Insert");
			
			//Get the track Id
			sb = new StringBuffer();
			sb.append("select TLOG_TRACKER_SEQ.CURRval from dual");
			String retVal = PristineDBUtil.getSingleColumnVal(conn, sb, "GetTrackId");

			if( retVal != null ){
				trackId = Integer.parseInt(retVal);
			}
		}
		catch (GeneralException gex) {
			logger.error("Error while getting Track Id: " + gex.getMessage());
			throw gex;
		}
			return trackId;
	}

	public int UpdateTLogStatus (Connection conn, int trackId, 
		String endDateTime, String status, int processCnt, int rejectCnt) 
												throws GeneralException
	{
		try {
			StringBuffer sb = new StringBuffer("UPDATE TLOG_TRACKER SET ");
			sb.append(" LOAD_END_TIME=To_DATE(").append(endDateTime);
			sb.append(", 'YYYYMMDDHH24MISS')");
			sb.append(", PROCESSED_RECORD_CNT = ").append(processCnt); 
			sb.append(", REJECTED_RECORD_CNT = ").append(rejectCnt);
			sb.append(", LOAD_STATUS = '").append(status).append("'");			
			sb.append(" WHERE TRACK_LOG_ID=").append(trackId);
	
			logger.debug("UpdateTLogStatus SQL:" + sb.toString());
			PristineDBUtil.execute(conn, sb, "UpdateTLogStatus - Update");
		}
		catch (Exception ex) {
			logger.error("UpdateTLogStatus() - Error while updating TLog status : " + ex.getMessage());
			logger.info("UpdateTLogStatus() - Continuing operation...");
			// throw gex;
		}
		return trackId;
	}
}