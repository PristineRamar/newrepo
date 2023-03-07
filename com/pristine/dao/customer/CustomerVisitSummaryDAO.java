package com.pristine.dao.customer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

//import com.pristine.dto.MovementDailyTransactionDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
//import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class CustomerVisitSummaryDAO {
	
	static Logger logger = Logger.getLogger("CustomerVisitSummaryDAO");
	
	public static final String INSERT_CUSTOMER_VISIT = "INSERT INTO " + 
			" CST_CUSTOMER_VISIT_SUMMARY ( STORE_ID, CUSTOMER_ID, CALENDAR_ID,"+
			" TRX_NO, SALE_MOVEMENT, REG_MOVEMENT, TOT_MOVEMENT, "+
			" SALE_MOVEMENT_VOL, REG_MOVEMENT_VOL, TOT_MOVEMENT_VOL," +
			" SALE_REVENUE, REG_REVENUE, TOT_REVENUE, SALE_MARGIN, REG_MARGIN,"+
			" TOT_MARGIN, SALE_MARGIN_PCT, REG_MARGIN_PCT, TOT_MARGIN_PCT ) "+ 
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	public boolean insertCustomerVisitSummary (Connection conn, 
			List<SummaryDataDTO> summaryDataList) throws GeneralException {						
		
	 PreparedStatement statement = null;
		try{				    	    
			statement = conn.prepareStatement(INSERT_CUSTOMER_VISIT);
			
			for(SummaryDataDTO summaryDto: summaryDataList){
				int counter = 0;

				statement.setDouble(++counter, summaryDto.getLocationId());
								
				if (summaryDto.getCustomerId() == 0){
					statement.setNull(++counter, java.sql.Types.INTEGER);
				}
				else{
					statement.setDouble(++counter, summaryDto.getCustomerId());
				}
				statement.setDouble(++counter, summaryDto.getcalendarId());
				statement.setDouble(++counter, summaryDto.getTransactionNo());
				statement.setDouble(++counter, summaryDto.getSaleMovement());
				statement.setDouble(++counter, summaryDto.getRegularMovement());				
				statement.setDouble(++counter, summaryDto.getTotalMovement());
				statement.setDouble(++counter, summaryDto.getsaleMovementVolume());
				statement.setDouble(++counter, summaryDto.getregMovementVolume());
				statement.setDouble(++counter, summaryDto.gettotMovementVolume() );
				statement.setDouble(++counter, summaryDto.getSaleRevenue());
				statement.setDouble(++counter, summaryDto.getRegularRevenue());
				//statement.setDouble(++counter, summaryDto.getTotalRevenue());
				statement.setDouble(++counter, summaryDto.getSaleRevenue() + summaryDto.getRegularRevenue());
				statement.setDouble(++counter, summaryDto.getSaleMargin());
				statement.setDouble(++counter, summaryDto.getRegularMargin());
				statement.setDouble(++counter, summaryDto.getTotalMargin());
				statement.setDouble(++counter, summaryDto.getSaleMarginPer());
				statement.setDouble(++counter, summaryDto.getRegularMarginPer());
				statement.setDouble(++counter, summaryDto.getTotalMarginPer());
				statement.addBatch();
	        	
	        }
	        	int[] count = statement.executeBatch();
	        	PristineDBUtil.commitTransaction(conn, "insertCustomerVisitSummary");
        		statement.clearBatch();
        		logger.debug("The number of records inserted: "+count.length);
		}catch (SQLException e)
		{
			logger.error("Error in insertCustomerVisitSummary - " + e);
			throw new GeneralException("Error", e);
		}finally{
			PristineDBUtil.close(statement);
		}
			return true;
}
	
	public void deleteCustomerVisitSummary(Connection conn, int storeId, 
							int calid) throws GeneralException,SQLException {
		try {
			StringBuffer sb = new StringBuffer();
			sb.append("delete from CST_CUSTOMER_VISIT_SUMMARY where STORE_ID = ");
			sb.append(storeId).append(" AND CALENDAR_ID =").append(calid);	    
			logger.debug("deleteCustomerVisitSummary SQL:" + sb);
			PristineDBUtil.execute(conn, sb, "deleteCustomerVisitSummary");		
			PristineDBUtil.commitTransaction(conn, "Data Deletion");
		} catch (GeneralException ex) {
			logger.error("Error while deleting Customer Visit Summary" + ex.getMessage());
			throw  ex;
		}
	}
	
	public CachedRowSet getCustomerVisitSummary(Connection conn, int storeId, 
							String calList) throws GeneralException,Exception{
				
		try{
			StringBuffer sb = new StringBuffer();			
			sb.append("select STORE_ID, CUSTOMER_ID, CALENDAR_ID");			
			sb.append(", TRX_NO ,SALE_MOVEMENT, REG_MOVEMENT");			
			sb.append(", TOT_MOVEMENT, SALE_REVENUE, REG_REVENUE, TOT_REVENUE");			
			sb.append(", SALE_MARGIN, REG_MARGIN, TOT_MARGIN");			
			sb.append(" from CUSTOMER_VISIT_SUMMARY");
			sb.append(" where STORE_ID = ").append(storeId);
			sb.append(" and CALENDAR_ID in ").append(calList);
			sb.append("order by CUSTOMER_ID");
			CachedRowSet result = null;
			logger.debug("getCustomerVisitSummary SQL :" + sb.toString());
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerVisitSummary");
			return result;
		}catch(Exception ex){
			logger.error("The exception is "+ex);
			conn.rollback();
			throw  ex;
		}
	}

}
