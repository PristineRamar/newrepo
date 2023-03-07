package com.pristine.dao.customer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.customer.CustomerSegmentDataDTO;
import com.pristine.dto.customer.CustomerSegmentDefDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class CustomerSegmentDAO {
	
static Logger logger = Logger.getLogger("CustomerSegmentDAO");
	
	public static final String INSERT_CUSTOMER_SEGMENT_DATA = "INSERT INTO CUSTOMER_SEGMENT_DATA " +
			"( CUSTOMER_SEGMENT_ID, STORE_ID, CALENDAR_ID, CUSTOMER_ID, VISIT_SEGMENT_ID, REVENUE_SEGMENT_ID)"+
		    " VALUES (CUSTOMER_SEGMENT_DATA_SEQ.nextval, ?, ?, ?, ?, ? )";
	
	public boolean insertCustomerSegmentData(Connection conn, List<CustomerSegmentDataDTO> custSegList) throws GeneralException,SQLException {	
		
		
		 PreparedStatement statement = null;
			try{				    	    
				statement = conn.prepareStatement(INSERT_CUSTOMER_SEGMENT_DATA);
				int itemNoInBatch = 0;
				
				for(CustomerSegmentDataDTO custSeg: custSegList){	

					statement.setDouble(1, custSeg.getCompStrId());					
					statement.setDouble(2, custSeg.getCalendarId());	
					
					if(custSeg.getCustomerId()!=null)
						statement.setDouble(3, custSeg.getCustomerId());			
					else
						statement.setNull(3, java.sql.Types.DOUBLE);
					
					statement.setDouble(4, custSeg.getVisitSegmentId());						
					statement.setDouble(5, custSeg.getRevSegmentId());															
					statement.addBatch();
		        	itemNoInBatch++;

		        	if(itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0){
		        		long startTime = System.currentTimeMillis();
		        		int[] count = statement.executeBatch();
		        		long endTime = System.currentTimeMillis();
		        		logger.debug("Time taken for insert:" + (endTime - startTime) + "ms");
		        		statement.clearBatch();
		        		itemNoInBatch = 0;
		        		PristineDBUtil.commitTransaction(conn, "Data Load");
		        		logger.debug("Number of records inserted:"+count.length);
		        	}
		        }
		        if(itemNoInBatch > 0){
		        	int[] count = statement.executeBatch();
		        	PristineDBUtil.commitTransaction(conn, "Data Load");
	        		statement.clearBatch();
	        		logger.debug("Number of records inserted:"+count.length);
		        }		
			}catch (SQLException e)
			{
				logger.error("Error while executing INSERT_CUSTOMER_SEGMENT_DATA"+e);
				throw new GeneralException("Error while executing INSERT_CUSTOMER_SEGMENT_DATA", e);
			}finally{
				PristineDBUtil.close(statement);
			}
				return true;
	}
	
	
	
	public ArrayList<CustomerSegmentDefDTO> getSegmentDataForStore(Connection conn, int storeId, int segmentType) 
										throws GeneralException, SQLException{
		
		StringBuffer sb = new StringBuffer();
		
		ArrayList<CustomerSegmentDefDTO> _segmentList = new ArrayList<CustomerSegmentDefDTO>();
		
		sb.append("SELECT D.SEGMENT_TYPE, D.SEGMENT_ID, D.RANGE_FROM, ");
		sb.append(" D.RANGE_TO, R.MIN_VALUE, R.MAX_VALUE");
		sb.append("	FROM CUSTOMER_SEGMENT_DEF D");
		sb.append(" LEFT OUTER JOIN CUSTOMER_SEGMENT_RANGE R");
		sb.append(" ON D.SEGMENT_TYPE = R.SEGMENT_TYPE");
		sb.append(" AND D.SEGMENT_ID = R.SEGMENT_ID");
		sb.append(" AND STORE_ID = ").append(storeId);
		sb.append(" WHERE D.SEGMENT_TYPE = ").append(segmentType);
		sb.append(" ORDER BY D.SORTING_ORDER");
		logger.debug("getSegmentDataForStore SQL:" + sb.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getCustomerSegmentData");

		if (result.size() > 0){

			while(result.next()){
				CustomerSegmentDefDTO visitDto = new CustomerSegmentDefDTO();
				visitDto.setSegmentId(result.getDouble("SEGMENT_ID"));
				visitDto.setRangeFrom(result.getDouble("RANGE_FROM"));
				visitDto.setRangeTo(result.getDouble("RANGE_TO"));
				visitDto.setRevenueMinRange(result.getDouble("MIN_VALUE"));
				visitDto.setRevenueMaxRange(result.getDouble("MAX_VALUE"));
				_segmentList.add(visitDto);
			}
		}
		return _segmentList;

	}
	
	
	public CachedRowSet getCustomerSegmentData(Connection conn, int type) throws GeneralException, SQLException{
		
		StringBuffer sbb = new StringBuffer("select SEGMENT_ID,SEGMENT_CLASSIFICATION,SEGMENT_TYPE,SEGMENT_NAME,SEGMENT_DESC,RANGE_FROM,RANGE_TO,SORTING_ORDER from customer_segment_def where SEGMENT_TYPE="+type);	    
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sbb, "getCustomerSegmentData");
		return result;
		
	}

	
}
