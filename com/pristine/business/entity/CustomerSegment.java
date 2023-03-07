package com.pristine.business.entity;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dao.customer.CustomerSalesSummaryDAO;
import com.pristine.dao.customer.CustomerSegmentDAO;
import com.pristine.dto.customer.CustomerSegmentDataDTO;
import com.pristine.dto.customer.CustomerSegmentDefDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;

public class CustomerSegment {
	
	private static Logger logger = Logger.getLogger("CustomerSegment");	
	private int _segmentWeeks = 0;
	private double _visitBenchmark = 0;
	
    HashMap<String, CustomerSegmentDefDTO> _revMap;
	
	
	private CustomerSalesSummaryDAO custSalesDao;
	private CustomerSegmentDAO custSegDao;

	
	
	
	public CustomerSegment(){
				
		custSalesDao = new CustomerSalesSummaryDAO();
		custSegDao = new CustomerSegmentDAO();
		_segmentWeeks = Integer.parseInt(PropertyManager.getProperty("SEGMENT_CALC_WEEKS"));
		_visitBenchmark= Double.parseDouble(PropertyManager.getProperty("VISIT_BENCHMARK"));
	    _revMap = new HashMap<String, CustomerSegmentDefDTO>();
	    logger.debug("Segment calculation duration:" + _segmentWeeks + " Weeks");
	    logger.debug("Visit Benchmark:" + _visitBenchmark);	    
	    
	}	
	
	public void populateSegmentData(java.util.Date endDate, int storeId, 
												int calid, Connection conn) 
						throws GeneralException, SQLException,ParseException {						
		
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy");
		String date = sdf.format(endDate);
		
	    //Get the last 52 weeks summary data
	    logger.debug("Get customer summary data for " + _segmentWeeks + " weeks");																
		CachedRowSet result = custSalesDao.getCustomerWeeklySummaryData(conn, date, storeId, _segmentWeeks);

		ArrayList<CustomerSegmentDefDTO> visitList = new ArrayList<CustomerSegmentDefDTO>();
		ArrayList<CustomerSegmentDefDTO> revList = new ArrayList<CustomerSegmentDefDTO>();
		
		
		if ( result.size() >0 ){
		    logger.debug("Total customers..." +  result.size());

		    logger.debug("Get Visit segment definition");
		    visitList = custSegDao.getSegmentDataForStore(conn, storeId, 1);
		    
		    if (visitList.size() < 1)
		    	logger.warn("There is not Visit segment definition data");
		    else
		    	logger.debug("Total Visit Segments:" + visitList.size());

		    logger.debug("Get Revenue segment definition");
		    revList = custSegDao.getSegmentDataForStore(conn, storeId, 2);

		    if (revList.size() < 1)
		    	logger.warn("There is not Revenue segment definition data");
		    else
		    	logger.debug("Total Revenue Segments:" + revList.size());
		    
		    if (result.size() > 0 && visitList.size() > 0 && revList.size() > 0){
		    	
		    	logger.debug("Calculate the customer segment");
		    	List<CustomerSegmentDataDTO> segList = calculateCustomerSegment(result, visitList, revList, storeId, calid);

			    if (segList.size() >0){
			    	logger.debug("Total Customers visited during last 52 weeks:" 
		    												+ segList.size());
			    	
					logger.debug("Inserting customer segment data");
					custSegDao.insertCustomerSegmentData(conn, segList);
					logger.debug("Inserted customer segment data");
			    	
			    }
			    else{
			    	logger.info("There is no Customer Segment data");
			    }
	    	}
		}
	}
	
	private List<CustomerSegmentDataDTO> calculateCustomerSegment(CachedRowSet customerRS, 
								ArrayList<CustomerSegmentDefDTO> visitList, 
		ArrayList<CustomerSegmentDefDTO> revList, double storeId, double calid){
		
		List<CustomerSegmentDataDTO> segList = new ArrayList<CustomerSegmentDataDTO>();
		
		try {
			while(customerRS.next()){
				
				double totRevenue = customerRS.getDouble("TOT_REVENUE") / _segmentWeeks;
				double totVisits = customerRS.getDouble("TOT_VISITS");
				
				double customerVisitSegment =0;
				double customerRevSegment =0;
				
				//logger.debug("Customer Visit:" + totVisits);
				//logger.debug("Customer Revenue:" + totRevenue);
				
				for (int ii=0; ii < visitList.size(); ii++){
					
					CustomerSegmentDefDTO visitDto = new CustomerSegmentDefDTO();
					visitDto = visitList.get(ii);
					
					//logger.debug("From Range:" + visitDto.getRangeFrom());
					//logger.debug("To Range:" + visitDto.getRangeTo());
					
					double visitMinVal=0;
					double visitMaxVal =0;					
					int visitFlag = 0;
					int revFlag = 0;
					
					//Max Range
					if (visitDto.getRangeTo() == null || visitDto.getRangeTo() == 0){
						visitMinVal = _visitBenchmark * visitDto.getRangeFrom()/100;
						//logger.debug("Visit Min value" + visitMinVal);
						
						
						if (totVisits > visitMinVal){
							customerVisitSegment = visitDto.getSegmentId();
							visitFlag =1;
						}
					}

					//Min
					else if (visitDto.getRangeFrom() == null || visitDto.getRangeFrom() == 0){
						visitMaxVal = _visitBenchmark * visitDto.getRangeTo()/100;
						//logger.debug("Visit Max value" + visitMaxVal);
						
						if (totVisits < visitMaxVal){
							customerVisitSegment = visitDto.getSegmentId();
							visitFlag = 1;
						}
					}
					//Mid
					else if (visitDto.getRangeFrom() != null && visitDto.getRangeTo() != null){
						visitMinVal = _visitBenchmark * visitDto.getRangeFrom()/100;
						visitMaxVal = _visitBenchmark * visitDto.getRangeTo()/100;
						//logger.debug("Visit Min value" + visitMinVal);
						//logger.debug("Visit Max value" + visitMaxVal);
						
						if (totVisits > visitMinVal && totVisits <= visitMaxVal){
							customerVisitSegment = visitDto.getSegmentId();
							visitFlag = 1;							
						}
					}

					if (visitFlag == 1){  
						
						double revBenchmark = visitDto.getRevenueMaxRange() - visitDto.getRevenueMinRange();
						
						for (int jj=0; jj < revList.size(); jj++){
							CustomerSegmentDefDTO revDto = new CustomerSegmentDefDTO();
							revDto = revList.get(jj);

							double revMinVal=0;
							double revMaxVal =0;					
							
							//Max Range
							if (revDto.getRangeTo() == null || revDto.getRangeTo() == 0){
								revMinVal = revBenchmark * revDto.getRangeFrom()/100;
								//logger.debug("Revenue Min value" + revMinVal);
								
								if (totRevenue > revMinVal){
									customerRevSegment = revDto.getSegmentId();
									revFlag =1;
								}
							}

							//Min
							else if (revDto.getRangeFrom() == null || revDto.getRangeFrom() == 0){
								revMaxVal = revBenchmark * revDto.getRangeTo()/100;
								//logger.debug("Revenue Max value" + revMaxVal);
								
								if (totRevenue < revMaxVal){
									customerRevSegment = revDto.getSegmentId();
									revFlag = 1;
								}
							}
							//Mid
							else if (revDto.getRangeFrom() != null && revDto.getRangeTo() != null){
								revMinVal = revBenchmark * revDto.getRangeFrom()/100;
								revMaxVal = revBenchmark * revDto.getRangeTo()/100;
								//logger.debug("Revenue Min value" + revMinVal);
								//logger.debug("Revenue Max value" + revMaxVal);
								
								if (totRevenue > revMinVal && totRevenue <= revMaxVal){
									customerRevSegment = revDto.getSegmentId();
									revFlag = 1;
								}
							}
						
							//If revenue segment calculated then exit from the loop
							if (revFlag == 1) break;
						
						}
					}
				
					//If visit segment calculated then exit from the loop
					if (visitFlag == 1) break;
				
				}
				
				if (customerVisitSegment > 0){
					CustomerSegmentDataDTO custSegDto = new CustomerSegmentDataDTO();
					
					if(customerRS.getObject("CUSTOMER_ID")!=null)
						custSegDto.setCustomerId(customerRS.getDouble("CUSTOMER_ID"));
					else
						custSegDto.setCustomerId(null);
	
					custSegDto.setVisitSegmentId(customerVisitSegment);
					custSegDto.setRevSegmentId(customerRevSegment);
					custSegDto.setCompStrId(storeId);
					custSegDto.setCalendarId(calid);
					segList.add(custSegDto);
					
					//logger.debug("Customer ID:" + custSegDto.getCustomerId());
					//logger.debug("Revenue Segment:" + custSegDto.getRevSegmentId());
					//logger.debug("Visit Segment:" + custSegDto.getVisitSegmentId());
				}
			}
			
		} catch (SQLException e) {
			logger.error("Error in calculateCustomerSegment" + e);
		}
	
	return segList;
	}
	
}

