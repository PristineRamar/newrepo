package com.pristine.customer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.customer.CustomerItem.CustomerPurchaseData;
import com.pristine.dao.customer.CustomerDAO;
import com.pristine.dto.customer.CustomerDTO;
import com.pristine.dto.customer.CustomerPredictionDTO;
import com.pristine.dto.customer.CustomerPurchaseDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;

public class CustomerSegment extends CustomerItem
{
	public CustomerSegment (Date weekStart, Date weekEnd)
	{
		super (weekStart, weekEnd);
	}

	protected void deleteCustomerPurchase (CustomerDAO dao)
	{
		dao.deleteCustomerSegmentPurchase (_Conn, _StoreId, _ReportDate);
	}
	
	protected void deletePrediction (CustomerDAO dao)
	{
		dao.deleteSegmentPrediction (_Conn, _StoreId, _ReportDate, _PredictionWeekDate);
	}
	
	public CachedRowSet getCustomerData (CustomerDAO dao, String storeNum, int custIdFirst, int custIdLast)
	{
		return dao.getCustomerSegments (_Conn, storeNum, _WeekStartDate, _WeekEndDate, custIdFirst, custIdLast, null, true);
	}
	
	protected void loadPurchased (CachedRowSet customers)
	{
		loadSegmentPurchased(customers);
	}
	
	private void loadSegmentPurchased (CachedRowSet customers)
	{
		logger.info("loadSegmentPurchased begin: processing Segment results");
		
		Map<Integer, Map<Integer, CustomerPurchaseData>> customerDataMap = new HashMap<Integer, Map<Integer, CustomerPurchaseData>>();;
		Map<Integer, CustomerPurchaseData> segmentDataMap = null;
		
		try
		{
			int curCustomerId = -1;
			int curSegmentId = -1;
			String curSegmentName = null;
			double curQuantity = 0;

			Calendar cal = Calendar.getInstance();
			Date curDate = new Date();
			cal.setTime(curDate);
			cal.set(Calendar.YEAR, 2000);
			curDate = cal.getTime(); 
			
			int[] primaryDays = null;
			boolean curWeightedItem = false;

			_Count = 0;
			_CountFailed = 0;
			int customerCount = 0;
			while ( customers.next() )
			{
				int customerId;
				int segmentId;
				double quantity;
				Date timestamp;
				boolean weightedSegment;
				
				try {
					segmentId = customers.getInt("SEGMENT_ID");
					
					customerId = customers.getInt("ID");
					timestamp = customers.getDate("POS_TIMESTAMP");
					int qty = customers.getInt("QUANTITY");
					if ( qty > 0 ) {
						quantity = qty;
						weightedSegment = false;
					}
					else {
						quantity = customers.getDouble("WEIGHT");
						weightedSegment = true;
					}
				}
				catch (SQLException sex) {
					logger.error("loadSegmentPurchased: error accessing result data", sex);
					continue;
				}

				try
				{
					cal = Calendar.getInstance();
					cal.setTime(timestamp);
					int day = cal.get(Calendar.DAY_OF_WEEK);
					Date date = DateUtil.getPureDate(timestamp);
					
					if ( customerId != curCustomerId )
					{
						// different card
						if ( curCustomerId != -1 )
						{
							if ( curQuantity > 0 ) {
								updateSegmentData (segmentDataMap, curSegmentId, curSegmentName, curQuantity, curDate, curWeightedItem);
							}
							
							customerCount++;
							// predict and save to database  
							if ( customerCount == 500 ) {
								processPurchased (customerDataMap);
								customerDataMap = new HashMap<Integer, Map<Integer, CustomerPurchaseData>>();
								customerCount = 0;
							}
						}
	
						segmentDataMap = new HashMap<Integer, CustomerPurchaseData>();
						customerDataMap.put(customerId, segmentDataMap);
						
						primaryDays = getPrimaryShoppingDays(customerId);
	
						curSegmentId = -1;
						curWeightedItem = false;
						curCustomerId = customerId;
					}
					
					// different segment?
					if ( segmentId != curSegmentId )
					{
						if ( curSegmentId != -1 ) {
							if ( curQuantity > 0 )
								updateSegmentData (segmentDataMap, curSegmentId, curSegmentName, curQuantity, curDate, curWeightedItem);
						}
	
						cal.setTime(curDate);
						cal.set(Calendar.YEAR, 2000);
						curDate = cal.getTime();
						
						curSegmentId = segmentId;
						curWeightedItem = weightedSegment;
						curQuantity = 0;
					}

					// different date?
					if ( !date.equals(curDate) )
					{
						cal.setTime(curDate);
						if ( cal.get(Calendar.YEAR) != 2000 ) {
							if ( curQuantity > 0 )
								updateSegmentData (segmentDataMap, curSegmentId, curSegmentName, curQuantity, curDate, curWeightedItem);
						}
						
						curDate = date;
						curQuantity = 0;
					}

					curQuantity += quantity;
						
					_Count++;
					if ( _Count % 100000 == 0 )
						logger.info("loadSegmentPurchased: Processed " + String.valueOf(_Count) + " records");
				}
				catch (Exception ex) {
					logger.error("loadSegmentPurchased error: customer=" + customerId, ex);
					_CountFailed++;
				}
			} // while
			
			// add last customer data
			updateSegmentData (segmentDataMap, curSegmentId, curSegmentName, curQuantity, curDate, curWeightedItem);
			processPurchased (customerDataMap);
		}
		catch (Exception ex) {
			logger.error("loadSegmentPurchased error:", ex);
		}
		
		logger.info("loadSegmentPurchased end: processing Segment results");
	}

	protected int updatePrediction (CustomerDAO dao, List<CustomerPredictionDTO> customers)
	{
		return dao.updateSegmentPrediction (_Conn, customers, _StoreNum, _ReportDate, _PredictionWeekDate);
	}
	
	protected void completeData (Map<Integer, Map<Integer, CustomerPurchaseData>> customerSegmentDataMap)
	{
		Set<Integer> custKeys = customerSegmentDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		while ( custIter.hasNext() )
		{
			int customerId = custIter.next();
			CustomerDTO customer = _PrimaryShoppingDays.get(customerId);
			if ( customer == null )
				continue;
			
			Map<Integer, CustomerPurchaseData> segmentDataMap = customerSegmentDataMap.get(customerId);
			Set<Integer> keys = segmentDataMap.keySet();
			Iterator<Integer> iter = keys.iterator();
			_LessThan6Times = new ArrayList<String>();
			_MoreThan3InAWeek = new ArrayList<String>();
			_NoneIn8Weeks = new ArrayList<String>();
			while ( iter.hasNext() )
			{
				int segmentId = iter.next();
				CustomerPurchaseData customerSegmentData = segmentDataMap.get(segmentId);
				setCustomerData(customer, segmentId, customerSegmentData);
			}	// while item
			
/*
			String tmpStr = "completeItemData::" + customer.customerNo;
			logger.debug (tmpStr + " LessThan6Times rule: count=" + _LessThan6Times.size() + " - " + Util.getCommaList (_LessThan6Times));
			if ( _MoreThan3InAWeek.size() > 0 )
				logger.debug (tmpStr + " MoreThan3InAWeek rule: count=" + _MoreThan3InAWeek.size() + " - " + Util.getCommaList (_MoreThan3InAWeek));
			logger.debug (tmpStr + " NoneIn8Weeks rule: count=" + _NoneIn8Weeks.size() + " - " + Util.getCommaList (_NoneIn8Weeks));
*/
		}	// while customer
	}
	
	private void updateSegmentData (Map<Integer, CustomerPurchaseData> segmentDataMap, int segmentId, String segmentName,
			double quantity, Date date, boolean weightedSegment)
	{
		if ( quantity > 0.0001 )
		{
			CustomerPurchaseData customerSegmentData = segmentDataMap.get(segmentId);
			if ( customerSegmentData == null ) {
				customerSegmentData = new CustomerPurchaseData();
				segmentDataMap.put(segmentId, customerSegmentData);
				customerSegmentData.segmentId = segmentId;
				customerSegmentData.quantity = new ArrayList<Double>(); 
				customerSegmentData.weeklyVisitCounts = Util.get52WeeklyVisitCounts(); 
				customerSegmentData.dailyCounts = Util.getIntArray(365); 
			}
			if ( customerSegmentData != null )
			{
				customerSegmentData.quantity.add(quantity);
				customerSegmentData.weighted = weightedSegment;
				
				// update day counts
				if ( customerSegmentData.lastPurchaseDate != null )
				{
					int dayDiff = (int) DateUtil.getDateDiff(date, customerSegmentData.lastPurchaseDate);
					if ( dayDiff <= 8 ) {
						customerSegmentData.count7Day++;
					}
					else if ( dayDiff <= 16 ) {
						customerSegmentData.count15Day++;
					}
					else if ( dayDiff <= 29 ) {
						customerSegmentData.count28Day++;
					}
					else if ( dayDiff <= 43 ) {
						customerSegmentData.count42Day++;
					}
				}
				
				int day = (int) DateUtil.getDateDiff(date, _WeekStartDate);
				customerSegmentData.dailyCounts[day] = 1;
				
				customerSegmentData.countTotal++;
				customerSegmentData.lastPurchaseDate = date;
				
				int weekDiff = (int) (DateUtil.getDateDiff(date, _WeekStartDate) / 7);
				customerSegmentData.weeklyVisitCounts[weekDiff]++;
			}
		}
	}

	protected void updatePurchaseFrequency (Map<Integer, Map<Integer, CustomerPurchaseData>> customerSegmentDataMap, CustomerDAO dao)
	{
		logger.info("updatePurchaseFrequency begin: " + String.valueOf(customerSegmentDataMap.size()) + " customers");
		
		List<CustomerPurchaseDTO> customers = new ArrayList<CustomerPurchaseDTO>();
		
		Set<Integer> custKeys = customerSegmentDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		while ( custIter.hasNext() )
		{
			int customerId = custIter.next();
			CustomerDTO customer = _PrimaryShoppingDays.get(customerId);
			if ( customer == null )
				continue;
			
			Map<Integer, CustomerPurchaseData> segmentDataMap = customerSegmentDataMap.get(customerId);
			Set<Integer> itemKeys = segmentDataMap.keySet();
			Iterator<Integer> itemIter = itemKeys.iterator();
			int count = 0, unknownCount = 0;
			while ( itemIter.hasNext() )
			{
				count++;
				
				int segmentId = itemIter.next();
				CustomerPurchaseData customerSegmentData = segmentDataMap.get(segmentId);
				if ( customerSegmentData != null && customerSegmentData.averageQuantity > 0.0001 )
				{
					CustomerPurchaseDTO dto = new CustomerPurchaseDTO();
					dto.customerId = customerId;
					dto.lastPurchaseDate = customerSegmentData.lastPurchaseDate;
					dto.segmentId = segmentId;
					dto.quantity = customerSegmentData.averageQuantity;
					dto.totalCount = customerSegmentData.countTotal;
					dto.enabled = customerSegmentData.meetsCriteria;
					customers.add(dto);
				}
			}
		}

		int count = dao.updateCustomerSegmentPurchase (_Conn, customers, _StoreId, _ReportDate);
		
		logger.info("updatePurchaseFrequency end: " + String.valueOf(count) + " records created/updated");
	}
	
	protected List<CustomerPredictionDTO> predictPurchases (Map<Integer, Map<Integer, CustomerPurchaseData>> customerSegmentDataMap, Date weekStartDate)
	{
		logger.info("predictPurchases begin: for " + weekStartDate.toString() + ", " + String.valueOf(customerSegmentDataMap.size()) + " customers");
		
		List<CustomerPredictionDTO> list = new ArrayList<CustomerPredictionDTO>();
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(weekStartDate);
		cal.add(Calendar.DATE, 7);
		Date weekEndDate = cal.getTime();
		
		Set<Integer> custKeys = customerSegmentDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		while ( custIter.hasNext() )
		{
			int customerId = custIter.next();
			CustomerDTO customer = _PrimaryShoppingDays.get(customerId);
			if ( customer == null )
				continue;

			// will customer visit in this week?
			//boolean willVisit = willCustomerVisit (customer, weekStartDate, weekEndDate);
			boolean willVisit = true;
			if ( !willVisit )
				continue;

			Map<Integer, CustomerPurchaseData> segmentDataMap = customerSegmentDataMap.get(customerId);
			_Prediction = new HashMap<Integer, List<Integer>>();
			for ( int ii = 0; ii < 7; ii++ ) {
				_Prediction.put(ii, new ArrayList<Integer>());
			}
			Set<Integer> keys = segmentDataMap.keySet();
			Iterator<Integer> iter = keys.iterator();
			while ( iter.hasNext() )
			{
				int segmentId = iter.next();
				CustomerPurchaseData customerItemData = segmentDataMap.get(segmentId);
				if ( customerItemData != null && customerItemData.averageQuantity > 0.0001 && customerItemData.meetsCriteria ) {
					boolean canPredict = canPredictPurchaseDay (customerId, segmentId, customerItemData, weekStartDate, weekEndDate);
				}
			}

			//logDayCounts (customerId, _Prediction);
			
			// check which day customer will visit
			int maxCountDay = getMaxItemCountDay(_Prediction);
			if ( maxCountDay != -1 )
			{
				int maxCount = 0;
				List<Integer> maxCountItems = null;
				try {
					maxCountItems = _Prediction.remove(maxCountDay);
					maxCount = maxCountItems.size();
				}
				catch (NullPointerException e) { 
					maxCount = 0;
				}
			
				//if ( (double)maxCount / totalCount > 0.5 )	// more than 50% items
				{
					addPredictionDTO (list, weekStartDate, customerId, segmentDataMap, maxCountItems, maxCountDay, -1);
					for ( int ii = 0; ii < _Prediction.size(); ii++ )
						addPredictionDTO (list, weekStartDate, customerId, segmentDataMap, _Prediction.get(ii), maxCountDay, -1);
				}
			}
		}
		
		logger.info("predictPurchases end");
		
		return list;
	}

	private void addPredictionDTO (List<CustomerPredictionDTO> list, Date weekStartDate, int customerId, Map<Integer,
			CustomerPurchaseData> segmentDataMap, List<Integer> segmentIds, int day, int day2)
	{
		if ( segmentIds == null )
			return;
		
		for ( int segmentId : segmentIds )
		{
			CustomerPredictionDTO custPredictionDTO = new CustomerPredictionDTO();
			
			CustomerPurchaseData customerItemData = segmentDataMap.get(segmentId);
			if ( customerItemData != null ) {
				custPredictionDTO.customerId = customerId;
				custPredictionDTO.segmentId = customerItemData.segmentId;
				custPredictionDTO.weekStartDate = weekStartDate;
				custPredictionDTO.quantity = customerItemData.averageQuantity;
				
				boolean exists = containsPrediction (list, custPredictionDTO);
				if ( !exists )
					list.add(custPredictionDTO);
			}
			else {
			}
		}
	}
	
	private boolean containsPrediction (List<CustomerPredictionDTO> list, CustomerPredictionDTO prediction)
	{
		for ( CustomerPredictionDTO elem : list )
		{
			boolean same = elem.customerId == prediction.customerId && elem.segmentId == prediction.segmentId;
			if ( same )
				return true;
		}
		return false;
	}
	
	private static Logger logger = Logger.getLogger("CustomerSegment");
}
