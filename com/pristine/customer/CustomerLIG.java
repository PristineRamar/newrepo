package com.pristine.customer;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
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
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pristine.customer.CustomerItem.CustomerPurchaseData;
import com.pristine.dao.customer.CustomerDAO;
import com.pristine.dto.customer.CustomerDTO;
import com.pristine.dto.customer.CustomerPredictionDTO;
import com.pristine.dto.customer.CustomerPurchaseDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;

public class CustomerLIG extends CustomerItem
{

	public CustomerLIG (Date weekStart, Date weekEnd)
	{
		super (weekStart, weekEnd);
	}

	protected void doAdditionalPreperation (CustomerDAO dao)
	{
		// Load LIGs of items that were predicted
		List<CustomerPredictionDTO> list = dao.getLIGsOfPredictedItems(_Conn, _StoreId, _ReportDate);
		for ( CustomerPredictionDTO dto : list )
		{
			String key = String.valueOf(dto.customerId) + ":" + String.valueOf(dto.lirId);
			List<CustomerPredictionDTO> predList = _LIGsOfPredictedItems.get(key);
			if ( predList == null ) {
				predList = new ArrayList<CustomerPredictionDTO>();
				_LIGsOfPredictedItems.put(key,  predList);
			}
			predList.add(dto);
			if ( predList.size() > 1 ) {
				//logger.debug("doAdditionalPreperation: key " + key + " count " + predList.size());
			}
		}
		logger.info("doAdditionalPreperation: " + _LIGsOfPredictedItems.size() + " item LIGs");
	}

	protected void deleteCustomerPurchase (CustomerDAO dao)
	{
		dao.deleteCustomerLIGPurchase (_Conn, _StoreId, _ReportDate);
	}
	
	protected void deletePrediction (CustomerDAO dao)
	{
		dao.deleteLIGPrediction (_Conn, _StoreId, _ReportDate, _PredictionWeekDate);
	}
	
	public CachedRowSet getCustomerData (CustomerDAO dao, String storeNum, int custIdFirst, int custIdLast)
	{
		return dao.getCustomerLIGs (_Conn, storeNum, _WeekStartDate, _WeekEndDate, custIdFirst, custIdLast, null, true);
	}
	
	protected void loadPurchased (CachedRowSet customers)
	{
		loadLIGPurchased (customers);
	}
	
	private void loadLIGPurchased (CachedRowSet customers)
	{
		logger.info("loadLIGPurchased begin: processing LIG results");
		
		Map<Integer, Map<Integer, CustomerPurchaseData>> customerDataMap = new HashMap<Integer, Map<Integer, CustomerPurchaseData>>();;
		Map<Integer, CustomerPurchaseData> lirDataMap = null;
		
		try
		{
			int curCustomerId = -1;
			int curLirCode = -1;
			String curLIGName = null;
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
				int lirId;
				double quantity;
				Date timestamp;
				boolean weightedLIG;
				
				try {
					lirId = customers.getInt("RET_LIR_ID");
					
					customerId = customers.getInt("ID");
					timestamp = customers.getDate("POS_TIMESTAMP");
					int qty = customers.getInt("QUANTITY");
					if ( qty > 0 ) {
						quantity = qty;
						weightedLIG = false;
					}
					else {
						quantity = customers.getDouble("WEIGHT");
						weightedLIG = true;
					}
				}
				catch (SQLException sex) {
					logger.error("loadLIGPurchased: error accessing result data", sex);
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
								updateLIGData (lirDataMap, curLirCode, curLIGName, curQuantity, curDate, curWeightedItem);
							}
						}
	
						customerCount++;
						// predict and save to database  
						if ( customerCount == 500 ) {
							processPurchased (customerDataMap);
							customerDataMap = new HashMap<Integer, Map<Integer, CustomerPurchaseData>>();
							customerCount = 0;
						}
						
						lirDataMap = new HashMap<Integer, CustomerPurchaseData>();
						customerDataMap.put(customerId, lirDataMap);
						
						primaryDays = getPrimaryShoppingDays(customerId);
	
						curLirCode = -1;
						curWeightedItem = false;
						curCustomerId = customerId;
					}
					
					// different lig?
					if ( lirId != curLirCode )
					{
						if ( curLirCode != -1 ) {
							if ( curQuantity > 0 )
								updateLIGData (lirDataMap, curLirCode, curLIGName, curQuantity, curDate, curWeightedItem);
						}
	
						cal.setTime(curDate);
						cal.set(Calendar.YEAR, 2000);
						curDate = cal.getTime();
						
						curLirCode = lirId;
						curWeightedItem = weightedLIG;
						curQuantity = 0;
					}

					// different date?
					if ( !date.equals(curDate) )
					{
						cal.setTime(curDate);
						if ( cal.get(Calendar.YEAR) != 2000 ) {
							if ( curQuantity > 0 )
								updateLIGData (lirDataMap, curLirCode, curLIGName, curQuantity, curDate, curWeightedItem);
						}
						
						curDate = date;
						curQuantity = 0;
					}

					curQuantity += quantity;
						
					_Count++;
					if ( _Count % 100000 == 0 )
						logger.info("loadLIGPurchased: Processed " + String.valueOf(_Count) + " records");
				}
				catch (Exception ex) {
					logger.error("loadLIGPurchased error: customer=" + customerId, ex);
					_CountFailed++;
				}
			} // while
			
			// add last customer data
			updateLIGData (lirDataMap, curLirCode, curLIGName, curQuantity, curDate, curWeightedItem);
			processPurchased (customerDataMap);
		}
		catch (Exception ex) {
			logger.error("loadLIGPurchased error:", ex);
		}
		
		logger.info("loadLIGPurchased end: processing LIG results");
	}

	protected int updatePrediction (CustomerDAO dao, List<CustomerPredictionDTO> customers)
	{
		return dao.updateLIGPrediction (_Conn, customers, _StoreNum, _ReportDate, _PredictionWeekDate);
	}
	
	protected void completeData (Map<Integer, Map<Integer, CustomerPurchaseData>> customerLIGDataMap)
	{
		Set<Integer> custKeys = customerLIGDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		while ( custIter.hasNext() )
		{
			int customerId = custIter.next();
			CustomerDTO customer = _PrimaryShoppingDays.get(customerId);
			if ( customer == null )
				continue;
			
			Map<Integer, CustomerPurchaseData> ligDataMap = customerLIGDataMap.get(customerId);
			Set<Integer> keys = ligDataMap.keySet();
			Iterator<Integer> iter = keys.iterator();
			_LessThan6Times = new ArrayList<String>();
			_MoreThan3InAWeek = new ArrayList<String>();
			_NoneIn8Weeks = new ArrayList<String>();
			while ( iter.hasNext() )
			{
				int lirId = iter.next();
				CustomerPurchaseData customerLigData = ligDataMap.get(lirId);
				setCustomerData(customer, lirId, customerLigData);
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
	
	private void updateLIGData (Map<Integer, CustomerPurchaseData> ligDataMap, int lirId, String lirName,
			double quantity, Date date, boolean weightedLIG)
	{
		if ( quantity > 0.0001 )
		{
			CustomerPurchaseData customerLigData = ligDataMap.get(lirId);
			if ( customerLigData == null ) {
				customerLigData = new CustomerPurchaseData();
				ligDataMap.put(lirId, customerLigData);
				customerLigData.lirId = lirId;
				customerLigData.quantity = new ArrayList<Double>(); 
				customerLigData.weeklyVisitCounts = Util.get52WeeklyVisitCounts(); 
				customerLigData.dailyCounts = Util.getIntArray(365); 
			}
			if ( customerLigData != null )
			{
				customerLigData.quantity.add(quantity);
				customerLigData.weighted = weightedLIG;
				
				// update day counts
				if ( customerLigData.lastPurchaseDate != null )
				{
					int dayDiff = (int) DateUtil.getDateDiff(date, customerLigData.lastPurchaseDate);
					if ( dayDiff <= 8 ) {
						customerLigData.count7Day++;
					}
					else if ( dayDiff <= 16 ) {
						customerLigData.count15Day++;
					}
					else if ( dayDiff <= 29 ) {
						customerLigData.count28Day++;
					}
					else if ( dayDiff <= 43 ) {
						customerLigData.count42Day++;
					}
				}
				
				int day = (int) DateUtil.getDateDiff(date, _WeekStartDate);
				customerLigData.dailyCounts[day] = 1;
				
				customerLigData.countTotal++;
				customerLigData.lastPurchaseDate = date;
				
				int weekDiff = (int) (DateUtil.getDateDiff(date, _WeekStartDate) / 7);
				customerLigData.weeklyVisitCounts[weekDiff]++;
			}
		}
	}

	protected void updatePurchaseFrequency (Map<Integer, Map<Integer, CustomerPurchaseData>> customerLigDataMap, CustomerDAO dao)
	{
		logger.info("updatePurchaseFrequency begin: " + String.valueOf(customerLigDataMap.size()) + " customers");
		
		List<CustomerPurchaseDTO> customers = new ArrayList<CustomerPurchaseDTO>();
		
		Set<Integer> custKeys = customerLigDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		while ( custIter.hasNext() )
		{
			int customerId = custIter.next();
			CustomerDTO customer = _PrimaryShoppingDays.get(customerId);
			if ( customer == null )
				continue;
			
			Map<Integer, CustomerPurchaseData> ligDataMap = customerLigDataMap.get(customerId);
			Set<Integer> itemKeys = ligDataMap.keySet();
			Iterator<Integer> itemIter = itemKeys.iterator();
			int count = 0, unknownCount = 0;
			while ( itemIter.hasNext() )
			{
				count++;
				
				int lirId = itemIter.next();
				CustomerPurchaseData customerLigData = ligDataMap.get(lirId);
				if ( customerLigData != null && customerLigData.averageQuantity > 0.0001 )
				{
					CustomerPurchaseDTO dto = new CustomerPurchaseDTO();
					dto.customerId = customerId;
					dto.lastPurchaseDate = customerLigData.lastPurchaseDate;
					dto.lirId = lirId;
					dto.quantity = customerLigData.averageQuantity;
					dto.totalCount = customerLigData.countTotal;
					dto.enabled = customerLigData.meetsCriteria;
					customers.add(dto);
				}
			}
		}

		int count = dao.updateCustomerLIGPurchase (_Conn, customers, _StoreId, _ReportDate);
		
		logger.info("updatePurchaseFrequency end: " + String.valueOf(count) + " records created/updated");
	}
	
	protected List<CustomerPredictionDTO> predictPurchases (Map<Integer, Map<Integer, CustomerPurchaseData>> customerLigDataMap, Date weekStartDate)
	{
		logger.info("predictPurchases begin: for " + weekStartDate.toString() + ", " + String.valueOf(customerLigDataMap.size()) + " customers");
		
		List<CustomerPredictionDTO> list = new ArrayList<CustomerPredictionDTO>();
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(weekStartDate);
		cal.add(Calendar.DATE, 7);
		Date weekEndDate = cal.getTime();
		
		Set<Integer> custKeys = customerLigDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		int countItemLIGs = 0;
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

			Map<Integer, CustomerPurchaseData> ligDataMap = customerLigDataMap.get(customerId);
			_Prediction = new HashMap<Integer, List<Integer>>();
			for ( int ii = 0; ii < 7; ii++ ) {
				_Prediction.put(ii, new ArrayList<Integer>());
			}
			Set<Integer> keys = ligDataMap.keySet();
			Iterator<Integer> iter = keys.iterator();
			while ( iter.hasNext() )
			{
				int lirId = iter.next();
				CustomerPurchaseData customerItemData = ligDataMap.get(lirId);
				boolean canPredict = false;
				if ( customerItemData != null && customerItemData.averageQuantity > 0.0001 && customerItemData.meetsCriteria ) {
					canPredict = canPredictPurchaseDay (customerId, lirId, customerItemData, weekStartDate, weekEndDate);
				}
				if ( !canPredict )
				{
					// Add LIG to prediction if item was predicted
					String key = String.valueOf(customerId) + ":" + String.valueOf(lirId);
					List<CustomerPredictionDTO> predList = _LIGsOfPredictedItems.get(key);
					if ( predList != null )
					{
						for ( CustomerPredictionDTO dto : predList ) {
							if ( dto.weekStartDate.equals(weekStartDate) ) {
								addLigToPrediction(lirId, weekStartDate);
								countItemLIGs++;
								//logger.debug("predictPurchases: key " + key + " - week " + weekStartDate);
								break;
							}
						}
					}
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
					addPredictionDTO (list, weekStartDate, customerId, ligDataMap, maxCountItems, maxCountDay, -1);
					for ( int ii = 0; ii < _Prediction.size(); ii++ )
						addPredictionDTO (list, weekStartDate, customerId, ligDataMap, _Prediction.get(ii), maxCountDay, -1);
				}
			}
		}
		
		logger.info("predictPurchases end: " + countItemLIGs + " item LIGs added");
		
		return list;
	}

	protected boolean canPredictPurchaseDay (int customerId, int lirId, CustomerPurchaseData purchaseData, Date weekStartDate, Date weekEndDate)
	{
		//return super.canPredictPurchaseDay (customerId, lirId, itemData, weekStartDate, weekEndDate);
		boolean canPredict = false;

		StringBuffer sb = new StringBuffer("canPredictPurchaseDay::");
		sb.append(customerId).append(':').append(lirId).append(": ");
		int[] dailyCounts = purchaseData.dailyCounts;
		Map<Integer, Integer> frequencyCounts = Util.setupFrequencies(dailyCounts, sb);
		purchaseData.frequencyCounts = frequencyCounts;
		
		sb.append(Util.getFrequencyCounts(frequencyCounts));
		sb.append('.');
		
		int totalCount = Util.getTotalFrequencyCount(frequencyCounts);
		
		List<Integer> days = new ArrayList<Integer>();
		int nFreq = frequencyCounts.size();
		int freqTotal = 0;
		int probabilityCount = 0;
		String tmpStr; 
		while ( nFreq > 0 )
		{
			int modeFreq = -1;
			int modeCount = 0;
			try {
				modeFreq = Util.getMaxCountFrequency(frequencyCounts);
				modeCount = frequencyCounts.remove(modeFreq);
			}
			catch (NullPointerException e) {
				modeCount = 0;
			}
			sb.append(" freq ").append(modeFreq).append('=').append(modeCount);
			Date nextDate = getNextPurchaseDate (purchaseData.lastPurchaseDate, weekStartDate, weekEndDate, modeFreq);
			if ( nextDate != null )
			{
				probabilityCount += modeCount;
				Calendar cal = Calendar.getInstance();
				cal.setTime(nextDate);
				int day = cal.get(Calendar.DAY_OF_WEEK) - 1;
				days.add(day);
				tmpStr = " in prediction week";
			}
			else {
				tmpStr = " in prediction week";
			}
			sb.append(tmpStr);
			
			nFreq = frequencyCounts.size();
		}
		
		purchaseData.freqProbabilityCount = probabilityCount;
		purchaseData.freqTotalCount = totalCount;
		
		double probability = (double)probabilityCount / totalCount;
		//probability = 0.1;
		if ( probability > _PercentOfFequencyCountThreshold )
		{
			sb.append(" Probability (").append(probability).append(')');
			sb.append(" > ").append(_PercentOfFequencyCountThreshold).append(' ');
			for  ( int day : days ) {
				List<Integer> items = _Prediction.get(day);
				items.add(purchaseData.lirId);
			}
			canPredict = true;
		}
		else
		{
			// Probability fails prediction, try average frequency
			sb.append(" <= ").append(_PercentOfFequencyCountThreshold).append(' ');
			boolean doAverage = true;
			if ( doAverage )
			{
				int averageFreq = (int)Math.round((double)freqTotal / totalCount);
				sb.append("Avg freq=").append(averageFreq);
				purchaseData.freqAverage = averageFreq;
				Date nextDate = getNextPurchaseDate(purchaseData.lastPurchaseDate, weekStartDate, weekEndDate, averageFreq);
				if ( nextDate != null )
				{
					addLigToPrediction (lirId, nextDate);
					tmpStr = " in prediction weekXX";
					canPredict = true;
				}
				else {
					tmpStr = " not in prediction weekXX";
				}
				sb.append(tmpStr);
			}
		}
		
		sb.append(" freq avg=").append(purchaseData.freqAverage);
		//logger.debug(sb.toString());
		
		return canPredict;
	}

	private void addLigToPrediction (int lirId, Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int day = cal.get(Calendar.DAY_OF_WEEK) - 1;
		List<Integer> items = _Prediction.get(day);
		items.add(lirId);
	}
	
	private void addPredictionDTO (List<CustomerPredictionDTO> list, Date weekStartDate, int customerId, Map<Integer,
			CustomerPurchaseData> lirDataMap, List<Integer> lirIds, int day, int day2)
	{
		if ( lirIds == null )
			return;
		
		for ( int lirId : lirIds )
		{
			CustomerPredictionDTO custPredictionDTO = new CustomerPredictionDTO();
			
			CustomerPurchaseData customerItemData = lirDataMap.get(lirId);
			if ( customerItemData != null ) {
				custPredictionDTO.customerId = customerId;
				custPredictionDTO.lirId = customerItemData.lirId;
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
			boolean same = elem.customerId == prediction.customerId && elem.lirId == prediction.lirId;
			if ( same )
				return true;
		}
		return false;
	}
	
	private void writePurchaseFrequency (Map<Integer, Map<Integer, CustomerPurchaseData>> customerLIGDataMap)
	{
		_WorkBook = new HSSFWorkbook();
		_Sheet = _WorkBook.createSheet();
		
		// write header
		writePurchaseFrequency_HeaderRow ("LIG");
		_RowCount = 2;
		
		Set<Integer> custKeys = customerLIGDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		StringBuffer custSB = new StringBuffer();
		StringBuffer lirSB = new StringBuffer();
		while ( custIter.hasNext() )
		{
			int customerId = custIter.next();
			CustomerDTO customer = _PrimaryShoppingDays.get(customerId);
			if ( customer == null )
				continue;
			
			Map<Integer, CustomerPurchaseData> lirDataMap = customerLIGDataMap.get(customerId);
			Set<Integer> lirKeys = lirDataMap.keySet();
			Iterator<Integer> lirIter = lirKeys.iterator();
			while ( lirIter.hasNext() )
			{
				int lirId = lirIter.next();
				CustomerPurchaseData customerLIGData = lirDataMap.get(lirId);
				if ( customerLIGData != null && customerLIGData.averageQuantity > 0.0001 )
				{
					if ( _RowCount == 65535 )
						break;
					if ( customerLIGData.purchaseFrequency != PurchaseFrequency.Unknown )
					{
						HSSFRow row = _Sheet.createRow(_RowCount++);
						writePurchaseFrequencyRow (row, customer, lirId, customerLIGData);
						custSB.append(",'").append(customer.customerNo).append("'");
						lirSB.append(',').append(lirId);
					}
				}
			}
			
			if ( _RowCount == 65535 )
				break;
		}
		
		logger.info("Customers: " + custSB.toString());
		logger.info("LIGs: " + lirSB.toString());
		
		try {
			String fileName = "Customer LIG Frequency - " + _StoreNum;
			Util.saveToFile (_WorkBook, fileName);
		}
		catch (GeneralException ex) {
		}
	}
	
	private static Logger logger = Logger.getLogger("CustomerLIG");
	private Map<String, List<CustomerPredictionDTO>> _LIGsOfPredictedItems = new HashMap<String, List<CustomerPredictionDTO>>();
}
