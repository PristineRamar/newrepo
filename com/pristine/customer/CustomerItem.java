package com.pristine.customer;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
//import java.util.Random;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

//import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.customer.CustomerDAO;
//import com.pristine.dataload.CustomerItem.CustomerShoppingDays;
//import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.customer.CustomerPredictionDTO;
import com.pristine.dto.customer.CustomerPurchaseDTO;
import com.pristine.dto.customer.CustomerDTO;
//import com.pristine.dto.CustomerRecordDTO;
//import com.pristine.dto.DayCountDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

public class CustomerItem
{
	public CustomerItem (Date weekStart, Date weekEnd)
	{
		_WeekStartDate = weekStart;
		_WeekEndDate = weekEnd;
		
		try
		{
	        PropertyManager.initialize("analysis.properties");
			try {
				_StopAfter = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
			}
			catch (Exception ex) {
				_StopAfter = -1;
			}
	    }
		catch (Exception ex) {
	        logger.error("Error initializing", ex);
	    }
	}

	public void setupDates ()
	{
		Calendar cal = Calendar.getInstance();
		
		cal.setTime(_WeekStartDate);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		_WeekStartDate = cal.getTime();
		
		cal.setTime(_WeekEndDate);
		cal.set(Calendar.HOUR_OF_DAY, 23);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		cal.set(Calendar.MILLISECOND, 900);
		_WeekEndDate = cal.getTime();
	}

	public void loadPurchased (String storeNum)
	{
		logger.info("loadPurchased begin: storeNum=" + storeNum);
		
		if ( storeNum != null )
		{
			Calendar cal = Calendar.getInstance();
			cal.setTime(_WeekStartDate);
			//Date weekStartDate = cal.getTime();
			
			CustomerDAO dao = new CustomerDAO();
			_PrimaryShoppingDays = getPrimaryShoppingDays (dao);
//			//logger.info("loadItemPurchased: " + _PrimaryShoppingDays.size() + " customers");
			try
			{
				// Clean up existing entries
				_ReportDate = DateUtil.getNextSunday(_WeekEndDate);
				
				_StoreId = dao.getStoreId (_Conn, storeNum);
				
				boolean doIt = true;
				if ( _PredictionWeek == 53 ) {
					if ( doIt )
						deleteCustomerPurchase (dao);
				}

				_PredictionWeekDate = _ReportDate;	// 53rd week
				if ( _PredictionWeek == 54 ) {		// 54th week
					_PredictionWeekDate = DateUtil.getNextSunday(_PredictionWeekDate);	
				}
				if ( _PredictionWeek == 55 ) {		// 55th week
					_PredictionWeekDate = DateUtil.getNextSunday(_PredictionWeekDate);	
					_PredictionWeekDate = DateUtil.getNextSunday(_PredictionWeekDate);	
				}
				doIt = true;
				if ( doIt ) {
					deletePrediction (dao);
				}
				
				List<Integer> customerIds = dao.getStoreCustomers (_Conn, storeNum);
				//List<Integer> customerIds = dao.getEnabledStoreCustomers(_Conn, storeNum);
				//customerIds = new ArrayList<Integer>();
				//customerIds.add(472); customerIds.add(475); customerIds.add(481); customerIds.add(482); customerIds.add(485); customerIds.add(487);
				int listSize = customerIds.size();
				int nBatches = listSize / _CustomerMax + ((listSize % _CustomerMax) == 0 ? 0 : 1);
				logger.info("loadPurchased: " + listSize + " customers in " + nBatches + " batches of " + _CustomerMax + " each");
				//nBatches = 1;
//				customerIds.add (new StringBuffer("'046105961147','046102132679','046100733583','046105135165','046105690900','046106095716','046100523678','046104458955','046100612001','046105961059','046100349003','046104331962','046105789661','046105922649','046102466169','046105568813','046103647006','046105385287','046106169479','046104868748','046101699561','046104331929','046100626014','046100637517','046100613467','046100626244','046105710796','046102384800','046105445715','046105961105','046105834406','046105710798','046105568722','046104459483','046105922589','046104738402','046105896164','046100625163','046102241936','046100629229','046104233478','046106096971','046100627864','046102415580','046105021540','046100620733','046101695194','046105360223','046100614286','046102281466','046105905898','046100616354','046105536571','046104739465','046105408553','046105789490','046105021606','046105311171','046105216117','046100463908','046100513470','046105633337','046105775970','046117131398','046105495021','046100612368','046105922644','046105707846','046104331916','046104963151','046106096807','046100726099','046105568682','046105028608','046104332074','046100701643','046100786547','046101573878','046102014174','046105961617','046105711769','046105706401','046100733566','046100351090','046103777883','046100506451','046102316557','046101572976','046100630159','046106095719','046105499513','046105385109','046117130515','046103582413','046100627556','046100511724','046100625173','046105633295','046100506985','046102452654'"));
				
				doAdditionalPreperation(dao);		// for LIG

				for (int ii = 0; ii < nBatches; ii++ )
				{
					int ind = ii * _CustomerMax;
					int custIdFirst = customerIds.get(ind);
					ind += _CustomerMax - 1;
					if ( ind >= listSize ) {
						ind = listSize - 1;
					}
					int custIdLast = customerIds.get(ind);
					//sb = new StringBuffer(); sb.append(100369);
					//CachedRowSet customerItems = dao.getCustomerItems (_Conn, storeNum, _WeekStartDate, _WeekEndDate, sb.toString(), null, true);
					logger.info("loadPurchased: Loading " + (ii+1) + " batch of " + nBatches + " batches");
					CachedRowSet customerItems = getCustomerData (dao, storeNum, custIdFirst, custIdLast);
					loadPurchased (customerItems);
				}
			}
			catch (GeneralException gex) {
				logger.error("Error:", gex);
			}
			catch (Exception ex) {
				logger.error("Error:", ex);
	        } catch (OutOfMemoryError err) {
				logger.error("Error: " + err.getMessage());
				logger.info("Exiting");
				System.exit(-1);
	        }

			String str = "loadPurchased end: storeNum=" + storeNum + ":: " + String.valueOf(_Count) + " successful, " + String.valueOf(_CountFailed) + " failed.";
			logger.info(str);
		}
	}

	protected void doAdditionalPreperation (CustomerDAO dao)
	{
	}
	
	protected void deleteCustomerPurchase (CustomerDAO dao)
	{
		dao.deleteCustomerItemPurchase (_Conn, _StoreId, _ReportDate);
	}
	
	protected void deletePrediction (CustomerDAO dao)
	{
		dao.deleteItemPrediction (_Conn, _StoreId, _ReportDate, _PredictionWeekDate);
	}
	
	public CachedRowSet getCustomerData (CustomerDAO dao, String storeNum, int custIdFirst, int custIdLast)
	{
		return dao.getCustomerItems (_Conn, storeNum, _WeekStartDate, _WeekEndDate, custIdFirst, custIdLast, null, true);
	}
	
	protected void loadPurchased (CachedRowSet customers)
	{
		loadItemPurchased(customers);
	}
	
	protected void loadItemPurchased (CachedRowSet customers)
	{
		logger.info("loadItemPurchased begin: " + String.valueOf(customers.size()) + " records");
		
		Map<Integer, Map<Integer, CustomerPurchaseData>> customerDataMap = new HashMap<Integer, Map<Integer, CustomerPurchaseData>>();
		Map<Integer, CustomerPurchaseData> itemDataMap = null;
		
		try
		{
			int curCustomerId = -1;
			int curItemCode = -1;
			String curItemName = null;
			double curQuantity = 0;

			Calendar cal = Calendar.getInstance();
			Date curDate = new Date();
			cal.setTime(curDate);
			cal.set(Calendar.YEAR, 2000);
			curDate = cal.getTime(); 
			
			boolean curWeightedItem = false;
			
			_Count = 0;
			_CountFailed = 0;
			int customerCount = 0;
			while ( customers.next() )
			{
				int customerId;
				int itemCode;
				String itemName = null;
				double quantity;
				Date timestamp;
				boolean weightedItem;
				
				try {
					itemCode = customers.getInt("ITEM_CODE");
					if ( _IgnoreItems.contains(itemCode) )		// skip item
						continue;
					
					customerId = customers.getInt("ID");
					timestamp = customers.getDate("POS_TIMESTAMP");
					int qty = customers.getInt("QUANTITY");
					if ( qty > 0 ) {
						quantity = qty;
						weightedItem = false;
					}
					else {
						quantity = customers.getDouble("WEIGHT");
						weightedItem = true;
					}
				}
				catch (SQLException sex) {
					logger.error("loadItemPurchased: error accessing result data", sex);
					continue;
				}

				try
				{
					cal = Calendar.getInstance();
					cal.setTime(timestamp);
					Date date = DateUtil.getPureDate(timestamp);
					
					if ( customerId != curCustomerId )
					{
						// different card
						if ( curCustomerId != -1 )
						{
							if ( curQuantity > 0 ) {
								updateItemData (itemDataMap, curItemCode, curItemName, curQuantity, curDate, curWeightedItem);
							}
							
							customerCount++;
							// predict and save to database  
							if ( customerCount == 500 ) {
								processPurchased (customerDataMap);
								customerDataMap = new HashMap<Integer, Map<Integer, CustomerPurchaseData>>();
								customerCount = 0;
							}
						}
	
						itemDataMap = new HashMap<Integer, CustomerPurchaseData>();
						customerDataMap.put(customerId, itemDataMap);
						
						curItemCode = -1;
						curWeightedItem = false;
						curCustomerId = customerId;
					}
					
					// different item?
					if ( itemCode != curItemCode )
					{
						if ( curItemCode != -1 ) {
							if ( curQuantity > 0 )
								updateItemData (itemDataMap, curItemCode, curItemName, curQuantity, curDate, curWeightedItem);
						}
	
						cal.setTime(curDate);
						cal.set(Calendar.YEAR, 2000);
						curDate = cal.getTime();
						
						curItemCode = itemCode;
						curItemName = itemName;
						curWeightedItem = weightedItem;
						curQuantity = 0;
					}

					// different date?
					if ( !date.equals(curDate) )
					{
						cal.setTime(curDate);
						if ( cal.get(Calendar.YEAR) != 2000 ) {
							if ( curQuantity > 0 )
								updateItemData (itemDataMap, curItemCode, curItemName, curQuantity, curDate, curWeightedItem);
						}
						
						curDate = date;
						curQuantity = 0;
					}

					curQuantity += quantity;
						
					_Count++;
					if ( _Count % 100000 == 0 )
						logger.info("loadItemPurchased: Processed " + String.valueOf(_Count) + " records");
				}
				catch (Exception ex) {
					logger.error("loadItemPurchased error: customer=" + customerId, ex);
					_CountFailed++;
				}
			} // while
			
			// add last customer data
			updateItemData (itemDataMap, curItemCode, curItemName, curQuantity, curDate, curWeightedItem);
			processPurchased (customerDataMap);
		}
		catch (Exception ex) {
			logger.error("loadItemPurchased error:", ex);
		}
		
		logger.info("loadItemPurchased end: successs=" + String.valueOf(_Count) + ", failure=" + String.valueOf(_CountFailed));
	}

	protected void processPurchased (Map<Integer, Map<Integer, CustomerPurchaseData>> customerItemDataMap)
	{
		CustomerDAO dao = new CustomerDAO();
		
		// calculate quantity averages
		completeData (customerItemDataMap);
		
		// write to excel
		//writeCustomerPurchaseData (_ItemsPurchased);
		//writePurchaseFrequency (_ItemsPurchased);

		boolean doIt = true;
		List<CustomerPredictionDTO> list = null;
		if ( doIt ) {
			// predict purchases and write to database
			list = predictPurchases (customerItemDataMap, _PredictionWeekDate);
		}
			
		doIt = true;
		if ( _PredictionWeek == 53 ) {
			if ( doIt )
				// write purchases to database
				updatePurchaseFrequency (customerItemDataMap, dao);
		}
		doIt = true;
		if ( doIt ) {
			// write predictions to database
			int count = updatePrediction (dao, list);
			logger.info("processPurchased week " + _PredictionWeek + ": " + String.valueOf(count) + " items predicted");
		}
	}
	
	protected int updatePrediction (CustomerDAO dao, List<CustomerPredictionDTO> customers)
	{
		return dao.updateItemPrediction (_Conn, customers, _StoreNum, _ReportDate, _PredictionWeekDate);
	}
	
	protected void completeData (Map<Integer, Map<Integer, CustomerPurchaseData>> customerItemDataMap)
	{
		Set<Integer> custKeys = customerItemDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		while ( custIter.hasNext() )
		{
			int customerId = custIter.next();
			CustomerDTO customer = _PrimaryShoppingDays.get(customerId);
			if ( customer == null )
				continue;
			
			Map<Integer, CustomerPurchaseData> itemDataMap = customerItemDataMap.get(customerId);
			Set<Integer> itemKeys = itemDataMap.keySet();
			Iterator<Integer> itemIter = itemKeys.iterator();
			_LessThan6Times = new ArrayList<String>();
			_MoreThan3InAWeek = new ArrayList<String>();
			_NoneIn8Weeks = new ArrayList<String>();
			while ( itemIter.hasNext() )
			{
				int itemCode = itemIter.next();
				CustomerPurchaseData customerItemData = itemDataMap.get(itemCode);
				setCustomerData(customer, itemCode, customerItemData);
			}	// while item
			
/*
			String tmpStr = "completeItemData::" + customer.customerNo;
			logger.debug (tmpStr + " LessThan6Times rule: count=" + _LessThan6Times.size() + " - " + Util.getCommaList (_LessThan6Times));
			if ( _MoreThan3InAWeek.size() > 0 )
				logger.debug (tmpStr + " MoreThan3InAWeek rule: count=" + _MoreThan3InAWeek.size() + " - " + Util.getCommaList (_MoreThan3InAWeek));
			logger.debug (tmpStr + " NoneIn6Weeks rule: count=" + _NoneIn8Weeks.size() + " - " + Util.getCommaList (_NoneIn8Weeks));
*/
		}	// while customer
	}
	
	protected void setCustomerData (CustomerDTO customer, int objId, CustomerPurchaseData purchaseData)
	{
		String objStr = String.valueOf(objId);
		int totalCount;
		int[] weeklyVisitCounts = purchaseData.weeklyVisitCounts;
		
		// more than 3 times in a single week?
		for ( int ii = 0; ii < 52; ii++ )
		{
			if ( weeklyVisitCounts[ii] > 3 ) {
				_MoreThan3InAWeek.add(objStr);
				//customerItemData.meetsCriteria = false;
				break;
			}
		}
		
		// at least once every 8 weeks
		int everyNweeks = 8;
		for ( int ii = 0; ii < (52-(everyNweeks-1)); ii += everyNweeks )
		{
			totalCount = 0;
			for ( int jj = 0; jj < everyNweeks; jj++ ) {
				totalCount += weeklyVisitCounts[ii+jj];
			}
			if ( totalCount == 0 ) {
				_NoneIn8Weeks.add(objStr);
				purchaseData.meetsCriteria = false;
				break;
			}
		}

		if ( !purchaseData.meetsCriteria )
		{
			purchaseData.meetsCriteria = true;
			
			// at least 6 times in 52 weeks
			totalCount = 0;
			for ( int ii = 0; ii < 52; ii++ ) {
				totalCount += weeklyVisitCounts[ii];
			}
			if ( totalCount < 6 ) {
				_LessThan6Times.add(objStr);
				purchaseData.meetsCriteria = false;
			}
			if ( purchaseData.meetsCriteria )
			{
				// average week gap less than 8 weeks?
				int weekGap = 0, weekGapCount = 0, weekGapTotal = 0;
				for ( int ii = 0; ii < 52; ii++, weekGap++ )
				{
					if ( weeklyVisitCounts[ii] > 0 ) {
						weekGapCount++;
						weekGapTotal += weekGap;
						weekGap = 0;
					}
				}
				weekGapCount++;
				weekGapTotal += weekGap;
				if ( ((double)weekGapTotal / weekGapCount) > 8.0 ) 
					purchaseData.meetsCriteria = false;
			}
		}
		
		int size = purchaseData.quantity.size();
		if ( (double)size / purchaseData.countTotal > _PercentOfVisitCount )
		{
			double avg;
			if ( purchaseData.weighted ) {
				double dTotal = 0;
				for ( int ii = 0; ii < size; ii++ ) {
					dTotal += purchaseData.quantity.get(ii);
				}
				avg = dTotal/size;
			}
			else {
				int total = 0;
				for ( int ii = 0; ii < size; ii++ ) {
					total += Math.round(purchaseData.quantity.get(ii));
				}
				avg = Math.round((double)total / size);
			}
			purchaseData.averageQuantity = avg;
		}
		
		int visitCount = customer.visitCount;
		purchaseData.purchaseFrequency = PurchaseFrequency.Unknown;
		double tmp = purchaseData.count7Day;
		//if ( (tmp / totalCount) > _PercentOfVisitCount ) {
		if ( (tmp / visitCount) > _PercentOfVisitCount ) {
			purchaseData.purchaseFrequency = PurchaseFrequency.Weekly;
		}
		else {
			tmp += purchaseData.count15Day;
			//if ( (tmp / totalCount) > _PercentOfVisitCount ) {
			if ( (tmp / visitCount) > _PercentOfVisitCount ) {
				purchaseData.purchaseFrequency = PurchaseFrequency.BiWeekly;
			}
			else {
				tmp += purchaseData.count28Day;
				if ( (tmp / visitCount) > _PercentOfVisitCount ) {
					purchaseData.purchaseFrequency = PurchaseFrequency.Every4Weeks;
				}
				else {
					tmp += purchaseData.count42Day;
					if ( (tmp / visitCount) > _PercentOfVisitCount ) {
						purchaseData.purchaseFrequency = PurchaseFrequency.Every6Weeks;
					}
				}
			}
		}
	}
	
	protected boolean isPrimaryDay (int[] primaryDays, int day)
	{
		int dayInd;
		int len = primaryDays != null ? primaryDays.length : 0;
		for ( dayInd = 0; dayInd < len; dayInd++ ) {
			if ( primaryDays[dayInd] == (day-1) )
				break;
		}
		return ( dayInd != len );
	}
	
	protected int[] getPrimaryShoppingDays (int customerId)
	{
		CustomerDTO customer =  _PrimaryShoppingDays.get(customerId);
		return ( customer != null ? customer.primaryDays : new int[0] ); 
	}
	
	protected Map<Integer, CustomerDTO> getPrimaryShoppingDays (CustomerDAO dao)
	{
		Map<Integer, CustomerDTO> shoppingDays = dao.getShoppingInfo (_Conn, _StoreNum, 17, 120, 8);
		int dbCount = shoppingDays.size();

		Set<Integer> custIds = shoppingDays.keySet();
		Iterator<Integer> iter = custIds.iterator();
		int count = 0;
		while ( iter.hasNext() )
		{
			int custId = iter.next();
			CustomerDTO dto = shoppingDays.get(custId);
			String days = Customer.getPrimaryShoppingDays(dto);
			if ( days != null )
			{
				String[] primaryDays = days.split("\\+");
				dto.primaryDays = new int[primaryDays.length];
				for ( int ii = 0; ii < primaryDays.length; ii++ ) {
					dto.primaryDays[ii] = Integer.parseInt(primaryDays[ii]);
				}
			}
			else {
				dto.primaryDays = new int[0];
				count++;
			}
		}

		logger.debug("getPrimaryShoppingDays: count=" + dbCount + " reduced by " + count + " due to 80%, 85% & 90% rule");
		return shoppingDays;
	}
	
	private void updateItemData (Map<Integer, CustomerPurchaseData> itemDataMap, int itemCode, String itemName,
			 double quantity, Date date, boolean weightedItem)
	{
		if ( quantity > 0.0001 )
		{
			CustomerPurchaseData customerItemData = itemDataMap.get(itemCode);
			if ( customerItemData == null ) {
				customerItemData = new CustomerPurchaseData();
				itemDataMap.put(itemCode, customerItemData);
				customerItemData.itemCode = itemCode;
				customerItemData.quantity = new ArrayList<Double>(); 
				customerItemData.weeklyVisitCounts = Util.get52WeeklyVisitCounts(); 
				customerItemData.dailyCounts = Util.getIntArray(365); 
			}
			if ( customerItemData != null )
			{
				customerItemData.quantity.add(quantity);
				customerItemData.weighted = weightedItem;
				
				// update day counts
				if ( customerItemData.lastPurchaseDate != null )
				{
					int dayDiff = (int) DateUtil.getDateDiff(date, customerItemData.lastPurchaseDate);
					if ( dayDiff <= 8 ) {
						customerItemData.count7Day++;
					}
					else if ( dayDiff <= 16 ) {
						customerItemData.count15Day++;
					}
					else if ( dayDiff <= 29 ) {
						customerItemData.count28Day++;
					}
					else if ( dayDiff <= 43 ) {
						customerItemData.count42Day++;
					}
				}
				
				int day = (int) DateUtil.getDateDiff(date, _WeekStartDate);
				customerItemData.dailyCounts[day] = 1;
				
				customerItemData.countTotal++;
				customerItemData.lastPurchaseDate = date;
				
				int weekDiff = (int) (DateUtil.getDateDiff(date, _WeekStartDate) / 7);
				customerItemData.weeklyVisitCounts[weekDiff]++;
			}
		}
	}

	protected List<CustomerPredictionDTO> predictPurchases (Map<Integer, Map<Integer, CustomerPurchaseData>> customerItemDataMap, Date weekStartDate)
	{
		logger.info("predictPurchases begin: for " + weekStartDate.toString() + ", " + String.valueOf(customerItemDataMap.size()) + " customers");
		
		List<CustomerPredictionDTO> list = new ArrayList<CustomerPredictionDTO>();
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(weekStartDate);
		cal.add(Calendar.DATE, 7);
		Date weekEndDate = cal.getTime();

		Set<Integer> custKeys = customerItemDataMap.keySet();
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

			Map<Integer, CustomerPurchaseData> itemDataMap = customerItemDataMap.get(customerId);
			_Prediction = new HashMap<Integer, List<Integer>>();
			for ( int ii = 0; ii < 7; ii++ ) {
				_Prediction.put(ii, new ArrayList<Integer>());
			}
			Set<Integer> itemKeys = itemDataMap.keySet();
			Iterator<Integer> itemIter = itemKeys.iterator();
			while ( itemIter.hasNext() )
			{
				int itemCode = itemIter.next();
				CustomerPurchaseData customerItemData = itemDataMap.get(itemCode);
				if ( customerItemData != null && customerItemData.averageQuantity > 0.0001 && customerItemData.meetsCriteria )
				{
					boolean canPredict = canPredictPurchaseDay (customerId, itemCode, customerItemData, weekStartDate, weekEndDate);
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
					addPredictionDTO (list, weekStartDate, customerId, itemDataMap, maxCountItems, maxCountDay, -1);
					for ( int ii = 0; ii < _Prediction.size(); ii++ )
						addPredictionDTO (list, weekStartDate, customerId, itemDataMap, _Prediction.get(ii), maxCountDay, -1);
				}
			}
		}
		
		logger.info("predictPurchases end: " + list.size() + " predictions");
		
		return list;
	}

	private void addPredictionDTO (List<CustomerPredictionDTO> list, Date weekStartDate, int customerId, Map<Integer, CustomerPurchaseData> itemDataMap,
								   List<Integer> items, int day, int day2)
	{
		if ( items == null )
			return;
		
		for ( int itemCode : items )
		{
			CustomerPredictionDTO custPredictionDTO = new CustomerPredictionDTO();
			
			CustomerPurchaseData customerItemData = itemDataMap.get(itemCode);
			custPredictionDTO.customerId = customerId;
			custPredictionDTO.itemCode = customerItemData.itemCode;
			custPredictionDTO.weekStartDate = weekStartDate;
			custPredictionDTO.quantity = customerItemData.averageQuantity;
			customerItemData.predicted = true;
			
			boolean exists = containsPrediction (list, custPredictionDTO);
			if ( !exists )
				list.add(custPredictionDTO);
		}
	}
	
	private boolean containsPrediction (List<CustomerPredictionDTO> list, CustomerPredictionDTO prediction)
	{
		for ( CustomerPredictionDTO elem : list )
		{
			boolean same = elem.customerId == prediction.customerId && elem.itemCode == prediction.itemCode;
			if ( same )
				return true;
		}
		return false;
	}

	protected void logDayCounts (int customerId, Map<Integer, List<Integer>> items)
	{
		StringBuffer sb = new StringBuffer("logDayCounts::customer ").append(customerId).append(": ");
		for ( int ii = 0; ii < 7; ii++ ) {
			int count = _Prediction.get(ii).size();
			if ( count > 0 )
				sb.append(',').append(ii).append('=').append(count);
		}
		logger.debug(sb.toString());
	}
	
	protected int getTotalItemCount (Map<Integer, List<Integer>> items)
	{
		int totalCount = 0;
		for ( int ii = 0; ii < 7; ii++ ) {
			totalCount += _Prediction.get(ii).size();
		}
		return totalCount;
	}
	
	protected int getMaxItemCountDay (Map<Integer, List<Integer>> items)
	{
		Set<Integer> keys = items.keySet();
		Iterator<Integer> keyIter = keys.iterator();
		int maxCount = 0;
		int maxCountDay = -1;
		while ( keyIter.hasNext() )
		{
			int key = keyIter.next();
			int count = items.get(key).size();
			if ( count > maxCount ) {
				maxCount = count;
				maxCountDay = key;
			}
		}
		return maxCountDay;
	}
	
	protected boolean willCustomerVisit (CustomerDTO customer, Date weekStartDate, Date weekEndDate)
	{
		boolean willVisit = true;
		return willVisit;
	}
	
	protected boolean canPredictPurchaseDay (int customerId, int objId, CustomerPurchaseData purchaseData, Date weekStartDate, Date weekEndDate)
	{
		boolean canPredict = false;

		StringBuffer sb = new StringBuffer("canPredictPurchaseDay::");
		sb.append(customerId).append(':').append(objId).append(": ");
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
			freqTotal += modeFreq * modeCount;
			sb.append(" freq ").append(modeFreq).append('=').append(modeCount);
			Date nextDate = getNextPurchaseDate(purchaseData.lastPurchaseDate, weekStartDate, weekEndDate, modeFreq);
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
				tmpStr = " not in prediction week";
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
				List<Integer> objects = _Prediction.get(day);
				objects.add(objId);
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
					Calendar cal = Calendar.getInstance();
					cal.setTime(nextDate);
					int day = cal.get(Calendar.DAY_OF_WEEK) - 1;
					List<Integer> objects = _Prediction.get(day);
					objects.add(objId);
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
		
/*
		cal.setTime(itemData.lastPurchaseDate);
		
		int days = 0;
		if ( itemData.purchaseFrequency == PurchaseFrequency.Weekly ) {
			days = 7;
		}
		else if ( itemData.purchaseFrequency == PurchaseFrequency.BiWeekly ) {
			days = 14;
		}
		else if ( itemData.purchaseFrequency == PurchaseFrequency.Every4Weeks ) {
			days = 28;
		}
		else if ( itemData.purchaseFrequency == PurchaseFrequency.Every6Weeks ) {
			days = 42;
		}
		if ( days > 0 )
		{
			Date predictedDate = weekStartDate;
			while ( predictedDate.before(weekEndDate) )
			{
				cal.add(Calendar.DATE, days);
				predictedDate = cal.getTime();
				canPredict = predictedDate.equals(weekStartDate) || (predictedDate.after(weekStartDate) && predictedDate.before(weekEndDate));
				if ( canPredict )
					break;
			}
		}
*/		
		return canPredict;
	}

	private boolean addTopFrequency (CustomerPurchaseData itemData, Map<Integer, Integer> frequencyCounts, int totalCount,
									 Date weekStartDate, Date weekEndDate, StringBuffer sb)
	{
		boolean ret = false;
		
		int modeFreq = -1;
		int modeCount = 0;
		try {
			modeFreq = Util.getMaxCountFrequency(frequencyCounts);
			modeCount = frequencyCounts.remove(modeFreq);
		}
		catch (NullPointerException e) {
			modeCount = 0;
		}
		if ( (double)modeCount / totalCount > 0.3 )	// > 30%
		{
			sb.append(" freq ").append(modeFreq).append('=').append(modeCount);
			Date nextDate = getNextPurchaseDate(itemData.lastPurchaseDate, weekStartDate, weekEndDate, modeFreq);
			if ( nextDate != null )
			{
				Calendar cal = Calendar.getInstance();
				cal.setTime(nextDate);
				int day = cal.get(Calendar.DAY_OF_WEEK) - 1;
				List<Integer> items = _Prediction.get(day);
				items.add(itemData.itemCode);
				
				ret = true;
			}
			else {
				sb.append(" not in prediction week");
			}
		}
		
		return ret;
	}
	
	protected Date getNextPurchaseDate (Date lastPurchaseDate, Date weekStartDate, Date weekEndDate, int frequency)
	{
		Date ret = null;
		if ( frequency > 0 )
		{
			Calendar cal = Calendar.getInstance();
			cal.setTime(lastPurchaseDate);
			Date predictedDate = weekStartDate;
			while ( predictedDate.before(weekEndDate) )
			{
				cal.add(Calendar.DATE, frequency);
				predictedDate = cal.getTime();
				boolean canPredict = predictedDate.equals(weekStartDate) || (predictedDate.after(weekStartDate) && predictedDate.before(weekEndDate));
				if ( canPredict ) {
					ret = predictedDate;
					break;
				}
			}
		}
		return ret;
	}
	
	protected void updatePurchaseFrequency (Map<Integer, Map<Integer, CustomerPurchaseData>> customerItemDataMap, CustomerDAO dao)
	{
		logger.info("updatePurchaseFrequency begin: " + String.valueOf(customerItemDataMap.size()) + " customers");
		
		List<CustomerPurchaseDTO> customers = new ArrayList<CustomerPurchaseDTO>();
		
		Set<Integer> custKeys = customerItemDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		while ( custIter.hasNext() )
		{
			int customerId = custIter.next();
			CustomerDTO customer = _PrimaryShoppingDays.get(customerId);
			if ( customer == null )
				continue;
			
			Map<Integer, CustomerPurchaseData> itemDataMap = customerItemDataMap.get(customerId);
			Set<Integer> itemKeys = itemDataMap.keySet();
			Iterator<Integer> itemIter = itemKeys.iterator();
			int count = 0, unknownCount = 0;
			while ( itemIter.hasNext() )
			{
				count++;
				
				int itemCode = itemIter.next();
				CustomerPurchaseData customerItemData = itemDataMap.get(itemCode);
				if ( customerItemData != null && customerItemData.averageQuantity > 0.0001 )
				{
					CustomerPurchaseDTO dto = new CustomerPurchaseDTO();
					dto.customerId = customerId;
					dto.lastPurchaseDate = customerItemData.lastPurchaseDate;
					dto.itemCode = itemCode;
					dto.quantity = customerItemData.averageQuantity;
					dto.totalCount = customerItemData.countTotal;
					dto.enabled = customerItemData.meetsCriteria;
					dto.predicted = customerItemData.predicted;
					customers.add(dto);
				}
			}
		}

		int count = dao.updateCustomerItemPurchase (_Conn, customers, _StoreId, _ReportDate);
		
		logger.info("updatePurchaseFrequency end: " + String.valueOf(count) + " records created/updated");
	}
	
	private void writeCustomerPurchaseData_HeaderRow()
	{
		HSSFRow row = _Sheet.createRow(0);
		int colCount = -1;
		Util.writeToCell(row, ++colCount, "Customer");
		Util.writeToCell(row, ++colCount, "Primary Shopping Day(s)");
		//Util.writeToCell(row, ++colCount, "Next Shopping Week");
		Util.writeToCell(row, ++colCount, "Next Purchase");
		
		row = _Sheet.createRow(1);
		colCount = -1;
		Util.writeToCell(row, ++colCount, "");
		Util.writeToCell(row, ++colCount, "");
		Util.writeToCell(row, ++colCount, "Item");
		Util.writeToCell(row, ++colCount, "Quantity");
		//Util.writeToCell(row, ++colCount, "Quantities");
		//Util.writeToCell(row, ++colCount, "Purchase Count");
		//Util.writeToCell(row, ++colCount, "Visit Count");
}
	
	private void writeCustomerPurchaseData (Map<Integer, Map<Integer, CustomerPurchaseData>> customerItemDataMap)
	{
		_WorkBook = new HSSFWorkbook();
		_Sheet = _WorkBook.createSheet();
		
		// write header
		writeCustomerPurchaseData_HeaderRow ();
		_RowCount = 2;
		
		Set<Integer> custKeys = customerItemDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		StringBuffer custSB = new StringBuffer();
		StringBuffer itemSB = new StringBuffer();
		while ( custIter.hasNext() )
		{
			int customerId = custIter.next();
			CustomerDTO customer = _PrimaryShoppingDays.get(customerId);
			if ( customer == null )
				continue;
			
			Map<Integer, CustomerPurchaseData> itemDataMap = customerItemDataMap.get(customerId);
			Set<Integer> itemKeys = itemDataMap.keySet();
			Iterator<Integer> itemIter = itemKeys.iterator();
			while ( itemIter.hasNext() )
			{
				int itemCode = itemIter.next();
				CustomerPurchaseData customerItemData = itemDataMap.get(itemCode);
				if ( customerItemData != null && customerItemData.averageQuantity > 0.0001 )
				{
					if ( _RowCount == 65535 )
						break;
					
					HSSFRow row = _Sheet.createRow(_RowCount++);
					int colCount = -1;
					Util.writeToCell(row, ++colCount, customer.customerNo);
					custSB.append(",'").append(customer.customerNo).append("'");
					String shoppingDays = "";
					for (int ii = 0; ii < customer.primaryDays.length; ii++ ) 
					{
						int day = customer.primaryDays[ii];
						switch ( day ) {
							case 0: shoppingDays += " Sunday"; break;
							case 1: shoppingDays += " Monday"; break;
							case 2: shoppingDays += " Tuesday"; break;
							case 3: shoppingDays += " Wednesday"; break;
							case 4: shoppingDays += " Thursday"; break;
							case 5: shoppingDays += " Friday"; break;
							case 6: shoppingDays += " Saturday"; break;
						}
					}
					Util.writeToCell(row, ++colCount, shoppingDays);
					Util.writeToCell(row, ++colCount, itemCode);
					itemSB.append(',').append(itemCode);
					//Util.writeToCell(row, ++colCount, customerItemData.itemName);
					StringBuffer sb = new StringBuffer();
					for ( int ii = 0; ii < customerItemData.quantity.size(); ii++ )
					{
						if ( ii > 0 )
							sb.append(',');
						
						double qty = customerItemData.quantity.get(ii);
						if ( customerItemData.weighted ) {
							sb.append(qty);
						}
						else {
							sb.append(Math.round(qty));
						}
						
						if ( sb.length() > 32000 )
							break;
					}
					//Util.writeToCell(row, ++colCount, sb.toString());
					Util.writeToCell(row, ++colCount, customerItemData.averageQuantity);
					//Util.writeToCell(row, ++colCount, customerItemData.quantity.size());
					//Util.writeToCell(row, ++colCount, customer.visitCount);
				}
			}
			
			if ( _RowCount == 65535 )
				break;
		}
		
		logger.info("Customers: " + custSB.toString());
		logger.info("Items: " + itemSB.toString());
		
		try {
			String fileName = "Customer Item Quantity - " + _StoreNum;
			Util.saveToFile (_WorkBook, fileName);
		}
		catch (GeneralException ex) {
		}
	}
	
	protected void writePurchaseFrequency_HeaderRow(String secondColumn)
	{
		HSSFRow row = _Sheet.createRow(0);
		int colCount = -1;
		Util.writeToCell(row, ++colCount, "Customer");
		Util.writeToCell(row, ++colCount, secondColumn);
		Util.writeToCell(row, ++colCount, "Frequency");
		Util.writeToCell(row, ++colCount, "Count");
		Util.writeToCell(row, ++colCount, "");
		Util.writeToCell(row, ++colCount, "");
		Util.writeToCell(row, ++colCount, "");
		Util.writeToCell(row, ++colCount, "");
		Util.writeToCell(row, ++colCount, "Visit Count");
		
		row = _Sheet.createRow(1);
		colCount = -1;
		Util.writeToCell(row, ++colCount, "");
		Util.writeToCell(row, ++colCount, "");
		Util.writeToCell(row, ++colCount, "");
		Util.writeToCell(row, ++colCount, "Week");
		Util.writeToCell(row, ++colCount, "2 Weeks");
		Util.writeToCell(row, ++colCount, "4 Weeks");
		Util.writeToCell(row, ++colCount, "6 Weeks");
		Util.writeToCell(row, ++colCount, "Total");
	}
	
	protected void writePurchaseFrequencyRow (HSSFRow row, CustomerDTO customer, int id, CustomerPurchaseData customerItemData)
	{
		int colCount = -1;
		
		Util.writeToCell(row, ++colCount, customer.customerNo);
		Util.writeToCell(row, ++colCount, id);
		Util.writeToCell(row, ++colCount, customerItemData.purchaseFrequency.toString());
		int tmp = customerItemData.count7Day;
		Util.writeToCell(row, ++colCount, tmp);
		tmp += customerItemData.count15Day;
		Util.writeToCell(row, ++colCount, tmp);
		tmp += customerItemData.count28Day;
		Util.writeToCell(row, ++colCount, tmp);
		tmp += customerItemData.count42Day;
		Util.writeToCell(row, ++colCount, tmp);
		Util.writeToCell(row, ++colCount, customerItemData.countTotal);
		
		Util.writeToCell(row, ++colCount, customer.totalCount);
	}
	
	private void writePurchaseFrequency (Map<Integer, Map<Integer, CustomerPurchaseData>> customerItemDataMap)
	{
		_WorkBook = new HSSFWorkbook();
		_Sheet = _WorkBook.createSheet();
		
		// write header
		writePurchaseFrequency_HeaderRow ("Item");
		_RowCount = 2;
		
		logger.info("writePurchaseFrequency: " + customerItemDataMap.size() + " customers");
		Set<Integer> custKeys = customerItemDataMap.keySet();
		Iterator<Integer> custIter = custKeys.iterator();
		StringBuffer custSB = new StringBuffer();
		StringBuffer itemSB = new StringBuffer();
		while ( custIter.hasNext() )
		{
			int customerId = custIter.next();
			CustomerDTO customer = _PrimaryShoppingDays.get(customerId);
			if ( customer == null )
				continue;
			
			Map<Integer, CustomerPurchaseData> itemDataMap = customerItemDataMap.get(customerId);
			Set<Integer> itemKeys = itemDataMap.keySet();
			Iterator<Integer> itemIter = itemKeys.iterator();
			int count = 0, unknownCount = 0;
			while ( itemIter.hasNext() )
			{
				count++;
				
				int itemCode = itemIter.next();
				CustomerPurchaseData customerItemData = itemDataMap.get(itemCode);
				if ( customerItemData != null && customerItemData.averageQuantity > 0.0001 )
				{
					if ( _RowCount == 65535 )
						break;
					if ( customerItemData.purchaseFrequency != PurchaseFrequency.Unknown )
					{
						HSSFRow row = _Sheet.createRow(_RowCount++);
						writePurchaseFrequencyRow (row, customer, itemCode, customerItemData);
						custSB.append(",'").append(customer.customerNo).append("'");
						itemSB.append(',').append(itemCode);
					}
					else {
						unknownCount++;
					}
				}
			}
			
			logger.info("writePurchaseFrequency: customer " + customer.customerNo + ":" + itemDataMap.size() + " items, count="
						+ count + ", unknown=" + unknownCount);
			
			if ( _RowCount == 65535 )
				break;
		}
		
		logger.info("Customers: " + custSB.toString());
		logger.info("Items: " + itemSB.toString());
		
		try {
			String fileName = "Customer Item Frequency - " + _StoreNum;
			Util.saveToFile (_WorkBook, fileName);
		}
		catch (GeneralException ex) {
		}
	}
	
	//
	// getters/setters
	//
	public void setConnection (Connection conn) { _Conn = conn; }
	public void setStoreNum (String v) { _StoreNum = v; }
	public void setPercentOfVisitCount (double v) { _PercentOfVisitCount = v; }
	public void setPercentOfFequencyCount (double v) { _PercentOfFequencyCountThreshold = v; }
	public void setIgnoreItems (String items)
	{
		String[] tmp = items.split(",");
		if ( tmp.length > 0 )
		{
			_IgnoreItems = new ArrayList<Integer>();
			for ( int ii = 0; ii < tmp.length; ii++ ) {
				_IgnoreItems.add(Integer.parseInt(tmp[ii]));
			}
		}
	}
	public void setPredictionWeek (int week) { _PredictionWeek = week; }
	public void setCustomerMax (int v) { _CustomerMax = v; }

	//
	// Classes
	//
	protected enum PurchaseFrequency
	{
		Unknown(0),
		Weekly(1),
		BiWeekly(2),
		Every4Weeks(3),
		Every6Weeks(4);
		
		private final int enumId;
		
		PurchaseFrequency (int id) { enumId = id; }
		//public static int toInt (PurchaseFrequency v) { return v.enumId; }
		public int toInt () { return enumId; }
		public PurchaseFrequency toEnum (int id) { return enumId == id ? this : null; }
		public String toString ()
		{
			String ret;
			switch ( enumId ) {
				case 1: ret = "Weekly"; break;
				case 2: ret = "BiWeekly"; break;
				case 3: ret = "4 Weeks"; break;
				case 4: ret = "6 Weeks"; break;
				
				case 0:
				default: ret = "Unknown"; break;
			}
			return ret;
		}
	}
	
	protected class FrequencyCount
	{
		int	frequency;
		int	count;
	}
	
	protected class CustomerPurchaseData
	{
		int				itemCode = -1;
		int				lirId = -1;
		int				segmentId = -1;
		boolean			weighted;
		List<Double>	quantity;
		double			averageQuantity = 0;
		int[] 			weeklyVisitCounts = null;
		int[] 			dailyCounts = null;
		boolean			meetsCriteria = true;
		boolean			predicted = false;

		Map<Integer, Integer> frequencyCounts = null;
		int				freqProbabilityCount = 0;			
		int				freqTotalCount = 0;			
		double			freqAverage = 0;			
		
		Date			lastPurchaseDate = null;
		int				countTotal = 0;
		int				count7Day  = 0;
		int				count15Day = 0;
		int				count28Day = 0;
		int				count42Day = 0;
		
		PurchaseFrequency purchaseFrequency;
	}
	
	//
	// Data members
	//
	private static Logger 		logger = Logger.getLogger("CustomerItem");

	protected Date  	_WeekStartDate;
	protected Date  	_WeekEndDate;
	protected Date  	_ReportDate;
	protected String	_StoreNum;
	protected int		_StoreId = -1;
	protected int		_WeekCount = 0;
	protected int		_CustomerMax = 2000;
	protected int  		_PredictionWeek = 53;
	protected Date  	_PredictionWeekDate;

	//protected Map<Integer, CustomerShoppingDays>	_ShoppingDays = null; 
	protected Map<Integer, CustomerDTO>	_PrimaryShoppingDays = null;
	protected List<Integer>	_IgnoreItems = new ArrayList<Integer>();
	protected List<String> 	_MoreThan3InAWeek = new ArrayList<String>();
	protected List<String> 	_NoneIn8Weeks = new ArrayList<String>();
	protected List<String> 	_LessThan6Times = new ArrayList<String>();
	protected Map<Integer, List<Integer>>	_Prediction = null;

	protected Connection	_Conn = null;
	protected int			_Count = 0;
	protected int			_CountFailed = 0;
	protected int			_StopAfter = -1;
	
	protected double	_PercentOfVisitCount = (double)0.6;
	protected double	_PercentOfFequencyCountThreshold = (double)0.6;
	
	// Excel objects
	protected HSSFWorkbook	_WorkBook;
	protected HSSFSheet		_Sheet;
	protected int			_RowCount = 0;
}
