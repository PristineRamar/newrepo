package com.pristine.customer;

import java.sql.Connection;
import java.text.DateFormat;
import java.util.*;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dto.customer.CustomerDTO;
import com.pristine.dto.customer.CustomerItemDTO;
import com.pristine.dto.customer.DayCountDTO;
import com.pristine.dao.customer.CustomerDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class Customer
{
	// 9 Oct 11 (10/9/11), 25 Nov 12 
	// Customer OPERATION=<Op No> [STARTDATE=<start date>] [STORENUM=<store no>] [WEEKCOUNT=<no>]
	// Customer OPERATION=1 STARTDATE=10/9/2011 STORENUM=0108 WEEKCOUNT=4 (set up using 4 weeks)
	// Customer OPERATION=1 STORENUM=0108 WEEKCOUNT=4 (set up using last 4 weeks)
	// Customer OPERATION=2 STARTDATE=10/9/2011 STORENUM=0108 WEEKCOUNT=52 (shopping day)
	// Customer OPERATION=3 STARTDATE=10/9/2011 STORENUM=0108 WEEKCOUNT=52 (customer visit count)
	// Customer OPERATION=4 STARTDATE=10/9/2011 STORENUM=0108 WEEKCOUNT=52 PREDICTIONWEEK=53 (item purchasing common and prediction)
	// Customer OPERATION=5 STARTDATE=10/9/2011 STORENUM=0108 WEEKCOUNT=52 PREDICTIONWEEK=53 (LIG purchasing common and prediction)
	// Customer OPERATION=6 STARTDATE=10/9/2011 STORENUM=0108 WEEKCOUNT=52 PREDICTIONWEEK=53 (segment purchasing common and prediction)
	// Customer OPERATION=7 STARTDATE=10/9/2011 STORENUM=0108 WEEKCOUNT=52 (prediction using item purchase frequency)
	// Customer OPERATION=11 STARTDATE=10/7/2012 STORENUM=0108 (item prediction verification)

	public Customer (Date weekStart, Date weekEnd)
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
	
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		PropertyConfigurator.configure("log4j-customer.properties");
        PropertyManager.initialize("analysis.properties");
        
		logCommand (args);

        try
        {
    		String storeNum = null;
        	Date startDate = null, endDate = null;
        	int weekCount = 52;
        	int operation = ARG_OPERATION_INVALID;
        	String startDateStr = null;
    		DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
    		int predWeek = 53;
    		boolean startDateProvided = false;
    		
    		for ( int ii = 0; ii < args.length; ii++ )
    		{
        		String arg = args[ii];
        		if ( arg.startsWith (ARG_OPERATION) ) {
        			operation = Integer.parseInt(arg.substring(ARG_OPERATION.length()));
        		}
        		else if ( arg.startsWith (ARG_STARTDATE) ) {
        			startDateStr = arg.substring(ARG_STARTDATE.length());
        			startDate = df.parse(startDateStr);
        		}
        		else if ( arg.startsWith (ARG_STORENUM) ) {
        			storeNum = arg.substring(ARG_STORENUM.length());
        		}
        		else if ( arg.startsWith (ARG_WEEKCOUNT) ) {
        			weekCount = Integer.parseInt(arg.substring(ARG_WEEKCOUNT.length()));
        		}
        		else if ( arg.startsWith (ARG_PREDICTION_WEEK) ) {
        			predWeek = Integer.parseInt(arg.substring(ARG_PREDICTION_WEEK.length()));
        		}
    		}

    		startDateProvided = true;
        	if ( startDate == null ) {
        		startDateProvided = false;
        		// set 52 weeks ago
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(new Date());
        		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        		cal.add(Calendar.DATE, - 7 * weekCount);
        		startDate = cal.getTime();
        		startDateStr = df.format(startDate);
        	}
        	if ( endDate == null ) {
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(startDate);
        		cal.add(Calendar.DATE, 6 + 7 * (weekCount-1));
        		endDate = cal.getTime();
        	}

        	try
        	{
    	        Connection conn = DBManager.getConnection();
    	        conn.setAutoCommit(false);
    	        
            	switch ( operation )
            	{
    	    		case ARG_OPERATION_SETUP:
    	    		{
    	            	Customer customer = new Customer (startDate, endDate);
    	            	customer.setConnection(conn);
    	            	customer._StoreNum = storeNum;
    	            	customer.setupDates();
    	    			customer.setupCustomers ();
    	    			break;
    	    		}
            		case ARG_OPERATION_SHOPPINGDAY:
            		{
    	            	Customer customer = new Customer (startDate, endDate);
    	            	customer.setConnection(conn);
    	            	customer._StoreNum = storeNum;
    	            	customer._DailyPercentOfWeekly = Double.parseDouble(PropertyManager.getProperty("CUSTOMER.DAILY_PERCENT_OF_WEEKLY", "0.1"));
    	            	customer._IgnoreWeekGap = Integer.parseInt(PropertyManager.getProperty("CUSTOMER.IGNORE_WEEK_GAP", "8"));
    	            	customer.loadShoppingDays (storeNum);
            			break;
            		}
            		case ARG_OPERATION_COMMON_ITEMS:
            		{
                    	CustomerItem customerItem = new CustomerItem (startDate, endDate);
                    	customerItem.setConnection(conn);
                    	customerItem.setStoreNum(storeNum);
                    	customerItem.setupDates();
                    	customerItem.setPercentOfVisitCount (Double.parseDouble(PropertyManager.getProperty("CUSTOMER.ITEM_PERCENT_OF_VISITS", "0.6")));
                    	customerItem.setPercentOfFequencyCount (Double.parseDouble(PropertyManager.getProperty("CUSTOMER.ITEM_PERCENT_OF_COUNT", "0.6")));
                    	customerItem.setCustomerMax(Integer.parseInt(PropertyManager.getProperty("CUSTOMER.ANALYZE_COUNT", "1000")));
                    	customerItem.setPredictionWeek(predWeek);
                    	String ignoreItems = PropertyManager.getProperty("CUSTOMER.IGNORE_ITEMS", null);
                    	if ( ignoreItems != null )
                    		customerItem.setIgnoreItems(ignoreItems);
                    	customerItem.loadPurchased (storeNum);
            			break;
            		}
            		case ARG_OPERATION_COMMON_LIG:
            		{
            			CustomerLIG customerLig = new CustomerLIG (startDate, endDate);
            			customerLig.setConnection(conn);
            			customerLig.setStoreNum(storeNum);
            			customerLig.setupDates();
            			customerLig.setPercentOfVisitCount (Double.parseDouble(PropertyManager.getProperty("CUSTOMER.LIG_PERCENT_OF_VISITS", "0.7")));
            			customerLig.setPercentOfFequencyCount (Double.parseDouble(PropertyManager.getProperty("CUSTOMER.LIG_PERCENT_OF_COUNT", "0.6")));
            			customerLig.setCustomerMax(Integer.parseInt(PropertyManager.getProperty("CUSTOMER.ANALYZE_COUNT", "1000")));
            			customerLig.setPredictionWeek(predWeek);
                    	String ignoreItems = PropertyManager.getProperty("CUSTOMER.IGNORE_ITEMS", null);
                    	if ( ignoreItems != null )
                    		customerLig.setIgnoreItems(ignoreItems);
                    	customerLig.loadPurchased (storeNum);
            			break;
            		}
            		case ARG_OPERATION_COMMON_SEGMENT:
            		{
            			CustomerSegment customerSegment = new CustomerSegment (startDate, endDate);
            			customerSegment.setConnection(conn);
            			customerSegment.setStoreNum(storeNum);
            			customerSegment.setupDates();
            			customerSegment.setPercentOfVisitCount (Double.parseDouble(PropertyManager.getProperty("CUSTOMER.SEGMENT_PERCENT_OF_VISITS", "0.7")));
            			customerSegment.setPercentOfFequencyCount (Double.parseDouble(PropertyManager.getProperty("CUSTOMER.SEGMENT_PERCENT_OF_COUNT", "0.6")));
            			customerSegment.setCustomerMax(Integer.parseInt(PropertyManager.getProperty("CUSTOMER.ANALYZE_COUNT", "1000")));
            			customerSegment.setPredictionWeek(predWeek);
                    	String ignoreItems = PropertyManager.getProperty("CUSTOMER.IGNORE_ITEMS", null);
                    	if ( ignoreItems != null )
                    		customerSegment.setIgnoreItems(ignoreItems);
            			customerSegment.loadPurchased(storeNum);
            			break;
            		}
            		case ARG_OPERATION_ITEM_PREDICTION_VERIFICATION:
            		{
            			if ( startDateProvided == false )
            			{
	                		Calendar cal = Calendar.getInstance();
	                		cal.setTime(new Date());
	                		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
	                		startDate = cal.getTime();
	                		startDateStr = df.format(startDate);
	                		
	                		cal.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
	                		endDate = cal.getTime();
            			}
                		
            			CustomerItemVerification verification = new CustomerItemVerification (startDate, endDate);
                    	verification.setConnection(conn);
                    	verification.setStoreNum(storeNum);
                    	verification.setupDates();
                    	verification.setPredictionWeek(predWeek);
                    	verification.setCustomerMax(Integer.parseInt(PropertyManager.getProperty("CUSTOMER.VERIFICATION_COUNT", "1000")));
                    	verification.verifyPrediction (storeNum);
            			break;
            		}
            	}

        		// Close the connection
        		PristineDBUtil.close(conn);
        	}
            catch (GeneralException ex) {
                logger.error("Error", ex);
            }
		}
        catch (Exception ex) {
            logger.error("Error", ex);
        }
        
		logger.info("Customer finished");
	}

	private void setupCustomers ()
	{
		logger.info("setupCustomers begin");
		
		try
		{
			CustomerDAO dao = new CustomerDAO();
			dao.createNewCustomers (_Conn, _WeekStartDate, _WeekEndDate);
			
			// Populate store id and other fields 
			String stores = _StoreNum != null ? "'" + _StoreNum + "'" : "'0108','0227'";
			List<CustomerDTO> inCompleteList = dao.getIncompleteCustomers (_Conn, stores, _WeekStartDate, _WeekEndDate);
			List<CustomerDTO> customers = new ArrayList<CustomerDTO>();
			int curCustId = -1;
			CustomerDTO curCustomer = null;
			boolean firstRecord = false;
			for ( CustomerDTO customer : inCompleteList )
			{
				int custId = customer.id;
				if ( custId != curCustId )
				{
					if ( curCustId != -1 ) {
						customers.add(curCustomer);
					}
					curCustomer = customer;
					curCustId = custId;
					firstRecord = true;
				}
				if ( !firstRecord && curCustomer.secondStoreId == -1 ) {
					curCustomer.secondStoreId = customer.storeId;
				}
				firstRecord = false;
			}
			
			if ( curCustomer != null )
				customers.add(curCustomer);		// last customer
			
			dao.updateCustomers (_Conn, customers);
		}
		catch (Exception ex) {
			logger.error("setupCustomers Error:", ex);
		}
		
		String str = "setupCustomers end:";
		logger.info(str);
	}

	private void loadShoppingDays (String storeNum)
	{
		logger.info("loadShoppingDays begin: storeNum=" + storeNum);
		
		if ( storeNum != null )
		{
			Calendar cal = Calendar.getInstance();
			cal.setTime(_WeekStartDate);
			//Date weekStartDate = cal.getTime();
			
			_Count = 0;
			_CountFailed = 0;
			CustomerDAO dao = new CustomerDAO();
			try
			{
				List<Integer> customerIds = dao.getStoreCustomers(_Conn, storeNum);
				//List<Integer> customerIds = dao.getEnabledStoreCustomers(_Conn, storeNum);
				int listSize = customerIds.size();
				int nBatches = listSize / _CustomerMax + ((listSize % _CustomerMax) == 0 ? 0 : 1);
				//nBatches = 1;
				for (int ii = 0; ii < nBatches; ii++ )
				{
					StringBuffer sb = new StringBuffer();
					for (int jj = ii * _CustomerMax; jj < (ii+1) * _CustomerMax && jj < listSize; jj++ )
					{
						if ( jj > ii * _CustomerMax )
							sb.append(',');
						sb.append(customerIds.get(jj));
					}
					//sb = new StringBuffer("'046102452654','046105707846','046106096807'");
					List<CustomerItemDTO> customers = dao.getShoppingData (_Conn, storeNum, _WeekStartDate, _WeekEndDate, sb.toString(), true);
					_ShoppingDays = new HashMap<Integer, CustomerShoppingDays>();
					loadShoppingDays (customers);
					int size = _ShoppingDays.size();
					//if ( size > 100 )
					//	break;
					//ii = nBatches;
					// update database
					
					boolean doit = true;
					if ( doit )
						updateShoppingInfo (_ShoppingDays);
				}
			}
			catch (GeneralException ex) {
				logger.error("Error:", ex);
			}
			
			String str = "loadShoppingDays end: storeNum=" + storeNum + ":: " + String.valueOf(_Count) + " successful, " + String.valueOf(_CountFailed) + " failed.";
			logger.info(str);
		}
	}
	
	private void loadShoppingDays (List<CustomerItemDTO> customers)
	{
		int count = customers.size();
		int countFailed = 0;
		int customerCount = 0;
		int customerDroppedCount = 0;
		
		String str = "loadShoppingDays2 start: " + count + " records ";
		logger.info(str);

		Map<Integer, CustomerShoppingDays> customerDayCount = _ShoppingDays;
		
		try
		{
			int curCustomerId = -1;
			String curCustomerNo = null;
			List<DayCountDTO> finalDayList = null;
			int[] weeklyVisitCounts = null;
			int[] dailyVisitCounts = null;
			List<DayCountDTO> weekDayList = Util.getSevenDTOs();

			Calendar cal = Calendar.getInstance();
			Date curDate = new Date();
			cal.setTime(curDate);
			cal.set(Calendar.YEAR, 2000);
			
			Date nextWeekStartDate = null;
			double weeklySpend = 0;
			int visitCountActual = 0;
			int visitCount2Week = 0;
			boolean visit2WeekSkipped = false;
			int gapInWeeks = -1;
			boolean skipCustomer = false;
			int customerTotalCount = customers.size();
			for ( int custInd = 0; custInd < customerTotalCount; custInd++ )
			{
				CustomerItemDTO customer = customers.get(custInd);
				int customerId = customer.id;
				String customerNo = customer.customerNo;
				double spend = customer.price;
				Date timestamp = customer.timestamp;
				
				cal = Calendar.getInstance();
				cal.setTime(timestamp);
				int day = cal.get(Calendar.DAY_OF_WEEK);
				Date date = DateUtil.getPureDate(timestamp);
				
				if ( customerId != curCustomerId )
				{
					// different card
					//if ( !skipCustomer && curCustomerId != -1 )
					if ( curCustomerId != -1 )
					{
						CustomerShoppingDays custDays = setupCustomerShoppingDays (weekDayList, finalDayList, weeklySpend,
																	visitCountActual, gapInWeeks, visitCount2Week,
																	dailyVisitCounts, weeklyVisitCounts, curCustomerNo);
						if ( custDays != null ) {
							custDays.customer.customerNo = curCustomerNo;
							custDays.customer.lastVisitDate = curDate;
							customerDayCount.put(curCustomerId, custDays);
						}
						else {
							customerDroppedCount++;
						}
					}
				
					int dayDiff = (int)DateUtil.getDateDiff(date, _WeekStartDate);
					int weekDiff = dayDiff / 7;
					skipCustomer = weekDiff > 4;
					if ( skipCustomer ) {
						_SkippedCustomers.add(customerNo);
					}
					
					curCustomerId = customerId;
					curCustomerNo = customerNo;
					nextWeekStartDate = null;
					weeklySpend = 0;
					visitCountActual = 0;
					visitCount2Week = 0;
					visit2WeekSkipped = false;
					gapInWeeks = 0;
					
					finalDayList = Util.getSevenDTOs();
					weeklyVisitCounts = Util.get52WeeklyVisitCounts();
					dailyVisitCounts = Util.getIntArray(365);
					
					customerCount++;
				}
				
				if ( skipCustomer )
				{
					//if ( customerCount > 0 )
					//	skippedCustomers.append(',');
					//skippedCustomers.append(customerNo);
					//continue;
				}
				
				if ( nextWeekStartDate == null ) {
					nextWeekStartDate = DateUtil.getEndOfSaturday(date);
				}
				
				// different date?
				if ( !date.equals(curDate) )
				{
					if ( date.after(nextWeekStartDate) )
					{
						nextWeekStartDate = DateUtil.getEndOfSaturday(date);
						
						updateFinalDayList (weekDayList, finalDayList, weeklySpend);

						// calculate week gap
						Date curSunday = DateUtil.getSunday(curDate);
						int weekDiff = (int) (DateUtil.getDateDiff(date, curSunday) / 7);
						if ( weekDiff > gapInWeeks )
							gapInWeeks = weekDiff;
						
						weeklySpend = 0;
						if ( visitCount2Week == 0 )
							visitCount2Week++;
						else if ( weekDiff == 1 )
						{
							if ( visit2WeekSkipped ) {
								visitCount2Week++;
								visit2WeekSkipped = false;
							}
							else
								visit2WeekSkipped = true;
						}
						else if ( weekDiff > 1 ) {
							visitCount2Week++;
							visit2WeekSkipped = false;
						}
					}

					int dayDiff = (int)DateUtil.getDateDiff(date, _WeekStartDate);
					dailyVisitCounts[dayDiff]++;
					int weekDiff = dayDiff / 7;
					weeklyVisitCounts[weekDiff]++;
					
					visitCountActual++;
					weekDayList.get(day-1).count++;
					curDate = date;
				}
				
				weekDayList.get(day-1).spend += spend;  
				weeklySpend += spend;
				
				//printWeekList (day, weekDayList);
				
				//SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
				//String dateStr = formatter.format(date);
				//logger.debug(customerNo + "," + weekDay + "," + dateStr);
			} // while
			
			// add last customer data
			CustomerShoppingDays custDays = setupCustomerShoppingDays (weekDayList, finalDayList, weeklySpend,
												visitCountActual, gapInWeeks, visitCount2Week, dailyVisitCounts, weeklyVisitCounts,
												curCustomerNo);
			if ( custDays != null ) {
				custDays.customer.customerNo = curCustomerNo;
				custDays.customer.lastVisitDate = curDate;
				customerDayCount.put(curCustomerId, custDays);
			}
			else {
				customerDroppedCount++;
			}
		}
		catch (Exception ex) {
			countFailed++;
		}
		
		_Count += count;
		_CountFailed += countFailed;

		logger.debug ("loadShoppingDays2:: Actual_17_115 rule: count=" + _Actual_17_115.size() + " - " + Util.getCommaList (_Actual_17_115));
		logger.debug ("loadShoppingDays2:: SkippedCustomers: count=" + _SkippedCustomers.size() + " - " + Util.getCommaList (_SkippedCustomers));
		logger.debug ("loadShoppingDays2:: NoVisitIn4Weeks rule: count=" + _NoVisitIn4Weeks.size() + " - " + Util.getCommaList (_NoVisitIn4Weeks));
		logger.debug ("loadShoppingDays2:: MoreThan4VisitInAWeek rule: count=" + _MoreThan4VisitInAWeek.size() + " - " + Util.getCommaList (_MoreThan4VisitInAWeek));
		logger.debug ("loadShoppingDays2:: VisitAndVisit2Diff: count=" + _VisitAndVisit2Diff.size() + " - " + Util.getCommaList (_VisitAndVisit2Diff));

		str = "loadShoppingDays2 end: processed " + count + ", failed " + countFailed + ", customers dropped " + customerDroppedCount;
		logger.info(str);
	}

	private CustomerShoppingDays setupCustomerShoppingDays (List<DayCountDTO> weekDayList, List<DayCountDTO> finalDayList, double weeklySpend,
			int visitCountActual, int gapInWeeks, int visitCount2Week, int[] dailyVisitCounts, int[] weeklyVisitCounts, String customerNo)
	{
		updateFinalDayList (weekDayList, finalDayList, weeklySpend);
		
		//if ( visitCountActual > _IgnoreVisitCountActual || gapInWeeks > _IgnoreWeekGap )
		//	return null;
		
		if ( visitCountActual <= 20 || visitCountActual >= 120 ) {
		//if ( visitCountActual <= 19 || visitCountActual >= 41 ) {
			_Actual_17_115.add(customerNo);
			return null;
		}
		
		int totalCount;
		
		// at least one visit in every 4 weeks
		for ( int ii = 0; ii < (52-3); ii += 4 )
		{
			totalCount = 0;
			for ( int jj = 0; jj < 4; jj++ ) {
				totalCount += weeklyVisitCounts[ii+jj];
			}
			if ( totalCount == 0 ) {
				_NoVisitIn4Weeks.add(customerNo);
			//	return null;
			}
		}
		
		// more than 4 visits in a single week?
		for ( int ii = 0; ii < 52; ii++ )
		{
			int count = weeklyVisitCounts[ii]; 
			if ( count > 5 ) {
				_MoreThan4VisitInAWeek.add(customerNo);
			//	return null;
			}
		}
		
/*
		// no visit in first 4 weeks?
		int totalCount = 0;
		for ( int ii = 0; ii < 4; ii++ )
			totalCount += weeklyVisitCounts[ii];
		if ( totalCount == 0 ) {
			_NoVisitFirst4Weeks.add(customerNo);
			return null;
		}
		
		// more than 14 visits in 4 weeks?
		for ( int ii = 0; ii < (52-3); ii++ )
		{
			totalCount = 0;
			for ( int jj = 0; jj < 4; jj++ ) {
				totalCount += weeklyVisitCounts[ii+jj];
			}
			if ( totalCount > 14 ) {
				_MoreThan14VisitIn4Weeks.add(customerNo);
				return null;
			}
		}
		
		// no visit in 8 weeks?
		for ( int ii = 0; ii < (52-7); ii++ )
		{
			totalCount = 0;
			for ( int jj = 0; jj < 8; jj++ ) {
				totalCount += weeklyVisitCounts[ii+jj];
			}
			if ( totalCount == 0 ) {
				_NoVisitIn8Weeks.add(customerNo);
				return null;
			}
		}
*/
	
		// visit count, gap in weeks
		int visitCount = 0;
		int gapInWeeks2 = 1, gapTemp = 1;
		for ( int ii = 0; ii < 52; ii++ )
		{
			if ( weeklyVisitCounts[ii] > 0 ) {
				visitCount++;
				if ( gapTemp > gapInWeeks2 )
					gapInWeeks2 = gapTemp;
				gapTemp = 1;
			}
			else
				gapTemp++;
		}
		
		if ( gapInWeeks2 != gapInWeeks ) {			// debugging
			_VisitAndVisit2Diff.add(customerNo);
			logger.debug("Customer=" + customerNo + ": gapInWeeks2=" + gapInWeeks2 + ", gapInWeeks=" + gapInWeeks);
		}
		
		CustomerShoppingDays custDays = new CustomerShoppingDays();
		
		CustomerDTO cust = new CustomerDTO();
		cust.visitCount = visitCount;
		cust.totalCount = visitCountActual;
		cust.gapInWeeks = gapInWeeks;
		
		custDays.customer = cust;
		int[] weekdayCounts = Util.getIntArray(7);
		Calendar cal = Calendar.getInstance();
		for ( int ii = 0; ii < dailyVisitCounts.length; ii++ ) {
			if ( dailyVisitCounts[ii] > 0 ) {
				cal.setTime(_WeekStartDate);
				cal.add(Calendar.DATE, ii);
				int day = cal.get(Calendar.DAY_OF_WEEK) - 1;
				weekdayCounts[day]++;
			}
		}
		custDays.dayCountList = finalDayList;
		custDays.weekdayCounts = weekdayCounts;
		StringBuffer sb = new StringBuffer();
		custDays.frequencyCounts = Util.setupFrequencies(dailyVisitCounts, sb);
		
		// find max count day, 2-days and 3-days
		setMaxDayCounts (custDays.dayCountList, cust);
		
		return custDays;
	}
	
	private void updateFinalDayList (List<DayCountDTO> weekDayList, List<DayCountDTO> finalList, double weeklySpend)
	{
		for ( int ii = 0; ii < 7; ii++ ) 
		{
			DayCountDTO dto = weekDayList.get(ii);
			if ( dto.count > 0 ) {
				boolean primaryDay = ((double)dto.spend / weeklySpend) > _DailyPercentOfWeekly;
				if ( primaryDay )
					finalList.get(ii).count += dto.count;
			}

			// reset
			dto.count = 0;
			dto.spend = 0;
		}
	}

	private void updateShoppingInfo (Map<Integer, CustomerShoppingDays> shoppingDays)
	{
		// update customer table
		Iterator<Integer> iter = shoppingDays.keySet().iterator();
		_Count = 0;
		_CountFailed = 0;
		while ( iter.hasNext() )
		{
			int id = iter.next();
			CustomerShoppingDays custDays = shoppingDays.get(id);
			CustomerDTO customer = custDays.customer;
			customer.id = id;
			
			// update customer table
			CustomerDAO dao = new CustomerDAO();
			try {
				boolean result = dao.updateShoppingInfo(_Conn, customer);
				if ( result )
					_Count++;
				else 
					_CountFailed++;
			}
			catch (Exception e) {
				// Error
			}
		}
	}

	private void setMaxDayCounts (List<DayCountDTO> dayCountList, CustomerDTO customer)
	{
		List<DayCountDTO> list = Util.duplicateList (dayCountList);
		
		getMaxDayCountKey (list, customer);
		getMax2DayCountKey (list, customer);
		getMax3DayCountKey (list, customer);
	}
	
	private List<String> getMaxDayCountKey (List<DayCountDTO> dayCountList, CustomerDTO customer)
	{
		int maxCount = 0;
		DayCountDTO maxDayCountDTO = null;
		for ( int ii = 0; ii < 7; ii++ )
		{
			DayCountDTO dayCountDTO = dayCountList.get(ii);
			int count = dayCountDTO.count;
			if ( count > maxCount ) {
				maxCount = count;
				maxDayCountDTO = dayCountDTO;
			}
		}
		
		List<String> ret = new ArrayList<String>();
		if ( maxDayCountDTO != null )
		{
			String key = String.valueOf(maxDayCountDTO.day);
			ret.add(key);
			
			if ( customer != null ) {
				customer.shoppingDay = key;
				customer.shoppingDayCount = maxCount;
				maxDayCountDTO.count = 0;
			}
		}
		
		return ret;
	}
	
	private List<String> getMax2DayCountKey (List<DayCountDTO> dayCountList, CustomerDTO customer)
	{
		List<String> keys = getMaxDayCountKey (dayCountList, null);
		if ( keys.size() > 0 )
		{
			int index =  Integer.parseInt(keys.get(0));
			DayCountDTO keyDTO = dayCountList.get(index);
			
			if ( customer != null ) {
				customer.shopping2Day = customer.shoppingDay + "+" + keys.get(0);
				customer.shopping2DayCount = customer.shoppingDayCount + keyDTO.count;
			}
			
			keyDTO.count = 0;
		}
			
		return keys;
	}
	
	private List<String> getMax3DayCountKey (List<DayCountDTO> dayCountList, CustomerDTO customer)
	{
		List<String> keys = getMaxDayCountKey (dayCountList, null);
		if ( keys.size() > 0 )
		{
			int index =  Integer.parseInt(keys.get(0));
			DayCountDTO keyDTO = dayCountList.get(index);
			
			if ( customer != null ) {
				customer.shopping3Day = customer.shopping2Day + "+" + keys.get(0);
				customer.shopping3DayCount = customer.shopping2DayCount + keyDTO.count;
			}
		}
		
		//keyDTO.count = 0;
		
		return keys;
	}
	
	public static String getPrimaryShoppingDays (CustomerDTO customer)
	{
		double probability = ((double)customer.shoppingDayCount / customer.totalCount);
		String days = customer.shoppingDay;
		if ( probability <= 0.7 )
		{
			double tmp = ((double)customer.shopping2DayCount / customer.totalCount);
			if ( tmp > 0.75 ) {
				probability = tmp;
				days = customer.shopping2Day;
			}
			else {
				tmp = ((double)customer.shopping3DayCount / customer.totalCount);
				if ( tmp > 0.8 ) {
					probability = tmp;
					days = customer.shopping3Day;
				}
				else {
					days = null;
				}
			}
		}
		
		return days;
	}

	public void setConnection (Connection conn) { _Conn = conn; }
	
	private static void logCommand (String[] args)
	{
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: Customer ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	}

	private void setupDates ()
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

	//
	// Data members
	//
	static Logger 		logger = Logger.getLogger("Customer");

    private Connection	_Conn = null;
	private int			_Count = 0;
	private int			_CountFailed = 0;
	private int			_StopAfter = -1;
	
	private Date  		_WeekStartDate;
	private Date  		_WeekEndDate;
	private String		_StoreNum;
	private int			_WeekCount = 0;
	
	private boolean		_OnlyPrediction = false;

	private Map<Integer, CustomerShoppingDays>	_ShoppingDays = null; 
	private Map<Integer, CustomerDTO>			_PrimaryShoppingDays = null;

	private double	_DailyPercentOfWeekly = 0.1;
//	private double	_PercentOfVisitCount = (double)0.8;
//	private int		_IgnoreVisitCountActual = 130;
	private int		_IgnoreWeekGap = 8;
	private int _CustomerMax = 1000;
	
	// For debugging
	private List<String> _SkippedCustomers = new ArrayList<String>();
	private List<String> _Actual_17_115 = new ArrayList<String>();
	//private List<String> _NoVisitFirst4Weeks = new ArrayList<String>();
	private List<String> _NoVisitIn4Weeks = new ArrayList<String>();
	//private List<String> _MoreThan14VisitIn4Weeks = new ArrayList<String>();
	private List<String> _MoreThan4VisitInAWeek = new ArrayList<String>();
	//private List<String> _NoVisitIn8Weeks = new ArrayList<String>();
	private List<String> _VisitAndVisit2Diff = new ArrayList<String>();
	
	// Main command flags
	private static final String ARG_OPERATION	= "OPERATION=";
	private static final String ARG_STARTDATE	= "STARTDATE=";
	private static final String ARG_STORENUM	= "STORENUM=";
	private static final String	ARG_WEEKCOUNT	= "WEEKCOUNT=";
	private static final String ARG_PREDICTION_WEEK	= "PREDICTIONWEEK=";

	// Secondary command flags
	private static final int ARG_OPERATION_INVALID				= -1;
	
	private static final int ARG_OPERATION_SETUP				= 1;
	private static final int ARG_OPERATION_SHOPPINGDAY			= 2;
	private static final int ARG_OPERATION_VISIT_COUNT			= 3;
	
	private static final int ARG_OPERATION_COMMON_ITEMS			= 4;
	private static final int ARG_OPERATION_COMMON_LIG			= 5;
	private static final int ARG_OPERATION_COMMON_SEGMENT		= 6;
	private static final int ARG_OPERATION_ITEM_SIZE_GROUPING 	= 7;
	
	private static final int ARG_OPERATION_ITEM_PREDICTION_VERIFICATION	= 11;

/*
	private String[] _Daykeys = { "0", "1", "2", "3", "4", "5", "6",
			  "0+1", "1+2", "2+3", "3+4", "4+5", "5+6", "6+0",
			  "0+1+2", "1+2+3", "2+3+4", "3+4+5", "4+5+6", "5+6+0", "6+0+1",
			  "0^1", "1^2", "2^3", "3^4", "4^5", "5^6", "6^0",
			  "0^1^2", "1^2^3", "2^3^4", "3^4^5", "4^5^6", "5^6^0", "6^0^1",
			};
*/

	//
	// Classes
	//
	private class CustomerShoppingDays
	{
		CustomerDTO				customer;
		List<DayCountDTO>		dayCountList;
		int[]					weekdayCounts;
		Map<Integer, Integer>	frequencyCounts;
	}

}
