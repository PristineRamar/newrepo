package com.pristine.customer;

import java.sql.SQLException;
import java.util.*;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.customer.CustomerDTO;
import com.pristine.dto.customer.CustomerPredictionDTO;
import com.pristine.dto.customer.CustomerVerificationDTO;
import com.pristine.dao.customer.CustomerDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;

import com.pristine.dto.customer.CustomerVerificationDTO;

public class CustomerItemVerification extends CustomerItem
{
	public CustomerItemVerification (Date weekStart, Date weekEnd)
	{
		super (weekStart, weekEnd);
	}

	public void verifyPrediction (String storeNum)
	{
		logger.info("verifyPrediction begin: store num=" + storeNum);
		
		if ( storeNum != null )
		{
			CustomerDAO dao = new CustomerDAO();
			try
			{
				Date reportDate = _WeekStartDate;
				_WeekEndDate = DateUtil.getEndOfSaturday(_WeekStartDate);
				
				_StoreId = dao.getStoreId (_Conn, storeNum);
				
				List<Integer> customerIds = dao.getCustomersFromCustomerPurchase (_Conn, _StoreId, reportDate);
				//List<Integer> customerList = new ArrayList<Integer>();
				//customerList.add(472); customerList.add(475); customerList.add(481); customerList.add(482); customerList.add(485); customerList.add(487);
				int listSize = customerIds.size();
				int nBatches = listSize / _CustomerMax + ((listSize % _CustomerMax) == 0 ? 0 : 1);
				logger.info("verifyPrediction: " + listSize + " customers in " + nBatches + " batches of " + _CustomerMax + " each");
				
				int count = 0, totalCount = 0;
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
					logger.info("verifyPrediction: Loading " + (ii+1) + " batch of " + nBatches + " batches");
					
					Map<String, CustomerVerificationDTO> predictions = dao.getPurchaseItemsForVerification (_Conn, _StoreId, reportDate, _WeekStartDate, custIdFirst, custIdLast);
					CachedRowSet purchasedItemsInWeek53 = dao.getCustomerItemMovement (_Conn, storeNum, _WeekStartDate, _WeekEndDate, custIdFirst, custIdLast);
					verifyPrediction (predictions, purchasedItemsInWeek53, reportDate, _WeekStartDate);
					totalCount += count;
					count = 0;
				}
			}
			catch (GeneralException ex) {
				logger.error("verifyPrediction error: ", ex);
			}
		}
		
		logger.info("verifyPrediction end");
	}

	private void verifyPrediction (Map<String, CustomerVerificationDTO> predictions, CachedRowSet purchasedItemsInWeek53,
			Date reportDate, Date predWeekDate)
	{
		logger.info("verifyPrediction2 begin: " + purchasedItemsInWeek53.size() + " items purchased in 53rd week");
		
		_Count = 0;
		_CountFailed = 0;
		try
		{
			List<CustomerVerificationDTO> list = new ArrayList<CustomerVerificationDTO>();
			Map<String, CustomerVerificationDTO> ligMap = null;
			CustomerDAO dao = new CustomerDAO();
			
			int count = 0; 
			while ( purchasedItemsInWeek53.next() )
			{
				try
				{
					String custIdStr = purchasedItemsInWeek53.getString("ID");
					String itemCodeStr = purchasedItemsInWeek53.getString("ITEM_CODE");
					Object obj = purchasedItemsInWeek53.getObject("RET_LIR_ID");
					String ligStr = obj != null ? purchasedItemsInWeek53.getString("RET_LIR_ID") : null;
					obj = purchasedItemsInWeek53.getObject("SEGMENT_ID");
					String segmentStr = obj != null ? purchasedItemsInWeek53.getString("SEGMENT_ID") : null;
					
					String key = custIdStr + ":" + itemCodeStr;
					CustomerVerificationDTO dto = predictions.get(key);
					if ( dto != null )
					{
						dto.purchasedItemIn53rdWeek = true;
						if ( ligStr != null )
							dto.purchasedLIGIn53rdWeek = true;
						if ( segmentStr != null )
							dto.purchasedSegmentIn53rdWeek = true;
					}
					else
					{
						// Is LIG same?
						if ( ligMap == null )
							ligMap = getLIGPredictionMap (predictions);
						if ( ligStr != null )
						{
							key = custIdStr + ":" + ligStr;
							dto = ligMap.get(key);
							if ( dto != null )
								dto.purchasedLIGIn53rdWeek = true;
						}
							
						if ( dto != null ) {
							// Is segment same?
						}
					}
					
					if ( dto != null )
					{
						list.add(dto);
						count++;
						if ( count == 5000 ) {
							// Update verification info
							int nUpdated = dao.updateVerificationInfo(_Conn, _StoreId, reportDate, predWeekDate, list);
							_Count += nUpdated;
							
							count = 0;
							list = new ArrayList<CustomerVerificationDTO>();
						}
					}
				}
				catch (SQLException ex) {
					logger.error("verifyPrediction2: error accessing result data", ex);
					continue;
				}
			}
			
			// last batch
			if ( count > 0 ) {
				int nUpdated = dao.updateVerificationInfo(_Conn, _StoreId, reportDate, predWeekDate, list);
				_Count += nUpdated;
			}
		}
		catch (SQLException sqlEx) {
			logger.error("verifyPrediction2", sqlEx);
		}
		
		logger.info("verifyPrediction2 end");
	}

	//
	// Return hashmap with LIG id as key
	//
	Map<String, CustomerVerificationDTO> getLIGPredictionMap (Map<String, CustomerVerificationDTO> itemMap)
	{
		Map<String, CustomerVerificationDTO> ligMap = new HashMap<String, CustomerVerificationDTO>();
		Set<String> keys = itemMap.keySet();
		Iterator<String> iter = keys.iterator();
		while ( iter.hasNext() )
		{
			String key = iter.next();
			CustomerVerificationDTO dto = itemMap.get(key);
			String ligKey = String.valueOf(dto.customerId) + ":" + String.valueOf(dto.lirId);
			ligMap.put(ligKey,  dto);
		}
		return ligMap;
	}
/*	
	private List<String> getUniqueCustomers (Map<String, CustomerVerificationDTO> custItems)
	{
		List<String> list = new ArrayList<String>();
		Set<String> custKeys = custItems.keySet();
		Iterator<String> custIter = custKeys.iterator();
		while ( custIter.hasNext() )
		{
			String[] tuple = custIter.next().split(":");
			if ( !list.contains(tuple[0]) ) {
				list.add (tuple[0]);
			}
		}
		return list;
	}
*/
	
	//
	// Data members
	//
	static Logger logger = Logger.getLogger("CustomerItemVerification");
}
/*
select cp.customer_id, cp.item_code, reg_price, sale_price, cp.report_date
from customer_purchase cp,
(select item_code, reg_price, sale_price from competitive_data_view d where start_date = '7-oct-12' and comp_str_id = 5651) comp_data
where cp.item_code = comp_data.item_code(+)
and cp.report_date = '7-oct-12'
and cp.store_id = 5651
--and d.start_date = '7-oct-12' 
and cp.item_code is not null
*/
