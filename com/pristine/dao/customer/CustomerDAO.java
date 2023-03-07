package com.pristine.dao.customer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.dto.customer.CustomerDTO;
import com.pristine.dto.customer.CustomerPurchaseDTO;
import com.pristine.dto.customer.CustomerItemDTO;
//import com.pristine.dto.customer.DayCountDTO;
import com.pristine.dto.customer.CustomerPredictionDTO;
import com.pristine.dto.customer.CustomerVerificationDTO;
import com.pristine.dto.customer.HouseHoldDTO;
//import com.pristine.dto.customer.CustomerPurchaseFrequencyDTO;
import com.pristine.dto.fileformatter.gianteagle.ZoneDTO;
import com.pristine.dao.IDAO;
import com.pristine.dao.CompStoreDAO;


public class CustomerDAO implements IDAO
{
	static Logger logger = Logger.getLogger("CustomerDAO");

	public int getStoreId (Connection conn, String storeNum) throws GeneralException
	{
		HashMap<String, Integer> storeMap = new CompStoreDAO().getCompStrId(conn);
		return storeMap.get(storeNum);
	}
	
	public List<Integer> getStoreCustomers (Connection conn, String storeNum) throws GeneralException
	{
		List<Integer> list = new ArrayList<Integer>();
		
		int storeId = getStoreId (conn, storeNum);

		StringBuffer sb = new StringBuffer();
		sb.append("select c.id from customer c");
		sb.append(" where c.store_id = ").append(storeId);
		//sb.append(" and c.active = 'Y'");
		sb.append(" order by c.id");
		
		String sql = sb.toString();
		logger.info ("getStoreCustomers SQL: " + sql);
		
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getStoreCustomers");
			while (result.next()) {
				list.add(result.getInt("id"));
			}
		}
		catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
		catch (SQLException sex) {
			logger.error("SQL: " + sb.toString(), sex);
		}

		return list;
	}

	public List<String> getNewCustomers (Connection conn)
	{
		List<String> list = new ArrayList<String>();
		
		StringBuffer sb = new StringBuffer();
		sb.append("select customer_card_no from movement_daily where customer_card_no is not null");
		sb.append(" and customer_card_no not in (select customer_no from customer)");
		
		String sql = sb.toString();
		logger.debug ("getNewCustomers SQL: " + sql);
		
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getNewCustomers");
			while (result.next()) {
				list.add(result.getString("customer_card_no"));
			}
		}
		catch (GeneralException ex) {
			logger.error("getNewCustomers SQL: " + sql, ex);
		}
		catch (SQLException sex) {
			logger.error("getNewCustomers SQL: " + sql, sex);
		}

		return list;
	}

	public void createNewCustomers (Connection conn, Date start, Date end)
		// start and end can be NULL
	{
		logger.info ("createNewCustomers begin: from " + start + " to " + end);
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT distinct customer_card_no FROM movement_daily WHERE customer_card_no IS NOT NULL");
		sb.append(" AND customer_card_no NOT IN (SELECT customer_no FROM customer)");
		if ( start != null ) {
			String startStr = formatter.format(start);
			sb.append(" AND pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		}
		if ( end != null ) {
			String endStr = formatter.format(end);
			sb.append(" AND pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		}
		
		String sql = sb.toString();
		logger.debug ("createNewCustomers SQL: " + sql);
		
		CachedRowSet result = null;
		int count = 0, countFailed = 0;
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "createNewCustomers");
			while (result.next())
			{
				String custNo = result.getString("customer_card_no");
				StringBuffer sb2 = new StringBuffer();
				sb2.append("INSERT INTO customer (id, customer_no) VALUES (");
				sb2.append("CUSTOMER_ID_SEQ.NEXTVAL, '").append(custNo).append("'");
				sb2.append(")");
				try {
					PristineDBUtil.execute(conn, sb2, "createNewCustomers");
					count++;
					if ( count % 10000 == 0 )
						logger.info ("createNewCustomers: created " + count + " customers");
				}
				catch (GeneralException ex) {
					logger.error("createNewCustomers SQL: " + sb2.toString(), ex);
					countFailed++;
				}
			}
		}
		catch (GeneralException ex) {
			logger.error("createNewCustomers SQL: " + sql, ex);
		}
		catch (SQLException sex) {
			logger.error("getNewCustomers SQL: " + sql, sex);
		}
		
		logger.info ("createNewCustomers end: created " + count + " customers, failed " + countFailed);
	}

	public List<CustomerDTO> getIncompleteCustomers (Connection conn, String stores, Date start, Date end)
	{
		// stores: comma separated list of store numbers (can be NULL)
		
		logger.info ("getIncompleteCustomers begin");
		
		List<CustomerDTO> list = new ArrayList<CustomerDTO>();
		
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT id, comp_str_id, COUNT(transaction_no) AS visit_count FROM");
		sb.append(" (");
		sb.append("  SELECT DISTINCT c.id, s.comp_str_id, m.transaction_no FROM movement_daily m, customer c, competitor_store s");
		sb.append("  WHERE m.customer_card_no = c.customer_no AND m.comp_str_no = s.comp_str_no");
		if ( stores != null ) {
			sb.append("  AND m.comp_str_no in (").append(stores).append(')');
		}
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		if ( start != null ) {
			String startStr = formatter.format(start);
			sb.append(" AND m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		}
		if ( end != null ) {
			String endStr = formatter.format(end);
			sb.append(" AND m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		}
		sb.append("  AND m.customer_card_no IN (SELECT customer_no FROM customer WHERE store_id IS NULL)");
		sb.append(" )");
		sb.append(" GROUP BY id, comp_str_id");
		sb.append(" ORDER BY id, visit_count DESC");

		String sql = sb.toString();
		logger.debug ("getIncompleteCustomers SQL: " + sql);
		
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getIncompleteCustomers");
			while (result.next())
			{
				CustomerDTO cust = new CustomerDTO();
				cust.id = result.getInt("id");
				cust.storeId = result.getInt("comp_str_id");
				cust.visitCount = result.getInt("visit_count");
				list.add(cust);
			}
		}
		catch (GeneralException ex) {
			logger.error("getIncompleteCustomers SQL: " + sql, ex);
		}
		catch (SQLException sex) {
			logger.error("getIncompleteCustomers SQL: " + sql, sex);
		}
		
		logger.info ("getIncompleteCustomers end: " + list.size() + " customers incomplete");
		return list;
	}

	public void updateCustomers (Connection conn, List<CustomerDTO> customers)
	{
		logger.info ("updateCustomers begin");
		
		int count = 0, countFailed = 0;
		for ( CustomerDTO cust : customers )
		{
			StringBuffer sb = new StringBuffer();
			sb.append("UPDATE customer set");
			sb.append(" store_id = ").append(cust.storeId);
			int storeId2 = cust.secondStoreId;
			if ( storeId2 != -1 ) {
				sb.append(", second_store_id = ").append(cust.secondStoreId);
			}
			sb.append(" WHERE id = ").append(cust.id);
			//String sql = sb.toString();
			//logger.debug ("updateCustomers SQL: " + sql);
		
			try
			{
				PristineDBUtil.execute(conn, sb, "updateCustomers");
				count++;
				if ( count % 10000 == 0 )
					logger.info ("updateCustomers: updated " + count + " customers");
			}
			catch (GeneralException ex) {
				logger.error("updateCustomers SQL: " + sb.toString(), ex);
				countFailed++;
			}
		}
		
		logger.info ("updateCustomers end: updated " + count + " customers, failed " + countFailed);
	}
	
	public List<CustomerItemDTO> getShoppingData (Connection conn, String storeNum, Date start, Date end, String customers, boolean bCustomerId)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		StringBuffer sb = new StringBuffer();
		sb.append("select c.id, c.customer_no, m.price, to_date(to_char(m.pos_timestamp,'YYMMDD'),'YYMMDD') as pos_timestamp from movement_daily m, customer c");
		sb.append(" where m.customer_card_no = c.customer_no");
		sb.append(" and m.comp_str_no = '").append(storeNum).append("'");
		sb.append(" and m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and c.active = 'Y'");
		if ( customers != null ) {
			if ( bCustomerId )
				sb.append(" and c.id in (").append(customers).append(')');
			else
				sb.append(" and c.customer_no in (").append(customers).append(')');
		}
		//sb.append(" and m.customer_card_no in ('046117129479','046105789565')");		// TEST
		sb.append(" order by c.id, m.pos_timestamp, m.price");
		
		logger.debug ("getShoppingData SQL: " + sb.toString());
		CachedRowSet result = null;
		List<CustomerItemDTO> customerList = new ArrayList<CustomerItemDTO>();
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getShoppingData");
			while ( result.next() )
			{
				try
				{
					CustomerItemDTO dto = new CustomerItemDTO();
					dto.id = result.getInt("ID");
					dto.customerNo = result.getString("CUSTOMER_NO");
					dto.price = result.getDouble("PRICE");
					dto.timestamp = result.getDate("POS_TIMESTAMP");
					customerList.add(dto);
				}
				catch (SQLException sex) {
					logger.error("SQL: " + sb.toString(), sex);
				}
			}
		}
		catch (SQLException sex) {
			logger.error("getShoppingData SQL: " + sb.toString(), sex);
		}
		catch (GeneralException ex) {
			logger.error("getShoppingData SQL: " + sb.toString(), ex);
		}
		
		return customerList;
	}

	public boolean updateShoppingInfo (Connection conn, CustomerDTO cust)
	{
		boolean ret = true;
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String dateStr = formatter.format(cust.lastVisitDate);
		
		StringBuffer sb = new StringBuffer("update CUSTOMER set");
		sb.append(" VISIT_COUNT = ").append(cust.visitCount);
		sb.append(", TOTAL_COUNT = ").append(cust.totalCount);
		sb.append(", MAX_WEEK_GAP = ").append(cust.gapInWeeks);
		sb.append(", LAST_VISIT_DATE = ").append("to_date('").append(dateStr).append("', 'YYMMDD')");
		if ( cust.shoppingDay != null ) {
			sb.append(", SHOPPING_DAY = '").append(cust.shoppingDay).append("'");
			sb.append(", SHOPPING_DAY_COUNT = ").append(cust.shoppingDayCount);
		}
		if ( cust.shopping2Day != null ) {
			sb.append(", SHOPPING_2DAY = '").append(cust.shopping2Day).append("'");
			sb.append(", SHOPPING_2DAY_COUNT = ").append(cust.shopping2DayCount);
		}
		if ( cust.shopping3Day != null ) {
			sb.append(", SHOPPING_3DAY = '").append(cust.shopping3Day).append("'");
			sb.append(", SHOPPING_3DAY_COUNT = ").append(cust.shopping3DayCount);
		}
		sb.append(" where ID = ").append(cust.id);
		
		//logger.debug ("updateShoppingInfo SQL: " + sb.toString());
		try {
			int updated = PristineDBUtil.executeUpdate(conn, sb, "CustomerDAO - updateShoppingInfo");
			ret = updated > 0;
		}
		catch (GeneralException gex) {
			logger.error("updateShoppingInfo SQL: " + sb.toString(), gex);
		}
		
		return ret;
	}

	public Map<Integer, CustomerDTO> getShoppingInfo (Connection conn, String storeNum, int minVisitCount, int maxVisitCount, int maxWeekGap)
	{
		StringBuffer sb = new StringBuffer("select ID, CUSTOMER_NO, SHOPPING_DAY, SHOPPING_DAY_COUNT, SHOPPING_2DAY, SHOPPING_2DAY_COUNT");
		sb.append(", SHOPPING_3DAY, SHOPPING_3DAY_COUNT, VISIT_COUNT, TOTAL_COUNT, LAST_VISIT_DATE from CUSTOMER");
		sb.append(" where ACTIVE = 'Y'");
		//sb.append(" and MAX_WEEK_GAP <= ").append(maxWeekGap);
		if ( storeNum != null )
		{
			try {
				int storeId = getStoreId(conn, storeNum);
				sb.append(" and STORE_ID = ").append(storeId);
			}
			catch (GeneralException gex) {
				logger.error("getShoppingInfo: Error getting store id for store " + storeNum, gex);
			}
		}
		
		logger.debug ("getShoppingInfo SQL: " + sb.toString());
		CachedRowSet result = null;
		Map<Integer, CustomerDTO> customers = new HashMap<Integer, CustomerDTO>();
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getShoppingInfo");
			while ( result.next() )
			{
				try {
					CustomerDTO dto = new CustomerDTO();
					int custId = result.getInt("ID");
					dto.id = custId;
					dto.customerNo = result.getString("CUSTOMER_NO");
					Object obj = result.getObject("SHOPPING_DAY");
					if ( obj != null ) {
						dto.shoppingDay = result.getString("SHOPPING_DAY");
						dto.shoppingDayCount = result.getInt("SHOPPING_DAY_COUNT");
					}
					obj = result.getObject("SHOPPING_2DAY");
					if ( obj != null ) {
						dto.shopping2Day = result.getString("SHOPPING_2DAY");
						dto.shopping2DayCount = result.getInt("SHOPPING_2DAY_COUNT");
					}
					obj = result.getObject("SHOPPING_3DAY");
					if ( obj != null ) {
						dto.shopping3Day = result.getString("SHOPPING_3DAY");
						dto.shopping3DayCount = result.getInt("SHOPPING_3DAY_COUNT");
					}
					obj = result.getObject("VISIT_COUNT");
					if ( obj != null )
						dto.visitCount = result.getInt("VISIT_COUNT");
					obj = result.getObject("TOTAL_COUNT");
					if ( obj != null )
						dto.totalCount = result.getInt("TOTAL_COUNT");
					obj = result.getObject("LAST_VISIT_DATE");
					if ( obj != null )
						dto.lastVisitDate = result.getDate("LAST_VISIT_DATE");
					
					customers.put(dto.id, dto);
				}
				catch (SQLException sex) {
					logger.error("SQL: " + sb.toString(), sex);
				}
			}
		}
		catch (SQLException sex) {
			logger.error("getShoppingInfo SQL: " + sb.toString(), sex);
		}
		catch (GeneralException gex) {
			logger.error("getShoppingInfo SQL: " + sb.toString(), gex);
		}
		
		return customers;
	}

	public CachedRowSet getCustomerPredictedItems (Connection conn, String storeNum, Date start, Date end,
			String customerIds, String itemCodes)
	{
		return getCustomerItems (conn, storeNum, start, end, customerIds, itemCodes, true);
	}

	public CachedRowSet getCustomerItems (Connection conn, String storeNum, Date start, Date end,
			String customers, String itemCodes, boolean bCustomerId)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		StringBuffer sb = new StringBuffer();
		sb.append("select c.id, i.item_code, m.quantity, m.weight");
		sb.append(", m.pos_timestamp from movement_daily m, customer c, item_lookup i");
		sb.append(" where m.customer_card_no = c.customer_no");
		sb.append(" and m.comp_str_no = '").append(storeNum).append("'");
		//sb.append(" and c.customer_no in ('046105042533',retrn'046104333335')");		// TEST
		//sb.append(" and c.id in (135,168,266,308)");		// TEST
		sb.append(" and m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc");
		if ( customers != null ) {
			if ( bCustomerId )
				sb.append(" and c.id in (").append(customers).append(')');
			else
				sb.append(" and c.customer_no in (").append(customers).append(')');
		}
		if ( itemCodes != null ) {
			sb.append(" and i.item_code in (").append(itemCodes).append(')');
		}
		//sb.append(" and m.customer_card_no in ('046117100025')");		// TEST
		//sb.append(" and i.item_code in (19791)");		// TEST
		sb.append(" order by c.id, i.item_code, m.pos_timestamp");

		logger.info ("getCustomerItems SQL: " + sb.toString());
		CachedRowSet result = null;
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerItems");
		}
		catch (GeneralException ex) {
			logger.error("getCustomerItems SQL: " + sb.toString(), ex);
		}
		
		//return customers;
		return result;
	}
	
	public CachedRowSet getCustomerItems (Connection conn, String storeNum, Date start, Date end,
			int custIdStart, int custIdEnd, String itemCodes, boolean bCustomerId)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		StringBuffer sb = new StringBuffer();
		sb.append("select c.id, i.item_code, m.quantity, m.weight");
		sb.append(", m.pos_timestamp from movement_daily m, customer c, item_lookup i");
		sb.append(" where m.customer_card_no = c.customer_no");
		sb.append(" and m.comp_str_no = '").append(storeNum).append("'");
		//sb.append(" and c.customer_no in ('046105042533',retrn'046104333335')");		// TEST
		//sb.append(" and c.id in (135,168,266,308)");		// TEST
		sb.append(" and m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc");
		if ( custIdStart != -1 ) {
			sb.append(" and c.id >=").append(custIdStart).append(" and c.id <=").append(custIdEnd);
		}
		if ( itemCodes != null ) {
			sb.append(" and i.item_code in (").append(itemCodes).append(')');
		}
		//sb.append(" and m.customer_card_no in ('046117100025')");		// TEST
		//sb.append(" and i.item_code in (19791)");		// TEST
		sb.append(" order by c.id, i.item_code, m.pos_timestamp");

		logger.info ("getCustomerItems SQL: " + sb.toString());
		CachedRowSet result = null;
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerItems");
		}
		catch (GeneralException ex) {
			logger.error("getCustomerItems SQL: " + sb.toString(), ex);
		}
		
		//return customers;
		return result;
	}

	public CachedRowSet getCustomerLIGs (Connection conn, String storeNum, Date start, Date end,
			String customers, String lirIds, boolean bCustomerId)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		StringBuffer sb = new StringBuffer();
		sb.append("select c.id, i.ret_lir_id, m.quantity, m.weight");
		sb.append(", m.pos_timestamp from movement_daily m, customer c, item_lookup i");
		sb.append(" where m.customer_card_no = c.customer_no");
		sb.append(" and m.comp_str_no = '").append(storeNum).append("'");
		//sb.append(" and c.customer_no in ('046105042533','046104333335')");		// TEST
		//sb.append(" and c.id in (135,168,266,308)");		// TEST
		sb.append(" and m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc");
		//sb.append(" and c.analyze = 'Y'");
		sb.append(" and c.active = 'Y'");
		if ( customers != null ) {
			if ( bCustomerId )
				sb.append(" and c.id in (").append(customers).append(')');
			else
				sb.append(" and c.customer_no in (").append(customers).append(')');
		}
		if ( lirIds != null ) {
			sb.append(" and i.ret_lir_id in (").append(lirIds).append(')');
		}
		else
			sb.append(" and i.ret_lir_id is not null");
		//sb.append(" and m.customer_card_no in ('046100629202')");		// TEST
		//sb.append(" order by c.id, m.pos_timestamp, i.ret_lir_id");
		sb.append(" order by c.id, i.ret_lir_id, m.pos_timestamp");
		logger.debug ("getCustomerLIGs SQL: " + sb.toString());
		
		CachedRowSet result = null;
		//List<CustomerRecordDTO> customerList = new ArrayList<CustomerRecordDTO>();
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerLIGs");
		}
		catch (GeneralException ex) {
			logger.error("getCustomerLIGs SQL: " + sb.toString(), ex);
		}
		
		return result;
	}

	public CachedRowSet getCustomerLIGs (Connection conn, String storeNum, Date start, Date end,
			int custIdStart, int custIdEnd, String lirIds, boolean bCustomerId)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		StringBuffer sb = new StringBuffer();
		sb.append("select c.id, i.ret_lir_id, m.quantity, m.weight");
		sb.append(", m.pos_timestamp from movement_daily m, customer c, item_lookup i");
		sb.append(" where m.customer_card_no = c.customer_no");
		sb.append(" and m.comp_str_no = '").append(storeNum).append("'");
		//sb.append(" and c.customer_no in ('046105042533','046104333335')");		// TEST
		//sb.append(" and c.id in (135,168,266,308)");		// TEST
		sb.append(" and m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc");
		//sb.append(" and c.analyze = 'Y'");
		sb.append(" and c.active = 'Y'");
		if ( custIdStart != -1 ) {
			sb.append(" and c.id >=").append(custIdStart).append(" and c.id <=").append(custIdEnd);
		}
		if ( lirIds != null ) {
			sb.append(" and i.ret_lir_id in (").append(lirIds).append(')');
		}
		else
			sb.append(" and i.ret_lir_id is not null");
		//sb.append(" and m.customer_card_no in ('046100629202')");		// TEST
		//sb.append(" order by c.id, m.pos_timestamp, i.ret_lir_id");
		sb.append(" order by c.id, i.ret_lir_id, m.pos_timestamp");
		logger.debug ("getCustomerLIGs SQL: " + sb.toString());
		
		CachedRowSet result = null;
		//List<CustomerRecordDTO> customerList = new ArrayList<CustomerRecordDTO>();
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerLIGs");
		}
		catch (GeneralException ex) {
			logger.error("getCustomerLIGs SQL: " + sb.toString(), ex);
		}
		
		return result;
	}

	public CachedRowSet getCustomerSegments (Connection conn, String storeNum, Date start, Date end,
			String customers, String segmentIds, boolean bCustomerId)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		StringBuffer sb = new StringBuffer();
		sb.append("select c.id, i.segment_id, m.quantity, m.weight");
		sb.append(", m.pos_timestamp from movement_daily m, customer c, item_lookup i");
		sb.append(" where m.customer_card_no = c.customer_no");
		sb.append(" and m.comp_str_no = '").append(storeNum).append("'");
		//sb.append(" and c.customer_no in ('046105042533','046104333335')");		// TEST
		//sb.append(" and c.id in (135,168,266,308)");		// TEST
		sb.append(" and m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc");
		//sb.append(" and c.analyze = 'Y'");
		sb.append(" and c.active = 'Y'");
		if ( customers != null ) {
			if ( bCustomerId )
				sb.append(" and c.id in (").append(customers).append(')');
			else
				sb.append(" and c.customer_no in (").append(customers).append(')');
		}
		if ( segmentIds != null ) {
			sb.append(" and i.segment_id in (").append(segmentIds).append(')');
		}
		else
			sb.append(" and i.segment_id is not null");
		//sb.append(" and m.customer_card_no in ('046100629202')");		// TEST
		//sb.append(" order by c.id, m.pos_timestamp, i.segment_id");
		sb.append(" order by c.id, i.segment_id, m.pos_timestamp");
		logger.info ("getCustomerSegments SQL: " + sb.toString());
		
		CachedRowSet result = null;
		//List<CustomerRecordDTO> customerList = new ArrayList<CustomerRecordDTO>();
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerSegments");
		}
		catch (GeneralException ex) {
			logger.error("getCustomerSegments SQL: " + sb.toString(), ex);
		}
		
		return result;
	}

	public CachedRowSet getCustomerSegments (Connection conn, String storeNum, Date start, Date end,
			int custIdStart, int custIdEnd, String segmentIds, boolean bCustomerId)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		StringBuffer sb = new StringBuffer();
		sb.append("select c.id, i.segment_id, m.quantity, m.weight");
		sb.append(", m.pos_timestamp from movement_daily m, customer c, item_lookup i");
		sb.append(" where m.customer_card_no = c.customer_no");
		sb.append(" and m.comp_str_no = '").append(storeNum).append("'");
		//sb.append(" and c.customer_no in ('046105042533','046104333335')");		// TEST
		//sb.append(" and c.id in (135,168,266,308)");		// TEST
		sb.append(" and m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc");
		//sb.append(" and c.analyze = 'Y'");
		sb.append(" and c.active = 'Y'");
		if ( custIdStart != -1 ) {
			sb.append(" and c.id >=").append(custIdStart).append(" and c.id <=").append(custIdEnd);
		}
		if ( segmentIds != null ) {
			sb.append(" and i.segment_id in (").append(segmentIds).append(')');
		}
		else
			sb.append(" and i.segment_id is not null");
		//sb.append(" and m.customer_card_no in ('046100629202')");		// TEST
		//sb.append(" order by c.id, m.pos_timestamp, i.segment_id");
		sb.append(" order by c.id, i.segment_id, m.pos_timestamp");
		logger.info ("getCustomerSegments SQL: " + sb.toString());
		
		CachedRowSet result = null;
		//List<CustomerRecordDTO> customerList = new ArrayList<CustomerRecordDTO>();
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerSegments");
		}
		catch (GeneralException ex) {
			logger.error("getCustomerSegments SQL: " + sb.toString(), ex);
		}
		
		return result;
	}

	public CachedRowSet getCustomerItemMovement (Connection conn, String storeNum, Date start, Date end, int custIdStart, int custIdEnd)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		StringBuffer sb = new StringBuffer();
		sb.append("select c.id, i.item_code, i.ret_lir_id, i.segment_id");
		sb.append(" from movement_daily m, item_lookup i, customer c");
		sb.append(" where m.comp_str_no = '").append(storeNum).append("'");
		sb.append(" and m.customer_card_no = c.customer_no");
		sb.append(" and m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc");
		if ( custIdStart != -1 ) {
			sb.append(" and c.id >=").append(custIdStart).append(" and c.id <=").append(custIdEnd);
		}
		sb.append(" order by c.id, i.item_code");

		logger.info ("getCustomerItemMovement SQL: " + sb.toString());
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerItemMovement");
		}
		catch (GeneralException ex) {
			logger.error("getCustomerItemMovement SQL: " + sb.toString(), ex);
		}
		
		//return customers;
		return result;
	}

	public CachedRowSet getCustomerItemMovement (Connection conn, String storeNum, Date start, Date end, String customers)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String startStr = formatter.format(start);
		String endStr = formatter.format(end);
		
		StringBuffer sb = new StringBuffer();
		sb.append("select c.id, i.item_code, i.ret_lir_id, i.segment_id");
		sb.append(" from movement_daily m, item_lookup i, customer c");
		sb.append(" where m.comp_str_no = '").append(storeNum).append("'");
		sb.append(" and m.customer_card_no = c.customer_no");
		sb.append(" and m.pos_timestamp >= ").append("To_DATE('").append(startStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and m.pos_timestamp <= ").append("To_DATE('").append(endStr).append("', 'YYYYMMDDHH24MI')");
		sb.append(" and decode(length(m.upc), 14, m.upc, '0' || m.upc) = i.upc");
		if ( customers != null ) {
			sb.append(" and c.id in (").append(customers).append(')');
		}
		sb.append(" order by c.id, i.item_code");

		logger.info ("getCustomerItemMovement SQL: " + sb.toString());
		CachedRowSet result = null;
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getCustomerItemMovement");
		}
		catch (GeneralException ex) {
			logger.error("getCustomerItemMovement SQL: " + sb.toString(), ex);
		}
		
		//return customers;
		return result;
	}

	//
	// CustomerPurchase Begin
	//
	public int updateCustomerItemPurchase (Connection conn, List<CustomerPurchaseDTO> customers, int storeId, Date reportDate)
	{
		return updateCustomerPurchase (conn, customers, storeId, reportDate, "ITEM_CODE");
	}

	private int updateCustomerPurchase (Connection conn, List<CustomerPurchaseDTO> customers, int storeId, Date reportDate, String field)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		
		int count = 0;
		try
		{
			String sql = new String(CUST_PURCH_VERIFICATION_INSERT);
			sql = sql.replace (SUBSTITUTE_STRING, field);				// raplace {XXXX} with database field name
			//logger.debug("updateCustomerPurchase: SQL: " + sql);
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
		
			int batchCount = 0;
			for ( int ii = 0; ii < customers.size(); ii++ )
			{
				CustomerPurchaseDTO dto = customers.get(ii);
				
				int paramPosition = 1;
/*
insert into CUSTOMER_PURCHASE_VERIFICATION (REPORT_DATE, CUSTOMER_ID, STORE_ID, LAST_PURCHASE_DATE, {XXXX}, PROBABILITY_FREQ_COUNT, 
TOTAL_FREQ_COUNT, QUANTITY, ENABLED, PREDICTED_FOR_WEEK53) values (to_date(?,'YYMMDD'),?,?,to_date(?,'YYMMDD'),?,?,?,?,?,?)
*/
				try
				{
					String dateStr = formatter.format(reportDate);
					pstmt.setObject(paramPosition++, dateStr);						// REPORT_DATE
					pstmt.setObject(paramPosition++, dto.customerId);				// CUSTOMER_ID
					pstmt.setObject(paramPosition++, storeId);						// STORE_ID
					dateStr = formatter.format(dto.lastPurchaseDate);
					pstmt.setObject(paramPosition++, dateStr);						// LAST_PURCHASE_DATE
					
					if ( field.equals("ITEM_CODE"))
						pstmt.setObject(paramPosition++, dto.itemCode);				// ITEM_CODE
					else if ( field.equals("RET_LIR_ID"))
						pstmt.setObject(paramPosition++, dto.lirId);				// RET_LIR_ID
					else if ( field.equals("SEGMENT_ID"))
						pstmt.setObject(paramPosition++, dto.segmentId);			// SEGMENT_ID
	
					pstmt.setObject(paramPosition++, dto.probabilityCount);			// PROBABILITY_FREQ_COUNT
					pstmt.setObject(paramPosition++, dto.totalCount);				// TOTAL_FREQ_COUNT
					pstmt.setObject(paramPosition++, dto.quantity);					// QUANTITY
					pstmt.setObject(paramPosition++, dto.enabled ? "Y" : "N");		// ENABLED
					pstmt.setObject(paramPosition++, dto.predicted ? "Y" : "N");	// PREDICTED_FOR_WEEK53
	
					pstmt.addBatch();
					batchCount++;
					
					if ( batchCount == _BatchUpdateSize )
					{
						pstmt.executeBatch();
						pstmt.close();
						conn.commit();
	
						pstmt = conn.prepareStatement(sql);
						
						count += batchCount;
						batchCount = 0;
					}

					if ( ii > 0 && ii % 50000 == 0 )
						logger.info("updateCustomerPurchase: " + String.valueOf(ii) + " records updated");
				}
				catch (SQLException ex) {
					logger.error("updateCustomerPurchase: ", ex);
				}
			}
			
			// Commit last batch
			if ( batchCount > 0 )
			{
				pstmt.executeBatch();
				pstmt.close();
				conn.commit();
				count += batchCount;
			}
		}
		catch (Exception ex) {
			logger.error("updateCustomerPurchase: ", ex);
		}
		
		logger.info("updateCustomerPurchase: " + String.valueOf(count) + " records updated");
		return count;
	}

	public void deleteCustomerItemPurchase (Connection conn, int storeId, Date reportDate)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String reportStr = formatter.format(reportDate);
		
		StringBuffer sb = new StringBuffer("delete from CUSTOMER_PURCHASE_VERIFICATION");
		sb.append(" where STORE_ID = ").append(storeId);
		sb.append(" and ITEM_CODE is not null");
		sb.append(" and REPORT_DATE = ").append("To_DATE('").append(reportStr).append("', 'YYMMDD')");
		
		logger.info("deleteCustomerItemPurchase SQL:" + sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "deleteCustomerItemPurchase");
		}
		catch (GeneralException gex) {
			logger.error("deleteCustomerItemPurchase SQL: " + sb.toString(), gex);
		}
	}
	
	public int updateCustomerLIGPurchase (Connection conn, List<CustomerPurchaseDTO> customers, int storeId, Date reportDate)
	{
		return updateCustomerPurchase (conn, customers, storeId, reportDate, "RET_LIR_ID");
	}

	public void deleteCustomerLIGPurchase (Connection conn, int storeId, Date reportDate)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String reportStr = formatter.format(reportDate);
		
		StringBuffer sb = new StringBuffer("delete from CUSTOMER_PURCHASE_VERIFICATION");
		sb.append(" where STORE_ID = ").append(storeId);
		sb.append(" and RET_LIR_ID is not null");
		sb.append(" and REPORT_DATE = ").append("To_DATE('").append(reportStr).append("', 'YYMMDD')");

		logger.info("deleteCustomerLIGPurchase SQL:" + sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "deleteCustomerLIGPurchase");
		}
		catch (GeneralException gex) {
			logger.error("deleteCustomerLIGPurchase SQL: " + sb.toString(), gex);
		}
	}

	public int updateCustomerSegmentPurchase (Connection conn, List<CustomerPurchaseDTO> customers, int storeId, Date reportDate)
	{
		return updateCustomerPurchase (conn, customers, storeId, reportDate, "SEGMENT_ID");
	}

	public void deleteCustomerSegmentPurchase (Connection conn, int storeId, Date reportDate)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String reportStr = formatter.format(reportDate);
		
		StringBuffer sb = new StringBuffer("delete from CUSTOMER_PURCHASE_VERIFICATION");
		sb.append(" where STORE_ID = ").append(storeId);
		sb.append(" and SEGMENT_ID is not null");
		sb.append(" and REPORT_DATE = ").append("To_DATE('").append(reportStr).append("', 'YYMMDD')");

		logger.debug("deleteCustomerSegmentPurchase SQL:" + sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "deleteCustomerSegmentPurchase");
		}
		catch (GeneralException gex) {
			logger.error("deleteCustomerSegmentPurchase SQL: " + sb.toString(), gex);
		}
	}
	
	public List<Integer> getCustomersFromCustomerPurchase (Connection conn, int storeId, Date reportDate)
	{
		List<Integer> customers = new ArrayList<Integer>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String reportStr = formatter.format(reportDate);
		
		StringBuffer sb = new StringBuffer("select distinct CUSTOMER_ID from CUSTOMER_PURCHASE_VERIFICATION");
		sb.append(" where STORE_ID = ").append(storeId);
		//sb.append(" and ITEM_CODE is null");
		sb.append(" and REPORT_DATE = ").append("To_DATE('").append(reportStr).append("', 'YYMMDD')");
		sb.append(" order by CUSTOMER_ID");

		logger.debug("getCustomersFromCustomerPurchase SQL:" + sb.toString());
		try
		{
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getCustomersFromCustomerPurchase");
			while (result.next())
			{
				int customer = result.getInt("CUSTOMER_ID");
				customers.add(customer);
			}
		}
		catch (SQLException sex) {
			logger.error("getCustomersFromCustomerPurchase SQL: " + sb.toString(), sex);
		}
		catch (GeneralException gex) {
			logger.error("getCustomersFromCustomerPurchase SQL: " + sb.toString(), gex);
		}
		
		return customers;
	}

	//
	// CustomerPurchase End
	//
	
	//
	// Prediction Begin
	//
	public void deleteItemPrediction (Connection conn, int storeId, Date reportDate, Date start)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String reportStr = formatter.format(reportDate);
		
		StringBuffer sb = new StringBuffer("delete from CUSTOMER_PREDICTION");
		sb.append(" where STORE_ID = ").append(storeId);
		sb.append(" and REPORT_DATE = ").append("To_DATE('").append(reportStr).append("', 'YYMMDD')");
		if ( start != null ) {
			String startStr = formatter.format(start);
			sb.append(" and WEEK_START_DATE = ").append("To_DATE('").append(startStr).append("', 'YYMMDD')");
		}
		sb.append(" and ITEM_CODE is not null");
		
		logger.debug("deleteItemPrediction SQL:" + sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "deleteItemPrediction");
		}
		catch (GeneralException gex) {
			logger.error("deleteItemPrediction SQL: " + sb.toString(), gex);
		}
	}
	
	public int updateItemPrediction (Connection conn, List<CustomerPredictionDTO> customers, String storeNum,
			Date reportDate, Date weekStartDate)
	{
		return updatePrediction (conn, customers, storeNum, reportDate, weekStartDate, "ITEM_CODE");
	}
	
	public int updatePrediction (Connection conn, List<CustomerPredictionDTO> customers, String storeNum,
			Date reportDate, Date weekStartDate, String field)
	{
		int count = 0;

		try
		{
			SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
			int storeId = getStoreId (conn, storeNum);
			
			String sql = new String(CUST_PREDICTION_INSERT);
			sql = sql.replace (SUBSTITUTE_STRING, field);		// raplace {XXXX} with database field name 
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			int batchCount = 0;
			for ( int ii = 0; ii < customers.size(); ii++ )
			{
				CustomerPredictionDTO dto = customers.get(ii);
				
				int paramPosition = 1;
/*
insert into CUSTOMER_PREDICTION (REPORT_DATE, CUSTOMER_ID, STORE_ID, WEEK_START_DATE, {XXXX}, QUANTITY) 
values (to_date(?,'YYMMDD'),?,?,to_date(?,'YYMMDD'),?,?)
*/
				try
				{
					String dateStr = formatter.format(reportDate);
					pstmt.setObject(paramPosition++, dateStr);						// REPORT_DATE
					pstmt.setObject(paramPosition++, dto.customerId);				// CUSTOMER_ID
					pstmt.setObject(paramPosition++, storeId);						// STORE_ID
					dateStr = formatter.format(weekStartDate);
					pstmt.setObject(paramPosition++, dateStr);						// WEEK_START_DATE
					
					if ( field.equals("ITEM_CODE"))
						pstmt.setObject(paramPosition++, dto.itemCode);				// ITEM_CODE
					else if ( field.equals("RET_LIR_ID"))
						pstmt.setObject(paramPosition++, dto.lirId);				// RET_LIR_ID
					else if ( field.equals("SEGMENT_ID"))
						pstmt.setObject(paramPosition++, dto.segmentId);			// SEGMENT_ID
	
					pstmt.setObject(paramPosition++, dto.quantity);					// QUANTITY
	
					pstmt.addBatch();
					batchCount++;
					
					if ( batchCount == _BatchUpdateSize )
					{
						pstmt.executeBatch();
						pstmt.close();
						conn.commit();
	
						pstmt = conn.prepareStatement(sql);
						
						count += batchCount;
						batchCount = 0;
					}
					
					if ( ii > 0 && ii % 2000 == 0 )
						logger.info("updatePrediction: " + String.valueOf(ii) + " records updated");
				}
				catch (SQLException ex) {
					logger.error("updatePrediction: ", ex);
				}
			}

			// Commit last batch
			if ( batchCount > 0 )
			{
				pstmt.executeBatch();
				pstmt.close();
				conn.commit();
				count += batchCount;
			}
		}
		catch (Exception ex) {
			logger.error("updatePrediction: ", ex);
		}
		catch (GeneralException gex) {
			logger.error("updatePrediction");
		}
		
		logger.info("updatePrediction: " + String.valueOf(count) + " records updated");
		return count;
	}
	
	public void deleteLIGPrediction (Connection conn, int storeId, Date reportDate, Date start)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String reportStr = formatter.format(reportDate);
		
		StringBuffer sb = new StringBuffer("delete from CUSTOMER_PREDICTION");
		sb.append(" where STORE_ID = ").append(storeId);
		sb.append(" and REPORT_DATE = ").append("To_DATE('").append(reportStr).append("', 'YYMMDD')");
		if ( start != null ) {
			String startStr = formatter.format(start);
			sb.append(" and WEEK_START_DATE = ").append("To_DATE('").append(startStr).append("', 'YYMMDD')");
		}
		sb.append(" and RET_LIR_ID is not null");
		
		logger.debug("deleteLIGPrediction SQL:" + sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "deleteLIGPrediction");
		}
		catch (GeneralException gex) {
			logger.error("deleteLIGPrediction SQL: " + sb.toString(), gex);
		}
	}
	
	public int updateLIGPrediction (Connection conn, List<CustomerPredictionDTO> customers, String storeNum,
			Date reportDate, Date weekStartDate)
	{
		return updatePrediction (conn, customers, storeNum, reportDate, weekStartDate, "RET_LIR_ID");
	}

	public void deleteSegmentPrediction (Connection conn, int storeId, Date reportDate, Date start)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String reportStr = formatter.format(reportDate);
		
		StringBuffer sb = new StringBuffer("delete from CUSTOMER_PREDICTION");
		sb.append(" where STORE_ID = ").append(storeId);
		sb.append(" and REPORT_DATE = ").append("To_DATE('").append(reportStr).append("', 'YYMMDD')");
		if ( start != null ) {
			String startStr = formatter.format(start);
			sb.append(" and WEEK_START_DATE = ").append("To_DATE('").append(startStr).append("', 'YYMMDD')");
		}
		sb.append(" and SEGMENT_ID is not null");
		
		logger.debug("deleteSegmentPrediction SQL:" + sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "deleteSegmentPrediction");
		}
		catch (GeneralException gex) {
			logger.error("deleteSegmentPrediction SQL: " + sb.toString(), gex);
		}
	}
	
	public int updateSegmentPrediction (Connection conn, List<CustomerPredictionDTO> customers, String storeNum,
			Date reportDate, Date weekStartDate)
	{
		return updatePrediction (conn, customers, storeNum, reportDate, weekStartDate, "SEGMENT_ID");
	}
	
	public List<CustomerPredictionDTO> getLIGsOfPredictedItems (Connection conn, int storeId, Date reportDate)
	{
		List<CustomerPredictionDTO> predictions = new ArrayList<CustomerPredictionDTO>();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String reportStr = formatter.format(reportDate);
		
		StringBuffer sb = new StringBuffer("SELECT DISTINCT c.customer_id, i.ret_lir_id, c.week_start_date");
		sb.append(" FROM customer_prediction c, item_lookup i");
		sb.append(" WHERE c.item_code = i.item_code AND c.item_code IS NOT NULL AND i.ret_lir_id IS NOT NULL");
		sb.append(" AND c.store_id = ").append(storeId);
		sb.append(" AND c.report_date = ").append("To_DATE('").append(reportStr).append("', 'YYMMDD')");
		sb.append(" ORDER BY c.customer_id, i.ret_lir_id");

		logger.debug("getLIGsOfPredictedItems SQL:" + sb.toString());
		try
		{
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getLIGsOfPredictedItems");
			while (result.next())
			{
				CustomerPredictionDTO dto = new CustomerPredictionDTO();
				dto.reportDate = reportDate;
				dto.storeId = storeId;
				dto.customerId = result.getInt("customer_id");
				dto.lirId = result.getInt("ret_lir_id");
				dto.weekStartDate = result.getDate("week_start_date");
				predictions.add(dto);
			}
		}
		catch (SQLException sex) {
			logger.error("getLIGsOfPredictedItems SQL: " + sb.toString(), sex);
		}
		catch (GeneralException gex) {
			logger.error("getLIGsOfPredictedItems SQL: " + sb.toString(), gex);
		}
		
		return predictions;
	}

	//
	// Prediction End
	//

	//
	// Verification Begin
	//
	public Map<String, CustomerVerificationDTO> getPurchaseItemsForVerification (Connection conn, int storeId, Date reportDate, Date predWeekDate,
			String customerIds)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String reportStr = formatter.format(reportDate);
		String predWeekDateStr = formatter.format(predWeekDate);
		
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT cp.customer_id, cp.item_code, i.ret_lir_id, i.segment_id, cp.enabled, cp.predicted_for_week53");
		sb.append(" FROM customer_purchase_verification cp, item_lookup i");
		sb.append(" WHERE cp.item_code = i.item_code");
		sb.append(" AND cp.store_id = ").append(storeId);
		sb.append(" AND cp.item_code is not null");
		sb.append(" AND cp.report_date = ").append("To_DATE('").append(reportStr).append("', 'YYMMDD')");
		if ( customerIds != null )
			sb.append(" AND cp.customer_id in (").append(customerIds).append(')');
		sb.append(" ORDER BY cp.customer_id, cp.item_code");

		logger.debug ("getPurchaseItemsForVerification SQL: " + sb.toString());
		CachedRowSet result = null;
		Map<String, CustomerVerificationDTO> list = new HashMap<String, CustomerVerificationDTO>();
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getPurchaseItemsForVerification");
			while (result.next())
			{
				CustomerVerificationDTO dto = new CustomerVerificationDTO();
				dto.reportDate = reportDate;
				dto.predictionWeekDate = predWeekDate;
				int customerId = result.getInt("customer_id");
				dto.customerId = customerId;
				
				int itemCode = result.getInt("item_code");
				dto.itemCode = itemCode;
				dto.lirId = result.getInt("ret_lir_id");
				dto.segmentId = result.getInt("segment_id");
				
				dto.enabled = result.getString("enabled") == "Y" ? true : false;
				dto.predicted = result.getString("predicted_for_week53") == "Y" ? true : false;
				
				String key = String.valueOf(String.valueOf(customerId)) + ":" + String.valueOf(itemCode);
				list.put(key, dto);
			}
		}
		catch (SQLException sex) {
			logger.error("SQL: " + sb.toString(), sex);
		}
		catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
		}
		
		return list;
	}

	public Map<String, CustomerVerificationDTO> getPurchaseItemsForVerification (Connection conn, int storeId, Date reportDate, Date predWeekDate,
			int custIdStart, int custIdEnd)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		String reportStr = formatter.format(reportDate);
		String predWeekDateStr = formatter.format(predWeekDate);
		
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT cp.customer_id, cp.item_code, i.ret_lir_id, i.segment_id, cp.enabled, cp.predicted_for_week53");
		sb.append(" FROM customer_purchase_verification cp, item_lookup i");
		sb.append(" WHERE cp.item_code = i.item_code");
		sb.append(" AND cp.store_id = ").append(storeId);
		sb.append(" AND cp.item_code is not null");
		sb.append(" AND cp.report_date = ").append("To_DATE('").append(reportStr).append("', 'YYMMDD')");
		if ( custIdStart != -1 ) {
			sb.append(" and cp.customer_id >=").append(custIdStart).append(" and cp.customer_id <=").append(custIdEnd);
		}
		sb.append(" ORDER BY cp.customer_id, cp.item_code");

		logger.info ("getPurchaseItemsForVerification SQL: " + sb.toString());
		CachedRowSet result = null;
		Map<String, CustomerVerificationDTO> list = new HashMap<String, CustomerVerificationDTO>();
		try
		{
			result = PristineDBUtil.executeQuery(conn, sb, "getPurchaseItemsForVerification");
			while (result.next())
			{
				CustomerVerificationDTO dto = new CustomerVerificationDTO();
				dto.reportDate = reportDate;
				dto.predictionWeekDate = predWeekDate;
				int customerId = result.getInt("customer_id");
				dto.customerId = customerId;
				
				int itemCode = result.getInt("item_code");
				dto.itemCode = itemCode;
				dto.lirId = result.getInt("ret_lir_id");
				dto.segmentId = result.getInt("segment_id");
				
				dto.enabled = result.getString("enabled") == "Y" ? true : false;
				dto.predicted = result.getString("predicted_for_week53") == "Y" ? true : false;
				
				String key = String.valueOf(String.valueOf(customerId)) + ":" + String.valueOf(itemCode);
				list.put(key, dto);
			}
		}
		catch (SQLException sex) {
			logger.error("SQL: " + sb.toString(), sex);
		}
		catch (GeneralException gex) {
			logger.error("SQL: " + sb.toString(), gex);
		}
		
		return list;
	}

	public int updateVerificationInfo (Connection conn, int storeId, Date reportDate, Date predWeekDate, List<CustomerVerificationDTO> list)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyMMdd");
		//String predWeekDateStr = formatter.format(predWeekDate);
		
		int count = 0;
		try
		{
			String sql = CUST_PURCH_VERIFICATION_UPDATE;
			//logger.debug("updateVerificationInfo: SQL: " + sql);

			PreparedStatement pstmt = conn.prepareStatement(sql);
			
			int batchCount = 0;
			for ( int ii = 0; ii < list.size(); ii++ )
			{
				CustomerVerificationDTO dto = list.get(ii);
				
				int paramPosition = 1;
				
				try
				{
/*
UPDATE customer_purchase_verification SET PURCHASED_ITEM_IN_WEEK53 = ?, PURCHASED_LIG_IN_WEEK53 = ?, PURCHASED_SEGMENT_IN_WEEK53 = ?
WHERE store_id = ? AND report_date = To_DATE(?, 'YYMMDD') AND customer_id = ?  AND item_code = ?
*/
					pstmt.setObject(paramPosition++, dto.purchasedItemIn53rdWeek ? "Y" : "N");		// PURCHASED_ITEM_IN_WEEK53
					pstmt.setObject(paramPosition++, dto.purchasedLIGIn53rdWeek ? "Y" : "N");		// PURCHASED_LIG_IN_WEEK53
					pstmt.setObject(paramPosition++, dto.purchasedSegmentIn53rdWeek ? "Y" : "N");	// PURCHASED_SEGMENT_IN_WEEK53
					pstmt.setObject(paramPosition++, storeId);						// STORE_ID
					String dateStr = formatter.format(reportDate);
					pstmt.setObject(paramPosition++, dateStr);						// REPORT_DATE
					pstmt.setObject(paramPosition++, dto.customerId);				// CUSTOMER_ID
					pstmt.setObject(paramPosition++, dto.itemCode);					// ITEM_CODE
	
					pstmt.addBatch();
					batchCount++;
					
					if ( batchCount == _BatchUpdateSize )
					{
						pstmt.executeBatch();
						pstmt.close();
						conn.commit();
	
						pstmt = conn.prepareStatement(sql);
						
						count += batchCount;
						batchCount = 0;
					}
	
					if ( ii > 0 && ii % 50000 == 0 )
						logger.info("updateVerificationInfo: " + String.valueOf(ii) + " records updated");
				}
				catch (SQLException ex) {
					logger.error("updateVerificationInfo: ", ex);
				}
			}
			
			// Commit last batch
			if ( batchCount > 0 )
			{
				pstmt.executeBatch();
				pstmt.close();
				conn.commit();
				count += batchCount;
			}
		}
		catch (Exception ex) {
			logger.error("updateVerificationInfo: ", ex);
		}
	
		logger.info("updateVerificationInfo: " + count + " records updated");
		return count;
	}

	public int getCustomerId(Connection conn, String card, int storeId) 
											throws GeneralException,Exception{
		int id = 0;
		try{						
		StringBuffer sbb = new StringBuffer("select CUSTOMER_ID from CUSTOMER_LOYALTY_INFO where LOYALTY_CARD_NO ='"+card+"'");	
		CachedRowSet resultcard = PristineDBUtil.executeQuery(conn, sbb, "getMovementsByTransactions fetching card");
		if(resultcard.next()){
			int customerId = new Integer(resultcard.getString("CUSTOMER_ID"));			
			return customerId;
			}
		else{
		sbb = new StringBuffer("select CUSTOMER_ID_SEQ.NEXTVAL from DUAL");
		resultcard = PristineDBUtil.executeQuery(conn, sbb, "get new customer ID");		
		while(resultcard.next())
			if(resultcard.getString("NEXTVAL") != null)
			id = new Integer(resultcard.getString("NEXTVAL"));
		//id = id + 1;
		/*sbb = new StringBuffer("insert into CUSTOMER (ID,STORE_ID) VALUES(");
		sbb.append(id);		
		sbb.append(",").append(storeId);
		sbb.append(")");
		 PristineDBUtil.execute(conn, sbb, "getMovementsByTransactions inserting customer");*/
		sbb = new StringBuffer("insert into CUSTOMER_LOYALTY_INFO (CUSTOMER_ID,LOYALTY_CARD_NO,CARD_TYPE, STORE_ID) VALUES(");
		sbb.append(id).append(" , '").append(card).append("' , ").append(new Double(1)).append(", " + storeId).append(")");
		PristineDBUtil.execute(conn, sbb, "getMovementsByTransactions inserting customer loyalty card");		 
		}
		}catch(Exception ex){
			logger.error("The exception is "+ex);
			conn.rollback();
			throw  ex;
		}
		catch(GeneralException ex){
			logger.error("The exception is "+ex);
			conn.rollback();
			throw  ex;
		}
		return id;
	}
	
	
	public HashMap<String, Integer> getCustomerList(Connection conn, Set<String> custCards){
		List<String> cardNoList = new ArrayList<String>();
		HashMap<String, Integer> custMap = new HashMap<String, Integer>();
		try{						
			int limitcount = 0;
			for(String cardNo:custCards){
				cardNoList.add(cardNo);
				limitcount++;
				if( limitcount > 0 && (limitcount%Constants.BATCH_UPDATE_COUNT == 0)){
					Object[] values = cardNoList.toArray();
					retrieveCardNoAndIdMapping(conn, custMap, values);
					cardNoList.clear();
	            }
				
				if(limitcount > 0 && (limitcount%100000 == 0)){
					logger.info("getCustomerList() - # of customer ids retreived - " + limitcount); 	
				}
			}
			if(cardNoList.size() > 0){
				Object[] values = cardNoList.toArray();
				retrieveCardNoAndIdMapping(conn, custMap, values);
				cardNoList.clear();
			}
			
			
			}
			catch (Exception e) {
				logger.error("getCustomerList() - error while getting customer id mapping - " + e.toString());
			}
			
		return custMap;
	}
	
	private void retrieveCardNoAndIdMapping(Connection conn, HashMap<String, Integer> custMap, Object... values){
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    StringBuffer sbb = new StringBuffer("select LOYALTY_CARD_NO, CUSTOMER_ID from CUSTOMER_LOYALTY_INFO where LOYALTY_CARD_NO IN (%s)");
		try{
			statement = conn.prepareStatement(String.format(sbb.toString(), PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				String cardNo = resultSet.getString("LOYALTY_CARD_NO");
				int customerId = resultSet.getInt("CUSTOMER_ID");
				custMap.put(cardNo, customerId);
			}
		}
		catch(Exception e){
			logger.error("retrieveCardNoAndIdMapping() - Error while getting customer id mapping - " + e.toString());	
		}
		finally{
			PristineDBUtil.close(statement);
			}
	}
	

	public int  setupNewCustomerCardsAndUpdateCache(Connection conn,
			HashMap<String, Integer> newCustomerMap, int dbCommitCount ) throws GeneralException, Exception {
		
		
		StringBuffer sb = new StringBuffer("insert into CUSTOMER_LOYALTY_INFO (CUSTOMER_ID,LOYALTY_CARD_NO,STORE_ID, CARD_TYPE) VALUES(");
		sb.append("CUSTOMER_ID_SEQ.NEXTVAL, ?, ?, 1 )");
		int itemNoInBatch = 0;
		PreparedStatement statement = null;
		int recordInsertCount = 0;
		try {
			statement = conn.prepareStatement(sb.toString());
			for (Map.Entry<String, Integer> entry : newCustomerMap.entrySet()) {
				String customerCardNo = entry.getKey();
			    int storeId = entry.getValue().intValue();
			    statement.setString(1, customerCardNo);
				statement.setInt(2, storeId);
				itemNoInBatch++;
				statement.addBatch();
			    if(itemNoInBatch %  dbCommitCount == 0){
	        		int[] count = statement.executeBatch();
	        		recordInsertCount += PristineDBUtil.getUpdateCount(count);
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        		PristineDBUtil.commitTransaction(conn, "Setting up customer cards");
		        }
			}
			if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
	        	recordInsertCount += PristineDBUtil.getUpdateCount(count);
        		statement.clearBatch();
        		PristineDBUtil.commitTransaction(conn, "Setting up customer cards");
	        }
			//logger.info("No. of new customers added - " + recordInsertCount);
		}catch(SQLException sqlException){
			logger.error("Error in insertWeeklyMovement - " + sqlException.getMessage());
			throw new GeneralException("SQL Exception in setupNewCards",sqlException);
		}
		catch(Exception exception){
			logger.error("Error in insertWeeklyMovement - " + exception.getMessage());
			throw new Exception("SQL Exception in setupNewCards",exception);
		}finally{
			PristineDBUtil.close(statement);
		}
		return recordInsertCount;
	}

	
	private int getSeqId(Connection conn){
		int id = 0;
		StringBuffer sbb = new StringBuffer("select CUSTOMER_ID_SEQ.NEXTVAL from DUAL");
		try{
			CachedRowSet resultcard = PristineDBUtil.executeQuery(conn, sbb, "get new customer ID");		
			while(resultcard.next())
				if(resultcard.getString("NEXTVAL") != null)
				id = new Integer(resultcard.getString("NEXTVAL"));
		}
		catch(GeneralException ge){
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}
	
	/**returns list of all customer id and customer card nos mapping. 
	 * @param conn Connection
	 * @throws GeneralException*/
	public HashMap<String, Integer> getAllCustomerCards(Connection conn, String processDate) throws GeneralException{
		HashMap<String, Integer> customerMap = new HashMap<String, Integer>();
		try{
			StringBuffer sbb = new StringBuffer("SELECT CUSTOMER_ID, LOYALTY_CARD_NO FROM CUSTOMER_LOYALTY_INFO");
			logger.info("Query : " + sbb.toString());
			CachedRowSet resultcard = PristineDBUtil.executeQuery(conn, sbb, "getMovementsByTransactions fetching card");
			while(resultcard.next()){
				int custID = resultcard.getInt("CUSTOMER_ID");
				String cardNo = resultcard.getString("LOYALTY_CARD_NO");
				customerMap.put(cardNo, custID);
			}
		}
		catch(Exception e){
			logger.error("Error while getting customer card numbers from CUSTOMER table." + e.getMessage());
		}
		return customerMap;
	}
	//
	// Verification End
	//

	//
	// Data Members
	//
	private final String SUBSTITUTE_STRING = "{XXXX}";  
	private final String CUST_PURCH_VERIFICATION_INSERT = "insert into CUSTOMER_PURCHASE_VERIFICATION (REPORT_DATE, CUSTOMER_ID, STORE_ID, LAST_PURCHASE_DATE, {XXXX}, PROBABILITY_FREQ_COUNT, TOTAL_FREQ_COUNT, QUANTITY, ENABLED, PREDICTED_FOR_WEEK53) values (to_date(?,'YYMMDD'),?,?,to_date(?,'YYMMDD'),?,?,?,?,?,?)";
	private final String CUST_PURCH_VERIFICATION_UPDATE = "UPDATE customer_purchase_verification SET PURCHASED_ITEM_IN_WEEK53 = ?, PURCHASED_LIG_IN_WEEK53 = ?, PURCHASED_SEGMENT_IN_WEEK53 = ? WHERE store_id = ? AND report_date = To_DATE(?, 'YYMMDD') AND customer_id = ? AND item_code = ?";
	private final String CUST_PREDICTION_INSERT = "insert into CUSTOMER_PREDICTION (REPORT_DATE, CUSTOMER_ID, STORE_ID, WEEK_START_DATE, {XXXX}, QUANTITY) values (to_date(?,'YYMMDD'),?,?,to_date(?,'YYMMDD'),?,?)";
	private int _BatchUpdateSize = 5000;
/*
insert into CUSTOMER_PURCHASE_VERIFICATION (REPORT_DATE, CUSTOMER_ID, STORE_ID, LAST_PURCHASE_DATE, ITEM_CODE, PROBABILITY_FREQ_COUNT, TOTAL_FREQ_COUNT, QUANTITY, ENABLED, PREDICTED_FOR_WEEK53) 
values (to_date(?,'YYMMDD'),?,?,to_date(?,'YYMMDD'),?,?,?,?,?,?)";
insert into CUSTOMER_PREDICTION (REPORT_DATE, CUSTOMER_ID, STORE_ID, WEEK_START_DATE, ITEM_CODE, QUANTITY) values (to_date(?,'YYMMDD'),?,?,to_date(?,'YYMMDD'),?,?) 
UPDATE customer_purchase_verification SET PURCHASED_ITEM_IN_WEEK53 = ?, PURCHASED_LIG_IN_WEEK53 = ?, , PURCHASED_SEGMENT_IN_WEEK53 = ?
WHERE store_id = ? AND report_date = To_DATE(?, 'YYMMDD') AND customer_id = ?  AND item_code = ?
*/

	public int setupNewCustomerCards(Connection conn,
			HashMap<String, Integer> newCustomerMap, int dbCommitCount) throws GeneralException {
		
		StringBuffer sb = new StringBuffer("insert into CUSTOMER_LOYALTY_INFO (CUSTOMER_ID,LOYALTY_CARD_NO,STORE_ID, CARD_TYPE) VALUES(");
		sb.append("CUSTOMER_ID_SEQ.NEXTVAL").append(", ?, ?, 1 )");
		int itemNoInBatch = 0;
		PreparedStatement statement = null;
		int recordInsertCount = 0;
		try {
			statement = conn.prepareStatement(sb.toString());
			for (Map.Entry<String, Integer> entry : newCustomerMap.entrySet()) {
			    String customerCardNo = entry.getKey();
			    int storeId = entry.getValue().intValue();
			    statement.setString(1, customerCardNo);
			    statement.setInt(2, storeId);
				itemNoInBatch++;
				statement.addBatch();
				if(itemNoInBatch %  dbCommitCount == 0){
	        		int[] count = statement.executeBatch();
	        		recordInsertCount += PristineDBUtil.getUpdateCount(count);
	        		statement.clearBatch();
	        		itemNoInBatch = 0;
	        		PristineDBUtil.commitTransaction(conn, "Setting up customer cards");
		        }
			}
			if(itemNoInBatch > 0){
	        	int[] count = statement.executeBatch();
	        	recordInsertCount += PristineDBUtil.getUpdateCount(count);
        		statement.clearBatch();
        		PristineDBUtil.commitTransaction(conn, "Setting up customer cards");
	        }
		}catch(SQLException sqlException){
			logger.error("Error in insertWeeklyMovement - " + sqlException.getMessage());
			throw new GeneralException("SQL Exception in setupNewCards",sqlException);
		}finally{
			PristineDBUtil.close(statement);
		}
		return recordInsertCount;
	}
	
    public void updateLoyaltyGroupData(Connection _Conn, HashMap<String, Integer> loyaltyMap) throws GeneralException {
		try {
	        PreparedStatement psmt = _Conn
					.prepareStatement(UpdateLoyaltyGroupSql());

	        logger.debug("Adding data into prepared statement...");
	        for (Map.Entry<String, Integer> entry : loyaltyMap.entrySet()) {
	        	addLoyaltyGroup(entry.getKey(), entry.getValue(), psmt);
			}
			
			psmt.executeBatch();
			psmt.close();
	        logger.debug("Commit update...");
			_Conn.commit();
			loyaltyMap = null;

		} catch (Exception se) {
			logger.debug(se);
			throw new GeneralException("addMovementData", se);
		}

	}
    
	private String UpdateLoyaltyGroupSql() {
		StringBuffer Sql = new StringBuffer();
		Sql.append("UPDATE CUSTOMER_LOYALTY_INFO SET HOUSEHOLD_NUMBER =? WHERE LOYALTY_CARD_NO =?");
		logger.debug("UpdateUOMSql " + Sql.toString());
		return Sql.toString();
	}
	
    public void addLoyaltyGroup(String loyaltyCardNumber, Integer LoyaltyGroup, PreparedStatement psmt) throws GeneralException {
		try {
			psmt.setObject(1, LoyaltyGroup);			
			psmt.setObject(2, loyaltyCardNumber);
			psmt.addBatch();

		} catch (Exception ex) {
			logger.debug(ex);
			throw new GeneralException("updateLoyaltyGroup", ex);

		}
	}
    
    private final String GET_LOYALTY_CARD_NUMBER = "SELECT LOYALTY_CARD_NO FROM CUSTOMER_LOYALTY_INFO";
    
    public List<String> getLoyaltyCardNumber(Connection conn) throws GeneralException{
    	List<String> loyaltyCardNumber = new ArrayList<String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try{
			statement = conn.prepareStatement(GET_LOYALTY_CARD_NUMBER);
			resultSet = statement.executeQuery();
			while(resultSet.next()){
				loyaltyCardNumber.add(resultSet.getString("LOYALTY_CARD_NO"));
			}
		}
		catch(SQLException sqlE){
			logger.error("Error while getting loyalty card number", sqlE);
			throw new GeneralException("Error while getting loyalty card number", sqlE);
		}
		return loyaltyCardNumber;
    	
    }
    
    private static String INSERT_INTO_CUSTOMER_LOYALTY_INFO = "insert into CUSTOMER_LOYALTY_INFO "
    		+ "(CUSTOMER_ID,LOYALTY_CARD_NO,HOUSEHOLD_NUMBER, CARD_TYPE) VALUES( CUSTOMER_ID_SEQ.NEXTVAL, ?, ?, ? )";
    
    public int insertintoCustomerLoyatyInfo(Connection conn, List<HouseHoldDTO> insertList) throws GeneralException{
    	PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalInsertCnt = 0;
		try {
			statement = conn.prepareStatement(INSERT_INTO_CUSTOMER_LOYALTY_INFO);

			for(HouseHoldDTO houseHoldDTO: insertList) {
				
					statement.setString(1, houseHoldDTO.getConsumerID());
					statement.setString(2, houseHoldDTO.getGroupID());
					statement.setInt(3, houseHoldDTO.getConsumerIDType());
					statement.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						// logger.debug("Total count of records processed: "
						// itemNoInBatch);
						int[] count = statement.executeBatch();
						totalInsertCnt = totalInsertCnt + count.length;
						statement.clearBatch();
						itemNoInBatch = 0;
					}
				}
			
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalInsertCnt = totalInsertCnt + count.length;
				statement.clearBatch();
			}
			statement.close();
		} catch (Exception e) {
			logger.error("Error in insertintoCustomerLoyatyInfo()  - " + e.toString());
			throw new GeneralException("Error in insertintoCustomerLoyatyInfo()", e);
		} finally {
			PristineDBUtil.close(statement);
		}

		return totalInsertCnt;
    }
    
    private static String UPDATE_CUSTOMER_LOYALTY_INFO = "UPDATE CUSTOMER_LOYALTY_INFO SET HOUSEHOLD_NUMBER =?, CARD_TYPE =? "
    		+ "WHERE LOYALTY_CARD_NO =?";
    
    public int updateCustomerLoyaltyInfo(Connection conn, List<HouseHoldDTO> updateList) throws GeneralException{
    	PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalInsertCnt = 0;
		try {
			statement = conn.prepareStatement(UPDATE_CUSTOMER_LOYALTY_INFO);

			for(HouseHoldDTO houseHoldDTO: updateList) {
					statement.setString(1, houseHoldDTO.getGroupID());
					statement.setInt(2, houseHoldDTO.getConsumerIDType());
					statement.setString(3, houseHoldDTO.getConsumerID());
					
					statement.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % 10000 == 0) {
						int[] count = statement.executeBatch();
						totalInsertCnt = totalInsertCnt + count.length;
						statement.clearBatch();
						itemNoInBatch = 0;
					}
				}
			
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalInsertCnt = totalInsertCnt + count.length;
				statement.clearBatch();
			}
			statement.close();
		} catch (Exception e) {
			logger.error("Error in updateCustomerLoyaltyInfo()  - " + e.toString());
			throw new GeneralException("Error in updateCustomerLoyaltyInfo()", e);
		} finally {
			PristineDBUtil.close(statement);
		}

		return totalInsertCnt;
    }
}

/*
CREATE TABLE CUSTOMER
(
    ID      				NUMBER(7,0) PRIMARY KEY,
    CUSTOMER_NO  			VARCHAR2(20 BYTE) NOT NULL,
    STORE_ID      			NUMBER(5,0) DEFAULT NULL,
    SECOND_STORE_ID 		NUMBER(5,0) DEFAULT NULL,
    SEGMENT_NO  			NUMBER(2,0) DEFAULT NULL,
    GENDER					NUMBER(1,0) DEFAULT NULL,
    FAMILY_COUNT			NUMBER(2,0) DEFAULT 0,
    SUNDAY_COUNT			NUMBER(3,0) DEFAULT 0,
    MONDAY_COUNT			NUMBER(3,0) DEFAULT 0,
    TUESDAY_COUNT			NUMBER(3,0) DEFAULT 0,
    WEDNESDAY_COUNT			NUMBER(3,0) DEFAULT 0,
    THURSDAY_COUNT			NUMBER(3,0) DEFAULT 0,
    FRIDAY_COUNT			NUMBER(3,0) DEFAULT 0,
    SATURDAY_COUNT			NUMBER(3,0) DEFAULT 0,
    SHOPPING_DAY			VARCHAR2(1 BYTE) DEFAULT NULL,
    SHOPPING_DAY_COUNT		NUMBER(2,0) DEFAULT NULL,
    SHOPPING_2DAY			VARCHAR2(3 BYTE) DEFAULT NULL,
    SHOPPING_2DAY_COUNT		NUMBER(3,0) DEFAULT NULL,
    SHOPPING_3DAY			VARCHAR2(5 BYTE) DEFAULT NULL,
    SHOPPING_3DAY_COUNT		NUMBER(3,0) DEFAULT NULL,
    SHOPPING_NONE_2DAY		VARCHAR2(3 BYTE) DEFAULT NULL,
    SHOPPING_NONE_2DAY_COUNT	NUMBER(2,0) DEFAULT NULL,
    VISIT_COUNT				NUMBER(3,0) DEFAULT NULL,
    TOTAL_COUNT				NUMBER(3,0) DEFAULT NULL,
    MAX_WEEK_GAP			NUMBER(2,0) DEFAULT 1,
    LAST_VISIT_DATE			DATE DEFAULT NULL,
    WEEKLY_AVERAGE_REGULAR	NUMBER(6,2) DEFAULT NULL,
    WEEKLY_AVERAGE_SALE		NUMBER(6,2) DEFAULT NULL,
    ACTIVE					CHAR(1) DEFAULT 'Y',
    ENABLED					CHAR(1) DEFAULT 'Y'
)
TABLESPACE "USER_CUSTOMER";

CREATE UNIQUE INDEX IDX_CUSTOMER_NO  ON CUSTOMER (CUSTOMER_NO) TABLESPACE "USER_CUSTOMER";
CREATE SEQUENCE CUSTOMER_ID_SEQ START WITH 1 INCREMENT BY 1 NOMAXVALUE;

CREATE TABLE CUSTOMER_PURCHASE_VERIFICATION
(
    REPORT_DATE 			DATE NOT NULL,
    CUSTOMER_ID      		NUMBER(7,0) NOT NULL,
    STORE_ID      	    	NUMBER(5,0) NOT NULL,
    ITEM_CODE       		NUMBER(6,0) DEFAULT NULL,
    RET_LIR_ID       		NUMBER(6,0) DEFAULT NULL,
    SEGMENT_ID       		NUMBER(6,0) DEFAULT NULL,
    BRAND_ID       			NUMBER(6,0) DEFAULT NULL,
    LAST_PURCHASE_DATE 		DATE NOT NULL,
    PROBABILITY_FREQ_COUNT	NUMBER(3,0) NOT NULL,
    TOTAL_FREQ_COUNT		NUMBER(3,0) NOT NULL,
    QUANTITY    			NUMBER(8,4) DEFAULT 0,
    ENABLED					CHAR(1) DEFAULT 'N',
    PREDICTED_FOR_WEEK53   CHAR(1) DEFAULT 'N',
    PURCHASED_ITEM_IN_WEEK53	  CHAR(1) DEFAULT 'N',
    PURCHASED_LIG_IN_WEEK53   	  CHAR(1) DEFAULT 'N',
    PURCHASED_SEGMENT_IN_WEEK53   CHAR(1) DEFAULT 'N',
    PURCHASED_SIZEGROUP_IN_WEEK53 CHAR(1) DEFAULT 'N'
)
TABLESPACE "USER_CUSTOMER";

CREATE INDEX IDX_CUST_PURCHASE ON CUSTOMER_PURCHASE_VERIFICATION (CUSTOMER_ID, LAST_PURCHASE_DATE)
	TABLESPACE "USER_CUSTOMER";
CREATE INDEX IDX_CUST_PURCHASE_REPORT ON CUSTOMER_PURCHASE_VERIFICATION (REPORT_DATE, CUSTOMER_ID)
	TABLESPACE "USER_CUSTOMER";

CREATE TABLE CUSTOMER_PREDICTION
(
    REPORT_DATE 			DATE NOT NULL,
    CUSTOMER_ID      	    NUMBER(7,0) NOT NULL,
    STORE_ID      	    	NUMBER(5,0) NOT NULL,
    WEEK_START_DATE 	    DATE NOT NULL,
    ITEM_CODE       	    NUMBER(6,0) DEFAULT NULL,
    RET_LIR_ID       	    NUMBER(6,0) DEFAULT NULL,
    SEGMENT_ID       	    NUMBER(6,0) DEFAULT NULL,
    BRAND_ID       		    NUMBER(6,0) DEFAULT NULL,
    QUANTITY    		    NUMBER(8,4) DEFAULT 0
)
TABLESPACE "USER_CUSTOMER";

CREATE INDEX IDX_CUST_PREDICTION ON CUSTOMER_PREDICTION (WEEK_START_DATE, CUSTOMER_ID)
	TABLESPACE "USER_CUSTOMER";
CREATE INDEX IDX_CUST_PRED_REPORT ON CUSTOMER_PREDICTION (REPORT_DATE, CUSTOMER_ID)
	TABLESPACE "USER_CUSTOMER";
	
	ALTER TABLE CUSTOMER_LOYALTY_INFO ADD STORE_ID NUMBER(5);
	ALTER TABLE CUSTOMER_LOYALTY_INFO
DROP CONSTRAINT CUSTOMER_LOYALTY_INFO_CUS_FK1;
*/
