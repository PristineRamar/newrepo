
package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.StoreDTO;
import com.pristine.dto.VariationAnalysisDto;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleStoreDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class StoreDAO implements IDAO
{

	static Logger	logger	= Logger.getLogger(StoreDAO.class);
	
	
	private static final String GET_STORE_INFO = "SELECT RPZ.ZONE_NUM, CS.COMP_STR_NO, CS.COMP_STR_ID, CS.DEPT1_ZONE_NO,  "
			+ "CS.DEPT2_ZONE_NO, CS.DEPT3_ZONE_NO, CS.PRICE_ZONE_ID FROM COMPETITOR_STORE CS "
			+ "INNER JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID " + "WHERE COMP_STR_ID = ?";

	private static final String GET_STORE_DETAILS = "SELECT CS.COMP_STR_ID, CS.COMP_STR_NO, CS.CLOSE_DATE, CS.PRICE_ZONE_ID, "
			+ " RPZ.ZONE_NUM FROM COMPETITOR_STORE CS LEFT JOIN RETAIL_PRICE_ZONE RPZ ON CS.PRICE_ZONE_ID = RPZ.PRICE_ZONE_ID "
			+ " WHERE COMP_CHAIN_ID = ?";

	private static final String GET_STORE_AND_COMP_NAME = "SELECT COMP_CHAIN_ID, COMP_CHAIN_NAME, SHRT_NAME FROM COMPETITOR_CHAIN";

	private static final String GET_STORE_AND_ZONE = "SELECT PZ.ZONE_NUM,PZ.PRICE_ZONE_ID,CS.COMP_STR_NO  FROM RETAIL_PRICE_ZONE PZ JOIN "
			+ "COMPETITOR_STORE CS ON CS.PRICE_ZONE_ID =PZ.PRICE_ZONE_ID ORDER BY PZ.PRICE_ZONE_ID";

	private static final String GET_EXISTING_STORES = "SELECT COMP_STR_NO  FROM COMPETITOR_STORE  "
			+ " WHERE COMP_CHAIN_ID = ?";

	private static final String GET_ZONE_ADD_FROM_STORE = "SELECT ADDR_LINE1,CITY,STATE,ZIP,TIME_ZONE  "
			+ "FROM COMPETITOR_STORE WHERE PRICE_ZONE_ID IN"
			+ "(SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE ZONE_NUM=? OR ZONE_NUM=?) AND  ROWNUM =1";
	
	private static final String ZONE_STR_COUNT="SELECT RPZ.ZONE_NUM ,COUNT(*) AS COUNT FROM RETAIL_PRICE_ZONE RPZ INNER JOIN COMPETITOR_STORE CS " + 
			"ON RPZ.PRICE_ZONE_ID = CS.PRICE_ZONE_ID GROUP BY  RPZ.ZONE_NUM";
	
	private static final String PRICE_ZONE_STR_COUNT="SELECT RPZ.PRICE_ZONE_ID ,COUNT(*) AS COUNT FROM RETAIL_PRICE_ZONE RPZ INNER JOIN COMPETITOR_STORE CS " + 
			"ON RPZ.PRICE_ZONE_ID = CS.PRICE_ZONE_ID GROUP BY  RPZ.PRICE_ZONE_ID";

	public ArrayList<VariationAnalysisDto> getActiveChains() throws GeneralException {

		Connection con = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		VariationAnalysisDto vDto = null;
		ArrayList<VariationAnalysisDto> allchain = null;

		String sql = null;
		try {
			allchain = new ArrayList<VariationAnalysisDto>();

			con = DBManager.getOracleConnection();

			sql = "SELECT comp_chain_id,comp_chain_name FROM competitor_chain order by comp_chain_name ";

			preparedStatement = con.prepareStatement(sql);

			rs = preparedStatement.executeQuery(sql);

			while (rs.next()) {
				vDto = new VariationAnalysisDto();

				vDto.setChainId(rs.getString("COMP_CHAIN_ID"));

				vDto.setCompChainName(rs.getString("comp_chain_name"));

				allchain.add(vDto);
			}

		} catch (SQLException se) {
			throw new GeneralException("Error while getting chains from DB" + se);
		} finally {
			PristineDBUtil.close(rs, preparedStatement, con);
		}

		return allchain;
	}

	public ArrayList<VariationAnalysisDto> getStates(String chainId) throws GeneralException {

		Connection con = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		VariationAnalysisDto vDto = null;
		ArrayList<VariationAnalysisDto> allchain = null;

		StringBuffer sql = null;
		try {
			sql = new StringBuffer();
			allchain = new ArrayList<VariationAnalysisDto>();

			con = DBManager.getOracleConnection();

			sql.append(" SELECT DISTINCT state");
			sql.append(" FROM COMPETITOR_STORE ");
			sql.append(" WHERE  comp_chain_id=" + chainId);
			sql.append(" ORDER BY state ");

			preparedStatement = con.prepareStatement(sql.toString());

			logger.debug("State List qry >>" + sql.toString());

			rs = preparedStatement.executeQuery(sql.toString());

			while (rs.next()) {
				vDto = new VariationAnalysisDto();

				vDto.setStateCode(rs.getString("state"));

				vDto.setStateName(rs.getString("state"));

				allchain.add(vDto);
			}

		} catch (SQLException se) {
			throw new GeneralException("Error while getting states from DB" + se);
		} finally {
			PristineDBUtil.close(rs, preparedStatement, con);
		}

		return allchain;
	}

	public ArrayList<VariationAnalysisDto> getStores(String chainId, String stateCode) throws GeneralException {
		Connection con = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		VariationAnalysisDto vDto = null;
		ArrayList<VariationAnalysisDto> allstores = null;

		StringBuffer sql = null;
		try {
			allstores = new ArrayList<VariationAnalysisDto>();
			sql = new StringBuffer();
			con = DBManager.getOracleConnection();

			sql.append(" SELECT COMP_STR_ID,CITY ||' , '|| ADDR_LINE1|| ' - ('|| COMP_STR_NO ||') ' AS STORE_NAME");
			sql.append(" FROM COMPETITOR_STORE ");
			sql.append(" WHERE  comp_chain_id= " + chainId);
			if (!stateCode.equalsIgnoreCase("ALL")) {
				sql.append(" AND STATE='" + stateCode + "'");
			}
			sql.append(" ORDER BY CITY ");

			preparedStatement = con.prepareStatement(sql.toString());

			logger.debug(" Qry to get Stores List>>" + sql.toString());

			rs = preparedStatement.executeQuery();

			while (rs.next()) {
				vDto = new VariationAnalysisDto();
				vDto.setStoreId(String.valueOf(rs.getInt("COMP_STR_ID")));
				vDto.setStoreName(rs.getString("STORE_NAME"));
				allstores.add(vDto);
			}
		} catch (SQLException se) {
			throw new GeneralException("Error while getting stores from DB", se);
		} finally {
			PristineDBUtil.close(rs, preparedStatement, con);
		}

		return allstores;
	}

	public ArrayList<VariationAnalysisDto> getDepartmentsList() throws GeneralException {
		Connection con = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		VariationAnalysisDto vDto = null;
		ArrayList<VariationAnalysisDto> allstores = null;

		String sql = null;
		try {
			allstores = new ArrayList<VariationAnalysisDto>();

			con = DBManager.getOracleConnection();

			sql = "SELECT id , name as names FROM department order by name";

			preparedStatement = con.prepareStatement(sql);

			rs = preparedStatement.executeQuery();

			while (rs.next()) {
				vDto = new VariationAnalysisDto();
				vDto.setDeptId(String.valueOf(rs.getInt("id")));
				vDto.setDeptName(rs.getString("names"));
				allstores.add(vDto);
			}
		} catch (SQLException se) {
			throw new GeneralException("Error while getting departments from DB", se);
		} finally {
			PristineDBUtil.close(rs, preparedStatement, con);
		}

		return allstores;
	}

	public CachedRowSet getActiveChains(Connection conn) throws GeneralException {
		StringBuffer query = new StringBuffer();
		query.append("select COMP_CHAIN_ID, COMP_CHAIN_NAME from competitor_chain where ACTIVE_INDICATOR = 'Y'");
		CachedRowSet result = PristineDBUtil.executeQuery(conn, query, "PerformanceGoal-GetActiveChainDetails");
		return result;

	}

	public CachedRowSet getChainDetails(Connection conn, int chainId) throws GeneralException {
		StringBuffer query = new StringBuffer();
		query.append(" select COMP_CHAIN_ID, COMP_CHAIN_NAME from competitor_chain where ACTIVE_INDICATOR = 'Y' AND ");
		query.append(" COMP_CHAIN_ID =").append(chainId);
		CachedRowSet result = PristineDBUtil.executeQuery(conn, query, "PerformanceGoal-GetActiveChainDetails");
		return result;

	}

	public CachedRowSet getStoresCheckedinLastXDays(Connection conn, int noOfDaysThreshold, int noofStoresThreshold,
			String fromDate) throws GeneralException {

		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		// Fill in the query
		sb.append("SELECT comp_str_id, count(comp_str_id) store_count from PERFORMANCE_STAT_VIEW where");
		sb.append(" current_status in (");
		sb.append(Constants.COMPLETED).append(',');
		sb.append(Constants.PARTIALLY_COMPLETED).append(')');
		sb.append(" and (GPS_VIOLATION is NULL or GPS_VIOLATION = 'N')");
		sb.append(" and status_chg_date >= TO_DATE(' ");
		sb.append(fromDate).append("', 'MM/DD/YYYY') - ");
		sb.append(noOfDaysThreshold);
		sb.append(" and status_chg_date <= TO_DATE(' ");
		sb.append(fromDate).append("', 'MM/DD/YYYY')");

		sb.append(" group by comp_str_id");
		sb.append(" having count(comp_str_id) >= ");
		sb.append(noofStoresThreshold);
		logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "StoreDAO - getStoresCheckedinLastXDays");
		return crs;
	}

	public CachedRowSet getSchedulesForStore(Connection conn, int noOfDaysThreshold, int storeId, String fromDate)
			throws GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();

		sb.append("SELECT schedule_id, to_char(  status_chg_date, 'MM/DD/YYYY') CHECK_DATE from PERFORMANCE_STAT_VIEW where");
		sb.append(" comp_str_id = ");
		sb.append(storeId);
		sb.append(" and current_status in (");
		sb.append(Constants.COMPLETED).append(',');
		sb.append(Constants.PARTIALLY_COMPLETED).append(')');
		sb.append(" and (GPS_VIOLATION is NULL or GPS_VIOLATION = 'N')");
		sb.append(" and status_chg_date >= TO_DATE(' ");
		sb.append(fromDate).append("', 'MM/DD/YYYY') - ");
		sb.append(noOfDaysThreshold);
		sb.append(" and status_chg_date <= TO_DATE(' ");
		sb.append(fromDate).append("', 'MM/DD/YYYY')");	
				// logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "StoreDAO - getSchedulesForStore");
		return crs;
	}

	public int getChainID(Connection conn, int storeId) throws GeneralException {
		int chainID = -1;
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT COMP_CHAIN_ID from COMPETITOR_STORE WHERE COMP_STR_ID = ");
		sb.append(storeId);

		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "StoreDAO - getChainID");
		try
		{
			if (crs.next())
				chainID = crs.getInt("COMP_CHAIN_ID");
		}
		catch (SQLException sqlce)
		{
			throw new GeneralException("StoreDAO - getChainID - get Cached Row Set", sqlce);
		}
		return chainID;
	}

	
	public CachedRowSet getChainsCheckedinLastXDays(Connection conn, int noOfDaysThreshold, int noofStoresThreshold, 
													String fromDate) throws GeneralException
	{

		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		// Fill in the query
		sb.append(" SELECT COMP_CHAIN_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID IN ");
		sb.append(" ( SELECT distinct comp_str_id from SCHEDULE where");
		sb.append(" current_status in (");
		sb.append(Constants.COMPLETED).append(',');
		sb.append(Constants.PARTIALLY_COMPLETED).append(')');
		sb.append("and status_chg_date > TO_DATE('");
		sb.append(fromDate).append("', 'MM/DD/YYYY') - ");
		sb.append(noOfDaysThreshold);
		sb.append(")");
		sb.append(" group by comp_chain_id");
		sb.append(" having count(comp_chain_id) >= ");
		sb.append(noofStoresThreshold);
		logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "StoreDAO - getStoresCheckedinLastXDays");
		return crs;
	}

	public ArrayList<VariationAnalysisDto> getCategory(String deptId) throws GeneralException {
		Connection con = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		VariationAnalysisDto vDto = null;
		ArrayList<VariationAnalysisDto> allstores = null;

		String sql = null;
		try
		{
			allstores = new ArrayList<VariationAnalysisDto>();

			con = DBManager.getOracleConnection();

			if (deptId.equalsIgnoreCase("All"))
			{
				sql = "SELECT id , name as names FROM category order by name";
			} else
			{
				sql = "SELECT id , name as names FROM category WHERE dept_id =" + deptId + " order by name";
			}

			preparedStatement = con.prepareStatement(sql);
			logger.debug("Category Qry>>" + sql);

			rs = preparedStatement.executeQuery();

			while (rs.next())
			{
				vDto = new VariationAnalysisDto();
				vDto.setCatId(String.valueOf(rs.getInt("id")));
				vDto.setCatName(rs.getString("names"));
				allstores.add(vDto);
			}
		}
		catch (SQLException se)
		{
			throw new GeneralException("Error while getting categories from DB", se);
		}
			PristineDBUtil.close(rs, preparedStatement, con);
		

		return allstores;
	}

	public ArrayList<VariationAnalysisDto> getItemList(String categories, String deptId) throws GeneralException {
		Connection con = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		VariationAnalysisDto vDto = null;
		ArrayList<VariationAnalysisDto> allItems = null;

		StringBuffer sql = null;
		try
		{
			sql = new StringBuffer();
			allItems = new ArrayList<VariationAnalysisDto>();

			con = DBManager.getOracleConnection();

			if (categories.contains("All"))
			{
				sql.append(" SELECT item_code,retailer_item_code,item_name  FROM item_lookup ");
				
				if (!deptId.contains("All"))
				{
					sql.append(" WHERE category_id in(SELECT id FROM category ");
					sql.append(" WHERE dept_id=" + deptId + ") ");
				}
				
				
			} else
			{
				sql.append(" SELECT item_code,retailer_item_code,item_name FROM item_lookup WHERE category_id in(" + categories + ")");
			}
			
			sql.append(" ORDER BY item_name");
			
			logger.info("Items Qry>>" + sql);

			preparedStatement = con.prepareStatement(sql.toString());

			rs = preparedStatement.executeQuery();

			while (rs.next())
			{
				vDto = new VariationAnalysisDto();
				vDto.setItemCode(String.valueOf(rs.getInt("item_code")));
				vDto.setRetailItemcode(String.valueOf(rs.getInt("retailer_item_code")));
				vDto.setItemName(rs.getString("item_name"));
				allItems.add(vDto);
			}
		}
		catch (SQLException se)
		{
			throw new GeneralException("Error while getting list of items from DB", se);
		}
		finally
		{
			PristineDBUtil.close(rs, preparedStatement, con);
		}

		return allItems;
	}
	
	public float findRelationship( Connection conn, int chainId, int checkListId, int str1Id, int str2Id) throws
		GeneralException {
		float retVal = 0.0f;
		StringBuffer sb = new StringBuffer();
		sb.append("select ROUND( max( MATCH_COUNT*100/COMMON_FOUND_COUNT), 2) ");
		sb.append(" from ZONECHECKSUMMARY_TEMP ");
		sb.append(" where comp_chain_id = ").append(chainId).append(" and ");
		sb.append(" list_id = ").append(checkListId).append(" and ");
		sb.append(" (comp_str_id1 = ").append(str1Id).append(" and ").append("comp_str_id2 = ").append(str2Id).append(" ) or ");
		sb.append(" (comp_str_id1 = ").append(str2Id).append(" and ").append("comp_str_id2 = ").append(str1Id).append(" ) ");

		//logger.debug(sb.toString());
		String relationshipVal = PristineDBUtil.getSingleColumnVal(conn, sb, "Store Relationship");
		if ( relationshipVal != null)
			retVal = Float.parseFloat(relationshipVal);
		return retVal;
	}

	/*
	 *****************************************************************
	 * Method to get the processing Chain Id
	 * Argument 1 : DB connection
	 * Return	  : int (chain Id)
	 * @throws GeneralException
	 * ****************************************************************
	 */		
	public int getSubscriberCompetitorChain(Connection conn) throws GeneralException {
		StringBuffer buffer = new StringBuffer();
		int chainId = -1;
		buffer.append(" SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN");
		buffer.append(" WHERE PRESTO_SUBSCRIBER='Y'");
		
		CachedRowSet result = PristineDBUtil.executeQuery(conn, buffer, "CompetitorChain");
		
		try {
			if( result.next()){
				chainId = result.getInt("COMP_CHAIN_ID");
			}
		}catch(Exception e){
			throw new GeneralException ( "Cached Rowset Access Exception", e);
		}

		return chainId;
		
	}

	public int getStoreID(Connection conn, String storenum, String altNum, int chainId) throws GeneralException {
		int storeId = -1;
		StringBuffer sb = new StringBuffer();
		// sb.append("SELECT COMP_STR_ID from COMPETITOR_STORE WHERE (COMP_STR_NO = ");
		// Modified on 28th Dec 2012 to handle stores without comp_str_no
		// sb.append("'").append(storenum).append("'");
		sb.append("SELECT COMP_STR_ID from COMPETITOR_STORE WHERE ( ");
		if (storenum != null && storenum != "")
			sb.append("COMP_STR_NO = ").append("'").append(storenum).append("'");
		if (altNum != null && altNum != "") {
			if (storenum != null && storenum != "")
				sb.append(" OR ");
			sb.append(" GLN = '").append(altNum.trim()).append("'");
		}

		sb.append(')');

		if (chainId > 0)
			sb.append(" AND COMP_CHAIN_ID = ").append(chainId);

		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "StoreDAO - getChainID");
		try {
			if (crs.next())
				storeId = crs.getInt("COMP_STR_ID");
		} catch (SQLException sqlce) {
			throw new GeneralException("StoreDAO - getStoreID - get Cached Row Set", sqlce);
		}
		return storeId;
	}

	public void insertStore(Connection conn, StoreDTO storeData) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" INSERT INTO COMPETITOR_STORE(");
		sb.append(" COMP_STR_ID, COMP_STR_NO, COMP_CHAIN_ID, NAME,");
		sb.append(" ADDR_LINE1, ADDR_LINE2, CITY,STATE,ZIP,TIME_ZONE,");
		sb.append(" PHARMACY_IND,GAS_STATION_IND,ACTIVE_INDICATOR,BANK_IND, FAST_FOOD_IND,COFFEE_SHOP_IND,");
		sb.append(" STORE_TYPE,HRS24IND,GLN,PI_AVAILABILITY,");
		sb.append(" STORE_MANAGER_NAME,CONTACT_NUMBER,FAX_NUMBER,");
		sb.append(" DIVISION_ID,REGION_ID,DISTRICT_ID,");
		sb.append(" STORE_CLASS,ADDL_TYPE1,ADDL_TYPE2,");
		sb.append(" OPEN_DATE,REMODEL_DATE,ACQ_DATE,CLOSE_DATE,ANNV_DATE,");
		sb.append(" SQ_FOOTAGE,DEPT1_ZONE_NO,DEPT2_ZONE_NO, DEPT3_ZONE_NO, PRICE_ZONE_ID, COMMENTS, ");
		// Added on 26th Nov 2013, to update timestamp when new store is added
		sb.append(" CREATE_TIMESTAMP, SOURCE_INFO )");
		sb.append(" VALUES (");
		sb.append( "COMP_STORE_SEQ.NEXTVAL,");
		PristineDBUtil.append(sb, storeData.strNum);
		PristineDBUtil.append(sb, storeData.chainId);
		PristineDBUtil.append(sb, storeData.strName);
		PristineDBUtil.append(sb, storeData.addrLine1);
		PristineDBUtil.append(sb, storeData.addrLine2);
		PristineDBUtil.append(sb, storeData.city);
		PristineDBUtil.append(sb, storeData.state);
		PristineDBUtil.append(sb, storeData.zip);
		PristineDBUtil.append(sb, storeData.timeZone);
		PristineDBUtil.append(sb, storeData.pharmacyInd);
		PristineDBUtil.append(sb, storeData.gasStationInd);
		PristineDBUtil.append(sb, "Y");
		PristineDBUtil.append(sb, storeData.bankInd);
		PristineDBUtil.append(sb, storeData.fastFoodInd);
		PristineDBUtil.append(sb, storeData.coffeeShopInd);
		PristineDBUtil.append(sb, storeData.storeType);
		PristineDBUtil.append(sb, storeData.is24HrInd);
		PristineDBUtil.append(sb, storeData.gblLocNum);
		PristineDBUtil.append(sb, "N");
		PristineDBUtil.append(sb, storeData.storeMgrName);
		PristineDBUtil.append(sb, storeData.storePhoneNo);
		PristineDBUtil.append(sb, storeData.storeFaxNo);
		PristineDBUtil.append(sb, storeData.divId);
		PristineDBUtil.append(sb, storeData.regId);
		PristineDBUtil.append(sb, storeData.distId);

		PristineDBUtil.append(sb, storeData.storeClass);
		PristineDBUtil.append(sb, storeData.addlType1);
		PristineDBUtil.append(sb, storeData.addlType2);
		PristineDBUtil.appendDate(sb, storeData.storeOpenDate);
		PristineDBUtil.appendDate(sb, storeData.storeReModelDate);
		PristineDBUtil.appendDate(sb, storeData.storeAcqDate);
		PristineDBUtil.appendDate(sb, storeData.storeCloseDate);
		PristineDBUtil.appendDate(sb, storeData.storeAnnvDate);
		PristineDBUtil.append(sb, Float.valueOf(storeData.sqFootage).intValue());
		PristineDBUtil.append(sb, storeData.dept1ZoneNum);
		PristineDBUtil.append(sb, storeData.dept2ZoneNum);
		PristineDBUtil.append(sb, storeData.dept3ZoneNum);
		PristineDBUtil.append(sb, storeData.zoneId);
		PristineDBUtil.append(sb, storeData.storeComment);
		sb.append("SYSDATE,");
		if (storeData.sourceInfo.equals("MARKET-DATA")) {
			sb.append("'MARKET-DATA'");
		} else {
			sb.append("'STORE-FILE'");
		}

		sb.append(")");
		//logger.debug(sb.toString());
		PristineDBUtil.execute(conn, sb, "ItemGrpDAO - Dept Insert");
		
	}

	public void updateStore(Connection conn, StoreDTO storeData) throws GeneralException {

		StringBuffer sb = new StringBuffer("update COMPETITOR_STORE set ");

		PristineDBUtil.appendUpdate(sb, "ADDR_LINE1", storeData.addrLine1);
		PristineDBUtil.appendUpdate(sb, "ADDR_LINE2", storeData.addrLine2);
		PristineDBUtil.appendUpdate(sb, "CITY", storeData.city);
		PristineDBUtil.appendUpdate(sb, "STATE", storeData.state);
		PristineDBUtil.appendUpdate(sb, "ZIP", storeData.zip);
		PristineDBUtil.appendUpdate(sb, "TIME_ZONE", storeData.timeZone);
		PristineDBUtil.appendUpdate(sb, "PHARMACY_IND", storeData.pharmacyInd);
		PristineDBUtil.appendUpdate(sb, "GAS_STATION_IND", storeData.gasStationInd);
		PristineDBUtil.appendUpdate(sb, "ACTIVE_INDICATOR","Y");
		PristineDBUtil.appendUpdate(sb, "BANK_IND",storeData.bankInd);
		PristineDBUtil.appendUpdate(sb, "FAST_FOOD_IND", storeData.fastFoodInd);
		PristineDBUtil.appendUpdate(sb, "COFFEE_SHOP_IND", storeData.coffeeShopInd);
		PristineDBUtil.appendUpdate(sb, "STORE_TYPE", storeData.storeType);
		PristineDBUtil.appendUpdate(sb, "HRS24IND", storeData.is24HrInd);
		PristineDBUtil.appendUpdate(sb, "GLN", storeData.gblLocNum);
		PristineDBUtil.appendUpdate(sb, "STORE_MANAGER_NAME", storeData.storeMgrName);
		PristineDBUtil.appendUpdate(sb, "CONTACT_NUMBER", storeData.storePhoneNo);
		PristineDBUtil.appendUpdate(sb, "FAX_NUMBER", storeData.storeFaxNo);
		PristineDBUtil.appendUpdate(sb, "DISTRICT_ID", storeData.distId);
		PristineDBUtil.appendUpdate(sb, "REGION_ID", storeData.regId);
		PristineDBUtil.appendUpdate(sb, "DIVISION_ID", storeData.divId);
		PristineDBUtil.appendUpdate(sb, "STORE_CLASS", storeData.storeClass);
		PristineDBUtil.appendUpdate(sb, "ADDL_TYPE1", storeData.addlType1);
		PristineDBUtil.appendUpdate(sb, "ADDL_TYPE2", storeData.addlType2);
		PristineDBUtil.appendUpdateDate(sb, "OPEN_DATE", storeData.storeOpenDate);
		PristineDBUtil.appendUpdateDate(sb, "REMODEL_DATE", storeData.storeReModelDate);
		PristineDBUtil.appendUpdateDate(sb, "ACQ_DATE", storeData.storeAcqDate);
		PristineDBUtil.appendUpdateDate(sb, "CLOSE_DATE", storeData.storeCloseDate);
		PristineDBUtil.appendUpdateDate(sb, "ANNV_DATE", storeData.storeAnnvDate);
		
		PristineDBUtil.appendUpdate(sb, "SQ_FOOTAGE", Float.valueOf(storeData.sqFootage).intValue());
		PristineDBUtil.appendUpdate(sb, "DEPT1_ZONE_NO", storeData.dept1ZoneNum);
		PristineDBUtil.appendUpdate(sb, "DEPT2_ZONE_NO", storeData.dept2ZoneNum);
		PristineDBUtil.appendUpdate(sb, "DEPT3_ZONE_NO", storeData.dept3ZoneNum);
		PristineDBUtil.appendUpdate(sb, "PRICE_ZONE_ID",  storeData.zoneId);
		PristineDBUtil.appendUpdate(sb, "COMMENTS",  storeData.storeComment);
		//sb.append(" COMMENTS = ");
		//PristineDBUtil.appendNoComa(sb, storeData.storeComment);
		
		//Added on 26th Nov 2013, to update timestamp when store is updated
		sb.append("UPDATE_TIMESTAMP = SYSDATE,");
		sb.append("SOURCE_INFO = 'STORE-FILE'");
				
		
		sb.append(" WHERE COMP_STR_ID = " + storeData.strId);

		//logger.debug(sb.toString());
		PristineDBUtil.executeUpdate(conn, sb, "Store DAO - update Store");

	}

	/**
	 * Returns a map containing store no as key and store id as value
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getStoreIdMap(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet rs = null;
		String sql = "SELECT COMP_STR_NO, COMP_STR_ID FROM COMPETITOR_STORE WHERE "
				+ "COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y')";
				
		
		HashMap<String, Integer> storeIdMap = new HashMap<String, Integer>();
		try {
			statement = conn.prepareStatement(sql);
			statement.setFetchSize(5000);
			rs = statement.executeQuery();
			while (rs.next()) {
				storeIdMap.put(rs.getString("COMP_STR_NO"), rs.getInt("COMP_STR_ID"));
			}
			logger.info("No of Stores - " + storeIdMap.size());
		} catch (SQLException ex) {
			logger.error("Error when retrieving store id map - " + ex.getMessage());
			throw new GeneralException("Error when retrieving store id map - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(statement);
		}
		return storeIdMap;
	}

	/**
	 * Returns a map containing store no as key and store info as value in StoreDTO
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, StoreDTO> getStoreInfoMap(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet rs = null;
		String sql = "SELECT COMP_STR_NO, CITY, STATE FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID = (SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER = 'Y')";

		HashMap<String, StoreDTO> storeInfoMap = new HashMap<String, StoreDTO>();
		try {
			statement = conn.prepareStatement(sql);
			statement.setFetchSize(5000);
			rs = statement.executeQuery();
			while (rs.next()) {
				StoreDTO sDto = new StoreDTO();
				sDto.city = rs.getString("CITY");
				sDto.state = rs.getString("STATE");
				storeInfoMap.put(rs.getString("COMP_STR_NO"), sDto);
			}
			logger.info("No of Stores - " + storeInfoMap.size());
		} catch (SQLException ex) {
			logger.error("Error when retrieving store info map - " + ex.getMessage());
			throw new GeneralException("Error when retrieving store info map - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(statement);
		}
		return storeInfoMap;
	}

	/**
	 * Returns Store Info for the given Comp Store Id
	 * @param conn			Connection
	 * @param compStrId		Comp Store id
	 * @return
	 * @throws GeneralException
	 */
	public StoreDTO getStoreInfo(Connection conn, Integer compStrId) throws GeneralException {
		String sql = "SELECT COMP_STR_NO, NAME FROM COMPETITOR_STORE WHERE COMP_STR_ID = ?";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		StoreDTO storeDTO = null;
		try {
			stmt = conn.prepareStatement(sql);
			stmt.setInt(1, compStrId);
			rs = stmt.executeQuery();
			if (rs.next()) {
				storeDTO = new StoreDTO();
				storeDTO.strNum = rs.getString("COMP_STR_NO");
				storeDTO.strName = rs.getString("NAME");
			}
		} catch (SQLException ex) {
			logger.error("Error when retrieving store info map - " + ex.getMessage());
			throw new GeneralException("Error when retrieving store info map - " + ex.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return storeDTO;
	}

	public StoreDTO getStoreInfoWithZone(Connection conn, Integer compStrId) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		StoreDTO storeDTO = null;
		try {
			statement = conn.prepareStatement(GET_STORE_INFO);
			statement.setInt(1, compStrId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				storeDTO = new StoreDTO();
				storeDTO.strId = resultSet.getInt("COMP_STR_ID");
				storeDTO.strNum = resultSet.getString("COMP_STR_NO");
				storeDTO.zoneNum = resultSet.getString("ZONE_NUM");
				storeDTO.zoneId = resultSet.getInt("PRICE_ZONE_ID");
				storeDTO.dept1ZoneNum = resultSet.getString("DEPT1_ZONE_NO");
				storeDTO.dept2ZoneNum = resultSet.getString("DEPT2_ZONE_NO");
				storeDTO.dept3ZoneNum = resultSet.getString("DEPT3_ZONE_NO");
			}
		} catch (SQLException e) {
			logger.error("Error while executing getStoreInfoWithZone");
			throw new GeneralException("Error while executing getStoreInfoWithZone", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeDTO;
	}
	
	
	public HashMap<String, Date> getClosedStores(Connection conn) throws GeneralException{
		HashMap<String, Date> closedStores = new HashMap<String, Date>();
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    try{
	    	statement = conn.prepareStatement("SELECT COMP_STR_ID, CLOSE_DATE FROM COMPETITOR_STORE WHERE CLOSE_DATE IS NOT NULL");
	    	resultSet = statement.executeQuery();
	    	while(resultSet.next()){
	    		closedStores.put(resultSet.getString("COMP_STR_ID"), resultSet.getDate("CLOSE_DATE"));
	    	}
	    }
	    catch(SQLException sqlE){
	    	logger.error("Error while executing getClosedStores");
			throw new GeneralException("Error while executing getClosedStores", sqlE);
	    }
	    
	    return closedStores;
	}
	
	public List<StoreDTO> getStoresDetail(Connection conn, int chainId) throws GeneralException {
		List<StoreDTO> storeDetails = new ArrayList<StoreDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		StoreDTO storeDTO = null;
		try {
			statement = conn.prepareStatement(GET_STORE_DETAILS);
			statement.setInt(1, chainId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				storeDTO = new StoreDTO();
				storeDTO.strId = resultSet.getInt("COMP_STR_ID");
				storeDTO.strNum = resultSet.getString("COMP_STR_NO");
				storeDTO.zoneNum = resultSet.getString("ZONE_NUM");
				storeDTO.zoneId = resultSet.getInt("PRICE_ZONE_ID");
				storeDTO.storeCloseDateAsDate = resultSet.getDate("CLOSE_DATE");
				storeDetails.add(storeDTO);
			}
		} catch (SQLException e) {
			logger.error("Error while executing getStoresDetail");
			throw new GeneralException("Error while executing getStoresDetail", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeDetails;
	}
	public HashMap<String, GiantEagleStoreDTO> getStoreandCompNameMap(Connection conn) throws GeneralException{
		HashMap<String, GiantEagleStoreDTO> storeandCompNameMap = new HashMap<String, GiantEagleStoreDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		GiantEagleStoreDTO giantEagleStoreDTO = null;
		try {
			statement = conn.prepareStatement(GET_STORE_AND_COMP_NAME);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				giantEagleStoreDTO = new GiantEagleStoreDTO();
				giantEagleStoreDTO.setBNR_CD(resultSet.getString("SHRT_NAME"));
				giantEagleStoreDTO.setCORP_NAME(resultSet.getString("COMP_CHAIN_NAME"));
				giantEagleStoreDTO.setCHAIN_ID(resultSet.getString("COMP_CHAIN_ID"));

				storeandCompNameMap.put(giantEagleStoreDTO.getBNR_CD(), giantEagleStoreDTO);

			}
		} catch (SQLException e) {
			logger.error("Error while executing getStoresDetail");
			throw new GeneralException("Error while executing getStoresDetail", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeandCompNameMap;

	}

	public HashMap<String, List<String>> getstoresinZone(Connection conn) {

		HashMap<String, List<String>> zoneList = new HashMap<>();

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			statement = conn.prepareStatement(GET_STORE_AND_ZONE);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String zoneNum = resultSet.getString("ZONE_NUM");
				String storeNum = resultSet.getString("COMP_STR_NO");
				List<String> tempList = new ArrayList<String>();
				if (zoneList.containsKey(zoneNum)) {
					tempList.addAll(zoneList.get(zoneNum));
				}

				tempList.add(storeNum);
				zoneList.put(zoneNum, tempList);

			}
		} catch (SQLException e) {
			logger.error("Error while executing getStoresDetail");

		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return zoneList;
	}

	public List<String> getExistingStores(Connection conn, int chainId) {
		logger.info("getExistingStores()-Inside");
		List<String> storeList = new ArrayList<String>();

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			statement = conn.prepareStatement(GET_EXISTING_STORES);
			statement.setInt(1, chainId);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				storeList.add(resultSet.getString("COMP_STR_NO"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing getStoresDetail");

		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		logger.info("storeList size" + storeList.size());
		return storeList;
	}

	public StoreDTO setStoreDetails(Connection conn, StoreDTO storeDTO, String zoneNum) {
		logger.info("setStoreDetails()-Inside for zone " + zoneNum);
		PreparedStatement statement = null;
		StoreDTO storeDTOclone = storeDTO;
		ResultSet resultSet = null;
		//Adding this condition beacuse the zoneNumbers for primary zones is changed wwith prefix ZP
		String ZoneNumnew="ZP"+ zoneNum;
		try {
			statement = conn.prepareStatement(GET_ZONE_ADD_FROM_STORE);
			statement.setString(1, zoneNum);
			statement.setString(2, ZoneNumnew);
			logger.debug("setStoreDetails()- parameter 1." + zoneNum + ";2 ." + ZoneNumnew);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {

				storeDTOclone.addrLine1 = (resultSet.getString("ADDR_LINE1"));
				storeDTOclone.zip = (resultSet.getString("ZIP"));
				storeDTOclone.city = (resultSet.getString("CITY"));
				storeDTOclone.state = (resultSet.getString("STATE"));
				storeDTOclone.timeZone = (resultSet.getString("TIME_ZONE"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing setStoreDetails" + e);

		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return storeDTOclone;
	}
	
	public HashMap<String, Integer> getZoneStrcount(Connection conn) {

		HashMap<String, Integer> zoneStrList = new HashMap<>();

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			statement = conn.prepareStatement(ZONE_STR_COUNT);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String zoneNum = resultSet.getString("zone_num");
				int storeCount = resultSet.getInt("COUNT");

				zoneStrList.put(zoneNum, storeCount);

			}
		}

		catch (Exception e) {
			logger.error("Error while executing getZoneStrcount()" + e);
		}

		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return zoneStrList;
	}

	public HashMap<String, String> getZoneandStoreInfo(Connection conn) {

		HashMap<String, String> zoneList = new HashMap<>();

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			statement = conn.prepareStatement(GET_STORE_AND_ZONE);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String zoneNum = resultSet.getString("ZONE_NUM");
				String storeNum = resultSet.getString("COMP_STR_NO");

				zoneList.put(storeNum, zoneNum);

			}
		} catch (SQLException e) {
			logger.error("getZoneandStoreInfo()-Error while executing getStoresDetail" + e);

		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return zoneList;
	}
	
	public HashMap<Integer, Integer> getPriceZoneStrcount(Connection conn) {

		HashMap<Integer, Integer> zoneStrList = new HashMap<>();

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			logger.debug("strcount query-  " + PRICE_ZONE_STR_COUNT);
			statement = conn.prepareStatement(PRICE_ZONE_STR_COUNT);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int zoneNum = resultSet.getInt("PRICE_ZONE_ID");
				int storeCount = resultSet.getInt("COUNT");

				zoneStrList.put(zoneNum, storeCount);

			}
		}

		catch (Exception e) {
			logger.error("Error while executing getZoneStrcount()" + e);
		}

		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return zoneStrList;
	}

	public int getActiveZonesCount(Connection conn) {

		int count = 0;

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {

			statement = conn.prepareStatement(
					"SELECT count(*) AS COUNT FROM RETAIL_PRICE_ZONE WHERE  ACTIVE_INDICATOR='Y' and primary_comp_str_id is not null");
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				count = resultSet.getInt("COUNT");
			}
		}

		catch (Exception e) {
			logger.error("Error while executing getActiveZonesCount()" + e);
		}

		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return count;
	}
	
	

}
