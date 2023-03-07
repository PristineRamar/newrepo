package com.pristine.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.offermgmt.PRCommonUtil;

public class CompStoreDAO implements IDAO {
	
	private static Logger logger = Logger.getLogger(CompStoreDAO.class);

	public int getChainId(Connection conn, int storeId) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT COMP_CHAIN_ID FROM COMPETITOR_STORE WHERE ");
		sb.append(" COMP_STR_ID = ").append(storeId);
		logger.debug(sb.toString());
		String compChainID = PristineDBUtil.getSingleColumnVal(conn, sb, "CompStoreDAo - getChainID");
		int chainId = -1;
		if (compChainID != null && !compChainID.equals(""))
			chainId = Integer.parseInt(compChainID);
		return chainId;
	}

	public CachedRowSet getKeyStoreList(Connection conn) throws GeneralException{
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE ");
		sb.append(" IS_KEY_STORE = 'Y'");
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getCompetitiveDataForStoreComparision");
		
		return result;
	}

	public List<String> getKeyStoreNumberList (Connection conn) throws GeneralException
	{
		StringBuffer sb = new StringBuffer();
		//sb.append("SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE IS_KEY_STORE = 'Y' ORDER BY COMP_STR_NO");
		//To get all the store ids
		sb.append("SELECT COMP_STR_NO FROM COMPETITOR_STORE ORDER BY COMP_STR_NO");
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getKeyStoreNumberList");
		List<String> ret = new ArrayList<String>();
		try
		{
			while ( result.next() ) {
				String storeNo = result.getString("COMP_STR_NO");
				ret.add(storeNo);
			}
		}
		catch (SQLException ex) {
		}
		return ret;
	}
	
	public List<String> getKeyLocationList (Connection conn) throws GeneralException
	{
		StringBuffer sb = new StringBuffer();
		//sb.append("SELECT COMP_STR_NO FROM COMPETITOR_STORE WHERE IS_KEY_STORE = 'Y' ORDER BY COMP_STR_NO");
		//To get all the store ids
		sb.append("SELECT COMP_STR_ID FROM COMPETITOR_STORE  WHERE COMP_CHAIN_ID = 50 ORDER BY COMP_STR_ID");
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getKeyStoreNumberList");
		List<String> ret = new ArrayList<String>();
		try
		{
			while ( result.next() ) {
				String storeNo = result.getString("COMP_STR_ID");
				ret.add(storeNo);
			}
		}
		catch (SQLException ex) {
		}
		return ret;
	}
	
	/*
	 * ****************************************************************
	 * Get the storenumebrs and store id
	 * Argument 1 : conn
	 * Argument 2 : districtNumber
	 * Argument 3 : storeNumber
	 * Return Store Id List
	 * catch GeneralException , SqlException 
	 * ****************************************************************
	 */

	public List<SummaryDataDTO> getStoreNumebrs(Connection conn,
			String districtNumber, String storeNumber) throws GeneralException {

		List<SummaryDataDTO> storeLsit = new ArrayList<SummaryDataDTO>();

		StringBuffer sql = new StringBuffer();
		sql.append("  select COMP_STR_ID,COMP_STR_NO,STATE,OPEN_DATE from COMPETITOR_STORE");
		if (storeNumber != null && storeNumber != "") {
			sql.append("  where COMP_STR_NO='" + storeNumber + "'");
		}
		else if (districtNumber != null && districtNumber != "") {
			sql.append(" where DISTRICT_ID='" + districtNumber + "'");
		}

		logger.debug("getStoreNumebrs SQL:" + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
														"getStoreNumebrs");
			while (result.next()) {
				SummaryDataDTO dataDto = new SummaryDataDTO();
				dataDto.setLocationId(result.getInt("COMP_STR_ID"));
				dataDto.setstoreNumber(result.getString("COMP_STR_NO"));
				dataDto.setstoreState(result.getString("STATE"));
				dataDto.setStoreOpenDate(result.getString("OPEN_DATE"));
				storeLsit.add(dataDto);
			}

			// logger.debug(" Store Count " +storeLsit.size());

		} catch (GeneralException gex) {
			logger.error("Error while fetching store data " + gex.getMessage());
			throw new GeneralException("getStoreNumebrs", gex);			
		} catch (SQLException sex) {
			logger.error("Error while fetching store data " + sex.getMessage());
			throw new GeneralException("getStoreNumebrs", sex);			
		}
		return storeLsit;
	}

	/*
	 * ****************************************************************
	 * Get the storedetails and store id
	 * Argument 1 : conn
	 * Argument 2 : storeNumber
	 * Return SummaryDataDTO
	 * catch GeneralException , SqlException 
	 * ****************************************************************
	 */

	public SummaryDataDTO getStoreDetails(Connection conn, 
			String storeNumber) throws GeneralException {

		SummaryDataDTO dataDto = new SummaryDataDTO();

		StringBuffer sql = new StringBuffer();
		sql.append("  select COMP_STR_ID,COMP_STR_NO,STATE,OPEN_DATE from COMPETITOR_STORE");
			sql.append("  where COMP_STR_NO='" + storeNumber + "'");

		logger.debug("getStoreDetails SQL:" + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
														"getStoreDetails");
			while (result.next()) {
				dataDto.setLocationId(result.getInt("COMP_STR_ID"));
				dataDto.setstoreNumber(result.getString("COMP_STR_NO"));
				dataDto.setstoreState(result.getString("STATE"));
				dataDto.setStoreOpenDate(result.getString("OPEN_DATE"));
			}

			// logger.debug(" Store Count " +storeLsit.size());

		} catch (GeneralException gex) {
			logger.error("Error while fetching store data " + gex.getMessage());
			throw new GeneralException("getStoreNumebrs", gex);			
		} catch (SQLException sex) {
			logger.error("Error while fetching store data " + sex.getMessage());
			throw new GeneralException("getStoreNumebrs", sex);			
		}
		return dataDto;
	}

	/*
	 * ****************************************************************
	 * Get the storedetails and store id
	 * Argument 1 : conn
	 * Argument 2 : storeNumber
	 * Return SummaryDataDTO
	 * catch GeneralException , SqlException 
	 * ****************************************************************
	 */

	public List<SummaryDataDTO> getStoreDetailsList(Connection conn, 
			List<String> storeNums) throws GeneralException {

		List<SummaryDataDTO> stores =  new ArrayList<>();
		

		StringBuffer sql = new StringBuffer();
		sql.append("  select COMP_STR_ID,COMP_STR_NO,STATE,OPEN_DATE from COMPETITOR_STORE");
		sql.append("  where COMP_STR_NO IN ( " + PRCommonUtil.getCommaSeperatedStringFromString(storeNums, true).replaceAll("\"", "'") + " ) ");

		logger.debug("getStoreDetails SQL:" + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
														"getStoreDetails");
			while (result.next()) {
				SummaryDataDTO dataDto = new SummaryDataDTO();
				dataDto.setLocationId(result.getInt("COMP_STR_ID"));
				dataDto.setstoreNumber(result.getString("COMP_STR_NO"));
				dataDto.setstoreState(result.getString("STATE"));
				dataDto.setStoreOpenDate(result.getString("OPEN_DATE"));
				stores.add(dataDto);
			}

			// logger.debug(" Store Count " +storeLsit.size());

		} catch (GeneralException gex) {
			logger.error("Error while fetching store data " + gex.getMessage());
			throw new GeneralException("getStoreNumebrs", gex);			
		} catch (SQLException sex) {
			logger.error("Error while fetching store data " + sex.getMessage());
			throw new GeneralException("getStoreNumebrs", sex);			
		}
		return stores;
	}

	
	
	
	
	
	/*
	 * ****************************************************************
	 * Get the storenumebrs and store id
	 * Argument 1 : conn
	 * Argument 2 : districtNumber
	 * Argument 3 : storeNumber
	 * Return Store Id List
	 * catch GeneralException , SqlException 
	 * ****************************************************************
	 */

	public HashMap<String, Integer> getCompStrId(Connection conn) throws GeneralException {

		HashMap<String, Integer> returnMap = new HashMap<String, Integer>();
		StringBuffer sql = new StringBuffer();
		sql.append("  select COMP_STR_ID,COMP_STR_NO from COMPETITOR_STORE");
		sql.append("  where COMP_CHAIN_ID='50'");
	//	logger.debug("Sql :"+sql.toString());
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,"getStoreNumebrs");
			
			while (result.next()) {
				returnMap.put(result.getString("COMP_STR_NO") ,result.getInt("COMP_STR_ID"));
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new GeneralException("Get CompStrId method Error");
		} 
		
	//	logger.debug(" Store Map size .... " + returnMap.size());
		
		return returnMap;
	}

	public HashMap<String, Integer> getDistrictList(Connection conn) throws GeneralException {
		
		logger.debug("getDistrictList Method");
		HashMap<String, Integer> districtList = new HashMap<String, Integer>();
		StringBuffer sql = new StringBuffer(" select ID from RETAIL_DISTRICT");
	    try{
	    	CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "Get District Id");
	    	while(result.next()){
	    		districtList.put(result.getString("ID"),result.getInt("ID"));
	    	}
	    logger.debug("District List size" + districtList.size());	
	     }catch(Exception exe){
	    	logger.error(exe);
	    	throw new GeneralException("Get District Id Error" , exe);
	    }
	
		return districtList;
	}
	
	
	public HashMap<String, Integer> getRegionList(Connection conn) throws GeneralException {
		
		logger.debug("Get Region List Method");
		HashMap<String, Integer> regionList = new HashMap<String, Integer>();
		StringBuffer sql = new StringBuffer(" select ID from RETAIL_REGION");
	    try{
	    	CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "Get Region Id");
	    	while(result.next()){
	    		regionList.put(result.getString("ID"),result.getInt("ID"));
	    	}
	    logger.debug("Region List size" + regionList.size());	
	     }catch(Exception exe){
	    	logger.error(exe);
	    	throw new GeneralException("Get Region Id Error" , exe);
	    }
	
		return regionList;
	}
	
	
	public HashMap<String, Integer> getDivisionList(Connection conn) throws GeneralException {
		
		logger.debug("Get Division List Method");
		HashMap<String, Integer> divisionList = new HashMap<String, Integer>();
		StringBuffer sql = new StringBuffer(" select ID from RETAIL_DIVISION");
	    try{
	    	CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "Get Division Id");
	    	while(result.next()){
	    		divisionList.put(result.getString("ID"),result.getInt("ID"));
	    	}
	    logger.debug("Division List size" + divisionList.size());	
	     }catch(Exception exe){
	    	logger.error(exe);
	    	throw new GeneralException("Get Division Id Error" , exe);
	    }
	
		return divisionList;
	}
	
	public int getCompChainId(Connection conn) throws GeneralException{
		
		logger.debug(" Get chain id....");
		int returnVal = 0;
		 
			try {
				StringBuffer sql = new StringBuffer();
				sql.append(" select COMP_CHAIN_ID from COMPETITOR_CHAIN");
				sql.append(" where PRESTO_SUBSCRIBER='Y'");
				
				String result = PristineDBUtil.getSingleColumnVal(conn, sql, "getCompChainId");
				
				if( result != null ){
					returnVal = Integer.parseInt(result);
				}
			} catch (GeneralException e) {
				logger.error(" Error while fetching the chain id..." , e);
				throw new GeneralException(" Error while fetching the chain id..." , e);
			} 
		 			
		return returnVal;
	}
	
	
	public CompetitiveDataDTO getCompeStoreDetails(Connection conn,
			String districtNumber, String storeNumber) throws GeneralException {

		CompetitiveDataDTO objCompDataDto = new CompetitiveDataDTO();
		
		try{
		StringBuffer sql = new StringBuffer();
		sql.append("  select COMP_STR_ID,DISTRICT_ID");
		sql.append(" ,REGION_ID,DIVISION_ID,COMP_CHAIN_ID");
		sql.append(" from COMPETITOR_STORE");
		if (storeNumber != "") {
			sql.append("  where COMP_STR_NO='" + storeNumber + "'");
		}
		if (districtNumber != "") {
			sql.append(" where DISTRICT_ID='" + districtNumber + "'");
		}

		logger.debug(" getStoreNumebrs Sql " + sql.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
																"getStoreNumebrs");
		while (result.next()) {
			
			objCompDataDto.setCompStrID(result.getInt("COMP_STR_ID"));
			objCompDataDto.setDistrictId(result.getString("DISTRICT_ID"));
			objCompDataDto.setRegionId(result.getString("REGION_ID"));
			objCompDataDto.setDivisionId(result.getString("DIVISION_ID"));
			objCompDataDto.setChainId(result.getString("COMP_CHAIN_ID"));
			
		}
			
		}catch( Exception exe){
			
			logger.error(" DB Error..... Method Name : getCompeStoreDetails " , exe);
			throw new GeneralException(" DB Error..... Method Name : getCompeStoreDetails " , exe);
			
		}
			// logger.debug(" Store Count " +storeLsit.size());

		 
		return objCompDataDto;
	}

	
	/*
	 * ****************************************************************
	 * Get the storenumebrs and store id
	 * Argument 1 : Conn
	 * Argument 2 : Chain Id
	 * Argument 3 : Store no in Array
	 * Return Store Id List
	 * catch GeneralException , SqlException 
	 * ****************************************************************
	 */

	public HashMap<String, Integer> getCompStoreData(Connection conn, 
					int compChain, String[] storeArr) throws GeneralException {

		HashMap<String, Integer> returnMap = new HashMap<String, Integer>();
		
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT COMP_STR_NO, COMP_STR_ID FROM COMPETITOR_STORE");
		sb.append(" WHERE COMP_CHAIN_ID=").append(compChain);
		
		if (storeArr != null && storeArr.length > 0) {
			sb.append(" AND COMP_STR_NO IN (");
			for (int i = 0; i < storeArr.length; i++) {
				
				if (i > 0)
					sb.append(", ");
			
				sb.append("'").append(storeArr[i]).append("'");
			}
			
			sb.append(")");
		}
		
		logger.debug("getCompStoreData: " + sb.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getCompStoreData");
			
			while (result.next()) {
				returnMap.put(result.getString("COMP_STR_NO") ,result.getInt("COMP_STR_ID"));
			}
		} catch (Exception e) {
			logger.error("Error while fetching Store data" + e.getMessage());
			throw new GeneralException("Error while fetching Store data");
		} 
		
		logger.debug(" Store Map size .... " + returnMap.size());
		
		return returnMap;
	}
	
	public List<SummaryDataDTO> getStoreDetailsListBasedOnForecastTable(Connection conn, 
			List<String> storeNums) throws GeneralException {

		List<SummaryDataDTO> stores =  new ArrayList<>();
		

		StringBuffer sql = new StringBuffer();
		sql.append("  select COMP_STR_ID,COMP_STR_NO,STATE,OPEN_DATE from COMPETITOR_STORE");
		sql.append("  where COMP_STR_NO IN ( " + PRCommonUtil.getCommaSeperatedStringFromString(storeNums, true).replaceAll("\"", "'") + " ) ");

		logger.debug("getStoreDetails SQL:" + sql.toString());

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql,
														"getStoreDetails");
			while (result.next()) {
				SummaryDataDTO dataDto = new SummaryDataDTO();
				dataDto.setLocationId(result.getInt("COMP_STR_ID"));
				dataDto.setstoreNumber(result.getString("COMP_STR_NO"));
				dataDto.setstoreState(result.getString("STATE"));
				dataDto.setStoreOpenDate(result.getString("OPEN_DATE"));
				stores.add(dataDto);
			}

			// logger.debug(" Store Count " +storeLsit.size());

		} catch (GeneralException gex) {
			logger.error("Error while fetching store data " + gex.getMessage());
			throw new GeneralException("getStoreNumebrs", gex);			
		} catch (SQLException sex) {
			logger.error("Error while fetching store data " + sex.getMessage());
			throw new GeneralException("getStoreNumebrs", sex);			
		}
		return stores;
	}
	
}
