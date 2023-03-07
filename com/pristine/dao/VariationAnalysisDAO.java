package com.pristine.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;


import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.VariationResultsDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;

public class VariationAnalysisDAO implements IDAO {
	static Logger logger = Logger.getLogger("com.pristine.dao.VariationAnalysisDAO");

	public VariationResultsDTO getAnalysisResultsForSchedule(Connection conn, int scheduleID, 
						int deptID) throws GeneralException {
		
		VariationResultsDTO results = new VariationResultsDTO();
		StringBuffer sb = new StringBuffer();

		String strDept = (deptID == Constants.NULLID)? "NULL" : Integer.toString(deptID);
		sb.append("SELECT ");
		sb.append(" AIV (").append(scheduleID).append(',');
		sb.append(strDept).append(") AIV,");
		sb.append(" DEPTH (").append(scheduleID).append(',');
		sb.append(strDept).append(") DEPTH,");
		sb.append(" SALE_PCT (").append(scheduleID).append(',');
		sb.append(strDept).append(") SALE,");
		sb.append(" REG_NET_PCT (").append(scheduleID).append(',');
		sb.append(strDept).append(") REG_NET,");
		sb.append(" NUM_OF_ITEMS (").append(scheduleID).append(',');
		sb.append(strDept).append(") NUM_OF_ITEMS, ");
		sb.append("TO_CHAR( START_DATE, 'MM/DD/YYYY') START_DATE, ");
		sb.append("TO_CHAR( END_DATE, 'MM/DD/YYYY') END_DATE, ");
		sb.append("TO_CHAR( STATUS_CHG_DATE, 'MM/DD/YYYY') STATUS_CHG_DATE, ");
		sb.append(" B.COMP_CHAIN_ID COMP_CHAIN_ID, ");
		sb.append(" B.COMP_STR_ID COMP_STR_ID ");
		sb.append( " FROM SCHEDULE A, COMPETITOR_STORE B ");
		sb.append( " WHERE SCHEDULE_ID =  ");
		sb.append(scheduleID);
		sb.append( " AND B.COMP_STR_ID = A.COMP_STR_ID ");
 
		logger.debug(sb);
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "VariationAnalysisDAO - getAnalysisResultsForSchedule");
		
		//Populate the value object ...
		
		try{
			while (crs.next()){
				results.setAiv(crs.getFloat("AIV"));
				results.setDepth(crs.getFloat("DEPTH"));
				
				//The reg values are in a comma separated format - RegNet,RegUp, RegDown 
				ArrayList<String> al  = PrestoUtil.csvStrToStrArray(crs.getString("REG_NET"));
				//logger.debug( "Reg Value" + crs.getString("REG_NET"));
				if( al.size() == 3 ){
					results.setNetReg(Float.parseFloat(al.get(0)));
					results.setRegUp(Float.parseFloat(al.get(1)));
					results.setRegDown(Float.parseFloat(al.get(2)));
				}else{
					logger.info("Did not get the regular net, up and down values !!!");
				}
				
				results.setSale(crs.getFloat("SALE"));
				results.setNumOfItems(crs.getInt("NUM_OF_ITEMS"));
				results.setStoreID(crs.getInt("COMP_STR_ID"));
				results.setDeptID(deptID);
				results.setChainID(crs.getInt("COMP_CHAIN_ID"));
				results.setNumOfStores(1);
				results.setWeekStartDate(crs.getString("START_DATE"));
				results.setWeekEndDate(crs.getString("END_DATE"));
				results.setAnalysisDate(crs.getString("STATUS_CHG_DATE"));
			}
		}catch (SQLException sqlce){
			throw new GeneralException("VariationAnalysisDAO - getAnalysisResultsForSchedule", sqlce);
		}
		return results;
	}
	
	
	public VariationResultsDTO getAnalysisResultsForSchedule(Connection conn, int chainId, int storeId, 
			int deptID, String startDate, String endDate) throws GeneralException {

		VariationResultsDTO results = new VariationResultsDTO();
		StringBuffer sb = new StringBuffer();
		
		String strDept = (deptID == Constants.NULLID)? "NULL" : Integer.toString(deptID);
		
		sb.append("SELECT ");
		sb.append( buildFunctionCall("NUM_OF_ITEMS2", chainId, storeId, strDept , 0, startDate, endDate) );
		sb.append(" FROM DUAL ");
		String value = PristineDBUtil.getSingleColumnVal(conn, sb, "variation analysis - getting number of items");
		int numofItems = 0;
		if( value != null)
			numofItems = Integer.parseInt(value);
		else
			throw new GeneralException("No items available to analyze");
		results.setNumOfItems(numofItems);
		if( numofItems == 0)
			return results;
			
		sb = new StringBuffer();
		sb.append("SELECT ");
		sb.append(buildFunctionCall("AIV2", chainId, storeId, strDept , 0, startDate, endDate) ).append(",");
		sb.append(buildFunctionCall("AIV2_CUTOFFX", chainId, storeId, strDept , 0, startDate, endDate) ).append(",");
		sb.append(buildFunctionCall("DEPTH2", chainId, storeId, strDept , 0, startDate, endDate)).append(",");
		sb.append(buildFunctionCall("SALE_PCT2", chainId, storeId, strDept , numofItems, startDate, endDate)).append(",");
		sb.append(buildFunctionCall("REG_NET_PCT2", chainId, storeId, strDept , numofItems, startDate, endDate));
		sb.append(" FROM DUAL ");
		
		logger.debug(sb);
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "VariationAnalysisDAO - getAnalysisResultsForSchedule");
		
		//Populate the value object ...
		
		try{
		while (crs.next()){
			results.setAiv(crs.getFloat("AIV2"));
			results.setAivXPct(crs.getFloat("AIV2_CUTOFFX"));
			results.setDepth(crs.getFloat("DEPTH2"));
			
			//The reg values are in a comma separated format - RegNet,RegUp, RegDown 
			ArrayList<String> al  = PrestoUtil.csvStrToStrArray(crs.getString("REG_NET_PCT2"));
			//logger.debug( "Reg Value" + crs.getString("REG_NET"));
			if( al.size() == 3 ){
				results.setNetReg(Float.parseFloat(al.get(0)));
				results.setRegUp(Float.parseFloat(al.get(1)));
				results.setRegDown(Float.parseFloat(al.get(2)));
			}else{
				logger.info("Did not get the regular net, up and down values !!!");
			}
			
			results.setSale(crs.getFloat("SALE_PCT2"));

			results.setNumOfStores(1);
			results.setStoreID(storeId);
			results.setDeptID(deptID);
			results.setChainID(chainId);
			/*
			 * Now set in the calling program
			results.setWeekStartDate(crs.getString("START_DATE"));
			results.setWeekEndDate(crs.getString("END_DATE"));
			results.setAnalysisDate(crs.getString("STATUS_CHG_DATE"));
			*/
		}
		}catch (SQLException sqlce){
		throw new GeneralException("VariationAnalysisDAO - getAnalysisResultsForSchedule", sqlce);
		}
		return results;
	}
	
	
	private String buildFunctionCall( String functionName, int chainId, int storeId, String strDept, 
			int numberofItems, String startDate, String endDate){
		
		StringBuffer sb = new StringBuffer();
		sb.append(functionName).append("(");
		sb.append(chainId).append(", ");
		sb.append(storeId).append(", ");
		sb.append(strDept).append(", ");
		if( numberofItems > 0)
			sb.append(numberofItems).append(", ");
		sb.append("'").append(startDate).append("', ");
		sb.append("'").append(endDate).append("') ").append(functionName);
		
		return sb.toString();
		
			
	}
	
	
	
	
	public void saveAnalysisResults(Connection conn, VariationResultsDTO resultObj) throws GeneralException{
		//delete for the current period
		deleteVariationResults(conn, resultObj);
		StringBuffer sb;
							
		//insert Analysis results
		sb = new StringBuffer();
		sb.append(" INSERT INTO VARIATION_RESULTS ( RESULT_ID, CHAIN_ID, STR_ID, DEPT_ID, ");
		sb.append(" COMPARED_TO_CHAIN_ID, COMPARED_TO_STR_ID, COMPARED_TO_DEPT_ID, ");
		sb.append(" WEEK_START_DATE, WEEK_END_DATE, ANALYSIS_DATE, ");
		sb.append(" AIV, SALE_PCT, REG_NET_PCT, REG_UP_PCT, REG_DOWN_PCT, DEPTH_PCT, ");
		sb.append(" AIV_NOTEWORTHY, DEPTH_NOTEWORTHY, SALE_NOTEWORTHY, REG_NET_NOTEWORTHY, REG_UP_NOTEWORTHY, REG_DOWN_NOTEWORTHY, "); 
		sb.append(" BASELINE_ID, NO_OF_ITEMS, NO_OF_STORES )VALUES ( " );
		
		sb.append( " VARIATION_RESULTS_SEQ.NEXTVAL").append(',');
		sb.append( resultObj.getChainID()).append(',');
		if(  resultObj.getStoreID() > Constants.NULLID)
			sb.append( resultObj.getStoreID()).append(',');
		else
			sb.append( " NULL ").append(',');
		if(  resultObj.getDeptID() > Constants.NULLID)
			sb.append( resultObj.getDeptID()).append(',');
		else
			sb.append( " NULL ").append(',');
		if(  resultObj.getComparedToChainID() > Constants.NULLID)
			sb.append( resultObj.getComparedToChainID()).append(',');
		else
			sb.append( " NULL ").append(',');
		if(  resultObj.getComparedToStoreID() > Constants.NULLID)
			sb.append( resultObj.getComparedToStoreID()).append(',');
		else
			sb.append( " NULL ").append(',');
		if(  resultObj.getComparedToDeptID() > Constants.NULLID)
			sb.append( resultObj.getComparedToDeptID()).append(',');
		else
			sb.append( " NULL ").append(',');
		sb.append( " TO_DATE ( '"). append(resultObj.getWeekStartDate()).append("', 'MM/DD/YY' ),");
		sb.append( " TO_DATE ( '"). append(resultObj.getWeekEndDate()).append("', 'MM/DD/YY' ),");
		sb.append( " TO_DATE ( '"). append(resultObj.getAnalysisDate()).append("', 'MM/DD/YY' ),");

		//sb.append( " SYSDATE ").append(',');
		
		sb.append( PrestoUtil.round( resultObj.getAiv(), 2)).append(',');
		//sb.append( PrestoUtil.round( resultObj.getAivXPct(), 2)).append(',');
		sb.append( PrestoUtil.round( resultObj.getSale(), 2)).append(',');
		sb.append( PrestoUtil.round( resultObj.getNetReg(), 2)).append(',');
		sb.append( PrestoUtil.round( resultObj.getRegUp(), 2)).append(',');
		sb.append( PrestoUtil.round( resultObj.getRegDown(), 2)).append(',');
		sb.append( PrestoUtil.round( resultObj.getDepth(), 2)).append(',');
		sb.append("'").append(resultObj.getAivNoteworthyInd()).append("', ");
		sb.append("'").append(resultObj.getDepthNoteworthyInd()).append("', ");
		sb.append("'").append(resultObj.getSaleNoteworthyInd()).append("', ");
		sb.append("'").append(resultObj.getRegNoteworthyInd()).append("', ");
		sb.append("'").append(resultObj.getRegUpNoteworthyInd()).append("', ");
		sb.append("'").append(resultObj.getRegDownNoteworthyInd()).append("', ");
		if(  resultObj.getBaselineID() > Constants.NULLID)
			sb.append(resultObj.getBaselineID()).append(',');
		else
			sb.append( " NULL ").append(',');
		sb.append(resultObj.getNumOfItems()).append(',');
		sb.append(resultObj.getNumOfStores()).append(')');
		//logger.info(sb);
		PristineDBUtil.execute(conn, sb, "Variation Results DAO - insert Results record");
	

	}


	private void deleteVariationResults(Connection conn,
			VariationResultsDTO resultObj) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		
		sb.append(" DELETE FROM VARIATION_RESULTS WHERE ");
		sb.append(" CHAIN_ID = ");
		sb.append(resultObj.getChainID());
		sb.append(" AND STR_ID ");
		if ( resultObj.getStoreID() == Constants.NULLID)
			sb.append(" is NULL ");
		else 
			sb.append(" = ").append(resultObj.getStoreID());
		
		sb.append(" AND DEPT_ID ");
		if ( resultObj.getDeptID() == Constants.NULLID)
			sb.append(" is NULL ");
		else 
			sb.append(" = ").append(resultObj.getDeptID());
		
		
		sb.append(" AND COMPARED_TO_CHAIN_ID  ");
		if ( resultObj.getComparedToChainID() == Constants.NULLID)
			sb.append(" is NULL ");
		else 
			sb.append(" = ").append(resultObj.getComparedToChainID());
		
		sb.append(" AND COMPARED_TO_STR_ID ");
		if ( resultObj.getComparedToStoreID() == Constants.NULLID)
			sb.append(" is NULL ");
		else 
			sb.append(" = ").append(resultObj.getComparedToStoreID());
		
		sb.append(" AND COMPARED_TO_DEPT_ID ");
		if ( resultObj.getComparedToDeptID() == Constants.NULLID)
			sb.append(" is NULL ");
		else 
			sb.append(" = ").append(resultObj.getComparedToDeptID());
		
		
		sb.append(" AND WEEK_START_DATE >= TO_DATE('").append(resultObj.getWeekStartDate()).append("','MM/DD/YY') ");
		sb.append(" AND WEEK_END_DATE <= TO_DATE('").append(resultObj.getWeekEndDate()).append("','MM/DD/YY')");
		//logger.info(sb.toString());
		
		PristineDBUtil.execute(conn, sb, "Variation Baseline DAO - delete baseline record");
	}
	
	public ArrayList<VariationResultsDTO> getVariationResults(Connection conn, int baselineDays) throws
			GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" Select RESULT_ID, CHAIN_ID, STR_ID, NVL(DEPT_ID, -1) DEPT_ID, compared_to_chain_id, ");
		sb.append(" compared_to_str_id, NVL(compared_to_dept_id, -1) compared_to_dept_id, ");
		sb.append(" TO_CHAR(week_start_date, 'MM/DD/YY') WEEK_START_DATE, ");
		sb.append(" TO_CHAR(week_end_date, 'MM/DD/YY') WEEK_END_DATE, ");
		sb.append(" TO_CHAR(analysis_date, 'MM/DD/YY') analysis_date, ");
		sb.append(" AIV, sale_pct, reg_net_pct, depth_pct, no_of_items, no_of_stores ");
		sb.append(" from Variation_results  where chain_id = compared_to_chain_id and ");
		sb.append(" str_id = compared_to_str_id and (dept_id = compared_to_dept_id or ");
		sb.append(" (dept_id is null and compared_to_dept_id is null) ) ");
		sb.append(" AND analysis_date > sysdate - ");
		sb.append( baselineDays);
	
		//logger.info(sb);
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "VariationAnalysisDAO - getVariationAnalysisResults");
		ArrayList<VariationResultsDTO> variationResultsList = new ArrayList<VariationResultsDTO> ();
		try{
			while (crs.next()){
				VariationResultsDTO results = new VariationResultsDTO();
				results.setResultID(crs.getInt("RESULT_ID"));
				results.setChainID(crs.getInt("CHAIN_ID"));
				results.setStoreID(crs.getInt("STR_ID"));
				results.setDeptID(crs.getInt("DEPT_ID"));
				results.setComparedToChainID(crs.getInt("compared_to_chain_id"));
				results.setComparedToStoreID(crs.getInt("compared_to_str_id"));
				results.setComparedToDeptID(crs.getInt("compared_to_dept_id"));
				results.setWeekStartDate(crs.getString("WEEK_START_DATE"));
				results.setWeekEndDate(crs.getString("WEEK_END_DATE"));
				results.setAnalysisDate("analysis_date");
				results.setAiv(crs.getFloat("AIV"));
				results.setDepth(crs.getFloat("depth_pct"));
				results.setNetReg(crs.getFloat("reg_net_pct"));
				results.setSale(crs.getFloat("sale_pct"));
				results.setNumOfItems(crs.getInt("no_of_items"));
				results.setNumOfStores(crs.getInt("no_of_stores"));
				variationResultsList.add(results);
			}
		}catch (SQLException sqlce){
			throw new GeneralException("VariationAnalysisDAO - getAnalysisResultsForSchedule", sqlce);
		}
		return variationResultsList;

	}
	
	public String getBaseResult(Connection conn, String selectItem, String additionalWhereItem, 
			int chainId, int storeId, int deptId, String startDate, String endDate, boolean showFlavor)
			throws GeneralException
	
	{
		StringBuffer sb = new StringBuffer();
		
		sb.append(" SELECT ").append(selectItem);
		
		if( showFlavor )
			sb.append(" FROM COMPETITIVE_DATA_VIEW " );
		else
			sb.append(" FROM COMP_DATA_VIEW_NO_FLAVOR " );
		
		sb.append(" where item_code in (select item_code from VARIATION_BASE_ITEM_LIST where COMP_CHAIN_ID =");
		sb.append(chainId).append(")"); //
		sb.append(" and schedule_id in ");
		sb.append("( select schedule_id from schedule where start_date >= ");
		sb.append("TO_DATE('").append(startDate).append("','MM/DD/YY') ");
		sb.append(" and end_date <= " );
		sb.append(" TO_DATE('").append(endDate).append("','MM/DD/YY')");
		sb.append(" and comp_str_id =  " ).append(storeId).append(")");
		sb.append(" and item_not_found_flg = 'N' ");
		if( additionalWhereItem != null )
			sb.append(" AND ").append( additionalWhereItem );
		if( deptId > Constants.NULLID ){
			sb.append(" AND item_code in ( Select item_code from item_lookup ");
			sb.append(" where dept_id =").append(deptId).append(")");
		}
		//logger.info(sb.toString());
		
		return PristineDBUtil.getSingleColumnVal(conn, sb, "getBaseResult");
	}
	
	public String getAggregateLevelResult(Connection conn, String selectItem, String additionalWhereItem, 
			int chainId, int storeId, int deptId, 
			//int comparedToChainId, int comparedToStoreId, int comparedToDeptId,
			String startDate, String endDate)
			throws GeneralException
	
	{
		StringBuffer sb = new StringBuffer();
		
		sb.append(" SELECT ").append(selectItem);
		sb.append(" FROM VARIATION_RESULTS WHERE ");
		sb.append(" CHAIN_ID = ").append(chainId);
		if( storeId != Constants.NULLID)
			sb.append(" AND STR_ID = ").append(storeId);
		
		if( deptId == Constants.NULLID)
			sb.append(" AND DEPT_ID IS NULL");
		else
			sb.append(" AND DEPT_ID = ").append(deptId);
		//Compared to Clause
		sb.append(" AND ( ");
		sb.append(" ( COMPARED_TO_CHAIN_ID IS NULL AND COMPARED_TO_STR_ID IS NULL AND   COMPARED_TO_DEPT_ID IS NULL )");
		sb.append(" OR ( COMPARED_TO_CHAIN_ID = ").append(chainId);
		if( storeId != Constants.NULLID)
			sb.append(" AND COMPARED_TO_STR_ID = ").append(storeId);
		else
			sb.append(" AND COMPARED_TO_STR_ID = STR_ID");
		
		if( deptId == Constants.NULLID)
			sb.append(" AND COMPARED_TO_DEPT_ID IS NULL").append("))");
		else
			sb.append(" AND COMPARED_TO_DEPT_ID = ").append(deptId).append("))");
		
		sb.append(" AND WEEK_START_DATE >= TO_DATE('").append(startDate).append("','MM/DD/YY') ");
		sb.append(" AND WEEK_END_DATE <= TO_DATE('").append(endDate).append("','MM/DD/YY')");
		
		if( additionalWhereItem != null )
			sb.append(" AND ").append( additionalWhereItem );
		
		//logger.info(sb.toString());
		
		return PristineDBUtil.getSingleColumnVal(conn, sb, "getAggregateResult");
	}
}
