/*
 * Title: Summary Goal DAO
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	27/10/2011	John Britto		New Creation
 *******************************************************************************
 */
package com.pristine.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.SummaryGoalDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class SummaryGoalDAO implements IDAO {
	

	private static Logger logger = Logger
			.getLogger("SummaryGoalWeeklyDAO");
	
	/*
	 *****************************************************************
	 * Method to delete the goal data
	 * Argument 1 : DB connection
	 * Argument 2 : Summary Goal DTO
	 * Return	  : void
	 * @throws Exception
	 * ****************************************************************
	 */		
	public ArrayList<SummaryGoalDTO> getSummaryGoalData(Connection conn, 
			int levelTypeId, String StoreIds, 
			HashMap<String, Integer>  weekEndMap) throws GeneralException {

		ArrayList<SummaryGoalDTO> listSummaryGoal = new ArrayList<SummaryGoalDTO>();
		
		StringBuffer sb = new StringBuffer(); 
		sb.append("SELECT TO_CHAR(WEEK_END_DATE, 'MM/DD/YYYY') AS WEEK_END_DATE");
		sb.append(", SUM(BUDGET) as BUDGET, SUM(FORECAST) as FORECAST");		
		sb.append(" FROM SUMMARY_GOAL_WEEKLY");
		sb.append(" WHERE LEVEL_ID in (").append(StoreIds) .append(")");
		sb.append(" AND LEVEL_TYPE_ID = ").append(levelTypeId) ;
		if (weekEndMap.size()>0) {
			sb.append(" AND WEEK_END_DATE IN ( ");
			int count = 0;
			for (String key : weekEndMap.keySet()) {
				if (count>0) sb.append(" ,");
				sb.append(String.format("to_date('%1s','MM/DD/YYYY')", key));
				count = count + 1;
			}
			sb.append(")");
		}
		sb.append(" GROUP BY  WEEK_END_DATE");
		//logger.debug(sb.toString());
		
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, 
													"getSummaryGoalData");

			while (result.next()) {
				SummaryGoalDTO objGoalDTO = new SummaryGoalDTO();
				objGoalDTO.setWeekEndDate(result.getString("WEEK_END_DATE"));
				objGoalDTO.setBudget(result.getDouble("BUDGET"));
				objGoalDTO.setForecast(result.getDouble("FORECAST"));
				listSummaryGoal.add(objGoalDTO);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		
		return listSummaryGoal;
	}
	
	/*
	 *****************************************************************
	 * Method to delete the goal data
	 * Argument 1 : DB connection
	 * Argument 2 : Summary Goal DTO
	 * Return	  : void
	 * @throws Exception
	 * ****************************************************************
	 */	
	public void deleteSummaryGoal(Connection conn, SummaryGoalDTO summaryDTO) 
													throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" DELETE FROM SUMMARY_GOAL_WEEKLY WHERE WEEK_END_DATE = ");
		sb.append(String.format("to_date('%1s','MM/DD/YYYY') ", summaryDTO.getWeekEndDate()));
		sb.append("	AND LEVEL_TYPE_ID = ").append(summaryDTO.getLevelTypeID());
		sb.append("	AND LEVEL_ID = ").append(summaryDTO.getLevelId());
		//logger.debug(sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "deleteSummaryGoal");
			//logger.debug("DELETE");	
		} 
		catch (GeneralException e) {
			logger.error(e);
		}
	}
	
	
	/*
	 *****************************************************************
	 * Method to insert goal data
	 * Argument 1 : DB connection
	 * Argument 2 : Summary Goal DTO
	 * Return	  : void
	 * @throws Exception
	 * ****************************************************************
	 */	
	public void insertSummaryGoal(Connection conn, SummaryGoalDTO summaryDTO) 
													throws GeneralException {

		StringBuffer sb = new StringBuffer();

		sb.append(" INSERT INTO SUMMARY_GOAL_WEEKLY(WEEK_END_DATE,");
		sb.append(" LEVEL_TYPE_ID, LEVEL_ID, BUDGET, FORECAST ) VALUES(");
		sb.append(String.format("to_date('%1s','MM/DD/YYYY'),", summaryDTO.getWeekEndDate()));
		sb.append(summaryDTO.getLevelTypeID());
		sb.append(", ").append(summaryDTO.getLevelId());
		
		//Check for null value
		if (summaryDTO.getBudget()==-99) {
			sb.append(", ").append("null");
		}
		else {
			sb.append(", ").append(summaryDTO.getBudget());
		}
		
		//Check for null value
		if (summaryDTO.getForecast()==-99) {
			sb.append(", ").append("null");
		}
		else {
			sb.append(", ").append(summaryDTO.getForecast());
		}
		
		sb.append(" )");
		//logger.debug(sb.toString());
		
		try {
			PristineDBUtil.execute(conn, sb, "insertSummaryGoal");
		} 
		catch (GeneralException e) {
			logger.error(e);
		}
	}
}
