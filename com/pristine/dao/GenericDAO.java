/*
 * Author : Naimish Start Date : Jul 22, 2009
 * 
 * Change Description Changed By Date
 * --------------------------------------------------------------
 */

package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

//import org.apache.log4j.Logger;

import com.pristine.exception.GeneralException;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.dto.RuleDTO;
import com.pristine.dto.RuleDTOList;

public class GenericDAO
{
//	private static Logger	logger	= Logger.getLogger(GenericDAO.class);

	public static HashMap<String, String> getConfigSettings(Connection connection) throws GeneralException
	{
		HashMap<String, String> hashMap = new HashMap<String, String>();
		PreparedStatement prepraredStmt = null;
		ResultSet resultSet = null;

		try
		{
			// connection = DBManager.getOracleConnection();

			String sql = "Select * from config_setting where Setting_Group ='Suspect'";

			prepraredStmt = connection.prepareStatement(sql);

			resultSet = prepraredStmt.executeQuery();

			GenericUtil.logMessage("Config settings retrieved successfully");

			while (resultSet.next())
			{
				hashMap.put(resultSet.getString("key_name"), resultSet.getString("VALUE"));
			}
		}
		catch (Exception ex)
		{
			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet, prepraredStmt);
		}

		return hashMap;
	}

	public RuleDTOList getRules() throws GeneralException
	{
		RuleDTOList ruleList = new RuleDTOList();
		
		PreparedStatement prepraredStmt = null;
		ResultSet resultSet = null;
		Connection connection = null;

		try
		{
			connection = DBManager.getOracleConnection();

			String sql = "select * from rule_definition_lookup order by rule_id";

			prepraredStmt = connection.prepareStatement(sql);

			resultSet = prepraredStmt.executeQuery();

			GenericUtil.logMessage("Rules retrieved successfully");

			while (resultSet.next())
			{
				RuleDTO rule = new RuleDTO();
				rule.setId(resultSet.getInt("rule_id"));
				rule.setName(resultSet.getString("rule_name"));
				rule.setDescription(resultSet.getString("rule_desc"));
				boolean enabled = resultSet.getString("enabled").equals("Y") ? true : false;
				rule.setEnabled(enabled);
				rule.setFunctionalArea(resultSet.getString("functional_area"));
				
				ruleList.add (rule);
			}
		}
		catch (Exception ex)
		{
			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet, prepraredStmt, connection);
		}

		return ruleList;
	}
	
	public HashMap<Integer, ArrayList<Integer>> getRulesMapping() throws GeneralException
	{
		HashMap<Integer, ArrayList<Integer>> hashMap = new HashMap<Integer, ArrayList<Integer>>();
		PreparedStatement prepraredStmt = null;
		ResultSet resultSet = null;
		Connection connection = null;
		int lastID = 0;
		ArrayList<Integer> removalRules = new ArrayList<Integer>();

		try
		{
			connection = DBManager.getOracleConnection();

			String sql = "select * from rules_mapping order by detection_rule_id, removal_rule_id";

			prepraredStmt = connection.prepareStatement(sql);

			resultSet = prepraredStmt.executeQuery();

			GenericUtil.logMessage("Rules Mapping retrieved successfully");

			while (resultSet.next())
			{
				if (lastID == 0)
					lastID = resultSet.getInt("detection_rule_id");

				if (lastID != resultSet.getInt("detection_rule_id"))
				{
					removalRules = new ArrayList<Integer>();
					hashMap.put(lastID, removalRules);

					lastID = resultSet.getInt("detection_rule_id");

					//removalRules.clear();
				}
				
				removalRules.add(resultSet.getInt("removal_rule_id"));
			}

			if (lastID != 0)
				hashMap.put(lastID, removalRules);
		}
		catch (Exception ex)
		{
			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet, prepraredStmt, connection);
		}

		return hashMap;
	}
}