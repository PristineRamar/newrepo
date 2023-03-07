package com.pristine.dao;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class ItemLoaderDAO {

	/**
	 * 
	 * @param conn
	 * @param keyColumn
	 * @param valueColumn
	 * @param tableName
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getTwoColumnsCache(Connection conn, String valueColumn, String keyColumn,
			String tableName, boolean convertToUppercase) throws GeneralException {

		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> getTableValues = new HashMap<String, Integer>();
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		sb.append(keyColumn);
		sb.append(", ");
		sb.append(valueColumn);
		sb.append(" FROM ");
		sb.append(tableName);

		try {
			String sql = sb.toString();
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			while (rs.next()) {
				if(convertToUppercase){
					getTableValues.put(rs.getString(keyColumn).trim().toUpperCase(), rs.getInt(valueColumn));
				}
				else{
					getTableValues.put(rs.getString(keyColumn), rs.getInt(valueColumn));
				}
			}

		} catch (Exception e) {
			throw new GeneralException("Exception in Getting key/value in getTwoColumnsCache()" + tableName, e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return getTableValues;

	}

	/**
	 * Three column names can be given to get out hash map values where key is
	 * an Object
	 * 
	 * @param conn
	 * @param returnObject
	 * @param keyColumn1
	 * @param keyColumn2
	 * @param valueColumn
	 * @param tableName
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getThreeColumnsCache(Connection conn,  String valueColumn,
			String keyColumn1, String keyColumn2, String tableName) throws GeneralException {

		PreparedStatement stmt = null;
		ResultSet rs = null;

		HashMap<String, Integer> getTableValues = new HashMap<String, Integer>();
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		sb.append(keyColumn1);
		sb.append(", ");
		sb.append(keyColumn2);
		sb.append(", ");
		sb.append(valueColumn);
		sb.append(" FROM ");
		sb.append(tableName);

		try {
			stmt = conn.prepareStatement(sb.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				//Forming key value by combining two values as a string
				String keyValue = rs.getString(keyColumn1) + "-" + rs.getString(keyColumn2);
				getTableValues.put(keyValue, rs.getInt(valueColumn));
			}

		} catch (Exception e) {
			throw new GeneralException("Exception in Getting key/value in getThreeColumnsCache()" + tableName, e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return getTableValues;

	}
}
