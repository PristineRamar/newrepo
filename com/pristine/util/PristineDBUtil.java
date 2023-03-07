
package com.pristine.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.exception.GeneralException;
import com.sun.rowset.CachedRowSetImpl;

public class PristineDBUtil
{
	static Logger	logger	= Logger.getLogger("com.pristine.util.PristineDBUtil");

	public static CachedRowSet getCachedRowSet(ResultSet s) throws SQLException
	{
		CachedRowSet crs = new CachedRowSetImpl();
		crs.populate(s);
		s.close();
		return crs;
	}

	public static CachedRowSet executeQuery(Connection conn, StringBuffer query, String method) throws GeneralException
	{
		CachedRowSet crs = null;
		Statement dbStatement = null;
		ResultSet rs = null;
		try
		{
			dbStatement = conn.createStatement();
			rs = dbStatement.executeQuery(query.toString());
			crs = getCachedRowSet(rs);

		}
		catch (SQLException sqle)
		{
			logger.error("Query in error : " + query);
				throw new GeneralException(method, sqle);
		}
		finally
		{
			try
			{
				dbStatement.close();
			}
			catch (SQLException sqle)
			{
				sqle.printStackTrace();
				throw new GeneralException(method, sqle);
			}
		}
		return crs;
	}

	public static void commitTransaction(Connection conn, String msg) throws GeneralException
	{
		try
		{
			conn.commit();
		}
		catch (SQLException sqle)
		{
			throw new GeneralException(msg, sqle);
		}
	}

	public static void rollbackTransaction(Connection conn, String msg)
	{
		try
		{
			conn.rollback();
		}
		catch (SQLException sqle)
		{
		}
	}

	public static void execute(Connection conn, StringBuffer query, String method) throws GeneralException
	{
		Statement dbStatement = null;
		try
		{
			dbStatement = conn.createStatement();
			dbStatement.execute(query.toString());
		}
		catch (SQLException sqle)
		{
			 logger.debug(query);
			throw new GeneralException(method, sqle);
		}
		finally
		{
			try
			{
				dbStatement.close();
			}
			catch (SQLException sqle)
			{
				throw new GeneralException(method, sqle);
			}
		}
	}

	public static int executeUpdate(Connection conn, StringBuffer query, String method) throws GeneralException
	{
		Statement dbStatement = null;
		int count = -1;
		try
		{
			dbStatement = conn.createStatement();
			count = dbStatement.executeUpdate(query.toString());
		}
		catch (SQLException sqle)
		{
			throw new GeneralException(method, sqle);
		}
		finally
		{
			try
			{
				dbStatement.close();
			}
			catch (SQLException sqle)
			{
				throw new GeneralException(method, sqle);
			}
		}
		return count;
	}

	// Added by Naimish
	public static final void close(Connection connection)
	{
		if (connection != null)
			try
			{
				connection.close();
				 connection = null;
			}
			catch (SQLException e)
			{
				logger.error("Exception in closing connection object", e);
			}

	}

	public static final void close(Statement statement)
	{

		if (statement != null)
			try
			{
				statement.close();
				statement = null;
			}
			catch (SQLException e)
			{
				logger.error("Error in closing Statement Object", e);
			}

	}

	public static final void close(ResultSet rs)
	{

		if (rs != null)
			try
			{
				rs.close();
				rs = null;
			}
			catch (SQLException e)
			{
				logger.error("Error in closing ResultSrt Object", e);
			}

	}

	public static final void close(ResultSet rs, Statement statement, Connection connection)
	{
		close(rs);
		close(statement);
		close(connection);
	}

	public static final void close(Statement statement, Connection connection)
	{
		close(statement);
		close(connection);
	}

	public static final void close(ResultSet rs, Statement statement)
	{
		close(rs);
		close(statement);
	}

	/* New method added */
	public static void close(Connection conn, String method) throws GeneralException
	{
		try
		{
			conn.close();
		}
		catch (SQLException sqle)
		{
			throw new GeneralException(method, sqle);
		}
	}
	
	/* New method added */
	public static String getSingleColumnVal(Connection conn,  StringBuffer query, String method) throws GeneralException {
        String value = null;
		try{
        	CachedRowSet crs =  executeQuery(conn, query, method);
        	if( crs.next()){
        		value = crs.getString(1);
        	}
        }catch(SQLException sqle){
        	throw new GeneralException (method, sqle);
        }
        return value;
	}
	
	public static void commitAndTerminate(Connection conn, Logger myLog, String procedure){
		try {
			if (PropertyManager.getProperty("DATALOAD.COMMIT", "")
					.equalsIgnoreCase("TRUE")) {
				myLog.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, procedure);
			} else {
				myLog.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn,procedure );
			}
		} catch (GeneralException ge) {
			logger.error("Error in commit", ge);
			System.exit(1);
		} finally {
			PristineDBUtil.close(conn);
		}
	}

	//Append for Insert
	public static void append(StringBuffer sb, String value) {
		
		PristineDBUtil.appendNoComa(sb, value);
		sb.append(',');
		
	}
	
	//Append for Insert
	public static void append(StringBuffer sb, int value) {
		if( value > 0 )
			sb.append(value).append(",");
		else
			sb.append("NULL, ");
	}

	public static void appendDate(StringBuffer sb, String date) {
		if( date != null )
			date = date.trim();
				
		if( date != null && !date.equals("")){
			sb.append("TO_DATE(' ").append(date).append("',");
			if( date.length() == 8)
				sb.append("'MM/DD/YY'),"); 
			else
				sb.append("'MM/DD/YYYY'),");	
		}else{
			sb.append("NULL, ");
		}	
		
	}

	public static void appendNoComa(StringBuffer sb, String value) {
		if( value != null )
			value = value.trim();
		
		if( value != null && !value.equals("")){
			value = value.replaceAll("\"", "");
			value = value.replaceAll("'", "''");
			sb.append('\'').append(value).append("'");
		}else{
			sb.append("NULL ");
		}		
	}

	public static void appendUpdate(StringBuffer sb, String column, String value) {
		sb.append(column).append(" = ");
		PristineDBUtil.append(sb,value);
	}
	public static void appendUpdate(StringBuffer sb, String column, int value) {
		sb.append(column).append(" = ");
		PristineDBUtil.append(sb,value);
		
	}

	public static void appendUpdateDate(StringBuffer sb, String column, String date) {
		sb.append(column).append(" = ");
		PristineDBUtil.appendDate(sb,date);		
	}
	
	/**
	 * This method returns a String with specified number of bind parameters
	 * @param length	Number of bind parameters
	 * @return String with specified number of bind parameters	
	 */
	public static String preparePlaceHolders(int length) {
	    StringBuilder builder = new StringBuilder();
	    for (int i = 0; i < length;) {
	        builder.append("?");
	        if (++i < length) {
	            builder.append(",");
	        }
	    }
	    return builder.toString();
	}

	/**
	 * This method is used when there are no other bind parameters to be set in Prepared Statement
	 * other than the ones present in the array.
	 * @param preparedStatement 	PreparedStatement in which values need to be set
	 * @param values				Array of Values that needs to be set in the Prepared Statement
	 * @throws SQLException
	 */
	public static void setValues(PreparedStatement preparedStatement, Object... values) throws SQLException {
	    for (int i = 0; i < values.length; i++) {
	        preparedStatement.setObject(i + 1, values[i]);
	    }
	}
	
	/**
	 * This method is used when there are other bind parameters to be set in Prepared Statement
	 * other than the ones present in the array.
	 * @param preparedStatement		PreparedStatement in which values need to be set
	 * @param startCount			Count starting which the parameters needs to be bound to the prepared statement
	 * @param values				Array of Values that needs to be set in the Prepared Statement
	 * @throws SQLException
	 */
	public static void setValues(PreparedStatement preparedStatement, int startCount, Object... values) throws SQLException {
	    for (int i = 0; i < values.length ; i++) {
	        preparedStatement.setObject(i + startCount , values[i]);
	    }
	}
	
	public static int getUpdateCount(int[] count) {
		int updateCount = 0; 
		for(int i = 0; i < count.length; i++){
			//logger.debug("Update Status: " + count[i]);
			if( count[i] > 0)
				updateCount += count[i];
		}
		return updateCount;
	}
	
	
	/**
	 * 
	 * @param colCount
	 * @param value
	 * @param statement
	 * @throws SQLException
	 */
	public static void setInt(int colCount, Integer value, PreparedStatement statement) throws SQLException{
		if(value == null){
			statement.setNull(colCount, Types.INTEGER);
		}else{
			statement.setInt(colCount, value);
		}
	}
	
	/**
	 * 
	 * @param colCount
	 * @param value
	 * @param statement
	 * @throws SQLException
	 */
	public static void setDouble(int colCount, Double value, PreparedStatement statement) throws SQLException{
		if(value == null){
			statement.setNull(colCount, Types.DOUBLE);
		}else{
			statement.setDouble(colCount, value);
		}
	}
	
	/**
	 * 
	 * @param colCount
	 * @param value
	 * @param statement
	 * @throws SQLException
	 */
	public static void setString(int colCount, String value, PreparedStatement statement) throws SQLException{
		if(value == null){
			statement.setNull(colCount, Types.VARCHAR);
		}else{
			statement.setString(colCount, value);
		}
	}
	
	/**
	 * Common method to replace retail calendar table name
	 * @param query
	 * @return
	 */
	public static String replaceCalendarName(String query, String exprToBeReplaced) {
		String replacedQuery = null;
		String calType = PropertyManager.getProperty("RETAIL_CALENDAR_TYPE", Constants.RETAIL_CALENDAR_BUSINESS);
		if (calType.equals(Constants.RETAIL_CALENDAR_PROMO)) {
			replacedQuery = query.replaceAll(exprToBeReplaced, "RETAIL_CALENDAR_PROMO");
		} else {
			replacedQuery = query.replaceAll(exprToBeReplaced, "RETAIL_CALENDAR");
		}

		return replacedQuery;
	}
	
	/**
	 * 
	 * @param colCount
	 * @param value
	 * @param statement
	 * @throws SQLException
	 */
	public static void setMultiplePriceQty(int colCount, MultiplePrice value, PreparedStatement statement) throws SQLException{
		if(value == null){
			statement.setNull(colCount, Types.INTEGER);
		}else{
			statement.setInt(colCount, value.multiple);
		}
	}
	
	/**
	 * 
	 * @param colCount
	 * @param value
	 * @param statement
	 * @throws SQLException
	 */
	public static void setMultiplePrice(int colCount, MultiplePrice value, PreparedStatement statement) throws SQLException{
		if(value == null){
			statement.setNull(colCount, Types.DOUBLE);
		}else{
			statement.setDouble(colCount, value.price);
		}
	}
}
