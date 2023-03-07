package com.pristine.dao.salesanalysis;

import java.sql.Connection;

import org.apache.log4j.Logger;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class DeleteAggregationDao {
	
	static Logger logger = Logger.getLogger("DeleteAggregationDao");
	
	 
	/*
	 * Method mainly used to get the backup records and insert the records into 
	 * backup table.
	 * Argument 1 : springTemplate
	 * Argument 2 : tableName
	 * Argument 3 : locationLevelId
	 * Argument 4 : productLevelId
	 * Argument 5 : calendarId
	 * @catch Exception
	 * @throws gendralException
	 */
	
	public void backupProcess(Connection _conn, String tableName,
			int locationLevelId, int productLevelId, int calendarId)
			throws GeneralException {

		// get the data from the table and insert the data into
		// backup table

		try {

			logger.debug("Insert the values into:"
					+ tableName.toUpperCase() + "_ARC");
			StringBuffer sql = new StringBuffer();
			String insertIntoArchive = PropertyManager.getProperty("DELETEAGGREGATION.INSERT_INTO_ARCHIVE");
			
			/*if (tableName.equalsIgnoreCase("SALES_AGGR_DAILY")
					|| tableName.equalsIgnoreCase("SALES_AGGR_DAILY_ROLLUP")) {

				sql = new StringBuffer();

				logger.debug("Calendar to Date Delete Process Begins (CTD)");
				
				if(!"NO".equalsIgnoreCase(insertIntoArchive)){
					sql.append(" insert into SALES_AGGR_CTD_ARC");
					sql.append(" (select * from SALES_AGGR_CTD where SUMMARY_CTD_ID");
					sql.append("  in(select SUMMARY_CTD_ID from " + tableName + " ");
					sql.append(" where CALENDAR_ID=" + calendarId
							+ " and LOCATION_LEVEL_ID=" + locationLevelId + "");
					sql.append("  and PRODUCT_LEVEL_ID=" + productLevelId + "))");
	
					logger.debug("backupProcess insert SQL:" + sql.toString());
	
					PristineDBUtil.execute(_conn, sql, "backupProcess");
	
					sql = new StringBuffer();
				}

				sql.append(" delete from sales_aggr_ctd where summary_ctd_id");
				sql.append(" in(select summary_ctd_id from " + tableName + "");
				sql.append(" where CALENDAR_ID=" + calendarId
						+ " and LOCATION_LEVEL_ID=" + locationLevelId + "");
				sql.append("  and PRODUCT_LEVEL_ID=" + productLevelId + ")");

				logger.debug("backupProcess delete SQL:" + sql.toString());

				PristineDBUtil.execute(_conn, sql, "backupProcess");

			}*/

			sql = new StringBuffer();
			
			if(!"NO".equalsIgnoreCase(insertIntoArchive)){
				sql.append(" insert into " + tableName.toUpperCase() + "_ARC");
				sql.append("  (select * from " + tableName.toUpperCase() + "");
				sql.append(" where LOCATION_LEVEL_ID=" + locationLevelId + "");
				sql.append(" and PRODUCT_LEVEL_ID =" + productLevelId + "");
				sql.append(" and CALENDAR_ID=" + calendarId + ")");
	
				logger.debug("backupProcess insert SQL:" + sql.toString());
	
				PristineDBUtil.execute(_conn, sql, "backupProcess");
	
				sql = new StringBuffer();
			}

			sql.append(" delete from " + tableName.toUpperCase() + "");
			sql.append(" where LOCATION_LEVEL_ID=" + locationLevelId + "");
			sql.append(" and PRODUCT_LEVEL_ID =" + productLevelId + "");
			sql.append(" and CALENDAR_ID=" + calendarId + "");

			logger.debug("backupProcess delete SQL:" + sql.toString());
			PristineDBUtil.execute(_conn, sql, "backupProcess");
		} catch (GeneralException e) {

			throw new GeneralException(" Error in Dao....", e);

		}

	}
}