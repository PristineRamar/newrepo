package com.pristine.dataload;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dao.DBManager;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ItemCleanUp {
	
	private static Logger	logger	= Logger.getLogger("ItemCleanUp");

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		logger.info("Item cleanup analysis started ...");
		PropertyManager.initialize(ItemCleanUp.class.getClassLoader().getResourceAsStream(Constants.ANALYSIS_CONFIG_FILE));
		PropertyManager.initialize(ItemCleanUp.class.getClassLoader().getResourceAsStream(Constants.CONST_DB_FILENAME));
		
	
		try {
			
			ItemCleanUp cleanupItem = new ItemCleanUp();
			cleanupItem.removeDuplicateUPC();
		} catch (GeneralException ge){
			ge.logException(logger);
		}
		logger.info("Item cleanup analysis completed ...");
		
	}
	
	public void removeDuplicateUPC() throws GeneralException {
		Connection conn = null;
		try {
			conn = DBManager.getConnection();
			//get UPC
			CachedRowSet crs =  getDuplicateUPC(conn);
			logger.info("Duplicate UPC count is " + crs.size() );
			String currentUPC = null;
			String currentItemCode = null;
			int count = 0;
			while (crs.next()){
				
				String newUPC = crs.getString("UPC");
				String newItemCode = crs.getString("ITEM_CODE");
				if ( currentUPC == null ){
					currentUPC = newUPC;
					currentItemCode = newItemCode;
				}
				else if ( newUPC.equals(currentUPC) ){
					performItemCleanup( conn, currentItemCode, newItemCode);
				}else{
					currentUPC = newUPC;
					currentItemCode = newItemCode;
					//if ( count > 25) break;
				}
				count++;
				if( count%500 == 0)
					logger.info(count + " items processed...");
			}
		}catch ( SQLException e){
			throw new GeneralException( "Cached Rowset exception likely...",e);
		}		
		finally {
			
			if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE")){
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Data Load");
			}
			else{
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "Data Load");
			}
			logger.info("Data Load successfully completed");
			if( conn != null )
				PristineDBUtil.close(conn);
		}
	}
	
	public CachedRowSet getDuplicateUPC(Connection conn) throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append("select item_code, item_name, retailer_item_code, UPC, dept_id ");
		sb.append("from item_lookup where upc in ");
		sb.append("( select upc from item_lookup group by upc having count(upc)>1)"); 
		sb.append("order by UPC, item_code");

		logger.debug(sb);
		//execute the statement
		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "ITEMDAO - getItemListFromSchedule");
		return crs;
	}
	
	public void performItemCleanup( Connection conn, String keepItemCode, String removeItemCode) throws GeneralException {
	
		logger.debug ("performing cleanup ...");
		
		try {
			StringBuffer sb = new StringBuffer();
			sb.append("UPDATE COMPETITIVE_DATA SET item_code = ").append( keepItemCode);
			sb.append(" WHERE item_code = ").append( removeItemCode);
			int count = PristineDBUtil.executeUpdate(conn, sb, "update comp data");
			logger.debug ("Update comp data " + count + " rows affected");
			
			sb = new StringBuffer();
			sb.append("UPDATE PRICE_CHECK_LIST_ITEMS SET item_code = ").append( keepItemCode);
			sb.append(" WHERE item_code = ").append( removeItemCode);
			count = PristineDBUtil.executeUpdate(conn, sb, "update price check List items");
			logger.debug ("update price check List items " + count + " rows affected");
			
			sb = new StringBuffer();
			sb.append(" DELETE from ITEM_LOOKUP where item_code = ").append( removeItemCode);
			count = PristineDBUtil.executeUpdate(conn, sb, "delete item_lookup");
			logger.debug ("delete item_lookup " + count + " rows affected");
			PristineDBUtil.commitTransaction(conn, "Item cleanup");
		} catch( GeneralException e) {
			logger.error("Error with Item code - " + removeItemCode);
			logger.error("Exception in Item cleanup", e);
			PristineDBUtil.rollbackTransaction(conn, "Item cleanup");
		}
		
		//perform commit;
	}
}
