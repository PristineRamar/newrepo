package com.pristine.dao.analytics;

import com.pristine.util.Constants;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
//import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.dao.IDAO;
import com.pristine.dto.StoreDTO;
import com.pristine.dao.analytics.SubstitutePair;

public class SubstitutesDAO implements IDAO {

	private static final int PRODUCT_LEVEL_ID_LIG_OR_ITEM = 111;
	
	private Properties properties = new Properties();
	private static Logger	logger	= Logger.getLogger("SubstitutesDAO");
	private static String nl = System.lineSeparator();	

	private String storeIdsString = "";

	public Connection conn; 
	public String batchId;
	
	public String analysisEndDate = null;
	private int locationLevelId = -1;
	private int locationId = -1;
	public String locationsSQLCondition = null;
	
	public int brandId = -1;	

	public String tLog = null;
	public String tmpTablespace = null;
	public String eligibilitySalePeriod = null; 
	public String eligibilitySaleQuantity = null;
	public String eligibilitySaleDays = null;
	
	public SubstitutesDAO() {
		super();
		String sqlFileName = "sql/SQLDefinitions.xml";
		 InputStream is = this.getClass().getClassLoader().getResourceAsStream(sqlFileName);
		 try {
			 // TODO: Is it better to put the sql in the xml?
			properties.loadFromXML(is);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setProperties(int substituteLevelId)
	{
		tLog = PropertyManager.getProperty("SUBS_TLOG_SOURCE_TABLE");
		logger.info("setProperties() - Properties Info: SUBS_TLOG_SOURCE_TABLE=" + tLog);
		tmpTablespace = PropertyManager.getProperty("SUBS_TMP_STORAGE_TABLESPACE");
		
	    if (substituteLevelId == Constants.ITEMLEVELID) // item
	    {
	    	eligibilitySalePeriod = PropertyManager.getProperty("SUBS_PERIOD_DAYS_BEFORE_FOR_ITEM"); 
	    	eligibilitySaleDays = PropertyManager.getProperty("SUBS_MINIMUM_MOVED_DAYS_FOR_ITEM");
	    	eligibilitySaleQuantity = PropertyManager.getProperty("SUBS_MINIMUM_MOVED_QUANTITY_FOR_ITEM");
		    logger.info("setProperties() - Properties Info: SUBS_PERIOD_DAYS_BEFORE_FOR_ITEM=" + eligibilitySalePeriod);
		    logger.info("setProperties() - Properties Info: SUBS_MINIMUM_MOVED_DAYS_FOR_ITEM=" + eligibilitySaleDays);
		    logger.info("setProperties() - Properties Info: SUBS_MINIMUM_MOVED_QUANTITY_FOR_ITEM=" + eligibilitySaleQuantity);
	    }
	    else if (substituteLevelId == Constants.PRODUCT_LEVEL_ID_LIG) //lig
	    {
	    	eligibilitySalePeriod = PropertyManager.getProperty("SUBS_PERIOD_DAYS_BEFORE_FOR_LIR"); 
	    	eligibilitySaleDays = PropertyManager.getProperty("SUBS_MINIMUM_MOVED_DAYS_FOR_LIR");
	    	eligibilitySaleQuantity = PropertyManager.getProperty("SUBS_MINIMUM_MOVED_QUANTITY_FOR_LIR");
		    logger.info("setProperties() - Properties Info: SUBS_PERIOD_DAYS_BEFORE_FOR_LIG=" + eligibilitySalePeriod);
		    logger.info("setProperties() - Properties Info: SUBS_MINIMUM_MOVED_DAYS_FOR_LIG=" + eligibilitySaleDays);
		    logger.info("setProperties() - Properties Info: SUBS_MINIMUM_MOVED_QUANTITY_FOR_LIG=" + eligibilitySaleQuantity);
	    }		    
	    else if (substituteLevelId == PRODUCT_LEVEL_ID_LIG_OR_ITEM) //lig or item
	    {
	    	eligibilitySalePeriod = PropertyManager.getProperty("SUBS_PERIOD_DAYS_BEFORE_FOR_LIR_OR_ITEM"); 
	    	eligibilitySaleDays = PropertyManager.getProperty("SUBS_MINIMUM_MOVED_DAYS_FOR_LIR_OR_ITEM");
	    	eligibilitySaleQuantity = PropertyManager.getProperty("SUBS_MINIMUM_MOVED_QUANTITY_FOR_LIR_OR_ITEM");
		    logger.info("setProperties() - Properties Info: SUBS_PERIOD_DAYS_BEFORE_FOR_LIG_OR_ITEM=" + eligibilitySalePeriod);
		    logger.info("setProperties() - Properties Info: SUBS_MINIMUM_MOVED_DAYS_FOR_LIG_OR_ITEM=" + eligibilitySaleDays);
		    logger.info("setProperties() - Properties Info: SUBS_MINIMUM_MOVED_QUANTITY_FOR_LIG_OR_ITEM=" + eligibilitySaleQuantity);
	    }		    
	    logger.info("setProperties() - Properties Info: SUBS_ANALYSIS_PERIOD_DAYS=" + PropertyManager.getProperty("SUBS_ANALYSIS_PERIOD_DAYS"));
	    logger.info("setProperties() - Properties Info: SUBS_FILTER_OVERALL_SCORE_LOWER_LIMIT=" + PropertyManager.getProperty("SUBS_FILTER_OVERALL_SCORE_LOWER_LIMIT"));
	    logger.info("setProperties() - Properties Info: SUBS_SUBSTITUTE_BRAND=" + PropertyManager.getProperty("SUBS_SUBSTITUTE_BRAND"));
	}
	
	
	/**
	 * Sets the sql string condition for comparing the stores   
	 * 
	 * @param locationLevelId - division level id or store level id 
	 * @param locationId - division level id or store level id
	 * @return 
	 * @throws
	 */
	public void setLocation(int plocationLevelId, int plocationId)
	{
		locationLevelId = plocationLevelId; 
		locationId = plocationId;
		
		//TODO Needs modification as to how the different level Ids be handled.
	    if (locationLevelId == Constants.DIVISION_LEVEL_ID) {
	    	locationsSQLCondition = " in ( " + getStoreIdsString(locationId) + " )";
	    }
	    else if (locationLevelId == Constants.ZONE_LEVEL_ID) {
	    	locationsSQLCondition = " in ( " + getStoreIdsString(locationId) + " )";
	    }
	    else if (locationLevelId == Constants.STORE_LEVEL_ID) {
	    	locationsSQLCondition = " = " + locationId;
	    }
	    else if (locationLevelId == Constants.lOCATION_LEVEL_ID_MARKETAREA) {
	    	locationsSQLCondition = " in ( " + getStoreIdsString(locationId) + " )";
	    }
	}	

	/**
	 * Gets the list of categories for processing   
	 * 
	 * @param conn
	 * @return List<Integer> - List of categories having active items
	 * @throws 
	 */
	public List<Integer> getEligibleCategories(int brandId, String categoryIds)
	{
		StringBuffer sql = new StringBuffer();
		List<Integer> categories = new ArrayList<Integer>();

		try
		{

		    sql.append("SELECT DISTINCT category.id " + 
					"FROM category INNER JOIN item_lookup ON category.id = item_lookup.category_id " +  
					"WHERE item_lookup.active_indicator = 'Y'");
		    
		    if (categoryIds != null) {
		    	sql.append(" AND item_lookup.category_id IN (" + categoryIds + ")");
		    }
			
		    if (brandId > 0) {
		    	//???
		    	/* sql.append(" AND item_lookup.sub_category_id IN" +
		        " ( SELECT DISTINCT sub_category_id" + */
		    	sql.append(" AND item_lookup.category_id IN" +
		        " ( SELECT DISTINCT category_id" +
		    	" FROM item_lookup " +
		            " WHERE brand_id = " + brandId + ")");
		    }
			
		    CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getEligibleCategories() - Get eligible segments.");		
			
			while ( result.next() ) {
				categories.add(result.getInt("id"));
			}
			
			return categories;
		}
		catch (GeneralException e) {
			logger.error("getEligibleCategories() - Exception=" + e.getMessage());
			e.printStackTrace();
			return categories;
		}
		catch (SQLException e) {
			logger.error("getEligibleCategories() - Exception=" + e.getMessage());
			e.printStackTrace();
			return categories;
		}
		
	}

	
	/**
	 * Gets the Brand Id corresponding to Brand Name from database   
	 * 
	 * @param conn
	 * @param brandName - Brand Name for which Brand Id to be fetched
	 * @return Brand Id - Brand Id corresponding to Brand Id
	 * @throws 
	 */
	public int getBrandId(String brandName)
	{
		StringBuffer sql = new StringBuffer();
		int brandId = -1;

		try
		{
		    sql.append("SELECT brand_id FROM brand_lookup WHERE brand_name = '" + brandName + "'");

			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getBrandId() - Get Brand Id.");		
			while ( result.next() ) {
				brandId = result.getInt("brand_id");
			}
			
			return brandId;
		}
		catch (GeneralException e) {
			logger.error("getBrandId() - Exception=" + e.getMessage());
			e.printStackTrace();
			return brandId;
		}
		catch (SQLException e) {
			logger.error("getBrandId() - Exception=" + e.getMessage());
			e.printStackTrace();
			return brandId;
		}
		
	}
	
	/**
	 * Analyze, Filter and Store the substitutes based on input parameters.   
	 * 
	 * @param conn
	 * @param substituteLevelId: Product level at which analysis is performed.
	 * @param itemFilter: Additional condition that filters the transactions for analysis.
	 * 
	 * @return List<Integer> - List of segments having active items
	 * @throws 
	 */
	public boolean prepareIneligibleCustomerIds(int substituteLevelId) throws GeneralException
	{
		
	    try
	    {
		    StringBuffer sql = new StringBuffer();
		    
		    //sql.append("drop table TMP_SUBS_OUTLIER_CUSTOMER_IDS");
		    //executeIfPossible(conn, sql, "Substitutes DAO - Drop table TMP_SUBS_OUTLIER_CUSTOMER_IDS.");
		    
		    //conn.commit();

		    //sql.delete(0, sql .length());
		    sql.append("INSERT INTO TMP_SUBS_OUTLIER_CUSTOMER_IDS (" + nl +
		    			" SELECT customer_id, (" + Integer.parseInt(PropertyManager.getProperty("SUBS_ANALYSIS_PERIOD_DAYS")) + " * 95) net_amt_limit, " +
		    			"   sum(net_amt) net_amt, " +
	    				"   (" + Integer.parseInt(PropertyManager.getProperty("SUBS_ANALYSIS_PERIOD_DAYS")) + " * 4) trx_cnt_limit, count(distinct trx_no) trx_cnt" +
						" FROM " + tLog + nl+
						" WHERE calendar_id in  " +
						"     ( SELECT calendar_id FROM retail_calendar " +
						"         WHERE start_date > to_date('" + analysisEndDate + "', 'MM/dd/yyyy') - " + Integer.parseInt(PropertyManager.getProperty("SUBS_ANALYSIS_PERIOD_DAYS")) +
						"           AND start_date <= to_date('" + analysisEndDate + "', 'MM/dd/yyyy') AND row_type = 'D' ) " +
						"   AND store_id " + locationsSQLCondition +
						" group by customer_id " +
						" having sum(net_amt)         > (" + Integer.parseInt(PropertyManager.getProperty("SUBS_ANALYSIS_PERIOD_DAYS")) + " * 95) " +
						"     or count(distinct trx_no) > (" + Integer.parseInt(PropertyManager.getProperty("SUBS_ANALYSIS_PERIOD_DAYS")) + " *  4) )"
		    );
	    	
		    logger.info("prepareIneligibleCustomerIds() - Preparing and storing in-eligible customers..");
	    	PristineDBUtil.execute(conn, sql, "Substitutes DAO - Create table TMP_SUBS_OUTLIER_CUSTOMER_IDS.");
	    	
	    	conn.commit();
	    	
	    	return true;
	    }
	    catch (GeneralException e) {
	    	logger.error("prepareIneligibleCustomerIds() - GeneralException=" + e.getMessage());
			e.printStackTrace();
			throw new GeneralException("Failed to prepare ineligible customers data!");
	    	//return false;
	    }
		catch (SQLException e) {
			logger.error("prepareIneligibleCustomerIds() - SQLException=" + e.getMessage());
			e.printStackTrace();
			throw new GeneralException("Failed to prepare ineligible customers data!");
			//return false;
	    }
		catch (Exception e) {
			logger.error("prepareIneligibleCustomerIds() - Exception=" + e.getMessage());
			e.printStackTrace();
			throw new GeneralException("Failed to prepare ineligible customers data!");
			//return false;
	    }
	}    

	/**
	 * Analyze, Filter and Store the substitutes based on input parameters.   
	 * 
	 * @param conn
	 * @param substituteLevelId: Product level at which analysis is performed.
	 * @param itemFilter: Additional condition that filters the transactions for analysis.
	 * 
	 * @return List<Integer> - List of segments having active items
	 * @throws 
	 */
	public boolean prepareEligibleProductsData(int substituteLevelId, String itemFilter) throws GeneralException
	{
		
	    try
	    {
		    StringBuffer sql = new StringBuffer();
		    
		    sql.append("DROP INDEX IDX_TMP_SUBS_PRODUCTS1");
		    executeIfPossible(conn, sql, "Substitutes DAO - Drop index IDX_TMP_SUBS_PRODUCTS1 for TMP_SUBS_PRODUCTS.");
		    
		    sql.delete(0, sql .length());
		    sql.append("drop index IDX_TMP_SUBS_PRODUCTS2");
		    executeIfPossible(conn, sql, "Substitutes DAO - Drop index IDX_TMP_SUBS_PRODUCTS2 for TMP_SUBS_PRODUCTS.");
		    
		    sql.delete(0, sql .length());
		    sql.append("drop table TMP_SUBS_PRODUCTS");
		    executeIfPossible(conn, sql, "Substitutes DAO - Drop table TMP_SUBS_PRODUCTS.");
		    
		    conn.commit();

		    sql.delete(0, sql .length());
		    sql.append("CREATE TABLE TMP_SUBS_PRODUCTS TABLESPACE " + tmpTablespace + " NOLOGGING PARALLEL " + nl +
		    " AS " + nl +
	    		getEligibleProductsQuery(substituteLevelId, itemFilter, 
	    				Integer.parseInt(eligibilitySalePeriod), Integer.parseInt(eligibilitySaleQuantity), Integer.parseInt(eligibilitySaleDays)) 
		    );
	    	// Execution of large query.  Takes some few minutes!
		    logger.info("prepareEligibleProductsData() - Preparing and storing eligible products..");
	    	PristineDBUtil.execute(conn, sql, "Substitutes DAO - Create table TMP_SUBS_PRODUCTS.");
	    	
	    	logger.info("prepareEligibleProductsData() - Indexing eligible products..");
		    sql.delete(0, sql .length());
		    sql.append("CREATE INDEX IDX_TMP_SUBS_PRODUCTS1 ON TMP_SUBS_PRODUCTS (CATEGORY_ID) TABLESPACE " + tmpTablespace);
		    PristineDBUtil.execute(conn, sql, "Substitutes DAO - IDX_TMP_SUBS_PRODUCTS1 for TMP_SUBS_PRODUCTS.");
		    
		    sql.delete(0, sql .length());
		    sql.append("CREATE INDEX IDX_TMP_SUBS_PRODUCTS2 ON TMP_SUBS_PRODUCTS (PROD_ID) TABLESPACE " + tmpTablespace);
		    PristineDBUtil.execute(conn, sql, "Substitutes DAO - IDX_TMP_SUBS_PRODUCTS2 for TMP_SUBS_PRODUCTS.");
	    	
	    	conn.commit();
	    	
	    	return true;
	    }
	    catch (GeneralException e) {
	    	logger.error("prepareEligibleProductsData() - GeneralException=" + e.getMessage());
			e.printStackTrace();
			throw new GeneralException("Failed to prepare eligible products data!");
	    	//return false;
	    }
		catch (SQLException e) {
			logger.error("prepareEligibleProductsData() - SQLException=" + e.getMessage());
			e.printStackTrace();
			throw new GeneralException("Failed to prepare eligible products data!");
			//return false;
	    }
		catch (Exception e) {
			logger.error("prepareEligibleProductsData() - Exception=" + e.getMessage());
			e.printStackTrace();
			throw new GeneralException("Failed to prepare eligible products data!");
			//return false;
	    }
	}    

	/**
	 * Returns the Query to return all eligible items for considering to be either main item or substitute item.   
	 * 
	 * @param substituteLevelId: Product level at which analysis is performed.
	 * @param itemFilter: Additional condition that filters the transactions for analysis.
	 * @param eligibilitySalePeriod: Period in days - Product should have been sold within this period.  
	 * Only then it will be considered for further analysis.    
 	 * @param eligibilitySaleQuantity: Product should have been sold at least n quantity per day 
	 * @param eligibilitySaleDays: Product should have been sold for at least n days
	 * @param substituteBrand: Brand Name - This is the brand name used for filtering on substitute items/LIGs.
	 * 
	 * @return StringBuffer: SQL string
	 * @throws 
	 */
	private StringBuffer getEligibleProductsQuery(int substituteLevelId, String itemFilter, 
			int eligibilitySalePeriod, int eligibilitySaleQuantity, int eligibilitySaleDays)
	{
	    StringBuffer sql = new StringBuffer();

	    if (itemFilter == null) {
	    	itemFilter = " 1 = 1 ";
	    }
	    else {
		    if (itemFilter.isEmpty()) {
		    	itemFilter = " 1 = 1 ";
		    }
	    }


	    if (substituteLevelId == Constants.ITEMLEVELID) // item
	    {
		    sql.append("SELECT category_id, sub_category_id, prod_id, days_moved,  avg_price FROM  " + nl +
		    	    "  ( SELECT prod_id, COUNT(distinct calendar_id) days_moved,  round(AVG(avg_price), 2) avg_price FROM  " + nl +
		    	    "    ( SELECT calendar_id, item_id prod_id, SUM(quantity + weight) mvt, AVG(unit_price) avg_price FROM   " + nl +
		    	    "          " + tLog + 
		    	    "        WHERE calendar_id in (select calendar_id from retail_calendar " +
		    	    "              where  start_date > to_date('" + analysisEndDate + "','MM/dd/yyyy') - " + eligibilitySalePeriod + " and start_date <= to_date('" + analysisEndDate + "','MM/dd/yyyy') and row_type ='D' ) " + nl +
		    	    "            and store_id " + locationsSQLCondition + nl +
		    	    "            and item_id in ( SELECT item_code FROM item_lookup WHERE " + itemFilter + " ) " + nl +
		    	    "        GROUP BY  calendar_id, item_id  " + nl +
		    	    "        HAVING SUM(quantity + weight) >= " + eligibilitySaleQuantity + nl +
		    	    "    )    " + nl +
		    	    " 	group by prod_id " + nl +
		    	    "  HAVING COUNT(calendar_id) >= " + eligibilitySaleDays + nl +
		    	    "  ) filtered_trx " + nl +
		    	    "  LEFT JOIN item_lookup on filtered_trx.prod_id = item_lookup.item_code");
		    	    
	    }
	    else if (substituteLevelId == Constants.PRODUCT_LEVEL_ID_LIG) //lig
	    {
		    sql.append("SELECT DISTINCT category_id, sub_category_id, prod_id, days_moved,  avg_price " + nl +
		    	       "  FROM ( SELECT prod_id, COUNT(distinct calendar_id) days_moved,  round(AVG(avg_price), 2) avg_price " + nl +
		    	       "    FROM ( SELECT calendar_id, ret_lir_id prod_id, SUM(quantity) mvt, AVG(unit_price) avg_price " + nl +
			    	   "      FROM ( SELECT calendar_id, item_id, quantity + weight quantity, unit_price " + nl +
			    	   "               FROM " + tLog +
			    	   "               WHERE calendar_id in (select calendar_id from retail_calendar " +
			    	   "                   WHERE start_date > to_date('" + analysisEndDate + "','MM/dd/yyyy') - " + eligibilitySalePeriod + " and start_date <= to_date('" + analysisEndDate + "','MM/dd/yyyy') and row_type ='D' ) " + nl +
			           "                 AND store_id " + locationsSQLCondition + nl +
			   	       "                 AND item_id in ( SELECT item_code FROM item_lookup WHERE " + itemFilter + " ) " + nl +
			   	       "           ) tl " +
			    	   "               LEFT JOIN item_lookup il on tl.item_id = il.item_code" +
			    	   "             GROUP BY  calendar_id, ret_lir_id  " + nl +
			    	   "             HAVING SUM(quantity) >= " + eligibilitySaleQuantity + nl +
			    	   "         )    " + nl +
			    	   " 	       GROUP BY prod_id " + nl +
			    	   "           HAVING COUNT(calendar_id) >= " + eligibilitySaleDays + nl +
			    	   "       ) filtered_trx " + nl +
			    	   "    LEFT JOIN item_lookup on filtered_trx.prod_id = item_lookup.ret_lir_id" + nl );	    
	    }
	    else if (substituteLevelId == PRODUCT_LEVEL_ID_LIG_OR_ITEM) //lig With items ???
	    {
		    sql.append("SELECT DISTINCT category_id, sub_category_id, prod_id, days_moved, avg_price " + nl +
		    	       "  FROM ( SELECT prod_id, COUNT(distinct calendar_id) days_moved,  round(AVG(avg_price), 2) avg_price " + nl +
		    	       "    FROM ( SELECT calendar_id, ret_lir_id prod_id, SUM(mvt) mvt, AVG(unit_price) avg_price " + nl +
			    	   "      FROM ( SELECT calendar_id, item_id, quantity + weight mvt, unit_price " + nl +
			    	   "               FROM " + tLog +
			    	   "               WHERE calendar_id in (select calendar_id from retail_calendar " +
			    	   "                   WHERE start_date > to_date('" + analysisEndDate + "','MM/dd/yyyy') - " + eligibilitySalePeriod + " and start_date <= to_date('" + analysisEndDate + "','MM/dd/yyyy') and row_type ='D' ) " + nl +
			           "                 AND store_id " + locationsSQLCondition + nl +
			   	       "                 AND item_id in ( SELECT item_code FROM item_lookup WHERE " + itemFilter + ") " + nl +
			   	       "           ) tl " +
			    	   "               LEFT JOIN (select item_code, case when ret_lir_id > 0 then ret_lir_id * -1 else item_code end ret_lir_id from item_lookup) il on tl.item_id = il.item_code" +
			    	   "             GROUP BY  calendar_id, ret_lir_id  " + nl +
			    	   "             HAVING SUM(mvt) >= " + eligibilitySaleQuantity + nl +
			    	   "         )    " + nl +
			    	   " 	       GROUP BY prod_id " + nl +
			    	   "           HAVING COUNT(calendar_id) >= " + eligibilitySaleDays + nl +
			    	   "       ) filtered_trx " + nl +
			    	   "    LEFT JOIN (select case when ret_lir_id > 0 then ret_lir_id * -1 else item_code end ret_lir_id, category_id, sub_category_id from item_lookup) item_lookup " +
			    	   "       on filtered_trx.prod_id = item_lookup.ret_lir_id" + nl );	    


	    }

	    // Breakpoint and get the SQL value here to debug the eligible items
	    return sql;
	}
	
	
	/**
	 * Analyze, Filter and Store the substitutes based on input parameters.   
	 * 
	 * @param conn
	 * @param substituteLevelId: Product level at which analysis is performed.  1:Item, 2:LIG
	 * @param itemFilter: Additional condition that filters the transactions for analysis.
	 * 
	 * @return List<Integer> - List of segments having active items
	 * @throws 
	 */
	public boolean prepareEligibleTrxData(int substituteLevelId, String itemFilter) throws GeneralException
	{
	    try
	    {
		    StringBuffer sql = new StringBuffer();
		    
		    sql.append("DROP INDEX IDX_TMP_SUBS_TRX1");
		    executeIfPossible(conn, sql, "Substitutes DAO - Drop index IDX_TMP_SUBS_TRX1 for TMP_SUBS_PRODUCTS.");
		    
		    sql.delete(0, sql .length());
		    sql.append("drop table TMP_SUBS_TRX");
		    executeIfPossible(conn, sql, "Substitutes DAO - Drop table TMP_SUBS_TRX.");
		    
		    conn.commit();

		    sql.delete(0, sql .length());
		    sql.append("CREATE TABLE TMP_SUBS_TRX TABLESPACE " + tmpTablespace + " NOLOGGING PARALLEL " + nl +
			    " AS " + nl +
	    	    " SELECT category_id, a_prod_id, b_prod_id, customer_id, trx_no, calendar_id, " + nl +
	    	    "     (CASE WHEN prod_id = a_prod_id THEN 1 ELSE 0 END) a_li_present, (CASE WHEN prod_id = b_prod_id THEN 1 ELSE 0 END) b_li_present, a_avg_price, b_avg_price, " + nl +
	    	    "     (CASE WHEN prod_id = a_prod_id THEN net_amt ELSE 0 END) a_net_amt, (CASE WHEN prod_id = b_prod_id THEN net_amt ELSE 0 END) b_net_amt " + nl +
	    	    "  FROM " + nl +
	    	    nl +
	    	    
	    	    /* Make Product Pairs */
	    	    "    ( SELECT item_a.category_id category_id, item_a.prod_id a_prod_id, item_b.prod_id b_prod_id, item_a.avg_price a_avg_price, item_b.avg_price b_avg_price " + nl +
	    	    "        FROM " + nl +
			    "            ( SELECT DISTINCT prod_id, avg_price, category_id from TMP_SUBS_PRODUCTS ) item_a " + nl +
				"          INNER JOIN " + nl +
				"            ( select DISTINCT prod_id, avg_price, category_id from TMP_SUBS_PRODUCTS ) item_b " + nl +
				"                ON item_a.category_id = item_b.category_id and item_a.prod_id <> item_b.prod_id " + nl + nl +
				"     ) item_pair " + nl + nl +
				
				"    INNER JOIN " +  nl +
				"    ( " + nl  );
				
				/* Get the transactions and line items for each item pair */
			    if (substituteLevelId == Constants.ITEMLEVELID) // item
			    {
				    sql.append("SELECT customer_id, trx_no, prod_id, calendar_id, net_amt, sc_trx_cnt FROM" +
					    		" (SELECT customer_id, trx_no, item_id prod_id, calendar_id, net_amt, " +
					    		"        count(distinct trx_no) over (partition by customer_id, sub_category_id) sc_item_cnt" +
					    		"     (SELECT customer_id, trx_no, item_id prod_id, calendar_id, net_amt, " + nl +
				    			"       FROM " + tLog + nl +
				    			"       WHERE calendar_id in ( SELECT calendar_id FROM retail_calendar " +
				    			"                          WHERE start_date > to_date('" + analysisEndDate + "', 'MM/dd/yyyy') - " + Integer.parseInt(PropertyManager.getProperty("SUBS_ANALYSIS_PERIOD_DAYS")) +
				    			"                          AND start_date <= to_date('" + analysisEndDate + "', 'MM/dd/yyyy') AND row_type = 'D' ) " + nl +
				    			"         AND  store_id " + locationsSQLCondition + nl +
				    			"         AND item_id IN ( SELECT DISTINCT prod_id FROM TMP_SUBS_PRODUCTS WHERE " + itemFilter + " ) " +
				    			"         AND  customer_id NOT IN (select distinct customer_id from TMP_SUBS_OUTLIER_CUSTOMER_IDS) )" +
					            "     LEFT JOIN item_lookup ON tl.item_id = item_lookup.item_code )" +
					            " where sc_item_cnt > 3"	/*TODO: Wait for some while before moving to properties file..*/
					    		);
			    }
			    else if (substituteLevelId == Constants.PRODUCT_LEVEL_ID_LIG) //lig
			    {
				    sql.append("SELECT customer_id, trx_no, prod_id, calendar_id, net_amt, sc_trx_cnt FROM " +
				    		"     (SELECT customer_id, trx_no, ret_lir_id prod_id, calendar_id, net_amt, " + nl +
				    		    "        count(distinct trx_no) over (partition by customer_id, sub_category_id) sc_trx_cnt" +
					    		"   FROM ( SELECT customer_id, trx_no, item_id, calendar_id, net_amt, " + nl +
				    			"       FROM " + tLog + nl+
				    			"       WHERE calendar_id in " + nl +
				    			"         ( SELECT calendar_id FROM retail_calendar " +
				    			"             WHERE start_date > to_date('" + analysisEndDate + "', 'MM/dd/yyyy') - " + Integer.parseInt(PropertyManager.getProperty("SUBS_ANALYSIS_PERIOD_DAYS")) +
				    			"             AND start_date <= to_date('" + analysisEndDate + "', 'MM/dd/yyyy') AND row_type = 'D' ) " +
				    			"         AND  store_id " + locationsSQLCondition + nl +
				    			"         AND  customer_id NOT IN (select distinct customer_id from TMP_SUBS_OUTLIER_CUSTOMER_IDS)" + nl +
				    			"       ) tl LEFT JOIN item_lookup ON tl.item_id = item_lookup.item_code" + nl +
			    				"   WHERE ret_lir_id IN ( SELECT DISTINCT prod_id FROM TMP_SUBS_PRODUCTS WHERE " + itemFilter + ") " +
			    				" )" + nl +
				                " where sc_trx_cnt > 3" + nl );
			    }
			    else if (substituteLevelId == PRODUCT_LEVEL_ID_LIG_OR_ITEM) //lig or items
			    {
				    sql.append("SELECT customer_id, trx_no, prod_id, calendar_id, net_amt, sc_trx_cnt FROM " +
				    		   "  ( SELECT customer_id, trx_no, ret_lir_id prod_id, calendar_id, net_amt, " + nl +
				    		   // 
				    		   "          count(distinct trx_no) over (partition by customer_id, sub_category_id) sc_trx_cnt" +
					    	   "         FROM ( SELECT customer_id, trx_no, item_id, calendar_id, net_amt" + nl +
				    		   "             FROM " + tLog + nl+
				    		   "             WHERE calendar_id in " + nl +
				    		   "               ( SELECT calendar_id FROM retail_calendar " +
				    		   "                 WHERE start_date > to_date('" + analysisEndDate + "', 'MM/dd/yyyy') - " + Integer.parseInt(PropertyManager.getProperty("SUBS_ANALYSIS_PERIOD_DAYS")) +
				    		   "                   AND start_date <= to_date('" + analysisEndDate + "', 'MM/dd/yyyy') AND row_type = 'D' ) " +
				    		   "               AND  store_id " + locationsSQLCondition + nl +
				    	       "               AND  customer_id NOT IN (select distinct customer_id from TMP_SUBS_OUTLIER_CUSTOMER_IDS) " + nl +		    			
				    		   "           ) tl LEFT JOIN (select sub_category_id, item_code, case when ret_lir_id > 0 then ret_lir_id * -1 else item_code end ret_lir_id from item_lookup) item_lookup ON tl.item_id = item_lookup.item_code" + nl +
			    			   "         WHERE ret_lir_id IN ( SELECT DISTINCT prod_id FROM TMP_SUBS_PRODUCTS WHERE " + itemFilter + ") " +
			    			   "       )" +
			    			   "  where sc_trx_cnt > 3" + nl );
			    }
				
			    sql.append("    ) li ON ( item_pair.a_prod_id = li.prod_id or item_pair.b_prod_id = li.prod_id ) " );

	    	// Execution of large query.  Takes few minutes! 
		    logger.info("prepareEligibleTrxData() - Preparing and storing eligible transactions..");
	    	PristineDBUtil.execute(conn, sql, "Substitutes DAO - Create table TMP_SUBS_TRX.");
	    	
		    sql.delete(0, sql .length());
		    sql.append("CREATE INDEX IDX_TMP_SUBS_TRX1 ON TMP_SUBS_TRX (CATEGORY_ID) TABLESPACE " + tmpTablespace );
		    logger.info("prepareEligibleProductsData() - Indexing eligible transactions..");
		    PristineDBUtil.execute(conn, sql, "Substitutes DAO - IDX_TMP_SUBS_TRX1.");
		    
	    	conn.commit();
	    	
	    	return true;
	    }
	    catch (GeneralException e) {
	    	logger.error("prepareEligibleTrxData() - GeneralException=" + e.getMessage());
			e.printStackTrace();
			throw new GeneralException("Failed to prepare eligible products data!");
	    	//return false;
	    }
		catch (SQLException e) {
			logger.error("prepareEligibleTrxData() - SQLException=" + e.getMessage());
			e.printStackTrace();
			throw new GeneralException("Failed to prepare eligible products data!");
			//return false;
	    }
		catch (Exception e) {
			logger.error("prepareEligibleTrxData() - Exception=" + e.getMessage());
			e.printStackTrace();
			throw new GeneralException("Failed to prepare eligible products data!");
			//return false;
	    }
	}    
	

	/**
	* Analyze, Filter and Store the substitutes based on input parameters.   
	* 
	* @param conn
	* @param substituteLevelId: Product level at which analysis is performed.
	* @param itemFilter: Additional condition that filters the transactions for analysis.
	* 
	* @return List<Integer> - List of segments having active items
	* @throws 
	*/
	public boolean storeProductSubstitutes()
	{

	try
	{
		StringBuffer subsSql = new StringBuffer();
	    StringBuffer sql = new StringBuffer();
	    String sql1 = null;
	    String sql2 = null;
	    
	    /* DUUUUPLICATE PIECE OF KEY SQL CODE ??? */
	    
		subsSql.append(" SELECT dat.*,  " + 
				"      (CASE WHEN overall_score >= 9 THEN 3 WHEN overall_score >= 6 THEN 2 WHEN overall_score >= 3 THEN 1 ELSE 0 END) overall_strength  " + 
				"    FROM  " + 
				"      ( SELECT dat.*, (aawab_cst_pct_score + aawab_b_trx_pct_score + aawab_aandb_trx_pct_score + size_diff_pct_score + price_diff_pct_score + group_score) overall_score " + 
				"        FROM  " + 
				"          ( SELECT dat.*, (CASE WHEN aawab_cst_pct > 5 THEN 2 WHEN aawab_cst_pct > 3 THEN 1 WHEN aawab_cst_pct > 2 THEN 0 ELSE -1 END) aawab_cst_pct_score, " + 
				"              (CASE WHEN aawab_aandb_trx_pct < 2 THEN 3 WHEN aawab_aandb_trx_pct < 3.5 THEN 2 WHEN aawab_aandb_trx_pct < 5 THEN 1 WHEN aawab_aandb_trx_pct < 15 THEN 0 ELSE -1 END) aawab_aandb_trx_pct_score, " + 
				"              (CASE WHEN aawab_b_trx_pct > 30 THEN 2 WHEN aawab_b_trx_pct > 20 THEN 1 WHEN aawab_b_trx_pct > 5 THEN 0 ELSE -1 END) aawab_b_trx_pct_score,  " + 
				"              (CASE WHEN size_diff_pct < 100 THEN 1 WHEN size_diff_pct < 400 THEN 0 ELSE -1 END) size_diff_pct_score,  " + 
				"              (CASE WHEN price_diff_pct < 100 THEN 1 WHEN price_diff_pct < 400 THEN 0 ELSE -1 END) price_diff_pct_score,  " +
				/* TODO: Remove AHOLD specific code ???*/
				"              (CASE " + /*" WHEN is_same_segment = 1 THEN 2 " + */ " WHEN is_same_lig = 1 THEN 2 WHEN is_same_subcategory = 1 THEN 1 WHEN is_same_category = 1 THEN 0 ELSE 0 END) group_score " + 
				 
				"	    	    FROM  " + 
				"              ( SELECT dat.*, case when nvl(a_uom_id, 0) = nvl(b_uom_id, 0) then round(nvl((100*abs(a_size - b_size))/nullif(a_size, 0), 0), 2) else 1000 end size_diff_pct, " + 
				"                    round(nvl((100*abs(A_AVG_NET_UNIT_PRICE - B_AVG_NET_UNIT_PRICE))/nullif(A_AVG_NET_UNIT_PRICE, 0), 0), 2) price_diff_pct, " + 
				"                    round(nvl(100*aawab_cst_cnt/nullif(aorb_cst_cnt, 0), 0), 2) aawab_cst_pct, round(nvl(100*aawab_b_trx_cnt/nullif(aawab_aorb_trx_cnt, 0), 0), 2) aawab_b_trx_pct,  " + 
				"                    round(nvl(100*aawab_aandb_trx_cnt/nullif(aawab_aorb_trx_cnt, 0), 0), 2) aawab_aandb_trx_pct " + 
				"                  from  " + 
				"                    (select * from product_pair_detail where batch_id = " + batchId + ") dat " + 
				"              ) dat " + 
				"          ) dat " + 
				"      ) dat ");
		
	    sql1 = "ID, ";                          sql2 = "PRODUCT_SUBSTITUTES_SEQ.NEXTVAL, ";
	    sql1 += "LOCATION_LEVEL_ID, ";          sql2 += locationLevelId + ", ";
	    sql1 += "LOCATION_ID, ";                sql2 += locationId + ", ";
	    
	    sql1 += "A_ITEM_TYPE, ";                sql2 += "a_item_type, ";
	    sql1 += "A_ITEM_ID, ";                  sql2 += "a_item_id, ";
	    sql1 += "B_ITEM_TYPE, ";                sql2 += "b_item_type, ";
	    sql1 += "B_ITEM_ID, ";                  sql2 += "b_item_id, ";
	    
	    sql1 += "AAWAB_CST_SCORE, ";            sql2 += "aawab_cst_pct_score, ";
	    sql1 += "AAWAB_B_TRX_PCT_SCORE, ";      sql2 += "aawab_b_trx_pct_score, ";
	    sql1 += "AAWAB_AANDB_SCORE, ";          sql2 += "aawab_aandb_trx_pct_score, ";
	    sql1 += "PRODUCT_LEVEL_SCORE, ";        sql2 += "group_score, ";

		sql1 += "SIZE_SCORE, ";                 sql2 += "size_diff_pct_score, ";
		sql1 += "PRICE_SCORE, ";                sql2 += "price_diff_pct_score, ";
		sql1 += "OVERALL_SCORE, ";              sql2 += "overall_score, ";
		sql1 += "OVERALL_STRENGTH, ";           sql2 += "overall_strength, ";
	
		sql1 += "AAWAB_CST_PCT, ";              sql2 += "aawab_cst_pct, ";
		sql1 += "AAWAB_B_TRX_PCT, ";            sql2 += "aawab_b_trx_pct, ";
		sql1 += "AAWAB_AANDB_TRX_PCT, ";        sql2 += "aawab_aandb_trx_pct, ";
		sql1 += "SIZE_DIFF_PCT, ";              sql2 += "size_diff_pct, ";
		sql1 += "PRICE_DIFF_PCT, ";             sql2 += "price_diff_pct, ";
	    
	    sql1 += "BATCH_ID, ";                   sql2 += batchId + ", ";
	    sql1 += "UPDATE_TIMESTAMP, ";           sql2 += "sysdate, ";
	    sql1 += "ACTIVE_INDICATOR ";            sql2 += "'Y' ";

	    sql.append("INSERT INTO product_substitutes " + nl +
	    		" ( " + sql1 + " ) " +
	    		"  SELECT " + nl +
	    		sql2 +
	    		"  FROM " + nl + 
	    		"    ( " + nl + nl +
	    		// This returns the sub-query string that retrieves the substitutes. 
	    		subsSql + nl + nl +
	    		"    ) " );

		// Execution of large query.  Takes few minutes!
	    logger.info("getSubstitutes() - sql=" + sql);
		PristineDBUtil.execute(conn, sql, "Substitutes DAO - Analyse and Save Substitutes.");
		
		conn.commit();
		
		return true;
	}
	catch (GeneralException e) {
		logger.error("getSubstitutes() - GeneralException=" + e.getMessage());
		e.printStackTrace();			
		return false;
	}
	catch (SQLException e) {
		logger.error("getSubstitutes() - SQLException=" + e.getMessage());
		e.printStackTrace();
		return false;
	}
	catch (Exception e) {
		logger.error("getSubstitutes() - Exception=" + e.getMessage());
		e.printStackTrace();
		return false;
	}
	}    
	    
	
/**
* Analyze, Filter and Store the substitutes based on input parameters.   
* 
* @param conn
* @param substituteLevelId: Product level at which analysis is performed.  1:Item, 2:LIG
* @param itemFilter: Additional condition that filters the transactions for analysis.
* 
* @return List<Integer> - List of segments having active items
* @throws 
*/
public boolean storeProductPairDetail(int substituteLevelId, String itemFilter)
{
	
	try
	{
	    StringBuffer sql = new StringBuffer();
	    String sql1 = null;
	    String sql2 = null;
		
	    sql1 = "ID, ";                       sql2 = "PRODUCT_PAIR_DETAIL_SEQ.NEXTVAL, ";
	    sql1 += "LOCATION_LEVEL_ID, ";        sql2 += locationLevelId + ", ";
	    sql1 += "LOCATION_ID, ";              sql2 += locationId + ", ";
	    
	    sql1 += "A_ITEM_TYPE, ";              sql2 += "a_item_type, ";
	    sql1 += "A_ITEM_ID, ";                sql2 += "a_prod_id, ";
	    sql1 += "B_ITEM_TYPE, ";              sql2 += "b_item_type, ";
	    sql1 += "B_ITEM_ID, ";                sql2 += "b_prod_id, ";
	    sql1 += "A_SIZE, ";                   sql2 += "a_size, ";
	    sql1 += "A_UOM_ID, ";                 sql2 += "a_uom_id, ";
	    sql1 += "B_SIZE, ";                   sql2 += "b_size, ";
	    sql1 += "B_UOM_ID, ";                 sql2 += "b_uom_id, ";
	    sql1 += "A_AVG_NET_UNIT_PRICE, ";     sql2 += "a_avg_price, ";
	    sql1 += "B_AVG_NET_UNIT_PRICE, ";     sql2 += "b_avg_price, ";
	
	    sql1 += "IS_SAME_CATEGORY, ";         sql2 += "is_same_category, ";
	    sql1 += "IS_SAME_SUBCATEGORY, ";      sql2 += "is_same_subcategory, ";
	    sql1 += "IS_SAME_SEGMENT, ";          sql2 += "is_same_segment, ";
	    sql1 += "IS_SAME_LIG, ";              sql2 += "is_same_lig, ";
	    
	    sql1 += "AORB_CST_CNT, ";             sql2 += "aorb_cst_cnt, ";
	    sql1 += "A_CST_CNT, ";                sql2 += "a_cst_cnt, ";
	    sql1 += "B_CST_CNT, ";                sql2 += "b_cst_cnt, ";
	    sql1 += "ANOB_CST_CNT, ";             sql2 += "anob_cst_cnt, ";
	    sql1 += "BNOA_CST_CNT, ";             sql2 += "bnoa_cst_cnt, ";
	    sql1 += "AANDB_CST_CNT, ";            sql2 += "aandb_cst_cnt, ";
	    sql1 += "AAWAB_CST_CNT, ";            sql2 += "aawab_cst_cnt, ";
	    sql1 += "AORB_TRX_CNT, ";             sql2 += "aorb_trx_cnt, ";
	    sql1 += "A_TRX_CNT, ";                sql2 += "a_trx_cnt, ";
	    sql1 += "B_TRX_CNT, ";                sql2 += "b_trx_cnt, ";
	    sql1 += "ANOB_TRX_CNT, ";             sql2 += "anob_trx_cnt, ";
	    sql1 += "BNOA_TRX_CNT, ";             sql2 += "bnoa_trx_cnt, ";
	    sql1 += "AANDB_TRX_CNT, ";            sql2 += "aandb_trx_cnt, ";
	    sql1 += "AAWAB_AORB_TRX_CNT, ";       sql2 += "aawab_aorb_trx_cnt, ";
	    sql1 += "AAWAB_A_TRX_CNT, ";          sql2 += "aawab_a_trx_cnt, ";
	    sql1 += "AAWAB_B_TRX_CNT, ";          sql2 += "aawab_b_trx_cnt, ";
	    sql1 += "AAWAB_ANOB_TRX_CNT, ";       sql2 += "aawab_anob_trx_cnt, ";
	    sql1 += "AAWAB_BNOA_TRX_CNT, ";       sql2 += "aawab_bnoa_trx_cnt, ";
	    sql1 += "AAWAB_AANDB_TRX_CNT, ";      sql2 += "aawab_aandb_trx_cnt, ";
	    sql1 += "AORB_CST_A_NET_AMT, ";       sql2 += "aorb_cst_a_net_amt, ";
	    sql1 += "AORB_CST_B_NET_AMT, ";       sql2 += "aorb_cst_b_net_amt, ";
	    sql1 += "A_CST_A_NET_AMT, ";          sql2 += "a_cst_a_net_amt, ";
	    sql1 += "A_CST_B_NET_AMT, ";          sql2 += "a_cst_b_net_amt, ";
	    sql1 += "B_CST_A_NET_AMT, ";          sql2 += "b_cst_a_net_amt, ";
	    sql1 += "B_CST_B_NET_AMT, ";          sql2 += "b_cst_b_net_amt, ";
	    sql1 += "ANOB_CST_A_NET_AMT, ";       sql2 += "anob_cst_a_net_amt, ";
	    sql1 += "BNOA_CST_B_NET_AMT, ";       sql2 += "bnoa_cst_b_net_amt, ";
	    sql1 += "AANDB_CST_A_NET_AMT, ";      sql2 += "aandb_cst_a_net_amt, ";
	    sql1 += "AANDB_CST_B_NET_AMT, ";      sql2 += "aandb_cst_b_net_amt, ";
	    sql1 += "AAWAB_CST_A_NET_AMT, ";      sql2 += "aawab_cst_a_net_amt, ";
	    sql1 += "AAWAB_CST_B_NET_AMT, ";      sql2 += "aawab_cst_b_net_amt, ";
	    
	    sql1 += "BATCH_ID, ";                 sql2 += batchId + ", ";
	    sql1 += "UPDATE_TIMESTAMP, ";         sql2 += "sysdate, ";
	    sql1 += "ACTIVE_INDICATOR ";         sql2 += "'Y' ";
	
	    sql.append("INSERT INTO product_pair_detail " + nl +
	    		" ( " + sql1 + " ) " +
	    		" WITH item_lookup_oz as " +
	    		"   (select item_code, category_id, sub_category_id, segment_id, ret_lir_id, " +  
	    	    "       case when uom_id = '1' then '2' else uom_id end uom_id, case when uom_id = '1' then item_size * 16 else item_size end item_size " +
                "     from item_lookup) " +
	    		"  SELECT " + nl +
	    		sql2 +
	    		"  FROM " + nl + 
	    		"    ( " + nl + nl +
	    		
	    		// This returns the sub-query string that retrieves the substitutes. 
	    		getSubstitutesQuery(substituteLevelId, itemFilter) + nl + nl +
	    		
	    		"    ) " );
	
		// Execution of large query.  Takes few minutes! 
	    logger.info("storeProductPairDetail() - sql=" + sql);
		PristineDBUtil.execute(conn, sql, "Substitutes DAO - Analyse and Save Substitutes.");
		
		conn.commit();
		
		return true;
	}
	catch (GeneralException e) {
		logger.error("storeProductPairDetail() - GeneralException=" + e.getMessage());
		e.printStackTrace();			
		return false;
	}
	catch (SQLException e) {
		logger.error("storeProductPairDetail() - SQLException=" + e.getMessage());
		e.printStackTrace();
		return false;
	}
	catch (Exception e) {
		logger.error("storeProductPairDetail() - Exception=" + e.getMessage());
		e.printStackTrace();
		return false;
	}
}    



	
	/**
	 * Returns the Query to analyze and filter the substitutes.   
	 * 
	 * @param substituteLevelId: Product level at which analysis is performed.  
	 * @param itemFilter: Additional condition that filters the transactions for analysis.
	 * 
	 * @return StringBuffer: SQL string
	 * @throws 
	 */
	private StringBuffer getSubstitutesQuery(int substituteLevelId, String itemFilter)
	{
	    StringBuffer sql = new StringBuffer();	    
	    String prod_code = null;

	    if (substituteLevelId == Constants.ITEMLEVELID) // item
	        prod_code = "item_code";
	    else if ( (substituteLevelId == Constants.PRODUCT_LEVEL_ID_LIG) || (substituteLevelId == PRODUCT_LEVEL_ID_LIG_OR_ITEM) )
	        prod_code = "ret_lir_id";
	    

	    /* TODO: DUUUUPLICATE PIECE OF KEY SQL CODE ??? */
	    
	    sql.append("SELECT ip_filtered_scores.*, (CASE WHEN overall_score >= 9 THEN 3 WHEN overall_score >= 6 THEN 2 WHEN overall_score >= 3 THEN 1 ELSE 0 END) overall_strength FROM " + nl +
	    	    "     (SELECT ip_scores.*, (aawab_cst_pct_score + aawab_b_trx_pct_score + aawab_aandb_trx_pct_score + size_diff_pct_score + price_diff_pct_score + group_score) overall_score FROM " + nl +
	    	    "       (SELECT ip_pct.*, (CASE WHEN aawab_cst_pct > 5 THEN 2 WHEN aawab_cst_pct > 3 THEN 1 WHEN aawab_cst_pct > 2 THEN 0 ELSE -1 END) aawab_cst_pct_score, " + nl +
	    	    "           (CASE WHEN aawab_aandb_trx_pct < 2 THEN 3 WHEN aawab_aandb_trx_pct < 3.5 THEN 2 WHEN aawab_aandb_trx_pct < 5 THEN 1 WHEN aawab_aandb_trx_pct < 15 THEN 0 ELSE -1 END) aawab_aandb_trx_pct_score, " + nl +
	    	    "           (CASE WHEN aawab_b_trx_pct > 30 THEN 2 WHEN aawab_b_trx_pct > 20 THEN 1 WHEN aawab_b_trx_pct > 5 THEN 0 ELSE -1 END) aawab_b_trx_pct_score, " + nl +
	    	    "           (CASE WHEN size_diff_pct < 100 THEN 1 WHEN size_diff_pct < 400 THEN 0 ELSE -1 END) size_diff_pct_score, " + nl +
	    	    "           (CASE WHEN price_diff_pct < 100 THEN 1 WHEN price_diff_pct < 400 THEN 0 ELSE -1 END) price_diff_pct_score, " + nl +
	    	    "           (CASE WHEN a_segment_id = b_segment_id THEN 2 WHEN a_lig = b_lig THEN 2 WHEN a_sub_category_id = b_sub_category_id THEN 1 WHEN a_category_id = b_category_id THEN 1 ELSE 0 END) group_score, " + nl +
	    	    "           (CASE WHEN a_lig = b_lig THEN 1 ELSE 0 END) is_same_lig, " +
	    	    "           (CASE WHEN a_segment_id = b_segment_id THEN 1 ELSE 0 END) is_same_segment, " +
	    	    "           (CASE WHEN a_sub_category_id = b_sub_category_id THEN 1 ELSE 0 END) is_same_subcategory, " +
	    	    "           (CASE WHEN a_category_id = b_category_id THEN 1 ELSE 0 END) is_same_category " +
	    	    
	    	    "         FROM " + nl +
	    	    
	    	    "           (SELECT ");
			    if (substituteLevelId == Constants.ITEMLEVELID) // item
			    {
			    	sql.append("a_prod_id, b_prod_id, " +
			    	Constants.ITEMLEVELID + " a_item_type, " + Constants.ITEMLEVELID + " b_item_type, " +
			    	"           a_items.ret_lir_id a_lig, b_items.ret_lir_id b_lig, ");
			    }
			    else if (substituteLevelId == Constants.PRODUCT_LEVEL_ID_LIG) 
			    {
			    	sql.append("a_prod_id, b_prod_id, " +
			    	Constants.PRODUCT_LEVEL_ID_LIG + " a_item_type, " + Constants.PRODUCT_LEVEL_ID_LIG + " b_item_type, " +
			    	"           a_items.ret_lir_id a_lig, b_items.ret_lir_id b_lig, ");
			    }
			    else if (substituteLevelId == PRODUCT_LEVEL_ID_LIG_OR_ITEM) 
			    {
			    	sql.append("abs(a_prod_id) a_prod_id, abs(b_prod_id) b_prod_id, " +			    	
			    	"           case when a_prod_id >= 0 then " + Constants.ITEMLEVELID + " when a_prod_id < 0 then " + Constants.PRODUCT_LEVEL_ID_LIG + " end a_item_type,  " +
			    	"           case when b_prod_id >= 0 then " + Constants.ITEMLEVELID + " when b_prod_id < 0 then " + Constants.PRODUCT_LEVEL_ID_LIG + " end b_item_type,  " +
			    	"           a_items.ret_lir_id a_lig, b_items.ret_lir_id b_lig, ");
			    }
			    
			    sql.append("    a_items.item_size a_size, b_items.item_size b_size, a_items.uom_id a_uom_id, b_items.uom_id b_uom_id, " + nl);			    	    
			    sql.append("    case when nvl(a_items.uom_id, 0) = nvl(b_items.uom_id, 0) then round(nvl((100*abs(a_items.item_size - b_items.item_size))/nullif(a_items.item_size, 0), 0), 2) else 1000 end size_diff_pct, " + nl +
			               "    round(nvl((100*abs(a_avg_price - b_avg_price))/nullif(a_avg_price, 0), 0), 2) price_diff_pct, ");
			    
			    sql.append(
	            "               a_trx_cnt, b_trx_cnt, bnoa_trx_cnt, anob_trx_cnt, aorb_trx_cnt, aandb_trx_cnt, " + nl +
		    	"               a_cst_cnt, b_cst_cnt, a_cst_a_net_amt, a_cst_b_net_amt, b_cst_a_net_amt, b_cst_b_net_amt, " +
		    	"               bnoa_cst_cnt, anob_cst_cnt, bnoa_cst_b_net_amt, anob_cst_a_net_amt, " +
		    	"               aorb_cst_cnt, aandb_cst_cnt, aorb_cst_a_net_amt, aorb_cst_b_net_amt, " + nl +
		    	"               aandb_cst_a_net_amt, aandb_cst_b_net_amt, " + nl +
		    	"               aawab_cst_cnt, aawab_cst_a_net_amt, aawab_cst_b_net_amt, " + nl +
		    	"               aawab_a_trx_cnt, aawab_b_trx_cnt, " + nl +
		    	"               aawab_anob_trx_cnt, aawab_bnoa_trx_cnt, " +	    	    
		    	"               aawab_aorb_trx_cnt, aawab_aandb_trx_cnt, " +
		    	"               a_avg_price, b_avg_price, " +    	    	    	    
	    	    "               round(nvl(100*aawab_cst_cnt/nullif(aorb_cst_cnt, 0), 0), 2) aawab_cst_pct, round(nvl(100*aawab_b_trx_cnt/nullif(aawab_aorb_trx_cnt, 0), 0), 2) aawab_b_trx_pct, round(nvl(100*aawab_aandb_trx_cnt/nullif(aawab_aorb_trx_cnt, 0), 0), 2) aawab_aandb_trx_pct, " + nl +
	    	    "               a_items.sub_category_id a_sub_category_id, b_items.sub_category_id b_sub_category_id, " + nl +
	    	    "               a_items.category_id a_category_id, b_items.category_id b_category_id, " + nl +
	    		"               a_items.segment_id a_segment_id, b_items.segment_id b_segment_id ");

			    sql.append("  FROM " + nl);
			    
			    // groupings below..
				sql.append("    ( SELECT " +
				"                   a_prod_id, b_prod_id, SUM(a_trx_cnt) a_trx_cnt, SUM(b_trx_cnt) b_trx_cnt, SUM(bnoa_trx_cnt) bnoa_trx_cnt, SUM(anob_trx_cnt) anob_trx_cnt, " + nl +
	    	    "                   SUM(aorb_trx_cnt) aorb_trx_cnt, SUM(aandb_trx_cnt) aandb_trx_cnt, " + nl +
	    	    "                   SUM(CASE WHEN a_trx_cnt > 0 THEN 1 ELSE 0 END) a_cst_cnt, SUM(CASE WHEN b_trx_cnt > 0 THEN 1 ELSE 0 END) b_cst_cnt, " + nl +

	    	    
	    	    "                   SUM(CASE WHEN a_trx_cnt > 0 THEN a_net_amt ELSE 0 END) a_cst_a_net_amt, SUM(CASE WHEN a_trx_cnt > 0 THEN b_net_amt ELSE 0 END) a_cst_b_net_amt, " + nl +
	    	    "                   SUM(CASE WHEN b_trx_cnt > 0 THEN a_net_amt ELSE 0 END) b_cst_a_net_amt, SUM(CASE WHEN b_trx_cnt > 0 THEN b_net_amt ELSE 0 END) b_cst_b_net_amt, " + nl +
	    	    "                   SUM(CASE WHEN a_trx_cnt = 0 THEN 1 ELSE 0 END) bnoa_cst_cnt, SUM(CASE WHEN b_trx_cnt = 0 THEN 1 ELSE 0 END) anob_cst_cnt, " + nl +
	    	    "                   SUM(CASE WHEN a_trx_cnt = 0 THEN b_net_amt ELSE 0 END) bnoa_cst_b_net_amt, SUM(CASE WHEN b_trx_cnt = 0 THEN a_net_amt ELSE 0 END) anob_cst_a_net_amt, " + nl +
	    	    "                   SUM(CASE WHEN aorb_trx_cnt > 0 THEN 1 ELSE 0 END) aorb_cst_cnt, SUM(CASE WHEN aandb_trx_cnt > 0 THEN 1 ELSE 0 END) aandb_cst_cnt, " + nl +
	    	    "                   SUM(CASE WHEN aorb_trx_cnt > 0 THEN a_net_amt ELSE 0 END) aorb_cst_a_net_amt, SUM(CASE WHEN aorb_trx_cnt > 0 THEN b_net_amt ELSE 0 END) aorb_cst_b_net_amt, " + nl +
	    	    "                   SUM(CASE WHEN aandb_trx_cnt > 0 THEN a_net_amt ELSE 0 END) aandb_cst_a_net_amt, SUM(CASE WHEN aandb_trx_cnt > 0 THEN b_net_amt ELSE 0 END) aandb_cst_b_net_amt, " + nl +
	    	    "                   SUM(aawab_indicator) aawab_cst_cnt, " + nl +
	    	    "                   SUM(CASE WHEN aawab_indicator = 1 THEN a_net_amt ELSE 0 END) aawab_cst_a_net_amt, SUM(CASE WHEN aawab_indicator = 1 THEN b_net_amt ELSE 0 END) aawab_cst_b_net_amt, " + nl +

	    	    // number of trx where substitute is purchased is here.  may require a criteria to check that min so many subs be present at customer level itself.
	    	    
	    	    "                   SUM(CASE WHEN aawab_indicator = 1 THEN a_trx_cnt ELSE 0 END) aawab_a_trx_cnt, SUM(CASE WHEN aawab_indicator = 1 THEN b_trx_cnt ELSE 0 END) aawab_b_trx_cnt, " + nl +
	    	    "                   SUM(CASE WHEN aawab_indicator = 1 THEN anob_trx_cnt ELSE 0 END) aawab_anob_trx_cnt, SUM(CASE WHEN aawab_indicator = 1 THEN bnoa_trx_cnt ELSE 0 END) aawab_bnoa_trx_cnt, " +	    	    
	    	    "                   SUM(CASE WHEN aawab_indicator = 1 THEN aorb_trx_cnt ELSE 0 END) aawab_aorb_trx_cnt, SUM(CASE WHEN aawab_indicator = 1 THEN aandb_trx_cnt ELSE 0 END) aawab_aandb_trx_cnt, " +

	    	    "                   AVG(a_avg_price) a_avg_price, AVG(b_avg_price) b_avg_price " +
	    	    "                 FROM " + nl +
	    	    					// trx counts
	    	    "                   ( SELECT a_prod_id, b_prod_id, customer_id, " + nl +
	    	    "                       SUM(a_trx_present) a_trx_cnt, SUM(b_trx_present) b_trx_cnt, " +
	    	    "                       SUM(bnoa_trx_present) bnoa_trx_cnt, SUM(anob_trx_present) anob_trx_cnt, " + nl +
	    	    "                       SUM(aorb_trx_present) aorb_trx_cnt, SUM(aandb_trx_present) aandb_trx_cnt, " +
	    	    "                       CASE WHEN SUM(a_trx_present) > 0 and SUM(b_trx_present) > 0 THEN 1 ELSE 0 END aawab_indicator, " +
	    	    "                       AVG(a_avg_price) a_avg_price, AVG(b_avg_price) b_avg_price, " + nl +
	    	    "                       sum(a_net_amt) a_net_amt, sum(b_net_amt) b_net_amt " +
	    	    "                     FROM " + nl + 
	    	    						// Is a present? is b present?
	    	    "                       ( SELECT a_prod_id, b_prod_id, customer_id, trx_no, " +
	    	    "                           max(a_li_present) a_trx_present, max(b_li_present) b_trx_present, " + nl +
	    	    "                           ( CASE WHEN max(a_li_present) = 0 THEN 1 ELSE 0 END ) bnoa_trx_present, ( CASE WHEN max(b_li_present) = 0 THEN 1 ELSE 0 END ) anob_trx_present, " + nl +
	    	    "                           1 aorb_trx_present, " + nl +
	    	    "                           ( CASE WHEN max(a_li_present) + max(b_li_present) >= 2 THEN 1 ELSE 0 END ) aandb_trx_present, " +
	    	    "                           AVG(a_avg_price) a_avg_price, AVG(b_avg_price) b_avg_price, " + nl +
	    	    "                           sum(a_net_amt) a_net_amt, sum(b_net_amt) b_net_amt " + nl +
	    	    "                         FROM " + nl +   
	    	    							//TODO: ??? will this work when sub-category id is input
	    	    "                           TMP_SUBS_TRX WHERE " + itemFilter  + nl +		
	    	    "                         GROUP BY a_prod_id, b_prod_id, customer_id, trx_no " + nl +
	    	    "                       ) item_pair_trx " + nl +
	    	    "                     GROUP BY a_prod_id, b_prod_id, customer_id " + nl +
	    	    "                   ) ip_customers " + nl +
	    	    "                 GROUP BY a_prod_id, b_prod_id " + nl +
				"               ) ip_aggr ");
	    
	    if (substituteLevelId == Constants.ITEMLEVELID) 
	    {
	    	sql.append(
	    		"                 LEFT JOIN item_lookup_oz a_items " + nl +
	    		"                   ON a_prod_id = a_items." + prod_code + nl +
	    		"                 LEFT JOIN item_lookup_oz b_items " + nl +
	    		"                   ON b_prod_id = b_items." + prod_code);
	    }
	    else
	    {	    	
	        sql.append(   
	        	"                 LEFT JOIN (" +
	    	    "                             SELECT -il.ret_lir_id ret_lir_id, item_size, uom_id, category_id, sub_category_id, segment_id " +
	    	    "                               FROM retailer_like_item_group lig INNER JOIN item_lookup_oz il " +
	    	    "                                 ON lig.ret_lir_item_code = il.item_code " +
	    	    "                             union " +
	    	    "                             SELECT item_code ret_lir_id, item_size, uom_id, category_id, sub_category_id, segment_id " +
	    	    "                               FROM item_lookup_oz il " +
	    	    "                               WHERE (ret_lir_id IS NULL OR ret_lir_id = 0) AND item_code < 900000 " +
	    	    "                     ) a_items " + nl +
	        	"                   ON a_prod_id = a_items." + prod_code + nl +
	        	"                 LEFT JOIN (" +
	    	    "                             SELECT -il.ret_lir_id ret_lir_id, item_size, uom_id, category_id, sub_category_id, segment_id " +
	    	    "                               FROM retailer_like_item_group lig INNER JOIN item_lookup_oz il " +
	    	    "                                 ON lig.ret_lir_item_code = il.item_code " +
	    	    "                             union " +
	    	    "                             SELECT item_code ret_lir_id, item_size, uom_id, category_id, sub_category_id, segment_id " +
	    	    "                               FROM item_lookup_oz il " +
	    	    "                               WHERE (ret_lir_id IS NULL OR ret_lir_id = 0) AND item_code < 900000 " +
	        	"                     ) b_items " + nl +
	        	"                   ON b_prod_id = b_items." + prod_code + nl);
	    }	    
	    sql.append("        ) ip_pct " + nl +
	    		"       ) ip_scores " + nl +
	    		"     ) ip_filtered_scores " +
	    
	    		/* Ignore low score item substitutes */
	    		"     WHERE overall_score >= " + PropertyManager.getProperty("SUBS_FILTER_OVERALL_SCORE_LOWER_LIMIT") + 
	    		"       AND aawab_cst_pct_score <> -1 and aawab_aandb_trx_pct_score <> -1 and aawab_b_trx_pct_score <> -1 " +
	    		"       AND price_diff_pct < 1000 AND size_diff_pct < 1000");
	    
	    // Breakpoint and get the SQL value here to debug
	    return sql;
	}	

	
	
	/**
	 * Returns a list of store information
	 * @param 
	 * @return
	 * @throws 
	 */
	public ArrayList<StoreDTO> getStoreInfoList(int divisionId) {
		PreparedStatement statement = null;
		ResultSet rs = null;

	    String tempStoreList = null;
	    String propertyKey = "SUBS_TEMP_STORE_LIST" + "_" + locationLevelId + "_" + locationId;

	    tempStoreList = PropertyManager.getProperty(propertyKey); 
	    logger.info("setProperties() - Properties Info: " + propertyKey + "=" + tempStoreList);
	    
		// TODO: ??? Put the configuration in database
		String sql = "SELECT comp_str_id, comp_str_no, name, city, state FROM competitor_store " +
				//"WHERE comp_str_id IN (" + tempStoreList + ") AND division_id = " + String.valueOf(divisionId);
				"WHERE comp_str_id IN (" + tempStoreList + ")";
		
						
		ArrayList<StoreDTO> storeInfoList = new ArrayList<StoreDTO>();
		try{
			statement = conn.prepareStatement(sql);
			statement.setFetchSize(5000);
			
			rs = statement.executeQuery();
			while (rs.next()){
				StoreDTO sDto = new StoreDTO();
				sDto.strId = rs.getInt("comp_str_id");
				sDto.strNum = rs.getString("comp_str_no");
				sDto.strName = rs.getString("name");
				sDto.city = rs.getString("city");
				sDto.state = rs.getString("state");
				storeInfoList.add(sDto);
			}
			//logger.info("No of Stores - " + storeInfoList.size());
			return storeInfoList;
	    }
		catch (SQLException e) {
			logger.error("getStores() - SQLException=" + e.getMessage());
			e.printStackTrace();
			return storeInfoList;
	    }
		catch (Exception e) {
			logger.error("getStores() - Exception=" + e.getMessage());
			e.printStackTrace();
			return storeInfoList;
	    }
	}

	
	/**
	 * Returns a list of substitute pairs information
	 * @param 
	 * @return
	 * @throws 
	 */
	public ArrayList<SubstitutePair> getPairList() {
		PreparedStatement statement = null;
		ResultSet rs = null;

		// Group only based on the medium and above substitute pairs
		String sql = "select case when a_item_type = " + Constants.ITEMLEVELID + " then a_item_id when a_item_type = " + Constants.PRODUCT_LEVEL_ID_LIG + " then -a_item_id else a_item_id end a_item_id, " +
				" case when b_item_type = " + Constants.ITEMLEVELID + " then b_item_id when b_item_type = " + Constants.PRODUCT_LEVEL_ID_LIG + " then -b_item_id else b_item_id end b_item_id " +
				" from product_substitutes " +		
				" WHERE batch_Id = " + batchId + " AND overall_strength >= 2" +
				" order by overall_score desc, a_item_id";
						
		ArrayList<SubstitutePair> pairs = new ArrayList<SubstitutePair>();
		try {
			
			statement = conn.prepareStatement(sql);
			statement.setFetchSize(5000);
			
			rs = statement.executeQuery();
			while (rs.next()){				
				pairs.add(new SubstitutePair(rs.getInt("a_item_id"), rs.getInt("b_item_id")));				
			}
			
			return pairs;
	    }		
		catch (SQLException e) {
			logger.error("getPairList() - SQLException=" + e.getMessage());
			e.printStackTrace();
			return pairs;
	    }
		catch (Exception e) {
			logger.error("getPairList() - Exception=" + e.getMessage());
			e.printStackTrace();
			return pairs;
	    }
	}
	
	
	/**
	 * Updates the group_item_type, group_item_id for the substitute pairs
	 * @param 
	 * @return
	 * @throws 
	 */
	public boolean storeGroups(ArrayList<ArrayList<Integer>> grps) {

		try {
			
			ArrayList<Integer> grp = new ArrayList<Integer>();
			StringBuffer itemIds = new StringBuffer();			
			StringBuffer sql = new StringBuffer();
			Integer groupItemType = 0;
			
			for (int i = 0; i < grps.size(); i++) {
				grp = grps.get(i);
				
				// Build SQL IN clause
				for (int j = 0; j < grp.size(); j++) {
					itemIds.append(grp.get(j));
					if ((j + 1) < grp.size()) itemIds.append(", ");
				}

				if (grp.get(0) < 0) {
					groupItemType = Constants.PRODUCT_LEVEL_ID_LIG;
				}
				else {					
					groupItemType = Constants.ITEMLEVELID;
				}
				sql.append("update product_substitutes " +
						" set group_item_type = " + groupItemType + ", group_item_id = " + Math.abs(grp.get(0)) +
						" where batch_id = " + batchId +
						" and overall_strength >= 2 " + 
						" and ( case when a_item_type = " + Constants.PRODUCT_LEVEL_ID_LIG + " then -a_item_id else a_item_id end in (" + itemIds + ")  " + 
						"    or case when b_item_type = " + Constants.PRODUCT_LEVEL_ID_LIG + " then -b_item_id else b_item_id end in (" + itemIds + ") )");
				
				PristineDBUtil.execute(conn, sql, "Substitutes DAO - Store Groups.");
				conn.commit();
				sql.delete(0, sql.length());
				itemIds.delete(0, itemIds.length());
			}			
			
			return true;
		}
		catch (GeneralException e) {
			logger.error("storeGroups() - GeneralException=" + e.getMessage());
			e.printStackTrace();
			
			return false;
		}
		catch (SQLException e) {
			logger.error("storeGroups() - SQLException=" + e.getMessage());
			e.printStackTrace();
			return false;
		}
		catch (Exception e) {
			logger.error("storeGroups() - Exception=" + e.getMessage());
			e.printStackTrace();
			return false;
		}

	}
	
	/**
	 * Returns a list of store information
	 * @param 
	 * @return
	 * @throws 
	 */
	public String getStoreIdsString(int divisionId) {
		try {
			
			if (storeIdsString == "")
			{
				ArrayList<StoreDTO> storeInfoList = getStoreInfoList(divisionId);
				
				boolean first = true;
				for ( StoreDTO sDto : storeInfoList ) {
					if (first) {
						storeIdsString += sDto.strId;	
						first = false;
					} else {
						storeIdsString += ", " + sDto.strId;
					}
				}
			}
			return storeIdsString;
	    }
		catch (Exception e) {
			logger.error("getStoreIdsString() - Exception=" + e.getMessage());
			e.printStackTrace();
			return storeIdsString;
	    }
	}
	

	private void executeIfPossible(Connection conn, StringBuffer query, String method) throws GeneralException
	{
		try
		{
			PristineDBUtil.execute(conn, query, method);
		}
	    catch (GeneralException e) {
	    	logger.error("getSubstitutes() - GeneralException=" + e.getMessage());
			e.printStackTrace();
	    }
		catch (Exception e) {
			logger.error("getSubstitutes() - Exception=" + e.getMessage());
			e.printStackTrace();
	    }
	}
	
	
	/**
	 * Updates the group_item_type, group_item_id for the substitute pairs
	 * @param 
	 * @return
	 * @throws 
	 */
	public boolean mergeSubstitutesData(String mergeLocationIds) {

		try {
			
			StringBuffer sql = new StringBuffer(); 
			
			String [] mergeLocationIdList = mergeLocationIds.split(",");
			
			sql.append(
					"insert into product_pair_detail " + 
			
					" with  " + 
					" ps1 as ( " + 
					"   select id, location_level_id, location_id, a_item_type, a_item_id, b_item_type, b_item_id, overall_score, overall_strength, group_item_type, group_item_id, batch_id from product_substitutes  " + 
					"     where location_level_id = " + Constants.lOCATION_LEVEL_ID_MARKETAREA + " and location_id = " + mergeLocationIdList[0] + 
					" ),  " + 
					" ps2 as ( " + 
					"   select id, location_level_id, location_id, a_item_type, a_item_id, b_item_type, b_item_id, overall_score, overall_strength, group_item_type, group_item_id, batch_id from product_substitutes  " + 
					"     where location_level_id = " + Constants.lOCATION_LEVEL_ID_MARKETAREA + " and location_id = " + mergeLocationIdList[1] + 
					" ),  " + 
					" ps as ( " + 
					"   select * from ps1 union select * from ps2  " + 
					" ), " + 
					" mps as ( " + 
					"   select case when (NVL(ps1.overall_score, 0) >= nvl(ps2.overall_score, 0) ) then ps1.id else ps2.id end selected_id, " + 
					"       ps1.id m1_id, ps1.location_level_id m1_location_level_id, ps1.location_id m1_location_id, ps1.a_item_type m1_a_item_type, ps1.a_item_id m1_a_item_id, ps1.b_item_type m1_b_item_type, ps1.b_item_id m1_b_item_id, ps1.overall_strength m1_overall_strength,  " + 
					"       ps2.id m2_id, ps2.location_level_id m2_location_level_id, ps2.location_id m2_location_id, ps2.a_item_type m2_a_item_type, ps2.a_item_id m2_a_item_id, ps2.b_item_type m2_b_item_type, ps2.b_item_id m2_b_item_id, ps2.overall_strength m2_overall_strength " + 
					"     from ps1 full join ps2 " + 
					"           on ps1.a_item_type = ps2.a_item_type and ps1.a_item_id = ps2.a_item_id " +
					"          and ps1.b_item_type = ps2.b_item_type and ps1.b_item_id = ps2.b_item_id " + 
					" ) " + 
 
					" SELECT PRODUCT_PAIR_DETAIL_SEQ.NEXTVAL, " + locationLevelId + ", " + locationId + ", ppd.A_ITEM_TYPE, ppd.A_ITEM_ID, ppd.B_ITEM_TYPE, ppd.B_ITEM_ID, " + 
					"     A_SIZE, A_UOM_ID, B_SIZE, B_UOM_ID, A_AVG_NET_UNIT_PRICE, B_AVG_NET_UNIT_PRICE, IS_SAME_CATEGORY, IS_SAME_SUBCATEGORY, IS_SAME_SEGMENT, IS_SAME_LIG,  " + 
					"     AORB_CST_CNT, A_CST_CNT, B_CST_CNT, ANOB_CST_CNT, BNOA_CST_CNT, AANDB_CST_CNT, AAWAB_CST_CNT, AORB_TRX_CNT,  " + 
					"     A_TRX_CNT, B_TRX_CNT, ANOB_TRX_CNT, BNOA_TRX_CNT, AANDB_TRX_CNT, AAWAB_AORB_TRX_CNT, AAWAB_A_TRX_CNT, AAWAB_B_TRX_CNT,  " + 
					"     AAWAB_ANOB_TRX_CNT, AAWAB_BNOA_TRX_CNT, AAWAB_AANDB_TRX_CNT,  " + 
					"     AORB_CST_A_NET_AMT, AORB_CST_B_NET_AMT, A_CST_A_NET_AMT, A_CST_B_NET_AMT, B_CST_A_NET_AMT, B_CST_B_NET_AMT, ANOB_CST_A_NET_AMT,  " + 
					"     BNOA_CST_B_NET_AMT, AANDB_CST_A_NET_AMT, AANDB_CST_B_NET_AMT, AAWAB_CST_A_NET_AMT, AAWAB_CST_B_NET_AMT,  " + 
					"     " + batchId + ", sysdate, ppd.ACTIVE_INDICATOR " + 
					"   FROM product_substitutes ps LEFT JOIN product_pair_detail ppd " + 
					"       ON ps.LOCATION_LEVEL_ID = ppd.LOCATION_LEVEL_ID " + 
					"       AND ps.LOCATION_ID      = ppd.LOCATION_ID " + 
					"       AND ps.A_ITEM_TYPE      = ppd.A_ITEM_TYPE " + 
					"       AND ps.A_ITEM_ID        = ppd.A_ITEM_ID " + 
					"       AND ps.B_ITEM_TYPE      = ppd.B_ITEM_TYPE " + 
					"       AND ps.B_ITEM_ID        = ppd.B_ITEM_ID " + 
					"       AND ps.batch_id         = ppd.batch_id " + 
					"     WHERE ps.ID IN (SELECT selected_id FROM mps) ");
			
				logger.debug(sql);
				PristineDBUtil.execute(conn, sql, "Substitutes DAO - Merge Substitutes.");
				conn.commit();
			
			return true;
		}
		catch (GeneralException e) {
			logger.error("mergeSubstitutesData() - GeneralException=" + e.getMessage());
			e.printStackTrace();
			
			return false;
		}
		catch (SQLException e) {
			logger.error("mergeSubstitutesData() - SQLException=" + e.getMessage());
			e.printStackTrace();
			return false;
		}
		catch (Exception e) {
			logger.error("mergeSubstitutesData() - Exception=" + e.getMessage());
			e.printStackTrace();
			return false;
		}

	}
	
	
}


