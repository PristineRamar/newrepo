/**
 * 
 */
package com.pristine.analytics.customer;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.dao.DBManager;
import com.pristine.dao.customer.CustomerSalesSummaryDAO;
import com.pristine.dto.StoreDTO;


/**
 * @author JK
 *
 */
public class TrxSummary {

	static Logger logger = Logger.getLogger("TrxSummary");
	
	private Connection _conn = null; // DB connection
	
	private String analysisPeriodStart = null;
	private String analysisPeriodEnd = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j-customer-trxsummary.properties");

		logger.info("main() - Started");		
		logger.info("main() - Comment: Takes input from RAMDISK rd_transaction_log table.");
		logger.info("main() - Comment: Populates item_cnt, visit_cnt and NEW fields visit_days_cnt and visit_cnt_segment.  Will not work for old table design!");
		logger.info("main() - Comment: Implemented analytic functions like PARTITION BY.  Can be slower than before!");
		
		logCommand(args);
		
		String strAnalysisPeriodStart = "";
		String strAnalysisPeriodEnd = "";
		String locationLevelId = "";
		String locationId = "";


		TrxSummary tS = new TrxSummary();
		logger.info("main() - TrxSummary object created.");
		logger.info("main() - Package.SpecificationTitle=" + tS.getClass().getPackage().getSpecificationTitle());
		logger.info("main() - Package.SpecificationVersion=" + tS.getClass().getPackage().getSpecificationVersion());

		try {
			
			// Read the 2 input parameters 
			
			//Example ANALYSIS_DATE_START=10/25/2014 ANALYSIS_DATE_END=10/31/2014 NOTES=RealOne
			
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];
				
				// Get the Location type from command line
				if (arg.startsWith("LOCATION_LEVEL_ID")) {
					//storeNum = arg.substring("STORE_ID=".length());
					locationLevelId = arg.substring("LOCATION_LEVEL_ID=".length());
				}

				// Get the Location from command line
				if (arg.startsWith("LOCATION_ID")) {
					//storeNum = arg.substring("STORE_ID=".length());
					locationId = arg.substring("LOCATION_ID=".length());
				}

				// Get the Analysis Start Date from command line
				if (arg.startsWith("ANALYSIS_DATE_START")) {
					strAnalysisPeriodStart = arg.substring("ANALYSIS_DATE_START=".length());
				}
				
				// Get the Analysis End Date from command line
				if (arg.startsWith("ANALYSIS_DATE_END")) {
					strAnalysisPeriodEnd = arg.substring("ANALYSIS_DATE_END=".length());
				}
			}
			
			tS.analysisPeriodStart = strAnalysisPeriodStart;
			tS.analysisPeriodEnd = strAnalysisPeriodEnd;
			
			tS.doTrxSummary();

		} catch (Exception e) {
			logger.error("main() - " + e.getMessage());

		} finally {
			PristineDBUtil.close(tS._conn);
			logger.info("main() - Ended");
		}
	}

	
	public TrxSummary(){
		try {
			PropertyManager.initialize("analysis.properties");
			_conn = DBManager.getConnection();

		} catch (GeneralException exe) {
			logger.error("TrxSummary() - " + exe);
			System.exit(1);
		}

	}
	
	public void doTrxSummary(){
		logger.info("doTrxSummary() - Started");
		try
		{

		    boolean result = false;
		    String customerIdString = null;		    

		    CustomerSalesSummaryDAO cSSD = new CustomerSalesSummaryDAO();
		    cSSD.conn = _conn;
 
	    	// Process for all customers

    		logger.info("doTrxSummary() - Processing for ALL customers who purchased between " + analysisPeriodStart + " and " + analysisPeriodEnd + ".");
    		
    		// (1) Analysis and (2) stores the transaction summary in database
    		result = cSSD.performTrxSummary(analysisPeriodStart, analysisPeriodEnd, 
    				customerIdString);
    		
    		if (!result)
    			logger.info("doTrxSummary() - FAILED for ALL customers!");
		    
		    
		    /*
		    List<Integer> customerIds = cSSD.getCustomerIds(analysisPeriodStart, analysisPeriodEnd);
	    	int customerCount = customerIds.size();
	    	
	    	if (customerCount > 0)
	    	{
	    		logger.info("doTrxSummary() - Processing for customers.. customerCount=" + customerIds.size());	
	    	}
	    	{
	    		logger.warn("doTrxSummary() - No customer record for processing!  customerCount=" + customerIds.size());
	    	}
	    	for (int i = 0; i < customerCount; i++)
	    	{
				boolean first = true;
				int bufferSize = 500;
				
				for ( int j = i + 1; j <= (i + bufferSize); j++) {
					if (first) {
						customerIdString += customerIds.get(j);	
						first = false;
					} else {
						customerIdString += ", " + customerIds.get(j);
					}
				}
				i = i + bufferSize - 1;
	    		
	    		logger.info("doTrxSummary() - Processing " + (i + 1) + " of " + customerCount + " for customerid=" + customerIds.get(i) + "..");
	    		
	    		// (1) Analysis, (2) filters and (3) stores the transaction summary in database
	    		result = cSSD.performTrxSummary(analysisPeriodStart, analysisPeriodEnd, 
	    				customerIdString);
	    		
	    		if (!result)
	    			logger.info("doTrxSummary() - FAILED for customerId=" + customerIds.get(i) + "!");
	    	}
			*/
		    /*
		    //500000054	906799924
		    
		    Integer maxCustomerId = 906800000;
	    	for (int i = 500000000; i < maxCustomerId; i++)
	    	{
	    		logger.info("doTrxSummary() - Processing for customer_id from " + (i + 1) + " to " + (i + 500) + "..");
	    		
	    		// (1) Analysis, (2) filters and (3) stores the transaction summary in database
	    		result = cSSD.performTrxSummary(analysisPeriodStart, analysisPeriodEnd, i);
	    		
	    		if (!result)
	    			logger.info("doTrxSummary() - FAILED for customer_id from " + (i + 1) + " to " + (i + 500) + "..");
	    		
	    		i = i + 500;
	    	}
	    	*/
		}catch(Exception e){
			logger.error("doTrxSummary() - Exception=" + e.getMessage());
			e.printStackTrace();
		}finally {
			logger.info("doTrxSummary() - Ended");
		}
	}
	
	// TODO why is this method here???
	/**
R	 ***************************************************************** 
	 * Static method to log the command line arguments
	 ***************************************************************** 
	 */

	private static void logCommand(String[] args) {
		StringBuffer sb = new StringBuffer("Arguments=");
		for (int ii = 0; ii < args.length; ii++) {
			if (ii > 0)
				sb.append(' ');
			sb.append(args[ii]);
		}

		logger.info("logCommand() - " + sb.toString());
	}
}
