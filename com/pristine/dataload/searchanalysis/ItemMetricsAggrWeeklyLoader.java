package com.pristine.dataload.searchanalysis;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.searchanalysis.ItemMetricsSummaryDao;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ItemMetricsAggrWeeklyLoader {

	/**
	 * @param args
	 */
	static Logger logger = Logger.getLogger("ItemMetricsAggrLoader");
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j.properties");
		logger.info("*** Search Item Analysis Weekly Process Begins ***");
		SearchItemWeekly.logCommand("ItemMetricsAggrWeeklyLoader", args);
		
		Date startDate = null;
		String strStartDate ="";
		int locationID = -1;
		int locationLevel = -1;
		int productID = -1;
		int productLevel = -1;

		

		SearchItemWeekly summaryWeekly = null;

		try {
			PropertyManager.initialize("analysis.properties");
			for (int ii = 0; ii < args.length; ii++) {
				String arg = args[ii];

				// Get the start/end date (for date range) from command line
				// Format should MM/dd/YYYY
				if (arg.startsWith("STARTDATE")) {
					strStartDate = arg.substring("STARTDATE=".length());
					
				}

				// Get the Store number from command line
				if (arg.startsWith("LOCATION_ID")) {
					locationID = Integer.parseInt(arg.substring("LOCATION_ID=".length()));
				}

				// Get the Store number from command line
				if (arg.startsWith("LOCATION_LEVEL")) {
					locationLevel = Integer.parseInt(arg.substring("LOCATION_LEVEL=".length()));
				}
				
				if (arg.startsWith("PRODUCT_ID")) {
					productID = Integer.parseInt(arg.substring("PRODUCT_ID=".length()));
				}

				// Get the Store number from command line
				if (arg.startsWith("PRODUCT_LEVEL")) {
					productLevel = Integer.parseInt(arg.substring("PRODUCT_LEVEL=".length()));
				}

			}
			if( strStartDate.equals("") || locationID < 0 || locationLevel < 0 || productID < 0 || productLevel < 0 ){
				logger.info("Invalid Parameters - STARTDATE=mm/dd/yyyy LOCATION_ID=n LOCATION_LEVEL=n PRODUCT_ID=n " +
						"PRODUCT_LEVEL=n");
				logger.info("n should be greater than 0 for all parameters");
				System.exit(0);
			
			}
			logger.debug("Loading Item Metrics Weekly Aggregated");
			ItemMetricsAggrWeeklyLoader imWeeklyLoader = new ItemMetricsAggrWeeklyLoader ();
			imWeeklyLoader.loadData(strStartDate, locationLevel,  locationID, productLevel, productID  );
			
		}catch (Exception e) {
			logger.error("Unexpected Exception ", e);
		}
		catch (GeneralException e) {
			logger.error( "Exception in Data Load ", e);

		} finally {
			logger.info("*** Item Metrics Aggr Process Ends ***");
		}
	}
	
	private void loadData( String strStartDate, int locationLevel, int locationID,
			int productLevel, int productID ) throws GeneralException {

		
		Connection _conn = DBManager.getConnection();
		try{
			//Create the Aggr table
			//create a sequence #
			// See at the end of the code for Scripts

			//Get the calendar Id
			RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();
			RetailCalendarDTO calWeek = objCalendarDao.getCalendarId(_conn, strStartDate, Constants.CALENDAR_WEEK);
			logger.debug( "calendarId = " + calWeek.getCalendarId());
			
			//DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			//Date startDate = dateFormat.parse(strStartDate);

			//Get the location Id string
			//1717,1718, 1719,1723 for testing purpose.
			// 1 is chain
			// 2 id division
			// 6 is zone
			
			ItemMetricsSummaryDao imsdao = new ItemMetricsSummaryDao ();
			
			String locationListSQL = imsdao.getIncludeStoreList(locationLevel, locationID);
			logger.info( "Location List = " + locationListSQL );
			
			String productListSQL = imsdao.getIncludeProductList(productLevel, productID);
			logger.info( "Product List = " + productListSQL );
			//Get the movement Data API
			//Calendar id, locationstr, Prod str, conn
			// use it to calculate the cost
			imsdao.getMovementInfo( _conn, calWeek.getCalendarId(), locationListSQL , productListSQL, locationLevel, locationID );
			
			//Get the Price Data API
			
		
			//Apply the price fields to the above object.
			
			//Insert into the AGGR table
			//Avoid Repetition of code			
		}finally {
			PristineDBUtil.close(_conn);
		}
	}
	
	

}

/*
CREATE TABLE ITEM_MS_WEEKLY_AGGR
(
  "ITEM_METRIC_SUMMARY_WEEKLY_ID" NUMBER,
  "CALENDAR_ID"                   NUMBER,
  "LOCATION_LEVEL_ID"             NUMBER(2,0),
  "LOCATION_ID"                   NUMBER(5,0),
  "PRODUCT_LEVEL_ID"              NUMBER(2,0),
  "PRODUCT_ID"                    NUMBER(6,0),
  "SALE_FLAG"                     CHAR(1 BYTE),
  "PROMOTION_ID"                  VARCHAR2(14 BYTE),
  "REG_PRICE"                     NUMBER(8,2),
  "REG_M_PACK"                    NUMBER(4,0),
  "REG_M_PRICE"                   NUMBER(7,2),
  "SALE_PRICE"                    NUMBER(8,2),
  "SALE_M_PACK"                   NUMBER(4,0),
  "SALE_M_PRICE"                  NUMBER(7,2),
  "FINAL_PRICE"                   NUMBER(8,2),
  "LIST_COST"                     NUMBER(7,2),
  "DEAL_COST"                     NUMBER(7,2),
  "FINAL_COST"                    NUMBER(7,2),
  "REG_REVENUE"                   NUMBER(12,2),
  "SALE_REVENUE"                  NUMBER(12,2),
  "TOT_REVENUE"                   NUMBER(12,2),
  "REG_MOVEMENT"                  NUMBER(10,2),
  "SALE_MOVEMENT"                 NUMBER(10,2),
  "TOT_MOVEMENT"                  NUMBER(10,2),
  "NET_MARGIN"                    NUMBER(10,2),
  "NET_MARGIN_PCT"                NUMBER(6,2),
  "REG_MARGIN"                    NUMBER(10,2),
  "REG_MARGIN_PCT"                NUMBER(6,2),
  "TOT_VISIT_CNT"                 NUMBER(8,0),
  "AVG_ORDER_SIZE"                NUMBER(10,2),
  "SALE_MARGIN"                   NUMBER(10,2),
  CONSTRAINT "ITEM_MS_WKLY_AGGR_PK1" PRIMARY KEY ("CALENDAR_ID", "LOCATION_LEVEL_ID", "LOCATION_ID", "PRODUCT_LEVEL_ID", "PRODUCT_ID") 
)  
TABLESPACE "USERS" ;

CREATE INDEX ITEM_MS_WKLY_AGGR_IDX ON ITEM_MS_WEEKLY_AGGR ( PRODUCT_ID, LOCATION_LEVEL_ID, LOCATION_ID)
  TABLESPACE "USERS" ;

CREATE SEQUENCE "ITEM_MS_WEEKLY_AGGR_SEQ" MINVALUE 1 MAXVALUE 999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER NOCYCLE ;

*/