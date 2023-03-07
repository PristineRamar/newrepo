/*
 * Title: TOPS  Update Item Code 
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.2	04-04-2012	John Britto	Batch update inmplemented 
 * Version 0.1	05-03-2012	John Britto	Initial Version 
 *******************************************************************************
 */


package com.pristine.dataload.salesanalysis;

import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemcodeDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class MDItemUpdateLoader {
	
	 
	static Logger logger = Logger.getLogger("MDItemUpdateLoader");
	private Connection 	_Conn = null;
	private Date _startdate=null;
	private Date _enddate=null;
		
	public MDItemUpdateLoader ()
	{
		try
		{
	        PropertyManager.initialize("analysis.properties");
	        
	        logger.info("Get DB connection");
        
	        _Conn = DBManager.getConnection();
	        _Conn.setAutoCommit(true);
	        
	        logger.info("DB connected");
	        
	    }
		catch (GeneralException gex) {
	        logger.error("Error initializing", gex);
	    }
		catch (Exception ex) {
	        logger.error("Error initializing", ex);
	    }
	}
	
	
	
	public static void main(String[] args) throws GeneralException
	{
		PropertyConfigurator.configure("log4j-movementdaily-itemupdate.properties");
		logger.info(" Item Code Update in Daily Movement Process Begins......");
	
		String storeNumber=null;
	
		Date startDate = null;
		Date endDate = null;
	
		DateFormat _format = new SimpleDateFormat("MM/dd/yyyy");
	 
		MDItemUpdateLoader objItemUpdate = null;
	
		try	{
			for(int ii=0;ii<args.length;ii++)
			{
				
				if(args[ii].startsWith("STORE="))
				{
					storeNumber=args[ii].substring("STORE=".length());
					if(storeNumber==null) {
						logger.error(" Store Number is missing in input");
						
						System.exit(1);
						
					}
				}
				
				if(args[ii].startsWith("STARTDATE="))
				{
					String startDateStr=args[ii].substring("STARTDATE=".length());
					
					if(startDateStr!=null)
					{
						try {
							startDate=_format.parse(startDateStr);
						}
						catch(ParseException exe) {
							logger.error(" Date Parsing Exception ");
							System.exit(1);
						}
					}
					else
					{
						logger.error(" Date is missing in the input ");
						System.exit(1);
					}
				}

			
				if(args[ii].startsWith("ENDDATE="))
				{
					String endDateStr=args[ii].substring("ENDDATE=".length());
					
					if(endDateStr!=null)
					{
						try {
							endDate=_format.parse(endDateStr);
						}
						catch(ParseException exe) {
							logger.error(" Date Parsing Exception ");
							System.exit(1);
						}
					}
				}
			}
			
		logger.debug("Processing Store Number   " + storeNumber);
		logger.debug("Processing Start Date " + startDate  );		
		logger.debug("Processing End Date " + endDate  );		
		
		//Create object
		objItemUpdate = new MDItemUpdateLoader();
		
		//Call update process
		objItemUpdate.Updatemovementitemcode(storeNumber, startDate, endDate);
		
		}
	    catch(Exception exe)
	    {
	    	throw new GeneralException("Error In Main", exe);
	    }
		finally {
		PristineDBUtil.close(objItemUpdate._Conn);
		}
	}

	

	private void Updatemovementitemcode(String storeNo, Date _processFrom, Date _processTo) throws GeneralException {
		
		CachedRowSet resultItemCodeList = null;

		ItemcodeDAO daoobject = new ItemcodeDAO();
	 
		
		try	{
			ComputeDateRanges(_processFrom, _processTo);
				
			logger.debug("Processing Date " + _startdate + " ~ " + _enddate);
			logger.debug("Get the UPC from Movement Daily");
			resultItemCodeList = daoobject.GetItemcodeList(_Conn, storeNo, _startdate, _enddate);
			logger.debug("Total processing UPC count " + resultItemCodeList.size());
			
			logger.debug("Update process begins...");		
			
			if(resultItemCodeList!=null){

				daoobject. UpdateItemcode ( _Conn, resultItemCodeList
						 ,storeNo, _startdate, _enddate);
				
			}
		}
		catch(Exception exe)
		{
			throw new GeneralException("Update Item code Method Error",exe);
		}
	}

	 
	private void ComputeDateRanges(Date fromDate, Date toDate) throws GeneralException {
		
		Calendar cal = Calendar.getInstance();
		// find the date with start time
		 
		cal.setTime(fromDate);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		_startdate = cal.getTime();
		// find the date with end time
		 
		Calendar Ecal = Calendar.getInstance();
		if (toDate !=null)
			Ecal.setTime(toDate);
		else
		Ecal.setTime(_startdate);

		Ecal.set(Calendar.HOUR_OF_DAY, 24 - 1);
		Ecal.set(Calendar.MINUTE, 59);
		Ecal.set(Calendar.SECOND, 59);
		Ecal.set(Calendar.MILLISECOND, 900);
		_enddate = Ecal.getTime();
					
		}

}
