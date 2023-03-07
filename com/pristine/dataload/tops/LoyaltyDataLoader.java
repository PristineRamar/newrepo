/*
 * Title: TOPS Data Update (Program to update Loyalty Group ID in CUSTOMER_LOYALTY_INFO)
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 1.0  02/02/2016  John Britto     Initial version
 *******************************************************************************
 */

package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.csvreader.CsvReader;
import com.pristine.dao.DBManager;
import com.pristine.dao.customer.CustomerDAO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class LoyaltyDataLoader extends PristineFileParser
{
	static Logger logger = Logger.getLogger("LoyaltyDataSetup");
  	Connection _Conn = null;
	private int _StopAfter = -1;
	private int updateDBAfter = 25000;
	private int writelogAfter = 25000;
	
	public LoyaltyDataLoader ()
	{
		super ("analysis.properties");
		try {
			//Create DB connection
			_Conn = DBManager.getConnection();
		}
		catch (GeneralException ex) {
			logger.error("Error while initializing - " + ex.toString());
		}
		
		//Get Loyalty commit count from configuration
		try{
			updateDBAfter = Integer.parseInt(PropertyManager.getProperty("LOYALTY_COMMIT_COUNT"));
			logger.debug("Update DB for every..." + updateDBAfter);
		}
		catch (Exception ex) {
		}
	}
	
	/*
	 * Main  Method for the batch 
	 * Argument 1 : Sub folder name
	 */
	
	public static void main(String[] args)
	{
		logCommand (args);
    	String subFolder = null;
    	boolean updateTransactions = false;

		for (int ii = 0; ii < args.length; ii++) {
			
			String arg = args[ii]; 
			
			if (arg.startsWith("SUBFOLDER")) {
				subFolder = arg.substring("SUBFOLDER=".length());
			}
		}
		
		LoyaltyDataLoader dataload = new LoyaltyDataLoader();
		dataload._UpdateTransactions = updateTransactions;
		dataload.process(subFolder);
    }
	
	public void processRecords(List listobj) throws GeneralException
	{
	}
	
	@SuppressWarnings("unchecked")
	private void process (String subFolder)
	{
		PropertyConfigurator.configure("log4j-loyalty-data-update.properties");
		
		// Update_Transactions
		String msg = "Loyalty Group Data Load started" + (_UpdateTransactions == true ? ": updating Transactions" : "");
		logger.info(msg);
		
        try
        {
		
			try {
				_StopAfter = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
			}
			catch (Exception ex) {
				_StopAfter = -1;
			}
			
			//getzip files
			ArrayList<String> zipFileList = getZipFiles (subFolder);
			
			//Start with -1 so that if any reqular files are present, they are processed first
			int nFilesProcessed = 0;
			int curZipFileCount = -1;
			boolean processZipFile = false;
			String zipFilePath = getRootPath() + "/" + subFolder;
			do
			{
				ArrayList<String> files = null;
				boolean commit = true;
			
				try
				{
					if( processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;

					files = getFiles (subFolder);
					if ( files != null && files.size() > 0 )
					{
						for ( int ii = 0; ii < files.size(); ii++ ) {
							String file = files.get(ii);
							nFilesProcessed++;
							commit = readAndLoadLoyaltyGroup(file, ',');
							
							if (commit){
								logger.info("Commit Transaction");
								_Conn.commit();
							}
							else{
								logger.info("Rollback Transaction");
								_Conn.rollback();
							}
						}
					}
				} catch (GeneralException ex) {
			        logger.error("Inner Exception - GeneralException", ex);
					logger.info("Rollback Transaction");
					_Conn.rollback();
			        commit = false;
			    } catch (Exception ex) {
					logger.info("Rollback Transaction");
			        logger.error("Inner Exception - JavaException", ex);
					_Conn.rollback();
			        commit = false;
			    }
		    
			    if( processZipFile){
			    	PrestoUtil.deleteFiles(files);
			    	files.clear();
			    	files.add(zipFileList.get(curZipFileCount));
			    }
				if( commit ){
					PrestoUtil.moveFiles(files, zipFilePath + "/" + Constants.COMPLETED_FOLDER);
				}
				else{
					PrestoUtil.moveFiles(files, zipFilePath + "/" + Constants.BAD_FOLDER);
				}
				
				curZipFileCount++;
				processZipFile = true;
				
        	} while (curZipFileCount < zipFileList.size());

			if ( nFilesProcessed > 0 )
				logger.info("Loyaly Data Update successfully completed");
			else 
				logger.info("No Loyaly Data file found");

        	PristineDBUtil.close(_Conn);
		}
        catch (Exception ex) {
            logger.error("Error", ex);
        }
        catch (GeneralException ex) {
            logger.error("Error", ex);
        }
	}
	
	private boolean readAndLoadLoyaltyGroup (String fileName, char delimiter) throws Exception, GeneralException {
		logger.info("Processing file: " + fileName);

		//Modified by RB
		HashMap<String, Integer> _customerLoyaltyMap = new HashMap<String, Integer>();
		boolean ret;
		
        CsvReader reader = readFile(fileName, delimiter);
        String line[];
        
        CustomerDAO customerDao = new CustomerDAO();
        int count = 0, countFailed = 0;
        
        try
        {
	        while (reader.readRecord())
	        {
	            line = reader.getValues();
	            count++;
	            logger.debug("Processing record count....." + count);
	            if ( line.length > 0 )
	            {
	            	try {
						// 0: Chain
	            		// 1: Loyalty Group Id
	            		// 2: Loyalty Card Number 
	            		// 3: Loyalty Type (1:Phone, 2:Loyalty Card, 3:E-mail Id) 

	            		int loyatyGroupNo =Integer.parseInt(line[1].trim());
	            		String loyatyCardNo = line[2].trim();
	            		int loyaltyCardType = Integer.parseInt(line[3].trim());
	            		
	            		if (loyaltyCardType == Constants.LOYALTY_CARD_TYPE_CARD){
		            		if (loyatyCardNo.length() == 11)
		            			loyatyCardNo = "0" + line[2].trim();
		            		else if (loyatyCardNo.length() == 10)
		            			loyatyCardNo = "00" + line[2].trim();
		            		else if (loyatyCardNo.length() == 9)
		            			loyatyCardNo = "000" + line[2].trim();
	            		}
	            		
						if (!_customerLoyaltyMap.containsKey(loyatyCardNo))
							_customerLoyaltyMap.put(loyatyCardNo, loyatyGroupNo);
						
					} catch (Exception e) {
						logger.error("Error occured in row "+ count + " : "+  e);
						countFailed++;
					}
	            	
                    if ( count % writelogAfter == 0 ) {
        				logger.info("Processed " + String.valueOf(count) + " records");
                    }
	            	
	            	if ( _customerLoyaltyMap.size() % updateDBAfter == 0)
	            	{
	            		logger.debug("Update into DB");
	            		//call DB update
	            		customerDao.updateLoyaltyGroupData(_Conn, _customerLoyaltyMap);
	            		//Reset Array list
	            		_customerLoyaltyMap.clear();
	            		
	            	}
	            	
                    if ( _StopAfter > 0 && count >= _StopAfter ) {
                    	break;
                    }
	            }
	        }
        	if ( _customerLoyaltyMap.size() > 0)
        	{
        		//call DB update
        		customerDao.updateLoyaltyGroupData(_Conn, _customerLoyaltyMap);
        	}
        	
			logger.info("Processed " + String.valueOf(count) + " records.");
			logger.info("Error in " + String.valueOf(countFailed) + " records.");
	
			ret = countFailed < count;
        }
        catch (Exception ex) {
        	logger.fatal("Fatal error", ex);
        	ret = false;
        	throw ex;
        }
        finally {
        	if( reader != null){
        		reader.close();
        	}
        }
     
        return ret;
	}
	

	private static void logCommand (String[] args)
	{
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: LoyaltyDataLoader ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	}
	
	private boolean _UpdateTransactions = false;
}
