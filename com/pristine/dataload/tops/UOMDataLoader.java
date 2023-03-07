/*
 * Title: TOPS Data Update (Program to update UOM ID in Item Lookup)
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 1.0  08/24/2015  John Britto     Initial version
 *******************************************************************************
 */

package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.csvreader.CsvReader;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.UOMDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class UOMDataLoader extends PristineFileParser
{
	static Logger logger = Logger.getLogger("UOMDataSetup");
  	Connection _Conn = null;
	private int _StopAfter = -1;
	private HashMap<String, Integer> _itemUOMHashMap = new HashMap<String, Integer>();
	private HashMap<String, Integer> _noUOMHashMap = new HashMap<String, Integer>();
	private int updateDBAfter = 25000;
	private int writelogAfter = 25000;
	
	public UOMDataLoader ()
	{
		super ("analysis.properties");
		try {
			//Create DB connection
			_Conn = DBManager.getConnection();
		}
		catch (GeneralException ex) {
			logger.error("Error while initializing - " + ex.toString());
		}
		
		//Get item commit count from configuration
		try{
			updateDBAfter = Integer.parseInt(PropertyManager.getProperty("UOM_COMMIT_COUNT"));
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
		
		UOMDataLoader dataload = new UOMDataLoader();
		dataload._UpdateTransactions = updateTransactions;
		dataload.process(subFolder);
    }
	
	public void processRecords(List listobj) throws GeneralException
	{
	}
	
	@SuppressWarnings("unchecked")
	private void process (String subFolder)
	{
		PropertyConfigurator.configure("log4j-uom-data-update.properties");
		
		// Update_Transactions
		String msg = "UOM Data Load started" + (_UpdateTransactions == true ? ": updating Transactions" : "");
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
						//Get the UOM and Id
						getUOMList();	
						
						for ( int ii = 0; ii < files.size(); ii++ ) {
							String file = files.get(ii);
							nFilesProcessed++;
							commit = readAndLoadUOM(file, '|');
							
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
				logger.info("UOM Data Update successfully completed");
			else 
				logger.info("No UOM Data file found");

        	PristineDBUtil.close(_Conn);
		}
        catch (Exception ex) {
            logger.error("Error", ex);
        }
        catch (GeneralException ex) {
            logger.error("Error", ex);
        }
	}
	
	private boolean readAndLoadUOM (String fileName, char delimiter) throws Exception, GeneralException {
		logger.info("Processing file: " + fileName);

		//Modified by RB
		HashMap<String, UOMDataDTO> _itemUOMOutMap = new HashMap<String, UOMDataDTO>();
		boolean ret;
		
        CsvReader reader = readFile(fileName, delimiter);
        String line[];
        
        ItemDAO itemDao = new ItemDAO();
        int count = 0, countFailed = 0, countSkipped = 0, uomMissedCount = 0;
        _noUOMHashMap = new HashMap<String, Integer>();
        
        try
        {
	        while (reader.readRecord())
	        {
	            line = reader.getValues();
	            count++;
	            //logger.debug("Processing record count....." + count);
	            if ( line.length > 0 )
	            {
	            	try {
						// 0: Retail Item Code	            		
	            		String retailItemCode = line[0].trim();

						// 1: Source Vendor
	            		// 2: Source Item
	            		// 3: Item Name
	            		// 4: UPC
	            		// 5: Size
	            		// 6: UOM
	            		// 7: PrePrice
	            		
						String uom = line[6].trim();
						
						//Added by RB
						double PrePrice = Double.parseDouble(line[7].trim());
						
						int uomId = 0;
						if (_itemUOMHashMap.containsKey(uom)){
							uomId = _itemUOMHashMap.get(uom);
						
					    //Added by RB
						UOMDataDTO UOMDataDTO = new UOMDataDTO();
						UOMDataDTO.uomid = uomId;
						UOMDataDTO.preprice = PrePrice;
							
						if (!_itemUOMOutMap.containsKey(retailItemCode))
						     //Modified by RB
							_itemUOMOutMap.put(retailItemCode, UOMDataDTO);
						}
						else{
							uomMissedCount ++;
							
							if (_noUOMHashMap.containsKey(uom))
								_noUOMHashMap.put(uom, _noUOMHashMap.get(uom)+ 1);
							else
								_noUOMHashMap.put(uom, 1);
						}
						
					} catch (Exception e) {
						logger.error("Error occured in row "+ count + " : "+  e);
						countFailed++;
					}
	            	
                    if ( count % writelogAfter == 0 ) {
        				logger.info("Processed " + String.valueOf(count) + " records");
                    }
	            	
	            	if ( _itemUOMOutMap.size() % updateDBAfter == 0)
	            	{
	            		logger.debug("Update into DB");
	            		//call DB update
	            		itemDao.updateUOMData(_Conn, _itemUOMOutMap);
	            		//Reset Array list
	            		_itemUOMOutMap.clear();
	            		
	            	}
	            	
                    if ( _StopAfter > 0 && count >= _StopAfter ) {
                    	break;
                    }
	            }
	        }
        	if ( _itemUOMOutMap.size() > 0)
        	{
        		//logger.debug("Update into DB");
        		//call DB update
        		itemDao.updateUOMData(_Conn, _itemUOMOutMap);
        	}
        	
			logger.info("Processed " + String.valueOf(count) + " records.");
			logger.info("Error in " + String.valueOf(countFailed) + " records.");
			logger.info("Skipped " + String.valueOf(countSkipped) + " records.");
			logger.info("UOM not exist for " + uomMissedCount + " records");
			
			if (_noUOMHashMap.size() > 0){
				//StringBuilder sbm = new StringBuilder();
				for (Map.Entry<String, Integer> entry : _noUOMHashMap.entrySet()) {
					//if (sbm.length() > 0)
					//	sbm.append(", ");
					//sbm.append(entry.getKey());
					logger.info("UOM ID for:" + entry.getKey() + " not exist for:" + entry.getValue());
				}
				//logger.info("UOM not exist for " + sbm.toString());
			}
			
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
	
	private void getUOMList()
	{
		ItemDAO objItem = new ItemDAO();
		
		try {
			logger.info("Preload Item code and UPC for all active items");
			_itemUOMHashMap = objItem.getUOMAndName(_Conn);
			logger.info("Total UOM count: " + _itemUOMHashMap.size());
		
		} catch (GeneralException e) {
			logger.error(e.getCause());
		}
		
	}
	
	private static void logCommand (String[] args)
	{
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: MovementDataLoad ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	}
	
	private boolean _UpdateTransactions = false;
}
