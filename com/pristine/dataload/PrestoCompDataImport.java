package com.pristine.dataload;
/*
 * This program load Competitive data provided by external agency
 * Used for TOPS
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

/*
 * Deployment notes
 * 1) Added properties for Header row and Data Separator
 * 2) Input file is a path from Directory
 * 3) Comp Store Id.
 * 4) Create folder BadData and CompletedData
 * 
 */
public class PrestoCompDataImport extends PristineFileParser{

	private static Logger  logger = Logger.getLogger("PrestoCompDataLoad");
	
	HashMap <String, Integer> rawUOMList = new HashMap <String, Integer>();  
	HashMap <String, String> uomMap;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j-comp-data.properties");
		logger.info("Data Load started");
		ArrayList <String> fileList = new ArrayList <String> ();
		// Changes to include Zip file processing for Competitor Data Load
		ArrayList <String> zipFileList = new ArrayList <String> ();
		
		PropertyManager.initialize("analysis.properties");
		String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		Connection conn = null;
		PrestoCompDataImport topsDataloader = new PrestoCompDataImport();
		try{
			conn = DBManager.getConnection();
			if (args.length > 2){
				logger.debug("Insufficient Arguments - Root Path and file name");
				System.exit(1);
			}
			logger.debug( "arg[0] = " +  args[0]);
			if( args.length > 0){
				rootPath = rootPath + "/"+ args[0];
			}
			logger.debug("Root Path is " + rootPath);
			 
			if( args.length== 2){
				String fileName = rootPath;
				if( !rootPath.equals(""))
					fileName = fileName +"/";
				fileName = fileName + args[1];
				fileList.add(fileName);
			}
			else{
				File dir = new File(rootPath);

				String[] children = dir.list();
				if (children != null) {
				    for (int i=0; i<children.length; i++) {
				        // Get filename of file or directory
				        String filename = children[i];
				        //logger.debug(filename);
				        if( filename.contains(".csv") || filename.contains(".txt"))
				        	fileList.add(rootPath + "/" +filename);
				        // Changes to include Zip file processing for Competitor Data Load - Starts
				        else if(filename.contains(".zip"))
				        	zipFileList.add(rootPath + "/" +filename);
				        // Changes to include Zip file processing for Competitor Data Load - Ends
				    }
				}
				
			}
			
			if( fileList.isEmpty() && zipFileList.isEmpty()){
				logger.info("No file present to load");
				System.exit(1);
			}
			
			if(!fileList.isEmpty())
				for(String file : fileList){
					logger.info("Loading file:" + file);
					topsDataloader.compDataLoad(conn,file);
				}

			
		}catch(GeneralException ge){
			logger.error("Error in Load", ge);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			PrestoUtil.moveFiles(fileList, rootPath + "/" + Constants.BAD_FOLDER);
			System.exit(1);
		}catch(Exception e){
			logger.error("Error in Load", e);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			PrestoUtil.moveFiles(fileList, rootPath + "/" + Constants.BAD_FOLDER);
			System.exit(1);
		}
		

		try{
			if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE")){
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Data Load");
				PrestoUtil.moveFiles(fileList, rootPath + "/" + Constants.COMPLETED_FOLDER);
			}
			else{
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "Data Load");
			}
		}catch(GeneralException ge){
			logger.error("Error in commit", ge);
			System.exit(1);
		}
		
		// Changes to include Zip file processing for Competitor Data Load
		topsDataloader.processZipFiles(conn, zipFileList, rootPath, args[0]);
		logger.info("Data Load successfully completed");

	}

	/**
	 * Unzips files and loads competitive data
	 * @param conn			Database Connection
	 * @param zipFileList	List of Zip files
	 * @param relativePath	Path in which the zip files are present
	 */
	public void processZipFiles(Connection conn, ArrayList<String> zipFileList, String rootPath, String relativePath){
		try{
			for(String zipFileName : zipFileList){
				PrestoUtil.unzip(zipFileName, rootPath);
				ArrayList<String> fileList = getFiles(relativePath);
				for(String file : fileList){
					logger.info("Loading file:" + file);
					compDataLoad(conn,file);
				}
				PrestoUtil.deleteFiles(fileList);
		    	fileList.clear();
		    	fileList.add(zipFileName);
		    	logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Data Load");
		    	PrestoUtil.moveFiles(fileList, rootPath + "/" + Constants.COMPLETED_FOLDER);
			}
		}catch(GeneralException ge){
			logger.error("Error in Loading data from Zip files", ge);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			PrestoUtil.moveFiles(zipFileList, rootPath + "/" + Constants.BAD_FOLDER);
		}finally{
			PristineDBUtil.close(conn);
		}
	}
	
	public void compDataLoad(Connection conn,String fileName)throws GeneralException{
		
		//Read the first line
		//Read the rest of the lines
		//Build Comp Data List
		//Identify the store
		//Create Schedule (delete if it exists)
		//Insert or update....
		//Run Performance Stats

		CsvReader reader = null;
		int compStrId = -1;
		int reccount = 0, newcount = 0,notCarried=0, exceptionCount =0;
		HashSet<Integer> schedulesProcessed = new HashSet <Integer>(); 
		String separator = PropertyManager.getProperty("DATALOAD.SEPARATOR", ",");
		int lineCount = 0;
		try {
	    	
			reader = new CsvReader(new FileReader(fileName));
			reader.setDelimiter(separator.charAt(0));
			
	    	String line[]; 
	    	
	    	ArrayList <CompetitiveDataDTO> compDataList = new  ArrayList <CompetitiveDataDTO>();
	    	CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(conn);
	    	String checkUser = PropertyManager.getProperty("DATALOAD.CHECK_USER_ID", Constants.PRESTO_LOAD_USER);
	    	String checkList = PropertyManager.getProperty("DATALOAD.CHECK_LIST_ID", Constants.PRESTO_LOAD_CHECKLIST);
	    	boolean headerPresent = true;
	    	if( PropertyManager.getProperty("DATALOAD.HEADER_PRESENT", "true").equalsIgnoreCase("false"))
	    		headerPresent = false;
	    	compDataDAO.setParameters(checkUser, checkList);
	    	ItemDAO itemdao = new ItemDAO (); 
	    	
	    	//Populate UOM Map ToDO
	    	uomMap = itemdao.getUOMList(conn, "PRESTO_COMPDATA");
	    	int plItemCount = 0;
	    	while (reader.readRecord()) {
	    		line = reader.getValues();
	    		
	    		lineCount++;
	    		if( headerPresent && lineCount == 1)
	    			continue;
    			CompetitiveDataDTO compData = parseCompDataRecord(line);
    			if( compData != null){
    				compData.newUOM = "";
    				compDataList.add(compData);
    				boolean insert = false;
					try{
						insert = compDataDAO.insertCompData(conn, compData, false);
						
						if( !insert){
							//add it to the Not Carried record.
							//compDataDAO.insertNonCarriedItem(conn, compData);
						}
						if( !schedulesProcessed.contains(compData.scheduleId))
							schedulesProcessed.add(compData.scheduleId);
					}
					catch(GeneralException ge){
						if( ge.getErrorLevel() == GeneralException.LEVEL_SEVERE){
							logger.error(ge.getMessage());
							logger.error("Item is " + compData.itemName);
							throw ge;
						}
						exceptionCount++;
						continue;
					}
					finally{
						
					}
//						
					if(insert){
						newcount++;
						if(compData.plFlag != null){
							if(compData.plFlag.equals("Y")){
								plItemCount++;
							}
						}
					}
					else{
						notCarried++;
					}
    				
	    		}
    			
    			// Janani - June 20 2012 - Changes to log progress and commit transactions
    			if(lineCount % Constants.MAX_ITEMS_ALLOWED == 0){
    				logger.info("Comitting : " + Constants.MAX_ITEMS_ALLOWED + " records. ");
    				PristineDBUtil.commitTransaction(conn, "Comp Data Load");
    			}
    			
    			if(lineCount % Constants.LOG_RECORD_COUNT == 0){
    				logger.info("Processed " + lineCount + " records");
    			}
	    		
/*    			if( lineCount >= 10)
	    			break; */ 
	    	  
	    	}

	    	if( headerPresent) 
	    		lineCount = lineCount -1;
	    	
	    	logger.info("No of Lines = " + lineCount);
	    	logger.info("No of Comp Data = " + compDataList.size());
			logger.info("New record count "+ newcount);
			logger.info("Not Carried record count "+ notCarried);
			logger.info("Ignored items count "+ exceptionCount);
			logger.info("No of PL items = "+ plItemCount);
			
			//Now call performance schedule
			PriceChangeStats pcs = new PriceChangeStats();
			String loadLIGValue = PropertyManager.getProperty("DATALOAD.ROLLUPLIG", "FALSE");
			String priceIndexFlagUpdate = PropertyManager.getProperty("DATALOAD.UPDATEPRICEINDEXFLAG", "FALSE");
			logger.debug("LIG Load Flag value is " + loadLIGValue);
			CompDataLIGRollup ligRollup = new CompDataLIGRollup (); 
			CompDataPISetup piDataSetup = new CompDataPISetup ();
			
			PristineDBUtil.commitTransaction(conn, "Comp Data Setup");
			for ( int schId : schedulesProcessed){
				pcs.calculatePriceChangeStats(conn,schId, plItemCount);
				if(loadLIGValue.equalsIgnoreCase("true")) {
					ligRollup.LIGLevelRollUp(conn, schId);
					piDataSetup.setupSchedule(conn, schId);
				}
				
				try{
					if( priceIndexFlagUpdate.equalsIgnoreCase("true")){
						itemdao.updatePriceIndexFlag( conn, schId);
						logger.info( "Price Index Flag Updated for Schedule " + schId);
					}
				}catch(Exception e){
					logger.error( "Error in setting Price Index Enabled Flag ", e);
				}
				
				
				PristineDBUtil.commitTransaction(conn, "Comp Data LIG");
				
			}
			
	    	//printUOMList();
	    	
	    } catch (FileNotFoundException e) {
	    	throw new GeneralException( "File Not Found Exception in Comp Data Load", e);
	    } catch (IOException e) {
	    	logger.info("Error at Line = " + lineCount);
	    	throw new GeneralException( "IO Exception in Comp Data Load", e);
	    }finally{
	    	try{
	    		if( reader != null)
	    			reader.close();
	    	}catch(Exception e){
	    		
	    	}
	    }
	 }

	private CompetitiveDataDTO parseCompDataRecord(String line[]) throws GeneralException {
                    /*
                     * 0 - Str Num
                     * 1 - Check Date
                     * 2 - NA (Retailer Item code)
                     * 3 - Item Desc
                     * 4 - Reg Qty
                     * 5 - Reg Price
                     * 6 - Size UOM
                     * 7 - UPC
                     * 8 - Outside Ind
                     * 9 - Additional Comments
                     * 10 - Sale Qty
                     * 11 - Sale Price
                     * 12 - Sale Date
                    */

                    //logger.debug("***** New Record ****");
                    CompetitiveDataDTO compData = new CompetitiveDataDTO();
                    compData.compStrNo = line[0];
                    
                    //Cast the UPC. Expected length of the UPC is 11, but there are
                    //UPC with 10 digit. So change the UPC to 11 by prefixing 0 to it
                    //If the second parameter of the function is true, then it makes the UPC to 11 digit
                    if(line[7].length() == 10)
                    	compData.upc = PrestoUtil.castUPC(line[7], true);
                    else
                    	compData.upc = line[7];
                   // logger.debug("UPC = " + compData.upc);

                    //There are rows with Reg quantity and Reg price with empty values. 
                    //Assign 0 if reg quantity/reg price is empty or null                    
                    
                    if( line[4] != null && !line[4].equals(""))
                    	compData.regMPack = Integer.parseInt(line[4]);
                    else
                        compData.regMPack = 0;
                   
                    if( line[5] != null && !line[5].equals(""))
                    	compData.regPrice = Float.parseFloat(line[5]);
                    else
                        compData.regPrice = 0;

                    if( compData.regMPack <= 1){
                            compData.regMPack = 0;
                            compData.regMPrice =0;
                    }
                    else{
                            compData.regMPrice = compData.regPrice ;
                            compData.regPrice = 0;
                    }

                    if( line[10] != null && !line[10].equals(""))
                    	compData.saleMPack = ((Float)(Float.parseFloat(line[10]))).intValue();
                    else
                    	compData.saleMPack = 0;
                    
                    if( line[11] != null && !line[11].equals(""))
                        compData.fSalePrice = Float.parseFloat(line[11]);
                    else
                    	compData.fSalePrice = 0;

                    if( compData.saleMPack <= 1){
                            compData.fSaleMPrice = 0;
                            compData.saleMPack = 0;
                    }
                    else{
                            compData.fSaleMPrice = compData.fSalePrice;
                            compData.fSalePrice=0;
                    }

                    compData.outSideRangeInd = line[8];
                    compData.saleInd = "N";
                    compData.priceNotFound ="N";
                    compData.itemNotFound = "N";
                    if( compData.regPrice == 0 && compData.regMPrice == 0
                            && (compData.fSalePrice > 0 || compData.fSaleMPrice > 0) )
                            compData.priceNotFound ="Y";
                    else if (compData.regPrice == 0 && compData.regMPrice == 0 )
                            compData.itemNotFound = "Y";

                    if( compData.fSalePrice > 0 || compData.fSaleMPrice > 0)
                            compData.saleInd = "Y";

                    //Format the date
                    compData.checkDate = line[1];
                    RDSDataLoad.setupWeekStartEndDate(compData);

                    splitUOMSize( compData ,line[6]);

                    compData.itemName = line[3].trim();
                    if(compData.itemName == "")
                    {
                    	compData.itemName = line[13].trim();
                    }
                    compData.itemName  = compData.itemName .replaceAll("'", "''");
                    //logger.debug("Item Name = " + compData.itemName);
                    
                    try{
                    	String plFlag = line[14];
                    	if(plFlag != null && !Constants.EMPTY.equals(plFlag)){
                    		compData.plFlag = plFlag;
                    	}else{
                    		compData.plFlag = "N";
                    	}
                    }catch(Exception e){
                    	
                    }


                   // logger.debug("***** End Record ****");
                    return compData;
                    //return null;
		
		
	}
	
	private void splitUOMSize( CompetitiveDataDTO compData ,String sizeUOM) {
		
		int i = 0;

		i = 0;
		while ( i < sizeUOM.length()){
			int c = sizeUOM.charAt(i);
			// Non character
		    if ((c >= 48 && c <= 57 )|| (c == 46)){ 
			      i++;
			      continue;
		    }
			else
				  break;
		}
		compData.size = sizeUOM.substring(0,i);
		
		try {
			if( compData.size.length() > 0){
				 float size = Float.parseFloat(compData.size);
			}
		}
		catch(Exception e){
			logger.error("Size in Error = " + compData.size);
			logger.error("Exception in parsing size", e);
			compData.size = null;
		}
		
		
		compData.uom = sizeUOM.substring(i).trim();
		//logger.debug( "Size = " + compData.size + "uom = " + compData.uom );
		
		if(!rawUOMList.containsKey(compData.uom) )
			rawUOMList.put(compData.uom , 1);
		else{
			int count = rawUOMList.get(compData.uom);
			count++;
			rawUOMList.put(compData.uom , count);
		}
		/*
		1  	LB
		2  	OZ
		3  	CT
		4  	ML
		5  	DZ
		24 	PIR
		7  	QT
		25 	FT
		26 	DOS
		27 	ROL
		13 	EA
		14 	GA
		15 	SF
		16 	PT
		18 	PK
		20 	GR
		21 	LT
		23 	FZ
		28 	CF*/
		
		if( compData.uom.equals("EA") || compData.uom.equals("LT")||
			compData.uom.equals("FZ") || compData.uom.equals("QT") ||
			compData.uom.equals("SF") || compData.uom.equals("CT") || 
			compData.uom.equals("OZ") || compData.uom.equals("LB")|| compData.uom.equals("ML"))
			;
		else if( compData.uom.equalsIgnoreCase("count") || 
				compData.uom.equalsIgnoreCase("SH")|| compData.uom.equalsIgnoreCase("ct")||
				compData.uom.equalsIgnoreCase("COUN") || compData.uom.equalsIgnoreCase("CO"))
			compData.uom = "CT";
		else if( compData.uom.equalsIgnoreCase("FOZ") || 
				compData.uom.equalsIgnoreCase("FLZ") || compData.uom.equalsIgnoreCase("FL OZ")||
				compData.uom.equalsIgnoreCase("FLO") || compData.uom.equalsIgnoreCase("FLOZ") )
			compData.uom = "FZ";
		else if( compData.uom.equalsIgnoreCase("ROLL") || compData.uom.equalsIgnoreCase("RL"))
			compData.uom = "ROL";
		else if( compData.uom.equalsIgnoreCase("SQF")||compData.uom.equalsIgnoreCase("SQ"))
			compData.uom = "SF";
		else if (compData.uom.equalsIgnoreCase("LTR"))
			compData.uom = "LT";
		else if (compData.uom.equalsIgnoreCase("GAL") || compData.uom.equalsIgnoreCase("GL"))
			compData.uom = "GA";
		else if (compData.uom.equalsIgnoreCase("PINT"))
			compData.uom = "PT";
		else if (compData.uom.equalsIgnoreCase("DOZ"))
			compData.uom = "DZ";
		else if (compData.uom.equalsIgnoreCase("oz")|| compData.uom.equalsIgnoreCase("OUNCE")||
				compData.uom.equalsIgnoreCase("Z") ||compData.uom.equalsIgnoreCase("O") )
			compData.uom = "OZ";
		else if (compData.uom.equalsIgnoreCase("PK")){
			compData.uom = null;
			compData.itemPack= compData.size;
			compData.size = null;
		}
		else{
			//logger.debug(" UOM unmapped:" + compData.uom );
			compData.itemName += " " + sizeUOM.replaceAll("'", "''"); 
			compData.uom = null;
		}
		
		if( compData.uom != null && uomMap != null && uomMap.containsKey(compData.uom)){
			compData.uomId = uomMap.get(compData.uom);
		}
		

	}
	
	private void printUOMList(){
		Iterator<String> keyItr = rawUOMList.keySet().iterator();
		
		while ( keyItr.hasNext()){
			String key = keyItr.next();
			int count = rawUOMList.get(key);
			logger.debug(" UOM = " + key + ", No of Occurences = " + count);
		}
	
	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		// TODO Auto-generated method stub
		
	}
	
}
