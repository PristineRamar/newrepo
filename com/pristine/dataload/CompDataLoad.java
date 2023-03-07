package com.pristine.dataload;
/*
 * This program load Competitive data provided by external agency
 * Used for TOPS
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;


public class CompDataLoad {

	private static Logger  logger = Logger.getLogger("TopsDataLoad");
	
	HashMap <String, Integer> rawUOMList = new HashMap <String, Integer>();  
	HashMap <String, String> uomMap;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j.properties");
		logger.info("Data Load started");
		ArrayList <String> fileList = new ArrayList <String> ();
		
		PropertyManager.initialize("analysis.properties");
		String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		Connection conn = null;
		try{
			conn = DBManager.getConnection();
			CompDataLoad topsDataloader = new CompDataLoad();
			if (args.length > 2){
				logger.debug("Insufficient Arguments - C (Comp Data Load) and file name");
				System.exit(1);
			}
			logger.debug( "arg[0] = " +  args[0]);
			
			 
			if( args.length == 2){
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
				        if( filename.contains(".dat"))
				        	fileList.add(rootPath + "/" +filename);
				    }
				}
				
			}
			
			if( fileList.isEmpty()){
				logger.info("No file present to load");
				System.exit(1);
			}
			if( args[0].equals("C")){
				for(String file : fileList){
					logger.info("Loading file:" + file);
					topsDataloader.compDataLoad(conn,file);
				}
			}
			else{
				logger.info("Incorrect load type - C->Comp Data Load and file name");
				System.exit(1);
			}
			
		}catch(GeneralException ge){
			logger.error("Error in Load", ge);
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
		logger.info("Data Load successfully completed");

	}

	public void compDataLoad(Connection conn,String fileName)throws GeneralException{
		
		//Read the first line
		//Read the rest of the lines
		//Build Comp Data List
		//Identify the store
		//Create Schedule (delete if it exists)
		//Insert or update....
		//Run Performance Stats
		
		String compStrNo=null;
		String transmissionDate;
		String storeAddress;
		int compStrId = -1;
		int reccount = 0, newcount = 0,notCarried=0, exceptionCount =0;
		HashSet<Integer> schedulesProcessed = new HashSet <Integer>(); 
		BufferedReader in = null;
	    try {
	    	in = new BufferedReader(new FileReader(fileName));  
	    	String line; 
	    	int lineCount = 0;
	    	ArrayList <CompetitiveDataDTO> compDataList = new  ArrayList <CompetitiveDataDTO>();
	    	CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(conn); 
	    	ItemDAO itemdao = new ItemDAO (); 
	    	
	    	//Populate UOM Map ToDO
	    	uomMap = itemdao.getUOMList(conn,"COMP_DATALOAD");
	    	
	    	while ((line = in.readLine()) != null) {
	    		lineCount++;
	    		//Header Record
	    		if( line.charAt(0)=='H'){
    				compStrNo =  line.substring(9,14);
    				transmissionDate = line.substring(1,9); 
    				storeAddress = line.substring(14);
    				logger.info("Competitor Store Number:" + compStrNo);
    				logger.info("Competitor :" + storeAddress);
    				logger.info("Tranmission Date" + transmissionDate);
	    		}
	    		else{
	    			//Build competitive_data
	    			CompetitiveDataDTO compData = parseCompDataRecord(line);
	    			 
	    			
	    			if( compData != null){
	    				compData.compStrNo = compStrNo;
	    				compData.newUOM = "";
	    				compDataList.add(compData);
	    				boolean insert = false;
						try{
							insert = compDataDAO.insertCompData(conn, compData, false);
							if( !insert){
								//add it to the Not Carried record.
								compDataDAO.insertNonCarriedItem(conn, compData);
							}
							if( !schedulesProcessed.contains(compData.scheduleId))
								schedulesProcessed.add(compData.scheduleId);
						}catch(GeneralException ge){
							if( ge.getErrorLevel() == GeneralException.LEVEL_SEVERE)
								throw ge;
							logger.error(ge.getMessage());
							logger.error("Item is " + compData.itemName);
							exceptionCount++;
							continue;
						}
						
						if(insert)
							newcount++;
						else{
							notCarried++;
						}
	    				
	    			}
	    		}
	    		
	    		//if( lineCount >= 20)
	    		//	break;
	    	  
	    	}

	    	
	    	logger.info("No of Lines = " + (lineCount - 1));
	    	logger.info("No of Comp Data = " + compDataList.size());
			logger.info("New record count "+ newcount);
			logger.info("Not Carried record count "+ notCarried);
			logger.info("Ignored items count "+ exceptionCount);
			
			//Now call performance schedule
			PriceChangeStats pcs = new PriceChangeStats();
			for ( int schId : schedulesProcessed){
				pcs.calculatePriceChangeStats(conn,schId,0);
			}
			
	    	printUOMList();
	    	
	    } catch (FileNotFoundException e) {
	    	throw new GeneralException( "File Not Found Exception in Comp Data Load", e);
	    } catch (IOException e) {
	    	throw new GeneralException( "IO Exception in Comp Data Load", e);
	    }finally{
	    	try{
	    		if( in != null)
	    			in.close();
	    	}catch(Exception e){
	    		
	    	}
	    }
	 }

	private CompetitiveDataDTO parseCompDataRecord(String line) throws GeneralException {
		/*
		upc 0 - 14
		reg multiple = 15 18
		reg Price = 19 25
		Sale Type = 26, 27
		check Date  33 40
		sale multiple = 41 44
		sale Price = 45 51
		item - 74 - 105
		uom - 106 - remaining
		*/
		CompetitiveDataDTO compData = null;
		//logger.debug("***** New Record ****");
		if ( line.length() > 104){
			compData  = new CompetitiveDataDTO();
			compData.upc = line.substring(2, 14);
			//logger.debug("UPC = " + compData.upc);
			
			//Reg Multiple
			String tempData = line.substring(15, 18).trim();
			//logger.debug("Reg Multiple = " + tempData);
			if( tempData.length() > 0)
				compData.regMPack = Integer.parseInt(tempData);
			
			//Reg Price
			tempData = line.substring(19, 25).trim();
			//logger.debug("Reg Price = " + tempData);
			if( tempData.length() > 0){
				if( compData.regMPack == 1){
					compData.regPrice = Float.parseFloat(tempData);
					compData.regMPack = 0;
				}
				else
					compData.regMPrice = Float.parseFloat(tempData);
			}
			
			
			//Sale Type
			tempData = line.substring(26, 27).trim();
			
			compData.itemNotFound ="N";
			compData.priceNotFound = "N";
			compData.saleInd = "N";
			//Check Date
			tempData = line.substring(32, 38);
			//Format the date
			compData.checkDate = tempData.substring(0,2) + "/" + tempData.substring(2,4) + 
								 "/20" + tempData.substring(4);
			RDSDataLoad.setupWeekStartEndDate(compData);
			//logger.debug("Check Date = " + compData.checkDate);
			
			//Sale Multiple
			tempData = line.substring(39, 42).trim();
			
			if( tempData.length() > 0){
				compData.saleMPack = Integer.parseInt(tempData);
				//logger.debug("Sale Multiple = " + tempData);
				//Sale Price
				tempData = line.substring(43, 49).trim();
				//logger.debug("Sale Price = " + tempData);
				if( compData.saleMPack == 1){
					compData.fSalePrice = Float.parseFloat(tempData);
					compData.saleMPack = 0;
				}
				else
					compData.fSaleMPrice = Float.parseFloat(tempData);
			
			
				tempData = line.substring(50, 51).trim();
				compData.retailType = tempData;
				//logger.debug("Sale Type = " + tempData);
				compData.saleInd = "Y";
			}
			//Item Name
			compData.itemName = line.substring(73, 103).trim();
			//logger.debug("Item Name = " + compData.itemName);
			compData.itemName  = compData.itemName .replaceAll("'", "''");
			//Item UOM
			tempData = line.substring(104).trim();
			//logger.debug("UOM Desc = " + tempData);
			if( tempData.length() > 0)
				splitUOMSize( compData ,tempData);
			
			
			
			
			//Fill the store Id
			//compData.compStrNo = nextLine[0];
		}
		
		//logger.debug("***** End Record ****");
		return compData;
		
		
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
		
		if( compData.size.length() > 0){
			 float size = Float.parseFloat(compData.size);
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
			logger.debug(" UOM unmapped:" + compData.uom );
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
	
}
