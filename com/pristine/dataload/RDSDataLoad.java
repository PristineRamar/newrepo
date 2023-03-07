package com.pristine.dataload;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class RDSDataLoad {

	private static Logger  logger = Logger.getLogger("com.pristine.main.AnalysisMain");

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-rds-comp-data.properties");
		logger.info("Data Load started");
		
		PropertyManager.initialize("analysis.properties");
		Connection conn = null;
		try{
			conn = DBManager.getConnection();
			RDSDataLoad rdsloader = new RDSDataLoad();
			if (args.length != 2){
				logger.debug("Insufficient Arguments - Enter I->Item Load or  C (R)-Comp Data Load and file name");
				System.exit(1);
			}
			logger.debug( "arg[0] = " +  args[0]);
			logger.info( "Loading file - arg[1] = " +  args[1]);
			String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
			String fileName = rootPath;
			if( !rootPath.equals(""))
				fileName = fileName +"/";
			fileName = fileName + args[1];
			
			if( args[0].equals("I")){
				logger.debug("args - " + "I");
				//rdsloader.itemLoad(conn, fileName);
			}else if (args[0].equals("C")){
				logger.debug("args - " + "C");
				//rdsloader.compDataLoad(conn,fileName);
			}else if (args[0].equals("R")){
				logger.debug("args - " + "R");
				//rdsloader.compDataLoadV2(conn,fileName);
				rdsloader.compDataLoadV3(conn,fileName);
			}
			else{
				logger.debug("Incorrect load type - Enter I->Item Load or  R-Comp Data Load and file name");
				System.exit(1);
			}
			
		}catch(GeneralException ge){
			logger.error("Error in Load", ge);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}

		try{
			if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE")){
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Data Load");
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
	
	public void itemLoad(Connection conn, String fileName) throws GeneralException{
		
		ItemDTO item = new ItemDTO();
		int reccount = 0, newcount = 0,duplicateCount=0 ;
		try{
			CsvReader reader = new CsvReader(new FileReader(fileName));
			String [] nextLine;

			ItemDAO itemdao = new ItemDAO();
			while (reader.readRecord()) {
				nextLine = reader.getValues();
				// nextLine[] is an array of values from the line
//				if( reccount< 10)
//					logger.debug(nextLine[0] + nextLine[1]);
				item.clear();
				item.retailerItemCode=nextLine[0].trim();
				item.upc= nextLine[4].trim();
				item.itemName=nextLine[1].trim().replaceAll("'", "''");
				item.deptName=nextLine[2].trim().replaceAll("'", "''");
				item.catName=nextLine[3].trim().replaceAll("'", "''");
				if( itemdao.insertItem(conn, item, true, false)) 
					newcount++;
				else {
					try{
						itemdao.updateItem(conn, item, true, false);
					}catch(GeneralException ge){
						logger.info("Error with item - " + item.itemName + " - " + item.retailerItemCode + " " + item.upc);
					}
					duplicateCount++;
				}
				reccount++;
				if( reccount%1000 == 0)
					logger.debug("Processed  "+ reccount + " records");
			}
			logger.info("Total record count "+ reccount);
			logger.info("new record count "+ newcount);
			logger.info("Duplicate record count "+ duplicateCount);
		}catch(IOException ioe){
			throw new GeneralException( "Exception in Item Data Load", ioe);
		}
		

		
	}

	public void compDataLoad(Connection conn,String fileName)throws GeneralException{
		String [] nextLine;
		CompetitiveDataDTO compData = new CompetitiveDataDTO();
		try{
			CsvReader reader = new CsvReader(new FileReader(fileName));
			
			int reccount = 0, newcount = 0,duplicateCount=0, exceptionCount =0;
			CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(conn); 
			while (reader.readRecord()) {
				nextLine = reader.getValues();
				compData.clear();
				
				reccount++;
				if( reccount%1000 == 0)
					logger.debug("Processed  "+ reccount + " records");
				
				// nextLine[] is an array of values from the line
//				if( reccount< 50){
//					logger.debug(nextLine[0] + " " +  nextLine[1] + " " + nextLine[2] + " " +  nextLine[3] + " " +  nextLine[4] + " " +  nextLine[5]);
					compData.retailerItemCode = nextLine[0];
					compData.compStrNo = nextLine[1];
					compData.checkDate = nextLine[2];
					compData.retailType = nextLine[3];
					compData.multiple = Integer.parseInt(nextLine[4]);
					compData.retailPrice = Float.parseFloat(nextLine[5]);
					if( nextLine.length > 6)
						compData.newUOM = nextLine[6];
					fillCompData(compData);
					boolean insert = false;
					try{
						insert = compDataDAO.insertCompData(conn, compData, false);
					}catch(GeneralException ge){
						logger.error(ge.getMessage());
						exceptionCount++;
						continue;
					}
					
					if(insert)
						newcount++;
					else{
						duplicateCount++;
//						logger.debug( "Duplicate record - " +nextLine[0] + " " +  nextLine[1] + " " + nextLine[2] + " " +  nextLine[3] + " " +  nextLine[4] + " " +  nextLine[5]);
					}
					
//				}else{
//					break;
//				}
				
			}
			logger.info("Total record count "+ reccount);
			logger.info("New record count "+ newcount);
			logger.info("Sale record count "+ duplicateCount);
			logger.info("Ignored items count "+ exceptionCount);
		}catch(IOException ioe){
			throw new GeneralException( "Exception in Item Data Load", ioe);
		}/*catch(GeneralException ge){
			logger.error(compData.retailerItemCode + " " +  compData.compStrNo + " " + compData.checkDate + " " +  
							compData.retailType + " " +  compData.multiple + " " +  compData.retailPrice);
			throw ge;
		}*/
	}
	
	private void fillCompData(CompetitiveDataDTO compData) throws GeneralException {
		compData.itemNotFound ="N";
		compData.retailType = compData.retailType.trim();
		if( compData.retailType.equalsIgnoreCase("Promo")){
			compData.saleInd ="Y";
			compData.priceNotFound ="Y";
		}else if ( compData.retailType.equalsIgnoreCase("Reg") || compData.retailType.equals("") ){
			compData.saleInd ="N";
			compData.priceNotFound ="N";
		}
		else {
			compData.saleInd ="Y";
			compData.priceNotFound ="Y";
		}
		if(compData.saleInd.equals("Y")){
//			logger.debug( "Item on Sale ");
			if ( compData.multiple == 1 ) {
				compData.fSalePrice = compData.retailPrice;
//				logger.debug( "Sale Price " + compData.salePrice);
			}else{
				compData.fSaleMPrice = compData.retailPrice;
				compData.saleMPack = compData.multiple;
//				logger.debug( "Sale Price " + compData.saleMPack + " for "+ compData.saleMPrice);
			}
		}else{
			if ( compData.multiple == 1 ) {
				compData.regPrice = compData.retailPrice;
//				logger.debug( "Reg Price " + compData.regPrice);

			}else{
				compData.regMPrice = compData.retailPrice;
				compData.regMPack = compData.multiple;
//				logger.debug( "Reg Price " + compData.regMPack + " for "+ compData.regMPrice);
			}
		}
		if ( compData.newUOM.equals("W"))
			compData.newUOM = "1";
		else if ( compData.newUOM.equals("E"))
			compData.newUOM = "13";
			
		setupWeekStartEndDate(compData);
//		logger.info( "checkDate = "+ compData.checkDate );
//		logger.info( "WeekStartDate = "+ compData.weekStartDate );
//		logger.info( "WeekEndDate = "+ compData.weekEndDate );
//		logger.info( "********* " );
	}
	
	
	public void compDataLoadV2(Connection conn,String fileName)throws GeneralException{
		String [] nextLine;
		CompetitiveDataDTO compData = new CompetitiveDataDTO();
		try{
			CsvReader reader = new CsvReader(new FileReader(fileName));
			
			int reccount = 0, newcount = 0,duplicateCount=0, exceptionCount =0;
			CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(conn); 
			while (reader.readRecord()) {
				nextLine = reader.getValues();
				compData.clear();
				
				reccount++;
				if( reccount%1000 == 0)
					logger.debug("Processed  "+ reccount + " records");
				
				// nextLine[] is an array of values from the line
//				if( reccount< 50){
//					logger.debug(nextLine[0] + " " +  nextLine[1] + " " + nextLine[2] + " " +  nextLine[3] + " " +  nextLine[4] + " " +  nextLine[5]);
					compData.retailerItemCode = nextLine[0];
					compData.itemNotFound ="N";
					compData.priceNotFound = "N";
					compData.compStrNo = nextLine[1];
					compData.checkDate = nextLine[2];
					//compute start and end date
					setupWeekStartEndDate(compData);
					
					compData.retailType = nextLine[3].trim();
					//Populate Sale Ind
					if ( compData.retailType.equalsIgnoreCase("Reg") || compData.retailType.equals("") ){
						compData.saleInd ="N";
					}else{
						compData.saleInd ="Y";
					}
					compData.multiple = Integer.parseInt(nextLine[4]);
					compData.retailPrice = Float.parseFloat(nextLine[5]);
					// Populate Reg Price
					if ( compData.multiple == 1 ) {
						compData.regPrice = compData.retailPrice;
					}else{
						compData.regMPrice = compData.retailPrice;
						compData.regMPack = compData.multiple;
					}
					if(compData.saleInd.equals("Y")){
						//Populate Sale Price
						compData.multiple = Integer.parseInt(nextLine[7]);
						compData.retailPrice = Float.parseFloat(nextLine[8]);
						if ( compData.multiple == 1 ) {
							compData.fSalePrice = compData.retailPrice;
						}else{
							compData.fSaleMPrice = compData.retailPrice;
							compData.saleMPack = compData.multiple;
						}
					}
					//Insert
					boolean insert = false;
					try{
						insert = compDataDAO.insertCompData(conn, compData, false);
					}catch(GeneralException ge){
						logger.error(ge.getMessage());
						exceptionCount++;
						continue;
					}
					
					if(insert)
						newcount++;
					else{
						duplicateCount++;
//						logger.debug( "Duplicate record - " +nextLine[0] + " " +  nextLine[1] + " " + nextLine[2] + " " +  nextLine[3] + " " +  nextLine[4] + " " +  nextLine[5]);
					}
					
//				}else{
//					break;
//				}
				
			}
			logger.info("Total record count "+ reccount);
			logger.info("New record count "+ newcount);
			logger.info("Sale record count "+ duplicateCount);
			logger.info("Ignored items count "+ exceptionCount);
		}catch(IOException ioe){
			throw new GeneralException( "Exception in Item Data Load", ioe);
		}/*catch(GeneralException ge){
			logger.error(compData.retailerItemCode + " " +  compData.compStrNo + " " + compData.checkDate + " " +  
							compData.retailType + " " +  compData.multiple + " " +  compData.retailPrice);
			throw ge;
		}*/
	}
	
	
	public void compDataLoadV3(Connection conn,String fileName)throws GeneralException{
		String [] nextLine;
		CompetitiveDataDTO compData = new CompetitiveDataDTO();
		try{
			CsvReader reader = new CsvReader(new FileReader(fileName));
			
			int reccount = 0, newcount = 0,duplicateCount=0, exceptionCount =0;
			CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(conn); 
			while (reader.readRecord()) {
				nextLine = reader.getValues();
				compData.clear();
				
				reccount++;
				if(reccount == 1){
					continue;
				}
				if( reccount%10000 == 0)
					logger.info("Processed  "+ reccount + " records");
				
				    //NEW FORMAT
					compData.checkDate = nextLine[0];
					if (!compData.checkDate.equals("")){
					setupWeekStartEndDate(compData);
					}
					
					long CompStrNo = Long.parseLong(nextLine[1]);
					compData.compStrNo = String.valueOf(CompStrNo);
					
					compData.upc = nextLine[2];
					compData.multiple = Integer.parseInt(nextLine[3]);
					compData.retailPrice = Float.parseFloat(nextLine[4]);
					// Populate Reg Price
					if ( compData.multiple == 1 ) {
						compData.regPrice = compData.retailPrice;
					}else{
						compData.regMPrice = compData.retailPrice;
						compData.regMPack = compData.multiple;
					}
					compData.retailType = nextLine[7].trim();
					//Populate Sale Ind
					if (compData.retailType.equals("") ){
						compData.saleInd ="N";
					}else{
						compData.saleInd ="Y";
					}
					if(compData.saleInd.equals("Y")){
						//Populate Sale Price
						compData.multiple = Integer.parseInt(nextLine[5]);
						compData.retailPrice = Float.parseFloat(nextLine[6]);
						if ( compData.multiple == 1 ) {
							compData.fSalePrice = compData.retailPrice;
						}else{
							compData.fSaleMPrice = compData.retailPrice;
							compData.saleMPack = compData.multiple;
						}
					}
					
					logger.debug(compData.checkDate + "--" +  compData.compStrNo + "--" + compData.upc + "--" +  compData.multiple + "--" +  compData.retailPrice + "--" + compData.regMPrice + "--" + compData.regMPack + "--" + compData.retailType + "--" + compData.multiple + "--" + compData.retailPrice + "--" + compData.fSaleMPrice + "--" + compData.saleMPack);
				
				
				    /*compData.upc = nextLine[2];
					compData.itemNotFound ="N";
					compData.priceNotFound = "N";
					compData.compStrNo = nextLine[0];
					compData.checkDate = nextLine[1];
					setupWeekStartEndDate(compData);
					
					compData.retailType = nextLine[8].trim();
					if ( compData.retailType.equalsIgnoreCase("Reg") || compData.retailType.equals("") ){
						compData.saleInd ="N";
					}else{
						compData.saleInd ="Y";
					}
					compData.multiple = Integer.parseInt(nextLine[3]);
					compData.retailPrice = Float.parseFloat(nextLine[4]);
					if ( compData.multiple == 1 ) {
						compData.regPrice = compData.retailPrice;
					}else{
						compData.regMPrice = compData.retailPrice;
						compData.regMPack = compData.multiple;
					}
					if(compData.saleInd.equals("Y")){
						compData.multiple = Integer.parseInt(nextLine[6]);
						compData.retailPrice = Float.parseFloat(nextLine[7]);
						if ( compData.multiple == 1 ) {
							compData.fSalePrice = compData.retailPrice;
						}else{
							compData.fSaleMPrice = compData.retailPrice;
							compData.saleMPack = compData.multiple;
						}
					}*/
					
					if (!compData.checkDate.equals(""))
					{
					//Insert
						boolean insert = false;
						try{
							insert = compDataDAO.insertCompData(conn, compData, true);
						}catch(GeneralException ge){
							logger.error(ge.getMessage(),ge);
							exceptionCount++;
							continue;
						}
					
						if(insert)
							newcount++;
						else{
							duplicateCount++;
					}
				}
			}
			logger.info("Total record count "+ reccount);
			logger.info("New record count "+ newcount);
			logger.info("Sale record count "+ duplicateCount);
			logger.info("Ignored items count "+ exceptionCount);
		}catch(IOException ioe){
			throw new GeneralException( "Exception in Item Data Load", ioe);
		}
	}
	
	public static void setupWeekStartEndDate(CompetitiveDataDTO compData){
		
		 DateFormat formatter ; 
	     Date date = null; 
	     formatter = new SimpleDateFormat("MM/dd/yyyy");
	     try{
	    	 // Added to fix issue with date in input file having year in yy format instead of yyyy
	    	 if(compData.checkDate.length() == 8){
	    		 formatter = new SimpleDateFormat(Constants.APP_DATE_MMDDYYFORMAT);
	    		 date = (Date)formatter.parse(compData.checkDate);
	    		 compData.checkDate = DateUtil.dateToString(date, Constants.APP_DATE_FORMAT);
	    	 }else{
	    		 date = (Date)formatter.parse(compData.checkDate);
	    	 }
	     }catch(ParseException pe){
	    	 logger.error("Error in Date Parsing" + compData.upc + pe);
	     }
	    
	     if(date != null){
	    	 compData.weekEndDate = DateUtil.getWeekEndDate(date);
	    	 compData.weekStartDate = DateUtil.getWeekStartDate(date,0);
	     }
		
		/*** Jan 15 fix **/
		
		//compData.weekStartDate = "04/01/2012";
		//compData.weekEndDate = "04/07/2012";
		//logger.info("Week End Date " + weekEndDate);
		//logger.info("Week Start Date " + weekStartDate);
		
	}

}
