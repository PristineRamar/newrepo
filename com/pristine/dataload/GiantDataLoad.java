package com.pristine.dataload;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.ItemGroupDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.ItemGroupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class GiantDataLoad {

	private static Logger  logger = Logger.getLogger("GaintDataLoad");

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j.properties");
		logger.info("Data Load started");
		
		PropertyManager.initialize("analysis.properties");
		Connection conn = null;
		try{
			conn = DBManager.getConnection();
			GiantDataLoad loader = new GiantDataLoad();
			if (args.length != 2){
				logger.debug("Insufficient Arguments - Enter D/I/C D->Department Setup I->Item Load or  C -Comp Data Load and file name");
				System.exit(1);
			}
			logger.info( "arg[0] = " +  args[0]);
			logger.info( "Loading file - arg[1] = " +  args[1]);
			String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
			String fileName = rootPath;
			if( !rootPath.equals(""))
				fileName = fileName +"/";
			fileName = fileName + args[1];
			
			if( args[0].equals("I")){
				loader.itemLoad(conn, fileName);
			}else if (args[0].equals("D")){
				loader.deptLoad(conn,fileName);
			}else if (args[0].equals("C")){
				loader.compDataLoad(conn,fileName);
			}else if (args[0].equals("U")){
				loader.updateItemData(conn,fileName);
			}
			
			else{
				logger.info("Incorrect load type - Enter I->Item Load or D - Dept Load or C-Comp Data Load and file name");
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
	
	private void itemLoad(Connection conn, String fileName) throws GeneralException{
		ItemDTO item = new ItemDTO();
		ItemGroupDTO itemGroup = new ItemGroupDTO();
		logger.info("Loading Item Info ...");
		int reccount = 0, newcount = 0, duplicateCount=0;
		try{
			CsvReader reader = new CsvReader(new FileReader(fileName));
			String [] nextLine;

			ItemDAO itemdao = new ItemDAO();
			ItemGroupDAO itemGrpdao = new ItemGroupDAO();
			
			while (reader.readRecord()) {
				nextLine = reader.getValues();
				// nextLine[] is an array of values from the line
				item.clear();
				itemGroup.clear();
				reccount++;
				itemGroup.catName = nextLine[0];
				itemGroup.subCatName = nextLine[1];
				itemGroup.segmentName = nextLine[2];
				itemGroup.segmentCode = nextLine[3];
				// Call Populate Ids
				if (!itemGrpdao.fillIDValues(conn, itemGroup)){
					logger.info("Segment Code does not exist " + itemGroup.toString());
					itemGroup.deptName = "UNKNOWN";
					setupGroupInfo(conn, itemGroup, itemGrpdao);
				}
				item.deptID = itemGroup.deptId;
				item.catID = itemGroup.catId;
				item.subCatID = itemGroup.subCatId;
				item.segmentID = itemGroup.segId;
				item.itemName = nextLine[4].replaceAll("'", "''");
				String sizeUOM =  nextLine[5];
				splitSizeUOM(item, sizeUOM);
				item.uomId = itemdao.populateUOM(conn, item.uom);
				item.retailerItemCode = nextLine[6];
				item.upc = nextLine[7];
				//item.privateLabelInd = nextLine[12];
				if ( nextLine.length >= 12)
					item.likeItemGrp = nextLine[11].replaceAll("'", "''");;
				/* Code for Item Insert/Update */
				/*
				if( itemdao.insertItem(conn, item, false)) 
					newcount++;
				else {
					try{
						itemdao.updateItem(conn, item, false);
					}catch(GeneralException ge){
						logger.info("Error with item - " + item.itemName + " - " + item.retailerItemCode + " " + item.upc);
					}
					duplicateCount++;
				}
				*/
				// Code for Like Item Update
				int count = itemdao.updateLikeItemGrp(conn, item);
				if ( count < 0){
					logger.info("Possible error in update - " + item.itemName + " - " + item.retailerItemCode + " " + item.upc);
				}
				else{
					if( count == 1) duplicateCount++;
				}
				
//				if( reccount < 30)
//					logger.debug(item.toString());
//				else
//					break;

				
				if( reccount%200 == 0)
					logger.debug("Processed  "+ reccount + " records");
			}
			logger.info("Total record count "+ reccount);
			logger.info("new record count "+ newcount);
			logger.info("Duplicate record count "+ duplicateCount);
			
		}catch(IOException ioe){
			throw new GeneralException( "Exception in Item Data Load", ioe);
		}catch(GeneralException ge){
			logger.info("Total record count "+ reccount);
			throw ge;
		}
	}

	private void splitSizeUOM(ItemDTO item, String sizeUOM){
		int i = 0;
		
		if( sizeUOM.contains("/")){
			int packIndex = sizeUOM.indexOf('/');
			String newSizeUOM ="";
			if ( packIndex  < sizeUOM.length()-1 )
				newSizeUOM =  sizeUOM.substring(packIndex+1);
			String pack = sizeUOM.substring(0, sizeUOM.indexOf('/'));
			while ( i < pack.length()){
				int c = pack.charAt(i);
				// Non character
			    if (c < 65 || c > 90){ 
				      i++;
				      continue;
			    }
				else
					  break;
			}
			item.pack = pack.substring(0,i);
			logger.debug("PAck is " + item.pack);
			sizeUOM = newSizeUOM;
		}
		i = 0;
		while ( i < sizeUOM.length()){
			int c = sizeUOM.charAt(i);
			// Non character
		    if (c < 65 || c > 90){ 
			      i++;
			      continue;
		    }
			else
				  break;
		}
		
		item.size = sizeUOM.substring(0,i);
		item.uom = sizeUOM.substring(i);
		
		//logger.debug(    "size = " + item.size + ", UOM = " + sizeUOM.substring(i));

	}
	private void deptLoad(Connection conn, String fileName) throws GeneralException{
		
		ItemGroupDTO itemGroup = new ItemGroupDTO();
		int reccount = 0;
		try{
			CsvReader reader = new CsvReader(new FileReader(fileName));
			String [] nextLine;

			ItemGroupDAO itemGrpdao = new ItemGroupDAO();
			while (reader.readRecord()) {
				nextLine = reader.getValues();
				// nextLine[] is an array of values from the line
				itemGroup.clear();
				itemGroup.deptName = nextLine[0];
				itemGroup.deptCode = nextLine[1];
				itemGroup.catName = nextLine[2];
				itemGroup.catCode = nextLine[3];
				itemGroup.subCatName = nextLine[4];
				itemGroup.subCatCode = nextLine[5];
				itemGroup.segmentName = nextLine[6];
				itemGroup.segmentCode = nextLine[7];
				
				setupGroupInfo(conn, itemGroup, itemGrpdao);
				
				reccount++;
				//if( reccount < 10)
					//logger.debug(itemGroup.toString());
				if( reccount%200 == 0)
					logger.debug("Processed  "+ reccount + " records");
			}
			logger.info("Total record count "+ reccount);
		}catch(IOException ioe){
			throw new GeneralException( "Exception in Item Data Load", ioe);
		}
		catch(GeneralException ge){
			logger.info("Total record count "+ reccount);
			throw ge;
		}
	}

	private void setupGroupInfo(Connection conn, ItemGroupDTO itemGroup,
			ItemGroupDAO itemGrpdao) throws GeneralException {
		
		itemGroup.deptId = itemGrpdao.populateDept(conn, itemGroup);
		itemGroup.catId = itemGrpdao.populateCategory(conn, itemGroup);
		itemGroup.subCatId = itemGrpdao.populateSubCategory(conn, itemGroup);
		itemGroup.segId = itemGrpdao.populateSegment(conn, itemGroup);
	}
	
	private void compDataLoad(Connection conn, String fileName) throws GeneralException{
		
		String [] nextLine;
		CompetitiveDataDTO compData = new CompetitiveDataDTO();
		ItemDTO item = new ItemDTO();
		try{
			CsvReader reader = new CsvReader(new FileReader(fileName));
			
			int reccount = 0, newcount = 0,duplicateCount=0, exceptionCount =0;
			
			CompetitiveDataDAO compDataDAO = new CompetitiveDataDAO(conn); 
			ItemDAO itemdao = new ItemDAO();
			while (reader.readRecord()) {
				nextLine = reader.getValues();
				compData.clear();
				item.clear();
				reccount++;
				if( reccount%25000 == 0) {
					logger.info("Processed  "+ reccount + " records");
					PristineDBUtil.commitTransaction(conn, "Giant Data Load");
				}
//				if( reccount< 2300){
//					continue;
//				}
//				
//				// nextLine[] is an array of values from the line
//				if( reccount< 2310){
					//logger.debug(nextLine[0] + " " +  nextLine[1] + " " + nextLine[2] + " " +  nextLine[3] + " " +  nextLine[4] + " " +  nextLine[5]);
					//compData.retailerItemCode = nextLine[10];
					//logger.debug("***** Data for Record - " + reccount+ " - Begining  ********");
					compData.upc = nextLine[0];
//					if( compData.upc.length() == 12){
//						compData.upc = "0" + compData.upc.substring(0,11);
//					}
					compData.itemNotFound ="N";
					compData.priceNotFound = "N";
					compData.compStrNo = nextLine[7];
					compData.checkDate = nextLine[5];
					//compute start and end date
					RDSDataLoad.setupWeekStartEndDate(compData);
					//logger.debug(" Week Start Date - " + compData.weekStartDate + " Week End Date - " + compData.weekEndDate);
					compData.retailType = nextLine[4].trim();
					//Populate Sale Ind
					if ( compData.retailType == null || compData.retailType.equals("") ){
						compData.saleInd ="N";
					}else{
						compData.saleInd ="Y";
					}
					String price = nextLine[1];
					compData.regPrice = Float.parseFloat(price);
					//logger.debug(" Reg Price " + compData.regPrice ); 
					if(compData.saleInd.equals("Y")){
						price = nextLine[3];
						compData.fSalePrice = Float.parseFloat(price);
						//logger.debug(" sale Price " + compData.fSalePrice ); 
					}
					
					// Get Item Name and Size/Uom
					compData.itemName = nextLine[9].trim().replaceAll("'", "''");
					String sizeUOM = nextLine[10].trim().toUpperCase();
					//logger.debug(" Item " + compData.itemName + " " + sizeUOM );
					if ( sizeUOM != null && ! sizeUOM.equals("")){
						splitSizeUOM(item, sizeUOM);
						compData.size = item.size;
						compData.uom = item.uom.trim();
						compData.itemPack = item.pack;
						
						if( !compData.uom.equals(""))
							compData.uomId = itemdao.populateUOM(conn, compData.uom);
						
						//logger.debug(" Item " + compData.itemName + " " + compData.size + compData.uom ); 
					}
					
					//Insert
					boolean insert = false;
					try{
						insert = compDataDAO.insertCompData(conn, compData, true);
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
				//logger.debug("***** Data for Record - " + reccount+ " - End  ********");

			}
			logger.info("Total record count "+ reccount);
			logger.info("New record count "+ newcount);
			logger.info("Duplicate record count "+ duplicateCount);
			logger.info("Ignored items count "+ exceptionCount);
		}catch(IOException ioe){
			throw new GeneralException( "Exception in Item Data Load", ioe);
		}catch(GeneralException ge){
			logger.error(compData.compStrNo + " " + compData.checkDate + " " + compData.itemName + " " +    
					compData.size + " " + compData.uom + "-" +  compData.uomId + " " +  compData.regPrice);
			throw ge;
		}
	}
	
	private void updateItemData(Connection conn, String fileName) throws GeneralException{
		
		String [] nextLine;
		ItemDTO item = new ItemDTO();
		try{
			CsvReader reader = new CsvReader(new FileReader(fileName));
			
			int reccount = 0, exceptionCount =0;
			
			ItemDAO itemdao = new ItemDAO();
			while (reader.readRecord()) {
				nextLine = reader.getValues();
				item.clear();
				reccount++;
				if( reccount%10000 == 0) {
					logger.info("Processed  "+ reccount + " records");
					PristineDBUtil.commitTransaction(conn, "Giant Data Load");
		
				}
//				if( reccount< 2300){
//					continue;
//				}
//				
//				// nextLine[] is an array of values from the line
//				if( reccount< 10){
					//logger.debug(nextLine[0] + " " +  nextLine[1] + " " + nextLine[2] + " " +  nextLine[3] + " " +  nextLine[4] + " " +  nextLine[5]);
					//compData.retailerItemCode = nextLine[10];
					//logger.debug("***** Data for Record - " + reccount+ " - Begining  ********");
					item.upc = nextLine[0];
//					if( item.upc.length() == 12){
//						item.upc = "0" + item.upc.substring(0,11);
//					}
					// Get Item Name and Size/Uom
					item.rdsItemName = nextLine[9].trim().replaceAll("'", "''");
					
					
					try{
						int count = itemdao.updateRDSDesc(conn, item);
						if( count < 0){ exceptionCount++;
							logger.info("Item Name - " +  item.rdsItemName + "UPC - "  + item.upc); 
						}
					}catch(GeneralException ge){
						logger.error(ge.getMessage());
						exceptionCount++;
						continue;
					}
					
					
//				}else{
//					break;
//				}
				//logger.debug("***** Data for Record - " + reccount+ " - End  ********");

			}
			logger.info("RDS Desc Update - Total record count "+ reccount);
			logger.info("RDS Desc Update - Ignored items count "+ exceptionCount);
		}catch(IOException ioe){
			throw new GeneralException( "Exception in Item Data Load", ioe);
		}catch(GeneralException ge){
			logger.error(item.upc+ " " + item.itemName+ " " );
			throw ge;
		}
	}
}
