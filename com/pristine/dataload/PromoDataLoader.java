package com.pristine.dataload;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.PromoDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PromoDataLoader extends PristineFileParser  {

    private static Logger logger = Logger.getLogger("PromoDataLoad");
    private int notCarried = 0;
    private int carriedItem = 0;
    private int recordCount=0;
    private int skippedCount=0;
    private int bogoCount=0;
    private int specialSaleCount=0;
    private int processedCount=0;
    private int errorCount=0;
    private int storeSpecificCount = 0;
    private int startDayMisMatchCount =0;
    private int endDayMisMatchCount =0;
    
    private Connection conn = null;
  	private CompetitiveDataDAO compDataDAO = null;
  	private ItemDAO itemdao = null;
  
  	private static boolean debug = false;
  	private static String debugItemNo = "";
  	private float minItemPrice=0;
  	
  	private boolean processZoneLevel;
  	private HashMap <String, Boolean> storeMap = new HashMap <String, Boolean>(); 
  	
  	private HashMap <String, ArrayList<String>> itemMap = new HashMap <String, ArrayList<String>>();
	
  	private HashSet<String> keyStoreList = new HashSet<String>();
  	
    public static void main(String[] args) {
        
    	ArrayList<String> fileList = null;
  
    	PromoDataLoader dataload = new PromoDataLoader();
    	PropertyConfigurator.configure("log4j-promo-data-load.properties");
		
        if( args.length < 1 ||  args.length > 2 ){
        	logger.info("Invalid Arguments, args[0] should be relative path [item-no]");
        	System.exit(-1);
        }
        if( args.length == 2){
        	debug = true;
        	debugItemNo = args[1];
        }
        
        dataload.processPromoFile(args[0]);
       
    }

	private void  processPromoFile(String relativePath ){
		conn = getOracleConnection();
		
		
	    try{      
			compDataDAO = new CompetitiveDataDAO(conn);
			itemdao = new ItemDAO();
			
			String checkUser = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_USER_ID", Constants.PRESTO_LOAD_USER);
			String checkList = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_LIST_ID", Constants.PRESTO_LOAD_CHECKLIST);
			compDataDAO.setParameters(checkUser , checkList);

			logger.info("Promo Data Load Started ....");

			CompStoreDAO storeDao = new CompStoreDAO (); 
			CachedRowSet storeList = storeDao.getKeyStoreList(conn);
			while (storeList.next()){
				String compStrId = storeList.getString("COMP_STR_ID");
				keyStoreList.add(compStrId);
			}
			
			
			//getzip files
			ArrayList<String> zipFileList = getZipFiles(relativePath);
			
			//Start with -1 so that if any reqular files are present, they are processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;
			
			String zipFilePath = getRootPath() + "/" + relativePath;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;
				
				
				try {
					if( processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;

					fileList = getFiles(relativePath);
					for (int i = 0; i < fileList.size(); i++) {
					    String files = fileList.get(i);
					    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
					    String fieldNames[] = new String[22];
				    	fieldNames[0] = "buyerCode";
				    	fieldNames[1] = "adDate1";
				    	fieldNames[2] = "adDate2";
				    	fieldNames[3] = "adDate3";
				    	fieldNames[4] = "adDate4";
				    	fieldNames[5] = "promoStartDate";
				    	fieldNames[6] = "promoEndDate";
				    	fieldNames[7] = "sourceVendorNo";
				    	fieldNames[8] = "itemNo";
				    	fieldNames[9] = "itemDesc";
				    	fieldNames[10] = "priceZone";
				    	fieldNames[11] = "storeNo";
				     	fieldNames[12] = "storeZoneData";
				    	fieldNames[13] = "regQty"; 
				    	fieldNames[14] = "regPrice";
				    	fieldNames[15] = "saleQty";
				    	fieldNames[16] = "salePrice";
				    	fieldNames[17] = "saveAmt";
				    	fieldNames[18] = "pageNo";
				    	fieldNames[19] = "blockNo";
				    	fieldNames[20] = "userChange";
				    	fieldNames[21] = "promoNo";
		
						
				    	super.headerPresent = true;
					    logger.info("Processing Zone Level Records ...");
				    	processZoneLevel = true;
				    	super.parseDelimitedFile(PromoDataDTO.class, files, '|',fieldNames, stopCount);
				    	
				    	logger.info("Processing Store Level Records ...");
				    	processZoneLevel = false;
				    	super.parseDelimitedFile(PromoDataDTO.class, files, '|',fieldNames, stopCount);
					    logger.info("No of records = " + recordCount);
					    logger.info("No of Processed records = " + processedCount);
					    logger.info("Skipped Count = " + skippedCount);
					    logger.info("Not Carried Item count " + notCarried);
					    logger.info("Carried Item count " + carriedItem);
		
					    logger.info("Store Specific Sale item count: " + storeSpecificCount);
					    logger.info("Bogo item count: " + bogoCount);
					    
					    logger.info("Special Sale item count: " + specialSaleCount);
					    logger.info("Start Day Mistmatches: " + startDayMisMatchCount);
					    logger.info("End Day Mistmatches: " + endDayMisMatchCount);
						      
					    			    
					    PristineDBUtil.commitTransaction(conn, "Promo Data Setup");
					}
				} catch (GeneralException ex) {
			        logger.error("Inner Exception - GeneralException", ex);
			        commit = false;
			    } catch (Exception ex) {
			        logger.error("Inner Exception - JavaException", ex);
			        commit = false;
			    }
			    
			    if( processZipFile){
			    	PrestoUtil.deleteFiles(fileList);
			    	fileList.clear();
			    	fileList.add(zipFileList.get(curZipFileCount));
			    }
			    String archivePath = getRootPath() + "/" + relativePath + "/";
				if( commit ){
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				}
				else{
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			}while (curZipFileCount < zipFileList.size());

	    }catch (GeneralException ex) {
	        logger.error("Outer Exception -  GeneralException", ex);
	    	
	    }
	    catch (Exception ex) {
	        logger.error("Outer Exception - JavaException", ex);
	    }
	    HashSet<Integer> schedulesProcessed = compDataDAO.getSchedulesProcessed();
	    /*** Jan 15 fix **/			    
	    for ( int schId : schedulesProcessed){
	    	logger.info("Schedule Id " + schId);
	    }

		logger.info("Promo Data Load successfully completed");

		super.performCloseOperation(conn, true);
		return;
	}

	
	@Override
	public void processRecords(List listobj) throws GeneralException {
		// TODO Auto-generated method stub
	
		List<PromoDataDTO> promoDataList = (List<PromoDataDTO>) listobj; 
		for (int j = 0; j < promoDataList.size(); j++) {
			PromoDataDTO promoData = promoDataList.get(j);
			
			/* Correct the start and end dates as per the retailer calendar */
			String promoStartDate = DateUtil.getWeekStartDate(DateUtil.toDate(promoData.getPromoStartDate()), 0);
			promoData.setPromoStartDate(promoStartDate);
			
			String promoEndDate = DateUtil.getWeekEndDate(DateUtil.toDate(promoData.getPromoEndDate()));
			promoData.setPromoEndDate(promoEndDate);
			
			if( debug) {
				printDebugRecords(promoData);
			}
			else{
				populatePromoRecord(promoData);
			}
			if( recordCount > 0 && recordCount%10000==0 ){
				logger.info("No of Records Processed = " + recordCount);
				PristineDBUtil.commitTransaction(conn, "Promo Data Setup");
			}
		}
	    PristineDBUtil.commitTransaction(conn, "Promo Data Setup");
	}

	private void populatePromoRecord(PromoDataDTO promoData) throws GeneralException {
		// If Promo Qty and Promo Retail is 0, 000, are Coupon records - These can be ignored (Used for system purposes)
		recordCount++;
		
		if( promoData.getSalePrice() < 0.01f && promoData.getSaleQty() < 0.01f){
			skippedCount++;
		}
		else if ( !isValidPromoNo(promoData.getPromoNo())){
			skippedCount++;

		}else{
			//get Promo Start date
			Date weekStartDate = DateUtil.toDate(promoData.getPromoStartDate());
			//Get current Week Start Date
			//Temp change - 0 changed to 10
			Date currentWeekStartDate = DateUtil.toDate(DateUtil.getWeekStartDate(0)) ;
			
			//Temp change - 28 changed to 6
			Date maxAdvanceStartDate = DateUtil.incrementDate(currentWeekStartDate, 28);

			// if Promo Start date <  current Week Start Date
			while ( weekStartDate.compareTo(currentWeekStartDate) < 0 ) {
				weekStartDate = DateUtil.incrementDate(weekStartDate, 7);
			}

			Date promoEndDate = DateUtil.toDate(promoData.getPromoEndDate());
			
			
			CompetitiveDataDTO compData = setupCompDataDTO(promoData, processZoneLevel);
			if( compData == null ) 
				skippedCount++;
			else {
			/* Check if the store is one of the 30 stores currently processed or not */
				try {
					if( storeMap.containsKey(compData.compStrNo)) {
						if( !storeMap.get(compData.compStrNo)) { //If not one of the currently processed stores
							compData = null;
						}
					}
					else
					{
						//get the store list and populate in storeMap as true
						ArrayList<String> storeList = compDataDAO.getStoreIdList(conn, compData.compStrNo);
						if( storeList.size()>0){
							//if one of the store is key store, add it to the list of processing stores
							boolean isKeyStore = false;
							for ( String storeId: storeList) {
								if( keyStoreList.contains(storeId))
									isKeyStore = true;
								
							}
							if( isKeyStore ){
								storeMap.put(compData.compStrNo, true);
							}
							else{
								storeMap.put(compData.compStrNo, false);
								compData = null;
							}
								
						}
					}
				}catch(GeneralException ge){
					//Mark the store as not processed
					storeMap.put( compData.compStrNo, false);
					compData = null;
				}catch(SQLException sqle){
					//Mark the store as not processed
					logger.error("SQLException", sqle);
					compData = null;
				}
				if( compData == null ) 
					skippedCount++;
				
				while ( compData!= null && weekStartDate.compareTo(promoEndDate) < 0  && weekStartDate.compareTo(maxAdvanceStartDate) < 0) {
				    compData.weekStartDate = DateUtil.getWeekStartDate(weekStartDate,0);
		            compData.weekEndDate = DateUtil.getWeekEndDate(weekStartDate);
		            compData.onAd = getAdIndicator( promoData, compData.weekEndDate);
		            //update CompetitiveData.
		            
		            
		            //Call it for multiple UPC
		        	
		        	 ArrayList<String> upcList=getUPCListFromItemCode(conn, promoData.getSourceVendorNo() + promoData.getItemNo()) ;


		        	 if( upcList != null ){
		        		 for( String upc: upcList){
		        			 compData.upc = upc;
		        			 boolean returnval = compDataDAO.insertCompData(conn, compData, false);
		        			 if( returnval)
				            	  processedCount++;
				              else
				            	  errorCount++;

		        		 }
		        	 }else{
		        		 errorCount++;
		        	 }
		        	 
	                	
		            //Increment WeekStart Date
					weekStartDate = DateUtil.incrementDate(weekStartDate, 7);
		            //logger.debug("Item Name = " + compData.itemName);
					
				}
			}
		}	
	}

	private boolean isValidPromoNo(String promoNo) {
		boolean retVal = true;
		
		try{
			int intPromoNo = Integer.parseInt(promoNo);
			if ( intPromoNo < 10000){
				retVal = false;
			}
		}catch(Exception e){
			
		}
		return retVal;
	}

	private void printDebugRecords(PromoDataDTO promoData) throws GeneralException {
		// TODO Auto-generated method stub
		recordCount++;
		/*
		if(promoData.getItemNo().equals( debugItemNo)){
			logger.debug(" Promo Start End: " + promoData.getPromoStartDate() + " - " + promoData.getPromoEndDate() + " Reg - " + promoData.getRegQty() + "/" +  promoData.getRegPrice() + " Sale - " + promoData.getSaleQty() + "/" +  promoData.getSalePrice());
			logger.debug(" item : " + promoData.getItemNo() + " - " + promoData.getItemDesc() + " Ad Dates - " + promoData.getAdDate1() + ", " +  promoData.getAdDate2() );
			logger.debug(" Zone: " + promoData.getPriceZone() + " Store " + promoData.getStoreNo() + " StoreZone - " + promoData.getStoreZoneData() );
		}*/
		
		int startDayOfweek = DateUtil.getdayofWeek(DateUtil.toDate(promoData.getPromoStartDate()));
		if( startDayOfweek != 1 )
			startDayMisMatchCount++;
		
		int endDayOfweek = DateUtil.getdayofWeek(DateUtil.toDate(promoData.getPromoEndDate()));
		if( endDayOfweek != 7 )
			endDayMisMatchCount++;
		
	}
	
	
	 private CompetitiveDataDTO setupCompDataDTO(PromoDataDTO promoData, boolean isZoneLevel) throws GeneralException {
	        CompetitiveDataDTO compData = new CompetitiveDataDTO();
        try {
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

        	//Determine if the price is Store or for Zone
        	
        	boolean isZoneRecord = false;
        	if(promoData.getStoreNo().equals("000") ){
        		if (promoData.getStoreZoneData().equals("000")) { //Zone Priced
        			compData.compStrNo = "10" + promoData.getPriceZone();
        			isZoneRecord = true;
        		}
        		else
        			compData.compStrNo = "0" + promoData.getPriceZone(); //DSD Price, Store # will be in Price Zone field
        	}
        	else{
        		compData.compStrNo = "0" + promoData.getStoreNo(); //Store Specific Pricing
        		storeSpecificCount++;
        	}
        		
        	if ( ( isZoneLevel && isZoneRecord) || ( !isZoneLevel  && !isZoneRecord)){   
	        	
    			//logger.debug("Item Record Present, Ret ITem code =  " + item.retailerItemCode);
	         
	            compData.newUOM = "";
	            //logger.debug("***Data UPC = " + compData.upc);
	            
	            compData.regMPack = promoData.getRegQty();
	            compData.regPrice = promoData.getRegPrice();

	            if (compData.regMPack <= 1) {
	                compData.regMPack = 0;
	                compData.regMPrice = 0;
	            } else {
	                compData.regMPrice = compData.regPrice;
	                compData.regPrice = 0;
	            }

	            compData.saleMPack = promoData.getSaleQty();
	            compData.fSalePrice = promoData.getSalePrice();

	            //Bogo Adjustment
	            if(Math.abs(compData.regPrice - compData.fSalePrice) < 0.01f && compData.saleMPack == 1 && 
	            		compData.regPrice > 0 ){
	            		compData.saleMPack = 2;
	            		bogoCount++;
	            }
	            	
	            if (compData.saleMPack <= 1) {
	                compData.fSaleMPrice = 0;
	                compData.saleMPack = 0;
	            } else {
	                compData.fSaleMPrice = compData.fSalePrice;
	                compData.fSalePrice = 0;
	            }
	            compData.outSideRangeInd = "N";
	            compData.saleInd = "N";
	            compData.priceNotFound = "N";
	            compData.itemNotFound = "N";
	            if (compData.regPrice == 0 && compData.regMPrice == 0 && (compData.fSalePrice > 0 || compData.fSaleMPrice > 0)) {
	                compData.priceNotFound = "Y";
	            } else if (compData.regPrice == 0 && compData.regMPrice == 0) {
	                compData.itemNotFound = "Y";
	            }

	            if (compData.fSalePrice > 0 || compData.fSaleMPrice > 0) {
	            	compData.saleInd = "Y";
	            }
	            
	            //If sale price is greater than regular price, it is a data issue, ignore the sale price.
	            if( (compData.fSalePrice > 0 || compData.fSaleMPrice > 0) && 
	            	(compData.regPrice > 0 || compData.regMPrice > 0 ))
	            {
	            	float unitSalePrice = compData.fSalePrice;
	            	float unitRegPrice = compData.regPrice;
	            	if( compData.saleMPack > 0)
	            		unitSalePrice = compData.fSaleMPrice/compData.saleMPack;
	            	if( compData.regMPack > 0)
	            		unitRegPrice = compData.regMPrice/compData.regMPack;
	            	if( unitSalePrice > unitRegPrice){
	                    compData.fSaleMPrice = 0;
	                    compData.saleMPack = 0;
	                    compData.fSalePrice = 0;
	                    compData.saleInd = "N";
	            	}
	            }
	            //Format the date
	            //Format the date as per the insert program
	            compData.checkDate = DateUtil.getDateFromCurrentDate(0);
	            //change MMDDYY format to MM/DD/YYYY format
	            
	            compData.effSaleEndDate = promoData.getPromoEndDate();
	            compData.effSaleStartDate = promoData.getPromoStartDate();
	            //????
	            compData.effRegRetailStartDate = "";

	            compData.promoNumber = promoData.getPromoNo();
	            if( compData.regPrice <= minItemPrice && compData.regMPrice <= minItemPrice)
	            	compData = null;
    		
        			
        	}else{
        		compData = null;
        	}

      }catch (Exception e) {
        	logger.error("Building CompData DTO - JavaException", e);
        	compData = null;
      }
        return compData;
    }

	private String getAdIndicator(PromoDataDTO promoData, String weekEndDate) throws GeneralException {
		String onAd = "N";
		
		Date currWeekEndDate = DateUtil.toDate(weekEndDate);
		weekEndDate = DateUtil.dateToString(currWeekEndDate, Constants.APP_DATE_MMDDYYFORMAT);
		if(promoData.getAdDate1().equals(weekEndDate) ||  promoData.getAdDate2().equals(weekEndDate) ||
		   promoData.getAdDate3().equals(weekEndDate) || promoData.getAdDate4().equals(weekEndDate) ){
			onAd = "Y";
		}
		return onAd;
	}
	
	//promoData.getSourceVendorNo() + promoData.getItemNo()
	private ArrayList<String> getUPCListFromItemCode(Connection conn, String retailerItemCode) throws GeneralException {
		ArrayList<String> upcList = null;;

		ItemDTO item = new ItemDTO ();
		item.retailerItemCode = retailerItemCode; 
			
		if( itemMap.containsKey(item.retailerItemCode)){
			upcList = itemMap.get(item.retailerItemCode);
		}else{
			upcList = itemdao.getItemFromRetailItemCodeMap(conn, item);
			if( upcList == null){
				notCarried++;
    			logger.info("No Item Record, Ret ITem code =  " + item.retailerItemCode);
			}
			itemMap.put(item.retailerItemCode, upcList);
			
				
		}
		return upcList;
	}

}
