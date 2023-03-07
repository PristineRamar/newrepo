/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pristine.dataload;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.CostDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.CostDTO;
import com.pristine.dto.ItemDTO;

import com.pristine.dto.PriceAndCostDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.NumberUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import  org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author sakthidasan
 */
public class CostDataLoad extends PristineFileParser {

    private static Logger logger = Logger.getLogger("CostDataLoad");
    private static LinkedHashMap PRICEANDCOST_FIELD = null;
    int newcount = 0, notCarried = 0;
    int recordCount=0;
    int bogoCount=0;
  	Connection conn = null;
  	private CompetitiveDataDAO compDataDAO = null;
  	private CostDAO  costDAO = null;
  	HashSet<String> notCarriedList = new HashSet<String> ();
  	HashSet<String> processedUPCList = new HashSet<String> ();
  	
  	float minItemPrice=0;
  	PrestoItemLoad itemLoader;
  	
  	private boolean setupitem = false;
  	
    public static void main(String[] args) {
        
    	ArrayList<String> fileList = null;
  
    	CostDataLoad dataload = new CostDataLoad();
    	PropertyConfigurator.configure("log4j-cost-retail.properties");
		
        if( args.length < 2){
        	logger.info("Invalid Arguments, args[0] should be relative path and args[1] = setupitem or setupcost");
        	System.exit(-1);
        }
        dataload.processRetailCostFile(args[0], args[1]);
       
    }

	private void  processRetailCostFile(String relativePath , String setupVal){
		ArrayList<String> fileList = null;
		conn = getOracleConnection();
		
		
		//getzip files
		ArrayList<String> zipFileList = null;
		
		
		String zipFilePath = getRootPath() + "/" + relativePath;
    	if( setupVal.equalsIgnoreCase("setupitem"))
    		setupitem = true;
    	else
    		setupitem = false;

    	if( setupitem)
    		logger.info( "Setting up Item Information ");
    	else
    		logger.info( "Setting up Cost/Retail Information ");
    			
    	try {
    		zipFileList = getZipFiles(relativePath);
			compDataDAO = new CompetitiveDataDAO(conn);
			costDAO = new CostDAO();
			itemLoader = new PrestoItemLoad(logger);
    	}catch(GeneralException ge){
			logger.error("Error in setting up objects", ge);
			return;
    	}

		//Start with -1 so that if any reqular files are present, they are processed first
		int curZipFileCount = -1;
		boolean processZipFile = false;

		do {
			boolean commit = true;
		    try{      
		    	
				if( setupitem ){
					//make all items inactive first.
					ItemDAO itemdao = new ItemDAO (); 
					itemdao.updateActiveIndicatorFlag(conn,-1);
					
				}
				String checkUser = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_USER_ID", Constants.PRESTO_LOAD_USER);
				String checkList = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_LIST_ID", Constants.PRESTO_LOAD_CHECKLIST);
				String minPrice = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.MIN_ITEM_PRICE", "0.10");
				minItemPrice = Float.parseFloat(minPrice);
				
				compDataDAO.setParameters(checkUser , checkList);
		         
			
				if( processZipFile)
					PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;

				fileList = getFiles(relativePath);
				for (int i = 0; i < fileList.size(); i++) {
				    String files = fileList.get(i);
				    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
				    List<PriceAndCostDTO> cost = parseTextFile(PriceAndCostDTO.class, files, getPriceAndCost_Field(), stopCount);
				    //processRecords(cost);
		
				    logger.info("No of records = " + recordCount);
				    logger.info("New record count " + newcount);
				    logger.info("Not Carried record count " + notCarried);
				    logger.info("Not Carried UPC count " + notCarriedList.size());
				    logger.info("Bogo item count: " + bogoCount);
				    logger.info("******* Item Setup Stats ****");
				    itemLoader.printLoadStats();
				    PristineDBUtil.commitTransaction(conn, "Cost Data Setup");
				    HashSet<Integer> schedulesProcessed = compDataDAO.getSchedulesProcessed();
	/*** Jan 15 fix **/			    
				    for ( int schId : schedulesProcessed){
				    	logger.info("Schedule Id " + schId);
				    }
				}
			} catch (GeneralException ex) {
		        logger.error("GeneralException", ex);
		        commit = false;
		    } catch (Exception ex) {
		        logger.error("JavaException", ex);
		        commit = false;
		    }
			
			try{
				if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE") && commit){
					logger.info("Committing transacation");
					PristineDBUtil.commitTransaction(conn, "Data Load");
					
					if( processZipFile){
				    	PrestoUtil.deleteFiles(fileList);
				    	fileList.clear();
				    	fileList.add(zipFileList.get(curZipFileCount));
				    }

					//Move the file if the job is a costsetup
					if( !setupitem){
						
						String archivePath = getRootPath() + "/" + relativePath + "/";
						PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
					}
				}
				else{
					logger.info("Rolling back transacation");
					
					if( !commit ){
						String archivePath = getRootPath() + "/" + relativePath + "/";
						PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
					}
			
					
					PristineDBUtil.rollbackTransaction(conn, "Data Load");
				}
			}catch(GeneralException ge){
				logger.error("Error in commit", ge);
				return;
			}
			
			curZipFileCount++;
			processZipFile = true;
		}
		while(curZipFileCount < zipFileList.size());
		PristineDBUtil.close(conn);
		logger.info("Data Load successfully completed");
	
		return;
	}

	public void processRecords(List listObject) throws GeneralException {
		List<PriceAndCostDTO> cost = (List<PriceAndCostDTO>) listObject; 
		for (int j = 0; j < cost.size(); j++) {

			recordCount++;
			
		    PriceAndCostDTO priceandCostDTO = cost.get(j);
	        priceandCostDTO.setUpc(priceandCostDTO.getCommodityCode()+priceandCostDTO.getManufacturersCode()+priceandCostDTO.getProductCode());

		    if(setupitem){
		    	/* Item Related Setup */		    
			    if( !processedUPCList.contains(priceandCostDTO.getUpc()) ){
			    	if( !setupItemData(priceandCostDTO))
			    		continue;
			    	processedUPCList.add(priceandCostDTO.getUpc());
			    }
			    
			    itemLoader.setupRetailerItemCode(conn, priceandCostDTO.getVendorNo()+ priceandCostDTO.getItemNo(), priceandCostDTO.getUpc(), true);
		    }else{
		    	// Performing Cost/Prices related setup
			    if( !populatePrices(priceandCostDTO)) 
			    	continue;

			    CompetitiveDataDTO compDTO = setupCompDataDTO(priceandCostDTO);
			    if( compDTO != null ) {
				    CostDTO costRec = setupCostDTO(priceandCostDTO);
				    if( costRec == null)
				    	continue;
				    boolean insert = loadCompDataDTO(compDataDAO, compDTO, costRec);
				    if (insert) {
				        newcount++;
				    } else {
				        notCarried++;
				        if( !notCarriedList.contains(compDTO.upc)){
				        	notCarriedList.add(compDTO.upc);
				        }
				    }
			    }
		    }
		    if( recordCount%10000 == 0){
		    	
		    	logger.info("No of records Processed " + recordCount);
		    	PristineDBUtil.commitTransaction(conn, "Data Load");
		    }
		    //Count of Total records No of records inserted and number of records rejected

		    // System.out.println("movement---- "+priceandCostDTO.getVendorNo());
		}
	}
	
	private boolean setupItemData(PriceAndCostDTO priceandCostDTO) throws GeneralException {
		boolean retVal = true;
		
		try {
			ItemDTO itemDTO = new ItemDTO ();	
			itemDTO.deptCode = itemLoader.replaceLeadingZeros(priceandCostDTO.getDeptCode());
			//remove the trailing zeros
			
			itemDTO.deptName = priceandCostDTO.getDeptName().trim();
			itemDTO.catCode = itemLoader.replaceLeadingZeros(priceandCostDTO.getCatCode());
			itemDTO.catName = priceandCostDTO.getCatName().trim();
			itemDTO.subCatCode = itemLoader.replaceLeadingZeros(priceandCostDTO.getSubCatCode());
			itemDTO.subCatName = priceandCostDTO.getSubCatName().trim();
			itemDTO.segmentCode = itemLoader.replaceLeadingZeros(priceandCostDTO.getSegmentCode());
			itemDTO.segmentName = priceandCostDTO.getSegmentName().trim();
			itemDTO.upc = priceandCostDTO.getUpc().substring(2);
			itemDTO.itemName = priceandCostDTO.getItemName().trim();
			itemDTO.uom = priceandCostDTO.getSizeCode();
			itemDTO.size= priceandCostDTO.getStrSizeUnits();
			itemDTO.retailerItemCode= priceandCostDTO.getVendorNo()+priceandCostDTO.getItemNo();
			itemDTO.privateLabelCode= priceandCostDTO.getPrivateLabelInd();
			
			itemDTO.likeItemId = 0;
			//logger.debug(itemDTO.toString());
			itemLoader.setupItem(itemDTO, conn , "TOPS_ITEM_SETUP", null);
		}catch(Exception e){
            logger.error("setupItem - JavaException" + ", Rec count = + " + recordCount, e);
			retVal = false;
		}
		return retVal;
	}
	

    private boolean loadCompDataDTO(CompetitiveDataDAO compDataDAO, CompetitiveDataDTO compData, CostDTO costRec) {
        boolean returnval = true;
        try {


            try {
                returnval = compDataDAO.insertCompData(getOracleConnection(), compData, false);
                if( returnval ){
                	
                	//If data is at zone level, then it needs to be copied to store level
        			ArrayList<String> storeIdList = compDataDAO.getStoreIdList(conn, compData.compStrNo);
        			Iterator<String> strItr = storeIdList.iterator();
        			
        			int i = 0;
        			while(strItr.hasNext())
        			{
        				String compStrIdVal = strItr.next();
        				i++;
//        				if( i>=2){
//        					logger.debug("Multiple StoreId for " + compData.compStrNo);
//        				}
        				compData.compStrId = Integer.parseInt(compStrIdVal);
        			
        		//		get ScheduleId
        				compData.scheduleId= compDataDAO.getScheduleID(conn, compData);
                	
	                	//logger.debug("Inserting Cost Data ....");
	                	costRec.itemCode = compData.itemcode;
	                	costRec.compStrId = compData.compStrId;
	                	costRec.scheduleId = compData.scheduleId;
	                	costRec.weekStartDate = compData.weekStartDate;
                	
	                	//Setup the Price to calculate the Margin
	                	if( compData.saleInd.equals("Y"))
	                	{
	                		costRec.price = (compData.saleMPack > 0)? compData.fSaleMPrice/compData.saleMPack: compData.fSalePrice; 
	                		costRec.isPriceChanged = true;
	                	}else{
	                		costRec.price = (compData.regMPack > 0)? compData.regMPrice/compData.regMPack: compData.regPrice; 
	                		costRec.isPriceChanged = true;
	                		if( !compData.effRegRetailStartDate.equals ("")){
		            			Date effRegRetailStartDate = DateUtil.toDate(compData.effRegRetailStartDate);
		            			Date currentWeekStartDate = DateUtil.toDate(compData.weekStartDate);
		            			if(  DateUtil.getDateDiff(currentWeekStartDate, effRegRetailStartDate) > 8 )
		            				costRec.isPriceChanged = false;
	                		}
	                	}
	                	
	                	returnval = costDAO.insertCostData(conn, costRec);
        			}
                }

            } catch (Exception e) {
            	logger.error("CompDao - JavaException" + ", Rec count = + " + recordCount, e);
                returnval = false;
            }

        } catch (GeneralException ex) {
            returnval = false;
            logger.error("CompDao - GeneralException" + ", Rec count = + " + recordCount, ex);
        }
        return returnval;
    }

    private CostDTO setupCostDTO(PriceAndCostDTO priceandCostDTO){
    	CostDTO costDTO = new CostDTO ();
    	try{
    		
    		costDTO.listCost = priceandCostDTO.getCurrentCost()/priceandCostDTO.getCasePack();
    		
    		/*
    		Per item cost =
    			((Smaller of (CURR-COST, PROMO-COST) - (BB-AMT + BA-AMT)) / CasePack) -
    			BS-AMT
    			*/

    		float smallerCost = priceandCostDTO.getCurrentCost();
    		if(priceandCostDTO.getPromoCost() >0.0 && smallerCost > priceandCostDTO.getPromoCost())
    			smallerCost = priceandCostDTO.getPromoCost();
    		
    		costDTO.dealCost = (smallerCost -  (priceandCostDTO.getBbAmount() + priceandCostDTO.getBaAmount()))/priceandCostDTO.getCasePack();
    		costDTO.dealCost = costDTO.dealCost - priceandCostDTO.getBsAmount();
    		if( Math.abs(costDTO.listCost - costDTO.dealCost) < 0.01f)
    			costDTO.dealCost = 0;
    		costDTO.listCostEffDate= formatDate(priceandCostDTO.getCostEffDate());
    		costDTO.promoCostStartDate = formatDate(priceandCostDTO.getPromoCostEffDate());
    		costDTO.promoCostEndDate = formatDate(priceandCostDTO.getPromoCostEndDate());
    		
    	} catch (Exception e) {
        	logger.error("Building Cost DTO - JavaException" + ", Rec count = + " + recordCount, e);
        	costDTO = null;
        }
    	
    	return costDTO;
    }
    private CompetitiveDataDTO setupCompDataDTO(PriceAndCostDTO priceCostDTO) {
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

        	if( priceCostDTO.getSourceCode().equals("1"))
        		compData.compStrNo = priceCostDTO.getSourceCode()+ priceCostDTO.getZone();
        	else
        		compData.compStrNo = priceCostDTO.getZone();
            
            compData.upc = priceCostDTO.getUpc();
            //logger.debug("***Data UPC = " + compData.upc);
            
            compData.upc = PrestoUtil.castUPC(compData.upc , true);
         
            compData.newUOM = "";
            //logger.debug("***Data UPC = " + compData.upc);
            
            compData.regMPack = priceCostDTO.getRtlQuanity();
            compData.regPrice = priceCostDTO.getCurrRetail();

            if (compData.regMPack <= 1) {
                compData.regMPack = 0;
                compData.regMPrice = 0;
            } else {
                compData.regMPrice = compData.regPrice;
                compData.regPrice = 0;
            }

            compData.saleMPack = 0;
            compData.fSalePrice = 0;
            compData.fSaleMPrice = 0;

            /* 11/16/11 - Code commented to not to load sale from Cost Retail file 
            compData.saleMPack = priceCostDTO.getPromoRtlQuantity();
            compData.fSalePrice = priceCostDTO.getPromoRetail();

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
            
            11/16/11 End */

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
            /* 11/16/11 - Code commented to not to load sale from Cost Retail file 
            compData.effSaleEndDate = formatDate(priceCostDTO.getPromoRetailEndDate());
            compData.effSaleStartDate = formatDate(priceCostDTO.getPromoRetailEffDate());
             */            
            compData.effSaleEndDate = "";
            compData.effSaleStartDate = "";

            compData.effRegRetailStartDate = formatDate(priceCostDTO.getRetailEffDate());

            RDSDataLoad.setupWeekStartEndDate(compData);
            //logger.debug("Item Name = " + compData.itemName);


            if( compData.regPrice <= minItemPrice && compData.regMPrice <= minItemPrice)
            	compData = null;


        } catch (Exception e) {
        	logger.error("Building CompData DTO - JavaException" + ", Rec count = + " + recordCount, e);
        	compData = null;
        }
        return compData;
    }

    private String formatDate(String inputDate){
    	String formatDate = "";
        if( inputDate.equals("000000"))
        	formatDate = "";  
        else{
        	formatDate =
        		inputDate.substring(0, 2) + "/" + inputDate.substring(2, 4) + "/"
        		+ "20" + inputDate.substring(4);
        	//logger.debug("EffDate = " + inputDate + " -> " + formatDate);
        }
        return formatDate;

    }
    private boolean populatePrices(PriceAndCostDTO priceandCostDTO) {
    	boolean retVal = true;
    	
    	try{
	        float Retail = Float.parseFloat(priceandCostDTO.getStrCurrRetail().trim());
	        float cost = Float.parseFloat(priceandCostDTO.getStrCurrentCost().trim());
	        float Promo = Float.parseFloat(priceandCostDTO.getStrPromoCost().trim());
	        float PromoRetail = Float.parseFloat(priceandCostDTO.getStrPromoRetail().trim());
	        float bbAmount=Float.parseFloat(priceandCostDTO.getStrBbAmount().trim());
	        float baAmount=Float.parseFloat(priceandCostDTO.getStrBaAmount().trim());
	        float bsAmount=Float.parseFloat(priceandCostDTO.getStrBsAmount().trim());
	        float sizeUnit=Float.parseFloat(priceandCostDTO.getStrSizeUnits().trim());
	        int casePack = Integer.parseInt(priceandCostDTO.getCompanyPack().trim());
	        
	        priceandCostDTO.setCurrRetail(Retail / 100);
	        priceandCostDTO.setCurrentCost(NumberUtil.RoundFloat(cost / 10000, 2)); //Round it to 2
	        priceandCostDTO.setPromoCost(NumberUtil.RoundFloat(Promo / 10000, 2));
	        priceandCostDTO.setPromoRetail(PromoRetail / 100);
	        priceandCostDTO.setBbAmount(bbAmount/100);
	        priceandCostDTO.setBaAmount(baAmount/100);
	        priceandCostDTO.setBsAmount(bsAmount/100);
	        priceandCostDTO.setSizeUnits(sizeUnit);
	        priceandCostDTO.setCasePack(casePack);
    	}catch (Exception e){
    		logger.error("Populate Prices - JavaException" + ", Rec count = + " + recordCount, e);
        	retVal = false;
    	}
        return retVal;
    }

    private void PrintCost(PriceAndCostDTO cost) {
        System.out.println("1-" + cost.getVendorNo() + "-2-" + cost.getItemNo() + "-3-" + cost.getDeptarmentNo() + "-4-");
    }

    public static LinkedHashMap getPriceAndCost_Field() {
        if (PRICEANDCOST_FIELD == null) {
            PRICEANDCOST_FIELD = new LinkedHashMap();
            PRICEANDCOST_FIELD.put("vendorNo", "0-6");
            PRICEANDCOST_FIELD.put("itemNo", "6-12");
            PRICEANDCOST_FIELD.put("deptarmentNo", "12-14");
            PRICEANDCOST_FIELD.put("sourceCode", "14-15");
            PRICEANDCOST_FIELD.put("zone", "15-19");
            PRICEANDCOST_FIELD.put("costEffDate", "19-25");
            PRICEANDCOST_FIELD.put("strCurrentCost", "25-34");
            PRICEANDCOST_FIELD.put("promoCostEffDate", "34-40");
            PRICEANDCOST_FIELD.put("promoCostEndDate", "40-46");
            PRICEANDCOST_FIELD.put("strPromoCost", "46-55");
            PRICEANDCOST_FIELD.put("retailEffDate", "55-61");
            PRICEANDCOST_FIELD.put("strCurrRetail", "61-68");
            PRICEANDCOST_FIELD.put("promoRetailEffDate", "68-74");
            PRICEANDCOST_FIELD.put("promoRetailEndDate", "74-80");
            PRICEANDCOST_FIELD.put("strPromoRetail", "80-87");
            PRICEANDCOST_FIELD.put("targetCompRetail", "87-92");
            PRICEANDCOST_FIELD.put("compSymbol", "92-95");
            PRICEANDCOST_FIELD.put("rtlQuanity", "95-97");
            PRICEANDCOST_FIELD.put("promoRtlQuantity", "97-99");
            PRICEANDCOST_FIELD.put("targetDate", "99-107");
            PRICEANDCOST_FIELD.put("bbEffDate", "107-115");
            PRICEANDCOST_FIELD.put("bbEndDate", "115-123");
            PRICEANDCOST_FIELD.put("strBbAmount", "123-130");
            PRICEANDCOST_FIELD.put("baEffDate", "130-138");
            PRICEANDCOST_FIELD.put("strBaAmount", "138-145");
            PRICEANDCOST_FIELD.put("bsEffDate", "145-153");
            PRICEANDCOST_FIELD.put("bsEndDate", "153-161");
            PRICEANDCOST_FIELD.put("strBsAmount", "161-168");
            PRICEANDCOST_FIELD.put("filler", "168-180");
            PRICEANDCOST_FIELD.put("commodityCode", "180-184");
            PRICEANDCOST_FIELD.put("manufacturersCode", "184-189");
            PRICEANDCOST_FIELD.put("productCode", "189-194");
            PRICEANDCOST_FIELD.put("strSizeUnits", "194-201");
            PRICEANDCOST_FIELD.put("sizeCode", "201-204");
            PRICEANDCOST_FIELD.put("companyPack", "204-210");
            
            PRICEANDCOST_FIELD.put("perishableInd", "210-223");
            PRICEANDCOST_FIELD.put("majorDeptDesc", "223-238");
            PRICEANDCOST_FIELD.put("portfolioMgr", "238-268");
            PRICEANDCOST_FIELD.put("deptCode", "268-273");
            PRICEANDCOST_FIELD.put("deptName", "273-303");
            
            
            PRICEANDCOST_FIELD.put("catCode", "303-308");
            PRICEANDCOST_FIELD.put("catName", "308-338");
            PRICEANDCOST_FIELD.put("subCatCode", "338-343");
            PRICEANDCOST_FIELD.put("subCatName", "343-373");
            
            PRICEANDCOST_FIELD.put("segmentCode", "373-378");
            PRICEANDCOST_FIELD.put("segmentName", "378-408");
            PRICEANDCOST_FIELD.put("itemName", "408-438");
            PRICEANDCOST_FIELD.put("privateLabelInd", "438-439");


        }
        return PRICEANDCOST_FIELD;
    }
}
