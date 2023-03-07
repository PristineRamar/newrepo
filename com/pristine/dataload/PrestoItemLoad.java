package com.pristine.dataload;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ItemDAO;
import com.pristine.dao.ItemLoaderDAO;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dto.ItemDTO;
//import com.pristine.dto.salesanalysis.ProductDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
//import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PrestoItemLoad extends PristineFileParser{

	 private static Logger logger = Logger.getLogger("ItemDataLoad");
	 private static String loadType;
	 private static final String LIG_LOAD = "LIG_LOAD";
	 private static final String LIG_ITEM_CODE_SETUP = "LIG_ITEM_CODE_SETUP";
	 private static final String LIG_ITEM_CODE_CLEANUP = "LIG_ITEM_CODE_CLEANUP";

	 private static final String ITEM_LOAD = "ITEM_LOAD";
	 private static final String BRAND_LOAD = "BRAND_LOAD";
	 
	//New variable added to bypass product group tables
	 private static boolean isPriceCheck = false;
	 
	 int newcount = 0, updateCount = 0;
	 int recordCount=0;
	 
	 int updateFailedCount = 0;
	 Connection conn = null;
	 private ShopRiteDataProcessor dataProcessor = new ShopRiteDataProcessor ();
	 private ItemDAO itemdao = new ItemDAO();
	 HashMap <String, String> uomMap = new HashMap <String, String> (); 
	 HashMap <String, String> translateUOMMap = new HashMap <String, String> ();
	 private static int stopCount;
	 
	 private static boolean ignoreDeptShortName = false;
	 private ProductGroupDAO productDAO = null;
	 int deptNameCodeEmptyCount = 0;
	 int catNameCodeEmptyCount = 0;
	 int subCatNameCodeEmptyCount = 0;
	 
	 public PrestoItemLoad(){
		 logger = Logger.getLogger("ItemDataLoad");
		 productDAO = new ProductGroupDAO();
	 }
	 public PrestoItemLoad(Logger myLog){
		 logger = myLog;
		 productDAO = new ProductGroupDAO();
	 }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PrestoItemLoad itemDataLoad = new PrestoItemLoad();
    	PropertyConfigurator.configure("log4j.properties");

    	String isPriceCheckStr = PropertyManager.getProperty("ITEM_LOAD.IS_PRICE_CHECK", "false");
    	if(isPriceCheckStr.equalsIgnoreCase("true")){
    		isPriceCheck = true;
    	}
    	
    	stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
    	
        if( args.length < 1){
        	logger.info("Invalid Arguments,  args[0] - Type of Load  and arg[1] should be file name including the path");
        	System.exit(-1);
        }
        loadType = args[0];
        if ( loadType.equals(LIG_LOAD)){
        	if(  args.length == 2)
        		itemDataLoad.processLIG(args[1]);
        	else {
        		logger.info("Invalid Arguments,  args[0] - Type of Load  and arg[1] should be file name including the path");
        		System.exit(-1);
        	}
        }
        else if ( loadType.equals(LIG_ITEM_CODE_SETUP) ){
        	//itemDataLoad.setupItemCodeForLIG();
        	
        	boolean processAllItems = false;
        	if( args.length == 2 && args[1].equalsIgnoreCase("TRUE") )
        		processAllItems = true;
        	itemDataLoad.setupItemCodeForLIGV2(processAllItems);
        }
        if ( loadType.equals(LIG_ITEM_CODE_CLEANUP) ){
        	itemDataLoad.cleanItemCodeForLIG();
        }
        else if ( loadType.equals(ITEM_LOAD) ){
        	/*
        	 * There are 3 parameters - Item File Root Path, optional - Dept Short Name, optional Ignore Dept Short Name. 
        	 * By Default Dept Short Name (2nd parameter) is assumed as not present in the input file. If true 
        	 * it will take the dept short name from the file
        	 * By Default Ignore Dept Short Name (3rd parameter) is assumed as false, If true it will save the 
        	 * dept full name instead of short name. This parameter works only if 2nd parameter is true
        	 */
        	boolean hasDeptShortName = false;
        
        	if( args.length >= 3) 
        		hasDeptShortName = args[2].equalsIgnoreCase("true");
        	if( args.length == 4 && hasDeptShortName) 
        	{        		 
        		 ignoreDeptShortName = args[3].equalsIgnoreCase("true");
        	}
        	itemDataLoad.processItemFile(args[1], hasDeptShortName);
        	 
        }
        else if ( loadType.equals(BRAND_LOAD) ){
        	itemDataLoad.processBrandFile(args[1]);
        }
        
	}

	private void cleanItemCodeForLIG() {
		conn = getOracleConnection();
		//Identify the LIR where LIR_ITEM_CODE is null
		//For each LIR, get the min ITEM_CODE for that famil
		//Update LIR table.
		
		logger.info("LIR ITEM Code Cleanup started ...");
		boolean commit = true;
		 try{
		    	
			 CachedRowSet crs = itemdao.getLIRItems(conn, false);
			 while (crs.next()){
				 int lirItemCodePrev = crs.getInt("RET_LIR_ITEM_CODE_PREV");
				 if( lirItemCodePrev < 0) continue;
				 
				 int lirItemCodeNew = crs.getInt("RET_LIR_ITEM_CODE");
				 if( lirItemCodeNew == lirItemCodePrev) continue;
				 
				 recordCount++;
	
				 boolean updateStatus;
				 String message ="";
				 try{
					 updateStatus = itemdao.updateNewLirItemCode(conn, lirItemCodePrev, lirItemCodeNew);
				 }catch (GeneralException ex) {
					 updateStatus = false;
					 logger.info("Lig cleanup error",ex);
					 
				 }
				 if ( updateStatus)
					 updateCount++;
				 else{
					 updateFailedCount++;
					 logger.info("Failed for New Item = " + lirItemCodeNew + " Prev Item Code = " + lirItemCodePrev 
							 + " msg ->" + message);
				 }
				 
				 
				 
			 }
		 }catch (GeneralException ex) {
		        logger.error("GeneralException", ex);
		        commit = false;
		 } catch (Exception ex) {
		        logger.error("JavaException", ex);
		        commit = false;
		 }
		    logger.info( "# of Records = " + recordCount);
		    logger.info( "# of Update Records = " + updateCount);
		    logger.info( "# of Update Failed Records = " + updateFailedCount);

		 performCloseOperation(conn, commit);
		logger.info("LIR ITEM Code CLeanup successfully completed");
		
	}
	public void processRecords(List listObject) throws GeneralException {
		 
		if (loadType.equals(LIG_LOAD))
			loadLIG(listObject);
		else if (loadType.equals(BRAND_LOAD))
			loadBrandInfo(listObject);
		else
			//loadItemRecords(listObject);
			loadItemRecordsV2(listObject);

	}
	
	private void loadLIG(List listObject) throws GeneralException {
		
		List<ItemDTO> itemList = (List<ItemDTO>) listObject; 
		for (int j = 0; j < itemList.size(); j++) {

		    ItemDTO itemDTO = itemList.get(j);
		    itemDTO.likeItemGrp = itemDTO.likeItemGrp.replaceAll("'", "''").trim();
		  //adjust UPC

		    itemDTO.upc = PrestoUtil.castUPC(itemDTO.upc, false);
		     
		    // Create STandardUPC
		    if( itemDTO.upc.length()== 12){
		    	itemDTO.standardUPC= itemDTO.upc.substring(1);
		    	int checkDigit = dataProcessor.findUPCCheckDigit(itemDTO.standardUPC);
		    	itemDTO.standardUPC = itemDTO.standardUPC + Integer.toString(checkDigit);
		    }
		    
		    if( !itemDTO.likeItemGrp.equals("")){
		    	itemDTO.likeItemId = itemdao.setupRetailerLikeItem(conn, null,itemDTO.likeItemGrp);
		    	//Update item Lookup master for Item code
		    	int update = itemdao.updateLikeItemGrp(conn, itemDTO);
		    	if( update < 1)
		    		updateFailedCount++;
		    	else
		    		updateCount++; 
		    }

		    recordCount++;
		    if( recordCount%1000 == 0){
		    	
		    	logger.info("No of records Processed " + recordCount);
		    	PristineDBUtil.commitTransaction(conn, "Data Load");
		    }
		}
	}

	private void loadBrandInfo(List listObject) throws GeneralException {
		
		List<ItemDTO> itemList = (List<ItemDTO>) listObject; 
		for (int j = 0; j < itemList.size(); j++) {

		    ItemDTO itemDTO = itemList.get(j);
		    itemDTO.brandName = itemDTO.brandName.replaceAll("'", "''").trim();
		    //adjust UPC
		    itemDTO.upc = PrestoUtil.castUPC(itemDTO.upc, false);
		     
		    // Create STandardUPC
		    if( itemDTO.upc.length()== 12){
		    	itemDTO.standardUPC= itemDTO.upc.substring(1);
		    	int checkDigit = dataProcessor.findUPCCheckDigit(itemDTO.standardUPC);
		    	itemDTO.standardUPC = itemDTO.standardUPC + Integer.toString(checkDigit);
		    }
		    
		    if( !itemDTO.brandName.equals("")){
		    	itemDTO.brandId = itemdao.setupBrand(conn, itemDTO.brandName, null);
		    	//Update item Lookup master for Item code
		    	int update = itemdao.updateBrandInfo(conn, itemDTO);
		    	if( update < 1)
		    		updateFailedCount++;
		    	else
		    		updateCount++; 
		    }

		    recordCount++;
		    if( recordCount%1000 == 0){
		    	
		    	logger.info("No of records Processed " + recordCount);
		    	PristineDBUtil.commitTransaction(conn, "Data Load");
		    }
		}
	}
	
	private void loadItemRecords(List listObject) throws GeneralException {
		List<ItemDTO> itemList = (List<ItemDTO>) listObject; 
		String catCodesToChangeSubCatToCat[] = PropertyManager.getProperty("CAT_CODES_TO_CHANGE_SUBCAT_TO_CAT", "").split(",");
		List<String> catCodesToChangeSubCatToCatList = Arrays.asList(catCodesToChangeSubCatToCat);
		for (int j = 0; j < itemList.size(); j++) {

		    ItemDTO itemDTO = itemList.get(j);
		    
		    String [] namecode = parseNameCode(itemDTO.deptName);
		    itemDTO.deptName = namecode[0];
		    itemDTO.deptCode= namecode[1];
		    
	
		    namecode = parseNameCode(itemDTO.catName);
		    itemDTO.catName = namecode[0];
		    itemDTO.catCode= namecode[1];
		    
		    namecode = parseNameCode(itemDTO.subCatName);
		    itemDTO.subCatName = namecode[0];
		    itemDTO.subCatCode= namecode[1];
		    

		    namecode = parseNameCode(itemDTO.segmentName);
		    itemDTO.segmentName = namecode[0];
		    itemDTO.segmentCode= namecode[1];
		    
			setupItem(itemDTO, conn, "AHOLD_ITEM_SETUP", catCodesToChangeSubCatToCatList);
			
		    if( recordCount%10000 == 0){
		    	
		    	logger.info("No of records Processed " + recordCount);
		    	PristineDBUtil.commitTransaction(conn, "Data Load");
		    }

		    //fill UOM_Id
		    //insert 
		    //if false update

		    
		}
	}
	
	
	private void loadItemRecordsV2(List listObject) throws GeneralException {
		List<ItemDTO> itemList = (List<ItemDTO>) listObject; 
		String catCodesToChangeSubCatToCat[] = PropertyManager.getProperty("CAT_CODES_TO_CHANGE_SUBCAT_TO_CAT", "").split(",");
		List<String> catCodesToChangeSubCatToCatList = Arrays.asList(catCodesToChangeSubCatToCat);
//		List<ItemDTO> updateItemLookupList = new ArrayList<ItemDTO>();
		for (int j = 0; j < itemList.size(); j++) {
			long ustartTime = System.currentTimeMillis();
		    ItemDTO itemDTO = itemList.get(j);
		  
		    if( itemDTO.upc.charAt(0) == '\'')
		    	itemDTO.upc = itemDTO.upc.substring(1); 
		    
		    if(isPriceCheck)
		    	setupItem(itemDTO, conn , "PRICE_CHECK_SETUP", catCodesToChangeSubCatToCatList);
		    else{
		    	setupItem(itemDTO, conn , "AHOLD_ITEM_SETUP", catCodesToChangeSubCatToCatList);
		    	// Added to setup retailer_item_code_map
		    	setupRetailerItemCode(conn, itemDTO.getRetailerItemCode(), itemDTO.getUpc(), true);
		    	
		    }
			
		    if( recordCount%10000 == 0){
		    	
		    	logger.info("No of records Processed " + recordCount);
		    	PristineDBUtil.commitTransaction(conn, "Data Load");
		    }
		    long uendTime = System.currentTimeMillis();
			logger.debug("Time taken to complete loadItemRecordsV2 : " + (uendTime - ustartTime));
		}
		    
			itemdao.updateItemFromList(conn);
			PristineDBUtil.commitTransaction(conn, "Data Load");
		    
		
	}

	
	/*
	 * Argument modeOfCalling added by dinesh 31-07-2012
	 * To Avoid the ligsetup for Tops 
	 * 
	 */

	public void setupItem(ItemDTO itemDTO, Connection dbConn, String modeOfcalling,
			List<String> catCodesToChangeSubCatToCatList) throws GeneralException {
		// Changes for incorporating Brand
		if(PropertyManager.getProperty("ITEM_LOAD.BRAND_FIELD_NO") != null){
			int brandFieldNo = Integer.parseInt(PropertyManager.getProperty("ITEM_LOAD.BRAND_FIELD_NO"));
			Class c = itemDTO.getClass();
			try{
				Field sField = c.getField("field"+brandFieldNo);
				Object value = sField.get(itemDTO);
				itemDTO.brandName = trim(String.valueOf(value));
			}catch(NoSuchFieldException exception){
				logger.warn("No field in the name " + "field" + brandFieldNo + "was found");
			}catch(IllegalAccessException exception){
				logger.warn("No field in the name " + "field" + brandFieldNo + "was found");
			}
		}
		
		if(PropertyManager.getProperty("ITEM_LOAD.MANF_FIELD_NO") != null){
			int manfFieldNo = Integer.parseInt(PropertyManager.getProperty("ITEM_LOAD.MANF_FIELD_NO"));
			Class c = itemDTO.getClass();
			try{
				Field sField = c.getField("field"+manfFieldNo);
				Object value = sField.get(itemDTO);
				itemDTO.manufactName = trim(String.valueOf(value));
			}catch(NoSuchFieldException exception){
				logger.warn("No field in the name " + "field" + manfFieldNo + "was found");
			}catch(IllegalAccessException exception){
				logger.warn("No field in the name " + "field" + manfFieldNo + "was found");
			}
		}
		// Changes for incorporating Brand - Ends
		
		// Changes for incorporating Pre Price Ind
		if(PropertyManager.getProperty("ITEM_LOAD.PREPRICE_FIELD_NO") != null){
			int prePriceInd = Integer.parseInt(PropertyManager.getProperty("ITEM_LOAD.PREPRICE_FIELD_NO"));
			Class c = itemDTO.getClass();
			try{
				Field sField = c.getField("field"+prePriceInd);
				Object value = sField.get(itemDTO);
				itemDTO.prePriceInd = trim(String.valueOf(value));
			}catch(NoSuchFieldException exception){
				logger.warn("No field in the name " + "field" + prePriceInd + "was found");
			}catch(IllegalAccessException exception){
				logger.warn("No field in the name " + "field" + prePriceInd + "was found");
			}
		}
		// Changes for incorporating Pre Priced Ind - Ends
		
		itemDTO.majDeptName = trim(itemDTO.majDeptName);
		itemDTO.deptName = itemDTO.deptName.replaceAll("'", "''").trim();
		itemDTO.catName = itemDTO.catName.replaceAll("'", "''").trim();
		if( itemDTO.subCatName != null )
			itemDTO.subCatName = itemDTO.subCatName.replaceAll("'", "''").trim();
		if( itemDTO.segmentName  != null)
			itemDTO.segmentName = itemDTO.segmentName.replaceAll("'", "''").trim();
		itemDTO.itemName = itemDTO.itemName.replaceAll("'", "''").trim();
		
		itemDTO.majDeptCode = trim(itemDTO.majDeptCode);
		itemDTO.deptCode = trim(itemDTO.deptCode);
		itemDTO.catCode = trim(itemDTO.catCode);
		itemDTO.subCatCode = trim(itemDTO.subCatCode);
		itemDTO.segmentCode = trim(itemDTO.segmentCode);
		itemDTO.retailerItemCode = trim(itemDTO.retailerItemCode);
		itemDTO.upc  = trim(itemDTO.upc);
		
		if (catCodesToChangeSubCatToCatList != null 
				&& catCodesToChangeSubCatToCatList.contains(itemDTO.catCode)) {
			itemDTO.catName = itemDTO.subCatName;
			itemDTO.catCode = itemDTO.subCatCode;
			itemDTO.subCatName = itemDTO.segmentName;
			itemDTO.subCatCode = itemDTO.segmentCode;
		}
		
		//Added by RB
		int chainId = Integer.parseInt(PropertyManager.getProperty("SUBSCRIBER_CHAIN_ID"));
		if(chainId == 52){
			itemDTO.UserAttrVal1 = trim(itemDTO.field27);
			itemDTO.UserAttrVal2 = trim(itemDTO.field28);
		
			if(trim(itemDTO.field29).equals("DEL"))
			{
				itemDTO.UserAttrVal3 = "";
			}
			else if(trim(itemDTO.field29).equals("NEW"))
			{
				itemDTO.UserAttrVal3 = "";
			}
			else if(trim(itemDTO.field29).equals("CHG"))
			{
			itemDTO.UserAttrVal3 = "";
			}
			else
			{
				itemDTO.UserAttrVal3 = trim(itemDTO.field29);
			}
		
			try{
				itemDTO.privateLabelFlag = trim(itemDTO.field30);
				itemDTO.UserAttrVal4 = trim(itemDTO.field31);
			}catch(Exception e){
				logger.debug("No Private Label Column in Input File.");
				itemDTO.privateLabelFlag = "";
			}	
		}
		else if(chainId == 53){
			itemDTO.lobName = itemDTO.field25.replaceAll("'", "''").trim();
			itemDTO.lobCode = trim(itemDTO.field26);
		}
					
		//adjust UPC
		itemDTO.upc = PrestoUtil.castUPC(itemDTO.upc, false);
		 
		// Create STandardUPC
		if( itemDTO.upc.length()== 12 && itemDTO.upc.charAt(0)== '0'){
			itemDTO.standardUPC= itemDTO.upc.substring(1);
			int checkDigit = dataProcessor.findUPCCheckDigit(itemDTO.standardUPC);
			itemDTO.standardUPC = itemDTO.standardUPC + Integer.toString(checkDigit);
		}else{
			itemDTO.standardUPC = itemDTO.upc;
		}
		
		String uomId;
		if( uomMap.containsKey(itemDTO.uom))
		{
			uomId = uomMap.get(itemDTO.uom);
		}else{
			uomId = itemdao.populateUOM(dbConn, itemDTO.uom);
			if (uomId == null && translateUOMMap.containsKey(itemDTO.uom)){
				itemDTO.uom = translateUOMMap.get(itemDTO.uom);
				if( uomMap.containsKey(itemDTO.uom))
			    	uomId = uomMap.get(itemDTO.uom);
			    else
			    	uomId = itemdao.populateUOM(dbConn, itemDTO.uom);
			}
			uomMap.put(itemDTO.uom, uomId);
		}
			
		if ( uomId != null){
			itemDTO.uomId = uomId; 
		}
		//Line commented to test Promo Loader.
		//itemDTO.retailerItemCode = replaceLeadingZeros(itemDTO.retailerItemCode);
		//logger.debug(itemDTO.toString());
		
		if( itemDTO.likeItemGrp != null && itemDTO.likeItemCode.equalsIgnoreCase("0"))
			itemDTO.likeItemCode = null;
		
		//Included on 8th July 2015, for RiteAid, there are items with LIR CODE and without LIR NAME
		//When there is LIR CODE, but there is no LIR NAME, then use LIR CODE as LIR NAME
		//If LIR CODE is there
		if ((itemDTO.likeItemCode != null && !itemDTO.likeItemCode.equals(""))
				&& (itemDTO.likeItemGrp == null || itemDTO.likeItemGrp.equals(""))) {
			itemDTO.likeItemGrp = itemDTO.likeItemCode;
		}
		
		if( (itemDTO.likeItemGrp != null && !itemDTO.likeItemGrp.equals("")) || 
				(itemDTO.likeItemCode != null && !itemDTO.likeItemCode.equals("") )){
			itemDTO.likeItemGrp = itemDTO.likeItemGrp.replaceAll("'", "''").trim();
			itemDTO.likeItemCode  = trim(itemDTO.likeItemCode);
			
			boolean concatLikeItemGroupandCode = false;
			concatLikeItemGroupandCode = Boolean.parseBoolean(PropertyManager.getProperty("CONCAT_LIKEITEMGROUP_LIKEITEMCODE", "FALSE"));
			 if(concatLikeItemGroupandCode) {
			 	//Concating Like item group and Like item Code - This is only for Rite Aid
				itemDTO.likeItemGrp = itemDTO.likeItemGrp + " - " + itemDTO.likeItemCode;
			}
			
			itemDTO.likeItemId = itemdao.setupRetailerLikeItem(conn, itemDTO.likeItemCode, itemDTO.likeItemGrp);
		}
		
		// Changes to incorporate brand
		itemdao.populateBrand(dbConn, itemDTO);
		itemdao.populateManufacturer(dbConn, itemDTO);
		// Changes to incorporate brand - Ends
		
		recordCount++;
		try{
			
			if( modeOfcalling.equalsIgnoreCase("TOPS_ITEM_SETUP")){
				//NU:: 16th Sep 2016, update representing item as well
				if( !itemdao.insertItem(dbConn, itemDTO, true, isPriceCheck) ){
					if(!isPriceCheck && !checkIfItemValid(itemDTO))
						return;
					//logger.info("Updating item ...");
					itemdao.updateItem(dbConn, itemDTO, true, isPriceCheck);
					if(!isPriceCheck && !checkIfItemValid(itemDTO))
						return;
					
					//Close the scope of itemDTO here.
					//Clone and create new obj with all properties.
					ItemDTO itemDTOForLig;
					try {
						itemDTOForLig = (ItemDTO)itemDTO.clone();
					} catch (CloneNotSupportedException e) {
						throw new GeneralException("Clone Exception", e);
					}
					//Update 'L' to upc, standardUPC and retailerItemCode of the new obj 
					//Update its corresponding lig item. Changed on 10th July 2012
					//To handle when an item moves from one lig to other
					itemDTOForLig.upc = 'L' + itemDTOForLig.upc;
					itemDTOForLig.standardUPC = 'L' + itemDTOForLig.standardUPC;
					itemDTOForLig.retailerItemCode = 'L' + itemDTOForLig.retailerItemCode;
					ItemDTO ligItem = null;
					ligItem = itemdao.getItemDetails(dbConn, itemDTOForLig);
					
					if(ligItem != null){
						logger.debug("Itemdetails Ret lir Id: "+ligItem.likeItemId);
						//TO avoid More than one representation item for the same RET_LIR_ID 
						//Added new condition to check any active representation item is available. Changed on 15th MAY 2017 (By Dinesh)
						int minLirCode =  Integer.parseInt(PropertyManager.getProperty("ITEMLOAD.MIN_LIR_ITEM_CODE", "970000"));
						//If defined as -1, then take default value
						int lirMinItemCode = (minLirCode == -1) ? 970000 : minLirCode;
						int lirItemCode = 0;
						lirItemCode = itemdao.getLirMinItemCode(dbConn, ligItem.likeItemId, lirMinItemCode);
						if(lirItemCode == 0){
							logger.debug("Inside method to update LIR id active ind");
							itemDTOForLig.setItemCode(ligItem.getItemCode());
							itemdao.updateItem(dbConn, itemDTOForLig, true, isPriceCheck);
							logger.debug(itemDTO.getUpc() + "\t" + itemDTO.getItemCode());
						}
						
					}
					
					updateCount++;
				}else
					newcount++;
			} else if( modeOfcalling.equalsIgnoreCase("PRICE_CHECK_SETUP")){
				if( !itemdao.insertItem(dbConn, itemDTO, true, isPriceCheck) ){
					if(!isPriceCheck && !checkIfItemValid(itemDTO))
						return;
					//logger.info("Updating item ...");
					itemdao.updateItem(dbConn, itemDTO, true, isPriceCheck);
					if(!isPriceCheck && !checkIfItemValid(itemDTO))
						return;
					updateCount++;
				}else
					newcount++;
			}
			else if( modeOfcalling.equalsIgnoreCase("AHOLD_ITEM_SETUP")){
				if( !itemdao.insertItem(dbConn, itemDTO, true, isPriceCheck) ){
					if(!checkIfItemValid(itemDTO))
						return;
					//Get the last ret_lir_id 
					int prvLirId = 0;
					
					prvLirId = itemdao.getRetLirId(dbConn,itemDTO.upc, itemDTO.retailerItemCode);
					//logger.info("Updating item ...");
					itemdao.updateItem(dbConn, itemDTO, true, isPriceCheck);
					if(!checkIfItemValid(itemDTO))
						return;
					// Loading Product Group Tables - Price Index Portfolio Support
					productDAO.populateProductGroupHierarchy(conn, itemDTO);
					
					
					//Close the scope of itemDTO here.
					//Clone and create new obj with all properties.
					ItemDTO itemDTOForLig;
					try {
						itemDTOForLig = (ItemDTO)itemDTO.clone();
					} catch (CloneNotSupportedException e) {
						throw new GeneralException("Clone Exception", e);
					}
					//Update 'L' to upc, standardUPC and retailerItemCode of the new obj 
					//Update its corresponding lig item. Changed on 10th July 2012
					//To handle when an item moves from one lig to other
					itemDTOForLig.upc = 'L' + itemDTOForLig.upc;
					itemDTOForLig.standardUPC = 'L' + itemDTOForLig.standardUPC;
					itemDTOForLig.retailerItemCode = 'L' + itemDTOForLig.retailerItemCode;
					//TO avoid More than one representation item for the same RET_LIR_ID 
					//Added new condition to check any active representation item is available. Changed on 15th MAY 2017 (By Dinesh)
					int minLirCode =  Integer.parseInt(PropertyManager.getProperty("ITEMLOAD.MIN_LIR_ITEM_CODE", "970000"));
					//If defined as -1, then take default value
					int lirMinItemCode = (minLirCode == -1) ? 970000 : minLirCode;
					int lirItemCode = 0;
					lirItemCode = itemdao.getLirMinItemCode(conn, itemDTOForLig.likeItemId, lirMinItemCode);
					logger.debug("Item code value: "+lirItemCode);
					ItemDTO ligItem = null;
					if(lirItemCode == 0){
						ligItem = itemdao.getItemDetails(dbConn, itemDTOForLig);
						logger.debug("Lig item code value: "+ligItem);
					}
					if(ligItem != null){
						itemDTOForLig.setItemCode(ligItem.getItemCode());
						itemdao.updateItem(dbConn, itemDTOForLig, true, isPriceCheck);
						
						logger.debug(itemDTO.getUpc() + "\t" + itemDTO.getItemCode());
						// Loading Product Group Tables - Price Index Portfolio Support
						itemdao.setLirItemInProductGroup(conn, ligItem.getItemCode());
						
						//Remove the link with new and prev lig.
						//Make ret_lir_item_code as null in RETAILER_LIKE_ITEM_GROUP table
						//So that the item_code of the lig is found again. (This is to make the code work
						//Even when this program is run with LIG_ITEM_CODE_SETUP FALSE 
						if(itemDTOForLig.likeItemId != prvLirId)
						{	
							if(itemDTOForLig.likeItemId >0)
								itemdao.updateLirItemCode(conn,itemDTOForLig.likeItemId);
							if(prvLirId>0)
								itemdao.updateLirItemCode(conn,prvLirId);
						}
					}
					
					updateCount++;
					
				}else{
					// Loading Product Group Tables - Price Index Portfolio Support
					productDAO.populateProductGroupHierarchy(conn, itemDTO);
				}
			}else
				newcount++;
		}catch(GeneralException ex){
			logger.error("****** UPC is " + itemDTO.upc + " record number : " +  recordCount);
			updateFailedCount++;
			logger.error("GeneralException", ex);
		}
	}
	
	private String [] parseNameCode(String inputName) {
		String [] namecode = new String [2];
		
		String modifiedName = inputName.replaceFirst("\\(TP\\)", "").trim();
		modifiedName  = modifiedName.replaceFirst("-", ""); 
		String [] modifiedNameArr = modifiedName.split("\\(");
		namecode[0] = modifiedNameArr[0];
		namecode[1] = modifiedNameArr[1].split("\\)")[0];
		namecode[1] = replaceLeadingZeros(namecode[1]);
		return namecode;
	}
	
	public String replaceLeadingZeros(String s){
		String retStr = s;
		for ( int i = 0; i < s.length(); i++){
			if( s.charAt(i)=='0') continue;
			else {
				retStr = s.substring(i);
				break;
			}
		}
		return retStr;
		
			
	}
	
	private void printHashMap(HashMap<String, String> map){
		Iterator <String> it = map.keySet().iterator(); 
		while(it.hasNext()) { 
			String key = it.next(); 
			String val = map.get(key);
			logger.info("TOPS Uom = " + key +  ", UOMID = " +val );
		}
	}
	
	


	private void setupUOMTranslation(){
		translateUOMMap.put("GALLO", "GA");
		translateUOMMap.put("SQF", "SF");
		translateUOMMap.put("OOZ", "OZ");
		translateUOMMap.put("OUNC", "OZ");
		translateUOMMap.put("SQ FT", "SF");
		translateUOMMap.put("COUN", "CT");
		translateUOMMap.put("EACH", "EA");
		translateUOMMap.put("QUART", "QT");
		translateUOMMap.put("POUND", "LB");
		translateUOMMap.put("GRAM", "GR");
		translateUOMMap.put("ROLL", "ROL");
		translateUOMMap.put("PINT", "PT");
		translateUOMMap.put("INCH", "IN");
		translateUOMMap.put("FLOZ", "FZ");
		translateUOMMap.put("PACK", "PK");
		translateUOMMap.put("YDS", "YD");
		translateUOMMap.put("OZS", "OZ");
		translateUOMMap.put("OZZ", "OZ");
		translateUOMMap.put("LBA", "LB");
		translateUOMMap.put("OZO", "OZ");
		translateUOMMap.put("POUN", "LB");
		translateUOMMap.put("PKG", "PK");
		translateUOMMap.put("EAC", "EA");
		translateUOMMap.put("PKT", "PK");
		translateUOMMap.put("LBS", "LB");
		translateUOMMap.put("GAL", "GA");
		translateUOMMap.put("GALLON", "GA");
		translateUOMMap.put("DOZ", "DZ");
		translateUOMMap.put("EACHN", "EA");
		translateUOMMap.put("LITER", "LT");
		translateUOMMap.put("LTR", "LT");
		translateUOMMap.put("SINGLE", "EA");
		translateUOMMap.put("SINGL", "EA");
		translateUOMMap.put("SQ", "SF");
		translateUOMMap.put("SFT", "SF");
		translateUOMMap.put("OUNCE", "OZ");
		translateUOMMap.put("PNT", "PT");
		translateUOMMap.put("PAC", "PK");
		translateUOMMap.put("PACKAG", "PK");
		translateUOMMap.put("FLZ", "FZ");
		translateUOMMap.put("FLO", "FZ");
		translateUOMMap.put("FL", "FZ");
		translateUOMMap.put("FOZ", "FZ");
		translateUOMMap.put("OZF", "FZ");
		
		translateUOMMap.put("GRMS", "GR");
		translateUOMMap.put("CO", "CT");
		
		

	}
	
	
	private void setupItemCodeForLIG() {
		conn = getOracleConnection();
		//Identify the LIR where LIR_ITEM_CODE is null
		//For each LIR, get the min ITEM_CODE for that famil
		//Update LIR table.
		
		logger.info("LIR ITEM Code Setup started ...");
		boolean commit = true;
		 try{
		    	
			 CachedRowSet crs = itemdao.getLIRItems(conn, false);
			 while (crs.next()){
				 int retLirId = crs.getInt("RET_LIR_ID");
				 int lirItemCode = itemdao.getLirMinItemCode( conn, retLirId, -1);
				 recordCount++;
				 if (lirItemCode > 0 ){
					 boolean updateStatus = itemdao.setupLirItemCode(conn, retLirId, lirItemCode);
					 if ( updateStatus)
						 updateCount++;
					 else
						 updateFailedCount++;
				 }
			 }
		 }catch (GeneralException ex) {
		        logger.error("GeneralException", ex);
		        commit = false;
		 } catch (Exception ex) {
		        logger.error("JavaException", ex);
		        commit = false;
		 }
		    logger.info( "# of Records = " + recordCount);
		    logger.info( "# of New Records = " + newcount);
		    logger.info( "# of Update Records = " + updateCount);
		    logger.info( "# of Update Failed Records = " + updateFailedCount);

		 performCloseOperation(conn, commit);
		logger.info("LIR ITEM Code Setup successfully completed");
	}
	
	
	
	private void setupItemCodeForLIGV2(boolean processAllItems) {
		conn = getOracleConnection();
		//Identify the LIR where LIR_ITEM_CODE is null
		//For each LIR, get the min ITEM_CODE for that family
		//Update LIR table.
		boolean checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
		int lirMinItemCode = 0;
		
		//Handling LIR_ITEM_CODE_SEQ out of sequence error
		//If defined in analysis.property, take that value, other wise take default value 970000
		int minLirCode =  Integer.parseInt(PropertyManager.getProperty("ITEMLOAD.MIN_LIR_ITEM_CODE", "970000"));
		//If defined as -1, then take default value
		lirMinItemCode = (minLirCode == -1) ? 970000 : minLirCode;
		
		int preRetLirId = 0;
		int modelItemCode= 0;
		boolean itemLigChanged = false;
		logger.info("LIR ITEM Code Setup started ...");
		boolean commit = true;
		 try{

			 boolean onlyNullItemLirCodes =  !processAllItems;
			 CachedRowSet crs = itemdao.getLIRItems(conn, onlyNullItemLirCodes);
			 while (crs.next()){
				 recordCount++;
				 itemLigChanged = false;
				 int retLirId = crs.getInt("RET_LIR_ID");
				 logger.debug("Ret Lid Id - " + retLirId);
				 //Get representing item's item code for this LIG (must be active item)
				 int lirItemCode = itemdao.getLirMinItemCode( conn, retLirId, lirMinItemCode);
				 //There is no active representing item
				 if ( lirItemCode == 0 ) {
					 //Setup LIR ITem code			
					 //Get model item code which is the item code of smallest UPC in that lig
					 modelItemCode = itemdao.getLirMinItemCode( conn, retLirId, 0);		
					 
					 String upcAndRetLirId = null;
					 //Handling of items having same upc's but falling under different lig 
					 if(!checkRetailerItemCode){
						 upcAndRetLirId = itemdao.getUpcAndRetLirIdOfLigItem(conn, modelItemCode);
						if (upcAndRetLirId.split(",")[0].equals("")) {
							preRetLirId = 0;
						} else if (!upcAndRetLirId.split(",")[0].equals("")
								&& Integer.parseInt(upcAndRetLirId.split(",")[1]) == 0) {
							preRetLirId = retLirId;
						} else {
							preRetLirId = Integer.parseInt(upcAndRetLirId.split(",")[1]);
						}
					 }else{
						 upcAndRetLirId = itemdao.getUpcRetItemCodeAndRetLirIdOfLigItem(conn, modelItemCode);
						 if(upcAndRetLirId.split(",")[0].equals("") && upcAndRetLirId.split(",")[1].equals("")) {
							 preRetLirId = 0;
						 }
						 else if (!upcAndRetLirId.split(",")[0].equals("") && !upcAndRetLirId.split(",")[1].equals("")
								 && Integer.parseInt(upcAndRetLirId.split(",")[2]) == 0) {
							 preRetLirId = retLirId;
						 }
						 else {
						 preRetLirId = Integer.parseInt(upcAndRetLirId.split(",")[2]);
						 }
					 }
					 //Check whether the item is linked to any other group	
					 logger.debug("preRetLirId: " + preRetLirId);
					 if(preRetLirId>0)
					 {//>0 -- This item's UPC is representing different LIG
						 if(!checkRetailerItemCode){
							 String upc =  upcAndRetLirId.split(",")[0];
							 //Mark RET_LIR_ITEM_CODE as null (Removing the relation with that LIG)
							 itemdao.updateLirItemCode(conn,preRetLirId);
							 //Update the item info with new RET_LIR_ID
							 itemdao.updateLirItem(conn, retLirId, preRetLirId, modelItemCode, upc);
							 itemLigChanged =true;
						 }else{
							 String upc = upcAndRetLirId.split(",")[0];
							 String retItemCode = upcAndRetLirId.split(",")[1];
							 itemdao.updateLirItemCode(conn,preRetLirId);
							 itemdao.updateLirItem(conn, retLirId, preRetLirId, modelItemCode, upc, retItemCode);
							 itemLigChanged =true;
						 }
					 }
					 else if(modelItemCode >0)
					 {	
						 logger.debug("modelItemCode: "+modelItemCode);
						 itemdao.setLirItem( conn, retLirId, modelItemCode);
					 }
					 lirItemCode  = itemdao.getLirMinItemCode( conn, retLirId, lirMinItemCode);
				 }
				 
				 
				 //Find new representation for the existing lig
				 if(itemLigChanged && !isPriceCheck)
				 {
					 int itemCode;
					 itemCode = itemdao.getLirMinItemCode( conn, preRetLirId, 0);					 
					 //Make sure, the existing lig's new representation is not the just changed item
					 if(itemCode!= 0 && itemCode != modelItemCode)
					 {
						//Insert new item
						 itemdao.setLirItem( conn, preRetLirId, itemCode);
						 lirItemCode  = itemdao.getLirMinItemCode( conn, preRetLirId, lirMinItemCode);
						 if (lirItemCode > 0 )
							//Update the RET_LIR_ID
							 itemdao.setupLirItemCode(conn, preRetLirId, lirItemCode);
					 }
				 }
				  
				 //logger.debug("LIR Item Code " + lirItemCode);
				 if (lirItemCode > 0 ){
					 if (!isPriceCheck)
						 itemdao.setLirItemInProductGroup(conn, lirItemCode);
					 boolean updateStatus = itemdao.setupLirItemCode(conn, retLirId, lirItemCode);
					 if ( updateStatus)
						 updateCount++;
					 else
						 updateFailedCount++;
				 }
			 }
		 }catch (GeneralException ex) {
			    updateFailedCount++;
		        logger.error("GeneralException", ex);
		        commit = false;
		 } catch (Exception ex) {
			    updateFailedCount++;
		        logger.error("JavaException", ex);
		        commit = false;
		 }
		    logger.info( "# of Records = " + recordCount);
		    logger.info( "# of New Records = " + newcount);
		    logger.info( "# of Update Records = " + updateCount);
		    logger.info( "# of Update Failed Records = " + updateFailedCount);

		 performCloseOperation(conn, commit);
		logger.info("LIR ITEM Code Setup successfully completed");
	}
	

	
	
	private void  processLIG(String fileName ){
		conn = getOracleConnection();
		boolean commit = true;
	    try{
	    	
	    	String fieldNames[] = new String[2];
	    	fieldNames[0] = "likeItemGrp";
	    	fieldNames[1] = "upc";
	    	parseDelimitedFile(ItemDTO.class, fileName, ',',fieldNames, stopCount);
		    printHashMap(uomMap);
	    }catch (GeneralException ex) {
	        logger.error("GeneralException", ex);
	        commit = false;
	    } catch (Exception ex) {
	        logger.error("JavaException", ex);
	        commit = false;
	    }
	    printLoadStats();
	    
		performCloseOperation(conn, commit);
		logger.info("Data Load successfully completed");
	
		return;
	}
	
	private void  processItemFile(String relativePath, boolean hasDeptShortName ){
		conn = getOracleConnection();
		boolean commit = true;
	    try{
	    	
	    	//Setup UOM Translation Map
	    	setupUOMTranslation();
	    	boolean checkRetailerItemCode = Boolean
					.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
	    	// Changes to incorporate Brand and Manufacturer
	    	cacheCoreTables(conn);
	    	int noOfFields = Integer.parseInt(PropertyManager.getProperty("ITEM_LOAD.NO_OF_FIELDS", "27"));
	    	
	    	int arraysize = hasDeptShortName?noOfFields:noOfFields - 1;
	    	if( isPriceCheck)
	    		arraysize = hasDeptShortName?20:19;
	    	String fieldNames[] = new String[arraysize];
	    	
	    	/* Fields used for TOPS File
	    	fieldNames[0] = "categoryMgrName";
	    	fieldNames[1] = "deptName";
	    	fieldNames[2] = "catName";
	    	fieldNames[3] = "subCatName";
	    	fieldNames[4] = "segmentName";
	    	fieldNames[5] = "itemName";
	    	fieldNames[6] = "upc";
	    	fieldNames[7] = "size";
	    	fieldNames[8] = "uom";
	    	fieldNames[9] = "retailerItemCode";
	    	fieldNames[10] = "privateLabelInd";
	    	*/
	
	    		    		    	
	    	if(ignoreDeptShortName)
	    	{
	    		//Consider dept full name instead of short name
	    		itemdao.ignoreDeptShortName = true;
	    	}
	    	
	    	super.headerPresent = Boolean.getBoolean(PropertyManager.getProperty("ITEM.DATALOAD.HEADER_PRESENT", "FALSE"));
	    	
	    	int i = 0;
	    	fieldNames[i] = "majDeptName"; //0
	    	i++;
	    	
	    	fieldNames[i] = "majDeptCode"; //1
	    	i++;
	    	
	    	fieldNames[i] = "deptName"; //2
	    	i++;
	    	
	    	fieldNames[i] = "deptCode"; //3
	    	i++;
	    	if( hasDeptShortName){
	    		fieldNames[i] = "deptShortName";
	    		i++;
	    	}
	    	
	    	
	    	fieldNames[i] = "catName"; //4
	    	i++;
	    	
	    	fieldNames[i] = "catCode"; //5
	    	i++;
	    	
	    	fieldNames[i] = "subCatName"; //6
	    	i++;
	    	
	    	fieldNames[i] = "subCatCode"; //7
	    	i++;
	    	
	    	fieldNames[i] = "segmentName"; //8
	    	i++;
	    	
	    	fieldNames[i] = "segmentCode"; //9
	    	i++;
	    	
	    	
	    	fieldNames[i] = "retailerItemCode"; //10
	    	i++;
	    	
	    	fieldNames[i] = "itemName"; //11
	    	i++;
	    	
	    	fieldNames[i] = "size"; //12
	    	i++;
	    	
	    	fieldNames[i] = "uom"; //13
	    	i++;
	    	
	    	fieldNames[i] = "pack"; //14
	    	i++;
	    	
	    	fieldNames[i] = "privateLabelCode"; //15
	    	i++;
	    	fieldNames[i] = "likeItemGrp"; //16
	    	i++;
	    	
	    	fieldNames[i] = "likeItemCode"; //17
	    	i++;

	    	fieldNames[i] = "upc"; //18
	    	i++;
	    	
	    	if(PropertyManager.getProperty("ITEM_LOAD.BRAND_FIELD_NO") != null
	    			&& isPriceCheck){
	    		for(; i<arraysize ; i++){
		    		fieldNames[i] = "field"+i;
		    	}
	    	}
	    	
	    	if(!isPriceCheck){
		    	fieldNames[i] = "levelType";
		    	i++;
		    	
		    	fieldNames[i] = "empty";
		    	i++;
		    	
		    	fieldNames[i] = "portfolioCode";
		    	i++;
		    	
		    	fieldNames[i] = "portfolio";
		    	i++;
		    	
		    	// Changes to incorporate Brand
		    	for(; i<arraysize ; i++){
		    		fieldNames[i] = "field"+i;
		    		logger.debug(fieldNames[i] + ", " + i);
		    	}
		    	// Changes to incorporate Brand - Ends
	    	}
	    	
	    	//Changes done by Pradeep for SAS enhancements
	    	boolean updateFlag = true;
	    	if(PropertyManager.getProperty("ITEM_LOAD.UPDATE_ACTIVE_FLAG") != null){
	    		updateFlag = Boolean.parseBoolean(
	    				PropertyManager.getProperty("ITEM_LOAD.UPDATE_ACTIVE_FLAG"));
	    	}
	    	//Changes ends
	    	
	    	if(updateFlag)
	    		itemdao.updateActiveIndicatorFlag(conn, -1);
	    	ArrayList<String> fileList = getFiles(relativePath);
	    	for ( int j = 0; j < fileList.size(); j++){
	    		logger.info("processing - " + fileList.get(j));
	    		parseDelimitedFile(ItemDTO.class, fileList.get(j), '|',fieldNames, stopCount);
	    	}
	    	//To update Product group Active Indicators (Changes done By Dinesh on (04/18/2017))
	    	productDAO.populateProdGroupActiveInd(conn);
		    printHashMap(uomMap);
	    }catch (GeneralException ex) {
	        logger.error("GeneralException", ex);
	        commit = false;
	    } catch (Exception ex) {
	        logger.error("Record in error is " + recordCount);
	        logger.error("JavaException", ex);
	        commit = false;
	    }finally{
	    	printLoadStats();
	 		performCloseOperation(conn, commit);
	    }
	   
		logger.info("Data Load successfully completed");
	
		return;
	}
	
	private void  processBrandFile(String fileName ){
		conn = getOracleConnection();
		boolean commit = true;
	    try{
	    	
	    	String fieldNames[] = new String[2];
	    	fieldNames[0] = "brandName";
	    	fieldNames[1] = "upc";
	    	parseDelimitedFile(ItemDTO.class, fileName, ',',fieldNames, stopCount);
		    printHashMap(uomMap);
	    }catch (GeneralException ex) {
	        logger.error("GeneralException", ex);
	        commit = false;
	    } catch (Exception ex) {
	        logger.error("JavaException", ex);
	        commit = false;
	    }
	    printLoadStats();
		performCloseOperation(conn, commit);
		logger.info("Data Load successfully completed");
	
		return;
	}
	
	/*
	private void performCloseOperation(boolean commit) {
		try{
			if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE") && commit){
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
		}finally{
			PristineDBUtil.close(conn);
		}
	}*/
	
	public void printLoadStats() {
		logger.info( "# of Records = " + recordCount);
	    logger.info( "# of New Records = " + newcount);
	    logger.info( "# of Existing Records = " + updateCount);
	    logger.info( "# of Failed Records = " + updateFailedCount);
	    logger.info( "# of Records with no dept info = " + deptNameCodeEmptyCount);
	    logger.info( "# of Records with no category info = " + catNameCodeEmptyCount);
	    logger.info( "# of Records with no sub category info = " + subCatNameCodeEmptyCount);
	}
	public boolean setupRetailerItemCode(Connection conn, String retailerItemCode, String upc, boolean active) throws GeneralException {
		boolean retVal=true;
		try {
			
	
			ItemDTO item = new ItemDTO (); 
			item.upc = PrestoUtil.castUPC(upc, false);
			item.retailerItemCode = retailerItemCode;
			logger.debug("Item Details. Item Code: " + item.itemCode + " UPC: " + item.upc + " ,retailer item code: " + item.retailerItemCode);
			if( !itemdao.isRetailerItemCodeExist(conn,item)){
				if(active){
					ItemDTO retItem = itemdao.getItemDetails(conn, item);
					if( retItem != null){
						item.itemCode = retItem.itemCode;
						itemdao.addRetailerItemCode(conn, item);
					}else{
						retVal = false;
					}
				}
			}else{
				ItemDTO retItem = itemdao.getItemDetails(conn, item);
				if( retItem != null){
					item.itemCode = retItem.itemCode;
					if(active){
						itemdao.updateRetailerItemCode(conn, item, true);
					}else{
						itemdao.updateRetailerItemCode(conn, item, false);
					}
				}
			}
		}catch( GeneralException ge){
			retVal = false;
			logger.error("General Exception in setting up Retailer Item Code", ge);
		}
		catch( Exception e){
			retVal = false;
			logger.error("Java Exception in setting up Retailer Item Code", e);
		}
		return retVal;
	}
	
	public boolean isUpcActiveInRetailerItemCodeMap (Connection conn, String upc, String retailerItemCode) throws GeneralException {
		boolean retVal = true;
		try{
			retVal = itemdao.isUpcActiveInRetailerItemCodeMap(conn, upc, retailerItemCode);
		}catch( GeneralException ge){
			retVal = false;
			logger.error("General Exception in retrieving Retailer Item Code details", ge);
		}
		return retVal;
	}
	
	public boolean checkIfItemValid(ItemDTO item){
		boolean validItem = true;
		if(item.isEmptyDepartment){
			validItem = false;
			deptNameCodeEmptyCount++;
		}
		if(item.isEmptyCategory){
			validItem = false;
			catNameCodeEmptyCount++;
		}
		if(item.isEmptySubCategory){
			validItem = false;
			subCatNameCodeEmptyCount++;
		}
		return validItem;
	}
	
	public String findPrePriceInd(double prePrice) {
		return prePrice <= 0 ? "0" : "1";
	}
/**
	 * To initializing all the hash map need
	 * @param connection
	 * @throws GeneralException
	 */
	public void cacheCoreTables(Connection connection) throws GeneralException {
		itemdao.init(connection);
	}
}
