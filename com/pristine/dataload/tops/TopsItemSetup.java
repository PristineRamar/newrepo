/*
 * Title: TOPS   Tops Item Loader
 *
 *************************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		  Date		    Name			     Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	 04-05-2012	    Dinesh Kumar V	     Initial  Version 
 **************************************************************************************
 */
package com.pristine.dataload.tops;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dataload.PrestoItemLoad;
import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class TopsItemSetup extends PristineFileParser  {

    private static Logger logger = Logger.getLogger("TopsItemSetup");
    private static LinkedHashMap<String, String> ITEM_FIELD = null;
    int addCount =0, deleteCount =0, updateCount = 0, countofLB=0, shelfSizeNotMatching=0;
    StringBuffer sb = new StringBuffer();
    int recordCount=0;
  	Connection _Conn = null;
  	PrestoItemLoad itemLoader = new PrestoItemLoad() ;
  	boolean isFullFile = false;
  	private int _fileProcessRecCount = 0;
  	private HashMap<String, String> _gasUPCMap = new HashMap<String, String>();
  	HashMap <String, Integer> _productMap = new HashMap<String, Integer>(); 
  	HashMap <String, Integer> _categoryMap = new HashMap<String, Integer>();
  	ProductGroupDAO productDAO = new ProductGroupDAO();
  	
	/*
	 *****************************************************************
	 * Class constructor
	 * Argument : FullFileFlag
	 * @throws Exception
	 * ****************************************************************
	 */	
	public TopsItemSetup (String fullFileFlag)
	{
        try
		{
        	//Create DB connection
        	_Conn = DBManager.getConnection();
        	
        	if (fullFileFlag.equalsIgnoreCase("1")) {
        		isFullFile = true;
        		logger.info("Process Full file");
        	}
        	else
        		logger.info("Process partial file");
	    }
		catch (GeneralException gex) {
	        logger.error(gex);
	    } 
	}
	
	/*
	 *****************************************************************
	 * Main class of the method
	 * Argument 1: Relative path
	 * Argument 2: FullFilemode ( 1 - full file , No Argument - partial file) 
	 * @throws Exception
	 * ****************************************************************
	 */	
  	
	public static void main(String[] args) {

    	PropertyConfigurator.configure("log4j-item-tops.properties");
    	String fileRelPath ="";
    	String fullFileFlag = "";
    	boolean isProcessLIG = false; // Loading Product Group Tables - Price Index Portfolio Support
    	logCommand (args);
		
        if( args.length > 0){
        	fileRelPath = args[0];
        }
        	else {	
        	logger.info("Invalid Arguments, args[0] should be relative path  [optional FullFile or DeltaFile]");
        	System.exit(-1);
        }
        if( args.length > 1){
        	// Loading Product Group Tables - Price Index Portfolio Support
        	if(args[1].startsWith("LIG_ITEM_CODE_SETUP")){
        		isProcessLIG = true;
        	}else{
        		fullFileFlag = args[1];
        	}
        }
        //Setup the variable isFullFile based on the optional 2nd argument
        
        //logger.debug("Create object for TopsItemSetup....");
		TopsItemSetup topsItemLoad = new TopsItemSetup(fullFileFlag);
		
        //logger.debug("Start the item setup process....");		
		if(!isProcessLIG){
			topsItemLoad.processItemFile(fileRelPath);
		}
		// Loading Product Group Tables - Price Index Portfolio Support
		else{
			topsItemLoad.setupLIGInProductGroup();
		}
	}

	private void processItemFile(String relativePath) {
		ArrayList<String> fileList = null;
		
		try {
			//String zipFilePath = getRootPath() + "/" + relativePath;
			String zipFilePath = getRootPath() + "/" + relativePath;
			logger.debug("File details " + zipFilePath);
			
			//getzip files
			ArrayList<String> zipFileList = getZipFiles (relativePath);
			
			//Start with -1 so that if any reqular files are present, they are processed first
			int curZipFileCount = -1;
			
			if (zipFileList.size() > 0)
				curZipFileCount = 0;
			
			boolean processZipFile = true;
			int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));

			try {
				if ( isFullFile){
					
					//make all items inactive first.
					ItemDAO itemdao = new ItemDAO (); 
					logger.info("Set all the items as inactive");
					itemdao.updateActiveIndicatorFlag(_Conn, -1);
					itemdao.updateActiveIndicatorFlagInItemCodeMap(_Conn);
				}
			}
			catch(GeneralException ge){
				logger.error("Error while setting the item inactive", ge);
				logger.info("Rollback Transaction...");
				PristineDBUtil.rollbackTransaction(_Conn, "Item Data Load");
				return;
			}
				
			//do {
				boolean commit = true;
			    try{

			    	// load the gas upc from Properties File
			    	loadGasUpc();
			    				    	
					if(processZipFile) //Unzip the zip file
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath) ;

					//Get the file list
					fileList = getFiles(relativePath);

					if (fileList.size() >0) {
						logger.info("No of input files " + fileList.size());
						//itemLoader.cacheCoreTables(_Conn);
						//Process file by file
						for (int i = 0; i < fileList.size(); i++) {

							String file = fileList.get(i);

							//Parse the text file and convert the same into DTO List
							logger.info("Process input file " + (i+1));
							parseTextFile(ItemDTO.class, file, getItem_Field(), stopCount);
							logger.info("Total processed records...." + recordCount);
							logger.info("Records for Addition......." + addCount);
							logger.info("Records for Update........." + updateCount);
							logger.info("Records for Delete........." + deleteCount);
							
							_fileProcessRecCount = 0;
						}
						productDAO.populateProdGroupActiveInd(_Conn);
											
					}
					else{
						logger.warn("There is no input files to process");
				    	commit = false;
					}
					 // Loading Product Group Tables - Price Index Portfolio Support
					// Insert GAS POS in Product Group Tables.
					setUpGasInProductGroupTables();
//					PristineDBUtil.commitTransaction(_Conn, "Data Load");
					logger.info("Number of LB processed: "+countofLB+" and Number of size conversion not matching with ShelfSize: "+shelfSizeNotMatching);
//					PristineDBUtil.close(_Conn);
					logger.info("Item Data Load successfully completed");
					
			    } catch (GeneralException ex) {
			    	logger.error("GeneralException", ex);
			    	commit = false;
			    } catch (Exception ex) {
			    	logger.error("JavaException", ex);
			    	commit = false;
			    }finally{
			    	performCloseOperation(_Conn, commit);
			    }
			
				if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE") && commit){
					logger.info("Committing transacation");
//						PristineDBUtil.commitTransaction(_Conn, "Data Load");
					logger.info("Transaction Committed");
					
					if( processZipFile){
						logger.info("Delete unzipped files");
						PrestoUtil.deleteFiles(fileList);
				    	fileList.clear();
				    	fileList.add(zipFileList.get(curZipFileCount));
				    }
					logger.info("Move file to COMPLETED folder");
					String archivePath = getRootPath() + "/" + relativePath + "/";
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
					
				}
				else{
					logger.info("Rolling back transacation");
					if( !commit ){
						logger.info("Move file to BAD folder");
						String archivePath = getRootPath() + "/" + relativePath + "/";
						PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
					}
//					PristineDBUtil.rollbackTransaction(_Conn, "Item Data Load");
				}
				
				curZipFileCount++;
				processZipFile = true;
//	    }while (curZipFileCount < zipFileList.size());
			

		} catch (GeneralException e) {
			 logger.error(" Exception Error...." , e);
		}
	
	}

	public void processRecords(List listObject) throws GeneralException {
		List<ItemDTO> itemList = (List<ItemDTO>) listObject; 
		//ItemDAO objItemDao = new ItemDAO();
		ItemDAO itemdao = new ItemDAO (); 
		
		
		for (int j = 0; j < itemList.size(); j++) {
			/*logger.debug("*******************************************");*/
			/*logger.debug("Processing record..." + j);*/
			//Changes to Handle Natural organic categories By Dinesh(10/25/2016)
			ItemDTO objItemDto = itemList.get(j);
			if(objItemDto.deptName.equals(PropertyManager.getProperty("NATURAL_ORAGNIC_DEPT_NAME",""))&& 
					objItemDto.deptCode.equals(PropertyManager.getProperty("NATURAL_ORAGNIC_DEPT_CODE","")) || 
			objItemDto.deptCode.equals(PropertyManager.getProperty("NATURAL_ORAGNIC_DEPT_CODE",""))){
				objItemDto.catName = "N&O-"+objItemDto.subCatName;
				objItemDto.catCode = "NO-"+objItemDto.subCatCode;
				objItemDto.subCatName = objItemDto.segmentName;
				objItemDto.subCatCode = "NO-"+objItemDto.segmentCode;
//				objItemDto.segmentCode = "NO-"+objItemDto.segmentCode;
			}
			
			objItemDto.sectorName = objItemDto.sectorName.trim();
			objItemDto.merchDept = objItemDto.merchDept.trim();
			objItemDto.portfolio = objItemDto.portfolio.trim();
			objItemDto.financeDept = objItemDto.financeDept.trim();
			objItemDto.brandName = objItemDto.itemName.replaceAll("^\\s+", "").substring(0, 8).replaceAll(" ", ""); // Change for Brand Setup
			objItemDto.brandCode = objItemDto.brandName;
			// Code added to check the Size is in oz. If size in Oz then convert
			// to LB and check with the shelf size. If it match then update the Size using Shelf size
			if (objItemDto.getUom().trim().toUpperCase().equals("LB")){
				countofLB++;
				String size1 = objItemDto.getSize();
				String size2 = objItemDto.getShelfSize();
				String auom = objItemDto.getUom();
				//If the Size is given in ounces and Shelf size is in LB then change UOM to OZ
				if ((Double.valueOf(objItemDto.getShelfSize()) * 16) == Double.valueOf(objItemDto.getSize())) {
					objItemDto.setUom("OZ");
				}
				//If shelf size and actual size were different and actual size not equal to 1
				else if(!((Double.valueOf(objItemDto.getShelfSize()) * 16) == Double.valueOf(objItemDto.getSize())) && 
						Double.valueOf(objItemDto.getSize()) != 1){
					objItemDto.setSize(String.valueOf((Double.valueOf(objItemDto.getShelfSize()) * 16)));
					objItemDto.setUom("OZ");
				}
				//If shelf size and actual size were different and actual size equal to 1
				else if(!((Double.valueOf(objItemDto.getShelfSize()) * 16) == Double.valueOf(objItemDto.getSize())) && 
						Double.valueOf(objItemDto.getSize()) == 1){
					objItemDto.setUom("EA");
				} else {
					shelfSizeNotMatching++;
				}
				logger.debug("Size 1 in feed: "+size1+", Size 2 in feed: "+size2+", UOM in feed: "+auom
				+", Converted Size: "+objItemDto.getSize()+", Converted UOM:"+objItemDto.getUom());
			}
			//Reuse Setitem
			setupItemData(objItemDto);
			
			//setupProductGroupHierarchyV2(objItemDto, itemdao);
			
			PristineDBUtil.commitTransaction(_Conn, "Process Commited");

			if (objItemDto.operationMode.equalsIgnoreCase("A")){
				addCount ++;
			}
			else if (objItemDto.operationMode.equalsIgnoreCase("C"))	{
				updateCount++;
			}
			else if (objItemDto.operationMode.equalsIgnoreCase("D")){
				deleteCount++;
			}
			
			recordCount++;
		}
		_fileProcessRecCount = _fileProcessRecCount + itemList.size();
		logger.info("Processed Record Count " + _fileProcessRecCount);
		
	}
	
	
	private boolean setupItemData(ItemDTO itemDTO) throws GeneralException {
		boolean retVal = true;
		
		try {
			itemDTO.operationMode = itemDTO.operationMode.trim();
			itemDTO.retailerItemCode = itemDTO.retailerItemCode.trim();
			itemDTO.upc = itemDTO.upc;
			itemDTO.size = itemDTO.size.trim();
			itemDTO.pack = itemDTO.pack.trim();
			itemDTO.itemName = itemDTO.itemName.trim();
			itemDTO.privateLabelCode = itemDTO.privateLabelCode.trim();
			itemDTO.deptCode = itemLoader.replaceLeadingZeros(itemDTO.deptCode);
			itemDTO.deptName = itemDTO.deptName.trim();
			itemDTO.catCode = itemLoader.replaceLeadingZeros(itemDTO.catCode);
			itemDTO.catName = itemDTO.catName.trim();
			itemDTO.subCatCode = itemLoader.replaceLeadingZeros(itemDTO.subCatCode);
			itemDTO.subCatName = itemDTO.subCatName.trim();
			itemDTO.segmentCode = itemLoader.replaceLeadingZeros(itemDTO.segmentCode);
			itemDTO.segmentName = itemDTO.segmentName.trim();
			itemDTO.likeItemId = 0;
			itemDTO.uom = itemDTO.uom.trim();
			itemDTO.prePriceInd = itemLoader.findPrePriceInd(itemDTO.getPrePrice());
			
		if ((itemDTO.operationMode.equalsIgnoreCase("A")) || (itemDTO.operationMode.equalsIgnoreCase("C")))	{
				// Changes to load Internal Item Code for TOPS
				String retailerItemCode = itemDTO.getRetailerItemCode();
				itemDTO.setRetailerItemCode(itemDTO.internalItemCode);
				// Changes to load Internal Item Code for TOPS - Ends
				itemLoader.setupItem(itemDTO, _Conn , "TOPS_ITEM_SETUP", null);
				itemDTO.setRetailerItemCode(retailerItemCode); // Changes to load Internal Item Code for TOPS
				setupRetailerItemCode(_Conn, itemDTO.getRetailerItemCode(), itemDTO.getUpc(), true, itemDTO.internalItemCode);
			}
			else if (itemDTO.operationMode.equalsIgnoreCase("D")){
				//Disable the active indicator for the item
				setupRetailerItemCode(_Conn, itemDTO.getRetailerItemCode(), itemDTO.getUpc(), false, itemDTO.internalItemCode);
				//DeactivateItem(PrestoUtil.castUPC(itemDTO.upc, false));
				if(!itemLoader.isUpcActiveInRetailerItemCodeMap(_Conn, itemDTO.getUpc(), itemDTO.internalItemCode)){
					DeactivateItem(_Conn, itemDTO.internalItemCode, itemDTO.getUpc(), false);
					//NU:: 16th Sep 2016, deactive corresponding lig representing item also
					int updateCount = DeactivateItem(_Conn, itemDTO.internalItemCode, itemDTO.getUpc(), true);
					logger.debug("No of LIG representing items de-activated for Item:" + "L" + itemDTO.internalItemCode + "L" + itemDTO.getUpc() + "-"
							+ updateCount);
				}
			}
		
			// Loading Product Group Tables - Price Index Portfolio Support
			if ((itemDTO.operationMode.equalsIgnoreCase("A")) || (itemDTO.operationMode.equalsIgnoreCase("C")))	{
				
				productDAO.populateProductGroupHierarchy(_Conn, itemDTO);
			}
			
		}catch(Exception e){
            logger.error("setupItem - JavaException" + ", Rec count = + " + recordCount, e);
			retVal = false;
		}
		return retVal;
	}
	
	
	public boolean setupRetailerItemCode(Connection conn, String retailerItemCode, String upc, boolean active, String internalItemCode) throws GeneralException {
		boolean retVal=true;
		try {
			
			ItemDAO itemdao = new ItemDAO();
			ItemDTO item = new ItemDTO (); 
			item.upc = PrestoUtil.castUPC(upc, false);
			//Set item.retailerItemCode as vendor + item number to check it is available in RETAILER_ITEM_CODE_MAP.
			item.retailerItemCode = retailerItemCode;
			if( !itemdao.isRetailerItemCodeExist(conn,item)){
				if(active){
					//Set item.retailerItemCode as internal item code and get the item details from ITEM_LOOKUP.
					item.retailerItemCode = internalItemCode;
					ItemDTO retItem = itemdao.getItemDetails(conn, item);
					if( retItem != null){
						//retItem.itemCode = retItem.itemCode;
						//revert back item.retailerItemCode  to vendor + item number to insert vendor + item number in  RETAILER_ITEM_CODE_MAP.
						retItem.retailerItemCode = retailerItemCode;
						itemdao.addRetailerItemCode(conn, retItem);
					}else{
						retVal = false;
					}
					
				}
			}else{
				//Set item.retailerItemCode as internal item code and get the item details from ITEM_LOOKUP.
				item.retailerItemCode = internalItemCode;
				ItemDTO retItem = itemdao.getItemDetails(conn, item);
				if( retItem != null){
					//revert back item.retailerItemCode  to vendor + item number to insert vendor + item number in  RETAILER_ITEM_CODE_MAP.
					retItem.retailerItemCode = retailerItemCode;
					if(active){
						itemdao.updateRetailerItemCode(conn, retItem, true);
					}else{
						itemdao.updateRetailerItemCode(conn, retItem, false);
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
	
	
	/*
	 *********************************************************************
	 * Method to deactivate deleted items
	 * Argument 1: UPC code
	 * @throws GeneralException
	 * ******************************************************************
	 */	
	private int DeactivateItem (Connection _conn, String retailerItemCode, String upc, boolean isLigRepItem) throws GeneralException{
		ItemDAO itemdao = new ItemDAO (); 
		int updateCount = 0;
		//logger.debug("Deactivate UPC " + upc_code);
		updateCount = itemdao.deactivateItemByUPC(_Conn, PrestoUtil.castUPC(upc, false), retailerItemCode, isLigRepItem);
		return updateCount;
	}
	
	
	/*
	 *********************************************************************
	 * Method to create mapping for item file fields
	 * @throws
	 * ******************************************************************
	 */	
	private LinkedHashMap<String, String> getItem_Field() {
		
		if( ITEM_FIELD == null){
			ITEM_FIELD  = new LinkedHashMap<String, String>();
			ITEM_FIELD.put("operationMode", "0-1");
			ITEM_FIELD.put("retailerItemCode", "1-13");
			ITEM_FIELD.put("upc", "13-27");
			//ITEM_FIELD.put("sourceCode", "28-28"); - 2:Zone price, 1:Store Price - not used 
			ITEM_FIELD.put("size", "28-35");
			ITEM_FIELD.put("uom", "36-38");
			ITEM_FIELD.put("pack", "38-44");
			ITEM_FIELD.put("itemName", "44-74");
			ITEM_FIELD.put("privateLabelCode", "74-75");
			//Data from 75-123 is not used
			// Loading Product Group Tables - Price Index Portfolio Support
			ITEM_FIELD.put("posDept", "92-94");
			ITEM_FIELD.put("posDeptDesc", "94-124");
			// Loading Product Group Tables - Price Index Portfolio Support - Ends
			ITEM_FIELD.put("sectorName", "124-137");
			ITEM_FIELD.put("merchDept", "137-167");
			ITEM_FIELD.put("financeDept", "167-197");
			ITEM_FIELD.put("portfolio", "197-227");
			ITEM_FIELD.put("deptCode", "227-232");
			ITEM_FIELD.put("deptName", "232-262");
			ITEM_FIELD.put("catCode", "262-267");
			ITEM_FIELD.put("catName", "267-297");
			ITEM_FIELD.put("subCatCode", "297-302");
			ITEM_FIELD.put("subCatName", "302-332");
			ITEM_FIELD.put("segmentCode", "332-337");
			ITEM_FIELD.put("segmentName", "337-367");
			ITEM_FIELD.put("internalItemCode", "367-373"); // Internal Item Code for TOPS
			ITEM_FIELD.put("shelfSize", "374-380");
			ITEM_FIELD.put("uom", "381-389");
			ITEM_FIELD.put("prePrice", "389-395");
		}
		return ITEM_FIELD;
	}
	

	/*
	 *********************************************************************
	 * Method to insert or update or delete Product group and its relation
	 * Argument 1: objItemDto
	 * Argument 2: itemdao
	 * @throws GeneralException
	 * ******************************************************************
	 */	
	private void setupProductGroupHierarchyV2(ItemDTO objItemDto, ItemDAO itemdao) throws GeneralException
	{
		
		try {
			String itemUpc = objItemDto.getUpc();
			int itemCode = getItemCode(itemUpc);
			int categoryId = 0;
			int financeCode = 0;
			int merchCode = 0;

			String operMode = objItemDto.getOperationMode();
			
			/*** Process Finance Department ***/
			
			// Check the _gasUpc map contains itemUpc
			// if gas upc coming to process means avoid the insert process
			
			
			if( ! _gasUPCMap.containsKey(itemUpc)){
				
			processProductGroup(operMode, Constants.FINANCEDEPARTMENT, 
					objItemDto.getFinanceDept(), Constants.ITEMLEVELID, 
					itemCode, objItemDto.getFinanceDeptCode(), itemdao , ""); 
			}
			else{
				
				logger.info(" Ietm Code.... " + itemCode +"---" + itemUpc);
							
				processProductGroup(operMode, Constants.FINANCEDEPARTMENT, 
						"GAS", Constants.ITEMLEVELID, 
						itemCode, "", itemdao , "GAS"); 
			}

			/*** Process Portfolio ***/
			if (!_categoryMap.containsKey(objItemDto.getCatCode())) {
				categoryId = itemdao.getcategoryId(_Conn , objItemDto.getCatCode());
				_categoryMap.put(objItemDto.getCatCode(), categoryId);
			}
			else{
				categoryId = _categoryMap.get(objItemDto.getCatCode());
			}
			
			
			//Update Portfolio Data
			processProductGroup(operMode, Constants.PORTFOLIO, 
					objItemDto.getPortfolio(), Constants.CATEGORYLEVELID, 
					categoryId, objItemDto.getPortfolioCode() , itemdao , ""); 
			
			/*** Process Merchandise ***/		
			//Get product code for finance department
			if (_productMap.containsKey(Constants.FINANCEDEPARTMENT + "_" + objItemDto.getFinanceDept())) {
				
				financeCode = _productMap.get(Constants.FINANCEDEPARTMENT + "_" + objItemDto.getFinanceDept());
			}
			else{
				financeCode = itemdao.getGroupId(_Conn, Constants.FINANCEDEPARTMENT, "", objItemDto.getFinanceDept());
			}

			//Update Merchantise Data
			
			processProductGroup(operMode, Constants.MERCHANTISEDEPARTMENT, 
					objItemDto.getMerchDept() , Constants.FINANCEDEPARTMENT,
					financeCode, objItemDto.getMerchDeptCode() , itemdao ,"");
			
			//*** Process Sector ***//*
			//Get product code for Merchantise
			
			if (_productMap.containsKey(Constants.MERCHANTISEDEPARTMENT + "_" + objItemDto.getMerchDept() )) {
				
				merchCode = _productMap.get(Constants.MERCHANTISEDEPARTMENT + "_" + objItemDto.getMerchDept());
			}
			else{
				merchCode = itemdao.getGroupId(_Conn, Constants.MERCHANTISEDEPARTMENT, "", objItemDto.getMerchDept());
			}

			//Update Sector Data
			processProductGroup(operMode, Constants.SECTOR, 
					objItemDto.getSectorName(), Constants.MERCHANTISEDEPARTMENT, 
					merchCode, objItemDto.getSectorCode() , itemdao , "");
		
		} catch (Exception e) {
			logger.error(" Error In setupProductGroupHierarchyV2 " , e);
			throw new GeneralException(" Error In setupProductGroupHierarchyV2 " , e);
		}
	}
	
	
	/*
	 *********************************************************************
	 * Method to insert or update or delete Product group and its relation
	 * Argument 1: operMode
	 * Argument 2: proLevelId
	 * Argument 3: productName
	 * Argument 4: childProLevelId
	 * Argument 5: childProduct
	 * Argument 6: code
	 * Argument 7: itemdao
	 * @throws GeneralException
	 * ******************************************************************
	 */	
	private void processProductGroup(String operMode, int proLevelId, 
		String productName, int childProLevelId, int childProduct,  
			String code, ItemDAO itemdao, String addonType) throws GeneralException{
		
		int productId = 0;
		
		if( _productMap.containsKey( proLevelId + "_" + productName)){
			productId = _productMap.get(proLevelId + "_" + productName);
		}
		else{
			productId = itemdao.processProductGroup(_Conn, productName, code, addonType,  proLevelId);
			_productMap.put(proLevelId + "_" + productName , productId);
		}
			
		
		if (operMode.equalsIgnoreCase("A") || operMode.equalsIgnoreCase("C")){								
			//logger.info(" Insert Or Update.... " +proLevelId +"_" + productName+"_"+childProduct);
			if( productId > 0)
			itemdao.processProductGroupChild(_Conn , productId ,childProduct, proLevelId , childProLevelId);
		 
		}
		else if (operMode.equalsIgnoreCase("D")){
			if (childProLevelId == Constants.ITEMLEVELID) {
				
				//logger.info(" Delete.... " +proLevelId +"_" + productName+"_"+childProduct);
				itemdao.deleteProductGroupRelation(_Conn, productId, proLevelId, childProduct , childProLevelId);
			}
		}
	}
	
	
	
	   
	/*
	 *****************************************************************
	 * Method to get Item Code from given UPC
	 * Argument 1: UPC code
	 * @throws Exception
	 * ****************************************************************
	 */	
	public int getItemCode(String strUpc) throws GeneralException{
    	int itemCode=-1;
    	String itemUpc = PrestoUtil.castUPC(strUpc, false);
    	//logger.debug("UPC Original...." + strUpc);
		ItemDAO objItemDao = new ItemDAO();
		itemCode = objItemDao.getItemCodeForUPC(_Conn, itemUpc);
		return itemCode;
	}
	
    
	/*
	 *****************************************************************
	 * Static method to log the command line arguments
	 *****************************************************************
	 */	
    private static void logCommand (String[] args) {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: TopsItemSetup ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }	
    
	
    /*
	 *********************************************************************
	 * Method to load GAS UPC list
	 * @throws 
	 * ******************************************************************
	 */	
	private void loadGasUpc() {
			
		// Load the Gas Upc from Properties File 
		String gasUPCDetails = PropertyManager.getProperty("SA_GAS_UPC", null);

		if (gasUPCDetails != null) {
			String[] gasItem = gasUPCDetails.split(",");
			for (int ii = 0; ii < gasItem.length; ii++) {
				logger.info("GAS UPC" + gasItem[ii]);

				_gasUPCMap.put(gasItem[ii].trim(), gasItem[ii].trim());
			}
		}

		
		
	}

	// Loading Product Group Tables - Price Index Portfolio Support
	/**
	 * This method inserts/updates LIG itemcodes into product_group_relation table
	 * Item Code > 970000
	 */
	private void setupLIGInProductGroup() {
		logger.debug("Inside setupLIGInProductGroup() of TopsItemSetup");
		ItemDAO itemDAO = new ItemDAO();
		try{
			
			CachedRowSet crs = itemDAO.getLIRItems(_Conn, false);
			while (crs.next()){
				recordCount++;
				int retLirId= crs.getInt("RET_LIR_ID");
				int modelItemCode = itemDAO.getLirMinItemCode( _Conn, retLirId, 0);
				itemDAO.setLirItemInProductGroup(_Conn, modelItemCode); 
			}
			PristineDBUtil.commitTransaction(_Conn, "LIG setup in product group");
		}catch(SQLException exception){
			logger.error("Exception in setting LIG items in product group " + exception);
		}catch(GeneralException exception){
			logger.error("Exception in setting LIG items in product group " + exception);
		}
	}
	
	// Loading Product Group Tables - Price Index Portfolio Support
	private void setUpGasInProductGroupTables() throws GeneralException{
		logger.debug("Inside setUpGasInProductGroupTables() of TopsItemSetup");
		ProductGroupDAO productDAO = new ProductGroupDAO();
		ItemDAO itemDAO = new ItemDAO();
		ItemDTO itemDTO = new ItemDTO();
		itemDTO.posDept = String.valueOf(Constants.GASPOSDEPARTMENT);
		itemDTO.posDeptDesc = "GAS";
		itemDTO.financeDept = "GAS";
		String gasUPC = PropertyManager.getProperty("SA_GAS_UPC");
		String[] gasUpcArr = gasUPC.split(",");
		for(String upc : gasUpcArr){
			int itemCode = itemDAO.getItemCodeForUPC(_Conn, PrestoUtil.castUPC(upc, false));
			itemDTO.itemCode = itemCode;
			itemDTO.upc = upc;
			productDAO.populateProductGroupHierarchy(_Conn, itemDTO);
		}
	}
}
