package com.pristine.dataload.tops;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

import com.pristine.dao.ItemDAO;
import com.pristine.dao.offermgmt.BrandRelationDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.LigDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.pricingalert.ExportToExcel;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;


/**
 * This program uses the following steps
 * 
 * Input is the Category ID and LIG file (i.e. path containing the LIG file).
 * 
 * Step1 - Read the records in the file and ignore records where the size family and brand family are blank
 * This steps member items in LIG as  because it meets the above condition and reduces the # of records to process
 * 
 * Step 2- Identify items belonging to the category and ignore other items
 * 
 * Step 3 - Create Group based on Size relationship. If Size relationship is , assign it to its own PG
 * 
 * Step 4 - Assign Size Lead based on whether item belongs to KVI or K2 or smaller size
 * 
 * Step 5 - Create Price Group based on Brand Family
 * 
 * Step 6 - Assign PG Lead Item
 * 
 * Step 7 - Assign PG Name
 * 
 * Step 8 - Build Tier Relationship
 * 
 * Step 9 - PRint to excel
 */
public class PriceGroupFormatter extends PristineFileParser{
	private static Logger logger = Logger.getLogger("PriceGroupFormatter");
	
	private Connection conn = null;
	private ItemDAO itemdao = null;
	private static int columnCount = 10;
	private static int retailerItemCodeLength = 6;
	private int recordCount = 0;
	HashMap<String, Integer> retItemCodeAndItsVersion = new HashMap<String, Integer>();
	private HashMap<String, LigDTO> ligRecords = new HashMap<String, LigDTO> ();
	private HashMap<String, LigDTO> categoryRecords = new HashMap<String, LigDTO> ();
	private HashMap<String, List<LigDTO>> initialPG = new HashMap<String, List<LigDTO>> ();
	private HashMap<String, List<LigDTO>> finalPG = new HashMap<String, List<LigDTO>> ();
	private String categoryName;
	private String chainName = "TOPS";
	private HashMap<Integer, String> brandOverrideMap;
	
	private String startDate = null;
	
	
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public void setInitialPriceGroup(HashMap<String, List<LigDTO>> inputPG) {
		initialPG = inputPG;
		
	}
	
	public HashMap<String, List<LigDTO>> getInitialPriceGroup() {
		return initialPG;
	}
	
	public HashMap<String, List<LigDTO>> getFinalPriceGroup() {
		return finalPG;
		
	}

	
	
	public PriceGroupFormatter(){
		PropertyConfigurator.configure("log4j-lig-loader.properties");
		PropertyManager.initialize("analysis.properties");
		conn = getOracleConnection();
		itemdao = new ItemDAO();
	}
	
	/**
	 * Main method of Retail Price Setup Batch
	 * @param args[0]	Relative path of the input file
	 */
	public static void main(String[] args) {
		PriceGroupFormatter pgFormatter = new PriceGroupFormatter();
		pgFormatter.formatData(args);
	}
	
	private void formatData(String[] args) {
		try {
			logger.info("Price Group formatter Starts");
			
			String relativePath = null;
			String categoryId = null;
			for (int ii = 0; ii < args.length; ii++) {

				String arg = args[ii];

				if (arg.startsWith("RELATIVE_PATH")) {
					relativePath = arg.substring("RELATIVE_PATH=".length());
				}

				if (arg.startsWith("CATEGORY_ID")) {
					categoryId = arg.substring("CATEGORY_ID=".length());
				}
			}
			
			if( categoryId == null ){
				logger.error("Category Id is not passed, pass it as CATEGORY_ID=");
				return;
			}
			
			if( relativePath == null ){
				logger.error("Relative Path not passed, pass it as RELATIVE_PATH=");
				return;
			}
			
			ArrayList<String> zipFileList = getZipFiles(relativePath);
			ArrayList<String> fileList = getFiles(relativePath);

			// Continue only if there are files
			if ( zipFileList.size() > 0 || fileList.size() > 0) {
				processLIGFile(relativePath);
				logger.info( "No of records in file : " + recordCount );
				logger.info( "No of records after initial filtering : " + ligRecords.size());
				prepareData(Integer.parseInt(categoryId));
				logger.info( "No of records after category : " + categoryRecords.size());
				prepareInitialPriceGroup();
				identifySizeLead();
				prepareFinalPriceGroup();
				AssignTiersOnFinalPG();
				AssignPGLead();
				SetupBrandRelationship();
				
				createPriceGroupExcel(relativePath, categoryId);
				performCloseOperation(true);
				logger.info("Price Group formatter Complete");
			} else {
				logger.error("No input files present");
			}
		} catch (GeneralException ge) {
			logger.error("Error in Price Group Formatter. Transaction is rollbacked - ", ge);
			performCloseOperation(false);
		}finally{
			
		}

	}
	
	
	private void  processLIGFile(String relativePath) throws GeneralException{
		try
		{		
			//getzip files
			ArrayList<String> zipFileList = getZipFiles(relativePath);
			
			//Start with -1 so that if any regular files are present, they are processed first
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
						
						long fileProcessStartTime = System.currentTimeMillis(); 
						
					    String files = fileList.get(i);
					    logger.info("File Name - " + fileList.get(i));
					    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
					    
					    String fieldNames[] = new String[11];
		
				    	fieldNames[0] = "lineGroupIdentifier";
				    	fieldNames[1] = "retailerItemCode";
				    	fieldNames[2] = "internalItemNo";
				    	fieldNames[3] = "lineGroupStatus";
				    	fieldNames[4] = "sizeClass";
				    	fieldNames[5] = "sizeFamily";
				    	fieldNames[6] = "brandClass";
				    	fieldNames[7] = "brandFamily";
				    	fieldNames[8] = "upc";
				    	fieldNames[9] = "kviCode";
				    	
				    	String []fileFields  = mapFileField(fieldNames, columnCount); 
				    	recordCount = 0;
				    	super.parseDelimitedFile(LigDTO.class, files, '|',fileFields, stopCount);
				    	
						logger.info("Number of records processed - " + recordCount);
					    long fileProcessEndTime = System.currentTimeMillis();
					    logger.info("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime) + "ms");
					}
				} catch (GeneralException | Exception ex) {
					logger.error("GeneralException", ex);
					commit = false;
				}
				
				if( processZipFile){
			    	PrestoUtil.deleteFiles(fileList);
			    	fileList.clear();
			    	fileList.add(zipFileList.get(curZipFileCount));
			    }
			    String archivePath = getRootPath() + "/" + relativePath + "/";
			    
				if (commit) {
					// if zip file, move the zip file
					//PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
					throw new GeneralException("Error in processLIGInfo()");
				}
				curZipFileCount++;
				processZipFile = true;
			}while (curZipFileCount < zipFileList.size());
		}catch (GeneralException | Exception ex) {
	        logger.error("Outer Exception -  GeneralException", ex);
	        throw new GeneralException("Error in processLIGInfo()");
	    }
	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		loadLIGInfo(listobj);
	}
	
	private void loadLIGInfo(List ligListObject) throws GeneralException{
		List<LigDTO> itemList = (List<LigDTO>) ligListObject;
		for (int j = 0; j < itemList.size(); j++) {
			recordCount++;
			LigDTO ligDTO = itemList.get(j);
			
			String retItemCode = ligDTO.getRetailerItemCode().substring(0, ligDTO.getRetailerItemCode().indexOf("_"));
			retItemCode = retItemCode.substring(retItemCode.length() - retailerItemCodeLength);
			ligDTO.setRetailerItemCodeNoVer(retItemCode);
			
			String strItemCodeVersion = ligDTO.getRetailerItemCode().substring(ligDTO.getRetailerItemCode().indexOf("_") + 1);
			int itemCodeVersion = Integer.valueOf(strItemCodeVersion);
			
			if( retItemCodeAndItsVersion.containsKey(retItemCode)){
				int processedItemCodeVersion = retItemCodeAndItsVersion.get(retItemCode);
				if( processedItemCodeVersion > itemCodeVersion) {
					continue;
				}
			}
			
			//Ignore items that doesn't belong to Size relationship or Brand Relationship
			if( ligDTO.getSizeFamily().isEmpty() &&  ligDTO.getBrandFamily().isEmpty()){
				continue;
			}
			
			
			//let the for loop continue
			retItemCodeAndItsVersion.put(retItemCode,itemCodeVersion);
			ligRecords.put(retItemCode, ligDTO);
		}

	}

	
	private void prepareData(int CategoryId) throws GeneralException {
		
		//Load any Brand/tier overriders
		
		BrandRelationDAO brandRelationDao = new BrandRelationDAO();
		brandOverrideMap = brandRelationDao.getBrandRelationshipOverride(conn, Constants.CATEGORYLEVELID, CategoryId);
		
		List<ItemDTO> itemsInCategory = itemdao.getItemDetailsInACategory(conn, Constants.CATEGORYLEVELID, CategoryId);
		
		categoryName = itemdao.getProductGroupName(conn, Constants.CATEGORYLEVELID, CategoryId).trim();
		categoryName = StringUtils.replace( categoryName, " ", "-");
		categoryName = StringUtils.replace( categoryName, "/", "-");
		
		logger.info("Preparing Price Group for Category: " + categoryName);
		
		HashMap <String, ItemDTO> itemMap  = new HashMap <String, ItemDTO> ();
		
		//put it in a HashMap
		for( ItemDTO  item:itemsInCategory){
			itemMap.put(item.retailerItemCode, item);
		}
		
		categoryRecords.clear();
		
		
		HashMap <String, List<LigDTO>> ligMap = new HashMap<String, List<LigDTO>>();

		//Keep only records for the category
		for (String key : ligRecords.keySet()) {
			    if( itemMap.containsKey(key)) {
			    	LigDTO  valueDTO = ligRecords.get(key);
			    	
			    	ItemDTO item = itemMap.get(key);
			    	//Add size, Brand, UOM to valueDTO
			    	valueDTO.setBrandName(item.brandName);
			    	valueDTO.setItemSize(item.size);
			    	valueDTO.setUomName(item.uom);
			    	valueDTO.setItemName(item.itemName);
			    	//assume initially as Item level relationship but will get adjusted below in the next block.
			    	valueDTO.setItemLevelRelationship(true);
			    	valueDTO.setItemLevelBrandRelationship(true);
			    	valueDTO.setItemLevelSizeRelationship(true);
			    	// Add one record for LIG and not all members
	
			    	String ligName = valueDTO.getLineGroupIdentifier().trim();
			    	if( !ligName.isEmpty()){
			    		if(ligMap.containsKey(ligName)){
			    			List<LigDTO> ligItemRecList = ligMap.get(ligName);
			    			
			    			
			    			//TO DO SG ->
			    			// Look for exact matches in Size and Relation of new record with Original records, If exists do not consider
			    			boolean extactMatch=false;
			    			for( LigDTO ligItemRec : ligItemRecList){
			    				if(ligItemRec.getSizeFamily().trim().equals(valueDTO.getSizeFamily().trim()) && 
			    				   ligItemRec.getBrandFamily().trim().equals(valueDTO.getBrandFamily().trim()))
			    					extactMatch = true;
			    			}
			    			
			    			boolean keep = false;
			    			for( LigDTO ligItemRec : ligItemRecList){
			    				keep  = compareSizeBrandRelationships( ligItemRec, valueDTO);
			    				if( keep ) break;
			    			}
			    			
			    			if( keep && !extactMatch){
					    		categoryRecords.put(key, valueDTO);
			    				ligItemRecList.add(valueDTO);
			    			}
				    			
				    	} else {
				    		categoryRecords.put(key, valueDTO);
				    		ArrayList<LigDTO> al = new ArrayList<LigDTO> ();
				    		al.add(valueDTO);
				    		ligMap.put(ligName, al);
					    }
			    } else
			    	categoryRecords.put(key, valueDTO);
		    }
		}
		

		for (String ligName : ligMap.keySet()){
			List<LigDTO> ligItemRecList = ligMap.get(ligName);
			if( ligItemRecList.size() == 1){
				LigDTO valueDTO = ligItemRecList.get(0);
				valueDTO.setItemLevelRelationship(false);
		    	valueDTO.setItemLevelBrandRelationship(false);
		    	valueDTO.setItemLevelSizeRelationship(false);

			} else {
				fillSizeAndBrandRelationships(ligItemRecList);
			}
		}
		
	}

	
	private void fillSizeAndBrandRelationships(List<LigDTO> ligItemRecList) {
		// TODO Auto-generated method stub
		//find the item with most Size Relationship
		//find the item with most Brand Relationship
		//Use it to populate any empty Size and Brand Relationships
		
		HashMap <String, Integer> brandCountMap = new HashMap <String, Integer> ();
		HashMap <String, Integer> sizeCountMap = new HashMap <String, Integer> ();
		for( LigDTO ligItemRec: ligItemRecList){
			
			String brandFamily = ligItemRec.getBrandFamily().trim();
			int count = 1;
			if(!brandFamily.isEmpty() ) {
				if( brandCountMap.containsKey(brandFamily)){
					count = brandCountMap.get(brandFamily);
					count++;
				}
				brandCountMap.put(brandFamily, count);
			}
		
			String sizeFamily = ligItemRec.getSizeFamily().trim();
			count = 1;
			if(!sizeFamily.isEmpty()) {
				if( sizeCountMap.containsKey(sizeFamily)) {
					count = sizeCountMap.get(sizeFamily);
					count++;
				}
				sizeCountMap.put(sizeFamily, count);
			}
		}
		
		//If there is only one size relationship and one brand relationship, the make Item level relationship as false
		
		if( sizeCountMap.size() == 1  && brandCountMap.size() == 1 ) {
			
			String sizeFamily="";
			String sizeClass="";
			String brandFamily = "";
			String brandClass = "";
			List<String> itemCodeToRemoveList = new ArrayList<String>();
			String itemCodeToKeep ="";
			int count = 0;
			
			for( LigDTO ligItemRec: ligItemRecList){
				
				ligItemRec.setItemLevelSizeRelationship(false);
				ligItemRec.setItemLevelBrandRelationship(false);
				
				String tempBrandFamily = ligItemRec.getBrandFamily().trim();
				if( !tempBrandFamily.isEmpty()){
					brandFamily = tempBrandFamily;
					brandClass = ligItemRec.getBrandClass().trim();
				}
				String tempSizeFamily = ligItemRec.getSizeFamily().trim();
				if( !tempSizeFamily.isEmpty()){
					sizeFamily = tempSizeFamily;
					sizeClass = ligItemRec.getSizeClass();
				}
				
				if( count > 0 ) 
					itemCodeToRemoveList.add(ligItemRec.getRetailerItemCodeNoVer());
				else 
					itemCodeToKeep = 	ligItemRec.getRetailerItemCodeNoVer(); 
				
				count++;
			}
			
			//Merge the records
			
			LigDTO ligItemRec = categoryRecords.get(itemCodeToKeep);
			ligItemRec.setBrandFamily(brandFamily);
			ligItemRec.setBrandClass(brandClass);
			ligItemRec.setSizeClass(sizeClass);
			ligItemRec.setSizeFamily(sizeFamily);
			
			for( String retailerItemCode: itemCodeToRemoveList)
				categoryRecords.remove(retailerItemCode);
			
		}
		
		if( brandCountMap.size() > 1 &&  sizeCountMap.size() == 1 ||    sizeCountMap.size() > 1 &&  brandCountMap.size() == 1 ) {
			for( LigDTO ligItemRec: ligItemRecList){
				//ligItemRec.setItemLevelBrandRelationship(false);
				logger.error("Price Group Issue with Item Code : "  + ligItemRec.getRetailerItemCodeNoVer() + " ITEM: " + ligItemRec.getItemName() + " LIR NAME:" + ligItemRec.getLineGroupIdentifier()) ;
			}
		}
		
		for( LigDTO ligItemRec: ligItemRecList){
			ligItemRec.setItemLevelRelationship(ligItemRec.isItemLevelBrandRelationship() || ligItemRec.isItemLevelSizeRelationship());
		}
		/*
		int brandFamilyCount = 0;
		String brandFamilyNameToUse = "";
		for(String key : brandCountMap.keySet()){
			int count = brandCountMap.get(key);
			if( count > brandFamilyCount){
				brandFamilyCount = count;
				brandFamilyNameToUse = key;
			}
		}
		
		int sizeFamilyCount = 0;
		String sizeFamilyNameToUse ="";
		for(String key : sizeCountMap.keySet()){
			int count = sizeCountMap.get(key);
			if( count > sizeFamilyCount){
				sizeFamilyCount = count;
				sizeFamilyNameToUse = key;
			}
		}
		
		
		for( LigDTO ligItemRec: ligItemRecList){
			String brandFamily = ligItemRec.getBrandFamily().trim();

			if( brandCountMap.size() < 2){

				if(brandFamily.isEmpty() && !brandFamilyNameToUse.isEmpty() )
					ligItemRec.setBrandFamily(brandFamilyNameToUse);
		
			}
			
			if( sizeCountMap.size() < 2){
				String sizeFamily = ligItemRec.getSizeFamily().trim();
				if(sizeFamily.isEmpty() && !sizeFamilyNameToUse.isEmpty())
					ligItemRec.setSizeFamily(sizeFamilyNameToUse);
			}
		}
		
		boolean duplicateExists = false;
		
		do {
			duplicateExists = false;
			LigDTO baseLigItem = ligItemRecList.get(0);
				
			if( ligItemRecList.size() > 1){
				for( int i = 1; i < ligItemRecList.size(); i++){
					LigDTO compareLigItem = ligItemRecList.get(i);
					//Check for duplicate		
					if(baseLigItem.getSizeFamily().trim().equals(compareLigItem.getSizeFamily().trim()) && 
							baseLigItem.getBrandFamily().trim().equals(compareLigItem.getBrandFamily().trim())){
						duplicateExists = true;
						if( !baseLigItem.getBrandClass().trim().isEmpty() && compareLigItem.getBrandClass().trim().isEmpty())
							compareLigItem.setBrandClass(baseLigItem.getBrandClass());
						break;
					}
				}
			}
			//If exists remove the one with empty
			if( duplicateExists){
				categoryRecords.remove(baseLigItem.getRetailerItemCodeNoVer());
				ligItemRecList.remove(0);
				
			}
		} while (duplicateExists);
		
		if( ligItemRecList.size() == 1){
			LigDTO valueDTO = ligItemRecList.get(0);
			valueDTO.setItemLevelRelationship(false);
		}*/
	}

	public boolean compareSizeBrandRelationships(LigDTO ligItemOrg,
			LigDTO ligItemNew) {
		boolean keepNew = false;
		
		boolean itemOrgHasSizeRelation = !ligItemOrg.getSizeFamily().trim().isEmpty();
		boolean itemNewHasSizeRelation = !ligItemNew.getSizeFamily().trim().isEmpty();
		
		boolean itemOrgHasBrandRelation = !ligItemOrg.getBrandFamily().trim().isEmpty();
		boolean itemNewHasBrandRelation = !ligItemNew.getBrandFamily().trim().isEmpty();
		
		if( itemOrgHasSizeRelation || itemNewHasSizeRelation){
			// If new and orginal have Size and they are different keep
			if( itemOrgHasSizeRelation && itemNewHasSizeRelation && 
					!ligItemOrg.getSizeFamily().trim().equals(ligItemNew.getSizeFamily().trim()))
				keepNew = true;
			
			// If Org no size and B has size , keep true
			if( !itemOrgHasSizeRelation && itemNewHasSizeRelation)
				keepNew = true;
		}
		
		if( itemOrgHasBrandRelation || itemNewHasBrandRelation){
			// If new and orginal have Brand and they are different keep
			if( itemOrgHasBrandRelation && itemNewHasBrandRelation && 
					!ligItemOrg.getBrandFamily().trim().equals(ligItemNew.getBrandFamily().trim()))
				keepNew = true;
			// If Org no Brand and B has Brand , keep true
			if( !itemOrgHasBrandRelation && itemNewHasBrandRelation)
				keepNew = true;
		}
		
		return keepNew;
	}

	public void initializeValues(HashMap<String, LigDTO> ligRecs, HashMap<String, LigDTO> catRecs, String catName, String retailerName){
		ligRecords = ligRecs;
		categoryRecords=catRecs;
		categoryName = catName;
		chainName = retailerName;
		brandOverrideMap = new HashMap<Integer, String> ();
	}

	public void performCloseOperation(boolean commit){
		super.performCloseOperation(conn, commit);

	}
	
	public void prepareInitialPriceGroup() {
		
		initialPG.clear();
		
		int pgCounter = 1;
		
		for(LigDTO valueDTO : categoryRecords.values()){
			String sizeFamilyName = valueDTO.getSizeFamily().trim();
			if( sizeFamilyName.isEmpty()){
				//Create a PG as PG_1, PG_2 etc 
				List<LigDTO> al = new ArrayList<LigDTO>();
				al.add(valueDTO);
				initialPG.put("PG_"+pgCounter, al);
				pgCounter++;
				continue;
			}
			List<LigDTO> al = null;
			if( initialPG.containsKey(sizeFamilyName)){
				al = initialPG.get(sizeFamilyName);
				al.add(valueDTO);
			}else {
				al = new ArrayList<LigDTO>();
				al.add(valueDTO);
				initialPG.put(sizeFamilyName, al);
			}
		}
		
		logger.info( "No of initial Price Groups : " + initialPG.size());
	}
	
	//assign Size lead

	public void identifySizeLead() {
		
		int sizeLeadCount = 0;
		for( List<LigDTO> candidatePGList: initialPG.values()) {
			
			String sizeLeadItemCode = null;
			float sizeLeadItemSize = -1;
			boolean isKVI = false;
			boolean isK2 = false;
			
			for( LigDTO valueDTO : candidatePGList){
				
				boolean currentItemAsSizeLead = false;
				boolean doSizeCheck = false;
				//logger.debug("Size Family : " + valueDTO.getSizeFamily());
				if(valueDTO.getSizeFamily().isEmpty()) continue;
				if( sizeLeadItemCode == null){
					currentItemAsSizeLead = true;
				}else if(valueDTO.getKviCode().trim().equals(Constants.KVI)) {
					if( !isKVI) currentItemAsSizeLead = true;
					else doSizeCheck = true;
				
				}else if( !isKVI && valueDTO.getKviCode().trim().equals(Constants.K2)){
					if( !isK2) currentItemAsSizeLead = true;
					else doSizeCheck = true;
				} else {
					//size match
					doSizeCheck = true;
				}
				
				if( doSizeCheck ){
					float currentItemSize = -1;
					if(!valueDTO.getItemSize().isEmpty())
						currentItemSize = Float.parseFloat(valueDTO.getItemSize().trim());
					if( sizeLeadItemSize > 0 &&  currentItemSize > 0 &&  currentItemSize < sizeLeadItemSize)
						currentItemAsSizeLead = true;
				}
				
				if( currentItemAsSizeLead){
					sizeLeadItemCode = valueDTO.getRetailerItemCodeNoVer();
					isKVI = valueDTO.getKviCode().trim().equals(Constants.KVI);
					isK2 = valueDTO.getKviCode().trim().equals(Constants.K2);
					if(! valueDTO.getItemSize().isEmpty())
						sizeLeadItemSize = Float.parseFloat(valueDTO.getItemSize().trim());
					else 
						sizeLeadItemSize = -1;
						
				}
			}
			
			if( sizeLeadItemCode != null ){
				for( LigDTO valueDTO : candidatePGList){
					if( valueDTO.getRetailerItemCodeNoVer().equals(sizeLeadItemCode)) {
						valueDTO.setSizeLead("Y");
						sizeLeadCount++;
						break;
					}
				}
			}
				
		}
		logger.info( "No of Size Leads Identified : " + sizeLeadCount);
		
	}
	
	
	public void prepareFinalPriceGroup() {
		finalPG.clear();
		
		int pgCounter = 1;
		
		do{
			if(initialPG.size() > 0 ){
				List<String> candidatePGNames = new ArrayList<String> (initialPG.keySet());
				String firstPG = candidatePGNames.get(0);
				List<LigDTO> firstPGItems = initialPG.get(firstPG);
				
				Set<String> brandFamilySet = new HashSet<String>();
				for( LigDTO pgItem: firstPGItems){
					if( !pgItem.getBrandFamily().isEmpty())
						brandFamilySet.add(pgItem.getBrandFamily().trim());
				}
				if( brandFamilySet.size() == 0){
					
					//Check if there is option to merge by LIG Names
					List<String> matchLigGroups = matchPGByLIG(initialPG, firstPGItems);
					
					if( matchLigGroups.size() > 1 ){
						  //** Merge the two PGs
						  // Remove second PG
						  // Let the loop cont
						mergePriceGroups(matchLigGroups);
						
					} else {
						//The following is no condition
						//This family has only size relationship and can be a PG by itself
						finalPG.put(firstPG, firstPGItems);
						//remove it from the initial PG
						initialPG.remove(firstPG);
					}
				}else {
					
					List<String> matchingBrandFamilyPG = matchPGByBrandFamily(initialPG, brandFamilySet);
					
					if( matchingBrandFamilyPG.size() == 1){
						
						// if yes
						  //** Merge the two PGs
						  // Remove second PG
						  // Let the loop cont
						List<String> matchLigGroups = matchPGByLIG(initialPG, firstPGItems);
						if( matchLigGroups.size() > 1){
							mergePriceGroups(matchLigGroups);
						}else {
							// No Condition
							firstPG = matchingBrandFamilyPG.get(0);
							finalPG.put(categoryName + "_PG_" + pgCounter,initialPG.get(firstPG));
							pgCounter++;
							initialPG.remove(firstPG);
						}
					}else{
						//Merge PGs since they have common Brand Family
						mergePriceGroups(matchingBrandFamilyPG);
					}
				}
				
			}
			
		}while(initialPG.size()>0);
		
		logger.info( "No of Final Price Groups : " + finalPG.size());
	}

	private List<String> matchPGByBrandFamily( HashMap<String, List<LigDTO>> initialPG2, Set<String> brandFamilySet) {
		
		Set<String> pgsToMerge = new HashSet<String> ();
		for( String key : initialPG2.keySet()){
			
			List<LigDTO> pgItemList = initialPG2.get(key);
			for( LigDTO valueDTO : pgItemList){
				if(!valueDTO.getBrandFamily().isEmpty() && brandFamilySet.contains(valueDTO.getBrandFamily().trim()) ){
					pgsToMerge.add(key);
					break;
				}
			}
			
		}
		return new ArrayList<String>(pgsToMerge);
	}

	
	//This was added since support by items within LIG relationship was added
	protected List<String> matchPGByLIG( HashMap<String, List<LigDTO>> initialPG2, List<LigDTO> firstPGItems) {
		
		Set<String> ligSet = new HashSet<String>();
		for( LigDTO pgItem: firstPGItems){
			if( !pgItem.getLineGroupIdentifier().isEmpty())
				ligSet.add(pgItem.getLineGroupIdentifier().trim());
			else
				ligSet.add(pgItem.getRetailerItemCodeNoVer());
		}
		
		Set<String> pgsToMerge = new HashSet<String> ();
		if( ligSet.size()>0) {
			for( String key : initialPG2.keySet()){
				
				List<LigDTO> pgItemList = initialPG2.get(key);
				for( LigDTO valueDTO : pgItemList){
					if(!valueDTO.getLineGroupIdentifier().isEmpty() ){
						if( ligSet.contains(valueDTO.getLineGroupIdentifier().trim())){
							pgsToMerge.add(key);
							break; 
						}
					}
					else {
						if( ligSet.contains(valueDTO.getRetailerItemCodeNoVer())){
							pgsToMerge.add(key);
							break; 
						}
					}
				}
					
			}
			
		}
		return new ArrayList<String>(pgsToMerge);
	}
	

	
	private void mergePriceGroups( List <String> mergePGSet) {
		String firstPG = null;
		List<LigDTO> firstPGItems = null;
		for( String pgName: mergePGSet){
			if( firstPG == null ){
				firstPG = pgName; 
				firstPGItems = initialPG.get(firstPG);
			} else {
				String secondPG = pgName;
				List<LigDTO> secondPGItems = initialPG.get(secondPG);
				//Merge first and second List
				firstPGItems.addAll(secondPGItems);
				initialPG.remove(secondPG);
			}
		}
	}

	/**
	 * This method returns an array of fields in input file
	 * @param fieldNames
	 * @param columnCount2
	 * @return String array
	 */
	public static String[] mapFileField(String[] fieldNames, int columnCount2) {
		int fieldLength = columnCount2 < fieldNames.length? columnCount2 :fieldNames.length;
		String [] fileFields = new String[fieldLength];
		for ( int i = 0; i < fieldLength; i++)
			fileFields[i] = fieldNames[i];
		return fileFields;
	}


	
	public void AssignPGLead() {
		
		int pgLeadCount = 0;
		for( String pgGroupName: finalPG.keySet()) {
		
			List<LigDTO> finalPGList = finalPG.get(pgGroupName);
			//1 - if PG name is same as Size lead, then, use it to assign PG Lead
			// continue the loop
			
			LigDTO firstDTO = finalPGList.get(0);
				
			if(!firstDTO.getSizeFamily().isEmpty() && pgGroupName.equals(firstDTO.getSizeFamily())){
				setPriceGroupLead(finalPGList,firstDTO.getSizeFamily());
				pgLeadCount++;
				continue;
 			}

			boolean isNationalPresent = false;
			boolean isCorpBrandPresent = false;
			//- 2 Indicate presence of any NB or CORP_BRAND items
			for(  LigDTO valueDTO : finalPGList){
				if( valueDTO.getTier() == 1){
					isCorpBrandPresent = true;
				}
				else if( valueDTO.getTier() == 0){
					isNationalPresent = true;
				}
			}

			//3 - Assign the tier for PG
			int tierForPGLead = -1;
			if( isNationalPresent ) tierForPGLead = 0;
			else if (isCorpBrandPresent) tierForPGLead = 1;
			
			// Are there KVI or K2 or not within the tier
		
			String pgLeadItemCode = null;
			float pgLeadItemSize = -1;
			boolean isKVI = false;
			boolean isK2 = false;
				
			for( LigDTO valueDTO : finalPGList){
				
				boolean currentItemAsPGLead = false;
				boolean doSizeCheck = false;
				if(valueDTO.getTier() !=  tierForPGLead) continue;
				
				if( pgLeadItemCode == null){
					currentItemAsPGLead = true;
				}else if(valueDTO.getKviCode().trim().equals(Constants.KVI)) {
					if( !isKVI) currentItemAsPGLead = true;
					else doSizeCheck = true;
				
				}else if( !isKVI && valueDTO.getKviCode().trim().equals(Constants.K2)){
					if( !isK2) currentItemAsPGLead = true;
					else doSizeCheck = true;
				} else {
					//size match
					doSizeCheck = true;
				}
				
				if( doSizeCheck ){
					float currentItemSize = -1;
					if(!valueDTO.getItemSize().isEmpty())
						currentItemSize = Float.parseFloat(valueDTO.getItemSize().trim());
					if( pgLeadItemSize > 0 &&  currentItemSize > 0 &&  currentItemSize < pgLeadItemSize)
						currentItemAsPGLead = true;
				}
				
				if( currentItemAsPGLead){
					pgLeadItemCode = valueDTO.getRetailerItemCodeNoVer();
					isKVI = valueDTO.getKviCode().trim().equals(Constants.KVI);
					isK2 = valueDTO.getKviCode().trim().equals(Constants.K2);
					if(! valueDTO.getItemSize().isEmpty())
						pgLeadItemSize = Float.parseFloat(valueDTO.getItemSize().trim());
					else 
						pgLeadItemSize = -1;
				}
				
			}
			
			//For that tier, pick the KVI or K2 or lowest size in the family and assign it as Lead
			//If size Family, ensure PG lead is same as Size lead
			
			if( pgLeadItemCode != null ){
				for( LigDTO valueDTO : finalPGList){
					if( valueDTO.getRetailerItemCodeNoVer().equals(pgLeadItemCode)) {
						if( valueDTO.getSizeFamily().isEmpty())
							valueDTO.setPriceGroupLead("Y");
						else
							setPriceGroupLead(finalPGList,valueDTO.getSizeFamily());
						pgLeadCount++;
						break;
					}
				}
			}

		}
		
		logger.info("No of Price Groups Leads Identified - " + pgLeadCount);
	    
	}

	protected void setPriceGroupLead(List<LigDTO> finalPGList, String sizeFamily) {
		for( LigDTO valueDTO : finalPGList){
			if(!valueDTO.getSizeFamily().isEmpty() && valueDTO.getSizeFamily().equals(sizeFamily) && 
					!valueDTO.getSizeLead().isEmpty() && valueDTO.getSizeLead().equals("Y")){
				valueDTO.setPriceGroupLead("Y");
				break;
			}
		}
		
	}

	public void AssignTiersOnFinalPG() {

		for( List<LigDTO> finalPGList: finalPG.values()) {
		
			for( LigDTO valueDTO : finalPGList){
				
				String brandClass = valueDTO.getBrandClass();
				
				//Tops specific Brand Class
				if( brandClass.contains("NATIONAL_BRAND")){
					valueDTO.setTier(0);
					valueDTO.setBrandClass("NATIONAL");
				}
				else if( brandClass.contains("NATIONAL")){
					valueDTO.setTier(0);
				}
				else if (brandClass.contains("CORP_BRAND_FC"))
					valueDTO.setTier(3);
				else if (brandClass.contains("CORP_BRAND_2"))
					valueDTO.setTier(2);
				else if (brandClass.contains("CORP_BRAND"))
					valueDTO.setTier(1);
				//Ahold Specific brand class and tiers
				else if (brandClass.contains("STORE_BRAND_TIER_2"))
					valueDTO.setTier(99);
				else if (brandClass.contains("STORE_BRAND_GRID_1"))
					valueDTO.setTier(1);
				else if (brandClass.contains("STORE_BRAND_GRID_2"))
					valueDTO.setTier(2);
				else if (brandClass.contains("STORE_BRAND_GRID_3"))
					valueDTO.setTier(3);
				else if (brandClass.contains("STORE_BRAND_GRID_4"))
					valueDTO.setTier(4);
				else if (brandClass.contains("STORE_BRAND_GRID_5"))
					valueDTO.setTier(5);
				else if (brandClass.contains("STORE_BRAND_GRID_6"))
					valueDTO.setTier(6);
				else if (brandClass.contains("STORE_BRAND_GRID_7"))
					valueDTO.setTier(7);
				else if (brandClass.contains("STORE_BRAND_GRID_8"))
					valueDTO.setTier(8);
				
				//GEt the tier and apply overrides
				
				if( valueDTO.getTier() > 0 && brandOverrideMap.containsKey(valueDTO.getTier())){
					String tierOverride = brandOverrideMap.get(valueDTO.getTier());
					valueDTO.setTierOverride(tierOverride);
				}
			}
		}
	}
	
	

	public void SetupBrandRelationship() {

			
		
		for( String pgGroupName: finalPG.keySet()) {

			
			List<LigDTO> finalPGList = finalPG.get(pgGroupName);

			LigDTO firstDTO = finalPGList.get(0);
			
			//if only size relationship, skip it as Brand relationship doesn't apply
			if(!firstDTO.getSizeFamily().isEmpty() && pgGroupName.equals(firstDTO.getSizeFamily())) continue;
				
			List<LigDTO> newRelationshipList = new ArrayList<LigDTO> ();
			
			for( LigDTO valueDTO : finalPGList){
				//Skip for National Brand
				if( valueDTO.getTier() == 0) continue;
				List<LigDTO> dependentItemList = identifyDependentItems( valueDTO, finalPGList);
				
				int count = 0;
				
				for( LigDTO dependentDTO : dependentItemList){
					count ++;
					
					//Add the dependent LIG only if the relationship is not at Item level
					if( count == 1 ){
						//Add relationship to base item
						valueDTO.setDependentItemName(dependentDTO.getItemName());
						if( !dependentDTO.isItemLevelRelationship())
							valueDTO.setDependentLIG(dependentDTO.getLineGroupIdentifier());
						valueDTO.setDependentRetailerItemCode(dependentDTO.getRetailerItemCodeNoVer());
					}else {
						//Clone Base item 
						LigDTO newDependent = cloneDTO(valueDTO);
						newDependent.setDependentItemName(dependentDTO.getItemName());
						if( !dependentDTO.isItemLevelRelationship())
							newDependent.setDependentLIG(dependentDTO.getLineGroupIdentifier());
						newDependent.setDependentRetailerItemCode(dependentDTO.getRetailerItemCodeNoVer());
						newRelationshipList.add(newDependent);
					}
					
				}
		
			}
			
			if( newRelationshipList.size()> 0)
				finalPGList.addAll(newRelationshipList);
		}
	}

	public LigDTO cloneDTO(LigDTO baseDTO) {
		 LigDTO dependentDTO  = new LigDTO(); 
		 dependentDTO.setBrandClass(baseDTO.getBrandClass());
		 dependentDTO.setBrandFamily(baseDTO.getBrandFamily());
		 dependentDTO.setBrandName(baseDTO.getBrandName());
		 dependentDTO.setInternalItemNo(baseDTO.getInternalItemNo());
		 dependentDTO.setItemCode(baseDTO.getItemCode());
		 dependentDTO.setItemName(baseDTO.getItemName());
		 dependentDTO.setItemSize(baseDTO.getItemSize());
		 dependentDTO.setKviCode(baseDTO.getKviCode());
		 dependentDTO.setLineGroupIdentifier(baseDTO.getLineGroupIdentifier());
		 dependentDTO.setRetailerItemCode(baseDTO.getRetailerItemCode());
		 dependentDTO.setRetailerItemCodeNoVer(baseDTO.getRetailerItemCodeNoVer());
		 dependentDTO.setSizeClass(baseDTO.getSizeClass());
		 dependentDTO.setSizeFamily(baseDTO.getSizeFamily());
		 dependentDTO.setUomName(baseDTO.getUomName());
		 dependentDTO.setUpc(baseDTO.getUpc());
		 dependentDTO.setTier(baseDTO.getTier());
		 dependentDTO.setTierOverride(baseDTO.getTierOverride());
		 dependentDTO.setItemLevelRelationship(baseDTO.isItemLevelRelationship());
		 return (dependentDTO);
		 
	}

	private List<LigDTO> identifyDependentItems(LigDTO baseDTO,
			List<LigDTO> finalPGList) {
		
		int tierForDependent= -1;
		
		//For FC brand, NB is the dependent
		//For TOPS Brand, NB is the dependent
		// FOR Value Brand, TOPS brand is the dependent
		if( chainName.equals("TOPS")){
			if( baseDTO.getTier() == 3 )
				tierForDependent = 0;
			else
				tierForDependent = baseDTO.getTier() - 1;
		} else {
			tierForDependent =0;
		}
		
		List<LigDTO> dependentList = new ArrayList<LigDTO>();
		
		for( LigDTO dependDTO: finalPGList){
			
			if( dependDTO.getTier() == tierForDependent && !baseDTO.getBrandFamily().isEmpty() &&  !dependDTO.getBrandFamily().isEmpty()
					&& baseDTO.getBrandFamily().equals(dependDTO.getBrandFamily()))
				dependentList.add(dependDTO);
		}
		return dependentList;
	}

	public void createPriceGroupExcel(String relativePath, String CategoryId) throws GeneralException {
		
		//Get the root Path
		String baseFolder = getRootPath() + "/" + relativePath;
		//Get the Template file
		String pgTempFile = baseFolder +"/PGTemplate/PriceGroup_Template.xls";
		String pgDestFile = baseFolder +"/PGOutput/PG_"+ categoryName +"_CatID-"+ CategoryId +".xls";
		//GEt the destination path
		//Copy the excel using the Alert program
		ExportToExcel excelExport = new ExportToExcel();
		boolean copyStatus = excelExport.copyExcel(pgTempFile, pgDestFile);
		if( !copyStatus){
			logger.error("Unable to create Excel file");
			return;
		}
		
		try{
			InputStream inp = new FileInputStream(pgDestFile);    
			    
			HSSFWorkbook wb = new HSSFWorkbook(inp);    
			HSSFSheet sheet = wb.getSheetAt(0);
			HSSFRow headerRow = sheet.getRow(0);
			HSSFCellStyle rowStyle = headerRow.getRowStyle(); 
			
			String weekStartDate = DateUtil.getWeekStartDate(-1);
			if( startDate != null )
				weekStartDate = startDate;
			
			int rowCount = 0;
			
			for( String pgGroupName: finalPG.keySet()) {
				List<LigDTO> finalPGList = finalPG.get(pgGroupName);
				for( LigDTO valueDTO : finalPGList){
					
					if ( !valueDTO.isPrintRow()) continue; // Do not print if the row is marked not be printed.
					
					int colCount = 0;
					HSSFRow row = sheet.getRow(++rowCount); 
					if(row == null){
						row = sheet.createRow(rowCount);
						if( rowStyle != null) row.setRowStyle(rowStyle);
					}
					setCellValue(row, colCount, valueDTO.getBrandName());
					setCellValue(row, ++colCount, valueDTO.getLineGroupIdentifier());
					setCellValue(row, ++colCount, valueDTO.getRetailerItemCodeNoVer());
					setCellValue(row, ++colCount, valueDTO.getItemName());
					setCellValue(row, ++colCount, valueDTO.getItemSize());
					setCellValue(row, ++colCount, valueDTO.getUomName());
					setCellValue(row, ++colCount,"Chain");
					setCellValue(row, ++colCount, chainName);
					setCellValue(row, ++colCount, weekStartDate);
					setCellValue(row, ++colCount, ""); //EndDate
					setCellValue(row, ++colCount, pgGroupName);
					String brandClass = "";
					/*if(valueDTO.getTier() == 0)
						brandClass = "NATIONAL";
					if(valueDTO.getTier() > 0)
						brandClass = "STORE";
					else */
						brandClass = valueDTO.getBrandClass(); 
					setCellValue(row, ++colCount, brandClass); 
					setCellValue(row, ++colCount, valueDTO.getPriceGroupLead());
					if( valueDTO.getTier() > 0 )
						setCellValue(row, ++colCount, Integer.toString(valueDTO.getTier()));
					else
						setCellValue(row, ++colCount, "");
					setCellValue(row, ++colCount, ""); //Brand Reln type (Shelf / Unit) 
					setCellValue(row, ++colCount, valueDTO.getTierOverride()); //Price Relationships among Brands
					String dependentItem ="";
					if(valueDTO.getDependentLIG() != null && !valueDTO.getDependentLIG().isEmpty())
						dependentItem = valueDTO.getDependentLIG();
					else if(valueDTO.getDependentItemName() != null && !valueDTO.getDependentItemName().isEmpty())
						dependentItem = valueDTO.getDependentItemName() + "||Item_Code="+valueDTO.getDependentRetailerItemCode();
					setCellValue(row, ++colCount, dependentItem);
					setCellValue(row, ++colCount, ""); //Precedence among Brands 
					setCellValue(row, ++colCount, ""); //Precedence among size/Brands
					setCellValue(row, ++colCount, valueDTO.getSizeLead());
					setCellValue(row, ++colCount, valueDTO.getSizeFamily());
					setCellValue(row, ++colCount, valueDTO.isItemLevelRelationship() && !valueDTO.getLineGroupIdentifier().trim().isEmpty()?"Y":"");
					setCellValue(row, ++colCount, valueDTO.getSizeClass());
					setCellValue(row, ++colCount, valueDTO.getBrandFamily());
				}
			}
			FileOutputStream fileOut = new FileOutputStream(pgDestFile);  
		    wb.write(fileOut);  
		    fileOut.close(); 
			
		}catch(Exception fe){
			logger.error("File Processing Exception : ", fe);
			throw new GeneralException( fe.getMessage(), fe);
		}

		
		
	}

	private void setCellValue(HSSFRow row, int colCount, String value) {
		HSSFCell cell;
		cell = row.getCell(colCount);
		if (cell == null)    
		    cell = row.createCell(colCount);
		cell.setCellValue(value);		
	}

	
	
	//Unused
	private void prepareDataOld(int CategoryId) throws GeneralException {
		
		List<ItemDTO> itemsInCategory = itemdao.getItemDetailsInACategory(conn, Constants.CATEGORYLEVELID, CategoryId);
		
		categoryName = itemdao.getProductGroupName(conn, Constants.CATEGORYLEVELID, CategoryId).trim();
		categoryName = StringUtils.replace( categoryName, " ", "-");
		categoryName = StringUtils.replace( categoryName, "/", "-");
		
		logger.info("Preparing Price Group for Category: " + categoryName);
		
		HashMap <String, ItemDTO> itemMap  = new HashMap <String, ItemDTO> ();
		
		//put it in a HashMap
		for( ItemDTO  item:itemsInCategory){
			itemMap.put(item.retailerItemCode, item);
		}
		
		categoryRecords.clear();
		
		
		HashMap <String, String> ligMap = new HashMap<String, String>();

		//Keep only records for the category
		for (String key : ligRecords.keySet()) {
			    if( itemMap.containsKey(key)) {
			    	LigDTO  valueDTO = ligRecords.get(key);
			    	
			    	ItemDTO item = itemMap.get(key);
			    	//Add size, Brand, UOM to valueDTO
			    	valueDTO.setBrandName(item.brandName);
			    	valueDTO.setItemSize(item.size);
			    	valueDTO.setUomName(item.uom);
			    	valueDTO.setItemName(item.itemName);
			    	// Add one record for LIG and not all members
	
			    	String ligName = valueDTO.getLineGroupIdentifier().trim();
			    	if( !ligName.isEmpty()){
			    		if(ligMap.containsKey(ligName) ){
			    			String retailerItemCodeVal = ligMap.get(ligName);
			    			LigDTO pgValueDTO = categoryRecords.get(retailerItemCodeVal);
			    			
			    			if( !valueDTO.getSizeFamily().trim().isEmpty()){
			    				pgValueDTO.setSizeFamily(valueDTO.getSizeFamily().trim());
			    				pgValueDTO.setItemSize(valueDTO.getItemSize());
			    				pgValueDTO.setUomName(valueDTO.getUomName());
			    			}
			    			
			    			if( !valueDTO.getBrandFamily().trim().isEmpty()){
			    				pgValueDTO.setBrandFamily(valueDTO.getBrandFamily().trim());
			    			}
			    			
			    			
			    			if( !valueDTO.getBrandClass().trim().isEmpty()){
			    				pgValueDTO.setBrandClass(valueDTO.getBrandClass().trim());
			    			}
				    			
				    	} else {
				    		categoryRecords.put(key, valueDTO);
				    		ligMap.put(ligName, key);
					    }
			    } else
			    	categoryRecords.put(key, valueDTO);
		    }
		}

		
	}
	
	/* This method goes over the final Price group and converts any LB to OZ provided the price group 
	 * contains a mix of both
	 */
	public void normalizeUOM() {
		
		HashSet<String> UOMSet = new  HashSet<String> ();
		
		HashMap<String, List<LigDTO>>finalPG  = getFinalPriceGroup();
		Set<String> keyList = finalPG.keySet();
		
	
		for( String key : keyList){
			List<LigDTO> al = finalPG.get(key);
			UOMSet.clear();
			
			for( LigDTO ligRec : al){
				String uom = ligRec.getUomName();
				if( uom.equals("EA") ){
					uom = "CT";
					ligRec.setUomName(uom);
				}
				
				if( uom.equals("DZ") ){
					uom = "CT";
					ligRec.setUomName(uom);
					float itemSize = Float.parseFloat(ligRec.getItemSize());
					itemSize = itemSize * 12;
					ligRec.setItemSize(Float.toString(itemSize));
				
				}
				
				if( !uom.isEmpty())
					UOMSet.add(uom);
			}
			//Checking for Multiple UOM
			if( UOMSet.size()>1 && UOMSet.contains("OZ") || UOMSet.contains("LB") ||  UOMSet.contains("LT") ){
				for( LigDTO ligRec : al){
					String uom = ligRec.getUomName();
					float itemSize = Float.parseFloat(ligRec.getItemSize());
					if( uom.equals("LB"))
						itemSize = itemSize * 16;
					else if ( uom.equals("LT"))
						itemSize = (float) (itemSize * 33.814);
					else if ( uom.equals("PT"))
						itemSize =  itemSize * 16;
					else if ( uom.equals("QT"))
						itemSize = itemSize * 32 ;
					
					ligRec.setItemSize(Float.toString(itemSize));
					ligRec.setUomName("OZ");
				}
			}
		}
			
	}

}


/*
 * 
 *  CREATE TABLE PR_RELATION_OVERRIDE
 *  (
 *  	product_level_id number(2),
 *  	product_id  number(6),
 *  	tier    number(2),
 *  	brand_relation_ovr varchar2(40)
 *  
 *  
 *  )
 * 
 * Create Synonym for the base schema
 * 
 * 
 * Butter
 * insert into PR_RELATION_OVERRIDE ( product_level_id, product_id, tier, brand_relation_ovr) 
 * values ( 4, 1302, 1, '20-40% below');
 * 
 * Ice cream
 * insert into PR_RELATION_OVERRIDE ( product_level_id, product_id, tier, brand_relation_ovr) 
 * values ( 4, 1266, 1, '20-40% below');
 * */

