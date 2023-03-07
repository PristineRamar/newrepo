package com.pristine.dataload.gianteagle;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dataload.tops.PriceGroupFormatter;
import com.pristine.dataload.tops.PriceGroupFormatterV2;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.LigDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/* In the Input spread-sheet 
 * a) Ensure Brand Sheet matches the layout - remove 2 extra columns
 * b) Keep the relationship as Text - <= 20% instead of Numeric
 * c) Ensure National And Store Brand are setup correctly. 
 * d) Ensure Size and Brand Tabs have the right name
 * e) Cover Sheet needs to be added
 * g) Review UOM
 * 
 */

public class PriceGroupFormatterGaintEagle  extends PristineFileParser {

	private static Logger logger = Logger.getLogger("PriceGroupFormatterGE");
	protected static int recordCount = 0;
	
	protected HashMap<String, LigDTO> ligRecords = new HashMap<String, LigDTO> ();
	protected HashMap<String, LigDTO> categoryRecords = new HashMap<String, LigDTO> ();
	
	protected HashMap<String, List<LigDTO>> relatedItemGroup = new HashMap<String, List<LigDTO>> ();
	protected Connection conn = null;
	
	protected String categoryName = null;
	protected int categoryId = -1;
	protected int categoryLevelId = Constants.CATEGORYLEVELID;
	protected String majorCategoryName = null;
	protected int leadTier = 6;
	
	
	protected PriceGroupFormatterV2 pgFormatter; 
	
	public PriceGroupFormatterGaintEagle(){
		PropertyConfigurator.configure("log4j-lig-loader.properties");
		PropertyManager.initialize("analysis.properties");
		pgFormatter = new PriceGroupFormatterV2();
	}
	
	public static void main(String[] args) {
		
		PriceGroupFormatterGaintEagle pgGE = new PriceGroupFormatterGaintEagle ();
		try{
			pgGE.formatData(args);
		} catch(GeneralException ge){
			logger.error("Error in Price Group Formatter. ", ge);
		}
		catch(Exception ex){
			logger.error("Record Count "+  recordCount);
			logger.error("Unexpected Exception. ", ex);
		}

	}

	private void formatData(String[] args) throws GeneralException {
		
		String relativePath = null;
	
		for (int ii = 0; ii < args.length; ii++) {

			String arg = args[ii];
			if (arg.startsWith("RELATIVE_PATH")) {
				relativePath = arg.substring("RELATIVE_PATH=".length());
			}
		}
		
		if( relativePath == null ){
			logger.error("Relative Path not passed, pass it as RELATIVE_PATH=");
			return;
		}
		
		ArrayList<String> fileList = getFiles(relativePath);
		
		if (fileList.size() > 0) {
			logger.info("# of input files present : " + fileList.size() );
			
			
			for( String fileName : fileList){

				//Reset the values
				relatedItemGroup.clear();
				categoryName = null;
				majorCategoryName = null;
				categoryId = -1;
				
				logger.info("Processing File : " + fileName);
				boolean readStatus = readRecords(fileName);
				logger.info("Total Record Count " + recordCount);
				

				
				cleanUpData();
				prepareDataAtLIRLevel();
				setItemRank();
				resetItemLevelRelationShip();
				setupDepedentRelation();
				setStartDate();
				
				HashMap<String, List<LigDTO>> cleanGroupList = new HashMap<String, List<LigDTO>> (); 
				
				Set<String> keyList = relatedItemGroup.keySet();
				for( String key : keyList){
					List<LigDTO> al = relatedItemGroup.get(key);
					if(key.substring(0, 6).equals("SIZE--"))
						key = key.substring(6); 
					cleanGroupList.put(key, al);
				}
				//prepareData(fileName);
				//Then initiate TOPS program and see if it can be used to format data.
				
				pgFormatter.initializeValues(null, null, categoryName, "GIANT EAGLE CORP");
				pgFormatter.setInitialPriceGroup(cleanGroupList);
				pgFormatter.identifySizeLead();
				pgFormatter.prepareFinalPriceGroup();
				pgFormatter.AssignTiersOnFinalPG();
				pgFormatter.mergeSizeBrandRows();
				pgFormatter.AssignPGLead();
				pgFormatter.normalizeUOM();
				pgFormatter.addLIGRows();
				pgFormatter.createPriceGroupExcel(relativePath, Integer.toString(categoryId));
			}
			//pgFormatter.performCloseOperation(true);
			super.performCloseOperation(conn, false);
			
		}
		else{
			logger.error("No input files present");
		}

	}
	

	protected void setStartDate() throws GeneralException{

	
		String startDate = DateUtil.getWeekStartDate(-1);
		RetailCalendarDAO calendarDAO = new RetailCalendarDAO(); 
		RetailCalendarDTO calDTO = calendarDAO.getCalendarId(conn, startDate, Constants.CALENDAR_WEEK);
				
		
		pgFormatter.setStartDate(calDTO.getStartDate());

	}
	
	
	private void setupDepedentRelation() {
		Set<String> keyList = relatedItemGroup.keySet();
		for( String key : keyList){
			if( key.substring(0, 6).equals("BRAND-") ){
				List<LigDTO > alBrandRelatedList = relatedItemGroup.get(key);
				List<LigDTO > storeBrandList = new ArrayList<LigDTO > (); 
				List<LigDTO > nationalBrandList = new ArrayList<LigDTO > ();

				//segregate SB And NB
				for( LigDTO ligrec: alBrandRelatedList){
					if( ligrec.getBrandClass().trim().equalsIgnoreCase("STORE"))
						storeBrandList.add(ligrec);
					else
						nationalBrandList.add(ligrec);
				}
				
				

				
				//If there is one SB and One NB, Assign NB to SB and clear NB override
				//IF there are multiple NB, clone SB and assign it
				
				for( LigDTO storeRec : storeBrandList){
					
					int count = 1;
					for( LigDTO nationalRec : nationalBrandList){
						
						if( count == 1) {
							storeRec.setDependentItemName(nationalRec.getItemName());
							//if( !nationalRec.isItemLevelRelationship())
								storeRec.setDependentLIG(nationalRec.getLineGroupIdentifier());
							storeRec.setDependentRetailerItemCode(nationalRec.getRetailerItemCodeNoVer());
						} else {
							LigDTO newDependent = pgFormatter.cloneDTO(storeRec);
							newDependent.setDependentItemName(nationalRec.getItemName());
							//if( !dependentDTO.isItemLevelRelationship())
								newDependent.setDependentLIG(nationalRec.getLineGroupIdentifier());
							newDependent.setDependentRetailerItemCode(nationalRec.getRetailerItemCodeNoVer());
							alBrandRelatedList.add(newDependent);
						}
						nationalRec.setTierOverride("");
						count++;
					}
				}
					
			}
		}
		
	}

	/* Checks if the Item Spans multiple relationship. If not reset itemlevel Relationships to false */
	protected void resetItemLevelRelationShip() {
		HashMap<String, Integer> ligCountMap = new HashMap<String, Integer>(); 
		Set<String> keyList = relatedItemGroup.keySet();
		for( String key : keyList){
			List<LigDTO> al = relatedItemGroup.get(key);
			for( LigDTO ligRec : al){
				if(ligRec.getLirId() > 0 ){
					String lig = ligRec.getLineGroupIdentifier();
					
					if( ligCountMap.containsKey(lig)){
						int count = ligCountMap.get(lig).intValue();
						count++;
						ligCountMap.put(lig, Integer.valueOf(count));
					}else{
						ligCountMap.put(lig, Integer.valueOf(1));
					}
					
				}
			}
		}
		
		//If the LIG is appearing only once, then set itemLevelRelationship as false
		keyList = relatedItemGroup.keySet();
		for( String key : keyList){
			List<LigDTO> al = relatedItemGroup.get(key);
			for( LigDTO ligRec : al){
				boolean reset = true;
				if(ligRec.getLirId() > 0 ){
					String lig = ligRec.getLineGroupIdentifier();
					int count = ligCountMap.get(lig).intValue();
					if( count > 1) reset = false;
				}
				
				if( reset) {
					ligRec.setItemLevelRelationship(false);
					ligRec.setItemLevelBrandRelationship(false);
					ligRec.setItemLevelSizeRelationship(false);
				}
				
				
			}
		}
		
	}

	/* Within a relationship, if the same item is repeated, remove it */
	protected void cleanUpData(){
		
		int totalRecordCount = 0;
		Collection<List<LigDTO>> cl = relatedItemGroup.values();
		for(List<LigDTO> al : cl ){
			totalRecordCount += al.size();
		}
		logger.info("Size of Collection Record Count before Cleanup is " + totalRecordCount);
		
		Set<String> keyList = relatedItemGroup.keySet();
		for( String key : keyList){
			List<LigDTO> al = relatedItemGroup.get(key);
			List<LigDTO> newAL = new ArrayList<LigDTO>();
			Set<String> itemCodeSet = new HashSet<String>();
			for( LigDTO ligRec : al){
				String retailerItemCode = ligRec.getRetailerItemCodeNoVer();
				if( !itemCodeSet.contains(retailerItemCode)) {
					itemCodeSet.add(retailerItemCode);
					newAL.add(ligRec);
				}
			}
			al.clear();
			al.addAll(newAL);
			
		}
		
		//Print the count after removing the Item Code duplicates
		totalRecordCount = 0;
		cl = relatedItemGroup.values();
		for(List<LigDTO> al : cl ){
			totalRecordCount += al.size();
		}
		logger.info("Size of Collection Record Count after Duplicate Item Cleanup " + totalRecordCount);
		
		//Attach LIR Name and remove any duplicate LIR ...
		
		
			
	}
	
	/* within a single relationship, items are rolled up to LIR level*/
	protected void prepareDataAtLIRLevel() throws GeneralException{
		
		ItemDAO itemdao = new ItemDAO();
		
		conn = getOracleConnection();
		List<ItemDTO> itemsInCategory = itemdao.getItemDetailsInACategory(conn, categoryLevelId, categoryId);
		HashMap <String, ItemDTO> itemMap  = new HashMap <String, ItemDTO> ();
		
		//put it in a HashMap
		for( ItemDTO  item:itemsInCategory){
			itemMap.put(item.retailerItemCode, item);
		}
		
		//zet LIT 
		
		Collection<List<LigDTO>> cl = relatedItemGroup.values();
		for(List<LigDTO> al : cl ){
			for(LigDTO ligDTO : al ){
				ItemDTO item = itemMap.get(ligDTO.getRetailerItemCodeNoVer());
				if( item != null){
					String likeItem = item.getLikeItemGrp();
					if( likeItem != null){
						ligDTO.setLineGroupIdentifier(likeItem);
						ligDTO.setLirId(item.getLikeItemId());
					}
					else {
						ligDTO.setLineGroupIdentifier("");
					}
					ligDTO.setItemCode(item.getItemCode());
				}else {
					ligDTO.setLineGroupIdentifier("");
					ligDTO.setRetailerItemCodeNoVer(null);
				}
				
				
			}
		}
		
		Set<String> deleteGroup = new HashSet<String>();
		Set<String> keyList = relatedItemGroup.keySet();
		for( String key : keyList){
			List<LigDTO> al = relatedItemGroup.get(key);
			List<LigDTO> newAL = new ArrayList<LigDTO>();
			Set<String> lirSet = new HashSet<String>();
			for( LigDTO ligRec : al){
				
				//Skip inactive items
				if( ligRec.getRetailerItemCodeNoVer() == null)
					continue;
				
				String lineGroupIdentifier = ligRec.getLineGroupIdentifier();
				if( lineGroupIdentifier!= null && !lineGroupIdentifier.isEmpty() ) 
				{
					if( !lirSet.contains(lineGroupIdentifier)){
						lirSet.add(lineGroupIdentifier);
						newAL.add(ligRec);
					}
				}
				else
					newAL.add(ligRec);
			}
			if( newAL.isEmpty()){
				deleteGroup.add(key);
			} else{
				al.clear();
				al.addAll(newAL);
			}
		}
		
		for( String key: deleteGroup){
			relatedItemGroup.remove(key);
		}
		//Print the count after removing LIR duplicates
		int totalRecordCount = 0;
		cl = relatedItemGroup.values();
		for(List<LigDTO> al : cl ){
			totalRecordCount += al.size();
		}
		logger.info("Size of Collection Record Count after Duplicate LIR Cleanup: " + totalRecordCount);
		
	}
	
	/* The ranking is based on the rank defined for the price check list */

	
	private void setItemRank(){
		com.pristine.dao.offermgmt.PricingEngineDAO pricingEngine = new com.pristine.dao.offermgmt.PricingEngineDAO ();
		HashMap<ItemKey, List<PriceCheckListDTO>> itemList =  pricingEngine.getPriceCheckListInfo(conn, -1, -1, Constants.CATEGORYLEVELID, categoryId,null);
		
		
		ItemKey itemkey = new ItemKey(0,0);
		Set<String> keyList = relatedItemGroup.keySet();
		for( String key : keyList){
			List<LigDTO> al = relatedItemGroup.get(key);
			for( LigDTO ligRec : al){
				ligRec.setKviCode("");
				int itemCodeOrLirID = ligRec.getLirId();
				int isLir = PRConstants.LIG_ITEM_INDICATOR;
				if( itemCodeOrLirID <= 0){
					isLir = PRConstants.NON_LIG_ITEM_INDICATOR;
					itemCodeOrLirID = ligRec.getItemCode();
				}
				
				itemkey.setItemCodeOrRetLirId(itemCodeOrLirID);
				itemkey.setLirIndicator(isLir);
				
				int itemRank = 99;
				if( itemList.containsKey(itemkey)){
					
					List<PriceCheckListDTO> itemListIDs = itemList.get(itemkey);
					
					for( PriceCheckListDTO checklistDTO  : itemListIDs){
						if( checklistDTO.getPrecedence() != null) {
							int precedence = checklistDTO.getPrecedence();
							if( precedence > 0 && precedence <itemRank){
								ligRec.setKviCode(Integer.toString(precedence));
								//logger.debug( "Setting Item Rank Successful !!!");
								//a new code -- 7/19
								itemRank = precedence;
							}
						}
					}
					
				}
			
			}
			
		}
	}
	

	//This method will clean up duplicate LIGs. Keep only one Size etc.
	private void prepareData(String fileName) {
		categoryRecords.clear();
		StringTokenizer st = new StringTokenizer(fileName, "/");
		while (st.hasMoreElements()) {
			categoryName = (String) st.nextElement();
		}
		logger.debug("Initial Category Name = " +  categoryName);
		categoryName = categoryName.substring(0, categoryName.indexOf('.') );
		logger.debug("Final Category Name = " +  categoryName);
		
		//HashMap <String, String> ligMap = new HashMap<String, String>();
		HashMap <String, List<LigDTO>> ligMap = new HashMap<String, List<LigDTO>>();

		for (String key : ligRecords.keySet()) {
			LigDTO  valueDTO = ligRecords.get(key);
			String ligName =  valueDTO.getLineGroupIdentifier();
			if(ligName == null || ligName.isEmpty()){
				categoryRecords.put(key, valueDTO);
			}else{
				
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
	    				keep  = pgFormatter.compareSizeBrandRelationships( ligItemRec, valueDTO);
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
	    	}
		    
		}
		logger.debug("# of Records to be processed = " +  categoryRecords.size());
		
		for (String ligName : ligMap.keySet()){
			List<LigDTO> ligItemRecList = ligMap.get(ligName);
			if( ligItemRecList.size() == 1){
				LigDTO valueDTO = ligItemRecList.get(0);
				valueDTO.setItemLevelRelationship(false);
			} else {
				//fillSizeAndBrandRelationships(ligItemRecList);
			}
		}
	}

	protected boolean readRecords(String fileName)  throws GeneralException {
		try {
			FileInputStream fileInputStream = new FileInputStream(fileName);
			XSSFWorkbook workbook = new XSSFWorkbook(fileInputStream);
			boolean coverSheetVal = false;
			for (int i=0; i<workbook.getNumberOfSheets(); i++) {
			    String sheetName = workbook.getSheetName(i).toUpperCase();
			    
			    if (sheetName.contains("COVER")){
			    	coverSheetVal = readCoverSheet(workbook, i);
			    }
			    else if (sheetName.contains("SIZE")){
			    	readSizeSheet(workbook, i);
			    }
			    else if (sheetName.contains("BRAND")){
			    	readBrandSheet(workbook, i);
			    }
			    
			}
			
			if( ! coverSheetVal ){
				logger.error("Missing Cover Sheet or Incomplete Cover Sheet");
				return coverSheetVal;
			}
		} catch( IOException ioe){
			throw new GeneralException("File IO Exception in readRecords method", ioe);
		}
		return true;
	}
	
	protected boolean readSizeSheet(XSSFWorkbook workbook, int index) {
		boolean retVal = true;
		XSSFSheet sheet = workbook.getSheetAt(index);
		
		int rowCount = sheet.getLastRowNum();
		if( rowCount >= 1){
			Row row = sheet.getRow(0);
	        int n = row.getLastCellNum();
	        
	        if( n + 1 < 9) {
	        	retVal = false;
	        }
			for ( int i = 1; i < rowCount+1 && retVal ; i++){
				
				recordCount++;
				row = sheet.getRow(i);
				Cell cell;
				LigDTO ligDTO = new LigDTO ();

				cell = row.getCell(3);
	            if( cell != null){
	            	cell.setCellType(Cell.CELL_TYPE_STRING);
	            	ligDTO.setRetailerItemCode(cell.getStringCellValue().trim());  //LIR Name is set to family code
	            }
            	ligDTO.setRetailerItemCodeNoVer(ligDTO.getRetailerItemCode());  //Item Code
            	
	            ligDTO.setItemName(row.getCell(4).getStringCellValue().trim());  //Item Name
	            double itemSize = row.getCell(5).getNumericCellValue();
	            ligDTO.setItemSize(Double.toString(itemSize));  //Item Size
	            ligDTO.setUomName(row.getCell(6).getStringCellValue().trim());  //UOM
	            if( row.getCell(0) != null)
	            	ligDTO.setBrandName(row.getCell(0).getStringCellValue().trim());  //Brand
	            ligDTO.setUpc("");  //UPC
	            cell = row.getCell(1);
	            
	            if( cell != null){
	            	cell.setCellType(Cell.CELL_TYPE_STRING);
	            	ligDTO.setLineGroupIdentifier(cell.getStringCellValue().trim());  //LIR Name is set to family code
	            } else
	            	ligDTO.setLineGroupIdentifier("");
	            
	            ligDTO.setSizeFamily(row.getCell(7).getStringCellValue().trim());  //Size Family
	            cell = row.getCell(8);
	            if( cell != null ){
	            	cell.setCellType(Cell.CELL_TYPE_STRING);
	            	String sizeClass = getSizeClass(row.getCell(8).getStringCellValue().trim());
	            	ligDTO.setSizeClass(sizeClass);  //Size Class - Small/Medium...
	            }
	            else
	            	ligDTO.setSizeClass("");
	            ligDTO.setBrandFamily("");  //Brand Family
	            ligDTO.setBrandClass("");  //Brand Class
	            ligDTO.setKviCode("");
	            if( !ligDTO.getSizeFamily().isEmpty() ){
	            	String key = "SIZE--" + ligDTO.getSizeFamily();
	            	if( relatedItemGroup.containsKey(key)) {
	            		List<LigDTO> al = relatedItemGroup.get(key);
	            		al.add(ligDTO);
	            	} else {
	            		List<LigDTO> al = new ArrayList<LigDTO>();
	            		al.add(ligDTO);
	            		relatedItemGroup.put(key, al);
	            	}
	            }
				
			}
				
		
		}
		return retVal;		
	}

	private String getSizeClass(String sizeClassValue) {
		String retSizeClass = sizeClassValue;
		
		if( sizeClassValue.equals("1"))
			retSizeClass = "EXTRA SMALL";
		else if( sizeClassValue.equals("2"))
			retSizeClass = "SMALL";
		else if( sizeClassValue.equals("3"))
			retSizeClass = "MEDIUM";
		else if( sizeClassValue.equals("4"))
			retSizeClass = "LARGE";
		else if( sizeClassValue.equals("5"))
			retSizeClass = "EXTRA LARGE";
			
		return retSizeClass;
	}

	protected boolean readBrandSheet(XSSFWorkbook workbook, int index) {
		// TODO Auto-generated method stub
		boolean retVal = true;
		XSSFSheet sheet = workbook.getSheetAt(index);
		
		int rowCount = sheet.getLastRowNum();
		if( rowCount >= 1){
			
			Row row = sheet.getRow(0);
	        int n = row.getLastCellNum();
	        
	        if( n + 1 < 20) {
	        	retVal = false;
	        }
			for ( int i = 1; i < rowCount+1 ; i++){
				recordCount++;
				row = sheet.getRow(i);
				Cell cell;
				LigDTO ligDTO = new LigDTO ();

				//Populate the GE brand first
				cell = row.getCell(3);
	            if( cell != null){
	            	cell.setCellType(Cell.CELL_TYPE_STRING);
	            	ligDTO.setRetailerItemCode(cell.getStringCellValue().trim());  //LIR Name is set to family code
	            }
				
	            ligDTO.setRetailerItemCodeNoVer(ligDTO.getRetailerItemCode());  //Item Code
	            ligDTO.setItemName(row.getCell(4).getStringCellValue().trim());  //Item Name
	            double itemSize = row.getCell(5).getNumericCellValue();
	            ligDTO.setItemSize(Double.toString(itemSize));  //Item Size
	            ligDTO.setUomName(row.getCell(6).getStringCellValue().trim());  //UOM
	            if( row.getCell(0)!= null)
	            	ligDTO.setBrandName(row.getCell(0).getStringCellValue().trim());  //Brand
	            ligDTO.setUpc("");  //UPC
	            cell = row.getCell(1);
	            if( cell != null){
	            	cell.setCellType(Cell.CELL_TYPE_STRING);
	            	ligDTO.setLineGroupIdentifier(cell.getStringCellValue().trim());  //LIR Name is set to family code
	            }else
	            	ligDTO.setLineGroupIdentifier("");
	            
	            
	            ligDTO.setSizeFamily("");  //Size Family
	            ligDTO.setSizeClass("");
	            ligDTO.setBrandFamily(row.getCell(16).getStringCellValue().trim());
	            ligDTO.setBrandClass(row.getCell(7).getStringCellValue().trim());
	            
	            cell = row.getCell(8);
 	            if( cell != null){
 	            	cell.setCellType(Cell.CELL_TYPE_STRING);
 	            	String strTier = cell.getStringCellValue();
 	            	ligDTO.setTier(Integer.parseInt(strTier));
 	            }
	            
	            cell = row.getCell(19);
            	cell.setCellType(Cell.CELL_TYPE_STRING);
	            ligDTO.setTierOverride(cell.getStringCellValue().trim());
	            
	            //delete this code
	            //if( !ligDTO.getBrandFamily().isEmpty() ){
	            //	ligRecords.put(ligDTO.getRetailerItemCodeNoVer(), ligDTO);
	            //}
	            
	            //Add Dependent Item as another Row

	            
	            
	            
				LigDTO ligDTONB = new LigDTO ();

				//Populate the GE brand first
				cell = row.getCell(12);
	            if( cell != null){
	            	cell.setCellType(Cell.CELL_TYPE_STRING);
	            	ligDTONB.setRetailerItemCode(cell.getStringCellValue().trim());  //LIR Name is set to family code
	            }
				
	            ligDTONB.setRetailerItemCodeNoVer(ligDTONB.getRetailerItemCode());  //Item Code
	            ligDTONB.setItemName(row.getCell(13).getStringCellValue().trim());  //Item Name
	            itemSize = row.getCell(14).getNumericCellValue();
	            ligDTONB.setItemSize(Double.toString(itemSize));  //Item Size
	            ligDTONB.setUomName(row.getCell(15).getStringCellValue().trim());  //UOM
	            if( row.getCell(9) != null)
	            	ligDTONB.setBrandName(row.getCell(9).getStringCellValue().trim());  //Brand
	            ligDTONB.setUpc("");  //UPC
	            
	            cell = row.getCell(10);
	            if( cell != null){
	            	cell.setCellType(Cell.CELL_TYPE_STRING);
	            	ligDTONB.setLineGroupIdentifier(cell.getStringCellValue().trim());  //LIR Name is set to family code
	            } else
	            	ligDTONB.setLineGroupIdentifier("");
	            
	            
	            ligDTONB.setSizeFamily("");  //Size Family
	            ligDTONB.setSizeClass("");
	            ligDTONB.setBrandFamily(row.getCell(16).getStringCellValue().trim());
	            ligDTONB.setBrandClass(row.getCell(17).getStringCellValue().trim());
	            
	            cell = row.getCell(19);
            	cell.setCellType(Cell.CELL_TYPE_STRING);
            	ligDTONB.setTierOverride(cell.getStringCellValue().trim());
            	
            	cell = row.getCell(18);
 	            if( cell != null){
 	            	cell.setCellType(Cell.CELL_TYPE_STRING);
 	            	String strTier = cell.getStringCellValue();
 	            	ligDTONB.setTier(Integer.parseInt(strTier));
 	            }
 	            
            	
	            if( !ligDTO.getBrandFamily().isEmpty() ){
	            	String key = "BRAND-" + ligDTO.getBrandFamily();
	            	if( relatedItemGroup.containsKey(key)) {
	            		List<LigDTO> al = relatedItemGroup.get(key);
	            		al.add(ligDTO);
	            		al.add(ligDTONB);
	            	} else {
	            		List<LigDTO> al = new ArrayList<LigDTO>();
	            		al.add(ligDTO);
	            		al.add(ligDTONB);
	            		relatedItemGroup.put(key, al);
	            		
	            	}
	            }
	            
			}
			
		
		}
		return retVal;
	}

	protected boolean readCoverSheet(XSSFWorkbook workbook, int index) {
		
		boolean retVal = false;
		XSSFSheet sheet = workbook.getSheetAt(index);
		
		int rowCount = sheet.getLastRowNum();
		if( rowCount > 0){
			
			for ( int i = 0; i < rowCount+1 && i < 4; i++){
				
				//get the Columns;
				Row row = sheet.getRow(i);
		        int n = row.getLastCellNum();
		        String title = row.getCell(0).getStringCellValue().toUpperCase().trim();
		        
				
		        if( title.equals("MAJOR CATEGORY"))
		        	majorCategoryName = row.getCell(1).getStringCellValue();
		        else if  (title.equals("CATEGORY"))
		        	categoryName = row.getCell(1).getStringCellValue();
		        else if  (title.equals("CATEGORY ID"))
		        	categoryId = (int)row.getCell(1).getNumericCellValue();
			        
			    else if  (title.equals("LEAD TIER"))
		        	leadTier = (int)row.getCell(1).getNumericCellValue();
			}
		}
		
		if ( majorCategoryName != null && categoryName != null && categoryId > 0 )
			retVal = true;
		
		return retVal;
	}

	
	
	@Override
	public void processRecords(List listobj) throws GeneralException {
		
		
	}

}
