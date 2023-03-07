package com.pristine.dataload;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CheckListDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.ItemGroupDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.ItemGroupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ShopRiteDataProcessor extends PristineFileParser {
	private static Logger  logger = Logger.getLogger("ShopRiteDataLoad");
	private HashMap<String, Integer> uomMap = new HashMap<String, Integer> ();
	private ItemDAO itemdao = new ItemDAO ();
	private static int invalidSizeUOMCount = 0;
	private Connection dbconn = null;
	private int reccount = 0, newcount = 0, duplicateCount=0, errorCount =0, updateCount = 0;
	private int zeroedTableItem = 0;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j.properties");
		logger.info("Data Load started");
		
		PropertyManager.initialize("analysis.properties");
		
		ShopRiteDataProcessor dataLoader = new ShopRiteDataProcessor ();
		dataLoader.loadData(args);
		logger.info("Data Load successfully completed");

	}

	private void loadData(String[] args){
		boolean commit = true; 
		try{
			dbconn = super.getOracleConnection();
			if (args.length < 1){
				logger.debug("Insufficient Arguments -Enter (item_import,item_list_setup, dept_cat_setup), [filename]");
				System.exit(1);
			}
			logger.info( "arg[0] = " +  args[0]);
			String fileName = null;
			if (args.length > 1) {
				logger.info( "Loading file - arg[1] = " +  args[1]);
				fileName = getRootPath();
				if( !fileName.equals(""))
					fileName = fileName +"/";
				fileName = fileName + args[1];
			}
			if( args[0].equalsIgnoreCase("item_import")){
				this.itemLoad(dbconn, fileName);
			}else if( args[0].equalsIgnoreCase("special_list_load")){
				this.specialListLoad(dbconn, fileName);
			}
			else if( args[0].equalsIgnoreCase("item_list_setup")){
				this.setupTopItemList(dbconn);
			}
			else if( args[0].equalsIgnoreCase("dept_cat_setup")){
				this.setupItemHierarchy(dbconn, fileName);
			}
			else{
				logger.info("Incorrect options - Enter (item_import,item_list_setup, dept_cat_setup), [filename]");
				System.exit(1);
			}
		}catch(GeneralException ge){
			logger.error("Error in Load", ge);
			commit = false;
			PristineDBUtil.rollbackTransaction(dbconn, "Data Load");
		}
		super.performCloseOperation(dbconn, commit);
	}
	


	private void setupItemHierarchy(Connection conn, String fileName) throws GeneralException {
		// TODO Auto-generated method stub
		
		LinkedHashMap deptCatField = new LinkedHashMap();
		deptCatField.put("deptCode", "0-5");
		deptCatField.put("catCode", "0-10");
		deptCatField.put("deptName", "10-12");
		deptCatField.put("catName", "13-40");

		 List<ItemGroupDTO> itemList = parseTextFile(ItemGroupDTO.class, fileName, deptCatField, -1);
		    //processRecords(cost);

		    logger.info("No of records = " + reccount);
		    logger.info("Successfully Processed Count " + updateCount);
		    logger.info("Error Records " + errorCount);


	}

	private void setupTopItemList(Connection conn) throws GeneralException {
		// TODO Auto-generated method stub
		
		/*
		 * 93	NON-FOODS-TOP-25       243

92	ICECREAM-TOP-50            296

103	ICECREAM-TOP-100           296

120	GROCERY-TOP-100            237


90	DAIRY-TOP-500             250     

91	HBA-TOP-500               244


89	FROZEN-TOP-500            240


95	GROCERY-TOP-500           237


88	GROCERY-TOP-1000          237

A TOP 10
B TOP 25
C TOP 50
D TOP 100
E TOP 200
H TOP 500
L TOP 1000

		 */
		//pass checklistId, itemRank and deptId 
		
		//delete the checks
		logger.info( " Setting up the Top item lists - Started");
		
		CheckListDAO checkListDAO = new CheckListDAO();
		checkListDAO.deleteCheckListId(conn, 93);	// Non Foods
		checkListDAO.setupTopCheckList( conn, 93, 243, "'A','B'");
		
		checkListDAO.deleteCheckListId(conn, 92);	// IceCream Top 50
		checkListDAO.setupTopCheckList( conn, 92, 296, "'A','B', 'C'");
		
		checkListDAO.deleteCheckListId(conn, 103);	// IceCream Top 100
		checkListDAO.setupTopCheckList( conn, 103, 296, "'A','B', 'C', 'D' ");
		
		checkListDAO.deleteCheckListId(conn, 90);	// Dairy Top 500
		checkListDAO.setupTopCheckList( conn, 90, 250, "'A','B', 'C', 'D','E', 'H'");
		
		checkListDAO.deleteCheckListId(conn, 91);	// HBA Top 500 
		checkListDAO.setupTopCheckList( conn, 91, 244, "'A','B', 'C', 'D','E', 'H'");
		
		checkListDAO.deleteCheckListId(conn, 89);	// Frozen Top 500
		checkListDAO.setupTopCheckList( conn, 89, 240, "'A','B', 'C', 'D','E', 'H'");
		
		checkListDAO.deleteCheckListId(conn, 95);	// Grocery Top 500
		checkListDAO.setupTopCheckList( conn, 95, 237, "'A','B', 'C', 'D','E', 'H'");
		
		checkListDAO.deleteCheckListId(conn, 88);	// Grocery Top 1000
		checkListDAO.setupTopCheckList( conn, 88, 237, "'A','B', 'C', 'D','E', 'H', 'L'");
		
		
		logger.info( " Setting up the Top item lists - Completed");
		
	}

	private void itemLoad(Connection conn, String fileName) throws GeneralException{
		ItemDTO item = new ItemDTO();
		ItemGroupDTO itemGroup = new ItemGroupDTO();
		ItemGroupDAO itemGroupDAO = new ItemGroupDAO ();
		logger.info("Loading Item Info ...");
		try{
			itemdao.clearItemRank(conn);
		    // command line parameter
			itemdao.updateActiveIndicatorFlag(conn,-1);
			
			FileInputStream fstream = new FileInputStream(fileName);
		    // Get the object of DataInputStream
		    DataInputStream in = new DataInputStream(fstream);
	        BufferedReader br = new BufferedReader(new InputStreamReader(in));
		    
	        String strLine;
		    
		    
		    //Read File Line By Line
			while ((strLine = br.readLine()) != null)   {
			      // Print the content on the console
				reccount++;
				//logger.debug(strLine);
				
				item.clear();
				itemGroup.clear();
				
				item.upc = strLine.substring(0, 15);
				item.privateLabelCode = strLine.substring(15,16);
				if (item.privateLabelCode.equals("P"))
					item.privateLabelFlag = "Y";
				else 
					item.privateLabelFlag = "N";
				String sizeUOM = strLine.substring(16, 23);
				
				//logger.debug( "size Uom = "+ sizeUOM);
				itemGroup.deptCode = strLine.substring(23, 28);
				//logger.debug( "Dept code = "+ itemGroup.deptCode);
				itemGroup.catCode = strLine.substring(23, 33);
				
				item.deptID = itemGroupDAO.getDepartmentId(conn, itemGroup.deptCode);
				if ( item.deptID < 0 ){  
					logger.info( "Dept code = "+ itemGroup.deptCode + " is not setup");
					errorCount++;
					continue;
				}
				item.catID = itemGroupDAO.getCategoryId(conn, item.deptID, itemGroup.catCode);
				if ( item.catID < 0 ){  
					logger.info( "cat code = "+ itemGroup.catCode + " is not setup. Record #:"+ reccount);
					errorCount++;
					continue;
				}
				item.retailerItemCode = strLine.substring(49, 58);
				item.likeItemGrp = strLine.substring(58, 68).trim();
				item.itemRank = strLine.substring(73, 74).trim();
				item.itemName = (strLine.substring(120, strLine.length())).replaceAll("'", "''").trim();
			
				splitSizeUOM(conn,item,sizeUOM);
				// if the 15th position of upc is X, then replace it with a checkdigit
				if( item.upc.charAt(14)=='X' && item.upc.charAt(0) == '0' 
					&& item.upc.charAt(1) == '0' && item.upc.charAt(2) == '0'){
					item.standardUPC = item.upc.substring(3,14);
					int checkDigit = findUPCCheckDigit(item.standardUPC);
					item.standardUPC = item.standardUPC + Integer.toString(checkDigit);
				}else if( item.upc.charAt(0)=='0' && item.upc.charAt(1) == '0' 
					&& item.upc.charAt(2) == '0' && item.upc.charAt(3) == '0'){
					item.standardUPC = item.upc.substring(4,15);
					int checkDigit = findUPCCheckDigit(item.standardUPC);
					item.standardUPC = item.standardUPC + Integer.toString(checkDigit);
				}
				
				else{
					item.standardUPC = item.upc.substring(0,15);
				}

				if( !item.likeItemGrp.equals("")){
					if( isValidLikeItem(item.likeItemGrp)){
						item.likeItemId = itemdao.setupRetailerLikeItem(conn, item.likeItemGrp, item.itemName);
					}else {
						zeroedTableItem++;
					}
				}
				
				//logger.debug("Item Object = " + item.toString());
				try{
					
					if( itemdao.insertItem(conn, item, false, false)) 
						newcount++;
					else {
						
						/*
						//get dept_name and category name
						ItemDTO existingItem = itemdao.getItemDetails(conn, item);
						if( (existingItem.deptID != item.deptID) || (existingItem.catID != item.catID) )
							item.updateTimeStamp = true;
						else
							item.updateTimeStamp = false;
						*/
						itemdao.updateItem(conn, item, false, false);
						updateCount++;
					}
						
					
				}catch(GeneralException ge){
						logger.info("Error with item - " + item.itemName + " - " + item.retailerItemCode + " " + item.upc);
						errorCount++;
				}
					//duplicateCount++;
	
				if( reccount % 5000 == 0) 
					logger.info("Processed records - " + reccount);
			    
				//if( reccount > 35) break;
			}
		    //Close the input stream
		    in.close();
			logger.info("Total record count "+ reccount);
			logger.info("new record count "+ newcount);
			logger.info("Existing record count "+ duplicateCount);
			logger.info("Updated record count "+ updateCount);
			logger.info("Error record count "+ errorCount);
			logger.info("zeroedTableItem " + zeroedTableItem); 
			logger.info("Invalid UOM count "+ invalidSizeUOMCount);
			Iterator<String> uomitr = uomMap.keySet().iterator();
			while (uomitr.hasNext()){
				String key = uomitr.next();
				logger.info(" UOM - " + key + ", Count = " + uomMap.get(key));
			}
			
		}catch(IOException ioe){
			throw new GeneralException( "Exception in Item Data Load", ioe);
		}catch(GeneralException ge){
			logger.info("Total record count "+ reccount);
			throw ge;
		}
	}
	
	private boolean isValidLikeItem(String likeItemGrp) {
		boolean isLikeItem = false;
		if(likeItemGrp.length() > 5 ){
			String tableItem = likeItemGrp.substring(5); 
			try {
				int tableItemNo = Integer.parseInt(tableItem);
				if( tableItemNo  > 0 )
					isLikeItem = true;
			}catch(Exception e){
			}
		}
		return isLikeItem;
	}

	private void splitSizeUOM(Connection conn, ItemDTO item, String sizeUOM) throws GeneralException {
		int i = 0;
		
		if( sizeUOM.contains("/")){
			invalidSizeUOMCount++;
			item.itemName = item.itemName  + " - " +  sizeUOM.replaceAll("'", "''");;
			return;
			
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
		
		item.uom = sizeUOM.substring(i).trim();
		
		// If the size is not numeric, don't populate uom
		if( item.size != null && !item.size.equals("")){
			try {
				float f = Float.parseFloat(item.size);
			} catch( Exception e){
				item.size = "";
				item.uom = "";
				item.itemName = item.itemName  + " - " +  sizeUOM.replaceAll("'", "''");;
				invalidSizeUOMCount++;
				return;
			}
		}
		
		if( item.uom.equals("EACH"))
			item.uom="EA";
		else if( item.uom.equals("LITER"))
			item.uom="LT";
		else if( item.uom.equals("DOZEN"))
			item.uom="DOZ";
		else if ( item.uom.equals("DOZ"))
			item.uom=item.uom;
		else if (item.uom.equals("FL OZ"))
			item.uom="FOZ";
		else if (item.uom.equals("FL.OZ"))
			item.uom="FOZ";
		else if (item.uom.equals("FL"))
			item.uom="FOZ";
		else if (item.uom.equals("O"))
			item.uom="OZ";
		else if (item.uom.equals("Z"))
			item.uom="OZ";
		else if (item.uom.equals("E"))
			item.uom="EA";
		else if (item.uom.equals("PACK"))
			item.uom="PK";
		else if (item.uom.equals("PAIR"))
			item.uom="PIR";
		else if (item.uom.equals("SQ"))
			item.uom="SF";
		else if (item.uom.length() > 2)
			item.uom= item.uom.substring(0,2);
		
		if (item.uom.equals("CO"))
			item.uom="CT";
			
		String uomId = itemdao.populateUOM(conn, item.uom);
		
		if ( uomId == null){
			item.size = "";
			item.uom = "";
			item.itemName = item.itemName  + " - " +  sizeUOM.replaceAll("'", "''");;
			invalidSizeUOMCount++;
			return;
		}else
			item.uomId = uomId;
		
		item.itemName  = item.itemName + " " + item.size + " " + item.uom;
		
		/*
			1) Get the UPC logic for standard upc
			3) Populate logic for Companion Codes 
			*/
		
		
		/*if( uomMap.containsKey(item.uom)){
			int count = uomMap.get(item.uom);
			count++;
			uomMap.put(item.uom, count);
		}else
			uomMap.put(item.uom, 1);
		*/
		//logger.debug(    "size = " + item.size + ", UOM = " + sizeUOM.substring(i));

	}
	
	public int findUPCCheckDigit(String upc){
		
		//Note - for odd and even digits, the indexes are flipped
		int sumofOddDigits =  translatetoInt(upc,0) + translatetoInt(upc, 2) +
			translatetoInt(upc, 4)+ translatetoInt(upc, 6)+ translatetoInt(upc, 8)+ translatetoInt(upc, 10);
		
		int sumofEvenDigits =  translatetoInt(upc,1) + translatetoInt(upc, 3) +  
			translatetoInt(upc, 5)+ translatetoInt(upc, 7)+ translatetoInt(upc, 9);
		int totalSum = sumofOddDigits*3 + sumofEvenDigits;
		int remainder = totalSum % 10;
		
		if ( remainder != 0)
			remainder = 10 - remainder;
		//logger.debug("Check Digit for UPC -" + upc + " is " + remainder );
		return remainder;
		
		
	}
	
	private int translatetoInt(String upc, int position){
	 	return Integer.parseInt(upc.substring(position, position+1));
	}
	
	/*
	1) Steps - Setup the check list on the server first
	2) Run this program
	*/
	private void specialListLoad(Connection conn, String fileName) throws GeneralException{
		logger.info("Loading Special List .......");
		int reccount = 0, errorCount =0;
		HashMap <String, Integer> listMap = new HashMap <String, Integer> ();
		CheckListDAO checkListDao = new CheckListDAO ();
		try{
		    // command line parameter
		    FileInputStream fstream = new FileInputStream(fileName);
		    // Get the object of DataInputStream
		    DataInputStream in = new DataInputStream(fstream);
	        BufferedReader br = new BufferedReader(new InputStreamReader(in));
		    String strLine;
		    //Read File Line By Line
			while ((strLine = br.readLine()) != null)   {
			      // Print the content on the console
				if (strLine.length() < 25 )continue;
				reccount++;
				//logger.debug(strLine);
				String listName =  strLine.substring(0, 10).trim();
				String upc =  strLine.substring(10, 25);
				//
				int listId = -1;
				if( listMap.containsKey(listName)) {
					listId = listMap.get(listName);
				}else{
					listId = checkListDao.getCheckListId(conn, listName);
					//logger.debug("List id for "+ listName + " is " + listId );
					//delete the list id if present
					if( listId > 0)
						checkListDao.deleteCheckListId(conn, listId);
					else {
						logger.debug("Missing List - "+ listName );
						checkListDao.createCheckList(conn, listName);
						listId = checkListDao.getCheckListId(conn, listName);
					}
					listMap.put(listName, listId);
					
				}
				if( listId > 0){
					//Insert the item. if fails, increment the error count
					if( !checkListDao.insertCheckListItem( conn, listId, -1, upc)){
						errorCount++;
						logger.info(" Error Record, List Name " +  listName + ", UPC = " + upc);
					}
						
				}
				if( reccount % 500 == 0) 
					logger.info("Processed records - " + reccount);
			    
				//if( reccount > 10) break;
			}
		    //Close the input stream
		    in.close();
			logger.info("Total record count "+ reccount);
			logger.info("Error record count "+ errorCount);
			
		}catch(IOException ioe){
			throw new GeneralException( "Exception in Item Data Load", ioe);
		}
	}

	@Override
	public void processRecords(List listObject) throws GeneralException {
		List<ItemGroupDTO> itemGrpList = (List<ItemGroupDTO>) listObject;
		ItemGroupDAO itemGrpdao = new ItemGroupDAO (); 
		for (int j = 0; j < itemGrpList.size(); j++) {
			reccount++;
		    ItemGroupDTO itemGrp = itemGrpList.get(j);
		    itemGrp.deptName = itemGrp.deptName.replaceAll("'", "");
		    itemGrp.deptName = itemGrp.deptName.replaceAll(",", "-");
		    itemGrp.catName = itemGrp.catName.replaceAll("'", "");
		    itemGrp.catName = itemGrp.catName.replaceAll(",", "-"); //Added this so that upload files will not get into error
		  //  itemGrp.useDeptCode= true;
		  //  itemGrp.useCatCode = true;
		    //logger.debug("Dept " + itemGrp.deptCode + " - " + itemGrp.deptName + " Category " + itemGrp.catCode + " - " + itemGrp.catName);
		    try{
			    itemGrp.deptId = itemGrpdao.populateDept(dbconn, itemGrp);
			    itemGrpdao.updateDepartment( dbconn, itemGrp.deptId, itemGrp.deptName, itemGrp.deptCode );
			    itemGrp.catId = itemGrpdao.populateCategory(dbconn, itemGrp);
			    itemGrpdao.updateCategory( dbconn, itemGrp.deptId, itemGrp.catId, itemGrp.catName, itemGrp.catCode );
			    updateCount++;
		    }catch(GeneralException ge){
			    logger.error( "Rec No "+ reccount + ", Dept " + 
			    	itemGrp.deptCode + " - " + itemGrp.deptName + " Category " + itemGrp.catCode + " - " + itemGrp.catName);

		    	logger.error("Error with Item Hierarchy Setup ", ge);
		    }
		}
	}
}
