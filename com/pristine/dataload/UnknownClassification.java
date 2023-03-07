package com.pristine.dataload;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Font;

import com.pristine.dao.ProductGroupDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;

public class UnknownClassification {
	
	private static Logger  logger = Logger.getLogger("UnknownClassification");
	private ProductGroupDAO pgdao = new ProductGroupDAO();
	private HSSFWorkbook workBook = null;
	private HSSFSheet sheet;
	private int excelRowCount = 0;
	
	int similarDeptId = -1;
	int similarCatId = -1;
	int similarSubCatId = -1;
	int similarSegmentId = -1;
	int noOfDepts = 0;
	int noOfCats = 0;
	int noOfSubCats = 0;
	int noOfSegments = 0;

	private int noOfItemsUpdated = 0;

	private final int SEARCH_STRING_LENGTH = 10;
	
	/* This is method 1 - It categorized Unknown items based on Name, manufacturer code, uom etc matching
	 * with items that match the same criteria.
	 */
	
	public void categorizeUnknownItems(Connection conn, int chainId)throws GeneralException {
		logger.info("Categorizing Unknown Items .....");
		
		//For a given chain, identify the pairs
		//For each item, setup the potential list of pairs
		// sub cat match, uom and size
		
		//For each item, get the item, uom and size. If uom or size is blank, move to the next
		// otherwise find the list that matches the item. 
		//Add them to the probable table
		int couldNotbeAssignedCount = 0;
		try {
			//pgdao.performCleanup(conn, chainId);
			//Get the list of Unknown Items
			String [] header = new String[] { "Unknown Item", " Classify ?", 
					 "UPC", "New Dept","New Category", "New Sub Category", "Similar Sample Item", "Sample Item UPC"};
					
			initializeExcelOutput(header);
			CachedRowSet itemList = pgdao.getItemList(conn, -1, true,0);
			logger.info( "No of Unknown Items is " + itemList.size() );
			int count = 0;
			ItemDTO item = new ItemDTO();
			
			while( itemList.next()){
				item.clear();
				item.deptID = PrestoUtil.getIntValue(itemList, "DEPT_ID");
				item.catID = PrestoUtil.getIntValue(itemList, "CATEGORY_ID"); 
				item.subCatID= PrestoUtil.getIntValue(itemList, "SUB_CATEGORY_ID");
				item.size = itemList.getString("ITEM_SIZE");
				item.uomId= itemList.getString("UOM_ID");
				item.itemCode= itemList.getInt("ITEM_CODE");
				item.upc = itemList.getString("STANDARD_UPC");
				item.manufactCode = null;
				if( item.upc.length() > 9){
					item.manufactCode = item.upc.substring(0,Constants.MANUFACTURER_END_INDEX+3);
				}
				
				item.itemName = itemList.getString("ALT_ITEM_NAME");
				if ( item.itemName == null || item.itemName.equals(""))
					item.itemName = itemList.getString("ITEM_NAME");

				
				count++;
			
				//Get Like items
				ArrayList<ItemDTO> similarItemList = pgdao.getCrossDeptLikeItems( conn, item);
				
				if( similarItemList.size() == 0){
					item.manufactCode = item.upc.substring(0,Constants.MANUFACTURER_END_INDEX+2);
					similarItemList = pgdao.getCrossDeptLikeItems( conn, item);
				}
				
				ItemDTO similarItem = identifyHeirarchy(similarItemList);
				if(noOfDepts > 1 ||noOfCats > 1 || noOfSubCats > 1 ){
					similarItem = narrowByNameBasedLookup(conn, item, similarItem);
					if(noOfDepts > 1 ||noOfCats > 1 || noOfSubCats > 1 ){
						similarItem = narrowByUPCBasedLookup(conn, item, similarItem);
					}
				}
				
				if ( noOfDepts > 1 || similarItem == null){
					couldNotbeAssignedCount++;
					logger.info("Many/No Departments, Could not Assign Item code - "+ item.itemCode + ", "  + item.itemName + ", " + item.upc);
				}
				printClassificationDetails( conn, item, similarItem);
				
				if ( count%50 == 0)
					logger.info( count + " items processed");
					

			}
			FileOutputStream stream = new FileOutputStream("C:/TEMP/UnknownClassification.xls");
			workBook.write(stream);
			stream.close();
			logger.info( count + " items processed");
			logger.info( "No of Items cannot be assigned " + couldNotbeAssignedCount );
			logger.info( "No of Items assigned " + (itemList.size() - couldNotbeAssignedCount));
			logger.info( "No of Items Updated " + noOfItemsUpdated);
		}catch(SQLException sqlce){
			throw new GeneralException ( "Cached Row access exception ", sqlce);
		}
		catch(IOException ioe){
			throw new GeneralException ( "Excel File operation exception ", ioe);
		}
		
	}
	private void initializeExcelOutput(String[] header){
		workBook = new HSSFWorkbook();
		sheet = workBook.createSheet();
		
		//Create a Font bold style
		Font font = workBook.createFont();
	    font.setBoldweight(Font.BOLDWEIGHT_BOLD);
	    // Fonts are set into a style so create a new one to use.
	    HSSFCellStyle boldStyle = workBook.createCellStyle();
	    boldStyle.setFont(font);
	    
	    HSSFCellStyle  wrapStyle = workBook.createCellStyle();
		wrapStyle.setWrapText(true);

		 
		HSSFRow headerRow = sheet.createRow(excelRowCount);
		int colCount = 0;
		for( int i = 0; i < header.length; i++){
			 HSSFCell cell = headerRow.createCell(colCount);
			 cell.setCellValue(header[i]);
			 cell.setCellStyle(wrapStyle);
			 cell.setCellStyle(boldStyle);
			 colCount++;
		}
		excelRowCount++;
	}

	private void printClassificationDetails(Connection conn, ItemDTO item, ItemDTO similarItem )
					throws GeneralException{
		boolean canBeClassified = true;
		if( similarItem == null || noOfDepts > 1)
			canBeClassified = false;
		
		if( canBeClassified ){
			updateHierarchy(conn, item, similarItem);
		}
		/*HSSFRow row  = sheet.createRow(excelRowCount);
		int colCount = 0;
		HSSFCell cell = row.createCell(colCount);
		cell.setCellValue(item.itemName);
		colCount++;
		
		String value = (noOfDepts > 1)? "Many Depts": "No";
		cell = row.createCell(colCount);
		cell.setCellValue(canBeClassified?"Yes":value);
		colCount++;
		
		cell = row.createCell(colCount);
		cell.setCellValue(item.upc);
		colCount++;
		
		if( canBeClassified ){
			cell = row.createCell(colCount);
			cell.setCellValue(similarItem.deptName);
		}
		colCount++;
		
		if( canBeClassified && noOfCats == 1){
			cell = row.createCell(colCount);
			cell.setCellValue(similarItem.catName);
		}
		colCount++;
		
		if( canBeClassified && noOfSubCats == 1){
			cell = row.createCell(colCount);
			cell.setCellValue(similarItem.subCatName);
		}
		colCount++;
		
		if( canBeClassified ){
			cell = row.createCell(colCount);
			cell.setCellValue(similarItem.itemName);
			colCount++;
			
			cell = row.createCell(colCount);
			cell.setCellValue(similarItem.upc);

		}
		colCount++;
		excelRowCount++;*/
	}
	
	/* This approach uses cross database to see if items can be classified */
	public void categorizeUnknownItemsMethod2(Connection conn, int chainId)throws GeneralException {

		logger.info("Categorizing Unknown Items - Method 2.....");
		String [] header = new String[] { "Unknown Item", " Classify ?", 
				 "UPC", "Known Dept","Known Category", "Known Sub Category", "Known Item", 
				 "New Dept", "New Category", "New Sub Category"};
		
		initializeExcelOutput(header);
		try {
			initializeExcelOutput(header);
			ItemDTO currentDBItem = new ItemDTO();
			int count = 0;
			int couldNotbeAssignedCount = 0;
			CachedRowSet itemList = pgdao.getUnknownCarriedInOtherDB(conn);
			logger.info("No of Unknown item that can be potentially classified - " + itemList.size());
			while( itemList.next()){
				currentDBItem.clear();
				currentDBItem.itemName = itemList.getString("ITEM_NAME");
				currentDBItem.itemCode = itemList.getInt("ITEM_CODE");
				currentDBItem.upc = itemList.getString("STANDARD_UPC");
				currentDBItem.uomId = itemList.getString("UOM_ID");
				count++;
				if( count%200 == 0){
					logger.info(count + " no of items processed");
				}
				ArrayList<ItemDTO> peerItemList = pgdao.getPeerItemsFromOtherDB(conn,currentDBItem.upc);
				
				
				ArrayList<ItemDTO> similarItemList = pgdao.getItemHierarchy(conn,currentDBItem.upc, peerItemList);
				
				ItemDTO similarItem = identifyHeirarchy(similarItemList);
				ItemDTO peerItem = null;
				if( peerItemList.size() > 0)
					peerItem = peerItemList.get(0);
				/* This is second check to narrow down the Category or Sub category further */
				if(noOfDepts > 1 ||noOfCats > 1 || noOfSubCats > 1 ){
					similarItem = narrowByNameBasedLookup(conn, currentDBItem, similarItem);
					if(noOfDepts > 1 ||noOfCats > 1 || noOfSubCats > 1 ){
						similarItem = narrowByUPCBasedLookup(conn, currentDBItem, similarItem);
						
					}
					
				}
				
				if ( noOfDepts > 1 || similarItem == null){
					couldNotbeAssignedCount++;
					logger.info("Many Departments, Could not Assign UPC - "+ currentDBItem.upc+ ", "  + currentDBItem.itemName);
				}
				printClassificationDetailsM2(conn,currentDBItem, similarItem, peerItem);
			}
		
			FileOutputStream stream = new FileOutputStream("C:/TEMP/UnknownClassificationM2.xls");
			workBook.write(stream);
			stream.close();
			logger.info( count + " items processed");
			logger.info( "No of Items cannot be assigned " + couldNotbeAssignedCount );
			logger.info( "No of Items assigned " + (count - couldNotbeAssignedCount));
			logger.info( "No of Items Updated " + noOfItemsUpdated);
		}catch(SQLException sqlce){
			throw new GeneralException ( "Cached Row access exception ", sqlce);
		}
		catch(IOException ioe){
			throw new GeneralException ( "Excel File operation exception ", ioe);
		}
	}
	
	private void printClassificationDetailsM2( Connection conn, ItemDTO item, ItemDTO similarItem, ItemDTO peerItem )
					throws GeneralException{
		boolean canBeClassified = true;
		if( similarItem == null || noOfDepts > 1)
			canBeClassified = false;
		
		if( canBeClassified ){
			updateHierarchy(conn, item, similarItem);
		}
		HSSFRow row  = sheet.createRow(excelRowCount);
		int colCount = 0;
		HSSFCell cell = row.createCell(colCount);
		cell.setCellValue(item.itemName);
		colCount++;
		
		String value = (noOfDepts > 1)? "Many Depts": "No";
		cell = row.createCell(colCount);
		cell.setCellValue(canBeClassified?"Yes":value);
		colCount++;
		
		cell = row.createCell(colCount);
		cell.setCellValue(item.upc);
		colCount++;
		
		if( canBeClassified ){
			cell = row.createCell(colCount);
			cell.setCellValue(peerItem.deptName);
		}
		colCount++;
		
		if( canBeClassified){
			cell = row.createCell(colCount);
			cell.setCellValue(peerItem.catName);
		}
		colCount++;
		
		if( canBeClassified){
			cell = row.createCell(colCount);
			cell.setCellValue(peerItem.subCatName);
		}
		colCount++;
		
		if( canBeClassified ){
			cell = row.createCell(colCount);
			cell.setCellValue(peerItem.itemName);
		}
		colCount++;
		if( canBeClassified ){
			cell = row.createCell(colCount);
			cell.setCellValue(similarItem.deptName);
		}
		colCount++;
		
		if( canBeClassified && noOfCats == 1){
			cell = row.createCell(colCount);
			cell.setCellValue(similarItem.catName);
		}
		colCount++;
		
		if( canBeClassified && noOfSubCats == 1){
			cell = row.createCell(colCount);
			cell.setCellValue(similarItem.subCatName);
		}
		colCount++;
		excelRowCount++;
	}
		

	private ItemDTO identifyHeirarchy(ArrayList<ItemDTO> similarItemList){
	
		similarDeptId = -1;
		similarCatId = -1;
		similarSubCatId = -1;
		similarSegmentId = -1;
		noOfDepts = 0;
		noOfCats = 0;
		noOfSubCats = 0;
		noOfSegments = 0;
		
		Iterator<ItemDTO> similarItemItr = similarItemList.iterator();
		while (similarItemItr.hasNext()){
			ItemDTO similarItem = similarItemItr.next();
			if( similarDeptId != similarItem.deptID){
				similarDeptId = similarItem.deptID;
				noOfDepts++;
			}
			if( similarCatId != similarItem.catID){
				similarCatId = similarItem.catID;
				noOfCats++;
			}
			if( similarSubCatId != similarItem.subCatID){
				similarSubCatId = similarItem.subCatID;
				noOfSubCats++;
			}
			
			if( similarSegmentId != similarItem.segmentID && similarItem.segmentID != -1){
				similarSegmentId = similarItem.segmentID;
				noOfSegments++;
			}
		}
		
		ItemDTO similarItem = null;
		if( similarItemList.size() > 0){
			similarItem = similarItemList.get(0);
		}
		
		return similarItem;
	}
	
	private ItemDTO  narrowByNameBasedLookup(Connection conn, ItemDTO currentItem, ItemDTO similarItem) throws GeneralException {
		String tempItemName = currentItem.itemName;
		currentItem.itemName = getInitialPortion(currentItem.itemName);
		//logger.info( "doing name based lookup ... start");
		if( noOfDepts == 1)
			currentItem.deptID = similarItem.deptID;
		else 
			currentItem.deptID = -1;
		if( noOfCats == 1)
			currentItem.catID = similarItem.catID;
		else
			currentItem.catID = -1;
		currentItem.subCatID = -1;
		currentItem.manufactCode = null;
		//currentItem.manufactCode = currentItem.upc.substring(0,Constants.MANUFACTURER_END_INDEX+2);
		
		ArrayList<ItemDTO> similarItemList = pgdao.getNarrowItemTree(conn, currentItem);
		currentItem.itemName = tempItemName;
		if( similarItemList.size() > 0)
			similarItem = identifyHeirarchy(similarItemList);
		//logger.info( "doing name based lookup ... end");
		return similarItem;
		
	}
	
	private ItemDTO narrowByUPCBasedLookup(Connection conn, ItemDTO currentItem, ItemDTO similarItem) throws GeneralException {
		
		//logger.info( "doing UPC based lookup ... start");
		String tempItemName = currentItem.itemName;
		currentItem.itemName = null;
		if( noOfDepts == 1 && similarItem != null)
			currentItem.deptID = similarItem.deptID;
		else 
			currentItem.deptID = -1;
		if( noOfCats == 1 && similarItem != null)
			currentItem.catID = similarItem.catID;
		else
			currentItem.catID = -1;
		currentItem.subCatID = -1;
		currentItem.manufactCode = currentItem.upc.substring(0,Constants.MANUFACTURER_END_INDEX+3);
		ArrayList<ItemDTO> similarItemList = pgdao.getNarrowItemTree(conn, currentItem);
		if( similarItemList.size() == 0){
			currentItem.manufactCode = currentItem.upc.substring(0,Constants.MANUFACTURER_END_INDEX+2);
			similarItemList = pgdao.getNarrowItemTree(conn, currentItem);
		}
		currentItem.itemName = tempItemName;
		if( similarItemList.size() > 0)
			similarItem = identifyHeirarchy(similarItemList);
		//logger.info( "doing UPC based lookup ... end");
		return similarItem;
	}
	
	private String getInitialPortion(String itemName){
		int length = itemName.length();
		int newStringlength = 0;
		StringBuffer sb = new StringBuffer('%');
		boolean isMoreSpace = false;
		for ( int i = 0; i < length; i++){
			if( itemName.charAt(i) == ' ' || itemName.charAt(i) == '\'' ){ 
				if (!isMoreSpace){
					sb.append('%');
				}
				isMoreSpace = true;
			}else{
				isMoreSpace = false;
				newStringlength++;
				sb.append(itemName.charAt(i));
			}
			if(newStringlength >= SEARCH_STRING_LENGTH )
				break;
		}
		return sb.toString();
		
	}
	
	private void updateHierarchy(Connection conn, ItemDTO item, ItemDTO similarItem) throws GeneralException{
		

		item.deptID = similarItem.deptID;
		if( noOfCats == 1)
			item.catID = similarItem.catID;
		else
			item.catID = -1;
		
		if( noOfSubCats == 1)
			item.subCatID = similarItem.subCatID;
		else
			item.subCatID = -1;
		
		if( noOfSegments == 1)
			item.segmentID = similarItem.segmentID;
		else
			item.segmentID = -1;
		
		pgdao.updateItemHierarchy(conn, item);
		noOfItemsUpdated++;

	}
}
