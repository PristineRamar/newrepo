package com.pristine.test.dataload;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.ItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.fileformatter.gianteagle.ItemFileFormatter;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

public class GEItemFileFormatterJunitTest extends PristineFileParser{
	private static Logger logger = Logger.getLogger("ItemFileFormatterGEJUnit");
	static TreeMap<Integer, String> allColumns = new TreeMap<>();
	List<ItemDTO> itemList = new ArrayList<ItemDTO>();
	String relativeOutputPath = PropertyManager.getProperty("ITEM_FORMATTER_RELATIVE_OUTPUT_PATH", "ItemFormatter");
	String relativeInputPath = PropertyManager.getProperty("ITEM_FORMATTER_RELATIVE_INPUT_PATH", "ItemData");
	String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", ""); 
	
	
	private void createDirectory(){
		 String inputPath = rootPath+"/"+relativeInputPath;
		 String outputPath = rootPath+"/"+relativeOutputPath;
		 
		 PrestoUtil.createDirIfNotExists(inputPath);
		 PrestoUtil.createDirIfNotExists(outputPath);
	}
	
	public GEItemFileFormatterJunitTest(){
		createDirectory();
	}
	private void  processItemFile(String fileName){
	    try{
	    	super.headerPresent = false;
	    	fillAllColumns();
	    	String fieldNames[] = new String[allColumns.size()];
	    	int i = 0;
			for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
				fieldNames[i] = columns.getValue();
				i++;
			}
		    // Changes to incorporate Brand - Ends
	    	int stopCount  = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
	    	List<String> fileList = getFiles(relativeOutputPath);
	    	for ( int j = 0; j < fileList.size(); j++){
	    		if(fileList.get(j).toUpperCase().contains(fileName.toUpperCase())){
	    			logger.info("processing - " + fileList.get(j));
		    		parseDelimitedFile(ItemDTO.class, fileList.get(j), '|',fieldNames, stopCount);
	    		}
	    	}
	    }catch (GeneralException ex) {
	        logger.error("GeneralException", ex);
	    } catch (Exception ex) {
	        logger.error("JavaException", ex);
	    }
		return;
	}
	
	private void copyInputFiles(String fileName) throws IOException{
		
		String filePath = "com/pristine/test/InputFiles/"+fileName;
//		logger.info("Path: "+this.getClass().getClassLoader());
		ClassLoader loader = this.getClass().getClassLoader();
		loader.getResource(filePath);
		String currentDirectory;
		File file = new File(filePath);
		currentDirectory = file.getAbsolutePath().replace("\\", "/");
		logger.info("Current working directory : "+currentDirectory);
		String outputPath = rootPath+"/"+relativeInputPath+"/"+fileName;
		FileUtils.copyFile(new File(currentDirectory),new File(outputPath));
	}
	
	/**
	 * Single Non LIG item(Retailer item code and UPC Combinations) given as an input.
	 * Expected output: One entry needs to be created for the given item
	 */
	@Test
	public void testCase1(){
		
		try {
			
			copyInputFiles("ItemFormatter_testCase1.txt");
			
			String args[] = {"INPUT_FOLDER="+relativeInputPath, "OUTPUT_FOLDER="+relativeOutputPath, "MODE=FORMAT"};
			ItemFileFormatter.main(args);
			
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase1.txt");
			assertEquals("More than one entry has been created", 1,itemList.size());
			itemList = new ArrayList<ItemDTO>();
			for(ItemDTO itemDTO: itemList){
				assertEquals("UPC is not matching", "1200000893",itemDTO.getUpc());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Single Non LIG item with different SPLR NO
	 * Expected output: One entry needs to be created for the given item
	 */
	@Test
	public void testCase2(){
		
		try {
			copyInputFiles("ItemFormatter_testCase2.txt");
			String args[] = {"INPUT_FOLDER="+relativeInputPath, "OUTPUT_FOLDER="+relativeOutputPath, "MODE=FORMAT"};
			ItemFileFormatter.main(args);
			
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase2.txt");
			assertEquals("More than one entry has been created", 1,itemList.size());
			for(ItemDTO itemDTO: itemList){
				assertEquals("UPC is not matching", "1200000893",itemDTO.getUpc());
			}
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Same item in both LIG and Non LIG entries in the input feed
	 * Expected output: One entry needs to be created for the given item at LIG level
	 */
	@Test
	public void testCase3(){
		try {
			copyInputFiles("ItemFormatter_testCase3.txt");
			String args[] = {"INPUT_FOLDER="+relativeInputPath, "OUTPUT_FOLDER="+relativeOutputPath, "MODE=FORMAT"};
			ItemFileFormatter.main(args);
			
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase3.txt");
			assertEquals("More than one entry has been created", 1,itemList.size());
			
			for(ItemDTO itemDTO: itemList){
				assertEquals("UPC is not matching", "1200000893",itemDTO.getUpc());
				assertEquals("Family Code for some item is not matching", "4398",itemDTO.getLikeItemCode());
			}
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Same item has Non LIG and LIG entries with Different family codes
	 * Expected output: one entry needs to be created for the given item at LIG level. (Minimum family code number will be considered as Family code)
	 */
	@Test
	public void testCase4(){
		
		try {
			copyInputFiles("ItemFormatter_testCase4.txt");
			String args[] = {"INPUT_FOLDER="+relativeInputPath, "OUTPUT_FOLDER="+relativeOutputPath, "MODE=FORMAT"};
			ItemFileFormatter.main(args);
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase4.txt");
			assertEquals("More than one entry has been created", 1,itemList.size());
			
			for(ItemDTO itemDTO: itemList){
				assertEquals("UPC is not matching", "1200000893",itemDTO.getUpc());
				assertEquals("Family Code for some item is not matching", "4308",itemDTO.getLikeItemCode());
			}
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Multiple items with Multiple Family codes which is linked with each other.
	 * Expected output: Items which is related with each other based on the Family code should be considered as single LIG. 
	 * And for those items one Family code must be assigned
	 */
	@Test
	public void testCase5(){
		
		
		try {
			copyInputFiles("ItemFormatter_testCase5.txt");
			String args[] = {"INPUT_FOLDER="+relativeInputPath, "OUTPUT_FOLDER="+relativeOutputPath, "MODE=FORMAT"};
			ItemFileFormatter.main(args);
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase5.txt");
			assertEquals("# of entries were not matching", 12,itemList.size());
			
			for(ItemDTO itemDTO: itemList){
				assertEquals("Family Code for some item is not matching", "4308",itemDTO.getLikeItemCode());
			}
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Different Items with different LIG/ Family codes
	 * Expected output: New entries for different items with differet LIG needs to be created. 
	 */
	@Test
	public void testCase6(){
		
		try {
			copyInputFiles("ItemFormatter_testCase6.txt");
			String args[] = {"INPUT_FOLDER="+relativeInputPath, "OUTPUT_FOLDER="+relativeOutputPath, "MODE=FORMAT"};
			ItemFileFormatter.main(args);
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase6.txt");
			assertEquals("# of entries were not matching", 5,itemList.size());
			
			for(ItemDTO itemDTO: itemList){
				
				if(itemDTO.getUpc().equals("1200000874") || itemDTO.getUpc().equals("1200000893")){
					assertEquals("Family Code for some item is not matching", "439899",itemDTO.getLikeItemCode());
				}
				if(itemDTO.getUpc().equals("7800008361") || itemDTO.getUpc().equals("1200000916")){
					assertEquals("Family Code for some item is not matching", "439889",itemDTO.getLikeItemCode());
				}
				if(itemDTO.getUpc().equals("1200000841")){
					assertEquals("Family Code for some item is not matching", "439878",itemDTO.getLikeItemCode());
				}
			}
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Different Non LIG items(Retailer item code and UPC Combinations) given as an input.
	 * Expected output: One entry needs to be created for each given items
	 */
	@Test
	public void testCase7(){
		
		try {
			copyInputFiles("ItemFormatter_testCase7.txt");
			String args[] = {"INPUT_FOLDER="+relativeInputPath, "OUTPUT_FOLDER="+relativeOutputPath, "MODE=FORMAT"};
			ItemFileFormatter.main(args);
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase7.txt");
			assertEquals("# of entries were not matching", 2,itemList.size());
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Different items in different LIG group. None of the items from different LIG's are related to any other LIG's.
	 * Expected output: Items with respect to different LIG's needs to be created
	 */
	@Test
	public void testCase8() {

		try {
			copyInputFiles("ItemFormatter_testCase8.txt");
			String args[] = { "INPUT_FOLDER=" + relativeInputPath, "OUTPUT_FOLDER=" + relativeOutputPath, "MODE=FORMAT" };
			ItemFileFormatter.main(args);
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase8.txt");
			assertEquals("# of entries were not matching", 6, itemList.size());
			for (ItemDTO itemDTO : itemList) {

				if (itemDTO.getUpc().equals("1200000916") || itemDTO.getUpc().equals("1200000874") || itemDTO.getUpc().equals("1200000893")) {
					assertEquals("Family Code for some item is not matching", "439899", itemDTO.getLikeItemCode());
				}
				if (itemDTO.getUpc().equals("12000008999") || itemDTO.getUpc().equals("1200000899") || itemDTO.getUpc().equals("1200000999")) {
					assertEquals("Family Code for some item is not matching", "439889", itemDTO.getLikeItemCode());
				}
			}
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Same items in different LIG group.
	 * Expected output: unique items with one LIG group needs to be created using MINIMUM Family code
	 */
	@Test
	public void testCase9() {

		try {
			copyInputFiles("ItemFormatter_testCase9.txt");
			String args[] = { "INPUT_FOLDER=" + relativeInputPath, "OUTPUT_FOLDER=" + relativeOutputPath, "MODE=FORMAT" };
			ItemFileFormatter.main(args);
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase9.txt");
			assertEquals("# of entries were not matching", 3, itemList.size());
			for (ItemDTO itemDTO : itemList) {

				if (itemDTO.getUpc().equals("1200000916") || itemDTO.getUpc().equals("1200000874") || itemDTO.getUpc().equals("1200000893")) {
					assertEquals("Family Code for some item is not matching", "439889", itemDTO.getLikeItemCode());
				}
			}
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Different items in one LIG and one of the item is linked with another LIG. And in 2nd LIG one of the item is linked with 3rd LIG items
	 * Expected output: unique items with one LIG group needs to be created using MINIMUM Family code
	 */
	@Test
	public void testCase10() {

		try {
			copyInputFiles("ItemFormatter_testCase10.txt");
			String args[] = { "INPUT_FOLDER=" + relativeInputPath, "OUTPUT_FOLDER=" + relativeOutputPath, "MODE=FORMAT" };
			ItemFileFormatter.main(args);
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase10.txt");
			assertEquals("# of entries were not matching", 7, itemList.size());
			for (ItemDTO itemDTO : itemList) {
				assertEquals("Family Code for some item is not matching", "439879", itemDTO.getLikeItemCode());
			}
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Different items in one LIG and one of the item is linked with another LIG. And in 2nd LIG one of the item is linked with 3rd LIG items
	 * Expected output: unique items with one LIG group needs to be created using MINIMUM Family code 
	 * and respective Family description must be assigned to all the items
	 */
	@Test
	public void testCase11() {

		try {
			copyInputFiles("ItemFormatter_testCase11.txt");
			String args[] = { "INPUT_FOLDER=" + relativeInputPath, "OUTPUT_FOLDER=" + relativeOutputPath, "MODE=FORMAT" };
			ItemFileFormatter.main(args);
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase11.txt");
			assertEquals("# of entries were not matching", 7, itemList.size());
			for (ItemDTO itemDTO : itemList) {
				assertEquals("Family Code for some item is not matching", "439879", itemDTO.getLikeItemCode());
				assertEquals("Family Desc for some item is not matching", "PEPSI 7 PK(439879)", itemDTO.getLikeItemGrp());
			}
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Same item has Non LIG and LIG entries with Different family codes
	 * Expected output: one entry needs to be created for the given item at LIG level. 
	 * Minimum family code number will be considered as Family code and respective family desc should be considered
	 */
	@Test
	public void testCase12(){
		
		try {
			copyInputFiles("ItemFormatter_testCase12.txt");
			String args[] = {"INPUT_FOLDER="+relativeInputPath, "OUTPUT_FOLDER="+relativeOutputPath, "MODE=FORMAT"};
			ItemFileFormatter.main(args);
			itemList = new ArrayList<ItemDTO>();
			processItemFile("ItemFormatter_testCase12.txt");
			assertEquals("More than one entry has been created", 1,itemList.size());
			
			for(ItemDTO itemDTO: itemList){
				assertEquals("UPC is not matching", "1200000893",itemDTO.getUpc());
				assertEquals("Family Code for some item is not matching", "4308",itemDTO.getLikeItemCode());
				assertEquals("Family Code for some item is not matching", "PEPSI(TYPE 4) 24 OZ 6 PK(4308)",itemDTO.getLikeItemGrp());
			}
			itemList = new ArrayList<ItemDTO>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void fillAllColumns() {

		boolean hasDeptShortName = false;
		int noOfFields = Integer.parseInt(PropertyManager.getProperty("ITEM_LOAD.NO_OF_FIELDS", "24"));

		int arraysize = hasDeptShortName ? noOfFields : noOfFields - 1;
		int i = 1;
		allColumns.put(i++, "majDeptName");
		allColumns.put(i++, "majDeptCode");
		allColumns.put(i++, "deptName");
		allColumns.put(i++, "deptCode");
		allColumns.put(i++, "deptShortName");
		allColumns.put(i++, "catName");
		allColumns.put(i++, "catCode");
		allColumns.put(i++, "subCatName");
		allColumns.put(i++, "subCatCode");
		allColumns.put(i++, "segmentName");
		allColumns.put(i++, "segmentCode");
		allColumns.put(i++, "retailerItemCode");
		allColumns.put(i++, "itemName");
		allColumns.put(i++, "size");
		allColumns.put(i++, "uom");
		allColumns.put(i++, "pack");
		allColumns.put(i++, "privateLabelCode");
		allColumns.put(i++, "likeItemGrp");
		allColumns.put(i++, "likeItemCode");
		allColumns.put(i++, "upc");
		allColumns.put(i++, "levelType");
		allColumns.put(i++, "empty");
		allColumns.put(i++, "portfolioCode");
		allColumns.put(i++, "portfolio");

		// Changes to incorporate Brand
		for (; i < arraysize; i++) {
			allColumns.put(i++, "field" + i);
		}
	}


	@Override
	public void processRecords(List listobj) throws GeneralException {
		itemList = (List<ItemDTO>) listobj; 
		
	}
}
