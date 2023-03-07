package com.pristine.fileformatter.ahold;


import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dto.ItemDTO;
import com.pristine.dto.ItemDetailDTO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class ItemDetailFileFormatter extends PristineFileParser {
	static Logger logger = Logger.getLogger("ItemDetailFileFormatter");
	private int stopCount = -1;
	ArrayList <ItemDetailDTO> updateItemDetailList = new ArrayList <ItemDetailDTO> ();
	ArrayList <ItemDTO> itemDetailListBusiness = new ArrayList <ItemDTO> ();
	HashMap<ItemDetailKey, String> ligDescMap = new HashMap<>();
	Set<ItemDetailKey> ligDescNotFoundInBusinessFeed = new HashSet<>();
	private int recordCount = 0;
	private String ITEM_FEED_TYPE = "IT";
	private String ITEM_FEED_TYPE_IT = "IT";
	private String ITEM_FEED_TYPE_BUSINESS = "BUSINESS";
	public ItemDetailFileFormatter ()
	{
		 super ();
    }

	public static void main(String[] args) {
		
		PropertyConfigurator.configure("log4j-item-ahold.properties");
		PropertyManager.initialize("analysis.properties");
    	String subFolder = null;
    	String destFolder = null;
    	String itemFeedBusinessFolder = null;
    	String encoding = "UTF8";
		
		logger.info("main() - Started");
 
		for (int ii = 0; ii < args.length; ii++) {
			
			String arg = args[ii]; 
			
			if (arg.startsWith("SUBFOLDER")) {
				subFolder = arg.substring("SUBFOLDER=".length());
			}
			
			if (arg.startsWith("DESTFOLDER")) {
				destFolder = arg.substring("DESTFOLDER=".length());
			}
			
			if (arg.startsWith("BUSINESS_FEED_FOLDER")) {
				itemFeedBusinessFolder = arg.substring("BUSINESS_FEED_FOLDER=".length());
			}

			if (arg.startsWith("ENCODING")) {
				encoding = arg.substring("ENCODING=".length());
			}
	 
		}

		ItemDetailFileFormatter itemDetail= new ItemDetailFileFormatter();
		itemDetail.processItemDetailFile(subFolder, destFolder, itemFeedBusinessFolder, encoding );
	}
		

	private void processItemDetailFile(String subFolder, String destFolder, String itemFeedBusinessFolder,
			String encoding) {
		
		String fieldName[] = new String[47];
		fieldName[0] = "productKey";
		fieldName[1] = "retailerItemCode";
		fieldName[2] = "itemDescription";
		fieldName[3] = "itemStatus";
		fieldName[4] = "isSeasonal";
		fieldName[5] = "lineGroup";
		fieldName[6] = "promotedProductLine";
		fieldName[7] = "sizeFamily";
		fieldName[8] = "manufacturer";
		fieldName[9] = "brand";
		fieldName[10] = "isPrivateLabel";
		fieldName[11] = "isPrePriced";
		fieldName[12] = "uom";
		fieldName[13] = "itemSize";
		fieldName[14] = "multiPack";
		fieldName[15] = "pack";
		fieldName[16] = "salvageValuePct";
		fieldName[17] = "deptId";
		fieldName[18] = "deptDescr";
		fieldName[19] = "subdeptId";
		fieldName[20] = "subdeptDescr";
		fieldName[21] = "catId";
		fieldName[22] = "catDescr";
		fieldName[23] = "subCatId";
		fieldName[24] = "subCatDescr";
		fieldName[25] = "segmentId";
		fieldName[26] = "segmentDescr";
		fieldName[27] = "attribute1Type";
		fieldName[28] = "attribute1Value";
		fieldName[29] = "attribute2Type";
		fieldName[30] = "attribute2Value";
		fieldName[31] = "attribute3Type";
		fieldName[32] = "attribute3Value";
		fieldName[33] = "upc";
		fieldName[34] = "upcNumber";
		fieldName[35] = "attribute5Type";
		fieldName[36] = "attribute5Value";
		fieldName[37] = "ligDesc";
		fieldName[38] = "attribute6Value";
		fieldName[39] = "attribute7Type";
		fieldName[40] = "attribute7Value";
		fieldName[41] = "attribute8Type";
		fieldName[42] = "attribute8Value";
		fieldName[43] = "attribute9Type";
		fieldName[44] = "attribute9Value";
		fieldName[45] = "attribute10Type";
		fieldName[46] = "attribute10Value";
		
		logger.info("ItemDetailFileFormatter starts");
		ArrayList<String> fileList;
		try 
		{
			
			parseBusinessItemFeed(itemFeedBusinessFolder, encoding);
			
			if(itemDetailListBusiness.size() == 0){
				throw new GeneralException("Business item feed not processed!");
			}
			
			createLIGDescMapFromBusinessFeed();
			
			ITEM_FEED_TYPE = ITEM_FEED_TYPE_IT;

			fileList = getFiles(subFolder);
			
			for (int j = 0; j < fileList.size(); j++)
			{
	    		logger.info("processing - " + fileList.get(j));
	    		parseDelimitedFileV2(ItemDetailDTO.class, fileList.get(j), '|',fieldName, stopCount, "UTF-8");
	    	}
			
			int itemsInITFeed = updateItemDetailList.size();
			// Add missing items from business item feed 
			addMissingItemsFromBusinessFeed(updateItemDetailList, itemDetailListBusiness);
			
			
			int itemsAddedFromBusiness = updateItemDetailList.size() - itemsInITFeed;
			
			logger.info("processItemDetailFile() - # of records processed: " + recordCount);
			logger.info("processItemDetailFile() - # of items added from business item feed: " + itemsAddedFromBusiness);
			logger.info("processItemDetailFile() - Total # of items: " + updateItemDetailList.size());
			writeIntoFile(destFolder, updateItemDetailList);

			
			logLigDescNotFoundInBusinessFeed();
		} 
		catch (GeneralException e) 
		{
			logger.error("Exception in processing Item Details Records", e);
		}finally {
			PristineDBUtil.close(getOracleConnection());
		}
		
	}
	
	@Override
	public void processRecords(List listobj) throws GeneralException {
		
		if(ITEM_FEED_TYPE.equals(ITEM_FEED_TYPE_IT)){
			processItemFeedFromIT(listobj);
		}else if(ITEM_FEED_TYPE.equals(ITEM_FEED_TYPE_BUSINESS)){
			itemDetailListBusiness.addAll(listobj);
		}
	}
	
	private void processItemFeedFromIT(List listobj){
		for ( int jj = 0; jj <listobj.size();jj++)
		{
			ItemDetailDTO itemDetailDTO = (ItemDetailDTO) listobj.get(jj);
			
			String deptDescr = itemDetailDTO.getDeptDescr().trim();
			itemDetailDTO.setDeptDescr(deptDescr);
			
			String deptId = Integer.toString(Integer.parseInt(itemDetailDTO.getDeptId()));
			itemDetailDTO.setDeptId(deptId);
			
			String deptName = itemDetailDTO.getDeptDescr().trim();
			itemDetailDTO.setDeptDescr(deptName);
			if ( deptName.length() > 2 ){
				itemDetailDTO.setShortName(deptName.substring(0, 2));
			}else {
				itemDetailDTO.setShortName(deptName);
			}
			
			
			String category = itemDetailDTO.getCatDescr().trim();
			itemDetailDTO.setCatDescr(category);
			
			String subCategory = itemDetailDTO.getSubCatDescr().trim();
			itemDetailDTO.setSubCatDescr(subCategory);
			
			String retailerItemCode = Integer.toString(Integer.parseInt(itemDetailDTO.getRetailerItemCode()));
			itemDetailDTO.setRetailerItemCode(retailerItemCode);
			
			String catCode = Integer.toString(Integer.parseInt(itemDetailDTO.getCatId()));
			itemDetailDTO.setCatId(catCode);
			
			String subCatCode = Integer.toString(Integer.parseInt(itemDetailDTO.getSubCatId()));
			//itemDetailDTO.setSubCatId(subCatCode);
			// Format sub category code as category code + sub category code
			if(itemDetailDTO.getCatId().length() > 0){
				if(subCatCode.length() == 1)
					itemDetailDTO.setSubCatId(itemDetailDTO.getCatId() + "00" + subCatCode);
				else if(subCatCode.length() == 2)
					itemDetailDTO.setSubCatId(itemDetailDTO.getCatId() + "0" + subCatCode);
				else
					itemDetailDTO.setSubCatId(itemDetailDTO.getCatId() + subCatCode);
			}else
				itemDetailDTO.setSubCatId(subCatCode);
			
			
			itemDetailDTO.setUpcNumber( PrestoUtil.castUPC(itemDetailDTO.getUpcNumber(), false));
			
			
			// Changed by Pradeep on 05/11/2018
			// Check this item in business item feed for LIG description
			// Use LIG desc from business feed
			// If not present, then use existing logic of LIG description 
			ItemDetailKey itemDetailKey = new ItemDetailKey(itemDetailDTO.getUpcNumber(), itemDetailDTO.getRetailerItemCode());
			// Append LIG Code to LIG name if length = 35, since the new feed can contain only max of 35 characters for LIG name
			if(itemDetailDTO.getLigDesc().length() == 35){
				if(ligDescMap.containsKey(itemDetailKey)){
					
					itemDetailDTO.setLigDesc(ligDescMap.get(itemDetailKey));
				
				}else{
					
					ligDescNotFoundInBusinessFeed.add(itemDetailKey);
					
					itemDetailDTO.setLigDesc(itemDetailDTO.getLigDesc() + ((itemDetailDTO.getLineGroup().length() > 0)
							? "_" + Integer.parseInt(itemDetailDTO.getLineGroup()) : ""));	
				}
			}else	
				itemDetailDTO.setLigDesc(itemDetailDTO.getLigDesc());	
			
			if(itemDetailDTO.getLineGroup() != null && !itemDetailDTO.getLineGroup().equals("")){
				int lineGroup = Integer.parseInt(itemDetailDTO.getLineGroup());
				if(lineGroup > 0)
					itemDetailDTO.setLineGroup(String.valueOf(lineGroup));
				else
					itemDetailDTO.setLineGroup("");
			}
			
			recordCount++;
			updateItemDetailList.add(itemDetailDTO);
						
		}
	}
	
	private void writeIntoFile(String destFolder, ArrayList<ItemDetailDTO> updateItemDetailList) {
		long fileProcessStartTime = System.currentTimeMillis(); 
		try 
		{
	 
			DateFormat dt = new SimpleDateFormat("_yyyy-MM-dd");
	        Date date = new Date();
	        String d =dt.format(date).toString();
	        String fname = "itemLoadFormatted";
			
			String outputPath;
			if( destFolder !=  null )
				outputPath = getRootPath() + "/" +destFolder+"/"+fname+d+".txt";
			else
				outputPath = getRootPath() +"/"+fname+d+".txt";
			
			logger.debug("output path is " + outputPath);
			File file = new File(outputPath); 
			FileWriter fw = new FileWriter(file);
			PrintWriter pw = new PrintWriter(fw);
			String header = "Major Department" + "|" +
			                "Major Dept Code"  + "|" +
			                "Department"  + "|" +
			                "Dept Code" + "|" +
			                "Dept Short Name" + "|" +
			                "Category Name" + "|" + 
			                "Category Code" + "|" +
			                "Sub Category Name" + "|" +
			                "Sub Category Code" + "|" +
			                "Segment name" + "|" +
			                "Segment Code" + "|" +
			                "Item Code" + "|" +
			                "Item Description" + "|" +
			                "Size" + "|" +
			                "Uom" + "|" +
			                "Pack" + "|" +
			                "Private Label Code" + "|" +
			                "Like Item Group Name" + "|" +
			                "Like Item Code" + "|" +
			                "UPC" + "|" +
			                "Level Type" + "|" +
			                "Group Name" + "|" +   
			                "Group code" + "|" +
			                "Manufacturer" + "|" +
			                "Brand" + "|" + 
			                "PrePriceInd";
			

			    // Commented - Since item loader does not handle header in the file though corresponding property is set in properties file            
			    //pw.print(header);
			    //pw.println();
			
			    for(ItemDetailDTO itemDetailDTO : updateItemDetailList)
			    {
			    pw.print("");  //Major Department
			    pw.print("|"); 
			    pw.print("");  //Major Dept Code
			    pw.print("|");
				pw.print(itemDetailDTO.getDeptDescr()); //Department
				pw.print("|");
				pw.print(itemDetailDTO.getDeptId()); //Dept Code
				pw.print("|");
				pw.print(itemDetailDTO.getShortName()); //Dept Short Name
				pw.print("|");
				pw.print(itemDetailDTO.getCatDescr()); //Category Name
				pw.print("|");
				pw.print(itemDetailDTO.getCatId()); //Category Code
				pw.print("|");
				pw.print(itemDetailDTO.getSubCatDescr()); //Sub Category Name
				pw.print("|");
				pw.print(itemDetailDTO.getSubCatId()); //Sub Category Code
				pw.print("|");
				pw.print(""); //Segment name
				pw.print("|");
				pw.print(""); //Segment Code
				pw.print("|");
				pw.print(itemDetailDTO.getRetailerItemCode()); //Item Code  
				pw.print("|");
				pw.print(itemDetailDTO.getItemDescription()); //Item Description
				pw.print("|");
				pw.print(itemDetailDTO.getItemSize()); //Size
				pw.print("|");
				pw.print(itemDetailDTO.getUom()); //Uom
				pw.print("|");
				pw.print(itemDetailDTO.getPack()); //Pack
				pw.print("|");
				pw.print(itemDetailDTO.getIsPrivateLabel()); //Private Label Code
				pw.print("|");
				pw.print(itemDetailDTO.getLigDesc());  //Like Item Group Name
				pw.print("|");
				pw.print(itemDetailDTO.getLineGroup());
				pw.print("|");
				pw.print(itemDetailDTO.getUpcNumber());  //UPC
				pw.print("|");
				pw.print("0"); //Level Type
				pw.print("|");
				pw.print(""); //Group Name /Portfolio Name
				pw.print("|");
				pw.print(""); //Group code /Portfolio Code
				pw.print("|");
				pw.print(itemDetailDTO.getManufacturer()); //Manufacturer
				pw.print("|");
				pw.print(itemDetailDTO.getBrand()); //Brand
				pw.print("|");
				pw.print(itemDetailDTO.getIsPrePriced()); //Pre-Priced Indicator
				
				pw.println("");
			}
			pw.flush();
			pw.close();
			fw.close();
			long fileProcessEndTime = System.currentTimeMillis();
			logger.info("Time taken to write into the file - " + (fileProcessEndTime - fileProcessStartTime) + "ms");
			logger.info("file created.");
		}
		catch (Exception exception)
		{
			logger.error("Exception while writing formatted Item Details Records", exception);
		}
	}
	

	/**
	 * 
	 * @param itemFeedIT
	 * @param itemFeedBusiness
	 */
	private void addMissingItemsFromBusinessFeed(List<ItemDetailDTO> itemFeedIT, List<ItemDTO> itemFeedBusiness){
		
		Set<ItemDetailKey> processedItemsIT = itemFeedIT.stream().map(ItemDetailDTO::getItemDetailKey)
				.collect(Collectors.toSet());

		
		for(ItemDTO itemDTO: itemFeedBusiness){
			if(!processedItemsIT.contains(itemDTO.getItemDetailKey())){
				ItemDetailDTO itemDetailDTO = getItemDetailDTO(itemDTO);
				itemFeedIT.add(itemDetailDTO);
				recordCount++;
			}
		}
		
	}
	
	public ItemDetailDTO getItemDetailDTO(ItemDTO itemDTO){
		ItemDetailDTO itemDetailDTO = new ItemDetailDTO();
		
		/*
		 
		fieldName[i] = "majDeptName";
		fieldName[i] = "majDeptCode";
		fieldName[i] = "deptName";
		fieldName[i] = "deptCode";
		fieldName[i] = "deptShortName";
		fieldName[i] = "catName";
		fieldName[i] = "catCode";
		fieldName[i] = "subCatName";
		fieldName[i] = "subCatCode";
		fieldName[i] = "segmentName";
		fieldName[i] = "segmentCode";
		fieldName[i] = "retailerItemCode";
		fieldName[i] = "itemName";
		fieldName[i] = "size";
		fieldName[i] = "uom";
		fieldName[i] = "pack";
		fieldName[i] = "privateLabelCode";
		fieldName[i] = "likeItemGrp";
		fieldName[i] = "likeItemCode";
		fieldName[i] = "upc";
		 
		 */
		
		itemDetailDTO.setDeptDescr(itemDTO.deptName);
		itemDetailDTO.setDeptId(itemDTO.deptCode);
		itemDetailDTO.setShortName(itemDTO.deptShortName);
		itemDetailDTO.setCatDescr(itemDTO.catName);
		itemDetailDTO.setCatId(itemDTO.catCode);
		itemDetailDTO.setSubCatDescr(itemDTO.subCatName);
		itemDetailDTO.setSubCatId(itemDTO.subCatCode);
		itemDetailDTO.setRetailerItemCode(itemDTO.retailerItemCode);
		itemDetailDTO.setItemDescription(itemDTO.itemName);
		itemDetailDTO.setItemSize(itemDTO.size);
		itemDetailDTO.setUom(itemDTO.uom);
		itemDetailDTO.setPack(itemDTO.pack);
		itemDetailDTO.setIsPrivateLabel(itemDTO.privateLabelCode);
		itemDetailDTO.setLigDesc(itemDTO.likeItemGrp);
		itemDetailDTO.setLineGroup(itemDTO.likeItemCode);
		itemDetailDTO.setUpcNumber(PrestoUtil.castUPC(itemDTO.getUpc(), false));
		itemDetailDTO.setManufacturer(Constants.EMPTY);
		itemDetailDTO.setBrand(itemDTO.brandName);
		itemDetailDTO.setIsPrePriced("0");
		return itemDetailDTO;
	}
	
	private String[] getFieldNames(){
		String fieldName[] = new String[26];

		int i = 0;
    	fieldName[i] = "majDeptName"; // 1
    	i++;
    	
    	fieldName[i] = "majDeptCode";// 2
    	i++;
    	
    	fieldName[i] = "deptName"; // 3 
    	i++;
    	
    	fieldName[i] = "deptCode"; // 4
    	i++;
		
    	fieldName[i] = "deptShortName"; // 5 
		i++;
    	
    	fieldName[i] = "catName"; // 6
    	i++;
    	
    	fieldName[i] = "catCode"; // 7 
    	i++;
    	
    	fieldName[i] = "subCatName"; // 8
    	i++;
    	
    	fieldName[i] = "subCatCode"; // 9
    	i++;
    	
    	fieldName[i] = "segmentName"; // 10
    	i++;
    	
    	fieldName[i] = "segmentCode"; // 11
    	i++;
    	
    	fieldName[i] = "retailerItemCode"; // 12
    	i++;
    	
    	fieldName[i] = "itemName"; // 13
    	i++;
    	
    	fieldName[i] = "size"; // 14
    	i++;
    	
    	fieldName[i] = "uom"; // 15
    	i++;
    	
    	fieldName[i] = "pack"; // 16
    	i++;
    	
    	fieldName[i] = "privateLabelCode"; // 17
    	i++;
    	
    	fieldName[i] = "likeItemGrp"; // 18
    	i++;
		
    	fieldName[i] = "likeItemCode"; // 19
    	i++;

    	fieldName[i] = "upc"; // 20
    	i++;
    	
    	fieldName[i] = "field20"; // 21
    	i++;
    	
    	fieldName[i] = "field21"; // 22
    	i++;
    	
    	fieldName[i] = "field23"; // 23
    	i++;
    	
    	fieldName[i] = "portfolio"; // 25
    	i++;
    	
    	fieldName[i] = "manufactName"; // 26
    	i++;
    	
    	fieldName[i] = "brandName"; // 27
    	i++;
    	
    	/*pw.print("|");
		pw.print("0"); //Level Type
		pw.print("|");
		pw.print(""); //Group Name /Portfolio Name
		pw.print("|");
		pw.print(""); //Group code /Portfolio Code
		pw.print("|");
		pw.print(itemDetailDTO.getManufacturer()); //Manufacturer
		pw.print("|");
		pw.print(itemDetailDTO.getBrand()); //Brand
		pw.print("|");
		pw.print(itemDetailDTO.getIsPrePriced()); //Pre-Priced Indicator
*/    	
    	
    	return fieldName;
	}

	
	private void logLigDescNotFoundInBusinessFeed(){
		StringBuilder sb = new StringBuilder();
		for(ItemDetailKey itemDetailKey: ligDescNotFoundInBusinessFeed){
			sb.append(itemDetailKey.toString() + ", ");
		}
		logger.info("***LIG Description not found for below items***");
		logger.info("# of items in IT feed for which LIG name not found in business feed: " + ligDescNotFoundInBusinessFeed.size());
		logger.info(sb.toString());
	}
	
	/**
	 * 
	 * @param businessFeedFolder
	 * @throws GeneralException
	 */
	private void parseBusinessItemFeed(String businessFeedFolder, String encoding) throws GeneralException {

		if (businessFeedFolder != null) {
			String[] fieldNames = getFieldNames();
			ArrayList<String> fileList = getFiles(businessFeedFolder);
			ITEM_FEED_TYPE = ITEM_FEED_TYPE_BUSINESS;
			for (int j = 0; j < fileList.size(); j++) {
				logger.info("processing business item feed - " + fileList.get(j));
				parseDelimitedFileV2(ItemDTO.class, fileList.get(j), '|', fieldNames, stopCount, encoding);
			}

			logger.info(
					"parseBusinessItemFeed() - Total # of items in business feed: " + itemDetailListBusiness.size());
		}
	}
	
	/**
	 * 
	 */
	private void createLIGDescMapFromBusinessFeed(){

		itemDetailListBusiness.forEach(item -> {
			if (item.getLikeItemCode() != null && !Constants.EMPTY.equals(item.getLikeItemCode())) {
				ItemDetailKey itemDetailKey = new ItemDetailKey(PrestoUtil.castUPC(item.upc, false),
						item.retailerItemCode);
				ligDescMap.put(itemDetailKey, item.likeItemGrp);
			}
		});
		
	}
}
