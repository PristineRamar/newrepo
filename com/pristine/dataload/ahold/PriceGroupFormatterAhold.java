package com.pristine.dataload.ahold;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dataload.tops.PriceGroupFormatter;
import com.pristine.dto.LigDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.PropertyManager;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;

public class PriceGroupFormatterAhold  extends PristineFileParser {

	private static Logger logger = Logger.getLogger("PriceGroupFormatterAhold");
	private int recordCount = 0;
	
	private HashMap<String, LigDTO> ligRecords = new HashMap<String, LigDTO> ();
	private HashMap<String, LigDTO> categoryRecords = new HashMap<String, LigDTO> ();
	private String categoryName = "";
	private PriceGroupFormatter pgFormatter; 
	
	public PriceGroupFormatterAhold(){
		PropertyConfigurator.configure("log4j-lig-loader.properties");
		PropertyManager.initialize("analysis.properties");
		pgFormatter = new PriceGroupFormatter();
	}
	
	public static void main(String[] args) {
		
		PriceGroupFormatterAhold pgHold = new PriceGroupFormatterAhold ();
		try{
			pgHold.formatData(args);
		} catch(GeneralException ge){
			logger.error("Error in Price Group Formatter. ", ge);
		}
		catch(Exception ex){
			logger.error("Unexpected Exception. ", ex);
		}

	}

	private void formatData(String[] args) throws GeneralException {
		
		String relativePath = null;
		String sheetName = null;
		String categoryName = null;
		String categoryId = null;
		for (int ii = 0; ii < args.length; ii++) {

			String arg = args[ii];
			if (arg.startsWith("RELATIVE_PATH")) {
				relativePath = arg.substring("RELATIVE_PATH=".length());
			}
			if (arg.startsWith("EXCEL_SHEET")) {
				sheetName = arg.substring("EXCEL_SHEET=".length());
			}
			if (arg.startsWith("CATEGORY")) {
				categoryName = arg.substring("CATEGORY=".length());
			}
			if (arg.startsWith("CATEGORY_ID")) {
				categoryId = arg.substring("CATEGORY_ID=".length());
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
				logger.info("Processing File : " + fileName);
				readRecords(fileName, sheetName,categoryName);
				logger.info("Total Record Count " + recordCount);
				logger.info("Total Active Record Count " + ligRecords.size());
				
				prepareData(fileName);
				//Then initiate TOPS program and see if it can be used to format data.
				
				
				pgFormatter.initializeValues(ligRecords, categoryRecords, categoryName, "AHOLD USA");
				pgFormatter.prepareInitialPriceGroup();
				pgFormatter.identifySizeLead();
				pgFormatter.prepareFinalPriceGroup();
				pgFormatter.AssignTiersOnFinalPG();
				pgFormatter.AssignPGLead();
				pgFormatter.SetupBrandRelationship();
				//Note - Category ID is hard coded as 0 and this needs to be passed.
				pgFormatter.createPriceGroupExcel(relativePath, categoryId);
			}
			pgFormatter.performCloseOperation(true);

			
		}
		else{
			logger.error("No input files present");
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

	private void readRecords(String fileName, String sheetName, String categoryName) throws GeneralException {
		
		try {
			FileInputStream fileInputStream = new FileInputStream(fileName);
			HSSFWorkbook workbook = new HSSFWorkbook(fileInputStream);
			HSSFSheet worksheet = workbook.getSheet(sheetName);
			Iterator<Row> iterator = worksheet.iterator();
			int count = 0;
			ligRecords.clear();
			while (iterator.hasNext()) {
				count++;
				if( count < 3) continue;
				recordCount++;
	            Row nextRow = iterator.next();
	            String status = nextRow.getCell(13).getStringCellValue();  //Status - Active
	            String excelCategory = nextRow.getCell(27).getStringCellValue();  //Status - Active

	            if( status.equals("ACTIVE") && excelCategory.equals(categoryName)){
	            	LigDTO ligDTO = new LigDTO ();
	            	ligDTO.setRetailerItemCode(nextRow.getCell(0).getStringCellValue());  //Item Code
		            ligDTO.setRetailerItemCodeNoVer(nextRow.getCell(1).getStringCellValue());  //Item Code
		            ligDTO.setItemName(nextRow.getCell(3).getStringCellValue());  //Item Name
		            double itemSize = nextRow.getCell(5).getNumericCellValue();
		            ligDTO.setItemSize(Double.toString(itemSize));  //Item Size
		            ligDTO.setUomName(nextRow.getCell(6).getStringCellValue());  //UOM
		            ligDTO.setBrandName(nextRow.getCell(7).getStringCellValue());  //Brand
		            ligDTO.setUpc(nextRow.getCell(10).getStringCellValue());  //UPC
		            ligDTO.setLineGroupIdentifier(nextRow.getCell(11).getStringCellValue());  //LIR Name
		            ligDTO.setSizeFamily(nextRow.getCell(21).getStringCellValue());  //Size Family
		            ligDTO.setSizeClass(nextRow.getCell(22).getStringCellValue());  //Size Class - Small/Medium...
		            ligDTO.setBrandFamily(nextRow.getCell(23).getStringCellValue());  //Brand Family
		            ligDTO.setBrandClass(nextRow.getCell(24).getStringCellValue());  //Brand Class
		            ligDTO.setKviCode("");
		            
		            if( !ligDTO.getSizeFamily().isEmpty() || !ligDTO.getBrandFamily().isEmpty() ){
		            	ligRecords.put(ligDTO.getRetailerItemCodeNoVer(), ligDTO);
		            }
	            }
			}
			
		} catch( IOException ioe){
			throw new GeneralException("File IO Exception in readRecords method", ioe);
		}
	}


	
	@Override
	public void processRecords(List listobj) throws GeneralException {
		
		
		
	}

}

/*
if(ligMap.containsKey(ligName)){
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

}else{
	categoryRecords.put(key, valueDTO);
	ligMap.put(ligName, key);
}
*/