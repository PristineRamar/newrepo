package com.pristine.dataload.tops;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.PlanogramDAO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.PlanogramDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PlanogramDataLoad extends PristineFileParser {


	private static Logger logger = Logger.getLogger("PlanogramDataLoad");
	private Connection conn = null;
	private ItemDAO itemdao = null;
	private int recordCount = 0;
	private String storeNo = null;
	private LinkedHashMap PLANOGRAM_FIELD = null;
	private List<PlanogramDTO> planogramList = new ArrayList<PlanogramDTO> ();
	private PlanogramDAO planogramdao = new PlanogramDAO();
	private Set<String> distinctSkippedUpcSet = new HashSet<>();
	private Set<String> distinctSkippedStoreSet = new HashSet<>();
	
	public PlanogramDataLoad(){
		//***Change the File name for Log 4j
		PropertyConfigurator.configure("log4j-planogram-loading.properties");
		PropertyManager.initialize("analysis.properties");
		conn = getOracleConnection();
		itemdao = new ItemDAO();
	}
	
	public static void main(String[] args) {
		PlanogramDataLoad pgram = new PlanogramDataLoad();
		pgram.processPlanogramData(args);
	}

	private void processPlanogramData(String[] args) {
		try {
			logger.info("Planogram Data load Starts");
			
			String relativePath = null;
		
			for (int ii = 0; ii < args.length; ii++) {

				String arg = args[ii];

				if (arg.startsWith("RELATIVE_PATH")) {
					relativePath = arg.substring("RELATIVE_PATH=".length());
				}

				if (arg.startsWith("STORE_NO")) {
					storeNo = arg.substring("STORE_NO=".length());
				}
			}
			
			if( relativePath == null ){
				logger.error("Relative Path not passed, pass it as RELATIVE_PATH=");
				return;
			}
			ArrayList<String> zipFileList = getZipFiles(relativePath);
			ArrayList<String> fileList = getFiles(relativePath);
			// Continue only if there are files
			if ( zipFileList.size() > 0 || fileList.size() > 0) {
				preparePlanoGramFile(relativePath);
			} else {
				logger.error("No input files present");
			}
		} catch (GeneralException | Exception ex) {
			PristineDBUtil.rollbackTransaction(conn, "Planogram Load failed");
			logger.error("Error in Price Group Formatter. Transaction is rollbacked - ", ex);
		}finally{
			PristineDBUtil.close(conn);
			logger.info("Planogram Data load Completed");
		}
	}

	private void preparePlanoGramFile(String relativePath) throws GeneralException{
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
			    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
				
				for (int i = 0; i < fileList.size(); i++) {
					
					long fileProcessStartTime = System.currentTimeMillis(); 
					
				    String files = fileList.get(i);
				    logger.info("File Name - " + fileList.get(i));
				    
			    	recordCount = 0;
			    	
			    	parseTextFile(PlanogramDTO.class, files, getPlanogramFields(), stopCount);
			    	
					logger.info("Number of records processed - " + recordCount);
				    long fileProcessEndTime = System.currentTimeMillis();
				    logger.info("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime) + "ms");
				}
				prepareData();
				int skippedCount = planogramdao.insertPlanogramData(conn, planogramList);
				logger.info("Rows skipped -- " + skippedCount);
				logSkippedRows();
				
				
				PristineDBUtil.commitTransaction(conn, "Planogram Load");
				
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
				PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
			} else {
				PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				throw new GeneralException("Error in preparePlanoGramFile()");
			}
			curZipFileCount++;
			processZipFile = true;
		}while (curZipFileCount < zipFileList.size());
	
	}
	private void prepareData() throws GeneralException {
		
		//Set the records to a list object
		//Set store id, item code and trim capacity 
		//collect the stores being processed in set
		//delete for the stores
		//commit
		
		CompStoreDAO compStrdao = new CompStoreDAO (); 
		int chainId = compStrdao.getCompChainId(conn);
		logger.debug("Subsriber chain id = " + chainId);
		
		String [] storeArray;
		//Prepare  set of stores
		if (storeNo != null) {
			storeArray = storeNo.split(",");
		}else {
			HashSet<String> storeSet = new HashSet<String>();
			for( PlanogramDTO pgdto: planogramList) {
				storeSet.add(pgdto.getStoreNo().substring(1));
			}
			storeArray = storeSet.toArray(new String[0]);
		}
		
		//Get the store list
		HashMap<String, Integer> storeMap = compStrdao.getCompStoreData(conn, chainId,storeArray);
		
		//Delete from Planogram
		Collection<Integer> storeCollection = storeMap.values();
		for( int storeId: storeCollection){
			planogramdao.deletePlanogramInfo(conn, storeId);
		}
		
		//commit the delete transactions
		PristineDBUtil.commitTransaction(conn, "Committing delete Planogramdata");
		
		HashMap<ItemDetailKey, String>itemCodeMap = itemdao.getAllItemsFromItemLookupV2(conn);
		
		//Loop and set 
		//Set Store Id and ItemCode 
		//If item code is na, make it -1 and same with StoreId
		
		for( PlanogramDTO pgramDto: planogramList) {
			//set the item code and store itd
			String upc = PrestoUtil.castUPC(pgramDto.getUpc(),false);
			ItemDetailKey itemDetailKey = new ItemDetailKey(upc, pgramDto.getRetailerItemCode());
			
			if(itemCodeMap.containsKey(itemDetailKey) ){
				String strItemCode = itemCodeMap.get(itemDetailKey);
				pgramDto.setItemCode(Integer.parseInt(strItemCode));
			}
			else {
				pgramDto.setItemCode(-1);
				distinctSkippedUpcSet.add("UPC - " + itemDetailKey.getUpc() + " Ret item code - " + itemDetailKey.getRetailerItemCode());
			}
				
			String strNum = pgramDto.getStoreNo().substring(1);
			
			if( storeMap.containsKey(strNum)){
				pgramDto.setStoreId(storeMap.get(strNum));
			}
			else{ 
				pgramDto.setStoreId(-1);
				distinctSkippedStoreSet.add(strNum);
			}
		}
		
	}

	private LinkedHashMap getPlanogramFields() {
		if (PLANOGRAM_FIELD == null) {
			PLANOGRAM_FIELD = new LinkedHashMap();
			PLANOGRAM_FIELD.put("storeNo", "0-5");
			PLANOGRAM_FIELD.put("retailerItemCode", "5-17");
			PLANOGRAM_FIELD.put("aisleFix", "17-37");
			PLANOGRAM_FIELD.put("aislePos", "37-40");
			PLANOGRAM_FIELD.put("shelf", "40-60");
			PLANOGRAM_FIELD.put("shelfPos", "60-63");
			PLANOGRAM_FIELD.put("shelfProdPos", "63-66");
			PLANOGRAM_FIELD.put("upc", "66-80");
			PLANOGRAM_FIELD.put("planogramNo", "80-89");
			PLANOGRAM_FIELD.put("capacity", "165-176");
		}
		return PLANOGRAM_FIELD;
		
	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<PlanogramDTO> pgList = (List<PlanogramDTO> ) listobj;
		planogramList.addAll(pgList);
		recordCount += pgList.size();
	}

	
	private void logSkippedRows(){
		StringBuilder sb = new StringBuilder();
		for(String item: distinctSkippedUpcSet){
			sb.append("[" + item + "], ");
		}
		if(!sb.toString().isEmpty()){
			logger.info("# of items skipped - " + distinctSkippedUpcSet.size());
			logger.info("Skipped items:");
			logger.info(sb.toString());
		}
		sb = new StringBuilder();
		for(String strNum: distinctSkippedStoreSet){
			sb.append(strNum + ",");
		}
		if(!sb.toString().isEmpty()){
			logger.info("# of stores skipped - " + distinctSkippedStoreSet.size());
			logger.info("Skipped stores:");
			logger.info(sb.toString());
		}
	}

}




/*
 * Table setup
 drop table planogram_info;
 create table planogram_info (
   store_id number(5) not null,
   ITEM_CODE NUMBER(6) not null,
   AISLE_FIX VARCHAR2(20),
   AISLE_POS NUMBER(3),
   SHELF VARCHAR2(20),
   SHELF_POS NUMBER(3),
   SHELF_PROD_POS NUMBER(3),
   PLANOGRAM_NBR VARCHAR2(10),
   CAPACITY NUMBER(5),
   FACINGS  NUMBER(3),
   update_timestamp date not null   
 );
 	 
CREATE INDEX PLANOGRAM_IDX
ON planogram_info (store_id, ITEM_CODE)
COMPUTE STATISTICS ;
 
 
 */