package com.pristine.dataload.prestoload;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.StoreDAO;
import com.pristine.dao.StoreGroupDAO;
import com.pristine.dto.LocationDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;

import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class StoreSetup extends PristineFileParser {

	private static Logger logger = Logger.getLogger("StoreSetup");
    
    private int recordCount=0;
    private int skippedCount=0;
    private int processedCount=0;
    private int newCount=0;
    private int updateCount=0;
    private int errorCount=0;
    private final static String COMPETITOR_STORES = "LOAD_COMPETITOR_STORES=";
    private Connection conn = null;
    private static int columnCount = 45;
    private static int defaultChainID = -1;
    private static String chainFilter = null;
    
    private StoreDAO strdao = new StoreDAO();
    private  StoreGroupDAO strGrpdao= new StoreGroupDAO();
    private static boolean LOAD_COMPETITOR_STORES = false;
  	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		StoreSetup dataload = new StoreSetup();
    	
		PropertyConfigurator.configure("log4j.properties");
		
        if( args.length < 1  || args.length > 4){
        	logger.info("Invalid Arguments, args[0] should be relative path [columnLength] [defaultChainId] [ChainFilter] ");
        	System.exit(-1);
        }
        //To avoid setup Location Hierarchy function during the competitor store setup process
        for (String arg : args) {
            if (arg.startsWith("LOAD_COMPETITOR_STORES")) {
            	if(arg.substring(COMPETITOR_STORES.length()).equalsIgnoreCase("TRUE")){
            		LOAD_COMPETITOR_STORES = true;
            	}
    		}
        }
        if(!LOAD_COMPETITOR_STORES){
	        if( args.length > 1){
	        	// -1 is value to use the default column count value
	        	int tempColCount = Integer.parseInt(args[1]);
	        	if( tempColCount  > 0)
	        		columnCount = tempColCount;
	        }
	        
	        if( args.length > 2){
	        	// -1 is value to use the default chain value 
	        	int tempChainId = Integer.parseInt(args[2]);
	        	if( tempChainId  > 0)
	        		defaultChainID = tempChainId;
	        }
	
	        if( args.length > 3){
	        	chainFilter = args[3]; 
	        }
        }
        dataload.setStores(args[0]);

	}

	private void setStores(String relativePath) {
		// TODO Auto-generated method stub
		conn = getOracleConnection();
		boolean commit = true;

		try {
			ArrayList<String> fileList = getFiles(relativePath);
			for (int i = 0; i < fileList.size(); i++) {
			    String files = fileList.get(i);
			    int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));
			    String fieldNames[] = new String[46];

		    	fieldNames[0] = "chainId";
		    	fieldNames[1] = "chainName";
		    	fieldNames[2] = "strNum";
		    	fieldNames[3] = "addrLine1";
		    	fieldNames[4] = "addrLine2";
		    	fieldNames[5] = "city";
		    	fieldNames[6] = "state";
		    	fieldNames[7] = "zip";
		    	fieldNames[8] = "gblLocNum";
		    	fieldNames[9] = "zoneNum";
		    	fieldNames[10] = "zoneName";
		    	fieldNames[11] = "is24HrInd";
		     	fieldNames[12] = "pharmacyInd";
		    	fieldNames[13] = "gasStationInd"; 
		    	fieldNames[14] = "bankInd";
		    	fieldNames[15] = "fastFoodInd";
		    	fieldNames[16] = "coffeeShopInd";
		    	fieldNames[17] = "storeClass";
		    	fieldNames[18] = "storeType";
		    	fieldNames[19] = "addlType1";
		    	fieldNames[20] = "addlType2";
		    	fieldNames[21] = "storeOpenDate";
		    	fieldNames[22] = "storeReModelDate";
		    	fieldNames[23] = "storeAcqDate";
		    	fieldNames[24] = "storeCloseDate";
		    	fieldNames[25] = "storeAnnvDate";
		    	fieldNames[26] = "sqFootage";
		    	fieldNames[27] = "dept1ZoneNum";
		    	fieldNames[28] = "dept2ZoneNum";
		    	fieldNames[29] = "dept3ZoneNum";
		    	fieldNames[30] = "storeMgrName";
		    	fieldNames[31] = "storePhoneNo";
		    	fieldNames[32] = "storeFaxNo";
		    	fieldNames[33] = "distName";
		    	fieldNames[34] = "distMgrName";
		    	fieldNames[35] = "distPhoneNo";
		    	fieldNames[36] = "distFaxNo";
		    	fieldNames[37] = "regName";
		    	fieldNames[38] = "regMgrName";
		    	fieldNames[39] = "regPhoneNo";
		    	fieldNames[40] = "regFaxNo";		    	
		    	fieldNames[41] = "divName";
		    	fieldNames[42] = "divMgrName";
		    	fieldNames[43] = "divPhoneNo";
		    	fieldNames[44] = "divFaxNo";
		    	fieldNames[45] = "storeComments";
		    	
		    	
		    	String []fileFields  = mapFileField(fieldNames, columnCount); 
		    	
				
		    	super.headerPresent = true;
			    logger.info("Processing Store Records ...");
		    	super.parseDelimitedFile(StoreDTO.class, files, '|',fileFields, stopCount);

			    PristineDBUtil.commitTransaction(conn, "Store Data Setup");
			}
		}catch (GeneralException ex) {
	        logger.error("GeneralException", ex);
	        commit = false;
		} catch (Exception ex) {
	        logger.error("JavaException", ex);
	        commit = false;
		}
    
	/*	
	if( commit ){
		PrestoUtil.moveFiles(fileList, getRootPath() + "/" + Constants.COMPLETED_FOLDER);
	}
	else{
		PrestoUtil.moveFiles(fileList, getRootPath() + "/" + Constants.BAD_FOLDER);
	}*/

	    logger.info("Record Count " + recordCount);
	    logger.info("No of Processed records = " + processedCount);
	    logger.info("No of Processed NEW = " + newCount);
	    logger.info("No of Processed updated = " + updateCount);
	    logger.info("Skipped Count = " + skippedCount);
	    logger.info("Error Count " + errorCount);

		logger.info("Store Setup completed");
	
		super.performCloseOperation(conn, commit);
		return;
	}
	
	private String[] mapFileField(String[] fieldNames, int columnCount2) {
		// TODO Auto-generated method stub
		int fieldLength = columnCount2 < fieldNames.length? columnCount2 :fieldNames.length;
		String [] fileFields = new String[fieldLength];
		for ( int i = 0; i < fieldLength; i++)
			fileFields[i] = fieldNames[i];
		return fileFields;
	}

	public void processRecords(List listobj) throws GeneralException {
		// TODO Auto-generated method stub
	
		int zoneUpdate = 0;
		int resultUpdate = 0;
		if(!LOAD_COMPETITOR_STORES){
			zoneUpdate = strGrpdao.updatePriceZoneActiveIndicator(conn);
		}
		List<StoreDTO> storeDataList = (List<StoreDTO>) listobj; 
		for (int j = 0; j < storeDataList.size(); j++) {
			StoreDTO storeData = storeDataList.get(j);
			
			//skip the record the chain name does not match.
			if (chainFilter != null){
				if( !storeData.chainName.equalsIgnoreCase(chainFilter))
					continue;
					
			}
			//skip the record
			if( storeData.chainId == 0){
				if( defaultChainID > 0 )
					storeData.chainId = defaultChainID; 
				else
					continue;
			}
						 
			recordCount++;

			//setup Store Name
			storeData.strName = storeData.chainName + "-"+ storeData.city;
			
			//hardcode Timezone
			storeData.timeZone = "EST";

			//Setup Zone
			storeData.zoneId = strGrpdao.getZoneId(conn, storeData.zoneNum, storeData.zoneName);
			if( storeData.zoneId < 0)
			{
				storeData.zoneId = strGrpdao.insertPriceZone(conn, storeData.zoneNum, storeData.zoneName, null);
			}
			else
			{
				resultUpdate = strGrpdao.updatePriceZone(conn, storeData.zoneNum, storeData.zoneName, null, storeData.zoneId);
			}
			
			//setup Division, Region and District
			//To avoid setup during competitor store file process
			if(!LOAD_COMPETITOR_STORES){
				setupLocationHierarchy(storeData) ;
			}
			
			//Modified on 28th Dec 2012 to handle store without comp_str_no (Ahold)
			//Ignore if both gln and comp_str_no is not available
			if ((storeData.strNum == null && storeData.gblLocNum == null)
					|| (storeData.strNum == "" && storeData.gblLocNum == ""))
				continue;
			
			storeData.strId = strdao.getStoreID(conn, storeData.strNum, storeData.gblLocNum, storeData.chainId);
			
			//Added on 17th September 2014, to handle store open/close date with dummy time e.g. 10/28/1996 0:00:00
			if(storeData.storeOpenDate != null && storeData.storeOpenDate != ""){
				if(storeData.storeOpenDate.indexOf(" ") > -1){
					storeData.storeOpenDate = storeData.storeOpenDate.substring(0, storeData.storeOpenDate.indexOf(" "));
				}
			}
			
			if(storeData.storeCloseDate != null && storeData.storeCloseDate != ""){
				if(storeData.storeCloseDate.indexOf(" ") > -1){
					storeData.storeCloseDate = storeData.storeCloseDate.substring(0, storeData.storeCloseDate.indexOf(" "));
				}
			}
			
			if( storeData.strId < 0){
				// setup store			
				strdao.insertStore(conn, storeData);
				newCount++;
			}else{
				//update Store
				strdao.updateStore(conn, storeData);
				updateCount++;
			}
			
			processedCount++;
			
			if( processedCount%10 == 0)
			    PristineDBUtil.commitTransaction(conn, "Store Data Setup");
			if( recordCount%500 == 0)
			    logger.info("No of records processed = " + recordCount);
			
		}
		
	}

	private void setupLocationHierarchy(StoreDTO storeData) throws GeneralException {
		
		//Division
		LocationDTO location = new LocationDTO ();
		location.name = storeData.divName;
		location.manager = storeData.divMgrName;
		location.phoneNo= storeData.divPhoneNo;
		location.parentId = -1;
		
		//get Division id
		storeData.divId = strGrpdao.getLocationId(conn, "RETAIL_DIVISION", null, location);
		if( storeData.divId < 0){
			storeData.divId = strGrpdao.insertLocation(conn, "RETAIL_DIVISION", null, location);
		}else{
			location.locationId = storeData.divId;
			strGrpdao.updateLocation(conn, "RETAIL_DIVISION", null, location);
		}
		
		//Region
		location.name = storeData.regName;
		location.manager = storeData.regMgrName;
		location.phoneNo= storeData.regPhoneNo;
		location.parentId = storeData.divId;
		storeData.regId = strGrpdao.getLocationId(conn, "RETAIL_REGION", "DIVISION_ID", location);
		if( storeData.regId < 0){
			storeData.regId = strGrpdao.insertLocation(conn, "RETAIL_REGION", "DIVISION_ID", location);
		}else {
			location.locationId = storeData.regId;
			strGrpdao.updateLocation(conn, "RETAIL_REGION", "DIVISION_ID", location);
		}
		
		//District
		location.name = storeData.distName;
		location.manager = storeData.distMgrName;
		location.phoneNo= storeData.distPhoneNo;
		location.parentId = storeData.regId;
		storeData.distId = strGrpdao.getLocationId(conn, "RETAIL_DISTRICT", "REGION_ID", location);
		 
		if( storeData.distId < 0){
			storeData.distId = strGrpdao.insertLocation(conn, "RETAIL_DISTRICT", "REGION_ID", location);
		}else {
			location.locationId = storeData.distId;
			strGrpdao.updateLocation(conn, "RETAIL_DISTRICT", "REGION_ID", location);
		}
		
	}

}
