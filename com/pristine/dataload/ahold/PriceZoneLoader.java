package com.pristine.dataload.ahold;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAOV2;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.offermgmt.ProductLocationMappingDAO;
import com.pristine.dto.offermgmt.ProductLocationMappingDTO;
import com.pristine.dto.offermgmt.ProductLocationMappingKey;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PriceZoneLoader extends PristineFileParser {
	static Logger logger = Logger.getLogger("PriceZoneLoader");
	int prdLocMappingInsertCnt, prdLocMappingUpdateCnt, prdLocMappingDeleteCnt, prdStrInsertCnt, ignoredCount, updateFailedCount;
	Set<String> distinctIgnoredCatCode, distinctIgnoredZoneNo, distinctIgnoredStoreNo;
	private List<ProductLocationMappingDTO> updatedRecordList;
	private HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> finalMap;
//	String chainId;
	int stopCount = -1;
	Connection conn = null;
	RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
	private HashMap<String, List<Integer>> categoryCodeIdMap;
	private HashMap<String, Integer> strIdNumMap;
	private HashMap<String, Integer> priceZoneMap;
	ProductLocationMappingDAO productLocationMappingDAO = new ProductLocationMappingDAO();
	CompetitiveDataDAOV2 competitiveDataDAOV2 = new CompetitiveDataDAOV2(conn);

	public PriceZoneLoader() {
		super("analysis.properties");
		try {
			conn = DBManager.getConnection();

		} catch (GeneralException ex) {

		}
	}

	public static void main(String[] args) throws GeneralException {
		PropertyConfigurator.configure("log4j-PriceZoneLoader.properties");
		String FilePath = PropertyManager.getProperty("DATALOAD.ROOTPATH");
		PropertyManager.initialize("analysis.properties");
		String subFolder = null;

		logger.info("main() - Started");

		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];
			if (arg.startsWith("SUBFOLDER")) {
				subFolder = arg.substring("SUBFOLDER=".length());
			}
		}

		 PriceZoneLoader priceZoneMappingImport = new PriceZoneLoader();
	     priceZoneMappingImport.setPriceZoneMapping(subFolder);
//	     logger.info((Object)"Main Method completed");
	}

	void processPriceZoneMappingFile(String subFolder) {
		try {
//			String chainId = retailPriceDAO.getChainId(conn);
//			categoryCodeIdMap = productLocationMappingDAO.getCategoryCodeCategoryId(conn);
//			strIdNumMap = productLocationMappingDAO.getStoreIds(conn, chainId);
//			priceZoneMap = productLocationMappingDAO.getPriceZoneId(conn);

			ArrayList<String> fileList = getFiles(subFolder);
			String fieldNames[] = new String[4];
			setHeaderPresent(true);
			fieldNames[0] = "category";
			fieldNames[1] = "primary Competitor";
			fieldNames[2] = "price Zone";
			fieldNames[3] = "store";
			logger.info("Price Zone Mapping starts");

			for (int j = 0; j < fileList.size(); j++) {
				clearVariables();
				logger.info("processing - " + fileList.get(j));
				parseDelimitedFile(ProductLocationMappingDTO.class, fileList.get(j), '|', fieldNames, stopCount);
				logger.info("Total Lines Ignored: " + ignoredCount);
				loadRecords(updatedRecordList);
				PristineDBUtil.commitTransaction(conn, "batch record update");
			}
			logger.info("Price Zone Mapping ends");
		} catch (GeneralException ge) {
			ge.printStackTrace();
			logger.error(ge.toString(), ge);
			logger.error("Error while processing Price Zone Mapping File - " + ge);
			PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
		} catch (Exception e) {
			logger.error("Error while processing Price Zone Mapping File - " + e);
		} finally {
			PristineDBUtil.close(getOracleConnection());
		}
	}

	private void clearVariables(){
		distinctIgnoredCatCode = new HashSet<String>();
		distinctIgnoredZoneNo = new HashSet<String>();
		distinctIgnoredStoreNo = new HashSet<String>();
		updatedRecordList = new ArrayList<ProductLocationMappingDTO>();
		finalMap = new HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>>();
		ignoredCount = 0;
		prdLocMappingInsertCnt = 0;
		prdLocMappingUpdateCnt = 0;
		prdLocMappingDeleteCnt = 0;
		prdStrInsertCnt = 0;
		updateFailedCount = 0;
	}
	
	public void processRecords(List listobj) throws GeneralException {
		PRCommonUtil prCommonUtil = new PRCommonUtil();
		for (int jj = 0; jj < listobj.size(); jj++) {
			ProductLocationMappingDTO priceZoneMappingDTO = (ProductLocationMappingDTO) listobj.get(jj);
			String storeNum =prCommonUtil.integerAsString(priceZoneMappingDTO.getStore());
			//priceZoneMappingDTO.setStore(storeNum);
			if(!priceZoneMap.containsKey(priceZoneMappingDTO.getPriceZone())){
				logger.info("Price zone not matching"+priceZoneMappingDTO.getPriceZone());
			}
			
			if (!priceZoneMappingDTO.getCategory().equals("0")
					&& categoryCodeIdMap.containsKey(priceZoneMappingDTO.getCategory())
					&& strIdNumMap.containsKey(storeNum)
					&& priceZoneMap.containsKey(priceZoneMappingDTO.getPriceZone())) {
				List<Integer> categoryIdList = categoryCodeIdMap.get(priceZoneMappingDTO.getCategory());
				categoryIdList.forEach(categoryId -> {
					ProductLocationMappingDTO tempProdDTO = new ProductLocationMappingDTO();
					tempProdDTO.setCategory(priceZoneMappingDTO.getCategory());
					tempProdDTO.setPriceZone(priceZoneMappingDTO.getPriceZone());
					tempProdDTO.setProductId(categoryId);
					tempProdDTO.setLocationId(priceZoneMap.get(priceZoneMappingDTO.getPriceZone()));
					tempProdDTO.setStoreId(strIdNumMap.get(storeNum));
					tempProdDTO.setLocationLevelId(Constants.ZONE_LEVEL_ID);
					tempProdDTO.setProductLevelId(Constants.CATEGORYLEVELID);
					updatedRecordList.add(tempProdDTO);
				});
				
			} else {
				ignoredCount++;
				if (!categoryCodeIdMap.containsKey(priceZoneMappingDTO.getCategory()))
					distinctIgnoredCatCode.add(priceZoneMappingDTO.getCategory());
				if (!strIdNumMap.containsKey(storeNum))
					distinctIgnoredStoreNo.add(storeNum);
				if (!priceZoneMap.containsKey(priceZoneMappingDTO.getPriceZone()))
					distinctIgnoredZoneNo.add(priceZoneMappingDTO.getPriceZone());
			}
		}
	}

	private void loadRecords(List<ProductLocationMappingDTO> updatedRecordList) throws GeneralException {
		HashMap<ProductLocationMappingKey, Long> productMappingId = new HashMap<ProductLocationMappingKey, Long>();
		for (ProductLocationMappingDTO priceZoneMapDTO : updatedRecordList) {
			ProductLocationMappingKey productLocationMappingKey = new ProductLocationMappingKey(
					priceZoneMapDTO.productLevelId, priceZoneMapDTO.productId, priceZoneMapDTO.locationLevelId,
					priceZoneMapDTO.locationId);
			List<ProductLocationMappingDTO> tempList;
			if (finalMap.get(productLocationMappingKey) != null) {
				tempList = finalMap.get(productLocationMappingKey);
			} else {
				tempList = new ArrayList<ProductLocationMappingDTO>();
			}
			tempList.add(priceZoneMapDTO);
			finalMap.put(productLocationMappingKey, tempList);
		}

		//update product location mapping id
		logger.info("Getting Product Mapping Id is Started");
		productMappingId = productLocationMappingDAO.getProductMappingId(conn);
		logger.info("Getting Product Mapping Id is Completed");

		//Insert
		logger.info("Inserting into PR_PRODUCT_LOCATION_MAPPING is Started");
		prdLocMappingInsertCnt = productLocationMappingDAO.insertProductLocationMapping(conn, finalMap, productMappingId);
		logger.info("Inserting into PR_PRODUCT_LOCATION_MAPPING is Completed");
		logger.info("No of Records Inserted in PR_PRODUCT_LOCATION_MAPPING : " + prdLocMappingInsertCnt);

		//Update
		logger.info("Updating PR_PRODUCT_LOCATION_MAPPING is Started");
		prdLocMappingUpdateCnt = productLocationMappingDAO.updateProductLocationMapping(conn, finalMap, productMappingId);
		logger.info("Updating PR_PRODUCT_LOCATION_MAPPING is Completed");
		logger.info("No of Records Updated in PR_PRODUCT_LOCATION_MAPPING : " + prdLocMappingUpdateCnt);
		
		//prdLocMappingInsertCnt = productLocationMappingDAO.populateProductLocationMapping(conn, finalMap);
		//logger.info("No of Records Inserted/Updated in PR_PRODUCT_LOCATION_MAPPING : " + prdLocMappingInsertCnt);
		
		logger.info("Truncating of PR_PRODUCT_LOCATION_STORE is Started");
		productLocationMappingDAO.truncateProductLocationStore(conn);
		logger.info("Truncating of PR_PRODUCT_LOCATION_STORE is Completed");

		logger.info("Inserting into PR_PRODUCT_LOCATION_STORE is Started");
		//To remove duplicate store entries which has same product details
		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> uniqueProdAndStoreMap = 
				new HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>>();
		
		for(Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> entry: finalMap.entrySet()){
			List<Integer> storeList= new ArrayList<Integer>();
			List<ProductLocationMappingDTO> productMapping = new ArrayList<ProductLocationMappingDTO>();
			List<ProductLocationMappingDTO> tempList = entry.getValue();
			for(ProductLocationMappingDTO productLocationMappingDTO: tempList){
				if(!storeList.contains(productLocationMappingDTO.getStoreId())){
					storeList.add(productLocationMappingDTO.getStoreId());
					productMapping.add(productLocationMappingDTO);
				}
			}
			uniqueProdAndStoreMap.put(entry.getKey(), productMapping);
		}
		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> finalMap;
		
		prdStrInsertCnt = productLocationMappingDAO.populateProductLocationStore(conn, uniqueProdAndStoreMap);
		logger.info("Inserting into PR_PRODUCT_LOCATION_STORE is Completed");
		logger.info("No of Records Inserted in PR_PRODUCT_LOCATION_STORE : " + prdStrInsertCnt);
		
		logger.info("Deletion of unmatched records from PR_PRODUCT_LOCATION_MAPPING is Started");
		prdLocMappingDeleteCnt = productLocationMappingDAO.deleteUnmatchedRecords(conn);
		logger.info("Deletion of unmatched records from PR_PRODUCT_LOCATION_MAPPING is Completed");
		logger.info("No of Records Delete from PR_PRODUCT_LOCATION_MAPPING : " + prdLocMappingDeleteCnt);
		
		logger.info("Following Categories --- " + PRCommonUtil.getCommaSeperatedStringFromStrSet(distinctIgnoredCatCode) + " are ignored");
		logger.info("Following Zones --- " + PRCommonUtil.getCommaSeperatedStringFromStrSet(distinctIgnoredZoneNo) + " are ignored");
		logger.info("Following Stores --- " + PRCommonUtil.getCommaSeperatedStringFromStrSet(distinctIgnoredStoreNo) + " are ignored");
	}
	
	public void setPriceZoneMapping(String subFolder) {
        try {
        	
        	String chainId = retailPriceDAO.getChainId(conn);
			categoryCodeIdMap = productLocationMappingDAO.getCategoryCodeCategoryId(conn);
			strIdNumMap = productLocationMappingDAO.getStoreIds(conn, chainId);
			priceZoneMap = productLocationMappingDAO.getPriceZoneId(conn);
            
            String dataSource = PropertyManager.getProperty("PRICE_ZONE_LOADER_DATA_SOURCE");
            if (dataSource.toLowerCase().equals("db")) {
            	logger.info("Generating Price zone Mapping file from DB");
                generatePriceZoneMappingFile(subFolder);
            }
            processPriceZoneMappingFile(subFolder);
        }
        catch (GeneralException e) {
            e.printStackTrace();
            logger.error((Object)("Error while processing Price Zone Mapping File - " + e));
        }
    }
	
	public void generatePriceZoneMappingFile(String subFolder) {
        try {
            List<ProductLocationMappingDTO> priceZoneMappingDetailList = new ArrayList<ProductLocationMappingDTO>();
            List<Integer> productIds = new ArrayList<Integer>();
            List<Integer> productId = getCategoryId(categoryCodeIdMap);
            String zoneNumber = getZoneNumber(priceZoneMap);
            for (Integer productIdList : productId) {
                productIds.add(productIdList);
                if (productIds.size() != 1000) continue;
                String productIdValue = PRCommonUtil.getCommaSeperatedStringFromIntArray(productIds);
                productLocationMappingDAO.getPrizeZoneMappingDetail(conn, productIdValue, zoneNumber, priceZoneMappingDetailList);
                productIds.clear();
            }
            if (productIds.size() > 0) {
                String productIdValue = PRCommonUtil.getCommaSeperatedStringFromIntArray(productIds);
                productLocationMappingDAO.getPrizeZoneMappingDetail(conn, productIdValue, zoneNumber, priceZoneMappingDetailList);
                productIds.clear();
            }
            HashMap<String, String> primaryMatchingZone = productLocationMappingDAO.getPrimaryMatchingZoneNum(conn);
            writeIntoFile(priceZoneMappingDetailList,primaryMatchingZone,subFolder);
        }
        catch (GeneralException e) {
            logger.error((Object)("Error while generating Price Zone Mapping File - " + e));
            e.printStackTrace();
        }
    }
	
	private List<Integer> getCategoryId(HashMap<String, List<Integer>> categoryCodeIdMap) {
        ArrayList<Integer> catrgoryIds = new ArrayList<Integer>();
        for (Map.Entry<String, List<Integer>> categoryIdEntry : categoryCodeIdMap.entrySet()) {
        	categoryIdEntry.getValue().forEach(categoryId -> {
        		 catrgoryIds.add(categoryId);
        	});
        }
        return catrgoryIds;
    }

    private String getZoneNumber(HashMap<String, Integer> priceZoneMap) {
        ArrayList<Integer> ZoneNumberList = new ArrayList<Integer>();
        String ZoneNumber = null;
        for (Map.Entry<String, Integer> priceZoneMapEntry : priceZoneMap.entrySet()) {
            ZoneNumberList.add(priceZoneMapEntry.getValue());
        }
        ZoneNumber = PRCommonUtil.getCommaSeperatedStringFromIntArray(ZoneNumberList);
        return ZoneNumber;
    }
	
    
    private void writeIntoFile(List<ProductLocationMappingDTO> prizeZoneMappingDetailList,HashMap<String, String> primaryMatchingZone
    		,String subFolder) {
        try {
            String FilePath = PropertyManager.getProperty("DATALOAD.ROOTPATH");
            SimpleDateFormat dt = new SimpleDateFormat("_yyyy-MM-dd");
            Date date = new Date();
            String d = dt.format(date).toString();
            String fname = "PriceZoneMapping";
            String outputPath = String.valueOf(FilePath)+"/"+subFolder + "/" + fname + d + ".CSV";
            logger.debug("output path is " + outputPath);
            File file = new File(outputPath);
            FileWriter fw = new FileWriter(file);
            PrintWriter pw = new PrintWriter(fw);
            String header = "CATEGORY|Primary Competitor|Price Zone|STORE";
            pw.print(header);
            pw.println();
            for (ProductLocationMappingDTO productLocationMappingDTO : prizeZoneMappingDetailList) {
            	if(primaryMatchingZone.containsKey(productLocationMappingDTO.getPriceZone())){
            		 pw.print(productLocationMappingDTO.getCategory());
                     pw.print("|");
                     pw.print(productLocationMappingDTO.getPrimaryCompetitor());
                     pw.print("|");
                     pw.print(primaryMatchingZone.get(productLocationMappingDTO.getPriceZone()));
                     pw.print("|");
                     pw.print(productLocationMappingDTO.getStore());
                     pw.println("");
            	}
            }
            pw.flush();
            pw.close();
            fw.close();
            logger.debug((Object)"file created.");
        }
        catch (Exception e) {
            logger.error((Object)"Exception while writing formatted prize Zone mapping Details Records", (Throwable)e);
            e.printStackTrace();
        }
    }
}
