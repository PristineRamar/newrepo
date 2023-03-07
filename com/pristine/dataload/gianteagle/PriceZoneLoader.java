package com.pristine.dataload.gianteagle;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAOV2;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.ProductLocationMappingDAO;
import com.pristine.dataload.gianteagle.PriceZoneLoader;
import com.pristine.dto.PriceGroupAndCategoryKey;
import com.pristine.dto.offermgmt.ProductLocationMappingDTO;
import com.pristine.dto.offermgmt.ProductLocationMappingKey;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.service.pricezoneloader.PriceZoneSetupService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

@SuppressWarnings("rawtypes")
public class PriceZoneLoader  extends PristineFileParser{

	static Logger logger = Logger.getLogger("PriceZoneLoader");
	int prdLocMappingInsertCnt, prdLocMappingUpdateCnt, prdLocMappingDeleteCnt, prdStrInsertCnt, ignoredCount, updateFailedCount;
	Set<String> distinctIgnoredCatCode, distinctIgnoredZoneNo, distinctIgnoredStoreNo;
	private List<ProductLocationMappingDTO> updatedRecordList;
	private HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> finalMap;
	String chainId;
	int stopCount = -1;
	Connection conn = null;
	RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
	//private HashMap<String, Integer> categoryCodeIdMap;
	private HashMap<String, Integer> strIdNumMap;
	private HashMap<String, Integer> priceZoneMap;
	ProductLocationMappingDAO productLocationMappingDAO = new ProductLocationMappingDAO();
	CompetitiveDataDAOV2 competitiveDataDAOV2 = new CompetitiveDataDAOV2(conn);
	int defaultProdId;
	public PriceZoneLoader() {
		super("analysis.properties");
		try {
			conn = DBManager.getConnection();

		} catch (GeneralException ex) {

		}
	}
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-PriceZoneLoader.properties");
		PropertyManager.initialize("analysis.properties");
		String subFolder = null;

		logger.info("main() - Started");

		for (int ii = 0; ii < args.length; ii++) {
			String arg = args[ii];
			if (arg.startsWith("SUB_FOLDER")) {
				subFolder = arg.substring("SUB_FOLDER=".length());
			}
		}

		PriceZoneLoader aholdPriceZoneMappingImport = new PriceZoneLoader();
		aholdPriceZoneMappingImport.processPriceZoneMappingFile(subFolder);
	}
	@Override
	public void processRecords(List listobj) throws GeneralException {
		PRCommonUtil prCommonUtil = new PRCommonUtil();
		for (int jj = 0; jj < listobj.size(); jj++) {
			ProductLocationMappingDTO priceZoneMappingDTO = (ProductLocationMappingDTO) listobj.get(jj);
			String storeNum =prCommonUtil.integerAsString(priceZoneMappingDTO.getStore());
			//priceZoneMappingDTO.setStore(storeNum);
			if (strIdNumMap.containsKey(storeNum)
					&& priceZoneMap.containsKey(priceZoneMappingDTO.getPriceZone())) {
				
				priceZoneMappingDTO.setProductId(defaultProdId);
				priceZoneMappingDTO.setLocationId(priceZoneMap.get(priceZoneMappingDTO.getPriceZone()));
				priceZoneMappingDTO.setStoreId(strIdNumMap.get(storeNum));
				priceZoneMappingDTO.setLocationLevelId(Constants.ZONE_LEVEL_ID);
				priceZoneMappingDTO.setProductLevelId(Constants.ALLPRODUCTS);
				updatedRecordList.add(priceZoneMappingDTO);
			} else {
				ignoredCount++;
				/*if (!categoryCodeIdMap.containsKey(priceZoneMappingDTO.getCategory()))
					distinctIgnoredCatCode.add(priceZoneMappingDTO.getCategory());*/
				if (!strIdNumMap.containsKey(storeNum));
					distinctIgnoredStoreNo.add(storeNum);
				if (!priceZoneMap.containsKey(priceZoneMappingDTO.getPriceZone()))
					distinctIgnoredZoneNo.add(priceZoneMappingDTO.getPriceZone());
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	void processPriceZoneMappingFile(String subFolder) {
		try {
			chainId = retailPriceDAO.getChainId(conn);
			//categoryCodeIdMap = productLocationMappingDAO.getCategoryCodeCategoryId(conn);
			strIdNumMap = productLocationMappingDAO.getStoreIds(conn, chainId);
			priceZoneMap = productLocationMappingDAO.getPriceZoneId(conn);
			defaultProdId = Integer.parseInt(PropertyManager.
					getProperty("DEFAULT_PRODUCT_ID_IN_PROD_LOC_MAPPING", "0"));
			ArrayList<String> fileList = getFiles(subFolder);
			String fieldNames[] = new String[5];
			setHeaderPresent(true);
			fieldNames[0] = "category";
			fieldNames[1] = "primary Competitor";
			fieldNames[2] = "price Zone";
			fieldNames[3] = "store";
			fieldNames[4] = "prcGrpCode";
			logger.info("Price Zone Mapping starts");

			for (int j = 0; j < fileList.size(); j++) {
				clearVariables();
				logger.info("processing - " + fileList.get(j));
				parseDelimitedFile(ProductLocationMappingDTO.class, fileList.get(j), '|', fieldNames, stopCount);
				logger.info("Total Lines Ignored: " + ignoredCount);
				
				attachMappingForAllProducts();
				
				loadRecords(updatedRecordList);
				
				PristineDBUtil.commitTransaction(conn, "batch record update");
			}
			logger.info("Price Zone Mapping ends");
		} catch (GeneralException ge) {
			ge.printStackTrace();
			logger.error(ge.toString(), ge);
			logger.error("Error while processing Price Zone Mapping File - ", ge);
			PristineDBUtil.rollbackTransaction(conn, "Unexpected Exception");
		} catch (Exception e) {
			logger.error("Error while processing Price Zone Mapping File - ", e);
		} finally {
			PristineDBUtil.close(getOracleConnection());
		}
	}
	
	/**
	 * 
	 * @throws GeneralException
	 */
	private void attachMappingForAllProducts() throws GeneralException{
		
		List<ProductLocationMappingDTO> categoriesToBeSetup = productLocationMappingDAO.getCategoriesByPrcGrpCode(conn);
		List<ProductLocationMappingDTO> toBeAdded = new ArrayList<>();
		
		for(ProductLocationMappingDTO categoryDTO: categoriesToBeSetup){
			for(ProductLocationMappingDTO productLocationMappingDTO: updatedRecordList){
				if(productLocationMappingDTO.prcGrpCode.equals(categoryDTO.prcGrpCode)){
					ProductLocationMappingDTO productLocationMappingDTO2 = new ProductLocationMappingDTO();
					productLocationMappingDTO2.setProductLevelId(categoryDTO.getProductLevelId());
					productLocationMappingDTO2.setProductId(categoryDTO.getProductId());
					productLocationMappingDTO2.setLocationLevelId(productLocationMappingDTO.locationLevelId);
					productLocationMappingDTO2.setLocationId(productLocationMappingDTO.locationId);
					productLocationMappingDTO2.setStoreId(productLocationMappingDTO.storeId);
					productLocationMappingDTO2.setPriceZone(productLocationMappingDTO.getPriceZone());
					productLocationMappingDTO2.prcGrpCode = productLocationMappingDTO.prcGrpCode; 
					toBeAdded.add(productLocationMappingDTO2);
				}
			}
		}
		
		updatedRecordList.addAll(toBeAdded);
		
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
	
	private void loadRecords(List<ProductLocationMappingDTO> updatedRecordList) throws GeneralException, CloneNotSupportedException {
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

		PriceZoneSetupService priceZoneSetupService = new PriceZoneSetupService();
		ItemDAO itemDAO = new ItemDAO();
		logger.info("identifyDSDAndWhseZoneMapping() - Retrieving count of items for prc grp codes...");
		
		HashMap<PriceGroupAndCategoryKey, Integer> prcCodeCountMap = itemDAO.getItemCountForPrcGrp(conn);
		
		logger.info("identifyDSDAndWhseZoneMapping() - Retrieving count of items for prc grp codes is completed.");
		
		
		HashMap<Integer, Integer> primaryMatchingZone = productLocationMappingDAO.getPrimaryMatchingZoneId(conn);
		
		priceZoneSetupService.identifyDSDAndWhseZoneMapping(finalMap, prcCodeCountMap, primaryMatchingZone);
		
		priceZoneSetupService.groupZonesByMapping(finalMap, primaryMatchingZone);
		
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
		
		logger.info("Deleting of PR_PRODUCT_LOCATION_STORE is Started");
		productLocationMappingDAO.truncateProductLocationStore(conn);
		logger.info("Deleting of PR_PRODUCT_LOCATION_STORE is Completed");

		logger.info("Inserting into PR_PRODUCT_LOCATION_STORE is Started");
		prdStrInsertCnt = productLocationMappingDAO.populateProductLocationStore(conn, finalMap);
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
}
