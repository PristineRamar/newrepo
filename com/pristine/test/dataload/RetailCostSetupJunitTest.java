package com.pristine.test.dataload;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.pristine.dao.ItemDAO;
import com.pristine.dataload.prestoload.RetailCostSetup;
import com.pristine.dataload.prestoload.RetailPriceSetup;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.RetailCostDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class RetailCostSetupJunitTest extends PristineFileParser {
	private static Logger logger = Logger.getLogger("RetailCostSetupJunitTest");

	HashMap<ItemDetailKey, List<RetailCostDTO>> costMap = new HashMap<>();
	private HashMap<ItemDetailKey, String> itemCodeMap = new HashMap<>();
	private HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap = null;
	private HashMap<Integer, Integer> itemCodeCategoryMap = null;
	HashMap<String, String> primaryMatchingZone = new HashMap<>();
	ItemDAO itemDAO = new ItemDAO();
	RetailCostSetup retailCostSetup = null;
	Connection conn;

	/**
	 * Normal scenrio. All zone level records
	 */

	@Before
	public void init() {
		//PropertyConfigurator.configure("log4j-retail-price-setup.properties");
		PropertyManager.initialize("analysis.properties");
		retailCostSetup = new RetailCostSetup();
	}

	/**
	 * Case 1: One DSD zone maps to One warehouse zone
	 */
	
	@Test
	public void testCase1() {
		try {

			logger.info("******************************************************************");
			
			logger.info("testCase1() - Running test case 1...Cost_TestCase1_One_DSD_with_One_Whse.txt");
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-106-5-404741", "GE-106-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			
			processFile("Cost_TestCase1_One_DSD_with_One_Whse.txt");

			retailCostSetup.rollupDSDZonesToWhseZone(costMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			RetailCostDTO expectedCostDTO = getExpectedRecord();

			costMap.forEach((key, values) -> {
				values.stream().filter(price -> price.isWhseZoneRolledUpRecord()).forEach(retailCostDTO -> {
					assertEquals("Expected Warehouse zone not found", expectedCostDTO.getLevelId(),
							retailCostDTO.getLevelId());
				});
			});
			
			logger.info("testCase1() - Running test case 1 is completed");
			
			logger.info("******************************************************************");
		} catch (GeneralException | Exception e) {

			logger.error("Error testing case 1", e);

		}
	}

	/**
	 * Multiple DSD zones maps to one warehouse zone
	 */
	@Test
	public void testCase2() {
		try {

			logger.info("******************************************************************");

			logger.info("testCase2() - Running test case 2...Cost_TestCase2_Multi_DSD_with_OneWhse.txt");
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-106-5-404741", "GE-106-1");
			zoneMap.put("GE-106-5-33659", "GE-106-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			processFile("Cost_TestCase2_Multi_DSD_with_OneWhse.txt");

			retailCostSetup.rollupDSDZonesToWhseZone(costMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			RetailCostDTO expectedCostDTO = getExpectedRecord();

			costMap.forEach((key, values) -> {
				values.stream().filter(cost -> cost.isWhseZoneRolledUpRecord()).forEach(retailCostDTO -> {
					assertEquals("Expected Warehouse zone not found", expectedCostDTO.getLevelId(),
							retailCostDTO.getLevelId());
				});
			});
			
			
			logger.info("testCase2() - Running test case 2 is completed");
			
			logger.info("******************************************************************");
			
		} catch (GeneralException | Exception e) {

			logger.error("Error testing case 2", e);

		}
	}

	/**
	 * Multiple DSD zones maps to multiple warehouse zones
	 */
	@Test
	public void testCase3() {
		try {

			logger.info("******************************************************************");

			logger.info("testCase3() - Running test case 3...Cost_TestCase3_Multi_DSD_with_Multi_Whse.txt");
			
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-106-5-404741", "GE-106-1");
			zoneMap.put("GE-1-5-192272", "GE-1-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			
			
			processFile("Cost_TestCase3_Multi_DSD_with_Multi_Whse.txt");

			retailCostSetup.rollupDSDZonesToWhseZone(costMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			costMap.forEach((key, values) -> {
				Set<String> mappedWhseZones = new HashSet<>();
				values.stream().forEach(retailPriceDTO -> {
					RetailCostDTO expectedCostDTO;
					try {
						expectedCostDTO = getExpectedRecord(retailPriceDTO);
						mappedWhseZones.add(expectedCostDTO.getLevelId());
					} catch (Exception e) {
						e.printStackTrace();
					}
				});

				values.stream().filter(cost -> cost.isWhseZoneRolledUpRecord()).forEach(retailCostDTO -> {
					assertEquals("Expected Warehouse zone not found",true,
							mappedWhseZones.contains(retailCostDTO.getLevelId()));
				});
			});
			
			logger.info("testCase3() - Running test case 3 is completed");
			
			logger.info("******************************************************************");
			
		} catch (GeneralException | Exception e) {

			logger.error("Error testing case 3", e);

		}
	}

	
	/**
	 * No DSD zones. Only warehouse zones. No artificial record in output
	 */
	@Test
	public void testCase4() {
		try {

			logger.info("******************************************************************");

			logger.info("testCase4() - Running test case 4...Cost_TestCase4_No_DSD.txt");
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-106-5-404741", "GE-106-1");
			zoneMap.put("GE-1-5-192272", "GE-1-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			
			processFile("Cost_TestCase4_No_DSD.txt");

			retailCostSetup.rollupDSDZonesToWhseZone(costMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			costMap.forEach((key, values) -> {
				boolean noRollupRecord = true;
				for (RetailCostDTO retailCostDTO : values) {
					if (retailCostDTO.isWhseZoneRolledUpRecord()) {
						noRollupRecord = false;
					}
				}

				assertEquals("Warehouse rolled up record found. DSD rollup is failed.", true, noRollupRecord);

			});
			
			logger.info("testCase4() - Running test case 4 is completed");
			
			logger.info("******************************************************************");
		} catch (GeneralException | Exception e) {

			logger.error("Error testing case 4", e);

		}
	}
	
	/**
	 * DSD zone with one warehous zone parent and parent has another parent 
	 */
	@Test
	public void testCase5() {
		try {

			logger.info("******************************************************************");

			logger.info("testCase5() - Running test case 5...Cost_TestCase6_Whse_zone_with_parent.txt");
			
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-1-5-192261", "GE-106-1");
			zoneMap.put("GE-1-5-12345", "GE-106-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			processFile("Cost_TestCase6_Whse_zone_with_parent.txt");

			retailCostSetup.rollupDSDZonesToWhseZone(costMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			RetailCostDTO expectedCostDTO = getExpectedRecord();

			
			expectedCostDTO.setListCost(3.89f);
			
			costMap.forEach((key, values) -> {
				values.stream().filter(cost -> cost.isWhseZoneRolledUpRecord()).forEach(retailCostDTO -> {
					assertEquals("Expected Parent Not found", new Double(expectedCostDTO.getListCost()),
							new Double(retailCostDTO.getListCost()));
				});
			});
			
			logger.info("testCase6() - Running test case 6 is completed");
			
			logger.info("******************************************************************");
		} catch (GeneralException | Exception e) {

			logger.error("Error testing case 6", e);

		}
	}
	
	
	/**
	 * Multiple DSD maps to one warehouse. Max cost case
	 */
	@Test
	public void testCase6() {
		try {

			logger.info("******************************************************************");

			logger.info("testCase6() - Running test case 6...Cost_TestCase6_Multi_DSD_Same_Whse.txt");
			
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-1-5-192261", "GE-106-1");
			//zoneMap.put("GE-24-1", "GE-106-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			processFile("Cost_TestCase5_Whse_zone_with_parent.txt");

			retailCostSetup.rollupDSDZonesToWhseZone(costMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			RetailCostDTO expectedCostDTO = getExpectedRecord();

			costMap.forEach((key, values) -> {
				values.stream().filter(cost -> cost.isWhseZoneRolledUpRecord()).forEach(retailCostDTO -> {
					assertEquals("Expected Parent Not found", expectedCostDTO.getLevelId(),
							retailCostDTO.getLevelId());
				});
			});
			
			logger.info("testCase5() - Running test case 5 is completed");
			
			logger.info("******************************************************************");
		} catch (GeneralException | Exception e) {

			logger.error("Error testing case 1", e);

		}
	}

	private RetailCostDTO getExpectedRecord() throws CloneNotSupportedException {
		RetailCostDTO expected = new RetailCostDTO();
		for (Map.Entry<ItemDetailKey, List<RetailCostDTO>> costEntry : costMap.entrySet()) {
			String itemCode = itemCodeMap.get(costEntry.getKey());
			if (itemCodeCategoryMap.containsKey(Integer.parseInt(itemCode))) {
				int productId = itemCodeCategoryMap.get(Integer.parseInt(itemCode));
				if (dsdAndWhseZoneMap.containsKey(productId)) {
					HashMap<String, String> zoneMap = dsdAndWhseZoneMap.get(productId);
					for (RetailCostDTO retailCostDTO : costEntry.getValue()) {
						if (retailCostDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
							if (zoneMap.containsKey(retailCostDTO.getLevelId())) {
								// Get warehouse zones
								String whseZone = zoneMap.get(retailCostDTO.getLevelId());
								RetailCostDTO retailCostDTO2 = (RetailCostDTO) retailCostDTO.clone();
								retailCostDTO2.setLevelId(whseZone);
								retailCostDTO2.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);
								expected = retailCostDTO2;
							}
						}
					}
				}
			}
		}

		return expected;
	}

	private RetailCostDTO getExpectedRecord(RetailCostDTO retailCostDTO) throws CloneNotSupportedException {
		RetailCostDTO expected = new RetailCostDTO();
		if (itemCodeCategoryMap.containsKey(Integer.parseInt(retailCostDTO.getItemcode()))) {
			int productId = itemCodeCategoryMap.get(Integer.parseInt(retailCostDTO.getItemcode()));
			if (dsdAndWhseZoneMap.containsKey(productId)) {
				HashMap<String, String> zoneMap = dsdAndWhseZoneMap.get(productId);
				if (zoneMap.containsKey(retailCostDTO.getLevelId())) {
					// Get warehouse zones
					String whseZone = zoneMap.get(retailCostDTO.getLevelId());
					RetailCostDTO retailCostDTO2 = (RetailCostDTO) retailCostDTO.clone();
					retailCostDTO2.setLevelId(whseZone);
					retailCostDTO2.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);
					expected = retailCostDTO2;
				}
			}
		}
		return expected;
	}

	private void processFile(String testFileName) throws Exception, GeneralException {
		RetailPriceSetup retailPriceSetup = new RetailPriceSetup();
		String fileName = copyInputFiles(testFileName);
		int columnCount = retailPriceSetup.getColumnCount(fileName, '|');
		
		
		String fieldNames[]  = retailCostSetup.initializeFieldMap(fileName, '|');

		String[] fileFields = RetailPriceSetup.mapFileField(
				fieldNames, columnCount);
		

		logger.info("Processing Retail Price Records ...");
		// To get total stores were available for each UPC
		super.parseDelimitedFile(RetailCostDTO.class, fileName, '|', fileFields, -1);
	}

	private String copyInputFiles(String fileName) throws IOException {

		String filePath = "com/pristine/test/InputFiles/" + fileName;
		// logger.info("Path: "+this.getClass().getClassLoader());
		ClassLoader loader = this.getClass().getClassLoader();
		loader.getResource(filePath);
		String currentDirectory;
		File file = new File(filePath);
		currentDirectory = file.getAbsolutePath().replace("\\", "/");
		logger.info("Current working directory : " + currentDirectory);
		/*
		 * String outputPath = rootPath+"/"+relativeInputPath+"/"+fileName;
		 * FileUtils.copyFile(new File(currentDirectory),new File(outputPath));
		 */

		return currentDirectory;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<RetailCostDTO> retailCostDataList = (List<RetailCostDTO>) listobj;
		for (RetailCostDTO retailCostDTO : retailCostDataList) {
			retailCostDTO.setUpc(PrestoUtil.castUPC(retailCostDTO.getUpc(), false));
			ItemDetailKey itemDetailKey = new ItemDetailKey(retailCostDTO.getUpc(),
					retailCostDTO.getRetailerItemCode());
			itemCodeMap.put(itemDetailKey, "99999");
			retailCostDTO.setItemcode("99999");
			if (costMap.containsKey(itemDetailKey)) {
				List<RetailCostDTO> tempList = costMap.get(itemDetailKey);
				tempList.add(retailCostDTO);
				costMap.put(itemDetailKey, tempList);
			} else {
				List<RetailCostDTO> tempList = new ArrayList<>();
				tempList.add(retailCostDTO);
				costMap.put(itemDetailKey, tempList);
			}
		}
	}
}
