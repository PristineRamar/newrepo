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
import com.pristine.dataload.prestoload.RetailPriceSetup;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class RetailPriceSetupJunitTest extends PristineFileParser {
	private static Logger logger = Logger.getLogger("RetailPriceSetupJunitTest");

	HashMap<String, List<RetailPriceDTO>> priceMap = new HashMap<>();
	private HashMap<String, String> itemCodeMap = new HashMap<>();
	private HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap = null;
	private HashMap<Integer, Integer> itemCodeCategoryMap = null;
	HashMap<String, String> primaryMatchingZone = new HashMap<>();
	ItemDAO itemDAO = new ItemDAO();
	RetailPriceSetup retailPriceSetup = null;
	Connection conn;

	/**
	 * Normal scenrio. All zone level records
	 */

	@Before
	public void init() {
		//PropertyConfigurator.configure("log4j-retail-price-setup.properties");
		PropertyManager.initialize("analysis.properties");
		retailPriceSetup = new RetailPriceSetup();
	}

	/**
	 * Case 1: One DSD zone maps to One warehouse zone
	 */
	@Test
	public void testCase1() {
		try {

			logger.info("******************************************************************");
			
			logger.info("testCase1() - Running test case 1...Price_TestCase1_One_DSD_with_One_Whse.txt");
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-106-5-404741", "GE-106-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			processFile("Price_TestCase1_One_DSD_with_One_Whse.txt");

			retailPriceSetup.rollupDSDZonesToWhseZone(priceMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			RetailPriceDTO expectedPriceDTO = getExpectedRecord();

			priceMap.forEach((key, values) -> {
				values.stream().filter(price -> price.isWhseZoneRolledUpRecord()).forEach(retailPriceDTO -> {
					assertEquals("Expected Warehouse zone not found", expectedPriceDTO.getLevelId(),
							retailPriceDTO.getLevelId());
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

			logger.info("testCase2() - Running test case 2...Price_TestCase2_Multi_DSD_with_OneWhse.txt");
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-106-5-404741", "GE-106-1");
			zoneMap.put("GE-106-5-33659", "GE-106-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			
			processFile("Price_TestCase2_Multi_DSD_with_OneWhse.txt");

			retailPriceSetup.rollupDSDZonesToWhseZone(priceMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			RetailPriceDTO expectedPriceDTO = getExpectedRecord();

			priceMap.forEach((key, values) -> {
				values.stream().filter(price -> price.isWhseZoneRolledUpRecord()).forEach(retailPriceDTO -> {
					assertEquals("Expected Warehouse zone not found", expectedPriceDTO.getLevelId(),
							retailPriceDTO.getLevelId());
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

			logger.info("testCase3() - Running test case 3...Price_TestCase3_Multi_DSD_with_Multi_Whse.txt");
			
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-73-5-32912", "GE-106-1");
			zoneMap.put("GE-71-5-32912", "GE-106-1");
			zoneMap.put("GE-1-5-32912", "GE-1-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			
			processFile("Price_TestCase3_Multi_DSD_with_Multi_Whse.txt");

			retailPriceSetup.rollupDSDZonesToWhseZone(priceMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			priceMap.forEach((key, values) -> {
				Set<String> mappedWhseZones = new HashSet<>();
				values.stream().forEach(retailPriceDTO -> {
					RetailPriceDTO expectedPriceDTO;
					try {
						expectedPriceDTO = getExpectedRecord(retailPriceDTO);
						mappedWhseZones.add(expectedPriceDTO.getLevelId());
					} catch (Exception e) {
						e.printStackTrace();
					}
				});

				values.stream().filter(price -> price.isWhseZoneRolledUpRecord()).forEach(retailPriceDTO -> {
					assertEquals("Expected Warehouse zone not found",true,
							mappedWhseZones.contains(retailPriceDTO.getLevelId()));
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

			logger.info("testCase4() - Running test case 4...Price_TestCase4_No_DSD.txt");
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-73-5-32912", "GE-106-1");
			zoneMap.put("GE-71-5-32912", "GE-106-1");
			zoneMap.put("GE-1-5-32912", "GE-1-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			
			processFile("Price_TestCase4_No_DSD.txt");

			retailPriceSetup.rollupDSDZonesToWhseZone(priceMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			priceMap.forEach((key, values) -> {
				boolean noRollupRecord = true;
				for (RetailPriceDTO retailPriceDTO : values) {
					if (retailPriceDTO.isWhseZoneRolledUpRecord()) {
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

			logger.info("testCase5() - Running test case 5...Price_TestCase5_Whse_zone_with_parent.txt");
			
			itemCodeCategoryMap = new HashMap<>();

			itemCodeCategoryMap.put(99999, 222);
			
			dsdAndWhseZoneMap = new HashMap<>();
			
			HashMap<String, String> zoneMap = new HashMap<>();
			
			zoneMap.put("GE-1-5-192261", "GE-106-1");
			//zoneMap.put("GE-24-1", "GE-106-1");
			
			dsdAndWhseZoneMap.put(222, zoneMap);
			
			
			processFile("Price_TestCase5_Whse_zone_with_parent.txt");

			retailPriceSetup.rollupDSDZonesToWhseZone(priceMap, itemCodeMap, itemCodeCategoryMap, dsdAndWhseZoneMap);

			RetailPriceDTO expectedPriceDTO = getExpectedRecord();

			priceMap.forEach((key, values) -> {
				values.stream().filter(price -> price.isWhseZoneRolledUpRecord()).forEach(retailPriceDTO -> {
					assertEquals("Expected Parent Not found", expectedPriceDTO.getLevelId(),
							retailPriceDTO.getLevelId());
				});
			});
			
			logger.info("testCase5() - Running test case 5 is completed");
			
			logger.info("******************************************************************");
		} catch (GeneralException | Exception e) {

			logger.error("Error testing case 1", e);

		}
	}

	private RetailPriceDTO getExpectedRecord() throws CloneNotSupportedException {
		RetailPriceDTO expected = new RetailPriceDTO();
		for (Map.Entry<String, List<RetailPriceDTO>> priceEntry : priceMap.entrySet()) {
			String itemCode = itemCodeMap.get(priceEntry.getKey());
			if (itemCodeCategoryMap.containsKey(Integer.parseInt(itemCode))) {
				int productId = itemCodeCategoryMap.get(Integer.parseInt(itemCode));
				if (dsdAndWhseZoneMap.containsKey(productId)) {
					HashMap<String, String> zoneMap = dsdAndWhseZoneMap.get(productId);
					for (RetailPriceDTO retailPriceDTO : priceEntry.getValue()) {
						if (retailPriceDTO.getLevelTypeId() == Constants.ZONE_LEVEL_TYPE_ID) {
							if (zoneMap.containsKey(retailPriceDTO.getLevelId())) {
								// Get warehouse zones
								String whseZone = zoneMap.get(retailPriceDTO.getLevelId());
								if (zoneMap.containsKey(whseZone)) {
									whseZone = zoneMap.get(whseZone);
								}
								RetailPriceDTO retailPriceDTO2 = (RetailPriceDTO) retailPriceDTO.clone();
								retailPriceDTO2.setLevelId(whseZone);
								retailPriceDTO2.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);
								expected = retailPriceDTO2;
							}
						}
					}
				}
			}
		}

		return expected;
	}

	private RetailPriceDTO getExpectedRecord(RetailPriceDTO retailPriceDTO) throws CloneNotSupportedException {
		RetailPriceDTO expected = new RetailPriceDTO();
		if (itemCodeCategoryMap.containsKey(Integer.parseInt(retailPriceDTO.getItemcode()))) {
			int productId = itemCodeCategoryMap.get(Integer.parseInt(retailPriceDTO.getItemcode()));
			if (dsdAndWhseZoneMap.containsKey(productId)) {
				HashMap<String, String> zoneMap = dsdAndWhseZoneMap.get(productId);
				if (zoneMap.containsKey(retailPriceDTO.getLevelId())) {
					// Get warehouse zones
					String whseZone = zoneMap.get(retailPriceDTO.getLevelId());
					if (zoneMap.containsKey(whseZone)) {
						whseZone = zoneMap.get(whseZone);
					}
					RetailPriceDTO retailPriceDTO2 = (RetailPriceDTO) retailPriceDTO.clone();
					retailPriceDTO2.setLevelId(whseZone);
					retailPriceDTO2.setLevelTypeId(Constants.ZONE_LEVEL_TYPE_ID);
					expected = retailPriceDTO2;
				}
			}
		}
		return expected;
	}

	private void processFile(String testFileName) throws Exception, GeneralException {
		RetailPriceSetup retailPriceSetup = new RetailPriceSetup();
		String fileName = copyInputFiles(testFileName);
		int columnCount = retailPriceSetup.getColumnCount(fileName, '|');
		String fieldNames[] = new String[columnCount];

		fieldNames[0] = "upc";
		fieldNames[1] = "retailerItemCode";
		fieldNames[2] = "levelTypeId";
		fieldNames[3] = "levelId";
		fieldNames[4] = "regEffectiveDate";
		fieldNames[5] = "regPrice";
		fieldNames[6] = "regQty";
		fieldNames[7] = "saleStartDate";
		fieldNames[8] = "saleEndDate";
		fieldNames[9] = "salePrice";
		fieldNames[10] = "saleQty";

		// Check if there are additional columns
		if (columnCount > 11) {
			fieldNames[11] = "prcGrpCode";
		}

		String[] fileFields = RetailPriceSetup.mapFileField(fieldNames, columnCount);

		logger.info("Processing Retail Price Records ...");
		// To get total stores were available for each UPC
		super.parseDelimitedFile(RetailPriceDTO.class, fileName, '|', fileFields, -1);
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
		List<RetailPriceDTO> retailPriceDataList = (List<RetailPriceDTO>) listobj;
		for (RetailPriceDTO retailPriceDTO : retailPriceDataList) {
			retailPriceDTO.setUpc(PrestoUtil.castUPC(retailPriceDTO.getUpc(), false));
		}

		for (RetailPriceDTO retailPriceDTO : retailPriceDataList) {
			String priceMapKey = retailPriceDTO.getUpc() + "-" + retailPriceDTO.getRetailerItemCode();
			
			itemCodeMap.put(priceMapKey, "99999");
			retailPriceDTO.setItemcode("99999");
			if (priceMap.containsKey(priceMapKey)) {
				List<RetailPriceDTO> tempList = priceMap.get(priceMapKey);
				tempList.add(retailPriceDTO);
				priceMap.put(priceMapKey, tempList);
			} else {
				List<RetailPriceDTO> tempList = new ArrayList<>();
				tempList.add(retailPriceDTO);
				priceMap.put(priceMapKey, tempList);
			}
		}
	}
}
