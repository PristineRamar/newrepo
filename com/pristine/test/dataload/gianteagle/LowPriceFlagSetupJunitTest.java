package com.pristine.test.dataload.gianteagle;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.fileformatter.gianteagle.GiantEaglePriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.LowPriceFlagSetupService;
import com.pristine.test.dataload.TestHelper;
import com.pristine.util.PrestoUtil;

public class LowPriceFlagSetupJunitTest {

	
	LowPriceFlagSetupService lowPriceFlagSetupService = new LowPriceFlagSetupService();

	String upc = PrestoUtil.castUPC("000111112222", false);
	String RITEM_NO1 = "12345";
	String itemCode = "99999";
	
	
	HashMap<Integer, HashMap<String, String>> dsdAndWhseZoneMap = new HashMap<>();
 	HashMap<Integer, Integer> itemCodeCategoryMap = new HashMap<>();
	
	/**
	 * Low price flag with Low price end date
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void testCase1() throws GeneralException, Exception{
		
		Set<String> skippedRetailerItemcodes = new HashSet<>();
		
		List<GiantEaglePriceDTO> priceList = new ArrayList<>();
		GiantEaglePriceDTO giantEaglePriceDTO = TestHelper.getGEPriceDTO(RITEM_NO1, "Y", "", "08/15/2018", "", "GE", "106","1", "");
		
		priceList.add(giantEaglePriceDTO);
		
		HashMap<ItemDetailKey, String> itemCodeMap = new HashMap<>();
		ItemDetailKey itemDetailKey = new ItemDetailKey(upc, RITEM_NO1);
		itemCodeMap.put(itemDetailKey, itemCode);
		
		
		HashMap<String, List<String>> upcMap = new HashMap<>();
		List<String> upcs = new ArrayList<>();
		upcs.add(upc);
		
		upcMap.put(RITEM_NO1, upcs);
		
		HashMap<String, Integer> zoneIdMap = new HashMap<>();
		zoneIdMap.put("GE-106-1", 999);
		
		List<GiantEaglePriceDTO> outList = lowPriceFlagSetupService.prepareItemsWithLowPriceFlags(priceList,
				itemCodeMap, zoneIdMap, skippedRetailerItemcodes, upcMap, 2303, dsdAndWhseZoneMap, itemCodeCategoryMap, "08/15/2018");
		
		
		String exptectedLowPriceEndDate = "08/15/2018";
		int expectedLocationId = 999;
		int expectedItemCode = 99999;
	
		GiantEaglePriceDTO giantEaglePriceDTOOuput = outList.get(0);
		
		assertEquals("Low price flag is not matching", "Y", giantEaglePriceDTOOuput.getLOW_PRC_FG());
		assertEquals("Low price end date is not matching", exptectedLowPriceEndDate, giantEaglePriceDTOOuput.getLOW_PRC_END_DTE());
		assertEquals("Location id is not matching", expectedLocationId, giantEaglePriceDTOOuput.getLocationId());
		assertEquals("Item code is not matching", expectedItemCode, giantEaglePriceDTOOuput.getItemCode());
		
	}
	
	
	
	/**
	 * New Low price flag with New Low price end date
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void testCase2() throws GeneralException, Exception{
		
		Set<String> skippedRetailerItemcodes = new HashSet<>();
		
		List<GiantEaglePriceDTO> priceList = new ArrayList<>();
		GiantEaglePriceDTO giantEaglePriceDTO = TestHelper.getGEPriceDTO(RITEM_NO1, "", "Y", "", "08/15/2018", "GE", "106","1", "");
		
		priceList.add(giantEaglePriceDTO);
		
		HashMap<ItemDetailKey, String> itemCodeMap = new HashMap<>();
		ItemDetailKey itemDetailKey = new ItemDetailKey(upc, RITEM_NO1);
		itemCodeMap.put(itemDetailKey, itemCode);
		
		
		HashMap<String, List<String>> upcMap = new HashMap<>();
		List<String> upcs = new ArrayList<>();
		upcs.add(upc);
		
		upcMap.put(RITEM_NO1, upcs);
		
		HashMap<String, Integer> zoneIdMap = new HashMap<>();
		zoneIdMap.put("GE-106-1", 999);
		
		List<GiantEaglePriceDTO> outList = lowPriceFlagSetupService.prepareItemsWithLowPriceFlags(priceList,
				itemCodeMap, zoneIdMap, skippedRetailerItemcodes, upcMap, 2303, dsdAndWhseZoneMap, itemCodeCategoryMap, "08/15/2018");
		
		
		String exptectedNewLowPriceEndDate = "08/15/2018";
		int expectedLocationId = 999;
		int expectedItemCode = 99999;
	
		GiantEaglePriceDTO giantEaglePriceDTOOuput = outList.get(0);
		
		assertEquals("Low price flag is not matching", "Y", giantEaglePriceDTOOuput.getNEW_LOW_PRC_FG());
		assertEquals("Low price end date is not matching", exptectedNewLowPriceEndDate, giantEaglePriceDTOOuput.getNEW_LOW_PRC_END_DTE());
		assertEquals("Location id is not matching", expectedLocationId, giantEaglePriceDTOOuput.getLocationId());
		assertEquals("Item code is not matching", expectedItemCode, giantEaglePriceDTOOuput.getItemCode());
		
	}
	
	/**
	 * New Low price flag with No end date
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void testCase3() throws GeneralException, Exception{
		
		Set<String> skippedRetailerItemcodes = new HashSet<>();
		
		List<GiantEaglePriceDTO> priceList = new ArrayList<>();
		GiantEaglePriceDTO giantEaglePriceDTO = TestHelper.getGEPriceDTO(RITEM_NO1, "", "Y", "", "", "GE", "106","1", "");
		
		priceList.add(giantEaglePriceDTO);
		
		HashMap<ItemDetailKey, String> itemCodeMap = new HashMap<>();
		ItemDetailKey itemDetailKey = new ItemDetailKey(upc, RITEM_NO1);
		itemCodeMap.put(itemDetailKey, itemCode);
		
		
		HashMap<String, List<String>> upcMap = new HashMap<>();
		List<String> upcs = new ArrayList<>();
		upcs.add(upc);
		
		upcMap.put(RITEM_NO1, upcs);
		
		HashMap<String, Integer> zoneIdMap = new HashMap<>();
		zoneIdMap.put("GE-106-1", 999);
		
		List<GiantEaglePriceDTO> outList = lowPriceFlagSetupService.prepareItemsWithLowPriceFlags(priceList,
				itemCodeMap, zoneIdMap, skippedRetailerItemcodes, upcMap, 2303, dsdAndWhseZoneMap, itemCodeCategoryMap, "08/15/2018");
		
		
		String exptectedNewLowPriceEndDate = "";
		int expectedLocationId = 999;
		int expectedItemCode = 99999;
	
		GiantEaglePriceDTO giantEaglePriceDTOOuput = outList.get(0);
		
		assertEquals("Low price flag is not matching", "Y", giantEaglePriceDTOOuput.getNEW_LOW_PRC_FG());
		assertEquals("Low price end date is not matching", exptectedNewLowPriceEndDate, giantEaglePriceDTOOuput.getNEW_LOW_PRC_END_DTE());
		assertEquals("Location id is not matching", expectedLocationId, giantEaglePriceDTOOuput.getLocationId());
		assertEquals("Item code is not matching", expectedItemCode, giantEaglePriceDTOOuput.getItemCode());
		
	}
	
	/**
	 * Low price flag with No end date
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void testCase4() throws GeneralException, Exception{
		
		Set<String> skippedRetailerItemcodes = new HashSet<>();
		
		List<GiantEaglePriceDTO> priceList = new ArrayList<>();
		GiantEaglePriceDTO giantEaglePriceDTO = TestHelper.getGEPriceDTO(RITEM_NO1, "Y", "", "", "", "GE", "106","1", "");
		
		priceList.add(giantEaglePriceDTO);
		
		HashMap<ItemDetailKey, String> itemCodeMap = new HashMap<>();
		ItemDetailKey itemDetailKey = new ItemDetailKey(upc, RITEM_NO1);
		itemCodeMap.put(itemDetailKey, itemCode);
		
		
		HashMap<String, List<String>> upcMap = new HashMap<>();
		List<String> upcs = new ArrayList<>();
		upcs.add(upc);
		
		upcMap.put(RITEM_NO1, upcs);
		
		HashMap<String, Integer> zoneIdMap = new HashMap<>();
		zoneIdMap.put("GE-106-1", 999);
		
		List<GiantEaglePriceDTO> outList = lowPriceFlagSetupService.prepareItemsWithLowPriceFlags(priceList,
				itemCodeMap, zoneIdMap, skippedRetailerItemcodes, upcMap, 2303, dsdAndWhseZoneMap, itemCodeCategoryMap, "08/15/2018");
		
		
		String exptectedLowPriceEndDate = "";
		int expectedLocationId = 999;
		int expectedItemCode = 99999;
	
		GiantEaglePriceDTO giantEaglePriceDTOOuput = outList.get(0);
		
		assertEquals("Low price flag is not matching", "Y", giantEaglePriceDTOOuput.getLOW_PRC_FG());
		assertEquals("Low price end date is not matching", exptectedLowPriceEndDate, giantEaglePriceDTOOuput.getLOW_PRC_END_DTE());
		assertEquals("Location id is not matching", expectedLocationId, giantEaglePriceDTOOuput.getLocationId());
		assertEquals("Item code is not matching", expectedItemCode, giantEaglePriceDTOOuput.getItemCode());
		
	}
	
	
	
	/**
	 * Both New Low price flag and Low price flag with No end date
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void testCase5() throws GeneralException, Exception{
		
		Set<String> skippedRetailerItemcodes = new HashSet<>();
		
		List<GiantEaglePriceDTO> priceList = new ArrayList<>();
		GiantEaglePriceDTO giantEaglePriceDTO = TestHelper.getGEPriceDTO(RITEM_NO1, "Y", "Y", "", "", "GE", "106","1", "");
		
		priceList.add(giantEaglePriceDTO);
		
		HashMap<ItemDetailKey, String> itemCodeMap = new HashMap<>();
		ItemDetailKey itemDetailKey = new ItemDetailKey(upc, RITEM_NO1);
		itemCodeMap.put(itemDetailKey, itemCode);
		
		
		HashMap<String, List<String>> upcMap = new HashMap<>();
		List<String> upcs = new ArrayList<>();
		upcs.add(upc);
		
		upcMap.put(RITEM_NO1, upcs);
		
		HashMap<String, Integer> zoneIdMap = new HashMap<>();
		zoneIdMap.put("GE-106-1", 999);
		
		List<GiantEaglePriceDTO> outList = lowPriceFlagSetupService.prepareItemsWithLowPriceFlags(priceList,
				itemCodeMap, zoneIdMap, skippedRetailerItemcodes, upcMap, 2303, dsdAndWhseZoneMap, itemCodeCategoryMap, "08/15/2018");
		
		
		String exptectedLowPriceEndDate = "";
		String exptectedNewLowPriceEndDate = "";
		int expectedLocationId = 999;
		int expectedItemCode = 99999;
	
		GiantEaglePriceDTO giantEaglePriceDTOOuput = outList.get(0);
		
		assertEquals("Low price flag is not matching", "Y", giantEaglePriceDTOOuput.getLOW_PRC_FG());
		assertEquals("Low price flag is not matching", "Y", giantEaglePriceDTOOuput.getNEW_LOW_PRC_FG());
		assertEquals("Low price end date is not matching", exptectedLowPriceEndDate, giantEaglePriceDTOOuput.getLOW_PRC_END_DTE());
		assertEquals("Low price end date is not matching", exptectedNewLowPriceEndDate, giantEaglePriceDTOOuput.getNEW_LOW_PRC_END_DTE());
		assertEquals("Location id is not matching", expectedLocationId, giantEaglePriceDTOOuput.getLocationId());
		assertEquals("Item code is not matching", expectedItemCode, giantEaglePriceDTOOuput.getItemCode());
		
	}
	
	
	/**
	 * Both New Low price flag and Low price flag with end date
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void testCase6() throws GeneralException, Exception{
		
		Set<String> skippedRetailerItemcodes = new HashSet<>();
		
		List<GiantEaglePriceDTO> priceList = new ArrayList<>();
		GiantEaglePriceDTO giantEaglePriceDTO = TestHelper.getGEPriceDTO(RITEM_NO1, "Y", "Y", "09/10/2018", "10/15/2018", "GE", "106","1", "");
		
		priceList.add(giantEaglePriceDTO);
		
		HashMap<ItemDetailKey, String> itemCodeMap = new HashMap<>();
		ItemDetailKey itemDetailKey = new ItemDetailKey(upc, RITEM_NO1);
		itemCodeMap.put(itemDetailKey, itemCode);
		
		
		HashMap<String, List<String>> upcMap = new HashMap<>();
		List<String> upcs = new ArrayList<>();
		upcs.add(upc);
		
		upcMap.put(RITEM_NO1, upcs);
		
		HashMap<String, Integer> zoneIdMap = new HashMap<>();
		zoneIdMap.put("GE-106-1", 999);
		
		List<GiantEaglePriceDTO> outList = lowPriceFlagSetupService.prepareItemsWithLowPriceFlags(priceList,
				itemCodeMap, zoneIdMap, skippedRetailerItemcodes, upcMap, 2303, dsdAndWhseZoneMap, itemCodeCategoryMap, "08/15/2018");
		
		
		String exptectedLowPriceEndDate = "09/10/2018";
		String exptectedNewLowPriceEndDate = "10/15/2018";
		int expectedLocationId = 999;
		int expectedItemCode = 99999;
	
		GiantEaglePriceDTO giantEaglePriceDTOOuput = outList.get(0);
		
		assertEquals("Low price flag is not matching", "Y", giantEaglePriceDTOOuput.getLOW_PRC_FG());
		assertEquals("Low price flag is not matching", "Y", giantEaglePriceDTOOuput.getNEW_LOW_PRC_FG());
		assertEquals("Low price end date is not matching", exptectedLowPriceEndDate, giantEaglePriceDTOOuput.getLOW_PRC_END_DTE());
		assertEquals("Low price end date is not matching", exptectedNewLowPriceEndDate, giantEaglePriceDTOOuput.getNEW_LOW_PRC_END_DTE());
		assertEquals("Location id is not matching", expectedLocationId, giantEaglePriceDTOOuput.getLocationId());
		assertEquals("Item code is not matching", expectedItemCode, giantEaglePriceDTOOuput.getItemCode());
		
	}
	
	
	/**
	 * Both New Low price flag and Low price flag with end date
	 * @throws GeneralException
	 * @throws Exception
	 */
	@Test
	public void testCase7() throws GeneralException, Exception{
		
		Set<String> skippedRetailerItemcodes = new HashSet<>();
		
		List<GiantEaglePriceDTO> priceList = new ArrayList<>();
		GiantEaglePriceDTO giantEaglePriceDTO = TestHelper.getGEPriceDTO(RITEM_NO1, "Y", "Y", "07/10/2018", "07/15/2018", "GE", "106","1", "");
		
		priceList.add(giantEaglePriceDTO);
		
		HashMap<ItemDetailKey, String> itemCodeMap = new HashMap<>();
		ItemDetailKey itemDetailKey = new ItemDetailKey(upc, RITEM_NO1);
		itemCodeMap.put(itemDetailKey, itemCode);
		
		
		HashMap<String, List<String>> upcMap = new HashMap<>();
		List<String> upcs = new ArrayList<>();
		upcs.add(upc);
		
		upcMap.put(RITEM_NO1, upcs);
		
		HashMap<String, Integer> zoneIdMap = new HashMap<>();
		zoneIdMap.put("GE-106-1", 999);
		
		List<GiantEaglePriceDTO> outList = lowPriceFlagSetupService.prepareItemsWithLowPriceFlags(priceList,
				itemCodeMap, zoneIdMap, skippedRetailerItemcodes, upcMap, 2303, dsdAndWhseZoneMap, itemCodeCategoryMap, "08/15/2018");
		
		
		String exptectedLowPriceEndDate = "";
		String exptectedNewLowPriceEndDate = "";
		int expectedLocationId = 999;
		int expectedItemCode = 99999;
	
		GiantEaglePriceDTO giantEaglePriceDTOOuput = outList.get(0);
		
		assertEquals("Low price flag is not matching", "N", giantEaglePriceDTOOuput.getLOW_PRC_FG());
		assertEquals("Low price flag is not matching", "N", giantEaglePriceDTOOuput.getNEW_LOW_PRC_FG());
		assertEquals("Low price end date is not matching", exptectedLowPriceEndDate, giantEaglePriceDTOOuput.getLOW_PRC_END_DTE());
		assertEquals("Low price end date is not matching", exptectedNewLowPriceEndDate, giantEaglePriceDTOOuput.getNEW_LOW_PRC_END_DTE());
		assertEquals("Location id is not matching", expectedLocationId, giantEaglePriceDTOOuput.getLocationId());
		assertEquals("Item code is not matching", expectedItemCode, giantEaglePriceDTOOuput.getItemCode());
		
	}
	
}
