package com.pristine.test.dataload.gianteagle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Test;
import com.pristine.dto.PriceGroupAndCategoryKey;
import com.pristine.dto.offermgmt.ProductLocationMappingDTO;
import com.pristine.dto.offermgmt.ProductLocationMappingKey;
import com.pristine.exception.GeneralException;
import com.pristine.service.pricezoneloader.PriceZoneSetupService;

public class PriceZoneLoaderJunitTest {
	PriceZoneSetupService priceZoneSetupService = new PriceZoneSetupService();

	
	/**
	 * One ware-house zone, one dsd supplies only to that zone (Exp: DSD parent will be this ware-house)
	 * Example: GE-1-5-12345 maps to GE-1-1
	 */
	@Test
	public void testCase1() {

		HashMap<Integer, Integer> primaryMatchingZone = new HashMap<>();
		primaryMatchingZone.put(210, 2909);
		
		HashMap<PriceGroupAndCategoryKey, Integer> prcCodeCountMap = new HashMap<>();

		// Input formation

		List<ProductLocationMappingDTO> productLocationList = new ArrayList<>();
		// Warehouse zone
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));

		// DSD zone
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 4, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 5, "5"));

		PriceGroupAndCategoryKey priceGroupAndCategoryKey = PriceZoneLoaderTestHelper.getPriceGroupAndCatKey("1", 1234);
		prcCodeCountMap.put(priceGroupAndCategoryKey, 400);

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMap = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocationList);

		// Expectation setup
		List<ProductLocationMappingDTO> productLocListExpected = new ArrayList<>();
		// Warehouse zone
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));

		// DSD zone
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 4, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 5, "5", 6, 2909));

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapExpected = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocListExpected);

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapOutput = priceZoneSetupService
				.identifyDSDAndWhseZoneMapping(productLocMap, prcCodeCountMap, primaryMatchingZone);

		for (Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> outputEntry : productLocMapOutput.entrySet()) {
			List<ProductLocationMappingDTO> expectedList = productLocMapExpected.get(outputEntry.getKey());

			assertEquals("Not Matching", expectedList.size(), outputEntry.getValue().size());

			for(ProductLocationMappingDTO productLocationMappingExpected: expectedList){
				for(ProductLocationMappingDTO productLocationMappingOuput: outputEntry.getValue()){
					if(productLocationMappingExpected.storeId == productLocationMappingOuput.storeId){
						assertTrue(EqualsBuilder.reflectionEquals(productLocationMappingExpected, productLocationMappingOuput));
					}
				}
			}
		}
	}
	
	/**
	 * One ware-house zone, multiple dsd's suppling only to these zones (Exp: All DSD parent will be this ware-house)
	 * Example: GE-1-5-12345 and GE-1-5-6789 maps to GE-1-1
	 */
	@Test
	public void testCase2() {

		HashMap<Integer, Integer> primaryMatchingZone = new HashMap<>();
		primaryMatchingZone.put(210, 2909);

		HashMap<PriceGroupAndCategoryKey, Integer> prcCodeCountMap = new HashMap<>();

		// Input formation

		List<ProductLocationMappingDTO> productLocationList = new ArrayList<>();
		// Warehouse zone
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));

		// DSD zone 1
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5"));
		
		
		// DSD zone 2
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 4, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 5, "5"));

		PriceGroupAndCategoryKey priceGroupAndCategoryKey = PriceZoneLoaderTestHelper.getPriceGroupAndCatKey("1", 1234);
		prcCodeCountMap.put(priceGroupAndCategoryKey, 400);

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMap = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocationList);

		// Expectation setup
		List<ProductLocationMappingDTO> productLocListExpected = new ArrayList<>();
		// Warehouse zone
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));

		// DSD zone 1
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5", 6, 2909));

		// DSD zone 2
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 4, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 5, "5", 6, 2909));

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapExpected = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocListExpected);

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapOutput = priceZoneSetupService
				.identifyDSDAndWhseZoneMapping(productLocMap, prcCodeCountMap, primaryMatchingZone);

		for (Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> outputEntry : productLocMapOutput.entrySet()) {
			List<ProductLocationMappingDTO> expectedList = productLocMapExpected.get(outputEntry.getKey());

			assertEquals("Not Matching", expectedList.size(), outputEntry.getValue().size());

			for(ProductLocationMappingDTO productLocationMappingExpected: expectedList){
				for(ProductLocationMappingDTO productLocationMappingOuput: outputEntry.getValue()){
					if(productLocationMappingExpected.storeId == productLocationMappingOuput.storeId){
						assertTrue(EqualsBuilder.reflectionEquals(productLocationMappingExpected, productLocationMappingOuput));
					}
				}
			}

		}
	}
	
	/**
	 * One ware-house zone, one dsd supplies to this ware-house and another ware-house, this ware-house has larger number of items
	 * (Exp: DSD parent will be this ware-house) 
	 * Example: GE-1-5-12345 maps to GE-1-1 and GE-1-3 but GE-1-1 has max items.
	 * So, GE-1-1 is the parent 
	 *
	 */
	
	@Test
	public void testCase3() {

		HashMap<Integer, Integer> primaryMatchingZone = new HashMap<>();
		primaryMatchingZone.put(210, 2909);

		HashMap<PriceGroupAndCategoryKey, Integer> prcCodeCountMap = new HashMap<>();

		// Input formation

		List<ProductLocationMappingDTO> productLocationList = new ArrayList<>();
		// Warehouse zone
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));

		
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 1, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 2, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 3, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 4, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 5, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 6, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 7, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 8, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 9, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 10, "3"));

		
		// DSD zone 1
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 4, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 5, "5"));

		PriceGroupAndCategoryKey priceGroupAndCategoryKey = PriceZoneLoaderTestHelper.getPriceGroupAndCatKey("1", 1234);
		prcCodeCountMap.put(priceGroupAndCategoryKey, 400);

		priceGroupAndCategoryKey = PriceZoneLoaderTestHelper.getPriceGroupAndCatKey("3", 1234);
		prcCodeCountMap.put(priceGroupAndCategoryKey, 100);
		
		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMap = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocationList);

		// Expectation setup
		List<ProductLocationMappingDTO> productLocListExpected = new ArrayList<>();
		// Warehouse zone
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));

		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 1, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 2, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 3, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 4, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 5, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 6, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 7, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 8, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 9, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 10, "3"));
		
		
		// DSD zone 1
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 4, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 5, "5", 6, 2909));

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapExpected = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocListExpected);

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapOutput = priceZoneSetupService
				.identifyDSDAndWhseZoneMapping(productLocMap, prcCodeCountMap, primaryMatchingZone);

		for (Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> outputEntry : productLocMapOutput.entrySet()) {
			List<ProductLocationMappingDTO> expectedList = productLocMapExpected.get(outputEntry.getKey());

			assertEquals("Not Matching", expectedList.size(), outputEntry.getValue().size());

			for(ProductLocationMappingDTO productLocationMappingExpected: expectedList){
				for(ProductLocationMappingDTO productLocationMappingOuput: outputEntry.getValue()){
					if(productLocationMappingExpected.storeId == productLocationMappingOuput.storeId){
						assertTrue(EqualsBuilder.reflectionEquals(productLocationMappingExpected, productLocationMappingOuput));
					}
				}
			}

		}
	}
	
	
	/**
	 * One ware-house zone, one dsd supplies to this ware-house and another ware-house, another ware-house has 
	 * larger number of items(Exp: DSD parent will be another ware-house) 
	 * Example: GE-1-5-12345 maps to GE-1-1 and GE-1-3 but GE-1-3 has max items.
	 * So, GE-1-3 is the parent 
	 *
	 */
	@Test
	public void testCase4() {

		HashMap<Integer, Integer> primaryMatchingZone = new HashMap<>();
		primaryMatchingZone.put(210, 2909);

		HashMap<PriceGroupAndCategoryKey, Integer> prcCodeCountMap = new HashMap<>();

		// Input formation

		List<ProductLocationMappingDTO> productLocationList = new ArrayList<>();
		// Warehouse zone
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));

		
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 1, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 2, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 3, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 4, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 5, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 6, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 7, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 8, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 9, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 10, "3"));

		
		// DSD zone 1
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 4, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 5, "5"));

		PriceGroupAndCategoryKey priceGroupAndCategoryKey = PriceZoneLoaderTestHelper.getPriceGroupAndCatKey("1", 1234);
		prcCodeCountMap.put(priceGroupAndCategoryKey, 100);

		priceGroupAndCategoryKey = PriceZoneLoaderTestHelper.getPriceGroupAndCatKey("3", 1234);
		prcCodeCountMap.put(priceGroupAndCategoryKey, 400);
		
		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMap = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocationList);

		// Expectation setup
		List<ProductLocationMappingDTO> productLocListExpected = new ArrayList<>();
		// Warehouse zone
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));

		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 1, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 2, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 3, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 4, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 5, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 6, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 7, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 8, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 9, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 10, "3"));
		
		
		// DSD zone 1
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5", 6, 2910));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5", 6, 2910));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5", 6, 2910));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 4, "5", 6, 2910));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 5, "5", 6, 2910));

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapExpected = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocListExpected);

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapOutput = priceZoneSetupService
				.identifyDSDAndWhseZoneMapping(productLocMap, prcCodeCountMap, primaryMatchingZone);

		for (Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> outputEntry : productLocMapOutput.entrySet()) {
			List<ProductLocationMappingDTO> expectedList = productLocMapExpected.get(outputEntry.getKey());

			assertEquals("Not Matching", expectedList.size(), outputEntry.getValue().size());

			for(ProductLocationMappingDTO productLocationMappingExpected: expectedList){
				for(ProductLocationMappingDTO productLocationMappingOuput: outputEntry.getValue()){
					if(productLocationMappingExpected.storeId == productLocationMappingOuput.storeId){
						assertTrue(EqualsBuilder.reflectionEquals(productLocationMappingExpected, productLocationMappingOuput));
					}
				}
			}

		}
	}
	
	/**
	 * One ware-house zone, multiple dsd's suppling to multiple ware-house zones. (all DSD will have parent) 
	 * Example: GE-1-5-12345
	 * supplies to GE-1-1 and GE-106-1 as well. In this case stores are spanning multiple warehouse zones.
	 * All the DSDs will not have parent location information
	 */
	@Test
	public void testCase5() {

		HashMap<Integer, Integer> primaryMatchingZone = new HashMap<>();
		primaryMatchingZone.put(210, 2909);

		HashMap<PriceGroupAndCategoryKey, Integer> prcCodeCountMap = new HashMap<>();

		// Input formation

		List<ProductLocationMappingDTO> productLocationList = new ArrayList<>();
		// Warehouse zone
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));

		
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 1, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 2, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 3, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 4, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 5, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 6, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 7, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 8, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 9, "3"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 10, "3"));

		
		// DSD zone 1
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5"));
		// Same DSD supplies to some other warehouse zone as well
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 123, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 235, "5"));

		PriceGroupAndCategoryKey priceGroupAndCategoryKey = PriceZoneLoaderTestHelper.getPriceGroupAndCatKey("1", 1234);
		prcCodeCountMap.put(priceGroupAndCategoryKey, 100);

		priceGroupAndCategoryKey = PriceZoneLoaderTestHelper.getPriceGroupAndCatKey("3", 1234);
		prcCodeCountMap.put(priceGroupAndCategoryKey, 400);
		
		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMap = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocationList);

		// Expectation setup
		List<ProductLocationMappingDTO> productLocListExpected = new ArrayList<>();
		// Warehouse zone
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));

		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 1, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 2, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 3, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 4, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 5, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 6, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 7, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 8, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 9, "3"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2910, 10, "3"));
		
		
		// DSD zone 1
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5", 0, 0));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5", 0, 0));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5", 0, 0));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 123, "5", 0, 0));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 235, "5", 0, 0));

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapExpected = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocListExpected);

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapOutput = priceZoneSetupService
				.identifyDSDAndWhseZoneMapping(productLocMap, prcCodeCountMap, primaryMatchingZone);

		for (Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> outputEntry : productLocMapOutput.entrySet()) {
			List<ProductLocationMappingDTO> expectedList = productLocMapExpected.get(outputEntry.getKey());

			assertEquals("Not Matching", expectedList.size(), outputEntry.getValue().size());

			for(ProductLocationMappingDTO productLocationMappingExpected: expectedList){
				for(ProductLocationMappingDTO productLocationMappingOuput: outputEntry.getValue()){
					if(productLocationMappingExpected.storeId == productLocationMappingOuput.storeId){
						assertTrue(EqualsBuilder.reflectionEquals(productLocationMappingExpected, productLocationMappingOuput));
					}
				}
			}

		}
	}
	
	/**
	 * Combining zones together.
	 * Example: GE-24-1 and GE-26-1 should be combined to GE-106-1
	 * Parent location should populated for GE-24-1 and GE-26-1 as GE-106-1 
	 * @throws CloneNotSupportedException
	 * @throws GeneralException
	 */
	@Test
	public void testCase6() throws CloneNotSupportedException, GeneralException {

		HashMap<Integer, Integer> primaryMatchingZone = new HashMap<>();
		primaryMatchingZone.put(210, 2909);

		// Input formation

		List<ProductLocationMappingDTO> productLocationList = new ArrayList<>();
		// Warehouse zone
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));
		
		//Warehouse zone 2
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 222, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 111, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 333, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 123, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 454, "1"));

			
		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMap = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocationList);

		// Expectation setup
		List<ProductLocationMappingDTO> productLocListExpected = new ArrayList<>();
		// Warehouse zone
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 1, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 2, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 3, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 4, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 5, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 6, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 7, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 8, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 9, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 10, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 222, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 111, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 333, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 123, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 2909, 454, "1"));

		
		
		//Warehouse zone 2
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 222, "1", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 111, "1", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 333, "1", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 123, "1", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 454, "1", 6, 2909));

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapExpected = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocListExpected);

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapOutput = priceZoneSetupService
				.groupZonesByMapping(productLocMap, primaryMatchingZone);

		for (Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> outputEntry : productLocMapOutput.entrySet()) {
			List<ProductLocationMappingDTO> expectedList = productLocMapExpected.get(outputEntry.getKey());

			assertEquals("Not Matching", expectedList.size(), outputEntry.getValue().size());

			for(ProductLocationMappingDTO productLocationMappingExpected: expectedList){
				for(ProductLocationMappingDTO productLocationMappingOuput: outputEntry.getValue()){
					if(productLocationMappingExpected.storeId == productLocationMappingOuput.storeId){
						assertTrue(EqualsBuilder.reflectionEquals(productLocationMappingExpected, productLocationMappingOuput));
					}
				}
			}
		}
	}
	
	
	/**
	 * One ware-house zone, one dsd supplies only to that zone but that zone has a parent warehouse zone
	 * Example: GE-1-5-12345 maps to GE-24-1. GE-106-1 is the parent of GE-24-1 
	 */
	@Test
	public void testCase7() {

		HashMap<Integer, Integer> primaryMatchingZone = new HashMap<>();
		primaryMatchingZone.put(210, 2909);
		
		HashMap<PriceGroupAndCategoryKey, Integer> prcCodeCountMap = new HashMap<>();

		// Input formation

		List<ProductLocationMappingDTO> productLocationList = new ArrayList<>();
		// Warehouse zone
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 1, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 2, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 3, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 4, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 5, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 6, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 7, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 8, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 9, "1"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 10, "1"));

		// DSD zone
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 4, "5"));
		productLocationList.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 5, "5"));

		PriceGroupAndCategoryKey priceGroupAndCategoryKey = PriceZoneLoaderTestHelper.getPriceGroupAndCatKey("1", 1234);
		prcCodeCountMap.put(priceGroupAndCategoryKey, 400);

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMap = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocationList);

		// Expectation setup
		List<ProductLocationMappingDTO> productLocListExpected = new ArrayList<>();
		// Warehouse zone
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 1, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 2, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 3, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 4, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 5, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 6, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 7, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 8, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 9, "1"));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 210, 10, "1"));

		// DSD zone
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 1, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 2, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 3, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 4, "5", 6, 2909));
		productLocListExpected.add(PriceZoneLoaderTestHelper.getProductLocationMapDTO(4, 1234, 6, 209, 5, "5", 6, 2909));

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapExpected = PriceZoneLoaderTestHelper
				.getProductLocationMap(productLocListExpected);

		HashMap<ProductLocationMappingKey, List<ProductLocationMappingDTO>> productLocMapOutput = priceZoneSetupService
				.identifyDSDAndWhseZoneMapping(productLocMap, prcCodeCountMap, primaryMatchingZone);

		for (Map.Entry<ProductLocationMappingKey, List<ProductLocationMappingDTO>> outputEntry : productLocMapOutput.entrySet()) {
			List<ProductLocationMappingDTO> expectedList = productLocMapExpected.get(outputEntry.getKey());

			assertEquals("Not Matching", expectedList.size(), outputEntry.getValue().size());

			for(ProductLocationMappingDTO productLocationMappingExpected: expectedList){
				for(ProductLocationMappingDTO productLocationMappingOuput: outputEntry.getValue()){
					if(productLocationMappingExpected.storeId == productLocationMappingOuput.storeId){
						assertTrue(EqualsBuilder.reflectionEquals(productLocationMappingExpected, productLocationMappingOuput));
					}
				}
			}
		}
	}
}
