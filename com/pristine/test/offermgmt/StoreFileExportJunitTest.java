package com.pristine.test.offermgmt;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;


import com.pristine.dao.offermgmt.StoreFileExportGEDAO;
import com.pristine.dataload.offermgmt.QuarterlyRecomStoreFileExportFF;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.offermgmt.StoreFileExportDTO;


import static org.junit.Assert.assertEquals;
public class StoreFileExportJunitTest {
	
	QuarterlyRecomStoreFileExportFF storeFileEportFF = new QuarterlyRecomStoreFileExportFF();
	Map<Integer,List<StoreDTO>> zoneToStoreMapping;
	
	@Before
	public void initialize() throws SQLException {
		List<StoreDTO> storeList = new ArrayList<StoreDTO>();
		StoreDTO storeDTO = new StoreDTO();
		storeDTO.setPriceZoneId(150);
		storeDTO.setStrId(1);
		storeDTO.setStrNum("1");
		storeList.add(storeDTO);	
		
		StoreDTO storeDTO1= new StoreDTO();
		storeDTO1.setPriceZoneId(150);
		storeDTO1.setStrId(2);
		storeDTO1.setStrNum("2");
		storeList.add(storeDTO1);	
		
		StoreDTO storeDTO2 = new StoreDTO();
		storeDTO2.setPriceZoneId(150);
		storeDTO2.setStrId(3);
		storeDTO2.setStrNum("3");
		storeList.add(storeDTO2);	
		
		StoreDTO storeDTO3 = new StoreDTO();
		storeDTO3.setPriceZoneId(129);
		storeDTO3.setStrId(4);
		storeDTO3.setStrNum("4");
		storeList.add(storeDTO3);	
		storeFileEportFF.zoneStoreMapping = storeList.stream().collect(Collectors.groupingBy(StoreDTO::getPriceZoneId));
		
		RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
		retailPriceDTO.setLocationId(1);
		retailPriceDTO.setSaleStartDate("07/18/2022");
		retailPriceDTO.setSaleEndDate(null);
		retailPriceDTO.setItemcode("100");
		List<RetailPriceDTO> clearancePriceList = new ArrayList<RetailPriceDTO>();
		clearancePriceList .add(retailPriceDTO);

		RetailPriceDTO retailPriceDTO1 = new RetailPriceDTO();
		retailPriceDTO1.setLocationId(2);
		retailPriceDTO1.setSaleStartDate("07/18/2022");
		retailPriceDTO1.setSaleEndDate("07/26/2022");
		retailPriceDTO1.setItemcode("101");
		clearancePriceList.add(retailPriceDTO1);
		
		RetailPriceDTO retailPriceDTO3 = new RetailPriceDTO();
		retailPriceDTO3.setLocationId(2);
		retailPriceDTO3.setSaleStartDate("07/18/2022");
		retailPriceDTO3.setSaleEndDate(null);
		retailPriceDTO3.setItemcode("100");
		clearancePriceList.add(retailPriceDTO3);
		storeFileEportFF.clearancePriceData = clearancePriceList.stream().collect(Collectors.groupingBy(RetailPriceDTO::getLocationId));
		

	}
	
	//Multiple items with same zone
	@Test
	public void testCase1() throws SQLException {
		initialize();
		List<StoreFileExportDTO> zoneLevelFileExportList = new ArrayList<StoreFileExportDTO>();
		StoreFileExportDTO storeFileExportDTO = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(100);
		storeFileExportDTO.setLocationId(150);
		zoneLevelFileExportList.add(storeFileExportDTO);
		
		StoreFileExportDTO storeFileExportDTO1 = new StoreFileExportDTO();
		storeFileExportDTO1.setProductId(101);
		storeFileExportDTO1.setLocationId(150);
		zoneLevelFileExportList.add(storeFileExportDTO1);
		
		List<StoreFileExportDTO> zoneToStoreConvertedList = storeFileEportFF.convertZoneLevelToStoreLvelList(zoneLevelFileExportList);
		assertEquals("Matches", zoneToStoreConvertedList.size(),6);
		
	}
	
	//Multiple items with different item
	@Test
	public void testCase2() throws SQLException {
		initialize();
		List<StoreFileExportDTO> zoneLevelFileExportList = new ArrayList<StoreFileExportDTO>();
		List<Integer> locationIds = new ArrayList<Integer>();
		StoreFileExportDTO storeFileExportDTO = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(100);
		storeFileExportDTO.setLocationId(150);
		locationIds.add(150);
		zoneLevelFileExportList.add(storeFileExportDTO);
		
		StoreFileExportDTO storeFileExportDTO1 = new StoreFileExportDTO();
		storeFileExportDTO1.setProductId(101);
		storeFileExportDTO1.setLocationId(129);
		locationIds.add(129);
		zoneLevelFileExportList.add(storeFileExportDTO1);
		List<StoreFileExportDTO> zoneToStoreConvertedList = storeFileEportFF.convertZoneLevelToStoreLvelList(zoneLevelFileExportList);
		//long count = zoneToStoreMapping.entrySet().stream().filter(oneStore->locationIds.contains(oneStore.getKey())).count();
		assertEquals("Matches", zoneToStoreConvertedList.size(),4);
		
	}
	
	//Items with invalid location id
	@Test
	public void testCase3() throws SQLException {
		initialize();
		List<StoreFileExportDTO> zoneLevelFileExportList = new ArrayList<StoreFileExportDTO>();
		List<Integer> locationIds = new ArrayList<Integer>();
		StoreFileExportDTO storeFileExportDTO = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(100);
		storeFileExportDTO.setLocationId(151);
		locationIds.add(151);
		
		StoreFileExportDTO storeFileExportDTO1 = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(101);
		storeFileExportDTO.setLocationId(128);
		locationIds.add(128);

		zoneLevelFileExportList.add(storeFileExportDTO1);
		List<StoreFileExportDTO> zoneToStoreConvertedList = storeFileEportFF.convertZoneLevelToStoreLvelList(zoneLevelFileExportList);
		assertEquals("Matches", zoneToStoreConvertedList.size(),0);
	}
	
	//items having clearance on the effective date
	@Test
	public void testCase4() throws SQLException {
		initialize();
		List<StoreFileExportDTO> storeLevelList = new ArrayList<StoreFileExportDTO>();
		List<Integer> locationIds = new ArrayList<Integer>();
		StoreFileExportDTO storeFileExportDTO = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(100);
		storeFileExportDTO.setLocationId(150);
		storeFileExportDTO.setStrId(1);
		storeFileExportDTO.setRegEffectiveDate("2022-07-18");
		storeLevelList.add(storeFileExportDTO);
		
		StoreFileExportDTO storeFileExportDTO1 = new StoreFileExportDTO();
		storeFileExportDTO1.setProductId(100);
		storeFileExportDTO1.setLocationId(150);
		storeFileExportDTO1.setStrId(2);
		storeFileExportDTO1.setRegEffectiveDate("2022-07-18");
		storeLevelList.add(storeFileExportDTO1);
	
		StoreFileExportDTO storeFileExportDTO2 = new StoreFileExportDTO();
		storeFileExportDTO2.setProductId(101);
		storeFileExportDTO2.setLocationId(150);
		storeFileExportDTO2.setStrId(2);
		storeFileExportDTO2.setRegEffectiveDate("2022-07-25");
		storeLevelList.add(storeFileExportDTO2);
		
		List<StoreFileExportDTO> zoneToStoreConvertedList = storeFileEportFF.excludeClearanceStores(storeLevelList);
		assertEquals("Matches", zoneToStoreConvertedList.size(),0);
	}
	
	// items are in  clearance does not fall on the effective date
	@Test
	public void testCase5() throws SQLException {
		initialize();
		List<StoreFileExportDTO> storeLevelList = new ArrayList<StoreFileExportDTO>();
		List<Integer> locationIds = new ArrayList<Integer>();
		StoreFileExportDTO storeFileExportDTO = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(100);
		storeFileExportDTO.setLocationId(150);
		storeFileExportDTO.setStrId(1);
		storeFileExportDTO.setRegEffectiveDate("2022-07-15");
		locationIds.add(151);
		storeLevelList.add(storeFileExportDTO);
		
		StoreFileExportDTO storeFileExportDTO1 = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(100);
		storeFileExportDTO.setLocationId(150);
		storeFileExportDTO.setStrId(2);
		storeFileExportDTO.setRegEffectiveDate("2022-06-12");
		storeLevelList.add(storeFileExportDTO1);
	
		StoreFileExportDTO storeFileExportDTO2 = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(101);
		storeFileExportDTO.setLocationId(150);
		storeFileExportDTO.setStrId(2);
		storeFileExportDTO.setRegEffectiveDate("2022-07-14");

		storeLevelList.add(storeFileExportDTO2);
		List<StoreFileExportDTO> zoneToStoreConvertedList = storeFileEportFF.excludeClearanceStores(storeLevelList);
		assertEquals("Matches", zoneToStoreConvertedList.size(),storeLevelList.size());
	}
	
	//Product id does not match with item id   in clearance
	@Test
	public void testCase6() throws SQLException {
		initialize();
		List<StoreFileExportDTO> storeLevelList = new ArrayList<StoreFileExportDTO>();
		List<Integer> locationIds = new ArrayList<Integer>();
		StoreFileExportDTO storeFileExportDTO = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(102);
		storeFileExportDTO.setLocationId(150);
		storeFileExportDTO.setStrId(1);
		storeFileExportDTO.setRegEffectiveDate("2022-07-25");
		locationIds.add(151);
		storeLevelList.add(storeFileExportDTO);
		
		StoreFileExportDTO storeFileExportDTO1 = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(103);
		storeFileExportDTO.setLocationId(150);
		storeFileExportDTO.setStrId(2);
		storeFileExportDTO.setRegEffectiveDate("2022-07-25");
		storeLevelList.add(storeFileExportDTO1);
	
		StoreFileExportDTO storeFileExportDTO2 = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(103);
		storeFileExportDTO.setLocationId(150);
		storeFileExportDTO.setStrId(3);
		storeFileExportDTO.setRegEffectiveDate("2022-07-25");

		storeLevelList.add(storeFileExportDTO2);
		List<StoreFileExportDTO> zoneToStoreConvertedList = storeFileEportFF.excludeClearanceStores(storeLevelList);
		assertEquals("Matches", zoneToStoreConvertedList.size(),storeLevelList.size());
	}
	
	//items are in clearance does not match with store id
	@Test
	public void testCase7() throws SQLException {
		initialize();
		List<StoreFileExportDTO> storeLevelList = new ArrayList<StoreFileExportDTO>();
		List<Integer> locationIds = new ArrayList<Integer>();
		StoreFileExportDTO storeFileExportDTO = new StoreFileExportDTO();
		storeFileExportDTO.setProductId(100);
		storeFileExportDTO.setLocationId(150);
		storeFileExportDTO.setStrId(10);
		storeFileExportDTO.setRegEffectiveDate("2022-07-18");
		locationIds.add(151);
		storeLevelList.add(storeFileExportDTO);
		
		StoreFileExportDTO storeFileExportDTO1 = new StoreFileExportDTO();
		storeFileExportDTO1.setProductId(100);
		storeFileExportDTO1.setLocationId(150);
		storeFileExportDTO1.setStrId(10);
		storeFileExportDTO1.setRegEffectiveDate("2022-07-18");
		storeLevelList.add(storeFileExportDTO1);
	
		StoreFileExportDTO storeFileExportDTO2 = new StoreFileExportDTO();
		storeFileExportDTO2.setProductId(100);
		storeFileExportDTO2.setLocationId(150);
		storeFileExportDTO2.setStrId(10);
		storeFileExportDTO2.setRegEffectiveDate("2022-07-18");

		storeLevelList.add(storeFileExportDTO2);
		List<StoreFileExportDTO> zoneToStoreConvertedList = storeFileEportFF.excludeClearanceStores(storeLevelList);
		assertEquals("Matches", zoneToStoreConvertedList.size(),storeLevelList.size());
	}
}
