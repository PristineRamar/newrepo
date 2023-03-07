package com.pristine.test.offermgmt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.fileformatter.gianteagle.ZoneDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PriceExportDTO;

class StorePriceExportIntTestDataProvider {
	HashMap<String, Integer> storeZoneMapping;
	List<StoreDTO> storeData;
	List<ZoneDTO> allZoneData;
	List<ZoneDTO> globalZoneData;
	HashMap<String, Integer> itemData;
	List<PriceExportDTO> itemRUData;
	HashMap<Integer, String> zoneIdAndNoMap;
	HashMap<Integer, String> virtualZoneIdToNumMap;
	HashMap<String, Integer> individualZoneNumToIdMap;
	RetailCalendarDTO retailCalendarDTO;
	
	StorePriceExportIntTestDataProvider() {
		storeZoneMapping = new HashMap<String, Integer>();
		storeData = new ArrayList<StoreDTO>();
		allZoneData = new ArrayList<ZoneDTO>();
		globalZoneData = new ArrayList<ZoneDTO>();
		itemData = new HashMap<String, Integer>();
		itemRUData = new ArrayList<PriceExportDTO>();
		zoneIdAndNoMap = new HashMap<Integer, String>();
		virtualZoneIdToNumMap = new HashMap<Integer, String>();
		individualZoneNumToIdMap = new HashMap<String, Integer>();
		retailCalendarDTO = new RetailCalendarDTO();
		
		setStoreZoneMapping();
		setStoreData();
		setAllZoneData();
		setGlobalZoneData();
		setItemData();
		setItemRUData();
		setZoneIdAndNoMap();
		setVirtualZoneIdToNumMap();
		setIndividualZoneNumToIdMap();
		setRetailCalendarDTO();
	}
	
	private void setStoreZoneMapping() {
		storeZoneMapping.put("41", 4);
		storeZoneMapping.put("42", 4);
		storeZoneMapping.put("43", 4);
		storeZoneMapping.put("44", 4);
		storeZoneMapping.put("161", 16);
		storeZoneMapping.put("162", 16);
		storeZoneMapping.put("163", 16);
		storeZoneMapping.put("164", 16);
		storeZoneMapping.put("491", 49);
		storeZoneMapping.put("492", 49);
		storeZoneMapping.put("493", 49);
		storeZoneMapping.put("494", 49);
		storeZoneMapping.put("661", 66);
		storeZoneMapping.put("662", 66);
		storeZoneMapping.put("663", 66);
		storeZoneMapping.put("664", 66);
		storeZoneMapping.put("671", 67);
		storeZoneMapping.put("672", 67);
		storeZoneMapping.put("673", 67);
		storeZoneMapping.put("674", 67);
	}

	private void setStoreData() {
		StoreDTO storeDTO = new StoreDTO();
		storeDTO.setZoneId(4);
		storeDTO.strNum = "41";
		storeDTO.strId = 41;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(4);
		storeDTO.strNum = "42";
		storeDTO.strId = 42;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(4);
		storeDTO.strNum = "43";
		storeDTO.strId = 43;
		storeData.add(storeDTO);
	
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(4);
		storeDTO.strNum = "44";
		storeDTO.strId = 44;
		storeData.add(storeDTO);
	
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(16);
		storeDTO.strNum = "161";
		storeDTO.strId = 161;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(16);
		storeDTO.strNum = "162";
		storeDTO.strId = 162;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(16);
		storeDTO.strNum = "163";
		storeDTO.strId = 163;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(16);
		storeDTO.strNum = "164";
		storeDTO.strId = 164;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(49);
		storeDTO.strNum = "491";
		storeDTO.strId = 491;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(49);
		storeDTO.strNum = "492";
		storeDTO.strId = 492;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(49);
		storeDTO.strNum = "493";
		storeDTO.strId = 493;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(49);
		storeDTO.strNum = "494";
		storeDTO.strId = 494;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(66);
		storeDTO.strNum = "661";
		storeDTO.strId = 661;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(66);
		storeDTO.strNum = "662";
		storeDTO.strId = 662;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(66);
		storeDTO.strNum = "663";
		storeDTO.strId = 663;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(66);
		storeDTO.strNum = "664";
		storeDTO.strId = 664;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(67);
		storeDTO.strNum = "671";
		storeDTO.strId = 671;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(67);
		storeDTO.strNum = "672";
		storeDTO.strId = 672;
		storeData.add(storeDTO);
		
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(67);
		storeDTO.strNum = "673";
		storeDTO.strId = 673;
		storeData.add(storeDTO);
	
		storeDTO = new StoreDTO();
		storeDTO.setZoneId(67);
		storeDTO.strNum = "674";
		storeDTO.strId = 674;
		storeData.add(storeDTO);
	}

	private void setAllZoneData() {
		ZoneDTO zoneDTO = new ZoneDTO();
		zoneDTO.setZnId(4);
		zoneDTO.setZnName("Z_Four");
		zoneDTO.setZnNo("4");
		zoneDTO.setZoneType("W");
		zoneDTO.setGlobalZn("N");
		allZoneData.add(zoneDTO);
		
		zoneDTO = new ZoneDTO();
		zoneDTO.setZnId(16);
		zoneDTO.setZnName("Z_Sixteen");
		zoneDTO.setZnNo("16");
		zoneDTO.setZoneType("W");
		zoneDTO.setGlobalZn("N");
		allZoneData.add(zoneDTO);

		zoneDTO = new ZoneDTO();
		zoneDTO.setZnId(49);
		zoneDTO.setZnName("Z_Forty-Nine");
		zoneDTO.setZnNo("49");
		zoneDTO.setZoneType("I");
		zoneDTO.setGlobalZn("N");
		allZoneData.add(zoneDTO);
		
		zoneDTO = new ZoneDTO();
		zoneDTO.setZnId(66);
		zoneDTO.setZnName("Z_Sixty-Six");
		zoneDTO.setZnNo("66");
		zoneDTO.setZoneType("I");
		zoneDTO.setGlobalZn("N");
		allZoneData.add(zoneDTO);
		
		zoneDTO = new ZoneDTO();
		zoneDTO.setZnId(67);
		zoneDTO.setZnName("Z_Sixty-Seven");
		zoneDTO.setZnNo("67");
		zoneDTO.setZoneType("I");
		zoneDTO.setGlobalZn("N");
		allZoneData.add(zoneDTO);
		
		zoneDTO = new ZoneDTO();
		zoneDTO.setZnId(1000);
		zoneDTO.setZnName("Z_Thousand");
		zoneDTO.setZnNo("1000");
		zoneDTO.setZoneType("W");
		zoneDTO.setGlobalZn("Y");
		allZoneData.add(zoneDTO);
	}

	private void setGlobalZoneData() {
		ZoneDTO zoneDTO = new ZoneDTO();
		zoneDTO.setZnId(4);
		zoneDTO.setZnName("Z_Four");
		zoneDTO.setZnNo("4");
		zoneDTO.setZoneType("W");
		zoneDTO.setGlobalZn("N");
		globalZoneData.add(zoneDTO);
		
		zoneDTO = new ZoneDTO();
		zoneDTO.setZnId(16);
		zoneDTO.setZnName("Z_Sixteen");
		zoneDTO.setZnNo("16");
		zoneDTO.setZoneType("W");
		zoneDTO.setGlobalZn("N");
		globalZoneData.add(zoneDTO);
	}

	private void setItemData() {
		itemData.put("1", 1);
		itemData.put("2", 2);
		itemData.put("3", 3);
		itemData.put("4", 4);
		itemData.put("5", 5);
		itemData.put("6", 6);
		itemData.put("7", 7);
		itemData.put("8", 8);
		itemData.put("9", 9);
		itemData.put("10", 10);
		itemData.put("11", 11);
		itemData.put("12", 12);
		itemData.put("13", 13);
		itemData.put("14", 14);
		itemData.put("15", 15);
		itemData.put("16", 16);
		itemData.put("17", 17);
		itemData.put("18", 18);
		itemData.put("19", 19);
		itemData.put("20", 20);
		itemData.put("21", 21);
		itemData.put("22", 22);
		itemData.put("23", 23);
		itemData.put("24", 24);
		itemData.put("25", 25);
		itemData.put("26", 26);
		itemData.put("27", 27);
		itemData.put("28", 28);
		itemData.put("29", 29);
		itemData.put("30", 30);
	}

	private void setItemRUData() {
		PriceExportDTO priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(1);
		priceExportDTO.setRetailerItemCode("1");
		priceExportDTO.setRecommendationUnit("RU_One");
		priceExportDTO.setRecommendationUnitId(1);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(2);
		priceExportDTO.setRetailerItemCode("2");
		priceExportDTO.setRecommendationUnit("RU_Two");
		priceExportDTO.setRecommendationUnitId(2);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(3);
		priceExportDTO.setRetailerItemCode("3");
		priceExportDTO.setRecommendationUnit("RU_Three");
		priceExportDTO.setRecommendationUnitId(3);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(4);
		priceExportDTO.setRetailerItemCode("4");
		priceExportDTO.setRecommendationUnit("RU_Four");
		priceExportDTO.setRecommendationUnitId(4);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(5);
		priceExportDTO.setRetailerItemCode("5");
		priceExportDTO.setRecommendationUnit("RU_Five");
		priceExportDTO.setRecommendationUnitId(5);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(6);
		priceExportDTO.setRetailerItemCode("6");
		priceExportDTO.setRecommendationUnit("RU_Six");
		priceExportDTO.setRecommendationUnitId(6);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(7);
		priceExportDTO.setRetailerItemCode("7");
		priceExportDTO.setRecommendationUnit("RU_Seven");
		priceExportDTO.setRecommendationUnitId(7);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(8);
		priceExportDTO.setRetailerItemCode("8");
		priceExportDTO.setRecommendationUnit("RU_Eight");
		priceExportDTO.setRecommendationUnitId(8);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(9);
		priceExportDTO.setRetailerItemCode("9");
		priceExportDTO.setRecommendationUnit("RU_Nine");
		priceExportDTO.setRecommendationUnitId(9);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(10);
		priceExportDTO.setRetailerItemCode("10");
		priceExportDTO.setRecommendationUnit("RU_Ten");
		priceExportDTO.setRecommendationUnitId(10);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(11);
		priceExportDTO.setRetailerItemCode("11");
		priceExportDTO.setRecommendationUnit("RU_One");
		priceExportDTO.setRecommendationUnitId(1);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(12);
		priceExportDTO.setRetailerItemCode("12");
		priceExportDTO.setRecommendationUnit("RU_Two");
		priceExportDTO.setRecommendationUnitId(2);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(13);
		priceExportDTO.setRetailerItemCode("13");
		priceExportDTO.setRecommendationUnit("RU_Three");
		priceExportDTO.setRecommendationUnitId(3);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(14);
		priceExportDTO.setRetailerItemCode("14");
		priceExportDTO.setRecommendationUnit("RU_Four");
		priceExportDTO.setRecommendationUnitId(4);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(15);
		priceExportDTO.setRetailerItemCode("15");
		priceExportDTO.setRecommendationUnit("RU_Five");
		priceExportDTO.setRecommendationUnitId(5);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(16);
		priceExportDTO.setRetailerItemCode("16");
		priceExportDTO.setRecommendationUnit("RU_Six");
		priceExportDTO.setRecommendationUnitId(6);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(17);
		priceExportDTO.setRetailerItemCode("17");
		priceExportDTO.setRecommendationUnit("RU_Seven");
		priceExportDTO.setRecommendationUnitId(7);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(18);
		priceExportDTO.setRetailerItemCode("18");
		priceExportDTO.setRecommendationUnit("RU_Eight");
		priceExportDTO.setRecommendationUnitId(8);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(19);
		priceExportDTO.setRetailerItemCode("19");
		priceExportDTO.setRecommendationUnit("RU_Nine");
		priceExportDTO.setRecommendationUnitId(9);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(20);
		priceExportDTO.setRetailerItemCode("20");
		priceExportDTO.setRecommendationUnit("RU_Ten");
		priceExportDTO.setRecommendationUnitId(10);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(21);
		priceExportDTO.setRetailerItemCode("21");
		priceExportDTO.setRecommendationUnit("RU_One");
		priceExportDTO.setRecommendationUnitId(1);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(22);
		priceExportDTO.setRetailerItemCode("22");
		priceExportDTO.setRecommendationUnit("RU_Two");
		priceExportDTO.setRecommendationUnitId(2);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(23);
		priceExportDTO.setRetailerItemCode("23");
		priceExportDTO.setRecommendationUnit("RU_Three");
		priceExportDTO.setRecommendationUnitId(3);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(24);
		priceExportDTO.setRetailerItemCode("24");
		priceExportDTO.setRecommendationUnit("RU_Four");
		priceExportDTO.setRecommendationUnitId(4);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(25);
		priceExportDTO.setRetailerItemCode("25");
		priceExportDTO.setRecommendationUnit("RU_Five");
		priceExportDTO.setRecommendationUnitId(5);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(26);
		priceExportDTO.setRetailerItemCode("26");
		priceExportDTO.setRecommendationUnit("RU_Six");
		priceExportDTO.setRecommendationUnitId(6);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(27);
		priceExportDTO.setRetailerItemCode("27");
		priceExportDTO.setRecommendationUnit("RU_Seven");
		priceExportDTO.setRecommendationUnitId(7);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(28);
		priceExportDTO.setRetailerItemCode("28");
		priceExportDTO.setRecommendationUnit("RU_Eight");
		priceExportDTO.setRecommendationUnitId(8);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(29);
		priceExportDTO.setRetailerItemCode("29");
		priceExportDTO.setRecommendationUnit("RU_Nine");
		priceExportDTO.setRecommendationUnitId(9);
		itemRUData.add(priceExportDTO);
		
		priceExportDTO = new PriceExportDTO();
		priceExportDTO.setItemCode(30);
		priceExportDTO.setRetailerItemCode("30");
		priceExportDTO.setRecommendationUnit("RU_Ten");
		priceExportDTO.setRecommendationUnitId(10);
		itemRUData.add(priceExportDTO);
	}

	private void setZoneIdAndNoMap() {
		zoneIdAndNoMap.put(1, "1");
		zoneIdAndNoMap.put(2, "2");
		zoneIdAndNoMap.put(3, "3");
		zoneIdAndNoMap.put(4, "4");
		zoneIdAndNoMap.put(49, "49");
		zoneIdAndNoMap.put(66, "66");
		zoneIdAndNoMap.put(67, "67");
	}

	private void setVirtualZoneIdToNumMap() {
		virtualZoneIdToNumMap.put(30, "30");
	}

	private void setIndividualZoneNumToIdMap() {
		individualZoneNumToIdMap.put("49", 49);
		individualZoneNumToIdMap.put("66", 66);
		individualZoneNumToIdMap.put("67", 67);
	}

	private void setRetailCalendarDTO() {
		retailCalendarDTO.setCalendarId(1);
	}

	HashMap<String, Integer> getStoreZoneMapping() {
		return storeZoneMapping;
	}
	
	List<StoreDTO> getStoreData() {
		return storeData;
	}
	
	List<ZoneDTO> getAllZoneData() {
		return allZoneData;
	}
	
	List<ZoneDTO> getGlobalZoneData() {
		return globalZoneData;
	}
	
	HashMap<String, Integer> getItemData() {
		return itemData;
	}
	
	List<PriceExportDTO> getItemRUData() {
		return itemRUData;
	}
	
	HashMap<Integer, String> getZoneIdAndNoMap() {
		return zoneIdAndNoMap;
	}
	
	String getBaseChainId() {
		return "100";
	}
	
	HashMap<Integer, String> getVirtualZoneIdToNumMap() {
		return virtualZoneIdToNumMap;
	}
	
	HashMap<String, Integer> getIndividualZoneNumToIdMap() {
		return individualZoneNumToIdMap;
	}
	
	RetailCalendarDTO getRetailCalendarDTO() {
		return retailCalendarDTO;
	}
	
	List<PriceExportDTO> approvedItemsForSample() {
		List<PriceExportDTO> approvedItemsList = new ArrayList<PriceExportDTO>();
		PriceExportDTO approvedItem = new PriceExportDTO();
		approvedItem.setZoneType("R");
		approvedItem.setRunId(1L);
		approvedItem.setRetLirId(1);
		approvedItem.setItemCode(1);
		approvedItem.setRetailerItemCode("1");
		approvedItem.setPriceZoneId(4);
		approvedItem.setPriceZoneNo("4");
		approvedItem.setZoneType("W");
		approvedItem.setRegEffDate("01/06/2050 00:00:00");
		approvedItem.setRecommendedRegPrice(new MultiplePrice(1, 1.99));
		approvedItem.setCurrentRegPrice(new MultiplePrice(1, 1.49));
		approvedItem.setCoreRetail(0.99);
		approvedItem.setVdpRetail(approvedItem.getRecommendedRegPrice().getUnitPrice());
		approvedItem.setDiffRetail(approvedItem.getRecommendedRegPrice().getUnitPrice() - approvedItem.getCurrentRegPrice().getUnitPrice());
		approvedItem.setPriceExportType("N");
		approvedItem.setItemType("H");
		approvedItem.setProductLevelId(1);
		approvedItem.setChildLocationLevelId(1);
		approvedItem.setChildLocationId(1);
		approvedItemsList.add(approvedItem);
		
		approvedItem = new PriceExportDTO();
		approvedItem.setZoneType("R");
		approvedItem.setRunId(1L);
		approvedItem.setRetLirId(1);
		approvedItem.setItemCode(11);
		approvedItem.setRetailerItemCode("11");
		approvedItem.setPriceZoneId(4);
		approvedItem.setPriceZoneNo("4");
		approvedItem.setZoneType("W");
		approvedItem.setRegEffDate("01/06/2050 00:00:00");
		approvedItem.setRecommendedRegPrice(new MultiplePrice(1, 11.99));
		approvedItem.setCurrentRegPrice(new MultiplePrice(1, 11.49));
		approvedItem.setCoreRetail(10.49);
		approvedItem.setVdpRetail(approvedItem.getRecommendedRegPrice().getUnitPrice());
		approvedItem.setDiffRetail(approvedItem.getRecommendedRegPrice().getUnitPrice() - approvedItem.getCurrentRegPrice().getUnitPrice());
		approvedItem.setPriceExportType("N");
		approvedItem.setItemType("H");
		approvedItem.setProductLevelId(1);
		approvedItem.setChildLocationLevelId(1);
		approvedItem.setChildLocationId(1);
		approvedItemsList.add(approvedItem);
		
		approvedItem = new PriceExportDTO();
		approvedItem.setZoneType("R");
		approvedItem.setRunId(1L);
		approvedItem.setRetLirId(1);
		approvedItem.setItemCode(11);
		approvedItem.setRetailerItemCode("11");
		approvedItem.setPriceZoneId(16);
		approvedItem.setPriceZoneNo("16");
		approvedItem.setZoneType("W");
		approvedItem.setRegEffDate("01/06/2050 00:00:00");
		approvedItem.setRecommendedRegPrice(new MultiplePrice(1, 11.49));
		approvedItem.setCurrentRegPrice(new MultiplePrice(1, 10.99));
		approvedItem.setCoreRetail(10.49);
		approvedItem.setVdpRetail(approvedItem.getRecommendedRegPrice().getUnitPrice());
		approvedItem.setDiffRetail(approvedItem.getRecommendedRegPrice().getUnitPrice() - approvedItem.getCurrentRegPrice().getUnitPrice());
		approvedItem.setPriceExportType("N");
		approvedItem.setItemType("H");
		approvedItem.setProductLevelId(1);
		approvedItem.setChildLocationLevelId(1);
		approvedItem.setChildLocationId(1);
		approvedItemsList.add(approvedItem);
		
		approvedItem = new PriceExportDTO();
		approvedItem.setZoneType("R");
		approvedItem.setRunId(2L);
		approvedItem.setRetLirId(2);
		approvedItem.setItemCode(12);
		approvedItem.setRetailerItemCode("12");
		approvedItem.setPriceZoneId(16);
		approvedItem.setPriceZoneNo("16");
		approvedItem.setZoneType("W");
		approvedItem.setRegEffDate("01/06/2050 00:00:00");
		approvedItem.setRecommendedRegPrice(new MultiplePrice(1, 12.99));
		approvedItem.setCurrentRegPrice(new MultiplePrice(1, 12.49));
		approvedItem.setCoreRetail(11.99);
		approvedItem.setVdpRetail(approvedItem.getRecommendedRegPrice().getUnitPrice());
		approvedItem.setDiffRetail(approvedItem.getRecommendedRegPrice().getUnitPrice() - approvedItem.getCurrentRegPrice().getUnitPrice());
		approvedItem.setPriceExportType("N");
		approvedItem.setItemType("H");
		approvedItem.setProductLevelId(1);
		approvedItem.setChildLocationLevelId(1);
		approvedItem.setChildLocationId(1);
		approvedItemsList.add(approvedItem);
		
		approvedItem = new PriceExportDTO();
		approvedItem.setZoneType("R");
		approvedItem.setRunId(3L);
		approvedItem.setRetLirId(3);
		approvedItem.setItemCode(3);
		approvedItem.setRetailerItemCode("3");
		approvedItem.setPriceZoneId(66);
		approvedItem.setPriceZoneNo("66");
		approvedItem.setZoneType("I");
		approvedItem.setRegEffDate("01/06/2050 00:00:00");
		approvedItem.setRecommendedRegPrice(new MultiplePrice(1, 3.99));
		approvedItem.setCurrentRegPrice(new MultiplePrice(1, 3.49));
		approvedItem.setCoreRetail(2.99);
		approvedItem.setVdpRetail(approvedItem.getRecommendedRegPrice().getUnitPrice());
		approvedItem.setDiffRetail(approvedItem.getRecommendedRegPrice().getUnitPrice() - approvedItem.getCurrentRegPrice().getUnitPrice());
		approvedItem.setPriceExportType("N");
		approvedItem.setItemType("H");
		approvedItem.setProductLevelId(1);
		approvedItem.setChildLocationLevelId(1);
		approvedItem.setChildLocationId(1);
		approvedItemsList.add(approvedItem);
		
		approvedItem = new PriceExportDTO();
		approvedItem.setZoneType("R");
		approvedItem.setRunId(3L);
		approvedItem.setRetLirId(3);
		approvedItem.setItemCode(13);
		approvedItem.setRetailerItemCode("13");
		approvedItem.setPriceZoneId(67);
		approvedItem.setPriceZoneNo("67");
		approvedItem.setZoneType("I");
		approvedItem.setRegEffDate("01/06/2050 00:00:00");
		approvedItem.setRecommendedRegPrice(new MultiplePrice(1, 13.99));
		approvedItem.setCurrentRegPrice(new MultiplePrice(1, 13.49));
		approvedItem.setCoreRetail(12.49);
		approvedItem.setVdpRetail(approvedItem.getRecommendedRegPrice().getUnitPrice());
		approvedItem.setDiffRetail(approvedItem.getRecommendedRegPrice().getUnitPrice() - approvedItem.getCurrentRegPrice().getUnitPrice());
		approvedItem.setPriceExportType("N");
		approvedItem.setItemType("H");
		approvedItem.setProductLevelId(1);
		approvedItem.setChildLocationLevelId(1);
		approvedItem.setChildLocationId(1);
		approvedItemsList.add(approvedItem);
		
		approvedItem = new PriceExportDTO();
		approvedItem.setZoneType("R");
		approvedItem.setRunId(4L);
		approvedItem.setRetLirId(4);
		approvedItem.setItemCode(4);
		approvedItem.setRetailerItemCode("4");
		approvedItem.setPriceZoneId(1000);
		approvedItem.setPriceZoneNo("1000");
		approvedItem.setZoneType("W");
		approvedItem.setRegEffDate("01/06/2050 00:00:00");
		approvedItem.setRecommendedRegPrice(new MultiplePrice(1, 4.99));
		approvedItem.setCurrentRegPrice(new MultiplePrice(1, 4.49));
		approvedItem.setCoreRetail(3.99);
		approvedItem.setVdpRetail(approvedItem.getRecommendedRegPrice().getUnitPrice());
		approvedItem.setDiffRetail(approvedItem.getRecommendedRegPrice().getUnitPrice() - approvedItem.getCurrentRegPrice().getUnitPrice());
		approvedItem.setPriceExportType("N");
		approvedItem.setItemType("H");
		approvedItem.setProductLevelId(1);
		approvedItem.setChildLocationLevelId(1);
		approvedItem.setChildLocationId(1);
		approvedItemsList.add(approvedItem);
		
		approvedItem = new PriceExportDTO();
		approvedItem.setZoneType("R");
		approvedItem.setRunId(4L);
		approvedItem.setRetLirId(4);
		approvedItem.setItemCode(14);
		approvedItem.setRetailerItemCode("14");
		approvedItem.setPriceZoneId(1000);
		approvedItem.setPriceZoneNo("1000");
		approvedItem.setZoneType("W");
		approvedItem.setRegEffDate("01/06/2050 00:00:00");
		approvedItem.setRecommendedRegPrice(new MultiplePrice(1, 14.99));
		approvedItem.setCurrentRegPrice(new MultiplePrice(1, 15.49));
		approvedItem.setCoreRetail(13.99);
		approvedItem.setVdpRetail(approvedItem.getRecommendedRegPrice().getUnitPrice());
		approvedItem.setDiffRetail(approvedItem.getRecommendedRegPrice().getUnitPrice() - approvedItem.getCurrentRegPrice().getUnitPrice());
		approvedItem.setPriceExportType("N");
		approvedItem.setItemType("H");
		approvedItem.setProductLevelId(1);
		approvedItem.setChildLocationLevelId(1);
		approvedItem.setChildLocationId(1);
		approvedItemsList.add(approvedItem);
		
		return approvedItemsList;
	}

	public HashMap<Long, Integer> getRunIdToItemCountMapFromExportQueue(List<PriceExportDTO> exportQ){
		HashMap<Long, Integer> runIdToItemCountMap = new HashMap<Long, Integer>();
		int count;
		long runId;
		for (PriceExportDTO item : exportQ) {
			runId = item.getRunId();
			if(runIdToItemCountMap.containsKey(runId)) {
				count = runIdToItemCountMap.get(runId);
				runIdToItemCountMap.put(runId, ++count);
			} else {
				runIdToItemCountMap.put(runId, 1);
			}
		}
		return runIdToItemCountMap;
	}
	
	public HashMap<String, List<Long>> segregateRunIdsByNormalAndTestZones(List<PriceExportDTO> exportQ) {
		HashMap<String, List<Long>> zoneTypeToRunIdMapFromExportQueue = new HashMap<String, List<Long>>();
		zoneTypeToRunIdMapFromExportQueue.put("N", new ArrayList<>());
		zoneTypeToRunIdMapFromExportQueue.put("T", new ArrayList<>());
		String zoneType;
		for (PriceExportDTO item : exportQ) {
			zoneType = item.getZoneType();
			if("W".equalsIgnoreCase(zoneType) || "I".equalsIgnoreCase(zoneType)) {
				if(!zoneTypeToRunIdMapFromExportQueue.get("N").contains(item.getRunId()))
					zoneTypeToRunIdMapFromExportQueue.get("N").add(item.getRunId());
			}
			else if("T".equalsIgnoreCase(zoneType)) {
				if(!zoneTypeToRunIdMapFromExportQueue.get("T").contains(item.getRunId()))
					zoneTypeToRunIdMapFromExportQueue.get("T").add(item.getRunId());
			}
		}
		return zoneTypeToRunIdMapFromExportQueue;
	}
	
}
