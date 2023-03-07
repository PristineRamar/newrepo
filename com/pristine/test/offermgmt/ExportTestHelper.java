package com.pristine.test.offermgmt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.fileformatter.gianteagle.ZoneDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PriceExportDTO;
import com.pristine.util.offermgmt.PRFormatHelper;

public class ExportTestHelper {

	public static List<Long> getTestRunIds() {
		List<Long> runIds = new ArrayList<>();
		runIds.add(7898L);
		return runIds;
	}

	public static List<String> getItemStoreCombinationOfTestzone() {
		
		List<String> itemStoreList = new ArrayList<>();
		itemStoreList.add(1234+"_"+9876);
		return itemStoreList;
	}

	public static HashMap<String, List<Long>> getRunIds() {
		HashMap<String, List<Long>> runIdMap = new HashMap<>();
		List<Long> normalRunIds = new ArrayList<>();
		normalRunIds.add(7741L);
		normalRunIds.add(7573L);
		
		List<Long> testRunIds = new ArrayList<>();
		
		runIdMap.put("N", normalRunIds);
		runIdMap.put("T", testRunIds);
		return runIdMap;
	}
	
	//for approved items
	public static List<PRItemDTO> getObjectList() {
		List<PRItemDTO> objectList = new ArrayList<>();
		//runId, roductLevId, prodId, locLevId, locId, attr6, retLirId, zoneId, zoneNUm, retItemCOde, recRegPrc, recCurPrc, effDate, prcType, 
		//apprby, prrvName, Vdpret, coreRet, Impact, pred, partnum, ovrRegM, ovrReg, ovrPrc, diff, zoneName, recUnit, priceCheckListTypeId
				
		objectList.add(new PRItemDTO(7741,1,392914,6,3,"H",0,3,"16","114864",new MultiplePrice(0,97.49),new MultiplePrice(0,94.99),
				"03/31/2021","N","prestolive","prestolive", 97.49,0.0,2.5,"","C969",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
				"IGNITIONCOILS_RU",0,"N"));
		objectList.add(new PRItemDTO(7741,1,495183,6,3,"H",0,3,"16","857011",new MultiplePrice(0,147.99),new MultiplePrice(0,142.99),
				"03/31/2021","N","prestolive","prestolive",147.99,0.0,5,"","C1433",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
				"IGNITIONCOILS_RU",0,"N"));
		objectList.add(new PRItemDTO(7741,1,377718,6,3,"H",0,3,"16","35057",new MultiplePrice(0,5.59),new MultiplePrice(0,4.99),
				"03/31/2021","N","prestolive","prestolive",5.59,0.0,8.4,"","CP036",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
				"IGNITIONCOILS_RU",0,"N"));
		objectList.add(new PRItemDTO(7741,1,494124,6,3,"H",355637,3,"16","849783",new MultiplePrice(0,43.49),new MultiplePrice(0,39.99),
				"03/31/2021","N","prestolive","prestolive",43.49,0.0,7,"","GN10446",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
				"IGNITIONCOILS_RU",0,"N"));
		objectList.add(new PRItemDTO(7741,1,379746,6,3,"H",0,3,"16","48058",new MultiplePrice(0,225.99),new MultiplePrice(0,219.99),
				"03/31/2021","N","prestolive","prestolive",225.99,0.0,30,"","48662",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
				"IGNITIONCOILS_RU",0,"N"));
		
		return objectList;
	}
	
	//for ec items
	public static List<PRItemDTO> getObjectList1() {
		List<PRItemDTO> objectList = new ArrayList<>();
		//runId, roductLevId, prodId, locLevId, locId, attr6, retLirId, zoneId, zoneNUm, retItemCOde, recRegPrc, recCurPrc, effDate, prcType, 
		//apprby, prrvName, Vdpret, coreRet, Impact, pred, partnum, ovrRegM, ovrReg, ovrPrc, diff, zoneName, recUnit, priceCheckListTypeId
		objectList.add(new PRItemDTO(7741,1,494124,6,3,"H",355637,3,"16","849783",new MultiplePrice(0,43.49),new MultiplePrice(0,39.99),
				"03/31/2021","N","prestolive","prestolive",43.49,0.0,7,"","GN10446",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
				"IGNITIONCOILS_RU",28,""));
		objectList.add(new PRItemDTO(7741,1,379746,6,3,"H",0,3,"16","48058",new MultiplePrice(0,225.99),new MultiplePrice(0,219.99),
				"03/31/2021","N","prestolive","prestolive",225.99,0.0,30,"","48662",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
				"IGNITIONCOILS_RU",29,""));
		
		return objectList;
	}
	
	// for store lock items
	public static List<PRItemDTO> getObjectList2() {
		List<PRItemDTO> objectList = new ArrayList<>();
		// runId, roductLevId, prodId, locLevId, locId, attr6, retLirId, zoneId,
		// zoneNUm, retItemCOde, recRegPrc, recCurPrc, effDate, prcType,
		// apprby, prrvName, Vdpret, coreRet, Impact, pred, partnum, ovrRegM, ovrReg,
		// ovrPrc, diff, zoneName, recUnit, priceCheckListTypeId
		objectList.add(new PRItemDTO(7741,1,377718,6,3,"H",0,3,"16","35057",new MultiplePrice(0,5.59),new MultiplePrice(0,4.99),
				"03/31/2021","N","prestolive","prestolive",5.59,0.0,8.4,"","CP036",0,0,new MultiplePrice(0,0.0),0,
				"ADV-ORLY-NAPA-PEP","IGNITIONCOILS_RU",25,""));

		return objectList;
	}

	//for sf items
		public static List<PRItemDTO> getObjectListForSFlimit() {
			List<PRItemDTO> objectList = new ArrayList<>();
			//runId, roductLevId, prodId, locLevId, locId, attr6, retLirId, zoneId, zoneNUm, retItemCOde, recRegPrc, recCurPrc, effDate, prcType, 
			//apprby, prrvName, Vdpret, coreRet, Impact, pred, partnum, ovrRegM, ovrReg, ovrPrc, diff, zoneName, recUnit, priceCheckListTypeId
					
		/*	objectList.add(new PRItemDTO(7741,1,392914,6,3,"H",0,3,"16","114864",new MultiplePrice(0,97.49),new MultiplePrice(0,94.99),
					"03/31/2021","N","prestolive","prestolive", 97.49,0.0,2.5,"","C969",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N", 0, 0, 0, 0, null));*/
			objectList.add(new PRItemDTO(7741,1,495183,6,3,"H",0,3,"16","857011",new MultiplePrice(0,147.99),new MultiplePrice(0,142.99),
					"03/31/2021","N","prestolive","prestolive",147.99,0.0,5,"","C1433",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N"));
			objectList.add(new PRItemDTO(7741,1,377718,6,3,"H",0,3,"16","35057",new MultiplePrice(0,5.59),new MultiplePrice(0,4.99),
					"03/31/2021","N","prestolive","prestolive",5.59,0.0,8.4,"","CP036",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N"));
			objectList.add(new PRItemDTO(7741,1,494124,6,3,"H",355637,3,"16","849783",new MultiplePrice(0,43.49),new MultiplePrice(0,39.99),
					"03/31/2021","N","prestolive","prestolive",43.49,0.0,7,"","GN10446",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N"));
			objectList.add(new PRItemDTO(7741,1,379746,6,3,"H",0,3,"16","48058",new MultiplePrice(0,225.99),new MultiplePrice(0,219.99),
					"03/31/2021","N","prestolive","prestolive",225.99,0.0,30,"","48662",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N"));
			
			return objectList;
		}
		
	//*******************************************************************************************************//
		public static List<Long> getRunIdList() {
			List<Long> runIds = new ArrayList<>();
			runIds.add(10827L);
			runIds.add(10828L);
			return runIds;
		}
		public static HashMap<String,List<Long>> getEmergencyRunIdMap() {
			HashMap<String,List<Long>> runIdMap = new HashMap<>();
			List<Long> runIds = getRunIdList();
			runIdMap.put("N", runIds);
			return runIdMap;
		}
		public static HashMap<String,List<Long>> getRegZoneRunIdMap() {
			HashMap<String,List<Long>> runIdMap = new HashMap<>();
			List<Long> runIds = getRunIdList();
			runIdMap.put("N", runIds);
			return runIdMap;
		}
		public static HashMap<String,List<Long>> getTestZoneRunIdMap() {
			HashMap<String,List<Long>> runIdMap = new HashMap<>();
			List<Long> runIds = getRunIdList();
			runIdMap.put("T", runIds);
			return runIdMap;
		}

		public static List<PriceExportDTO> getApprovedItemObjList() {
			List<PriceExportDTO> objectList = new ArrayList<>();
			//runId, roductLevId, prodId, locLevId, locId, attr6, retLirId, zoneId, zoneNUm, retItemCOde, recRegPrc, recCurPrc, effDate, prcType, 
			//apprby, prrvName, Vdpret, coreRet, Impact, pred, partnum, ovrRegM, ovrReg, ovrPrc, diff, zoneName, recUnit, priceCheckListTypeId
					
			objectList.add(new PriceExportDTO(10827L,1,392914,6,3,"H",0,3,"4","114864",new MultiplePrice(0,97.49),new MultiplePrice(0,94.99),
					"03/31/2021","N","prestolive","prestolive", 97.49,0.0,2.5,"","C969",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N","02/02/2022 00:00:00", 123));
			objectList.add(new PriceExportDTO(10827L,1,495183,6,3,"H",0,3,"16","857011",new MultiplePrice(0,147.99),new MultiplePrice(0,142.99),
					"03/31/2021","N","prestolive","prestolive",147.99,0.0,5,"","C1433",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N","02/02/2022 00:00:00", 234));
			objectList.add(new PriceExportDTO(10827L,1,377718,6,3,"H",0,3,"16","35057",new MultiplePrice(0,5.59),new MultiplePrice(0,4.99),
					"03/31/2021","N","prestolive","prestolive",5.59,0.0,8.4,"","CP036",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N","02/02/2022 00:00:00", 345));
			objectList.add(new PriceExportDTO(10828L,1,494124,6,3,"H",355637,3,"49","849783",new MultiplePrice(0,43.49),new MultiplePrice(0,39.99),
					"03/31/2021","N","prestolive","prestolive",43.49,0.0,7,"","GN10446",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N","02/02/2022 00:00:00", 456));
			objectList.add(new PriceExportDTO(10828L,1,379746,6,3,"H",0,3,"16","48058",new MultiplePrice(0,225.99),new MultiplePrice(0,219.99),
					"03/31/2021","N","prestolive","prestolive",225.99,0.0,30,"","48662",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N","02/02/2022 00:00:00", 567));
			
			return objectList;
		}
		
		public static List<PriceExportDTO> getGlobalZoneApprovedItemObjList() {
			List<PriceExportDTO> objectList = new ArrayList<>();
			//runId, roductLevId, prodId, locLevId, locId, attr6, retLirId, zoneId, zoneNUm, retItemCOde, recRegPrc, recCurPrc, effDate, prcType, 
			//apprby, prrvName, Vdpret, coreRet, Impact, pred, partnum, ovrRegM, ovrReg, ovrPrc, diff, zoneName, recUnit, priceCheckListTypeId
					
			objectList.add(new PriceExportDTO(10827L,1,392914,6,3,"H",0,3,"1000","114864",new MultiplePrice(0,97.49),new MultiplePrice(0,94.99),
					"03/31/2021","N","prestolive","prestolive", 97.49,0.0,2.5,"","C969",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N","02/02/2022 00:00:00", 123));
			objectList.add(new PriceExportDTO(10827L,1,495183,6,3,"H",0,3,"1000","857011",new MultiplePrice(0,147.99),new MultiplePrice(0,142.99),
					"03/31/2021","N","prestolive","prestolive",147.99,0.0,5,"","C1433",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N","02/02/2022 00:00:00", 234));
			objectList.add(new PriceExportDTO(10827L,1,377718,6,3,"H",0,3,"1000","35057",new MultiplePrice(0,5.59),new MultiplePrice(0,4.99),
					"03/31/2021","N","prestolive","prestolive",5.59,0.0,8.4,"","CP036",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N","02/02/2022 00:00:00", 345));
			objectList.add(new PriceExportDTO(10828L,1,494124,6,3,"H",355637,3,"49","849783",new MultiplePrice(0,43.49),new MultiplePrice(0,39.99),
					"03/31/2021","N","prestolive","prestolive",43.49,0.0,7,"","GN10446",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N","02/03/2022 00:00:00", 456));
			objectList.add(new PriceExportDTO(10828L,1,379746,6,3,"H",0,3,"16","48058",new MultiplePrice(0,225.99),new MultiplePrice(0,219.99),
					"03/31/2021","N","prestolive","prestolive",225.99,0.0,30,"","48662",0,0,new MultiplePrice(0,0.0),0,"ADV-ORLY-NAPA-PEP",
					"IGNITIONCOILS_RU",0,"N","02/03/2022 00:00:00", 567));
			
			return objectList;
		}
		
		public static List<ZoneDTO> getActiveZones(){
			List<ZoneDTO> objectList = new ArrayList<>();
			objectList.add(new ZoneDTO("1000",9,"W","Z-1000","Y"));
			objectList.add(new ZoneDTO("67",10,"I","VI","N"));
			objectList.add(new ZoneDTO("4", 2, "W", "ADV-ORLY", "N"));
			objectList.add(new ZoneDTO("16", 3, "W", "ADV-ORLY-NAPA", "N"));
			objectList.add(new ZoneDTO("49",6,"I","AK-HI","N"));
			objectList.add(new ZoneDTO("66",7,"I","PR","N"));
			objectList.add(new ZoneDTO("9995",30,"T","TestZone-9995","N"));
			objectList.add(new ZoneDTO("9996",31,"T","TestZone-9996","N"));
			objectList.add(new ZoneDTO("9997",32,"T","TestZone-9997","N"));
			objectList.add(new ZoneDTO("9998",33,"T","TestZone-9998","N"));
			objectList.add(new ZoneDTO("9999",34,"T","TestZone-9999","N"));
			objectList.add(new ZoneDTO("30",8,"W","VIRTUAL-ZONE","N"));
			objectList.add(new ZoneDTO("4", 2, "W", "ADV-ORLY", "N"));
			objectList.add(new ZoneDTO("16", 3, "W", "ADV-ORLY-NAPA", "N"));
			
			return objectList;
		}
		
		public static List<ZoneDTO> getZonesUnderGlobalZone(){
			List<ZoneDTO> objectList = new ArrayList<>();			
			objectList.add(new ZoneDTO("4", 2, "W", "ADV-ORLY", "N"));
			objectList.add(new ZoneDTO("16", 3, "W", "ADV-ORLY-NAPA", "N"));
			
			return objectList;
		}

		public static List<Integer> getExcludeZoneIdForVirtualZone() {
			List<Integer> excludedZones = new ArrayList<>();	
			excludedZones.add(49);
			excludedZones.add(66);
			excludedZones.add(67);
			return excludedZones;
		}
		
		public static List<StoreDTO> getActiveStores(){
			List<StoreDTO> activeStores = new ArrayList<>();
			activeStores.add(new StoreDTO("4286", 26923, 3));
			activeStores.add(new StoreDTO("718", 680, 3));
			activeStores.add(new StoreDTO("719", 681,3));
			activeStores.add(new StoreDTO("720",682,3));
			activeStores.add(new StoreDTO("721",683,3));
			activeStores.add(new StoreDTO("722",684,3));
			activeStores.add(new StoreDTO("723",685,3));
			activeStores.add(new StoreDTO("724",686,3));
			activeStores.add(new StoreDTO("725",687,3));
			activeStores.add(new StoreDTO("726",688,3));
			activeStores.add(new StoreDTO("727",689,3));
			activeStores.add(new StoreDTO("728",690,3));
			activeStores.add(new StoreDTO("729",691,3));
			activeStores.add(new StoreDTO("730", 692,2));
			
			return activeStores;
		}
		
		public static List<String> getExcludeEcomStores(){
			List<String> excludeEcomStores = new ArrayList<>();
			excludeEcomStores.add("6998");
			excludeEcomStores.add("6999");
			return excludeEcomStores;			
		}
		
		public static List<String> getExcludeNDDStores(){
			List<String> nddStoreList = new ArrayList<>();
			String nddStores = "6546,6547,6548,6549,6550,6552,6553,6554,6555,6556,6557,6558,6559,6563,6564,6565,6566,6567,6568,"
					+ "6569,6570,6572,6573,6574,6575,6577,6578,6579,6580,6901,6902,6903,6904,6905,6906,6907,6908,6909,6910,6911,"
					+ "6912,6913,6914,6915,6916,6917,6918,6921,6922,6923,6924,6925,6926,6927,6928,6929,6930,6932,6933,6934,6935,"
					+ "6936,6937,6938,6939,6940,6941,6942,6943,6944,6945,6946,6947,6948,6949,6950,6951,6952,6953,6954,6955,6956,"
					+ "6957,6958,6959,6960,6961,6962,6963,6964,6965,6966,6967,6968,6969,6970,6971,6972,6973,6974,6976,6977,6978,"
					+ "6979,6980,6981,6982,6984,6996,6997,6998,6999";			
			nddStoreList = Arrays.asList(nddStores.split(","));
			return nddStoreList;
		}
	

		public static List<PriceExportDTO> getRUdataOfItem(){
			
			List<PriceExportDTO> ruData = new ArrayList<>();
			
			ruData.add(new PriceExportDTO(25914, "92444", "PERFORMANCE AIR_RU", 335));
			ruData.add(new PriceExportDTO(25915, "92446", "PERFORMANCE AIR_RU",	335));
			ruData.add(new PriceExportDTO(25916, "92447", "PERFORMANCE AIR_RU",	335));
			ruData.add(new PriceExportDTO(25917,"92448","DRESS UP AIR FILTERS_RU",2949));
			ruData.add(new PriceExportDTO(25918,"92449", "DRESS UP AIR FILTERS_RU",	2949));
			ruData.add(new PriceExportDTO(25919, "92450", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25920, "92451", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25921, "92453", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25922, "92454", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25923, "92455", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25924, "92457", "DRESS UP AIR FILTERS_RU", 949));
			ruData.add(new PriceExportDTO(25925, "92458", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25926, "92459", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25927, "92461", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25928, "92462", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25929, "92463", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25930, "92464", "DRESS UP AIR FILTERS_RU", 2949));
			ruData.add(new PriceExportDTO(25931, "92466", "DRESS UP AIR FILTERS_RU", 2949));
			
			return ruData;
		}
		
		public static RetailCalendarDTO getCalendarDetailForWeek() {
			RetailCalendarDTO calDto = new RetailCalendarDTO(5435, "02/20/2022", "02/26/2022", 5001);
			return calDto;
		}
		
		public static List<PriceExportDTO> getEmergencyItemObjList() {
			List<PriceExportDTO> objectList = new ArrayList<>();
					
			objectList.add(new PriceExportDTO(392914, "C969", "S", "114864", 67.2, "02/28/2022", null, 365, 
					28, "prestolive", "4286", "26923", "16", "prestolive"));
			objectList.add(new PriceExportDTO(495183, "C1433", "S", "857011", 68.2, "02/28/2022", null, 365, 
					28, "prestolive", "4286", "26923", "16", "prestolive"));
			objectList.add(new PriceExportDTO(377718, "CP036", "S", "35057", 67.2, "02/28/2022", null, 365, 
					28, "prestolive", "4286", "26923", "16", "prestolive"));
			objectList.add(new PriceExportDTO(494124, "C969", "S", "GN10446", 60.2, "02/28/2022", null, 365, 
					28, "prestolive", "4286", "26923", "16", "prestolive"));			
			objectList.add(new PriceExportDTO(379746, "C969", "S", "48662", 67.2, "02/28/2022", null, 365, 
					28, "prestolive", "4286", "26923", "16", "prestolive"));
			
			
			return objectList;
		}
		
		public static HashMap<String, List<RetailPriceDTO>> getRetailPriceOfItems(){
			
			HashMap<String, List<RetailPriceDTO>> retailItem = new HashMap<>();
			List<RetailPriceDTO> objectList = new ArrayList<>();
			
			/*int calendarId, String itemcode, int levelTypeId, String levelId, float regPrice, int regQty,
			float regMPrice, float salePrice, int saleQty, float saleMPrice, String promotionFlag,
			String regEffectiveDate, String saleStartDate, String saleEndDate*/
			
			objectList.add(new RetailPriceDTO(5435, "392914", 0, "100", 60.0f, 1, 0.0f, 0.0f, 0, 0.0f, "N","", "", ""));
			objectList.add(new RetailPriceDTO(5435, "392914", 1, "16", 62.2f, 1, 0.0f, 0.0f, 0, 0.0f, "N","", "", ""));
			objectList.add(new RetailPriceDTO(5435, "495183", 1, "100", 66.2f, 1, 0.0f, 0.0f, 0, 0.0f, "N","", "", ""));
			objectList.add(new RetailPriceDTO(5435, "495183", 1, "49", 64.2f, 1, 0.0f, 0.0f, 0, 0.0f, "N","", "", ""));
			objectList.add(new RetailPriceDTO(5435, "495183", 1, "66", 70.2f, 1, 0.0f, 0.0f, 0, 0.0f, "N","", "", ""));
			objectList.add(new RetailPriceDTO(5435, "377718", 1, "100", 68.0f, 1, 0.0f, 0.0f, 0, 0.0f, "N","", "", ""));
			objectList.add(new RetailPriceDTO(5435, "494124", 1, "100", 61.0f, 1, 0.0f, 0.0f, 0, 0.0f, "N","", "", ""));
			objectList.add(new RetailPriceDTO(5435, "379746", 1, "100", 65.0f, 1, 0.0f, 0.0f, 0, 0.0f, "N","", "", ""));
			objectList.add(new RetailPriceDTO(5435, "379746", 1, "67", 68.0f, 1, 0.0f, 0.0f, 0, 0.0f, "N","", "", ""));
				
			retailItem = (HashMap<String, List<RetailPriceDTO>>) objectList.stream().collect(Collectors
					.groupingBy(RetailPriceDTO::getItemcode));
			
			return retailItem;
		}
}
