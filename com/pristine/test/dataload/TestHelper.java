package com.pristine.test.dataload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.ScanBackDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleAllowanceDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleCostDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEaglePriceDTO;
import com.pristine.util.Constants;

public class TestHelper {

	public static void setCostDetails(List<GiantEagleCostDTO> costList, String upc, String whItemNo, String startDate, String costZoneNo,
			String splrNo, String costStatCode, double bsCstAkaStoreCst, double dlvdCstAkaWhseCst, String longTermReflectFlag, String bnrCd) {

		GiantEagleCostDTO giantEagleCostDTO = new GiantEagleCostDTO();
		giantEagleCostDTO.setUPC(upc);
		giantEagleCostDTO.setWHITEM_NO(whItemNo);
		giantEagleCostDTO.setSTRT_DTE(startDate);
		giantEagleCostDTO.setCST_ZONE_NO(costZoneNo);
		giantEagleCostDTO.setSPLR_NO(splrNo);
		giantEagleCostDTO.setCST_STAT_CD(costStatCode);
		giantEagleCostDTO.setBS_CST_AKA_STORE_CST(bsCstAkaStoreCst);
		giantEagleCostDTO.setDLVD_CST_AKA_WHSE_CST(dlvdCstAkaWhseCst);
		giantEagleCostDTO.setLONG_TERM_REFLECT_FG(longTermReflectFlag);
		giantEagleCostDTO.setBNR_CD(bnrCd);
		costList.add(giantEagleCostDTO);
	}
	
	public static void setAllwDetails(List<GiantEagleAllowanceDTO> allowanceList, String upc, String whItemNo, String startDate, String endDate, 
			String costZoneNo, String splrNo, String allwStatCode, double allwAmt, double dealCost, String longTermReflectFlag, String bnrCd){
		GiantEagleAllowanceDTO giantEagleAllowanceDTO = new GiantEagleAllowanceDTO();
		giantEagleAllowanceDTO.setUPC(upc);
		giantEagleAllowanceDTO.setWHITEM_NO(whItemNo);
		giantEagleAllowanceDTO.setSTRT_DTE(startDate);
		giantEagleAllowanceDTO.setEND_DTE(endDate);
		giantEagleAllowanceDTO.setCST_ZONE_NO(costZoneNo);
		giantEagleAllowanceDTO.setSPLR_NO(splrNo);
		giantEagleAllowanceDTO.setALLW_STAT_CD(allwStatCode);
		giantEagleAllowanceDTO.setALLW_AMT(allwAmt);
		giantEagleAllowanceDTO.setDEAL_CST(dealCost);
		giantEagleAllowanceDTO.setLONG_TERM_REFLECT_FG(longTermReflectFlag);
		giantEagleAllowanceDTO.setBNR_CD(bnrCd);
		allowanceList.add(giantEagleAllowanceDTO);
	}
	
	public static void setRetailCalendar(HashMap<String, RetailCalendarDTO> calendarDetails, String startDate, String weekStartDate,
			String weekEndDate) {
		RetailCalendarDTO retailCalendarDTO = new RetailCalendarDTO();
		retailCalendarDTO.setStartDate(weekStartDate);
		retailCalendarDTO.setEndDate(weekEndDate);
		calendarDetails.put(startDate, retailCalendarDTO);
	}
	
	public static void setExpectedItemOutput(HashMap<String, HashMap<String, GiantEagleCostDTO>> multipleWeekCostDetails, 
			String costEffDate, String upc, String whItemNo, String startDate, String costZoneNo, String splrNo,
			String costStatCode, double bsCstAkaStoreCst, double dlvdCstAkaWhseCst, String longTermReflectFlag, String bnrCd, 
			String dealStartDate, String dealEndDate, double dealCost, double allwAmt){
		GiantEagleCostDTO giantEagleCostDTO = new GiantEagleCostDTO();
		giantEagleCostDTO.setUPC(upc);
		giantEagleCostDTO.setWHITEM_NO(whItemNo);
		giantEagleCostDTO.setSTRT_DTE(startDate);
		giantEagleCostDTO.setCST_ZONE_NO(costZoneNo);
		giantEagleCostDTO.setSPLR_NO(splrNo);
		giantEagleCostDTO.setCST_STAT_CD(costStatCode);
		giantEagleCostDTO.setBS_CST_AKA_STORE_CST(bsCstAkaStoreCst);
		giantEagleCostDTO.setDLVD_CST_AKA_WHSE_CST(dlvdCstAkaWhseCst);
		giantEagleCostDTO.setLONG_TERM_REFLECT_FG(longTermReflectFlag);
		giantEagleCostDTO.setBNR_CD(bnrCd);
		giantEagleCostDTO.setDealCost(dealCost);
		giantEagleCostDTO.setALLW_AMT(allwAmt);
		giantEagleCostDTO.setDealStartDate(dealStartDate);
		giantEagleCostDTO.setDealEndDate(dealEndDate);
		String key = bnrCd + "-" + upc + "-" + costZoneNo;
		if ((!Constants.EMPTY.equals(splrNo) && splrNo != null)) {
			key = key + "-" + splrNo;
			giantEagleCostDTO.setDLVD_CST_AKA_WHSE_CST(giantEagleCostDTO.getBS_CST_AKA_STORE_CST());
		}
		
		HashMap<String, GiantEagleCostDTO> itemAndCostDetails = new HashMap<>();
		itemAndCostDetails.put(key, giantEagleCostDTO);
		multipleWeekCostDetails.put(costEffDate, itemAndCostDetails);
	}
	
	public static void setScanBackDetails(List<ScanBackDTO> scanBackInputList, String retailerItemCode, String zoneNum, String splrNo, String bnrCode, 
			String scanBackAmt, String scanBackStartDate, String scanBackEndDate){
		ScanBackDTO scanBackDTO = new ScanBackDTO();
		scanBackDTO.setRetailerItemCode(retailerItemCode);
		scanBackDTO.setZoneNumber(zoneNum);
		scanBackDTO.setSplrNo(splrNo);
		scanBackDTO.setBnrCD(bnrCode);
		scanBackDTO.setScanBackAmt(scanBackAmt);
		scanBackDTO.setScanBackStartDate(scanBackStartDate);
		scanBackDTO.setScanBackEndDate(scanBackEndDate);
		scanBackInputList.add(scanBackDTO);
	}
	
	public static void setScanbackExpectedOutput(HashMap<String, HashMap<String, ScanBackDTO>> expectedScanbackDetails, 
			String retailerItemCode, String zoneNum, String splrNo, String bnrCode, float scanBackTotalAmt, 
			String scanBackAmt1, String scanBackStartDate1, String scanBackEndDate1, 
			String scanBackAmt2, String scanBackStartDate2, String scanBackEndDate2, 
			String scanBackAmt3, String scanBackStartDate3, String scanBackEndDate3, String processingWeek) {
		
		HashMap<String, ScanBackDTO> scanBackMap = new HashMap<>();
		ScanBackDTO scanBackDTO = new ScanBackDTO();
		scanBackDTO.setRetailerItemCode(retailerItemCode);
		scanBackDTO.setZoneNumber(zoneNum);
		scanBackDTO.setSplrNo(splrNo);
		scanBackDTO.setBnrCD(bnrCode);
		scanBackDTO.setScanBackAmt1(scanBackAmt1);
		scanBackDTO.setScanBackStartDate1(scanBackStartDate1);
		scanBackDTO.setScanBackEndDate1(scanBackEndDate1);
		scanBackDTO.setScanBackAmt2(scanBackAmt2);
		scanBackDTO.setScanBackStartDate2(scanBackStartDate2);
		scanBackDTO.setScanBackEndDate2(scanBackEndDate2);
		scanBackDTO.setScanBackAmt3(scanBackAmt3);
		scanBackDTO.setScanBackStartDate3(scanBackStartDate3);
		scanBackDTO.setScanBackEndDate3(scanBackEndDate3);
		scanBackDTO.setScanBackTotalAmt(scanBackTotalAmt);

		String key = scanBackDTO.getRetailerItemCode() + "-" + scanBackDTO.getBnrCD() + "-" + scanBackDTO.getZoneNumber();
		if (scanBackDTO.getSplrNo() != null && !scanBackDTO.getSplrNo().isEmpty()) {
			key = key + "-" + scanBackDTO.getSplrNo();
		}
		scanBackMap.put(key, scanBackDTO);
		expectedScanbackDetails.put(processingWeek, scanBackMap);
	}
	
	public static void setUPCMap(String upc, String prcGrpCd, HashMap<String, List<ItemDTO>> upcMap){
		
		List<ItemDTO> itemList = new ArrayList<>();
		if(upcMap.containsKey(upc)){
			itemList = upcMap.get(upc);
		}
		ItemDTO itemDTO = new ItemDTO();
		itemDTO.setUpc(upc);
		itemDTO.setPrcGrpCd(prcGrpCd);
		itemList.add(itemDTO);
		upcMap.put(upc, itemList);
	}
	
	
	public static GiantEaglePriceDTO getGEPriceDTO(String RITEM_NO, String lowPriceFlag, String newLowPriceFlag,
			String lowPriceEndDate, String newLowPriceEndDate, String bannerCode,String zoneNo, String prcGrpCode, String splrNo) {
		GiantEaglePriceDTO giantEaglePriceDTO = new GiantEaglePriceDTO();
		giantEaglePriceDTO.setRITEM_NO(RITEM_NO);
		giantEaglePriceDTO.setLOW_PRC_FG(lowPriceFlag);
		giantEaglePriceDTO.setLOW_PRC_END_DTE(lowPriceEndDate);
		giantEaglePriceDTO.setNEW_LOW_PRC_FG(newLowPriceFlag);
		giantEaglePriceDTO.setNEW_LOW_PRC_END_DTE(newLowPriceEndDate);
		giantEaglePriceDTO.setBNR_CD(bannerCode);
		giantEaglePriceDTO.setZN_NO(zoneNo);
		giantEaglePriceDTO.setPRC_GRP_CD(prcGrpCode);
		giantEaglePriceDTO.setSPLR_NO(splrNo);
		return giantEaglePriceDTO;
	}
	
}
