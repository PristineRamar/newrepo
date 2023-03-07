package com.pristine.dto.offermgmt.promotion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.itemClassification.ItemClassificationDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PromoEngineDataDTO {
	
	private static PromoEngineDataDTO promoEngineDataDTO = null;
	
	//private constructor 
	private PromoEngineDataDTO(){}
	
	//Getter method to retrieve the singleton instance of this data holder
	public static PromoEngineDataDTO getPromoEngineDataDTO(){
		if(promoEngineDataDTO != null){	
		} else {
			promoEngineDataDTO = new PromoEngineDataDTO();
		}
		return promoEngineDataDTO;
	}

	public List<RetailCalendarDTO> calendarsForBaseData = new ArrayList<RetailCalendarDTO>();

	//Full calendar list
	public List<RetailCalendarDTO> retailCalendarCache = new ArrayList<RetailCalendarDTO>();

	//Authorized items for the store list and given departments
	public HashMap<ProductKey, PRItemDTO> authroizedItemMap = new HashMap<ProductKey, PRItemDTO>();
	
	//HashMap <Dept id, HashMap<Authorized items>
	public HashMap<Integer, HashMap<ProductKey, PRItemDTO>> authroizedItemMapDeptWise = new HashMap<Integer, HashMap<ProductKey, PRItemDTO>>();
	
	//Items datamap for price and cost functions
	public HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();

	// HashMap<ItemCode, HashMap<WeekStartDate, PRItemSaleInfoDTO>>
	public HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>> saleDetailMap = new HashMap<ProductKey, HashMap<String, PRItemSaleInfoDTO>>();

	// HashMap<ItemCode, HashMap<WeekStartDate, PRItemAdInfoDTO>>
	public HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>> adDetailMap = new HashMap<ProductKey, HashMap<String, PRItemAdInfoDTO>>();
	
	//HashMap <ItemCode , List<Min X Sale Prices>
//	public HashMap<Integer, List<Double>>  minXSalePercentages = new HashMap<Integer, List<Double>>();	
	
	//HashMap <ItemCode , List<Min X Sale Prices>
//	public HashMap<Integer, List<MultiplePrice>>  minXMultiplePrices = new HashMap<Integer, List<MultiplePrice>>();	
	
	//List<ItemCode> Items on BOGO
//	public Set<Integer> itemsOnBOGOinLastXWeeks = new HashSet<Integer>();
	
	//Past sale price
	public HashMap<Integer, Set<SalePriceKey>>  pastSalePrices = new HashMap<Integer, Set<SalePriceKey>>();	
	
	
	public HashMap<Integer, ArrayList<PromoAdSaleInfo>> adDetailsHistory = new HashMap<Integer, ArrayList<PromoAdSaleInfo>>();	
	
	// HashMap<ItemCode, HashMap<WeekStartDate, PRItemDisplayInfoDTO>>
	public HashMap<ProductKey, HashMap<String, PRItemDisplayInfoDTO>> displayDetailMap = new HashMap<ProductKey, HashMap<String, PRItemDisplayInfoDTO>>();

	//HashMap<ItemCode, Item classification data>
	public HashMap<ProductKey, ItemClassificationDTO> itemClassificationMap = new HashMap<ProductKey, ItemClassificationDTO>();
	
	//HashMap <ItemCode, Count of recommended Households for this item>
	public HashMap<ProductKey, ItemClassificationDTO> hhRecommendedItemMap = new HashMap<ProductKey, ItemClassificationDTO>();

	//HashMap<LIG item code, List<LIG members>> - Mapping of lig item code and lig members
	public HashMap<Integer, List<PRItemDTO>> ligAndItsMember = new HashMap<Integer, List<PRItemDTO>>();
	
	public HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsInBlocks = new HashMap<PageBlockNoKey, List<PromoItemDTO>>();
	
	
	//List of Actual Ad items listed in Promotion week
	public AdDetail actualAdDetail = new AdDetail();
		
	//The list non perishable departments in the promotion process
	public Set<Integer> productIdsToProcess = new HashSet<Integer>();
	
	//The list of stores present in the TOPS chain
	public List<Integer> allStores  = new ArrayList<Integer>();
	
	//HashMap<StartDate, CalendarData>
	public HashMap<String, RetailCalendarDTO> allWeekPromoCalendarDetails = new HashMap<String, RetailCalendarDTO>();
	
	//The recommendation week for which Promotion Engine is running
	public RetailCalendarDTO recWeekCalDTO = new RetailCalendarDTO();
	
	//HashMap<Item, HashMap<Store, Zone>>
	public HashMap<Integer, HashMap<Integer, Integer>> itemStoreMap  = new HashMap<Integer, HashMap<Integer, Integer>>();
	
	//HashMap<DeptId, List<BlockNo>>
	public HashMap<Integer, List<PageBlockNoKey>> deptBlockMap  = new HashMap<Integer, List<PageBlockNoKey>>();
	
	public HashMap<ProductKey, ItemClassificationDTO> itemClassificationFullData = new HashMap<ProductKey, ItemClassificationDTO>();

	//Method to retrieve the String format of NP departments being processed
	public String getDepartmentListAsString(){
		return PRCommonUtil.getCommaSeperatedStringFromIntSet(productIdsToProcess);
	}
	
	public HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfCategoryOnFirstPage = new HashMap<ProductKey, HashMap<PromoTypeLookup, Double>>();
	public HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> avgSaleMarPCTOfItemOnFirstPage = new HashMap<ProductKey, HashMap<PromoTypeLookup, Double>>();
	public HashMap<ProductKey, HashMap<PromoTypeLookup, Double>> minDealCostOfItemOnAd = new HashMap<ProductKey, HashMap<PromoTypeLookup, Double>>();
	
	
	// Strategy Map
	public HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<>();
	
	public PRStrategyDTO inputDTO = new PRStrategyDTO();
	
	public HashMap<ItemKey, List<PriceCheckListDTO>> priceCheckListInfo = new HashMap<>();
	
	public HashMap<RecWeekKey,HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap = new HashMap<>();
	
	public HashMap<RecWeekKey, HashMap<Integer, HashMap<ProductKey, List<PromoItemDTO>>>> itemGroups = new HashMap<>();
	
	public HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = new HashMap<String, RetailCalendarDTO>();
}
