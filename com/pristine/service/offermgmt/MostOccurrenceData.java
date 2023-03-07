package com.pristine.service.offermgmt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

//import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
//import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRCommonUtil;

public class MostOccurrenceData {

	public Object getMaxOccurance(Collection<PRItemDTO> itemList, String propName) {
		List<MaxOccurance> maxOccurances = new ArrayList<MaxOccurance>();
		Object output = null;
		Object object = null;

		for (PRItemDTO item : itemList) {
			MaxOccurance maxOccurance = new MaxOccurance();
			boolean isAlreadyInList = false;
			
			if (propName == "ItemSize") {
				object = returnNullIfZeroDouble(item.getItemSize());
			} else if (propName == "UOM") {
				object = item.getUOMName();
			} else if (propName == "UOMId") {
				object = item.getUOMId();
			} else if (propName == "DeptProductId") {
				object = returnNullIfZeroInt(item.getDeptProductId());
			} else if (propName == "PortProductId") {
				object = returnNullIfZeroInt(item.getPortfolioProductId());
			} else if (propName == "CatProductId") {
				object = returnNullIfZeroInt(item.getCategoryProductId());
			} else if (propName == "SubCatProductId") {
				object = returnNullIfZeroInt(item.getSubCatProductId());
			} else if (propName == "SegProductId") {
				object = returnNullIfZeroInt(item.getSegmentProductId());
			} else if (propName == "CurRegMPack") {
				object = returnNullIfZeroInt(item.getRegMPack());
			} else if (propName == "CurRegPrice") {
				object = returnNullIfZeroDouble(item.getRegPrice());
			} else if (propName == "CurRegMPrice") {
				object = returnNullIfZeroDouble(item.getRegMPrice());				
			} else if (propName == "CurRegPriceInMultiples") {
				object = PRCommonUtil.getCurRegPrice(item);
			} else if (propName == "PreRegPrice") {
				object = returnNullIfZeroDouble(item.getPreRegPrice());
			} else if (propName == "RegPriceEffDate") {
				object = item.getCurRegPriceEffDate();
			} else if (propName == "CurListCost") {
				object = returnNullIfZeroDouble(item.getListCost());
			} else if (propName == "PreListCost") {
				object = returnNullIfZeroDouble(item.getPreListCost());
			} else if (propName == "CurVipCost") {
				object = returnNullIfZeroDouble(item.getVipCost());
			} else if (propName == "PreVipCost") {
				object = returnNullIfZeroDouble(item.getPreVipCost());
			} else if (propName == "CurCompRegPrice") {
				//object = returnNullIfZeroDouble(item.getActCompRegPrice());
				object = item.getCompPrice();
			} else if (propName == "PreCompRegPrice") {
				//object = returnNullIfZeroDouble(item.getActPreCompRegPrice());
				object = item.getCompPreviousPrice();
			} else if (propName == "CompPriceCheckDate") {
				object = item.getCompPriceCheckDate();
			} else if (propName == "RecRegPrice") {
				if (item.getRecommendedRegPrice() != null) {
//					MultiplePrice multiplePrice = new MultiplePrice(item.getRecommendedRegMultiple(),
//							item.getRecommendedRegPrice());
					// object = item.getRecommendedRegPrice();
//					object = multiplePrice;
					object = item.getRecommendedRegPrice();
				}
			} else if(propName == "CurListCostEffDate"){
				object = item.getListCostEffDate();
			} else if (propName == "VendorId") {
				object = item.getVendorId();
			} else if (propName == "PredictionStatus") {
				object = item.getPredictionStatus();				
			} else if (propName == "OverridePredictionStatus") {
				object = item.getOverridePredictionStatus();				
			} else if (propName == "PrePrice") {
				object = returnNullIfZeroInt(item.getIsPrePriced());
			} else if (propName == "LocPrice") {
				object = returnNullIfZeroInt(item.getIsLocPriced());
			} else if (propName == "DistFlag"){
				object = item.getDistFlag();
			} else if (propName == "NoOfAuthorizedStores"){
				object = item.getNoOfStoresItemAuthorized();
			} else if (propName == "CurSaleMultiplePrice") {
				object = item.getCurSaleInfo().getSalePrice();
//			} else if (propName == "CurSalePromoTypeId") {
//				object = item.getCurSaleInfo().getSalePromoTypeLookup() != null ? item.getCurSaleInfo().getSalePromoTypeLookup().getPromoTypeId() : null;
//			} else if (propName == "CurSaleStartDate") {
//				object = item.getCurSaleInfo().getSaleStartDate();
//			} else if (propName == "CurSaleEndDate") {
//				object = item.getCurSaleInfo().getSaleEndDate();
//			} else if (propName == "CurSaleCost") {
//				object = item.getCurSaleInfo().getSaleCost();
			} else if (propName == "RecWeekSaleMultiplePrice") {
				object = item.getRecWeekSaleInfo().getSalePrice();
			} else if (propName == "RecWeekSalePromoTypeId") {
				object = item.getRecWeekSaleInfo() != null && item.getRecWeekSaleInfo().getPromoTypeId()>0
						? item.getRecWeekSaleInfo().getPromoTypeId() : null;
			} else if (propName == "RecWeekSaleStartDate") {
				object = item.getRecWeekSaleInfo().getSaleStartDate();
			} else if (propName == "RecWeekSaleEndDate") {
				object = item.getRecWeekSaleInfo().getSaleEndDate();
			} else if (propName == "RecWeekSaleWeekStartDate") {
				object = item.getRecWeekSaleInfo().getSaleWeekStartDate();
			} else if (propName == "RecWeekSaleCost") {
				object = item.getRecWeekSaleCost();
			} else if (propName == "FutWeekSaleMultiplePrice") {
				object = item.getFutWeekSaleInfo().getSalePrice();
			} else if (propName == "FutWeekSalePromoTypeId") {
				object = item.getFutWeekSaleInfo() != null && item.getFutWeekSaleInfo().getPromoTypeId()>0
						? item.getFutWeekSaleInfo().getPromoTypeId() : null;
			} else if (propName == "FutWeekSaleStartDate") {
				object = item.getFutWeekSaleInfo().getSaleStartDate();
			} else if (propName == "FutWeekSaleWeekStartDate") {
				object = item.getFutWeekSaleInfo().getSaleWeekStartDate();
			} else if (propName == "FutWeekSaleEndDate") {
				object = item.getFutWeekSaleInfo().getSaleEndDate();
//			} else if (propName == "FutWeekSaleCost") {
//				object = item.getFutWeekSaleInfo().getSaleCost();
//			} else if (propName == "CurAdStartDate") {
//				object = item.getCurAdInfo().getWeeklyAdStartDate();
//			} else if (propName == "CurAdPageNo") {
//				object = item.getCurAdInfo().getAdPageNo();
//			} else if (propName == "CurAdBlockNo") {
//				object = item.getCurAdInfo().getAdBlockNo();
			} else if (propName == "RecWeekAdStartDate") {
				object = item.getRecWeekAdInfo().getWeeklyAdStartDate();
			} else if (propName == "RecWeekAdPageNo") {
				object = returnNullIfZeroInt(item.getRecWeekAdInfo().getAdPageNo());
			} else if (propName == "RecWeekAdBlockNo") {
				object = returnNullIfZeroInt(item.getRecWeekAdInfo().getAdBlockNo());
			} else if (propName == "FutWeekAdStartDate") {
				object = item.getFutWeekAdInfo().getWeeklyAdStartDate();
			} else if (propName == "FutWeekAdPageNo") {
				object = returnNullIfZeroInt(item.getFutWeekAdInfo().getAdPageNo());
			} else if (propName == "FutWeekAdBlockNo") {
				object = returnNullIfZeroInt(item.getFutWeekAdInfo().getAdBlockNo());
//			} else if (propName == "CurDisplayStartDate") {
//				object = item.getCurDisplayInfo().getDisplayWeekStartDate();
//			} else if (propName == "CurDisplayTypeId") {
//				object = item.getCurDisplayInfo().getDisplayTypeLookup() != null ? item.getCurDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : null;
			} else if (propName == "RecWeekDisplayStartDate") {
				object = item.getRecWeekDisplayInfo().getDisplayWeekStartDate();
			} else if (propName == "RecWeekDisplayTypeId") {
				object = item.getRecWeekDisplayInfo().getDisplayTypeLookup() != null && item.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId()>0
						? item.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : null;
			} else if (propName == "FutWeekDisplayStartDate") {
				object = item.getFutWeekDisplayInfo().getDisplayWeekStartDate();
			} else if (propName == "FutWeekDisplayTypeId") {
				object = item.getFutWeekDisplayInfo().getDisplayTypeLookup() != null && item.getFutWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId()>0
						? item.getFutWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : null;
			} else if (propName == "CurCompSalePrice") {
				object = item.getCompCurSalePrice();
//			} else if (propName == "CurSalePredStatus") {
//				object = item.getCurSaleInfo().getSalePredStatus() != null ? item.getCurSaleInfo().getSalePredStatus().getStatusCode() : null;
			} else if (propName == "RecWeekSalePredStatusAtCurReg") {
				object = item.getRecWeekSaleInfo().getSalePredStatusAtCurReg() != null
						? item.getRecWeekSaleInfo().getSalePredStatusAtCurReg().getStatusCode() : null;
			} else if (propName == "RecWeekSalePredStatusAtRecReg") {
				object = item.getRecWeekSaleInfo().getSalePredStatusAtRecReg() != null
						? item.getRecWeekSaleInfo().getSalePredStatusAtRecReg().getStatusCode() : null;
			} else if (propName == "RecPriceEffectiveDate") {
				object = item.getRecPriceEffectiveDate();
			} else if (propName == "FutureRecRetail") {
				object = item.getFutureRecRetail();
			} else if (propName == "DealCost") {
				object = returnNullIfZeroDouble(item.getDealCost());
			} else if (propName == "minDealCost") {
				object = returnNullIfZeroDouble(item.getMinDealCost());
			} else if (propName == "RecWeekDealCost") {
				object = returnNullIfZeroDouble(item.getRecWeekDealCost());
			} else if (propName == "BrandId") {
				object = returnNullIfZeroInt(item.getBrandId());
			} else if (propName == "BrandName") {
				object = item.getBrandName();
			} else if (propName == "CoreCost") {
				object = returnNullIfZeroDouble(item.getCwacCoreCost());
			} else if (propName == "CoreRetail") {
				object = returnNullIfZeroDouble(item.getCoreRetail());
			}else if (propName == "OriginalListCost") {
				object = returnNullIfZeroDouble(item.getOriginalListCost());
			}
			if (object != null) {
				// Get from ArrayList
				for (MaxOccurance mod : maxOccurances) {
					if (mod.actualValue.equals(object)) {
						maxOccurance = mod;
						isAlreadyInList = true;
						break;
					}
				}
				//Keep object and its occurrence count e.g. object1 3, object2 2, object3 1
				maxOccurance.actualValue = object;
				maxOccurance.occuranceCount = maxOccurance.occuranceCount + 1;
				maxOccurance.totalMovement = maxOccurance.totalMovement + item.getAvgMovement();

				if (!isAlreadyInList)
					maxOccurances.add(maxOccurance);
			}
		}
		if (maxOccurances.size() > 0) {
			Collections.sort(maxOccurances, new MaxOccuranceComparator());
			// sortByMultipleValues(maxOccurances);
			output = maxOccurances.get(0).actualValue;
			}
			
		return output;
	}

	private Object returnNullIfZeroDouble(Double val) {
		Object object = null;
		if (val != null && val == 0)
			object = null;
		else
			object = val;

		return object;
	}
	
	//Ignore properties with 0 value
	private Object returnNullIfZeroInt(Integer val) {
		Object object = null;
		if (val != null && val == 0)
			object = null;
		else
			object = val;

		return object;
	}
	
	public Object getMaxOccurance(List<?> inputList) {
		List<MaxOccurance> maxOccurances = new ArrayList<MaxOccurance>();
		Object output = null;

		for (Object object : inputList) {
			MaxOccurance maxOccurance = new MaxOccurance();
			boolean isAlreadyInList = false;

			if (object != null) {
				// Get from ArrayList
				for (MaxOccurance mod : maxOccurances) {
					if (mod.actualValue.equals(object)) {
						maxOccurance = mod;
						isAlreadyInList = true;
						break;
					}
				}
				// Keep object and its occurrence count e.g. object1 3, object2 2, object3 1
				maxOccurance.actualValue = object;
				maxOccurance.occuranceCount = maxOccurance.occuranceCount + 1;

				if (!isAlreadyInList)
					maxOccurances.add(maxOccurance);
			}
		}
		if (maxOccurances.size() > 0) {
			Collections.sort(maxOccurances, new MaxOccuranceComparator());
			output = maxOccurances.get(0).actualValue;
		}

		return output;
	}
}

class MaxOccuranceComparator implements Comparator<MaxOccurance> {
	public int compare(MaxOccurance a, MaxOccurance b) {
		Integer occuranceCount = b.occuranceCount.compareTo(a.occuranceCount);
		return occuranceCount == 0 ? b.totalMovement.compareTo(a.totalMovement) : occuranceCount;
	}
}

class MaxOccurance {
	public Object actualValue;
	public Integer occuranceCount = 0;
	public Double totalMovement = 0d;
}
