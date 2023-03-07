package com.pristine.service.offermgmt;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRGuidelinePI;
import com.pristine.dto.offermgmt.PRItemDTO;
//import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PriceIndexCalculation {
	private static Logger logger = Logger.getLogger("PriceIndexCalculation");
	
	public Double getFutureSimpleIndex(List<PRItemDTO> itemList, boolean preferenceToOverridePrice){
		logger.info("Item List Size - " + itemList.size());
		Double futurePI = null;
		futurePI = computeSimpleIndex(itemList, preferenceToOverridePrice, "FUTURE");		
		logger.info("Future PI - " + futurePI);
		return futurePI;
	}
	
	public Double getCurrentSimpleIndex(List<PRItemDTO> itemList){
		Double currentPI = null;
		currentPI = computeSimpleIndex(itemList, false, "CURRENT");
		return currentPI;
	}
	
	public Double getCurrentSimpleIndex(MultiplePrice basePrice, MultiplePrice compPrice){
		Double simpleIndex = null;
		double baseUnitPrice = 0;
		double compUnitPrice = 0;
		
		if(basePrice != null && compPrice != null) {
			String indexCalcType =  PropertyManager.getProperty("PI_CALC_TYPE", "").trim();
			baseUnitPrice = PRCommonUtil.getUnitPrice(basePrice, false);
			compUnitPrice = PRCommonUtil.getUnitPrice(compPrice, false);
			
			if (baseUnitPrice > 0) {
				simpleIndex = compUnitPrice / baseUnitPrice * 100;
				 
				if (indexCalcType.equals(Constants.PI_CALC_TYPE_REVERSE)) {
					simpleIndex = simpleIndex > 0 ? ((1 / (simpleIndex / 100)) * 100) : 0;
				}
			}

			if(simpleIndex != null)
				simpleIndex = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(simpleIndex));
		}
		
		 
		return simpleIndex;
	}
	
	/***
	 * Check if the item level price meets the category level index,
	 * if there is no price or comp price then it still return as 
	 * true, so that those items are not considered while price adjustment
	 * @param item
	 * @return
	 */
	public boolean isItemMeetFutureSimpleIndex(PRItemDTO item, PRGuidelinePI guidelinePI){
		boolean isPIMeet = false;
		List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
		itemList.add(item);
//		if (item.getRecommendedRegPrice() != null && item.getCompPrice() != null && item.getRecommendedRegPrice() > 0
//				&& item.getCompPrice().price > 0) {
		if (item.getRecommendedRegPrice() != null && item.getCompPrice() != null && item.getRecommendedRegPrice().price > 0
				&& item.getCompPrice().price > 0) {
			Double pi = getFutureSimpleIndex(itemList, true);	
			isPIMeet = isPriceIndexMeetsGoal(pi, guidelinePI);
			logger.debug("item level pi:" + pi + ",target:" + ". Target:" + "Min:" + guidelinePI.getMinValue()
					+ ",Max:" + guidelinePI.getMaxValue());
		} else {
			isPIMeet = true;
		}
		return isPIMeet;
	}
	
	private Double computeSimpleIndex(List<PRItemDTO> itemList, boolean preferenceToOverridePrice, String piType) {
		Double simpleIndex = null;
		double totalBasePrice = 0;
		double totalCompPrice = 0;

		String indexCalcType =  PropertyManager.getProperty("PI_CALC_TYPE", "").trim();
		 
		for (PRItemDTO item : itemList) {
			Double basePrice;
			Double overridePrice;
			double compPrice;

			if (piType.equals("FUTURE")) {
				if(item.getRecommendedRegPrice() == null){
					basePrice = 0d;
				}else {
//					MultiplePrice multiplePrice = new MultiplePrice(item.getRecommendedRegMultiple(), item.getRecommendedRegPrice());
					MultiplePrice multiplePrice = item.getRecommendedRegPrice();
					basePrice = PRCommonUtil.getUnitPrice(multiplePrice, false);
				}
			} else {
				basePrice = PRCommonUtil.getUnitPrice(item.getRegMPack(), item.getRegPrice(), item.getRegMPrice(), false);
			}

			if(item.getOverrideRegPrice() == null){
				overridePrice = 0d;
			}else {
				MultiplePrice multiplePrice = new MultiplePrice(item.getOverrideRegMultiple(), item.getOverrideRegPrice());
				overridePrice = PRCommonUtil.getUnitPrice(multiplePrice, false);
			}
			
			compPrice = PRCommonUtil.getUnitPrice(item.getCompPrice(), false) ;

			if ((!item.isLir()) && basePrice > 0 && compPrice > 0) {
				if (preferenceToOverridePrice) {
					totalBasePrice += (overridePrice > 0 ? overridePrice : basePrice);
				} else {
					totalBasePrice += basePrice;
				}
				totalCompPrice += compPrice;
			}
		}

		if (totalBasePrice > 0) {
			simpleIndex = totalCompPrice / totalBasePrice * 100;
			// 13th May 2016, Added reverse price index support
			if (indexCalcType.equals(Constants.PI_CALC_TYPE_REVERSE)) {
				simpleIndex = simpleIndex > 0 ? ((1 / (simpleIndex / 100)) * 100) : 0;
			}
		}

		if(simpleIndex != null)
			simpleIndex = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(simpleIndex));
		
		return simpleIndex;
	}
	
	public boolean isPriceIndexMeetsGoal(double curPriceIndex, PRGuidelinePI guidelinePI) {
		boolean isPIMeetsGoal = true;

		// Min alone present
		if (guidelinePI.getMinValue() != Constants.DEFAULT_NA && guidelinePI.getMaxValue() == Constants.DEFAULT_NA) {
			if(curPriceIndex < guidelinePI.getMinValue()){
				isPIMeetsGoal = false;
			}
		} else if (guidelinePI.getMaxValue() != Constants.DEFAULT_NA && guidelinePI.getMinValue() == Constants.DEFAULT_NA) {
			// Max alone present
			if(curPriceIndex > guidelinePI.getMaxValue()){
				isPIMeetsGoal = false;
			}
		} else if (guidelinePI.getMinValue() != Constants.DEFAULT_NA && guidelinePI.getMaxValue() != Constants.DEFAULT_NA) {
			// Both present
			if(curPriceIndex < guidelinePI.getMinValue()){
				isPIMeetsGoal = false;
			}
		}

		return isPIMeetsGoal;
	}
	
	
	public boolean isBadCompCheck(MultiplePrice basePrice, MultiplePrice compPrice, double minBadIndex, double maxBadIndex) {
		boolean isBadCompCheck = false;
		
		PriceIndexCalculation pic = new PriceIndexCalculation();
		Double simpleIndex =  pic.getCurrentSimpleIndex(basePrice, compPrice);
		
		//Is it below lower limit
		if(simpleIndex != null && minBadIndex > 0 && simpleIndex <= minBadIndex) {
			isBadCompCheck = true;
		} if(simpleIndex != null && maxBadIndex > 0 && simpleIndex >= maxBadIndex) {
			isBadCompCheck = true;
		}
		
		return isBadCompCheck;
	}
	
}
