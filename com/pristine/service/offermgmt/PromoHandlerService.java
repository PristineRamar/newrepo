package com.pristine.service.offermgmt;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PromoHandlerService {

	private static Logger logger = Logger.getLogger("PromoHandlerService");
	private PRItemDTO inputItem;
	private PRExplainLog explainLog;

	FilterPriceRange filterPriceRange = new FilterPriceRange();
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();

	public PromoHandlerService(PRItemDTO inputItem, PRExplainLog explainLog) {
		this.inputItem = inputItem;
		this.explainLog = explainLog;
	}

	public boolean isPromoItemHandled(List<PRItemDTO> prItemList, HashMap<ItemKey, PRItemDTO> itemDataMap,
			PRRecommendationRunHeader recommendationRunHeader) throws ParseException {
		boolean isPromoItemHandled = false;
		ItemKey itemKey = PRCommonUtil.getItemKey(inputItem);
		boolean isItemInLongTermPromo = isItemInLongTermPromo(itemDataMap, recommendationRunHeader);
		boolean isItemPromotedInFuture = inputItem.isItemPromotedForFutWeek();
		
		//NU:: 25th Nov 2016, if the item is promoted in future, then don't recommend
		if (isItemInLongTermPromo || isItemPromotedInFuture) {
			itemDataMap.get(itemKey).setProcessed(true);
			prItemList.add(inputItem);
			logger.debug("Current retail is retained as the item is in long term promotion for item code:" + inputItem.getItemCode() + ",ret lir id:"
					+ inputItem.getRetLirId());
			recommendLongTermPromoItem(isItemInLongTermPromo, isItemPromotedInFuture);
			isPromoItemHandled = true;
		}
		return isPromoItemHandled;
	}

	private void recommendLongTermPromoItem(boolean isItemInLongTermPromo, boolean isItemPromotedInFuture) {
		MultiplePrice curPrice = PRCommonUtil.getMultiplePrice(inputItem.getRegMPack(), inputItem.getRegPrice(), inputItem.getRegMPrice());
		logger.debug("Recommending long term promo item is Started...");

		if (curPrice != null) {
			inputItem.setPriceRange(new Double[] { curPrice.price });
//			inputItem.setRecommendedRegMultiple(curPrice.multiple);
//			inputItem.setRecommendedRegPrice(curPrice.price);
			inputItem.setRecommendedRegPrice(new MultiplePrice(curPrice.multiple, curPrice.price));
			
			if(isItemInLongTermPromo) {
				guidelineAndConstraintLog.setMessage("Current price is retained as the item is in long term promotion");
			} else {
				guidelineAndConstraintLog.setMessage("Current price is retained as the item is promoted in future weeks");
			}
			
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		} else {
			guidelineAndConstraintLog.setMessage("Current Price not available");
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		}

		inputItem.setExplainLog(explainLog);
		logger.debug("Recommending long term promo item is Completed...");
//		logger.debug("Final Price: " + inputItem.getRecommendedRegMultiple() + "/" + inputItem.getRecommendedRegPrice());
		logger.debug("Final Price: " +  PRCommonUtil.getPriceForLog(inputItem.getRecommendedRegPrice()));
	}
	
	private boolean isItemInLongTermPromo(HashMap<ItemKey, PRItemDTO> itemDataMap, PRRecommendationRunHeader recommendationRunHeader)
			throws ParseException {
		boolean isItemInLongTermPromo = false;
		int longTermPromoWeeks = Integer.parseInt(PropertyManager.getProperty("LONG_TERM_PROMO_WEEKS"));
		PricingEngineService pricingEngineService = new PricingEngineService();
		List<PRItemDTO> ligMembersOrNonLig = new ArrayList<PRItemDTO>();
		List<PRItemDTO> allItems = new ArrayList<PRItemDTO>();
		
		allItems.addAll(itemDataMap.values());
		
		//If lig get all lig members
		if(inputItem.getRetLirId() > 0) {
			List<PRItemDTO> ligMembers = pricingEngineService.getLigMembers(inputItem.getRetLirId(), allItems);
			ligMembersOrNonLig.addAll(ligMembers);
		} else {
			ligMembersOrNonLig.add(inputItem);
		}
		
		
		//Even if any one of the item in the LIG is long term promoted, then don't recommended for the entire LIG
		for (PRItemDTO itemDTO : ligMembersOrNonLig) {
			logger.debug("ItemCode:" + itemDTO.getItemCode() + ",promoDetails:" + itemDTO.getRecWeekSaleInfo().toString());
		
			// Item is promoted and has sale price,
			if (itemDTO.isItemPromotedForRecWeek() && itemDTO.getRecWeekSaleInfo().getSalePrice() != null
					&& itemDTO.getRecWeekSaleInfo().getSaleStartDate() != null) {
				// if there is start date, but no end date
				if (itemDTO.getRecWeekSaleInfo().getSaleStartDate() != null && itemDTO.getRecWeekSaleInfo().getSaleEndDate() == null) {
					isItemInLongTermPromo = true;
				} else {
					// promotion ends after 6 weeks from recommendation week
					long promoDuration = DateUtil.getDateDiff(itemDTO.getRecWeekSaleInfo().getSaleEndDate(), recommendationRunHeader.getStartDate());
					if ((promoDuration + 1) > 7 * longTermPromoWeeks) {
						isItemInLongTermPromo = true;
					}
				}
			}
			//Even one of the item is in long term promo
			if(isItemInLongTermPromo) {
				break;
			}
		}

		return isItemInLongTermPromo;
	}
}
