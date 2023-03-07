package com.pristine.service.offermgmt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.constraint.CostConstraint;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class NoMovementItemService {
	private static Logger logger = Logger.getLogger("NoMovementItemService");
	private PRItemDTO inputItem;
	private PRExplainLog explainLog;

	FilterPriceRange filterPriceRange = new FilterPriceRange();
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();

	public NoMovementItemService(PRItemDTO inputItem, PRExplainLog explainLog) {
		this.inputItem = inputItem;
		this.explainLog = explainLog;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.PRE_PRICE.getConstraintTypeId());
	}

	public boolean isNoMovementItemHandled(List<PRItemDTO> prItemList, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		boolean isNoMovementItemHandled = false;
		long lastXWeeksMov = 0;
		ItemKey itemKey = PRCommonUtil.getItemKey(inputItem);
		PricingEngineService pricingEngineService = new PricingEngineService();

		if (inputItem.getRetLirId() > 0) {
			//Take lig movement
			ItemKey ik = new ItemKey(inputItem.getRetLirId(), PRConstants.LIG_ITEM_INDICATOR);
			lastXWeeksMov = itemDataMap.get(ik).getLastXWeeksMov();
		} else {
			lastXWeeksMov = inputItem.getLastXWeeksMov();
		}

		// 15th July 2016, if an non-lig item or all item in a lig didn't move
		// in last 2 years
		if (lastXWeeksMov == 0) {
			itemDataMap.get(itemKey).setProcessed(true);
			pricingEngineService.updateConflicts(inputItem);
			prItemList.add(inputItem);
			logger.debug("Current retail is retained as it there is no movement: item code:" + inputItem.getItemCode() + ",ret lir id:"
					+ inputItem.getRetLirId());
			recommendNoMovementItem();
			isNoMovementItemHandled = true;
			List<String> additionalDetails = new ArrayList<String>();
			   additionalDetails.add(String.valueOf(Integer.parseInt(PropertyManager.getProperty("PR_MOV_HISTORY_WEEKS"))));
			   pricingEngineService.writeAdditionalExplainLog(inputItem, new ArrayList<PRItemDTO>(),
			     ExplainRetailNoteTypeLookup.CUR_REG_RETAINED_NO_MOV_IN_LAST_X_YEARS, additionalDetails);
		}
		return isNoMovementItemHandled;
	}

	private void recommendNoMovementItem() {
		MultiplePrice curPrice = null;
		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;

		logger.debug("Recommending no movement item is Started...");

		curPrice = PRCommonUtil.getMultiplePrice(inputItem.getRegMPack(), inputItem.getRegPrice(), inputItem.getRegMPrice());

		if (curPrice != null) {
			logger.debug("Current Price: " + curPrice.toString());
			
			//NU:: 18th Nov 2016, if the current retail is multiple, after applying the objective,
			//it becomes single qty item. e.g. if current retail is 2/5, the recommended retail
			//becomes 1/5. So unit price is assigned here
//			priceRange.setStartVal(curPrice.price);
//			priceRange.setEndVal(curPrice.price);
			
			double unitPrice = PRCommonUtil.getUnitPrice(curPrice, true);
			priceRange.setStartVal(unitPrice);
			priceRange.setEndVal(unitPrice);

			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange);
			guidelineAndConstraintLog.setOutputPriceRange(priceRange);

			CostConstraint costConstraint = new CostConstraint(inputItem, priceRange, explainLog);

			// Update just cost and rounding range alone and don't apply it
			guidelineAndConstraintOutputLocal = costConstraint.getCostRange();
			priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;

			strategyDTO.getConstriants().getRoundingConstraint().getRoundingDigits(inputItem, priceRange, explainLog);

//			inputItem.setPriceRange(new Double[] { curPrice.price });
//			inputItem.setRecommendedRegMultiple(curPrice.multiple);
//			inputItem.setRecommendedRegPrice(curPrice.price);
			
			inputItem.setPriceRange(new Double[] { unitPrice });
//			inputItem.setRecommendedRegMultiple(1);
//			inputItem.setRecommendedRegPrice(unitPrice);
			inputItem.setRecommendedRegPrice(new MultiplePrice(1, unitPrice));
		} else {
			guidelineAndConstraintLog.setMessage("Current Price not available");
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		}

		inputItem.setExplainLog(explainLog);
		logger.debug("Recommending no movement item is Completed...");
//		logger.debug("Final Price: " + inputItem.getRecommendedRegMultiple() + "/" + inputItem.getRecommendedRegPrice());
		logger.debug("Final Price: " +  PRCommonUtil.getPriceForLog(inputItem.getRecommendedRegPrice()));
	}
}
