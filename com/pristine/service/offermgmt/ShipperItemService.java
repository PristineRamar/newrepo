package com.pristine.service.offermgmt;

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
import com.pristine.service.offermgmt.constraint.CostConstraint;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class ShipperItemService {
	private static Logger logger = Logger.getLogger("ShipperItemService");
	private PRItemDTO inputItem;
	private PRExplainLog explainLog;

	FilterPriceRange filterPriceRange = new FilterPriceRange();
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();

	public ShipperItemService(PRItemDTO inputItem, PRExplainLog explainLog) {
		this.inputItem = inputItem;
		this.explainLog = explainLog;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.PRE_PRICE.getConstraintTypeId());
	}

	public boolean isShipperItemHandled(List<PRItemDTO> prItemList, HashMap<ItemKey, PRItemDTO> itemDataMap) {
		boolean isShipperItemHandled = false;
		boolean isNonLigShipperItem = false;
		boolean isAllLigMemIsShipperItem = false;
		ItemKey itemKey = PRCommonUtil.getItemKey(inputItem);
		PricingEngineService pricingEngineService = new PricingEngineService();

		if (inputItem.getRetLirId() > 0) {
			ItemKey ik = new ItemKey(inputItem.getRetLirId(), PRConstants.LIG_ITEM_INDICATOR);
			if(itemDataMap.get(ik)!=null)
			{
				isAllLigMemIsShipperItem = itemDataMap.get(ik).isAllLigMemIsShipperItem();
			}
		} else {
			isNonLigShipperItem = inputItem.isShipperItem();
		}

		// 15th July 2016, if an non-lig item or all item in a lig is shipper
		// item, then retain current price
		if (isNonLigShipperItem || isAllLigMemIsShipperItem) {
			itemDataMap.get(itemKey).setProcessed(true);
			pricingEngineService.updateConflicts(inputItem);
			prItemList.add(inputItem);
			logger.debug("Current retail is retained as it is shipper item: item code:" + inputItem.getItemCode() + ",ret lir id:"
					+ inputItem.getRetLirId());
			recommendShipperItem();
			isShipperItemHandled = true;
		}
		return isShipperItemHandled;
	}

	private void recommendShipperItem() {
		MultiplePrice curPrice = null;
		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;

		logger.debug("Recommending shipper item is Started...");

		curPrice = PRCommonUtil.getMultiplePrice(inputItem.getRegMPack(), inputItem.getRegPrice(), inputItem.getRegMPrice());

		if (curPrice != null) {
			logger.debug("Current Price: " + curPrice.toString());

			priceRange.setStartVal(curPrice.price);
			priceRange.setEndVal(curPrice.price);

			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange);
			guidelineAndConstraintLog.setOutputPriceRange(priceRange);

			CostConstraint costConstraint = new CostConstraint(inputItem, priceRange, explainLog);

			// Update just cost and rounding range alone and don't apply it
			guidelineAndConstraintOutputLocal = costConstraint.getCostRange();
			priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;

			strategyDTO.getConstriants().getRoundingConstraint().getRoundingDigits(inputItem, priceRange, explainLog);

			inputItem.setPriceRange(new Double[] { curPrice.price });
//			inputItem.setRecommendedRegMultiple(curPrice.multiple);
//			inputItem.setRecommendedRegPrice(curPrice.price);
			inputItem.setRecommendedRegPrice(new MultiplePrice(curPrice.multiple, curPrice.price));
		} else {
			guidelineAndConstraintLog.setMessage("Current Price not available");
			explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		}

		inputItem.setExplainLog(explainLog);
		logger.debug("Recommending shipper item is Completed...");
//		logger.debug("Final Price: " + inputItem.getRecommendedRegMultiple() + "/" + inputItem.getRecommendedRegPrice());
		logger.debug("Final Price: " +  PRCommonUtil.getPriceForLog(inputItem.getRecommendedRegPrice()));
	}
}
