package com.pristine.service.offermgmt.constraint;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PrePriceConstraint {
	private static Logger logger = Logger.getLogger("PrePriceConstraint");
	private PRItemDTO inputItem;
	private PRExplainLog explainLog;
	
	FilterPriceRange filterPriceRange = new FilterPriceRange();	
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
	
	public PrePriceConstraint(PRItemDTO inputItem, PRExplainLog explainLog) {
		this.inputItem = inputItem;
		this.explainLog = explainLog;		
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.PRE_PRICE.getConstraintTypeId());
	}
	
	public void applyPrePriceConstraint() {
		//If there is no current price, then take first rounding digit, as presto can never
		//recommend null or 0 price
		double prePrice = 0;
		int multiple = 1;
		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();
		PRRange priceRange = new PRRange();
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutputLocal;
		Double[] priceRangeArr = null;
		
		logger.debug("Applying of Pre-Price Constraint is Started...");
		//Get Current Price
		if(inputItem.getRegPrice() == null || inputItem.getRegPrice()== 0){
			//curRegPrice = inputItem.getStrategyDTO().getConstriants().getRoundingConstraint().roundupPriceToNextRoundingDigit(0);
		}else{
			multiple = inputItem.getRegMPack() == null ? 1 : inputItem.getRegMPack();
			double mPrice = inputItem.getRegMPrice() == null ? 0 : inputItem.getRegMPrice();
			double price = inputItem.getRegPrice() == null ? 0 : inputItem.getRegPrice();
			prePrice = (multiple > 1 && mPrice > 0) ? mPrice : price;
			prePrice = PRFormatHelper.roundToTwoDecimalDigitAsDouble(prePrice);
		}
		logger.debug("Current Price: " + multiple + "/" + prePrice);
		
		priceRange.setStartVal(prePrice);
		priceRange.setEndVal(prePrice);
		guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
		guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(priceRange);
		guidelineAndConstraintLog.setOutputPriceRange(priceRange);
		if (prePrice == 0)
			guidelineAndConstraintLog.setMessage("Current Price not available");
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		logger.debug("Pre-Price Price: " + priceRange.toString());
		
		CostConstraint costConstraint = new CostConstraint(inputItem, priceRange, explainLog);
		
		//Apply cost and rounding only when current price is not available
		//Otherwise Update just cost and rounding range alone and don't apply it
		if (prePrice == 0)
			guidelineAndConstraintOutputLocal = costConstraint.applyCostConstraint();
		else
			guidelineAndConstraintOutputLocal = costConstraint.getCostRange();
		priceRange = guidelineAndConstraintOutputLocal.outputPriceRange;

		priceRangeArr = strategyDTO.getConstriants().getRoundingConstraint()
				.getRoundingDigits(inputItem, priceRange, explainLog);
		if (prePrice == 0 && priceRangeArr.length > 0)
			prePrice = priceRangeArr[0];
		 
		inputItem.setPriceRange(new Double[] { prePrice });
//		inputItem.setRecommendedRegMultiple(multiple);
//		inputItem.setRecommendedRegPrice(prePrice);
		inputItem.setRecommendedRegPrice(new MultiplePrice(multiple, prePrice));
		
		inputItem.setExplainLog(explainLog);
		logger.debug("Applying of Pre-Price Constraint is Ended...");
//		logger.debug("Final Price: " + inputItem.getRecommendedRegMultiple() + "/" + inputItem.getRecommendedRegPrice());
		logger.debug("Final Price: " +  PRCommonUtil.getPriceForLog(inputItem.getRecommendedRegPrice()));
	}
}
