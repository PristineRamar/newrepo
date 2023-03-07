package com.pristine.service.offermgmt.recommendationrule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.offermgmt.PRCommonUtil;

public class CheckNextDollarCrossingPPRule extends FilterPricePointsRule {
	
	private static Logger logger = Logger.getLogger("CheckNextDollarCrossingPPRule");
	private PRItemDTO itemDTO;
	private List<Double> actualPricePoints;
	/**
	 * 
	 */
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<String, RetailPriceDTO> itemPriceHistory;
	String inpBaseWeekStartDate, priceType;
	int noSaleInLastXWeeks = 0;
	private final String ruleCode = "R15";
	private Double curUnitRegPrice;

	public CheckNextDollarCrossingPPRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
	}

	public CheckNextDollarCrossingPPRule(HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap, PRItemDTO itemDTO,
			List<Double> actualPricePoints, Double curUnitRegPrice) {
		super(itemDTO, actualPricePoints, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.actualPricePoints = actualPricePoints;
		this.recommendationRuleMap = recommendationRuleMap;
		this.curUnitRegPrice = curUnitRegPrice;
	}

	@Override
	public List<Double> applyRule() throws Exception, GeneralException {
		PricingEngineService pricingEngineService = new PricingEngineService();
		
		List<Double> finalFilteredPricePoint = new ArrayList<>();
		if(isRuleEnabled(ruleCode)) {
			logger.debug("filterFinalPricePoints() :: Pricing Rule - R15 is enabled and applied to item :" + itemDTO.getItemCode());
			
			List<Double> tempPriceRange = new ArrayList<Double>();
			int curUnitRegPriceWholeNumber = (int) Math.floor(curUnitRegPrice);
			for (Double pricePoint : actualPricePoints) {
				// Price point <= current price and price points in the same digit as current price
				if (curUnitRegPriceWholeNumber == (int) Math.floor(pricePoint) || pricePoint <= curUnitRegPrice) {
					tempPriceRange.add(pricePoint);
				}
			}
			
			logger.debug("Don't cross to next range " + ".Cur Reg Unit Price:"
					+ curUnitRegPrice + ",Actual price points: " + PRCommonUtil.getCommaSeperatedStringFromDouble(actualPricePoints)
					+ ",Filtered price points: " + PRCommonUtil.getCommaSeperatedStringFromDouble(tempPriceRange));
			
			//update explain log
			if (actualPricePoints.size() > 0 && tempPriceRange.size() == 0) {
				//If there is no option, stay with all price points
				tempPriceRange.addAll(actualPricePoints);
			} else {
				String pricePoints = getIgnoredPricePoints(tempPriceRange.stream().collect(Collectors.toSet()));
				if (pricePoints != "") {
					logger.debug("3-1. Addition log");
					List<String> additionalDetails = new ArrayList<String>();
					additionalDetails.add(pricePoints);

					pricingEngineService.writeAdditionalExplainLog(itemDTO, ExplainRetailNoteTypeLookup.NO_NEXT_RANGE,
							additionalDetails);
				}
			}
			finalFilteredPricePoint.addAll(tempPriceRange);
		}else {
			finalFilteredPricePoint.addAll(actualPricePoints);
		}
		
		return finalFilteredPricePoint;
	}
}
