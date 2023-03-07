package com.pristine.service.offermgmt.recommendationrule;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.RecommendationRuleMapDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class CheckRegRetailChangedInLastXWeeksRule extends RetainCurrentRetailRule {
	private static Logger logger = Logger.getLogger("CheckRegRetailChangedInLastXWeeksRule");
	private PRItemDTO itemDTO;
	private int regRetailChangeInLastXWeeks = Integer.parseInt(PropertyManager.getProperty("REG_RETAIL_CHANGE_IN_LAST_X_WEEKS"));
	HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap;
	HashMap<String, RetailPriceDTO> itemPriceHistory;
	String inpBaseWeekStartDate;
	DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
	private final String ruleCode = "R16";

	public CheckRegRetailChangedInLastXWeeksRule(PRItemDTO itemDTO, HashMap<String, List<RecommendationRuleMapDTO>> recommendationRuleMap,
			HashMap<String, RetailPriceDTO> itemPriceHistory, String inpBaseWeekStartDate) {
		super(itemDTO, recommendationRuleMap);
		this.itemDTO = itemDTO;
		this.recommendationRuleMap = recommendationRuleMap;
		this.itemPriceHistory = itemPriceHistory;
		this.inpBaseWeekStartDate = inpBaseWeekStartDate;
	}

	@Override
	public boolean isCurrentRetailRetained() throws Exception, GeneralException {
		boolean retainCurrentRetail = false;
		PricingEngineService pricingEngineService = new PricingEngineService();

		// If Price unchanged in last x week it is false otherwise price got changed then it is true
		boolean isPriceChangedInLasXWeeks = pricingEngineService.isRegPriceUnchangedInLastXWeeks(itemPriceHistory, inpBaseWeekStartDate,
				regRetailChangeInLastXWeeks) ? false : true;

		boolean isCostConstraintNotViolated = isCostConstraintViolated() ? false : true;
		MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice());

		if (isRuleEnabled(ruleCode) && isPriceChangedInLasXWeeks && isCostConstraintNotViolated && checkPriceRangeUsingRelatedItem()
				&& isRecEffDateWithinXWeeks(pricingEngineService)) {
			retainCurrentRetail = true;
		}

		/*logger.debug("Item Code:" + itemDTO.getItemCode() + " Is price unchanged in last 13 weeks: "
				+ pricingEngineService.isRegPriceUnchangedInLastXWeeks(itemPriceHistory, inpBaseWeekStartDate, regRetailChangeInLastXWeeks)
				+ " Cost violated: " + isCostConstraintViolated() + " Brand/Size in range: " + checkPriceRangeUsingRelatedItem()
				+ " Is reg price and Current price is diff: "+isCostConstraintNotViolated+" retainCurrentRetail:" + retainCurrentRetail);*/
		return retainCurrentRetail;
	}

	private boolean isRecEffDateWithinXWeeks(PricingEngineService pricingEngineService) throws GeneralException {

		boolean isRecEffDateWithinXWeeks = true;
		String priceChangeEffDate = pricingEngineService.getItemPriceChangeEffDate(itemPriceHistory,
				DateUtil.toDateAsLocalDate(inpBaseWeekStartDate), regRetailChangeInLastXWeeks);
		if (priceChangeEffDate != null && itemDTO.isFutureRetailRecommended() && itemDTO.getRecPriceEffectiveDate() != null && itemDTO.getRecPriceEffectiveDate()!="") {

			LocalDate lastPriceChangeDate = LocalDate.parse(priceChangeEffDate, formatter)
					.plus(regRetailChangeInLastXWeeks, ChronoUnit.WEEKS).plus(6, ChronoUnit.DAYS);
			LocalDate recPriceEffDate = LocalDate.parse(itemDTO.getRecPriceEffectiveDate(), formatter);
			if (recPriceEffDate.isAfter(lastPriceChangeDate)) {
				isRecEffDateWithinXWeeks = false;
			}
		}
		return isRecEffDateWithinXWeeks;
	}

	@Override
	public boolean isRuleEnabled() {
		return isRuleEnabled(ruleCode);
	}
}
