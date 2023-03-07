package com.pristine.service.offermgmt.promotion;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.lookup.offermgmt.promotion.PromoObjectiveTypeLookup;
import com.pristine.util.PropertyManager;

public class PromotionObjectiveService {

	
	private static Logger logger = Logger.getLogger("PromotionObjectiveService");
	
	/**
	 * Applies objective and finalizes the promo retail and type
	 * 
	 * @param candidateItemMap
	 */
	public void applyObjective(HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap) {

		logger.info("applyObjective() - Applying objective...");
		for (Map.Entry<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> weekEntry : candidateItemMap.entrySet()) {
			HashMap<ProductKey, List<PromoItemDTO>> itemMap = weekEntry.getValue();
			for (Map.Entry<ProductKey, List<PromoItemDTO>> itemEntry : itemMap.entrySet()) {
				PromoItemDTO promoItemDTO = itemEntry.getValue().get(0);

				List<PromoItemDTO> filteredByGuidelines = itemEntry.getValue().stream()
						.filter(p -> p.isGuidelinesSatisfiedPromo() && !p.isDefaultEntry()).collect(Collectors.toList());
				
				PromoItemDTO defaultScenario = itemEntry.getValue().stream().filter(p -> p.isDefaultEntry())
						.findFirst().orElse(null);
				filteredByGuidelines.add(defaultScenario);

				if (promoItemDTO.getObjectiveTypeId() == PromoObjectiveTypeLookup.MAXIMIZE_UNITS.getObjectiveTypeId()) {
					maximizeUnits(filteredByGuidelines);
				} else if (promoItemDTO.getObjectiveTypeId() == PromoObjectiveTypeLookup.MAXIMIZE_MARGIN
						.getObjectiveTypeId()) {
					maximizeMargin(filteredByGuidelines);
				} else if (promoItemDTO.getObjectiveTypeId() == PromoObjectiveTypeLookup.MAXIMIZE_SALES
						.getObjectiveTypeId()) {
					maximizeSales(filteredByGuidelines);
				} else if (promoItemDTO
						.getObjectiveTypeId() == PromoObjectiveTypeLookup.MAXIMIZE_SALES_WHILE_MAINTINING_MARGIN
								.getObjectiveTypeId()) {
					maximizeSalesByMainitainingMargin(filteredByGuidelines);
				}
			}
		}
		logger.info("applyObjective() - Applying objective is completed.");
	}

	/**
	 * Finalizes the promotion with max predicted movement
	 * 
	 * @param promoOptions
	 */
	private void maximizeUnits(List<PromoItemDTO> promoOptions) {
		PromoItemDTO promoItemDTO = promoOptions.stream()
				.collect(Collectors.maxBy(Comparator.comparingDouble(PromoItemDTO::getPredMov))).get();
		promoItemDTO.setFinalized(true);
	}

	/**
	 * Finalizes the promotion with max predicted margin
	 * 
	 * @param promoOptions
	 */
	private void maximizeMargin(List<PromoItemDTO> promoOptions) {
		PromoItemDTO promoItemDTO = promoOptions.stream()
				.collect(Collectors.maxBy(Comparator.comparingDouble(PromoItemDTO::getPredMar))).get();
		promoItemDTO.setFinalized(true);
	}

	/**
	 * Finalizes the promotion with max predicted sales
	 * 
	 * @param promoOptions
	 */
	private void maximizeSales(List<PromoItemDTO> promoOptions) {
		PromoItemDTO promoItemDTO = promoOptions.stream()
				.collect(Collectors.maxBy(Comparator.comparingDouble(PromoItemDTO::getPredRev))).get();
		promoItemDTO.setFinalized(true);
	}
	
	
	/**
	 * Finalizes the promotion with max predicted sales
	 * 
	 * @param promoOptions
	 */
	private void maximizeSalesByMainitainingMargin(List<PromoItemDTO> promoOptions) {
		PromoItemDTO defaultScenario = promoOptions.stream().filter(p -> p.isDefaultEntry()).findFirst().orElse(null);
		if(defaultScenario == null) {
			logger.error("Default not found: " + promoOptions.iterator().next().toString());
		}
		List<PromoItemDTO> filteredOptions = getPromoOptionsByMarginBenefit(promoOptions, defaultScenario);
		if (filteredOptions.size() > 0) {
			PromoItemDTO promoItemDTO = filteredOptions.stream()
					.collect(Collectors.maxBy(Comparator.comparingDouble(PromoItemDTO::getPredRev))).get();
			promoItemDTO.setFinalized(true);
		} else {
			logger.error(
					"maximizeSalesByMainitainingXPctOrDollarMarginRate() - None of the options are maintaining margin");
			defaultScenario.setFinalized(true);
		}
	}
	
	/**
	 * 
	 * @param promoOptions
	 * @param defaultScenario
	 * @return filters better margin options
	 */
	private List<PromoItemDTO> getPromoOptionsByMarginBenefit(List<PromoItemDTO> promoOptions,
			PromoItemDTO defaultScenario) {
		List<PromoItemDTO> filteredOptions = new ArrayList<>();

		double minimumMarginGain = Double.parseDouble(PropertyManager.getProperty("OR_PROMO_MIN_MARGIN_GAIN", "0"));

		double currentMargin = defaultScenario.getPredMar();

		filteredOptions = promoOptions.stream().filter(
				p -> p.getPredMar() > currentMargin && Math.abs(p.getPredMar() - currentMargin) > minimumMarginGain)
				.collect(Collectors.toList());

		filteredOptions.add(defaultScenario);

		return filteredOptions;
	}
	
}
