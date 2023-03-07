package com.pristine.service.offermgmt.promotion;

import java.util.HashMap;
import java.util.List;
import com.pristine.dto.offermgmt.PRGuidelineMargin;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PromoGuidelineService {

	
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param candidateItemMap
	 * @param retLirMap
	 */
	public void applyGuidelines(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap, 
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		
		applyMarginGuidline(recommendationInputDTO, candidateItemMap, retLirMap);
		
	}	
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param candidateItemMap
	 * @param retLirMap
	 */
	private void applyMarginGuidline(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) {
		candidateItemMap.forEach((recWeekKey, itemMap) -> {
			itemMap.forEach((productKey, promoOptions) -> {
				PromoItemDTO repItemDTO = promoOptions.iterator().next();
				if (repItemDTO.getStrategyDTO().getGuidelines().getMarginGuideline() != null) {
					for (PRGuidelineMargin prGuidelineMargin : repItemDTO.getStrategyDTO().getGuidelines()
							.getMarginGuideline()) {
						if (prGuidelineMargin.getMinMarginPct() > 0) {
							promoOptions.forEach(promoOption -> {
								double unitSalePrice = PRCommonUtil.getUnitPrice(promoOption.getSaleInfo().getSalePrice(), true);
								double finalCost = promoOption.getDealCost() != null && promoOption.getDealCost() > 0
										? promoOption.getDealCost()
										: promoOption.getListCost();
								double margin = unitSalePrice - finalCost;
								double marginPct = (margin / unitSalePrice) * 100;
								if(marginPct >= prGuidelineMargin.getMinMarginPct()) {
									promoOption.setGuidelinesSatisfiedPromo(true);	
								} else {
									promoOption.setGuidelinesSatisfiedPromo(false);
								}
							});
						}
					}
				}
			});
		});
	}
	
}
