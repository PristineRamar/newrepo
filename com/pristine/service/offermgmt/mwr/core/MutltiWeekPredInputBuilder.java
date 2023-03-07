package com.pristine.service.offermgmt.mwr.core;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.offermgmt.PRCommonUtil;

public class MutltiWeekPredInputBuilder {


	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger("MutltiWeekPredInputBuilder");
	/**
	 * 
	 * @param weeklyItemDataMap
	 * @return multi week prediction input
	 */
	public List<MultiWeekPredEngineItemDTO> buildInputForMultiWeekPrediction(
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			RecommendationInputDTO recommendationInputDTO) {
		HashMap<ItemKey, HashMap<RecWeekKey, MWRItemDTO>> itemWeekMap = groupByItems(weeklyItemDataMap);

		List<MultiWeekPredEngineItemDTO> predictionInput = new ArrayList<>();

		int scenarioId = 1;

		for (Map.Entry<ItemKey, HashMap<RecWeekKey, MWRItemDTO>> itemEntry : itemWeekMap.entrySet()) {

			int tempScenarioId = 0;

			for (Map.Entry<RecWeekKey, MWRItemDTO> weekEntry : itemEntry.getValue().entrySet()) {

				MWRItemDTO mwrItemDTO = weekEntry.getValue();

				if (!mwrItemDTO.isLir()) {
					// Add current and sale details as one price point
					if (mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {

						MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = convertToMultiWeekPredInput(mwrItemDTO,
								weekEntry.getKey(), recommendationInputDTO, 0, 0.0, scenarioId);

						predictionInput.add(multiWeekPredEngineItemDTO);

					}

					int subScanarioId = scenarioId;
					MultiplePrice currentPrice = null;
					if(mwrItemDTO.getRegPrice() != null) {
						currentPrice = new MultiplePrice(mwrItemDTO.getRegMultiple(), mwrItemDTO.getRegPrice());	
					}
					
					boolean isCurPriceFound = false;
					// Add all possible price points
					if (mwrItemDTO.getPriceRange() != null) {
						for (Double pricePoint : mwrItemDTO.getPriceRange()) {
							if (pricePoint != null) {
								if(currentPrice != null && currentPrice.getUnitPrice() ==  pricePoint) {
									isCurPriceFound = true;
								}
								MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = convertToMultiWeekPredInput(
										mwrItemDTO, weekEntry.getKey(), recommendationInputDTO, 1, pricePoint,
										subScanarioId++);

								predictionInput.add(multiWeekPredEngineItemDTO);
							}
						}
					}

					if(currentPrice != null && !isCurPriceFound) {
						MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = convertToMultiWeekPredInput(
								mwrItemDTO, weekEntry.getKey(), recommendationInputDTO, 1, currentPrice.getUnitPrice(),
								subScanarioId++);

						predictionInput.add(multiWeekPredEngineItemDTO);
					}
					
					// Record only maximum scenario id. It is possible that a week in between might have less price points.
					if (subScanarioId > tempScenarioId)
						tempScenarioId = subScanarioId;
				}
			}

			scenarioId = tempScenarioId;
			scenarioId++; // Scnario 3
		}

		return predictionInput;
	}
	
	
	
	/**
	 * 
	 * @param weeklyItemDataMap
	 * @return multi week prediction input
	 */
	public List<MultiWeekPredEngineItemDTO> buildInputForPrediction(
			HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap,
			RecommendationInputDTO recommendationInputDTO) {
		HashMap<ItemKey, HashMap<RecWeekKey, MWRItemDTO>> itemWeekMap = groupByItems(weeklyItemDataMap);

		LocalDate baseWeek = LocalDate.parse(recommendationInputDTO.getStartWeek(), PRCommonUtil.getDateFormatter());
		List<MultiWeekPredEngineItemDTO> predictionInput = new ArrayList<>();

		for (Map.Entry<ItemKey, HashMap<RecWeekKey, MWRItemDTO>> itemEntry : itemWeekMap.entrySet()) {

			for (Map.Entry<RecWeekKey, MWRItemDTO> weekEntry : itemEntry.getValue().entrySet()) {
				LocalDate weekStartDate = LocalDate.parse(weekEntry.getKey().weekStartDate,
						PRCommonUtil.getDateFormatter());
				if (weekStartDate.isAfter(baseWeek) || weekStartDate.isEqual(baseWeek)) {
					int scenarioId = 0;
					MWRItemDTO mwrItemDTO = weekEntry.getValue();
					if (!mwrItemDTO.isLir()) {
						boolean isSaleWeek = false;
						// Add current and sale details as one price point
						if (mwrItemDTO.getSalePrice() != null && mwrItemDTO.getSalePrice() > 0) {
							isSaleWeek = true;
							scenarioId++;
							MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = convertToMultiWeekPredInput(
									mwrItemDTO, weekEntry.getKey(), recommendationInputDTO, 0, 0.0, scenarioId);

							predictionInput.add(multiWeekPredEngineItemDTO);

						}

						MultiplePrice recommendedPrice = mwrItemDTO.getRecommendedRegPrice();
						MultiplePrice curPrice = new MultiplePrice(mwrItemDTO.getRegMultiple(),
								mwrItemDTO.getRegPrice());
						// Condition added to pass the recommended retaail when override is done for an
						// item to use current retail
						// Fix added  when issue was identified for FF , when item is overriden with
						// current retail it was getting passed to prediction Engine and new and cur
						// units were different after update reccs
						if (mwrItemDTO.getOverrideRegPrice() != null
								&& mwrItemDTO.getOverrideRegPrice().getUnitPrice() > 0
								&& curPrice.equals(mwrItemDTO.getOverrideRegPrice())
								&& mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().price > 0) {
							scenarioId++;
							MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = convertToMultiWeekPredInput(
									mwrItemDTO, weekEntry.getKey(), recommendationInputDTO,
									mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().multiple,
									mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().price, scenarioId);

							predictionInput.add(multiWeekPredEngineItemDTO);
						} else if (recommendedPrice != null) {
							scenarioId++;
							MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = convertToMultiWeekPredInput(
									mwrItemDTO, weekEntry.getKey(), recommendationInputDTO, recommendedPrice.multiple,
									recommendedPrice.price, scenarioId);
							predictionInput.add(multiWeekPredEngineItemDTO);
						}

						if (mwrItemDTO.getRegPrice() != null && !isSaleWeek) {
							scenarioId++;
							MultiplePrice currPrice = new MultiplePrice(mwrItemDTO.getRegMultiple(),
									mwrItemDTO.getRegPrice());
							//condition added to use the current retail scenario when retail before override 
							//is different then current retail and item is oevrriden to retain curr retail
							if ((currPrice.price > 0 && !currPrice.equals(recommendedPrice))
									|| (mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec() != null
											&& mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().getUnitPrice() > 0
											&& !mwrItemDTO.getRecommendedRegPriceBeforeUpdateRec().equals(currPrice))) {
								MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = convertToMultiWeekPredInput(
										mwrItemDTO, weekEntry.getKey(), recommendationInputDTO, currPrice.multiple,
										currPrice.price, scenarioId);
								predictionInput.add(multiWeekPredEngineItemDTO);
							}
						}

					}
				}
			}
		}

		return predictionInput;
	}
	
	/**
	 * 
	 * @param weeklyItemDataMap
	 * @return grouped by items
	 */
	private HashMap<ItemKey, HashMap<RecWeekKey, MWRItemDTO>> groupByItems(HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap){
		HashMap<ItemKey, HashMap<RecWeekKey, MWRItemDTO>> groupedByItems = new HashMap<>();

		weeklyItemDataMap.forEach((recWeekKey, itemDataMap) ->{
		
			itemDataMap.forEach((itemKey, mwrItemDTO) -> {
				HashMap<RecWeekKey, MWRItemDTO> weeklyItem = new HashMap<>();
				
				if(groupedByItems.containsKey(itemKey)){
					weeklyItem = groupedByItems.get(itemKey);
				}
				weeklyItem.put(recWeekKey, mwrItemDTO);
				groupedByItems.put(itemKey, weeklyItem);
				
			});
		});

		return groupedByItems;
	}
	
	
	/**
	 * 
	 * @param mwrItemDTO
	 * @param recWeekKey
	 * @return
	 */
	private MultiWeekPredEngineItemDTO convertToMultiWeekPredInput(MWRItemDTO mwrItemDTO, RecWeekKey recWeekKey,
			RecommendationInputDTO recommendationInputDTO, int qty, Double price, int scenarioId) {
		MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = new MultiWeekPredEngineItemDTO();
		multiWeekPredEngineItemDTO.setScenarioId(scenarioId);
		multiWeekPredEngineItemDTO.setStartDate(recWeekKey.weekStartDate);
		multiWeekPredEngineItemDTO.setCalendarId(recWeekKey.calendarId);
		multiWeekPredEngineItemDTO.setItemCode(mwrItemDTO.getItemCode());
		multiWeekPredEngineItemDTO.setRetLirId(mwrItemDTO.getRetLirId());
		multiWeekPredEngineItemDTO.setLocationLevelId(recommendationInputDTO.getLocationLevelId());
		multiWeekPredEngineItemDTO.setLocationId(recommendationInputDTO.getLocationId());
		multiWeekPredEngineItemDTO.setProductLevelId(recommendationInputDTO.getProductLevelId());
		multiWeekPredEngineItemDTO.setProductId(recommendationInputDTO.getProductId());
		multiWeekPredEngineItemDTO.setUPC(mwrItemDTO.getUpc());
		if(price > 0){
			multiWeekPredEngineItemDTO.setRegMultiple(qty);
			multiWeekPredEngineItemDTO.setRegPrice(price);
			multiWeekPredEngineItemDTO.setSaleMultiple(0);
			multiWeekPredEngineItemDTO.setSalePrice(0);	
			multiWeekPredEngineItemDTO.setPromoTypeId(0);
			multiWeekPredEngineItemDTO.setAdPageNo(0);
			multiWeekPredEngineItemDTO.setAdBlockNo(0);
			multiWeekPredEngineItemDTO.setDisplayTypeId(0);
		}else{
			if(mwrItemDTO.getRegPrice() != null && mwrItemDTO.getRegPrice() > 0) {
				multiWeekPredEngineItemDTO.setRegMultiple(mwrItemDTO.getRegMultiple() == null ? 0 : mwrItemDTO.getRegMultiple());
				multiWeekPredEngineItemDTO.setRegPrice(mwrItemDTO.getRegPrice() == null ? 0 : mwrItemDTO.getRegPrice());	
			} else {
				multiWeekPredEngineItemDTO.setRegMultiple(mwrItemDTO.getRecommendedRegPrice() != null ? mwrItemDTO.getRecommendedRegPrice().multiple : 0);
				multiWeekPredEngineItemDTO.setRegPrice(mwrItemDTO.getRecommendedRegPrice() != null ? mwrItemDTO.getRecommendedRegPrice().price : 0);
			}
			multiWeekPredEngineItemDTO.setSaleMultiple(mwrItemDTO.getSaleMultiple() == null ? 0 : mwrItemDTO.getSaleMultiple());
			multiWeekPredEngineItemDTO.setSalePrice(mwrItemDTO.getSalePrice() == null ? 0 : mwrItemDTO.getSalePrice());	
			multiWeekPredEngineItemDTO.setPromoTypeId(mwrItemDTO.getPromoTypeId() == null ? 0 : mwrItemDTO.getPromoTypeId());
			multiWeekPredEngineItemDTO.setAdPageNo(mwrItemDTO.getAdPageNo() == null ? 0 : mwrItemDTO.getAdPageNo());
			multiWeekPredEngineItemDTO.setAdBlockNo(mwrItemDTO.getAdBlockNo() == null ? 0 : mwrItemDTO.getAdBlockNo());
			multiWeekPredEngineItemDTO.setDisplayTypeId(mwrItemDTO.getDisplayTypeId() == null ? 0 : mwrItemDTO.getDisplayTypeId());
		}
		
		/*
		 * if (mwrItemDTO.isNoRecentWeeksMovement()) {
		 * multiWeekPredEngineItemDTO.setPredictionStatus(PredictionStatus.
		 * NO_RECENT_MOVEMENT.getStatusCode());
		 * multiWeekPredEngineItemDTO.setPredictedMovement(-1D);
		 * multiWeekPredEngineItemDTO.setNoRecentWeeksMov(mwrItemDTO.
		 * isNoRecentWeeksMovement()); }
		 */
		
		multiWeekPredEngineItemDTO.setSendToPrediction(mwrItemDTO.isSendToPrediction());
		
		if (!mwrItemDTO.isSendToPrediction()) {
			multiWeekPredEngineItemDTO.setPredictionStatus(PredictionStatus.SUCCESS.getStatusCode());
			multiWeekPredEngineItemDTO.setPredictedMovement(mwrItemDTO.getAvgMovement());
			
		}

		return multiWeekPredEngineItemDTO;
	}
	
	
}
