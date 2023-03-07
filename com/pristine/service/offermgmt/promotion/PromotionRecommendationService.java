package com.pristine.service.offermgmt.promotion;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.multiWeekPrediction.MultiWeekPredEngineItemDTO;
import com.pristine.dto.offermgmt.mwr.MultiWeekPredKey;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.service.offermgmt.mwr.core.MutliWeekPredictionComponent;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PromotionRecommendationService {

	private static Logger logger = Logger.getLogger("PromotionRecommendationService");
	private Connection conn;
	private HashMap<String, RetailCalendarDTO> calDetails;
	public PromotionRecommendationService(Connection conn, HashMap<String, RetailCalendarDTO> calDetails) {
		this.conn = conn;
		this.calDetails = calDetails;
	}

	/**
	 * Core promo recommendation
	 * 
	 * @param candidateItemMap
	 * @throws Exception
	 * @throws GeneralException
	 */
	public void recommendPromotions(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap, 
			HashMap<Integer, List<PRItemDTO>> retLirMap)
			throws GeneralException, Exception {

		// 1. Apply guidelines
		new PromoGuidelineService().applyGuidelines(recommendationInputDTO, candidateItemMap, retLirMap);
		
		// 2. Predict movement for given items and promo options
		predictMovement(recommendationInputDTO, candidateItemMap, retLirMap);

		// 3. Apply objective to finalize promo retail and type
		new PromotionObjectiveService().applyObjective(candidateItemMap);

		// 4. Group level promotions
		applyRecommendationsToDependentItems(candidateItemMap, recommendationInputDTO);

		// 5. Apply lig constraint
		applyLigConstraint(candidateItemMap, retLirMap, recommendationInputDTO);
	}

	/**
	 * 
	 * @param weeklyItemDataMap
	 * @param retLirMap
	 * @param recommendationInputDTO
	 */
	private void applyLigConstraint(
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> weeklyItemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, RecommendationInputDTO recommendationInputDTO) {
		
		weeklyItemDataMap.forEach((recWeekKey, candidateItemMap) -> {
			candidateItemMap.forEach((productKey, promoOptions) -> {
				if (productKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
					if (retLirMap.containsKey(productKey.getProductId())) {
						PromoItemDTO finalizedLig = promoOptions.stream().filter(p -> p.isFinalized()).findFirst()
								.orElse(null);
						if (finalizedLig != null) {
							MultiWeekPredEngineItemDTO multiWeekPredEngineLIGDTO = convertToMultiWeekPredInput(
									finalizedLig, recWeekKey, recommendationInputDTO, 0, 0.0, 0);

							MultiWeekPredKey multiWeekPredKeyLIG = MultiWeekPredEngineItemDTO
									.getMultiWeekPredKey(multiWeekPredEngineLIGDTO);
							List<PRItemDTO> ligMembers = retLirMap.get(productKey.getProductId());
							for (PRItemDTO prItemDTO : ligMembers) {
								ProductKey productKeyLigMember = new ProductKey(Constants.ITEMLEVELID,
										prItemDTO.getItemCode());
								List<PromoItemDTO> ligMemberPromoOptions = candidateItemMap.get(productKeyLigMember);
								if(ligMemberPromoOptions != null) {
									for (PromoItemDTO ligMemberPromoOption : ligMemberPromoOptions) {
										ligMemberPromoOption.setFinalized(false);
										MultiWeekPredEngineItemDTO multiWeekPredEngineLIGMember = convertToMultiWeekPredInput(
												ligMemberPromoOption, recWeekKey, recommendationInputDTO, 0, 0.0, 0);
										MultiWeekPredKey multiWeekPredKeyLIGMember = MultiWeekPredEngineItemDTO
												.getMultiWeekPredKey(multiWeekPredEngineLIGMember);
										multiWeekPredKeyLIGMember.setItemCode(multiWeekPredKeyLIG.getItemCode());
										if (multiWeekPredKeyLIG.equals(multiWeekPredKeyLIGMember)) {
											ligMemberPromoOption.setFinalized(true);
										}
									}
								}
							}
						}
					}
				}
			});
		});
	}
	
	/**
	 * 
	 * @param recommendationInputDTO
	 * @param candidateItemMap
	 * @throws GeneralException
	 * @throws Exception
	 */
	private void predictMovement(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap, 
			HashMap<Integer, List<PRItemDTO>> retLirMap)
			throws GeneralException, Exception {
		boolean useRegPriceForSalesCalc = Boolean
				.parseBoolean(PropertyManager.getProperty("OR_USE_REG_PRICE_FOR_SALES_CALC", "FALSE"));
		MutliWeekPredictionComponent mutliWeekPredictionComponent = new MutliWeekPredictionComponent(conn);

		logger.info("predictMovement() - Setting up data for prediction...");

		List<MultiWeekPredEngineItemDTO> predictionInput = setupDataForPrediction(recommendationInputDTO,
				candidateItemMap, retLirMap);

		logger.info("predictMovement() - # of records to be predicted - " + predictionInput.size());

		logger.info("predictMovement() - Prediction is started...");
		List<MultiWeekPredEngineItemDTO> predictionOutput = mutliWeekPredictionComponent.callMultiWeekPrediction(
				recommendationInputDTO, predictionInput, calDetails, new ArrayList<>());

		logger.info("predictMovement() - # of records to be predicted - " + predictionOutput.size());

		logger.info("predictMovement() - Updating predicted movement...");

		logger.info("predictMovement() - # of records with predicted movement 0 or -1: "
				+ predictionOutput.stream().filter(m -> m.getPredictedMovement() <= 0).count());
		
		updateWeeklyPredictedMovement(candidateItemMap, predictionOutput, calDetails,
				recommendationInputDTO, useRegPriceForSalesCalc);

		updatePredictionsForLIG(candidateItemMap, retLirMap, recommendationInputDTO, useRegPriceForSalesCalc);
		
		logger.info("predictMovement() - Updating predicted movement is completed.");
	}

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param candidateItemMap
	 * @return input for predicition
	 */
	private List<MultiWeekPredEngineItemDTO> setupDataForPrediction(RecommendationInputDTO recommendationInputDTO,
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> candidateItemMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap) throws Exception {

		HashMap<ProductKey, HashMap<RecWeekKey, List<PromoItemDTO>>> candidateItemsByWeek = groupByItems(
				candidateItemMap);

		HashMap<Integer, List<MultiWeekPredEngineItemDTO>> scenarios = buildScenerios(recommendationInputDTO,
				candidateItemsByWeek);

		List<MultiWeekPredEngineItemDTO> multiWeekPredInput = mergeByScenario(scenarios);

		List<MultiWeekPredEngineItemDTO> finalList = handleLIGMembersAndNonLIG(multiWeekPredInput, retLirMap);

		return finalList;
	}

	
	
	private List<MultiWeekPredEngineItemDTO> handleLIGMembersAndNonLIG(
			List<MultiWeekPredEngineItemDTO> multiWeekPredInput, HashMap<Integer, List<PRItemDTO>> retLirMap)
			throws CloneNotSupportedException {
		List<MultiWeekPredEngineItemDTO> finalList = new ArrayList<>();
		for (MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO : multiWeekPredInput) {
			if (multiWeekPredEngineItemDTO.getRetLirId() == 0) {
				finalList.add(multiWeekPredEngineItemDTO);
			} else {
				if (retLirMap.containsKey(multiWeekPredEngineItemDTO.getRetLirId())) {
					List<PRItemDTO> ligMembers = retLirMap.get(multiWeekPredEngineItemDTO.getRetLirId());
					for (PRItemDTO prItemDTO : ligMembers) {
						MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTOLigMember = (MultiWeekPredEngineItemDTO) multiWeekPredEngineItemDTO
								.clone();
						multiWeekPredEngineItemDTOLigMember.setItemCode(prItemDTO.getItemCode());
						multiWeekPredEngineItemDTOLigMember.setUPC(prItemDTO.getUpc());
						finalList.add(multiWeekPredEngineItemDTOLigMember);
					}
				}
			}
		}
		return finalList;
	}
	
	/**
	 * 
	 * @param scenarios
	 * @return final list for prediction
	 */
	private List<MultiWeekPredEngineItemDTO> mergeByScenario(
			HashMap<Integer, List<MultiWeekPredEngineItemDTO>> scenarios) {
		List<MultiWeekPredEngineItemDTO> finalList = new ArrayList<>();

		scenarios.forEach((id, list) -> {
			finalList.addAll(list);
		});

		return finalList;
	}

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param candidateItemsByWeek
	 * @return scenraios by promotions and prices
	 */
	private HashMap<Integer, List<MultiWeekPredEngineItemDTO>> buildScenerios(
			RecommendationInputDTO recommendationInputDTO,
			HashMap<ProductKey, HashMap<RecWeekKey, List<PromoItemDTO>>> candidateItemsByWeek) {

		HashMap<Integer, List<MultiWeekPredEngineItemDTO>> scenarios = new HashMap<>();

		int scenarioId = 1;

		for (Map.Entry<ProductKey, HashMap<RecWeekKey, List<PromoItemDTO>>> itemEntry : candidateItemsByWeek
				.entrySet()) {

			int tempScenarioId = 0;
			for (Map.Entry<RecWeekKey, List<PromoItemDTO>> weekEntry : itemEntry.getValue().entrySet()) {

				PromoItemDTO promoItemDTOReg = weekEntry.getValue().get(0);

				MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTOReg = convertToMultiWeekPredInput(promoItemDTOReg,
						weekEntry.getKey(), recommendationInputDTO, promoItemDTOReg.getRegPrice().multiple,
						promoItemDTOReg.getRegPrice().price, scenarioId);

				addScenario(scenarioId, multiWeekPredEngineItemDTOReg, scenarios);

				int subScanarioId = scenarioId;

				for (PromoItemDTO promoItemDTO : weekEntry.getValue()) {

					if (promoItemDTO.getSaleInfo() == null || promoItemDTO.getSaleInfo().getPromoTypeId() == PromoTypeLookup.NONE.getPromoTypeId()) {

						logger.debug("Sale price is null/reg price  for " + promoItemDTO.getProductKey().toString());

					} else {
						subScanarioId++;
						MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = convertToMultiWeekPredInput(
								promoItemDTO, weekEntry.getKey(), recommendationInputDTO, 0, 0.0, subScanarioId);

						addScenario(subScanarioId, multiWeekPredEngineItemDTO, scenarios);
					}
				}
				// Record only maximum scenario id. It is possible that a week in between might have less price points.
				if (subScanarioId > tempScenarioId)
					tempScenarioId = subScanarioId;
			}

			scenarioId = tempScenarioId;
			scenarioId++;
		}

		return scenarios;

	}

	/**
	 * 
	 * @param scenarioId
	 * @param multiWeekPredEngineItemDTO
	 * @param scenarios
	 */
	private void addScenario(int scenarioId, MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO,
			HashMap<Integer, List<MultiWeekPredEngineItemDTO>> scenarios) {

		List<MultiWeekPredEngineItemDTO> predInput = new ArrayList<>();
		if (scenarios.containsKey(scenarioId)) {
			predInput = scenarios.get(scenarioId);
		}
		predInput.add(multiWeekPredEngineItemDTO);
		scenarios.put(scenarioId, predInput);
	}

	/**
	 * 
	 * @param promoItemDTO
	 * @param recWeekKey
	 * @return multi week prediction input
	 */
	private MultiWeekPredEngineItemDTO convertToMultiWeekPredInput(PromoItemDTO promoItemDTO, RecWeekKey recWeekKey,
			RecommendationInputDTO recommendationInputDTO, int qty, Double price, int scenarioId) {
		MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = new MultiWeekPredEngineItemDTO();
		multiWeekPredEngineItemDTO.setScenarioId(scenarioId);
		multiWeekPredEngineItemDTO.setStartDate(recWeekKey.weekStartDate);
		multiWeekPredEngineItemDTO.setCalendarId(recWeekKey.calendarId);
		multiWeekPredEngineItemDTO.setItemCode(promoItemDTO.getProductKey().getProductId());
		multiWeekPredEngineItemDTO.setRetLirId(promoItemDTO.getRetLirId());
		multiWeekPredEngineItemDTO.setLocationLevelId(recommendationInputDTO.getLocationLevelId());
		multiWeekPredEngineItemDTO.setLocationId(recommendationInputDTO.getLocationId());
		multiWeekPredEngineItemDTO.setProductLevelId(Constants.CATEGORYLEVELID);
		multiWeekPredEngineItemDTO.setProductId(promoItemDTO.getCategoryId());
		multiWeekPredEngineItemDTO.setUPC(promoItemDTO.getUpc());
		if (price > 0) {
			multiWeekPredEngineItemDTO.setRegMultiple(qty);
			multiWeekPredEngineItemDTO.setRegPrice(price);
			multiWeekPredEngineItemDTO.setSaleMultiple(0);
			multiWeekPredEngineItemDTO.setSalePrice(0);
			multiWeekPredEngineItemDTO.setPromoTypeId(0);
			multiWeekPredEngineItemDTO.setAdPageNo(0);
			multiWeekPredEngineItemDTO.setAdBlockNo(0);
			multiWeekPredEngineItemDTO.setDisplayTypeId(0);
		} else {
			multiWeekPredEngineItemDTO.setRegMultiple(promoItemDTO.getRegPrice().multiple);
			multiWeekPredEngineItemDTO.setRegPrice(promoItemDTO.getRegPrice().price);
			multiWeekPredEngineItemDTO.setSaleMultiple(promoItemDTO.getSaleInfo().getSalePrice().multiple);
			multiWeekPredEngineItemDTO.setSalePrice(promoItemDTO.getSaleInfo().getSalePrice().price);
			multiWeekPredEngineItemDTO
					.setPromoTypeId(promoItemDTO.getSaleInfo().getPromoTypeId());
			if (promoItemDTO.getAdInfo() == null) {
				multiWeekPredEngineItemDTO.setAdPageNo(0);
				multiWeekPredEngineItemDTO.setAdBlockNo(0);
			} else {
				multiWeekPredEngineItemDTO.setAdPageNo(promoItemDTO.getAdInfo().getAdPageNo());
				multiWeekPredEngineItemDTO.setAdBlockNo(promoItemDTO.getAdInfo().getAdBlockNo());
			}

			if (promoItemDTO.getDisplayInfo() == null) {

				multiWeekPredEngineItemDTO.setDisplayTypeId(0);
			} else {

				multiWeekPredEngineItemDTO
						.setDisplayTypeId(promoItemDTO.getDisplayInfo().getDisplayTypeLookup().getDisplayTypeId());
			}

			if (multiWeekPredEngineItemDTO.getPromoTypeId() == PromoTypeLookup.BMSM.getPromoTypeId()) {
				multiWeekPredEngineItemDTO.setMinQtyReqd(promoItemDTO.getSaleInfo().getMinQtyReqd() == null ? 0
						: promoItemDTO.getSaleInfo().getMinQtyReqd());
				multiWeekPredEngineItemDTO
						.setOfferUnitType(promoItemDTO.getSaleInfo().getOfferUnitType() == null ? Constants.EMPTY
								: promoItemDTO.getSaleInfo().getOfferUnitType());
				multiWeekPredEngineItemDTO.setOfferValue(promoItemDTO.getSaleInfo().getOfferValue() == null ? 0
						: promoItemDTO.getSaleInfo().getOfferValue());
			}

		}

		return multiWeekPredEngineItemDTO;
	}

	/**
	 * 
	 * @param weeklyItemDataMap
	 * @return grouped by items
	 */
	private HashMap<ProductKey, HashMap<RecWeekKey, List<PromoItemDTO>>> groupByItems(
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> weeklyItemDataMap) {
		HashMap<ProductKey, HashMap<RecWeekKey, List<PromoItemDTO>>> groupedByItems = new HashMap<>();

		weeklyItemDataMap.forEach((recWeekKey, itemDataMap) -> {
			itemDataMap.forEach((productKey, promoList) -> {
				PromoItemDTO promoItemDTO = promoList.iterator().next();
				if((productKey.getProductLevelId() == Constants.ITEMLEVELID && promoItemDTO.getRetLirId() == 0)
						|| productKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
					HashMap<RecWeekKey, List<PromoItemDTO>> weeklyItem = new HashMap<>();

					if (groupedByItems.containsKey(productKey)) {
						weeklyItem = groupedByItems.get(productKey);
					}
					weeklyItem.put(recWeekKey, promoList);
					groupedByItems.put(productKey, weeklyItem);
				}
			});
		});

		return groupedByItems;
	}

	/**
	 * 
	 * @param weeklyItemDataMap
	 * @param multiWeekPredOuput
	 * @param calDetails
	 */
	public void updateWeeklyPredictedMovement(
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> weeklyItemDataMap,
			List<MultiWeekPredEngineItemDTO> multiWeekPredOuput, HashMap<String, RetailCalendarDTO> calDetails,
			RecommendationInputDTO recommendationInputDTO, 
			boolean useRegPriceForSalesCalc) {

		multiWeekPredOuput.stream().forEach(multiWeekPredEngineItemDTO -> {

			ProductKey productKey = new ProductKey(Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID,
					multiWeekPredEngineItemDTO.getItemCode());

			RetailCalendarDTO retailCalendarDTO = calDetails.get(multiWeekPredEngineItemDTO.getStartDate());

			RecWeekKey recWeekKey = new RecWeekKey(retailCalendarDTO.getCalendarId(), retailCalendarDTO.getStartDate());

			HashMap<ProductKey, List<PromoItemDTO>> itemMap = weeklyItemDataMap.get(recWeekKey);

			if (itemMap.containsKey(productKey)) {
				List<PromoItemDTO> promoTypes = itemMap.get(productKey);
				for (PromoItemDTO promoItemDTO : promoTypes) {
					if (multiWeekPredEngineItemDTO.getSalePrice() == 0) {
						promoItemDTO.setPredMovReg(multiWeekPredEngineItemDTO.getPredictedMovement());
						promoItemDTO.setPredStatusReg(
								PredictionStatus.get(multiWeekPredEngineItemDTO.getPredictionStatus()));

						if(multiWeekPredEngineItemDTO.getPredictedMovement() > 0) {
							promoItemDTO.setPredRevReg(
									PRCommonUtil.getSalesDollar(promoItemDTO.getRegPrice(), promoItemDTO.getPredMovReg()));

							promoItemDTO.setPredMarReg(PRCommonUtil.getMarginDollar(promoItemDTO.getRegPrice(),
									promoItemDTO.getListCost(), promoItemDTO.getPredMovReg()));
						}
						
						if (promoItemDTO.getSaleInfo().getPromoTypeId() == PromoTypeLookup.NONE
								.getPromoTypeId()) {
							promoItemDTO.setPredMov(multiWeekPredEngineItemDTO.getPredictedMovement());
							promoItemDTO.setPredStatus(
									PredictionStatus.get(multiWeekPredEngineItemDTO.getPredictionStatus()));
							if (multiWeekPredEngineItemDTO.getPredictedMovement() > 0) {
								promoItemDTO.setPredRev(PRCommonUtil.getSalesDollar(promoItemDTO.getRegPrice(),
										promoItemDTO.getPredMovReg()));

								promoItemDTO.setPredMar(PRCommonUtil.getMarginDollar(promoItemDTO.getRegPrice(),
										promoItemDTO.getListCost(), promoItemDTO.getPredMovReg()));
							}
						}
					} else {
						MultiWeekPredEngineItemDTO multiWeekPredEngineItemTemp = convertToMultiWeekPredInput(
								promoItemDTO, recWeekKey, recommendationInputDTO, 0, 0.0, 0);
						if (MultiWeekPredEngineItemDTO.getMultiWeekPredKey(multiWeekPredEngineItemDTO)
								.equals(MultiWeekPredEngineItemDTO.getMultiWeekPredKey(multiWeekPredEngineItemTemp))) {
							promoItemDTO.setPredMov(multiWeekPredEngineItemDTO.getPredictedMovement());
							promoItemDTO.setPredStatus(
									PredictionStatus.get(multiWeekPredEngineItemDTO.getPredictionStatus()));

							if (multiWeekPredEngineItemDTO.getPredictedMovement() > 0) {
								if (useRegPriceForSalesCalc) {
									promoItemDTO.setPredRev(PRCommonUtil.getSalesDollar(promoItemDTO.getRegPrice(),
											promoItemDTO.getPredMov()));

									promoItemDTO.setPredMar(PRCommonUtil.getMarginDollar(promoItemDTO.getRegPrice(),
											promoItemDTO.getListCost(), promoItemDTO.getPredMov()));
									promoItemDTO.setPredMarRate(PRCommonUtil
											.getDirectMarginPCT(promoItemDTO.getPredRev(), promoItemDTO.getPredMar()));
								} else {
									promoItemDTO.setPredRev(PRCommonUtil.getSalesDollar(
											promoItemDTO.getSaleInfo().getSalePrice(), promoItemDTO.getPredMov()));

									promoItemDTO.setPredMar(
											PRCommonUtil.getMarginDollar(promoItemDTO.getSaleInfo().getSalePrice(),
													promoItemDTO.getDealCost() != null && promoItemDTO.getDealCost() > 0
															? promoItemDTO.getDealCost()
															: promoItemDTO.getListCost(),
													promoItemDTO.getPredMov()));
									promoItemDTO.setPredMarRate(PRCommonUtil
											.getDirectMarginPCT(promoItemDTO.getPredRev(), promoItemDTO.getPredMar()));

								}
							}
						}
					}
				}
			}
		});
	}
	
	/**
	 * 
	 * @param weeklyItemDataMap
	 * @param retLirMap
	 * @param recommendationInputDTO
	 */
	private void updatePredictionsForLIG(HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> weeklyItemDataMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, RecommendationInputDTO recommendationInputDTO,
			boolean useRegPriceForSalesCalc) {
		weeklyItemDataMap.forEach((recWeekKey, candidateItemMap) -> {
			candidateItemMap.forEach((productKey, promoOptions) -> {
				if (productKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
					for (PromoItemDTO promoItemDTO : promoOptions) {
						updateLigLevelMovement(candidateItemMap, retLirMap, productKey.getProductId(), promoItemDTO,
								recommendationInputDTO, recWeekKey, useRegPriceForSalesCalc);
					}
				}
			});
		});
	}
	
	/**
	 * 
	 * @param candidateItemMap
	 * @param retLirMap
	 * @param retLirId
	 * @param ligPromoOption
	 * @param recommendationInputDTO
	 * @param recWeekKey
	 */
	private void updateLigLevelMovement(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap,
			HashMap<Integer, List<PRItemDTO>> retLirMap, int retLirId, PromoItemDTO ligPromoOption,
			RecommendationInputDTO recommendationInputDTO, RecWeekKey recWeekKey, boolean useRegPriceForSalesCalc) {
		List<PromoItemDTO> allPromoOptions = new ArrayList<>();
		List<PromoItemDTO> matchingPromoOptions = new ArrayList<>();
		List<PromoItemDTO> regMovementList = new ArrayList<>();
		if (retLirMap.containsKey(retLirId)) {
			List<PRItemDTO> ligMembers = retLirMap.get(retLirId);
			for (PRItemDTO prItemDTO : ligMembers) {
				ProductKey productKeyLigMember = new ProductKey(Constants.ITEMLEVELID, prItemDTO.getItemCode());
				List<PromoItemDTO> ligPromoOptions = candidateItemMap.get(productKeyLigMember);
				if (ligPromoOptions != null) {
					regMovementList.add(ligPromoOptions.iterator().next());
					allPromoOptions.addAll(ligPromoOptions);
				}
			}
		}

		List<PromoItemDTO> validPred = regMovementList.stream().filter(p -> p.getPredMovReg() > 0)
				.collect(Collectors.toList());
		double regMovLig = validPred.stream().collect(Collectors.summingDouble(PromoItemDTO::getPredMovReg));
		ligPromoOption.setPredMovReg(regMovLig);

		if (ligPromoOption.getPredMovReg() > 0) {
			ligPromoOption.setPredRevReg(
					PRCommonUtil.getSalesDollar(ligPromoOption.getRegPrice(), ligPromoOption.getPredMovReg()));

			ligPromoOption.setPredMarReg(PRCommonUtil.getMarginDollar(ligPromoOption.getRegPrice(),
					ligPromoOption.getListCost(), ligPromoOption.getPredMovReg()));
		}

		MultiWeekPredEngineItemDTO multiWeekPredEngineLIGDTO = convertToMultiWeekPredInput(ligPromoOption, recWeekKey,
				recommendationInputDTO, 0, 0.0, 0);

		MultiWeekPredKey multiWeekPredKeyLIG = MultiWeekPredEngineItemDTO
				.getMultiWeekPredKey(multiWeekPredEngineLIGDTO);

		allPromoOptions.forEach(promoItemDTO -> {
			MultiWeekPredEngineItemDTO multiWeekPredEngineItemDTO = convertToMultiWeekPredInput(promoItemDTO,
					recWeekKey, recommendationInputDTO, 0, 0.0, 0);
			MultiWeekPredKey multiWeekPredKey = MultiWeekPredEngineItemDTO
					.getMultiWeekPredKey(multiWeekPredEngineItemDTO);
			multiWeekPredKey.setItemCode(multiWeekPredKeyLIG.getItemCode());
			if (multiWeekPredKeyLIG.equals(multiWeekPredKey)) {
				matchingPromoOptions.add(promoItemDTO);
			}
		});

		if (matchingPromoOptions.size() > 0) {
			List<PromoItemDTO> validPredPromo = matchingPromoOptions.stream()
					.filter(p -> p.getPredMov() != null && p.getPredMov() > 0).collect(Collectors.toList());
			double promoMovLig = validPredPromo.stream().collect(Collectors.summingDouble(PromoItemDTO::getPredMov));
			/*
			 * double promoRevLig = matchingPromoOptions.stream() .collect(Collectors.summingDouble(PromoItemDTO::getPredRev));
			 * double promoMarLig = matchingPromoOptions.stream() .collect(Collectors.summingDouble(PromoItemDTO::getPredMar));
			 */

			ligPromoOption.setPredMov(promoMovLig);

			if (ligPromoOption.getPredMov() > 0) {
				if (useRegPriceForSalesCalc) {
					ligPromoOption.setPredRev(
							PRCommonUtil.getSalesDollar(ligPromoOption.getRegPrice(), ligPromoOption.getPredMov()));

					ligPromoOption.setPredMar(PRCommonUtil.getMarginDollar(ligPromoOption.getRegPrice(),
							ligPromoOption.getListCost(), ligPromoOption.getPredMov()));
					ligPromoOption.setPredMarRate(
							PRCommonUtil.getDirectMarginPCT(ligPromoOption.getPredRev(), ligPromoOption.getPredMar()));
				} else {
					ligPromoOption.setPredRev(PRCommonUtil.getSalesDollar(ligPromoOption.getSaleInfo().getSalePrice(),
							ligPromoOption.getPredMov()));

					ligPromoOption.setPredMar(PRCommonUtil.getMarginDollar(ligPromoOption.getSaleInfo().getSalePrice(),
							ligPromoOption.getDealCost() != null && ligPromoOption.getDealCost() > 0
									? ligPromoOption.getDealCost()
									: ligPromoOption.getListCost(),
							ligPromoOption.getPredMov()));
					ligPromoOption.setPredMarRate(
							PRCommonUtil.getDirectMarginPCT(ligPromoOption.getPredRev(), ligPromoOption.getPredMar()));
				}
			}
		}
	}
	
	/**
	 * 
	 * @param weeklyItemDataMap
	 * @param retLirMap
	 * @param recommendationInputDTO
	 */
	private void applyRecommendationsToDependentItems(
			HashMap<RecWeekKey, HashMap<ProductKey, List<PromoItemDTO>>> weeklyItemDataMap,
			RecommendationInputDTO recommendationInputDTO) {

		PromotionSelectionService promotionSelectionService = new PromotionSelectionService();

		weeklyItemDataMap.forEach((recWeekKey, candidateItemMap) -> {

			HashMap<ProductKey, HashMap<ProductKey, List<PromoItemDTO>>> leadDependentItemsMap = promotionSelectionService
					.getItemsByLeadItem(candidateItemMap);
			candidateItemMap.forEach((productKey, promoOptions) -> {

				PromoItemDTO repItemDTO = promoOptions.iterator().next();
				if (repItemDTO.isLeadItem()) {
					PromoItemDTO finalizedLead = promoOptions.stream().filter(p -> p.isFinalized()).findFirst()
							.orElse(null);
					if (finalizedLead != null) {
						
						MultiplePrice salePriceLead = finalizedLead.getSaleInfo().getSalePrice();
						
						HashMap<ProductKey, List<PromoItemDTO>> dependentItemMap = leadDependentItemsMap
								.get(productKey);

						dependentItemMap.forEach((dependentProductKey, depPromoOptions) -> {
							for (PromoItemDTO ligMemberPromoOption : depPromoOptions) {
								ligMemberPromoOption.setFinalized(false);
								MultiplePrice salePriceDep = ligMemberPromoOption.getSaleInfo().getSalePrice();
								if(salePriceLead.equals(salePriceDep)) {
									ligMemberPromoOption.setFinalized(true);
								}
							}
						});
					}
				}
			});
		});
	}
}
