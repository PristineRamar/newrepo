package com.pristine.service.offermgmt.promotion;

import java.math.RoundingMode;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.promotion.PromoSelectionDAO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.promotion.PromoAnalysisDTO;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.PromoObjectiveTypeLookup;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PromotionSelectionService {

	PromoSelectionDAO promoSelectionDAO = new PromoSelectionDAO();
	private static Logger logger = Logger.getLogger("PromotionSelectionService");

	public void selectPromotions(Connection conn, HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap,
			HashMap<Integer, List<PRItemDTO>> lirAndItsItem, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {

		// 1. Get promotion options from promotion intelligence database
		List<PromoAnalysisDTO> promoOptions = getPromotionOptionsFromPIDB(conn, candidateItemMap,
				recommendationInputDTO);

		logger.info("selectPromotions() - # of promo options retrieved: " + promoOptions.size());
		
		// 2. Assign promo options to candidates
		assignPromoOptionsToCandidates(candidateItemMap, promoOptions);

		// 3. Select promo options by objective
		selectTopXPromoOptionsByObjective(candidateItemMap);

		// 4. Add new promo options
		addNewPromotions(candidateItemMap);

		// 5. Handling group level promotions
		copyPromoOptionsToDependentItems(candidateItemMap, lirAndItsItem);
		
		// 6. copy promo options to lig members
		// Note: promotion analysis is done at LIG level. So, promo options for lig members will not be there in promo analysis DB
		// LIG level promo options should be copied to members as well
		copyPromoOptionsToLigMembers(candidateItemMap, lirAndItsItem);
		
		// 7. Set reg price as promo price for sale not found items
		setRegPriceAsPromotion(candidateItemMap);
	}

	/**
	 * 
	 * @param conn
	 * @param candidateItemMap
	 * @throws GeneralException
	 */
	private List<PromoAnalysisDTO> getPromotionOptionsFromPIDB(Connection conn,
			HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {

		logger.info("Total items: " + candidateItemMap.size());
		HashMap<ProductKey, List<PromoItemDTO>> filteredCandidateItemMap = filterByLeadAndNonGroupItems(
				candidateItemMap);

		logger.info("Filtered items: " + filteredCandidateItemMap.size());
		
		// 1. Get Non Lig items
		List<Integer> nonLigItemCodes = getNonLigItems(filteredCandidateItemMap);

		// 2. Get LIG
		List<Integer> ligRetLirIds = getLigItems(filteredCandidateItemMap);

		// 3. Get promo options from DB
		List<PromoAnalysisDTO> promoOptions = promoSelectionDAO.getPromoOptionsFromPIDB(conn, nonLigItemCodes,
				ligRetLirIds, filteredCandidateItemMap, recommendationInputDTO);

		return promoOptions;
	}

	/**
	 * 
	 * @param candidateItemMap
	 * @param promoOptions
	 */
	private void assignPromoOptionsToCandidates(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap,
			List<PromoAnalysisDTO> promoOptions) {

		double minimumSalePct = Double.parseDouble(PropertyManager.getProperty("OR_MINIMUM_SALE_PCT", "5"));
		
		HashMap<ProductKey, List<PromoAnalysisDTO>> promoOptionsMap = (HashMap<ProductKey, List<PromoAnalysisDTO>>) promoOptions
				.stream().collect(Collectors.groupingBy(PromoAnalysisDTO::getProductKey));

		logger.info("assignPromoOptionsToCandidates() -  # of items having promo options: " + promoOptionsMap.size());

		candidateItemMap.forEach((productKey, promoList) -> {

			PromoItemDTO repDTO = promoList.get(0);
			if (promoOptionsMap.containsKey(productKey)) {
				List<PromoAnalysisDTO> itemPromoOptions = promoOptionsMap.get(productKey);
				List<PromoItemDTO> optionsToBeAdded = new ArrayList<>();
				Set<MultiplePrice> uniquePricePoint = new HashSet<>();
				itemPromoOptions.forEach(promoAnalysisDTO -> {
					try {
						boolean isPromoToBeAdded = false;
						PromoItemDTO promoItemDTO = null;
						double saleP = PRFormatHelper
								.roundToTwoDecimalDigitAsDouble(promoAnalysisDTO.getSale_qty() * promoAnalysisDTO.getSale_price());
						MultiplePrice regPrice = repDTO.getRegPrice();
						MultiplePrice salePrice = new MultiplePrice(promoAnalysisDTO.getSale_qty(), saleP);
						double unitRegPrice = PRCommonUtil.getUnitPrice(regPrice, true);
						double unitSalePrice = PRCommonUtil.getUnitPrice(salePrice, true);
						
						double salePct = ((unitRegPrice - unitSalePrice)/unitRegPrice) * 100;
						
						if(unitSalePrice >= unitRegPrice) {
							logger.info("unitSalePrice >= unitRegPrice: " + repDTO.toString());
						} else if(salePct < minimumSalePct){
							logger.info("salePct < minimumSalePct: " + promoAnalysisDTO.toString());
						} else {
							if (repDTO.getSaleInfo() == null
									|| (repDTO.getSaleInfo() != null && repDTO.getSaleInfo().getSalePrice() == null)) {
								promoItemDTO = repDTO;
							} else {
								promoItemDTO = (PromoItemDTO) repDTO.clone();
								promoItemDTO.setDefaultEntry(false);
								isPromoToBeAdded = true;
							}
							transformObjects(promoItemDTO, promoAnalysisDTO);
							
							//MultiplePrice salePrice = promoItemDTO.getSaleInfo().getSalePrice();
							//MultiplePrice salePriceCurrent = repDTO.getSaleInfo().getSalePrice();
							
							//double unitSalePriceCurr = PRCommonUtil.getUnitPrice(salePriceCurrent, true);
							
							//double currentVsNewDiff = Math.abs(unitSalePriceCurr - unitSalePrice);
							/*if(currentVsNewDiff <= 0.05 && currentVsNewDiff != 0) {
								logger.info("Less difference: " + promoItemDTO.toString());
							}else */
							if(!uniquePricePoint.contains(salePrice)){
								uniquePricePoint.add(salePrice);
								if(isPromoToBeAdded) {
									optionsToBeAdded.add(promoItemDTO);		
								}
							}
						}
						//logger.debug("added promo option: " + productKey.toString());
					} catch (Exception e) {
						logger.error("assignPromoOptionsToCandidates() - Error transforming objectes", e);
					}
				});
				
				if(optionsToBeAdded.size() > 0) {
					promoList.addAll(optionsToBeAdded);
					candidateItemMap.put(productKey, promoList);
					//logger.debug("assignPromoOptionsToCandidates() - promo list size: " + promoList.size());	
				}
			}
		});
	}

	/**
	 * 
	 * @param promoItemDTO
	 * @param promoAnalysisDTO
	 */
	private void transformObjects(PromoItemDTO promoItemDTO, PromoAnalysisDTO promoAnalysisDTO) {

		// TODO Ad and Display scenario???
		/*PRItemAdInfoDTO adInfo = new PRItemAdInfoDTO();
		PRItemDisplayInfoDTO displayInfo = new PRItemDisplayInfoDTO();*/

		PRItemSaleInfoDTO saleInfo = new PRItemSaleInfoDTO();

		double saleP = PRFormatHelper
				.roundToTwoDecimalDigitAsDouble(promoAnalysisDTO.getSale_qty() * promoAnalysisDTO.getSale_price());
		MultiplePrice salePrice = new MultiplePrice(promoAnalysisDTO.getSale_qty(), saleP);
		saleInfo.setSalePrice(salePrice);
		saleInfo.setPromoTypeId(promoAnalysisDTO.getPromo_type_id());
		saleInfo.setMinQtyReqd(promoAnalysisDTO.getSale_qty());
		saleInfo.setOfferUnitType(promoAnalysisDTO.getOffer_type());
		saleInfo.setOfferValue(promoAnalysisDTO.getOffer_value());
		/*promoItemDTO.setAdInfo(adInfo);
		promoItemDTO.setDisplayInfo(displayInfo);*/
		promoItemDTO.setSaleInfo(saleInfo);

		// Set incremental metrics
		promoItemDTO.setAvgIncrementalUnits(promoAnalysisDTO.getNet_incremental_units_avg());
		promoItemDTO.setAvgIncrementalSales(promoAnalysisDTO.getNet_incremental_sales_avg());
		promoItemDTO.setAvgIncrementalMargin(promoAnalysisDTO.getNet_incremental_margin_avg());
		promoItemDTO.setPromoAnalysisDTO(promoAnalysisDTO);

	}

	/**
	 * 
	 * @param candidateItemMap
	 */
	private void selectTopXPromoOptionsByObjective(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap) {
		int topXPromosFromHistory = Integer.parseInt(PropertyManager.getProperty("OR_TOP_X_PROMO", "2"));

		// Added for handling items when there is no current promo option
		boolean rankAllOptions = Boolean.parseBoolean(PropertyManager.getProperty("OR_RANK_ALL_PROMO_OPTIONS", "FALSE"));
		
		logger.info("selectTopXPromoOptionsByRank() - Top " + topXPromosFromHistory
				+ " promotions will be selected from history.");

		logger.info("selectTopXPromoOptionsByRank() - Rank all: " + rankAllOptions);
		
		candidateItemMap.forEach((productKey, promoList) -> {

			List<PromoItemDTO> selectedPromoOptions = new ArrayList<>();

			PromoItemDTO defaultItemDTO = promoList.stream().filter(p -> p.isDefaultEntry()).findFirst().orElse(null);

			// Rank all options or exclude the current promotion
			List<PromoItemDTO> promoOptionsToBeRanked = null;
			if (rankAllOptions) {
				promoOptionsToBeRanked = promoList;
			} else {
				selectedPromoOptions.add(defaultItemDTO);
				promoOptionsToBeRanked = promoList.stream()
						.filter(p -> (!p.isDefaultEntry()
								&& !p.getSaleInfo().getSalePrice().equals(defaultItemDTO.getSaleInfo().getSalePrice())))
						.collect(Collectors.toList());
			}

			sortPromoOptionsByObjective(defaultItemDTO, promoOptionsToBeRanked);

			int rank = 0;
			for (PromoItemDTO promoItemDTO : promoOptionsToBeRanked) {
				rank++;
				
				promoItemDTO.setRank(rank);
				// Reset defalut flag when all promotions need to be ranked
				if(rankAllOptions) {
					if(rank == 1) {
						promoItemDTO.setDefaultEntry(true);
					}else {
						promoItemDTO.setDefaultEntry(false);
					}
				}
				if (rank <= topXPromosFromHistory) {
					selectedPromoOptions.add(promoItemDTO);
				} else {
					logger.debug("selectTopXPromoOptionsByRank() - Promo option skipped: " + promoItemDTO.toString());
				}
			}
			
			candidateItemMap.put(defaultItemDTO.getProductKey(), selectedPromoOptions);
		});
	}

	/**
	 * 
	 * @param promoItemDTO
	 * @param promoOptions
	 */
	private void sortPromoOptionsByObjective(PromoItemDTO promoItemDTO, List<PromoItemDTO> promoOptions) {
		//logger.debug("sortPromoOptionsByObjective() - Objective: " + promoItemDTO.getObjectiveTypeId());
		if (promoItemDTO.getObjectiveTypeId() == PromoObjectiveTypeLookup.MAXIMIZE_UNITS.getObjectiveTypeId()) {

			promoOptions.sort((p1, p2) -> Double.compare(p2.getAvgIncrementalUnits(), p1.getAvgIncrementalUnits()));

		} else if (promoItemDTO.getObjectiveTypeId() == PromoObjectiveTypeLookup.MAXIMIZE_MARGIN.getObjectiveTypeId()) {

			promoOptions.sort((p1, p2) -> Double.compare(p2.getAvgIncrementalMargin(), p1.getAvgIncrementalMargin()));

		} else if (promoItemDTO.getObjectiveTypeId() == PromoObjectiveTypeLookup.MAXIMIZE_SALES.getObjectiveTypeId()) {

			promoOptions.sort((p1, p2) -> Double.compare(p2.getAvgIncrementalSales(), p1.getAvgIncrementalSales()));

		} else if (promoItemDTO
				.getObjectiveTypeId() == PromoObjectiveTypeLookup.MAXIMIZE_SALES_WHILE_MAINTINING_MARGIN
						.getObjectiveTypeId()) {

			promoOptions.sort((p1, p2) -> Double.compare(p2.getAvgIncrementalSales(), p1.getAvgIncrementalSales()));
		}
	}

	/**
	 * 
	 * Adds new promotions
	 * 
	 * @param candidateItemMap
	 */
	private void addNewPromotions(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap) {

		boolean addNewPromotions = Boolean.parseBoolean(PropertyManager.getProperty("OR_ADD_NEW_PROMOTIONS", "FALSE"));
		double minimumSalePct = Double.parseDouble(PropertyManager.getProperty("OR_MINIMUM_SALE_PCT", "5"));
		// int newPromoOptionsToBeTried = Integer.parseInt(PropertyManager.getProperty("OR_NO_OF_NEW_PROMO_OPTIONS", "2"));
		// 1. Add promotions by case 1

		// 2. Add promotions by case 2

		// 3. Add promotions by case 3
		if (addNewPromotions) {
			DecimalFormat df = new DecimalFormat("#.#");
			df.setRoundingMode(RoundingMode.FLOOR);
			double pctOption = Double.parseDouble(PropertyManager.getProperty("OR_NEW_PROMO_RETAIL_P_OR_M_PCT", "0"));
			candidateItemMap.forEach((productKey, promoList) -> {
				if (productKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
					List<PromoItemDTO> newPromoOptions = new ArrayList<>();
					promoList.forEach(promoDTO -> {
						if (promoDTO.getSaleInfo() != null) {
							if(!promoDTO.getRegPrice().equals(promoDTO.getSaleInfo().getSalePrice())) {
								if (promoDTO.getSaleInfo().getPromoTypeId() == PromoTypeLookup.STANDARD.getPromoTypeId()) {
									double unitSaleRetail = PRCommonUtil.getUnitPrice(promoDTO.getSaleInfo().getSalePrice(),
											true);
									double pctValue = (unitSaleRetail * pctOption) / 100;
									double newSaleRetail1NoRounding = Double
											.valueOf(df.format((unitSaleRetail - pctValue)));
									double newSaleRetail2NoRounding = Double
											.valueOf(df.format((unitSaleRetail + pctValue)));

									double newSaleRetail1Rounded = PRFormatHelper.round((newSaleRetail1NoRounding + 0.09),
											2);
									double newSaleRetail2Rounded = PRFormatHelper.round((newSaleRetail2NoRounding + 0.09),
											2);

									
									double unitRegPrice = PRCommonUtil.getUnitPrice(promoDTO.getRegPrice(), true);
									
									double salePct1 = ((unitRegPrice - newSaleRetail1Rounded)/unitRegPrice) * 100;
									double salePct2 = ((unitRegPrice - newSaleRetail2Rounded)/unitRegPrice) * 100;
									
									if (salePct1 >= minimumSalePct) {
										addPromoOption(promoDTO, newPromoOptions, newSaleRetail1Rounded);
									}

									if (salePct2 >= minimumSalePct) {
										addPromoOption(promoDTO, newPromoOptions, newSaleRetail2Rounded);
									}
								}
							}
						}
					});
					if (newPromoOptions.size() > 0) {
						promoList.addAll(newPromoOptions);

						List<PromoItemDTO> uniquePromoList = new ArrayList<>();
						Set<MultiplePrice> uniquePricePoints = new HashSet<>();
						
						PromoItemDTO defaultItemDTO = promoList.stream().filter(p -> p.isDefaultEntry()).findFirst().orElse(null);
						uniquePricePoints.add(defaultItemDTO.getSaleInfo().getSalePrice());
						uniquePromoList.add(defaultItemDTO);
						
						List<PromoItemDTO> otherPromotions = promoList.stream().filter(p -> !p.isDefaultEntry())
								.collect(Collectors.toList());
						
						otherPromotions.forEach(p -> {
							if (!uniquePricePoints.contains(p.getSaleInfo().getSalePrice())
									|| p.getSaleInfo().getPromoTypeId() != PromoTypeLookup.STANDARD.getPromoTypeId()) {
								uniquePricePoints.add(p.getSaleInfo().getSalePrice());
								uniquePromoList.add(p);
							}
						});
						
						candidateItemMap.put(productKey, uniquePromoList);
					}
				}
			});
		}
	}

	
	/**
	 * 
	 * @param basePromoDTO
	 * @param promoList
	 */
	private void addPromoOption(PromoItemDTO basePromoDTO, List<PromoItemDTO> promoList, double newSaleRetail) {
		
		try {
			PromoItemDTO newPromoOption = (PromoItemDTO) basePromoDTO.clone();	
			PRItemSaleInfoDTO prItemSaleInfoDTO = new PRItemSaleInfoDTO();
			MultiplePrice salePrice = new MultiplePrice(1, newSaleRetail);
			prItemSaleInfoDTO.setSalePrice(salePrice);
			prItemSaleInfoDTO.setPromoTypeId(PromoTypeLookup.STANDARD.getPromoTypeId());
			newPromoOption.setSaleInfo(prItemSaleInfoDTO);
			newPromoOption.setRank(0);
			newPromoOption.setDefaultEntry(false);
			promoList.add(newPromoOption);
		}catch(Exception e) {
			logger.error("addNewPromotions - Error--creating new promotions", e);
		}
	}
	
	/**
	 * 
	 * @param candidateItemMap
	 * @return list of lir ids
	 */
	private List<Integer> getLigItems(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap) {

		Set<ProductKey> productKeys = candidateItemMap.keySet();

		List<Integer> lirIds = productKeys.stream().filter(p -> p.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG)
				.map(ProductKey::getProductId).collect(Collectors.toList());

		return lirIds;
	}

	/**
	 * 
	 * @param candidateItemMap
	 * @return list of non lig items
	 */
	private List<Integer> getNonLigItems(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap) {

		List<Integer> nonligItems = new ArrayList<Integer>();

		candidateItemMap.forEach((productKey, promoList) -> {

			PromoItemDTO promoItemDTO = promoList.get(0);
			if (promoItemDTO.getRetLirId() == 0 && productKey.getProductLevelId() == Constants.ITEMLEVELID) {
				nonligItems.add(productKey.getProductId());
			}

		});
		return nonligItems;
	}

	/**
	 * 
	 * @param candidateItemMap
	 * @param lirAndItsItem
	 */
	private void copyPromoOptionsToLigMembers(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap,
			HashMap<Integer, List<PRItemDTO>> lirAndItsItem) {
		candidateItemMap.forEach((productKey, promoList) -> {
			if (productKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
				List<PromoItemDTO> promoOptionsLig = promoList.stream().filter(p -> !p.isDefaultEntry())
						.collect(Collectors.toList());
				List<PRItemDTO> ligMembers = lirAndItsItem.get(productKey.getProductId());
				if (ligMembers != null) {
					ligMembers.forEach(ligMember -> {
						ProductKey productKeyLigMember = new ProductKey(Constants.ITEMLEVELID, ligMember.getItemCode());
						List<PromoItemDTO> promoOptionsLigMember = candidateItemMap.get(productKeyLigMember);
						if (promoOptionsLigMember != null) {
							PromoItemDTO promoItemRepLigMember = promoOptionsLigMember.stream()
									.filter(p -> p.isDefaultEntry()).findFirst().orElse(null);

							PromoItemDTO promoItemRepLig = promoList.stream().filter(p -> p.isDefaultEntry())
									.findFirst().orElse(null);

							promoItemRepLigMember.setSaleInfo(promoItemRepLig.getSaleInfo());

							for (PromoItemDTO promoItemDTO : promoOptionsLig) {
								try {
									PromoItemDTO promoItemMember = (PromoItemDTO) promoItemDTO.clone();
									promoItemMember.setProductKey(productKeyLigMember);
									promoItemMember.setUpc(promoItemRepLigMember.getUpc());
									promoOptionsLigMember.add(promoItemMember);
								} catch (Exception e) {
									logger.error("Error adding promo option");
								}
							}
						}
					});
				}
			}
		});
	}
	
	/**
	 * 
	 * @param candidateItems
	 * @return items filtered by lead and non group items
	 */
	private HashMap<ProductKey, List<PromoItemDTO>> filterByLeadAndNonGroupItems(
			HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap) {

		HashMap<ProductKey, List<PromoItemDTO>> filteredCandidateMap = new HashMap<>();
		candidateItemMap.forEach((productKey, promoList) -> {
			PromoItemDTO promoItemDTO = promoList.iterator().next();
			if(promoItemDTO.isLeadItem() || !promoItemDTO.isGroupLevelPromo()) {
				filteredCandidateMap.put(productKey, promoList);
			}
		});
		return filteredCandidateMap;
	}
	
	
	/**
	 * 
	 * @param candidateItemMap
	 * @param lirAndItsItem
	 */
	private void copyPromoOptionsToDependentItems(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap,
			HashMap<Integer, List<PRItemDTO>> lirAndItsItem) {
		HashMap<ProductKey, HashMap<ProductKey, List<PromoItemDTO>>> leadDependentItemsMap = getItemsByLeadItem(
				candidateItemMap);
		logger.info("Lead map size: " + leadDependentItemsMap.size());
		candidateItemMap.forEach((productKey, promoList) -> {
			PromoItemDTO repItemDTO = promoList.iterator().next();
			if (repItemDTO.isLeadItem()) {
				List<PromoItemDTO> promoOptionsLead = promoList.stream().filter(p -> !p.isDefaultEntry())
						.collect(Collectors.toList());

				HashMap<ProductKey, List<PromoItemDTO>> depItemMap = leadDependentItemsMap.get(productKey);

				if(depItemMap != null) {
					depItemMap.forEach((depProductKey, depPromoList) -> {

						PromoItemDTO promoItemRepDependent = depPromoList.stream().filter(p -> p.isDefaultEntry())
								.findFirst().orElse(null);

						PromoItemDTO promoItemRepLead = promoList.stream().filter(p -> p.isDefaultEntry())
								.findFirst().orElse(null);
						
						promoItemRepDependent.setSaleInfo(promoItemRepLead.getSaleInfo());
						promoItemRepDependent.setRank(promoItemRepLead.getRank());
						
						for (PromoItemDTO promoItemDTO : promoOptionsLead) {
							try {
								PromoItemDTO promoItemMember = (PromoItemDTO) promoItemRepDependent.clone();
								promoItemMember.setSaleInfo(promoItemDTO.getSaleInfo());
								promoItemMember.setRank(promoItemDTO.getRank());
								promoItemMember.setDefaultEntry(false);
								depPromoList.add(promoItemMember);
							} catch (Exception e) {
								logger.error("Error adding promo option");
							}
						}
					});
				}else {
					logger.error("Lead item not found: " + productKey.toString());
				}
			}
		});
	}
	
	
	/**
	 * 
	 * @param candidateItemMap
	 * @return map of lead and dependent items
	 */
	public HashMap<ProductKey, HashMap<ProductKey, List<PromoItemDTO>>> getItemsByLeadItem(
			HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap) {
		HashMap<ProductKey, HashMap<ProductKey, List<PromoItemDTO>>> leadDependentItemsMap = new HashMap<>();
		candidateItemMap.forEach((productKey, promoList) -> {
			PromoItemDTO promoItemDTO = promoList.iterator().next();
			if (promoItemDTO.getAnchorProductKey() != null) {
				HashMap<ProductKey, List<PromoItemDTO>> dependentMap = new HashMap<>();
				if (leadDependentItemsMap.containsKey(promoItemDTO.getAnchorProductKey())) {
					dependentMap = leadDependentItemsMap.get(promoItemDTO.getAnchorProductKey());
				}
				dependentMap.put(productKey, promoList);
				leadDependentItemsMap.put(promoItemDTO.getAnchorProductKey(), dependentMap);
			}
		});
		return leadDependentItemsMap;
	}
	
	
	private void setRegPriceAsPromotion(HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap) {
		candidateItemMap.forEach((productKey, promoList) -> {
			promoList.forEach(p -> {
				if (p.getSaleInfo() == null) {
					PRItemSaleInfoDTO saleInfo = new PRItemSaleInfoDTO();
					saleInfo.setSalePrice(p.getRegPrice());
					saleInfo.setPromoTypeId(PromoTypeLookup.NONE.getPromoTypeId());
					p.setSaleInfo(saleInfo);
				}
				if (p.getSaleInfo() != null && p.getSaleInfo().getSalePrice() == null) {
					PRItemSaleInfoDTO saleInfo = new PRItemSaleInfoDTO();
					saleInfo.setSalePrice(p.getRegPrice());
					saleInfo.setPromoTypeId(PromoTypeLookup.NONE.getPromoTypeId());
					p.setSaleInfo(saleInfo);
				}
			});
		});
	}
}
