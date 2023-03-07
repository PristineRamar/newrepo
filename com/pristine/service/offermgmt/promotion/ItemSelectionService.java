package com.pristine.service.offermgmt.promotion;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.promotion.PromotionEngineDAO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.MostOccurrenceData;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class ItemSelectionService {

	private static Logger logger = Logger.getLogger("PromotionSelectionService");
	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param itemMap
	 * @return selected candidates 
	 * @throws GeneralException 
	 */
	public HashMap<ProductKey, List<PromoItemDTO>> selectCandidateItems(Connection conn,
			RecommendationInputDTO recommendationInputDTO, List<Integer> storeIds, 
			HashMap<ProductKey, List<PromoItemDTO>> itemMap, RecWeekKey recWeekKey ,
			HashMap<Integer, List<PRItemDTO>> lirAndItsItems) throws GeneralException {
		HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap = new HashMap<>();

		boolean usePredefinedItems = Boolean
				.parseBoolean(PropertyManager.getProperty("OR_USE_PREDEFINIED_ITEMS", "FALSE"));
		if (usePredefinedItems) {

			String predefinedItemsSource = PropertyManager.getProperty("OR_CANDIDATE_ITEMS_SOURCE", "PROMO");
			if(recommendationInputDTO.getLeadLocationId() > 0) {
				
				candidateItemMap = getItemsPromotedFromLeadZone(conn, recommendationInputDTO, storeIds, itemMap,
						recWeekKey, lirAndItsItems);
				
			} else if (predefinedItemsSource.equals(PRConstants.OR_ITEMS_SOURCE_DB)) {

				logger.info("selectCandidateItems() - Selecting items from Item selection table...");
				// Get Items from DB
				candidateItemMap = getCandidatesFromDB(conn, recommendationInputDTO, storeIds, itemMap, recWeekKey,
						lirAndItsItems);

				logger.info("selectCandidateItems() - # of items selected from DB: " + candidateItemMap.size());
				
			} else if (predefinedItemsSource.equals(PRConstants.OR_ITEMS_SOURCE_ALREADY_PROMOTED)) {

				// Get items that are already promoted
				logger.info("selectCandidateItems() - Using already promoted items as candidates");
				candidateItemMap = getAlreadyPromotedItems(conn, recommendationInputDTO, storeIds, itemMap,
						recWeekKey.weekStartDate, lirAndItsItems);
				logger.info("selectCandidateItems() - # of items promoted: " + candidateItemMap.size());
			}

		} else {

			// Trigger item selection process

		}

		return candidateItemMap;
	}
	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @return
	 * @throws GeneralException 
	 */
	public HashMap<ProductKey, List<PromoItemDTO>> getAlreadyPromotedItems(Connection conn,
			RecommendationInputDTO recommendationInputDTO, List<Integer> storeIds,
			HashMap<ProductKey, List<PromoItemDTO>> itemMap, String weekStartDate, 
			HashMap<Integer, List<PRItemDTO>> lirAndItsItems) throws GeneralException {
		HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap = new HashMap<>();

		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		HashMap<Integer, List<PRItemSaleInfoDTO>> saleInfoMap = pricingEngineDAO.getSaleDetails(conn,
				recommendationInputDTO.getProductLevelId(), recommendationInputDTO.getProductId(),
				recommendationInputDTO.getChainId(), recommendationInputDTO.getLocationLevelId(),
				recommendationInputDTO.getLocationId(), weekStartDate, 1, storeIds, false);

		
		HashMap<Integer, List<PRItemAdInfoDTO>> adInfoMap = pricingEngineDAO.getAdDetails(conn,
				recommendationInputDTO.getProductLevelId(), recommendationInputDTO.getProductId(),
				recommendationInputDTO.getChainId(), recommendationInputDTO.getLocationLevelId(),
				recommendationInputDTO.getLocationId(), weekStartDate, 1, storeIds, false);
		
		//LocalDate recWeekDate = LocalDate.parse(recommendationInputDTO.getStartWeek(), PRCommonUtil.getDateFormatter());
		itemMap.forEach((productKey, promoList) -> {
			if (productKey.getProductLevelId() == Constants.ITEMLEVELID) {
				int itemCode = productKey.getProductId();
				if (saleInfoMap.containsKey(itemCode)) {
					List<PRItemSaleInfoDTO> saleInfoList = saleInfoMap.get(itemCode);
					PRItemSaleInfoDTO saleInfo = saleInfoList.get(0);

					/*
					 * LocalDate startDate = LocalDate.parse(saleInfo.getSaleStartDate(), PRCommonUtil.getDateFormatter()); if
					 * (recWeekDate.isEqual(startDate)) {
					 */
					PromoItemDTO promoItemDTO = promoList.get(0);
					if (saleInfo.getOfferValue() > 0 && saleInfo.getMinQtyReqd() > 0) {
						if (saleInfo.getOfferUnitType().equals("D")) {
							double regPrice = PRCommonUtil.getUnitPrice(promoItemDTO.getRegPrice(), true);
							double newSalePrice = regPrice - saleInfo.getOfferValue();
							double salePrRound3 = PRFormatHelper.round((newSalePrice * saleInfo.getMinQtyReqd()), 3);
							double salePrRound2 = PRFormatHelper.round(salePrRound3, 2);
							MultiplePrice salePrice = new MultiplePrice(saleInfo.getMinQtyReqd(), salePrRound2);
							if(saleInfo.getOfferValue() > regPrice) {
								saleInfo.setSalePrice(promoItemDTO.getRegPrice());	
							}else {
								saleInfo.setSalePrice(salePrice);	
							}
						} else if (saleInfo.getOfferUnitType().equals("P") && saleInfo.getMinQtyReqd() > 0) {
							double regPrice = PRCommonUtil.getUnitPrice(promoItemDTO.getRegPrice(), true);
							double discount = (regPrice * saleInfo.getOfferValue()) / 100;
							double newSalePrice = regPrice - discount;
							double salePrRound3 = PRFormatHelper.round((newSalePrice * saleInfo.getMinQtyReqd()), 3);
							double salePrRound2 = PRFormatHelper.round(salePrRound3, 2);
							MultiplePrice salePrice = new MultiplePrice(saleInfo.getMinQtyReqd(), salePrRound2);
							saleInfo.setSalePrice(salePrice);
						}
					}
					
					if(saleInfo != null && saleInfo.getSalePrice() != null) {
						if(saleInfo.getSalePrice().price == 0) {
							saleInfo.setSalePrice(promoItemDTO.getRegPrice());	
						}	
					}
					
					
					promoItemDTO.setSaleInfo(saleInfo);
					if(adInfoMap.containsKey(itemCode)) {
						List<PRItemAdInfoDTO> adInfoList = adInfoMap.get(itemCode);
						PRItemAdInfoDTO adInfo = adInfoList.get(0);
						promoItemDTO.setAdInfo(adInfo);
					}
					
					
					promoItemDTO.setDefaultEntry(true);
					candidateItemMap.put(productKey, promoList);
					// }
				}
			}
		});

		// LIG level data
		itemMap.forEach((productKey, promoList) -> {
			if (productKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
				int retLirId = productKey.getProductId();
				List<PRItemDTO> ligMembers = lirAndItsItems.get(retLirId);
				
				PRItemSaleInfoDTO ligLevelSaleInfo = getSaleInfoForLIG(ligMembers, itemMap);
				PRItemAdInfoDTO adInfoLIG = getAdInfoForLIG(ligMembers, itemMap);
				if(ligLevelSaleInfo != null) {
					PromoItemDTO promoItemDTOLig = promoList.get(0);
					if(ligLevelSaleInfo.getSalePrice() != null) {
						if(ligLevelSaleInfo.getSalePrice().price == 0) {
							ligLevelSaleInfo.setSalePrice(promoItemDTOLig.getRegPrice());	
						}	
					}
					promoItemDTOLig.setSaleInfo(ligLevelSaleInfo);
					if(adInfoLIG != null) {
						promoItemDTOLig.setAdInfo(adInfoLIG);
					}
					promoItemDTOLig.setDefaultEntry(true);
					for (PRItemDTO prItemDTO : ligMembers) {
						ProductKey productKeyLigMember = new ProductKey(Constants.ITEMLEVELID, prItemDTO.getItemCode());
						if (itemMap.containsKey(productKeyLigMember)) {
							List<PromoItemDTO> promoOptionsLigMember = itemMap.get(productKeyLigMember);
							PromoItemDTO promoItemDTO = promoOptionsLigMember.get(0);
							promoItemDTO.setSaleInfo(ligLevelSaleInfo);
							if(adInfoLIG != null) {
								promoItemDTO.setAdInfo(adInfoLIG);
							}
							promoItemDTO.setDefaultEntry(true);
							promoItemDTOLig.setStrategyDTO(promoItemDTO.getStrategyDTO());
							promoItemDTOLig.setObjectiveTypeId(promoItemDTO.getObjectiveTypeId());
							candidateItemMap.put(productKeyLigMember, promoOptionsLigMember);
						}
					}
					
					candidateItemMap.put(productKey, promoList);
				} 
			}
		});

		return candidateItemMap;
	}
	
	
	private PRItemSaleInfoDTO getSaleInfoForLIG(List<PRItemDTO> ligMembers, HashMap<ProductKey, List<PromoItemDTO>> itemMap) {
		MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
		PRItemDTO sampleItem = ligMembers.get(0);
		ProductKey productKeySampleItem = new ProductKey(Constants.ITEMLEVELID, sampleItem.getItemCode());
		PRItemSaleInfoDTO prItemSaleInfoDTO = new PRItemSaleInfoDTO();
		if(itemMap.containsKey(productKeySampleItem)) {
			List<PromoItemDTO> promoOptionsLigMember = itemMap.get(productKeySampleItem);
			PromoItemDTO promoItemDTO = promoOptionsLigMember.get(0);
			if(promoItemDTO.getSaleInfo() != null) {
				prItemSaleInfoDTO = promoItemDTO.getSaleInfo(); 
			}
		}
		
		List<MultiplePrice> salePrices = new ArrayList<MultiplePrice>();
		List<Integer> promoTypes = new ArrayList<Integer>();
		
		for(PRItemDTO prItemDTO: ligMembers) {
			ProductKey productKeyLigMember = new ProductKey(Constants.ITEMLEVELID, prItemDTO.getItemCode());
			if(itemMap.containsKey(productKeyLigMember)) {
				List<PromoItemDTO> promoOptionsLigMember = itemMap.get(productKeyLigMember);
				PromoItemDTO promoItemDTO = promoOptionsLigMember.get(0);
				if(promoItemDTO.getSaleInfo() != null) {
					salePrices.add(promoItemDTO.getSaleInfo().getSalePrice());
					promoTypes.add(promoItemDTO.getSaleInfo().getPromoTypeId());
				}
			}
		}
		
		/*for(Integer promoType: promoTypes) {
			logger.debug("Promo Type: " + promoType);
		}
		
		for(MultiplePrice salePrice: salePrices) {
			logger.debug("Sale price: " + salePrice);	
		}*/
		
		if(salePrices.size() > 0) {
			HashMap<MultiplePrice, Integer> salePriceMap = new HashMap<>();
			salePrices.forEach(multiPrice -> {
				int count = 1;
				if(salePriceMap.containsKey(multiPrice)) {
					count = salePriceMap.get(multiPrice);
					count++;
				}
				salePriceMap.put(multiPrice, count);
			});
			
			TreeMap<Integer, List<MultiplePrice>> saleCountMap = new TreeMap<>();
			salePriceMap.forEach((price, count) -> {
				List<MultiplePrice> tempList = new ArrayList<>();
				if(saleCountMap.containsKey(count)) {
					tempList = saleCountMap.get(count);
				}
				tempList.add(price);
				saleCountMap.put(count, tempList);
			});
			
			
			int maxCount = Collections.max(saleCountMap.keySet());
			
			List<MultiplePrice> maxCountPrices = saleCountMap.get(maxCount);
			if(maxCountPrices.size() > 1) {
				Optional<MultiplePrice> mostCommonPrice = maxCountPrices.stream().min(Comparator.comparingDouble(MultiplePrice::getUnitPrice));
				if(mostCommonPrice.isPresent()) {
					// Get most common sale price
					Integer commonPromoTypeId = (Integer) mostOccurrenceData.getMaxOccurance(promoTypes);
					prItemSaleInfoDTO.setSalePrice(mostCommonPrice.get());
					prItemSaleInfoDTO.setPromoTypeId(commonPromoTypeId);	
				}
			}else {
				// Get most common sale price
				MultiplePrice commonSalePrice = (MultiplePrice) mostOccurrenceData.getMaxOccurance(salePrices);
				Integer commonPromoTypeId = (Integer) mostOccurrenceData.getMaxOccurance(promoTypes);

				//logger.debug("Common promo type: " + commonPromoTypeId);
				if(commonPromoTypeId == null) {
					prItemSaleInfoDTO = null;
				}else {
					prItemSaleInfoDTO.setSalePrice(commonSalePrice);
					prItemSaleInfoDTO.setPromoTypeId(commonPromoTypeId);	
				}
			}
		} else {
			prItemSaleInfoDTO = null;
		}
		
		return prItemSaleInfoDTO;
	}
	
	
	/**
	 * 
	 * @param ligMembers
	 * @param itemMap
	 * @return Ad info
	 */
	private PRItemAdInfoDTO getAdInfoForLIG(List<PRItemDTO> ligMembers,
			HashMap<ProductKey, List<PromoItemDTO>> itemMap) {
		PRItemAdInfoDTO prItemAdInfoDTO = new PRItemAdInfoDTO();
		for (PRItemDTO prItemDTO : ligMembers) {
			ProductKey productKeyLigMember = new ProductKey(Constants.ITEMLEVELID, prItemDTO.getItemCode());
			if (itemMap.containsKey(productKeyLigMember)) {
				List<PromoItemDTO> promoOptionsLigMember = itemMap.get(productKeyLigMember);
				PromoItemDTO promoItemDTO = promoOptionsLigMember.get(0);
				if (promoItemDTO.getAdInfo() != null) {
					prItemAdInfoDTO = promoItemDTO.getAdInfo();
				}
			}
		}
		return prItemAdInfoDTO;
	}
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param storeIds
	 * @param itemMap
	 * @param weekStartDate
	 * @param lirAndItsItems
	 * @return candidate items from DB
	 * @throws GeneralException 
	 */
	public HashMap<ProductKey, List<PromoItemDTO>> getCandidatesFromDB(Connection conn,
			RecommendationInputDTO recommendationInputDTO, List<Integer> storeIds,
			HashMap<ProductKey, List<PromoItemDTO>> itemMap, RecWeekKey recWeekKey,
			HashMap<Integer, List<PRItemDTO>> lirAndItsItems) throws GeneralException {
		HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap = new HashMap<>();
		PromotionEngineDAO promotionEngineDAO = new PromotionEngineDAO();

		List<PromoItemDTO> candidates = promotionEngineDAO.getPromoCandidates(conn, recommendationInputDTO,
				recWeekKey.calendarId);

		HashMap<ProductKey, List<PromoItemDTO>> promoItemMap = (HashMap<ProductKey, List<PromoItemDTO>>) candidates
				.stream().collect(Collectors.groupingBy(PromoItemDTO::getProductKey));

		itemMap.forEach((productKey, promoList) -> {
			if (promoItemMap.containsKey(productKey)) {
				List<PromoItemDTO> promoListTemp = promoItemMap.get(productKey);
				PromoItemDTO promoItemDTO = promoList.iterator().next();
				PromoItemDTO promoItemDTOTemp = promoListTemp.iterator().next();
				promoItemDTO.setAnchorProdId(promoItemDTOTemp.getAnchorProdId());
				promoItemDTO.setAnchorProdLevelId(promoItemDTOTemp.getAnchorProdLevelId());
				promoItemDTO.setGroupLevelPromo(promoItemDTOTemp.isGroupLevelPromo());
				promoItemDTO.setLeadItem(promoItemDTOTemp.isLeadItem());
				promoItemDTO.setDefaultEntry(true);
				logger.debug(promoItemDTOTemp);
				logger.debug(promoItemDTO);
				candidateItemMap.put(productKey, promoList);
			}
		});

		itemMap.forEach((productKey, promoList) -> {
			if (productKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG
					&& candidateItemMap.containsKey(productKey)) {
				PromoItemDTO promoItemDTOLig = promoList.get(0);
				List<PRItemDTO> ligMembers = lirAndItsItems.get(productKey.getProductId());
				ligMembers.forEach(itemDTO -> {
					ProductKey productKeyLigMember = new ProductKey(Constants.ITEMLEVELID, itemDTO.getItemCode());
					if (itemMap.containsKey(productKeyLigMember)) {
						List<PromoItemDTO> promoListMember = itemMap.get(productKeyLigMember);
						PromoItemDTO promoItemDTO = promoListMember.iterator().next();
						promoItemDTO.setDefaultEntry(true);
						promoItemDTOLig.setStrategyDTO(promoItemDTO.getStrategyDTO());
						promoItemDTOLig.setObjectiveTypeId(promoItemDTO.getObjectiveTypeId());
						candidateItemMap.put(productKeyLigMember, promoListMember);
					}
				});
			}
		});

		return candidateItemMap;
	}
	
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @param storeIds
	 * @param itemMap
	 * @param weekStartDate
	 * @param lirAndItsItems
	 * @return candidate items from DB
	 * @throws GeneralException 
	 */
	public HashMap<ProductKey, List<PromoItemDTO>> getItemsPromotedFromLeadZone(Connection conn,
			RecommendationInputDTO recommendationInputDTO, List<Integer> storeIds,
			HashMap<ProductKey, List<PromoItemDTO>> itemMap, RecWeekKey recWeekKey,
			HashMap<Integer, List<PRItemDTO>> lirAndItsItems) throws GeneralException {
		HashMap<ProductKey, List<PromoItemDTO>> candidateItemMap = new HashMap<>();
		PromotionEngineDAO promotionEngineDAO = new PromotionEngineDAO();
		
		HashMap<ProductKey, List<PRItemSaleInfoDTO>> leadPromoRecMap = promotionEngineDAO
				.getPromoRecommendationForLeadZone(conn, recommendationInputDTO.getLeadLocationLevelId(),
						recommendationInputDTO.getLeadLocationId(), recommendationInputDTO.getProductLevelId(),
						recommendationInputDTO.getProductId(), recWeekKey.weekStartDate);

		itemMap.forEach((productKey, promoList) -> {
			if (leadPromoRecMap.containsKey(productKey)) {
				List<PRItemSaleInfoDTO> promoListTemp = leadPromoRecMap.get(productKey);
				PromoItemDTO promoItemDTO = promoList.iterator().next();
				PRItemSaleInfoDTO promoItemDTOTemp = promoListTemp.iterator().next();
				MultiplePrice salePrice = promoItemDTOTemp.getSalePrice();
				MultiplePrice regPrice = promoItemDTO.getRegPrice();
				double unitRegPrice = PRCommonUtil.getUnitPrice(regPrice, true);
				double unitSalePrice = PRCommonUtil.getUnitPrice(salePrice, true);
				if(unitRegPrice > unitSalePrice) {
					promoItemDTO.setSaleInfo(promoItemDTOTemp);
					promoItemDTO.setDefaultEntry(true);
					candidateItemMap.put(productKey, promoList);
					logger.debug(promoItemDTOTemp);
					logger.debug(promoItemDTO);
				}
			}
		});

		return candidateItemMap;
	}
}
