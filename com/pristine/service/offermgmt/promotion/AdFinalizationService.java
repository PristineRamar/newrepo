package com.pristine.service.offermgmt.promotion;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
//import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.promotion.BlockDetail;
import com.pristine.dto.offermgmt.promotion.PageBlockNoKey;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.lookup.offermgmt.promotion.AdObjectiveLookup;
import com.pristine.util.PropertyManager;

public class AdFinalizationService {

	private static Logger logger = Logger.getLogger("AdFinalizationService");
	
	private double minSalesLiftPCTPerBlock =  Integer.valueOf(PropertyManager.getProperty("AD_REC_MIN_SALES_LIFT_PCT_PER_BLOCK"));
	private int minSalesPerBlock =   Integer.valueOf(PropertyManager.getProperty("AD_REC_MIN_SALES_PER_BLOCK"));
	
	/**
	 * @param actualAdBlockDetails
	 * @param actualAdItemsLIGNonLigSummary
	 * @param indLevelSummary
	 * @param ppgAndItsItemLevelSummary
	 * @return
	 */
	public HashMap<PageBlockNoKey, List<PromoItemDTO>> finalizeItems(HashMap<PageBlockNoKey, BlockDetail> actualAdBlockDetails,
			HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary,
			List<PromoItemDTO> indItemSummary, List<PromoItemDTO> ppgAndItsItemSummary, double inputMinMarginPCT) {
		logger.info("finalizeItems is strated...");
//		logger.debug("ppgAndItsItemSummary:" + ppgAndItsItemSummary.toString());
		
		HashMap<PageBlockNoKey, List<PromoItemDTO>> finalizedItemMap = new HashMap<PageBlockNoKey, List<PromoItemDTO>>();
		
		HashMap<ProductKey, PromoItemDTO> alreadyPickedItems = new HashMap<ProductKey, PromoItemDTO>();
		Set<Integer> alreadyPickedCategories = new HashSet<Integer>();
		
		// first process blocks where there is closer item match 
//		boolean isProcessCloseMatchingItems = true;
//		processBlock(isProcessCloseMatchingItems, alreadyPickedItems, alreadyPickedCategories, finalizedItemMap, actualAdBlockDetails,
//				actualAdItemsLIGNonLigSummary, indItemSummary, ppgAndItsItemSummary, inputMinMarginPCT);
//
//		isProcessCloseMatchingItems = false;
//		// process non-processed blocks now
//		processBlock(isProcessCloseMatchingItems, alreadyPickedItems, alreadyPickedCategories, finalizedItemMap, actualAdBlockDetails,
//				actualAdItemsLIGNonLigSummary, indItemSummary, ppgAndItsItemSummary, inputMinMarginPCT);
		
		processBlock(alreadyPickedItems, alreadyPickedCategories, finalizedItemMap, actualAdBlockDetails,
				actualAdItemsLIGNonLigSummary, indItemSummary, ppgAndItsItemSummary, inputMinMarginPCT);
		
		logger.info("finalizeItems is completed...");
		return finalizedItemMap;
	}
	
	
	private void processBlock(HashMap<ProductKey, PromoItemDTO> alreadyPickedItems, Set<Integer> alreadyPickedCategories,
			HashMap<PageBlockNoKey, List<PromoItemDTO>> finalizedItemMap, HashMap<PageBlockNoKey, BlockDetail> actualAdBlockDetails,
			HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary, List<PromoItemDTO> indItemSummary,
			List<PromoItemDTO> ppgAndItsItemSummary, double inputMinMarginPCT) {

		List<PromoItemDTO> indItemSummaryOfDept = new ArrayList<PromoItemDTO>();
		List<PromoItemDTO> ppgAndItsItemSummaryOfDept = new ArrayList<PromoItemDTO>();

		for (Map.Entry<PageBlockNoKey, BlockDetail> blockDetailsEntry : actualAdBlockDetails.entrySet()) {
			BlockDetail actualBlockDetail = blockDetailsEntry.getValue();
			PageBlockNoKey pageBlockNoKey = blockDetailsEntry.getKey();

			List<PromoItemDTO> actualBlockItems = actualAdItemsLIGNonLigSummary.get(pageBlockNoKey);

			// Set<ProductKey> actualBlockItemSet =
			// actualBlockItems.stream().map(PromoItemDTO::getProductKey).collect(Collectors.toSet());

			// Get items belong to actual ad department
			indItemSummaryOfDept = getItemsOfDepartment(indItemSummary, actualBlockDetail.getDepartments(), false);
			ppgAndItsItemSummaryOfDept = getItemsOfDepartment(ppgAndItsItemSummary, actualBlockDetail.getDepartments(), false);

			// logger.debug("ppgAndItsItemSummaryOfDept:" + ppgAndItsItemSummaryOfDept.toString());

			logger.debug("indItemSummaryOfDept.size():" + indItemSummaryOfDept.size() + ",ppgAndItsItemSummaryOfDept.size():"
					+ ppgAndItsItemSummaryOfDept.size());
			// logger.debug("itemsAlreadyPickedForRec:" + actualBlockItemSet);

			// Find actual ad items in other blocks of the department
			HashMap<ProductKey, PromoItemDTO> otherActualItemsOfDep = findActualItemsInOtherBlock(actualAdItemsLIGNonLigSummary, actualAdBlockDetails,
					actualBlockDetail.getDepartments(), pageBlockNoKey);

			logger.debug("otherActualItemsOfDep:" + otherActualItemsOfDep.toString());

			// Ignore brands already present in other blocks of same department in actual ad and already picked in recommendations
			List<PromoItemDTO> filteredIndItemSummary = filterItems(indItemSummaryOfDept, alreadyPickedItems, alreadyPickedCategories,
					otherActualItemsOfDep);
			List<PromoItemDTO> filteredPPGAndItsItemSummary = filterItemsInPPG(ppgAndItsItemSummaryOfDept, alreadyPickedItems,
					alreadyPickedCategories, otherActualItemsOfDep);

			// logger.debug("filteredPPGAndItsItemSummary:" + filteredPPGAndItsItemSummary.toString());

			logger.debug("filteredIndItemSummary.size():" + filteredIndItemSummary.size() + ",filteredPPGAndItsItemSummary.size():"
					+ filteredPPGAndItsItemSummary.size());

			// Find the ppg summary again
			updatePPGLevelSummary(filteredPPGAndItsItemSummary);

			// logger.debug("filteredPPGAndItsItemSummary:" + filteredPPGAndItsItemSummary.toString());

			// Pick ind / PPG whose sales > actual block sales and margin > 0
			PromoItemDTO selectedItemOrPPG = applyObjective(AdObjectiveLookup.MAXIMIZE_SALES_WITHOUT_REDUCING_MARGIN.getObjectiveTypeId(),
					actualBlockDetail, filteredIndItemSummary, filteredPPGAndItsItemSummary, inputMinMarginPCT);

			// logger.debug("filteredIndAndPPGItemSummary.size():" + filteredPPGAndItsItemSummary.size());

			List<PromoItemDTO> finalItemList = null;
			if (selectedItemOrPPG != null) {
				finalItemList = getIndOrPPGItems(selectedItemOrPPG, filteredPPGAndItsItemSummary);
			} else {
				// If there is no recommendations, put same as actuals
				finalItemList = actualBlockItems;
			}

			logger.debug("pageBlockNoKey:" + pageBlockNoKey.toString() + ",Final Ad Items:" + finalItemList.toString());
			finalizedItemMap.put(pageBlockNoKey, finalItemList);

			for (PromoItemDTO promoItemDTO : finalItemList) {
				if (alreadyPickedItems.get(promoItemDTO.getProductKey()) == null) {
					alreadyPickedItems.put(promoItemDTO.getProductKey(), promoItemDTO);
					alreadyPickedCategories.add(promoItemDTO.getCategoryId());
				}
			}
		}
	}
	
	/**
	 * Returns items of department(s)
	 * @param itemList
	 * @param deptIdMap
	 * @param fetchPPGSummary
	 * @return
	 */
	private List<PromoItemDTO> getItemsOfDepartment(List<PromoItemDTO> itemList, HashMap<Integer, String> deptIdMap, boolean fetchPPGSummary) {
		List<PromoItemDTO> itemsInDept = new ArrayList<PromoItemDTO>();
		for (PromoItemDTO promoItemDTO : itemList) {
			if (deptIdMap.get(promoItemDTO.getDeptId()) != null) {
				if (!fetchPPGSummary) {
					itemsInDept.add(promoItemDTO);
				} else if (fetchPPGSummary && promoItemDTO.isPPGLevelSummary()) {
					itemsInDept.add(promoItemDTO);
				}
			}
		}
		return itemsInDept;
	}
	
	/***
	 * If ind item returns only that item, if it is ppg summary, returns all its ppg items
	 * @param indOrPPGItem
	 * @return
	 */
	private List<PromoItemDTO> getIndOrPPGItems(PromoItemDTO indOrPPGItem, List<PromoItemDTO> filteredPPGAndItsItemSummary) {
		List<PromoItemDTO> finalItemList = new ArrayList<PromoItemDTO>();
		if (indOrPPGItem.isPPGLevelSummary()) {
			finalItemList = filteredPPGAndItsItemSummary.stream()
					.filter(p -> p.getPpgGroupId().equals(indOrPPGItem.getPpgGroupId())
							&& p.getPpgPromoCombinationId() == indOrPPGItem.getPpgPromoCombinationId() && !p.isPPGLevelSummary())
					.collect(Collectors.toList());
		} else {
			finalItemList.add(indOrPPGItem);
		}

		return finalItemList;
	}
	
	/**
	 * Ignored items that are already in the actual block or already picked in recommendation
	 * @param indItemSummary
	 * @param actualBlockItemSet
	 * @param alreadyPickedItemSet
	 * @return
	 */
	private List<PromoItemDTO> filterItems(List<PromoItemDTO> indItemSummary, HashMap<ProductKey, PromoItemDTO> alreadyPickedItemSet,
			Set<Integer> alreadyPickedCategories, HashMap<ProductKey, PromoItemDTO> otherActualItemsOfDep) {
		List<PromoItemDTO> filteredList = new ArrayList<PromoItemDTO>();
		Set<Integer> actualOtherItemsBrand = new HashSet<Integer>();
		
		// Get brand promoted in actual other blocks
		for (Map.Entry<ProductKey, PromoItemDTO> actualItems : otherActualItemsOfDep.entrySet()) {
			if(actualItems.getValue().getBrandId() > 0) {
				actualOtherItemsBrand.add(actualItems.getValue().getBrandId());
			}
		}
		logger.debug("actualItemsBrand:" + actualOtherItemsBrand);
		
		for(PromoItemDTO promoItemDTO : indItemSummary) {
			PromoItemDTO alreadyPickedItem = alreadyPickedItemSet.get(promoItemDTO.getProductKey());
			PromoItemDTO actualItem = otherActualItemsOfDep.get(promoItemDTO.getProductKey());
			boolean ignoreItem = false;
			
			//Ignore already picked items or category
			if (alreadyPickedItem != null || actualItem != null || alreadyPickedCategories.contains(promoItemDTO.getCategoryId())
					|| actualOtherItemsBrand.contains(promoItemDTO.getBrandId())) {
				ignoreItem = true;
			}
			
			//if(!actualBlockItemSet.contains(promoItemDTO.getProductKey()) && !alreadyPickedItemSet.contains(promoItemDTO.getProductKey())) {
			if(!ignoreItem) {
				filteredList.add(promoItemDTO);
			}
		}
		
		return filteredList;
	}
	
	private List<PromoItemDTO> filterItemsInPPG(List<PromoItemDTO> ppgAndItsItemSummary, HashMap<ProductKey, PromoItemDTO> alreadyPickedItemSet,
			Set<Integer> alreadyPickedCategories, HashMap<ProductKey, PromoItemDTO> otherActualItemsOfDep) {
		List<PromoItemDTO> filteredList = new ArrayList<PromoItemDTO>();
		List<PromoItemDTO> ppgGroupItems = new ArrayList<PromoItemDTO>();
		
		for (PromoItemDTO ppgAndItsItem : ppgAndItsItemSummary) {
			if (ppgAndItsItem.isPPGLevelSummary()) {
				ppgGroupItems.add(ppgAndItsItem);
			}
		}
		
		for (PromoItemDTO ppgGroupItem : ppgGroupItems) {

//			logger.debug("ppgAndItsItem.getPpgGroupId():" + ppgGroupItem.getPpgGroupId());
			
			List<PromoItemDTO> ppgItems = ppgAndItsItemSummary.stream()
					.filter(p -> p.getPpgGroupId().equals(ppgGroupItem.getPpgGroupId()) && !p.isPPGLevelSummary()
							&& p.getPpgPromoCombinationId() == ppgGroupItem.getPpgPromoCombinationId()).collect(Collectors.toList());
			
			List<PromoItemDTO> tempList = filterItems(ppgItems, alreadyPickedItemSet, alreadyPickedCategories, otherActualItemsOfDep);

//			logger.debug("ppgItems.size():" + ppgItems.size());

			// Ignore entire group even if one of the item in the PPG is already picked
//			if (ppgItems.size() == tempList.size()) {
				filteredList.add(ppgGroupItem);
				filteredList.addAll(tempList);
//			}

		}
		return filteredList;
	}
	
	/**
	 * Update the ppg level predictions
	 * @param ppgAndItsItems
	 * @return
	 */
	private void updatePPGLevelSummary(List<PromoItemDTO> ppgAndItsItems) {
		for (PromoItemDTO ppgItemSummary : ppgAndItsItems) {
			if (ppgItemSummary.isPPGLevelSummary()) {

				Long ppgGroupId = ppgItemSummary.getPpgGroupId();
				int ppgPromoCombinationId = ppgItemSummary.getPpgPromoCombinationId();

				ppgItemSummary.setPredMov(ppgAndItsItems.stream().filter(
						p -> p.getPpgGroupId().equals(ppgGroupId) && p.getPpgPromoCombinationId() == ppgPromoCombinationId && !p.isPPGLevelSummary())
						.mapToDouble(p -> p.getPredMov()).sum());

				ppgItemSummary.setPredMar(ppgAndItsItems.stream().filter(
						p -> p.getPpgGroupId().equals(ppgGroupId) && p.getPpgPromoCombinationId() == ppgPromoCombinationId && !p.isPPGLevelSummary())
						.mapToDouble(p -> p.getPredMar()).sum());

				ppgItemSummary.setPredRev(ppgAndItsItems.stream().filter(
						p -> p.getPpgGroupId().equals(ppgGroupId) && p.getPpgPromoCombinationId() == ppgPromoCombinationId && !p.isPPGLevelSummary())
						.mapToDouble(p -> p.getPredRev()).sum());

				ppgItemSummary.setNoOfHHRecommendedTo(ppgAndItsItems.stream().filter(
						p -> p.getPpgGroupId().equals(ppgGroupId) && p.getPpgPromoCombinationId() == ppgPromoCombinationId && !p.isPPGLevelSummary())
						.mapToInt(p -> p.getNoOfHHRecommendedTo()).sum());

				ppgItemSummary.setPredMovReg(ppgAndItsItems.stream().filter(
						p -> p.getPpgGroupId().equals(ppgGroupId) && p.getPpgPromoCombinationId() == ppgPromoCombinationId && !p.isPPGLevelSummary())
						.mapToDouble(p -> p.getPredMovReg()).sum());

				ppgItemSummary.setPredMarReg(ppgAndItsItems.stream().filter(
						p -> p.getPpgGroupId().equals(ppgGroupId) && p.getPpgPromoCombinationId() == ppgPromoCombinationId && !p.isPPGLevelSummary())
						.mapToDouble(p -> p.getPredMarReg()).sum());

				ppgItemSummary.setPredRevReg(ppgAndItsItems.stream().filter(
						p -> p.getPpgGroupId().equals(ppgGroupId) && p.getPpgPromoCombinationId() == ppgPromoCombinationId && !p.isPPGLevelSummary())
						.mapToDouble(p -> p.getPredRevReg()).sum());
			}
		}
	}

	private PromoItemDTO applyObjective(int objectiveTypeId, BlockDetail actualBlockDetail, List<PromoItemDTO> indSummaryItems,
			List<PromoItemDTO> ppgSummaryItems, double inputMinMarginPCT) {

		PromoItemDTO promoItemDTO = null;

		if (objectiveTypeId == AdObjectiveLookup.NONE.getObjectiveTypeId()) {
			promoItemDTO = filterItemBasedOnMetrics(actualBlockDetail, indSummaryItems, ppgSummaryItems);
		} else if (objectiveTypeId == AdObjectiveLookup.HIGHEST_SALES.getObjectiveTypeId()) {
			promoItemDTO = getHighestSalesItems(actualBlockDetail, indSummaryItems, ppgSummaryItems);
		} else if (objectiveTypeId == AdObjectiveLookup.HIGHEST_SALES_WITH_MIN_MARGIN.getObjectiveTypeId()) {
			promoItemDTO = getHighestSalesItemsWithMinMargin(actualBlockDetail, indSummaryItems, ppgSummaryItems, inputMinMarginPCT);
		} else if (objectiveTypeId == AdObjectiveLookup.MAXIMIZE_SALES_WITHOUT_REDUCING_MARGIN.getObjectiveTypeId()) {
			promoItemDTO = maximizeSalesWithoutReducingMargin(actualBlockDetail, indSummaryItems, ppgSummaryItems);
		}

		return promoItemDTO;
	}
	
	/**
	 * Filter items based on sales and margin
	 * @param indSummaryItems
	 * @param ppgSummaryItems
	 * @return
	 */
	private PromoItemDTO filterItemBasedOnMetrics(BlockDetail actualBlockDetail, List<PromoItemDTO> indSummaryItems,
			List<PromoItemDTO> ppgSummaryItems) {
		List<PromoItemDTO> filteredList = new ArrayList<PromoItemDTO>();
		PromoItemDTO finalizedIndOrPPGItem = null;
		
		// Revenue > actual block revenue, margin > 0, Revenue > min revenue, revenue lift > min lift

		
		// Filter individual items
		List<PromoItemDTO> indItemFilter = indSummaryItems.stream()
				.filter(p -> p.getPredRev() > actualBlockDetail.getSalePricePredictionMetrics().getPredRev() && p.getPredMar() > 0
						&& p.getPredRev() > minSalesPerBlock && p.getSaleRevLiftPCTAgainstRegRev() > minSalesLiftPCTPerBlock)
				.collect(Collectors.toList());

		// Filter PPG items
		List<PromoItemDTO> ppgItemFilter = ppgSummaryItems.stream()
				.filter(p -> p.isPPGLevelSummary() && p.getPredRev() > actualBlockDetail.getSalePricePredictionMetrics().getPredRev()
						&& p.getPredMar() > 0 && p.getPredRev() > minSalesPerBlock
						&& p.getSaleRevLiftPCTAgainstRegRev() > minSalesLiftPCTPerBlock)
				.collect(Collectors.toList());

		
		filteredList.addAll(indItemFilter);
		filteredList.addAll(ppgItemFilter);
		
		// Sort the list by sales, Lift % in reg price sales, household, margin 
		// and pick the first item
		if(filteredList.size() > 0) {
			finalizedIndOrPPGItem = sortItemBasedOnMetrics(filteredList);
		}

		return finalizedIndOrPPGItem;
	}
	
	private PromoItemDTO getHighestSalesItems(BlockDetail actualBlockDetail, List<PromoItemDTO> indSummaryItems, List<PromoItemDTO> ppgSummaryItems) {
		PromoItemDTO finalizedIndOrPPGItem = null;
		List<PromoItemDTO> indAndPPGItems = new ArrayList<PromoItemDTO>();
		
		indAndPPGItems.addAll(indSummaryItems);
		indAndPPGItems.addAll(ppgSummaryItems.stream().filter(p -> p.isPPGLevelSummary()).collect(Collectors.toList()));

		Comparator<PromoItemDTO> sortByPredRev = (a, b) -> Double.compare(b.getPredRev(), a.getPredRev());
		
		List<PromoItemDTO> sortedItems = indAndPPGItems.stream().sorted(sortByPredRev).collect(Collectors.toList());
		
		finalizedIndOrPPGItem = sortedItems.get(0);
		
		return finalizedIndOrPPGItem;
	}
	
	private PromoItemDTO getHighestSalesItemsWithMinMargin(BlockDetail actualBlockDetail, List<PromoItemDTO> indSummaryItems,
			List<PromoItemDTO> ppgSummaryItems, Double inputMinMarginPCT) {
		PromoItemDTO finalizedIndOrPPGItem = null;
		List<PromoItemDTO> indAndPPGItems = new ArrayList<PromoItemDTO>();

		indAndPPGItems.addAll(indSummaryItems.stream()
				.filter(p -> p.getSaleMarginPCT() > inputMinMarginPCT)
				.collect(Collectors.toList()));

		indAndPPGItems.addAll(
				ppgSummaryItems.stream().filter(p -> p.isPPGLevelSummary() && p.getSaleMarginPCT() > inputMinMarginPCT).collect(Collectors.toList()));

		Comparator<PromoItemDTO> sortByPredRev = (a, b) -> Double.compare(b.getPredRev(), a.getPredRev());

		List<PromoItemDTO> sortedItems = indAndPPGItems.stream().sorted(sortByPredRev).collect(Collectors.toList());

		if (sortedItems.size() > 0) {
			finalizedIndOrPPGItem = sortedItems.get(0);
		}

		return finalizedIndOrPPGItem;
	}
	
	private PromoItemDTO maximizeSalesWithoutReducingMargin(BlockDetail actualBlockDetail, List<PromoItemDTO> indSummaryItems,
			List<PromoItemDTO> ppgSummaryItems) {
		List<PromoItemDTO> filteredList = new ArrayList<PromoItemDTO>();
		PromoItemDTO finalizedIndOrPPGItem = null;
		
		// Sales >= actual block sales, margin >= actual block margin

		
		// Filter individual items
		List<PromoItemDTO> indItemFilter = new ArrayList<PromoItemDTO>();
		
		if (indSummaryItems != null) {
			indItemFilter = indSummaryItems.stream().filter(p -> p.getPredRev() >= actualBlockDetail.getSalePricePredictionMetrics().getPredRev()
					&& p.getPredMar() >= actualBlockDetail.getSalePricePredictionMetrics().getPredMar()).collect(Collectors.toList());
		}

		// Filter PPG items
		List<PromoItemDTO> ppgItemFilter  = new ArrayList<PromoItemDTO>();
		if (ppgSummaryItems != null) {
			ppgItemFilter = ppgSummaryItems.stream()
					.filter(p -> p.isPPGLevelSummary() && p.getPredRev() >= actualBlockDetail.getSalePricePredictionMetrics().getPredRev()
							&& p.getPredMar() >= actualBlockDetail.getSalePricePredictionMetrics().getPredMar())
					.collect(Collectors.toList());
		}

		
		filteredList.addAll(indItemFilter);
		filteredList.addAll(ppgItemFilter);
		
		// Sort the list by sales, Lift % in reg price sales, household, margin 
		// and pick the first item
		if(filteredList.size() > 0) {
			finalizedIndOrPPGItem = sortByRevMarHH(filteredList);
		}

		return finalizedIndOrPPGItem;
	}
	
	
	private PromoItemDTO sortItemBasedOnMetrics(List<PromoItemDTO> filteredIndAndPPGItemSummary) {
		PromoItemDTO finalizedItem = new PromoItemDTO();
		
		//Sort the list by sales, Lift % in reg price sales, household, margin 
		// and pick the first item
		
		Comparator<PromoItemDTO> sortByPredRev = (a, b) -> Double.compare(b.getPredRev(), a.getPredRev());
		
		Comparator<PromoItemDTO> sortByLiftPCT = (a, b) -> Double.compare(b.getSaleRevLiftPCTAgainstRegRev(), a.getSaleRevLiftPCTAgainstRegRev());
		
		Comparator<PromoItemDTO> sortByHHCnt = (a, b) -> Integer.compare(b.getNoOfHHRecommendedTo(), a.getNoOfHHRecommendedTo());
		
		Comparator<PromoItemDTO> sortByPredMar = (a, b) -> Double.compare(b.getPredMar(), a.getPredMar());
		
		List<PromoItemDTO> sortedItems = filteredIndAndPPGItemSummary.stream()
				.sorted(sortByPredRev.thenComparing(sortByLiftPCT).thenComparing(sortByHHCnt).thenComparing(sortByPredMar))
				.collect(Collectors.toList());
		
		finalizedItem = sortedItems.get(0);
		
		return finalizedItem;
	}
	
	private PromoItemDTO sortByRevMarHH(List<PromoItemDTO> filteredIndAndPPGItemSummary) {
		PromoItemDTO finalizedItem = new PromoItemDTO();

		// Sort the list by sales, Lift % in reg price sales, household, margin
		// and pick the first item

		Comparator<PromoItemDTO> sortByPredRev = (a, b) -> Double.compare(b.getPredRev(), a.getPredRev());

		Comparator<PromoItemDTO> sortByPredMar = (a, b) -> Double.compare(b.getPredMar(), a.getPredMar());

		Comparator<PromoItemDTO> sortByHHCnt = (a, b) -> Integer.compare(b.getNoOfHHRecommendedTo(), a.getNoOfHHRecommendedTo());

		List<PromoItemDTO> sortedItems = filteredIndAndPPGItemSummary.stream()
				.sorted(sortByPredRev.thenComparing(sortByPredMar).thenComparing(sortByHHCnt)).collect(Collectors.toList());

		finalizedItem = sortedItems.get(0);

		return finalizedItem;
	}
	
	@SuppressWarnings("unused")
	private List<PromoItemDTO> getMatchingActualBlockItems(List<PromoItemDTO> indItemSummary, List<PromoItemDTO> actualBlockItems) {
		List<PromoItemDTO> filteredList = new ArrayList<PromoItemDTO>();

		for (PromoItemDTO promoItemDTO : indItemSummary) {
			if (promoItemDTO.getPpgGroupId() == 0) {
				for (PromoItemDTO actualAdItem : actualBlockItems) {
					if (promoItemDTO.getProductKey().equals(actualAdItem.getProductKey())) {
						filteredList.add(promoItemDTO);
						break;
					}
				}
			}
		}

		return filteredList;
	}

	@SuppressWarnings("unused")
	private List<PromoItemDTO> getMatchingActualBlockItemsInPPG(List<PromoItemDTO> ppgAndItsItemSummary, List<PromoItemDTO> actualBlockItems) {
		List<PromoItemDTO> filteredList = new ArrayList<PromoItemDTO>();
		List<PromoItemDTO> ppgGroupItems = new ArrayList<PromoItemDTO>();

		for (PromoItemDTO ppgAndItsItem : ppgAndItsItemSummary) {
			if (ppgAndItsItem.isPPGLevelSummary()) {
				ppgGroupItems.add(ppgAndItsItem);
			}
		}

		for (PromoItemDTO ppgGroupItem : ppgGroupItems) {
			List<PromoItemDTO> ppgItems = ppgAndItsItemSummary.stream().filter(p -> p.getPpgGroupId().equals(ppgGroupItem.getPpgGroupId())
					&& !p.isPPGLevelSummary() && p.getPpgPromoCombinationId() == ppgGroupItem.getPpgPromoCombinationId())
					.collect(Collectors.toList());

			// Even if one of the actual ad item present in the PPG
			boolean isPPGAdded = false;
			for (PromoItemDTO ppgItem : ppgItems) {
				for (PromoItemDTO actualAdItem : actualBlockItems) {
					if (ppgItem.getProductKey().equals(actualAdItem.getProductKey())) {
						isPPGAdded = true;
						break;
					}
				}
			}

			if (isPPGAdded) {
				filteredList.add(ppgGroupItem);
			}
		}
		return filteredList;
	}
	
	private HashMap<ProductKey, PromoItemDTO> findActualItemsInOtherBlock(HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary,
			HashMap<PageBlockNoKey, BlockDetail> actualAdBlockDetails, HashMap<Integer, String> curDeptIds, PageBlockNoKey curPageBlockNoKey) {

		HashMap<ProductKey, PromoItemDTO> otherActualItemsOfDep = new HashMap<ProductKey, PromoItemDTO>();

		for (Map.Entry<PageBlockNoKey, BlockDetail> blockDetailsEntry : actualAdBlockDetails.entrySet()) {
			PageBlockNoKey pageBlockNoKey = blockDetailsEntry.getKey();
			
			//ignore current block
			if(!pageBlockNoKey.equals(curPageBlockNoKey)) {
				List<PromoItemDTO> actualBlockItems = actualAdItemsLIGNonLigSummary.get(pageBlockNoKey);
				for (PromoItemDTO promoItemDTO : actualBlockItems) {
					if (curDeptIds.get(promoItemDTO.getDeptId()) != null) {
						otherActualItemsOfDep.put(promoItemDTO.getProductKey(), promoItemDTO);
					}
				}
			}
		}

		return otherActualItemsOfDep;
	}
	
//	private void processBlock(boolean isProcessCloseMatchingItems, HashMap<ProductKey, PromoItemDTO> alreadyPickedItems,
//			Set<Integer> alreadyPickedCategories, HashMap<PageBlockNoKey, List<PromoItemDTO>> finalizedItemMap,
//			HashMap<PageBlockNoKey, BlockDetail> actualAdBlockDetails, HashMap<PageBlockNoKey, List<PromoItemDTO>> actualAdItemsLIGNonLigSummary,
//			List<PromoItemDTO> indItemSummary, List<PromoItemDTO> ppgAndItsItemSummary, double inputMinMarginPCT) {
//		
//		List<PromoItemDTO> indItemSummaryOfDept = new ArrayList<PromoItemDTO>();
//		List<PromoItemDTO> ppgAndItsItemSummaryOfDept = new ArrayList<PromoItemDTO>();
//		
//		for (Map.Entry<PageBlockNoKey, BlockDetail> blockDetailsEntry : actualAdBlockDetails.entrySet()) {
//			BlockDetail actualBlockDetail = blockDetailsEntry.getValue();
//			PageBlockNoKey pageBlockNoKey = blockDetailsEntry.getKey();
//
//			if (!actualBlockDetail.isProcessed()) {
//				List<PromoItemDTO> actualBlockItems = actualAdItemsLIGNonLigSummary.get(pageBlockNoKey);
//
//				// Set<ProductKey> actualBlockItemSet =
//				// actualBlockItems.stream().map(PromoItemDTO::getProductKey).collect(Collectors.toSet());
//
//				// Get items belong to actual ad department
//				indItemSummaryOfDept = getItemsOfDepartment(indItemSummary, actualBlockDetail.getDepartments(), false);
//				ppgAndItsItemSummaryOfDept = getItemsOfDepartment(ppgAndItsItemSummary, actualBlockDetail.getDepartments(), false);
//
//				// logger.debug("ppgAndItsItemSummaryOfDept:" + ppgAndItsItemSummaryOfDept.toString());
//
//				// logger.debug("actualBlockItemSet:" + actualBlockItemSet);
//				// logger.debug("itemsAlreadyPickedForRec:" + actualBlockItemSet);
//
//				// Ignore items already present in actual ad block and already picked in recommendations
//				List<PromoItemDTO> filteredIndItemSummary = filterAlreadyPickedItems(indItemSummaryOfDept, alreadyPickedItems,
//						alreadyPickedCategories);
//				List<PromoItemDTO> filteredPPGAndItsItemSummary = filterAlreadyPickedItemsInPPG(ppgAndItsItemSummaryOfDept, alreadyPickedItems,
//						alreadyPickedCategories);
//
//				// logger.debug("filteredPPGAndItsItemSummary:" + filteredPPGAndItsItemSummary.toString());
//
//				logger.debug("filteredIndItemSummary.size():" + filteredIndItemSummary.size());
//				logger.debug("filteredPPGAndItsItemSummary.size():" + filteredPPGAndItsItemSummary.size());
//
//				// Find the ppg summary again
//				updatePPGLevelSummary(filteredPPGAndItsItemSummary);
//
//				
//				List<PromoItemDTO> matchingFilteredIndItemSummary = null;
//				List<PromoItemDTO> matchingFilteredPPGAndItsItemSummary = null;
//				
//				boolean isFinalizeItem = false;
//				// process close matching items
//				if (isProcessCloseMatchingItems) {
//					matchingFilteredIndItemSummary = getMatchingActualBlockItems(filteredIndItemSummary, actualBlockItems);
//					matchingFilteredPPGAndItsItemSummary = getMatchingActualBlockItemsInPPG(filteredPPGAndItsItemSummary, actualBlockItems);
//
//					logger.debug("matching filteredIndItemSummary.size():" + matchingFilteredIndItemSummary.size());
//					logger.debug("matching filteredPPGAndItsItemSummary.size():" + matchingFilteredPPGAndItsItemSummary.size());
//
//					if (matchingFilteredIndItemSummary.size() > 0 || matchingFilteredPPGAndItsItemSummary.size() > 0) {
//						isFinalizeItem = true;
//						actualBlockDetail.setProcessed(true);
//					}
//
//				} else if (!isProcessCloseMatchingItems) {
//					isFinalizeItem = true;
//					actualBlockDetail.setProcessed(true);
//					matchingFilteredIndItemSummary = filteredIndItemSummary;
//					matchingFilteredPPGAndItsItemSummary = filteredPPGAndItsItemSummary;
//				}
//
//				if (isFinalizeItem) {
//					// logger.debug("filteredPPGAndItsItemSummary:" + filteredPPGAndItsItemSummary.toString());
//
//					// Pick ind / PPG whose sales > actual block sales and margin > 0
//					PromoItemDTO selectedItemOrPPG = applyObjective(AdObjectiveLookup.MAXIMIZE_SALES_WITHOUT_REDUCING_MARGIN.getObjectiveTypeId(),
//							actualBlockDetail, matchingFilteredIndItemSummary, matchingFilteredPPGAndItsItemSummary, inputMinMarginPCT);
//
//					// logger.debug("filteredIndAndPPGItemSummary.size():" + filteredPPGAndItsItemSummary.size());
//
//					List<PromoItemDTO> finalItemList = null;
//					if (selectedItemOrPPG != null) {
//						finalItemList = getIndOrPPGItems(selectedItemOrPPG, filteredPPGAndItsItemSummary);
//					} else {
//						// If there is no recommendations, put same as actuals
//						finalItemList = actualBlockItems;
//					}
//
//					logger.debug("pageBlockNoKey:" + pageBlockNoKey.toString() + ",Final Ad Items:" + finalItemList.toString());
//					finalizedItemMap.put(pageBlockNoKey, finalItemList);
//
//					for (PromoItemDTO promoItemDTO : finalItemList) {
//						if (alreadyPickedItems.get(promoItemDTO.getProductKey()) == null) {
//							alreadyPickedItems.put(promoItemDTO.getProductKey(), promoItemDTO);
//							alreadyPickedCategories.add(promoItemDTO.getCategoryId());
//						}
//					}
//				}
//			}
//		}
//	}
}
