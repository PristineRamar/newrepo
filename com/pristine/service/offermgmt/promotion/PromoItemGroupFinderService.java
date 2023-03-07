package com.pristine.service.offermgmt.promotion;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.promotion.PromoItemGroupFinderDAO;
import com.pristine.dao.offermgmt.promotion.PromotionEngineDAO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.promotion.BlockDetail;
import com.pristine.dto.offermgmt.promotion.BrandDetail;
import com.pristine.dto.offermgmt.promotion.PageBlockNoKey;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.dto.offermgmt.promotion.PromoProductGroup;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.offermgmt.PRCommonUtil;

public class PromoItemGroupFinderService {

	private static Logger logger = Logger.getLogger("PromoItemGroupFinderService");

	public List<PromoItemDTO> getActualAdDetails(Connection conn, int locationLevelId, int locationId, int productLevelId, int productId,
			String weekStartDate, int noOfPreviousWeeks) throws GeneralException {
		List<PromoItemDTO> promoAdDetailsList = new ArrayList<PromoItemDTO>();
		PromotionEngineDAO promoDAO = new PromotionEngineDAO();
		Set<Integer> productIdSet = new HashSet<Integer>();
		productIdSet.add(productId);

		promoAdDetailsList = promoDAO.getAdDetails(conn, locationLevelId, locationId, productLevelId, productIdSet, weekStartDate, noOfPreviousWeeks,
				-1, true);

		return promoAdDetailsList;
	}

	// Group items
	public HashMap<String, HashMap<PageBlockNoKey, List<PromoItemDTO>>> groupByAdWeekPageBlock(List<PromoItemDTO> adDetails) {
		HashMap<String, HashMap<PageBlockNoKey, List<PromoItemDTO>>> groupedByAdWeekPageBlockMap = new HashMap<String, HashMap<PageBlockNoKey, List<PromoItemDTO>>>();

		// Group items by week and page&block within it
		for (PromoItemDTO adDetail : adDetails) {
			HashMap<PageBlockNoKey, List<PromoItemDTO>> pageBlockMap = new HashMap<PageBlockNoKey, List<PromoItemDTO>>();
			List<PromoItemDTO> itemsInPageBlock = new ArrayList<PromoItemDTO>();
			if (adDetail.getAdInfo() != null && adDetail.getSaleInfo() != null) {
				String weekStartDate = adDetail.getAdInfo().getWeeklyAdStartDate();
				if (groupedByAdWeekPageBlockMap.get(weekStartDate) != null) {
					pageBlockMap = groupedByAdWeekPageBlockMap.get(weekStartDate);
				}

				PageBlockNoKey pageBlockNoKey = new PageBlockNoKey(adDetail.getAdInfo().getAdPageNo(), adDetail.getAdInfo().getAdBlockNo());
				if (pageBlockMap.get(pageBlockNoKey) != null) {
					itemsInPageBlock = pageBlockMap.get(pageBlockNoKey);
				}

				itemsInPageBlock.add(adDetail);
				pageBlockMap.put(pageBlockNoKey, itemsInPageBlock);

				groupedByAdWeekPageBlockMap.put(weekStartDate, pageBlockMap);
			}
		}

		return groupedByAdWeekPageBlockMap;
	}

	// Analysis each block
	public List<BlockDetail> analysisBlock(HashMap<String, HashMap<PageBlockNoKey, List<PromoItemDTO>>> itemsGroupedByWeekAndPageBlock,
			HashMap<ProductKey, HashMap<ProductKey, PromoItemDTO>> itemsByCategory) throws GeneralException {
		List<BlockDetail> blockDetails = new ArrayList<BlockDetail>();

		int totalPastBlocks = 0;
		
		
		// Each week
		for (Map.Entry<String, HashMap<PageBlockNoKey, List<PromoItemDTO>>> pageBlockMap : itemsGroupedByWeekAndPageBlock.entrySet()) {
			
			totalPastBlocks = totalPastBlocks + pageBlockMap.getValue().size();
			
			// Each page&block in the week
			for (Map.Entry<PageBlockNoKey, List<PromoItemDTO>> itemsInPageBlock : pageBlockMap.getValue().entrySet()) {
				BlockDetail blockDetail = new BlockDetail();
				List<Double> regUnitPrices = new ArrayList<Double>();
				List<BrandDetail> brandDetails = new ArrayList<BrandDetail>();
				HashMap<Integer, BrandDetail> brands = new HashMap<Integer, BrandDetail>();

				
				// Each item in the block
				for (PromoItemDTO promoItemDTO : itemsInPageBlock.getValue()) {
					ProductKey categorykey = new ProductKey(Constants.CATEGORYLEVELID, promoItemDTO.getCategoryId());
					// Distinct category id's
					if (promoItemDTO.getCategoryId() > 0) {
						blockDetail.getCategoryIds().add(categorykey);
					}
					//blockDetail.setPromoType(promoItemDTO.getSaleInfo().getSalePromoTypeLookup());
					blockDetail.setPageBlockNoKey(itemsInPageBlock.getKey());
					if (promoItemDTO.getSaleInfo().getSalePrice() != null) {
						blockDetail.getSalePrices().add(promoItemDTO.getSaleInfo().getSalePrice());
					}

					// Distinct brand id's
					if (promoItemDTO.getBrandId() > 0) {
						BrandDetail brandDetail = new BrandDetail();
						brandDetail.setBranId(promoItemDTO.getBrandId());
						brandDetail.setItemSize(promoItemDTO.getItemSize());
						brandDetail.setItemUOMID(promoItemDTO.getUomID());
						brandDetails.add(brandDetail);

						blockDetail.getBrandIds().add(promoItemDTO.getBrandId());
					}
					// Reg prices of same item may vary across the location, considering that price
					// may give wrong reg price variation and due to this, other items which are in this range
					// would be picked wrongly. For e.g. item 1 varies from 1.49 to 2.49, then it will pick
					// items ranges between 1.49 to 2.19, which is incorrect
					// So use current reg price
					if (itemsByCategory.get(categorykey) != null) {
						HashMap<ProductKey, PromoItemDTO> tempMap = itemsByCategory.get(categorykey);
						PromoItemDTO tempItemDTO = tempMap.get(promoItemDTO.getProductKey());
						if (tempItemDTO != null) {
							regUnitPrices.add(PRCommonUtil.getUnitPrice(tempItemDTO.getRegPrice(), false));
						}
					}
				}

				// Fill Brand Details
				for (Integer brandId : blockDetail.getBrandIds()) {
					BrandDetail brandDetail = new BrandDetail();
					brandDetail.setBranId(brandId);

					// Set min size
					BrandDetail tempBrandDetail = brandDetails.stream().filter(p -> p.getBrandId() == brandId)
							.min(Comparator.comparing(BrandDetail::getItemSize)).get();
					brandDetail.setItemMinSize(tempBrandDetail.getItemSize());

					// Set max size
					tempBrandDetail = brandDetails.stream().filter(p -> p.getBrandId() == brandId).max(Comparator.comparing(BrandDetail::getItemSize))
							.get();
					brandDetail.setItemMaxSize(tempBrandDetail.getItemSize());

					brands.put(brandDetail.getBrandId(), brandDetail);
				}

				blockDetail.setBrands(brands);

				// Set min and max reg price
				if (regUnitPrices.size() > 0) {
					Collections.sort(regUnitPrices);
					blockDetail.setMinRegPrice(regUnitPrices.get(0));

					Collections.reverse(regUnitPrices);
					blockDetail.setMaxRegPrice(regUnitPrices.get(0));
				}

				blockDetail.setWeeklyAdStartDate(pageBlockMap.getKey());

				blockDetails.add(blockDetail);
			}
		}
		
		logger.info("Total No of past blocks:" + totalPastBlocks);

		return blockDetails;
	}

	// Group block by similar set of categories
	public HashMap<Set<ProductKey>, List<BlockDetail>> groupByCategories(List<BlockDetail> blockDetails) {
		HashMap<Set<ProductKey>, List<BlockDetail>> groupedBySameSetOfCategories = new HashMap<Set<ProductKey>, List<BlockDetail>>();

		// Each block
		for (BlockDetail blockDetail : blockDetails) {
			List<BlockDetail> tempBlockDetail = new ArrayList<BlockDetail>();
			if (groupedBySameSetOfCategories.get(blockDetail.getCategoryIds()) != null) {
				tempBlockDetail = groupedBySameSetOfCategories.get(blockDetail.getCategoryIds());
			}
			tempBlockDetail.add(blockDetail);
			groupedBySameSetOfCategories.put(blockDetail.getCategoryIds(), tempBlockDetail);
		}

		return groupedBySameSetOfCategories;
	}

	// Group block by similar set of multiple brands
	public HashMap<Set<Integer>, List<BlockDetail>> groupByBrands(List<BlockDetail> blockDetails) {
		HashMap<Set<Integer>, List<BlockDetail>> groupedBySameSetOfBrands = new HashMap<Set<Integer>, List<BlockDetail>>();
		for (BlockDetail blockDetail : blockDetails) {
			List<BlockDetail> tempBlockDetail = new ArrayList<BlockDetail>();
			if (groupedBySameSetOfBrands.get(blockDetail.getBrandIds()) != null) {
				tempBlockDetail = groupedBySameSetOfBrands.get(blockDetail.getBrandIds());
			}
			tempBlockDetail.add(blockDetail);
			groupedBySameSetOfBrands.put(blockDetail.getBrandIds(), tempBlockDetail);
		}

		return groupedBySameSetOfBrands;
	}
	
	public List<PromoProductGroup> findGroups(HashMap<Set<ProductKey>, List<BlockDetail>> groupedBySameSetOfCategories,
			HashMap<Set<Integer>, List<BlockDetail>> groupedBySameSetOfBrands,
			HashMap<ProductKey, HashMap<ProductKey, PromoItemDTO>> itemsByCategory) {
		List<PromoProductGroup> productGroups = new ArrayList<PromoProductGroup>();
		
		int blockIgnoredNoRegRange = 0, blockIgnoredSingleBrandCombination = 0, blockIgnoredSingleCategoryCombination = 0;
		int blockIgnoredSingleItem = 0;
		int acceptedBlocks = 0, totalBlocks = 0;
		
		for (Map.Entry<Set<ProductKey>, List<BlockDetail>> blocks : groupedBySameSetOfCategories.entrySet()) {
			
			
			logger.debug("blocks.getKey():" + blocks.getKey() + ",blocks.getValue().size():" + blocks.getValue().size());

			// Each block
			for (BlockDetail blockDetail : blocks.getValue()) {
				totalBlocks = totalBlocks + 1;
				boolean isIgnoreBlock = false;
				
				HashSet<String> adDetails = new HashSet<String>();
				// If there is only one brand and there is price range
				if (blockDetail.getMinRegPrice() != null && blockDetail.getMinRegPrice() > 0 && blockDetail.getMaxRegPrice() != null
						&& blockDetail.getMaxRegPrice() > 0) {

					PromoProductGroup promoProductGroup = new PromoProductGroup();
					HashMap<ProductKey, PromoItemDTO> items = new HashMap<ProductKey, PromoItemDTO>();

					// Get all items matches the category id and brand id and reg unit price is within the range
					for (ProductKey categoryIdKey : blockDetail.getCategoryIds()) {

						// Items with in the grouped category
						if (itemsByCategory.get(categoryIdKey) != null) {

							for (PromoItemDTO itemDTO : itemsByCategory.get(categoryIdKey).values()) {

								// Items within the brand
								if (blockDetail.getBrandIds().contains(itemDTO.getBrandId())) {
									double regUnitPrice = PRCommonUtil.getUnitPrice(itemDTO.getRegPrice(), false);

									BrandDetail brandDetail = blockDetail.getBrands().get(itemDTO.getBrandId());
									double minSizeInBlock = 0, maxSizeInBlock = 0;
									if (brandDetail != null) {
										minSizeInBlock = brandDetail.getItemMinSize();
										maxSizeInBlock = brandDetail.getItemMaxSize();
									}

									// Items within the price range and within the size
									if (regUnitPrice >= blockDetail.getMinRegPrice() && regUnitPrice <= blockDetail.getMaxRegPrice()
											&& minSizeInBlock > 0 && maxSizeInBlock > 0 && itemDTO.getItemSize() >= minSizeInBlock
											&& itemDTO.getItemSize() <= maxSizeInBlock) {
										items.put(itemDTO.getProductKey(), itemDTO);
										// logger.debug("1.itemDTO.getProductKey():" + itemDTO.getProductKey());
									}
								}
							}
						}
					}

					String additionalDetails = blockDetail.getWeeklyAdStartDate() + "_" + blockDetail.getCategoryIds() + "_" + blockDetail.getBrandIds() + "_"
							+ blockDetail.getPageBlockNoKey().getPageNumber() + "-" + blockDetail.getPageBlockNoKey().getBlockNumber() + "_"
							+ blockDetail.getSalePrices().toString();
					
					adDetails.add(additionalDetails);
					
					if(items.size() <= 1) {
						isIgnoreBlock = true;
						
						logger.debug("Block ignored as only one item is present:" + additionalDetails);
						blockIgnoredSingleItem= blockIgnoredSingleItem + 1;
					} else if (groupedBySameSetOfBrands.get(blockDetail.getBrandIds()) != null && groupedBySameSetOfBrands.get(blockDetail.getBrandIds()).size() <=1) {
						isIgnoreBlock = true;
						
						logger.debug("Block ignored as this brand combination occured only once in the past:" + additionalDetails);
						blockIgnoredSingleBrandCombination = blockIgnoredSingleBrandCombination + 1;
					} else if (blocks.getValue().size() <= 1) {
						isIgnoreBlock = true;
						
						logger.debug("Block ignored as this category combination occured only once in the past:" + additionalDetails);
						blockIgnoredSingleCategoryCombination = blockIgnoredSingleCategoryCombination + 1;
					}

					// Create group only if more than time it is grouped in the past
					// && blocks.getValue().size() > 1
					if (!isIgnoreBlock) {
						promoProductGroup.setMinRegUnitPrice(blockDetail.getMinRegPrice());
						promoProductGroup.setMaxRegUnitPrice(blockDetail.getMaxRegPrice());
						promoProductGroup.setSupportedPromoType(blockDetail.getPromoType());
						promoProductGroup.setItems(items);
						promoProductGroup.getAdditionalDetail().setAdDetails(adDetails);
						productGroups.add(promoProductGroup);
						acceptedBlocks = acceptedBlocks + 1;
					}  
					
				} else {
					blockIgnoredNoRegRange = blockIgnoredNoRegRange + 1;
				}
			}

		}
		
		logger.info("Total Blocks:" + totalBlocks + ",No of accepted blocks:" + acceptedBlocks + ",Ignored(No Reg Range):" + blockIgnoredNoRegRange 
				+ ",Ignored(Single Item):" + blockIgnoredSingleItem + ",Ignored(Only one brand combination):" + blockIgnoredSingleBrandCombination
				+ ",Ignored(Only one category combination):" + blockIgnoredSingleCategoryCombination);
		
		return productGroups;
		
	}

	// Find item that can be grouped across the categories
	public List<PromoProductGroup> groupItemAcrossCategories(HashMap<Set<ProductKey>, List<BlockDetail>> groupedBySameSetOfCategories,
			HashMap<ProductKey, HashMap<ProductKey, PromoItemDTO>> itemsByCategory) {
		List<PromoProductGroup> productGroups = new ArrayList<PromoProductGroup>();

		for (Map.Entry<Set<ProductKey>, List<BlockDetail>> blocks : groupedBySameSetOfCategories.entrySet()) {
			HashSet<String> adDetails = new HashSet<String>();

			logger.debug("blocks.getKey:" + blocks.getKey());
			
			// Each block
			for (BlockDetail blockDetail : blocks.getValue()) {
				// If there is only one brand and there is price range
				if (blockDetail.getBrandIds().size() == 1 && blockDetail.getMinRegPrice() != null && blockDetail.getMinRegPrice() > 0
						&& blockDetail.getMaxRegPrice() != null && blockDetail.getMaxRegPrice() > 0) {

					PromoProductGroup promoProductGroup = new PromoProductGroup();
					HashMap<ProductKey, PromoItemDTO> items = new HashMap<ProductKey, PromoItemDTO>();

					// Get all items matches the category id and brand id and reg unit price is within the range
					for (ProductKey categoryIdKey : blockDetail.getCategoryIds()) {

						// Items with in the grouped category
						if (itemsByCategory.get(categoryIdKey) != null) {

							for (PromoItemDTO itemDTO : itemsByCategory.get(categoryIdKey).values()) {

								// Items within the brand
								if (itemDTO.getBrandId() == blockDetail.getBrandIds().stream().findFirst().get()) {
									double regUnitPrice = PRCommonUtil.getUnitPrice(itemDTO.getRegPrice(), false);

									BrandDetail brandDetail = blockDetail.getBrands().get(itemDTO.getBrandId());
									double minSizeInBlock = 0, maxSizeInBlock = 0;
									if (brandDetail != null) {
										minSizeInBlock = brandDetail.getItemMinSize();
										maxSizeInBlock = brandDetail.getItemMaxSize();
									}

									// logger.debug(
									// "1.itemDTO.getProductKey():" + itemDTO.getProductKey().toString() +
									// ",itemDTO.getRegPrice():" + regUnitPrice
									// + ",itemDTO.getItemSize():" + itemDTO.getItemSize() + ",blockDetail:" +
									// blockDetail.toString());

									// Items within the price range and within the size
									if (regUnitPrice >= blockDetail.getMinRegPrice() && regUnitPrice <= blockDetail.getMaxRegPrice()
											&& minSizeInBlock > 0 && maxSizeInBlock > 0 && itemDTO.getItemSize() >= minSizeInBlock
											&& itemDTO.getItemSize() <= maxSizeInBlock) {
										items.put(itemDTO.getProductKey(), itemDTO);
										// logger.debug("1.itemDTO.getProductKey():" + itemDTO.getProductKey());
									}
								}
							}
						}
					}

					adDetails.add(blockDetail.getWeeklyAdStartDate() + "_" + blockDetail.getCategoryIds() + "_" + blockDetail.getPageBlockNoKey().getPageNumber() + "-"
							+ blockDetail.getPageBlockNoKey().getBlockNumber() + "_" + blockDetail.getSalePrices().toString());

					//Create group only if more than time it is grouped in the past
					if (items.size() > 0 && blocks.getValue().size() > 1) {
						promoProductGroup.setMinRegUnitPrice(blockDetail.getMinRegPrice());
						promoProductGroup.setMaxRegUnitPrice(blockDetail.getMaxRegPrice());
						promoProductGroup.setSupportedPromoType(blockDetail.getPromoType());
						promoProductGroup.setItems(items);
						promoProductGroup.getAdditionalDetail().setAdDetails(adDetails);
//						promoProductGroup.getAdditionalDetail().setCategoryGroupedTogetherCnt(blocks.getValue().size());
						productGroups.add(promoProductGroup);
					}
				}
			}
		}

		return productGroups;
	}

	// Find multiple brands that can be grouped together
	public List<PromoProductGroup> groupItemAcrossBrands(HashMap<Set<Integer>, List<BlockDetail>> groupedBySameSetOfBrands,
			HashMap<ProductKey, HashMap<ProductKey, PromoItemDTO>> itemsByCategory) {
		List<PromoProductGroup> productGroups = new ArrayList<PromoProductGroup>();

		for (Map.Entry<Set<Integer>, List<BlockDetail>> blocks : groupedBySameSetOfBrands.entrySet()) {

			HashSet<String> adDetails = new HashSet<String>();

			// Each block
			for (BlockDetail blockDetail : blocks.getValue()) {

				if (blockDetail.getMinRegPrice() != null && blockDetail.getMinRegPrice() > 0 && blockDetail.getMaxRegPrice() != null
						&& blockDetail.getMaxRegPrice() > 0) {

					PromoProductGroup promoProductGroup = new PromoProductGroup();
					HashMap<ProductKey, PromoItemDTO> items = new HashMap<ProductKey, PromoItemDTO>();

					// Get all items matches the category id and brand id and reg unit price is within the range
					for (ProductKey categoryIdKey : blockDetail.getCategoryIds()) {

						// Items with in the grouped category
						if (itemsByCategory.get(categoryIdKey) != null) {

							for (PromoItemDTO itemDTO : itemsByCategory.get(categoryIdKey).values()) {

								// Items within the brand
								if (blockDetail.getBrandIds().contains(itemDTO.getBrandId())) {
									double regUnitPrice = PRCommonUtil.getUnitPrice(itemDTO.getRegPrice(), false);

									BrandDetail brandDetail = blockDetail.getBrands().get(itemDTO.getBrandId());
									double minSizeInBlock = 0, maxSizeInBlock = 0;
									if (brandDetail != null) {
										minSizeInBlock = brandDetail.getItemMinSize();
										maxSizeInBlock = brandDetail.getItemMaxSize();
									}

									// logger.debug(
									// "2.itemDTO.getProductKey():" + itemDTO.getProductKey().toString() +
									// ",itemDTO.getRegPrice():" + regUnitPrice
									// + ",itemDTO.getItemSize():" + itemDTO.getItemSize() + ",blockDetail:" +
									// blockDetail.toString());

									// Items within the price range
									if (regUnitPrice >= blockDetail.getMinRegPrice() && regUnitPrice <= blockDetail.getMaxRegPrice()
											&& minSizeInBlock > 0 && maxSizeInBlock > 0 && itemDTO.getItemSize() >= minSizeInBlock
											&& itemDTO.getItemSize() <= maxSizeInBlock) {
										items.put(itemDTO.getProductKey(), itemDTO);
										// logger.debug("2.itemDTO.getProductKey():" + itemDTO.getProductKey());
									}
								}
							}
						}

					}

					adDetails.add(blockDetail.getWeeklyAdStartDate() + "_" + blockDetail.getCategoryIds() + "_" + blockDetail.getPageBlockNoKey().getPageNumber() + "-"
							+ blockDetail.getPageBlockNoKey().getBlockNumber() + "_" + blockDetail.getSalePrices().toString());

					//Create group only if more than time it is grouped in the past
					
					if (items.size() > 0 && blocks.getValue().size() > 1) {
						promoProductGroup.setMinRegUnitPrice(blockDetail.getMinRegPrice());
						promoProductGroup.setMaxRegUnitPrice(blockDetail.getMaxRegPrice());
						promoProductGroup.setSupportedPromoType(blockDetail.getPromoType());
						promoProductGroup.setItems(items);
						promoProductGroup.getAdditionalDetail().setAdDetails(adDetails);
//						promoProductGroup.getAdditionalDetail().setBlockGroupedTogetherCnt(blocks.getValue().size());
						productGroups.add(promoProductGroup);
					}
				}
			}
		}

		return productGroups;
	}

	// Roll up to LIG and non-lig
	public List<PromoProductGroup> rollupToLigAndNonLig(List<PromoProductGroup> promoProductGroups) {
		List<PromoProductGroup> outputProductGroups = new ArrayList<PromoProductGroup>();

		for (PromoProductGroup promoProductGroup : promoProductGroups) {
			PromoProductGroup tempPromoProductGroup = new PromoProductGroup();
			copyPromoProductGroup(promoProductGroup, tempPromoProductGroup);
			HashMap<ProductKey, PromoItemDTO> ligAndNonLigItems = new HashMap<ProductKey, PromoItemDTO>();

			for (Map.Entry<ProductKey, PromoItemDTO> item : promoProductGroup.getItems().entrySet()) {
				if (item.getValue().getRetLirId() > 0) {
					// Currently one of the lig member is picked, as only item code is needed for now
					ProductKey ligKey = new ProductKey(Constants.PRODUCT_LEVEL_ID_LIG, item.getValue().getRetLirId());
					if (!ligAndNonLigItems.containsKey(ligKey)) {
						ligAndNonLigItems.put(ligKey, item.getValue());
					}
				} else {
					ligAndNonLigItems.put(item.getKey(), item.getValue());
				}
			}
			// Consider group only if it has more than 1 item
			if (ligAndNonLigItems.size() > 1) {
				tempPromoProductGroup.setItems(ligAndNonLigItems);
				outputProductGroups.add(tempPromoProductGroup);
			}
		}

		return outputProductGroups;
	}

	public void copyPromoProductGroup(PromoProductGroup srcPromoProductGroup, PromoProductGroup destPromoProductGroup) {
		destPromoProductGroup.setGroupId(srcPromoProductGroup.getGroupId());
		destPromoProductGroup.setMinRegUnitPrice(srcPromoProductGroup.getMinRegUnitPrice());
		destPromoProductGroup.setMaxRegUnitPrice(srcPromoProductGroup.getMaxRegUnitPrice());
		destPromoProductGroup.setSupportedPromoType(srcPromoProductGroup.getSupportedPromoType());
		destPromoProductGroup.setAdditionalDetail(srcPromoProductGroup.getAdditionalDetail());
	}

	public HashMap<ProductKey, PRItemDTO> convertAuthorizedItemsToMap(List<PRItemDTO> authorizedItems) {
		HashMap<ProductKey, PRItemDTO> authorizedItemMap = new HashMap<ProductKey, PRItemDTO>();

		for (PRItemDTO itemDTO : authorizedItems) {
			ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, itemDTO.getItemCode());
			if (authorizedItemMap.get(productKey) == null) {
				authorizedItemMap.put(new ProductKey(Constants.ITEMLEVELID, itemDTO.getItemCode()), itemDTO);
			}
		}

		return authorizedItemMap;
	}

	// group item by category
	public HashMap<ProductKey, HashMap<ProductKey, PromoItemDTO>> groupItemsByCategory(HashMap<ProductKey, PRItemDTO> authorizedItemMap) {
		HashMap<ProductKey, HashMap<ProductKey, PromoItemDTO>> itemsByCategory = new HashMap<ProductKey, HashMap<ProductKey, PromoItemDTO>>();

		for (Map.Entry<ProductKey, PRItemDTO> item : authorizedItemMap.entrySet()) {
			ProductKey categoryKey = new ProductKey(Constants.CATEGORYLEVELID, item.getValue().getCategoryProductId());
			HashMap<ProductKey, PromoItemDTO> itemCategoryMap = new HashMap<ProductKey, PromoItemDTO>();
			PromoItemDTO promoItemDTO = new PromoItemDTO();

			if (itemsByCategory.get(categoryKey) != null) {
				itemCategoryMap = itemsByCategory.get(categoryKey);
			}

			// copy PRItemDTO to PromoItemDTO
			copyPRItemDTOToPromoItemDTO(item.getValue(), promoItemDTO);
			itemCategoryMap.put(promoItemDTO.getProductKey(), promoItemDTO);

			itemsByCategory.put(categoryKey, itemCategoryMap);
		}

		return itemsByCategory;
	}

	private void copyPRItemDTOToPromoItemDTO(PRItemDTO itemDTO, PromoItemDTO promoItemDTO) {
		promoItemDTO.setCategoryId(itemDTO.getCategoryProductId());
		promoItemDTO.setBrandId(itemDTO.getBrandId());
		promoItemDTO.setRegPrice(PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice()));
		promoItemDTO.setProductKey(new ProductKey(Constants.ITEMLEVELID, itemDTO.getItemCode()));
		promoItemDTO.setRetLirId(itemDTO.getRetLirId());
		promoItemDTO.setItemSize(itemDTO.getItemSize());
		promoItemDTO.setUomID((itemDTO.getUOMId().equals("") || itemDTO.getUOMId().isEmpty()) ? 0 : Integer.valueOf(itemDTO.getUOMId().trim()));
	}

	// public List<PromoProductGroup> removeDuplicateGroups(List<PromoProductGroup> promoProductGroups) {
	// List<PromoProductGroup> outputProductGroups = new ArrayList<PromoProductGroup>();
	//
	// // Find hashcode of each product group
	// for (PromoProductGroup promoProductGroup : promoProductGroups) {
	// boolean isDistinctGroup = true;
	// for(PromoProductGroup distinctPromoProductGroup : outputProductGroups){
	// if(promoProductGroup.getItems().keySet().size() == distinctPromoProductGroup.getItems().keySet().size()){
	// if(new HashSet<ProductKey>(promoProductGroup.getItems().keySet())
	// .equals(new HashSet<ProductKey>(distinctPromoProductGroup.getItems().keySet()))){
	// isDistinctGroup = false;
	// }
	// }
	// }
	//
	// if(isDistinctGroup){
	// outputProductGroups.add(promoProductGroup);
	// }
	// }
	//
	// return outputProductGroups;
	// }

	public List<PromoProductGroup> removeDuplicateGroups(List<PromoProductGroup> promoProductGroups) {
		List<PromoProductGroup> outputProductGroups = new ArrayList<PromoProductGroup>();
		HashMap<Integer, PromoProductGroup> distinctProductGroups = new HashMap<Integer, PromoProductGroup>();

		logger.info("Total product groups:" + promoProductGroups.size());
		// Find hashcode of each product group
		for (PromoProductGroup promoProductGroup : promoProductGroups) {
			
			//if the combination is already present, update occurrence and log alone
			if (distinctProductGroups.get(promoProductGroup.hashCode()) != null) {
				PromoProductGroup similiarExistingGroup = distinctProductGroups.get(promoProductGroup.hashCode());
				similiarExistingGroup.getAdditionalDetail().setNoOfOccurance(similiarExistingGroup.getAdditionalDetail().getNoOfOccurance() + 1);
				similiarExistingGroup.getAdditionalDetail().getAdDetails().addAll(promoProductGroup.getAdditionalDetail().getAdDetails());
			} else {
				promoProductGroup.getAdditionalDetail().setNoOfOccurance(1);
				distinctProductGroups.put(promoProductGroup.hashCode(), promoProductGroup);
			}
			 
		}
		
		logger.info("Total product groups after duplicates removed:" + distinctProductGroups.size());
		
		// Combination with more than once 
		for (Map.Entry<Integer, PromoProductGroup> distinctGroups : distinctProductGroups.entrySet()) {
			if(distinctGroups.getValue().getAdditionalDetail().getNoOfOccurance() > 1) {
				outputProductGroups.add(distinctGroups.getValue());
			}
		}

		

		logger.info("Total product groups after single combinations removed:" + outputProductGroups.size());
		
		return outputProductGroups;
	}

	// The method is required to get a lead item whose sale and promotion type can be applied to all members of PPG
	// The lead item will be the item with highest total movement in last one year in a PPG group
	public List<PromoProductGroup> getLeadItemForGroup(List<PromoProductGroup> distinctGroups, HashMap<ProductKey, Long> lastXWeeksTotalMov,
			List<PRItemDTO> authorizedItems) {

		List<PromoProductGroup> distinctGroupsWithLeadItems = new ArrayList<PromoProductGroup>();

		// loop through the distinct PPG groups identified
		for (PromoProductGroup promoProductGroup : distinctGroups) {

			ProductKey leadItem = null;
			long maxMovement = (long) -1; // default total movement value -1
			HashMap<ProductKey, Long> totalMovementOfPPGGroup = new HashMap<ProductKey, Long>();

			// loop through each item in a PPG group
			for (Map.Entry<ProductKey, PromoItemDTO> entry : promoProductGroup.getItems().entrySet()) {
				ProductKey itemKey = new ProductKey(entry.getKey().getProductLevelId(), entry.getKey().getProductId());
				if (leadItem == null) {
					// Initialize the lead item with the first item of the PPG group
					leadItem = new ProductKey(itemKey.getProductLevelId(), itemKey.getProductId());
				}

				// Getting the total movement of NON Lig items
				if (itemKey.getProductLevelId() == Constants.ITEMLEVELID) {
					long itemMovement = (long) 0;
					if (lastXWeeksTotalMov.get(itemKey) != null) {
						itemMovement = lastXWeeksTotalMov.get(itemKey);

						// Updating the lead item if the total movement is this item is greater
						if (maxMovement < itemMovement) {
							maxMovement = itemMovement;
							leadItem = new ProductKey(itemKey.getProductLevelId(), itemKey.getProductId());
						}
					}
					totalMovementOfPPGGroup.put(itemKey, itemMovement);

					// Getting the total movement of LIG items
					// by summing up the total movement of LIG members
				} else if (itemKey.getProductLevelId() == Constants.PRODUCT_LEVEL_ID_LIG) {
					long tempMovement = (long) 0;

					for (PRItemDTO item : authorizedItems) {
						if (item.getRetLirId() > 0 && item.getRetLirId() == entry.getValue().getRetLirId()) {

							ProductKey itemKeyTemp = new ProductKey(Constants.ITEMLEVELID, item.getItemCode());
							if (lastXWeeksTotalMov.get(itemKeyTemp) != null) {
								tempMovement = tempMovement + lastXWeeksTotalMov.get(itemKeyTemp);
							}
						}
					}

					totalMovementOfPPGGroup.put(itemKey, tempMovement);

					// Updating the lead item if the total movement is this item is greater
					if (maxMovement < tempMovement) {
						maxMovement = tempMovement;
						leadItem = new ProductKey(itemKey.getProductLevelId(), itemKey.getProductId());
					}
				}
			}

			promoProductGroup.setLeadItem(leadItem);
			promoProductGroup.setLastXWeeksTotalMovement(totalMovementOfPPGGroup);

			// Marking the respective Promo Item as lead item in PPG group
			for (Map.Entry<ProductKey, PromoItemDTO> entry : promoProductGroup.getItems().entrySet()) {
				if (entry.getKey().equals(leadItem)) {
					entry.getValue().setPPGLeadItem(true);
				} else {
					entry.getValue().setPPGLeadItem(false);
				}

			}

			distinctGroupsWithLeadItems.add(promoProductGroup);
		}
		return distinctGroupsWithLeadItems;
	}

	// To get the last 52 weeks (1 year) of total movement for all authorized items of the store list
	public HashMap<ProductKey, Long> getLastXWeeksMovForStoreList(Connection conn, int locationLevelId, int locationId,
			String weekStartDateForAnalysis, int noOfWeeksItemMovementHistory, List<PRItemDTO> authorizedItems) throws GeneralException {

		PromoItemGroupFinderDAO promoItemGroupFinderDAO = new PromoItemGroupFinderDAO();
		HashMap<ProductKey, Long> lastXWeeksTotalMov = new HashMap<ProductKey, Long>();
		HashMap<ProductKey, Long> queryOutput = new HashMap<ProductKey, Long>();

		logger.debug("getLastXWeeksMovForStoreList() is started...");

		Set<Integer> itemCodeList = new HashSet<Integer>();
		// int limitcount = 0;
		for (PRItemDTO item : authorizedItems) {
			itemCodeList.add(item.getItemCode());
			// limitcount++;
			if (itemCodeList.size() > 0 && (itemCodeList.size() % Constants.LIMIT_COUNT == 0)) {
				queryOutput = promoItemGroupFinderDAO.getLastXWeeksMovForStoreList(conn, locationLevelId, locationId, weekStartDateForAnalysis,
						noOfWeeksItemMovementHistory, itemCodeList);
				if (queryOutput != null && queryOutput.size() > 0) {
					lastXWeeksTotalMov.putAll(queryOutput);
				}
				itemCodeList.clear();
			}
		}
		if (itemCodeList.size() > 0) {
			queryOutput = promoItemGroupFinderDAO.getLastXWeeksMovForStoreList(conn, locationLevelId, locationId, weekStartDateForAnalysis,
					noOfWeeksItemMovementHistory, itemCodeList);
			if (queryOutput != null && queryOutput.size() > 0) {
				lastXWeeksTotalMov.putAll(queryOutput);
			}
			itemCodeList.clear();
		}

		logger.debug("getLastXWeeksMovForStoreList() is completed...");

		return lastXWeeksTotalMov;
	}

	// Function to remove the duplicate(exact match) and subset PPG groups with same lead item
	public List<PromoProductGroup> filterMatchingGroups(List<PromoProductGroup> distinctGroupsWithLeadItems) {

		// int percentageMatch = 90;
		int key = 1; // Key for hasmap of unique PPG Groups

		HashMap<Integer, PromoProductGroup> uniqueFinalGroups = new HashMap<Integer, PromoProductGroup>();

		// Looping the input PPG groups list
		for (PromoProductGroup group : distinctGroupsWithLeadItems) {
			boolean addGroup = true;
			int assignKey = key;

			// Looping through the output unique PPG group map to validate and remove
			// if the output contains any duplicate or subset of PPG group
			for (Map.Entry<Integer, PromoProductGroup> filteredGroup : uniqueFinalGroups.entrySet()) {
				if (group.getLeadItem().equals(filteredGroup.getValue().getLeadItem())) {

					int countGroupKeys = group.getItems().keySet().size();
					int countFilteredGroupKeys = filteredGroup.getValue().getItems().keySet().size();
					int countMatchingKeys = 0;

					for (ProductKey groupKey : group.getItems().keySet()) {
						for (ProductKey filteredGroupKey : filteredGroup.getValue().getItems().keySet()) {

							// Checking the matching items between two PPG groups
							if (groupKey.equals(filteredGroupKey)) {
								countMatchingKeys++;
							}
						}
					}

					// If matching items count equals to either of the PPG group size then one of them is a subset
					if (countMatchingKeys == countGroupKeys || countMatchingKeys == countFilteredGroupKeys) {
						// Filtered Group is a subset of group
						// Remove filtered group and add larger group with same lead item
						if (countGroupKeys > countFilteredGroupKeys) {
							assignKey = filteredGroup.getKey();
						} else {
							addGroup = false;
						}
					}
				}
			}

			if (addGroup) { // Adding the PPG group if it is unique and not a subset of existing group
				key++;
				uniqueFinalGroups.put(assignKey, group);
			}
		}

		return new ArrayList<PromoProductGroup>(uniqueFinalGroups.values());
	}

}
