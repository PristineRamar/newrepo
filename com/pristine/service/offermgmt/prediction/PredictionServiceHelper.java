package com.pristine.service.offermgmt.prediction;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PredictionServiceHelper {
	private static Logger logger = Logger.getLogger("PredictionServiceHelper");

	// Find price points which is not already predicted in PredictionEngineInput
	// Update price points which is not already predicted
	public PredictionEngineInput findItemsToBePredicted(int productLevelId, int productId,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetails, List<PredictionInputDTO> predictionInputDTOs, boolean isLIGModel) throws Exception {
		//logger.debug("Inside findItemsToBePredicted()");
		// PredictionInputDTO itemsToBePredicted = new PredictionInputDTO();
		PredictionEngineInput predictionEngineInput = new PredictionEngineInput();
		List<PredictionEngineItem> itemToBePredicted = new ArrayList<PredictionEngineItem>();
		List<PredictionEngineItem> alreadyCachedItems = new ArrayList<PredictionEngineItem>();
		PredictionDetailKey predictionDetailKey;
		try {
			// fillPredictionEngineInput(predictionEngineInput, predictionInputDTO);
			predictionEngineInput.setProductLevelId(productLevelId);
			predictionEngineInput.setProductId(productId);
			// If more than one location presents then set parallel to true
			if (predictionInputDTOs.size() > 1) {
				predictionEngineInput.setIsParallel(String.valueOf(Constants.YES));
			}

			for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
				// Check if items are present
				if (predictionInputDTO.predictionItems != null) {
					// Check if item is already predicted, if not then keep it in PredictionEngineItem
					for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
						if (predictionItemDTO.pricePoints != null) {
							for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
								predictionDetailKey = new PredictionDetailKey(predictionInputDTO.locationLevelId, predictionInputDTO.locationId,
										predictionItemDTO.itemCodeOrLirId, pricePointDTO.getRegQuantity(), pricePointDTO.getFormattedRegPrice(),
										pricePointDTO.getSaleQuantity(), pricePointDTO.getFormattedSalePrice(), pricePointDTO.getAdPageNo(),
										pricePointDTO.getAdBlockNo(), pricePointDTO.getPromoTypeId(), pricePointDTO.getDisplayTypeId());

								// If already predicted, update flag, movement,explain
								if (predictionDetails.get(predictionDetailKey) != null) {
									pricePointDTO.setIsAlreadyPredicted(true);
									pricePointDTO
											.setPredictedMovement(Double.valueOf(predictionDetails.get(predictionDetailKey).getPredictedMovement()));
									pricePointDTO.setExplainPrediction(predictionDetails.get(predictionDetailKey).getExplainPrediction());
									pricePointDTO.setPredictionStatus(
											PredictionStatus.get(predictionDetails.get(predictionDetailKey).getPredictionStatus()));
									pricePointDTO.setQuestionablePrediction(predictionDetails.get(predictionDetailKey).getQuestionablePrediction());
								} else {
									pricePointDTO.setIsAlreadyPredicted(false);
								}
								
								PredictionEngineItem predictionEngineItem = new PredictionEngineItem(predictionInputDTO.locationLevelId, predictionInputDTO.locationId,
										predictionInputDTO.startWeekDate, PRConstants.PREDICTION_DEAL_TYPE, predictionItemDTO.itemCodeOrLirId,
									    predictionItemDTO.upc, pricePointDTO.getFormattedRegPrice(),
										pricePointDTO.getRegQuantity(),
										(predictionInputDTO.useSubstFlag.equals(String.valueOf(Constants.YES)) ? Constants.YES : Constants.NO),
										predictionItemDTO.mainOrImpactFlag, predictionItemDTO.usePrediction, predictionItemDTO.isAvgMovAlreadySet,
										predictionInputDTO.startPeriodDate, pricePointDTO.getSaleQuantity(),
										pricePointDTO.getFormattedSalePrice(), pricePointDTO.getAdPageNo(), pricePointDTO.getAdBlockNo(),
										pricePointDTO.getPromoTypeId(), pricePointDTO.getDisplayTypeId(), predictionItemDTO.subsScenarioId,
										predictionItemDTO.predictedMovement, predictionItemDTO.predictionStatus,
										predictionItemDTO.getRetLirId());
								
								if(pricePointDTO.getIsAlreadyPredicted()) {
									alreadyCachedItems.add(predictionEngineItem);
								} else {
									itemToBePredicted.add(predictionEngineItem);
								}
							}
						}
					}
				}
			}

			if(isLIGModel) {
				// NU::2nd Mar 2018, to pass all LIG members to prediction as it was using LIG model
				// If only few price points are cached, make it as non-cached, so it is called again with all price points
				addNonCachedPricePoints(predictionInputDTOs, alreadyCachedItems, itemToBePredicted);
			}

			int noOfItemsInCache = (int) alreadyCachedItems.stream().mapToInt(PredictionEngineItem::getItemCode).distinct().count();
			int noOfItemsToBePredicted = (int) itemToBePredicted.stream().mapToInt(PredictionEngineItem::getItemCode).distinct().count();
			
			if(noOfItemsInCache > 0) {
				logger.info("findItemsToBePredicted() - Cache used for items: " + noOfItemsInCache + ", Price Points: "
						+ alreadyCachedItems.size());	
			}

			logger.info("findItemsToBePredicted() - Prediction to be used for items: " + noOfItemsToBePredicted
					+ ", Price Points: " + itemToBePredicted.size());
			
			predictionEngineInput.setPredictionEngineItems(itemToBePredicted);
		} catch (Exception e) {
			logger.error("Error in findItemsToBePredicted() -- " + e.toString(), e);
			throw new Exception();
		}
		return predictionEngineInput;
	}
	
	private void addNonCachedPricePoints(List<PredictionInputDTO> predictionInputDTOs, List<PredictionEngineItem> alreadyCachedItems,
			List<PredictionEngineItem> itemToBePredicted) {

		// Loop the actual input
		for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
			int locationLevelId = predictionInputDTO.locationLevelId;
			int locationId = predictionInputDTO.locationId;

			if (predictionInputDTO.predictionItems != null) {
				// Get distinct LIG's
				Set<Integer> distinctLIGs = predictionInputDTO.predictionItems.stream().filter(x -> x.getRetLirId() > 0)
						.map(PredictionItemDTO::getRetLirId).collect(Collectors.toSet());

				// Loop each LIG
				for (Integer retLirId : distinctLIGs) {
					HashSet<PredictionDetailKey> uniquePricePoints = new HashSet<>();

					List<PredictionItemDTO> ligMembers = predictionInputDTO.predictionItems.stream().filter(x -> x.getRetLirId() == retLirId)
							.collect(Collectors.toList());

					// Get unique price points
					for (PredictionItemDTO predictionItemDTO : ligMembers) {
						if (predictionItemDTO.pricePoints != null) {
							for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
								uniquePricePoints
										.add(convertPricePointDTO(locationLevelId, locationId, 0, pricePointDTO));
							}
						}
					}

					// loop each unique price points
					for (PredictionDetailKey uniquePricePoint : uniquePricePoints) {
						boolean isPricePointNotCachedInOneOfTheMember = false;
						// Loop each members
						for (PredictionItemDTO predictionItemDTO : ligMembers) {
							for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
								PredictionDetailKey pdk = convertPricePointDTO(locationLevelId, locationId, 0, pricePointDTO);

								if (pdk.equals(uniquePricePoint) & !pricePointDTO.getIsAlreadyPredicted()) {
									isPricePointNotCachedInOneOfTheMember = true;
								}
							}
							if (isPricePointNotCachedInOneOfTheMember) {
								break;
							}
						}

						// If a price point is non-cached for few of the members
						if (isPricePointNotCachedInOneOfTheMember) {
							// go through each member
							for (PredictionItemDTO predictionItemDTO : ligMembers) {
								List<PredictionEngineItem> tempCachedItems = alreadyCachedItems.stream()
										.filter(x -> x.getItemCode() == predictionItemDTO.getItemCodeOrLirId()).collect(Collectors.toList());

								// reset the flag
								for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
									PredictionDetailKey pdk = convertPricePointDTO(locationLevelId, locationId, 0, pricePointDTO);
									if (pdk.equals(uniquePricePoint)) {
										pricePointDTO.setIsAlreadyPredicted(false);
									}
								}

								for (PredictionEngineItem cachedItem : tempCachedItems) {
									PredictionDetailKey pdk = convertPredictionEngineItem(locationLevelId, locationId, 0, cachedItem);
									if (pdk.equals(uniquePricePoint)) {
										itemToBePredicted.add(cachedItem);
									}
								}
							}
						}
					}
				}
			}
		}
	}

	public void addMissingLigMembers(Connection conn, List<PredictionInputDTO> predictionInputDTOs, ItemService itemService,
			boolean isPassMissingLigMembers, HashMap<Integer, List<PRItemDTO>> ligMap) throws GeneralException, OfferManagementException {
		// Update for all locations, it may cause issue when multiple location are passed
		// but as of now, now where multiple locations are called

		if (isPassMissingLigMembers) {
			// Filter LIG's which are part of input

			for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
				if (predictionInputDTO.predictionItems != null) {
					int locationLevelId = predictionInputDTO.locationLevelId;
					int locationId = predictionInputDTO.locationId;
					int productLevelId = predictionInputDTO.productLevelId;
					int productId = predictionInputDTO.productId;

					// Get LIG and all its authorized items
					if (ligMap == null && productLevelId > 0 && productId > 0 && locationLevelId > 0 && locationId > 0) {
						//logger.debug("Getting Lig authorized items is started...");

						List<Integer> priceZoneStores = itemService.getPriceZoneStores(conn, productLevelId, productId, locationLevelId, locationId);

						List<PRItemDTO> authorizedItems = itemService.getAuthorizedItemsOfZoneAndStore(conn, productLevelId, productId,
								locationLevelId, locationId, priceZoneStores);

						ligMap = (HashMap<Integer, List<PRItemDTO>>) authorizedItems.stream().filter(x -> x.getRetLirId() > 0)
								.collect(Collectors.groupingBy(PRItemDTO::getRetLirId));

						//logger.debug("ligMap.size()-" + ligMap.size() + ",Getting Lig authorized items is completed...");
					}
					
					// Update ret lir id of the items
					
					for (Map.Entry<Integer, List<PRItemDTO>> lig : ligMap.entrySet()) {
						for (PRItemDTO authorizedLigMember : lig.getValue()) {
							List<PredictionItemDTO> matchingItem = predictionInputDTO.predictionItems.stream()
									.filter(x -> x.getItemCodeOrLirId() == authorizedLigMember.getItemCode()).collect(Collectors.toList());

							for (PredictionItemDTO predictionItemDTO : matchingItem) {
								predictionItemDTO.setRetLirId(lig.getKey());
							}
						}
					}

					// Distinct LIG in the input
					Set<Integer> distinctLIGs = predictionInputDTO.predictionItems.stream().filter(x -> x.getRetLirId() > 0)
							.map(PredictionItemDTO::getRetLirId).collect(Collectors.toSet());

					// Loop each LIG
					for (Map.Entry<Integer, List<PRItemDTO>> lig : ligMap.entrySet()) {

						int retLirId = lig.getKey();

						if (distinctLIGs.contains(retLirId)) {

							HashSet<PredictionDetailKey> uniquePricePoints = new HashSet<>();

							// Get all LIG members in the input
							List<PredictionItemDTO> ligMembers = predictionInputDTO.predictionItems.stream().filter(x -> x.getRetLirId() == retLirId)
									.collect(Collectors.toList());

							// Get all distinct price points of all its members
							for (PredictionItemDTO predictionItemDTO : ligMembers) {
								for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
									uniquePricePoints.add(convertPricePointDTO(locationLevelId, locationId, predictionItemDTO, pricePointDTO));
								}
							}

							// Loop each authorized lig members
							for (PRItemDTO authorizedLigMember : lig.getValue()) {
								Optional<PredictionItemDTO> matchingItem = ligMembers.stream()
										.filter(x -> x.getItemCodeOrLirId() == authorizedLigMember.getItemCode()).findFirst();

								if (matchingItem.isPresent()) {

									// If item present, then add any missing price points
									for (PricePointDTO pricePointDTO : matchingItem.get().pricePoints) {
										PredictionDetailKey tempKey = convertPricePointDTO(locationLevelId, locationId, matchingItem.get(),
												pricePointDTO);
										if (!uniquePricePoints.contains(tempKey)) {
											PricePointDTO tempPricePointDTO = convertPredictionDetailKey(tempKey);
											tempPricePointDTO.setInputPricePoint(false);
											matchingItem.get().pricePoints.add(tempPricePointDTO);
											/*
											 * logger.debug("Missing price point: " + tempPricePointDTO.toString() +
											 * " added for Lig member :" + matchingItem.get().itemCodeOrLirId +
											 * " added for LIG:" + retLirId);
											 */
										}
									}

									matchingItem.get().setRetLirId(retLirId);

								} else {
									// if item not present the add the item with all the price points
									PredictionItemDTO predictionItemDTO = new PredictionItemDTO();

									predictionItemDTO.itemCodeOrLirId = authorizedLigMember.getItemCode();
									predictionItemDTO.upc = authorizedLigMember.getUpc();
									predictionItemDTO.lirInd = false;
									predictionItemDTO.setRetLirId(retLirId);
									predictionItemDTO.setInputItem(false);

									for (PredictionDetailKey predictionDetailKey : uniquePricePoints) {
										predictionItemDTO.pricePoints = new ArrayList<>();
										predictionItemDTO.pricePoints.add(convertPredictionDetailKey(predictionDetailKey));
									}
									//logger.debug("Missing Lig member :" + predictionItemDTO.itemCodeOrLirId + " added for LIG:" + retLirId);
									predictionInputDTO.predictionItems.add(predictionItemDTO);
								}
							}
						}
					}
				}
			}
		}
	}
	
	public HashMap<Integer, PredictionEngineInput> breakItemsToBatches(int predictionEngineBatchCount, PredictionEngineInput predictionEngineInput) {
		
		HashMap<Integer, PredictionEngineInput> itemInBatches = new HashMap<Integer, PredictionEngineInput>();
		PredictionEngineInput finalPredictionEngineInput = new PredictionEngineInput();
		
		copyPredictionEngineInput(predictionEngineInput, finalPredictionEngineInput);
		finalPredictionEngineInput.predictionEngineItems = new ArrayList<PredictionEngineItem>();
		
		// Find distinct non-lig and LIG
		HashSet<ItemKey> distinctNonLIG = new HashSet<ItemKey>();
		HashSet<ItemKey> distinctLIG = new HashSet<ItemKey>();
		for (PredictionEngineItem predictionEngineItem : predictionEngineInput.predictionEngineItems) {
//			logger.debug("predictionEngineItem.usePrediction:" + predictionEngineItem.usePrediction);
			ItemKey itemKey = null;
			if (predictionEngineItem.usePrediction) {
				if (predictionEngineItem.retLirId > 0) {
					itemKey = PRCommonUtil.getItemKey(predictionEngineItem.retLirId, true);
					distinctLIG.add(itemKey);
				} else {
					itemKey = PRCommonUtil.getItemKey(predictionEngineItem.itemCode, false);
					distinctNonLIG.add(itemKey);
				}
			}
		}

		//logger.info("breakItemsToBatches() - Total Distinct Items to be predicted: " + distinctNonLIG.size() + "," + distinctLIG.size());

		int itemCount = 0, batchCount = 1;
		// Add all non lig's
		for (ItemKey itemKey : distinctNonLIG) {
			if(itemCount <= predictionEngineBatchCount) {
				for (PredictionEngineItem pei : predictionEngineInput.predictionEngineItems) {
					if (itemKey.getItemCodeOrRetLirId() == pei.itemCode) {
						finalPredictionEngineInput.predictionEngineItems.add(pei);
					}
				}
			} else {
				itemInBatches.put(batchCount, finalPredictionEngineInput);
				
				Set<Integer> nonLigItems = finalPredictionEngineInput.predictionEngineItems.stream().map(PredictionEngineItem::getItemCode)
						.collect(Collectors.toSet());
				
//				logger.debug("Batch " + batchCount + " has total items:" + nonLigItems.size() + "Items:" + nonLigItems);
				
				finalPredictionEngineInput = new PredictionEngineInput();
				copyPredictionEngineInput(predictionEngineInput, finalPredictionEngineInput);
				finalPredictionEngineInput.predictionEngineItems = new ArrayList<PredictionEngineItem>();
				
				for (PredictionEngineItem pei : predictionEngineInput.predictionEngineItems) {
					if (itemKey.getItemCodeOrRetLirId() == pei.itemCode) {
						finalPredictionEngineInput.predictionEngineItems.add(pei);
					}
				}
				
				itemCount = 1;
				batchCount++;
			}
			itemCount++;
		}
		
		// Add all lig's
		for (ItemKey itemKey : distinctLIG) {
			HashSet<Integer> tempItemCodeSet = new HashSet<Integer>();
			tempItemCodeSet.addAll(predictionEngineInput.predictionEngineItems.stream().filter(x -> x.retLirId == itemKey.getItemCodeOrRetLirId())
					.map(PredictionEngineItem::getItemCode).collect(Collectors.toSet()));
			
			if(itemCount + tempItemCodeSet.size()  <= predictionEngineBatchCount) {
				for (PredictionEngineItem pei : predictionEngineInput.predictionEngineItems) {
					if (tempItemCodeSet.contains(pei.itemCode)) {
						finalPredictionEngineInput.predictionEngineItems.add(pei);
					}
				}
			} else {
				itemInBatches.put(batchCount, finalPredictionEngineInput);
				
				Set<Integer> nonLigItems = finalPredictionEngineInput.predictionEngineItems.stream().filter(x -> x.retLirId == 0).map(PredictionEngineItem::getItemCode)
						.collect(Collectors.toSet());
				
				Set<Integer> ligItems = finalPredictionEngineInput.predictionEngineItems.stream().filter(x -> x.retLirId > 0).map(PredictionEngineItem::getRetLirId)
						.collect(Collectors.toSet());
				
//				logger.debug("Batch " + batchCount + " has non lig items:"
//						+ nonLigItems.size() + ", non lig items:" + nonLigItems + ",total Ligs:" + ligItems.size() + ",Lig items:" + ligItems);
				
				finalPredictionEngineInput = new PredictionEngineInput();
				copyPredictionEngineInput(predictionEngineInput, finalPredictionEngineInput);
				finalPredictionEngineInput.predictionEngineItems = new ArrayList<PredictionEngineItem>();
				
				for (PredictionEngineItem pei : predictionEngineInput.predictionEngineItems) {
					if (tempItemCodeSet.contains(pei.itemCode)) {
						finalPredictionEngineInput.predictionEngineItems.add(pei);
					}
				}
				
				itemCount = 1;
				batchCount++;
			}
			itemCount = itemCount + tempItemCodeSet.size();
		}
		 
		if(finalPredictionEngineInput.predictionEngineItems != null && finalPredictionEngineInput.predictionEngineItems.size() > 0) {
			itemInBatches.put(batchCount, finalPredictionEngineInput);
			
			Set<Integer> nonLigItems = finalPredictionEngineInput.predictionEngineItems.stream().filter(x -> x.retLirId == 0).map(PredictionEngineItem::getItemCode)
					.collect(Collectors.toSet());
			
			Set<Integer> ligItems = finalPredictionEngineInput.predictionEngineItems.stream().filter(x -> x.retLirId > 0).map(PredictionEngineItem::getRetLirId)
					.collect(Collectors.toSet());
			
//			logger.debug("Batch " + batchCount + " has non lig items:"
//					+ nonLigItems.size() + ", non lig items:" + nonLigItems + ",total Ligs:" + ligItems.size() + ",Lig items:" + ligItems);
			
		}
		
		return itemInBatches;
	}

	public void copyPredictionEngineInput(PredictionEngineInput predictionEngineInputSrc,
			PredictionEngineInput predictionEngineInputSrcDest){
		 
		predictionEngineInputSrcDest.setProductLevelId(predictionEngineInputSrc.getProductLevelId());  
		predictionEngineInputSrcDest.setProductId(predictionEngineInputSrc.getProductId());  
		predictionEngineInputSrcDest.setIsParallel(predictionEngineInputSrc.getIsParallel());
		
	}	
	
	private PredictionDetailKey convertPricePointDTO(int locationLevelId, int locationId, PredictionItemDTO predictionItemDTO,
			PricePointDTO pricePointDTO) {
		PredictionDetailKey predictionDetailKey = new PredictionDetailKey(locationLevelId, locationId, predictionItemDTO.itemCodeOrLirId,
				pricePointDTO.getRegQuantity(), pricePointDTO.getRegPrice(), pricePointDTO.getSaleQuantity(), pricePointDTO.getSalePrice(),
				pricePointDTO.getAdPageNo(), pricePointDTO.getAdBlockNo(), pricePointDTO.getPromoTypeId(), pricePointDTO.getDisplayTypeId());

		return predictionDetailKey;
	}
	
	private PredictionDetailKey convertPricePointDTO(int locationLevelId, int locationId, int itemCode, PricePointDTO pricePointDTO) {
		PredictionDetailKey predictionDetailKey = new PredictionDetailKey(locationLevelId, locationId, itemCode, pricePointDTO.getRegQuantity(),
				pricePointDTO.getRegPrice(), pricePointDTO.getSaleQuantity(), pricePointDTO.getSalePrice(), pricePointDTO.getAdPageNo(),
				pricePointDTO.getAdBlockNo(), pricePointDTO.getPromoTypeId(), pricePointDTO.getDisplayTypeId());

		return predictionDetailKey;
	}

	private PricePointDTO convertPredictionDetailKey(PredictionDetailKey predictionDetailKey) {
		PricePointDTO pricePointDTO = new PricePointDTO();

		pricePointDTO.setRegQuantity(predictionDetailKey.getRegQuantity());
		pricePointDTO.setRegPrice(predictionDetailKey.getRegPrice());
		pricePointDTO.setSaleQuantity(predictionDetailKey.getSaleQuantity());
		pricePointDTO.setSalePrice(predictionDetailKey.getSalePrice());
		pricePointDTO.setAdPageNo(predictionDetailKey.getAdPageNo());
		pricePointDTO.setAdBlockNo(predictionDetailKey.getBlockNo());
		pricePointDTO.setPromoTypeId(predictionDetailKey.getPromoTypeId());
		pricePointDTO.setDisplayTypeId(predictionDetailKey.getDisplayTypeId());

		return pricePointDTO;
	}
		
	private PredictionDetailKey convertPredictionEngineItem(int locationLevelId, int locationId, int itemCode, PredictionEngineItem predictionEngineItem) {
		PredictionDetailKey predictionDetailKey = new PredictionDetailKey(locationLevelId, locationId, itemCode,
				predictionEngineItem.getRegQuantity(), predictionEngineItem.getRegPrice(), predictionEngineItem.getSaleQuantity(),
				predictionEngineItem.getSalePrice(), predictionEngineItem.getAdPageNo(), predictionEngineItem.getAdBlockNo(),
				predictionEngineItem.getPromoTypeId(), predictionEngineItem.getDisplayTypeId());

		return predictionDetailKey;
	}
}
