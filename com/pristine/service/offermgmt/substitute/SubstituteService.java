package com.pristine.service.offermgmt.substitute;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
//import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.prediction.PredictionDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRRecommendationRunHeader;
import com.pristine.dto.offermgmt.PRSubstituteItem;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
//import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionOutputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
//import com.pristine.dto.offermgmt.substitute.SubjstituteAdjRMainItemDTO;
//import com.pristine.dto.offermgmt.substitute.SubstituteAdjRInputDTO;
//import com.pristine.dto.offermgmt.substitute.SubstituteAdjROutputDTO;
//import com.pristine.dto.offermgmt.substitute.SubstituteAdjRSubsItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ItemKey;
//import com.pristine.service.offermgmt.MostOccurrenceData;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionEngineValue;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
//import com.pristine.service.offermgmt.prediction.PredictionStatus;
//import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class SubstituteService {
	private static Logger logger = Logger.getLogger("SusbstituteService");
	
	private SubstituteAdjustmentRService substituteAdjustmentRService;
	private PredictionDAO predictionDAO;
	
	public SubstituteService(boolean isOnline) {
		this.substituteAdjustmentRService = new SubstituteAdjustmentRServiceImpl(isOnline);
		this.predictionDAO = new PredictionDAO();
	}
	
	public SubstituteAdjustmentRService getSubstituteAdjustmentRService() {
		return substituteAdjustmentRService;
	}

	public void setSubstituteAdjustmentRService(SubstituteAdjustmentRService substituteAdjustmentRService) {
		this.substituteAdjustmentRService = substituteAdjustmentRService;
	}

	public PredictionDAO getPredictionDAO() {
		return predictionDAO;
	}

	public void setPredictionDAO(PredictionDAO predictionDAO) {
		this.predictionDAO = predictionDAO;
	}

	// Adjust the current and new retail predicted movement of substitute main item
//	public void adjustPredWithSubsEffect(Connection conn, int locationLevelId, int locationId, int productLevelId, int productId,
//			int recWeekCalendarId, HashMap<ItemKey, List<PRSubstituteItem>> substituteItems, List<PRItemDTO> recommendedItems,
//			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, RetailCalendarDTO curCalDTO, PRRecommendationRunHeader recommendationRunHeader)
//			throws GeneralException {
//
////		PredictionDAO predictionDAO = new PredictionDAO();
//		List<SubjstituteAdjRMainItemDTO> subsAdjRmainItemDTOForRecPrice = new ArrayList<SubjstituteAdjRMainItemDTO>();
//		List<SubjstituteAdjRMainItemDTO> subsAdjRmainItemDTOForCurPrice = new ArrayList<SubjstituteAdjRMainItemDTO>();
//		// to update the prediction cache
//		HashMap<PredictionDetailKey, Double> predictionDetailMap = new HashMap<PredictionDetailKey, Double>();
//
//		//Get prediction
//		List<PredictionInputDTO> predictionInputDTOs = new ArrayList<PredictionInputDTO>();
//		PredictionInputDTO  predictionInputDTO = new PredictionInputDTO();
//		predictionInputDTO.locationLevelId = locationLevelId;
//		predictionInputDTO.locationId = locationId;
//		predictionInputDTOs.add(predictionInputDTO);
//		
//		//Get already predicted movement which will have prediction without substitution
//		//So that cache can be used from next time
//		HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetail = predictionDAO.getAllPredictedMovement(conn, productLevelId, productId,
//				recommendationRunHeader.getCalendarId(), recommendationRunHeader.getCalendarId(), predictionInputDTOs, false);
//		
//		// Form input to R
//		formSubsRServiceInput(substituteItems, recommendedItems, subsAdjRmainItemDTOForRecPrice, subsAdjRmainItemDTOForCurPrice, saleDetails,
//				curCalDTO, recommendationRunHeader, predictionDetail, locationLevelId, locationId);
//
//		logger.debug("subsAdjRmainItemDTO.size():" + subsAdjRmainItemDTOForRecPrice.size());
//
//		// Adjust movement for recommended price
//		if (subsAdjRmainItemDTOForRecPrice.size() > 0) {
//			adjustPredictedMovement(locationLevelId, locationId, productLevelId, productId, recommendedItems, subsAdjRmainItemDTOForRecPrice,
//					predictionDetailMap, "REC_PRICE", predictionDetail);
//		} else {
//			logger.info("Recommended Price - No need to adjust the predicted movement with substitution effect as it is already adjusted");
//		}
//
//		// Adjust movement for current price
//		if (subsAdjRmainItemDTOForCurPrice.size() > 0) {
//			adjustPredictedMovement(locationLevelId, locationId, productLevelId, productId, recommendedItems, subsAdjRmainItemDTOForCurPrice,
//					predictionDetailMap, "CUR_PRICE", predictionDetail);
//		} else {
//			logger.info("Current Price - No need to adjust the predicted movement with substitution effect as it is already adjusted");
//		}
//
//		// Save the new movement in prediction cache
//		//To handle test case, conn case will be null
//		if(conn != null) {
//			updatePredCache(conn, recWeekCalendarId, predictionDetailMap);	
//		}
//	}
//
//	private void adjustPredictedMovement(int locationLevelId, int locationId, int productLevelId, int productId, List<PRItemDTO> recommendedItems,
//			List<SubjstituteAdjRMainItemDTO> subsAdjRmainItemDTO, HashMap<PredictionDetailKey, Double> predictionDetailMap, String operType,
//			HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetail)
//			throws GeneralException {
//		List<SubstituteAdjROutputDTO> subsOutput = new ArrayList<SubstituteAdjROutputDTO>();
//		HashSet<ItemKey> movAdjRetLirId = new HashSet<ItemKey>();
//		SubstituteAdjRInputDTO subsAdjRInputDTO = new SubstituteAdjRInputDTO();
//		subsAdjRInputDTO.setLocationLevelId(locationLevelId);
//		subsAdjRInputDTO.setLocationId(locationId);
//		subsAdjRInputDTO.setProductLevelId(productLevelId);
//		subsAdjRInputDTO.setProductId(productId);
//		subsAdjRInputDTO.setMainItems(subsAdjRmainItemDTO);
//
//		// Call R
//		subsOutput = callRService(subsAdjRInputDTO);
//
//		// Update the movement in the object
//		adjustPredictedMovement(recommendedItems, subsOutput, movAdjRetLirId, predictionDetailMap, locationLevelId, locationId, operType,
//				predictionDetail);
//
//		// Update the lig level reg and sale predicted movement
//		updateLigMovement(recommendedItems, movAdjRetLirId, operType);
//
//	}
//	
//	public List<SubstituteAdjROutputDTO> callRService(SubstituteAdjRInputDTO subsAdjRInputDTO) throws GeneralException {
//		List<SubstituteAdjROutputDTO> subsOutput = substituteAdjustmentRService.getSubsAdjustedMov(subsAdjRInputDTO);
////		List<SubstituteAdjROutputDTO> subsOutput = new ArrayList<SubstituteAdjROutputDTO>();
//		return subsOutput;
//	}
//
//	// Prepare the input structure to R program
//	private void formSubsRServiceInput(HashMap<ItemKey, List<PRSubstituteItem>> substituteItems, List<PRItemDTO> recommendedItems,
//			List<SubjstituteAdjRMainItemDTO> subsAdjRmainItemDTOForRecPrice, List<SubjstituteAdjRMainItemDTO> subsAdjRmainItemDTOForCurPrice,
//			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, RetailCalendarDTO curCalDTO, PRRecommendationRunHeader recommendationRunHeader,
//			HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetail, int locationLevelId, int locationId) {
//		// Loop each substitute items
//		for (Entry<ItemKey, List<PRSubstituteItem>> entry : substituteItems.entrySet()) {
//			ItemKey itemKey = entry.getKey();
//			List<PRSubstituteItem> subsItems = entry.getValue();
//			List<PRItemDTO> mainItems = new ArrayList<PRItemDTO>();
//
//			// If main item is lig, pick all its member
//			if (itemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR) {
//				for (PRItemDTO itemDTO : recommendedItems) {
//					if (itemKey.getItemCodeOrRetLirId() == itemDTO.getRetLirId() && !itemDTO.isLir()) {
//						mainItems.add(itemDTO);
//					}
//				}
//			} else {
//				// If main item is non-lig pick only that item
//				for (PRItemDTO itemDTO : recommendedItems) {
//					if (itemKey.getItemCodeOrRetLirId() == itemDTO.getItemCode() && !itemDTO.isLir()) {
//						mainItems.add(itemDTO);
//					}
//				}
//			}
//
//			// Loop each main item
//			for (PRItemDTO itemDTO : mainItems) {
//				
//				PredictionDetailKey pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(),
//						itemDTO.getRecommendedRegPrice().multiple, itemDTO.getRecommendedRegPrice().price, 0, 0d, 0, 0, 0, 0);
//				PredictionDetailDTO predictionDetailDTO = predictionDetail.get(pdk);
//				
//				// If there is valid predicted mov for rec price and not adjusted before
//				if (predictionDetailDTO != null && predictionDetailDTO.getPredictedMovementWithoutSubsEffect() == 0
//						&& predictionDetailDTO.getPredictionStatus() == PredictionStatus.SUCCESS.getStatusCode()
//						&& predictionDetailDTO.getPredictedMovement() > 0) {
//					addMainAndSubItem(recommendedItems, subsItems, itemDTO, subsAdjRmainItemDTOForRecPrice, "REC_PRICE", saleDetails, curCalDTO,
//							recommendationRunHeader);
//				} else if (predictionDetailDTO != null && predictionDetailDTO.getPredictedMovement() > 0) {
//					itemDTO.setPredMovWOSubsEffect(predictionDetailDTO.getPredictedMovementWithoutSubsEffect());
//				}
//
//				MultiplePrice curPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice());
//
//				if (curPrice != null) {
//					pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), curPrice.multiple, curPrice.price, 0, 0d, 0, 0,
//							0, 0);
//					predictionDetailDTO = predictionDetail.get(pdk);
//
//					// If there is valid predicted mov for cur price and not adjusted before
//					if (predictionDetailDTO != null && predictionDetailDTO.getPredictedMovementWithoutSubsEffect() == 0
//							&& predictionDetailDTO.getPredictedMovement() > 0) {
//						addMainAndSubItem(recommendedItems, subsItems, itemDTO, subsAdjRmainItemDTOForCurPrice, "CUR_PRICE", saleDetails, curCalDTO,
//								recommendationRunHeader);
//					} else if (predictionDetailDTO != null && predictionDetailDTO.getPredictedMovement() > 0) {
//						itemDTO.setCurRegPredMovWOSubsEffect(predictionDetailDTO.getPredictedMovementWithoutSubsEffect());
//					}
//
//				}
//				
//			}
//		}
//	}
//
//	// Add main item and subs item details like itemcode, price...
//	private void addMainAndSubItem(List<PRItemDTO> recommendedItems, List<PRSubstituteItem> subsItems, PRItemDTO mainItemDTO,
//			List<SubjstituteAdjRMainItemDTO> subsAdjRmainItemDTO, String priceType, HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails,
//			RetailCalendarDTO curCalDTO, PRRecommendationRunHeader recommendationRunHeader ) {
//
//		SubjstituteAdjRMainItemDTO mainItem = new SubjstituteAdjRMainItemDTO();
//
//		mainItem.setItemKey(new ItemKey(mainItemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR));
//		if (priceType.equals("REC_PRICE")) {
//			mainItem.setRecWeekRegPrice(mainItemDTO.getRecommendedRegPrice());
//			if(mainItemDTO.getRecWeekSaleInfo().getSalePrice() != null) {
//				mainItem.setRecWeekSalePrice(mainItemDTO.getRecWeekSaleInfo().getSalePrice());	
//			}
//			mainItem.setRegPrediction(mainItemDTO.getPredictedMovement());
//			if (mainItemDTO.isItemPromotedForRecWeek() && mainItemDTO.getRecWeekSaleInfo().getSalePredStatusAtRecReg() != null
//					&& mainItemDTO.getRecWeekSaleInfo().getSalePredStatusAtRecReg() == PredictionStatus.SUCCESS) {
//				mainItem.setSalePrediction(mainItemDTO.getRecWeekSaleInfo().getSalePredMovAtRecReg());
//			}
//			logger.debug("Rec Price - mainItem.getSalePrediction():" + mainItem.getSalePrediction());
//		} else if (priceType.equals("CUR_PRICE")) {
//			MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(mainItemDTO.getRegMPack(), mainItemDTO.getRegPrice(),
//					mainItemDTO.getRegMPrice());
//			mainItem.setRecWeekRegPrice(curRegPrice);
//			if(mainItemDTO.getCurSaleInfo().getSalePrice() != null) {
//				mainItem.setRecWeekSalePrice(mainItemDTO.getCurSaleInfo().getSalePrice());	
//			}
//			mainItem.setRegPrediction(mainItemDTO.getCurRegPricePredictedMovement());
//			if (mainItemDTO.isItemPromotedForRecWeek() && mainItemDTO.getRecWeekSaleInfo().getSalePredStatusAtCurReg() != null
//					&& mainItemDTO.getRecWeekSaleInfo().getSalePredStatusAtCurReg() == PredictionStatus.SUCCESS) {
//				mainItem.setSalePrediction(mainItemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg());
//			}
//			logger.debug("Cur Price - mainItem.getSalePrediction():" + mainItem.getSalePrediction());
//		}
//
//		List<SubstituteAdjRSubsItemDTO> subsAdjRSubsItems = new ArrayList<SubstituteAdjRSubsItemDTO>();
//		// Add substitute items
//		for (PRSubstituteItem subsItem : subsItems) {
//			SubstituteAdjRSubsItemDTO subsAdjRSubsItem = new SubstituteAdjRSubsItemDTO();
//
//			for (PRItemDTO recItem : recommendedItems) {
//				ItemKey recItemKey = PRCommonUtil.getItemKey(recItem);
//				if (recItemKey.equals(subsItem.getSubsProductKey())) {
//					subsAdjRSubsItem.setItemKey(subsItem.getSubsProductKey());
//					subsAdjRSubsItem.setRecWeekRegPrice(recItem.getRecommendedRegPrice());
//					subsAdjRSubsItem
//							.setCurRegPrice(PRCommonUtil.getMultiplePrice(recItem.getRegMPack(), recItem.getRegPrice(), recItem.getRegMPrice()));
//					
//					//Cur sale price
//					subsAdjRSubsItem.setCurSalePrice(
//							getSalePrice(recommendedItems, saleDetails, curCalDTO.getStartDate(), subsItem.getSubsProductKey()));
//					
//					logger.debug("subs item code:" + subsItem.getSubsProductKey().getItemCodeOrRetLirId() + ",cur sale price:" +
//							subsAdjRSubsItem.getCurSalePrice());
//					
//					//Rec week sale price
//					subsAdjRSubsItem.setRecWeekSalePrice(
//							getSalePrice(recommendedItems, saleDetails, recommendationRunHeader.getStartDate(), subsItem.getSubsProductKey()));
//					
//					logger.debug("subs item code:" + subsItem.getSubsProductKey().getItemCodeOrRetLirId() + ",rec sale price:" +
//							subsAdjRSubsItem.getRecWeekSalePrice());
//					
//					subsAdjRSubsItem.setImpactFactor(subsItem.getImpactFactor());
//
//					subsAdjRSubsItems.add(subsAdjRSubsItem);
//					break;
//				}
//			}
//		}
//
//		mainItem.setSubstituteItems(subsAdjRSubsItems);
//		//add main item only if there is active substitute items for this
//		if (subsAdjRSubsItems.size() > 0) {
//			subsAdjRmainItemDTO.add(mainItem);
//		} else {
//			logger.debug("main item is not added as there is no substitute items");
//		}
//	}
//
//	// Update the predicted movement with adjusted movement
//	private void adjustPredictedMovement(List<PRItemDTO> recommendedItems, List<SubstituteAdjROutputDTO> subsOutput, HashSet<ItemKey> movAdjRetLirId,
//			HashMap<PredictionDetailKey, Double> predictionDetailMap, int locationLevelId, int locationId, String operType,
//			HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetail) {
//		for (PRItemDTO itemDTO : recommendedItems) {
//			ItemKey itemKey = PRCommonUtil.getItemKey(itemDTO);
//
//			// Read the output
//			for (SubstituteAdjROutputDTO subsROutput : subsOutput) {
//				// Look for matching item
//				if (itemKey.equals(subsROutput.getItemKey())) {
//					PredictionDetailKey pdk = null;
//
//					if (operType.equals("REC_PRICE")) {
//
//						itemDTO.setPredMovWOSubsEffect(itemDTO.getPredictedMovement());
//
//						logger.debug("Rec/Sale Price predicted movement of item: " + itemKey.toString() + " is adjusted from: "
//								+ itemDTO.getPredictedMovement() + "/" +  itemDTO.getRecWeekSaleInfo().getSalePredMovAtRecReg() 
//								+ " to: " + subsROutput.getAdjRegMov() + "/" +  subsROutput.getAdjSaleMov()  + " with substitution effects");
//
//						itemDTO.setPredictedMovement(subsROutput.getAdjRegMov());
//						itemDTO.getRecWeekSaleInfo().setSalePredMovAtRecReg(subsROutput.getAdjSaleMov());
//
//						// Add reg price key
//						pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), itemDTO.getRecommendedRegPrice().multiple,
//								itemDTO.getRecommendedRegPrice().price, 0, 0d, 0, 0, 0, 0);
//						predictionDetailMap.put(pdk, subsROutput.getAdjRegMov());
//
//						if (itemDTO.isItemPromotedForRecWeek()) {
//							// Add sale price key
//							pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(),
//									itemDTO.getRecommendedRegPrice().multiple, itemDTO.getRecommendedRegPrice().price,
//									(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().multiple : 0),
//									(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().price : 0),
//									itemDTO.getRecWeekAdInfo().getAdPageNo(), itemDTO.getRecWeekAdInfo().getAdBlockNo(),
//									(itemDTO.getRecWeekSaleInfo().getSalePromoTypeLookup() != null
//											? itemDTO.getRecWeekSaleInfo().getSalePromoTypeLookup().getPromoTypeId() : 0),
//									(itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() != null
//											? itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : 0));
//
//							predictionDetailMap.put(pdk, subsROutput.getAdjSaleMov());
//						}
//						// }
//					} else if (operType.equals("CUR_PRICE")) {
//
//						itemDTO.setCurRegPredMovWOSubsEffect(itemDTO.getCurRegPricePredictedMovement());
//
//						logger.debug("Cur Reg/Sale Price predicted movement of item: " + itemKey.toString() + " is adjusted from: "
//								+ itemDTO.getCurRegPricePredictedMovement() + "/" +  itemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg() 
//								+ " to: " + subsROutput.getAdjRegMov() + "/" +  subsROutput.getAdjSaleMov()  + " with substitution effects");
//						
//
//						itemDTO.setCurRegPricePredictedMovement(subsROutput.getAdjRegMov());
//						itemDTO.getRecWeekSaleInfo().setSalePredMovAtCurReg(subsROutput.getAdjSaleMov());
//
//						// Add reg price key
//						MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
//								itemDTO.getRegMPrice());
//						pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), curRegPrice.multiple, curRegPrice.price, 0,
//								0d, 0, 0, 0, 0);
//						predictionDetailMap.put(pdk, subsROutput.getAdjRegMov());
//
//						if (itemDTO.isItemPromotedForRecWeek()) {
//							pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), curRegPrice.multiple, curRegPrice.price,
//									(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().multiple : 0),
//									(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().price : 0),
//									itemDTO.getRecWeekAdInfo().getAdPageNo(), itemDTO.getRecWeekAdInfo().getAdBlockNo(),
//									(itemDTO.getRecWeekSaleInfo().getSalePromoTypeLookup() != null
//											? itemDTO.getRecWeekSaleInfo().getSalePromoTypeLookup().getPromoTypeId() : 0),
//									(itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() != null
//											? itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : 0));
//
//							predictionDetailMap.put(pdk, subsROutput.getAdjSaleMov());
//						}
//					}
//
//					if (itemDTO.getRetLirId() > 0) {
//						movAdjRetLirId.add(new ItemKey(itemDTO.getRetLirId(), PRConstants.LIG_ITEM_INDICATOR));
//					}
//
//					break;
//				}
//			}
//		}
//	}
//
//	// Update the lig level movement
//	private void updateLigMovement(List<PRItemDTO> recommendedItems, HashSet<ItemKey> movAdjRetLirId, String operType) {
//		LIGConstraint ligConstraint = new LIGConstraint();
//		PricingEngineService pricingEngineService = new PricingEngineService();
//		for (ItemKey itemKey : movAdjRetLirId) {
//			List<PRItemDTO> ligMembers = pricingEngineService.getLigMembers(itemKey.getItemCodeOrRetLirId(), recommendedItems);
//			PRItemDTO ligRepresentingItem = pricingEngineService.getLigRepresentingItem(itemKey.getItemCodeOrRetLirId(), recommendedItems);
//			if (ligRepresentingItem != null) {
//
//				if (operType.equals("REC_PRICE")) {
//					ligRepresentingItem.setPredMovWOSubsEffect(ligRepresentingItem.getPredictedMovement());
//					logger.debug("lig:" + ligRepresentingItem.getRetLirId() + " rec/sale price movement before adjustment:"
//							+ ligRepresentingItem.getPredictedMovement() + "/" + ligRepresentingItem.getRecWeekSaleInfo().getSalePredMovAtRecReg());
//					ligRepresentingItem = ligConstraint.sumPredictionForLIG(ligRepresentingItem, ligMembers);
//					// TODO:: sum lig level sale prediction
//					logger.debug("lig:" + ligRepresentingItem.getRetLirId() + " rec/sale price movement after adjustment:"
//							+ ligRepresentingItem.getPredictedMovement() + "/" + ligRepresentingItem.getRecWeekSaleInfo().getSalePredMovAtRecReg());
//					ligRepresentingItem = ligConstraint.sumRecWeekSalePredAtRecRegForLIG(ligRepresentingItem, ligMembers);
//
//				} else if (operType.equals("CUR_PRICE")) {
//					ligRepresentingItem.setCurRegPredMovWOSubsEffect(ligRepresentingItem.getCurRegPricePredictedMovement());
//					logger.debug("lig:" + ligRepresentingItem.getRetLirId() + " cur/sale price movement before adjustment:"
//							+ ligRepresentingItem.getCurRegPricePredictedMovement() + "/"
//							+ ligRepresentingItem.getRecWeekSaleInfo().getSalePredMovAtCurReg());
//					ligRepresentingItem = ligConstraint.sumCurRetailPredictionForLIG(ligRepresentingItem, ligMembers);
//					// TODO:: sum lig level sale prediction
//					logger.debug("lig:" + ligRepresentingItem.getRetLirId() + " cur/sale price movement after adjustment:"
//							+ ligRepresentingItem.getCurRegPricePredictedMovement() + "/"
//							+ ligRepresentingItem.getRecWeekSaleInfo().getSalePredMovAtCurReg());
//					ligRepresentingItem = ligConstraint.sumRecWeekSalePredAtCurRegForLIG(ligRepresentingItem, ligMembers);
//				}
//
//			}
//		}
//	}
//
//	private void updatePredCache(Connection conn, int recWeekCalendarId, HashMap<PredictionDetailKey, Double> movementMap)
//			throws GeneralException {
////		PredictionDAO predictionDAO = new PredictionDAO();
////		predictionDAO.updatePredictedMovement(conn, recWeekCalendarId, recWeekCalendarId, movementMap);
//	}
//
//	public List<Integer> convertSubsMainItemToCodes(HashMap<ItemKey, List<PRSubstituteItem>> substituteItems,
//			HashMap<ItemKey, PRItemDTO> itemDataMap) {
//		List<Integer> itemCodes = new ArrayList<Integer>();
//		// Loop each substitute items
//		for (Entry<ItemKey, List<PRSubstituteItem>> entry : substituteItems.entrySet()) {
//			ItemKey itemKey = entry.getKey();
//			// If main item is lig, pick all its member
//			if (itemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR) {
//				for (PRItemDTO itemDTO : itemDataMap.values()) {
//					if (itemKey.getItemCodeOrRetLirId() == itemDTO.getRetLirId() && !itemDTO.isLir()) {
//						itemCodes.add(itemDTO.getItemCode());
//					}
//				}
//			} else {
//				// If main item is non-lig pick only that item
//				for (PRItemDTO itemDTO : itemDataMap.values()) {
//					if (itemKey.getItemCodeOrRetLirId() == itemDTO.getItemCode()  && !itemDTO.isLir()) {
//						itemCodes.add(itemDTO.getItemCode());
//					}
//				}
//			}
//		}
//		return itemCodes;
//	}
//
//	private MultiplePrice getSalePrice(List<PRItemDTO> recommendedItems, HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, String weekStartDate,
//			ItemKey itemKey) {
//		MultiplePrice salePrice = null;
//		List<PRItemDTO> items = new ArrayList<PRItemDTO>();
//		List<PRItemDTO> itemWithSalePrice = new ArrayList<PRItemDTO>();
//		
//		if (itemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR) {
//			for (PRItemDTO itemDTO : recommendedItems) {
//				if (itemKey.getItemCodeOrRetLirId() == itemDTO.getRetLirId() && !itemDTO.isLir()) {
//					items.add(itemDTO);
//				}
//			}
//		} else {
//			// If main item is non-lig pick only that item
//			for (PRItemDTO itemDTO : recommendedItems) {
//				if (itemKey.getItemCodeOrRetLirId() == itemDTO.getItemCode() && !itemDTO.isLir()) {
//					items.add(itemDTO);
//				}
//			}
//		}
//		
//		//Get all items sales price
//		for(PRItemDTO itemDTO : items) {
//			if (saleDetails.get(itemDTO.getItemCode()) != null) {
//				List<PRItemSaleInfoDTO> salePrices = saleDetails.get(itemDTO.getItemCode());
//				for (PRItemSaleInfoDTO saleInfoDTO : salePrices) {
//					if (saleInfoDTO.getSaleWeekStartDate().equals(weekStartDate)) {
//						PRItemDTO tempItemDTO = new PRItemDTO();
//						tempItemDTO.getCurSaleInfo().setSalePrice(saleInfoDTO.getSalePrice());
//						itemWithSalePrice.add(tempItemDTO);
//						break;
//					}
//				}
//			}
//		}
//		
//		//Get common price
//		MostOccurrenceData mod = new MostOccurrenceData();
//		Object object = mod.getMaxOccurance(itemWithSalePrice, "CurSaleMultiplePrice");
//		salePrice = (object != null ? (MultiplePrice) object : null);
//		return salePrice;
//	}

	
	public void adjustPredWithSubsEffect(Connection conn, PRRecommendationRunHeader recommendationRunHeader, RetailCalendarDTO curCalDTO,
			List<PRSubstituteItem> substituteItems, List<PRItemDTO> recommendedItems, HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails,
			List<ExecutionTimeLog> executionTimeLogs) throws GeneralException, Exception, OfferManagementException {

		PredictionService predictionService = new PredictionService();
		PredictionOutputDTO predictionOutputDTO = new PredictionOutputDTO();
		HashSet<ItemKey> movAdjRetLirId = new HashSet<ItemKey>();
		HashMap<PredictionDetailKey, PredictionEngineValue> predictionOutputMap = new HashMap<PredictionDetailKey, PredictionEngineValue>();
		
		// Get prediction
		List<PredictionInputDTO> tempPredictionInputDTOs = new ArrayList<PredictionInputDTO>();
		PredictionInputDTO tempPredictionInputDTO = new PredictionInputDTO();
		tempPredictionInputDTO.locationLevelId = recommendationRunHeader.getLocationLevelId();
		tempPredictionInputDTO.locationId = recommendationRunHeader.getLocationId();
		tempPredictionInputDTOs.add(tempPredictionInputDTO);

		// Get already predicted movement which will have prediction without substitution
		// So that cache can be used from next time
		HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetail = predictionDAO.getAllPredictedMovement(conn,
				recommendationRunHeader.getProductLevelId(), recommendationRunHeader.getProductId(), recommendationRunHeader.getCalendarId(),
				recommendationRunHeader.getCalendarId(), tempPredictionInputDTOs, false);
		
		
		// Form input
		PredictionInputDTO predictionInputDTO = formSubstitutePredictionInput(substituteItems, recommendedItems, saleDetails, curCalDTO,
				recommendationRunHeader, predictionDetail);

		if(predictionInputDTO.predictionItems.size() > 0 ) {
			
			// Call substitution
			logger.info("Substitute prediction adjustment is started...");
			predictionOutputDTO = predictionService.predictSubstituteImpact(conn, predictionInputDTO, executionTimeLogs);
			logger.info("Substitute prediction adjustment is completed...");	
			
			if (predictionOutputDTO.predictionItems != null && predictionOutputDTO.predictionItems.size() > 0) {
				// Adjust movement at item and lig level
				adjustPredictedMovement(predictionOutputDTO, substituteItems, recommendedItems, movAdjRetLirId, predictionOutputMap, recommendationRunHeader);
				
				// Update the lig level reg and sale predicted movement
				updateLigMovement(recommendedItems, movAdjRetLirId);
				
				// Update prediction cache
				updatePredictionCache(conn, recommendationRunHeader, predictionOutputMap);	
			}
		}
	}
	
	private PredictionInputDTO formSubstitutePredictionInput(List<PRSubstituteItem> substituteItems, List<PRItemDTO> recommendedItems,
			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, RetailCalendarDTO curCalDTO, PRRecommendationRunHeader recommendationRunHeader,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetail) {

		List<PredictionItemDTO> predictionItems = new ArrayList<PredictionItemDTO>();
		HashSet<Integer> distinctMainItemCodes = new HashSet<Integer>();
		HashSet<Integer> distinctSubItemCodes = new HashSet<Integer>();
		HashSet<Integer> distinctItemCodes = new HashSet<Integer>();

		PredictionInputDTO predictionInputDTO = fillPredictionInputDTO(recommendationRunHeader);

		// Find distinct item codes
		for (PRSubstituteItem substituteItem : substituteItems) {
			// Find main item codes
			distinctMainItemCodes.addAll(getDistinctItems(substituteItem.getBaseProductKey(), recommendedItems));

			// Find subs item codes
			distinctSubItemCodes.addAll(getDistinctItems(substituteItem.getSubsProductKey(), recommendedItems));
		}

		distinctItemCodes.addAll(distinctMainItemCodes);
		distinctItemCodes.addAll(distinctSubItemCodes);

		// Add Scenario 1 - Current Regular price
		for (Integer itemCode : distinctItemCodes) {
			PredictionItemDTO predictionItemDTO = addPredictionItems(1, itemCode, recommendedItems, saleDetails, curCalDTO, "CUR_REG_PRICE",
					predictionDetail, recommendationRunHeader);
			if (predictionItemDTO != null && predictionItemDTO.pricePoints.size() > 0) {
				predictionItems.add(predictionItemDTO);
			}
		}

		// Add Scenario 2 - Recommended Regular price
		for (Integer itemCode : distinctItemCodes) {
			PredictionItemDTO predictionItemDTO = addPredictionItems(2, itemCode, recommendedItems, saleDetails, curCalDTO, "REC_WEEK_REG_PRICE",
					predictionDetail, recommendationRunHeader);
			if (predictionItemDTO != null && predictionItemDTO.pricePoints.size() > 0) {
				predictionItems.add(predictionItemDTO);
			}
		}

		// Add Scenario 3 - Current Regular + Sale price
		for (Integer itemCode : distinctItemCodes) {
			PredictionItemDTO predictionItemDTO = addPredictionItems(3, itemCode, recommendedItems, saleDetails, curCalDTO,
					"CUR_REG_PLUS_REC_WEEK_SALE_PRICE", predictionDetail, recommendationRunHeader);
			if (predictionItemDTO != null && predictionItemDTO.pricePoints.size() > 0) {
				predictionItems.add(predictionItemDTO);
			}
		}

		// Add Scenario 4 - Recommended Regular + Sale price
		for (Integer itemCode : distinctItemCodes) {
			PredictionItemDTO predictionItemDTO = addPredictionItems(4, itemCode, recommendedItems, saleDetails, curCalDTO,
					"REC_REG_PLUS_REC_WEEK_SALE_PRICE", predictionDetail, recommendationRunHeader);
			if (predictionItemDTO != null && predictionItemDTO.pricePoints.size() > 0) {
				predictionItems.add(predictionItemDTO);
			}
		}

		predictionInputDTO.predictionItems = predictionItems;

		return predictionInputDTO;
	}

	private PredictionInputDTO fillPredictionInputDTO(PRRecommendationRunHeader recommendationRunHeader) {

		PredictionInputDTO predictionInputDTO = new PredictionInputDTO();
		predictionInputDTO.locationLevelId = recommendationRunHeader.getLocationLevelId();
		predictionInputDTO.locationId = recommendationRunHeader.getLocationId();
		predictionInputDTO.productLevelId = recommendationRunHeader.getProductLevelId();
		predictionInputDTO.productId = recommendationRunHeader.getProductId();
		predictionInputDTO.startCalendarId = recommendationRunHeader.getCalendarId();
		predictionInputDTO.startWeekDate = recommendationRunHeader.getStartDate();

		return predictionInputDTO;
	}
	
	private HashSet<Integer> getDistinctItems(ItemKey itemKey, List<PRItemDTO> recommendedItems) {
		HashSet<Integer> distinctItemCodes = new HashSet<Integer>();
		if (itemKey.getLirIndicator() == PRConstants.LIG_ITEM_INDICATOR) {
			List<PRItemDTO> ligMembers = new PricingEngineService().getLigMembers(itemKey.getItemCodeOrRetLirId(), recommendedItems);
			for (PRItemDTO itemDTO : ligMembers) {
				// MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);

				// Add only if both rec and cur reg price is there
				// if (itemDTO.getRecommendedRegPrice() != null && curRegPrice != null) {
				distinctItemCodes.add(itemDTO.getItemCode());
				// }
			}
		} else {
			PRItemDTO itemDTO = getItemDTO(itemKey, recommendedItems);
			if (itemDTO != null) {
				distinctItemCodes.add(itemKey.getItemCodeOrRetLirId());
			} else {
				logger.debug("One of the non-lig substitute item is inactive:" + itemKey.toString());
			}

		}
		return distinctItemCodes;
	}
	
	private PRItemDTO getItemDTO(ItemKey itemKey, List<PRItemDTO> recommendedItems) {
		PRItemDTO outputItemDTO = null;
		for (PRItemDTO itemDTO : recommendedItems) {
			ItemKey tempItemKey = PRCommonUtil.getItemKey(itemDTO);
			if (itemKey.equals(tempItemKey)) {
				outputItemDTO = itemDTO;
				break;
			}
		}
		return outputItemDTO;
	}
	
	private PredictionItemDTO addPredictionItems(int subsScenarioId, Integer itemCode, List<PRItemDTO> recommendedItems,
			HashMap<Integer, List<PRItemSaleInfoDTO>> saleDetails, RetailCalendarDTO curCalDTO, String priceType,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetail, PRRecommendationRunHeader recommendationRunHeader) {
		PredictionItemDTO predictionItemDTO = null;
		// Get item detail
		ItemKey itemKey = new ItemKey(itemCode, PRConstants.NON_LIG_ITEM_INDICATOR);
		PRItemDTO itemDTO = getItemDTO(itemKey, recommendedItems);
		if (itemDTO != null) {
			predictionItemDTO = new PredictionItemDTO();
			predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();

			predictionItemDTO.subsScenarioId = subsScenarioId;
			predictionItemDTO.itemCodeOrLirId = itemDTO.getItemCode();
			predictionItemDTO.upc = itemDTO.getUpc();
			predictionItemDTO.lirInd = false;

			addPricePoint(predictionItemDTO, itemDTO, priceType, predictionDetail, recommendationRunHeader);
		}
		return predictionItemDTO;
	}
	
	private void addPricePoint(PredictionItemDTO predictionItemDTO, PRItemDTO itemDTO, String pricePointType,
			HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetail, PRRecommendationRunHeader recommendationRunHeader) {
		PricePointDTO pricePointDTO = new PricePointDTO();
		Integer regQuantity = 0;
		Double regPrice = 0d, salePrice = 0d;
		int saleQuantity = 0, promoTypeId = 0, adPageNo = 0, adBlockNo = 0, displayTypeId = 0;
		boolean isAddSalePrice = false, isAddPricePoint = false;

		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);
		MultiplePrice recRegPrice = itemDTO.getRecommendedRegPrice();
		
		int locationLevelId = recommendationRunHeader.getLocationLevelId();
		int locationId = recommendationRunHeader.getLocationId();

		if (pricePointType.equals("CUR_REG_PRICE") && curRegPrice != null) {
			regQuantity = curRegPrice.multiple;
			regPrice = curRegPrice.price;
			
			predictionItemDTO.predictedMovement = (itemDTO.getCurRegPricePredictedMovement() != null ? itemDTO.getCurRegPricePredictedMovement() : 0);
			
			//NU::6th Nov 2017, as current reg price prediction status is not saved in the database,
			//when re-recommendation is called, all the status is set as -1, 
			//so substitution again calls the prediction unnecessary
			if(predictionItemDTO.predictedMovement > 0) {
				predictionItemDTO.predictionStatus = PredictionStatus.SUCCESS.getStatusCode();
			} else {
				predictionItemDTO.predictionStatus = (itemDTO.getCurRegPricePredictionStatus() != null ? itemDTO.getCurRegPricePredictionStatus()
						: PredictionStatus.PREDICTION_APP_EXCEPTION.getStatusCode());
			}
			
			
			PredictionDetailKey pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), regQuantity, regPrice, 0, 0d, 0, 0,
					0, 0);
			PredictionDetailDTO predictionDetailDTO = predictionDetail.get(pdk);

			// If there is valid predicted mov for rec price and not adjusted before
			if (predictionDetailDTO != null && predictionDetailDTO.getPredictedMovementWithoutSubsEffect() == 0
					&& predictionDetailDTO.getPredictionStatus() == PredictionStatus.SUCCESS.getStatusCode()
					&& predictionDetailDTO.getPredictedMovement() > 0) {
				isAddPricePoint = true;
			}
		} else if (pricePointType.equals("REC_WEEK_REG_PRICE") && recRegPrice != null && (curRegPrice != null && !curRegPrice.equals(recRegPrice))) {
			regQuantity = recRegPrice.multiple;
			regPrice = recRegPrice.price;
			
			predictionItemDTO.predictedMovement = (itemDTO.getPredictedMovement() != null ? itemDTO.getPredictedMovement() : 0);
			
			predictionItemDTO.predictionStatus = (itemDTO.getPredictionStatus() != null ? itemDTO.getPredictionStatus()
					: PredictionStatus.PREDICTION_APP_EXCEPTION.getStatusCode());
			
			PredictionDetailKey pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), regQuantity, regPrice, 0, 0d, 0, 0,
					0, 0);
			PredictionDetailDTO predictionDetailDTO = predictionDetail.get(pdk);

			// If there is valid predicted mov for rec price and not adjusted before
			if (predictionDetailDTO != null && predictionDetailDTO.getPredictedMovementWithoutSubsEffect() == 0
					&& predictionDetailDTO.getPredictionStatus() == PredictionStatus.SUCCESS.getStatusCode()
					&& predictionDetailDTO.getPredictedMovement() > 0) {
				isAddPricePoint = true;
			}
			
		} else if (pricePointType.equals("CUR_REG_PLUS_REC_WEEK_SALE_PRICE") && curRegPrice != null && itemDTO.isItemPromotedForRecWeek()) {
			regQuantity = curRegPrice.multiple;
			regPrice = curRegPrice.price;
			
			predictionItemDTO.predictedMovement = (itemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg() != null
					? itemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg() : 0);
			
			predictionItemDTO.predictionStatus = (itemDTO.getRecWeekSaleInfo().getSalePredStatusAtCurReg() != null
					? itemDTO.getRecWeekSaleInfo().getSalePredStatusAtCurReg().getStatusCode()
					: PredictionStatus.PREDICTION_APP_EXCEPTION.getStatusCode());
			
			isAddSalePrice = true;
			
			PredictionDetailKey pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), regQuantity, regPrice,
					(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().multiple : 0),
					(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().price : 0),
					itemDTO.getRecWeekAdInfo().getAdPageNo(), itemDTO.getRecWeekAdInfo().getAdBlockNo(),
					(itemDTO.getRecWeekSaleInfo() != null
							? itemDTO.getRecWeekSaleInfo().getPromoTypeId() : 0),
					(itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() != null
							? itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : 0));
			PredictionDetailDTO predictionDetailDTO = predictionDetail.get(pdk);

			// If there is valid predicted mov for rec price and not adjusted before
			if (predictionDetailDTO != null && predictionDetailDTO.getPredictedMovementWithoutSubsEffect() == 0
					&& predictionDetailDTO.getPredictionStatus() == PredictionStatus.SUCCESS.getStatusCode()
					&& predictionDetailDTO.getPredictedMovement() > 0) {
				isAddPricePoint = true;
			}
			
		} else if (pricePointType.equals("REC_REG_PLUS_REC_WEEK_SALE_PRICE") && recRegPrice != null && itemDTO.isItemPromotedForRecWeek()
				&& (curRegPrice != null && !curRegPrice.equals(recRegPrice))) {
			regQuantity = recRegPrice.multiple;
			regPrice = recRegPrice.price;
			
			predictionItemDTO.predictedMovement = (itemDTO.getRecWeekSaleInfo().getSalePredMovAtRecReg() != null
					? itemDTO.getRecWeekSaleInfo().getSalePredMovAtRecReg() : 0);
			
			predictionItemDTO.predictionStatus = (itemDTO.getRecWeekSaleInfo().getSalePredStatusAtRecReg() != null
					? itemDTO.getRecWeekSaleInfo().getSalePredStatusAtRecReg().getStatusCode()
					: PredictionStatus.PREDICTION_APP_EXCEPTION.getStatusCode());
			
			isAddSalePrice = true;
			
			PredictionDetailKey pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), regQuantity, regPrice,
					(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().multiple : 0),
					(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().price : 0),
					itemDTO.getRecWeekAdInfo().getAdPageNo(), itemDTO.getRecWeekAdInfo().getAdBlockNo(),
					(itemDTO.getRecWeekSaleInfo() != null
							? itemDTO.getRecWeekSaleInfo().getPromoTypeId() : 0),
					(itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() != null
							? itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : 0));
			PredictionDetailDTO predictionDetailDTO = predictionDetail.get(pdk);
			
			// If there is valid predicted mov for rec price and not adjusted before
			if (predictionDetailDTO != null && predictionDetailDTO.getPredictedMovementWithoutSubsEffect() == 0
					&& predictionDetailDTO.getPredictionStatus() == PredictionStatus.SUCCESS.getStatusCode()
					&& predictionDetailDTO.getPredictedMovement() > 0) {
				isAddPricePoint = true;
			}
		}

		if (isAddSalePrice) {
			saleQuantity = itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().multiple : 0;
			salePrice = itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().price : 0;
			promoTypeId = (itemDTO.getRecWeekSaleInfo() != null
					? itemDTO.getRecWeekSaleInfo().getPromoTypeId() : 0);
			adPageNo = itemDTO.getRecWeekAdInfo().getAdPageNo();
			adBlockNo = itemDTO.getRecWeekAdInfo().getAdBlockNo();
			displayTypeId = itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() != null
					? itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : 0;
		}

		pricePointDTO.setRegQuantity(regQuantity);
		pricePointDTO.setRegPrice(regPrice);
		pricePointDTO.setSaleQuantity(saleQuantity);
		pricePointDTO.setSalePrice(salePrice);
		pricePointDTO.setPromoTypeId(promoTypeId);
		pricePointDTO.setAdPageNo(adPageNo);
		pricePointDTO.setAdBlockNo(adBlockNo);
		pricePointDTO.setDisplayTypeId(displayTypeId);
		
		if(isAddPricePoint) {
			logger.debug("ItemCode:" + itemDTO.getItemCode() + ",predictedMovement:" + predictionItemDTO.predictedMovement + ",predictionStatus:"
					+ predictionItemDTO.predictionStatus);
			predictionItemDTO.pricePoints.add(pricePointDTO);	
		}
	}
	
	
	// Update the predicted movement with adjusted movement
	private void adjustPredictedMovement(PredictionOutputDTO predictionOutputDTO, List<PRSubstituteItem> substituteItems,
			List<PRItemDTO> recommendedItems, HashSet<ItemKey> movAdjRetLirId,
			HashMap<PredictionDetailKey, PredictionEngineValue> predictionOutputMap, PRRecommendationRunHeader recommendationRunHeader) {
		
		HashSet<Integer> distinctSubItemCodes = new HashSet<Integer>();
		int locationLevelId = recommendationRunHeader.getLocationLevelId();
		int locationId = recommendationRunHeader.getLocationId();
		
		// Loop all substitute items
		for (PRSubstituteItem substituteItem : substituteItems) {
			// Find distinct item codes
			distinctSubItemCodes.addAll(getDistinctItems(substituteItem.getSubsProductKey(), recommendedItems));
		}

		// Convert prediction output to map for easier access
		if (predictionOutputDTO.predictionItems != null) {
			for (PredictionItemDTO predictionItemDTO : predictionOutputDTO.predictionItems) {
				if (predictionItemDTO.pricePoints != null) {
					for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
						PredictionDetailKey pdk = new PredictionDetailKey(predictionOutputDTO.locationLevelId, predictionOutputDTO.locationId,
								predictionItemDTO.itemCodeOrLirId, pricePointDTO.getRegQuantity(), pricePointDTO.getRegPrice(),
								pricePointDTO.getSaleQuantity(), pricePointDTO.getSalePrice(), pricePointDTO.getAdPageNo(),
								pricePointDTO.getAdBlockNo(), pricePointDTO.getPromoTypeId(), pricePointDTO.getDisplayTypeId());
						PredictionEngineValue pev = new PredictionEngineValue(pricePointDTO.getPredictedMovement(), pricePointDTO.getPredictionStatus(),
								pricePointDTO.getSubstituteImpactMin(), pricePointDTO.getSubstituteImpactMax(), pricePointDTO.getConfidenceLevelLower(),
								pricePointDTO.getConfidenceLevelUpper(), pricePointDTO.getPredictedMovementBeforeSubsAdjustment());
						
						if(pricePointDTO.getPredictionStatus() != PredictionStatus.PREDICTION_APP_EXCEPTION) {
							predictionOutputMap.put(pdk, pev);
						}
					}
				}
			}
		}
		
		 
		//Update movement for each substitute items
		for(Integer itemCode: distinctSubItemCodes) {
			ItemKey itemKey = new ItemKey(itemCode, PRConstants.NON_LIG_ITEM_INDICATOR);
			PRItemDTO itemDTO = getItemDTO(itemKey, recommendedItems);
			PredictionDetailKey pdk = null;
			PredictionEngineValue pev = null;
			
			// Update cur price
			MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
					itemDTO.getRegMPrice());
			
			if (curRegPrice != null) {
				pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), curRegPrice.multiple, curRegPrice.price, 0, 0d, 0,
						0, 0, 0);
				if (pdk != null && predictionOutputMap.get(pdk) != null) {
					pev = predictionOutputMap.get(pdk);
					itemDTO.setCurRegPricePredictedMovement(pev.predictedMovement);
					itemDTO.setCurRegPredMovWOSubsEffect(pev.predictedMovementBeforeSubsAdjustment);
				}
			}
		 
			
			// Update rec price
			if(itemDTO.getRecommendedRegPrice() != null) {
				pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), itemDTO.getRecommendedRegPrice().multiple,
						itemDTO.getRecommendedRegPrice().price, 0, 0d, 0, 0, 0, 0);
				
				if(pdk != null && predictionOutputMap.get(pdk) != null) {
					pev = predictionOutputMap.get(pdk);
					itemDTO.setPredictedMovement(pev.predictedMovement);
					itemDTO.setPredMovWOSubsEffect(pev.predictedMovementBeforeSubsAdjustment);
				}	
			}
			
			
			if (itemDTO.isItemPromotedForRecWeek() && curRegPrice != null) {
				// Update cur + sale
				pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(), curRegPrice.multiple, curRegPrice.price,
						(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().multiple : 0),
						(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().price : 0),
						(itemDTO.getRecWeekAdInfo() != null ? itemDTO.getRecWeekAdInfo().getAdPageNo() : 0), 
						(itemDTO.getRecWeekAdInfo() != null ? itemDTO.getRecWeekAdInfo().getAdBlockNo() : 0),
						(itemDTO.getRecWeekSaleInfo() != null
								? itemDTO.getRecWeekSaleInfo().getPromoTypeId() : 0),
						(itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() != null
								? itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : 0));
				
				if(pdk != null && predictionOutputMap.get(pdk) != null) {
					pev = predictionOutputMap.get(pdk);
					itemDTO.getRecWeekSaleInfo().setSalePredMovAtCurReg(pev.predictedMovement);
				}
				
				// if item effective date is future, then the sale prediction for recommendation week
				// would be cur price + recommended week sale price, as effective of recommended price
				// will be the future date
				if (!itemDTO.isFutureRetailRecommended()) {
					// Update rec + sale
					pdk = new PredictionDetailKey(locationLevelId, locationId, itemDTO.getItemCode(),
							itemDTO.getRecommendedRegPrice().multiple, itemDTO.getRecommendedRegPrice().price,
							(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().multiple : 0),
							(itemDTO.getRecWeekSaleInfo().getSalePrice() != null ? itemDTO.getRecWeekSaleInfo().getSalePrice().price : 0),
							itemDTO.getRecWeekAdInfo().getAdPageNo(), itemDTO.getRecWeekAdInfo().getAdBlockNo(),
							(itemDTO.getRecWeekSaleInfo() != null
									? itemDTO.getRecWeekSaleInfo().getPromoTypeId() : 0),
							(itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup() != null
									? itemDTO.getRecWeekDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : 0));
					
					if(pdk != null && predictionOutputMap.get(pdk) != null) {
						pev = predictionOutputMap.get(pdk);
						itemDTO.getRecWeekSaleInfo().setSalePredMovAtRecReg(pev.predictedMovement);
					}
				} else {
					itemDTO.getRecWeekSaleInfo().setSalePredMovAtRecReg(itemDTO.getRecWeekSaleInfo().getSalePredMovAtCurReg());
				}
				
			}
			
			if (itemDTO.getRetLirId() > 0) {
				movAdjRetLirId.add(new ItemKey(itemDTO.getRetLirId(), PRConstants.LIG_ITEM_INDICATOR));
			}

		}
	}

	private void updateLigMovement(List<PRItemDTO> recommendedItems, HashSet<ItemKey> movAdjRetLirId) {
		LIGConstraint ligConstraint = new LIGConstraint();
		PricingEngineService pricingEngineService = new PricingEngineService();
		String log = ""; 
		
		for (ItemKey itemKey : movAdjRetLirId) {
			List<PRItemDTO> ligMembers = pricingEngineService.getLigMembers(itemKey.getItemCodeOrRetLirId(), recommendedItems);
			PRItemDTO ligRepresentingItem = pricingEngineService.getLigRepresentingItem(itemKey.getItemCodeOrRetLirId(), recommendedItems);
			if (ligRepresentingItem != null) {
				
				ligRepresentingItem.setPredMovWOSubsEffect(ligRepresentingItem.getPredictedMovement());
				log = "lig:" + ligRepresentingItem.getRetLirId() + " rec/sale price movement before - after adjustment:"
						+ ligRepresentingItem.getPredictedMovement() + "/" + ligRepresentingItem.getRecWeekSaleInfo().getSalePredMovAtRecReg() + "--";
			 
				ligRepresentingItem = ligConstraint.sumPredictionForLIG(ligRepresentingItem, ligMembers);
				log = log + ligRepresentingItem.getPredictedMovement() + "/" + ligRepresentingItem.getRecWeekSaleInfo().getSalePredMovAtRecReg();
				
				ligRepresentingItem = ligConstraint.sumRecWeekSalePredAtRecRegForLIG(ligRepresentingItem, ligMembers);
				logger.debug(log);
				
				ligRepresentingItem.setCurRegPredMovWOSubsEffect(ligRepresentingItem.getCurRegPricePredictedMovement());
				log = "lig:"  + ligRepresentingItem.getRetLirId() + " cur/sale price movement before  - after adjustment:"
						+ ligRepresentingItem.getCurRegPricePredictedMovement() + "/"
						+ ligRepresentingItem.getRecWeekSaleInfo().getSalePredMovAtCurReg() + "--";
				
				ligRepresentingItem = ligConstraint.sumCurRetailPredictionForLIG(ligRepresentingItem, ligMembers);
				log = log + ligRepresentingItem.getCurRegPricePredictedMovement() + "/" + ligRepresentingItem.getRecWeekSaleInfo().getSalePredMovAtCurReg();
				
				ligRepresentingItem = ligConstraint.sumRecWeekSalePredAtCurRegForLIG(ligRepresentingItem, ligMembers);
				logger.debug(log);

			}
		}
	}
	
	private void updatePredictionCache(Connection conn, PRRecommendationRunHeader recommendationRunHeader,
			HashMap<PredictionDetailKey, PredictionEngineValue> movementMap) throws GeneralException {
		new PredictionDAO().updatePredictedMovement(conn, recommendationRunHeader.getCalendarId(), recommendationRunHeader.getCalendarId(),
				movementMap);
	}

}
