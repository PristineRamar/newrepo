package com.pristine.test.offermgmt.prediction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionEngineItem;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRConstants;

public class TestHelper {

	public static PredictionInputDTO getPredictionInputDTO(int locationLevelId, int locationId, int productLevelId, int productId) {
		PredictionInputDTO predictionInputDTO = new PredictionInputDTO();
		predictionInputDTO.predictionItems = new ArrayList<>();
		
		predictionInputDTO.locationLevelId = locationLevelId;
		predictionInputDTO.locationId = locationId;
		predictionInputDTO.productLevelId = productLevelId;
		predictionInputDTO.productId = productId;
		
		return predictionInputDTO;
	}
	
	public static PredictionItemDTO getPredictionItemDTO(int itemCode, String UPC, int retLirId) {
		PredictionItemDTO predictionItemDTO = new PredictionItemDTO();
		
		predictionItemDTO.setItemCodeOrLirId(itemCode);
		predictionItemDTO.setRetLirId(retLirId);
		predictionItemDTO.upc= UPC;
		
		return predictionItemDTO;
	}
	
	public static void addPricePoint(PredictionItemDTO predictionItemDTO, MultiplePrice regPrice, MultiplePrice salePrice, int pageNo, int blockNo,
			int promoTypeId, int displayTypeId) {
		List<PricePointDTO> pricePoints = new ArrayList<PricePointDTO>();
		if (predictionItemDTO.pricePoints != null) {
			pricePoints = predictionItemDTO.pricePoints;
		}

		PricePointDTO pricePointDTO = new PricePointDTO();
		pricePointDTO.setRegQuantity(regPrice.multiple);
		pricePointDTO.setRegPrice(regPrice.price);
		pricePointDTO.setSaleQuantity(salePrice.multiple);
		pricePointDTO.setSalePrice(salePrice.price);
		pricePointDTO.setAdPageNo(pageNo);
		pricePointDTO.setAdBlockNo(blockNo);
		pricePointDTO.setPromoTypeId(promoTypeId);
		pricePointDTO.setDisplayTypeId(displayTypeId);
		pricePoints.add(pricePointDTO);

		predictionItemDTO.pricePoints = pricePoints;
	}
	
	public static PredictionDetailKey getPredictionDetailKey(int locationLevelId, int locationId, int itemCode, MultiplePrice regPrice,
			MultiplePrice salePrice, int pageNo, int blockNo, int promoTypeId, int displayTypeId) {

		PredictionDetailKey predictionDetailKey = new PredictionDetailKey(locationLevelId, locationId, itemCode, regPrice.multiple, regPrice.price,
				salePrice.multiple, salePrice.price, pageNo, blockNo, promoTypeId, displayTypeId);

		return predictionDetailKey;
	}
	
	public static PredictionDetailKey getPredictionDetailKey(int locationLevelId, int locationId, PredictionItemDTO predictionItemDTO,
			PricePointDTO pricePointDTO) {

		PredictionDetailKey predictionDetailKey = new PredictionDetailKey(locationLevelId, locationId, predictionItemDTO.itemCodeOrLirId,
				pricePointDTO.getRegQuantity(), pricePointDTO.getRegPrice(), pricePointDTO.getSaleQuantity(), pricePointDTO.getSalePrice(),
				pricePointDTO.getAdPageNo(), pricePointDTO.getAdBlockNo(), pricePointDTO.getPromoTypeId(), pricePointDTO.getDisplayTypeId());

		return predictionDetailKey;
	}
	
	public static PredictionDetailDTO getPredictionDetailDTO(double predictedMovement, PredictionStatus predictionStatus) {

		PredictionDetailDTO predictionDetailDTO =  new PredictionDetailDTO();
		
		predictionDetailDTO.setPredictedMovement(predictedMovement);
		predictionDetailDTO.setPredictionStatus(predictionStatus.getStatusCode());

		return predictionDetailDTO;
	}
	
	public static void addItemToLig(HashMap<Integer, List<PRItemDTO>> ligMap, int itemCode, String UPC, int retLirId) {
		PRItemDTO itemDTO = new PRItemDTO();
		
		itemDTO.setItemCode(itemCode);
		itemDTO.setUpc(UPC);
		itemDTO.setRetLirId(retLirId);
		
		List<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
		if(ligMap.get(retLirId) !=null) {
			ligMembers = ligMap.get(itemDTO.getRetLirId());
		}
		
		ligMembers.add(itemDTO);
		
		ligMap.put(itemDTO.getRetLirId(), ligMembers);
	}
	
	public static PredictionEngineItem getPredictionEngineItem(int locationLevelId, int locationId, String weekStartDate, int itemCode, String UPC,
			MultiplePrice regPrice, MultiplePrice salePrice, int pageNo, int blockNo, int promoTypeId, int displayTypeId, int retLirId) {
		PredictionEngineItem predictionEngineItem = new PredictionEngineItem(locationLevelId, locationId, weekStartDate,
				PRConstants.PREDICTION_DEAL_TYPE, itemCode, UPC, regPrice.price, regPrice.multiple, Constants.NO, 'M', true, false,
				"", salePrice.multiple, salePrice.price, pageNo, blockNo, promoTypeId, displayTypeId, 0, 0, 0,
				retLirId);
		return predictionEngineItem;
	}
}
