package com.pristine.service.offermgmt;

import java.util.List;

import com.pristine.dto.offermgmt.PRProductGroupProperty;
import com.pristine.util.Constants;

import org.apache.log4j.Logger;

public class PricingEngineHelper {

	private static Logger logger = Logger.getLogger("PricingEngineHelper");
	
	// Return Product Property of the passed property
	public static PRProductGroupProperty getProductGroupProperty(List<PRProductGroupProperty> productGroupProperties, int productLevelId,
			int productId) {
		PRProductGroupProperty prProductGroupProperty = new PRProductGroupProperty();

		for (PRProductGroupProperty prdGrpPrpty : productGroupProperties) {
			if (prdGrpPrpty.getProductLevelId() == productLevelId && prdGrpPrpty.getProductId() == productId) {
				prProductGroupProperty = prdGrpPrpty;
				break;
			}
		}

		return prProductGroupProperty;
	}
	
	public static boolean isUsePrediction(List<PRProductGroupProperty> productGroupProperties, int productLevelId,
			int productId, int locationLevelId, int locationId){
				
		PRProductGroupProperty prProductGroupProperty = getProductGroupProperty(productGroupProperties, productLevelId, productId);
		
		boolean isUsePrediction = prProductGroupProperty.getIsUsePrediction();

		logger.debug("locationLevelId:" + locationLevelId + ",prProductGroupProperty.getIsUsePrediction():" + prProductGroupProperty.getIsUsePrediction());
		
		//For store don't use prediction
		if(locationLevelId == Constants.STORE_LEVEL_ID){
			//prProductGroupProperty.setIsUsePrediction(false);
			isUsePrediction = false;
		}
		
		//return prProductGroupProperty.getIsUsePrediction();
		return isUsePrediction;
	}
}
