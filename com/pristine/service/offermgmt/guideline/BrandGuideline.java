package com.pristine.service.offermgmt.guideline;

//import java.text.DecimalFormat;

import com.pristine.dto.offermgmt.PRConstraintRounding;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRConstants;

public class BrandGuideline {

	
	
	
	public double[] applyStoreBrandGuideline(PRRange brandGuidelineRange, String storeBrandRelationOperator, PRRange inputRange,
			PRConstraintRounding rConstraintDTO){
		//For ahold, whenever there is cost change, and there is relation between store and national brand
		//The price range to be picked in a different way, by giving importance to brand relation and getting
		//rounding digits within brand relation or closer to brand relation (if the input range doesn't fetch any rounding digits)
		
		/*	> 10-15%			> 10%			> 1-2$			> 1$			>
		 * 	< 10-15%			< 15%			< 1-2$			< 1$			<
		 * 	>= 10-15%			>= 10%			>= 1-2$			>= 1$			>=
		 * 	<= 10-15%			<= 10%			<= 1-2$			<= 1$			<=
		 * 	= 10-15%			= 10%			= 1-2$			= 1$			=
		 * 	10-15% above		10% above		above 1-2$		above 1$		above
		 * 	10-15% below		10% below		below 1-2$		below 1$		below*/
		
		/*Relation		Brand Range			Final Range
			below			4.20 -- 4.20		4.20 -- 4.20
			above			4.20 -- 4.20		4.20 -- 4.20
			<				   - -- 4.19		   - -- 4.19, 3.65 -- 4.19	
			<=				   - -- 4.20		   - -- 4.19, 3.65 -- 4.19
			=				4.20 -- 4.20		4.20 -- 4.20
			>				4.21 - 				4.21 --     , 4.21 -- 4.34 
			>=				4.20 - 	     		4.21 --     , 4.21 -- 4.56
				
		within range	8.32 - 8.91	
												8.91 - 8.91
												8.32 - 8.32
												8.32 - 8.34
												8.88 - 8.91
												8.32 - 8.91*/

		
		Boolean isBrandGuidelineRangeIsBetweenRange = true;	
		double[] finalRoundingDigits;
		//Differentiate between within range or single range (10-15% above or above, >10-15% or >10%) 
		//>10%, >, <10%, < -- If any of the Price Point is not available
		if(brandGuidelineRange.getStartVal() == Constants.DEFAULT_NA || brandGuidelineRange.getEndVal() == Constants.DEFAULT_NA){
			isBrandGuidelineRangeIsBetweenRange = false;
		//If both the price points are same	
		}else if (brandGuidelineRange.getStartVal() == brandGuidelineRange.getEndVal()){
			isBrandGuidelineRangeIsBetweenRange = false;
		}			
		
		//Get Rounding Digit from the input range
		finalRoundingDigits = rConstraintDTO.getRange(inputRange.getStartVal(), inputRange.getEndVal());
		
		//Rounding Digit is not found from the input range
		if(finalRoundingDigits.length == 0){
			if(isBrandGuidelineRangeIsBetweenRange){
				//Get all rounding digits within the brand price range
				double[] brandRoudingDigits;
				brandRoudingDigits = rConstraintDTO.getRange(brandGuidelineRange.getStartVal(), brandGuidelineRange.getEndVal());
				double closerRoundingDigit;
				
				//If the brand price range has rounding digits
				if(brandRoudingDigits.length > 0){
					//Pick a rounding digit which is closer to end point of input range
					closerRoundingDigit = getClosestRoundingDigit(brandRoudingDigits, inputRange.getEndVal());					
				}else {
					//pick a rounding price point which is closer to brand start/end price point based on operator type
					//Get next price point
					if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(storeBrandRelationOperator)
							|| PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(storeBrandRelationOperator)
							|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(storeBrandRelationOperator)) {
						closerRoundingDigit = rConstraintDTO.roundupPriceToNextRoundingDigit(brandGuidelineRange.getStartVal());
					}
					//get previous price point
					else {
						closerRoundingDigit = rConstraintDTO.roundupPriceToPreviousRoundingDigit(brandGuidelineRange.getEndVal());
					}		
				}	
				
				finalRoundingDigits = new double[1];
				finalRoundingDigits[0] = closerRoundingDigit;
				
			}
			//Get rounding digit from brand price range
			else{
				//Find price point from which previous / next rounding digit to be found
				double inputPricePoint, outputPricePoint;
				//When it comes here, both the price points will be same or any one price point is available
				if(brandGuidelineRange.getStartVal() != Constants.DEFAULT_NA){
					inputPricePoint = brandGuidelineRange.getStartVal();
				}
				else{
					inputPricePoint = brandGuidelineRange.getEndVal();
				}
				
				//Get next price point
				if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(storeBrandRelationOperator)
						|| PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(storeBrandRelationOperator)
						|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(storeBrandRelationOperator)) {
					outputPricePoint = rConstraintDTO.roundupPriceToNextRoundingDigit(inputPricePoint);
				}
				//get previous price point
				else {
					outputPricePoint = rConstraintDTO.roundupPriceToPreviousRoundingDigit(inputPricePoint);
				}		
				finalRoundingDigits = new double[1];
				finalRoundingDigits[0] = outputPricePoint;
			}			
		}
		
		return finalRoundingDigits;
	}
	
	//Find the closest rounding digit of the input price point
	private double getClosestRoundingDigit(double[] roundingDigits, double inputPricePoint){
//		DecimalFormat format = new DecimalFormat("#########.00");
		double closestDiff = 0;
		double closestPrice = 0;
		boolean first = true;
		
		for(double price : roundingDigits){
			if(first){
				closestDiff = Math.abs(inputPricePoint - price);
				closestPrice = price;
				first = false;
			}else{
				double diff = Math.abs(inputPricePoint - price);
				if(diff < closestDiff){
					closestDiff = diff;
					closestPrice = price;
				}
			}
		}
		
		if(!first)
			return closestPrice;
		else
			return inputPricePoint;
	}
	
	public double[] getRangeWithinBrandRange(PRRange inputRange, PRRange brandRange, PRConstraintRounding rConstraintDTO){
		
		//Input is within store brand and store brand start and end is not same and rounding digit found
		//get closest price of input range
		
		double[] finalRoundingDigits = new double[0];
		//If any of the price point is not available
		if(brandRange.getStartVal() == Constants.DEFAULT_NA || brandRange.getEndVal() == Constants.DEFAULT_NA){
			finalRoundingDigits = new double[0];
		//If both the price points are same	
		}else if (brandRange.getStartVal() == brandRange.getEndVal()){
			finalRoundingDigits = new double[0];
		}else {
			//See if input range is within brand range
			boolean isInputRangeWithinBrand = false;
			
			if(inputRange.getStartVal() != Constants.DEFAULT_NA && inputRange.getEndVal() != Constants.DEFAULT_NA){
				if(inputRange.getStartVal() >= brandRange.getStartVal() && inputRange.getEndVal() <= brandRange.getEndVal()){
					isInputRangeWithinBrand = true;
				}				
			}else if(inputRange.getStartVal() != Constants.DEFAULT_NA){
				if(inputRange.getStartVal() >= brandRange.getStartVal() && inputRange.getStartVal() <= brandRange.getEndVal()){
					isInputRangeWithinBrand = true;
				}	
			}else if(inputRange.getEndVal() != Constants.DEFAULT_NA){
				if(inputRange.getEndVal() >= brandRange.getStartVal() && inputRange.getEndVal() <= brandRange.getEndVal()){
					isInputRangeWithinBrand = true;
				}
			}
			
			if (isInputRangeWithinBrand) {				
				//Get Rounding Digit from the input range
				finalRoundingDigits = rConstraintDTO.getRange(inputRange.getStartVal(), inputRange.getEndVal(), "NO_CLOSEST");
				
				if(finalRoundingDigits.length == 0) {
					double[] brandRoudingDigits;
					brandRoudingDigits = rConstraintDTO.getRange(brandRange.getStartVal(), brandRange.getEndVal());
					double closerRoundingDigit;
	
					// If the brand price range has rounding digits
					if (brandRoudingDigits.length > 0) {
						// Pick a rounding digit which is closer to end point of input range
						closerRoundingDigit = getClosestRoundingDigit(brandRoudingDigits, inputRange.getEndVal());
						finalRoundingDigits = new double[1];
						finalRoundingDigits[0] = closerRoundingDigit;
					} else {
						finalRoundingDigits = new double[0];
					}
				}
			} else {
				finalRoundingDigits = new double[0];
			}
		}
		
		
		return finalRoundingDigits;
	}
}
