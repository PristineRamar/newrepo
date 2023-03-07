package com.pristine.util;

public class BusinessUtil {
	
	public BusinessUtil() {
		// TODO Auto-generated constructor stub
	}
	
	
	public double getSimplePriceIndex(double basePrice, int baseMultiple, 
									double compPrice, int compMultiple){
		double priceIndex=0;
		
		if (basePrice > 0 && compPrice > 0){
			String PICalcType = PropertyManager.getProperty("PI_CALC_TYPE", "");
		
			double unitBasePrice = getUnitPrice(basePrice, baseMultiple);
			double unitCompPrice = getUnitPrice(compPrice, compMultiple);
		
			//#NORMAL OR REVERSE
			if (PICalcType.equals(Constants.PI_CALC_TYPE_REVERSE)) {
				priceIndex = unitBasePrice / unitCompPrice * 100;
			}
			else {
				priceIndex = unitCompPrice /unitBasePrice  * 100;
			}
		}

		return priceIndex;
	}

	public double getUnitPrice(double price, int multipe){
		double unitPrice = 0;
		
		if (price > 0  && multipe > 0){
			if (multipe > 1)
				unitPrice = price / multipe;
			else
				unitPrice = price;
		}
		
		return unitPrice;
	}


}
