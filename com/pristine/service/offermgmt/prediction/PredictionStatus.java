package com.pristine.service.offermgmt.prediction;

import java.util.*;

public enum PredictionStatus {
	
	/*
	 *  -1 - Prediction App Exception
	 *  0 - successful prediction
	 * 	1 - No movement data for any UPC/categoryId passed
	 * 	2 - No price data for any UPC/categoryId passed
	 * 	3 - No movement data for specific UPC
	 * 	4 - No price data for specific UPC
	 * 	5 - Other error in data preparation
	 * 	6 - Model not present
	 *  8 - No recent movement (LIR not moved recently)
	 *  9 -  Shipper Item
     *  10 -  New Item
	 * 	20 - Error in code before calculating Curve Averages
	 * 	21 - Error while calculating CurveAverage1
	 * 	22 - Error while calculating CurveAverage2
	 * 	23 - Error while computing Hierarchy
	 * 	24 - Error while scoring
	 * 	30 - Other misc Error
	 *  31 - No substitution impact on the impacted item
	 *  32 - More than one LIG corresponding to main items
	 * */
	
	
	UNDEFINED(-99),
	PREDICTION_APP_EXCEPTION(-1),
    SUCCESS(0),    
    ERROR_NO_MOV_DATA_ANY_UPC(1),
    ERROR_NO_PRICE_DATA_ANY_UPC(2),
    ERROR_NO_MOV_DATA_SPECIFIC_UPC(3),
    ERROR_NO_PRICE_DATA_SPECIFIC_UPC(4),
    ERROR_DATA_PREPERATION(5),
    ERROR_MODEL_UNAVAILABLE(6),
    NO_RECENT_MOVEMENT(8),
    SHIPPER_ITEM(9),
    NEW_ITEM(10),
    ERROR_CAL_CURVE_AVERAGE(20),
    ERROR_CAL_CURVE_AVERAGE_1(21),
    ERROR_CAL_CURVE_AVERAGE_2(22),
    ERROR_COMPUTING_HIERARCHY(23),
    ERROR_SCORING(24),
    ERROR_MISC(30),
	ERROR_NO_SUBST_IMPACT(31),
	ERROR_MORE_THAN_ONE_LIG_SUBST_ANALYSIS(32);
    
    /*ERROR_NO_HISTORY(1), 
    ERROR_MODEL_UNAVAILABLE(2), 
    ERROR_PREDICTION(9);*/
    
    private final int statusCode;
    
	// Lookup table
	private static final Map<Integer,PredictionStatus> lookup = new HashMap<Integer,PredictionStatus>();

	// Populate the lookup table on loading time
	static {
		for (PredictionStatus s : EnumSet.allOf(PredictionStatus.class))
			lookup.put(s.getStatusCode(), s);
	}    
    PredictionStatus(int statusCode)
    {
    	this.statusCode = statusCode; 
    }
    
    public int getStatusCode()
    {
    	return statusCode;
    }
    
	// This method can be used for reverse lookup purpose
	public static PredictionStatus get(int statusCode) {
		return lookup.get(statusCode) != null ? lookup.get(statusCode) :PredictionStatus.UNDEFINED;
	}    
}
