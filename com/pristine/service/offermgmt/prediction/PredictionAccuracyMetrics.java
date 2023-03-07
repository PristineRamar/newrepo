package com.pristine.service.offermgmt.prediction;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum PredictionAccuracyMetrics {

	UNDEFINED(-99),
	NO_ITEMS(102),
	IGNORED_NO(225),
	ACTUAL_MOV_ZERO_NO(226),
	ACTUAL_MOV_GT_5000_NO(227),
	ACTUAL_MOV_LT_500_NO(228),		
	RMSE(121),
	RMSE_HIGHER_MOVER(122),
	RMSE_SLOWER_MOVER(123),
	AVG_ERR_PCT(141),
	AVG_ERR_PCT_HIGHER_MOVER(142),
	AVG_ERR_PCT_SLOWER_MOVER(143),
	FORECAST_CLOSER_NO(161),
	FORECAST_CLOSER_PCT(162),
	FORECAST_HIGHER_NO(163),
	FORECAST_HIGHER_PCT(164),
	FORECAST_HIGHER_AVG_PCT(165),
	FORECAST_LOWER_NO(166),
	FORECAST_LOWER_PCT(167),
	FORECAST_LOWER_AVG_PCT(168),
	FORECAST_GT_3_TIMES_OF_ACTUAL_NO(169),
	FORECAST_GT_3_TIMES_OF_ACTUAL_PCT(170),
	FORECAST_LT_ONE_THIRD_OF_ACTUAL_NO(171),
	FORECAST_LT_ONE_THIRD_OF_ACTUAL_PCT(172),
	FORECAST_WITHIN_25_PCT_NO(173),
	FORECAST_WITHIN_25_PCT_PCT(174),	
	FORECAST_INACCURATE_MAX_PCT(175),
	BOTH_FORECAST_HIGHER_NO(176),
	BOTH_FORECAST_HIGHER_PCT(177),
	BOTH_FORECAST_LOWER_NO(178),
	BOTH_FORECAST_LOWER_PCT(179),
	WITHOUT_FORECAST_NO(180),	
	WITHOUT_FORECAST_PCT(215),	
	CLIENT_TOP_INACCURATE_NAME(201),
	CLIENT_TOP_INACCURATE_FORECAST(203),
	CLIENT_TOP_INACCURATE_ACTUAL(204),
	PRESTO_TOP_INACCURATE_NAME(211),
	PRESTO_TOP_INACCURATE_FORECAST(212),
	PRESTO_TOP_INACCURATE_ACTUAL(214);
	 
    private final int metricsId;
    
	// Lookup table
	private static final Map<Integer,PredictionAccuracyMetrics> lookup = new HashMap<Integer,PredictionAccuracyMetrics>();

	// Populate the lookup table on loading time
	static {
		for (PredictionAccuracyMetrics s : EnumSet.allOf(PredictionAccuracyMetrics.class))
			lookup.put(s.getMetricsId(), s);
	}    
	
	PredictionAccuracyMetrics(int metricsId)
    {
    	this.metricsId = metricsId; 
    }
    
    public int getMetricsId()
    {
    	return metricsId;
    }
    
	// This method can be used for reverse lookup purpose
	public static PredictionAccuracyMetrics get(int statusCode) {
		return lookup.get(statusCode) != null ? lookup.get(statusCode) :PredictionAccuracyMetrics.UNDEFINED;
	}    
}
