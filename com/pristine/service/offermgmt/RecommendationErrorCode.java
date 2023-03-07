package com.pristine.service.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum RecommendationErrorCode {
	GENERAL_EXCEPTION(99), DB_MULTI_COMPETITOR_DATA(1), DB_PRODUCT_GROUP_PROPERTY(2), PROCESS_ORDER_CODE_REC(3),
	MARK_ITEM_WHOSE_ZONE_STORE_PRICE_SAME(4), DB_GET_ALL_STRATEGIES(5), DB_GET_AUTHORIZED_ITEMS(6),
	DB_GET_STORE_ITEMS(7), POPULATE_LIG(8), POPULATE_STORE_ITEM_MAP(9), FIND_EACH_ITEM_STRATEGY(10),
	DB_GET_STRATEGY_DEFINITION(11), COPY_PRE_LOC_MIN_MAX_TO_STORE(12), PROCESS_ZONE_ITEMS(13) ,PROCESS_STORE_ITEMS(14),
	NO_AUTHORIZED_ITEM_FOUND(15), INFINITE_LOOP(16), FIND_IF_CUR_RETAIL_SAME_ACROSS_STORES(17),
	BALANCING_PI_MARGIN(18), SUBSTITUTION_SERVICE(19), STRATEGY_DELETED(20);

	private final int errorCode;	 

	RecommendationErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public int getErrorCode() {
		return errorCode;
	}	 
	
	// Lookup table
	private static final Map<Integer, RecommendationErrorCode> lookup = new HashMap<Integer, RecommendationErrorCode>();

	// Populate the lookup table on loading time
	static {
		for (RecommendationErrorCode s : EnumSet.allOf(RecommendationErrorCode.class))
			lookup.put(s.getErrorCode(), s);
	}

	// This method can be used for reverse lookup purpose
	public static RecommendationErrorCode get(int errorCode) {
		return lookup.get(errorCode);
	}
}
