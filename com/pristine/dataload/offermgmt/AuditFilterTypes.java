package com.pristine.dataload.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum AuditFilterTypes {
	
	RETAILS_CHANGED(1), RETAIL_INCREASED(2), RETAIL_DECREASED(3), RETAILS_OVERIDDEN(4), MANAGER_REVIEW(5),
	OUT_OF_NORM(6), LOWER_SIZES_NOT_PRICED_LOWER(7), SIZE_REL_VIOL(8), RETAILS_LT_LC(9), RETAILS_LT_LC_NO_PROMO(10),
	RETAILS_LT_PROMO(11), MARGIN_LT_X_PCT(12), MARGIN_RT_X_PCT(13), RETAILS_LT_X_PCT_COMP(14), RETAILS_GT_X_PCT_COMP(15),
	RETAIL_CHANGE_LT_X_PCT(16), RETAIL_CHANGE_GT_X_PCT(17), RETAILS_TO_REDUCE_MAR_OPP(18), ZERO_CURR_RETAILS(19), ZERO_COST(20),
	COST_CAHNGE_GT_X_PCT(21), RETAIL_CHANGE_X_TIMES(22), LINE_PRICE_VIOLATIONS(23), KVI_WITH_COMP_X_MONTHS_OLD(24), KVI_WITH_NO_COMP(25),
	ENDING_DIGIT_VIOL(26), RETAILS_COMP_VIOLATION(27), MARGIN_VIOLATION(28), RETAILS_CHANGE_VIOLATION(29), BRAND_VIOLATION(30), SIZE_VIOLATION(31);
	
	private final int paramId;	 

	AuditFilterTypes(int paramId) {
		this.paramId = paramId;
	}

	public int getAuditParamId() {
		return paramId;
	}	 
	
	// Lookup table
	private static final Map<Integer, AuditFilterTypes> lookup = new HashMap<Integer, AuditFilterTypes>();

	// Populate the lookup table on loading time
	static {
		for (AuditFilterTypes s : EnumSet.allOf(AuditFilterTypes.class))
			lookup.put(s.getAuditParamId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static AuditFilterTypes get(int paramId) {
		return lookup.get(paramId);
	}
}
