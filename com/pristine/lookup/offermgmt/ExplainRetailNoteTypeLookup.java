package com.pristine.lookup.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ExplainRetailNoteTypeLookup {
	CUR_REG_RETAINED_AS_NO_VALID_PRED(1),
	CUR_REG_RETAINED_LONG_TERM_PROMO(2),
	CUR_REG_RETAINED_NO_MARGIN_BENEFIT(3),
	CUR_REG_RETAINED_PRICE_LESS_THAN_SALE_PRICE(4),
	CUR_REG_RETAINED_RETAIL_RECENTLY_INCREASED(5),
	CUR_REG_RETAINED_RETAIL_RECENTLY_DECREASED(6),
	CUR_REG_RETAINED_EXCEED_HIGHEST_RETAIL(7),
	CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_INCREASE(8),
	CUR_REG_RETAINED_RETAIL_UNCHANGED_CANNOT_DECREASE(9),
	CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_SALE(10),
	CUR_REG_RETAINED_RETAIL_UNCHANGED_NO_NEXT_RANGE(11),
	PP_IGNORED_RECENTLY_INCREASED(12),
	PP_IGNORED_RECENTLY_DECREASED(13),
	PP_IGNORED_CANNOT_INCREASE(14),
	PP_IGNORED_NO_NEXT_RANGE(15),
	PP_IGNORED_CANNOT_DECREASE(16),
	PP_IGNORED_NO_SALE(17),
	PP_IGNORED_EXCEED_HIGHEST_RETAIL(18),
	CUR_REG_RETAINED_UNIT_PRICE_LESSER_DIFFERENCE(19), 
	CUR_REG_RETAINED_NO_MOV_IN_LAST_X_YEARS(20),
	CUR_REG_RETAINED_SINCE_ITEM_IN_TPR(21),
	NO_NEXT_RANGE(22),
	CUR_REG_RETAINED_PROMO_ENDS_AFTER_X_WEEKS(23),
	CUR_REG_RETAINED_RETAIL_CHANGED_WITHIN_X_WEEKS(24),
	RECOMMENDED_PRICE_USING_RULES(25),
	CUR_REG_RETAINED_WHILE_MAXIMIZE_MARGIN_BY_MAINTAINIG_MOV(26),
	CUR_REG_RETAINED_WHILE_MAXIMIZE_MARGIN_DOLLAR(27),
	USER_OVERRIDE_RETAINED(28),
	CUR_REG_RETAINED_FUTURE_RETAIL_CHANGE_IN_X_WEEKS(29),
	AUC_OVERRIDE_PRICE(30),
	MISSING_TIER(31),
	PRICE_POINTS_FILTERED(32),
	COMP_OVERRIDE_PRICE(33),
	BRAND_GUIDELINE(34),
	COMP_OVERRIDE_COST(35),
	CURR_RETAIL_OVERRIDEN_BY_MAP(36),
	PENDING_RETAIL_RECOMMENDED(37),
	CUR_REG_RETAINED_SINCE_ITEM_HAS_FUTURE_PRICE(38),
	CUR_REG_RETAINED_SINCE_ITEM_IS_ON_CLEARNCE(39),
	CUR_REG_RETAINED_SINCE_LIG_HAS_FUTURE_PRICE(40),
	CUR_REG_RETAINED_SINCE_PROMO_STARTS_WITHIN_X_WEEKS(41),
	CUR_REG_RETAINED_SINCE_LIG_PROMO_STARTS_WITHIN_X_WEEKS(42);
	/*
	
	12	Price points <<%>> ignored - Higher than current retail. (Retail increased on %, no further increase in retail)
	13	Price points <<%>> ignored - Higher than current retail. (Retail decreased on %, no further increase in retail)
	14	Price points <<%>> ignored - More than % of current retail. (Current price unchanged last % weeks)
	15	Price points <<%>> ignored - Cross over to next dollar range. (Current price unchanged last % weeks. Retails <$%)
	16	Price points <<%>> ignored - Below the highest sale retail last % weeks. (Current price unchanged last % weeks)
	17	Price points <<%>> ignored - Below % than current retail. (Current price unchanged last % weeks. Never on sale)
	18	Price points <<%>> ignored - Exceeds % of highest reg retail in last % weeks
	22	Price points <<%>> ignored - Cross over to next dollar range
	
	
	*/
	private final int noteTypeLookupId;

	ExplainRetailNoteTypeLookup(int noteTypeLookupId) {
		this.noteTypeLookupId = noteTypeLookupId;
	}

	public int getNoteTypeLookupId() {
		return noteTypeLookupId;
	}

	// Lookup table
	private static final Map<Integer, ExplainRetailNoteTypeLookup> lookup = new HashMap<Integer, ExplainRetailNoteTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (ExplainRetailNoteTypeLookup s : EnumSet.allOf(ExplainRetailNoteTypeLookup.class))
			lookup.put(s.getNoteTypeLookupId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static ExplainRetailNoteTypeLookup get(int noteTypeLookupId) {
		return lookup.get(noteTypeLookupId);
	}
}
