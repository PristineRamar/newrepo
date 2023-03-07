package com.pristine.lookup.offermgmt.promotion;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum GEPromoCodeLookup {
	TRUE_BOGO(116), AMOUNT_OFF(117), FREE_ITEM(118), GEAC(1057), PICK_5(2317), GROUP_LEVEL_AMOUNT_OFF(1777), OTHER(0);

	private final int promoCode;

	GEPromoCodeLookup(int promoCode) {
			this.promoCode = promoCode;
		}

	public int getPromoCode() {
		return promoCode;
	}

	// Lookup table
	private static final Map<Integer, GEPromoCodeLookup> lookup = new HashMap<Integer, GEPromoCodeLookup>();

	// Populate the lookup table on loading time
	static {
		for (GEPromoCodeLookup s : EnumSet.allOf(GEPromoCodeLookup.class))
			lookup.put(s.getPromoCode(), s);
	}

	// This method can be used for reverse lookup purpose
	public static GEPromoCodeLookup get(int promoCode) {
		return lookup.get(promoCode);
	}
}
