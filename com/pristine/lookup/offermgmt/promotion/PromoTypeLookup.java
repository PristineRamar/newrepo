package com.pristine.lookup.offermgmt.promotion;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum PromoTypeLookup {
	NONE(0), BOGO(1), BUX_X_GET_Y_SAME(2), MUST_BUY(3), STANDARD(9), BUX_X_GET_Y_DIFF(8), MEAL_DEAL(5), SUPER_COUPON(10), 
	AMOUNT_OFF(11), PICK_5(12), REWARD_PROMO(13), BMSM(106);

	private final int promoTypeId;

	PromoTypeLookup(int promoTypeId) {
		this.promoTypeId = promoTypeId;
	}

	public int getPromoTypeId() {
		return promoTypeId;
	}

	// Lookup table
	private static final Map<Integer, PromoTypeLookup> lookup = new HashMap<Integer, PromoTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (PromoTypeLookup s : EnumSet.allOf(PromoTypeLookup.class))
			lookup.put(s.getPromoTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static PromoTypeLookup get(int promoTypeId) {
		return lookup.get(promoTypeId);
	}
}
