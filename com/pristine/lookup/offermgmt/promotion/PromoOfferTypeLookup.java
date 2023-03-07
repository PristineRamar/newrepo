package com.pristine.lookup.offermgmt.promotion;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum PromoOfferTypeLookup {
	FREE_ITEM(1), REWARD_POINTS(2), PLUS_UP_REWARD(3), COUPON(4), GAS_POINTS(5), INSTANT_SAVINGS(6), OFF(7), DIGITAL_COUPON(8);

	private final int promoOfferTypeId;

	PromoOfferTypeLookup(int promoOfferTypeId) {
		this.promoOfferTypeId = promoOfferTypeId;
	}

	public int getPromoOfferTypeId() {
		return promoOfferTypeId;
	}

	// Lookup table
	private static final Map<Integer, PromoOfferTypeLookup> lookup = new HashMap<Integer, PromoOfferTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (PromoOfferTypeLookup s : EnumSet.allOf(PromoOfferTypeLookup.class))
			lookup.put(s.getPromoOfferTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static PromoOfferTypeLookup get(int promoOfferTypeId) {
		return lookup.get(promoOfferTypeId);
	}
}
