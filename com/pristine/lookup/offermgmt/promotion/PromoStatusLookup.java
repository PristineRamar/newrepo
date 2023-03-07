package com.pristine.lookup.offermgmt.promotion;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum PromoStatusLookup {
	
	IN_PROGRESS(1), WAITING_FOR_APPROVAL(2), APPROVED(3), REJECTED(4), TO_BE_REVIEWED(5), 
	PROCESSED(6), MODIFY(7), RETURNED(8), WAITING_FOR_MERCHANT_APPROVAL(9), RETURNED_FROM_PROCESSED(10), RETURNED_FROM_CM(11), UNKNOWN(12);

	private final int promoStatusId;

	PromoStatusLookup(int promoStatusId) {
		this.promoStatusId = promoStatusId;
	}

	public int getPromoStatusId() {
		return promoStatusId;
	}

	// Lookup table
	private static final Map<Integer, PromoStatusLookup> lookup = new HashMap<Integer, PromoStatusLookup>();

	// Populate the lookup table on loading time
	static {
		for (PromoStatusLookup s : EnumSet.allOf(PromoStatusLookup.class))
			lookup.put(s.getPromoStatusId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static PromoStatusLookup get(int promoStausId) {
		return lookup.get(promoStausId);
	}
}
