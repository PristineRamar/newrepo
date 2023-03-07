package com.pristine.lookup.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum PriceTestStatusLookUp {
	REQUESTED(1), DATA_AGGREGATION_INPROGRESS(2), DATA_AGGREGATION_COMPLETE(3), MODEL_BUILDING_INPROGRESS(4),
	MODEL_BUILDING_COMPLETED(5), RECOMMENDATION_IN_PROGRESS(6), RECOMMENDATION_IN_COMPLETED(7), ERROR_IN_IMS(9),
	ERROR_IN_MODEL_BUILDING(10), ERROR_IN_RECOMMENDATION(11), NO_RECENT_MOVEMENT(12);

	private final int priceTestTypeLookupId;

	PriceTestStatusLookUp(int checkListTypeLookupId) {
		this.priceTestTypeLookupId = checkListTypeLookupId;
	}

	public int getPriceTestTypeLookupId() {
		return priceTestTypeLookupId;
	}

	// Lookup table
	private static final Map<Integer, PriceTestStatusLookUp> lookup = new HashMap<Integer, PriceTestStatusLookUp>();

	// Populate the lookup table on loading time
	static {
		for (PriceTestStatusLookUp s : EnumSet.allOf(PriceTestStatusLookUp.class))
			lookup.put(s.getPriceTestTypeLookupId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static PriceTestStatusLookUp get(int priceTestLookUpID) {
		return lookup.get(priceTestLookUpID);
	}
}
