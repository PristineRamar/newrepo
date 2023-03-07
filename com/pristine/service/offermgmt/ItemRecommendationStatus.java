package com.pristine.service.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ItemRecommendationStatus {

	UNDEFINED(-99), SUCCESS(0), GENERAL_FAILURE(1), NO_STRATEGY(2), CUR_RETAIL_UNAVAILABLE(3), CUR_COST_UNAVAILABLE(4),
	ITEM_NOT_CONSIDERED_FOR_RECOMMENDATION(5), NO_LEAD_ZONE_PRICE(6);

	private final int statusCode;

	// Lookup table
	private static final Map<Integer, ItemRecommendationStatus> lookup = new HashMap<Integer, ItemRecommendationStatus>();

	// Populate the lookup table on loading time
	static {
		for (ItemRecommendationStatus s : EnumSet.allOf(ItemRecommendationStatus.class))
			lookup.put(s.getStatusCode(), s);
	}

	ItemRecommendationStatus(int statusCode) {
		this.statusCode = statusCode;
	}

	public int getStatusCode() {
		return statusCode;
	}

	// This method can be used for reverse lookup purpose
	public static ItemRecommendationStatus get(int statusCode) {
		return lookup.get(statusCode) != null ? lookup.get(statusCode) : ItemRecommendationStatus.UNDEFINED;
	}
}
