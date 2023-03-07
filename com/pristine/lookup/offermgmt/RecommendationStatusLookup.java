package com.pristine.lookup.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum RecommendationStatusLookup {
	
	RECOMMENDED(1), REVIEW_IN_PROGRESS(2), REVIEW_COMPLETED(3), REVIEW_REJECTED(4), REC_ERROR(5), APPROVED(
			6), EXPORTED_PARTIALLY(7), EXPORTED(8), EFFECTIVE_DATE_UPDATE(9), EMERGENCY_APPROVED(10);

	private final int statusId;

	RecommendationStatusLookup(int statusId) {
		this.statusId = statusId;
	}

	public int getStatusId() {
		return statusId;
	}

	// Lookup table
	private static final Map<Integer, RecommendationStatusLookup> lookup = new HashMap<Integer, RecommendationStatusLookup>();

	// Populate the lookup table on loading time
	static {
		for (RecommendationStatusLookup s : EnumSet.allOf(RecommendationStatusLookup.class))
			lookup.put(s.getStatusId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static RecommendationStatusLookup get(int statusId) {
		return lookup.get(statusId);
	}
}
