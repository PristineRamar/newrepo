package com.pristine.lookup.offermgmt.promotion;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum AdObjectiveLookup {
	NONE(0), HIGHEST_SALES(1), HIGHEST_SALES_WITH_MIN_MARGIN(3), MAXIMIZE_SALES_WITHOUT_REDUCING_MARGIN(4);
	
	private final int objectiveTypeId;

	AdObjectiveLookup(int objectiveTypeId) {
		this.objectiveTypeId = objectiveTypeId;
	}

	public int getObjectiveTypeId() {
		return objectiveTypeId;
	}

	// Lookup table
	private static final Map<Integer, AdObjectiveLookup> lookup = new HashMap<Integer, AdObjectiveLookup>();

	// Populate the lookup table on loading time
	static {
		for (AdObjectiveLookup s : EnumSet.allOf(AdObjectiveLookup.class))
			lookup.put(s.getObjectiveTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static AdObjectiveLookup get(int objectiveTypeId) {
		return lookup.get(objectiveTypeId);
	}
}
