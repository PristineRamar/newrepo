package com.pristine.service.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ObjectiveTypeLookup {

	LOWEST_PRICE_TO_CUSTOMER(1), 
	HIGHEST_MARGIN_DOLLAR(2), 
	HIGHEST_REVENUE(4), 
	MINIMUM_REVENUE_DOLLAR(8), 
	MINIMUM_MARGIN_DOLLAR(9), 
	USE_GUIDELINES_AND_CONSTRAINTS(13), 
	HIGHEST_MARGIN_DOLLAR_USING_CURRENT_RETAIL(14), 
	HIGHEST_MARGIN_DOLLAR_MAINTAIN_CUR_SALE(15), 
	HIGHEST_MARGIN_DOLLAR_MAINTAIN_CUR_MOV(16), 
	HIGHEST_MOV_MAINTAIN_CUR_MARGIN_DOLLAR(17);

	private final int objectiveTypeId;

	ObjectiveTypeLookup(int objectiveTypeId) {
		this.objectiveTypeId = objectiveTypeId;
	}

	public int getObjectiveTypeId() {
		return objectiveTypeId;
	}

	// Lookup table
	private static final Map<Integer, ObjectiveTypeLookup> lookup = new HashMap<Integer, ObjectiveTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (ObjectiveTypeLookup s : EnumSet.allOf(ObjectiveTypeLookup.class))
			lookup.put(s.getObjectiveTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static ObjectiveTypeLookup get(int objectiveTypeId) {
		return lookup.get(objectiveTypeId);
	}
}
