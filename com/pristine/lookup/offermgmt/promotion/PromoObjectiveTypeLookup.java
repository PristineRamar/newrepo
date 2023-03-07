package com.pristine.lookup.offermgmt.promotion;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum PromoObjectiveTypeLookup {
	NONE(0), MAXIMIZE_UNITS(4), MAXIMIZE_SALES(2), MAXIMIZE_MARGIN(3), MAXIMIZE_SALES_WHILE_MAINTINING_MARGIN(1);

	private final int objectiveTypeId;

	PromoObjectiveTypeLookup(int objectiveTypeId) {
		this.objectiveTypeId = objectiveTypeId;
	}

	public int getObjectiveTypeId() {
		return objectiveTypeId;
	}

	// Lookup table
	private static final Map<Integer, PromoObjectiveTypeLookup> lookup = new HashMap<Integer, PromoObjectiveTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (PromoObjectiveTypeLookup s : EnumSet.allOf(PromoObjectiveTypeLookup.class))
			lookup.put(s.getObjectiveTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static PromoObjectiveTypeLookup get(int objectiveTypeId) {
		return lookup.get(objectiveTypeId);
	}
}
