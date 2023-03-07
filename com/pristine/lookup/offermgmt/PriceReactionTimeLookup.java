package com.pristine.lookup.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum PriceReactionTimeLookup {

	IMMEDIATE(1), WEEK_BEFORE(2), WEEK_AFTER(3);

	private final int typeId;

	public int getTypeId() {
		return typeId;
	}

	PriceReactionTimeLookup(int typeId) {
		this.typeId = typeId;
	}

	// Lookup table
	private static final Map<Integer, PriceReactionTimeLookup> lookup = new HashMap<Integer, PriceReactionTimeLookup>();

	// Populate the lookup table on loading time
	static {
		for (PriceReactionTimeLookup s : EnumSet.allOf(PriceReactionTimeLookup.class))
			lookup.put(s.getTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static PriceReactionTimeLookup get(int typeId) {
		return lookup.get(typeId);
	}

}
