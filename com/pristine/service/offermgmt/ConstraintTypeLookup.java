package com.pristine.service.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ConstraintTypeLookup {

	PRE_PRICE(1), LOCKED_PRICE(2), MIN_MAX(3), ROUNDING(4), THRESHOLD(5), NO_OF_PRICE_CHANGES(6), LIG(7), COST(8),
	LOWER_HIGHER(9), GUARD_RAIL(10), FREIGHT(11),MAP(12);

	private final int constraintTypeId;	 

	ConstraintTypeLookup(int constraintTypeId) {
		this.constraintTypeId = constraintTypeId;
	}

	public int getConstraintTypeId() {
		return constraintTypeId;
	}	 
	
	// Lookup table
	private static final Map<Integer, ConstraintTypeLookup> lookup = new HashMap<Integer, ConstraintTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (ConstraintTypeLookup s : EnumSet.allOf(ConstraintTypeLookup.class))
			lookup.put(s.getConstraintTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static ConstraintTypeLookup get(int constraintTypeId) {
		return lookup.get(constraintTypeId);
	}
}
