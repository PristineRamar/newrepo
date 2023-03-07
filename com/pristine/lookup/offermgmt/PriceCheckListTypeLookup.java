package com.pristine.lookup.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum PriceCheckListTypeLookup {
	KVI(1),
	K2(2),
	SUPER_KVI(3),
	VT(4),
	WIC(5),
	HIGH_IMPACT(11),
	LOW_IMPACT(12),
	PROMOTED(13);
	
	private final int checkListTypeLookupId;

	PriceCheckListTypeLookup(int checkListTypeLookupId) {
		this.checkListTypeLookupId = checkListTypeLookupId;
	}

	public int getCheckListTypeLookupId() {
		return checkListTypeLookupId;
	}

	// Lookup table
	private static final Map<Integer, PriceCheckListTypeLookup> lookup = new HashMap<Integer, PriceCheckListTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (PriceCheckListTypeLookup s : EnumSet.allOf(PriceCheckListTypeLookup.class))
			lookup.put(s.getCheckListTypeLookupId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static PriceCheckListTypeLookup get(int checkListTypeLookupId) {
		return lookup.get(checkListTypeLookupId);
	}
}
