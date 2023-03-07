package com.pristine.service.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum GuidelineTypeLookup {

	MARGIN(1), PRICE_INDEX(2), COMPETITION(3), BRAND(4), SIZE(5), LEAD_ZONE(7);

	private final int guielineTypeId;	 

	GuidelineTypeLookup(int guielineTypeId) {
		this.guielineTypeId = guielineTypeId;
	}

	public int getGuidelineTypeId() {
		return guielineTypeId;
	}	 
	
	// Lookup table
	private static final Map<Integer, GuidelineTypeLookup> lookup = new HashMap<Integer, GuidelineTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (GuidelineTypeLookup s : EnumSet.allOf(GuidelineTypeLookup.class))
			lookup.put(s.getGuidelineTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static GuidelineTypeLookup get(int guielineTypeId) {
		return lookup.get(guielineTypeId);
	}
}
 
