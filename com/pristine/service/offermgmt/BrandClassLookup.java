package com.pristine.service.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum BrandClassLookup {

	STORE(1), NATIONAL(2);

	private final int brandClassId;	 

	BrandClassLookup(int brandClassId) {
		this.brandClassId = brandClassId;
	}

	public int getBrandClassId() {
		return brandClassId;
	}	 
	
	// Lookup table
	private static final Map<Integer, BrandClassLookup> lookup = new HashMap<Integer, BrandClassLookup>();

	// Populate the lookup table on loading time
	static {
		for (BrandClassLookup s : EnumSet.allOf(BrandClassLookup.class))
			lookup.put(s.getBrandClassId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static BrandClassLookup get(int brandClassId) {
		return lookup.get(brandClassId);
	}
}
