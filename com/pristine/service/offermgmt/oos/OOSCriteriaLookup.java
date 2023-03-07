package com.pristine.service.offermgmt.oos;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum OOSCriteriaLookup {

	CRITERIA_1(1), CRITERIA_2(2), CRITERIA_3(3), CRITERIA_4(4), CRITERIA_5(5), CRITERIA_6(6);

	private final int oosCriteriaid;	 

	OOSCriteriaLookup(int oosCriteriaid) {
		this.oosCriteriaid = oosCriteriaid;
	}

	public int getOOSCriteriaId() {
		return oosCriteriaid;
	}	 
	
	// Lookup table
	private static final Map<Integer, OOSCriteriaLookup> lookup = new HashMap<Integer, OOSCriteriaLookup>();

	// Populate the lookup table on loading time
	static {
		for (OOSCriteriaLookup s : EnumSet.allOf(OOSCriteriaLookup.class))
			lookup.put(s.getOOSCriteriaId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static OOSCriteriaLookup get(int oosCriteriaid) {
		return lookup.get(oosCriteriaid);
	}
}