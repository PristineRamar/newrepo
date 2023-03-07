package com.pristine.lookup;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum AuditTrailTypeLookup {

	RECOMMENDATION(1),
	RE_RECOMMENDATION(11),
	CLEARANCE_PRICE_EXPORT(12);

	private final int auditTrailTypeId;

	AuditTrailTypeLookup(int auditTrailTypeId) {
		this.auditTrailTypeId = auditTrailTypeId;
	}

	public int getAuditTrailTypeId() {
		return auditTrailTypeId;
	}

	// Lookup table
	private static final Map<Integer, AuditTrailTypeLookup> lookup = new HashMap<Integer, AuditTrailTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (AuditTrailTypeLookup s : EnumSet.allOf(AuditTrailTypeLookup.class))
			lookup.put(s.getAuditTrailTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static AuditTrailTypeLookup get(int auditTrailTypeId) {
		return lookup.get(auditTrailTypeId);
	}

}
