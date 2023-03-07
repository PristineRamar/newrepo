package com.pristine.lookup;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum AuditTrailStatusLookup {

	
	AUDIT_TYPE(3),
	AUDIT_SUB_TYPE_RECC (301),
	AUDIT_SUB_TYPE_ERROR (312),
	AUDIT_SUB_TYPE_UPDATE_RECC (313),
	AUDIT_SUB_TYPE_EXPORT(310),
	SUB_STATUS_TYPE_RECC(1),
	SUB_STATUS_TYPE_ERROR (5),
	SUB_STATUS_TYPE_UPDATE_RECC(11),
	SUB_STATUS_TYPE_PARTIAL_EXPORT(7),
	SUB_STATUS_TYPE_EXPORT(8),
	SUB_STATUS_TYPE_A_RECORDS(18),
	SUB_STATUS_TYPE_D_RECORDS(19);
	

	private final int auditTrailTypeId;

	AuditTrailStatusLookup(int auditTrailTypeId) {
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
