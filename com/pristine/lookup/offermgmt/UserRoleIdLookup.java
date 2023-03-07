package com.pristine.lookup.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum UserRoleIdLookup {
	
	PRICING_DIRECTOR (1), PRICING_MANAGER (2), PRISTINE_SYSTEM_ADMINISTRATOR (5), 
	PRESTO_APPLICATION_ADMINISTRATOR (6), PRICING_ANALYST (7), CATEGORY_MANAGER (8), 
	EXECUTIVE_MANAGEMENT (3), ASSOCIATE_CATEGORY_MANAGER (9), DIRECTOR (10), VICE_PRESIDENT (11), 
	SENIOR_VICE_PRESIDENT (12), EXECUTIVE_VICE_PRESIDENT (13), PM_DIRECTOR (14), PM_VICE_PRESIDENT (15);

	private final int roleId;

	UserRoleIdLookup(int roleId) {
		this.roleId = roleId;
	}

	public int getRoleId() {
		return roleId;
	}

	// Lookup table
	private static final Map<Integer, UserRoleIdLookup> lookup = new HashMap<Integer, UserRoleIdLookup>();

	// Populate the lookup table on loading time
	static {
		for (UserRoleIdLookup s : EnumSet.allOf(UserRoleIdLookup.class))
			lookup.put(s.getRoleId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static UserRoleIdLookup get(int roleId) {
		return lookup.get(roleId);
	}
}
