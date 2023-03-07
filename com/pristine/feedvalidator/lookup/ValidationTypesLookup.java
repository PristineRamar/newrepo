package com.pristine.feedvalidator.lookup;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ValidationTypesLookup {
	stringType("STRING"),
	doubleType("DOUBLE"),
	dateType("DATE"),
	numberType("NUMBER"),
	isMandatory("MANDATORY"),
	validatePrestoData("VALIDATE_PRESTO_DATA"),
	costStatusCode("COST_STATUS_CODE"),
	allowanceStatusCode("ALLOW_STATUS_CODE"),
	bannerCode("BNR_CODE"),
	yesOrNoFlag("YES_OR_NO_FLAG"),
	calWeekType("WEEK_CALENDAR");
	
	private final String validationType;

	ValidationTypesLookup(String validationType) {
		this.validationType = validationType;
	}
	
	public String getValidationType() {
		return validationType;
	}

	// Lookup table
	private static final Map<String, ValidationTypesLookup> lookup = new HashMap<>();

	// Populate the lookup table on loading time
	static {
		for (ValidationTypesLookup s : EnumSet.allOf(ValidationTypesLookup.class))
			lookup.put(s.getValidationType(), s);
	}

	// This method can be used for reverse lookup purpose
	public static ValidationTypesLookup get(String propertyName) {
		return lookup.get(propertyName);
	}
}
