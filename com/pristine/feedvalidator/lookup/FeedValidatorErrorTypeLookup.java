package com.pristine.feedvalidator.lookup;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum FeedValidatorErrorTypeLookup {
	mandatoryStringErrorCode(1),
	mandatoryDoubleErrorCode(2),
	mandatoryDateErrorCode(3),
	mandatoryNumberErrorCode(4),
	doubleParsingErrorCode(5),
	dateParsingErrorCode(6),
	numberParsingErrorCode(7),
	dataNotInPrestoErrorCode(8),
	expectedValuesNotMatchingErrorCode(9);
	
	private final int errorCode;

	FeedValidatorErrorTypeLookup(Integer validationType) {
		this.errorCode = validationType;
	}
	
	public Integer getErrorCode() {
		return errorCode;
	}

	// Lookup table
	private static final Map<Integer, FeedValidatorErrorTypeLookup> lookup = new HashMap<>();

	// Populate the lookup table on loading time
	static {
		for (FeedValidatorErrorTypeLookup s : EnumSet.allOf(FeedValidatorErrorTypeLookup.class))
			lookup.put(s.getErrorCode(), s);
	}

	// This method can be used for reverse lookup purpose
	@SuppressWarnings("unlikely-arg-type")
	public static FeedValidatorErrorTypeLookup get(String propertyName) {
		return lookup.get(propertyName);
	}
}
