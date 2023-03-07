package com.pristine.feedvalidator.lookup;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import com.pristine.lookup.offermgmt.ExplainRetailNoteTypeLookup;

public enum PropertyNameLookup {
	
	UPC("UPC"),
	WHITEM_NO("WHITEM_NO"),
	STRT_DTE("STRT_DTE"),
	CST_ZONE_NO("CST_ZONE_NO"),
	SPLR_NO("SPLR_NO"),
	CST_STAT_CD("CST_STAT_CD"),
	BS_CST_AKA_STORE_CST("BS_CST_AKA_STORE_CST"),
	DLVD_CST_AKA_WHSE_CST("DLVD_CST_AKA_WHSE_CST"),
	LONG_TERM_REFLECT_FG("LONG_TERM_REFLECT_FG"),
	BNR_CD("BNR_CD");
	
	private final String propertyNameValue;

	PropertyNameLookup(String propertyNameValue) {
		this.propertyNameValue = propertyNameValue;
	}
	
	public String getPropertyNameValue() {
		return propertyNameValue;
	}

	// Lookup table
	private static final Map<String, PropertyNameLookup> lookup = new HashMap<>();

	// Populate the lookup table on loading time
	static {
		for (PropertyNameLookup s : EnumSet.allOf(PropertyNameLookup.class))
			lookup.put(s.getPropertyNameValue(), s);
	}

	// This method can be used for reverse lookup purpose
	public static PropertyNameLookup get(String propertyName) {
		return lookup.get(propertyName);
	}
}
