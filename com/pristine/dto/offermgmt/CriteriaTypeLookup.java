package com.pristine.dto.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
public enum CriteriaTypeLookup {

	FAMILY_UNIT_SALES(1), ITEM_UNIT_SALES(2), COST(3), ITEM_SETUP(4), PLC(5), 
	STOCKING_STATUS(6), COVERAGE_OR_CHOICE(7), DFM(8), 
	STORE_COUNT(9),TIER(10), ITEM_STATUS(11), LIG_UNIT_SALES(12), PRICE(13),NIPO_COST(14);
	
	private final int criteriaTypeId;
	
	/*
	1	FAMILY UNIT SALES
	2	ITEM UNIT SALES
	3	COST
	4	ITEM SET UP
	5	PLC
	6	STOCKING STATUS
	7	COVERAGE / CHOICE
	8	DISCRETIONARY-FAILURE-MAINTENANCE
	9	STORE COUNT
	10 TIER 
	*/
	
	CriteriaTypeLookup(int criteriaTypeId) {
		this.criteriaTypeId = criteriaTypeId;
	}

	public int getCriteriaTypeId() {
		return criteriaTypeId;
	}

	// Lookup table
	private static final Map<Integer, CriteriaTypeLookup> lookup = new HashMap<Integer, CriteriaTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (CriteriaTypeLookup s : EnumSet.allOf(CriteriaTypeLookup.class))
			lookup.put(s.getCriteriaTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static CriteriaTypeLookup get(int criteriaTypeId) {
		return lookup.get(criteriaTypeId);
	}
}
