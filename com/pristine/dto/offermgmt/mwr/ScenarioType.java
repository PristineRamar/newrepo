package com.pristine.dto.offermgmt.mwr;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ScenarioType {

	NONE(0), TOTAL_CURRENT(1), TOTAL_NEW(2), PROMO_CURRENT(3), PROMO_NEW(4), BASE_CURRENT(5), BASE_NEW(6);

	private final int scenarioTypeId;

	ScenarioType(int scenarioTypeId) {
		this.scenarioTypeId = scenarioTypeId;
	}

	public int getScenarioTypeId() {
		return scenarioTypeId;
	}

	// Lookup table
	private static final Map<Integer, ScenarioType> lookup = new HashMap<Integer, ScenarioType>();

	// Populate the lookup table on loading time
	static {
		for (ScenarioType s : EnumSet.allOf(ScenarioType.class))
			lookup.put(s.getScenarioTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static ScenarioType get(int scenarioTypeId) {
		return lookup.get(scenarioTypeId);
	}

}
