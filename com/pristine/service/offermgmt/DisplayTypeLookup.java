package com.pristine.service.offermgmt;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum DisplayTypeLookup {
	/*	1-FAST WALL, 2-LOBBY, 3-SHIPPER, 4-END, 5-OFF SHELF, 6-WING, 7-BREAD TABLE, 8-JUMP
	9-MOD, 10-JUMPSHELF, 11-COMBO END, 12-MASS, 13-END-DSD */
	
	NONE(0), FAST_WALL(1), LOBBY(2), SHIPPER(3), END(4), OFF_SHELF(5), WING(6), BREAD_TABLE(7), JUMP(8), MOD(9), JUMP_SHELF(10),
	COMBO_END(11), MASS(12), END_DSD(13);
	 
	private final int displayTypeId;

	DisplayTypeLookup(int displayTypeId) {
		this.displayTypeId = displayTypeId;
	}

	public int getDisplayTypeId() {
		return displayTypeId;
	}

	// Lookup table
	private static final Map<Integer, DisplayTypeLookup> lookup = new HashMap<Integer, DisplayTypeLookup>();

	// Populate the lookup table on loading time
	static {
		for (DisplayTypeLookup s : EnumSet.allOf(DisplayTypeLookup.class))
			lookup.put(s.getDisplayTypeId(), s);
	}

	// This method can be used for reverse lookup purpose
	public static DisplayTypeLookup get(int displayTypeId) {
		return lookup.get(displayTypeId);
	}
}
