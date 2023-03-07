package com.pristine.dto.offermgmt;

import java.util.ArrayList;
import java.util.List;

public class PRSubstituteGroup {

	private int groupId;
	private int groupItemType;
	private int groupItemId;
	private List<PRSubstituteItem> subtituteItems = new ArrayList<PRSubstituteItem>();

	public int getGroupItemId() {
		return groupItemId;
	}

	public void setGroupItemId(int groupItemId) {
		this.groupItemId = groupItemId;
	}

	public int getGroupId() {
		return groupId;
	}

	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}

	public List<PRSubstituteItem> getSubtituteItems() {
		return subtituteItems;
	}

	public void setSubtituteItems(List<PRSubstituteItem> subtituteItems) {
		this.subtituteItems = subtituteItems;
	}

	public int getGroupItemType() {
		return groupItemType;
	}

	public void setGroupItemType(int groupItemType) {
		this.groupItemType = groupItemType;
	}
}
