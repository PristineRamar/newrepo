package com.pristine.dto;

public class StoreItemKey {
	private int levelTypeId;
	private String levelId;
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((levelId == null) ? 0 : levelId.hashCode());
		result = prime * result + levelTypeId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StoreItemKey other = (StoreItemKey) obj;
		if (levelId == null) {
			if (other.levelId != null)
				return false;
		} else if (!levelId.equals(other.levelId))
			return false;
		if (levelTypeId != other.levelTypeId)
			return false;
		return true;
	}

	public StoreItemKey(int levelTypeId, String levelId) {
		// TODO Auto-generated constructor stub
		this.levelTypeId = levelTypeId;
		this.levelId = levelId;
	}
	
	public int getLevelTypeId() {
		return levelTypeId;
	}
	public void setLevelTypeId(int levelTypeId) {
		this.levelTypeId = levelTypeId;
	}
	public String getLevelId() {
		return levelId;
	}
	public void setLevelId(String levelId) {
		this.levelId = levelId;
	}
	
}
