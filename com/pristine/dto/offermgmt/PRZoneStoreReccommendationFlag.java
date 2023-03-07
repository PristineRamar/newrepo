package com.pristine.dto.offermgmt;

public class PRZoneStoreReccommendationFlag {
	
	public Boolean isRecommendAtStoreLevel = false;
	public Boolean isCheckIfStoreLevelStrategyPresent = false;
	public Boolean isStateLevelStrategyPresent = false;
	public Boolean isVendorLevelStrategyPresent = false;
	public Boolean isStoreLevelStrategyPresent  = false;
	
	public PRZoneStoreReccommendationFlag(){
		this.isRecommendAtStoreLevel = false;
		this.isCheckIfStoreLevelStrategyPresent = false;
		this.isStateLevelStrategyPresent = false;
		this.isVendorLevelStrategyPresent = false;
		this.isStoreLevelStrategyPresent  = false;
	}
}
