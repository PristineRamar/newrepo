package com.pristine.dto.offermgmt;

import java.io.Serializable;

//public class PRStrategyDTO implements Serializable, Cloneable{
	public class PRStrategyDTO implements  Serializable, Cloneable{
	/**
		 * 
		 */
		private static final long serialVersionUID = 6183207433761436848L;
	private long strategyId;
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private int startCalendarId;
	private int endCalendarId;
	private int applyTo;
	private int priceCheckListId;
	private int criteriaId;
	private String startDate;
	private String endDate;
	
	private PRObjectiveDTO objective = null;
	private PRGuidelinesDTO guidelines = null;
	private PRConstraintsDTO constriants = null;
	
	private char runType;
	
	private String predictedBy = null;
	
	private int vendorId;
	
	private char dsdRecommendationFlag = ' ' ;
	private int stateId;
	private String zoneNum;
	private int chainId;
	//Added for price Test
	private boolean priceTestZone=false;
	private int tempLocationID=0;
	private boolean isGlobalZone;
	/**  AI#111-PROM-2278 change**/
	private int priority=0;
	
	public long getStrategyId() {
		return strategyId;
	}
	public void setStrategyId(long strategyId) {
		this.strategyId = strategyId;
	}
	public int getLocationLevelId() {
		return locationLevelId;
	}
	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	public int getLocationId() {
		return locationId;
	}
	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	public int getProductLevelId() {
		return productLevelId;
	}
	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}
	public int getProductId() {
		return productId;
	}
	public void setProductId(int productId) {
		this.productId = productId;
	}
	public int getStartCalendarId() {
		return startCalendarId;
	}
	public void setStartCalendarId(int startCalendarId) {
		this.startCalendarId = startCalendarId;
	}
	public int getEndCalendarId() {
		return endCalendarId;
	}
	public void setEndCalendarId(int endCalendarId) {
		this.endCalendarId = endCalendarId;
	}
	public int getApplyTo() {
		return applyTo;
	}
	public void setApplyTo(int applyTo) {
		this.applyTo = applyTo;
	}
	public PRObjectiveDTO getObjective() {
		return objective;
	}
	public void setObjective(PRObjectiveDTO objective) {
		this.objective = objective;
	}
	public PRGuidelinesDTO getGuidelines() {
		return guidelines;
	}
	public void setGuidelines(PRGuidelinesDTO guidelines) {
		this.guidelines = guidelines;
	}
	public PRConstraintsDTO getConstriants() {
		return constriants;
	}
	public void setConstriants(PRConstraintsDTO constriants) {
		this.constriants = constriants;
	}
	public void copy(PRStrategyDTO strategyDTO){
		this.strategyId = strategyDTO.strategyId;
		this.locationLevelId = strategyDTO.locationLevelId;
		this.locationId = strategyDTO.locationId;
		this.productLevelId = strategyDTO.productLevelId;
		this.productId = strategyDTO.productId;
		this.applyTo = strategyDTO.applyTo;
		this.vendorId = strategyDTO.vendorId;
		this.stateId = strategyDTO.stateId;
		this.dsdRecommendationFlag = strategyDTO.dsdRecommendationFlag;
		
		this.startCalendarId = strategyDTO.startCalendarId;
		this.endCalendarId = strategyDTO.endCalendarId;
		this.startDate = strategyDTO.startDate;
		this.endDate = strategyDTO.endDate;
		this.runType = strategyDTO.runType;
		this.priority=strategyDTO.getPriority();
	}
	
	public char getRunType() {
		return runType;
	}
	public void setRunType(char runType) {
		this.runType = runType;
	}
	public int getPriceCheckListId() {
		return priceCheckListId;
	}
	public void setPriceCheckListId(int priceCheckListId) {
		this.priceCheckListId = priceCheckListId;
	}
	public String getPredictedBy() {
		return predictedBy;
	}
	public void setPredictedBy(String predictedBy) {
		this.predictedBy = predictedBy;
	}
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public int getVendorId() {
		return vendorId;
	}
	public void setVendorId(int vendorId) {
		this.vendorId = vendorId;
	}
	public char getDsdRecommendationFlag() {
		return dsdRecommendationFlag;
	}
	public void setDsdRecommendationFlag(char dsdRecommendationFlag) {
		this.dsdRecommendationFlag = dsdRecommendationFlag;
	}
	public int getStateId() {
		return stateId;
	}
	public void setStateId(int stateId) {
		this.stateId = stateId;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		PRStrategyDTO cloned = (PRStrategyDTO) super.clone();
		
		if (cloned.getObjective() != null)
			cloned.setObjective(((PRObjectiveDTO) cloned.getObjective().clone()));

		if (cloned.getGuidelines() != null)
			cloned.setGuidelines(((PRGuidelinesDTO) cloned.getGuidelines().clone()));

		if (cloned.getConstriants() != null)
			cloned.setConstriants(((PRConstraintsDTO) cloned.getConstriants().clone()));
		
		return cloned;
	}
	public int getCriteriaId() {
		return criteriaId;
	}
	public void setCriteriaId(int criteriaId) {
		this.criteriaId = criteriaId;
	}
	public String getZoneNum() {
		return zoneNum;
	}
	public void setZoneNum(String zoneNum) {
		this.zoneNum = zoneNum;
	}
	public int getChainId() {
		return chainId;
	}
	public void setChainId(int chainId) {
		this.chainId = chainId;
	}
	public boolean isPriceTestZone() {
		return priceTestZone;
	}
	public void setPriceTestZone(boolean priceTestZone) {
		this.priceTestZone = priceTestZone;
	}
	public int getTempLocationID() {
		return tempLocationID;
	}
	public void setTempLocationID(int tempLocationID) {
		this.tempLocationID = tempLocationID;
	}
	public boolean isGlobalZone() {
		return isGlobalZone;
	}
	public void setGlobalZone(boolean isGlobalZone) {
		this.isGlobalZone = isGlobalZone;
	}
	public int getPriority() {
		return priority;
	}
	public void setPriority(int priority) {
		this.priority = priority;
	}
	
}
