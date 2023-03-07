package com.pristine.dto.offermgmt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

//public class PRGuidelinesDTO implements Serializable, Cloneable{
	public class PRGuidelinesDTO implements  Cloneable{
	private List<PRGuidelineMargin> marginGuideline = null;
	private PRGuidelineComp compGuideline = null; 
	private List<PRGuidelinePI> piGuideline = null;
	private ArrayList<PRGuidelineBrand> brandGuideline = null;
	private ArrayList<PRGuidelineSize> sizeGuideline = null;
	private PRGuidelineLeadZoneDTO leadZoneGuideline = null; 
	
	private TreeMap<Integer, ArrayList<Integer>> execOrderMap = new TreeMap<Integer, ArrayList<Integer>>();
	private HashMap<Integer, Integer> guidelineIdMap = new HashMap<Integer, Integer>();
	
	public List<PRGuidelineMargin> getMarginGuideline() {
		return marginGuideline;
	}
	public void setMarginGuideline(List<PRGuidelineMargin> marginGuideline) {
		this.marginGuideline = marginGuideline;
	}
	public PRGuidelineComp getCompGuideline() {
		return compGuideline;
	}
	public void setCompGuideline(PRGuidelineComp compGuideline) {
		this.compGuideline = compGuideline;
	}	 
	public ArrayList<PRGuidelineSize> getSizeGuideline() {
		return sizeGuideline;
	}
	public void setSizeGuideline(ArrayList<PRGuidelineSize> sizeGuideline) {
		this.sizeGuideline = sizeGuideline;
	}
	
	public TreeMap<Integer, ArrayList<Integer>> getExecOrderMap() {
		//If more than one guideline is marked with same order, then sort by guideline as seen in the ui
		for(ArrayList<Integer> guidelineList : execOrderMap.values()){
			if(guidelineList.size() > 1){
				List<GuidelineDetail> sortByTypeId = new ArrayList<GuidelineDetail>();
				for(Integer guidelineId : guidelineList){
					GuidelineDetail guidelineDetail = new GuidelineDetail();
					guidelineDetail.guidelineId = guidelineId;
					guidelineDetail.guidelineTypeId = this.getGuidelineIdMap().get(guidelineId);
					sortByTypeId.add(guidelineDetail);
				}
				Collections.sort(sortByTypeId, new Comparator<GuidelineDetail>() {
					public int compare(GuidelineDetail c1, GuidelineDetail c2) {
						if (c1.guidelineTypeId > c2.guidelineTypeId)
							return 1;
						if (c1.guidelineTypeId < c2.guidelineTypeId)
							return -1;
						return 0;
					}
				});
				ArrayList<Integer> sortedGuidelineList = new ArrayList<Integer>();
				for(GuidelineDetail guidelineDetail : sortByTypeId){
					sortedGuidelineList.add(guidelineDetail.guidelineId);
				}
				guidelineList.clear();
				guidelineList.addAll(sortedGuidelineList);
			}
		}
		return execOrderMap;
	}
	public void setExecOrderMap(TreeMap<Integer, ArrayList<Integer>> execOrderMap) {
		this.execOrderMap = execOrderMap;
	}
	public HashMap<Integer, Integer> getGuidelineIdMap() {
		return guidelineIdMap;
	}
	public void setGuidelineIdMap(HashMap<Integer, Integer> guidelineIdMap) {
		this.guidelineIdMap = guidelineIdMap;
	}
	
	public void addGuidelineIdMap(int guidelineId, int guidelineTypeId){
		if(guidelineIdMap == null){
			guidelineIdMap = new HashMap<Integer, Integer>();
		}
		guidelineIdMap.put(guidelineId, guidelineTypeId);
	}
	
	public void addExecOrderMap(int execOrder, int guidelineId){
		if(execOrderMap == null){
			execOrderMap = new TreeMap<Integer, ArrayList<Integer>>();
		}
		ArrayList<Integer> tList = null;
		if(execOrderMap.get(execOrder) != null){
			tList = execOrderMap.get(execOrder);
			tList.add(guidelineId);
		}else{
			tList = new ArrayList<Integer>();
			tList.add(guidelineId);
		}
		execOrderMap.put(execOrder, tList);
	}	
	
	public List<PRGuidelinePI> getPiGuideline() {
		return piGuideline;
	}
	public void setPiGuideline(List<PRGuidelinePI> piGuideline) {
		this.piGuideline = piGuideline;
	}
	public ArrayList<PRGuidelineBrand> getBrandGuideline() {
		return brandGuideline;
	}
	public void setBrandGuideline(ArrayList<PRGuidelineBrand> brandGuideline) {
		this.brandGuideline = brandGuideline;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected Object clone() throws CloneNotSupportedException {
		PRGuidelinesDTO cloned = (PRGuidelinesDTO)super.clone();
		if(cloned.getMarginGuideline() != null){
			List<PRGuidelineMargin> clonedList = new ArrayList<PRGuidelineMargin>();
			for(PRGuidelineMargin guidelineMargin : cloned.getMarginGuideline()){
				clonedList.add((PRGuidelineMargin) guidelineMargin.clone());
			}
			cloned.setMarginGuideline(clonedList);
		}
		
		if(cloned.getPiGuideline() != null){
			List<PRGuidelinePI> clonedList = new ArrayList<PRGuidelinePI>();
			for(PRGuidelinePI guidelinePI : cloned.getPiGuideline()){
				clonedList.add((PRGuidelinePI) guidelinePI.clone());
			}
			cloned.setPiGuideline(clonedList);
		}
		
		if(cloned.getCompGuideline() != null)
			cloned.setCompGuideline(((PRGuidelineComp)cloned.getCompGuideline().clone()));
		
		if(cloned.getBrandGuideline() != null){
			ArrayList<PRGuidelineBrand> clonedList = new ArrayList<PRGuidelineBrand>();
			for(PRGuidelineBrand guidelineBrand : cloned.getBrandGuideline()){
				clonedList.add((PRGuidelineBrand) guidelineBrand.clone());
			}
			cloned.setBrandGuideline(clonedList);
		} 
		
		if (cloned.getSizeGuideline() != null) {
			ArrayList<PRGuidelineSize> clonedList = new ArrayList<PRGuidelineSize>();
			for(PRGuidelineSize guidelineSize : cloned.getSizeGuideline()){
				clonedList.add((PRGuidelineSize) guidelineSize.clone());
			}
			cloned.setSizeGuideline(clonedList);
		}
		
		if(cloned.getLeadZoneGuideline() != null)
			cloned.setLeadZoneGuideline(((PRGuidelineLeadZoneDTO)cloned.getLeadZoneGuideline().clone()));
		
		cloned.setExecOrderMap((TreeMap<Integer, ArrayList<Integer>>) cloned.getExecOrderMap().clone());
		cloned.setGuidelineIdMap((HashMap<Integer, Integer>) cloned.getGuidelineIdMap().clone());
		
	    return cloned;
	}
	public PRGuidelineLeadZoneDTO getLeadZoneGuideline() {
		return leadZoneGuideline;
	}
	public void setLeadZoneGuideline(PRGuidelineLeadZoneDTO leadZoneGuideline) {
		this.leadZoneGuideline = leadZoneGuideline;
	}
}

class GuidelineDetail{
	public int guidelineId;
	public int guidelineTypeId;
}