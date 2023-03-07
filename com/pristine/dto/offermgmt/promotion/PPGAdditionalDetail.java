package com.pristine.dto.offermgmt.promotion;

import java.util.HashSet;

import com.fasterxml.jackson.annotation.JsonProperty;


public class PPGAdditionalDetail {
	
//	@JsonProperty("bc")
//	private int blockGroupedTogetherCnt;
	
//	@JsonProperty("cc")
//	private int categoryGroupedTogetherCnt;
	
	@JsonProperty("nc")
	private int noOfOccurance;
	
	@JsonProperty("a-d")
	private HashSet<String> adDetails;
	
	public HashSet<String> getAdDetails() {
		return adDetails;
	}
	public void setAdDetails(HashSet<String> adDetails) {
		this.adDetails = adDetails;
	}
//	public int getBlockGroupedTogetherCnt() {
//		return blockGroupedTogetherCnt;
//	}
//	public void setBlockGroupedTogetherCnt(int blockGroupedTogetherCnt) {
//		this.blockGroupedTogetherCnt = blockGroupedTogetherCnt;
//	}
//	public int getCategoryGroupedTogetherCnt() {
//		return categoryGroupedTogetherCnt;
//	}
//	public void setCategoryGroupedTogetherCnt(int categoryGroupedTogetherCnt) {
//		this.categoryGroupedTogetherCnt = categoryGroupedTogetherCnt;
//	}
	public int getNoOfOccurance() {
		return noOfOccurance;
	}
	public void setNoOfOccurance(int noOfOccurance) {
		this.noOfOccurance = noOfOccurance;
	}
}
