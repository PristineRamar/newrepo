package com.pristine.dto.offermgmt;

//import java.io.Serializable;
//import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import net.sf.ehcache.pool.sizeof.annotations.IgnoreSizeOf;

import com.fasterxml.jackson.annotation.JsonProperty;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.pristine.service.offermgmt.ConstraintTypeLookup;

@IgnoreSizeOf
//public class PRExplainLog implements Serializable  {
public class PRExplainLog {

	@JsonProperty("logs")
	private List<PRGuidelineAndConstraintLog> guidelineAndConstraintLogs;
	
	@JsonProperty("a-d")
	private List<PRExplainAdditionalDetail> explainAdditionalDetail;
	
	public PRExplainLog(){
		this.guidelineAndConstraintLogs = new ArrayList<PRGuidelineAndConstraintLog>();
	}

	public List<PRGuidelineAndConstraintLog> getGuidelineAndConstraintLogs() {
		return guidelineAndConstraintLogs;
	}

	public void setGuidelineAndConstraintLogs(List<PRGuidelineAndConstraintLog> guidelineAndConstraintLogs) {
		this.guidelineAndConstraintLogs = guidelineAndConstraintLogs;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((guidelineAndConstraintLogs == null) ? 0 : guidelineAndConstraintLogs.hashCode());
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
		PRExplainLog other = (PRExplainLog) obj;
		if (guidelineAndConstraintLogs == null) {
			if (other.guidelineAndConstraintLogs != null)
				return false;
		} else if (!guidelineAndConstraintLogs.equals(other.guidelineAndConstraintLogs))
			return false;
		return true;
	}
	 
	public List<PRExplainAdditionalDetail> getExplainAdditionalDetail() {
		return explainAdditionalDetail;
	}

	public void setExplainAdditionalDetail(List<PRExplainAdditionalDetail> explainAdditionalDetail) {
		this.explainAdditionalDetail = explainAdditionalDetail;
	}
	
}
