package com.pristine.dto.offermgmt;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class MultiplePrice implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -6432724408287335534L;
	@JsonProperty("m")
	public Integer multiple;
	@JsonProperty("p")
	public Double price;

	//26th May 2016, added this default constructor
	//as class PRGuidelineAndConstraintLog is not
	//able to convert to json string
	public MultiplePrice() {
		
	}
	
	public MultiplePrice(Integer multiple, Double price) {
		if (multiple == null)
			this.multiple = 1;
		else if (multiple == 0)
			this.multiple = 1;
		else
			this.multiple = multiple;

		this.price = price;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((multiple == null) ? 0 : multiple.hashCode());
		result = prime * result + ((price == null) ? 0 : price.hashCode());
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
		MultiplePrice other = (MultiplePrice) obj;
		if (multiple == null) {
			if (other.multiple != null)
				return false;
		} else if (!multiple.equals(other.multiple))
			return false;
		if (price == null) {
			if (other.price != null)
				return false;
		} else if (!price.equals(other.price))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return multiple + "/" +  Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(price));
	}
	
	@JsonIgnore
	public double getUnitPrice() {
		return PRCommonUtil.getUnitPrice(this, true);
	}

//	public Integer getMultiple() {
//		return multiple;
//	}
//
//	public void setMultiple(Integer multiple) {
//		this.multiple = multiple;
//	}
//
//	public Double getPrice() {
//		return price;
//	}
//
//	public void setPrice(Double price) {
//		this.price = price;
//	}
}
