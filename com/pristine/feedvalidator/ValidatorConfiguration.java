package com.pristine.feedvalidator;

import java.util.List;

import com.pristine.feedvalidator.lookup.PropertyNameLookup;
import com.pristine.feedvalidator.lookup.ValidationTypesLookup;

public class ValidatorConfiguration {
	private PropertyNameLookup propertyName;
	List<ValidationTypesLookup> validationList;
	
	public List<ValidationTypesLookup> getValidationList() {
		return validationList;
	}
	public void setValidationList(List<ValidationTypesLookup> validationList) {
		this.validationList = validationList;
	}
	public PropertyNameLookup getPropertyName() {
		return propertyName;
	}
	public void setPropertyName(PropertyNameLookup propertyName) {
		this.propertyName = propertyName;
	}
}
