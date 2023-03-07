package com.pristine.dto.dataload.gianteagle;

public class GEHouseholdDTO implements Cloneable {
	
private	int householdno;
private String blockcode ;
private String zipcode;

public int getHouseholdno() {
	return householdno;
}
public void setHouseholdno(int householdno) {
	this.householdno = householdno;
}
public String getBlockcode() {
	return blockcode;
}
public void setBlockcode(String blockcode) {
	this.blockcode = blockcode;
}
public String getZipcode() {
	return zipcode;
}
public void setZipcode(String zipcode) {
	this.zipcode = zipcode;
}

@Override
public Object clone() throws CloneNotSupportedException {
	return super.clone();
}

}
