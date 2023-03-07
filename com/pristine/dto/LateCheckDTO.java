/*
 *	Author		: vaibhavkumar
 *	Start Date	: Aug 4, 2009
 *
 *	Change Description					Changed By			Date
 *	--------------------------------------------------------------
 */
package com.pristine.dto;

import java.io.Serializable;

public class LateCheckDTO implements Serializable
{
 /**
	 * 
	 */
	
 private static final long	serialVersionUID	= 1L;
 private Integer scheduleId;
 private String  chainName;
 private String  city;
 private String  userId;
 private String  firstName;
 private String  lastName;
 private String  email;
 private String  startDate;
 private String  checkDate;
 private String  endDate;
 private boolean isNotify;
 private String  priceCheckMsg;
 private String  supervisorMsg;
 private String  supervisorId;
 private String  checkListName;
 
 
 public Integer getScheduleId()
{
	return scheduleId;
}
public void setScheduleId(Integer scheduleId)
{
	this.scheduleId = scheduleId;
}
public String getChainName()
{
	return chainName;
}
public void setChainName(String chainName)
{
	this.chainName = chainName;
}
public String getCity()
{
	return city;
}
public void setCity(String city)
{
	this.city = city;
}
public String getUserId()
{
	return userId;
}
public void setUserId(String userId)
{
	this.userId = userId;
}
public String getFirstName()
{
	return firstName;
}
public void setFirstName(String firstName)
{
	this.firstName = firstName;
}
public String getLastName()
{
	return lastName;
}
public void setLastName(String lastName)
{
	this.lastName = lastName;
}
public String getEmail()
{
	return email;
}
public void setEmail(String email)
{
	this.email = email;
}
public String getStartDate()
{
	return startDate;
}
public void setStartDate(String startDate)
{
	this.startDate = startDate;
}
public String getCheckDate()
{
	return checkDate;
}
public void setCheckDate(String checkDate)
{
	this.checkDate = checkDate;
}
public String getEndDate()
{
	return endDate;
}
public void setEndDate(String endDate)
{
	this.endDate = endDate;
}
public boolean isNotify()
{
	return isNotify;
}
public void setNotify(boolean isNotify)
{
	this.isNotify = isNotify;
}
public String getPriceCheckMsg()
{
	return priceCheckMsg;
}
public void setPriceCheckMsg(String priceCheckMsg)
{
	this.priceCheckMsg = priceCheckMsg;
}
public String getSupervisorMsg()
{
	return supervisorMsg;
}
public void setSupervisorMsg(String supervisorMsg)
{
	this.supervisorMsg = supervisorMsg;
}
public String getSupervisorId()
{
	return supervisorId;
}
public void setSupervisorId(String supervisorId)
{
	this.supervisorId = supervisorId;
}
public String getCheckListName() {
	return checkListName;
}
public void setCheckListName(String checkListName) {
	this.checkListName = checkListName;
}
 

	
	
}
