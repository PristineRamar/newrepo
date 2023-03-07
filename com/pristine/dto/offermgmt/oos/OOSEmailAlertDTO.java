package com.pristine.dto.offermgmt.oos;

import java.util.ArrayList;
import java.util.List;

public class OOSEmailAlertDTO {
	private int locationLevelId;
	private int locationId;
	private String mailTo;
	private String mailCC;
	private String mailBCC;
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
	public String getMailTo() {
		return mailTo;
	}
	public void setMailTo(String mailTo) {
		this.mailTo = mailTo;
	}
	public String getMailCC() {
		return mailCC;
	}
	public void setMailCC(String mailCC) {
		this.mailCC = mailCC;
	}
	public String getMailBCC() {
		return mailBCC;
	}
	public void setMailBCC(String mailBCC) {
		this.mailBCC = mailBCC;
	}
	public List<String> getMailBCCList() {
		List<String> mailBCCList = null;
		if(mailBCC != null){
			String[] mailBCCArr = mailBCC.split(";");
			mailBCCList = new ArrayList<String>();
			for(String bcc: mailBCCArr){
				mailBCCList.add(bcc);
			}
		}
		return mailBCCList;
	}
	public List<String> getMailCCList() {
		List<String> mailCCList = null;
		if(mailCC != null){
			String[] mailCCArr = mailCC.split(";");
			mailCCList = new ArrayList<String>();
			for(String cc: mailCCArr){
				mailCCList.add(cc);
			}
		}
		return mailCCList;
	}
	public List<String> getMailToList() {
		List<String> mailToList = null;
		if(mailTo != null){
			String[] mailToArr = mailTo.split(";");
			mailToList = new ArrayList<String>();
			for(String to: mailToArr){
				mailToList.add(to);
			}
		}
		return mailToList;
	}
	
}
