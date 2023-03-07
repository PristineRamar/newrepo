package com.pristine.dto.fileformatter.gianteagle;

public class ZoneDTO {
	private String znNo;
	private int znId;
	private String mktAreaDscr;
	private String splrNo;
	private int prcGrpCd;
	private String bnrCd;
	private String prcGrpDscr;
	private String sysCd;
	private String actualZoneNum;
	private String zoneType;
	private String znName;
	private String globalZn;
	
	public String getZnNo() {
		return znNo;
	}
	public void setZnNo(String znNo) {
		this.znNo = znNo;
	}
	public String getMktAreaDscr() {
		return mktAreaDscr;
	}
	public void setMktAreaDscr(String mktAreaDscr) {
		this.mktAreaDscr = mktAreaDscr;
	}
	public String getSplrNo() {
		return splrNo;
	}
	public void setSplrNo(String splrNo) {
		this.splrNo = splrNo;
	}
	public int getPrcGrpCd() {
		return prcGrpCd;
	}
	public void setPrcGrpCd(int prcGrpCd) {
		this.prcGrpCd = prcGrpCd;
	}
	public String getBnrCd() {
		return bnrCd;
	}
	public void setBnrCd(String bnrCd) {
		this.bnrCd = bnrCd;
	}
	public String getPrcGrpDscr() {
		return prcGrpDscr;
	}
	public void setPrcGrpDscr(String prcGrpDscr) {
		this.prcGrpDscr = prcGrpDscr;
	}
	public String getSysCd() {
		return sysCd;
	}
	public void setSysCd(String sysCd) {
		this.sysCd = sysCd;
	}
	public String getActualZoneNum() {
		return actualZoneNum;
	}
	public void setActualZoneNum(String actualZoneNum) {
		this.actualZoneNum = actualZoneNum;
	}
	public String getZoneType() {
		return zoneType;
	}
	public void setZoneType(String zoneType) {
		this.zoneType = zoneType;
	}
	public int getZnId() {
		return znId;
	}
	public void setZnId(int znId) {
		this.znId = znId;
	}
	public String getZnName() {
		return znName;
	}
	public void setZnName(String znName) {
		this.znName = znName;
	}
	public String getGlobalZn() {
		return globalZn;
	}
	public void setGlobalZn(String globalZn) {
		this.globalZn = globalZn;
	}
	
	public ZoneDTO(String znNo, int znId, String zoneType, String znName, String globalZn) {
		super();
		this.znNo = znNo;
		this.znId = znId;
		this.zoneType = zoneType;
		this.znName = znName;
		this.globalZn = globalZn;
	}
	public ZoneDTO() {
	}
}
