package com.pristine.dto.webservice;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class AuditFilter {
	public String runId;
	public String reportId;
	public String filterType;
	public List<String> filteredItems;
	public String filteredItem;
	public int statusCode;
	public String statusMessage;
	public double auditSetting;
	public String auditSettingMessage;
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(" Run Id - " + runId);
		sb.append(" Report Id - " + runId);
		sb.append(" Filter Type - " + filterType);
		return sb.toString();
	}
}
