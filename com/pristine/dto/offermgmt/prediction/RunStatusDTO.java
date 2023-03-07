package com.pristine.dto.offermgmt.prediction;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class RunStatusDTO {
	public long runId = -1;
	public int statusCode;
	public String message;
	public int msgCnt = 0;
}
