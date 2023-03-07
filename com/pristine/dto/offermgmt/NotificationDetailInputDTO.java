package com.pristine.dto.offermgmt;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class NotificationDetailInputDTO {
	private int moduleId;
	private int notificationTypeId;
	private long notificationKey1;
	private long notificationKey2;
	private long notificationKey3;
	private long notificationKey4;
	
	public int getModuleId() {
		return moduleId;
	}
	public void setModuleId(int moduleId) {
		this.moduleId = moduleId;
	}
	public int getNotificationTypeId() {
		return notificationTypeId;
	}
	public void setNotificationTypeId(int notificationTypeId) {
		this.notificationTypeId = notificationTypeId;
	}
	public long getNotificationKey1() {
		return notificationKey1;
	}
	public void setNotificationKey1(long notificationKey1) {
		this.notificationKey1 = notificationKey1;
	}
	public long getNotificationKey2() {
		return notificationKey2;
	}
	public void setNotificationKey2(long notificationKey2) {
		this.notificationKey2 = notificationKey2;
	}
	public long getNotificationKey3() {
		return notificationKey3;
	}
	public void setNotificationKey3(long notificationKey3) {
		this.notificationKey3 = notificationKey3;
	}
	public long getNotificationKey4() {
		return notificationKey4;
	}
	public void setNotificationKey4(long notificationKey4) {
		this.notificationKey4 = notificationKey4;
	}
}
