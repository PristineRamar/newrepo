package com.pristine.dto.webservice;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Email {
	public String to;
	public String cc;
	public String subject;
	public String content;
	public String bcc;
	public String attachmentPath;
	public String status;
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
			sb.append("mail to - " + to + "\t");
			sb.append("mail cc - " + cc + "\t");
			sb.append("mail bcc - " + bcc + "\t");
			sb.append("subject - " + subject + "\t");
			sb.append("content -  " + content + "\t");
			sb.append("attachment -  " + attachmentPath);
		return sb.toString();
	}
}