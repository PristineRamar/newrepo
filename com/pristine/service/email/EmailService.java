package com.pristine.service.email;
import java.util.*;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;


/**
 * Sends email for given to, cc and bcc addresses from configured mail id
 * @author Pradeep
 *
 */

public class EmailService {
	private static Logger logger = Logger.getLogger("EmailService");
	private static final String TO_ADDRESS = "TO=";
	private static final String FROM_ADDRESS = "FROM=";
	private static final String CC_ADDRESS = "CC=";
	private static final String BCC_ADDRESS = "BCC=";
	private static final String SUBJECT = "SUBJECT=";
	private static final String MESSAGE = "MESSAGE=";
	private static final String ATTACHMENT_PATH = "ATTACHMENT_PATH=";
	private static final String IS_HTML = "IS_HTML=";
	private static final String IS_PRIORITY = "IS_PRIORITY=";
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-email.properties");
		PropertyManager.initialize("recommendation.properties");
		List<String> attachmentPath = new ArrayList<String>();
		List<String> to = new ArrayList<String>();
		List<String> cc = new ArrayList<String>();
		List<String> bcc = new ArrayList<String>();
		String fromAddress = null;
		String toAddresses = null;
		String ccAddresses = null;
		String bccAddresses = null;
		String mailSubject = null;
		String message = null;
		String attachment = null;
		boolean isHtml = false;
		boolean isPriority = false;
		logger.info(args);
		for(String arg: args){
			logger.info(arg);
			if(arg.startsWith(FROM_ADDRESS)){
				fromAddress = arg.substring(FROM_ADDRESS.length());
			}
			else if(arg.startsWith(TO_ADDRESS)){
				toAddresses = arg.substring(TO_ADDRESS.length());
			}
			else if(arg.startsWith(SUBJECT)){
				mailSubject = arg.substring(SUBJECT.length());
			}
			else if(arg.startsWith(MESSAGE)){
				message = arg.substring(MESSAGE.length());
			}
			else if(arg.startsWith(BCC_ADDRESS)){
				bccAddresses = arg.substring(BCC_ADDRESS.length());
			}
			else if(arg.startsWith(CC_ADDRESS)){
				ccAddresses = arg.substring(CC_ADDRESS.length());
			}
			else if(arg.startsWith(ATTACHMENT_PATH)){
				attachment = arg.substring(ATTACHMENT_PATH.length());
			}else if(arg.startsWith(IS_HTML)){
				isHtml = Boolean.parseBoolean(arg.substring(IS_HTML.length()));
			}
			else if (arg.startsWith("IS_PRIORITY=")) {
	            isPriority = Boolean.parseBoolean(arg.substring("IS_PRIORITY=".length()));
	        }
		}
		
		logger.info(fromAddress);
		logger.info(toAddresses);
		logger.info(mailSubject);
		logger.info(message);
		logger.info(isPriority);
		if(toAddresses == null || fromAddress == null || mailSubject == null || message == null){
			logger.error("TO, FROM, SUBJECT and  MESSAGE are mandatory! Unable to proceed.");
			System.exit(0);
		}
		
		String[] splitTo = toAddresses.split(";");
		for(String toAddress: splitTo){
			to.add(toAddress);
		}
		
		if(ccAddresses != null){
			String[] splitcc = ccAddresses.split(";");
			for(String ccAddress: splitcc){
				cc.add(ccAddress);
			}
		}
		if(bccAddresses != null){
			String[] splitbcc = bccAddresses.split(";");
			for(String bccAddress: splitbcc){
				bcc.add(bccAddress);
			}
		}
		if(attachment != null){
			String[] splitattach = attachment.split(";");
			for(String attach: splitattach){
				attachmentPath.add(attach);
			}
		}
		try {
			if(isHtml){
				EmailService.sendEmailAsHTML(fromAddress, to, cc, bcc, mailSubject, message, attachmentPath, isPriority);
			}else{
				EmailService.sendEmail(fromAddress, to, cc, bcc, mailSubject, message, attachmentPath);	
			}
			
		} catch (GeneralException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
/**
 * 
 * Sends mail to specified addresses with attachments if specified. from, to, subject are mandatory.
 * attachmentPath, cc and bcc are optional. null can be passed for these parameters 
 * @param from
 * @param to
 * @param cc
 * @param bcc
 * @param subject
 * @param mailContent
 * @param attachmentPath
 * @return mail sending status
 * @throws GeneralException
 */
	
	public static boolean sendEmail(String from, List<String> to, List<String> cc,
			List<String> bcc,
			String subject, String mailContent, List<String> attachmentPath)
			throws GeneralException {
		boolean isMailSent = false;
		int smtpPort = Integer.parseInt(PropertyManager.getProperty("MAIL.SMTP.PORT"));
		String smtpHost = PropertyManager.getProperty("MAIL.SMTP.HOST");
		final String userName = PropertyManager.getProperty("MAIL.FROMADDR");
		final String passoword = PropertyManager.getProperty("MAIL.PASSWORD");
		
		
		// Validate configuration.
		if (smtpPort == 0 || smtpHost.isEmpty() || userName.isEmpty()
				|| passoword.isEmpty()) {
			logger.error("Unable to send mail. Check following configurations."
						+ "\n 1. MAIL.SMTP.PORT "
						+ "\n 2. MAIL.SMTP.HOST "
						+ "\n 3. MAIL.USERNAME "
						+ "\n 4. MAIL.PASSWORD ");
			throw new GeneralException(
					"Unable to send mail. Configurations are missing");
		}

		try {
			Properties props = new Properties();
			props.put("mail.smtp.ssl.protocols", "TLSv1.2");   
			props.put("mail.smtp.starttls.enable", "true");
			props.put("mail.smtp.port", smtpPort);
			props.put("mail.smtp.host", smtpHost);
			props.put("mail.smtp.auth", "true");
			Session session = Session.getInstance(props, new Authenticator() {
				@Override
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(userName, passoword);
				}
			});
			// Create a default MimeMessage object.
			MimeMessage message = new MimeMessage(session);
			// Set From: header field of the header.
			message.setFrom(new InternetAddress(from));
			// Set To: header field of the header.
			for (String toAddress : to) {
				message.addRecipient(Message.RecipientType.TO,
						new InternetAddress(toAddress));
			}
			// Set To: header field of the header.
			if(cc != null){
				for (String ccAddress : cc) {
					message.addRecipient(Message.RecipientType.CC,
							new InternetAddress(ccAddress));
				}
			}
			
			if(bcc != null){
				for (String bccAddress : bcc) {
					message.addRecipient(Message.RecipientType.BCC,
							new InternetAddress(bccAddress));
				}
			}
			// Set Subject: header field
			message.setSubject(subject);
			// Create a multipart message
			Multipart multipart = new MimeMultipart();
			BodyPart messageBodyPart = new MimeBodyPart();
			// Set text message part
			messageBodyPart.setText(mailContent);
			multipart.addBodyPart(messageBodyPart);
			if(attachmentPath != null){
				for(String attachement: attachmentPath){
					String splittedFileName[] = attachement.split("/");
					String actualFileName = splittedFileName[splittedFileName.length - 1];
					// Create body parts for # of attachments.
					messageBodyPart = new MimeBodyPart();
					DataSource source = new FileDataSource(attachement);
					messageBodyPart.setDataHandler(new DataHandler(source));
					messageBodyPart.setFileName(actualFileName);
					multipart.addBodyPart(messageBodyPart);
				}
			}
			
			// Send the complete message parts
			message.setContent(multipart);
						
			// Send message
			Transport.send(message);
			isMailSent = true;
			logger.info("Mail sent successfully!");

		} catch (MessagingException mex) {
			logger.error("Error while sending mail", mex);
			throw new GeneralException("Error while sending mail", mex);
		}
		catch(Exception ex) {
			logger.error("Error while sending mail", ex);
			throw new GeneralException("Error while sending mail", ex);
		}

		return isMailSent;
	}
	/**
	 * Send EMail to user in HTML format
	 * @param from
	 * @param to
	 * @param cc
	 * @param bcc
	 * @param subject
	 * @param mailContent
	 * @param attachmentPath
	 * @return
	 * @throws GeneralException
	 */
	public static boolean sendEmailAsHTML(String from, List<String> to, List<String> cc,
			List<String> bcc,
			String subject, String mailContent, List<String> attachmentPath, Boolean isPriority)
			throws GeneralException {
		boolean isMailSent = false;
		int smtpPort = Integer.parseInt(PropertyManager.getProperty(
				"MAIL.SMTP.PORT", "0"));
		String smtpHost = PropertyManager.getProperty("MAIL.SMTP.HOST", "");
		final String userName = PropertyManager.getProperty("MAIL.FROMADDR", "");
		final String passoword = PropertyManager.getProperty("MAIL.PASSWORD", "");
		
		
		// Validate configuration.
		if (smtpPort == 0 || smtpHost.isEmpty() || userName.isEmpty()
				|| passoword.isEmpty()) {
			logger.error("Unable to send mail. Check following configurations."
						+ "\n 1. MAIL.SMTP.PORT "
						+ "\n 2. MAIL.SMTP.HOST "
						+ "\n 3. MAIL.USERNAME "
						+ "\n 4. MAIL.PASSWORD ");
			throw new GeneralException(
					"Unable to send mail. Configurations are missing");
		}

		try {
			Properties props = new Properties();
			props.put("mail.smtp.ssl.protocols", "TLSv1.2");
			props.put("mail.smtp.starttls.enable", "true");
			props.put("mail.smtp.port", smtpPort);
			props.put("mail.smtp.host", smtpHost);
			props.put("mail.smtp.auth", "true");
			
			Session session = Session.getInstance(props, new Authenticator() {
				@Override
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(userName, passoword);
				}
			});
			// Create a default MimeMessage object.
			MimeMessage message = new MimeMessage(session);
			// Set From: header field of the header.
			message.setFrom(new InternetAddress(from));
			// Set To: header field of the header.
			for (String toAddress : to) {
				message.addRecipient(Message.RecipientType.TO,
						new InternetAddress(toAddress));
			}
			// Set To: header field of the header.
			if(cc != null){
				for (String ccAddress : cc) {
					message.addRecipient(Message.RecipientType.CC,
							new InternetAddress(ccAddress));
				}
			}
			
			if(bcc != null){
				for (String bccAddress : bcc) {
					message.addRecipient(Message.RecipientType.BCC,
							new InternetAddress(bccAddress));
				}
			}
			
			if (isPriority) {
	               message.setHeader("Importance", "high");
	            }
			
			// Set Subject: header field
			message.setSubject(subject);
			// Create a multipart message
			Multipart multipart = new MimeMultipart();
			BodyPart messageBodyPart = new MimeBodyPart();
			// Set text message part
			//messageBodyPart.setText(mailContent);
			messageBodyPart.setContent(mailContent, "text/html");
			multipart.addBodyPart(messageBodyPart);
			if(attachmentPath != null){
				for(String attachement: attachmentPath){
					String splittedFileName[] = attachement.split("//");
					String actualFileName = splittedFileName[splittedFileName.length - 1];
					// Create body parts for # of attachments.
					messageBodyPart = new MimeBodyPart();
					DataSource source = new FileDataSource(attachement);
					messageBodyPart.setDataHandler(new DataHandler(source));
					messageBodyPart.setFileName(actualFileName);
					multipart.addBodyPart(messageBodyPart);
				}
			}
			// Send the complete message parts
			message.setContent(multipart, "text/html");
			// Send message
			Transport.send(message);
			isMailSent = true;
		} catch (MessagingException mex) {
			logger.error("Error while sending mail", mex);
			throw new GeneralException("Error while sending mail", mex);
		}
		catch(Exception ex) {
			logger.error("Error while sending mail", ex);
			throw new GeneralException("Error while sending mail", ex);
		}

		return isMailSent;
	}
}