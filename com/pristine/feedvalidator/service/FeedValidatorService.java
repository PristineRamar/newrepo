package com.pristine.feedvalidator.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.fileformatter.gianteagle.GiantEagleCostDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.email.EmailService;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import java.time.LocalDate;

public class FeedValidatorService {
	private static Logger logger = Logger.getLogger("FeedValidatorService");
	String rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
	private FileWriter fw = null;
	private PrintWriter pw = null;
	
	private void initializeFileWriter(String outputPath) throws Exception{
		File file = new File(outputPath);
		if (!file.exists())
			fw = new FileWriter(outputPath);
		else
			fw = new FileWriter(outputPath, false);
		pw = new PrintWriter(fw);
	}
	
	public void sendEMail(String outputPath, String mailSubject, String mailBody) throws GeneralException{
		
		String fromAddr = PropertyManager.getProperty("MAIL.FROMADDR", "");
		String toAddresses = PropertyManager.getProperty("MAIL.TOADDR", "");
		String[] splitTo = toAddresses.split(";");
		List<String> toAddr = new ArrayList<>();
		for(String toAddress: splitTo){
			toAddr.add(toAddress);
		}
		String ccAddresses = PropertyManager.getProperty("MAIL.CCADDR", "");
		String[] splitCc = ccAddresses.split(";");
		List<String> ccAddr = new ArrayList<>();
		for(String ccAddress: splitCc){
			ccAddr.add(ccAddress);
		}
		
		List<String> outputPathList = new ArrayList<>();
		if(outputPath!=null && !outputPath.isEmpty()){
			outputPathList.add(outputPath);
		}
	
		StringBuilder sb = new StringBuilder();
		sb.append("This is an automatic mail received due to an error occured in feed validation. Please see attached error messages with sample records.");
		sb.append("<br>");
		sb.append("-Presto Support team");
		sb.append("<br>");
		sb.append("<br>");
		sb.append(Constants.MAIL_CONFIDENTIAL_NOTE);
		sb.append("</font></body>");
		
		
		boolean isEmailSend = EmailService.sendEmailAsHTML(fromAddr, toAddr, ccAddr, null, mailSubject, sb.toString(),
				(outputPathList.size() > 0) ? outputPathList : null, false);

		if(!isEmailSend){
			logger.error("Error while sending email alert to monitoring team.");
		}else{
			logger.info("Email alert sent...");
		}
	}
	
	public void writeCostErrRecAndSendMail(List<GiantEagleCostDTO> costList, String outputFolder, boolean printDistErrRec, boolean sendMail)
			throws Exception, GeneralException {
		LocalDate date1 = LocalDate.now();
		String fileName = "Cost_error_records_"+date1+".txt";
		String outputPath = rootPath + "/" + outputFolder + "/" + fileName;
		initializeFileWriter(outputPath);
		
		HashMap<String, GiantEagleCostDTO> errDetailAndSampleRec = new HashMap<>();
		costList.stream().filter(s -> s.getErrorMessage() != null && !s.getErrorMessage().isEmpty()).forEach(value -> {
			if (printDistErrRec && errDetailAndSampleRec.containsKey(value.getErrorMessage())) {
				return;
			} else {
				errDetailAndSampleRec.put(value.getErrorMessage(), value);
				pw.print(value.getUPC());
				pw.print("|");
				pw.print(value.getWHITEM_NO());
				pw.print("|");
				pw.print(value.getSTRT_DTE());
				pw.print("|");
				pw.print(value.getCST_ZONE_NO());
				pw.print("|");
				pw.print(value.getSPLR_NO());
				pw.print("|");
				pw.print(value.getCST_STAT_CD());
				pw.print("|");
				pw.print(value.getBS_CST_AKA_STORE_CST());
				pw.print("|");
				pw.print(value.getDLVD_CST_AKA_WHSE_CST());
				pw.print("|");
				pw.print(value.getLONG_TERM_REFLECT_FG());
				pw.print("|");
				pw.print(value.getBNR_CD());
				pw.print("|");
				pw.print(value.getErrorMessage());
				pw.println("");
			}
		});
		pw.flush();
		fw.flush();
		pw.close();
		fw.close();
		
		String mailSubject = "RE: Error records in cost feed.";
		String mailBody ="This is an automatic mail received due to an error occured in feed validation. "
				+ "Please see attached error messages with sample records.";
		if(sendMail){
			sendEMail(outputPath, mailSubject, mailBody);
		}
	}
}
