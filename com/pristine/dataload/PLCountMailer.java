package com.pristine.dataload;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.PerformanceAnalysisDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.email.EmailService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PLCountMailer {
	private static Logger  logger = Logger.getLogger("PLCountMailer");
	Connection conn;
	public PLCountMailer(){
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ge) {
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
	}
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-pl-count-mailer.properties");
		PropertyManager.initialize("analysis.properties");
		PLCountMailer plCountMailer = new PLCountMailer();
		logger.info("***************************************");
		plCountMailer.analysePlItemsAndSendMail();
		logger.info("***************************************");
	}
	
	private void analysePlItemsAndSendMail(){
		
		try{
			ScheduleDAO scheduleDAO = new ScheduleDAO();
			PerformanceAnalysisDAO performanceAnalysisDAO = new PerformanceAnalysisDAO();
			Date today = new Date();
			SimpleDateFormat formatter = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
			String todayDate = formatter.format(today);
			logger.info("analysePlItemsAndSendMail() - Getting schedules for the day...");
			
			List<ScheduleInfoDTO> schedules = scheduleDAO.getSchedulesForTheDay(conn, todayDate);
			logger.info("analysePlItemsAndSendMail() - # of schedules found - " + schedules.size());
			int plItemCountThreshold = Integer.parseInt(PropertyManager.getProperty("PRICECHECK.MIN_PL_ITEMS_COUNT"));
			String toIds = PropertyManager.getProperty("PRICECHECK.MAIL_TO");
			StringBuilder sb = new StringBuilder();
			for(ScheduleInfoDTO schedule: schedules){
				int plItemCount = performanceAnalysisDAO.getPlItemsCountForSchedule(conn, schedule.getScheduleId());
				if(plItemCount < plItemCountThreshold && plItemCount != -1){
					sb.append("Schedule Id: " + schedule.getScheduleId());
					sb.append("\t Comp store: " + schedule.getStoreName());
					sb.append("\t PL Items count: " + plItemCount);
					sb.append("\n");
				}
			}
			
			if(!sb.toString().equals(Constants.EMPTY)){
				String[] mailIds = toIds.split(";");
				List<String> mailList = new ArrayList<>();
				for(String mailId: mailIds){
					mailList.add(mailId);
				}
				String from = PropertyManager.getProperty("MAIL.FROMADDR", "");
				String subject = "[Presto - Web scraping] - Lower PL items count";
				if (from.isEmpty())
					throw new GeneralException("Unable to send mail without MAIL.FROMADDR property in analysis.properties");
				logger.info("analysePlItemsAndSendMail() - Sending Email");
				EmailService.sendEmail(from, mailList, null, null, subject, sb.toString(), null);
			}
			
		}catch(GeneralException | Exception e){
			logger.error("Error -- analysePlItemsAndSendMail() ", e);
		}finally{
			PristineDBUtil.close(conn);
		}
	}

}
