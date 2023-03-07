package com.pristine.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

public class CustomWeekTest {
	private static Logger logger = Logger.getLogger("CustomWeekTest");
	public static void main(String[] args) {
		CustomWeekTest customWeekTest = new CustomWeekTest();
		PropertyManager.initialize("analysis.properties");
		try{
			customWeekTest.TestWeekStartDate();
			customWeekTest.TestWeekEndDate();
		}catch(Exception e){
			logger.error("Error", e);
		}
	}
	
	private void TestWeekStartDate() throws ParseException{
		String dateStr = "08/28/2016";
		SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		Date date = dateFormat.parse(dateStr);
		String weekStartDate = DateUtil.getWeekStartDate(date, 1);
		logger.info("Week Start Date: " + weekStartDate);
	}

	private void TestWeekEndDate() throws ParseException{
		String dateStr = "08/28/2016";
		SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		Date date = dateFormat.parse(dateStr);
		String weekEndDate = DateUtil.getWeekEndDate(date);
		logger.info("Week End Date: " + weekEndDate);
	}
	
}
