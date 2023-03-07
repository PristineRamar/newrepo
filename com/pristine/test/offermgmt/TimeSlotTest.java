package com.pristine.test.offermgmt;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.offermgmt.oos.OOSAnalysisDAO;
import com.pristine.dto.offermgmt.DayPartLookupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

public class TimeSlotTest extends PristineFileParser{
	
	Connection conn = null;
	private static Logger  logger = Logger.getLogger("OOSAlert");
	public TimeSlotTest(){
		conn = getOracleConnection();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PropertyConfigurator.configure("log4j-oos-alert.properties");
		PropertyManager.initialize("analysis.properties");
		TimeSlotTest timeSlotTest = new TimeSlotTest();
		timeSlotTest.testOOSTimeSlots();
	}
	
	private void testOOSTimeSlots() {
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		try{
			List<DayPartLookupDTO> dayPartLookup = oosAnalysisDAO.getDayPartLookup(conn);
			Date currentDate  = new Date();
			getPreviousDayPart(dayPartLookup, currentDate);
		}
		catch(GeneralException ge){
			logger.error("Error", ge);
		}
		catch(Exception ge){
			logger.error("Error", ge);
		}
	}

	@Override
	public void processRecords(List listobj) throws GeneralException {
		// TODO Auto-generated method stub
		
	}
	
	public void getPreviousDayPart(List<DayPartLookupDTO> dayPartLookup, Date currentDate) throws ParseException{
		//07:00	11:00
		//11:00	15:00
		//15:00	18:00 
		//18:00	21:00 
		//21:00	07:00
		//1. get current time.
		SimpleDateFormat hourAndMin = new SimpleDateFormat("HH:mm");
		SimpleDateFormat hourOnly = new SimpleDateFormat("HH");
		List<String> testList = new ArrayList<String>();
		testList.add("00:00");
		testList.add("00:30");
		testList.add("01:00");
		testList.add("01:30");
		testList.add("02:00");
		testList.add("02:30");
		testList.add("03:00");
		testList.add("03:30");
		testList.add("04:00");
		testList.add("04:30");
		testList.add("05:00");
		testList.add("05:30");
		testList.add("06:00");
		testList.add("06:30");
		testList.add("07:00");
		testList.add("07:30");
		testList.add("08:00");
		testList.add("08:30");
		testList.add("09:00");
		testList.add("09:30");
		testList.add("10:00");
		testList.add("10:30");
		testList.add("11:00");
		testList.add("11:30");
		testList.add("12:00");
		testList.add("12:30");
		testList.add("13:00");
		testList.add("13:30");
		testList.add("14:00");
		testList.add("14:30");
		testList.add("15:00");
		testList.add("15:30");
		testList.add("16:00");
		testList.add("16:30");
		testList.add("17:00");
		testList.add("17:30");
		testList.add("18:00");
		testList.add("18:30");
		testList.add("19:00");
		testList.add("19:30");
		testList.add("20:00");
		testList.add("20:30");
		testList.add("21:00");
		testList.add("21:30");
		testList.add("22:00");
		testList.add("22:30");
		testList.add("23:00");
		testList.add("23:59");
	for(String time: testList){
		Date actualProcessTime = hourAndMin.parse(time);
		Date actualProcessTimeHr = hourOnly.parse(time);
		int dayPartId = 0;
		for(DayPartLookupDTO oosDayPartLookupDTO: dayPartLookup){
			String startTimeStr = oosDayPartLookupDTO.getStartTime();
			String endTimeStr  = oosDayPartLookupDTO.getEndTime();
			Date startTime = hourAndMin.parse(startTimeStr);
			Date endTime = hourAndMin.parse(endTimeStr);
			if((actualProcessTime.after(startTime)|| actualProcessTime.equals(startTime))
					&& actualProcessTime.before(endTime)){
				dayPartId = oosDayPartLookupDTO.getDayPartId();
				String out = "dayPartId = " + dayPartId + " & given Time = " + time + " & Start time = " + oosDayPartLookupDTO.getStartTime() + " & End time = " + oosDayPartLookupDTO.getEndTime();
				logger.info(out);
				break;
			}
			else if(oosDayPartLookupDTO.isSlotSpanDays()){
				endTime = DateUtil.incrementDate(endTime, 1);
				if((actualProcessTime.after(actualProcessTimeHr)|| actualProcessTime.equals(actualProcessTimeHr))
						&& actualProcessTime.before(endTime)){
					dayPartId = oosDayPartLookupDTO.getDayPartId();
				String out = "dayPartId = " + dayPartId + " & given Time = " + time + " & Start time = " + oosDayPartLookupDTO.getStartTime() + " & End time = " + oosDayPartLookupDTO.getEndTime();
				logger.info(out);
				break;
				}
			}
		
		
		
		}
	}
		//2. find out current day part.
		//3. find out previous day part from order column.
	 
	}

	

}
