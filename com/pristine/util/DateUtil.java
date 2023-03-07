/*
 * Author : Suresh Start Date : Jul 22, 2009
 * 
 * Change Description Changed By Date
 * --------------------------------------------------------------
 */

package com.pristine.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.pristine.dto.CalendarYearlyDTO;
import com.pristine.exception.GeneralException;

public class DateUtil
{
	/*
	 * This method will return WeekStartDate based on the currentDate (passed as
	 * input) and the previous Number of weeks. It prevNoofWeeks is 0, it will
	 * return the week start for the current week. It it is 1, it will return
	 * the week start date for the Previous week and so and on
	 */
	
 //private static int MILLIS_IN_DAY=1000 * 60 * 60 * 24;
 public static final String APP_DATE_FORMAT = "MM/dd/yyyy";
 
static DateFormat format=DateFormat.getDateTimeInstance();
	public static String getWeekStartDate(Date currentDate, int prevNoOfWeeks)
	{
		return getWeekStartDate(currentDate, prevNoOfWeeks, Constants.APP_DATE_FORMAT);
	}
	
	public static String getWeekStartDate(Date currentDate, int prevNoOfWeeks, String dateFormat)
	{
		java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(dateFormat);
		Calendar c1 = Calendar.getInstance();
		c1.setTime(currentDate);
		int dow = c1.get(Calendar.DAY_OF_WEEK);
		
		int dayIndex = 0;
		if(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null){
			dayIndex = Integer.parseInt(PropertyManager.
					getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if(dayIndex > 0){
			c1.setFirstDayOfWeek(dayIndex + 1);
			c1.set(Calendar.DAY_OF_WEEK, dayIndex + 1);
			int weekStartDays = 7 * prevNoOfWeeks;
			c1.add(Calendar.DATE, weekStartDays * -1);
		}
		else{
			int weekStartDays = (dow - 1) + (7 * prevNoOfWeeks);
			c1.add(Calendar.DATE, weekStartDays * -1);
		}
		String weekStartDate = sdf.format(c1.getTime());
		return weekStartDate;
	}
	
	public static String dateToString(Date date, String format)
	{
		java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(format);
		Calendar c1 = Calendar.getInstance();
		c1.setTime(date);
		String strDate = sdf.format(c1.getTime());
		return strDate;
	}
	
	public static String localDateToString(LocalDate localDate, String format)
	{
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(format);
		return localDate.format(formatter);
	}
	
	public static LocalDate stringToLocalDate(String localDate, String format)
	{
		java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(format);
		return LocalDate.parse(localDate, formatter);
	}
	
	public static String getWeekStartDate(int prevNoofWeeks)
	{
		return getWeekStartDate(new Date(), prevNoofWeeks);
	}

	public static String getWeekEndDate()
	{
		return getWeekEndDate(new Date());
	}

	public static String getWeekEndDate(Date currentDate)
	{
		return getWeekEndDate(currentDate, Constants.APP_DATE_FORMAT);
	}
	
	public static String getWeekEndDate(Date currentDate, String dateFormat)
	{
		java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(dateFormat);
		Calendar c1 = Calendar.getInstance();
		c1.setTime(currentDate);
		int dow = c1.get(Calendar.DAY_OF_WEEK);
		int startDayIndex = 0;
		if(PropertyManager.getProperty("CUSTOM_WEEK_START_INDEX") != null){
			startDayIndex = Integer.parseInt(PropertyManager.
					getProperty("CUSTOM_WEEK_START_INDEX"));
		}
		if(startDayIndex > 0){
			c1.setFirstDayOfWeek(startDayIndex + 1);
			
			if(PropertyManager.getProperty("CUSTOM_WEEK_END_INDEX") != null){
				int endDayIndex = Integer.parseInt(PropertyManager.
						getProperty("CUSTOM_WEEK_END_INDEX"));
				c1.set(Calendar.DAY_OF_WEEK, endDayIndex + 1);
			}
			
			//int weekStartDays = 7 * prevNoOfWeeks;
			//int weekEndDays = 7 - dow;
			//c1.add(Calendar.DATE, weekEndDays);
		}else{
			// if the day of week is 2, we need to make it 7 which as Sat
			int weekEndDays = 7 - dow;
			c1.add(Calendar.DATE, weekEndDays);
		}
		String weekEndDate = sdf.format(c1.getTime());
		return weekEndDate;
	}
        public static String getCurrentDateTime()
        {
        return format.format(new Date());
        }
	public static java.sql.Date toSQLDate(String strDate) throws GeneralException
	{
		java.util.Date javaDate = toDate( strDate);
		java.sql.Date sqlDate = new  java.sql.Date (javaDate.getTime());
		return sqlDate;
	}
	
	public static Date toDate(String strDate) throws GeneralException
	{
		return toDate(strDate, Constants.APP_DATE_FORMAT);
	}
	
	public static Date toDate(String strDate, String dateFormat) throws GeneralException
	{

		DateFormat df = new SimpleDateFormat(dateFormat);
		Date javaDate = null;
		try
		{
			javaDate = df.parse(strDate);
		}
		catch (ParseException e)
		{
			throw new GeneralException("Date Parsing exception ", e);
		}
		return javaDate;
	}
	
	public static LocalDate toDateAsLocalDate(String strDate) throws GeneralException {
		LocalDate javaDate = null;
		java.time.format.DateTimeFormatter df = java.time.format.DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
		df = df.withLocale(Locale.US);
		javaDate = LocalDate.parse(strDate, df);
		return javaDate;
	}

	public static java.util.Date incrementDate(java.util.Date date, int increment)
	{
		//String startdate = null;
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, increment);
		return cal.getTime();
	}

	
	public static String getDateFromCurrentDate(int increment)
	{
		return getDateFromCurrentDate(increment, Constants.APP_DATE_FORMAT);
	}

	public static String getDateFromCurrentDate(int increment, String dateFormat)
	{
		String startdate = null;
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
		cal.add(Calendar.DATE, increment);
		startdate = formatter.format(cal.getTime());
		return startdate;
	}
	
	public static long getDateDiff(java.sql.Date date1, java.sql.Date date2)
	{
		return (date1.getTime() - date2.getTime()) / (1000 * 60 * 60 * 24);
	}

	public static long getDateDiff(java.util.Date date1, java.util.Date date2)
	{
		return (date1.getTime() - date2.getTime()) / (1000 * 60 * 60 * 24);
	}
	
	public static long getDateDiff(String date1, String date2) throws ParseException
	{
		String format = Constants.APP_DATE_FORMAT;
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date dateObj1 = sdf.parse(date1);
		Date dateObj2 = sdf.parse(date2); 
		return (dateObj1.getTime() - dateObj2.getTime()) / (1000 * 60 * 60 * 24);
	}
	
	
	
	public static java.sql.Date addDays (java.sql.Date date, int days)
	{
		return new java.sql.Date(date.getTime() + days * DAY_MILLI_SECONDS);
	}
	
	private static final long DAY_MILLI_SECONDS = 1000 * 60 * 60 * 24;
	
	public static final String DATE_FORMAT_NOW = "yyyy-MM-dd-HHmmss";

	public static String now() {
	    Calendar cal = Calendar.getInstance();
	    SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
	    return sdf.format(cal.getTime());

	  }

	public static int  getdayofWeek(String strDate, String dateFormat) throws GeneralException 
	{
		Date inpDate = toDate(strDate, dateFormat);
		return getdayofWeek(inpDate);
	}
	
	public static int getdayofWeek(Date inpDate)
	{
		Calendar c1 = Calendar.getInstance();
		c1.setTime(inpDate);
		int dow = c1.get(Calendar.DAY_OF_WEEK);
		return dow;
	}
	
	public static Date get13WeekPriorStartDate (Date date)
	{
		return getPriorWeekStartDate (date, 13);
	}

	public static Date getPreviousWeekStartDate (Date date,int noOfweeksBack)
	{
		
		return getPriorWeekStartDate (date, noOfweeksBack);
	}

	public static Date getPriorWeekStartDate (Date date, int numberOfWeeksPrior)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, - 7 * (numberOfWeeksPrior - 1));
		Date firstStartDate = cal.getTime();
		return firstStartDate;
	}
	
	public static String getDayOfWeek( int dow){
		String dayOfWeek = "";
		switch (dow){
		case 1:
			dayOfWeek = "Sunday";
			break;
		case 2:
			dayOfWeek = "Monday";
			break;
		case 3:
			dayOfWeek = "Tuesday";
			break;
		case 4:
			dayOfWeek = "Wednesday";
			break;
		case 5:
			dayOfWeek = "Thursday";
			break;
		case 6:
			dayOfWeek = "Friday";
			break;
		case 7:
			dayOfWeek = "Saturday";
			break;
		}
		return dayOfWeek;

	}
	
	
	public static String DataValidation(String strDate) throws GeneralException 
	{
		String returnVal = "";
		try
		{
		final java.text.SimpleDateFormat datfor = new java.text.SimpleDateFormat("dd-MM-yyyy");
		datfor.setLenient(false);
		final java.text.ParsePosition pos = new java.text.ParsePosition(0);
		final java.util.Date date = datfor.parse(strDate, pos);
		Pattern datePatt = Pattern.compile("([0-9]{2})-([0-9]{2})-([0-9]{4})");
		Matcher m = datePatt.matcher(strDate);
		if ((date == null) || (pos.getErrorIndex() != -1) || m.matches()==false) {

			if (date == null) {
				returnVal = "Invalid";
			}
			if (pos.getErrorIndex() != -1) {
				returnVal = "Invalid";
			}
			if (m.matches()==false) {
				returnVal = "Invalid";
			}
			returnVal = "InvalidstrDate";
		}
		else
		{
			
			returnVal=strDate;
		}
		}
		catch(Exception exe)
		{
			 throw new GeneralException("Date Parsing Exception");
		}
		return returnVal;
		 
	}
	
	/*
	 *****************************************************************
	 * Find the Date Difference
 	 * Argument 1:  Startdate
 	 * Argument 2: enddate
 	 * Retuen  invalid or ok 
 	 * @throws  ParseException
 	 ***************************************************************** 	 
 	 */

	public static String DateDifference(String Date1, String Date2) throws ParseException {
		 
		String Result="";
		String format = "dd-MM-yyyy"; 
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date dateObj1 = sdf.parse(Date1);
		Date dateObj2 = sdf.parse(Date2); 
		long diff = dateObj2.getTime() - dateObj1.getTime(); 
		int diffDays =  (int) (diff / (24* 1000 * 60 * 60)); 
		if(diffDays <0)
		{
			Result="Invalid";
			
		}
		else 
		{
		Result="";
		}
		return Result;
	}

	public static String FindYear(String startDate,String endDate) {
		
		 String[] arrayYear=startDate.split("-");
		 
         return arrayYear[2];
	}
    
	/*
	 *****************************************************************
	 * Get the Next day for the given date
 	 * Argument 1:  Startdate
 	 * Argument 2: enddate
 	 * Retuen Nextdate or invalid
 	 * @throws  ParseException
 	 ***************************************************************** 	 
 	 */
	
	public static String GetNextDay(String startdate, String enddate) throws ParseException {
		 
		  SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
		  
		  Date sdate=dateFormat.parse(startdate);
		  // get the next date
		  
		  Calendar calendar = Calendar.getInstance();
		  calendar.setTime(sdate);
		  calendar.add(Calendar.DATE, 1);
		  
		  String nextDate = dateFormat.format(calendar.getTime());
	 	  
		  // find the date difference
		  
		  if(DateDifference(nextDate,enddate).equalsIgnoreCase("Invalid"))
	 	 		  return "Invalid";
	 	   else
	 	         return nextDate;

	}
	
	/*
	 *****************************************************************
	 * Get the Next week for the given date
 	 * Argument 1:  Startdate
 	 * Argument 2: enddate
 	 * Retuen Nextdate or invalid
 	 * @throws  ParseException
 	 ***************************************************************** 	 
 	 */
	

	public static String getNextWeek(String Startdate, String endDate) throws ParseException {
		
		if(Startdate!=null && Startdate.equalsIgnoreCase("Invalid"))
		{
		return "Invalid";	
		}
		else
		{
		  // int TotalDays = 6 *(MILLIS_IN_DAY);	
	 	  SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
		  Date sdate=dateFormat.parse(Startdate);
		  // find the next week
		  Calendar calendar = Calendar.getInstance();
		  calendar.setTime(sdate);
		  calendar.add(Calendar.DATE, 6);
		  
		  String nextweak = dateFormat.format(calendar.getTime());
	 	   return nextweak;
		}
	}

	public static String getNextPeriod(String processdate, String endDate) throws ParseException {
		
		System.out.println("=-====Period"+processdate);
		System.out.println("------End date"+endDate);
		 
		if(processdate!=null && processdate.equalsIgnoreCase("Invalid"))
		{
		return "Invalid";	
		}
		else
		{
		  // long  MILLIS_IN_first = (14) *(MILLIS_IN_DAY);
		  // long Mile_in_second=(13) * (MILLIS_IN_DAY);
			
			
 	      SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
		  Date sdate=dateFormat.parse(processdate);
		  
		  Calendar calendar = Calendar.getInstance();
		  calendar.setTime(sdate);
		  calendar.add(Calendar.DATE, 27);
		  
		  String nextPeriod = dateFormat.format(calendar.getTime());
		  if(DateDifference(nextPeriod,endDate).equalsIgnoreCase("Invalid"))
		  return "Invalid";
		  else
	      return nextPeriod;
		}
		
	}
	
	
	
	public static String getNextPeriodFromConfig(String processdate, String endDate, 
			CalendarYearlyDTO calDTO, int periodCount) throws ParseException {
			
			System.out.println("=-====Period"+processdate);
			System.out.println("------End date"+endDate);
			 
			if(processdate!=null && processdate.equalsIgnoreCase("Invalid"))
			{
				return "Invalid";	
			}
			else
			{
			  // long  MILLIS_IN_first = (14) *(MILLIS_IN_DAY);
			  // long Mile_in_second=(13) * (MILLIS_IN_DAY);
			int daysCount = 0;
			switch(periodCount){
				case 1:
					daysCount = (Integer.parseInt(calDTO.getPeriod1()) * 7) - 1;
					break;
				case 2:
					daysCount = (Integer.parseInt(calDTO.getPeriod2()) * 7) - 1;
					break;
				case 3:
					daysCount = (Integer.parseInt(calDTO.getPeriod3()) * 7) - 1;
					break;
				case 4:
					daysCount = (Integer.parseInt(calDTO.getPeriod4()) * 7) - 1;
					break;
				case 5:
					daysCount = (Integer.parseInt(calDTO.getPeriod5()) * 7) - 1;
					break;
				case 6:
					daysCount = (Integer.parseInt(calDTO.getPeriod6()) * 7) - 1;
					break;
				case 7:
					daysCount = (Integer.parseInt(calDTO.getPeriod7()) * 7) - 1;
					break;
				case 8:
					daysCount = (Integer.parseInt(calDTO.getPeriod8()) * 7) - 1;
					break;
				case 9:
					daysCount = (Integer.parseInt(calDTO.getPeriod9()) * 7) - 1;
					break;
				case 10:
					daysCount = (Integer.parseInt(calDTO.getPeriod10()) * 7) - 1;
					break;
				case 11:
					daysCount = (Integer.parseInt(calDTO.getPeriod11()) * 7) - 1;
					break;
				case 12:
					daysCount = (Integer.parseInt(calDTO.getPeriod12()) * 7) - 1;
					break;
				case 13:
					daysCount = (Integer.parseInt(calDTO.getPeriod13()) * 7) - 1;
					break;
				default:
					daysCount = 27;
			}
			  SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
			  Date sdate=dateFormat.parse(processdate);
			  
			  Calendar calendar = Calendar.getInstance();
			  calendar.setTime(sdate);
			  calendar.add(Calendar.DATE, daysCount);
			  
			  String nextPeriod = dateFormat.format(calendar.getTime());
			  if(DateDifference(nextPeriod,endDate).equalsIgnoreCase("Invalid"))
			  return "Invalid";
			  else
		      return nextPeriod;
			}
			
		}

	public static String getQuarter(String processdate, String firstQuarter, String endDate) throws ParseException {
		
		if(processdate!=null && processdate.equalsIgnoreCase("Invalid"))
		{
		return "Invalid";	
		}
		else
		{
		// long a=0,b=0,c=0,d=0,e=0,f=0;
			int addDate = 0;
        int period=Integer.parseInt(firstQuarter);
		if(period==4)
		{
			/*a=(20) *(MILLIS_IN_DAY);
			b=(20) *(MILLIS_IN_DAY);
			c=(20) *(MILLIS_IN_DAY);
			d=(20) *(MILLIS_IN_DAY);
			e=(20) *(MILLIS_IN_DAY);
			f=(11) * (MILLIS_IN_DAY);*/
			addDate = 111;
		}
		else if(period==3)
		{
			/*a=(20) *(MILLIS_IN_DAY);
			b=(20) *(MILLIS_IN_DAY);
			c=(20) *(MILLIS_IN_DAY);
			d=(3) *(MILLIS_IN_DAY);
			e=(20) *(MILLIS_IN_DAY);
			f=0;*/
			addDate = 83;
		}
	     SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
		  Date sdate=dateFormat.parse(processdate);
		  
		  Calendar calendar = Calendar.getInstance();
		  calendar.setTime(sdate);
		  calendar.add(Calendar.DATE, addDate);
		  
		  String NextQuarter = dateFormat.format(calendar.getTime());
		  if(DateDifference(NextQuarter,endDate).equalsIgnoreCase("Invalid"))
			  return "Invalid";
		  else
		 	 return NextQuarter;
		}
	}
	
	public static String getQuarterForFiveWeeks(String processdate, 
			String firstQuarter, String endDate, CalendarYearlyDTO calendarDto, int quarterNo) throws ParseException {
		
		if(processdate!=null && processdate.equalsIgnoreCase("Invalid"))
		{
			return "Invalid";	
		}
		else
		{
			int addDate = 0;
			if(quarterNo == 1){
				addDate = (calendarDto.getQuater1Weeks() * 7) - 1;
			}else if(quarterNo == 2){
				addDate = (calendarDto.getQuater2Weeks() * 7) - 1;
			}else if(quarterNo == 3){
				addDate = (calendarDto.getQuater3Weeks() * 7) - 1;
			}else if(quarterNo == 4){
				addDate = (calendarDto.getQuater4Weeks() * 7) - 1;
		}
        
		
	     SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
		  Date sdate=dateFormat.parse(processdate);
		  
		  Calendar calendar = Calendar.getInstance();
		  calendar.setTime(sdate);
		  calendar.add(Calendar.DATE, addDate);
		  
		  String NextQuarter = dateFormat.format(calendar.getTime());
		  if(DateDifference(NextQuarter,endDate).equalsIgnoreCase("Invalid"))
			  return "Invalid";
		  else
		 	 return NextQuarter;
		}
	}

	public static String YearDifference(String Date1, String Date2) {
		String Result="";
		try {
			 
		String format = "dd-MM-yyyy"; 
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date dateObj1 = sdf.parse(Date1);
		Date dateObj2 = sdf.parse(Date2); 
		long diff = dateObj2.getTime() - dateObj1.getTime(); 
		int diffDays =  (int) (diff / (24* 1000 * 60 * 60));
		System.out.println("----Date Different"+diffDays);
		if(diffDays < 363 || diffDays > 363)
		{
			Result="Invalid";
			
		}
		else 
		{
		Result="";
		}
		} catch (ParseException e) {
		 
			e.printStackTrace();
		} 
		return Result;
		
	 
		 
	}
	
	public static java.util.Date getPureDate (int month, int day, int year)
	{
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MONTH, month-1);
		cal.set(Calendar.DAY_OF_MONTH, day);
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	public static java.util.Date getPureDate (java.util.Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	public static java.util.Date getNextSunday (java.util.Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DATE, 7);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTime();
	}

	public static java.util.Date getEndOfSaturday (java.util.Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
		cal.set(Calendar.HOUR, 23);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		cal.set(Calendar.MILLISECOND, 999);
		return cal.getTime();
	}

	public static java.util.Date getSunday (java.util.Date date)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		return cal.getTime();
	}

	public static DateTimeFormatter getDateFormatter() {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(APP_DATE_FORMAT);
		return formatter;
	}

	public static String getSpecificDate(int noOfDays) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(APP_DATE_FORMAT);
		Calendar c = Calendar.getInstance();		
		c.add(Calendar.DAY_OF_MONTH, noOfDays);
		String specificDate = dateFormat.format(c.getTime());
		return specificDate;
	}
	
	public static String getRecentDate(String str1, String str2) throws GeneralException {
		Date date1 = toDate(str1);
		Date date2 = toDate(str2);
		String recentDate = "";
		int comparisonVal = date1.compareTo(date2);
		if (comparisonVal < 0) {
//			System.out.println("Date 2 occurs after Date 1");
			recentDate = str1;
		} else if (comparisonVal > 0) {
//			System.out.println("Date 1 occurs after Date 2");
			recentDate = str2;
		} else {
//			System.out.println("Both dates are equal");
			recentDate = str1;
		}
		return recentDate;
	}
	
}
