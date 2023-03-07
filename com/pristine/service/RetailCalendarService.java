/*
 * Title: Retail calendar service
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	14/03/2017	John Britto		Initial Version 
 **********************************************************************************
 */
package com.pristine.service;
import java.sql.Connection;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;


public class RetailCalendarService {

	private static Logger logger = Logger.getLogger("RetailCalendarService");
	
	/*
	 * *******************************************************************
	 * Method to get the day calendar details for the given date range 
	 * Argument 1: DB connection 
	 * Argument 2: Start Date 
	 * Argument 3: End Date 
	 * Argument 4: Date row type
	 * Return: List<RetailCalendarDTO>
	 * *******************************************************************
	 */	
	public List<RetailCalendarDTO> getDayCalendarList(Connection _Conn,
		Date Startdate, Date Enddate, String rowType) throws GeneralException {
		
		// Object for RetailCalendarDAO
		RetailCalendarDAO objCalendarDao = new RetailCalendarDAO();
		
		logger.debug("Call dayCalendarList");
		
		List<RetailCalendarDTO> dayCalList = objCalendarDao.dayCalendarList(_Conn,
				Startdate, Enddate, rowType);
		
		if (dayCalList != null && dayCalList.size() > 0)
		{}
		else
			logger.debug("Call calendar data found;");			
		
		return dayCalList;
	}
	
	/**
	 * Get week no for the week 
	 * @param retailCalendar
	 * @param weekStartDate
	 * @return
	 */
	public int getActualNo(List<RetailCalendarDTO> retailCalendar, String weekStartDate) {
		int actualNo = 0;

		RetailCalendarDTO retailCalendarDTO = retailCalendar.stream()
				.filter((r) -> r.getRowType().equals("W") && r.getStartDate().equals(weekStartDate)).findFirst().orElse(null);

		if(retailCalendarDTO != null) {
			actualNo = retailCalendarDTO.getActualNo();
		}
		
		return actualNo;
	}
	
	/**
	 * Return special day id
	 * @param retailCalendar
	 * @param startDate
	 * @return
	 * @throws GeneralException 
	 */
	public boolean isSpecialWeek(List<RetailCalendarDTO> retailCalendar, String weekStartDate) {
		boolean isSpecialWeek = false;

		LocalDate weekStart = DateUtil.stringToLocalDate(weekStartDate, Constants.APP_DATE_FORMAT);
		LocalDate weekEnd = weekStart.plusDays(6);

		for (RetailCalendarDTO r : retailCalendar) {
			if (r.getRowType().equals("D") && r.getSpecialDayId() > 0
					&& (DateUtil.stringToLocalDate(r.getStartDate(), Constants.APP_DATE_FORMAT).isEqual(weekStart)
							|| DateUtil.stringToLocalDate(r.getStartDate(), Constants.APP_DATE_FORMAT).isAfter(weekStart))
					&& (DateUtil.stringToLocalDate(r.getStartDate(), Constants.APP_DATE_FORMAT).isEqual(weekEnd)
							|| DateUtil.stringToLocalDate(r.getStartDate(), Constants.APP_DATE_FORMAT).isBefore(weekEnd))) {
				isSpecialWeek = true;
				break;
			}
		}

		return isSpecialWeek;
	}
	
	public int getWeekMaxActualNo(List<RetailCalendarDTO> retailCalendar, int year) {
		int maxActualNo = 0;
		
		// Define comparator
        Comparator<RetailCalendarDTO> comparator = (p1, p2) -> Integer.compare(p1.getActualNo(), p2.getActualNo());
 
		RetailCalendarDTO retailCalendarDTO = retailCalendar.stream().filter((r) -> r.getRowType().equals("W") && r.getCalYear() == year).max(comparator).get();

		if (retailCalendarDTO != null) {
			maxActualNo = retailCalendarDTO.getActualNo();
		}

		return maxActualNo;
	}
	
	public RetailCalendarDTO getWeekCalendarDetail(List<RetailCalendarDTO> retailCalendar, int actualNo, int year) {
		RetailCalendarDTO retailCalendarDTO = retailCalendar.stream().filter((r) -> r.getRowType().equals("W") && r.getCalYear() == year
				&& r.getActualNo() == actualNo).findFirst()
				.orElse(null);
		return retailCalendarDTO;
	}
	
	public RetailCalendarDTO getWeekCalendarDetail(List<RetailCalendarDTO> retailCalendar, String weekStartDate) {
		RetailCalendarDTO retailCalendarDTO = retailCalendar.stream().filter((r) -> r.getRowType().equals("W") && r.getStartDate().equals(weekStartDate))
				.findFirst().orElse(null);
		return retailCalendarDTO;
	}
	
	public HashMap<String, RetailCalendarDTO> getAllWeeks(List<RetailCalendarDTO> retailCalendar) {
		HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = new HashMap<String, RetailCalendarDTO>();

		List<RetailCalendarDTO> weeklyRetailCalendar = retailCalendar.stream().filter((r) -> r.getRowType().equals("W")).collect(Collectors.toList());

		for (RetailCalendarDTO r : weeklyRetailCalendar) {
			allWeekCalendarDetails.put(r.getStartDate(), r);
		}

		return allWeekCalendarDetails;
	}
}