package com.pristine.service;

import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;

public class RetailCalendarPromoCacheService {

	private static Logger logger = Logger.getLogger("RetailCalendarPromoCacheService");
	
	private List<RetailCalendarDTO> retailCalendarPromoCache = null;
	
	public RetailCalendarPromoCacheService(List<RetailCalendarDTO> retailCalendarPromoCache) {
		this.retailCalendarPromoCache = retailCalendarPromoCache;
	}

	public RetailCalendarDTO getDayCalendarOfWeekStartDate(int weekNo, int calYear) {
		List<RetailCalendarDTO> weekRows = getWeekRows(calYear);
		sortByWeekStartDate(weekRows);
		logger.debug("weekNo:" +weekNo);
		String weekStartDate = weekRows.get(weekNo - 1).getStartDate();
		logger.debug("weekStartDate:" +weekStartDate);
		RetailCalendarDTO dayCalendarDTO = getDayCalendarId(weekStartDate, calYear);
		return dayCalendarDTO;
	}

	public RetailCalendarDTO getDayCalendarOfWeekEndDate(int weekNo, int calYear) {
		List<RetailCalendarDTO> weekRows = getWeekRows(calYear);
		sortByWeekStartDate(weekRows);
		String weekEndDate = weekRows.get(weekNo - 1).getEndDate();
		RetailCalendarDTO dayCalendarDTO = getDayCalendarId(weekEndDate, calYear);
		return dayCalendarDTO;
	}
	
	private List<RetailCalendarDTO> getWeekRows(int calYear) {
		return retailCalendarPromoCache.stream().filter((r) -> r.getRowType().equals(Constants.CALENDAR_WEEK) && r.getCalYear() == calYear)
				.collect(Collectors.toList());
	}
	
	private void sortByWeekStartDate(List<RetailCalendarDTO> weekRows) {
		Comparator<RetailCalendarDTO> dateComparator = (o1, o2) -> o1.getStartDateAsDate().compareTo(o2.getStartDateAsDate());
		weekRows.sort(dateComparator);
	}
	
	public RetailCalendarDTO getDayCalendarId(String date, int calYear) {
		return retailCalendarPromoCache.stream()
				.filter((r) -> r.getRowType().equals(Constants.CALENDAR_DAY) && r.getStartDate().equals(date) && r.getCalYear() == calYear)
				.findFirst().orElse(null);
	}
	
	public RetailCalendarDTO getWeekCalendarId(String date) {
		return retailCalendarPromoCache.stream()
				.filter((r) -> r.getRowType().equals(Constants.CALENDAR_WEEK) && r.getStartDate().equals(date))
				.findFirst().orElse(null);
	}
	//added for Peapod promoLoading
	public RetailCalendarDTO getWeekCalendar(String date) {

		LocalDate localDate = LocalDate.parse(date, PRCommonUtil.getDateFormatter());

		return retailCalendarPromoCache.stream()
				.filter((r) -> r.getRowType().equals(Constants.CALENDAR_WEEK)
						&& (r.getStartDateAsDate().isEqual(localDate) || r.getStartDateAsDate().isBefore(localDate))
						&& (r.getEndDateAsDate().isEqual(localDate) || r.getEndDateAsDate().isAfter(localDate)))
				.findFirst().orElse(null);
	}
	
	
}
