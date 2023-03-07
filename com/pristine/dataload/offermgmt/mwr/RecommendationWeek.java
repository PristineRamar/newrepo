package com.pristine.dataload.offermgmt.mwr;

import java.sql.Connection;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class RecommendationWeek {

	private static Logger logger = Logger.getLogger("RecommendationWeek");

	CommonDataHelper commonDataHelper;
	RetailCalendarDAO retailCalendarDAO;
	Connection conn;
	public RecommendationWeek(CommonDataHelper commonDataHelper, Connection conn) {

		this.commonDataHelper = commonDataHelper;
		retailCalendarDAO = new RetailCalendarDAO();
		this.conn = conn;

	}

	/**
	 * Sets recommedation week range and calendar ids
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	public void setupRecommendationWeeks(RecommendationInputDTO recommendationInputDTO) throws GeneralException {

		HashMap<String, RetailCalendarDTO> calDetails = commonDataHelper.getAllWeekCalendarDetails();

		setupRecommendationWeeks(recommendationInputDTO, calDetails);
	}

	/**
	 * 
	 * @param recommendationInputDTO
	 * @param calDetails
	 * @throws GeneralException
	 */
	public void setupRecommendationWeeks(RecommendationInputDTO recommendationInputDTO,
			HashMap<String, RetailCalendarDTO> calDetails) throws GeneralException {

		if (PRConstants.MW_QUARTER_RECOMMENDATION.equals(recommendationInputDTO.getRecType())) {

			// For quarter recommendation. Either past or future quarter
			setQuarterRecommendation(recommendationInputDTO, calDetails);

		} else if (PRConstants.MW_X_WEEKS_RECOMMENDATION.equals(recommendationInputDTO.getRecType())) {

			// For X weeks recommendation. Either past or future weeks
			setXWeeksRecommendation(recommendationInputDTO, calDetails);
		} else if (PRConstants.MW_WEEK_RECOMMENDATION.equals(recommendationInputDTO.getRecType())) {
			
			// For weekly recommendation
			setWeeklyRecommendation(recommendationInputDTO, calDetails);
		}
	}

	/**
	 * Sets X weeks recommendation
	 * 
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */

	private void setXWeeksRecommendation(RecommendationInputDTO recommendationInputDTO,
			HashMap<String, RetailCalendarDTO> calDetails) throws GeneralException {

		String curWkStartDate = DateUtil.getWeekStartDate(-1);

		Date currentStartDate = DateUtil.toDate(curWkStartDate);

		Date quarterStartDate = recommendationInputDTO.getQuarterStartDate() != null
				? DateUtil.toDate(recommendationInputDTO.getQuarterStartDate()) : null;

		Date quarterEndDate = recommendationInputDTO.getQuarterEndDate() != null
				? DateUtil.toDate(recommendationInputDTO.getQuarterEndDate()) : null;

		Date recStartWeek = recommendationInputDTO.getStartWeek() != null
				? DateUtil.toDate(recommendationInputDTO.getStartWeek()) : null;

		if (recommendationInputDTO.getQuarterStartDate() != null && recommendationInputDTO.getQuarterEndDate() != null
				&& currentStartDate.after(quarterStartDate) && currentStartDate.before(quarterEndDate)) {

			// 4. For future on-going quarter – use Quarter Start Week and Quarter End Week and “Recommendation Start Week” and
			// “Recommendation End Week”, “No of Weeks in Advance” to identity the base week. If “Recommendation Start Week” and
			// “Recommendation End Week” is not mentioned, then identify it using “No of Weeks in Advance” from current week
			recommendationInputDTO.setOnGoingDateRange(true);
			setOnGoingQuarterRemainingWeeks(currentStartDate, curWkStartDate, recommendationInputDTO, calDetails);

		} else if (recommendationInputDTO.getQuarterStartDate() != null
				&& recommendationInputDTO.getQuarterEndDate() != null && recommendationInputDTO.getStartWeek() != null
				&& recommendationInputDTO.getEndWeek() != null && currentStartDate.after(quarterStartDate)
				&& currentStartDate.after(quarterEndDate)) {

			// 6. For remaining weeks in the past quarter – use Quarter Start Week and Quarter End Week and “Recommendation Start
			// Week” and “Recommendation End Week”, “No of Weeks in Advance” to identity the base week

			recommendationInputDTO.setOnGoingDateRange(true);
			setPastQuarterRemainingWeeks(recommendationInputDTO, calDetails);

		} else if (recommendationInputDTO.getStartWeek() != null && recommendationInputDTO.getEndWeek() != null
				&& currentStartDate.before(recStartWeek)) {

			// 1. For X weeks recommendation (future) use the “Recommendation Start Week” and “Recommendation End Week”.
			// Current week will be the base week
			setFutureXWeeksRecommendation(currentStartDate, curWkStartDate, recommendationInputDTO, calDetails);

		} else if (recommendationInputDTO.getStartWeek() != null && recommendationInputDTO.getEndWeek() != null
				&& currentStartDate.after(recStartWeek)) {

			// 2. For X weeks recommendation (past) use the “Recommendation Start Week” and “Recommendation End Week”, “No of
			// Weeks in Advance” to identity the base week
			setPastXWeeksRecommendation(currentStartDate, curWkStartDate, recommendationInputDTO, calDetails);

		}
	}

	/**
	 * Sets quarter recommendation
	 * 
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setQuarterRecommendation(RecommendationInputDTO recommendationInputDTO,
			HashMap<String, RetailCalendarDTO> calDetails) throws GeneralException {

		int week=Integer.parseInt(PropertyManager.getProperty("WEEK_START", "0"));
		
		String curWkStartDate = DateUtil.getWeekStartDate(week);

		Date currentStartDate = DateUtil.toDate(curWkStartDate);

		logger.info("Curr. Start Date: " + curWkStartDate);
		
		Date quarterStartDate = recommendationInputDTO.getQuarterStartDate() != null
				? DateUtil.toDate(recommendationInputDTO.getQuarterStartDate()) : null;

		Date quarterEndDate = recommendationInputDTO.getQuarterEndDate() != null
				? DateUtil.toDate(recommendationInputDTO.getQuarterEndDate()) : null;

		if(recommendationInputDTO.getQuarterStartDate() != null && recommendationInputDTO.getQuarterEndDate() != null) {
			
			if (quarterEndDate.after(currentStartDate) && quarterStartDate.after(currentStartDate)) {

				// 3. For future quarter – use Quarter Start Week and Quarter End Week. Current week will be the base week
				setFutureQuarterRecommendation(curWkStartDate, recommendationInputDTO, calDetails);

			} else if ((quarterEndDate.after(currentStartDate) && quarterStartDate.equals(currentStartDate))
					|| (quarterEndDate.after(currentStartDate) && quarterStartDate.before(currentStartDate))) {

				// 1. For X weeks recommendation (future) use the “Recommendation Start Week” and “Recommendation End Week”.
				// Current week will be the base week
				setFutureXWeeksRecommendation(currentStartDate, curWkStartDate, recommendationInputDTO, calDetails);
			} else if (quarterStartDate.before(currentStartDate) && quarterEndDate.before(currentStartDate)) {

				// 5. For past quarter – use Quarter Start Week and Quarter End Week. Use “No of Weeks in Advance” to identity the
				// base week
				setPastQuarterRecommendation(recommendationInputDTO, calDetails);

			}
		} else {
			
			Date recStartWeek = DateUtil.incrementDate(currentStartDate, (recommendationInputDTO.getNoOfWeeksInAdvance() * 7));
			
			String startWeek = DateUtil.dateToString(recStartWeek, Constants.APP_DATE_FORMAT);
			
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			RetailCalendarDTO retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, startWeek, Constants.CALENDAR_QUARTER);
		
			
			recommendationInputDTO.setQuarterStartDate(retailCalendarDTO.getStartDate());
			recommendationInputDTO.setQuarterEndDate(retailCalendarDTO.getEndDate());
			
			RetailCalendarDTO actualStartWeekDTO = calDetails.get(recommendationInputDTO.getQuarterStartDate());

			// End week calendar id
			RetailCalendarDTO endWeekDTO = calDetails.get(recommendationInputDTO.getQuarterEndDate());
			
			
			// End week calendar id
			RetailCalendarDTO startWeekDTO = calDetails.get(startWeek);
			
			setCalendarIds(startWeekDTO.getCalendarId(), endWeekDTO.getCalendarId(), actualStartWeekDTO.getCalendarId(),
					endWeekDTO.getCalendarId(), recommendationInputDTO);

			recommendationInputDTO.setStartWeek(startWeek);
			recommendationInputDTO.setEndWeek(recommendationInputDTO.getQuarterEndDate());
			recommendationInputDTO.setActualStartWeek(recommendationInputDTO.getQuarterStartDate());
			recommendationInputDTO.setActualEndWeek(recommendationInputDTO.getQuarterEndDate());
			
			recommendationInputDTO.setBaseWeek(curWkStartDate);
			
			// start week calendar id of remaining weeks
			logger.info("setQuarterRecommendation() - Rec Details: " + recommendationInputDTO.toString());
		}
	}

	/**
	 * 
	 * @param conn
	 * @param dateAfterXWeeks
	 * @param dateAfterXWeeksStr
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setFutureXWeeksRecommendation(Date currentStartDate, String curWkStartDate,
			RecommendationInputDTO recommendationInputDTO, HashMap<String, RetailCalendarDTO> calDetails)
					throws GeneralException {
		// start week calendar id of remaining weeks
		logger.debug("Quarter start: " + recommendationInputDTO.getQuarterStartDate());
		RetailCalendarDTO actualStartWeekDTO = calDetails.get(recommendationInputDTO.getQuarterStartDate());

		// End week calendar id
		RetailCalendarDTO endWeekDTO = calDetails.get(recommendationInputDTO.getQuarterEndDate());
		
		
		Date qStartDate = DateUtil.toDate(recommendationInputDTO.getQuarterStartDate());
		
		Date recStartWeek = DateUtil.incrementDate(qStartDate, (recommendationInputDTO.getNoOfWeeksInAdvance() * 7) * -1);
		
		String startWeek = DateUtil.dateToString(recStartWeek, Constants.APP_DATE_FORMAT);
		
		// End week calendar id
		RetailCalendarDTO startWeekDTO = calDetails.get(startWeek);
		
		setCalendarIds(startWeekDTO.getCalendarId(), endWeekDTO.getCalendarId(), actualStartWeekDTO.getCalendarId(),
				endWeekDTO.getCalendarId(), recommendationInputDTO);

		recommendationInputDTO.setStartWeek(startWeek);
		recommendationInputDTO.setEndWeek(recommendationInputDTO.getQuarterEndDate());
		recommendationInputDTO.setActualStartWeek(recommendationInputDTO.getQuarterStartDate());
		recommendationInputDTO.setActualEndWeek(recommendationInputDTO.getQuarterEndDate());
		
		recommendationInputDTO.setBaseWeek(curWkStartDate);
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param dateAfterXWeeks
	 * @param dateAfterXWeeksStr
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setWeeklyRecommendation(RecommendationInputDTO recommendationInputDTO,
			HashMap<String, RetailCalendarDTO> calDetails) throws GeneralException {
		
		String reccWeekStartDate = DateUtil.getWeekStartDate(-1);
		String curWeekStartDate = DateUtil.getWeekStartDate(0);
				
		// End week calendar id
		RetailCalendarDTO reccStartWeekDTO = calDetails.get(reccWeekStartDate);
		
		
		setCalendarIds(reccStartWeekDTO.getCalendarId(), reccStartWeekDTO.getCalendarId(), reccStartWeekDTO.getCalendarId(),
				reccStartWeekDTO.getCalendarId(), recommendationInputDTO);

		recommendationInputDTO.setStartWeek(reccWeekStartDate);
		recommendationInputDTO.setEndWeek(reccStartWeekDTO.getEndDate());
		recommendationInputDTO.setActualStartWeek(reccWeekStartDate);
		recommendationInputDTO.setActualEndWeek(reccStartWeekDTO.getEndDate());
		
		recommendationInputDTO.setBaseWeek(curWeekStartDate);
	}

	/**
	 * 
	 * @param conn
	 * @param dateAfterXWeeks
	 * @param dateAfterXWeeksStr
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setPastXWeeksRecommendation(Date currentStartDate, String curWkStartDate,
			RecommendationInputDTO recommendationInputDTO, HashMap<String, RetailCalendarDTO> calDetails)
					throws GeneralException {
		// start week calendar id of remaining weeks
		RetailCalendarDTO startWeekDTO = calDetails.get(recommendationInputDTO.getStartWeek());

		// End week calendar id
		RetailCalendarDTO endWeekDTO = calDetails.get(recommendationInputDTO.getEndWeek());

		setCalendarIds(startWeekDTO.getCalendarId(), endWeekDTO.getCalendarId(), startWeekDTO.getCalendarId(),
				endWeekDTO.getCalendarId(), recommendationInputDTO);

		Date recStartWeek = DateUtil.toDate(recommendationInputDTO.getStartWeek());

		Date baseWeek = DateUtil.incrementDate(recStartWeek, (recommendationInputDTO.getNoOfWeeksInAdvance() * 7) * -1);

		recommendationInputDTO.setBaseWeek(DateUtil.dateToString(baseWeek, Constants.APP_DATE_FORMAT));
	}

	/**
	 * 
	 * @param conn
	 * @param dateAfterXWeeks
	 * @param dateAfterXWeeksStr
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setFutureQuarterRecommendation(String curWkStartDate, RecommendationInputDTO recommendationInputDTO,
			HashMap<String, RetailCalendarDTO> calDetails) throws GeneralException {
		// start week calendar id of remaining weeks
		RetailCalendarDTO startWeekDTO = calDetails.get(recommendationInputDTO.getQuarterStartDate());

		// End week calendar id
		RetailCalendarDTO endWeekDTO = calDetails.get(recommendationInputDTO.getQuarterEndDate());

		setCalendarIds(startWeekDTO.getCalendarId(), endWeekDTO.getCalendarId(), startWeekDTO.getCalendarId(),
				endWeekDTO.getCalendarId(), recommendationInputDTO);

		recommendationInputDTO.setBaseWeek(curWkStartDate);
		
		recommendationInputDTO.setStartWeek(recommendationInputDTO.getQuarterStartDate());
		recommendationInputDTO.setEndWeek(recommendationInputDTO.getQuarterEndDate());
		recommendationInputDTO.setActualStartWeek(recommendationInputDTO.getQuarterStartDate());
		recommendationInputDTO.setActualEndWeek(recommendationInputDTO.getQuarterEndDate());
	}

	/**
	 * 
	 * @param conn
	 * @param dateAfterXWeeks
	 * @param dateAfterXWeeksStr
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setPastQuarterRecommendation(RecommendationInputDTO recommendationInputDTO,
			HashMap<String, RetailCalendarDTO> calDetails) throws GeneralException {
		// start week calendar id of remaining weeks
		RetailCalendarDTO startWeekDTO = calDetails.get(recommendationInputDTO.getQuarterStartDate());

		// End week calendar id
		RetailCalendarDTO endWeekDTO = calDetails.get(recommendationInputDTO.getQuarterEndDate());

		setCalendarIds(startWeekDTO.getCalendarId(), endWeekDTO.getCalendarId(), startWeekDTO.getCalendarId(),
				endWeekDTO.getCalendarId(), recommendationInputDTO);

		Date recStartWeek = DateUtil.toDate(recommendationInputDTO.getQuarterStartDate());

		Date baseWeek = DateUtil.incrementDate(recStartWeek, (recommendationInputDTO.getNoOfWeeksInAdvance() * 7) * -1);

		recommendationInputDTO.setBaseWeek(DateUtil.dateToString(baseWeek, Constants.APP_DATE_FORMAT));
		
		recommendationInputDTO.setStartWeek(recommendationInputDTO.getQuarterStartDate());
		recommendationInputDTO.setEndWeek(recommendationInputDTO.getQuarterEndDate());
		recommendationInputDTO.setActualStartWeek(recommendationInputDTO.getQuarterStartDate());
		recommendationInputDTO.setActualEndWeek(recommendationInputDTO.getQuarterEndDate());
	}

	/**
	 * 
	 * @param conn
	 * @param currentQuarter
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setOnGoingQuarterRemainingWeeks(Date currentStartDate, String curWkStartDate,
			RecommendationInputDTO recommendationInputDTO, HashMap<String, RetailCalendarDTO> calDetails)
					throws GeneralException {

		if (recommendationInputDTO.getStartWeek() == null) {

			Date startWeek = DateUtil.incrementDate(currentStartDate, (recommendationInputDTO.getNoOfWeeksInAdvance() * 7));

			recommendationInputDTO.setStartWeek(DateUtil.dateToString(startWeek, Constants.APP_DATE_FORMAT));

			recommendationInputDTO.setEndWeek(recommendationInputDTO.getQuarterEndDate());

			recommendationInputDTO.setBaseWeek(curWkStartDate);

		} else {

			Date startWeek = DateUtil.toDate(recommendationInputDTO.getStartWeek());

			Date baseWeek = DateUtil.incrementDate(startWeek, (recommendationInputDTO.getNoOfWeeksInAdvance() * 7) * -1);

			recommendationInputDTO.setBaseWeek(DateUtil.dateToString(baseWeek, Constants.APP_DATE_FORMAT));

		}

		// start week calendar id of remaining weeks
		RetailCalendarDTO startWeekDTO = calDetails.get(recommendationInputDTO.getStartWeek());

		// End week calendar id
		RetailCalendarDTO endWeekDTO = calDetails.get(recommendationInputDTO.getEndWeek());

		// start week calendar id of remaining weeks
		RetailCalendarDTO acutalStartWeekDTO = calDetails.get(recommendationInputDTO.getQuarterStartDate());

		// End week calendar id
		RetailCalendarDTO acutalEndWeekDTO = calDetails.get(recommendationInputDTO.getQuarterEndDate());

		setCalendarIds(startWeekDTO.getCalendarId(), endWeekDTO.getCalendarId(), acutalStartWeekDTO.getCalendarId(),
				acutalEndWeekDTO.getCalendarId(), recommendationInputDTO);

	}

	/**
	 * 
	 * @param conn
	 * @param currentQuarter
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setPastQuarterRemainingWeeks(RecommendationInputDTO recommendationInputDTO,
			HashMap<String, RetailCalendarDTO> calDetails) throws GeneralException {

		Date startWeek = DateUtil.toDate(recommendationInputDTO.getStartWeek());

		Date baseWeek = DateUtil.incrementDate(startWeek, (recommendationInputDTO.getNoOfWeeksInAdvance() * 7) * -1);

		recommendationInputDTO.setBaseWeek(DateUtil.dateToString(baseWeek, Constants.APP_DATE_FORMAT));

		// start week calendar id of remaining weeks
		RetailCalendarDTO startWeekDTO = calDetails.get(recommendationInputDTO.getStartWeek());

		// End week calendar id
		RetailCalendarDTO endWeekDTO = calDetails.get(recommendationInputDTO.getEndWeek());

		// start week calendar id of remaining weeks
		RetailCalendarDTO acutalStartWeekDTO = calDetails.get(recommendationInputDTO.getQuarterStartDate());

		// End week calendar id
		RetailCalendarDTO acutalEndWeekDTO = calDetails.get(recommendationInputDTO.getQuarterEndDate());

		setCalendarIds(startWeekDTO.getCalendarId(), endWeekDTO.getCalendarId(), acutalStartWeekDTO.getCalendarId(),
				acutalEndWeekDTO.getCalendarId(), recommendationInputDTO);

	}

	/**
	 * 
	 * @param startCalendarId
	 * @param endCalendarId
	 * @param acutalStartCalendarId
	 * @param actualEndCalendarId
	 * @param recommendationInputDTO
	 */
	private void setCalendarIds(int startCalendarId, int endCalendarId, int acutalStartCalendarId,
			int actualEndCalendarId, RecommendationInputDTO recommendationInputDTO) {

		recommendationInputDTO.setStartCalendarId(startCalendarId);
		recommendationInputDTO.setEndCalendarId(endCalendarId);
		recommendationInputDTO.setActualStartCalendarId(acutalStartCalendarId);
		recommendationInputDTO.setActualEndCalendarId(actualEndCalendarId);

	}

	/**
	 * logs recommendation date range
	 */
	public void logRecWeeksDetail(RecommendationInputDTO recommendationInputDTO) {
		StringBuilder sb = new StringBuilder();
		logger.info("logRecTypeAndDateRange() - Multi Week Recommendation Inputs");

		sb.append("***Product and Location***\n");

		sb.append("Location Level Id: " + recommendationInputDTO.getLocationLevelId());
		sb.append("\t Location Id: " + recommendationInputDTO.getLocationId());
		sb.append("\t Product Level Id: " + recommendationInputDTO.getProductLevelId());
		sb.append("\t Product Id: " + recommendationInputDTO.getProductId());

		logger.info(sb.toString());

		sb = new StringBuilder();

		sb.append("***Multi week recommendation type***\n");

		if (PRConstants.MW_X_WEEKS_RECOMMENDATION.equals(recommendationInputDTO.getRecType())) {

			sb.append("Recommending for X Weeks: ");
			sb.append(
					"( " + recommendationInputDTO.getStartWeek() + " to " + recommendationInputDTO.getEndWeek() + " )");

		} else if (PRConstants.MW_QUARTER_RECOMMENDATION.equals(recommendationInputDTO.getRecType())
				|| Constants.EMPTY.equals(recommendationInputDTO.getRecType())) {

			sb.append("Recommending for Quarter: ");
			sb.append("( " + recommendationInputDTO.getQuarterStartDate() + " to "
					+ recommendationInputDTO.getQuarterEndDate() + " )");
		}
		
		sb.append("***Base Week:" + recommendationInputDTO.getBaseWeek());

		logger.info(sb.toString());
	}

}
