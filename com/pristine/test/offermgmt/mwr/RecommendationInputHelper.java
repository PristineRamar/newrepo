package com.pristine.test.offermgmt.mwr;

import java.util.Date;
import java.util.HashMap;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;

public class RecommendationInputHelper {

	public static HashMap<String, RetailCalendarDTO> calDatails = new HashMap<>();
	/**
	 * 
	 * @param startWeek
	 * @param endWeek
	 * @param quarterStartDate
	 * @param recType
	 * @return recommedation input 
	 */
	public static RecommendationInputDTO getRecommendationInputDTO(String startWeek, String endWeek,
			String quarterStartDate, String quarterEndDate, String recType, int weeksInAdvance) {

		RecommendationInputDTO recommendationInputDTO = new RecommendationInputDTO();

		recommendationInputDTO.setStartWeek(startWeek);
		recommendationInputDTO.setEndWeek(endWeek);
		recommendationInputDTO.setActualStartWeek(startWeek);
		recommendationInputDTO.setActualEndWeek(endWeek);
		recommendationInputDTO.setQuarterStartDate(quarterStartDate);
		recommendationInputDTO.setQuarterEndDate(quarterEndDate);
		recommendationInputDTO.setRecType(recType);
		recommendationInputDTO.setNoOfWeeksInAdvance(weeksInAdvance);
		return recommendationInputDTO;

	}
	
	/**
	 * 
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param startWeek
	 * @param endWeek
	 * @param quarterStartDate
	 * @param quarterEndDate
	 * @param recType
	 * @param weeksInAdvance
	 * @return recommendation input
	 */
	public static RecommendationInputDTO getRecommendationInputDTO(int locationLevelId, int locationId,
			int productLevelId, int productId, int chainId, int divisionId, int leadZoneId, int leadZoneDivisionId,
			String startWeek) {

		RecommendationInputDTO recommendationInputDTO = new RecommendationInputDTO();

		recommendationInputDTO.setLocationLevelId(locationLevelId);
		recommendationInputDTO.setLocationId(locationId);
		recommendationInputDTO.setProductId(productId);
		recommendationInputDTO.setProductLevelId(productLevelId);
		recommendationInputDTO.setChainId(chainId);
		recommendationInputDTO.setDivisionId(divisionId);
		recommendationInputDTO.setLeadZoneDivisionId(leadZoneDivisionId);
		recommendationInputDTO.setLeadZoneId(leadZoneId);
		recommendationInputDTO.setStartWeek(startWeek);

		return recommendationInputDTO;

	}
	
	
	public static void setRecommendationInputDTO(RecommendationInputDTO recommendationInputDTO, int locationLevelId,
			int locationId, int productLevelId, int productId, int chainId, int divisionId, int leadZoneId,
			int leadZoneDivisionId, String startWeek) {

		recommendationInputDTO.setLocationLevelId(locationLevelId);
		recommendationInputDTO.setLocationId(locationId);
		recommendationInputDTO.setProductId(productId);
		recommendationInputDTO.setProductLevelId(productLevelId);
		recommendationInputDTO.setChainId(chainId);
		recommendationInputDTO.setDivisionId(divisionId);
		recommendationInputDTO.setLeadZoneDivisionId(leadZoneDivisionId);
		recommendationInputDTO.setLeadZoneId(leadZoneId);
		recommendationInputDTO.setStartWeek(startWeek);

	}
	
	public static RetailCalendarDTO getCalendarId(int calId){
		RetailCalendarDTO retailCalendarDTO = new RetailCalendarDTO();
		retailCalendarDTO.setCalendarId(calId);
		
		return retailCalendarDTO;
	}
	
	public static RetailCalendarDTO getQuarterDTO(Date startDate){
		// Quarter after X weeks
		RetailCalendarDTO retailCalendarDTO = new RetailCalendarDTO();
		retailCalendarDTO.setStartDate(DateUtil.dateToString(startDate, Constants.APP_DATE_FORMAT));
		retailCalendarDTO.setCalendarId(2005);
		Date endDate = DateUtil.incrementDate(startDate, (13 * 7));
		
		calDatails.put(DateUtil.dateToString(endDate, Constants.APP_DATE_FORMAT), retailCalendarDTO);
		
		endDate = DateUtil.incrementDate(endDate, 6);
		retailCalendarDTO.setEndDate(DateUtil.dateToString(endDate, Constants.APP_DATE_FORMAT));
		
		
		calDatails.put(retailCalendarDTO.getStartDate(), retailCalendarDTO);
		calDatails.put(retailCalendarDTO.getEndDate(), retailCalendarDTO);
		
		return retailCalendarDTO;
		
	}
	
}
