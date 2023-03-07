package com.pristine.dataload.offermgmt.mwr;

import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dataload.offermgmt.mwr.config.MultiWeekRecConfigSettings;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.mwr.MWRItemDTO;
import com.pristine.dto.offermgmt.mwr.RecWeekKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;

public class CommonDataHelper {
	
	private HashMap<String, RetailCalendarDTO> allWeekCalendarDetails;
	private LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails;
	
	private List<RetailCalendarDTO> fullCalendar;
	
	private RetailCalendarDAO retailCalendarDAO;
	
	private int previousCalID;
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	public void getCommonData(Connection conn, RecommendationInputDTO recommendationInputDTO) throws GeneralException{
		
		retailCalendarDAO = new RetailCalendarDAO();
		
		// 1. Get week calendar ids for all weeks
		setAllWeekCalendarDetails(retailCalendarDAO.getAllWeeks(conn));
		
		// 2. Get week calendar details for moevement history
		setPreviousCalendarDetails(getPreviousCalendarFromDB(conn, recommendationInputDTO));
		
		// 3. Set chain id in recommendation input DTO
		setChainId(conn, recommendationInputDTO);
		
		// 4. Set division id in recommendation input DTO
		setDivisionId(conn, recommendationInputDTO);
		
		// 5. Set lead zone information in recommendation input DTO
		setLeadZoneInformation(conn, recommendationInputDTO);
		
		// 6. Set full calendar
		setFullCalendar(retailCalendarDAO.getFullRetailCalendar(conn));

		// 7.Check if given location id globalzone
		setIsGlobalZone(conn, recommendationInputDTO);
		
		//8.Get previous week's CalId 
		//Added to get Movement for 52 weeks for impact Calculation
		setlastWeekcalId(retailCalendarDAO.getPrevCalendarId(conn,  DateUtil.getWeekStartDate(0), Constants.CALENDAR_WEEK));
	
	}

	public void setIsGlobalZone(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {

		boolean isGlobalZone = new RetailPriceZoneDAO().getIsGlobalZone(conn, recommendationInputDTO.getLocationId());

		// Set Global zone flag
		recommendationInputDTO.setGlobalZone(isGlobalZone);
	}
	/**
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @return calendar data
	 * @throws GeneralException
	 */
	private LinkedHashMap<Integer, RetailCalendarDTO> getPreviousCalendarFromDB(Connection conn,
			RecommendationInputDTO recommendationInputDTO) throws GeneralException {

		int noOfWeeksIMSData = MultiWeekRecConfigSettings.getMwrNumberOfWeeksIMS();
		String curWkStartDate = DateUtil.getWeekStartDate(0);
		LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails = retailCalendarDAO.getAllPreviousWeeks(conn,
				curWkStartDate, noOfWeeksIMSData);

		return previousCalendarDetails;
	}
	
	/**
	 * Sets lead zone information in RecommendtionInputDTO
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setLeadZoneInformation(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {

		// NU:: 23rd Jun 2017,
		// TOPS doesn't want to recommend to dependent zone, but in order to show chain level
		// summary we wanted to run recommendation for all dependent zone, but TOPS doesn't want
		// to spend time in assigning strategy for dependent zone, they will set strategy only
		// for lead zones, they asked to use lead zone strategy. But primary competitor may be
		// different for dependent zone
		int leadZoneId = new PricingEngineDAO().getLeadAndDependentZone(conn, recommendationInputDTO.getLocationId());

		// Set lead zone id
		recommendationInputDTO.setLeadZoneId(leadZoneId);

		// Get lead zone division id
		int leadZoneDivisionId = new RetailPriceZoneDAO().getDivisionIdForZone(conn, leadZoneId);

		// Set lead zone division id

		recommendationInputDTO.setLeadZoneDivisionId(leadZoneDivisionId);

	}

	/**
	 * Sets chain id RecommendtionInputDTO
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setChainId(Connection conn, RecommendationInputDTO recommendationInputDTO) throws GeneralException {
		String chainId = new RetailPriceDAO().getChainId(conn);
		recommendationInputDTO.setChainId(Integer.parseInt(chainId));
	}

	/**
	 * Sets division id RecommendtionInputDTO
	 * 
	 * @param recommendationInputDTO
	 * @throws GeneralException
	 */
	private void setDivisionId(Connection conn, RecommendationInputDTO recommendationInputDTO) throws GeneralException {
		// Get lead zone division id
		int divisionId = new RetailPriceZoneDAO().getDivisionIdForZone(conn, recommendationInputDTO.getLocationId());

		// Set division id
		recommendationInputDTO.setLeadZoneDivisionId(divisionId);
	}

	
	
	/**
	 * 
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param startDate
	 * @param endDate
	 * @return inputDTO
	 */
	public static PRStrategyDTO convertRecInputToStrategyInput(RecommendationInputDTO recommendationInputDTO) {
		// Set inputs
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		inputDTO.setLocationLevelId(recommendationInputDTO.getLocationLevelId());
		inputDTO.setLocationId(recommendationInputDTO.getLocationId());
		inputDTO.setProductLevelId(recommendationInputDTO.getProductLevelId());
		inputDTO.setProductId(recommendationInputDTO.getProductId());
		inputDTO.setStartDate(recommendationInputDTO.getStartWeek());
		inputDTO.setEndDate(recommendationInputDTO.getStartWeek());
		inputDTO.setPriceTestZone(recommendationInputDTO.isPriceTestZone());
		inputDTO.setTempLocationID(recommendationInputDTO.getTempLocationID());
		inputDTO.setGlobalZone(recommendationInputDTO.isGlobalZone());
		return inputDTO;
	}
	
	
	public static PRStrategyDTO convertRecInputToStrategyInputForLeadZone(RecommendationInputDTO recommendationInputDTO) {
		// Set inputs
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		inputDTO.setLocationLevelId(recommendationInputDTO.getLocationLevelId());
		inputDTO.setLocationId(recommendationInputDTO.getLeadZoneId());
		inputDTO.setProductLevelId(recommendationInputDTO.getProductLevelId());
		inputDTO.setProductId(recommendationInputDTO.getProductId());
		inputDTO.setStartDate(recommendationInputDTO.getStartWeek());
		inputDTO.setEndDate(recommendationInputDTO.getStartWeek());

		return inputDTO;
	}
	
	
	public HashMap<String, RetailCalendarDTO> getAllWeekCalendarDetails() {
		return allWeekCalendarDetails;
	}

	public void setAllWeekCalendarDetails(HashMap<String, RetailCalendarDTO> allWeekCalendarDetails) {
		this.allWeekCalendarDetails = allWeekCalendarDetails;
	}

	public LinkedHashMap<Integer, RetailCalendarDTO> getPreviousCalendarDetails() {
		return previousCalendarDetails;
	}

	public void setPreviousCalendarDetails(LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails) {
		this.previousCalendarDetails = previousCalendarDetails;
	}
	
	
	public static Set<RecWeekKey> getRecWeekSet(HashMap<RecWeekKey, HashMap<ItemKey, MWRItemDTO>> weeklyItemDataMap) {
		return weeklyItemDataMap.keySet();
	}

	public List<RetailCalendarDTO> getFullCalendar() {
		return fullCalendar;
	}

	public void setFullCalendar(List<RetailCalendarDTO> fullCalendar) {
		this.fullCalendar = fullCalendar;
	}
	
	public void setlastWeekcalId(int prevCalendarId) {

		this.previousCalID = prevCalendarId;
	}

	public int getPreviousCalID() {
		return previousCalID;
	}

	public void setPreviousCalID(int previousCalID) {
		this.previousCalID = previousCalID;
	}

	/**
	 * 
	 * @param conn 
	 * @param recommendationInputDTO
	 * @throws GeneralException 
	 */
	public void setisPriceTestZone(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws GeneralException {

		HashMap<Integer, Integer> priceTestZoneSet = new RetailPriceZoneDAO().getPriceTestZones(conn);
		if (priceTestZoneSet.containsKey(recommendationInputDTO.getLocationId()))
			recommendationInputDTO.setPriceTestZone(true);
		else
			recommendationInputDTO.setPriceTestZone(false);

	}	

}
