package com.pristine.service.offermgmt.mwr.basedata;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.StrategyDAO;
import com.pristine.dataload.offermgmt.mwr.CommonDataHelper;
import com.pristine.dto.offermgmt.CriteriaDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.dto.offermgmt.mwr.RecommendationInputDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;

public class StrategyService {

	private static Logger logger = Logger.getLogger("BaseDataService");
	
	/**
	 * 
	 * @param conn
	 * @param locationLevelId
	 * @param locationId
	 * @param productLevelId
	 * @param productId
	 * @param leadZoneId
	 * @param leadZoneDivisionId
	 * @param divisionId
	 * @param startDate
	 * @param endDate
	 * @return map of all active strategies for given product and location
	 * @throws OfferManagementException
	 */
	public HashMap<StrategyKey, List<PRStrategyDTO>> getAllActiveStrategies(Connection conn,
			RecommendationInputDTO recommendationInputDTO) throws OfferManagementException {

		com.pristine.service.offermgmt.StrategyService strategyService = new com.pristine.service.offermgmt.StrategyService(
				new ArrayList<>());

		HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap = new HashMap<StrategyKey, List<PRStrategyDTO>>();

		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);
		PRStrategyDTO leadInputDTO = CommonDataHelper.convertRecInputToStrategyInputForLeadZone(recommendationInputDTO);

		if (recommendationInputDTO.getLeadZoneId() > 0) {

			HashMap<StrategyKey, List<PRStrategyDTO>> leadStrategyMap = strategyService.getAllActiveStrategies(conn,
					leadInputDTO, recommendationInputDTO.getLeadZoneDivisionId());

			HashMap<StrategyKey, List<PRStrategyDTO>> dependentStrategyMap = strategyService
					.getAllActiveStrategies(conn, inputDTO, recommendationInputDTO.getDivisionId());

			for (Map.Entry<StrategyKey, List<PRStrategyDTO>> entry : leadStrategyMap.entrySet()) {
				List<PRStrategyDTO> strategyList = new ArrayList<PRStrategyDTO>();
				if (strategyMap.containsKey(entry.getKey())) {
					strategyList = strategyMap.get(entry.getKey());
				}
				strategyList.addAll(entry.getValue());
				strategyMap.put(entry.getKey(), strategyList);
			}

			for (Map.Entry<StrategyKey, List<PRStrategyDTO>> entry : dependentStrategyMap.entrySet()) {
				List<PRStrategyDTO> strategyList = new ArrayList<PRStrategyDTO>();
				if (strategyMap.containsKey(entry.getKey())) {
					strategyList = strategyMap.get(entry.getKey());
				}
				strategyList.addAll(entry.getValue());
				strategyMap.put(entry.getKey(), strategyList);
			}
		} else {
			strategyMap = strategyService.getAllActiveStrategies(conn, inputDTO,
					recommendationInputDTO.getDivisionId());
		}

		return strategyMap;
	}

	/**
	 * 
	 * @return criteria details
	 * @throws GeneralException 
	 */
	public HashMap<Integer, List<CriteriaDTO>> getCriteriaDetailsFromStrategy(Connection conn,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap) throws GeneralException {
		HashMap<Integer, List<CriteriaDTO>> criteriaDetails = new HashMap<>();
		Set<Integer> criteriaSet = new HashSet<>();
		StrategyDAO strategyDAO = new StrategyDAO();
		strategyMap.forEach((strategyKey, strategyList) -> {
			strategyList.forEach(strategy -> {
				if(strategy.getCriteriaId() > 0) {
					criteriaSet.add(strategy.getCriteriaId());	
				}
			});
		});
		
		
		logger.debug("critria list size: " + criteriaSet.size());
		
		if(criteriaSet.size() > 0) {
			List<CriteriaDTO> criteriaList = strategyDAO.getStrategyCriteria(conn, criteriaSet);

			criteriaDetails = (HashMap<Integer, List<CriteriaDTO>>) criteriaList
					.stream().collect(Collectors.groupingBy(CriteriaDTO::getCriteriaId));
	
		}
				
		return criteriaDetails;
	}
	
	/***
	 * 
	 * @param conn
	 * @param recommendationInputDTO
	 * @return
	 * @throws OfferManagementException
	 */

	public List<Long> getAllActiveTempStrategies(Connection conn, RecommendationInputDTO recommendationInputDTO)
			throws OfferManagementException {

		com.pristine.service.offermgmt.StrategyService strategyService = new com.pristine.service.offermgmt.StrategyService(
				new ArrayList<>());
		List<Long> whatIfStartIds = new ArrayList<Long>();

		PRStrategyDTO inputDTO = CommonDataHelper.convertRecInputToStrategyInput(recommendationInputDTO);
		whatIfStartIds = strategyService.getAllActiveTempStrategies(conn, inputDTO,
				recommendationInputDTO.getDivisionId());

		return whatIfStartIds;
	}

	public Long getGlobalStrategy(Connection conn) throws OfferManagementException {

		com.pristine.service.offermgmt.StrategyService strategyService = new com.pristine.service.offermgmt.StrategyService(
				new ArrayList<>());
		Long globalStratID = strategyService.getGlobalStrategy(conn);

		return globalStratID;
	}
	

}
